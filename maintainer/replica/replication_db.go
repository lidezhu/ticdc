// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package replica

import (
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"go.uber.org/zap"
)

var _ replica.ReplicationDB[common.DispatcherID, *SpanReplication] = &ReplicationDB{}

// ReplicationDB is an in memory data struct that maintains the replication spans
type ReplicationDB struct {
	// for log ID
	changefeedID common.ChangeFeedID
	// allTasks maintains all the span tasks, it included the table trigger
	allTasks map[common.DispatcherID]*SpanReplication
	// group the tasks by the schema id, and table id for fast access
	schemaTasks map[int64]map[common.DispatcherID]*SpanReplication
	tableTasks  map[int64]map[common.DispatcherID]*SpanReplication
	// ReplicationDB is used for tracking scheduling status, the ddl dispatcher is
	// not included since it doesn't need to be scheduled
	replica.ReplicationDB[common.DispatcherID, *SpanReplication]

	ddlSpan *SpanReplication
	// LOCK protects the above maps
	lock            sync.RWMutex
	newGroupChecker func(groupID replica.GroupID) replica.GroupChecker[common.DispatcherID, *SpanReplication]
}

// NewReplicaSetDB creates a new ReplicationDB and initializes the maps
func NewReplicaSetDB(
	changefeedID common.ChangeFeedID, ddlSpan *SpanReplication, enableTableAcrossNodes bool,
) *ReplicationDB {
	db := &ReplicationDB{
		changefeedID:    changefeedID,
		ddlSpan:         ddlSpan,
		newGroupChecker: getNewGroupChecker(changefeedID, enableTableAcrossNodes),
	}
	db.reset()
	db.putDDLDispatcher(db.ddlSpan)
	return db
}

// GetTaskByID returns the replica set by the id, it will search the replicating, scheduling and absent map
func (db *ReplicationDB) GetTaskByID(id common.DispatcherID) *SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.allTasks[id]
}

// TaskSize returns the total task size in the db, it includes replicating, scheduling and absent tasks
func (db *ReplicationDB) TaskSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	// the ddl span is a special span, we don't need to schedule it
	return len(db.allTasks)
}

// TryRemoveAll removes non-scheduled tasks from the db and return the scheduled tasks
func (db *ReplicationDB) TryRemoveAll() []*SpanReplication {
	db.lock.Lock()
	defer db.lock.Unlock()

	tasks := make([]*SpanReplication, 0)
	// we need to add the replicating and scheduling tasks to the list, and then reset the db
	tasks = append(tasks, db.GetReplicatingWithoutLock()...)
	tasks = append(tasks, db.GetSchedulingWithoutLock()...)

	db.reset()
	db.putDDLDispatcher(db.ddlSpan)
	return tasks
}

// TryRemoveByTableIDs removes non-scheduled tasks from the db and return the scheduled tasks
func (db *ReplicationDB) TryRemoveByTableIDs(tableIDs ...int64) []*SpanReplication {
	db.lock.Lock()
	defer db.lock.Unlock()

	tasks := make([]*SpanReplication, 0)
	for _, tblID := range tableIDs {
		for _, task := range db.tableTasks[tblID] {
			db.removeSpanUnLock(task)
			// the task is scheduled
			if task.GetNodeID() != "" {
				tasks = append(tasks, task)
			}
		}
	}
	return tasks
}

// TryRemoveBySchemaID removes non-scheduled tasks from the db and return the scheduled tasks
func (db *ReplicationDB) TryRemoveBySchemaID(schemaID int64) []*SpanReplication {
	db.lock.Lock()
	defer db.lock.Unlock()

	tasks := make([]*SpanReplication, 0)
	for _, task := range db.schemaTasks[schemaID] {
		db.removeSpanUnLock(task)
		// the task is scheduled
		if task.GetNodeID() != "" {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// GetTasksByTableIDs returns the spans by the table ids
func (db *ReplicationDB) GetTasksByTableIDs(tableIDs ...int64) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var tasks []*SpanReplication
	for _, tableID := range tableIDs {
		for _, task := range db.tableTasks[tableID] {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// GetAllTasks returns all the spans in the db, it's used when the block event type is all, it will return the ddl span
func (db *ReplicationDB) GetAllTasks() []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	tasks := make([]*SpanReplication, 0, len(db.allTasks))
	for _, task := range db.allTasks {
		tasks = append(tasks, task)
	}
	return tasks
}

// IsTableExists checks if the table exists in the db
func (db *ReplicationDB) IsTableExists(tableID int64) bool {
	db.lock.RLock()
	defer db.lock.RUnlock()

	tm, ok := db.tableTasks[tableID]
	return ok && len(tm) > 0
}

// GetTaskSizeBySchemaID returns the size of the task by the schema id
func (db *ReplicationDB) GetTaskSizeBySchemaID(schemaID int64) int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sm, ok := db.schemaTasks[schemaID]
	if ok {
		return len(sm)
	}
	return 0
}

// GetTasksBySchemaID returns the spans by the schema id
func (db *ReplicationDB) GetTasksBySchemaID(schemaID int64) []*SpanReplication {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sm, ok := db.schemaTasks[schemaID]
	if !ok {
		return nil
	}
	replicaSets := make([]*SpanReplication, 0, len(sm))
	for _, v := range sm {
		replicaSets = append(replicaSets, v)
	}
	return replicaSets
}

// ReplaceReplicaSet replaces the old replica set with the new spans
func (db *ReplicationDB) ReplaceReplicaSet(oldReplications []*SpanReplication, newSpans []*heartbeatpb.TableSpan, checkpointTs uint64) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// first check  the old replica set exists, if not, return false
	for _, old := range oldReplications {
		if _, ok := db.allTasks[old.ID]; !ok {
			log.Panic("old replica set not found",
				zap.String("changefeed", db.changefeedID.Name()),
				zap.String("span", old.ID.String()))
		}
		oldCheckpointTs := old.GetStatus().GetCheckpointTs()
		if checkpointTs > oldCheckpointTs {
			checkpointTs = oldCheckpointTs
		}
		db.removeSpanUnLock(old)
	}

	var news []*SpanReplication
	old := oldReplications[0]
	for _, span := range newSpans {
		new := NewReplicaSet(
			old.ChangefeedID,
			common.NewDispatcherID(),
			old.GetTsoClient(),
			old.GetSchemaID(),
			span, checkpointTs)
		news = append(news, new)
	}
	// insert the new replica set
	db.addAbsentReplicaSetUnLock(news...)
}

// AddReplicatingSpan adds a replicating span to the replicating map, that means the span is already scheduled to a dispatcher
func (db *ReplicationDB) AddReplicatingSpan(span *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.allTasks[span.ID] = span
	db.addToSchemaAndTableMap(span)
	db.AddReplicatingWithoutLock(span)
}

// AddAbsentReplicaSet adds spans to the absent map
func (db *ReplicationDB) AddAbsentReplicaSet(spans ...*SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.addAbsentReplicaSetUnLock(spans...)
}

// MarkSpanAbsent move the span to the absent status
func (db *ReplicationDB) MarkSpanAbsent(span *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.MarkAbsentWithoutLock(span)
}

// MarkSpanScheduling move the span to the scheduling map
func (db *ReplicationDB) MarkSpanScheduling(span *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.MarkSchedulingWithoutLock(span)
}

// MarkSpanReplicating move the span to the replicating map
func (db *ReplicationDB) MarkSpanReplicating(span *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.MarkReplicatingWithoutLock(span)
}

// ForceRemove remove the span from the db
func (db *ReplicationDB) ForceRemove(id common.DispatcherID) {
	db.lock.Lock()
	defer db.lock.Unlock()
	span, ok := db.allTasks[id]
	if !ok {
		log.Warn("span not found, ignore remove action",
			zap.String("changefeed", db.changefeedID.Name()),
			zap.String("span", id.String()))
		return
	}

	log.Info("remove span",
		zap.String("changefeed", db.changefeedID.Name()),
		zap.String("span", id.String()))
	db.removeSpanUnLock(span)
}

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map
// it called when rename a table to another schema
func (db *ReplicationDB) UpdateSchemaID(tableID, newSchemaID int64) {
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, replicaSet := range db.tableTasks[tableID] {
		oldSchemaID := replicaSet.GetSchemaID()
		// update schemaID
		replicaSet.SetSchemaID(newSchemaID)

		// update schema map
		schemaMap, ok := db.schemaTasks[oldSchemaID]
		if ok {
			delete(schemaMap, replicaSet.ID)
			// clear the map if empty
			if len(schemaMap) == 0 {
				delete(db.schemaTasks, oldSchemaID)
			}
		}
		// add it to new schema map
		newMap, ok := db.schemaTasks[newSchemaID]
		if !ok {
			newMap = make(map[common.DispatcherID]*SpanReplication)
			db.schemaTasks[newSchemaID] = newMap
		}
		newMap[replicaSet.ID] = replicaSet
	}
}

func (db *ReplicationDB) UpdateStatus(span *SpanReplication, status *heartbeatpb.TableSpanStatus) {
	span.UpdateStatus(status)
	checker := db.GetGroupChecker(span.GetGroupID()) // Note: need RLock here

	db.lock.Lock()
	defer db.lock.Unlock()
	checker.UpdateStatus(span)
}

// BindSpanToNode binds the span to new node, it will remove the span from the old node and add it to the new node
// It also marks the span as scheduling.
func (db *ReplicationDB) BindSpanToNode(old, new node.ID, span *SpanReplication) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.BindReplicaToNodeWithoutLock(old, new, span)
}

// addAbsentReplicaSetUnLock adds spans to absent map
func (db *ReplicationDB) addAbsentReplicaSetUnLock(spans ...*SpanReplication) {
	for _, span := range spans {
		db.allTasks[span.ID] = span
		db.AddAbsentWithoutLock(span)
		db.addToSchemaAndTableMap(span)
	}
}

// removeSpanUnLock removes the spans from the db without lock
func (db *ReplicationDB) removeSpanUnLock(spans ...*SpanReplication) {
	for _, span := range spans {
		db.RemoveReplicaWithoutLock(span)

		tableID := span.Span.TableID
		schemaID := span.GetSchemaID()
		delete(db.schemaTasks[schemaID], span.ID)
		delete(db.tableTasks[tableID], span.ID)
		if len(db.schemaTasks[schemaID]) == 0 {
			delete(db.schemaTasks, schemaID)
		}
		if len(db.tableTasks[tableID]) == 0 {
			delete(db.tableTasks, tableID)
		}
		delete(db.allTasks, span.ID)
	}
}

// addToSchemaAndTableMap adds the span to the schema and table map
func (db *ReplicationDB) addToSchemaAndTableMap(span *SpanReplication) {
	tableID := span.Span.TableID
	schemaID := span.GetSchemaID()
	// modify the schema map
	schemaMap, ok := db.schemaTasks[schemaID]
	if !ok {
		schemaMap = make(map[common.DispatcherID]*SpanReplication)
		db.schemaTasks[schemaID] = schemaMap
	}
	schemaMap[span.ID] = span

	// modify the table map
	tableMap, ok := db.tableTasks[tableID]
	if !ok {
		tableMap = make(map[common.DispatcherID]*SpanReplication)
		db.tableTasks[tableID] = tableMap
	}
	tableMap[span.ID] = span
}

func (db *ReplicationDB) GetAbsentForTest(_ []*SpanReplication, maxSize int) []*SpanReplication {
	ret := db.GetAbsent()
	maxSize = min(maxSize, len(ret))
	return ret[:maxSize]
}

// Optimize the lock usage, maybe control the lock within checker
func (db *ReplicationDB) CheckByGroup(groupID replica.GroupID, batch int) replica.GroupCheckResult {
	checker := db.GetGroupChecker(groupID)

	db.lock.RLock()
	defer db.lock.RUnlock()
	return checker.Check(batch)
}

func (db *ReplicationDB) withRLock(action func()) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	action()
}

// reset resets the maps of ReplicationDB
func (db *ReplicationDB) reset() {
	db.schemaTasks = make(map[int64]map[common.DispatcherID]*SpanReplication)
	db.tableTasks = make(map[int64]map[common.DispatcherID]*SpanReplication)
	db.allTasks = make(map[common.DispatcherID]*SpanReplication)
	db.ReplicationDB = replica.NewReplicationDB[common.DispatcherID, *SpanReplication](db.changefeedID.String(),
		db.withRLock, db.newGroupChecker)
}

func (db *ReplicationDB) putDDLDispatcher(ddlSpan *SpanReplication) {
	// we don't need to schedule the ddl span, but added it to the allTasks map, so we can query it by id
	db.allTasks[ddlSpan.ID] = ddlSpan
	// dispatcher will report a block event with table ID 0,
	// so we need to add it to the table map
	db.tableTasks[ddlSpan.Span.TableID] = map[common.DispatcherID]*SpanReplication{
		ddlSpan.ID: ddlSpan,
	}
	// also put it to the schema map
	db.schemaTasks[ddlSpan.schemaID] = map[common.DispatcherID]*SpanReplication{
		ddlSpan.ID: ddlSpan,
	}
}

func (db *ReplicationDB) GetDDLDispatcher() *SpanReplication {
	return db.ddlSpan
}
