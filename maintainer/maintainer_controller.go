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

package maintainer

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

// Controller schedules and balance tables
// there are 3 main components in the controller, scheduler, ReplicationDB and operator controller
type Controller struct {
	bootstrapped bool

	schedulerController *scheduler.Controller
	operatorController  *operator.Controller
	replicationDB       *replica.ReplicationDB
	messageCenter       messaging.MessageCenter
	nodeManager         *watcher.NodeManager
	tsoClient           replica.TSOClient

	splitter               *split.Splitter
	enableTableAcrossNodes bool
	startCheckpointTs      uint64
	ddlDispatcherID        common.DispatcherID

	cfConfig     *config.ReplicaConfig
	changefeedID common.ChangeFeedID

	taskScheduler threadpool.ThreadPool
	taskHandlers  []*threadpool.TaskHandle
}

func NewController(changefeedID common.ChangeFeedID,
	checkpointTs uint64,
	pdapi pdutil.PDAPIClient,
	tsoClient replica.TSOClient,
	regionCache split.RegionCache,
	taskScheduler threadpool.ThreadPool,
	cfConfig *config.ReplicaConfig,
	ddlSpan *replica.SpanReplication,
	batchSize int, balanceInterval time.Duration,
) *Controller {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	enableTableAcrossNodes := false
	var splitter *split.Splitter
	if cfConfig != nil && cfConfig.Scheduler.EnableTableAcrossNodes {
		enableTableAcrossNodes = true
		splitter = split.NewSplitter(changefeedID, pdapi, regionCache, cfConfig.Scheduler)
	}
	replicaSetDB := replica.NewReplicaSetDB(changefeedID, ddlSpan, enableTableAcrossNodes)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	oc := operator.NewOperatorController(changefeedID, mc, replicaSetDB, nodeManager, batchSize)
	s := &Controller{
		startCheckpointTs:      checkpointTs,
		changefeedID:           changefeedID,
		bootstrapped:           false,
		ddlDispatcherID:        ddlSpan.ID,
		operatorController:     oc,
		messageCenter:          mc,
		replicationDB:          replicaSetDB,
		nodeManager:            nodeManager,
		taskScheduler:          taskScheduler,
		cfConfig:               cfConfig,
		tsoClient:              tsoClient,
		splitter:               splitter,
		enableTableAcrossNodes: enableTableAcrossNodes,
	}
	s.schedulerController = NewScheduleController(changefeedID, batchSize, oc, replicaSetDB, nodeManager, balanceInterval, s.splitter)
	return s
}

// HandleStatus handle the status report from the node
func (c *Controller) HandleStatus(from node.ID, statusList []*heartbeatpb.TableSpanStatus) {
	for _, status := range statusList {
		dispatcherID := common.NewDispatcherIDFromPB(status.ID)
		c.operatorController.UpdateOperatorStatus(dispatcherID, from, status)
		stm := c.GetTask(dispatcherID)
		if stm == nil {
			if status.ComponentStatus != heartbeatpb.ComponentState_Working {
				continue
			}
			if op := c.operatorController.GetOperator(dispatcherID); op == nil {
				// it's normal case when the span is not found in replication db
				// the span is removed from replication db first, so here we only check if the span status is working or not
				log.Warn("no span found, remove it",
					zap.String("changefeed", c.changefeedID.Name()),
					zap.String("from", from.String()),
					zap.Any("status", status),
					zap.String("span", dispatcherID.String()))
				// if the span is not found, and the status is working, we need to remove it from dispatcher
				_ = c.messageCenter.SendCommand(replica.NewRemoveDispatcherMessage(from, c.changefeedID, status.ID))
			}
			continue
		}
		nodeID := stm.GetNodeID()
		if nodeID != from {
			// todo: handle the case that the node id is mismatch
			log.Warn("node id not match",
				zap.String("changefeed", c.changefeedID.Name()),
				zap.Any("from", from),
				zap.Stringer("node", nodeID))
			continue
		}
		c.replicationDB.UpdateStatus(stm, status)
	}
}

func (c *Controller) GetTasksBySchemaID(schemaID int64) []*replica.SpanReplication {
	return c.replicationDB.GetTasksBySchemaID(schemaID)
}

func (c *Controller) GetTaskSizeBySchemaID(schemaID int64) int {
	return c.replicationDB.GetTaskSizeBySchemaID(schemaID)
}

func (c *Controller) GetAllNodes() []node.ID {
	aliveNodes := c.nodeManager.GetAliveNodes()
	nodes := make([]node.ID, 0, len(aliveNodes))
	for id := range aliveNodes {
		nodes = append(nodes, id)
	}
	return nodes
}

func (c *Controller) AddNewTable(table commonEvent.Table, startTs uint64) {
	if c.replicationDB.IsTableExists(table.TableID) {
		log.Warn("table already add, ignore",
			zap.String("changefeed", c.changefeedID.Name()),
			zap.Int64("schema", table.SchemaID),
			zap.Int64("table", table.TableID))
		return
	}
	span := spanz.TableIDToComparableSpan(table.TableID)
	tableSpan := &heartbeatpb.TableSpan{
		TableID:  table.TableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}
	tableSpans := []*heartbeatpb.TableSpan{tableSpan}
	if c.enableTableAcrossNodes {
		// split the whole table span base on the configuration, todo: background split table
		tableSpans = c.splitter.SplitSpans(context.Background(), tableSpan, len(c.nodeManager.GetAliveNodes()), 0)
	}
	c.addNewSpans(table.SchemaID, tableSpans, startTs)
}

// FinishBootstrap adds working state tasks to this controller directly,
// it reported by the bootstrap response
func (c *Controller) FinishBootstrap(
	cachedResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	isMysqlCompatibleBackend bool,
) (*Barrier, *heartbeatpb.MaintainerPostBootstrapRequest, error) {
	if c.bootstrapped {
		log.Panic("already bootstrapped",
			zap.String("changefeed", c.changefeedID.Name()),
			zap.Any("workingMap", cachedResp))
	}

	log.Info("all nodes have sent bootstrap response",
		zap.String("changefeed", c.changefeedID.Name()),
		zap.Int("size", len(cachedResp)))

	// 1. get the real start ts from the table trigger event dispatcher
	startTs := uint64(0)
	for node, resp := range cachedResp {
		log.Info("received bootstrap response",
			zap.Any("changefeed", resp.ChangefeedID),
			zap.Any("node", node),
			zap.Any("startTs", resp.CheckpointTs))
		if resp.CheckpointTs > startTs {
			startTs = resp.CheckpointTs
			// update the ddl dispatcher status
			status := c.replicationDB.GetDDLDispatcher().GetStatus()
			status.CheckpointTs = startTs
			c.replicationDB.UpdateStatus(c.replicationDB.GetDDLDispatcher(), status)
		}

	}
	if startTs == 0 {
		log.Panic("cant not found the start ts from the bootstrap response",
			zap.String("changefeed", c.changefeedID.Name()))
	}
	// 2. load tables from schema store using the start ts
	tables, err := c.loadTables(startTs)
	if err != nil {
		log.Error("load table from scheme store failed",
			zap.String("changefeed", c.changefeedID.Name()),
			zap.Error(err))
		return nil, nil, errors.Trace(err)
	}

	workingMap := make(map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication])
	for server, bootstrapMsg := range cachedResp {
		log.Info("received bootstrap response",
			zap.String("changefeed", c.changefeedID.Name()),
			zap.Any("server", server),
			zap.Int("size", len(bootstrapMsg.Spans)))
		for _, info := range bootstrapMsg.Spans {
			dispatcherID := common.NewDispatcherIDFromPB(info.ID)
			if dispatcherID == c.ddlDispatcherID {
				log.Info(
					"skip table trigger event dispatcher",
					zap.String("changefeed", c.changefeedID.Name()),
					zap.String("dispatcher", dispatcherID.String()),
					zap.String("server", server.String()))
				continue
			}
			status := &heartbeatpb.TableSpanStatus{
				ComponentStatus: info.ComponentStatus,
				ID:              info.ID,
				CheckpointTs:    info.CheckpointTs,
			}
			span := info.Span

			// working on remote, the state must be absent or working since it's reported by remote
			stm := replica.NewWorkingReplicaSet(c.changefeedID, dispatcherID, c.tsoClient, info.SchemaID, span, status, server)
			tableMap, ok := workingMap[span.TableID]
			if !ok {
				tableMap = utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](heartbeatpb.LessTableSpan)
				workingMap[span.TableID] = tableMap
			}
			tableMap.ReplaceOrInsert(span, stm)
		}
	}

	schemaInfos := map[int64]*heartbeatpb.SchemaInfo{}
	for _, table := range tables {
		if _, ok := schemaInfos[table.SchemaID]; !ok {
			schemaInfos[table.SchemaID] = getSchemaInfo(table, isMysqlCompatibleBackend)
		}
		tableInfo := getTableInfo(table, isMysqlCompatibleBackend)
		schemaInfos[table.SchemaID].Tables = append(schemaInfos[table.SchemaID].Tables, tableInfo)

		tableMap, ok := workingMap[table.TableID]
		if !ok {
			c.AddNewTable(table, c.startCheckpointTs)
		} else {
			span := spanz.TableIDToComparableSpan(table.TableID)
			tableSpan := &heartbeatpb.TableSpan{
				TableID:  table.TableID,
				StartKey: span.StartKey,
				EndKey:   span.EndKey,
			}
			log.Info("table already working in other server",
				zap.String("changefeed", c.changefeedID.Name()),
				zap.Int64("tableID", table.TableID))
			c.addWorkingSpans(tableMap)
			if c.enableTableAcrossNodes {
				holes := split.FindHoles(tableMap, tableSpan)
				// todo: split the hole
				c.addNewSpans(table.SchemaID, holes, c.startCheckpointTs)
			}
			// delete it
			delete(workingMap, table.TableID)
		}
	}
	// tables that not included in init table map, but we get from different nodes.
	// that can happen such as:
	// node1 with table trigger event dispatcher, node2 with table1, and both receive drop table1 ddl
	// table trigger event dispatcher write the ddl, while node2 not pass it yet
	// then node1 restarts.
	// node1 will get the startTs = ddl1.ts, and then the table1 will not be included in the initial table map
	// so we just ignore the table1 dispatcher.
	// here tableID is physical table id
	for tableID := range workingMap {
		log.Warn("found a tables not in initial table map",
			zap.String("changefeed", c.changefeedID.Name()),
			zap.Int64("id", tableID))
	}

	// rebuild barrier status
	barrier := NewBarrier(c, c.cfConfig.Scheduler.EnableTableAcrossNodes)
	barrier.HandleBootstrapResponse(cachedResp)

	// start scheduler
	c.taskHandlers = append(c.taskHandlers, c.schedulerController.Start(c.taskScheduler)...)
	// start operator controller
	c.taskHandlers = append(c.taskHandlers, c.taskScheduler.Submit(c.operatorController, time.Now()))

	c.bootstrapped = true

	initSchemaInfos := make([]*heartbeatpb.SchemaInfo, 0, len(schemaInfos))
	for _, info := range schemaInfos {
		initSchemaInfos = append(initSchemaInfos, info)
	}
	return barrier, &heartbeatpb.MaintainerPostBootstrapRequest{
		ChangefeedID:                  c.changefeedID.ToPB(),
		TableTriggerEventDispatcherId: c.ddlDispatcherID.ToPB(),
		Schemas:                       initSchemaInfos,
	}, nil
}

func (c *Controller) Stop() {
	for _, handler := range c.taskHandlers {
		handler.Cancel()
	}
}

// GetTask queries a task by dispatcherID, return nil if not found
func (c *Controller) GetTask(dispatcherID common.DispatcherID) *replica.SpanReplication {
	return c.replicationDB.GetTaskByID(dispatcherID)
}

// RemoveAllTasks remove all tasks
func (c *Controller) RemoveAllTasks() {
	c.operatorController.RemoveAllTasks()
}

// RemoveTasksBySchemaID remove all tasks by schema id
func (c *Controller) RemoveTasksBySchemaID(schemaID int64) {
	c.operatorController.RemoveTasksBySchemaID(schemaID)
}

// RemoveTasksByTableIDs remove all tasks by table id
func (c *Controller) RemoveTasksByTableIDs(tables ...int64) {
	c.operatorController.RemoveTasksByTableIDs(tables...)
}

// GetTasksByTableIDs get all tasks by table id
func (c *Controller) GetTasksByTableIDs(tableIDs ...int64) []*replica.SpanReplication {
	return c.replicationDB.GetTasksByTableIDs(tableIDs...)
}

// GetAllTasks get all tasks
func (c *Controller) GetAllTasks() []*replica.SpanReplication {
	return c.replicationDB.GetAllTasks()
}

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map
// it called when rename a table to another schema
func (c *Controller) UpdateSchemaID(tableID, newSchemaID int64) {
	c.replicationDB.UpdateSchemaID(tableID, newSchemaID)
}

// RemoveNode is called when a node is removed
func (c *Controller) RemoveNode(id node.ID) {
	c.operatorController.OnNodeRemoved(id)
}

// ScheduleFinished return false if not all task are running in working state
func (c *Controller) ScheduleFinished() bool {
	return c.replicationDB.GetAbsentSize() == 0 && c.operatorController.OperatorSize() == 0
}

func (c *Controller) TaskSize() int {
	return c.replicationDB.TaskSize()
}

func (c *Controller) GetTaskSizeByNodeID(id node.ID) int {
	return c.replicationDB.GetTaskSizeByNodeID(id)
}

func (c *Controller) addWorkingSpans(tableMap utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]) {
	tableMap.Ascend(func(span *heartbeatpb.TableSpan, stm *replica.SpanReplication) bool {
		c.replicationDB.AddReplicatingSpan(stm)
		return true
	})
}

func (c *Controller) addNewSpans(schemaID int64, tableSpans []*heartbeatpb.TableSpan, startTs uint64) {
	for _, span := range tableSpans {
		dispatcherID := common.NewDispatcherID()
		replicaSet := replica.NewReplicaSet(c.changefeedID,
			dispatcherID, c.tsoClient, schemaID, span, startTs)
		c.replicationDB.AddAbsentReplicaSet(replicaSet)
	}
}

func (c *Controller) loadTables(startTs uint64) ([]commonEvent.Table, error) {
	// todo: do we need to set timezone here?
	f, err := filter.NewFilter(c.cfConfig.Filter, "", c.cfConfig.ForceReplicate)
	if err != nil {
		return nil, errors.Cause(err)
	}

	schemaStore := appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore)
	tables, err := schemaStore.GetAllPhysicalTables(startTs, f)
	log.Info("get table ids", zap.Int("count", len(tables)), zap.String("changefeed", c.changefeedID.Name()))
	return tables, err
}

// only for test
// moveTable is used for inner api(which just for make test cases convience) to force move a table to a target node.
// moveTable only works for the complete table, not for the table splited.
func (c *Controller) moveTable(tableId int64, targetNode node.ID) error {
	if !c.replicationDB.IsTableExists(tableId) {
		// the table is not exist in this node
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("tableID", tableId)
	}

	nodes := c.nodeManager.GetAliveNodes()
	hasNode := false
	for _, node := range nodes {
		if node.ID == targetNode {
			hasNode = true
			break
		}
	}
	if !hasNode {
		return apperror.ErrNodeIsNotFound.GenWithStackByArgs("targetNode", targetNode)
	}

	replications := c.replicationDB.GetTasksByTableIDs(tableId)
	if len(replications) != 1 {
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("unexpected number of replications found for table in this node; tableID is %s, replication count is %s", tableId, len(replications))
	}

	replication := replications[0]

	op := c.operatorController.NewMoveOperator(replication, replication.GetNodeID(), targetNode)
	c.operatorController.AddOperator(op)

	// check the op is finished or not
	count := 0
	maxTry := 30
	for !op.IsFinished() && count < maxTry {
		time.Sleep(500 * time.Millisecond)
		count += 1
		log.Info("wait for move table operator finished", zap.Int("count", count))
	}

	if !op.IsFinished() {
		return apperror.ErrMoveTableTimeout.GenWithStackByArgs("move table operator is timeout")
	}

	return nil
}

func getSchemaInfo(table commonEvent.Table, isMysqlCompatibleBackend bool) *heartbeatpb.SchemaInfo {
	schemaInfo := &heartbeatpb.SchemaInfo{}
	if isMysqlCompatibleBackend {
		schemaInfo.SchemaID = table.SchemaID
	} else {
		schemaInfo.SchemaName = table.SchemaName
	}
	return schemaInfo
}

func getTableInfo(table commonEvent.Table, isMysqlCompatibleBackend bool) *heartbeatpb.TableInfo {
	tableInfo := &heartbeatpb.TableInfo{}
	if isMysqlCompatibleBackend {
		tableInfo.TableID = table.TableID
	} else {
		tableInfo.TableName = table.TableName
	}
	return tableInfo
}
