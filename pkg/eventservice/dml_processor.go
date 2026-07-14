// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventservice

import (
	"io"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

// TxnEvent represents a transaction, it may generates one or multiple DMLEvents
type TxnEvent struct {
	BatchDML                 *event.BatchDMLEvent
	CurrentDMLEvent          *event.DMLEvent
	DMLEventMaxRows          int32
	DMLEventMaxBytes         int64
	rawKVBytes               int64
	largeTxnThresholdInBytes int64
	shouldSplitTxn           bool
}

func newTxnEvent(
	batchDML *event.BatchDMLEvent,
	dispatcherID common.DispatcherID,
	tableID int64,
	tableInfo *common.TableInfo,
	startTs uint64,
	commitTs uint64,
	shouldSplitTxn bool,
) (*TxnEvent, error) {
	serverConfig := config.GetGlobalServerConfig()
	txn := &TxnEvent{
		BatchDML:                 batchDML,
		CurrentDMLEvent:          event.NewDMLEvent(dispatcherID, tableID, startTs, commitTs, tableInfo),
		DMLEventMaxRows:          serverConfig.Debug.EventService.DMLEventMaxRows,
		DMLEventMaxBytes:         serverConfig.Debug.EventService.DMLEventMaxBytes,
		largeTxnThresholdInBytes: serverConfig.Debug.EventService.LargeTxnThresholdInBytes,
		shouldSplitTxn:           shouldSplitTxn,
	}
	return txn, txn.BatchDML.AppendDMLEvent(txn.CurrentDMLEvent)
}

func (t *TxnEvent) AppendRow(
	rawEvent *common.RawKVEntry,
	decode func(
		rawKv *common.RawKVEntry,
		tableInfo *common.TableInfo,
		chk *chunk.Chunk,
	) (int, *integrity.Checksum, error),
	filter filter.Filter,
	filterContext filter.DMLFilterContext,
) error {
	if t.shouldSplitTxn && (t.CurrentDMLEvent.Len() >= t.DMLEventMaxRows || t.CurrentDMLEvent.GetSize() >= t.DMLEventMaxBytes) {
		newDMLEvent := event.NewDMLEvent(
			t.CurrentDMLEvent.DispatcherID,
			t.CurrentDMLEvent.PhysicalTableID,
			t.CurrentDMLEvent.StartTs,
			t.CurrentDMLEvent.CommitTs,
			t.CurrentDMLEvent.TableInfo)
		t.CurrentDMLEvent = newDMLEvent
		err := t.BatchDML.AppendDMLEvent(newDMLEvent)
		if err != nil {
			return err
		}
	}
	return t.CurrentDMLEvent.AppendRow(rawEvent, decode, filter, filterContext)
}

func (t *TxnEvent) observeRawKVBytes(rawEvent *common.RawKVEntry) {
	t.rawKVBytes += rawEvent.GetSize()
}

func (t *TxnEvent) exceedsLargeTxnThreshold() bool {
	return t.shouldSplitTxn && t.rawKVBytes > t.largeTxnThresholdInBytes
}

// dmlTypeFilterCacheSize follows common.RowType iota values: delete, insert, update.
const dmlTypeFilterCacheSize = int(common.RowTypeUpdate) + 1

// dmlProcessor handles DML event processing and batching
type dmlProcessor struct {
	mounter      event.Mounter
	schemaGetter schemaGetter

	filter         filter.Filter
	filterContext  filter.DMLFilterContext
	dispatcherStat *dispatcherStat
	spillDir       string

	// dmlTypeFilterCache caches the pre-decode filter result within the current transaction.
	// The cache is reset when a new transaction starts. It is safe because tableInfo
	// and startTs are fixed for the current transaction.
	dmlTypeFilterCache [dmlTypeFilterCacheSize]struct {
		valid  bool
		ignore bool
	}

	// insertRowCache is used to cache the split update event's insert part of the current transaction.
	// It will be used to append to the current DML event when the transaction is finished.
	// And it will be cleared when the transaction is finished.
	insertRowCache []*common.RawKVEntry

	// currentTxn is the transaction that is handling now
	currentTxn *TxnEvent

	batchDML             *event.BatchDMLEvent
	outputRawChangeEvent bool
	mode                 int64
}

// newDMLProcessor creates a new DML processor
func newDMLProcessor(
	mounter event.Mounter, schemaGetter schemaGetter,
	dmlFilter filter.Filter, outputRawChangeEvent bool, mode int64,
	enableIgnoreUpdateOnlyColumns bool,
) *dmlProcessor {
	filterContext := filter.DMLFilterContext{}
	if enableIgnoreUpdateOnlyColumns {
		filterContext.EnableIgnoreUpdateOnlyColumns = true
	}
	return &dmlProcessor{
		mounter:              mounter,
		schemaGetter:         schemaGetter,
		filter:               dmlFilter,
		filterContext:        filterContext,
		batchDML:             event.NewBatchDMLEvent(),
		insertRowCache:       make([]*common.RawKVEntry, 0),
		spillDir:             getLargeTxnInsertSpillDir(),
		outputRawChangeEvent: outputRawChangeEvent,
		mode:                 mode,
	}
}

// startTxn should be called after flush the current transaction
func (p *dmlProcessor) startTxn(
	dispatcherID common.DispatcherID,
	tableID int64,
	tableInfo *common.TableInfo,
	startTs uint64,
	commitTs uint64,
	shouldSplitTxn bool,
) error {
	if p.currentTxn != nil {
		log.Panic("there is a transaction not flushed yet")
	}
	p.resetDMLTypeFilterCache()
	var err error
	p.currentTxn, err = newTxnEvent(p.batchDML, dispatcherID, tableID, tableInfo, startTs, commitTs, shouldSplitTxn)
	return err
}

func (p *dmlProcessor) commitTxn() error {
	if err := p.flushCachedInsertRows(); err != nil {
		return err
	}
	p.currentTxn = nil
	return nil
}

func (p *dmlProcessor) flushCachedInsertRows() error {
	if p.currentTxn == nil || len(p.insertRowCache) == 0 {
		return nil
	}
	for _, insertRow := range p.insertRowCache {
		if err := p.currentTxn.AppendRow(insertRow, p.mounter.DecodeToChunk, p.filter, p.filterContext); err != nil {
			return err
		}
	}
	p.insertRowCache = make([]*common.RawKVEntry, 0)
	return nil
}

func (p *dmlProcessor) spillCachedInsertRows() error {
	if len(p.insertRowCache) == 0 {
		return nil
	}
	state, err := p.getOrCreateLargeTxnState()
	if err != nil {
		return err
	}
	for _, insertRow := range p.insertRowCache {
		if err := state.appendInsert(insertRow); err != nil {
			return err
		}
	}
	p.insertRowCache = make([]*common.RawKVEntry, 0)
	return nil
}

func (p *dmlProcessor) shouldSpillSplitUpdateInsert() bool {
	if p.currentTxn == nil {
		return false
	}
	return p.currentTxn.exceedsLargeTxnThreshold() || p.hasSpilledInsertsForCurrentTxn()
}

func (p *dmlProcessor) hasSpilledInsertsForCurrentTxn() bool {
	if p.currentTxn == nil || p.dispatcherStat == nil {
		return false
	}
	current := p.currentTxn.CurrentDMLEvent
	return p.hasLargeTxnState(current.GetStartTs(), current.GetCommitTs())
}

func (p *dmlProcessor) hasLargeTxnState(startTs uint64, commitTs uint64) bool {
	if p.dispatcherStat == nil {
		return false
	}
	state := p.dispatcherStat.getLargeTxnState()
	return state != nil &&
		state.startTs == startTs &&
		state.commitTs == commitTs &&
		state.getPhase() == largeTxnScanPhaseOriginal
}

func (p *dmlProcessor) flushSpilledInsertsIntoCurrentTxn() error {
	if p.currentTxn == nil || p.dispatcherStat == nil {
		return nil
	}
	state := p.dispatcherStat.getLargeTxnState()
	if state == nil || state.getPhase() != largeTxnScanPhaseOriginal {
		return nil
	}
	for {
		insertRow, err := state.nextInsert()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return p.dispatcherStat.cleanupLargeTxnState()
			}
			return err
		}
		if err := p.appendInsertRow(insertRow); err != nil {
			return err
		}
	}
}

func (p *dmlProcessor) getOrCreateLargeTxnState() (*largeTxnScanState, error) {
	if p.dispatcherStat == nil {
		return nil, errors.ErrSpillFileOp.GenWithStackByArgs(
			"dispatcher stat is required for large txn update spill")
	}
	currentDML := p.currentTxn.CurrentDMLEvent
	return p.dispatcherStat.getOrCreateLargeTxnState(
		p.spillDir,
		currentDML.GetTableID(),
		currentDML.TableInfo,
		currentDML.GetStartTs(),
		currentDML.GetCommitTs(),
	)
}

func (p *dmlProcessor) appendInsertRow(rawEvent *common.RawKVEntry) error {
	if p.currentTxn == nil {
		log.Panic("no current DML event to append to")
	}
	rawEvent.Key = event.RemoveKeyspacePrefix(rawEvent.Key)
	return p.currentTxn.AppendRow(rawEvent, p.mounter.DecodeToChunk, p.filter, p.filterContext)
}

// appendRow appends a row to the current DML event.
//
// This method processes a raw KV entry and appends it to the current DML event. It handles
// different types of operations (insert, delete, update) with special handling for updates
// that modify unique key values.
//
// Parameters:
//   - rawEvent: The raw KV entry containing the row data and operation type
//
// Returns:
//   - error: Returns an error if:
//   - Unique key change detection fails
//   - Update split operation fails
//   - Row append operation fails
//
// The method follows this logic:
// 1. Checks if there's a current DML event to append to
// 2. For non-update operations, directly appends the row
// 3. For update operations:
//   - Checks if the update modifies any unique key values
//   - If unique keys are modified, splits the update into delete+insert operations
//   - Caches the insert part for later processing
//   - Appends the delete part to the current event
//
// 4. For normal updates (no unique key changes), appends the row directly
func (p *dmlProcessor) appendRow(rawEvent *common.RawKVEntry) error {
	if p.currentTxn == nil {
		log.Panic("no current DML event to append to")
	}

	rawEvent.Key = event.RemoveKeyspacePrefix(rawEvent.Key)
	p.currentTxn.observeRawKVBytes(rawEvent)
	if p.shouldSpillSplitUpdateInsert() {
		if err := p.spillCachedInsertRows(); err != nil {
			return err
		}
	}

	rawType := rawEvent.GetType()
	if !rawEvent.IsUpdate() {
		updateMetricEventServiceSendDMLTypeCount(p.mode, rawType, false)
		ignore, err := p.shouldIgnoreRawEventByDMLType(rawEvent)
		if err != nil {
			return err
		}
		if ignore {
			return nil
		}
		return p.currentTxn.AppendRow(rawEvent, p.mounter.DecodeToChunk, p.filter, p.filterContext)
	}

	var (
		shouldSplit bool
		err         error
	)
	ignore, err := p.shouldIgnoreDMLByEventType(common.RowTypeUpdate, rawEvent.StartTs)
	if err != nil {
		return err
	}
	if ignore {
		updateMetricEventServiceSendDMLTypeCount(p.mode, rawType, false)
		return nil
	}

	if !p.outputRawChangeEvent {
		shouldSplit, err = event.IsUKChanged(rawEvent, p.currentTxn.CurrentDMLEvent.TableInfo)
		if err != nil {
			return err
		}
	}

	updateMetricEventServiceSendDMLTypeCount(p.mode, rawType, shouldSplit)

	if !shouldSplit {
		return p.currentTxn.AppendRow(rawEvent, p.mounter.DecodeToChunk, p.filter, p.filterContext)
	}

	log.Debug("split update event", zap.Uint64("startTs", rawEvent.StartTs),
		zap.Uint64("commitTs", rawEvent.CRTs),
		zap.String("table", p.currentTxn.CurrentDMLEvent.TableInfo.TableName.String()))
	deleteRow, insertRow, err := rawEvent.SplitUpdate()
	if err != nil {
		return err
	}
	ignoreInsert, err := p.shouldIgnoreRawEventByDMLType(insertRow)
	if err != nil {
		return err
	}
	if !ignoreInsert {
		if p.shouldSpillSplitUpdateInsert() {
			state, err := p.getOrCreateLargeTxnState()
			if err != nil {
				return err
			}
			if err := state.appendInsert(insertRow); err != nil {
				return err
			}
		} else {
			p.insertRowCache = append(p.insertRowCache, insertRow)
		}
	}
	ignoreDelete, err := p.shouldIgnoreRawEventByDMLType(deleteRow)
	if err != nil {
		return err
	}
	if ignoreDelete {
		return nil
	}
	return p.currentTxn.AppendRow(deleteRow, p.mounter.DecodeToChunk, p.filter, p.filterContext)
}

func (p *dmlProcessor) shouldIgnoreRawEventByDMLType(rawEvent *common.RawKVEntry) (bool, error) {
	rowType := common.RowTypeInsert
	if rawEvent.IsDelete() {
		rowType = common.RowTypeDelete
	} else if rawEvent.IsUpdate() {
		rowType = common.RowTypeUpdate
	}
	return p.shouldIgnoreDMLByEventType(rowType, rawEvent.StartTs)
}

func (p *dmlProcessor) shouldIgnoreDMLByEventType(rowType common.RowType, startTs uint64) (bool, error) {
	idx := int(rowType)
	if idx >= 0 && idx < len(p.dmlTypeFilterCache) {
		if p.dmlTypeFilterCache[idx].valid {
			return p.dmlTypeFilterCache[idx].ignore, nil
		}
	}

	if p.filter == nil {
		p.setDMLTypeFilterCache(rowType, false)
		return false, nil
	}
	ignore, err := p.filter.ShouldIgnoreDMLByEventType(
		rowType,
		p.currentTxn.CurrentDMLEvent.TableInfo,
		startTs,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	p.setDMLTypeFilterCache(rowType, ignore)
	return ignore, nil
}

func (p *dmlProcessor) setDMLTypeFilterCache(rowType common.RowType, ignore bool) {
	idx := int(rowType)
	if idx < 0 || idx >= len(p.dmlTypeFilterCache) {
		return
	}
	p.dmlTypeFilterCache[idx].valid = true
	p.dmlTypeFilterCache[idx].ignore = ignore
}

func (p *dmlProcessor) resetDMLTypeFilterCache() {
	for i := range p.dmlTypeFilterCache {
		p.dmlTypeFilterCache[i].valid = false
		p.dmlTypeFilterCache[i].ignore = false
	}
}

// getCurrentBatchDML returns the current batch DML event
func (p *dmlProcessor) getCurrentBatchDML() *event.BatchDMLEvent {
	return p.batchDML
}

// this should be called after the previous batchDML is flushed.
func (p *dmlProcessor) resetBatchDML() {
	p.batchDML = event.NewBatchDMLEvent()
}
