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
// See the License for the specific language governing permissions and
// limitations under the License.

package eventservice

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

// eventGetter is the interface for getting iterator of events
// The implementation of eventGetter is eventstore.EventStore
type eventGetter interface {
	GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (eventstore.EventIterator, error)
}

// schemaGetter is the interface for getting schema info and ddl events
// The implementation of schemaGetter is schemastore.SchemaStore
type schemaGetter interface {
	FetchTableDDLEvents(keyspace common.KeyspaceMeta, dispatcherID common.DispatcherID, tableID int64, filter filter.Filter, startTs, endTs uint64) ([]event.DDLEvent, error)
	GetTableInfo(keyspace common.KeyspaceMeta, tableID int64, ts uint64) (*common.TableInfo, error)
}

// eventScanner scans events from eventStore and schemaStore
type eventScanner struct {
	eventGetter  eventGetter
	schemaGetter schemaGetter
	mounter      event.Mounter
	mode         int64
}

// newEventScanner creates a new EventScanner
func newEventScanner(
	eventStore eventGetter,
	schemaStore schemastore.SchemaStore,
	mounter event.Mounter,
	mode int64,
) *eventScanner {
	return &eventScanner{
		eventGetter:  eventStore,
		schemaGetter: schemaStore,
		mounter:      mounter,
		mode:         mode,
	}
}

// scan retrieves and processes events from both eventStore and schemaStore based on the provided scanTask and limits.
// The function ensures that events are returned in chronological order, with DDL and DML events sorted by their commit timestamps.
// If there are DML and DDL events with the same commitTs, the DML event will be returned first.
//
// Time-ordered event processing:
//
//	Time/Commit TS -->
//	|
//	|    DML1   DML2      DML3      DML4  DML5
//	|     |      |         |         |     |
//	|     v      v         v         v     v
//	|    TS10   TS20      TS30      TS40  TS40
//	|                       |               |
//	|                       |              DDL2
//	|                      DDL1            TS40
//	|                      TS30
//
// - DML events with TS 10, 20, 30 are processed first
// - At TS30, DDL1 is processed after DML3 (same timestamp)
// - At TS40, DML4 is processed first, then DML5, then DDL2 (same timestamp)
//
// The scan operation may be interrupted when ANY of these limits are reached:
// - Maximum bytes processed (limit.MaxBytes) at a transaction boundary
// - Timeout duration (limit.Timeout) at a transaction boundary
// - Large transaction threshold inside the current transaction
//
// A transaction-boundary scan interruption is ONLY allowed when both conditions are met:
// 1. The current event's commit timestamp is greater than the lastCommitTs (a commit TS boundary is reached)
// 2. At least one DML event has been successfully scanned
//
// A current-transaction interruption is ONLY allowed when split transaction is enabled,
// the eventstore iterator provides a row-level scan position, and the current
// transaction fragment exceeds the large transaction threshold.
//
// Returns:
// - events: The scanned events in commitTs order
// - isBroken: true if the scan was interrupted due to reaching a limit, false otherwise
// - error: Any error that occurred during the scan operation
func (s *eventScanner) scan(
	ctx context.Context,
	dispatcherStat *dispatcherStat,
	dataRange common.DataRange,
	limit scanLimit,
) (int64, []event.Event, scanProgress, bool, error) {
	// Initialize scan session
	sess := newSession(ctx, dispatcherStat, dataRange, limit)
	defer sess.recordMetrics()
	strategy := newTxnScanStrategy(s, dispatcherStat)

	if handled, interrupted, err := strategy.resumePending(sess); handled {
		return sess.eventBytes, sess.events, sess.progress, interrupted, err
	}

	// Fetch DDL events
	start := time.Now()
	events, err := s.fetchDDLEvents(dispatcherStat, dataRange)
	if err != nil {
		return 0, nil, scanProgress{}, false, err
	}
	metrics.EventServiceGetDDLEventDuration.Observe(time.Since(start).Seconds())
	scanCtx := newTxnScanContext(s, sess, newEventMerger(events))

	iter, err := s.eventGetter.GetIterator(dispatcherStat.info.GetID(), dataRange)
	if err != nil {
		return 0, nil, scanProgress{}, false, err
	}
	if iter == nil {
		interrupted, err := strategy.onNoMoreRows(scanCtx)
		if err != nil || interrupted {
			return 0, sess.events, sess.progress, interrupted, err
		}
		resolved := event.NewResolvedEvent(dataRange.CommitTsEnd, dispatcherStat.id, dispatcherStat.epoch)
		events = append(events, resolved)
		sess.appendEvents(events)
		sess.progress = newTxnScanProgress(dataRange.CommitTsEnd, 0)
		return 0, sess.events, sess.progress, false, nil
	}

	// Execute event scanning and merging
	interrupted, scanErr := s.scanAndMergeEvents(scanCtx, strategy, iter)
	closeErr := s.closeIterator(iter)
	if scanErr != nil {
		if closeErr != nil {
			log.Warn("event store iterator close returned error after scan error",
				zap.Stringer("dispatcherID", dispatcherStat.info.GetID()),
				zap.Error(closeErr))
		}
		strategy.cleanup()
		return sess.eventBytes, sess.events, sess.progress, interrupted, scanErr
	}
	if closeErr != nil {
		strategy.cleanup()
		return 0, nil, scanProgress{}, false, closeErr
	}
	return sess.eventBytes, sess.events, sess.progress, interrupted, nil
}

// fetchDDLEvents retrieves DDL events which finishedTs are within the range (start, end]
func (s *eventScanner) fetchDDLEvents(stat *dispatcherStat, dataRange common.DataRange) ([]event.Event, error) {
	dispatcherID := stat.info.GetID()
	keyspaceMeta := common.KeyspaceMeta{
		ID:   stat.info.GetTableSpan().KeyspaceID,
		Name: stat.changefeedStat.changefeedID.Keyspace(),
	}
	ddlEvents, err := s.schemaGetter.FetchTableDDLEvents(
		keyspaceMeta,
		dispatcherID,
		dataRange.Span.TableID,
		stat.filter,
		dataRange.CommitTsStart,
		dataRange.CommitTsEnd,
	)
	if err != nil {
		log.Error("get ddl events failed", zap.Stringer("dispatcherID", dispatcherID),
			zap.Int64("tableID", dataRange.Span.TableID), zap.Error(err), zap.Int64("mode", s.mode))
		return nil, err
	}

	result := make([]event.Event, 0, len(ddlEvents))
	for _, item := range ddlEvents {
		result = append(result, &item)
	}
	return result, nil
}

// closeIterator closes the event iterator and records metrics.
func (s *eventScanner) closeIterator(iter eventstore.EventIterator) error {
	if iter == nil {
		return nil
	}
	eventCount, err := iter.Close()
	if eventCount != 0 {
		updateMetricEventStoreOutputKv(s.mode, float64(eventCount))
	}
	return err
}

// scanAndMergeEvents performs the main scanning and merging logic
func (s *eventScanner) scanAndMergeEvents(
	ctx *txnScanContext,
	strategy txnScanStrategy,
	iter eventstore.EventIterator,
) (bool, error) {
	session := ctx.session
	processor := ctx.processor
	dispatcher := session.dispatcherStat

	for {
		shouldStop, err := s.checkScanConditions(session)
		if err != nil {
			return false, err
		}
		if shouldStop {
			return false, nil
		}

		rawEvent, position, isNewTxn := nextEventWithScanPosition(iter)
		if rawEvent == nil {
			interrupted, err := strategy.onNoMoreRows(ctx)
			if err != nil || interrupted {
				return interrupted, err
			}
			interrupted, err = strategy.finishTxn(
				ctx,
				0,
				0,
				false,
			)
			if err != nil || interrupted {
				return interrupted, err
			}
			err = finalizeScan(ctx.merger, processor, session, session.dataRange.CommitTsEnd)
			return false, err
		}
		interrupted, err := strategy.beforeRow(ctx, rawEvent)
		if err != nil || interrupted {
			return interrupted, err
		}

		if isNewTxn {
			tableInfo, err := s.getTableInfo4Txn(dispatcher, ctx.tableID, rawEvent.CRTs-1)
			if err != nil {
				return false, err
			}
			interrupted, err := strategy.finishTxn(
				ctx,
				rawEvent.CRTs,
				getTableInfoUpdateTs(tableInfo),
				tableInfo == nil,
			)
			if err != nil || interrupted {
				return interrupted, err
			}
			// The table has been deleted, so the current raw event cannot be
			// decoded as DML. Resolve to its commit ts to skip it; resolving to
			// rawEvent.CRTs-1 can equal the scan start and cause a no-progress loop.
			if tableInfo == nil {
				err = finalizeScan(ctx.merger, processor, session, rawEvent.CRTs)
				return false, err
			}

			if session.exceedLimit(processor.batchDML.GetSize(), processor.batchDML) &&
				ctx.merger.canInterrupt(rawEvent.CRTs, processor.batchDML) {
				interruptScan(session, ctx.merger, processor, rawEvent.CRTs, rawEvent.StartTs)
				return true, nil
			}

			err = strategy.startTxn(ctx, rawEvent.StartTs, rawEvent.CRTs, tableInfo)
			if err != nil {
				return false, err
			}
		}

		session.observeRawEntry(rawEvent, position)
		interrupted, err = strategy.appendRow(ctx, rawEvent, position)
		if err != nil {
			log.Error("append row failed", zap.Error(err),
				zap.Stringer("dispatcherID", session.dispatcherStat.id),
				zap.Int64("tableID", ctx.tableID),
				zap.Uint64("startTs", rawEvent.StartTs),
				zap.Uint64("commitTs", rawEvent.CRTs),
				zap.Int64("mode", s.mode))
			return false, err
		}
		if interrupted {
			return true, nil
		}
	}
}

func nextEventWithScanPosition(
	iter eventstore.EventIterator,
) (*common.RawKVEntry, common.ScanPosition, bool) {
	if positionIter, ok := iter.(eventstore.EventIteratorWithScanPosition); ok {
		return positionIter.NextWithScanPosition()
	}
	rawEvent, isNewTxn := iter.Next()
	return rawEvent, nil, isNewTxn
}

func getTableInfoUpdateTs(tableInfo *common.TableInfo) uint64 {
	if tableInfo == nil {
		return 0
	}
	return tableInfo.GetUpdateTS()
}

// checkScanConditions checks context cancellation and dispatcher status
// return true if the scan should be stopped, false otherwise
func (s *eventScanner) checkScanConditions(session *scanSession) (bool, error) {
	if session.isContextDone() {
		log.Warn("scan exits since context done", zap.Stringer("dispatcherID", session.dispatcherStat.id), zap.Error(context.Cause(session.ctx)))
		return true, context.Cause(session.ctx)
	}
	return session.dispatcherStat.isRemoved.Load(), nil
}

func (s *eventScanner) getTableInfo4Txn(dispatcher *dispatcherStat, tableID int64, ts uint64) (*common.TableInfo, error) {
	keyspaceMeta := common.KeyspaceMeta{
		ID:   dispatcher.info.GetTableSpan().KeyspaceID,
		Name: dispatcher.info.GetChangefeedID().Keyspace(),
	}
	tableInfo, err := s.schemaGetter.GetTableInfo(keyspaceMeta, tableID, ts)
	if err == nil {
		return tableInfo, nil
	}

	if dispatcher.isRemoved.Load() {
		log.Warn("get table info failed, but the dispatcher is removed from the event service",
			zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID),
			zap.Uint64("ts", ts), zap.Error(err), zap.Int64("mode", s.mode))
		return nil, nil
	}

	if errors.Is(err, &schemastore.TableDeletedError{}) {
		log.Warn("get table info failed, since the table is deleted",
			zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID),
			zap.Uint64("ts", ts), zap.Int64("mode", s.mode))
		return nil, nil
	}

	log.Error("get table info failed, unknown reason",
		zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID),
		zap.Uint64("ts", ts), zap.Error(err), zap.Int64("mode", s.mode))
	return nil, err
}

func (s *eventScanner) commitTxn(
	session *scanSession,
	merger *eventMerger,
	processor *dmlProcessor,
	eventCommitTs, tableInfoUpdateTs uint64,
) error {
	var txnSize *txnSizeSample
	if processor.currentTxn != nil {
		sample := txnSizeSampleFromTxn(processor.currentTxn)
		txnSize = &sample
	}
	if err := processor.commitTxn(); err != nil {
		return err
	}
	if txnSize != nil {
		session.dispatcherStat.txnSizeMetrics.complete(*txnSize)
	}
	currentBatchDML := processor.getCurrentBatchDML()

	// Use DMLCount() instead of Len() to check if the batchDML is empty
	// because the batchDML may have some skipped rows, so the Len() can be 0 even if the batchDML is not empty
	if currentBatchDML == nil || currentBatchDML.DMLCount() == 0 {
		return nil
	}

	// Check if should flush the current batchDML and reset a new one
	tableUpdated := currentBatchDML.TableInfo.GetUpdateTS() != tableInfoUpdateTs
	hasNewDDL := merger.hasDDLLessThanCommitTs(eventCommitTs)
	if hasNewDDL || tableUpdated {
		events := merger.mergeWithPrecedingDDLs(currentBatchDML)
		session.appendEvents(events)
		processor.resetBatchDML()
	}
	return nil
}

// finalizeScan finalizes the scan when all events have been processed
// it's called when the iterator is nil, always indicates that all entries
// with the same commit-ts is processed, so it's ok to append resolved-ts event
func finalizeScan(
	merger *eventMerger,
	processor *dmlProcessor,
	sess *scanSession,
	endTs uint64,
) error {
	var txnSize *txnSizeSample
	if processor.currentTxn != nil {
		sample := txnSizeSampleFromTxn(processor.currentTxn)
		txnSize = &sample
	}
	if err := processor.commitTxn(); err != nil {
		return err
	}
	if txnSize != nil {
		sess.dispatcherStat.txnSizeMetrics.complete(*txnSize)
	}

	resolvedBatch := processor.getCurrentBatchDML()
	events := merger.mergeWithPrecedingDDLs(resolvedBatch)
	events = append(events, merger.resolveDDLEvents(endTs)...)

	resolveTs := event.NewResolvedEvent(endTs, sess.dispatcherStat.id, sess.dispatcherStat.epoch)
	events = append(events, resolveTs)
	sess.appendEvents(events)
	sess.progress = newTxnScanProgress(endTs, 0)
	return nil
}

// interruptScan handles scan interruption due to limits
// it's called when the scan exceeds the limit, and it commits the current transaction,
// but other there may have some entries with the same commit-ts not processed yet,
// so only append the resolved-ts event if the new commit-ts is different from the last commit-ts.
func interruptScan(
	session *scanSession,
	merger *eventMerger,
	processor *dmlProcessor,
	newCommitTs uint64,
	newStartTs uint64,
) {
	// Append current batch
	events := merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())

	if newCommitTs != merger.lastBatchDMLCommitTs {
		// lastCommitTs may be 0, if the scanner timeout and no one row scanned.
		// this usually happens when the CPU is overloaded.
		if merger.lastBatchDMLCommitTs == 0 {
			log.Info("interrupt scan when no DML event is scanned",
				zap.Stringer("dispatcherID", session.dispatcherStat.id),
				zap.Int64("tableID", session.dataRange.Span.TableID),
				zap.Uint64("newCommitTs", newCommitTs),
				zap.Int("scannedEntryCount", session.scannedEntryCount),
				zap.Int("txnCount", session.dmlCount),
				zap.Duration("duration", time.Since(session.startTime)))
		} else {
			// This means we interrupt the scan at a position where the commitTs is different from the last batchDML commitTs
			// In this case, we need to append the DDL less than or equal to the last batchDML commitTs and the resolved-ts event with the last batchDML commitTs
			events = append(events, merger.resolveDDLEvents(merger.lastBatchDMLCommitTs)...)
			resolvedTs := event.NewResolvedEvent(merger.lastBatchDMLCommitTs, session.dispatcherStat.id, session.dispatcherStat.epoch)
			events = append(events, resolvedTs)
			log.Debug("scan interrupted at different commitTs with new event", zap.Stringer("dispatcherID", session.dispatcherStat.id), zap.Uint64("CommitTs", merger.lastBatchDMLCommitTs), zap.Uint64("newCommitTs", newCommitTs), zap.Duration("duration", time.Since(session.startTime)))
		}
	} else {
		startTs := uint64(0)
		if processor.currentTxn != nil {
			startTs = processor.currentTxn.CurrentDMLEvent.GetStartTs()
		}
		log.Debug("scan interrupted at the same commitTs with new event", zap.Stringer("dispatcherID", session.dispatcherStat.id), zap.Uint64("startTs", startTs), zap.Uint64("commitTs", merger.lastBatchDMLCommitTs), zap.Uint64("newStartTs", newStartTs), zap.Uint64("newCommitTs", newCommitTs), zap.Duration("duration", time.Since(session.startTime)))
	}
	session.appendEvents(events)
}
