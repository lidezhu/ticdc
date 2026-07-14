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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// splitTxnScanStrategy allows a scan to stop inside a transaction. It owns
// row-level progress and the spill state needed to resume that transaction.
type splitTxnScanStrategy struct {
	scanner    *eventScanner
	dispatcher *dispatcherStat
}

func (s *splitTxnScanStrategy) resumePending(
	session *scanSession,
) (bool, bool, error) {
	state := s.dispatcher.getLargeTxnState()
	if state == nil || state.getPhase() != largeTxnScanPhaseDrainInserts {
		return false, false, nil
	}
	interrupted, err := s.drainLargeTxnInserts(session, state)
	return true, interrupted, err
}

func (s *splitTxnScanStrategy) onNoMoreRows(
	ctx *txnScanContext,
) (bool, error) {
	if ctx.processor.currentTxn != nil {
		return false, nil
	}
	s.dispatcher.txnSizeMetrics.flush()
	state := s.dispatcher.getLargeTxnState()
	if state == nil || state.getPhase() != largeTxnScanPhaseOriginal {
		return false, nil
	}

	s.dispatcher.markLargeTxnDrainInserts(state.startTs, state.commitTs, false, 0)
	ctx.session.progress = newTxnScanProgress(state.commitTs, state.startTs)
	return true, nil
}

func (s *splitTxnScanStrategy) beforeRow(
	ctx *txnScanContext,
	rawEvent *common.RawKVEntry,
) (bool, error) {
	if ctx.processor.currentTxn != nil {
		return false, nil
	}
	s.dispatcher.txnSizeMetrics.advanceTo(txnIdentity{
		startTs:  rawEvent.StartTs,
		commitTs: rawEvent.CRTs,
	})
	state := s.dispatcher.getLargeTxnState()
	if state == nil || state.getPhase() != largeTxnScanPhaseOriginal ||
		(rawEvent.StartTs == state.startTs && rawEvent.CRTs == state.commitTs) {
		return false, nil
	}

	s.dispatcher.markLargeTxnDrainInserts(state.startTs, state.commitTs, true, rawEvent.CRTs)
	ctx.session.progress = newTxnScanProgress(state.commitTs, state.startTs)
	return true, nil
}

func (s *splitTxnScanStrategy) startTxn(
	ctx *txnScanContext,
	startTs, commitTs uint64,
	tableInfo *common.TableInfo,
) error {
	if err := ctx.processor.startTxn(
		ctx.session.dispatcherStat.id,
		ctx.tableID,
		tableInfo,
		startTs,
		commitTs,
		true,
	); err != nil {
		return err
	}
	ctx.session.dmlCount++
	return nil
}

func (s *splitTxnScanStrategy) finishTxn(
	ctx *txnScanContext,
	nextCommitTs uint64,
	nextTableInfoUpdateTs uint64,
	nextTableDeleted bool,
) (bool, error) {
	session := ctx.session
	merger := ctx.merger
	processor := ctx.processor
	if processor.currentTxn == nil {
		if nextTableDeleted {
			s.dispatcher.txnSizeMetrics.flush()
		}
		return false, nil
	}
	currentStartTs := processor.currentTxn.CurrentDMLEvent.GetStartTs()
	currentCommitTs := processor.currentTxn.CurrentDMLEvent.GetCommitTs()

	if processor.hasSpilledInsertsForCurrentTxn() && merger.hasDDLAtCommitTs(currentCommitTs) {
		if err := processor.flushCachedInsertRows(); err != nil {
			return false, err
		}
		if err := processor.flushSpilledInsertsIntoCurrentTxn(); err != nil {
			return false, err
		}
	}

	if err := s.scanner.commitTxn(session, merger, processor, nextCommitTs, nextTableInfoUpdateTs); err != nil {
		return false, err
	}
	if !processor.hasLargeTxnState(currentStartTs, currentCommitTs) {
		return false, nil
	}

	events := merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())
	session.appendEvents(events)
	processor.resetBatchDML()

	hasFollowingTxn := nextCommitTs != 0 && !nextTableDeleted
	session.dispatcherStat.markLargeTxnDrainInserts(
		currentStartTs,
		currentCommitTs,
		hasFollowingTxn,
		nextCommitTs,
	)
	if len(session.lastRowPosition) > 0 {
		session.progress = newRowLevelScanProgress(currentCommitTs, currentStartTs, session.lastRowPosition)
	} else {
		session.progress = newTxnScanProgress(currentCommitTs, currentStartTs)
	}
	return true, nil
}

func (s *splitTxnScanStrategy) canInterruptCurrentTxn(
	ctx *txnScanContext,
	rawEvent *common.RawKVEntry,
	position common.ScanPosition,
) bool {
	if len(position) == 0 {
		return false
	}
	processor := ctx.processor
	if len(processor.insertRowCache) > 0 {
		return false
	}
	if processor.currentTxn == nil || !processor.currentTxn.exceedsLargeTxnThreshold() {
		return false
	}
	if !ctx.merger.canInterrupt(rawEvent.CRTs, processor.batchDML) {
		return false
	}
	return true
}

func (s *splitTxnScanStrategy) appendRow(
	ctx *txnScanContext,
	rawEvent *common.RawKVEntry,
	position common.ScanPosition,
) (bool, error) {
	if err := ctx.processor.appendRow(rawEvent); err != nil {
		return false, err
	}
	if !s.canInterruptCurrentTxn(ctx, rawEvent, position) {
		return false, nil
	}
	interruptCurrentTxn(
		ctx.session,
		ctx.merger,
		ctx.processor,
		rawEvent.CRTs,
		rawEvent.StartTs,
		position,
	)
	return true, nil
}

func (s *splitTxnScanStrategy) cleanup() {
	_ = s.dispatcher.cleanupLargeTxnState()
}

func interruptCurrentTxn(
	session *scanSession,
	merger *eventMerger,
	processor *dmlProcessor,
	commitTs uint64,
	startTs uint64,
	position common.ScanPosition,
) {
	if processor.currentTxn != nil {
		currentTxn := processor.currentTxn
		session.dispatcherStat.txnSizeMetrics.addFragment(txnSizeSampleFromTxn(currentTxn))
	}
	events := merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())
	session.appendEvents(events)
	session.progress = newRowLevelScanProgress(commitTs, startTs, position)
	log.Debug("scan interrupted inside a large txn",
		zap.Stringer("dispatcherID", session.dispatcherStat.id),
		zap.Uint64("startTs", startTs),
		zap.Uint64("commitTs", commitTs),
		zap.Int("scannedEntryCount", session.scannedEntryCount),
		zap.Duration("duration", time.Since(session.startTime)))
}

func (s *splitTxnScanStrategy) drainLargeTxnInserts(
	session *scanSession,
	state *largeTxnScanState,
) (bool, error) {
	drainedInsertCount := 0
	returnWithError := func(scanErr error) (bool, error) {
		if rollbackErr := state.rollbackDrain(); rollbackErr != nil {
			log.Warn("reset large transaction spill reader failed",
				zap.Stringer("dispatcherID", session.dispatcherStat.id),
				zap.Error(rollbackErr))
		}
		return false, scanErr
	}

	processor := newDMLProcessor(
		s.scanner.mounter,
		s.scanner.schemaGetter,
		session.dispatcherStat.filter,
		session.dispatcherStat.info.IsOutputRawChangeEvent(),
		s.scanner.mode,
		session.dispatcherStat.info.EnableIgnoreUpdateOnlyColumns())
	processor.dispatcherStat = session.dispatcherStat

	for {
		shouldStop, err := s.scanner.checkScanConditions(session)
		if err != nil {
			return returnWithError(err)
		}
		if shouldStop {
			return false, nil
		}

		entry, err := state.nextInsert()
		if err != nil {
			if session.dispatcherStat.isRemoved.Load() {
				return false, nil
			}
			if errors.Is(err, io.EOF) {
				if processor.currentTxn != nil {
					if err := processor.commitTxn(); err != nil {
						return returnWithError(err)
					}
					if processor.getCurrentBatchDML().DMLCount() != 0 {
						session.appendEvents([]event.Event{processor.getCurrentBatchDML()})
					}
				}
				state.commitDrainedInserts(drainedInsertCount)
				session.progress = newTxnScanProgress(state.commitTs, state.startTs)
				hasFollowingTxn, followingCommitTs := state.snapshotDrainInfo()
				shouldResolveCommitTs := (!hasFollowingTxn || followingCommitTs > state.commitTs) &&
					session.dataRange.CommitTsEnd == state.commitTs
				if err := session.dispatcherStat.cleanupLargeTxnState(); err != nil {
					log.Warn("cleanup drained large transaction spill failed",
						zap.Stringer("dispatcherID", session.dispatcherStat.id),
						zap.Error(err))
				}
				if shouldResolveCommitTs {
					resolved := event.NewResolvedEvent(state.commitTs, session.dispatcherStat.id, session.dispatcherStat.epoch)
					session.appendEvents([]event.Event{resolved})
					session.progress = newTxnScanProgress(state.commitTs, 0)
					return false, nil
				}
				return true, nil
			}
			return returnWithError(err)
		}
		drainedInsertCount++

		if processor.currentTxn == nil {
			if err := processor.startTxn(
				session.dispatcherStat.id,
				state.tableID,
				state.tableInfo,
				state.startTs,
				state.commitTs,
				true,
			); err != nil {
				return returnWithError(err)
			}
		}
		session.observeRawEntry(entry, nil)
		if err := processor.appendInsertRow(entry); err != nil {
			return returnWithError(err)
		}
		if session.exceedLimit(processor.batchDML.GetSize(), processor.batchDML) {
			if err := processor.commitTxn(); err != nil {
				return returnWithError(err)
			}
			session.appendEvents([]event.Event{processor.getCurrentBatchDML()})
			state.commitDrainedInserts(drainedInsertCount)
			session.progress = newTxnScanProgress(state.commitTs, state.startTs)
			return true, nil
		}
	}
}
