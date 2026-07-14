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
	"github.com/pingcap/ticdc/pkg/common"
)

// txnScanContext contains the state shared by all transaction scan strategies.
// Event iteration and DDL/DML ordering remain owned by eventScanner.
type txnScanContext struct {
	session   *scanSession
	merger    *eventMerger
	processor *dmlProcessor
	tableID   int64
}

func newTxnScanContext(
	scanner *eventScanner,
	session *scanSession,
	merger *eventMerger,
) *txnScanContext {
	dispatcher := session.dispatcherStat
	processor := newDMLProcessor(
		scanner.mounter,
		scanner.schemaGetter,
		dispatcher.filter,
		dispatcher.info.IsOutputRawChangeEvent(),
		scanner.mode,
		dispatcher.info.EnableIgnoreUpdateOnlyColumns())
	processor.dispatcherStat = dispatcher
	return &txnScanContext{
		session:   session,
		merger:    merger,
		processor: processor,
		tableID:   session.dataRange.Span.TableID,
	}
}

// txnScanStrategy defines the points where transaction atomicity changes the
// otherwise shared event scanning flow.
type txnScanStrategy interface {
	// resumePending handles work left by an earlier scan before a new event
	// iterator is opened. The first return value reports whether the scan has
	// been fully handled by the strategy.
	resumePending(*scanSession) (handled bool, interrupted bool, err error)
	// onNoMoreRows is called when the iterator has no row available.
	onNoMoreRows(*txnScanContext) (interrupted bool, err error)
	// beforeRow is called before a row is processed by the shared scan loop.
	beforeRow(*txnScanContext, *common.RawKVEntry) (interrupted bool, err error)
	startTxn(ctx *txnScanContext, startTs, commitTs uint64, tableInfo *common.TableInfo) error
	finishTxn(
		ctx *txnScanContext,
		nextCommitTs uint64,
		nextTableInfoUpdateTs uint64,
		nextTableDeleted bool,
	) (interrupted bool, err error)
	appendRow(
		ctx *txnScanContext,
		rawEvent *common.RawKVEntry,
		position common.ScanPosition,
	) (interrupted bool, err error)
	cleanup()
}

func newTxnScanStrategy(
	scanner *eventScanner,
	dispatcher *dispatcherStat,
) txnScanStrategy {
	if dispatcher.txnAtomicity.ShouldSplitTxn() {
		return &splitTxnScanStrategy{scanner: scanner, dispatcher: dispatcher}
	}
	return &atomicTxnScanStrategy{scanner: scanner}
}

// atomicTxnScanStrategy only allows interruption at transaction boundaries.
// It does not create row-level progress or large transaction spill state.
type atomicTxnScanStrategy struct {
	scanner *eventScanner
}

func (s *atomicTxnScanStrategy) resumePending(*scanSession) (bool, bool, error) {
	return false, false, nil
}

func (s *atomicTxnScanStrategy) onNoMoreRows(*txnScanContext) (bool, error) {
	return false, nil
}

func (s *atomicTxnScanStrategy) beforeRow(
	*txnScanContext,
	*common.RawKVEntry,
) (bool, error) {
	return false, nil
}

func (s *atomicTxnScanStrategy) startTxn(
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
		false,
	); err != nil {
		return err
	}
	ctx.session.dmlCount++
	return nil
}

func (s *atomicTxnScanStrategy) finishTxn(
	ctx *txnScanContext,
	nextCommitTs uint64,
	nextTableInfoUpdateTs uint64,
	_ bool,
) (bool, error) {
	err := s.scanner.commitTxn(
		ctx.session,
		ctx.merger,
		ctx.processor,
		nextCommitTs,
		nextTableInfoUpdateTs,
	)
	return false, err
}

func (s *atomicTxnScanStrategy) appendRow(
	ctx *txnScanContext,
	rawEvent *common.RawKVEntry,
	_ common.ScanPosition,
) (bool, error) {
	return false, ctx.processor.appendRow(rawEvent)
}

func (s *atomicTxnScanStrategy) cleanup() {}

var _ txnScanStrategy = (*atomicTxnScanStrategy)(nil)
var _ txnScanStrategy = (*splitTxnScanStrategy)(nil)
