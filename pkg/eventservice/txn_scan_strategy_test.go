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
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewTxnScanStrategy(t *testing.T) {
	info := newMockDispatcherInfoForTest(t)
	status := newChangefeedStatusForTest(t, info)
	dispatcher := newDispatcherStat(info, 1, 1, nil, status)
	scanner := newEventScanner(nil, NewMockSchemaStore(), &mockMounter{}, 0)

	dispatcher.txnAtomicity = config.AtomicityLevel("table")
	_, ok := newTxnScanStrategy(scanner, dispatcher).(*atomicTxnScanStrategy)
	require.True(t, ok)

	dispatcher.txnAtomicity = config.AtomicityLevel("none")
	_, ok = newTxnScanStrategy(scanner, dispatcher).(*splitTxnScanStrategy)
	require.True(t, ok)
}

func TestSplitTxnScanStrategyOwnsPendingMetricLifecycle(t *testing.T) {
	addPendingMetric := func(dispatcher *dispatcherStat) {
		dispatcher.txnSizeMetrics.addFragment(txnSizeSample{
			txn:       txnIdentity{startTs: 100, commitTs: 200},
			rawBytes:  70,
			threshold: 50,
		})
	}
	newStrategyAndContext := func() (*splitTxnScanStrategy, *txnScanContext, *dispatcherStat) {
		info := newMockDispatcherInfoForTest(t)
		status := newChangefeedStatusForTest(t, info)
		dispatcher := newDispatcherStat(info, 1, 1, nil, status)
		scanner := newEventScanner(nil, NewMockSchemaStore(), &mockMounter{}, 0)
		session := newSession(context.Background(), dispatcher, common.DataRange{
			Span: info.GetTableSpan(),
		}, scanLimit{})
		ctx := newTxnScanContext(scanner, session, newEventMerger(nil))
		strategy := &splitTxnScanStrategy{scanner: scanner, dispatcher: dispatcher}
		return strategy, ctx, dispatcher
	}

	t.Run("flush when iterator has no more rows", func(t *testing.T) {
		strategy, ctx, dispatcher := newStrategyAndContext()
		addPendingMetric(dispatcher)

		interrupted, err := strategy.onNoMoreRows(ctx)
		require.NoError(t, err)
		require.False(t, interrupted)
		require.Nil(t, dispatcher.txnSizeMetrics.pending)
	})

	t.Run("keep pending metric while current transaction is active", func(t *testing.T) {
		strategy, ctx, dispatcher := newStrategyAndContext()
		addPendingMetric(dispatcher)
		ctx.processor.currentTxn = &TxnEvent{}

		interrupted, err := strategy.beforeRow(ctx, &common.RawKVEntry{StartTs: 101, CRTs: 201})
		require.NoError(t, err)
		require.False(t, interrupted)
		require.NotNil(t, dispatcher.txnSizeMetrics.pending)

		ctx.processor.currentTxn = nil
		interrupted, err = strategy.beforeRow(ctx, &common.RawKVEntry{StartTs: 101, CRTs: 201})
		require.NoError(t, err)
		require.False(t, interrupted)
		require.Nil(t, dispatcher.txnSizeMetrics.pending)
	})

	t.Run("flush when the remaining transaction is deleted", func(t *testing.T) {
		strategy, ctx, dispatcher := newStrategyAndContext()
		addPendingMetric(dispatcher)

		interrupted, err := strategy.beforeRow(ctx, &common.RawKVEntry{StartTs: 100, CRTs: 200})
		require.NoError(t, err)
		require.False(t, interrupted)
		require.NotNil(t, dispatcher.txnSizeMetrics.pending)

		interrupted, err = strategy.finishTxn(ctx, 200, 0, true)
		require.NoError(t, err)
		require.False(t, interrupted)
		require.Nil(t, dispatcher.txnSizeMetrics.pending)
	})
}
