// Copyright 2026 PingCAP, Inc.
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

package logpuller

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func newRuntimeRegistryTestClient() *subscriptionClient {
	client := &subscriptionClient{
		regionTaskQueue:       NewPriorityQueue(),
		resolveLockTaskCh:     make(chan resolveLockTask, 1),
		regionRuntimeRegistry: newRegionRuntimeRegistry(),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	client.pdClock = pdutil.NewClock4Test()
	return client
}

func newRuntimeRegistryTestRegion(
	t *testing.T,
	client *subscriptionClient,
) (regionInfo, *subscribedSpan) {
	t.Helper()

	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(_ uint64) {}
	subSpan := client.newSubscribedSpan(
		SubscriptionID(1),
		heartbeatpb.TableSpan{
			TableID:  10,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
		100,
		consumeKVEvents,
		advanceResolvedTs,
		0,
		false,
	)
	region := newRegionInfo(
		tikv.NewRegionVerID(101, 1, 1),
		heartbeatpb.TableSpan{
			TableID:  10,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
		nil,
		subSpan,
		false,
	)
	return region, subSpan
}

func TestScheduleRegionRequestCreatesQueuedRuntimeState(t *testing.T) {
	client := newRuntimeRegistryTestClient()
	defer client.cancel()

	region, _ := newRuntimeRegistryTestRegion(t, client)
	client.scheduleRegionRequest(context.Background(), region, TaskLowPrior)

	task, err := client.regionTaskQueue.Pop(context.Background())
	require.NoError(t, err)
	queuedRegion := task.GetRegionInfo()
	require.Equal(t, uint64(1), queuedRegion.runtimeKey.generation)

	snapshots := client.regionRuntimeRegistry.snapshot()
	require.Len(t, snapshots, 1)
	require.Equal(t, regionPhaseQueued, snapshots[0].phase)
	require.False(t, snapshots[0].rangeLockAcquiredTime.IsZero())
	require.False(t, snapshots[0].requestEnqueueTime.IsZero())
	require.Equal(t, queuedRegion.runtimeKey, snapshots[0].key)
}

func TestDoHandleErrorCreatesNewRuntimeGeneration(t *testing.T) {
	client := newRuntimeRegistryTestClient()
	defer client.cancel()

	region, _ := newRuntimeRegistryTestRegion(t, client)
	client.scheduleRegionRequest(context.Background(), region, TaskLowPrior)

	task, err := client.regionTaskQueue.Pop(context.Background())
	require.NoError(t, err)
	queuedRegion := task.GetRegionInfo()
	require.Equal(t, uint64(1), queuedRegion.runtimeKey.generation)

	unlocked := queuedRegion.subscribedSpan.rangeLock.UnlockRange(
		queuedRegion.span.StartKey,
		queuedRegion.span.EndKey,
		queuedRegion.verID.GetID(),
		queuedRegion.verID.GetVer(),
		queuedRegion.resolvedTs(),
	)
	require.False(t, unlocked)

	errInfo := newRegionErrorInfo(queuedRegion, &eventError{
		err: &cdcpb.Error{
			ServerIsBusy: &errorpb.ServerIsBusy{Reason: "busy"},
		},
	})
	require.NoError(t, client.doHandleError(context.Background(), errInfo))

	retryTask, err := client.regionTaskQueue.Pop(context.Background())
	require.NoError(t, err)
	retryRegion := retryTask.GetRegionInfo()
	require.Equal(t, uint64(2), retryRegion.runtimeKey.generation)

	snapshots := client.regionRuntimeRegistry.snapshot()
	require.Len(t, snapshots, 1)
	require.Equal(t, retryRegion.runtimeKey, snapshots[0].key)
	require.Equal(t, regionPhaseQueued, snapshots[0].phase)
	require.Equal(t, 1, snapshots[0].retryCount)
}

func TestInferRegionSlowReason(t *testing.T) {
	now := time.Now()
	reason, ok := inferRegionSlowReason(regionRuntimeState{
		phase:          regionPhaseRetryPending,
		retryCount:     4,
		phaseEnterTime: now.Add(-time.Minute),
	}, now)
	require.True(t, ok)
	require.Equal(t, regionSlowReasonRetryStorm, reason)

	reason, ok = inferRegionSlowReason(regionRuntimeState{
		phase:         regionPhaseReplicating,
		lastEventTime: now.Add(-7 * resolveLockMinInterval),
	}, now)
	require.True(t, ok)
	require.Equal(t, regionSlowReasonResolvedTsNotAdvance, reason)
}
