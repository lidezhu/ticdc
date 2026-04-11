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
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestScheduleRegionRequestUpdatesRuntimeRegistry(t *testing.T) {
	client := newSubscriptionClientForTest()

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(uint64) {}
	subSpan := client.subscriptions.manager.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	regionSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'b'},
		EndKey:   []byte{'c'},
	}
	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), regionSpan, nil, subSpan, false)

	client.pipeline.scheduler.scheduleRegionRequest(context.Background(), region, TaskLowPrior)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	task, err := client.pipeline.requestRouter.regionTaskQueue.Pop(ctx)
	require.NoError(t, err)
	queued := task.GetRegionInfo()
	require.True(t, queued.runtimeKey.isValid())

	state, ok := client.runtime.registry.get(queued.runtimeKey)
	require.True(t, ok)
	require.Equal(t, regionPhaseQueued, state.phase)
	require.Equal(t, uint64(10), state.verID.GetID())
	require.False(t, state.rangeLockAcquiredTime.IsZero())
}

func TestOnRegionFailUpdatesRuntimeRegistry(t *testing.T) {
	client := newSubscriptionClientForTest()
	defer client.cancel()

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(uint64) {}
	subSpan := client.subscriptions.manager.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	lockRes := subSpan.rangeLock.LockRange(context.Background(), []byte{'b'}, []byte{'c'}, 10, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, lockRes.Status)

	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'b'},
		EndKey:   []byte{'c'},
	}, nil, subSpan, false)
	region.lockedRangeState = lockRes.LockedRangeState

	client.runtime.ensureRegion(&region, time.Now())
	require.True(t, region.runtimeKey.isValid())

	client.pipeline.errorHandler.reportFailure(newSendRequestToStoreFailure(region, regionFailureSourceWorkerSession, nil))

	state, ok := client.runtime.registry.get(region.runtimeKey)
	require.True(t, ok)
	require.Equal(t, regionPhaseDiscovered, state.phase)
	require.Equal(t, "send request to store error", state.lastError)
	require.Equal(t, 0, state.retryCount)
}

func TestHandleResolvedTsUpdatesRuntimeRegistry(t *testing.T) {
	client := newSubscriptionClientForTest()
	worker := &regionRequestWorker{client: client}

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	subSpan := client.subscriptions.manager.newSubscribedSpan(
		SubscriptionID(1),
		rawSpan,
		100,
		func(_ []common.RawKVEntry, _ func()) bool { return false },
		func(uint64) {},
		0,
		false,
	)

	lockRes := subSpan.rangeLock.LockRange(context.Background(), rawSpan.StartKey, rawSpan.EndKey, 10, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, lockRes.Status)
	lockRes.LockedRangeState.Initialized.Store(true)

	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), rawSpan, nil, subSpan, false)
	region.lockedRangeState = lockRes.LockedRangeState
	client.runtime.ensureRegion(&region, time.Now())

	state := newRegionFeedState(region, uint64(subSpan.subID), worker)
	state.start()

	resolvedTs := uint64(200)
	handleResolvedTs(subSpan, state, resolvedTs)

	stored, ok := client.runtime.registry.get(region.runtimeKey)
	require.True(t, ok)
	require.Equal(t, resolvedTs, stored.lastResolvedTs)
	require.False(t, stored.lastEventTime.IsZero())
}

func TestDoHandleErrorMarksRetryPendingForRetryableRegionError(t *testing.T) {
	client := newSubscriptionClientForTest()

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(uint64) {}
	subSpan := client.subscriptions.manager.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), rawSpan, nil, subSpan, false)
	client.runtime.ensureRegion(&region, time.Now())

	err := client.pipeline.errorHandler.handleFailure(
		context.Background(),
		newEventRegionFailure(region, &cdcpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{Reason: "busy"}}),
	)
	require.NoError(t, err)

	state, ok := client.runtime.registry.get(region.runtimeKey)
	require.True(t, ok)
	require.Equal(t, regionPhaseQueued, state.phase)
	require.Equal(t, 1, state.retryCount)
	require.Contains(t, state.lastError, "server_is_busy")
}

func TestDoHandleErrorRemovesRuntimeForRangeReload(t *testing.T) {
	client := newSubscriptionClientForTest()

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(uint64) {}
	subSpan := client.subscriptions.manager.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), rawSpan, nil, subSpan, false)
	client.runtime.ensureRegion(&region, time.Now())

	err := client.pipeline.errorHandler.handleFailure(context.Background(), newRPCCtxUnavailableFailure(region))
	require.NoError(t, err)

	_, ok := client.runtime.registry.get(region.runtimeKey)
	require.False(t, ok)

	select {
	case task := <-client.pipeline.scheduler.rangeTaskCh:
		require.Equal(t, rawSpan, task.span)
		require.Equal(t, subSpan, task.subscribedSpan)
	case <-time.After(time.Second):
		t.Fatal("expected range task to be scheduled")
	}
}

func TestDoHandleErrorRemovesRuntimeForCancelledRequest(t *testing.T) {
	client := newSubscriptionClientForTest()

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(uint64) {}
	subSpan := client.subscriptions.manager.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), rawSpan, nil, subSpan, false)
	client.runtime.ensureRegion(&region, time.Now())

	err := client.pipeline.errorHandler.handleFailure(
		context.Background(),
		newRequestCancelledFailure(region, regionFailureSourceDeregister),
	)
	require.NoError(t, err)

	_, ok := client.runtime.registry.get(region.runtimeKey)
	require.False(t, ok)
}

func TestHandleFailureRemovesRuntimeForStoppedSubscription(t *testing.T) {
	client := newSubscriptionClientForTest()

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(uint64) {}
	subSpan := client.subscriptions.manager.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), rawSpan, nil, subSpan, false)
	client.runtime.ensureRegion(&region, time.Now())

	err := client.pipeline.errorHandler.handleFailure(context.Background(), newSubscriptionStoppedFailure(region))
	require.NoError(t, err)

	_, ok := client.runtime.registry.get(region.runtimeKey)
	require.False(t, ok)
}
