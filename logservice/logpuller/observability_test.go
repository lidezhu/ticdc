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
	"errors"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

func TestSubscriptionClientGetObservabilitySnapshot(t *testing.T) {
	clock := pdutil.NewClock4Test().(*pdutil.Clock4Test)
	now := time.Unix(1700003600, 0)
	clock.SetTS(oracle.GoTimeToTS(now))

	client := newTestSubscriptionClient(t)
	client.pdClock = clock

	storeRegion := createTestRegionInfo(1, 101)
	storeRegion.rpcCtx = createRPCContext("tikv-1:20160", 11, 22)
	storeRegion.runtimeKey = client.regionRuntimeRegistry.allocKey(storeRegion.subscribedSpan.subID, storeRegion.verID.GetID())
	client.regionRuntimeRegistry.markDiscovered(storeRegion.runtimeKey, storeRegion, now.Add(-30*time.Minute))
	client.regionRuntimeRegistry.markQueued(
		storeRegion.runtimeKey,
		now.Add(-29*time.Minute),
		now.Add(-29*time.Minute),
		oracle.GoTimeToTS(now.Add(-20*time.Minute)),
	)
	client.regionRuntimeRegistry.markRequestSent(storeRegion.runtimeKey, 7, now.Add(-28*time.Minute))
	client.regionRuntimeRegistry.markReplicating(storeRegion.runtimeKey, now.Add(-28*time.Minute))

	store := newRequestedStore(storeRegion.rpcCtx.Addr)
	worker := &regionRequestWorker{
		workerID:     7,
		client:       client,
		store:        store,
		requestCache: newRequestCache(10),
	}
	ok, err := worker.add(context.Background(), createTestRegionInfo(1, 202), false)
	require.NoError(t, err)
	require.True(t, ok)

	session := newRegionRequestWorkerSession(
		worker.workerID,
		store.storeAddr,
		client,
		worker.requestCache,
	)
	session.setStage(WorkerSessionStateRunning)
	session.activeRegions.add(storeRegion.subscribedSpan.subID, storeRegion.verID.GetID(), &regionFeedState{})
	worker.setCurrentSession(session)
	store.addWorker(worker)
	client.requestedStores.stores.Store(store.storeAddr, store)

	stalledResolvedTs := oracle.GoTimeToTS(now.Add(-20 * time.Minute))
	span := client.subscribedSpans.newSubscribedSpan(SubscriptionID(2), heartbeatpb.TableSpan{
		TableID:  22,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
	}, stalledResolvedTs, func(_ []common.RawKVEntry, _ func()) bool { return false }, func(uint64) {}, 0, false)
	client.subscribedSpans.add(span.subID, span)
	span.initialized.Store(true)
	span.resolvedTs.Store(stalledResolvedTs)
	span.resolvedTsUpdated.Store(now.Add(-45 * time.Second).Unix())

	res := span.rangeLock.LockRange(context.Background(), []byte("a"), []byte("b"), 1, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	res.LockedRangeState.ResolvedTs.Store(stalledResolvedTs)
	res.LockedRangeState.Initialized.Store(false)
	res.LockedRangeState.Created = now.Add(-10 * time.Minute)

	res = span.rangeLock.LockRange(context.Background(), []byte("c"), []byte("d"), 2, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	res.LockedRangeState.ResolvedTs.Store(stalledResolvedTs)
	res.LockedRangeState.Initialized.Store(true)
	res.LockedRangeState.Created = now.Add(-9 * time.Minute)

	uninitializedRegion := newRegionInfo(
		tikv.NewRegionVerID(1, 1, 1),
		heartbeatpb.TableSpan{
			TableID:  22,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
		createRPCContext("tikv-2:20160", 33, 44),
		span,
		false,
	)
	uninitializedKey := client.regionRuntimeRegistry.allocKey(span.subID, 1)
	client.regionRuntimeRegistry.markDiscovered(uninitializedKey, uninitializedRegion, now.Add(-15*time.Minute))
	client.regionRuntimeRegistry.markRequestSent(uninitializedKey, 9, now.Add(-14*time.Minute))
	client.regionRuntimeRegistry.recordError(uninitializedKey, errors.New("still waiting init"), now.Add(-50*time.Second))

	initializedRegion := newRegionInfo(
		tikv.NewRegionVerID(2, 1, 1),
		heartbeatpb.TableSpan{
			TableID:  22,
			StartKey: []byte("c"),
			EndKey:   []byte("d"),
		},
		createRPCContext("tikv-2:20160", 34, 44),
		span,
		false,
	)
	initializedKey := client.regionRuntimeRegistry.allocKey(span.subID, 2)
	client.regionRuntimeRegistry.markDiscovered(initializedKey, initializedRegion, now.Add(-12*time.Minute))
	client.regionRuntimeRegistry.markQueued(
		initializedKey,
		now.Add(-11*time.Minute),
		now.Add(-11*time.Minute),
		stalledResolvedTs,
	)
	client.regionRuntimeRegistry.markRequestSent(initializedKey, 9, now.Add(-10*time.Minute))
	client.regionRuntimeRegistry.markReplicating(initializedKey, now.Add(-10*time.Minute))
	client.regionRuntimeRegistry.updateLastEvent(initializedKey, now.Add(-90*time.Second))

	snapshot := client.GetObservabilitySnapshot(4)
	require.Equal(t, now, snapshot.GeneratedAt)
	require.Equal(t, 3, snapshot.Runtime.TrackedRegionCount)
	require.Equal(t, 1, snapshot.Runtime.StalledSpanCount)
	require.Len(t, snapshot.Runtime.StalledSpans, 1)
	require.Equal(t, uint64(2), snapshot.Runtime.StalledSpans[0].SubscriptionID)
	require.Equal(t, int64(22), snapshot.Runtime.StalledSpans[0].TableID)
	require.Equal(t, stalledResolvedTs, snapshot.Runtime.StalledSpans[0].ResolvedTs)
	require.Equal(t, "20m0s", snapshot.Runtime.StalledSpans[0].ResolvedTsLag)
	require.Equal(t, "45s", snapshot.Runtime.StalledSpans[0].ResolvedTsUpdatedAgo)
	require.Equal(t, 2, snapshot.Runtime.StalledSpans[0].LockedRegionCount)
	require.Equal(t, 2, snapshot.Runtime.StalledSpans[0].UnlockedRangeCount)
	require.Len(t, snapshot.Runtime.StalledSpans[0].BlockedBy, 4)
	require.Equal(t, SpanResolvedTsBlockerUninitializedRegion, snapshot.Runtime.StalledSpans[0].BlockedBy[0].Type)
	require.Equal(t, uint64(1), snapshot.Runtime.StalledSpans[0].BlockedBy[0].RegionID)
	require.NotNil(t, snapshot.Runtime.StalledSpans[0].BlockedBy[0].Initialized)
	require.False(t, *snapshot.Runtime.StalledSpans[0].BlockedBy[0].Initialized)
	require.NotNil(t, snapshot.Runtime.StalledSpans[0].BlockedBy[0].Runtime)
	require.Equal(t, "wait_initialized", snapshot.Runtime.StalledSpans[0].BlockedBy[0].Runtime.Phase)
	require.Equal(t, "tikv-2:20160", snapshot.Runtime.StalledSpans[0].BlockedBy[0].Runtime.StoreAddr)
	require.Equal(t, uint64(9), snapshot.Runtime.StalledSpans[0].BlockedBy[0].Runtime.WorkerID)
	require.Equal(t, SpanResolvedTsBlockerUnlockedRange, snapshot.Runtime.StalledSpans[0].BlockedBy[1].Type)
	require.Equal(t, SpanResolvedTsBlockerInitializedRegion, snapshot.Runtime.StalledSpans[0].BlockedBy[2].Type)
	require.NotNil(t, snapshot.Runtime.StalledSpans[0].BlockedBy[2].Runtime)
	require.Equal(t, "replicating", snapshot.Runtime.StalledSpans[0].BlockedBy[2].Runtime.Phase)
	require.Equal(t, "1m30s", snapshot.Runtime.StalledSpans[0].BlockedBy[2].Runtime.LastEventAgo)
	require.Equal(t, SpanResolvedTsBlockerUnlockedRange, snapshot.Runtime.StalledSpans[0].BlockedBy[3].Type)
	require.Len(t, snapshot.Stores, 1)
	require.Equal(t, "tikv-1:20160", snapshot.Stores[0].StoreAddr)
	require.Equal(t, 1, snapshot.Stores[0].ActiveRegionCount)
	require.Equal(t, 1, snapshot.Stores[0].RequestCache.Total)
	require.Equal(t, 1, snapshot.Stores[0].RequestCache.Queued)
	require.Len(t, snapshot.Stores[0].Workers, 1)
	require.Equal(t, WorkerSessionStateRunning, snapshot.Stores[0].Workers[0].SessionState)
}

func createRPCContext(addr string, peerID uint64, storeID uint64) *tikv.RPCContext {
	return &tikv.RPCContext{
		Addr: addr,
		Peer: &metapb.Peer{
			Id:      peerID,
			StoreId: storeID,
		},
	}
}
