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

	client := &subscriptionClient{
		pdClock:               clock,
		regionRuntimeRegistry: newRegionRuntimeRegistry(),
		failureStats:          newFailureStats(),
	}
	client.ensureHelpers()

	region := createTestRegionInfo(1, 101)
	region.rpcCtx = createRPCContext("tikv-1:20160", 11, 22)
	region.runtimeKey = client.regionRuntimeRegistry.allocKey(region.subscribedSpan.subID, region.verID.GetID())
	client.regionRuntimeRegistry.markDiscovered(region.runtimeKey, region, now.Add(-30*time.Minute))
	client.regionRuntimeRegistry.markQueued(
		region.runtimeKey,
		now.Add(-29*time.Minute),
		now.Add(-29*time.Minute),
		oracle.GoTimeToTS(now.Add(-20*time.Minute)),
	)
	client.regionRuntimeRegistry.markRequestSent(region.runtimeKey, 7, now.Add(-28*time.Minute))
	client.regionRuntimeRegistry.markReplicating(region.runtimeKey, now.Add(-28*time.Minute))

	store := newRequestedStore(region.rpcCtx.Addr)
	worker := &regionRequestWorker{
		workerID:        7,
		store:           store,
		requestCache:    newRequestCache(10),
		runtimeRegistry: client.regionRuntimeRegistry,
	}
	ok, err := worker.add(context.Background(), createTestRegionInfo(1, 202), false)
	require.NoError(t, err)
	require.True(t, ok)

	session := newRegionRequestWorkerSession(
		worker.workerID,
		store.storeAddr,
		nil,
		nil,
		0,
		worker.requestCache,
		client.regionRuntimeRegistry,
		nil,
		nil,
	)
	session.setStage(WorkerSessionStateRunning)
	session.activeRegions.add(region.subscribedSpan.subID, region.verID.GetID(), &regionFeedState{})
	worker.setCurrentSession(session)
	store.addWorker(worker)
	client.requestedStores.stores.Store(store.storeAddr, store)

	span := client.newSubscribedSpan(SubscriptionID(2), heartbeatpb.TableSpan{
		TableID:  22,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
	}, 100, func(_ []common.RawKVEntry, _ func()) bool { return false }, func(uint64) {}, 0, false)
	client.subscribedSpans.add(span.subID, span)
	require.Equal(t, regionlock.LockRangeStatusSuccess,
		span.rangeLock.LockRange(context.Background(), []byte("a"), []byte("b"), 1, 1).Status)
	require.Equal(t, regionlock.LockRangeStatusSuccess,
		span.rangeLock.LockRange(context.Background(), []byte("c"), []byte("d"), 2, 1).Status)

	client.failureStats.record(
		newSendRequestToStoreFailure(region, regionFailureSourceWorkerSend, errors.New("boom")),
		now,
	)

	snapshot := client.GetObservabilitySnapshot(4)
	require.Equal(t, now, snapshot.GeneratedAt)
	require.Equal(t, 1, snapshot.Runtime.TrackedRegionCount)
	require.Equal(t, 1, snapshot.Runtime.SlowRegionCount)
	require.Len(t, snapshot.Runtime.SlowRegions, 1)
	require.Equal(t, "replicating", snapshot.Runtime.SlowRegions[0].Phase)
	require.Equal(t, "20m0s", snapshot.Runtime.SlowRegions[0].StuckFor)
	require.Equal(t, 1, snapshot.Runtime.SubscriptionWithUnlockedRange)
	require.Equal(t, 2, snapshot.Runtime.UnlockedRangeCount)
	require.Len(t, snapshot.Runtime.UnlockedRanges, 1)
	require.Len(t, snapshot.Stores, 1)
	require.Equal(t, "tikv-1:20160", snapshot.Stores[0].StoreAddr)
	require.Equal(t, 1, snapshot.Stores[0].ActiveRegionCount)
	require.Equal(t, 1, snapshot.Stores[0].RequestCache.Total)
	require.Equal(t, 1, snapshot.Stores[0].RequestCache.Queued)
	require.Len(t, snapshot.Stores[0].Workers, 1)
	require.Equal(t, WorkerSessionStateRunning, snapshot.Stores[0].Workers[0].SessionState)
	require.Len(t, snapshot.Failures, 1)
	require.Equal(t, "store_session", snapshot.Failures[0].Scope)
	require.Equal(t, "worker_send", snapshot.Failures[0].Source)
	require.Equal(t, "send_request_to_store", snapshot.Failures[0].Kind)
	require.Equal(t, uint64(1), snapshot.Failures[0].Count)
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
