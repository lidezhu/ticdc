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
	"errors"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

func TestRegionRuntimeRegistryAllocKey(t *testing.T) {
	registry := newRegionRuntimeRegistry()

	key1 := registry.allocKey(1, 101)
	key2 := registry.allocKey(1, 101)
	key3 := registry.allocKey(1, 102)

	require.Equal(t, regionRuntimeKey{subID: 1, regionID: 101, generation: 1}, key1)
	require.Equal(t, regionRuntimeKey{subID: 1, regionID: 101, generation: 2}, key2)
	require.Equal(t, regionRuntimeKey{subID: 1, regionID: 102, generation: 1}, key3)
}

func TestRegionRuntimeRegistryUpdateAndSnapshot(t *testing.T) {
	registry := newRegionRuntimeRegistry()
	key := registry.allocKey(1, 101)
	now := time.Unix(1700000000, 0)

	subSpan := &subscribedSpan{
		subID: 1,
		span: heartbeatpb.TableSpan{
			TableID:  42,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
	}
	region := regionInfo{
		verID: tikv.NewRegionVerID(101, 2, 3),
		span: heartbeatpb.TableSpan{
			TableID:  42,
			StartKey: []byte("b"),
			EndKey:   []byte("c"),
		},
		rpcCtx: &tikv.RPCContext{
			Addr: "tikv-1:20160",
			Peer: &metapb.Peer{Id: 11, StoreId: 22},
		},
		subscribedSpan: subSpan,
	}

	registry.markDiscovered(key, region, now)
	registry.markRangeLockWait(key, now.Add(250*time.Millisecond))
	registry.markQueued(key, now.Add(time.Second), now.Add(2*time.Second), 100)
	registry.markRPCReady(key, now.Add(2500*time.Millisecond))
	registry.markRequestEnqueued(key, now.Add(2700*time.Millisecond))
	registry.markRequestSent(key, 7, now.Add(3*time.Second))
	registry.updateResolvedTs(key, 12345, now.Add(4*time.Second))
	registry.recordError(key, errors.New("store busy"), now.Add(5*time.Second))

	state, ok := registry.get(key)
	require.True(t, ok)
	require.Equal(t, int64(42), state.tableID)
	require.Equal(t, regionPhaseWaitInitialized, state.phase)
	require.Equal(t, uint64(22), state.leaderStoreID)
	require.Equal(t, uint64(11), state.leaderPeerID)
	require.Equal(t, "tikv-1:20160", state.storeAddr)
	require.Equal(t, uint64(7), state.workerID)
	require.Equal(t, uint64(12345), state.lastResolvedTs)
	require.Equal(t, "store busy", state.lastError)
	require.Equal(t, 0, state.retryCount)
	require.Equal(t, now, state.timeline.discoveredAt)
	require.Equal(t, now.Add(250*time.Millisecond), state.timeline.rangeLockWaitAt)
	require.Equal(t, now.Add(time.Second), state.timeline.rangeLockedAt)
	require.Equal(t, now.Add(2*time.Second), state.timeline.queuedAt)
	require.Equal(t, now.Add(2500*time.Millisecond), state.timeline.rpcReadyAt)
	require.Equal(t, now.Add(3*time.Second), state.phaseSince)
	require.Equal(t, now.Add(3*time.Second), state.timeline.requestSentAt)
	require.Equal(t, now.Add(2700*time.Millisecond), state.timeline.workerEnqueuedAt)

	snapshots := registry.snapshot()
	require.Len(t, snapshots, 1)
	require.Equal(t, state, snapshots[0])

	snapshots[0].span.StartKey[0] = 'x'
	updated, ok := registry.get(key)
	require.True(t, ok)
	require.Equal(t, []byte("b"), updated.span.StartKey)
}

func TestRegionRuntimeRegistryMarkReplicating(t *testing.T) {
	registry := newRegionRuntimeRegistry()
	key := registry.allocKey(1, 101)
	now := time.Unix(1700000100, 0)

	registry.markReplicating(key, now)

	state, ok := registry.get(key)
	require.True(t, ok)
	require.Equal(t, regionPhaseReplicating, state.phase)
	require.Equal(t, now, state.timeline.replicatingSince)
	require.Equal(t, now, state.phaseSince)
}

func TestRegionRuntimeRegistryRemoveBySubscription(t *testing.T) {
	registry := newRegionRuntimeRegistry()
	key1 := registry.allocKey(1, 101)
	key2 := registry.allocKey(1, 102)
	key3 := registry.allocKey(2, 201)

	registry.markDiscovered(key1, regionInfo{}, time.Unix(1, 0))
	registry.markRemoved(key2, time.Unix(2, 0))
	registry.markReplicating(key3, time.Unix(3, 0))

	require.Len(t, registry.snapshot(), 3)
	require.Equal(t, 2, registry.removeBySubscription(1))

	_, ok := registry.get(key1)
	require.False(t, ok)
	_, ok = registry.get(key2)
	require.False(t, ok)

	state, ok := registry.get(key3)
	require.True(t, ok)
	require.Equal(t, regionPhaseReplicating, state.phase)
	require.Len(t, registry.snapshot(), 1)
}

func TestRegionRuntimeRegistryPhaseCounts(t *testing.T) {
	registry := newRegionRuntimeRegistry()
	now := time.Unix(1700000000, 0)

	key1 := registry.allocKey(1, 101)
	key2 := registry.allocKey(1, 102)
	key3 := registry.allocKey(2, 201)

	registry.markQueued(key1, now, now, 100)
	registry.markQueued(key2, now.Add(time.Second), now.Add(time.Second), 100)
	registry.markRequestSent(key3, 0, now.Add(2*time.Second))

	counts := registry.phaseCounts()
	require.Equal(t, 2, counts[regionPhaseQueued])
	require.Equal(t, 1, counts[regionPhaseWaitInitialized])
	require.Equal(t, 0, counts[regionPhaseReplicating])
}

func TestRegionRuntimeRegistryCollectSlowRegionReport(t *testing.T) {
	registry := newRegionRuntimeRegistry()
	now := time.Unix(1700003600, 0)

	replicatingKey := registry.allocKey(1, 101)
	replicatingRegion := regionInfo{
		span: heartbeatpb.TableSpan{
			TableID:  11,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
		subscribedSpan: &subscribedSpan{
			subID: 1,
			span:  heartbeatpb.TableSpan{TableID: 11},
		},
	}
	registry.markDiscovered(replicatingKey, replicatingRegion, now.Add(-30*time.Minute))
	registry.markQueued(
		replicatingKey,
		now.Add(-29*time.Minute),
		now.Add(-29*time.Minute),
		oracle.GoTimeToTS(now.Add(-20*time.Minute)),
	)
	registry.markRequestSent(replicatingKey, 7, now.Add(-28*time.Minute))
	registry.markReplicating(replicatingKey, now.Add(-28*time.Minute))

	queuedKey := registry.allocKey(1, 102)
	queuedRegion := regionInfo{
		span: heartbeatpb.TableSpan{
			TableID:  12,
			StartKey: []byte("b"),
			EndKey:   []byte("c"),
		},
		subscribedSpan: &subscribedSpan{
			subID: 1,
			span:  heartbeatpb.TableSpan{TableID: 12},
		},
	}
	registry.markDiscovered(queuedKey, queuedRegion, now.Add(-12*time.Minute))
	registry.markQueued(queuedKey, now.Add(-11*time.Minute), now.Add(-11*time.Minute), 100)

	recentKey := registry.allocKey(2, 201)
	recentRegion := regionInfo{
		span: heartbeatpb.TableSpan{
			TableID:  21,
			StartKey: []byte("x"),
			EndKey:   []byte("y"),
		},
		subscribedSpan: &subscribedSpan{
			subID: 2,
			span:  heartbeatpb.TableSpan{TableID: 21},
		},
	}
	registry.markDiscovered(recentKey, recentRegion, now.Add(-2*time.Minute))
	registry.markQueued(recentKey, now.Add(-time.Minute), now.Add(-time.Minute), 100)

	report := registry.collectSlowRegionReport(now, 2)
	require.Equal(t, 3, report.totalRegionCount)
	require.Equal(t, 2, report.slowRegionCount)
	require.Equal(t, 1, report.phaseCounts[regionPhaseReplicating])
	require.Equal(t, 1, report.phaseCounts[regionPhaseQueued])
	require.Len(t, report.samples, 2)

	require.Equal(t, uint64(1), report.samples[0].SubscriptionID)
	require.Equal(t, uint64(101), report.samples[0].RegionID)
	require.Equal(t, regionPhaseReplicating, report.samples[0].Phase)
	require.Equal(t, 20*time.Minute, report.samples[0].StuckFor)

	require.Equal(t, uint64(102), report.samples[1].RegionID)
	require.Equal(t, regionPhaseQueued, report.samples[1].Phase)
	require.Equal(t, 11*time.Minute, report.samples[1].StuckFor)
}
