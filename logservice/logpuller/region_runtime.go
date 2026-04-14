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
	"sort"
	"sync"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/tikv/client-go/v2/tikv"
)

type regionPhase string

const (
	regionPhaseUnknown         regionPhase = "unknown"
	regionPhaseDiscovered      regionPhase = "discovered"
	regionPhaseRangeLockWait   regionPhase = "range_lock_wait"
	regionPhaseQueued          regionPhase = "queued"
	regionPhaseRPCReady        regionPhase = "rpc_ready"
	regionPhaseWaitInitialized regionPhase = "wait_initialized"
	regionPhaseReplicating     regionPhase = "replicating"
	regionPhaseRetryPending    regionPhase = "retry_pending"
	regionPhaseRemoved         regionPhase = "removed"
)

var regionRuntimePhases = []regionPhase{
	regionPhaseUnknown,
	regionPhaseDiscovered,
	regionPhaseRangeLockWait,
	regionPhaseQueued,
	regionPhaseRPCReady,
	regionPhaseWaitInitialized,
	regionPhaseReplicating,
	regionPhaseRetryPending,
	regionPhaseRemoved,
}

type regionRuntimeIdentity struct {
	subID    SubscriptionID
	regionID uint64
}

type regionRuntimeKey struct {
	subID    SubscriptionID
	regionID uint64
	// generation distinguishes different runtime attempts of the same
	// subscription-region pair after retries / reloads.
	generation uint64
}

func (k regionRuntimeKey) isValid() bool {
	return k.subID != InvalidSubscriptionID && k.regionID != 0 && k.generation != 0
}

type regionRuntimeState struct {
	key regionRuntimeKey

	tableID int64
	span    heartbeatpb.TableSpan
	verID   tikv.RegionVerID

	leaderStoreID uint64
	leaderPeerID  uint64
	storeAddr     string
	workerID      uint64

	phase      regionPhase
	phaseSince time.Time

	lastEventTime  time.Time
	lastResolvedTs uint64

	lastError     string
	lastErrorTime time.Time
	retryCount    int

	// phaseSince tracks the current phase boundary.
	// The fields below keep distinct milestones for the current attempt even
	// after the phase has moved forward.
	rangeLockTime     time.Time
	workerEnqueueTime time.Time
	rpcReadyTime      time.Time
	requestSentTime   time.Time
	replicatingSince  time.Time
}

func (s regionRuntimeState) clone() regionRuntimeState {
	s.span = cloneTableSpan(s.span)
	return s
}

func (s *regionRuntimeState) applyRegionInfo(region regionInfo) {
	if region.subscribedSpan != nil {
		s.tableID = region.subscribedSpan.span.TableID
	}
	s.span = cloneTableSpan(region.span)
	s.verID = region.verID
	if region.rpcCtx == nil {
		return
	}

	s.storeAddr = region.rpcCtx.Addr
	if region.rpcCtx.Peer != nil {
		s.leaderPeerID = region.rpcCtx.Peer.Id
		s.leaderStoreID = region.rpcCtx.Peer.StoreId
	}
}

func cloneTableSpan(span heartbeatpb.TableSpan) heartbeatpb.TableSpan {
	cloned := span
	if len(span.StartKey) > 0 {
		cloned.StartKey = append([]byte(nil), span.StartKey...)
	}
	if len(span.EndKey) > 0 {
		cloned.EndKey = append([]byte(nil), span.EndKey...)
	}
	return cloned
}

// regionRuntimeRegistry keeps the latest lifecycle snapshot for each region
// request attempt so slow / stuck regions can be inspected without walking the
// scheduler, worker, and region state structures together.
type regionRuntimeRegistry struct {
	mu sync.RWMutex

	states      map[regionRuntimeKey]*regionRuntimeState
	generations map[regionRuntimeIdentity]uint64
}

func newRegionRuntimeRegistry() *regionRuntimeRegistry {
	return &regionRuntimeRegistry{
		states:      make(map[regionRuntimeKey]*regionRuntimeState),
		generations: make(map[regionRuntimeIdentity]uint64),
	}
}

func (r *regionRuntimeRegistry) allocKey(subID SubscriptionID, regionID uint64) regionRuntimeKey {
	r.mu.Lock()
	defer r.mu.Unlock()

	identity := regionRuntimeIdentity{subID: subID, regionID: regionID}
	r.generations[identity]++
	return regionRuntimeKey{
		subID:      subID,
		regionID:   regionID,
		generation: r.generations[identity],
	}
}

func (r *regionRuntimeRegistry) upsert(
	key regionRuntimeKey,
	update func(*regionRuntimeState),
) regionRuntimeState {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, ok := r.states[key]
	if !ok {
		state = &regionRuntimeState{
			key:   key,
			phase: regionPhaseUnknown,
		}
		r.states[key] = state
	}
	if update != nil {
		update(state)
	}
	return state.clone()
}

func (r *regionRuntimeRegistry) markRangeLockWait(
	key regionRuntimeKey,
	waitTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.phase = regionPhaseRangeLockWait
		state.phaseSince = waitTime
	})
}

func (r *regionRuntimeRegistry) updateRegionInfo(
	key regionRuntimeKey,
	region regionInfo,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.applyRegionInfo(region)
	})
}

func (r *regionRuntimeRegistry) markDiscovered(
	key regionRuntimeKey,
	region regionInfo,
	discoveredTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.applyRegionInfo(region)
		state.phase = regionPhaseDiscovered
		state.phaseSince = discoveredTime
	})
}

func (r *regionRuntimeRegistry) updateLastEvent(
	key regionRuntimeKey,
	lastEventTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.lastEventTime = lastEventTime
	})
}

func (r *regionRuntimeRegistry) updateResolvedTs(
	key regionRuntimeKey,
	resolvedTs uint64,
	lastEventTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.lastResolvedTs = resolvedTs
		if !lastEventTime.IsZero() {
			state.lastEventTime = lastEventTime
		}
	})
}

func (r *regionRuntimeRegistry) recordError(
	key regionRuntimeKey,
	err error,
	errTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		if err == nil {
			state.lastError = ""
		} else {
			state.lastError = err.Error()
		}
		state.lastErrorTime = errTime
	})
}

func (r *regionRuntimeRegistry) markRetryPending(
	key regionRuntimeKey,
	err error,
	retryTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		if err == nil {
			state.lastError = ""
		} else {
			state.lastError = err.Error()
		}
		state.lastErrorTime = retryTime
		state.retryCount++
		state.phase = regionPhaseRetryPending
		state.phaseSince = retryTime
	})
}

func (r *regionRuntimeRegistry) markRequestEnqueued(
	key regionRuntimeKey,
	enqueueTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.workerEnqueueTime = enqueueTime
	})
}

func (r *regionRuntimeRegistry) markQueued(
	key regionRuntimeKey,
	rangeLockTime time.Time,
	queuedTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.rangeLockTime = rangeLockTime
		state.phase = regionPhaseQueued
		state.phaseSince = queuedTime
	})
}

func (r *regionRuntimeRegistry) markRPCReady(
	key regionRuntimeKey,
	rpcReadyTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.rpcReadyTime = rpcReadyTime
		state.phase = regionPhaseRPCReady
		state.phaseSince = rpcReadyTime
	})
}

func (r *regionRuntimeRegistry) markWaitInitialized(
	key regionRuntimeKey,
	workerID uint64,
	sendTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.workerID = workerID
		state.requestSentTime = sendTime
		state.phase = regionPhaseWaitInitialized
		state.phaseSince = sendTime
	})
}

func (r *regionRuntimeRegistry) markReplicating(
	key regionRuntimeKey,
	replicatingTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.replicatingSince = replicatingTime
		state.phase = regionPhaseReplicating
		state.phaseSince = replicatingTime
	})
}

func (r *regionRuntimeRegistry) markRemoved(
	key regionRuntimeKey,
	removedTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.phase = regionPhaseRemoved
		state.phaseSince = removedTime
	})
}

func (r *regionRuntimeRegistry) get(key regionRuntimeKey) (regionRuntimeState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	state, ok := r.states[key]
	if !ok {
		return regionRuntimeState{}, false
	}
	return state.clone(), true
}

func (r *regionRuntimeRegistry) snapshot() []regionRuntimeState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snapshots := make([]regionRuntimeState, 0, len(r.states))
	for _, state := range r.states {
		snapshots = append(snapshots, state.clone())
	}
	sort.Slice(snapshots, func(i, j int) bool {
		left, right := snapshots[i].key, snapshots[j].key
		if left.subID != right.subID {
			return left.subID < right.subID
		}
		if left.regionID != right.regionID {
			return left.regionID < right.regionID
		}
		return left.generation < right.generation
	})
	return snapshots
}

func (r *regionRuntimeRegistry) phaseCounts() map[regionPhase]int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	counts := make(map[regionPhase]int, len(r.states))
	for _, state := range r.states {
		counts[state.phase]++
	}
	return counts
}

func (r *regionRuntimeRegistry) remove(key regionRuntimeKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.states[key]; !ok {
		return false
	}
	delete(r.states, key)
	return true
}

func (r *regionRuntimeRegistry) removeBySubscription(subID SubscriptionID) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	removed := 0
	for key := range r.states {
		if key.subID != subID {
			continue
		}
		delete(r.states, key)
		removed++
	}
	return removed
}
