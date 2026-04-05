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
	"strings"
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

type regionRuntimeIdentity struct {
	subID    SubscriptionID
	regionID uint64
}

type regionRuntimeKey struct {
	subID      SubscriptionID
	regionID   uint64
	generation uint64
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

	phase          regionPhase
	phaseEnterTime time.Time

	lastEventTime  time.Time
	lastResolvedTs uint64

	lastError     string
	lastErrorTime time.Time
	retryCount    int

	rangeLockAcquiredTime time.Time
	requestEnqueueTime    time.Time
	requestRPCReadyTime   time.Time
	requestSendTime       time.Time
	initializedTime       time.Time
	replicatingTime       time.Time
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

type regionRuntimeRegistry struct {
	mu sync.RWMutex

	states      map[regionRuntimeKey]*regionRuntimeState
	generations map[regionRuntimeIdentity]uint64
	retryCounts map[regionRuntimeIdentity]int
}

func newRegionRuntimeRegistry() *regionRuntimeRegistry {
	return &regionRuntimeRegistry{
		states:      make(map[regionRuntimeKey]*regionRuntimeState),
		generations: make(map[regionRuntimeIdentity]uint64),
		retryCounts: make(map[regionRuntimeIdentity]int),
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
		identity := regionRuntimeIdentity{subID: key.subID, regionID: key.regionID}
		state = &regionRuntimeState{
			key:        key,
			phase:      regionPhaseUnknown,
			retryCount: r.retryCounts[identity],
		}
		r.states[key] = state
	}
	if update != nil {
		update(state)
	}
	return state.clone()
}

func (r *regionRuntimeRegistry) transition(
	key regionRuntimeKey,
	phase regionPhase,
	phaseEnterTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.phase = phase
		state.phaseEnterTime = phaseEnterTime
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

func (r *regionRuntimeRegistry) updateWorker(key regionRuntimeKey, workerID uint64) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.workerID = workerID
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

func (r *regionRuntimeRegistry) incRetry(key regionRuntimeKey) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		identity := regionRuntimeIdentity{subID: key.subID, regionID: key.regionID}
		r.retryCounts[identity]++
		state.retryCount = r.retryCounts[identity]
	})
}

func (r *regionRuntimeRegistry) setRequestEnqueueTime(
	key regionRuntimeKey,
	enqueueTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.requestEnqueueTime = enqueueTime
	})
}

func (r *regionRuntimeRegistry) setRangeLockAcquiredTime(
	key regionRuntimeKey,
	acquiredTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.rangeLockAcquiredTime = acquiredTime
	})
}

func (r *regionRuntimeRegistry) setRPCReadyTime(
	key regionRuntimeKey,
	rpcReadyTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.requestRPCReadyTime = rpcReadyTime
	})
}

func (r *regionRuntimeRegistry) setRequestSendTime(
	key regionRuntimeKey,
	sendTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.requestSendTime = sendTime
	})
}

func (r *regionRuntimeRegistry) setInitializedTime(
	key regionRuntimeKey,
	initializedTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.initializedTime = initializedTime
	})
}

func (r *regionRuntimeRegistry) setReplicatingTime(
	key regionRuntimeKey,
	replicatingTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.replicatingTime = replicatingTime
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
	for identity := range r.generations {
		if identity.subID == subID {
			delete(r.generations, identity)
		}
	}
	for identity := range r.retryCounts {
		if identity.subID == subID {
			delete(r.retryCounts, identity)
		}
	}
	return removed
}

func (r *regionRuntimeRegistry) resetRetryCount(subID SubscriptionID, regionID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.retryCounts, regionRuntimeIdentity{subID: subID, regionID: regionID})
}

type regionSlowReason string

const (
	regionSlowReasonUnknown              regionSlowReason = "unknown"
	regionSlowReasonNoLeader             regionSlowReason = "no_leader"
	regionSlowReasonRPCCtxUnavailable    regionSlowReason = "rpc_ctx_unavailable"
	regionSlowReasonRangeLockWait        regionSlowReason = "range_lock_wait"
	regionSlowReasonQueueBlocked         regionSlowReason = "queue_blocked"
	regionSlowReasonWaitInitialized      regionSlowReason = "wait_initialized"
	regionSlowReasonResolvedTsNotAdvance regionSlowReason = "resolved_ts_not_advance"
	regionSlowReasonLockResolving        regionSlowReason = "lock_resolving"
	regionSlowReasonRetryStorm           regionSlowReason = "retry_storm"
	regionSlowReasonStoreBusy            regionSlowReason = "store_busy"
)

func inferRegionSlowReason(state regionRuntimeState, now time.Time) (regionSlowReason, bool) {
	switch state.phase {
	case regionPhaseRangeLockWait:
		return regionSlowReasonRangeLockWait, true
	case regionPhaseQueued, regionPhaseRPCReady:
		return regionSlowReasonQueueBlocked, true
	case regionPhaseWaitInitialized:
		return regionSlowReasonWaitInitialized, true
	case regionPhaseRetryPending:
		if state.retryCount >= 3 {
			return regionSlowReasonRetryStorm, true
		}
	}

	lastError := strings.ToLower(state.lastError)
	switch {
	case strings.Contains(lastError, "not_leader"), strings.Contains(lastError, "not leader"):
		return regionSlowReasonNoLeader, true
	case strings.Contains(lastError, "cannot get rpcctx"), strings.Contains(lastError, "rpcctx"):
		return regionSlowReasonRPCCtxUnavailable, true
	case strings.Contains(lastError, "congested"), strings.Contains(lastError, "server_is_busy"), strings.Contains(lastError, "server is busy"), strings.Contains(lastError, "get store error"), strings.Contains(lastError, "send request to store error"):
		return regionSlowReasonStoreBusy, true
	}

	if state.phase == regionPhaseReplicating && !state.lastEventTime.IsZero() && now.Sub(state.lastEventTime) > 6*resolveLockMinInterval {
		return regionSlowReasonResolvedTsNotAdvance, true
	}

	return regionSlowReasonUnknown, false
}

func (s *subscriptionClient) ensureRegionRuntime(region regionInfo) regionInfo {
	if s == nil || s.regionRuntimeRegistry == nil || region.isStopTask() {
		return region
	}
	if region.runtimeKey.generation != 0 {
		return region
	}
	region.runtimeKey = s.regionRuntimeRegistry.allocKey(region.subscribedSpan.subID, region.verID.GetID())
	s.regionRuntimeRegistry.updateRegionInfo(region.runtimeKey, region)
	s.regionRuntimeRegistry.transition(region.runtimeKey, regionPhaseDiscovered, time.Now())
	return region
}

func (s *subscriptionClient) newRegionRuntimeAttempt(region regionInfo) regionInfo {
	if s == nil || s.regionRuntimeRegistry == nil || region.isStopTask() {
		return region
	}
	if region.runtimeKey.generation != 0 {
		s.regionRuntimeRegistry.remove(region.runtimeKey)
	}
	region.runtimeKey = regionRuntimeKey{}
	return s.ensureRegionRuntime(region)
}

func (s *subscriptionClient) removeRegionRuntime(region regionInfo) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.remove(region.runtimeKey)
}

func (s *subscriptionClient) transitionRegionRuntime(region regionInfo, phase regionPhase, phaseEnterTime time.Time) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.transition(region.runtimeKey, phase, phaseEnterTime)
}

func (s *subscriptionClient) updateRegionRuntimeInfo(region regionInfo) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.updateRegionInfo(region.runtimeKey, region)
}

func (s *subscriptionClient) setRegionRuntimeRangeLockAcquiredTime(region regionInfo, acquiredTime time.Time) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.setRangeLockAcquiredTime(region.runtimeKey, acquiredTime)
}

func (s *subscriptionClient) setRegionRuntimeEnqueueTime(region regionInfo, enqueueTime time.Time) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.setRequestEnqueueTime(region.runtimeKey, enqueueTime)
}

func (s *subscriptionClient) setRegionRuntimeRPCReadyTime(region regionInfo, rpcReadyTime time.Time) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.setRPCReadyTime(region.runtimeKey, rpcReadyTime)
}

func (s *subscriptionClient) setRegionRuntimeSendTime(region regionInfo, sendTime time.Time) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.setRequestSendTime(region.runtimeKey, sendTime)
}

func (s *subscriptionClient) setRegionRuntimeInitializedTime(region regionInfo, initializedTime time.Time) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.setInitializedTime(region.runtimeKey, initializedTime)
}

func (s *subscriptionClient) setRegionRuntimeReplicatingTime(region regionInfo, replicatingTime time.Time) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.setReplicatingTime(region.runtimeKey, replicatingTime)
}

func (s *subscriptionClient) updateRegionRuntimeWorker(region regionInfo, workerID uint64) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.updateWorker(region.runtimeKey, workerID)
}

func (s *subscriptionClient) updateRegionRuntimeResolvedTs(region regionInfo, resolvedTs uint64, eventTime time.Time) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.updateResolvedTs(region.runtimeKey, resolvedTs, eventTime)
}

func (s *subscriptionClient) updateRegionRuntimeLastEvent(region regionInfo, eventTime time.Time) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.updateLastEvent(region.runtimeKey, eventTime)
}

func (s *subscriptionClient) recordRegionRuntimeError(region regionInfo, err error, errTime time.Time) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.recordError(region.runtimeKey, err, errTime)
}

func (s *subscriptionClient) incRegionRuntimeRetry(region regionInfo) {
	if s == nil || s.regionRuntimeRegistry == nil || region.runtimeKey.generation == 0 {
		return
	}
	s.regionRuntimeRegistry.incRetry(region.runtimeKey)
}
