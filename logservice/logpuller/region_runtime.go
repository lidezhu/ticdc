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
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/tikv/client-go/v2/oracle"
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

const (
	slowRegionPendingThreshold        = 10 * time.Minute
	slowRegionReplicatingLagThreshold = 6 * resolveLockMinInterval
)

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
	// timeline keeps the fixed milestones of the current attempt, so once the
	// region moves forward we can still tell how it got there.
	timeline regionRuntimeTimeline
}

type regionRuntimeTimeline struct {
	discoveredAt     time.Time
	rangeLockWaitAt  time.Time
	rangeLockedAt    time.Time
	queuedAt         time.Time
	rpcReadyAt       time.Time
	workerEnqueuedAt time.Time
	requestSentAt    time.Time
	replicatingSince time.Time
}

type slowRegionReport struct {
	totalRegionCount int
	slowRegionCount  int
	phaseCounts      map[regionPhase]int
	samples          []slowRegionSample
}

type slowRegionSample struct {
	SubscriptionID uint64
	RegionID       uint64
	Phase          regionPhase
	// StuckFor is phase age for pending phases and resolved-ts lag for
	// replicating phases.
	StuckFor  time.Duration
	StoreAddr string
	WorkerID  uint64
	LastError string
	Span      string
}

type slowRegionCandidate struct {
	state    *regionRuntimeState
	stuckFor time.Duration
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
		state.timeline.rangeLockWaitAt = waitTime
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
		state.timeline.discoveredAt = discoveredTime
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
		state.timeline.workerEnqueuedAt = enqueueTime
	})
}

func (r *regionRuntimeRegistry) markQueued(
	key regionRuntimeKey,
	rangeLockTime time.Time,
	queuedTime time.Time,
	initialResolvedTs uint64,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.timeline.rangeLockedAt = rangeLockTime
		state.timeline.queuedAt = queuedTime
		state.lastResolvedTs = initialResolvedTs
		state.phase = regionPhaseQueued
		state.phaseSince = queuedTime
	})
}

func (r *regionRuntimeRegistry) markRPCReady(
	key regionRuntimeKey,
	rpcReadyTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.timeline.rpcReadyAt = rpcReadyTime
		state.phase = regionPhaseRPCReady
		state.phaseSince = rpcReadyTime
	})
}

func (r *regionRuntimeRegistry) markRequestSent(
	key regionRuntimeKey,
	workerID uint64,
	sendTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.workerID = workerID
		state.timeline.requestSentAt = sendTime
		state.phase = regionPhaseWaitInitialized
		state.phaseSince = sendTime
	})
}

func (r *regionRuntimeRegistry) markReplicating(
	key regionRuntimeKey,
	replicatingTime time.Time,
) regionRuntimeState {
	return r.upsert(key, func(state *regionRuntimeState) {
		state.timeline.replicatingSince = replicatingTime
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

func (r *regionRuntimeRegistry) getLatest(
	subID SubscriptionID,
	regionID uint64,
) (regionRuntimeState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	identity := regionRuntimeIdentity{subID: subID, regionID: regionID}
	for generation := r.generations[identity]; generation > 0; generation-- {
		key := regionRuntimeKey{
			subID:      subID,
			regionID:   regionID,
			generation: generation,
		}
		state, ok := r.states[key]
		if ok {
			return state.clone(), true
		}
	}
	return regionRuntimeState{}, false
}

func (r *regionRuntimeRegistry) snapshot() []regionRuntimeState {
	r.mu.RLock()
	snapshots := make([]regionRuntimeState, 0, len(r.states))
	for _, state := range r.states {
		snapshots = append(snapshots, state.clone())
	}
	r.mu.RUnlock()

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

func normalizeDuration(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

func (s regionRuntimeState) phaseAge(now time.Time) time.Duration {
	if s.phaseSince.IsZero() {
		return 0
	}
	return normalizeDuration(now.Sub(s.phaseSince))
}

func (s regionRuntimeState) resolvedLag(now time.Time) time.Duration {
	if s.lastResolvedTs == 0 {
		return 0
	}
	return normalizeDuration(now.Sub(oracle.GetTimeFromTS(s.lastResolvedTs)))
}

func (s regionRuntimeState) slowThreshold() (time.Duration, bool) {
	switch s.phase {
	case regionPhaseDiscovered,
		regionPhaseRangeLockWait,
		regionPhaseQueued,
		regionPhaseRPCReady,
		regionPhaseWaitInitialized,
		regionPhaseRetryPending:
		return slowRegionPendingThreshold, true
	case regionPhaseReplicating:
		return slowRegionReplicatingLagThreshold, true
	default:
		return 0, false
	}
}

func (s regionRuntimeState) slowDuration(now time.Time) time.Duration {
	if s.phase == regionPhaseReplicating {
		if lag := s.resolvedLag(now); lag > 0 {
			return lag
		}
	}
	return s.phaseAge(now)
}

func betterSlowRegionCandidate(left, right slowRegionCandidate) bool {
	if left.stuckFor != right.stuckFor {
		return left.stuckFor > right.stuckFor
	}
	if left.state.key.subID != right.state.key.subID {
		return left.state.key.subID < right.state.key.subID
	}
	if left.state.key.regionID != right.state.key.regionID {
		return left.state.key.regionID < right.state.key.regionID
	}
	return left.state.key.generation < right.state.key.generation
}

func insertSlowRegionCandidate(
	candidates []slowRegionCandidate,
	candidate slowRegionCandidate,
	limit int,
) []slowRegionCandidate {
	if limit <= 0 {
		return candidates
	}

	originalLen := len(candidates)
	if originalLen == limit && !betterSlowRegionCandidate(candidate, candidates[originalLen-1]) {
		return candidates
	}

	insertAt := sort.Search(originalLen, func(i int) bool {
		return betterSlowRegionCandidate(candidate, candidates[i])
	})
	if originalLen < limit {
		candidates = append(candidates, slowRegionCandidate{})
		copy(candidates[insertAt+1:], candidates[insertAt:originalLen])
		candidates[insertAt] = candidate
		return candidates
	}

	copy(candidates[insertAt+1:], candidates[insertAt:originalLen-1])
	candidates[insertAt] = candidate
	return candidates
}

func makeSlowRegionSample(candidate slowRegionCandidate) slowRegionSample {
	return slowRegionSample{
		SubscriptionID: uint64(candidate.state.key.subID),
		RegionID:       candidate.state.key.regionID,
		Phase:          candidate.state.phase,
		StuckFor:       candidate.stuckFor,
		StoreAddr:      candidate.state.storeAddr,
		WorkerID:       candidate.state.workerID,
		LastError:      candidate.state.lastError,
		Span:           common.FormatTableSpan(&candidate.state.span),
	}
}

func (r *regionRuntimeRegistry) slowRegionCounts(now time.Time) (int, map[regionPhase]int) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	counts := make(map[regionPhase]int)
	total := 0
	for _, state := range r.states {
		threshold, ok := state.slowThreshold()
		if !ok {
			continue
		}
		if state.slowDuration(now) <= threshold {
			continue
		}
		total++
		counts[state.phase]++
	}
	return total, counts
}

// collectSlowRegionReport keeps slow-region logging aggregated: one tick gets
// one summary plus a capped sample set instead of one log line per region.
func (r *regionRuntimeRegistry) collectSlowRegionReport(
	now time.Time,
	sampleLimit int,
) slowRegionReport {
	report := slowRegionReport{
		phaseCounts: make(map[regionPhase]int),
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if sampleLimit < 0 {
		candidates := make([]slowRegionCandidate, 0)
		for _, state := range r.states {
			report.totalRegionCount++

			threshold, ok := state.slowThreshold()
			if !ok {
				continue
			}
			stuckFor := state.slowDuration(now)
			if stuckFor <= threshold {
				continue
			}

			report.slowRegionCount++
			report.phaseCounts[state.phase]++
			candidates = append(candidates, slowRegionCandidate{
				state:    state,
				stuckFor: stuckFor,
			})
		}

		sort.Slice(candidates, func(i, j int) bool {
			return betterSlowRegionCandidate(candidates[i], candidates[j])
		})
		report.samples = make([]slowRegionSample, 0, len(candidates))
		for _, candidate := range candidates {
			report.samples = append(report.samples, makeSlowRegionSample(candidate))
		}
		return report
	}

	candidates := make([]slowRegionCandidate, 0, sampleLimit)
	for _, state := range r.states {
		report.totalRegionCount++

		threshold, ok := state.slowThreshold()
		if !ok {
			continue
		}
		stuckFor := state.slowDuration(now)
		if stuckFor <= threshold {
			continue
		}

		report.slowRegionCount++
		report.phaseCounts[state.phase]++
		candidates = insertSlowRegionCandidate(candidates, slowRegionCandidate{
			state:    state,
			stuckFor: stuckFor,
		}, sampleLimit)
	}

	report.samples = make([]slowRegionSample, 0, len(candidates))
	for _, candidate := range candidates {
		report.samples = append(report.samples, makeSlowRegionSample(candidate))
	}
	return report
}
