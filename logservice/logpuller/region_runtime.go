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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
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
	generation     uint64
	Phase          regionPhase
	// StuckFor is phase age for pending phases and resolved-ts lag for
	// replicating phases.
	StuckFor  time.Duration
	StoreAddr string
	WorkerID  uint64
	LastError string
	Span      string
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

func (r *regionRuntimeRegistry) discoverRegion(region *regionInfo, now time.Time) bool {
	if region == nil || region.isStopRequest() {
		return false
	}
	if region.runtimeKey.isValid() {
		return true
	}
	if region.verID.GetID() == 0 || region.subscribedSpan == nil {
		log.Warn("skip region runtime discovery without region identity",
			zap.Uint64("regionID", region.verID.GetID()))
		return false
	}
	region.runtimeKey = r.allocKey(region.subscribedSpan.subID, region.verID.GetID())
	r.markDiscovered(region.runtimeKey, *region, now)
	return true
}

func (r *regionRuntimeRegistry) runtimeKeyForRegion(
	region regionInfo,
	action string,
) (regionRuntimeKey, bool) {
	if region.runtimeKey.isValid() {
		return region.runtimeKey, true
	}
	if region.isStopRequest() {
		return regionRuntimeKey{}, false
	}
	fields := []zap.Field{
		zap.String("action", action),
		zap.Uint64("regionID", region.verID.GetID()),
	}
	if region.subscribedSpan != nil {
		fields = append(fields, zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)))
	}
	log.Warn("skip region runtime update without key", fields...)
	return regionRuntimeKey{}, false
}

func (r *regionRuntimeRegistry) markRegionRangeLockWait(region regionInfo, now time.Time) {
	key, ok := r.runtimeKeyForRegion(region, "range lock wait")
	if !ok {
		return
	}
	r.markRangeLockWait(key, now)
}

func (r *regionRuntimeRegistry) markRegionRetryPending(region regionInfo, err error, now time.Time) {
	key, ok := r.runtimeKeyForRegion(region, "retry pending")
	if !ok {
		return
	}
	r.markRetryPending(key, err, now)
}

func (r *regionRuntimeRegistry) markRegionRPCReady(region regionInfo, now time.Time) {
	key, ok := r.runtimeKeyForRegion(region, "rpc ready")
	if !ok {
		return
	}
	r.updateRegionInfo(key, region)
	r.markRPCReady(key, now)
}

func (r *regionRuntimeRegistry) markRegionQueued(region regionInfo, acquiredTime, queuedTime time.Time) {
	key, ok := r.runtimeKeyForRegion(region, "queued")
	if !ok {
		return
	}
	r.markQueued(key, acquiredTime, queuedTime, region.resolvedTs())
}

func (r *regionRuntimeRegistry) markRegionWorkerEnqueued(region regionInfo, now time.Time) {
	key, ok := r.runtimeKeyForRegion(region, "worker enqueued")
	if !ok {
		return
	}
	r.markRequestEnqueued(key, now)
}

func (r *regionRuntimeRegistry) markRegionRequestSent(region regionInfo, workerID uint64, now time.Time) {
	key, ok := r.runtimeKeyForRegion(region, "request sent")
	if !ok {
		return
	}
	r.markRequestSent(key, workerID, now)
}

func (r *regionRuntimeRegistry) recordRegionError(region regionInfo, err error, now time.Time) {
	key, ok := r.runtimeKeyForRegion(region, "record error")
	if !ok {
		return
	}
	r.recordError(key, err, now)
}

func (r *regionRuntimeRegistry) removeRegion(region regionInfo, now time.Time) {
	key, ok := r.runtimeKeyForRegion(region, "remove")
	if !ok {
		return
	}
	r.markRemoved(key, now)
	r.remove(key)
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

func (s regionRuntimeState) slowSample(stuckFor time.Duration) slowRegionSample {
	return slowRegionSample{
		SubscriptionID: uint64(s.key.subID),
		RegionID:       s.key.regionID,
		generation:     s.key.generation,
		Phase:          s.phase,
		StuckFor:       stuckFor,
		StoreAddr:      s.storeAddr,
		WorkerID:       s.workerID,
		LastError:      s.lastError,
		Span:           common.FormatTableSpan(&s.span),
	}
}

func (s slowRegionSample) shouldAppearBefore(other slowRegionSample) bool {
	if s.StuckFor != other.StuckFor {
		return s.StuckFor > other.StuckFor
	}
	if s.SubscriptionID != other.SubscriptionID {
		return s.SubscriptionID < other.SubscriptionID
	}
	if s.RegionID != other.RegionID {
		return s.RegionID < other.RegionID
	}
	return s.generation < other.generation
}

func (r *slowRegionReport) recordSlowRegion(
	state regionRuntimeState,
	stuckFor time.Duration,
	sampleLimit int,
) {
	r.slowRegionCount++
	r.phaseCounts[state.phase]++
	if sampleLimit == 0 {
		return
	}

	sample := state.slowSample(stuckFor)
	if sampleLimit < 0 {
		r.samples = append(r.samples, sample)
		return
	}

	insertAt := len(r.samples)
	for i, existingSample := range r.samples {
		if sample.shouldAppearBefore(existingSample) {
			insertAt = i
			break
		}
	}
	if insertAt == len(r.samples) && len(r.samples) >= sampleLimit {
		return
	}
	if len(r.samples) < sampleLimit {
		r.samples = append(r.samples, slowRegionSample{})
	}
	copy(r.samples[insertAt+1:], r.samples[insertAt:])
	r.samples[insertAt] = sample
	if len(r.samples) > sampleLimit {
		r.samples = r.samples[:sampleLimit]
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

	if sampleLimit > 0 {
		report.samples = make([]slowRegionSample, 0, sampleLimit)
	}
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

		report.recordSlowRegion(state.clone(), stuckFor, sampleLimit)
	}

	if sampleLimit < 0 {
		sort.Slice(report.samples, func(i, j int) bool {
			return report.samples[i].shouldAppearBefore(report.samples[j])
		})
	}
	return report
}
