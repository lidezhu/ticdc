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

import "time"

type regionRuntimeTracker struct {
	registry *regionRuntimeRegistry
}

func newRegionRuntimeTracker() *regionRuntimeTracker {
	return &regionRuntimeTracker{
		registry: newRegionRuntimeRegistry(),
	}
}

func (t *regionRuntimeTracker) ensureRegion(region *regionInfo, now time.Time) {
	if t == nil || t.registry == nil {
		return
	}
	if region == nil || region.subscribedSpan == nil {
		return
	}
	if region.verID.GetID() == 0 {
		return
	}
	if !region.runtimeKey.isValid() {
		region.runtimeKey = t.registry.allocKey(region.subscribedSpan.subID, region.verID.GetID())
		t.registry.registerRegion(region.runtimeKey, *region, now)
	}
}

func (t *regionRuntimeTracker) updateRegionInfo(region regionInfo) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.updateRegionInfo(region.runtimeKey, region)
}

func (t *regionRuntimeTracker) transition(region regionInfo, phase regionPhase, now time.Time) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.transition(region.runtimeKey, phase, now)
}

func (t *regionRuntimeTracker) markRetryPending(region regionInfo, err error, now time.Time) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.markRetryPending(region.runtimeKey, err, now)
}

func (t *regionRuntimeTracker) markRPCReady(region regionInfo, now time.Time) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.markRPCReady(region.runtimeKey, now)
}

func (t *regionRuntimeTracker) markQueued(region regionInfo, acquiredTime, queuedTime time.Time) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.markQueued(region.runtimeKey, acquiredTime, queuedTime)
}

func (t *regionRuntimeTracker) setRequestEnqueueTime(region regionInfo, enqueueTime time.Time) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.setRequestEnqueueTime(region.runtimeKey, enqueueTime)
}

func (t *regionRuntimeTracker) recordError(region regionInfo, err error, now time.Time) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.recordError(region.runtimeKey, err, now)
}

func (t *regionRuntimeTracker) removeSubscription(subID SubscriptionID) {
	if t == nil || t.registry == nil {
		return
	}
	t.registry.removeBySubscription(subID)
}

func (t *regionRuntimeTracker) removeRegion(region regionInfo, now time.Time) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.transition(region.runtimeKey, regionPhaseRemoved, now)
	t.registry.remove(region.runtimeKey)
}

func (t *regionRuntimeTracker) markWaitInitialized(region regionInfo, workerID uint64, now time.Time) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.markWaitInitialized(region.runtimeKey, workerID, now)
}

func (t *regionRuntimeTracker) markReplicating(region regionInfo, now time.Time) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.markReplicating(region.runtimeKey, now)
}

func (t *regionRuntimeTracker) updateLastEvent(region regionInfo, now time.Time) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.updateLastEvent(region.runtimeKey, now)
}

func (t *regionRuntimeTracker) updateResolvedTs(region regionInfo, resolvedTs uint64, now time.Time) {
	if t == nil || t.registry == nil || !region.runtimeKey.isValid() {
		return
	}
	t.registry.updateResolvedTs(region.runtimeKey, resolvedTs, now)
}

func (t *regionRuntimeTracker) phaseCounts() map[regionPhase]int {
	if t == nil || t.registry == nil {
		return nil
	}
	return t.registry.phaseCounts()
}
