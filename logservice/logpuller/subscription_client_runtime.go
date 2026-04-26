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

func (s *subscriptionClient) ensureHelpers() {
	if s.requestedStores == nil {
		s.requestedStores = newRequestedStoreSet(s)
	}
	if s.regionScheduler == nil {
		s.regionScheduler = newRegionRequestScheduler(s)
	}
	if s.subscribedSpans == nil {
		s.subscribedSpans = newSubscribedSpanSet(s)
	}
	if s.staleLockResolver == nil {
		s.staleLockResolver = newStaleLockResolver(s)
	}
	if s.regionRuntimeRegistry == nil {
		s.regionRuntimeRegistry = newRegionRuntimeRegistry()
	}
	if s.failureStats == nil {
		s.failureStats = newFailureStats()
	}
}

func (s *subscriptionClient) markRegionDiscovered(region *regionInfo, now time.Time) {
	if region.verID.GetID() == 0 {
		return
	}
	if !region.runtimeKey.isValid() {
		region.runtimeKey = s.regionRuntimeRegistry.allocKey(region.subscribedSpan.subID, region.verID.GetID())
		s.regionRuntimeRegistry.markDiscovered(region.runtimeKey, *region, now)
	}
}

func (s *subscriptionClient) updateRegionRuntimeInfo(region regionInfo) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.updateRegionInfo(region.runtimeKey, region)
}

func (s *subscriptionClient) markRegionRangeLockWait(region regionInfo, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markRangeLockWait(region.runtimeKey, now)
}

func (s *subscriptionClient) markRegionRetryPending(region regionInfo, err error, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markRetryPending(region.runtimeKey, err, now)
}

func (s *subscriptionClient) markRegionRPCReady(region regionInfo, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markRPCReady(region.runtimeKey, now)
}

func (s *subscriptionClient) markRegionQueued(region regionInfo, acquiredTime, queuedTime time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markQueued(region.runtimeKey, acquiredTime, queuedTime, region.resolvedTs())
}

func (s *subscriptionClient) recordRegionRuntimeError(region regionInfo, err error, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.recordError(region.runtimeKey, err, now)
}

func (s *subscriptionClient) regionRuntimePhaseCounts() map[regionPhase]int {
	return s.regionRuntimeRegistry.phaseCounts()
}

func (s *subscriptionClient) removeSubscriptionRuntime(subID SubscriptionID) {
	s.regionRuntimeRegistry.removeBySubscription(subID)
}

func (s *subscriptionClient) removeRegionRuntime(region regionInfo, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markRemoved(region.runtimeKey, now)
	s.regionRuntimeRegistry.remove(region.runtimeKey)
}
