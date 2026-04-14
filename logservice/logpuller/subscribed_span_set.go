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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type subscribedSpanSet struct {
	client  *subscriptionClient
	mu      sync.RWMutex
	spanMap map[SubscriptionID]*subscribedSpan
}

type subscribedSpanEntry struct {
	subID SubscriptionID
	span  *subscribedSpan
}

func newSubscribedSpanSet(client *subscriptionClient) *subscribedSpanSet {
	return &subscribedSpanSet{
		client:  client,
		spanMap: make(map[SubscriptionID]*subscribedSpan),
	}
}

func (s *subscribedSpanSet) add(subID SubscriptionID, rt *subscribedSpan) {
	s.mu.Lock()
	s.spanMap[subID] = rt
	s.mu.Unlock()
}

func (s *subscribedSpanSet) get(subID SubscriptionID) *subscribedSpan {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.spanMap[subID]
}

func (s *subscribedSpanSet) delete(subID SubscriptionID) {
	s.mu.Lock()
	delete(s.spanMap, subID)
	s.mu.Unlock()
}

func (s *subscribedSpanSet) snapshot() []subscribedSpanEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]subscribedSpanEntry, 0, len(s.spanMap))
	for subID, span := range s.spanMap {
		entries = append(entries, subscribedSpanEntry{
			subID: subID,
			span:  span,
		})
	}
	return entries
}

func (s *subscribedSpanSet) requestedRegionCount() int {
	count := 0
	s.mu.RLock()
	for _, rt := range s.spanMap {
		count += rt.rangeLock.Len()
	}
	s.mu.RUnlock()
	return count
}

func (s *subscribedSpanSet) newSubscribedSpan(
	subID SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	filterLoop bool,
) *subscribedSpan {
	rangeLock := regionlock.NewRangeLock(uint64(subID), span.StartKey, span.EndKey, startTs)

	rt := &subscribedSpan{
		subID:      subID,
		span:       span,
		startTs:    startTs,
		filterLoop: filterLoop,
		rangeLock:  rangeLock,

		consumeKVEvents:   consumeKVEvents,
		advanceResolvedTs: advanceResolvedTs,
		advanceInterval:   advanceInterval,
	}
	rt.initialized.Store(false)
	rt.resolvedTsUpdated.Store(time.Now().Unix())
	rt.resolvedTs.Store(startTs)

	rt.tryResolveLock = func(regionID uint64, state *regionlock.LockedRangeState) {
		targetTs := rt.staleLocksTargetTs.Load()
		if state.ResolvedTs.Load() < targetTs && state.Initialized.Load() {
			task := resolveLockTask{
				keyspaceID: span.KeyspaceID,
				regionID:   regionID,
				targetTs:   targetTs,
				state:      state,
				create:     time.Now(),
			}
			if !s.client.staleLockResolver.tryEnqueue(task) {
				metrics.SubscriptionClientResolveLockTaskDropCounter.Inc()
			}
		}
	}
	return rt
}

func (s *subscribedSpanSet) setTableStopped(rt *subscribedSpan) {
	log.Info("subscription client starts to stop table",
		zap.Uint64("subscriptionID", uint64(rt.subID)))

	// Set stopped to true so we can stop handling region events from the table.
	// Then send a special singleRegionInfo so every store worker deregisters the table.
	if rt.stopped.CompareAndSwap(false, true) {
		s.client.regionScheduler.scheduleStopRegion(rt)
		if rt.rangeLock.Stop() {
			s.onTableDrained(rt)
		}
	}
}

func (s *subscribedSpanSet) onTableDrained(rt *subscribedSpan) {
	log.Info("subscription client stop span is finished",
		zap.Uint64("subscriptionID", uint64(rt.subID)))

	s.client.removeSubscriptionRuntime(rt.subID)

	err := s.client.ds.RemovePath(rt.subID)
	if err != nil {
		log.Warn("subscription client remove path failed",
			zap.Uint64("subscriptionID", uint64(rt.subID)),
			zap.Error(err))
	}
	s.delete(rt.subID)
}

func (s *subscribedSpanSet) getResolvedTsLag() float64 {
	pullerMinResolvedTs := uint64(0)
	s.mu.RLock()
	for _, rt := range s.spanMap {
		resolvedTs := rt.resolvedTs.Load()
		if pullerMinResolvedTs == 0 || resolvedTs < pullerMinResolvedTs {
			pullerMinResolvedTs = resolvedTs
		}
	}
	s.mu.RUnlock()
	if pullerMinResolvedTs == 0 {
		return 0
	}
	pdTime := s.client.pdClock.CurrentTime()
	phyResolvedTs := oracle.ExtractPhysical(pullerMinResolvedTs)
	lag := float64(oracle.GetPhysical(pdTime)-phyResolvedTs) / 1e3
	return lag
}

func (r *subscribedSpan) resolveStaleLocks(targetTs uint64) {
	util.MustCompareAndMonotonicIncrease(&r.staleLocksTargetTs, targetTs)
	res := r.rangeLock.IterAll(r.tryResolveLock)
	log.Debug("subscription client finds slow locked ranges",
		zap.Uint64("subscriptionID", uint64(r.subID)),
		zap.Any("ranges", res))
}
