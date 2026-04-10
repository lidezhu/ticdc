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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type spanManager struct {
	ctx   context.Context
	clock interface {
		CurrentTS() uint64
		CurrentTime() time.Time
	}
	events  *eventStreamController
	runtime *regionRuntimeTracker

	scheduler *regionScheduler
	router    *regionRequestRouter

	maintenance *spanMaintenance

	mu      sync.RWMutex
	spanMap map[SubscriptionID]*subscribedSpan
}

func newSpanManager(
	ctx context.Context,
	clock interface {
		CurrentTS() uint64
		CurrentTime() time.Time
	},
	events *eventStreamController,
	runtime *regionRuntimeTracker,
) *spanManager {
	return &spanManager{
		ctx:     ctx,
		clock:   clock,
		events:  events,
		runtime: runtime,
		spanMap: make(map[SubscriptionID]*subscribedSpan),
	}
}

func (m *spanManager) setPipeline(scheduler *regionScheduler, router *regionRequestRouter) {
	m.scheduler = scheduler
	m.router = router
}

func (m *spanManager) setMaintenance(maintenance *spanMaintenance) {
	m.maintenance = maintenance
}

func (m *spanManager) subscribe(
	subID SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	bdrMode bool,
) {
	if span.TableID == 0 {
		log.Panic("subscription client subscribe with zero TableID")
		return
	}

	subSpan := m.newSubscribedSpan(subID, span, startTs, consumeKVEvents, advanceResolvedTs, advanceInterval, bdrMode)
	m.mu.Lock()
	m.spanMap[subID] = subSpan
	m.mu.Unlock()

	areaSetting := dynstream.NewAreaSettingsWithMaxPendingSize(1*1024*1024*1024, dynstream.MemoryControlForPuller, "logPuller") // 1GB
	if err := m.events.addPath(subSpan.subID, subSpan, areaSetting); err != nil {
		log.Panic("subscription client add path failed",
			zap.Uint64("subscriptionID", uint64(subID)),
			zap.Error(err))
	}

	m.scheduler.enqueueRange(span, subSpan, subSpan.filterLoop, TaskLowPrior)
	log.Info("subscribes span done", zap.Uint64("subscriptionID", uint64(subID)),
		zap.Int64("tableID", span.TableID), zap.Uint64("startTs", startTs),
		zap.String("startKey", spanz.HexKey(span.StartKey)), zap.String("endKey", spanz.HexKey(span.EndKey)))
}

func (m *spanManager) unsubscribe(subID SubscriptionID) {
	m.mu.RLock()
	subSpan := m.spanMap[subID]
	m.mu.RUnlock()
	if subSpan == nil {
		log.Warn("unknown subscription", zap.Uint64("subscriptionID", uint64(subID)))
		return
	}

	log.Info("subscription client starts to stop table",
		zap.Uint64("subscriptionID", uint64(subSpan.subID)))

	if subSpan.stopped.CompareAndSwap(false, true) {
		m.router.enqueueStop(subSpan)
		if subSpan.rangeLock.Stop() {
			m.onTableDrained(subSpan)
		}
	}

	log.Info("unsubscribe span success",
		zap.Uint64("subscriptionID", uint64(subSpan.subID)),
		zap.Bool("exists", true))
}

func (m *spanManager) onTableDrained(subSpan *subscribedSpan) {
	log.Info("subscription client stop span is finished",
		zap.Uint64("subscriptionID", uint64(subSpan.subID)))

	m.runtime.removeSubscription(subSpan.subID)

	if err := m.events.removePath(subSpan.subID); err != nil {
		log.Warn("subscription client remove path failed",
			zap.Uint64("subscriptionID", uint64(subSpan.subID)),
			zap.Error(err))
	}

	m.mu.Lock()
	delete(m.spanMap, subSpan.subID)
	m.mu.Unlock()
}

func (m *spanManager) forEachSpan(fn func(SubscriptionID, *subscribedSpan)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for subID, subSpan := range m.spanMap {
		fn(subID, subSpan)
	}
}

func (m *spanManager) subscribedRegionCount() int {
	count := 0
	m.forEachSpan(func(_ SubscriptionID, subSpan *subscribedSpan) {
		count += subSpan.rangeLock.Len()
	})
	return count
}

func (m *spanManager) resolvedTsLag() float64 {
	minResolvedTs := uint64(0)
	m.forEachSpan(func(_ SubscriptionID, subSpan *subscribedSpan) {
		resolvedTs := subSpan.resolvedTs.Load()
		if minResolvedTs == 0 || resolvedTs < minResolvedTs {
			minResolvedTs = resolvedTs
		}
	})
	if minResolvedTs == 0 {
		return 0
	}
	currentTime := m.clock.CurrentTime()
	phyResolvedTs := oracle.ExtractPhysical(minResolvedTs)
	return float64(oracle.GetPhysical(currentTime)-phyResolvedTs) / 1e3
}

func (m *spanManager) newSubscribedSpan(
	subID SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	filterLoop bool,
) *subscribedSpan {
	rangeLock := regionlock.NewRangeLock(uint64(subID), span.StartKey, span.EndKey, startTs)

	subSpan := &subscribedSpan{
		subID:      subID,
		span:       span,
		startTs:    startTs,
		filterLoop: filterLoop,
		rangeLock:  rangeLock,

		consumeKVEvents:   consumeKVEvents,
		advanceResolvedTs: advanceResolvedTs,
		advanceInterval:   advanceInterval,
	}
	subSpan.initialized.Store(false)
	subSpan.resolvedTsUpdated.Store(time.Now().Unix())
	subSpan.resolvedTs.Store(startTs)

	subSpan.tryResolveLock = func(regionID uint64, state *regionlock.LockedRangeState) {
		targetTs := subSpan.staleLocksTargetTs.Load()
		if state.ResolvedTs.Load() < targetTs && state.Initialized.Load() {
			m.maintenance.enqueueResolveLockTask(resolveLockTask{
				keyspaceID: span.KeyspaceID,
				regionID:   regionID,
				targetTs:   targetTs,
				state:      state,
				create:     time.Now(),
			})
		}
	}
	return subSpan
}

type subscriptionAndTargetTs struct {
	subSpan  *subscribedSpan
	targetTs uint64
}

func gcResolveLastRunMap(resolveLastRun map[uint64]time.Time, now time.Time) map[uint64]time.Time {
	if len(resolveLastRun) <= resolveLastRunGCThreshold {
		return resolveLastRun
	}

	copied := make(map[uint64]time.Time, len(resolveLastRun))
	for regionID, lastRun := range resolveLastRun {
		if now.Sub(lastRun) < resolveLockMinInterval {
			copied[regionID] = lastRun
		}
	}
	return copied
}

type spanMaintenance struct {
	pdClock      interface{ CurrentTime() time.Time }
	lockResolver interface {
		Resolve(ctx context.Context, keyspaceID uint32, regionID uint64, targetTs uint64) error
	}
	resolveLockTaskCh chan resolveLockTask
	spans             *spanManager
}

func newSpanMaintenance(
	pdClock interface{ CurrentTime() time.Time },
	lockResolver interface {
		Resolve(ctx context.Context, keyspaceID uint32, regionID uint64, targetTs uint64) error
	},
	spans *spanManager,
) *spanMaintenance {
	return &spanMaintenance{
		pdClock:           pdClock,
		lockResolver:      lockResolver,
		resolveLockTaskCh: make(chan resolveLockTask, 1024),
		spans:             spans,
	}
}

func (m *spanMaintenance) enqueueResolveLockTask(task resolveLockTask) {
	select {
	case <-m.spans.ctx.Done():
	case m.resolveLockTaskCh <- task:
	default:
		metrics.SubscriptionClientResolveLockTaskDropCounter.Inc()
	}
}

func (m *spanMaintenance) runResolveLockChecker(ctx context.Context) error {
	resolveLockTicker := time.NewTicker(resolveLockTickInterval)
	defer resolveLockTicker.Stop()
	maxCacheSize := 1024
	subSpanAndTsCache := make([]subscriptionAndTargetTs, 0, maxCacheSize)

	getResolvedTargetTs := func(subSpan *subscribedSpan, currentTime time.Time) uint64 {
		resolvedTsUpdated := time.Unix(subSpan.resolvedTsUpdated.Load(), 0)
		if !subSpan.initialized.Load() || time.Since(resolvedTsUpdated) < resolveLockFence {
			return 0
		}
		resolvedTs := subSpan.resolvedTs.Load()
		resolvedTime := oracle.GetTimeFromTS(resolvedTs)
		if currentTime.Sub(resolvedTime) < resolveLockFence {
			return 0
		}
		return oracle.GoTimeToTS(resolvedTime.Add(resolveLockFence))
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resolveLockTicker.C:
		}
		currentTime := m.pdClock.CurrentTime()
		m.spans.forEachSpan(func(_ SubscriptionID, subSpan *subscribedSpan) {
			if subSpan == nil {
				return
			}
			targetTs := getResolvedTargetTs(subSpan, currentTime)
			if targetTs > 0 {
				subSpanAndTsCache = append(subSpanAndTsCache, subscriptionAndTargetTs{
					subSpan:  subSpan,
					targetTs: targetTs,
				})
			}
		})
		for _, subSpanAndTs := range subSpanAndTsCache {
			subSpanAndTs.subSpan.resolveStaleLocks(subSpanAndTs.targetTs)
		}
		subSpanAndTsCache = subSpanAndTsCache[:0]
		if cap(subSpanAndTsCache) > maxCacheSize {
			subSpanAndTsCache = make([]subscriptionAndTargetTs, 0, maxCacheSize)
		}
	}
}

func (m *spanMaintenance) handleResolveLockTasks(ctx context.Context) error {
	resolveLastRun := make(map[uint64]time.Time)

	doResolve := func(keyspaceID uint32, regionID uint64, state *regionlock.LockedRangeState, targetTs uint64) {
		if state.ResolvedTs.Load() > targetTs || !state.Initialized.Load() {
			return
		}

		lastRun, ok := resolveLastRun[regionID]
		if ok && time.Since(lastRun) < resolveLockMinInterval {
			return
		}

		if err := m.lockResolver.Resolve(ctx, keyspaceID, regionID, targetTs); err != nil {
			log.Warn("subscription client resolve lock fail",
				zap.Uint32("keyspaceID", keyspaceID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("targetTs", targetTs),
				zap.Time("lastRun", lastRun),
				zap.Any("state", state),
				zap.Error(err))
		}
		resolveLastRun[regionID] = time.Now()
	}

	gcTicker := time.NewTicker(resolveLockMinInterval * 3 / 2)
	defer gcTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gcTicker.C:
			resolveLastRun = gcResolveLastRunMap(resolveLastRun, time.Now())
		case task := <-m.resolveLockTaskCh:
			doResolve(task.keyspaceID, task.regionID, task.state, task.targetTs)
		}
	}
}

func (m *spanMaintenance) logSlowRegions(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		currentTime := m.pdClock.CurrentTime()
		m.spans.forEachSpan(func(subscriptionID SubscriptionID, subSpan *subscribedSpan) {
			attr := subSpan.rangeLock.IterAll(nil)
			ckptTime := oracle.GetTimeFromTS(attr.SlowestRegion.ResolvedTs)
			if attr.SlowestRegion.Initialized {
				if currentTime.Sub(ckptTime) > 6*resolveLockMinInterval {
					log.Info("subscription client finds a initialized slow region",
						zap.Uint64("subscriptionID", uint64(subscriptionID)),
						zap.Any("slowRegion", attr.SlowestRegion))
				}
				return
			}
			if currentTime.Sub(attr.SlowestRegion.Created) > 10*time.Minute {
				log.Info("subscription client initializes a region too slow",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			} else if currentTime.Sub(ckptTime) > 10*time.Minute {
				log.Info("subscription client finds a uninitialized slow region",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			}
			if len(attr.UnLockedRanges) > 0 {
				log.Info("subscription client holes exist",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("holes", attr.UnLockedRanges))
			}
		})
	}
}

func (r *subscribedSpan) resolveStaleLocks(targetTs uint64) {
	util.MustCompareAndMonotonicIncrease(&r.staleLocksTargetTs, targetTs)
	res := r.rangeLock.IterAll(r.tryResolveLock)
	log.Debug("subscription client finds slow locked ranges",
		zap.Uint64("subscriptionID", uint64(r.subID)),
		zap.Any("ranges", res))
}
