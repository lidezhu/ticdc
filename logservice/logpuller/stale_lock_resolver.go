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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	resolveLockMinInterval  time.Duration = 10 * time.Second
	resolveLockTickInterval time.Duration = 2 * time.Second
	resolveLockFence        time.Duration = 4 * time.Second

	// resolveLastRunGCThreshold is the size threshold to GC resolveLastRun and drop stale entries.
	resolveLastRunGCThreshold = 1024
)

type resolveLockTask struct {
	keyspaceID uint32
	regionID   uint64
	targetTs   uint64
	state      *regionlock.LockedRangeState
}

type staleLockResolver struct {
	client            *subscriptionClient
	resolveLockTaskCh chan resolveLockTask
}

func newStaleLockResolver(client *subscriptionClient) *staleLockResolver {
	return &staleLockResolver{
		client:            client,
		resolveLockTaskCh: make(chan resolveLockTask, 1024),
	}
}

func (s *staleLockResolver) run(ctx context.Context, g *errgroup.Group) {
	g.Go(func() error { return s.runResolveLockChecker(ctx) })
	g.Go(func() error { return s.handleResolveLockTasks(ctx) })
}

func (s *staleLockResolver) tryEnqueue(task resolveLockTask) bool {
	select {
	case <-s.client.ctx.Done():
		return true
	case s.resolveLockTaskCh <- task:
		return true
	default:
		return false
	}
}

type staleLockResolveTarget struct {
	span     *subscribedSpan
	targetTs uint64
}

func (s *staleLockResolver) runResolveLockChecker(ctx context.Context) error {
	resolveLockTicker := time.NewTicker(resolveLockTickInterval)
	defer resolveLockTicker.Stop()
	maxCacheSize := 1024
	targets := make([]staleLockResolveTarget, 0, maxCacheSize)
	// getResolvedTargetTs returns the targetTs to resolve stale locks. 0 means no need to resolve.
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
		currentTime := s.client.pdClock.CurrentTime()
		for _, entry := range s.client.subscribedSpans.snapshot() {
			if entry.span == nil {
				continue
			}
			targetTs := getResolvedTargetTs(entry.span, currentTime)
			if targetTs > 0 {
				targets = append(targets, staleLockResolveTarget{
					span:     entry.span,
					targetTs: targetTs,
				})
			}
		}
		for _, target := range targets {
			target.span.resolveStaleLocks(target.targetTs)
		}
		targets = targets[:0]
		if cap(targets) > maxCacheSize {
			targets = make([]staleLockResolveTarget, 0, maxCacheSize)
		}
	}
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

func (s *staleLockResolver) handleResolveLockTasks(ctx context.Context) error {
	resolveLastRun := make(map[uint64]time.Time)

	doResolve := func(keyspaceID uint32, regionID uint64, state *regionlock.LockedRangeState, targetTs uint64) {
		if state.ResolvedTs.Load() > targetTs || !state.Initialized.Load() {
			return
		}

		lastRun, ok := resolveLastRun[regionID]
		if ok {
			if time.Since(lastRun) < resolveLockMinInterval {
				return
			}
		}

		if err := s.client.lockResolver.Resolve(ctx, keyspaceID, regionID, targetTs); err != nil {
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
		case task := <-s.resolveLockTaskCh:
			doResolve(task.keyspaceID, task.regionID, task.state, task.targetTs)
		}
	}
}
