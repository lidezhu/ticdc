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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type regionErrorHandler struct {
	runtime     *regionRuntimeTracker
	scheduler   *regionScheduler
	spans       *spanManager
	regionCache *tikv.RegionCache
	failures    *failureBuffer
}

type recoveryAction string

const (
	recoveryActionRetryRegion  recoveryAction = "retry_region"
	recoveryActionReloadRange  recoveryAction = "reload_range"
	recoveryActionRemoveRegion recoveryAction = "remove_region"
)

func (a recoveryAction) String() string {
	return string(a)
}

type recoveryPlan struct {
	action   recoveryAction
	priority TaskType
}

type failureBuffer struct {
	sync.Mutex
	pending []regionFailureInfo
	ch      chan regionFailureInfo
	notify  chan struct{}
}

func newFailureBuffer() *failureBuffer {
	return &failureBuffer{
		pending: make([]regionFailureInfo, 0, 1024),
		ch:      make(chan regionFailureInfo, 1024),
		notify:  make(chan struct{}, 1024),
	}
}

func (b *failureBuffer) enqueue(failure regionFailureInfo) {
	b.Lock()
	defer b.Unlock()
	b.pending = append(b.pending, failure)
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

func (b *failureBuffer) run(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	dispatchOne := func() {
		b.Lock()
		if len(b.pending) == 0 {
			b.Unlock()
			return
		}
		failure := b.pending[0]
		b.pending = b.pending[1:]
		b.Unlock()
		select {
		case <-ctx.Done():
			log.Info("subscription client dispatch failure buffer done")
		case b.ch <- failure:
		}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			dispatchOne()
		case <-b.notify:
			dispatchOne()
		}
	}
}

func newRegionErrorHandler(
	runtime *regionRuntimeTracker,
	scheduler *regionScheduler,
	spans *spanManager,
	regionCache *tikv.RegionCache,
) *regionErrorHandler {
	return &regionErrorHandler{
		runtime:     runtime,
		scheduler:   scheduler,
		spans:       spans,
		regionCache: regionCache,
		failures:    newFailureBuffer(),
	}
}

// reportFailure must not block the caller, otherwise there may be deadlock.
func (h *regionErrorHandler) reportFailure(failure regionFailureInfo) {
	h.runtime.recordError(failure.regionInfo, failure.err, time.Now())
	if failure.subscribedSpan.rangeLock.UnlockRange(
		failure.span.StartKey, failure.span.EndKey,
		failure.verID.GetID(), failure.verID.GetVer(), failure.resolvedTs()) {
		h.spans.onTableDrained(failure.subscribedSpan)
		return
	}
	h.failures.enqueue(failure)
}

func (h *regionErrorHandler) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Info("subscription client handle errors and exit")
			return ctx.Err()
		case failure := <-h.failures.ch:
			if err := h.handleFailure(ctx, failure); err != nil {
				return err
			}
		}
	}
}

func (h *regionErrorHandler) retryRegion(ctx context.Context, failure regionFailureInfo, priority TaskType) {
	h.runtime.markRetryPending(failure.regionInfo, failure.err, time.Now())
	h.scheduler.scheduleRegionRequest(ctx, failure.regionInfo, priority)
}

func (h *regionErrorHandler) reloadRegionRange(ctx context.Context, failure regionFailureInfo) {
	h.runtime.removeRegion(failure.regionInfo, time.Now())
	h.scheduler.enqueueRange(failure.span, failure.subscribedSpan, failure.filterLoop, TaskHighPrior)
}

func (h *regionErrorHandler) planEventFailure(failure regionFailureInfo) (recoveryPlan, error) {
	eerr, ok := failure.err.(*eventError)
	if !ok || eerr == nil {
		return recoveryPlan{}, errors.New("invalid tikv event failure")
	}
	innerErr := eerr.err
	if notLeader := innerErr.GetNotLeader(); notLeader != nil {
		metricFeedNotLeaderCounter.Inc()
		if h.regionCache != nil {
			h.regionCache.UpdateLeader(failure.verID, notLeader.GetLeader(), failure.rpcCtx.AccessIdx)
		}
		return recoveryPlan{action: recoveryActionRetryRegion, priority: TaskHighPrior}, nil
	}
	if innerErr.GetEpochNotMatch() != nil {
		metricFeedEpochNotMatchCounter.Inc()
		return recoveryPlan{action: recoveryActionReloadRange}, nil
	}
	if innerErr.GetRegionNotFound() != nil {
		metricFeedRegionNotFoundCounter.Inc()
		return recoveryPlan{action: recoveryActionReloadRange}, nil
	}
	if innerErr.GetCongested() != nil {
		metricKvCongestedCounter.Inc()
		return recoveryPlan{action: recoveryActionRetryRegion, priority: TaskLowPrior}, nil
	}
	if innerErr.GetServerIsBusy() != nil {
		metricKvIsBusyCounter.Inc()
		return recoveryPlan{action: recoveryActionRetryRegion, priority: TaskLowPrior}, nil
	}
	if duplicated := innerErr.GetDuplicateRequest(); duplicated != nil {
		// TODO(qupeng): It's better to add a new machanism to deregister one region.
		metricFeedDuplicateRequestCounter.Inc()
		return recoveryPlan{}, errors.New("duplicate request")
	}
	if compatibility := innerErr.GetCompatibility(); compatibility != nil {
		return recoveryPlan{}, cerror.ErrVersionIncompatible.GenWithStackByArgs(compatibility)
	}
	if mismatch := innerErr.GetClusterIdMismatch(); mismatch != nil {
		return recoveryPlan{}, cerror.ErrClusterIDMismatch.GenWithStackByArgs(mismatch.Current, mismatch.Request)
	}

	log.Warn("empty or unknown cdc error",
		zap.Uint64("subscriptionID", uint64(failure.subscribedSpan.subID)),
		zap.Stringer("error", innerErr))
	metricFeedUnknownErrorCounter.Inc()
	return recoveryPlan{action: recoveryActionRetryRegion, priority: TaskHighPrior}, nil
}

func (h *regionErrorHandler) planFailure(ctx context.Context, failure regionFailureInfo) (recoveryPlan, error) {
	switch failure.kind {
	case regionFailureKindTiKVEvent:
		return h.planEventFailure(failure)
	case regionFailureKindRPCCtxUnavailable:
		metricFeedRPCCtxUnavailable.Inc()
		return recoveryPlan{action: recoveryActionReloadRange}, nil
	case regionFailureKindGetStore:
		metricGetStoreErr.Inc()
		if h.regionCache != nil {
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			// Cannot get the store the region belongs to, so we need to reload the region.
			h.regionCache.OnSendFail(bo, failure.rpcCtx, true, failure.err)
		}
		return recoveryPlan{action: recoveryActionReloadRange}, nil
	case regionFailureKindSendRequestToStore:
		metricStoreSendRequestErr.Inc()
		if h.regionCache != nil {
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			h.regionCache.OnSendFail(bo, failure.rpcCtx, regionScheduleReload, failure.err)
		}
		return recoveryPlan{action: recoveryActionRetryRegion, priority: TaskHighPrior}, nil
	case regionFailureKindRequestCancelled, regionFailureKindSubscriptionStopped:
		// The corresponding subscription has been unsubscribed, just ignore.
		return recoveryPlan{action: recoveryActionRemoveRegion}, nil
	default:
		return recoveryPlan{}, failure.err
	}
}

func (h *regionErrorHandler) applyRecovery(ctx context.Context, failure regionFailureInfo, plan recoveryPlan) {
	switch plan.action {
	case recoveryActionRetryRegion:
		h.retryRegion(ctx, failure, plan.priority)
	case recoveryActionReloadRange:
		h.reloadRegionRange(ctx, failure)
	case recoveryActionRemoveRegion:
		h.runtime.removeRegion(failure.regionInfo, time.Now())
	default:
		log.Panic("unknown recovery action", zap.Stringer("action", plan.action))
	}
}

func (h *regionErrorHandler) handleFailure(ctx context.Context, failure regionFailureInfo) error {
	log.Debug("cdc region failure",
		zap.Uint64("subscriptionID", uint64(failure.subscribedSpan.subID)),
		zap.Uint64("regionID", failure.verID.GetID()),
		zap.Stringer("failureScope", failure.scope),
		zap.Stringer("failureSource", failure.source),
		zap.Stringer("failureKind", failure.kind),
		zap.Error(failure.err))

	plan, err := h.planFailure(ctx, failure)
	if err != nil {
		// TODO(qupeng): for some errors it's better to just deregister the region from TiKVs.
		log.Warn("subscription client meets an internal error, fail the changefeed",
			zap.Uint64("subscriptionID", uint64(failure.subscribedSpan.subID)),
			zap.Stringer("failureScope", failure.scope),
			zap.Stringer("failureSource", failure.source),
			zap.Stringer("failureKind", failure.kind),
			zap.Error(err))
		return err
	}
	h.applyRecovery(ctx, failure, plan)
	return nil
}
