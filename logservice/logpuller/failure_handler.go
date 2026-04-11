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
	"golang.org/x/sync/errgroup"
)

type failureHandler struct {
	client *subscriptionClient
	buffer *failureBuffer
}

type workerSessionFailure struct {
	kind   regionFailureKind
	source regionFailureSource
	cause  error
}

func (f workerSessionFailure) toRegionFailure(region regionInfo) regionFailureInfo {
	switch f.kind {
	case regionFailureKindGetStore:
		return newGetStoreFailure(region, f.source, f.cause)
	case regionFailureKindSendRequestToStore:
		return newSendRequestToStoreFailure(region, f.source, f.cause)
	default:
		log.Panic("unknown worker session failure kind",
			zap.Stringer("failureKind", f.kind),
			zap.Stringer("failureSource", f.source),
			zap.Error(f.cause))
		return regionFailureInfo{}
	}
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

func newFailureHandler(client *subscriptionClient) *failureHandler {
	return &failureHandler{
		client: client,
		buffer: newFailureBuffer(),
	}
}

// run owns the whole failure pipeline: enqueue -> plan -> recovery.
func (h *failureHandler) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return h.buffer.run(ctx) })
	g.Go(func() error { return h.handleLoop(ctx) })
	return g.Wait()
}

// submitDirectFailure is for failures that do not need dynstream ordering.
func (h *failureHandler) submitDirectFailure(failure regionFailureInfo) {
	h.client.recordRegionRuntimeError(failure.regionInfo, failure.err, time.Now())
	if failure.subscribedSpan.rangeLock.UnlockRange(
		failure.span.StartKey, failure.span.EndKey,
		failure.verID.GetID(), failure.verID.GetVer(), failure.resolvedTs()) {
		h.client.onTableDrained(failure.subscribedSpan)
		return
	}
	h.buffer.enqueue(failure)
}

// submitOrderedFailure is for stale region states that must preserve ordering
// with previously dispatched region events.
func (h *failureHandler) submitOrderedFailure(state *regionFeedState) (regionFailureInfo, bool) {
	failure, removed := state.takeStoppedFailure()
	if !removed {
		return regionFailureInfo{}, false
	}
	state.controller.removeRegionState(SubscriptionID(state.requestID), state.getRegionID())
	h.submitDirectFailure(failure)
	return failure, true
}

// submitWorkerSessionFailure converts one worker/store-session failure into:
// ordered failures for started regions and direct failures for pending regions.
func (h *failureHandler) submitWorkerSessionFailure(
	session *regionWorkerSession,
	pendingRegions []regionInfo,
	sessionFailure workerSessionFailure,
) {
	if session != nil {
		for subID, states := range session.clearRegionStates() {
			for _, state := range states {
				state.markStopped(sessionFailure.toRegionFailure(state.getRegionInfo()))
				h.client.pushRegionEventToDS(subID, regionEvent{
					states: []*regionFeedState{state},
				})
			}
		}
	}

	for _, region := range pendingRegions {
		if region.isStopped() {
			continue
		}
		h.submitDirectFailure(sessionFailure.toRegionFailure(region))
	}
}

func (h *failureHandler) handleLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Info("subscription client handle failures and exit")
			return ctx.Err()
		case failure := <-h.buffer.ch:
			if err := h.handleFailure(ctx, failure); err != nil {
				return err
			}
		}
	}
}

func (h *failureHandler) retryRegion(ctx context.Context, failure regionFailureInfo, priority TaskType) {
	h.client.markRegionRetryPending(failure.regionInfo, failure.err, time.Now())
	h.client.scheduleRegionRequest(ctx, failure.regionInfo, priority)
}

func (h *failureHandler) reloadRegionRange(ctx context.Context, failure regionFailureInfo) {
	h.client.removeRegionRuntime(failure.regionInfo, time.Now())
	h.client.scheduleRangeRequest(ctx, failure.span, failure.subscribedSpan, failure.filterLoop, TaskHighPrior)
}

func (h *failureHandler) planEventFailure(failure regionFailureInfo) (recoveryPlan, error) {
	eerr, ok := failure.err.(*eventError)
	if !ok || eerr == nil {
		return recoveryPlan{}, errors.New("invalid tikv event failure")
	}

	innerErr := eerr.err
	if notLeader := innerErr.GetNotLeader(); notLeader != nil {
		metricFeedNotLeaderCounter.Inc()
		if h.client.regionCache != nil {
			h.client.regionCache.UpdateLeader(failure.verID, notLeader.GetLeader(), failure.rpcCtx.AccessIdx)
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

func (h *failureHandler) planFailure(ctx context.Context, failure regionFailureInfo) (recoveryPlan, error) {
	switch failure.kind {
	case regionFailureKindTiKVEvent:
		return h.planEventFailure(failure)
	case regionFailureKindRPCCtxUnavailable:
		metricFeedRPCCtxUnavailable.Inc()
		return recoveryPlan{action: recoveryActionReloadRange}, nil
	case regionFailureKindGetStore:
		metricGetStoreErr.Inc()
		if h.client.regionCache != nil {
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			h.client.regionCache.OnSendFail(bo, failure.rpcCtx, true, errors.Cause(failure.err))
		}
		return recoveryPlan{action: recoveryActionReloadRange}, nil
	case regionFailureKindSendRequestToStore:
		metricStoreSendRequestErr.Inc()
		if h.client.regionCache != nil {
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			h.client.regionCache.OnSendFail(bo, failure.rpcCtx, regionScheduleReload, errors.Cause(failure.err))
		}
		return recoveryPlan{action: recoveryActionRetryRegion, priority: TaskHighPrior}, nil
	case regionFailureKindRequestCancelled, regionFailureKindSubscriptionStopped:
		return recoveryPlan{action: recoveryActionRemoveRegion}, nil
	default:
		return recoveryPlan{}, failure.err
	}
}

func (h *failureHandler) applyRecoveryPlan(ctx context.Context, failure regionFailureInfo, plan recoveryPlan) {
	switch plan.action {
	case recoveryActionRetryRegion:
		h.retryRegion(ctx, failure, plan.priority)
	case recoveryActionReloadRange:
		h.reloadRegionRange(ctx, failure)
	case recoveryActionRemoveRegion:
		h.client.removeRegionRuntime(failure.regionInfo, time.Now())
	default:
		log.Panic("unknown recovery action", zap.Stringer("action", plan.action))
	}
}

func (h *failureHandler) handleFailure(ctx context.Context, failure regionFailureInfo) error {
	log.Debug("cdc region failure",
		zap.Uint64("subscriptionID", uint64(failure.subscribedSpan.subID)),
		zap.Uint64("regionID", failure.verID.GetID()),
		zap.Stringer("failureScope", failure.scope),
		zap.Stringer("failureSource", failure.source),
		zap.Stringer("failureKind", failure.kind),
		zap.Error(failure.err))

	plan, err := h.planFailure(ctx, failure)
	if err != nil {
		log.Warn("subscription client meets an internal error, fail the changefeed",
			zap.Uint64("subscriptionID", uint64(failure.subscribedSpan.subID)),
			zap.Stringer("failureScope", failure.scope),
			zap.Stringer("failureSource", failure.source),
			zap.Stringer("failureKind", failure.kind),
			zap.Error(err))
		return err
	}

	h.applyRecoveryPlan(ctx, failure, plan)
	return nil
}
