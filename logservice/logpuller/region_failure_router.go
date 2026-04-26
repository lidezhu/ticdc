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
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

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

func (s *regionRequestScheduler) submitDirectFailure(failure regionFailureInfo) {
	now := time.Now()
	s.client.regionRuntimeRegistry.recordRegionError(failure.regionInfo, failure.err, now)
	s.client.failureStats.record(failure, now)
	metrics.SubscriptionClientFailureCounter.WithLabelValues(
		failure.scope.String(),
		failure.source.String(),
		failure.kind.String(),
	).Inc()
	if failure.subscribedSpan.rangeLock.UnlockRange(
		failure.span.StartKey, failure.span.EndKey,
		failure.verID.GetID(), failure.verID.GetVer(), failure.resolvedTs()) {
		s.client.subscribedSpans.onTableDrained(failure.subscribedSpan)
		return
	}
	s.failureBuffer.enqueue(failure)
}

func (s *regionRequestScheduler) submitOrderedFailure(state *regionFeedState) (regionFailureInfo, bool) {
	failure, removed := state.detachFailure()
	if !removed {
		return regionFailureInfo{}, false
	}
	s.submitDirectFailure(failure)
	return failure, true
}

func (s *regionRequestScheduler) submitWorkerSessionFailure(
	startedRegions map[SubscriptionID]regionFeedStates,
	pendingRegions []regionInfo,
	sessionFailure workerSessionFailure,
) {
	for subID, states := range startedRegions {
		for _, state := range states {
			state.markStopped(normalizeWorkerSessionFailure(state.getRegionInfo(), sessionFailure))
			s.client.pushRegionEventToDS(subID, regionEvent{
				states: []*regionFeedState{state},
			})
		}
	}

	for _, region := range pendingRegions {
		if region.isStopRequest() {
			continue
		}
		s.submitDirectFailure(normalizeWorkerSessionFailure(region, sessionFailure))
	}
}

func (s *regionRequestScheduler) handleFailures(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Info("subscription client handle failures and exit")
			return ctx.Err()
		case failure := <-s.failureBuffer.ch:
			if err := s.handleFailure(ctx, failure); err != nil {
				return err
			}
		}
	}
}

func (s *regionRequestScheduler) retryRegion(ctx context.Context, failure regionFailureInfo, priority TaskType) {
	s.client.regionRuntimeRegistry.markRegionRetryPending(failure.regionInfo, failure.err, time.Now())
	s.scheduleRegionRequest(ctx, failure.regionInfo, priority)
}

func (s *regionRequestScheduler) reloadRegionRange(ctx context.Context, failure regionFailureInfo) {
	s.client.regionRuntimeRegistry.removeRegion(failure.regionInfo, time.Now())
	s.scheduleRangeRequest(ctx, failure.span, failure.subscribedSpan, failure.filterLoop, TaskHighPrior)
}

func (s *regionRequestScheduler) handleRegionFailure(ctx context.Context, failure regionFailureInfo) error {
	switch failure.kind {
	case regionFailureKindTiKVEvent:
		eventErr, ok := failure.err.(*eventError)
		if !ok || eventErr == nil {
			return errors.New("invalid tikv event failure")
		}

		innerErr := eventErr.err
		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			metricFeedNotLeaderCounter.Inc()
			if s.client.regionCache != nil {
				s.client.regionCache.UpdateLeader(failure.verID, notLeader.GetLeader(), failure.rpcCtx.AccessIdx)
			}
			s.retryRegion(ctx, failure, TaskHighPrior)
			return nil
		}
		if innerErr.GetEpochNotMatch() != nil {
			metricFeedEpochNotMatchCounter.Inc()
			s.reloadRegionRange(ctx, failure)
			return nil
		}
		if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			s.reloadRegionRange(ctx, failure)
			return nil
		}
		if innerErr.GetCongested() != nil {
			metricKvCongestedCounter.Inc()
			s.retryRegion(ctx, failure, TaskLowPrior)
			return nil
		}
		if innerErr.GetServerIsBusy() != nil {
			metricKvIsBusyCounter.Inc()
			s.retryRegion(ctx, failure, TaskLowPrior)
			return nil
		}
		if duplicated := innerErr.GetDuplicateRequest(); duplicated != nil {
			metricFeedDuplicateRequestCounter.Inc()
			return errors.New("duplicate request")
		}
		if compatibility := innerErr.GetCompatibility(); compatibility != nil {
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(compatibility)
		}
		if mismatch := innerErr.GetClusterIdMismatch(); mismatch != nil {
			return cerror.ErrClusterIDMismatch.GenWithStackByArgs(mismatch.Current, mismatch.Request)
		}

		log.Warn("empty or unknown cdc error",
			zap.Uint64("subscriptionID", uint64(failure.subscribedSpan.subID)),
			zap.Stringer("error", innerErr))
		metricFeedUnknownErrorCounter.Inc()
		s.retryRegion(ctx, failure, TaskHighPrior)
		return nil
	case regionFailureKindRPCCtxUnavailable:
		metricFeedRPCCtxUnavailable.Inc()
		s.reloadRegionRange(ctx, failure)
		return nil
	default:
		return errors.New("unexpected region failure kind")
	}
}

func (s *regionRequestScheduler) handleStoreSessionFailure(ctx context.Context, failure regionFailureInfo) error {
	switch failure.kind {
	case regionFailureKindGetStore:
		metricGetStoreErr.Inc()
		if s.client.regionCache != nil {
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			s.client.regionCache.OnSendFail(bo, failure.rpcCtx, true, errors.Cause(failure.err))
		}
		s.reloadRegionRange(ctx, failure)
		return nil
	case regionFailureKindSendRequestToStore:
		metricStoreSendRequestErr.Inc()
		if s.client.regionCache != nil {
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			s.client.regionCache.OnSendFail(bo, failure.rpcCtx, regionScheduleReload, errors.Cause(failure.err))
		}
		s.retryRegion(ctx, failure, TaskHighPrior)
		return nil
	default:
		return errors.New("unexpected store session failure kind")
	}
}

func (s *regionRequestScheduler) handleSubscriptionFailure(failure regionFailureInfo) error {
	switch failure.kind {
	case regionFailureKindRequestCancelled, regionFailureKindSubscriptionStopped:
		s.client.regionRuntimeRegistry.removeRegion(failure.regionInfo, time.Now())
		return nil
	default:
		return errors.New("unexpected subscription failure kind")
	}
}

func (s *regionRequestScheduler) handleFailure(ctx context.Context, failure regionFailureInfo) error {
	log.Debug("cdc region failure",
		zap.Uint64("subscriptionID", uint64(failure.subscribedSpan.subID)),
		zap.Uint64("regionID", failure.verID.GetID()),
		zap.Stringer("failureScope", failure.scope),
		zap.Stringer("failureSource", failure.source),
		zap.Stringer("failureKind", failure.kind),
		zap.Error(failure.err))

	var err error
	switch failure.scope {
	case regionFailureScopeRegion:
		err = s.handleRegionFailure(ctx, failure)
	case regionFailureScopeStoreSession:
		err = s.handleStoreSessionFailure(ctx, failure)
	case regionFailureScopeSubscription:
		err = s.handleSubscriptionFailure(failure)
	default:
		err = errors.New("unknown failure scope")
	}
	if err == nil {
		return nil
	}

	log.Warn("subscription client meets an internal error, fail the changefeed",
		zap.Uint64("subscriptionID", uint64(failure.subscribedSpan.subID)),
		zap.Stringer("failureScope", failure.scope),
		zap.Stringer("failureSource", failure.source),
		zap.Stringer("failureKind", failure.kind),
		zap.Error(err))
	return err
}
