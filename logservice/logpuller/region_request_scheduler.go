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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type regionRequestScheduler struct {
	client          *subscriptionClient
	rangeTaskCh     chan rangeTask
	regionTaskQueue *PriorityQueue
	failureBuffer   *failureBuffer
}

func newRegionRequestScheduler(client *subscriptionClient) *regionRequestScheduler {
	return &regionRequestScheduler{
		client:          client,
		rangeTaskCh:     make(chan rangeTask, 1024),
		regionTaskQueue: NewPriorityQueue(),
		failureBuffer:   newFailureBuffer(),
	}
}

func (s *regionRequestScheduler) close() {
	s.regionTaskQueue.Close()
}

func (s *regionRequestScheduler) runFailureBuffer(ctx context.Context) error {
	return s.failureBuffer.run(ctx)
}

func (s *regionRequestScheduler) scheduleStopRegion(span *subscribedSpan) {
	s.regionTaskQueue.Push(NewRegionPriorityTask(
		TaskHighPrior,
		regionInfo{subscribedSpan: span, filterLoop: span.filterLoop},
		s.client.pdClock.CurrentTS(),
	))
}

func (s *regionRequestScheduler) attachRPCContextForRegion(ctx context.Context, region regionInfo) (regionInfo, bool) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.client.regionCache.GetTiKVRPCContext(bo, region.verID, kvclientv2.ReplicaReadLeader, 0)
	if rpcCtx != nil {
		region.rpcCtx = rpcCtx
		return region, true
	}
	if err != nil {
		log.Debug("subscription client get rpc context fail",
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Error(err))
	}
	s.submitDirectFailure(newRPCCtxUnavailableFailure(region))
	return region, false
}

// handleRegions receives regionInfo from regionTaskQueue, attaches rpcCtx to them,
// then sends them to the corresponding requestedStore.
func (s *regionRequestScheduler) handleRegions(ctx context.Context, eg *errgroup.Group) error {
	defer s.client.requestedStores.clearPendingRequests()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		regionTask, err := s.regionTaskQueue.Pop(ctx)
		if err != nil {
			return err
		}

		region := regionTask.GetRegionInfo()
		if region.isStopped() {
			enqueued, err := s.client.requestedStores.enqueueRegionToAllStores(ctx, region)
			if err != nil {
				return err
			}
			if !enqueued {
				log.Debug("enqueue stop request failed, retry later",
					zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)))
				s.regionTaskQueue.Push(regionTask)
			}
			continue
		}

		region, ok := s.attachRPCContextForRegion(ctx, region)
		if !ok {
			continue
		}
		s.client.updateRegionRuntimeInfo(region)
		s.client.markRegionRPCReady(region, time.Now())

		store := s.client.requestedStores.getOrCreateRequestedStore(ctx, eg, region.rpcCtx.Addr)
		worker := store.getRequestWorker()
		force := regionTask.Priority() <= forcedPriorityBase

		ok, err = worker.add(ctx, region, force)
		if err != nil {
			log.Warn("subscription client add region request failed",
				zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
				zap.Uint64("regionID", region.verID.GetID()),
				zap.Error(err))
			return err
		}
		if !ok {
			s.regionTaskQueue.Push(regionTask)
			continue
		}

		log.Debug("subscription client will request a region",
			zap.Uint64("workID", worker.workerID),
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.String("addr", store.storeAddr))
	}
}

func (s *regionRequestScheduler) handleRangeTasks(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	// Limit the concurrent number of goroutines to convert range tasks to region tasks.
	g.SetLimit(1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-s.rangeTaskCh:
			g.Go(func() error {
				return s.divideSpanAndScheduleRegionRequests(ctx, task.span, task.subscribedSpan, task.filterLoop, task.priority)
			})
		}
	}
}

// divideSpanAndScheduleRegionRequests processes the specified span by dividing it into
// manageable regions and schedules requests to subscribe to these regions.
// 1. Load regions from PD.
// 2. Find the intersection of each region.span and the subscribedSpan.span.
// 3. Schedule a region request to subscribe the region.
func (s *regionRequestScheduler) divideSpanAndScheduleRegionRequests(
	ctx context.Context,
	span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	taskType TaskType,
) error {
	// Limit the number of regions loaded at a time to make the load more stable.
	limit := 1024
	nextSpan := span
	backoffBeforeLoad := false
	for {
		if backoffBeforeLoad {
			if err := util.Hang(ctx, loadRegionRetryInterval); err != nil {
				return err
			}
			backoffBeforeLoad = false
		}
		log.Debug("subscription client is going to load regions",
			zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
			zap.Any("span", common.FormatTableSpan(&nextSpan)))

		backoff := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		regions, err := s.client.regionCache.BatchLoadRegionsWithKeyRange(backoff, nextSpan.StartKey, nextSpan.EndKey, limit)
		if err != nil {
			log.Warn("subscription client load regions failed",
				zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
				zap.Any("span", common.FormatTableSpan(&nextSpan)),
				zap.Error(err))
			backoffBeforeLoad = true
			continue
		}
		regionMetas := make([]*metapb.Region, 0, len(regions))
		for _, region := range regions {
			if meta := region.GetMeta(); meta != nil {
				regionMetas = append(regionMetas, meta)
			}
		}
		regionMetas = regionlock.CutRegionsLeftCoverSpan(regionMetas, nextSpan)
		if len(regionMetas) == 0 {
			log.Warn("subscription client load regions with holes",
				zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
				zap.Any("span", common.FormatTableSpan(&nextSpan)))
			backoffBeforeLoad = true
			continue
		}

		for _, regionMeta := range regionMetas {
			regionSpan := heartbeatpb.TableSpan{
				StartKey:   regionMeta.StartKey,
				EndKey:     regionMeta.EndKey,
				KeyspaceID: subscribedSpan.span.KeyspaceID,
			}
			// NOTE: the End key return by the PD API will be nil to represent the biggest key.
			// So we need to fix it by calling spanz.HackSpan.
			regionSpan = common.HackTableSpan(regionSpan)

			// Find the intersection of the regionSpan returned by PD and the subscribedSpan.span.
			// The intersection is the span that needs to be subscribed.
			intersectSpan := common.GetIntersectSpan(subscribedSpan.span, regionSpan)
			if common.IsEmptySpan(intersectSpan) {
				log.Panic("subscription client check spans intersect shouldn't fail",
					zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)))
			}

			regionInfo := newRegionInfo(
				tikv.NewRegionVerID(regionMeta.Id, regionMeta.RegionEpoch.ConfVer, regionMeta.RegionEpoch.Version),
				intersectSpan,
				nil,
				subscribedSpan,
				filterLoop,
			)
			s.scheduleRegionRequest(ctx, regionInfo, taskType)

			nextSpan.StartKey = regionMeta.EndKey
			// If the nextSpan.StartKey is larger than the subscribedSpan.span.EndKey,
			// it means all span of the subscribedSpan have been requested. So we return.
			if common.EndCompare(nextSpan.StartKey, span.EndKey) >= 0 {
				return nil
			}
		}
	}
}

func (s *regionRequestScheduler) scheduleRegionRequest(ctx context.Context, region regionInfo, priority TaskType) {
	s.client.ensureRegionRuntime(&region, time.Now())
	lockRangeResult := region.subscribedSpan.rangeLock.LockRange(
		ctx, region.span.StartKey, region.span.EndKey, region.verID.GetID(), region.verID.GetVer())

	if lockRangeResult.Status == regionlock.LockRangeStatusWait {
		s.client.transitionRegionRuntime(region, regionPhaseRangeLockWait, time.Now())
		lockRangeResult = lockRangeResult.WaitFn()
	}

	switch lockRangeResult.Status {
	case regionlock.LockRangeStatusSuccess:
		region.lockedRangeState = lockRangeResult.LockedRangeState
		s.client.markRegionQueued(region, lockRangeResult.LockedRangeState.Created, time.Now())
		s.regionTaskQueue.Push(NewRegionPriorityTask(priority, region, s.client.pdClock.CurrentTS()))
	case regionlock.LockRangeStatusStale:
		s.client.removeRegionRuntime(region, time.Now())
		for _, retryRange := range lockRangeResult.RetryRanges {
			s.scheduleRangeRequest(ctx, retryRange, region.subscribedSpan, region.filterLoop, priority)
		}
	case regionlock.LockRangeStatusCancel:
		s.client.removeRegionRuntime(region, time.Now())
	default:
		return
	}
}

func (s *regionRequestScheduler) scheduleRangeRequest(
	ctx context.Context,
	span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	priority TaskType,
) bool {
	select {
	case <-ctx.Done():
		return false
	case s.rangeTaskCh <- rangeTask{
		span:           span,
		subscribedSpan: subscribedSpan,
		filterLoop:     filterLoop,
		priority:       priority,
	}:
		return true
	}
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

func (s *regionRequestScheduler) submitDirectFailure(failure regionFailureInfo) {
	s.client.recordRegionRuntimeError(failure.regionInfo, failure.err, time.Now())
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
		if region.isStopped() {
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
	s.client.markRegionRetryPending(failure.regionInfo, failure.err, time.Now())
	s.scheduleRegionRequest(ctx, failure.regionInfo, priority)
}

func (s *regionRequestScheduler) reloadRegionRange(ctx context.Context, failure regionFailureInfo) {
	s.client.removeRegionRuntime(failure.regionInfo, time.Now())
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
		s.client.removeRegionRuntime(failure.regionInfo, time.Now())
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
