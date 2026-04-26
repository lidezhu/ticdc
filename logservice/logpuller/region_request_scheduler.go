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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/util"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// regionRequestScheduler converts subscribed table spans into region-level
// requests and admits those requests into store-local workers.
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
		newStopRegionInfo(span),
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
			if err := s.client.requestedStores.broadcastStopRequest(ctx, region); err != nil {
				return err
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
	s.client.markRegionDiscovered(&region, time.Now())
	lockRangeResult := region.subscribedSpan.rangeLock.LockRange(
		ctx, region.span.StartKey, region.span.EndKey, region.verID.GetID(), region.verID.GetVer())

	if lockRangeResult.Status == regionlock.LockRangeStatusWait {
		s.client.markRegionRangeLockWait(region, time.Now())
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
