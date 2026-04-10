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
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type regionScheduler struct {
}

func (h regionScheduler) setTableStopped(client *subscriptionClient, rt *subscribedSpan) {
	log.Info("subscription client starts to stop table",
		zap.Uint64("subscriptionID", uint64(rt.subID)))

	// Set stopped to true so we can stop handling region events from the table.
	// Then send a special singleRegionInfo to regionRouter to deregister the table
	// from all TiKV instances.
	if rt.stopped.CompareAndSwap(false, true) {
		client.regionTaskQueue.Push(NewRegionPriorityTask(TaskHighPrior, regionInfo{
			subscribedSpan: rt,
			filterLoop:     rt.filterLoop,
		}, client.pdClock.CurrentTS()))
		if rt.rangeLock.Stop() {
			client.onTableDrained(rt)
		}
	}
}

func (h regionScheduler) handleRangeTasks(client *subscriptionClient, ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	// Limit the concurrent number of goroutines to convert range tasks to region tasks.
	g.SetLimit(1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-client.rangeTaskCh:
			g.Go(func() error {
				return h.divideSpanAndScheduleRegionRequests(
					client, ctx, task.span, task.subscribedSpan, task.filterLoop, task.priority,
				)
			})
		}
	}
}

// divideSpanAndScheduleRegionRequests processes the specified span by dividing it into
// manageable regions and schedules requests to subscribe to these regions.
// 1. Load regions from PD.
// 2. Find the intersection of each region.span and the subscribedSpan.span.
// 3. Schedule a region request to subscribe the region.
func (h regionScheduler) divideSpanAndScheduleRegionRequests(
	client *subscriptionClient,
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
		regions, err := client.regionCache.BatchLoadRegionsWithKeyRange(backoff, nextSpan.StartKey, nextSpan.EndKey, limit)
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

			verID := tikv.NewRegionVerID(regionMeta.Id, regionMeta.RegionEpoch.ConfVer, regionMeta.RegionEpoch.Version)
			regionInfo := newRegionInfo(verID, intersectSpan, nil, subscribedSpan, filterLoop)

			// Schedule a region request to subscribe the region.
			h.scheduleRegionRequest(client, ctx, regionInfo, taskType)

			nextSpan.StartKey = regionMeta.EndKey
			// If the nextSpan.StartKey is larger than the subscribedSpan.span.EndKey,
			// it means all span of the subscribedSpan have been requested. So we return.
			if common.EndCompare(nextSpan.StartKey, span.EndKey) >= 0 {
				return nil
			}
		}
	}
}

// scheduleRegionRequest locks the region's range and sends the region to regionTaskQueue,
// which will be handled by handleRegions.
func (h regionScheduler) scheduleRegionRequest(
	client *subscriptionClient,
	ctx context.Context,
	region regionInfo,
	priority TaskType,
) {
	client.ensureRegionRuntime(&region, time.Now())
	lockRangeResult := region.subscribedSpan.rangeLock.LockRange(
		ctx, region.span.StartKey, region.span.EndKey, region.verID.GetID(), region.verID.GetVer())

	if lockRangeResult.Status == regionlock.LockRangeStatusWait {
		client.transitionRegionRuntime(region, regionPhaseRangeLockWait, time.Now())
		lockRangeResult = lockRangeResult.WaitFn()
	}

	switch lockRangeResult.Status {
	case regionlock.LockRangeStatusSuccess:
		region.lockedRangeState = lockRangeResult.LockedRangeState
		client.markRegionRuntimeQueued(region, lockRangeResult.LockedRangeState.Created, time.Now())
		client.regionTaskQueue.Push(NewRegionPriorityTask(priority, region, client.pdClock.CurrentTS()))
	case regionlock.LockRangeStatusStale:
		client.removeRegionRuntime(region, time.Now())
		for _, retrySpan := range lockRangeResult.RetryRanges {
			h.scheduleRangeRequest(client, ctx, retrySpan, region.subscribedSpan, region.filterLoop, priority)
		}
	case regionlock.LockRangeStatusCancel:
		client.removeRegionRuntime(region, time.Now())
	default:
		return
	}
}

func (h regionScheduler) scheduleRangeRequest(
	client *subscriptionClient,
	ctx context.Context,
	span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	priority TaskType,
) {
	select {
	case <-ctx.Done():
	case client.rangeTaskCh <- rangeTask{
		span:           span,
		subscribedSpan: subscribedSpan,
		filterLoop:     filterLoop,
		priority:       priority,
	}:
	}
}
