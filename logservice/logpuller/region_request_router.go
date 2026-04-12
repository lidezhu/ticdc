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
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// requestedStore represents a store that has been connected.
type requestedStore struct {
	storeAddr string
	// Use to select a worker to send request.
	nextWorker atomic.Uint32

	requestWorkers struct {
		sync.RWMutex
		s []*regionRequestWorker
	}
}

func newRequestedStore(storeAddr string) *requestedStore {
	return &requestedStore{storeAddr: storeAddr}
}

func (rs *requestedStore) addWorker(worker *regionRequestWorker) {
	rs.requestWorkers.Lock()
	defer rs.requestWorkers.Unlock()
	rs.requestWorkers.s = append(rs.requestWorkers.s, worker)
}

func (rs *requestedStore) getRequestWorker() *regionRequestWorker {
	rs.requestWorkers.RLock()
	defer rs.requestWorkers.RUnlock()

	index := rs.nextWorker.Add(1) % uint32(len(rs.requestWorkers.s))
	return rs.requestWorkers.s[index]
}

func (rs *requestedStore) snapshotWorkers() []*regionRequestWorker {
	rs.requestWorkers.RLock()
	defer rs.requestWorkers.RUnlock()

	workers := make([]*regionRequestWorker, len(rs.requestWorkers.s))
	copy(workers, rs.requestWorkers.s)
	return workers
}

type regionRequestRouter struct {
	client          *subscriptionClient
	stores          sync.Map
	rangeTaskCh     chan rangeTask
	regionTaskQueue *PriorityQueue
}

func newRegionRequestRouter(client *subscriptionClient) *regionRequestRouter {
	return &regionRequestRouter{
		client:          client,
		rangeTaskCh:     make(chan rangeTask, 1024),
		regionTaskQueue: NewPriorityQueue(),
	}
}

func (r *regionRequestRouter) close() {
	r.regionTaskQueue.Close()
}

func (r *regionRequestRouter) submitRangeTask(ctx context.Context, task rangeTask) bool {
	select {
	case <-ctx.Done():
		return false
	case r.rangeTaskCh <- task:
		return true
	}
}

func (r *regionRequestRouter) scheduleStopTask(subscribedSpan *subscribedSpan) {
	r.regionTaskQueue.Push(NewRegionPriorityTask(
		TaskHighPrior,
		regionInfo{subscribedSpan: subscribedSpan, filterLoop: subscribedSpan.filterLoop},
		r.client.pdClock.CurrentTS(),
	))
}

func (r *regionRequestRouter) forEachStore(fn func(*requestedStore) bool) {
	r.stores.Range(func(_, value any) bool {
		return fn(value.(*requestedStore))
	})
}

func (r *regionRequestRouter) pendingRequestCount() int {
	pendingRegionReqCount := 0
	r.forEachStore(func(store *requestedStore) bool {
		for _, worker := range store.snapshotWorkers() {
			worker.requestCache.clearStaleRequest()
			pendingRegionReqCount += worker.requestCache.getPendingCount()
		}
		return true
	})
	return pendingRegionReqCount
}

func (r *regionRequestRouter) cleanupRequestedStores() {
	r.forEachStore(func(store *requestedStore) bool {
		for _, worker := range store.snapshotWorkers() {
			worker.requestCache.clear()
		}
		return true
	})
}

func (r *regionRequestRouter) perWorkerQueueSize() int {
	config := config.GetGlobalServerConfig()
	perWorkerQueueSize := config.Debug.Puller.PendingRegionRequestQueueSize / int(r.client.config.RegionRequestWorkerPerStore)
	if perWorkerQueueSize <= 0 {
		log.Warn("pending region request queue size is smaller than the number of workers, adjust per worker queue size to 1",
			zap.Int("pendingRegionRequestQueueSize", config.Debug.Puller.PendingRegionRequestQueueSize),
			zap.Uint("regionRequestWorkerPerStore", r.client.config.RegionRequestWorkerPerStore))
		return 1
	}
	return perWorkerQueueSize
}

func (r *regionRequestRouter) getOrCreateRequestedStore(
	ctx context.Context,
	eg *errgroup.Group,
	storeAddr string,
) *requestedStore {
	if value, ok := r.stores.Load(storeAddr); ok {
		return value.(*requestedStore)
	}

	store := newRequestedStore(storeAddr)
	r.stores.Store(storeAddr, store)

	perWorkerQueueSize := r.perWorkerQueueSize()
	for i := uint(0); i < r.client.config.RegionRequestWorkerPerStore; i++ {
		store.addWorker(newRegionRequestWorker(ctx, r.client, r.client.credential, eg, store, perWorkerQueueSize))
	}
	return store
}

func (r *regionRequestRouter) enqueueRegionToAllStores(ctx context.Context, region regionInfo) (bool, error) {
	enqueued := true
	var firstErr error

	r.forEachStore(func(store *requestedStore) bool {
		for _, worker := range store.snapshotWorkers() {
			ok, err := worker.add(ctx, region, true)
			if err != nil {
				firstErr = err
				enqueued = false
				return false
			}
			if !ok {
				enqueued = false
				// It is likely the store is busy, no need to try other workers in this store now.
				break
			}
		}
		return true
	})
	return enqueued, firstErr
}

func (r *regionRequestRouter) attachRPCContextForRegion(ctx context.Context, region regionInfo) (regionInfo, bool) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := r.client.regionCache.GetTiKVRPCContext(bo, region.verID, kvclientv2.ReplicaReadLeader, 0)
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
	r.client.failures.submitDirectFailure(newRPCCtxUnavailableFailure(region))
	return region, false
}

func (r *regionRequestRouter) routeStoppedRegionTask(ctx context.Context, regionTask PriorityTask) error {
	region := regionTask.GetRegionInfo()
	enqueued, err := r.enqueueRegionToAllStores(ctx, region)
	if err != nil {
		return err
	}
	if !enqueued {
		log.Debug("enqueue stop request failed, retry later",
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)))
		r.regionTaskQueue.Push(regionTask)
	}
	return nil
}

func (r *regionRequestRouter) routeActiveRegionTask(
	ctx context.Context,
	eg *errgroup.Group,
	regionTask PriorityTask,
) error {
	region := regionTask.GetRegionInfo()
	region, ok := r.attachRPCContextForRegion(ctx, region)
	if !ok {
		return nil
	}
	r.client.updateRegionRuntimeInfo(region)
	r.client.markRegionRPCReady(region, time.Now())

	store := r.getOrCreateRequestedStore(ctx, eg, region.rpcCtx.Addr)
	worker := store.getRequestWorker()
	force := regionTask.Priority() <= forcedPriorityBase

	ok, err := worker.add(ctx, region, force)
	if err != nil {
		log.Warn("subscription client add region request failed",
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Error(err))
		return err
	}
	if !ok {
		r.regionTaskQueue.Push(regionTask)
		return nil
	}

	log.Debug("subscription client will request a region",
		zap.Uint64("workID", worker.workerID),
		zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
		zap.Uint64("regionID", region.verID.GetID()),
		zap.String("addr", store.storeAddr))
	return nil
}

func (r *regionRequestRouter) routeRegionTask(
	ctx context.Context,
	eg *errgroup.Group,
	regionTask PriorityTask,
) error {
	region := regionTask.GetRegionInfo()
	if region.isStopped() {
		return r.routeStoppedRegionTask(ctx, regionTask)
	}
	return r.routeActiveRegionTask(ctx, eg, regionTask)
}

// runRegionTaskLoop receives regionInfo from regionTaskQueue, attaches rpcCtx to them,
// then routes them to the corresponding requestedStore.
func (r *regionRequestRouter) runRegionTaskLoop(ctx context.Context, eg *errgroup.Group) error {
	defer r.cleanupRequestedStores()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		regionTask, err := r.regionTaskQueue.Pop(ctx)
		if err != nil {
			return err
		}
		if err := r.routeRegionTask(ctx, eg, regionTask); err != nil {
			return err
		}
	}
}

func (r *regionRequestRouter) runRangeTaskLoop(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	// Limit the concurrent number of goroutines to convert range tasks to region tasks.
	g.SetLimit(1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-r.rangeTaskCh:
			g.Go(func() error {
				return r.divideSpanAndScheduleRegionRequests(ctx, task.span, task.subscribedSpan, task.filterLoop, task.priority)
			})
		}
	}
}

// divideSpanAndScheduleRegionRequests processes the specified span by dividing it into
// manageable regions and schedules requests to subscribe to these regions.
// 1. Load regions from PD.
// 2. Find the intersection of each region.span and the subscribedSpan.span.
// 3. Schedule a region request to subscribe the region.
func (r *regionRequestRouter) divideSpanAndScheduleRegionRequests(
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
		regions, err := r.client.regionCache.BatchLoadRegionsWithKeyRange(backoff, nextSpan.StartKey, nextSpan.EndKey, limit)
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
			r.scheduleRegionRequest(ctx, regionInfo, taskType)

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
// which will be handled by runRegionTaskLoop.
func (r *regionRequestRouter) scheduleRegionRequest(ctx context.Context, region regionInfo, priority TaskType) {
	r.client.ensureRegionRuntime(&region, time.Now())
	lockRangeResult := region.subscribedSpan.rangeLock.LockRange(
		ctx, region.span.StartKey, region.span.EndKey, region.verID.GetID(), region.verID.GetVer())

	if lockRangeResult.Status == regionlock.LockRangeStatusWait {
		r.client.transitionRegionRuntime(region, regionPhaseRangeLockWait, time.Now())
		lockRangeResult = lockRangeResult.WaitFn()
	}

	switch lockRangeResult.Status {
	case regionlock.LockRangeStatusSuccess:
		region.lockedRangeState = lockRangeResult.LockedRangeState
		r.client.markRegionQueued(region, lockRangeResult.LockedRangeState.Created, time.Now())
		r.regionTaskQueue.Push(NewRegionPriorityTask(priority, region, r.client.pdClock.CurrentTS()))
	case regionlock.LockRangeStatusStale:
		r.client.removeRegionRuntime(region, time.Now())
		for _, retryRange := range lockRangeResult.RetryRanges {
			r.scheduleRangeRequest(ctx, retryRange, region.subscribedSpan, region.filterLoop, priority)
		}
	case regionlock.LockRangeStatusCancel:
		r.client.removeRegionRuntime(region, time.Now())
	default:
		return
	}
}

func (r *regionRequestRouter) scheduleRangeRequest(
	ctx context.Context,
	span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	priority TaskType,
) {
	_ = r.submitRangeTask(ctx, rangeTask{
		span:           span,
		subscribedSpan: subscribedSpan,
		filterLoop:     filterLoop,
		priority:       priority,
	})
}
