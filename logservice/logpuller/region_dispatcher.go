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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type requestedStore struct {
	storeAddr string
	// Used to select a worker to send requests.
	nextWorker atomic.Uint32

	requestWorkers struct {
		sync.RWMutex
		s []*regionRequestWorker
	}
}

func (rs *requestedStore) getRequestWorker() *regionRequestWorker {
	rs.requestWorkers.RLock()
	defer rs.requestWorkers.RUnlock()

	index := rs.nextWorker.Add(1) % uint32(len(rs.requestWorkers.s))
	return rs.requestWorkers.s[index]
}

type regionDispatcher struct {
	client *subscriptionClient
}

func newRegionDispatcher(client *subscriptionClient) regionDispatcher {
	return regionDispatcher{client: client}
}

func (s *subscriptionClient) handleRegions(ctx context.Context, eg *errgroup.Group) error {
	return newRegionDispatcher(s).handleRegions(ctx, eg)
}

func (s *subscriptionClient) enqueueRegionToAllStores(ctx context.Context, region regionInfo) (bool, error) {
	return newRegionDispatcher(s).enqueueRegionToAllStores(ctx, region)
}

func (s *subscriptionClient) attachRPCContextForRegion(ctx context.Context, region regionInfo) (regionInfo, bool) {
	return newRegionDispatcher(s).attachRPCContextForRegion(ctx, region)
}

func (h regionDispatcher) getOrCreateStore(ctx context.Context, eg *errgroup.Group, storeAddr string) *requestedStore {
	if value, ok := h.client.stores.Load(storeAddr); ok {
		return value.(*requestedStore)
	}

	rs := &requestedStore{storeAddr: storeAddr}
	h.client.stores.Store(storeAddr, rs)

	config := config.GetGlobalServerConfig()
	perWorkerQueueSize := config.Debug.Puller.PendingRegionRequestQueueSize / int(h.client.config.RegionRequestWorkerPerStore)
	if perWorkerQueueSize <= 0 {
		log.Warn("pending region request queue size is smaller than the number of workers, adjust per worker queue size to 1",
			zap.Int("pendingRegionRequestQueueSize", config.Debug.Puller.PendingRegionRequestQueueSize),
			zap.Uint("regionRequestWorkerPerStore", h.client.config.RegionRequestWorkerPerStore))
		perWorkerQueueSize = 1
	}

	for i := uint(0); i < h.client.config.RegionRequestWorkerPerStore; i++ {
		requestWorker := newRegionRequestWorker(ctx, h.client, h.client.credential, eg, rs, perWorkerQueueSize)
		rs.requestWorkers.Lock()
		rs.requestWorkers.s = append(rs.requestWorkers.s, requestWorker)
		rs.requestWorkers.Unlock()
	}
	return rs
}

func (h regionDispatcher) clearWorkers() {
	h.client.stores.Range(func(_ any, value any) bool {
		rs := value.(*requestedStore)

		rs.requestWorkers.RLock()
		for _, worker := range rs.requestWorkers.s {
			worker.requestCache.clear()
		}
		rs.requestWorkers.RUnlock()

		return true
	})
}

// handleRegions receives regionInfo from regionTaskQueue, attaches rpcCtx to them,
// then sends them to the corresponding requestedStore.
func (h regionDispatcher) handleRegions(ctx context.Context, eg *errgroup.Group) error {
	defer h.clearWorkers()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Use blocking Pop to wait for tasks.
		regionTask, err := h.client.regionTaskQueue.Pop(ctx)
		if err != nil {
			return err
		}

		region := regionTask.GetRegionInfo()
		if region.isStopped() {
			enqueued, err := h.enqueueRegionToAllStores(ctx, region)
			if err != nil {
				return err
			}
			if !enqueued {
				log.Debug("enqueue stop request failed, retry later",
					zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)))
				h.client.regionTaskQueue.Push(regionTask)
			}
			continue
		}

		region, ok := h.attachRPCContextForRegion(ctx, region)
		// If attachRPCContextForRegion fails, the region will be re-scheduled.
		if !ok {
			continue
		}
		h.client.updateRegionRuntimeInfo(region)
		h.client.markRegionRuntimeRPCReady(region, time.Now())

		store := h.getOrCreateStore(ctx, eg, region.rpcCtx.Addr)
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
			h.client.regionTaskQueue.Push(regionTask)
			continue
		}

		log.Debug("subscription client will request a region",
			zap.Uint64("workID", worker.workerID),
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.String("addr", store.storeAddr))
	}
}

func (h regionDispatcher) enqueueRegionToAllStores(ctx context.Context, region regionInfo) (bool, error) {
	enqueued := true
	var firstErr error
	h.client.stores.Range(func(_ any, value any) bool {
		rs := value.(*requestedStore)
		rs.requestWorkers.RLock()
		workers := rs.requestWorkers.s
		rs.requestWorkers.RUnlock()
		for _, worker := range workers {
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

func (h regionDispatcher) attachRPCContextForRegion(ctx context.Context, region regionInfo) (regionInfo, bool) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := h.client.regionCache.GetTiKVRPCContext(bo, region.verID, kvclientv2.ReplicaReadLeader, 0)
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
	h.client.onRegionFail(newRegionErrorInfo(region, &rpcCtxUnavailableErr{verID: region.verID}))
	return region, false
}
