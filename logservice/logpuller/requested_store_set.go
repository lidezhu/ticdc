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
	"sort"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type requestedStoreSet struct {
	client *subscriptionClient
	stores sync.Map
}

func newRequestedStoreSet(client *subscriptionClient) *requestedStoreSet {
	return &requestedStoreSet{client: client}
}

func (s *requestedStoreSet) requestStats() RequestCacheSnapshot {
	var totals RequestCacheSnapshot
	s.stores.Range(func(_, value any) bool {
		store := value.(*requestedStore)
		for _, worker := range store.snapshotWorkers() {
			requestCacheSnapshotAdd(&totals, worker.requestCache.snapshot())
		}
		return true
	})
	return totals
}

func (s *requestedStoreSet) snapshotStores() []StoreObservability {
	snapshots := make([]StoreObservability, 0)
	s.stores.Range(func(_, value any) bool {
		store := value.(*requestedStore)
		snapshots = append(snapshots, store.snapshot())
		return true
	})
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].StoreAddr < snapshots[j].StoreAddr
	})
	return snapshots
}

func (s *requestedStoreSet) clearPendingRequests() {
	s.stores.Range(func(_, value any) bool {
		store := value.(*requestedStore)
		for _, worker := range store.snapshotWorkers() {
			worker.requestCache.clear()
		}
		return true
	})
}

func (s *requestedStoreSet) perWorkerQueueSize() int {
	config := config.GetGlobalServerConfig()
	perWorkerQueueSize := config.Debug.Puller.PendingRegionRequestQueueSize / int(s.client.config.RegionRequestWorkerPerStore)
	if perWorkerQueueSize <= 0 {
		log.Warn("pending region request queue size is smaller than the number of workers, adjust per worker queue size to 1",
			zap.Int("pendingRegionRequestQueueSize", config.Debug.Puller.PendingRegionRequestQueueSize),
			zap.Uint("regionRequestWorkerPerStore", s.client.config.RegionRequestWorkerPerStore))
		return 1
	}
	return perWorkerQueueSize
}

func (s *requestedStoreSet) getOrCreateRequestedStore(
	ctx context.Context,
	eg *errgroup.Group,
	storeAddr string,
) *requestedStore {
	if value, ok := s.stores.Load(storeAddr); ok {
		return value.(*requestedStore)
	}

	store := newRequestedStore(storeAddr)
	s.stores.Store(storeAddr, store)

	perWorkerQueueSize := s.perWorkerQueueSize()
	for i := uint(0); i < s.client.config.RegionRequestWorkerPerStore; i++ {
		store.addWorker(newRegionRequestWorker(ctx, s.client, eg, store, perWorkerQueueSize))
	}
	return store
}

func (s *requestedStoreSet) broadcastStopRequest(ctx context.Context, region regionInfo) error {
	var firstErr error
	s.stores.Range(func(_ any, value any) bool {
		store := value.(*requestedStore)
		for _, worker := range store.snapshotWorkers() {
			ok, err := worker.add(ctx, region, true)
			if err != nil {
				firstErr = err
				log.Warn("broadcast stop request failed",
					zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
					zap.Uint64("workerID", worker.workerID),
					zap.String("addr", store.storeAddr),
					zap.Error(err))
				return false
			}
			if !ok {
				log.Panic("forced stop request should always be enqueued",
					zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
					zap.Uint64("workerID", worker.workerID),
					zap.String("addr", store.storeAddr))
			}
		}
		return true
	})
	return firstErr
}
