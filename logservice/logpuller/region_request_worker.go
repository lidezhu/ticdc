// Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	pd "github.com/tikv/pd/client"
	"golang.org/x/sync/errgroup"
)

// To generate a workerID in `newRegionRequestWorker`.
var workerIDGen atomic.Uint64

// requestedStore groups region request workers that share one TiKV address.
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

// regionRequestWorker is responsible for sending region requests to a specific TiKV store.
// It owns the long-lived request cache and reconnection loop.
type regionRequestWorker struct {
	workerID uint64

	pd                         pd.Client
	clusterID                  uint64
	pushRegionEvent            func(SubscriptionID, regionEvent)
	runtimeRegistry            *regionRuntimeRegistry
	submitDirectFailure        func(regionFailureInfo)
	submitWorkerSessionFailure func(map[SubscriptionID]regionFeedStates, []regionInfo, workerSessionFailure)

	store *requestedStore

	// request cache with flow control
	requestCache *requestCache
}

func (s *regionRequestWorker) markRegionEnqueued(region regionInfo, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.runtimeRegistry.setRequestEnqueueTime(region.runtimeKey, now)
}

func (s *regionRequestWorker) handleSessionFailure(session *regionRequestWorkerSession, sessionFailure workerSessionFailure) {
	// runNextSession only returns after the session loops exit, so the started and
	// not-started sets are already quiescent here.
	snapshot := session.takeFailureSnapshot()
	s.submitWorkerSessionFailure(
		snapshot.startedRegions,
		snapshot.pendingRegions,
		sessionFailure,
	)
}

func (s *regionRequestWorker) runNextSession(
	ctx context.Context,
	credential *security.Credential,
) (*regionRequestWorkerSession, workerSessionFailure, bool, error) {
	session := newRegionRequestWorkerSession(
		s.workerID,
		s.store.storeAddr,
		s.pd,
		credential,
		s.clusterID,
		s.requestCache,
		s.runtimeRegistry,
		s.submitDirectFailure,
		s.pushRegionEvent,
	)
	result, err := session.run(ctx)
	if err != nil {
		return nil, workerSessionFailure{}, false, err
	}
	if result.canceled {
		return nil, workerSessionFailure{}, true, nil
	}
	return session, result.failure, false, nil
}

func (s *regionRequestWorker) runSessionLoop(
	ctx context.Context,
	credential *security.Credential,
) error {
	for {
		session, sessionFailure, canceled, err := s.runNextSession(ctx, credential)
		if err != nil {
			return err
		}
		if canceled {
			return nil
		}
		s.handleSessionFailure(session, sessionFailure)

		if err := util.Hang(ctx, time.Second); err != nil {
			return err
		}
	}
}

func newRegionRequestWorker(
	ctx context.Context,
	client *subscriptionClient,
	credential *security.Credential,
	g *errgroup.Group,
	store *requestedStore,
	requestCacheSize int,
) *regionRequestWorker {
	worker := &regionRequestWorker{
		workerID:                   workerIDGen.Add(1),
		pd:                         client.pd,
		clusterID:                  client.clusterID,
		pushRegionEvent:            client.pushRegionEventToDS,
		runtimeRegistry:            client.regionRuntimeRegistry,
		submitDirectFailure:        client.submitDirectFailure,
		submitWorkerSessionFailure: client.submitWorkerSessionFailure,
		store:                      store,
		requestCache:               newRequestCache(requestCacheSize),
	}

	g.Go(func() error {
		return worker.runSessionLoop(ctx, credential)
	})

	return worker
}

// add adds a region request to the worker window.
func (s *regionRequestWorker) add(ctx context.Context, region regionInfo, force bool) (bool, error) {
	ok, err := s.requestCache.add(ctx, region, force)
	if err != nil {
		return false, err
	}
	if ok {
		s.markRegionEnqueued(region, time.Now())
	}
	return ok, err
}
