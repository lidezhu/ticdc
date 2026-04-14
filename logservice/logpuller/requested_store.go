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
	"sync"
	"sync/atomic"
)

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
