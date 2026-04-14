// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

const (
	// add() waits briefly for space so the region scheduler can retry instead of
	// parking on one busy worker indefinitely.
	addReqRetryInterval          = time.Millisecond
	addReqRetryLimit             = 3
	abnormalRequestDurationInSec = 60 * 60 * 2 // 2 hours
)

type regionReqStage uint8

const (
	// queued -> processing -> sent -> finished
	// queued/processing can also go directly to finished when the session exits
	// before the request becomes an active region state.
	regionReqStageQueued regionReqStage = iota
	regionReqStageProcessing
	regionReqStageSent
	regionReqStageFinished
)

type regionReqKey struct {
	subID    SubscriptionID
	regionID uint64
	stop     bool
}

func newRegionReqKey(region regionInfo) regionReqKey {
	key := regionReqKey{subID: region.subscribedSpan.subID}
	if region.isStopped() {
		key.stop = true
		return key
	}
	key.regionID = region.verID.GetID()
	return key
}

// regionReq tracks one worker-local request from admission to completion.
type regionReq struct {
	regionInfo regionInfo
	createTime time.Time

	cache *requestCache
	key   regionReqKey
	stage regionReqStage
	// true when add() sees an older processing/sent request with the same key.
	// When the newer request is sent, markSent removes that older sent request
	// so the cache keeps only the newest live request for this key.
	replacesActive bool
}

func newRegionReq(cache *requestCache, region regionInfo) *regionReq {
	return &regionReq{
		regionInfo: region,
		createTime: time.Now(),
		cache:      cache,
		key:        newRegionReqKey(region),
		stage:      regionReqStageQueued,
	}
}

func (r *regionReq) markSent() {
	if r == nil || r.cache == nil {
		return
	}
	r.cache.markSent(r)
}

func (r *regionReq) resolve() {
	if r == nil || r.cache == nil {
		return
	}
	r.cache.resolve(r)
}

func (r *regionReq) finish() {
	if r == nil || r.cache == nil {
		return
	}
	r.cache.finish(r)
}

// requestCache is a per-worker request window.
//
// It owns two concerns together:
// 1. admission control: limit how many requests are outstanding in this worker
// 2. request lifecycle: track each request until it is initialized or terminated
//
// The source of truth is `requests`. A request enters `requests` in add() and
// stays there until resolve()/finish() removes it.
type requestCache struct {
	mu sync.Mutex

	// requests owns every live request in this worker, regardless of stage.
	// len(requests) is the "pending request count" exported to metrics/logs:
	// it includes queued, processing and sent requests.
	requests map[*regionReq]struct{}
	// current points to the newest live request for a key.
	// A queued duplicate updates the existing queued request in place. If an
	// older request is already processing or sent, the newer request is still
	// accepted and becomes the new current request for that key.
	current map[regionReqKey]*regionReq
	// ready is the FIFO of queued requests waiting for the send loop.
	// readyIdx lets us compact lazily instead of shifting on every pop.
	ready    []*regionReq
	readyIdx int

	// maxPendingCount limits the number of live requests in this worker for
	// non-force adds. A live request stays in `requests` from add() until it is
	// resolved or finished, so the limit covers queued, processing and sent
	// requests together.
	maxPendingCount int

	// readyAvailable wakes pop() when a new queued request becomes visible.
	readyAvailable chan struct{}
	// spaceAvailable wakes add() when pop()/finish()/resolve() releases capacity.
	spaceAvailable chan struct{}
}

func newRequestCache(maxPendingCount int) *requestCache {
	return &requestCache{
		requests:        make(map[*regionReq]struct{}),
		current:         make(map[regionReqKey]*regionReq),
		ready:           make([]*regionReq, 0, maxPendingCount),
		maxPendingCount: maxPendingCount,
		readyAvailable:  make(chan struct{}, 1),
		spaceAvailable:  make(chan struct{}, 1),
	}
}

// add admits a request into this worker window.
// If the same key is still queued, the newer region info replaces the old one.
// If the older request is already processing or sent, the newer request is
// still queued, becomes current[key], and emits a warn log. Stop requests keep
// the same compatibility path.
func (c *requestCache) add(ctx context.Context, region regionInfo, force bool) (bool, error) {
	start := time.Now()
	ticker := time.NewTicker(addReqRetryInterval)
	defer ticker.Stop()
	retries := addReqRetryLimit

	for {
		ok, err := c.tryAdd(region, force)
		if err != nil {
			return false, err
		}
		if ok {
			metrics.SubscriptionClientAddRegionRequestDuration.Observe(time.Since(start).Seconds())
			return true, nil
		}

		select {
		case <-ticker.C:
			retries--
			if retries <= 0 {
				return false, nil
			}
		case <-c.spaceAvailable:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

func (c *requestCache) tryAdd(region regionInfo, force bool) (bool, error) {
	req := newRegionReq(c, region)
	shouldNotifyReady := false

	c.mu.Lock()
	defer func() {
		c.mu.Unlock()
		if shouldNotifyReady {
			c.notifyReady()
		}
	}()

	if existing, ok := c.current[req.key]; ok {
		if existing.stage == regionReqStageQueued {
			existing.regionInfo = region
			return true, nil
		}
		req.replacesActive = true
		if !req.key.stop {
			log.Warn("duplicate active region request detected, keep newest request",
				zap.Uint64("subID", uint64(existing.key.subID)),
				zap.Uint64("regionID", existing.key.regionID),
				zap.Uint8("stage", uint8(existing.stage)),
				zap.Int("pendingCount", len(c.requests)))
		}
	}
	if len(c.requests) >= c.maxPendingCount && !force {
		return false, nil
	}

	c.requests[req] = struct{}{}
	c.current[req.key] = req
	c.ready = append(c.ready, req)
	shouldNotifyReady = true
	return true, nil
}

// pop takes the next queued request and moves it into processing state.
func (c *requestCache) pop(ctx context.Context) (*regionReq, error) {
	for {
		if req := c.tryPop(); req != nil {
			return req, nil
		}

		select {
		case <-c.readyAvailable:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *requestCache) tryPop() *regionReq {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.readyIdx < len(c.ready) {
		req := c.ready[c.readyIdx]
		c.ready[c.readyIdx] = nil
		c.readyIdx++

		if req == nil {
			continue
		}
		if _, ok := c.requests[req]; !ok || req.stage != regionReqStageQueued {
			continue
		}

		req.stage = regionReqStageProcessing
		c.compactReadyLocked()
		return req
	}

	c.compactReadyLocked()
	return nil
}

func (c *requestCache) markSent(req *regionReq) {
	removed := false
	c.mu.Lock()
	if _, ok := c.requests[req]; ok && req.stage == regionReqStageProcessing {
		if req.replacesActive {
			// Duplicate active regions are already unexpected; only this slow path
			// scans live requests to retire the older sent one after the newer
			// request has actually been sent.
			if old := c.findSentLocked(req.key, req); old != nil {
				removed = c.removeLocked(old) || removed
			}
		}
		req.stage = regionReqStageSent
	}
	c.mu.Unlock()

	if removed {
		c.notifySpace()
	}
}

func (c *requestCache) resolve(req *regionReq) {
	if !c.remove(req) {
		return
	}

	cost := time.Since(req.createTime).Seconds()
	if cost > 0 && cost < abnormalRequestDurationInSec {
		log.Debug("cdc resolve region request",
			zap.Uint64("subID", uint64(req.key.subID)),
			zap.Uint64("regionID", req.key.regionID),
			zap.Float64("cost", cost),
			zap.Int("pendingCount", c.getPendingCount()))
		metrics.RegionRequestFinishScanDuration.Observe(cost)
		return
	}
	log.Info("region request duration abnormal, skip metric",
		zap.Float64("cost", cost),
		zap.Uint64("regionID", req.key.regionID))
}

func (c *requestCache) finish(req *regionReq) {
	c.remove(req)
}

func (c *requestCache) remove(req *regionReq) bool {
	if req == nil {
		return false
	}

	removed := false
	c.mu.Lock()
	removed = c.removeLocked(req)
	c.mu.Unlock()

	if removed {
		c.notifySpace()
	}
	return removed
}

func (c *requestCache) takeUnsentRegions() []regionInfo {
	c.mu.Lock()
	regions := make([]regionInfo, 0, len(c.requests))
	removed := 0
	for req := range c.requests {
		if req.stage == regionReqStageSent {
			continue
		}
		regions = append(regions, req.regionInfo)
		if c.removeLocked(req) {
			removed++
		}
	}
	if removed > 0 {
		c.compactReadyLocked()
	}
	c.mu.Unlock()

	if removed > 0 {
		c.notifySpace()
	}
	return regions
}

// clear removes all requests and returns their regions.
func (c *requestCache) clear() []regionInfo {
	c.mu.Lock()
	regions := make([]regionInfo, 0, len(c.requests))
	for req := range c.requests {
		regions = append(regions, req.regionInfo)
		delete(c.requests, req)
		req.stage = regionReqStageFinished
	}
	removed := len(regions)
	c.current = make(map[regionReqKey]*regionReq)
	c.ready = c.ready[:0]
	c.readyIdx = 0
	c.mu.Unlock()

	if removed > 0 {
		c.notifySpace()
	}
	return regions
}

// getPendingCount returns the number of live requests currently owned by this
// worker. It includes queued, processing and sent requests.
func (c *requestCache) getPendingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.requests)
}

func (c *requestCache) removeLocked(req *regionReq) bool {
	if req == nil {
		return false
	}
	if _, ok := c.requests[req]; !ok {
		return false
	}

	stage := req.stage
	delete(c.requests, req)
	req.stage = regionReqStageFinished
	if c.current[req.key] == req {
		delete(c.current, req.key)
	}
	if stage != regionReqStageSent {
		c.compactReadyLocked()
	}
	return true
}

func (c *requestCache) findSentLocked(key regionReqKey, except *regionReq) *regionReq {
	for req := range c.requests {
		if req == except {
			continue
		}
		if req.key == key && req.stage == regionReqStageSent {
			return req
		}
	}
	return nil
}

func (c *requestCache) notifyReady() {
	select {
	case c.readyAvailable <- struct{}{}:
	default:
	}
}

func (c *requestCache) notifySpace() {
	select {
	case c.spaceAvailable <- struct{}{}:
	default:
	}
}

func (c *requestCache) compactReadyLocked() {
	if c.readyIdx == 0 {
		return
	}
	if c.readyIdx < len(c.ready) && c.readyIdx < 1024 {
		return
	}

	n := copy(c.ready, c.ready[c.readyIdx:])
	for i := n; i < len(c.ready); i++ {
		c.ready[i] = nil
	}
	c.ready = c.ready[:n]
	c.readyIdx = 0
}
