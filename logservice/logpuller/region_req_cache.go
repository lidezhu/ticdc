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
// The source of truth is `requests`. A request uses one slot from add() until it
// reaches a terminal state through resolve()/finish().
type requestCache struct {
	mu sync.Mutex

	requests map[regionReqKey]*regionReq
	ready    []*regionReq
	readyIdx int
	// queuedCount tracks requests that are still waiting for the send loop.
	// It preserves the old pendingQueue capacity semantics for force requests.
	queuedCount int

	maxPendingCount int

	readyAvailable chan struct{}
	spaceAvailable chan struct{}
}

func newRequestCache(maxPendingCount int) *requestCache {
	return &requestCache{
		requests:        make(map[regionReqKey]*regionReq),
		ready:           make([]*regionReq, 0, maxPendingCount),
		maxPendingCount: maxPendingCount,
		readyAvailable:  make(chan struct{}, 1),
		spaceAvailable:  make(chan struct{}, 1),
	}
}

// add admits a request into this worker window.
// If the same request is still queued, the latest region info replaces the old one.
// If the same request is already being processed or has been sent, the old request
// stays authoritative and we log the duplicate for further investigation.
func (c *requestCache) add(ctx context.Context, region regionInfo, force bool) (bool, error) {
	start := time.Now()
	ticker := time.NewTicker(addReqRetryInterval)
	defer ticker.Stop()
	retries := addReqRetryLimit

	for {
		if c.tryAdd(region, force) {
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

func (c *requestCache) tryAdd(region regionInfo, force bool) bool {
	req := newRegionReq(c, region)
	shouldNotifyReady := false

	c.mu.Lock()
	defer func() {
		c.mu.Unlock()
		if shouldNotifyReady {
			c.notifyReady()
		}
	}()

	if existing, ok := c.requests[req.key]; ok {
		return c.replacePendingLocked(existing, region)
	}
	if c.queuedCount >= c.maxPendingCount {
		return false
	}
	if len(c.requests) >= c.maxPendingCount && !force {
		return false
	}

	c.requests[req.key] = req
	c.ready = append(c.ready, req)
	c.queuedCount++
	shouldNotifyReady = true
	return true
}

func (c *requestCache) replacePendingLocked(existing *regionReq, region regionInfo) bool {
	if existing.stage == regionReqStageQueued {
		existing.regionInfo = region
		return true
	}
	log.Warn("region request already active when adding duplicate",
		zap.Uint64("subID", uint64(existing.key.subID)),
		zap.Uint64("regionID", existing.key.regionID),
		zap.Bool("stop", existing.key.stop),
		zap.Uint8("stage", uint8(existing.stage)))
	return true
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
	shouldNotifySpace := false
	c.mu.Lock()
	defer func() {
		c.mu.Unlock()
		if shouldNotifySpace {
			c.notifySpace()
		}
	}()

	for c.readyIdx < len(c.ready) {
		req := c.ready[c.readyIdx]
		c.ready[c.readyIdx] = nil
		c.readyIdx++

		if req == nil {
			continue
		}
		if current, ok := c.requests[req.key]; !ok || current != req || req.stage != regionReqStageQueued {
			continue
		}

		req.stage = regionReqStageProcessing
		c.queuedCount--
		c.compactReadyLocked()
		shouldNotifySpace = true
		return req
	}

	c.compactReadyLocked()
	return nil
}

func (c *requestCache) markSent(req *regionReq) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if current, ok := c.requests[req.key]; ok && current == req && req.stage == regionReqStageProcessing {
		req.stage = regionReqStageSent
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
	if current, ok := c.requests[req.key]; ok && current == req {
		stage := req.stage
		delete(c.requests, req.key)
		req.stage = regionReqStageFinished
		if stage == regionReqStageQueued {
			c.queuedCount--
		}
		if stage != regionReqStageSent {
			c.compactReadyLocked()
		}
		removed = true
	}
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
	for key, req := range c.requests {
		if req.stage == regionReqStageSent {
			continue
		}
		regions = append(regions, req.regionInfo)
		delete(c.requests, key)
		if req.stage == regionReqStageQueued {
			c.queuedCount--
		}
		req.stage = regionReqStageFinished
		removed++
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
	for key, req := range c.requests {
		regions = append(regions, req.regionInfo)
		delete(c.requests, key)
		req.stage = regionReqStageFinished
	}
	removed := len(regions)
	c.ready = c.ready[:0]
	c.readyIdx = 0
	c.queuedCount = 0
	c.mu.Unlock()

	if removed > 0 {
		c.notifySpace()
	}
	return regions
}

func (c *requestCache) getPendingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.requests)
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
