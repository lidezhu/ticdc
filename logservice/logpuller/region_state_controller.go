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

type regionStateController struct {
	workerID uint64
	client   *subscriptionClient

	requestCache *requestCache
	takeState    func(SubscriptionID, uint64) *regionFeedState
}

func newRegionStateController(
	workerID uint64,
	client *subscriptionClient,
	requestCache *requestCache,
	takeState func(SubscriptionID, uint64) *regionFeedState,
) *regionStateController {
	return &regionStateController{
		workerID:     workerID,
		client:       client,
		requestCache: requestCache,
		takeState:    takeState,
	}
}

func (c *regionStateController) runtimeRegistry() *regionRuntimeRegistry {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.regionRuntimeRegistry
}

func (c *regionStateController) markRequestStopped(subID SubscriptionID, regionID uint64) {
	if c == nil || c.requestCache == nil {
		return
	}
	c.requestCache.markStopped(subID, regionID)
}

func (c *regionStateController) resolveRequest(subID SubscriptionID, regionID uint64) bool {
	if c == nil || c.requestCache == nil {
		return false
	}
	return c.requestCache.resolve(subID, regionID)
}

func (c *regionStateController) removeRegionState(subID SubscriptionID, regionID uint64) *regionFeedState {
	if c == nil || c.takeState == nil {
		return nil
	}
	return c.takeState(subID, regionID)
}

func (c *regionStateController) getWorkerID() uint64 {
	if c == nil {
		return 0
	}
	return c.workerID
}
