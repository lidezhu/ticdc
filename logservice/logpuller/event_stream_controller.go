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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/dynstream"
)

type eventStreamController struct {
	ctx context.Context

	ds dynstream.DynamicStream[int, SubscriptionID, regionEvent, *subscribedSpan, *regionEventHandler]

	mu   sync.Mutex
	cond *sync.Cond

	paused atomic.Bool
}

func newEventStreamController(
	ctx context.Context,
	ds dynstream.DynamicStream[int, SubscriptionID, regionEvent, *subscribedSpan, *regionEventHandler],
) *eventStreamController {
	controller := &eventStreamController{
		ctx: ctx,
		ds:  ds,
	}
	controller.cond = sync.NewCond(&controller.mu)
	return controller
}

func (c *eventStreamController) wakeSubscription(subID SubscriptionID) {
	c.ds.Wake(subID)
}

func (c *eventStreamController) pushRegionEvent(subID SubscriptionID, event regionEvent) {
	if !c.paused.Load() {
		c.ds.Push(subID, event)
		return
	}

	c.mu.Lock()
	for c.paused.Load() {
		select {
		case <-c.ctx.Done():
			c.mu.Unlock()
			return
		default:
			c.cond.Wait()
		}
	}
	c.mu.Unlock()
	c.ds.Push(subID, event)
}

func (c *eventStreamController) addPath(subID SubscriptionID, span *subscribedSpan, area dynstream.AreaSettings) error {
	return c.ds.AddPath(subID, span, area)
}

func (c *eventStreamController) removePath(subID SubscriptionID) error {
	return c.ds.RemovePath(subID)
}

func (c *eventStreamController) runFeedback(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case feedback := <-c.ds.Feedback():
			switch feedback.FeedbackType {
			case dynstream.PauseArea:
				c.mu.Lock()
				c.paused.Store(true)
				c.mu.Unlock()
				log.Info("subscription client pause push region event")
			case dynstream.ResumeArea:
				c.mu.Lock()
				c.paused.Store(false)
				c.cond.Broadcast()
				c.mu.Unlock()
				log.Info("subscription client resume push region event")
			case dynstream.ReleasePath, dynstream.ResumePath:
				// Ignore it, because it is no need to pause and resume a path in puller.
			}
		}
	}
}

func (c *eventStreamController) metrics() dynstream.Metrics[int, SubscriptionID] {
	return c.ds.GetMetrics()
}

func (c *eventStreamController) close() {
	c.mu.Lock()
	c.paused.Store(false)
	c.cond.Broadcast()
	c.mu.Unlock()
	c.ds.Close()
}
