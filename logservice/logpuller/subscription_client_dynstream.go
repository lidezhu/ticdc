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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/dynstream"
)

func (s *subscriptionClient) wakeSubscription(subID SubscriptionID) {
	s.ds.Wake(subID)
}

func (s *subscriptionClient) pushRegionEventToDS(subID SubscriptionID, event regionEvent) {
	// fast path
	if !s.paused.Load() {
		s.ds.Push(subID, event)
		return
	}
	// slow path: wait until paused is false
	s.mu.Lock()
	for s.paused.Load() {
		select {
		case <-s.ctx.Done():
			s.mu.Unlock()
			return
		default:
			s.cond.Wait()
		}
	}
	s.mu.Unlock()
	s.ds.Push(subID, event)
}

func (s *subscriptionClient) handleDSFeedBack(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case feedback := <-s.ds.Feedback():
			switch feedback.FeedbackType {
			case dynstream.PauseArea:
				s.mu.Lock()
				s.paused.Store(true)
				s.mu.Unlock()
				log.Info("subscription client pause push region event")
			case dynstream.ResumeArea:
				s.mu.Lock()
				s.paused.Store(false)
				s.cond.Broadcast()
				s.mu.Unlock()
				log.Info("subscription client resume push region event")
			case dynstream.ReleasePath, dynstream.ResumePath:
				// Ignore it, because it is no need to pause and resume a path in puller.
			}
		}
	}
}
