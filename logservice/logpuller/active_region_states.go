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

import "sync"

// activeRegionStates tracks region feed states that have already been started
// in one worker session and are still waiting for more events or cleanup.
type activeRegionStates struct {
	mu            sync.RWMutex
	subscriptions map[SubscriptionID]regionFeedStates
	count         int
}

func newActiveRegionStates() activeRegionStates {
	return activeRegionStates{
		subscriptions: make(map[SubscriptionID]regionFeedStates),
	}
}

func (s *activeRegionStates) add(subscriptionID SubscriptionID, regionID uint64, state *regionFeedState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	states := s.subscriptions[subscriptionID]
	if states == nil {
		states = make(regionFeedStates)
		s.subscriptions[subscriptionID] = states
	}
	if _, exists := states[regionID]; !exists {
		s.count++
	}
	states[regionID] = state
}

func (s *activeRegionStates) get(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if states, ok := s.subscriptions[subscriptionID]; ok {
		return states[regionID]
	}
	return nil
}

func (s *activeRegionStates) take(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	s.mu.Lock()
	defer s.mu.Unlock()

	if statesMap, ok := s.subscriptions[subscriptionID]; ok {
		state := statesMap[regionID]
		if state != nil {
			delete(statesMap, regionID)
			s.count--
			if len(statesMap) == 0 {
				delete(s.subscriptions, subscriptionID)
			}
		}
		return state
	}
	return nil
}

func (s *activeRegionStates) takeSubscription(subscriptionID SubscriptionID) regionFeedStates {
	s.mu.Lock()
	defer s.mu.Unlock()

	states := s.subscriptions[subscriptionID]
	s.count -= len(states)
	delete(s.subscriptions, subscriptionID)
	return states
}

func (s *activeRegionStates) takeAll() map[SubscriptionID]regionFeedStates {
	s.mu.Lock()
	defer s.mu.Unlock()

	states := s.subscriptions
	s.subscriptions = make(map[SubscriptionID]regionFeedStates)
	s.count = 0
	return states
}

func (s *activeRegionStates) countActive() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}
