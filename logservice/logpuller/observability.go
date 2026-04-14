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
	"sort"
	"sync"
	"time"
)

const (
	defaultObservabilitySampleLimit = 8
	maxObservabilitySampleLimit     = 64
)

type WorkerSessionState string

const (
	WorkerSessionStateDisconnected     WorkerSessionState = "disconnected"
	WorkerSessionStateWaitingBootstrap WorkerSessionState = "waiting_bootstrap"
	WorkerSessionStateCheckingStore    WorkerSessionState = "checking_store"
	WorkerSessionStateConnecting       WorkerSessionState = "connecting"
	WorkerSessionStateRunning          WorkerSessionState = "running"
)

type RequestCacheSnapshot struct {
	Total      int `json:"total"`
	Queued     int `json:"queued"`
	Processing int `json:"processing"`
	Sent       int `json:"sent"`
}

type SlowRegionSnapshot struct {
	SubscriptionID uint64 `json:"subscription_id"`
	RegionID       uint64 `json:"region_id"`
	Phase          string `json:"phase"`
	StuckFor       string `json:"stuck_for"`
	StoreAddr      string `json:"store_addr,omitempty"`
	WorkerID       uint64 `json:"worker_id,omitempty"`
	LastError      string `json:"last_error,omitempty"`
	Span           string `json:"span,omitempty"`
}

type UnlockedRangeSnapshot struct {
	SubscriptionID uint64   `json:"subscription_id"`
	TableID        int64    `json:"table_id"`
	HoleCount      int      `json:"hole_count"`
	Holes          []string `json:"holes,omitempty"`
}

type RuntimeObservability struct {
	TrackedRegionCount            int                     `json:"tracked_region_count"`
	PhaseCounts                   map[string]int          `json:"phase_counts"`
	SlowRegionCount               int                     `json:"slow_region_count"`
	SlowRegionCountsByPhase       map[string]int          `json:"slow_region_counts_by_phase"`
	SlowRegions                   []SlowRegionSnapshot    `json:"slow_regions,omitempty"`
	SubscriptionWithUnlockedRange int                     `json:"subscription_with_unlocked_range"`
	UnlockedRangeCount            int                     `json:"unlocked_range_count"`
	UnlockedRanges                []UnlockedRangeSnapshot `json:"unlocked_ranges,omitempty"`
}

type WorkerObservability struct {
	WorkerID          uint64               `json:"worker_id"`
	SessionState      WorkerSessionState   `json:"session_state"`
	ActiveRegionCount int                  `json:"active_region_count"`
	RequestCache      RequestCacheSnapshot `json:"request_cache"`
}

type StoreObservability struct {
	StoreAddr         string                `json:"store_addr"`
	WorkerCount       int                   `json:"worker_count"`
	ActiveRegionCount int                   `json:"active_region_count"`
	RequestCache      RequestCacheSnapshot  `json:"request_cache"`
	Workers           []WorkerObservability `json:"workers,omitempty"`
}

type FailureSnapshot struct {
	Scope     string    `json:"scope"`
	Source    string    `json:"source"`
	Kind      string    `json:"kind"`
	Count     uint64    `json:"count"`
	LastAt    time.Time `json:"last_at"`
	LastError string    `json:"last_error,omitempty"`
}

type ObservabilitySnapshot struct {
	GeneratedAt time.Time            `json:"generated_at"`
	Runtime     RuntimeObservability `json:"runtime"`
	Stores      []StoreObservability `json:"stores,omitempty"`
	Failures    []FailureSnapshot    `json:"failures,omitempty"`
}

type failureCounterKey struct {
	scope  regionFailureScope
	source regionFailureSource
	kind   regionFailureKind
}

type failureCounterState struct {
	count     uint64
	lastAt    time.Time
	lastError string
}

type failureStats struct {
	mu     sync.RWMutex
	counts map[failureCounterKey]failureCounterState
}

func newFailureStats() *failureStats {
	return &failureStats{
		counts: make(map[failureCounterKey]failureCounterState),
	}
}

func (s *failureStats) record(failure regionFailureInfo, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := failureCounterKey{
		scope:  failure.scope,
		source: failure.source,
		kind:   failure.kind,
	}
	state := s.counts[key]
	state.count++
	state.lastAt = now
	if failure.err != nil {
		state.lastError = failure.err.Error()
	} else {
		state.lastError = ""
	}
	s.counts[key] = state
}

func (s *failureStats) snapshot() []FailureSnapshot {
	s.mu.RLock()
	snapshots := make([]FailureSnapshot, 0, len(s.counts))
	for key, state := range s.counts {
		snapshots = append(snapshots, FailureSnapshot{
			Scope:     key.scope.String(),
			Source:    key.source.String(),
			Kind:      key.kind.String(),
			Count:     state.count,
			LastAt:    state.lastAt,
			LastError: state.lastError,
		})
	}
	s.mu.RUnlock()

	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].Count != snapshots[j].Count {
			return snapshots[i].Count > snapshots[j].Count
		}
		if snapshots[i].Scope != snapshots[j].Scope {
			return snapshots[i].Scope < snapshots[j].Scope
		}
		if snapshots[i].Source != snapshots[j].Source {
			return snapshots[i].Source < snapshots[j].Source
		}
		return snapshots[i].Kind < snapshots[j].Kind
	})
	return snapshots
}

func normalizeObservabilitySampleLimit(sampleLimit int) int {
	if sampleLimit <= 0 {
		return defaultObservabilitySampleLimit
	}
	if sampleLimit > maxObservabilitySampleLimit {
		return maxObservabilitySampleLimit
	}
	return sampleLimit
}

func requestCacheSnapshotAdd(dst *RequestCacheSnapshot, src RequestCacheSnapshot) {
	dst.Total += src.Total
	dst.Queued += src.Queued
	dst.Processing += src.Processing
	dst.Sent += src.Sent
}
