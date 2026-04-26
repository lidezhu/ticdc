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

type SpanResolvedTsBlockerType string

const (
	SpanResolvedTsBlockerUnlockedRange       SpanResolvedTsBlockerType = "unlocked_range"
	SpanResolvedTsBlockerUninitializedRegion SpanResolvedTsBlockerType = "uninitialized_region"
	SpanResolvedTsBlockerInitializedRegion   SpanResolvedTsBlockerType = "initialized_region"
)

type RegionRuntimeBlockerSnapshot struct {
	Phase        string `json:"phase,omitempty"`
	PhaseAge     string `json:"phase_age,omitempty"`
	LastEventAgo string `json:"last_event_ago,omitempty"`
	StoreAddr    string `json:"store_addr,omitempty"`
	WorkerID     uint64 `json:"worker_id,omitempty"`
	LastError    string `json:"last_error,omitempty"`
}

type SpanResolvedTsBlockerSnapshot struct {
	Type        SpanResolvedTsBlockerType     `json:"type"`
	RegionID    uint64                        `json:"region_id,omitempty"`
	Span        string                        `json:"span,omitempty"`
	ResolvedTs  uint64                        `json:"resolved_ts"`
	Initialized *bool                         `json:"initialized,omitempty"`
	CreatedAgo  string                        `json:"created_ago,omitempty"`
	Runtime     *RegionRuntimeBlockerSnapshot `json:"runtime,omitempty"`
}

type StalledSpanSnapshot struct {
	SubscriptionID       uint64                          `json:"subscription_id"`
	TableID              int64                           `json:"table_id"`
	Span                 string                          `json:"span"`
	Initialized          bool                            `json:"initialized"`
	ResolvedTs           uint64                          `json:"resolved_ts"`
	ResolvedTsLag        string                          `json:"resolved_ts_lag"`
	ResolvedTsUpdatedAgo string                          `json:"resolved_ts_updated_ago"`
	LockedRegionCount    int                             `json:"locked_region_count"`
	UnlockedRangeCount   int                             `json:"unlocked_range_count"`
	BlockedBy            []SpanResolvedTsBlockerSnapshot `json:"blocked_by,omitempty"`
}

type RuntimeObservability struct {
	TrackedRegionCount int                   `json:"tracked_region_count"`
	PhaseCounts        map[string]int        `json:"phase_counts"`
	StalledSpanCount   int                   `json:"stalled_span_count"`
	StalledSpans       []StalledSpanSnapshot `json:"stalled_spans,omitempty"`
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

type ObservabilitySnapshot struct {
	GeneratedAt time.Time            `json:"generated_at"`
	Runtime     RuntimeObservability `json:"runtime"`
	Stores      []StoreObservability `json:"stores,omitempty"`
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
