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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	observabilityMetricInterval = 10 * time.Second
	stalledSpanLogInterval      = 30 * time.Second

	stalledSpanThreshold      = 30 * time.Second
	stalledSpanLogSampleLimit = 8
)

// observabilityReporter owns all read-only reporting for subscriptionClient:
// periodic Prometheus metrics, periodic stalled-span logs, and on-demand debug
// snapshots. It reads state from the scheduler, worker runtime registry and
// subscribed spans, but does not drive puller state transitions.
type observabilityReporter struct {
	client *subscriptionClient
}

func newObservabilityReporter(client *subscriptionClient) *observabilityReporter {
	return &observabilityReporter{client: client}
}

func (r *observabilityReporter) run(ctx context.Context, g *errgroup.Group) {
	g.Go(func() error { return r.updateMetrics(ctx) })
	g.Go(func() error { return r.logStalledSpans(ctx) })
}

// phaseCountsToStrings converts internal regionPhase keys to string keys for
// API responses and structured logs. Keeping this conversion at the edge lets
// the runtime registry use typed phases internally.
func phaseCountsToStrings(counts map[regionPhase]int) map[string]int {
	result := make(map[string]int, len(counts))
	for phase, count := range counts {
		result[string(phase)] = count
	}
	return result
}

// trackedRegionCount derives the total number of runtime-tracked regions from
// per-phase counts, avoiding a second registry scan for snapshots and logs.
func trackedRegionCount(counts map[regionPhase]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}

// resolvedTsBlockerTypes defines the blocker order used by metrics, logs and
// snapshots. Iterating a fixed list avoids unstable map iteration order and
// keeps zero-count blocker types visible.
var resolvedTsBlockerTypes = []regionlock.ResolvedTsBlockerType{
	regionlock.ResolvedTsBlockerUnlockedRange,
	regionlock.ResolvedTsBlockerUninitializedRegion,
	regionlock.ResolvedTsBlockerInitializedRegion,
}

// blockerCountsToStrings converts range-lock blocker types to string keys for
// observability output while preserving the fixed blocker order above.
func blockerCountsToStrings(counts map[regionlock.ResolvedTsBlockerType]int) map[string]int {
	result := make(map[string]int, len(resolvedTsBlockerTypes))
	for _, blockerType := range resolvedTsBlockerTypes {
		result[string(blockerType)] = counts[blockerType]
	}
	return result
}

type stalledSpanReport struct {
	stalledSpanCount        int
	blockerCounts           map[regionlock.ResolvedTsBlockerType]int
	maxResolvedTsLag        time.Duration
	maxResolvedTsUpdatedAgo time.Duration
	samples                 []stalledSpanSample
}

type stalledSpanSample struct {
	SubscriptionID       uint64
	TableID              int64
	Span                 string
	Initialized          bool
	ResolvedTs           uint64
	ResolvedTsLag        time.Duration
	ResolvedTsUpdatedAgo time.Duration
	LockedRegionCount    int
	UnlockedRangeCount   int
	BlockedBy            []SpanResolvedTsBlockerSnapshot
}

func newStalledSpanReport() stalledSpanReport {
	return stalledSpanReport{
		blockerCounts: make(map[regionlock.ResolvedTsBlockerType]int, len(resolvedTsBlockerTypes)),
	}
}

func (r *stalledSpanReport) recordSpan(
	blockerStats regionlock.ResolvedTsBlockerStatistics,
	resolvedTsLag time.Duration,
	resolvedTsUpdatedAgo time.Duration,
) {
	r.stalledSpanCount++
	for _, blockerType := range resolvedTsBlockerTypes {
		if blockerStats.BlockerTypeCounts[blockerType] > 0 {
			r.blockerCounts[blockerType]++
		}
	}
	if resolvedTsLag > r.maxResolvedTsLag {
		r.maxResolvedTsLag = resolvedTsLag
	}
	if resolvedTsUpdatedAgo > r.maxResolvedTsUpdatedAgo {
		r.maxResolvedTsUpdatedAgo = resolvedTsUpdatedAgo
	}
}

func (r *stalledSpanReport) keepSample(sample stalledSpanSample, limit int) {
	if limit <= 0 {
		return
	}
	insertAt := len(r.samples)
	for i, existingSample := range r.samples {
		if sample.shouldAppearBefore(existingSample) {
			insertAt = i
			break
		}
	}
	if insertAt == len(r.samples) && len(r.samples) >= limit {
		return
	}
	if len(r.samples) < limit {
		r.samples = append(r.samples, stalledSpanSample{})
	}
	copy(r.samples[insertAt+1:], r.samples[insertAt:])
	r.samples[insertAt] = sample
	if len(r.samples) > limit {
		r.samples = r.samples[:limit]
	}
}

func (s stalledSpanSample) shouldAppearBefore(other stalledSpanSample) bool {
	if s.ResolvedTsLag != other.ResolvedTsLag {
		return s.ResolvedTsLag > other.ResolvedTsLag
	}
	if s.ResolvedTsUpdatedAgo != other.ResolvedTsUpdatedAgo {
		return s.ResolvedTsUpdatedAgo > other.ResolvedTsUpdatedAgo
	}
	if s.SubscriptionID != other.SubscriptionID {
		return s.SubscriptionID < other.SubscriptionID
	}
	if s.TableID != other.TableID {
		return s.TableID < other.TableID
	}
	return s.Span < other.Span
}

func convertStalledSpanSamples(samples []stalledSpanSample) []StalledSpanSnapshot {
	snapshots := make([]StalledSpanSnapshot, 0, len(samples))
	for _, sample := range samples {
		snapshots = append(snapshots, StalledSpanSnapshot{
			SubscriptionID:       sample.SubscriptionID,
			TableID:              sample.TableID,
			Span:                 sample.Span,
			Initialized:          sample.Initialized,
			ResolvedTs:           sample.ResolvedTs,
			ResolvedTsLag:        sample.ResolvedTsLag.String(),
			ResolvedTsUpdatedAgo: sample.ResolvedTsUpdatedAgo.String(),
			LockedRegionCount:    sample.LockedRegionCount,
			UnlockedRangeCount:   sample.UnlockedRangeCount,
			BlockedBy:            sample.BlockedBy,
		})
	}
	return snapshots
}

func (r *observabilityReporter) buildStalledSpanSample(
	now time.Time,
	entry subscribedSpanEntry,
	resolvedTs uint64,
	resolvedTsLag time.Duration,
	resolvedTsUpdatedAgo time.Duration,
	blockerStats regionlock.ResolvedTsBlockerStatistics,
) stalledSpanSample {
	s := r.client
	span := entry.span
	tableSpan := span.span
	sample := stalledSpanSample{
		SubscriptionID:       uint64(entry.subID),
		TableID:              tableSpan.TableID,
		Span:                 common.FormatTableSpan(&tableSpan),
		Initialized:          span.initialized.Load(),
		ResolvedTs:           resolvedTs,
		ResolvedTsLag:        resolvedTsLag,
		ResolvedTsUpdatedAgo: resolvedTsUpdatedAgo,
		LockedRegionCount:    blockerStats.LockedRegionCount,
		UnlockedRangeCount:   blockerStats.UnlockedRangeCount,
		BlockedBy:            make([]SpanResolvedTsBlockerSnapshot, 0, len(blockerStats.Blockers)),
	}

	for _, blocker := range blockerStats.Blockers {
		blockerSpan := blocker.Span
		blockerSpan.KeyspaceID = tableSpan.KeyspaceID
		blockerSpan.TableID = tableSpan.TableID

		blockerType := SpanResolvedTsBlockerType(blocker.Type)
		switch blocker.Type {
		case regionlock.ResolvedTsBlockerUnlockedRange:
			blockerType = SpanResolvedTsBlockerUnlockedRange
		case regionlock.ResolvedTsBlockerUninitializedRegion:
			blockerType = SpanResolvedTsBlockerUninitializedRegion
		case regionlock.ResolvedTsBlockerInitializedRegion:
			blockerType = SpanResolvedTsBlockerInitializedRegion
		}

		createdAgo := ""
		if !blocker.Created.IsZero() {
			createdAgo = normalizeDuration(now.Sub(blocker.Created)).String()
		}
		blockerSnapshot := SpanResolvedTsBlockerSnapshot{
			Type:       blockerType,
			RegionID:   blocker.RegionID,
			Span:       common.FormatTableSpan(&blockerSpan),
			ResolvedTs: blocker.ResolvedTs,
			CreatedAgo: createdAgo,
		}

		switch blocker.Type {
		case regionlock.ResolvedTsBlockerUninitializedRegion,
			regionlock.ResolvedTsBlockerInitializedRegion:
			initialized := blocker.Initialized
			blockerSnapshot.Initialized = &initialized
			if runtimeState, ok := s.regionRuntimeRegistry.getLatest(entry.subID, blocker.RegionID); ok {
				phase := runtimeState.phase
				if phase == "" {
					phase = regionPhaseUnknown
				}
				lastEventAgo := ""
				if !runtimeState.lastEventTime.IsZero() {
					lastEventAgo = normalizeDuration(now.Sub(runtimeState.lastEventTime)).String()
				}
				runtimeSnapshot := RegionRuntimeBlockerSnapshot{
					Phase:        string(phase),
					PhaseAge:     runtimeState.phaseAge(now).String(),
					LastEventAgo: lastEventAgo,
					StoreAddr:    runtimeState.storeAddr,
					WorkerID:     runtimeState.workerID,
					LastError:    runtimeState.lastError,
				}
				blockerSnapshot.Runtime = &runtimeSnapshot
			}
		}
		sample.BlockedBy = append(sample.BlockedBy, blockerSnapshot)
	}
	return sample
}

// collectStalledSpanReport reports a span as stalled when its resolved-ts lag
// reaches stalledSpanThreshold. `sampleLimit` controls how many stalled spans
// are retained for logs / API snapshots; `blockerLimit` controls how many
// blockers are collected from each span's range lock.
func (r *observabilityReporter) collectStalledSpanReport(
	now time.Time,
	sampleLimit int,
	blockerLimit int,
) stalledSpanReport {
	s := r.client
	report := newStalledSpanReport()
	for _, entry := range s.subscribedSpans.snapshot() {
		if entry.span == nil {
			continue
		}
		span := entry.span
		resolvedTs := span.resolvedTs.Load()
		if resolvedTs == 0 {
			continue
		}
		resolvedTsLag := normalizeDuration(now.Sub(oracle.GetTimeFromTS(resolvedTs)))
		if resolvedTsLag < stalledSpanThreshold {
			continue
		}

		resolvedTsUpdatedAgo := time.Duration(0)
		if resolvedTsUpdatedUnix := span.resolvedTsUpdated.Load(); resolvedTsUpdatedUnix > 0 {
			resolvedTsUpdatedAgo = normalizeDuration(now.Sub(time.Unix(resolvedTsUpdatedUnix, 0)))
		}

		blockerStats := span.rangeLock.CollectResolvedTsBlockers(blockerLimit)
		report.recordSpan(blockerStats, resolvedTsLag, resolvedTsUpdatedAgo)
		if sampleLimit > 0 {
			report.keepSample(
				r.buildStalledSpanSample(now, entry, resolvedTs, resolvedTsLag, resolvedTsUpdatedAgo, blockerStats),
				sampleLimit,
			)
		}
	}
	return report
}

func (r *observabilityReporter) runtimeObservability(now time.Time, sampleLimit int) RuntimeObservability {
	s := r.client
	phaseCounts := s.regionRuntimeRegistry.phaseCounts()
	stalledSpanReport := r.collectStalledSpanReport(now, sampleLimit, sampleLimit)

	return RuntimeObservability{
		TrackedRegionCount: trackedRegionCount(phaseCounts),
		PhaseCounts:        phaseCountsToStrings(phaseCounts),
		StalledSpanCount:   stalledSpanReport.stalledSpanCount,
		StalledSpans:       convertStalledSpanSamples(stalledSpanReport.samples),
	}
}

func (s *subscriptionClient) GetObservabilitySnapshot(sampleLimit int) ObservabilitySnapshot {
	return s.observability.snapshot(sampleLimit)
}

func (r *observabilityReporter) snapshot(sampleLimit int) ObservabilitySnapshot {
	s := r.client
	sampleLimit = normalizeObservabilitySampleLimit(sampleLimit)
	now := s.pdClock.CurrentTime()
	return ObservabilitySnapshot{
		GeneratedAt: now,
		Runtime:     r.runtimeObservability(now, sampleLimit),
		Stores:      s.requestedStores.snapshotStores(),
	}
}

func (r *observabilityReporter) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(observabilityMetricInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.updatePrometheusMetrics()
		}
	}
}

func (r *observabilityReporter) updatePrometheusMetrics() {
	s := r.client
	if resolvedTsLag := s.subscribedSpans.getResolvedTsLag(); resolvedTsLag > 0 {
		metrics.LogPullerResolvedTsLag.Set(resolvedTsLag)
	}

	dsMetrics := s.ds.GetMetrics()
	metricSubscriptionClientDSChannelSize.Set(float64(dsMetrics.EventChanSize))
	metricSubscriptionClientDSPendingQueueLen.Set(float64(dsMetrics.PendingQueueLen))
	if len(dsMetrics.MemoryControl.AreaMemoryMetrics) > 1 {
		log.Panic("subscription client should have only one area")
	}
	if len(dsMetrics.MemoryControl.AreaMemoryMetrics) > 0 {
		areaMetric := dsMetrics.MemoryControl.AreaMemoryMetrics[0]
		metrics.DynamicStreamMemoryUsage.WithLabelValues(
			"log-puller",
			"max",
			"default",
			"default",
		).Set(float64(areaMetric.MaxMemory()))
		metrics.DynamicStreamMemoryUsage.WithLabelValues(
			"log-puller",
			"used",
			"default",
			"default",
		).Set(float64(areaMetric.MemoryUsage()))
	}

	requestStats := s.requestedStores.requestStats()
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Set(float64(requestStats.Total))
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("queued").Set(float64(requestStats.Queued))
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("processing").Set(float64(requestStats.Processing))
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("sent").Set(float64(requestStats.Sent))

	counts := s.regionRuntimeRegistry.phaseCounts()
	for _, phase := range regionRuntimePhases {
		metrics.SubscriptionClientRegionRuntimePhaseCount.WithLabelValues(string(phase)).
			Set(float64(counts[phase]))
	}

	report := r.collectStalledSpanReport(s.pdClock.CurrentTime(), 0, -1)
	metrics.SubscriptionClientStalledSpanCount.Set(float64(report.stalledSpanCount))
	for _, blockerType := range resolvedTsBlockerTypes {
		metrics.SubscriptionClientStalledSpanCountByBlockerType.WithLabelValues(string(blockerType)).
			Set(float64(report.blockerCounts[blockerType]))
	}
	metrics.SubscriptionClientStalledSpanMaxResolvedTsLag.Set(report.maxResolvedTsLag.Seconds())
	metrics.SubscriptionClientStalledSpanMaxResolvedTsUpdatedAge.Set(report.maxResolvedTsUpdatedAgo.Seconds())
	metrics.SubscriptionClientSubscribedRegionCount.Set(float64(s.subscribedSpans.requestedRegionCount()))
}

func (r *observabilityReporter) logStalledSpans(ctx context.Context) error {
	s := r.client
	ticker := time.NewTicker(stalledSpanLogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		now := s.pdClock.CurrentTime()
		report := r.collectStalledSpanReport(
			now,
			stalledSpanLogSampleLimit,
			stalledSpanLogSampleLimit,
		)
		if report.stalledSpanCount == 0 {
			continue
		}

		phaseCounts := s.regionRuntimeRegistry.phaseCounts()
		log.Info("subscription client stalled span summary",
			zap.Int("trackedRegionCount", trackedRegionCount(phaseCounts)),
			zap.Any("phaseCounts", phaseCountsToStrings(phaseCounts)),
			zap.String("stalledSpanThreshold", stalledSpanThreshold.String()),
			zap.Int("stalledSpanCount", report.stalledSpanCount),
			zap.Duration("maxResolvedTsLag", report.maxResolvedTsLag),
			zap.Duration("maxResolvedTsUpdatedAgo", report.maxResolvedTsUpdatedAgo),
			zap.Any("blockerCounts", blockerCountsToStrings(report.blockerCounts)),
			zap.Any("samples", convertStalledSpanSamples(report.samples)))
	}
}
