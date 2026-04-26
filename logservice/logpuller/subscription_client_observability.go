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
	"sort"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
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

// spanStallDurations returns whether a subscription is considered stalled.
// A span is stalled only when resolved-ts is both stale and far behind PD time,
// so recently updated spans with old resolved-ts are not reported too early.
func spanStallDurations(
	now time.Time,
	span *subscribedSpan,
) (resolvedTsUpdatedAgo time.Duration, resolvedTsLag time.Duration, stalled bool) {
	resolvedTsUpdatedUnix := span.resolvedTsUpdated.Load()
	resolvedTs := span.resolvedTs.Load()
	if resolvedTsUpdatedUnix == 0 || resolvedTs == 0 {
		return 0, 0, false
	}

	resolvedTsUpdatedAgo = normalizeDuration(now.Sub(time.Unix(resolvedTsUpdatedUnix, 0)))
	resolvedTsLag = normalizeDuration(now.Sub(oracle.GetTimeFromTS(resolvedTs)))
	return resolvedTsUpdatedAgo, resolvedTsLag,
		resolvedTsUpdatedAgo >= stalledSpanThreshold && resolvedTsLag >= stalledSpanThreshold
}

func betterStalledSpanSample(left, right stalledSpanSample) bool {
	if left.ResolvedTsUpdatedAgo != right.ResolvedTsUpdatedAgo {
		return left.ResolvedTsUpdatedAgo > right.ResolvedTsUpdatedAgo
	}
	if left.ResolvedTsLag != right.ResolvedTsLag {
		return left.ResolvedTsLag > right.ResolvedTsLag
	}
	if left.SubscriptionID != right.SubscriptionID {
		return left.SubscriptionID < right.SubscriptionID
	}
	if left.TableID != right.TableID {
		return left.TableID < right.TableID
	}
	return left.Span < right.Span
}

// insertStalledSpanSample keeps only the worst stalled spans. Samples are
// ordered by stale update time, then resolved-ts lag, then stable identifiers so
// repeated snapshots are deterministic.
func insertStalledSpanSample(
	samples []stalledSpanSample,
	sample stalledSpanSample,
	limit int,
) []stalledSpanSample {
	if limit <= 0 {
		return samples
	}
	originalLen := len(samples)
	if originalLen == limit && !betterStalledSpanSample(sample, samples[originalLen-1]) {
		return samples
	}

	index := sort.Search(originalLen, func(i int) bool {
		return betterStalledSpanSample(sample, samples[i])
	})
	if originalLen < limit {
		samples = append(samples, stalledSpanSample{})
	} else {
		if index >= limit {
			index = limit - 1
		}
	}
	copy(samples[index+1:], samples[index:])
	samples[index] = sample
	if len(samples) > limit {
		samples = samples[:limit]
	}
	return samples
}

func spanResolvedTsBlockerType(
	blockerType regionlock.ResolvedTsBlockerType,
) SpanResolvedTsBlockerType {
	switch blockerType {
	case regionlock.ResolvedTsBlockerUnlockedRange:
		return SpanResolvedTsBlockerUnlockedRange
	case regionlock.ResolvedTsBlockerUninitializedRegion:
		return SpanResolvedTsBlockerUninitializedRegion
	case regionlock.ResolvedTsBlockerInitializedRegion:
		return SpanResolvedTsBlockerInitializedRegion
	default:
		return SpanResolvedTsBlockerType(blockerType)
	}
}

func durationSinceString(now time.Time, timestamp time.Time) string {
	if timestamp.IsZero() {
		return ""
	}
	return normalizeDuration(now.Sub(timestamp)).String()
}

func convertRegionRuntimeBlocker(
	now time.Time,
	state regionRuntimeState,
) RegionRuntimeBlockerSnapshot {
	phase := state.phase
	if phase == "" {
		phase = regionPhaseUnknown
	}
	return RegionRuntimeBlockerSnapshot{
		Phase:        string(phase),
		PhaseAge:     state.phaseAge(now).String(),
		LastEventAgo: durationSinceString(now, state.lastEventTime),
		StoreAddr:    state.storeAddr,
		WorkerID:     state.workerID,
		LastError:    state.lastError,
	}
}

func (r *observabilityReporter) convertResolvedTsBlocker(
	now time.Time,
	subID SubscriptionID,
	parentSpan heartbeatpb.TableSpan,
	blocker regionlock.ResolvedTsBlocker,
) SpanResolvedTsBlockerSnapshot {
	s := r.client
	blockerSpan := blocker.Span
	blockerSpan.KeyspaceID = parentSpan.KeyspaceID
	blockerSpan.TableID = parentSpan.TableID
	snapshot := SpanResolvedTsBlockerSnapshot{
		Type:       spanResolvedTsBlockerType(blocker.Type),
		RegionID:   blocker.RegionID,
		Span:       common.FormatTableSpan(&blockerSpan),
		ResolvedTs: blocker.ResolvedTs,
		CreatedAgo: durationSinceString(now, blocker.Created),
	}

	switch blocker.Type {
	case regionlock.ResolvedTsBlockerUninitializedRegion,
		regionlock.ResolvedTsBlockerInitializedRegion:
		initialized := blocker.Initialized
		snapshot.Initialized = &initialized
		if runtimeState, ok := s.regionRuntimeRegistry.getLatest(subID, blocker.RegionID); ok {
			runtimeSnapshot := convertRegionRuntimeBlocker(now, runtimeState)
			snapshot.Runtime = &runtimeSnapshot
		}
	}
	return snapshot
}

func (r *observabilityReporter) makeStalledSpanSample(
	now time.Time,
	entry subscribedSpanEntry,
	resolvedTsUpdatedAgo time.Duration,
	resolvedTsLag time.Duration,
	blockerStats regionlock.ResolvedTsBlockerStatistics,
) stalledSpanSample {
	blockers := make([]SpanResolvedTsBlockerSnapshot, 0, len(blockerStats.Blockers))
	for _, blocker := range blockerStats.Blockers {
		blockers = append(blockers,
			r.convertResolvedTsBlocker(now, entry.subID, entry.span.span, blocker))
	}

	span := entry.span.span
	return stalledSpanSample{
		SubscriptionID:       uint64(entry.subID),
		TableID:              span.TableID,
		Span:                 common.FormatTableSpan(&span),
		Initialized:          entry.span.initialized.Load(),
		ResolvedTs:           entry.span.resolvedTs.Load(),
		ResolvedTsLag:        resolvedTsLag,
		ResolvedTsUpdatedAgo: resolvedTsUpdatedAgo,
		LockedRegionCount:    blockerStats.LockedRegionCount,
		UnlockedRangeCount:   blockerStats.UnlockedRangeCount,
		BlockedBy:            blockers,
	}
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

// collectStalledSpanReport walks subscribed spans and summarizes why their
// resolved-ts cannot advance. `sampleLimit` controls how many stalled spans are
// retained for logs / API snapshots; `blockerLimit` controls how many blockers
// are collected from each span's range lock.
func (r *observabilityReporter) collectStalledSpanReport(
	now time.Time,
	sampleLimit int,
	blockerLimit int,
) stalledSpanReport {
	s := r.client
	report := stalledSpanReport{
		blockerCounts: make(map[regionlock.ResolvedTsBlockerType]int, len(resolvedTsBlockerTypes)),
	}
	for _, entry := range s.subscribedSpans.snapshot() {
		if entry.span == nil {
			continue
		}
		resolvedTsUpdatedAgo, resolvedTsLag, stalled := spanStallDurations(now, entry.span)
		if !stalled {
			continue
		}

		blockerStats := entry.span.rangeLock.CollectResolvedTsBlockers(blockerLimit)
		report.stalledSpanCount++
		for _, blockerType := range resolvedTsBlockerTypes {
			if blockerStats.BlockerTypeCounts[blockerType] > 0 {
				report.blockerCounts[blockerType]++
			}
		}
		if resolvedTsLag > report.maxResolvedTsLag {
			report.maxResolvedTsLag = resolvedTsLag
		}
		if resolvedTsUpdatedAgo > report.maxResolvedTsUpdatedAgo {
			report.maxResolvedTsUpdatedAgo = resolvedTsUpdatedAgo
		}
		if sampleLimit <= 0 {
			continue
		}

		report.samples = insertStalledSpanSample(
			report.samples,
			r.makeStalledSpanSample(now, entry, resolvedTsUpdatedAgo, resolvedTsLag, blockerStats),
			sampleLimit,
		)
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
	r.updateResolvedTsLagMetric()
	r.updateDynamicStreamMetrics()
	r.updateRequestCacheMetrics()
	r.updateRuntimePhaseMetrics()
	r.updateStalledSpanMetrics(s.pdClock.CurrentTime())
	metrics.SubscriptionClientSubscribedRegionCount.Set(float64(s.subscribedSpans.requestedRegionCount()))
}

func (r *observabilityReporter) updateResolvedTsLagMetric() {
	resolvedTsLag := r.client.subscribedSpans.getResolvedTsLag()
	if resolvedTsLag > 0 {
		metrics.LogPullerResolvedTsLag.Set(resolvedTsLag)
	}
}

func (r *observabilityReporter) updateDynamicStreamMetrics() {
	dsMetrics := r.client.ds.GetMetrics()
	metricSubscriptionClientDSChannelSize.Set(float64(dsMetrics.EventChanSize))
	metricSubscriptionClientDSPendingQueueLen.Set(float64(dsMetrics.PendingQueueLen))
	if len(dsMetrics.MemoryControl.AreaMemoryMetrics) > 1 {
		log.Panic("subscription client should have only one area")
	}
	if len(dsMetrics.MemoryControl.AreaMemoryMetrics) == 0 {
		return
	}

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

func (r *observabilityReporter) updateRequestCacheMetrics() {
	requestStats := r.client.requestedStores.requestStats()
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Set(float64(requestStats.Total))
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("queued").Set(float64(requestStats.Queued))
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("processing").Set(float64(requestStats.Processing))
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("sent").Set(float64(requestStats.Sent))
}

func (r *observabilityReporter) updateRuntimePhaseMetrics() {
	counts := r.client.regionRuntimeRegistry.phaseCounts()
	for _, phase := range regionRuntimePhases {
		metrics.SubscriptionClientRegionRuntimePhaseCount.WithLabelValues(string(phase)).
			Set(float64(counts[phase]))
	}
}

func (r *observabilityReporter) updateStalledSpanMetrics(now time.Time) {
	report := r.collectStalledSpanReport(now, 0, -1)
	metrics.SubscriptionClientStalledSpanCount.Set(float64(report.stalledSpanCount))
	for _, blockerType := range resolvedTsBlockerTypes {
		metrics.SubscriptionClientStalledSpanCountByBlockerType.WithLabelValues(string(blockerType)).
			Set(float64(report.blockerCounts[blockerType]))
	}
	metrics.SubscriptionClientStalledSpanMaxResolvedTsLag.Set(report.maxResolvedTsLag.Seconds())
	metrics.SubscriptionClientStalledSpanMaxResolvedTsUpdatedAge.Set(report.maxResolvedTsUpdatedAgo.Seconds())
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
