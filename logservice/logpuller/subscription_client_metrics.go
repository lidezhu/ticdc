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
	"github.com/pingcap/ticdc/pkg/metrics"
)

func (s *subscriptionClient) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			resolvedTsLag := s.subscribedSpans.getResolvedTsLag()
			if resolvedTsLag > 0 {
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

			counts := s.regionRuntimePhaseCounts()
			for _, phase := range regionRuntimePhases {
				metrics.SubscriptionClientRegionRuntimePhaseCount.WithLabelValues(string(phase)).Set(float64(counts[phase]))
			}
			stalledSpanReport := s.collectStalledSpanReport(s.pdClock.CurrentTime(), 0, -1)
			metrics.SubscriptionClientStalledSpanCount.Set(float64(stalledSpanReport.stalledSpanCount))
			for _, blockerType := range resolvedTsBlockerTypes {
				metrics.SubscriptionClientStalledSpanCountByBlockerType.WithLabelValues(string(blockerType)).
					Set(float64(stalledSpanReport.blockerCounts[blockerType]))
			}
			metrics.SubscriptionClientStalledSpanMaxResolvedTsLag.Set(stalledSpanReport.maxResolvedTsLag.Seconds())
			metrics.SubscriptionClientStalledSpanMaxResolvedTsUpdatedAge.Set(stalledSpanReport.maxResolvedTsUpdatedAgo.Seconds())

			metrics.SubscriptionClientSubscribedRegionCount.Set(float64(s.subscribedSpans.requestedRegionCount()))
		}
	}
}
