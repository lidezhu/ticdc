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
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestSpanRegistryUpdateMetrics(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	registry := newSpanRegistry(nil, pdutil.NewClockWithValue4Test(now))

	span1 := &subscribedSpan{
		subID:     SubscriptionID(1),
		rangeLock: regionlock.NewRangeLock(1, []byte("a"), []byte("m"), 100),
		span:      heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("m")},
	}
	span1.resolvedTs.Store(oracle.GoTimeToTS(now.Add(-5 * time.Second)))

	span2 := &subscribedSpan{
		subID:     SubscriptionID(2),
		rangeLock: regionlock.NewRangeLock(2, []byte("m"), []byte("z"), 100),
		span:      heartbeatpb.TableSpan{TableID: 2, StartKey: []byte("m"), EndKey: []byte("z")},
	}
	span2.resolvedTs.Store(oracle.GoTimeToTS(now.Add(-2 * time.Second)))

	lockRange := func(t *testing.T, span *subscribedSpan, start, end string, regionID uint64) {
		t.Helper()
		res := span.rangeLock.LockRange(context.Background(), []byte(start), []byte(end), regionID, 100)
		require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	}

	lockRange(t, span1, "a", "g", 11)
	lockRange(t, span1, "g", "m", 12)
	lockRange(t, span2, "m", "z", 21)

	registry.Add(span1)
	registry.Add(span2)

	registry.UpdateMetrics()

	require.Equal(t, float64(3), testutil.ToFloat64(subscriptionClientSubscribedRegionCount))
	require.InDelta(t, 5.0, testutil.ToFloat64(logPullerResolvedTsLag), 0.001)
}

func TestSlowRegionLogsUseBoundedSummaries(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := pdutil.NewClockWithValue4Test(now)
	registry := newSpanRegistry(nil, clock)

	for i := range slowRegionLogSampleSize + 2 {
		subID := SubscriptionID(i + 1)
		span := newDebugTestSpan(subID, int64(i+1), now.Add(-2*time.Hour), clock)
		state := lockDebugTestRegion(t, span, "a", "z", uint64(i+1), now.Add(-2*time.Hour), true)
		state.Created = now.Add(-3 * time.Hour)
		registry.Add(span)
	}

	initializing := newDebugTestSpan(100, 100, now.Add(-2*time.Hour), clock)
	state := lockDebugTestRegion(t, initializing, "a", "z", 100, now.Add(-2*time.Hour), false)
	state.Created = now.Add(-11 * time.Minute)
	registry.Add(initializing)

	uninitialized := newDebugTestSpan(101, 101, now.Add(-2*time.Hour), clock)
	state = lockDebugTestRegion(t, uninitialized, "a", "z", 101, now.Add(-2*time.Hour), false)
	state.Created = now.Add(-time.Minute)
	registry.Add(uninitialized)

	withHole := newDebugTestSpan(102, 102, now, clock)
	state = lockDebugTestRegion(t, withHole, "a", "m", 102, now, true)
	state.Created = now
	registry.Add(withHole)

	stopped := newDebugTestSpan(103, 103, now.Add(-2*time.Hour), clock)
	state = lockDebugTestRegion(t, stopped, "a", "z", 103, now.Add(-2*time.Hour), true)
	state.Created = now.Add(-3 * time.Hour)
	stopped.stopped.Store(true)
	registry.Add(stopped)

	stats := registry.collectSlowRegionLogStats(now)
	require.Equal(t, slowRegionLogSampleSize+2, stats.initialized.count)
	require.Len(t, stats.initialized.sampleRegionIDs, slowRegionLogSampleSize)
	require.Equal(t, 1, stats.initializing.count)
	require.Equal(t, 11*time.Minute, stats.initializing.maxDuration)
	require.Equal(t, 1, stats.uninitialized.count)
	require.Equal(t, 2*time.Hour, stats.uninitialized.maxDuration)
	require.Equal(t, 1, stats.holes.subscriptionCount)
	require.Equal(t, 1, stats.holes.holeCount)
	require.Equal(t, 1, stats.holes.maxHoleCount)

	core, observedLogs := observer.New(zapcore.InfoLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	t.Cleanup(restore)
	stats.log()

	initializedLogs := observedLogs.FilterMessage("subscription client finds initialized slow regions").All()
	require.Len(t, initializedLogs, 1)
	require.EqualValues(t, slowRegionLogSampleSize+2, initializedLogs[0].ContextMap()["regionCount"])
	samples, ok := initializedLogs[0].ContextMap()["sampleRegionIDs"].([]any)
	require.True(t, ok)
	require.Len(t, samples, slowRegionLogSampleSize)
	require.Len(t, observedLogs.FilterMessage("subscription client initializes regions too slowly").All(), 1)
	require.Len(t, observedLogs.FilterMessage("subscription client finds uninitialized slow regions").All(), 1)
	require.Len(t, observedLogs.FilterMessage("subscription client finds holes").All(), 1)
}
