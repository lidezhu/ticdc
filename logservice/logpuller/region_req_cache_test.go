// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func createTestRegionInfo(subID SubscriptionID, regionID uint64) regionInfo {
	verID := tikv.NewRegionVerID(regionID, 1, 1)

	span := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("start"),
		EndKey:   []byte("end"),
	}

	subscribedSpan := &subscribedSpan{
		subID:   subID,
		startTs: 100,
		span:    span,
	}

	region := newRegionInfo(verID, span, nil, subscribedSpan, false)
	region.lockedRangeState = &regionlock.LockedRangeState{}
	return region
}

func TestRequestCacheAddNormalCase(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()
	region := createTestRegionInfo(1, 1)

	ok, err := cache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, cache.getPendingCount())

	req, err := cache.pop(ctx)
	require.NoError(t, err)
	require.Equal(t, region.verID.GetID(), req.regionInfo.verID.GetID())
	require.Equal(t, regionReqStageProcessing, req.stage)
}

func TestRequestCacheForceBypassesWindowLimit(t *testing.T) {
	cache := newRequestCache(1)
	ctx := context.Background()

	region1 := createTestRegionInfo(1, 1)
	ok, err := cache.add(ctx, region1, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, cache.getPendingCount())

	region2 := createTestRegionInfo(1, 2)
	ok, err = cache.add(ctx, region2, false)
	require.NoError(t, err)
	require.False(t, ok)

	region3 := createTestRegionInfo(1, 3)
	ok, err = cache.add(ctx, region3, true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, cache.getPendingCount())

	req, err := cache.pop(ctx)
	require.NoError(t, err)
	require.Equal(t, regionReqStageProcessing, req.stage)

	region4 := createTestRegionInfo(1, 4)
	ok, err = cache.add(ctx, region4, true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 3, cache.getPendingCount())
}

func TestRequestCacheAddContextCancellation(t *testing.T) {
	cache := newRequestCache(1)
	ctx := context.Background()

	ok, err := cache.add(ctx, createTestRegionInfo(1, 1), false)
	require.NoError(t, err)
	require.True(t, ok)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	ok, err = cache.add(canceledCtx, createTestRegionInfo(1, 2), false)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRequestCacheAddRetryLimitExceeded(t *testing.T) {
	cache := newRequestCache(1)
	ctx := context.Background()

	ok, err := cache.add(ctx, createTestRegionInfo(1, 1), false)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cache.add(ctx, createTestRegionInfo(1, 2), false)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestRequestCacheSpaceAvailableNotification(t *testing.T) {
	cache := newRequestCache(1)
	ctx := context.Background()

	region1 := createTestRegionInfo(1, 1)
	ok, err := cache.add(ctx, region1, false)
	require.NoError(t, err)
	require.True(t, ok)

	req, err := cache.pop(ctx)
	require.NoError(t, err)
	req.markSent()
	require.Equal(t, 1, cache.getPendingCount())

	req.resolve()
	require.Equal(t, 0, cache.getPendingCount())

	region2 := createTestRegionInfo(1, 2)
	ok, err = cache.add(ctx, region2, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, cache.getPendingCount())
}

func TestRequestCacheConcurrentAdds(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()

	const numGoroutines = 5
	done := make(chan error, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			region := createTestRegionInfo(SubscriptionID(id%3), uint64(id))
			ok, err := cache.add(ctx, region, false)
			if !ok {
				done <- context.DeadlineExceeded
				return
			}
			done <- err
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for concurrent adds")
		}
	}

	require.Equal(t, numGoroutines, cache.getPendingCount())
}

func TestRequestCacheReplacesQueuedDuplicateRequest(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()
	region := createTestRegionInfo(1, 1)
	region.filterLoop = false

	ok, err := cache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)

	updated := region
	updated.filterLoop = true

	ok, err = cache.add(ctx, updated, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, cache.getPendingCount())

	req, err := cache.pop(ctx)
	require.NoError(t, err)
	require.True(t, req.regionInfo.filterLoop)
	req.markSent()
	req.resolve()
	require.Equal(t, 0, cache.getPendingCount())
}

func TestRequestCacheKeepsDuplicateActiveRegion(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()
	region := createTestRegionInfo(1, 1)

	ok, err := cache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)

	firstReq, err := cache.pop(ctx)
	require.NoError(t, err)
	firstReq.markSent()
	require.Equal(t, 1, cache.getPendingCount())

	ok, err = cache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, cache.getPendingCount())

	secondReq, err := cache.pop(ctx)
	require.NoError(t, err)
	secondReq.markSent()
	require.Equal(t, 1, cache.getPendingCount())

	secondReq.resolve()
	require.Equal(t, 0, cache.getPendingCount())
}

func TestRequestCacheKeepsDuplicateStopRequest(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()
	stopRegion := createTestRegionInfo(1, 1)
	stopRegion.lockedRangeState = nil

	ok, err := cache.add(ctx, stopRegion, true)
	require.NoError(t, err)
	require.True(t, ok)

	firstReq, err := cache.pop(ctx)
	require.NoError(t, err)
	require.Equal(t, regionReqStageProcessing, firstReq.stage)

	ok, err = cache.add(ctx, stopRegion, true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, cache.getPendingCount())

	secondReq, err := cache.pop(ctx)
	require.NoError(t, err)
	require.NotSame(t, firstReq, secondReq)

	firstReq.finish()
	require.Equal(t, 1, cache.getPendingCount())
	secondReq.finish()
	require.Equal(t, 0, cache.getPendingCount())
}

func TestRequestCacheSnapshotTracksStages(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()
	region := createTestRegionInfo(1, 1)

	ok, err := cache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, RequestCacheSnapshot{
		Total:  1,
		Queued: 1,
	}, cache.snapshot())

	req, err := cache.pop(ctx)
	require.NoError(t, err)
	require.Equal(t, RequestCacheSnapshot{
		Total:      1,
		Processing: 1,
	}, cache.snapshot())

	req.markSent()
	require.Equal(t, RequestCacheSnapshot{
		Total: 1,
		Sent:  1,
	}, cache.snapshot())

	req.resolve()
	require.Equal(t, RequestCacheSnapshot{}, cache.snapshot())
}

func TestRequestCacheTakeUnsentRegionsKeepsSentRequests(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()

	processingRegion := createTestRegionInfo(1, 1)
	ok, err := cache.add(ctx, processingRegion, false)
	require.NoError(t, err)
	require.True(t, ok)
	processingReq, err := cache.pop(ctx)
	require.NoError(t, err)
	require.Equal(t, regionReqStageProcessing, processingReq.stage)

	sentRegion := createTestRegionInfo(1, 2)
	ok, err = cache.add(ctx, sentRegion, false)
	require.NoError(t, err)
	require.True(t, ok)

	queuedRegion := createTestRegionInfo(1, 3)
	ok, err = cache.add(ctx, queuedRegion, false)
	require.NoError(t, err)
	require.True(t, ok)

	sentReq, err := cache.pop(ctx)
	require.NoError(t, err)
	sentReq.markSent()

	regions := cache.takeUnsentRegions()
	require.Len(t, regions, 2)
	require.ElementsMatch(t,
		[]uint64{processingRegion.verID.GetID(), queuedRegion.verID.GetID()},
		[]uint64{regions[0].verID.GetID(), regions[1].verID.GetID()},
	)
	require.Equal(t, 1, cache.getPendingCount())

	sentReq.resolve()
	require.Equal(t, 0, cache.getPendingCount())
}

func TestRequestCacheClearRemovesAllRequests(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()

	region1 := createTestRegionInfo(1, 1)
	region2 := createTestRegionInfo(1, 2)
	ok, err := cache.add(ctx, region1, false)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = cache.add(ctx, region2, false)
	require.NoError(t, err)
	require.True(t, ok)

	req, err := cache.pop(ctx)
	require.NoError(t, err)
	req.markSent()

	regions := cache.clear()
	require.Len(t, regions, 2)
	require.Equal(t, 0, cache.getPendingCount())
}
