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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/tikv/client-go/v2/tikv"
)

func newBenchmarkResolvedTsSpan(b *testing.B, regionCount int) (*subscribedSpan, *regionFeedState) {
	b.Helper()

	startKey, endKey, _ := common.GetKeyspaceTableRange(common.DefaultKeyspaceID, 1)
	span := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: startKey,
		EndKey:   endKey,
	}
	subSpan := &subscribedSpan{
		subID:             1,
		span:              span,
		startTs:           100,
		rangeLock:         regionlock.NewRangeLock(1, startKey, endKey, 100),
		advanceResolvedTs: func(uint64) {},
		advanceInterval:   1,
	}
	subSpan.initialized.Store(true)
	subSpan.resolvedTs.Store(100)
	subSpan.resolvedTsUpdated.Store(1)

	var target *regionFeedState
	for i := 1; i <= regionCount; i++ {
		rangeStart := make([]byte, len(startKey)+8)
		copy(rangeStart, startKey)
		binary.BigEndian.PutUint64(rangeStart[len(startKey):], uint64(i))

		var rangeEnd []byte
		if i < regionCount {
			rangeEnd = make([]byte, len(startKey)+8)
			copy(rangeEnd, startKey)
			binary.BigEndian.PutUint64(rangeEnd[len(startKey):], uint64(i+1))
		} else {
			rangeEnd = append([]byte(nil), endKey...)
		}

		lockRes := subSpan.rangeLock.LockRange(context.Background(), rangeStart, rangeEnd, uint64(i), 1)
		if lockRes.Status != regionlock.LockRangeStatusSuccess {
			b.Fatalf("lock range failed, region: %d, status: %v", i, lockRes.Status)
		}
		lockRes.LockedRangeState.ResolvedTs.Store(100)
		lockRes.LockedRangeState.Initialized.Store(true)
		region := newRegionInfo(
			tikv.NewRegionVerID(uint64(i), 1, 1),
			heartbeatpb.TableSpan{TableID: 1, StartKey: rangeStart, EndKey: rangeEnd},
			&tikv.RPCContext{Addr: "benchmark"},
			subSpan,
			false,
		)
		region.lockedRangeState = lockRes.LockedRangeState
		state := newRegionFeedState(region, uint64(subSpan.subID), 1, nil, nil, nil)
		state.start()
		if i == regionCount {
			target = state
		}
	}
	return subSpan, target
}

func BenchmarkHandleResolvedTsScan(b *testing.B) {
	for _, regionCount := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("regions=%d", regionCount), func(b *testing.B) {
			span, state := newBenchmarkResolvedTsSpan(b, regionCount)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				span.lastAdvanceTime.Store(0)
				handleResolvedTs(span, state, 101+uint64(i))
			}
		})
	}
}

func BenchmarkRequestCacheAddPopFinish(b *testing.B) {
	regions := make([]regionInfo, 4096)
	for i := range regions {
		regions[i] = createTestRegionInfo(1, uint64(i+1))
	}
	cache := newRequestCache(len(regions))
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region := regions[i%len(regions)]
		ok, err := cache.add(ctx, region, false)
		if err != nil || !ok {
			b.Fatalf("add failed, ok: %v, err: %v", ok, err)
		}
		req, err := cache.pop(ctx)
		if err != nil {
			b.Fatalf("pop failed: %v", err)
		}
		req.finish()
	}
}

func BenchmarkRequestCacheTryAddFull(b *testing.B) {
	cache := newRequestCache(128)
	for i := 0; i < 128; i++ {
		ok, err := cache.tryAdd(createTestRegionInfo(1, uint64(i+1)), false)
		if err != nil || !ok {
			b.Fatalf("preload failed, ok: %v, err: %v", ok, err)
		}
	}
	region := createTestRegionInfo(1, 1000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, err := cache.tryAdd(region, false)
		if err != nil || ok {
			b.Fatalf("tryAdd full returned ok: %v, err: %v", ok, err)
		}
	}
}

func BenchmarkTxnMatcherPutAndMatch(b *testing.B) {
	prewrites := make([]*cdcpb.Event_Row, 1024)
	commits := make([]*cdcpb.Event_Row, 1024)
	for i := range prewrites {
		key := []byte(fmt.Sprintf("key-%04d", i))
		prewrites[i] = &cdcpb.Event_Row{
			Type:    cdcpb.Event_PREWRITE,
			OpType:  cdcpb.Event_Row_PUT,
			StartTs: uint64(i + 1),
			Key:     key,
			Value:   []byte("value"),
		}
		commits[i] = &cdcpb.Event_Row{
			Type:     cdcpb.Event_COMMIT,
			OpType:   cdcpb.Event_Row_PUT,
			StartTs:  uint64(i + 1),
			CommitTs: uint64(i + 1001),
			Key:      key,
		}
	}
	matcher := newMatcher()
	defer matcher.clear()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % len(prewrites)
		matcher.putPrewriteRow(prewrites[idx])
		if !matcher.matchRow(commits[idx], true) {
			b.Fatalf("match failed")
		}
	}
}
