// Copyright 2020 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

const (
	prewriteCacheSize       = 16
	clearCacheDelayInSecond = 5
)

var (
	prewriteCacheRowNum = metrics.LogPullerPrewriteCacheRowNum
	matcherCount        = metrics.LogPullerMatcherCount
)

type matchKey struct {
	startTs uint64
	key     string
}

func newMatchKey(row *cdcpb.Event_Row) matchKey {
	return matchKey{startTs: row.GetStartTs(), key: string(row.GetKey())}
}

type matcher struct {
	unmatchedValue   map[matchKey]*cdcpb.Event_Row
	cachedCommit     []*cdcpb.Event_Row
	cachedRollback   []*cdcpb.Event_Row
	lastPrewriteTime time.Time
}

func newMatcher() *matcher {
	matcherCount.Inc()
	return &matcher{
		unmatchedValue: make(map[matchKey]*cdcpb.Event_Row, prewriteCacheSize),
	}
}

func (m *matcher) putPrewriteRow(row *cdcpb.Event_Row) {
	key := newMatchKey(row)
	// tikv may send a fake prewrite event with empty value caused by txn heartbeat.
	// here we need to avoid the fake prewrite event overwrite the prewrite value.

	// when the old-value is disabled, the value of the fake prewrite event is empty.
	// when the old-value is enabled, the value of the fake prewrite event is also empty,
	// but the old value of the fake prewrite event is not empty.
	// We can distinguish fake prewrite events by whether the value is empty,
	// no matter the old-value is enabled or disabled
	if _, exist := m.unmatchedValue[key]; exist && len(row.GetValue()) == 0 {
		return
	}
	if m.unmatchedValue == nil {
		m.unmatchedValue = make(map[matchKey]*cdcpb.Event_Row, prewriteCacheSize)
	}
	m.unmatchedValue[key] = row
	m.lastPrewriteTime = time.Now()
	prewriteCacheRowNum.Inc()
}

// matchRow matches the commit event with the cached prewrite event
// the Value and OldValue will be assigned if a matched prewrite event exists.
func (m *matcher) matchRow(row *cdcpb.Event_Row, initialized bool) bool {
	if value, exist := m.unmatchedValue[newMatchKey(row)]; exist {
		// TiKV may send a fake prewrite event with empty value caused by txn heartbeat.
		//
		// We need to skip match if the region is not initialized,
		// as prewrite events may be sent out of order.
		if !initialized && len(value.GetValue()) == 0 {
			return false
		}
		row.Value = value.GetValue()
		row.OldValue = value.GetOldValue()
		delete(m.unmatchedValue, newMatchKey(row))
		prewriteCacheRowNum.Dec()
		return true
	}
	return false
}

func (m *matcher) cacheCommitRow(row *cdcpb.Event_Row) {
	m.cachedCommit = append(m.cachedCommit, row)
}

//nolint:unparam
func (m *matcher) matchCachedRow(initialized bool) []*cdcpb.Event_Row {
	if !initialized {
		log.Panic("must be initialized before match cached rows")
	}
	cachedCommit := m.cachedCommit
	m.cachedCommit = nil
	top := 0
	for i := 0; i < len(cachedCommit); i++ {
		cacheEntry := cachedCommit[i]
		ok := m.matchRow(cacheEntry, true)
		if !ok {
			// when cdc receives a commit log without a corresponding
			// prewrite log before initialized, a committed log  with
			// the same key and start-ts must have been received.
			log.Info("ignore commit event without prewrite",
				zap.Binary("key", cacheEntry.GetKey()),
				zap.Uint64("startTs", cacheEntry.GetStartTs()))
			continue
		}
		cachedCommit[top] = cacheEntry
		top++
	}
	return cachedCommit[:top]
}

func (m *matcher) rollbackRow(row *cdcpb.Event_Row) {
	delete(m.unmatchedValue, newMatchKey(row))
	prewriteCacheRowNum.Dec()
}

func (m *matcher) cacheRollbackRow(row *cdcpb.Event_Row) {
	m.cachedRollback = append(m.cachedRollback, row)
}

//nolint:unparam
func (m *matcher) matchCachedRollbackRow(initialized bool) {
	if !initialized {
		log.Panic("must be initialized before match cached rollback rows")
	}
	rollback := m.cachedRollback
	m.cachedRollback = nil
	for i := 0; i < len(rollback); i++ {
		cacheEntry := rollback[i]
		m.rollbackRow(cacheEntry)
	}
}

func (m *matcher) tryCleanUnmatchedValue() {
	if m.unmatchedValue == nil {
		return
	}
	// Only clear the unmatched value if it has been 10 seconds since the last prewrite event
	// and there is no unmatched value left.
	if time.Since(m.lastPrewriteTime) > clearCacheDelayInSecond*time.Second && len(m.unmatchedValue) == 0 {
		m.clearUnmatchedValue()
	}
}

func (m *matcher) clearUnmatchedValue() {
	m.lastPrewriteTime = time.Time{}
	for k := range m.unmatchedValue {
		delete(m.unmatchedValue, k)
	}
	m.unmatchedValue = nil
}

func (m *matcher) clear() {
	matcherCount.Dec()
	prewriteCacheRowNum.Sub(float64(len(m.unmatchedValue)))
	m.clearUnmatchedValue()
	m.cachedCommit = nil
	m.cachedRollback = nil
}
