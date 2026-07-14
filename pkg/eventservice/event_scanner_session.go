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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventservice

import (
	"context"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
)

// ScanLimit defines the limits for a scan operation
type scanLimit struct {
	// maxDMLBytes is the maximum number of bytes to scan
	maxDMLBytes int64

	// Only used in unit test, please do not set it in other places.
	// When it is set to true, the scan will count DMLEvent as 1 byte,
	// otherwise it will count the size of DMLEvent.
	isInUnitTest bool
}

type scanProgress struct {
	valid                bool
	txnCommitTs          uint64
	txnStartTs           uint64
	rowLevelScanPosition common.ScanPosition
}

func newTxnScanProgress(commitTs uint64, startTs uint64) scanProgress {
	return scanProgress{
		valid:       true,
		txnCommitTs: commitTs,
		txnStartTs:  startTs,
	}
}

func newRowLevelScanProgress(commitTs uint64, startTs uint64, position common.ScanPosition) scanProgress {
	progress := newTxnScanProgress(commitTs, startTs)
	if len(position) > 0 {
		progress.rowLevelScanPosition = make(common.ScanPosition, len(position))
		copy(progress.rowLevelScanPosition, position)
	}
	return progress
}

// scanSession manages the state and context of a scan operation.
type scanSession struct {
	ctx            context.Context
	dispatcherStat *dispatcherStat
	dataRange      common.DataRange

	limit scanLimit
	// State tracking
	startTime time.Time

	scannedBytes      int64
	scannedEntryCount int
	lastRowPosition   common.ScanPosition
	// dmlCount is the count of transactions.
	dmlCount int

	// Result collection, including DDL, BatchedDML, ResolvedTs events in the timestamp order.
	events     []event.Event
	eventBytes int64
	progress   scanProgress
}

// newSession creates a new scan session
func newSession(
	ctx context.Context,
	dispatcherStat *dispatcherStat,
	dataRange common.DataRange,
	limit scanLimit,
) *scanSession {
	return &scanSession{
		ctx:            ctx,
		dispatcherStat: dispatcherStat,
		dataRange:      dataRange,
		limit:          limit,
		startTime:      time.Now(),
		events:         make([]event.Event, 0),
	}
}

// observeRawEntry adds to the total bytes scanned
func (s *scanSession) observeRawEntry(entry *common.RawKVEntry, position common.ScanPosition) {
	s.scannedBytes += entry.GetSize()
	s.scannedEntryCount++
	if len(position) == 0 {
		s.lastRowPosition = nil
		return
	}
	s.lastRowPosition = make(common.ScanPosition, len(position))
	copy(s.lastRowPosition, position)
}

// isContextDone checks if the context is cancelled
func (s *scanSession) isContextDone() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

// recordMetrics records the scan duration metrics
func (s *scanSession) recordMetrics() {
	metrics.EventServiceScanDuration.Observe(time.Since(s.startTime).Seconds())
	metrics.EventServiceScannedCount.Observe(float64(s.scannedEntryCount))
	metrics.EventServiceScannedTxnCount.Observe(float64(s.dmlCount))
	metrics.EventServiceScannedDMLSize.Observe(float64(s.eventBytes))
}

func (s *scanSession) appendEvents(events []event.Event) {
	s.events = append(s.events, events...)

	if !s.limit.isInUnitTest {
		for _, item := range events {
			s.eventBytes += item.GetSize()
		}
		return
	}

	// only for unit test
	for _, item := range events {
		if item.GetType() == event.TypeBatchDMLEvent {
			batchDML := item.(*event.BatchDMLEvent)
			s.eventBytes += int64(len(batchDML.DMLEvents))
		} else {
			s.eventBytes++
		}
	}
}

func (s *scanSession) exceedLimit(nBytes int64, batchDMLs ...*event.BatchDMLEvent) bool {
	if s.limit.isInUnitTest && len(batchDMLs) > 0 {
		batchDML := batchDMLs[0]
		eventCount := len(batchDML.DMLEvents)
		return (s.eventBytes + int64(eventCount)) >= s.limit.maxDMLBytes
	}

	return (s.eventBytes + nBytes) >= s.limit.maxDMLBytes
}
