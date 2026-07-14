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

import "github.com/pingcap/ticdc/pkg/common/event"

// eventMerger handles merging of DML and DDL events in timestamp order
type eventMerger struct {
	ddlEvents []event.Event
	ddlIndex  int
	// Record the last batch dml that has been merge with preceding DDLs
	lastBatchDMLCommitTs uint64
}

// newEventMerger creates a new event merger
func newEventMerger(
	ddlEvents []event.Event,
) *eventMerger {
	return &eventMerger{
		ddlEvents: ddlEvents,
		ddlIndex:  0,
	}
}

// mergeWithPrecedingDDLs returns the DML event along with all preceding DDL events in timestamp order.
func (m *eventMerger) mergeWithPrecedingDDLs(batchDML *event.BatchDMLEvent) []event.Event {
	if batchDML == nil || batchDML.DMLCount() == 0 {
		return nil
	}

	commitTs := batchDML.GetCommitTs()
	var events []event.Event
	// Collect all DDL events with commitTs < dml.commitTs
	for m.ddlIndex < len(m.ddlEvents) && m.ddlEvents[m.ddlIndex].GetCommitTs() < commitTs {
		events = append(events, m.ddlEvents[m.ddlIndex])
		m.ddlIndex++
	}

	events = append(events, batchDML)

	m.lastBatchDMLCommitTs = commitTs
	return events
}

// resolveDDLEvents return all remaining DDL events that have not been processed yet.
func (m *eventMerger) resolveDDLEvents(endTs uint64) []event.Event {
	var events []event.Event
	for m.ddlIndex < len(m.ddlEvents) && m.ddlEvents[m.ddlIndex].GetCommitTs() <= endTs {
		events = append(events, m.ddlEvents[m.ddlIndex])
		m.ddlIndex++
	}
	return events
}

// hasDDLLessThanCommitTs return true if there are DDLs
func (m *eventMerger) hasDDLLessThanCommitTs(commitTs uint64) bool {
	return m.ddlIndex < len(m.ddlEvents) && m.ddlEvents[m.ddlIndex].GetCommitTs() < commitTs
}

// hasDDLAtCommitTs checks if there's a DDL event at the specified commitTs
// This method doesn't modify the ddlIndex, it's used for checking only
func (m *eventMerger) hasDDLAtCommitTs(commitTs uint64) bool {
	for i := m.ddlIndex; i < len(m.ddlEvents); i++ {
		ddlCommitTs := m.ddlEvents[i].GetCommitTs()
		if ddlCommitTs == commitTs {
			return true
		}
		// Since DDL events are sorted by commitTs, if we find a larger commitTs, we can stop
		if ddlCommitTs > commitTs {
			break
		}
	}
	return false
}

// canInterrupt determines if we can interrupt the scan at the current position when reaching scan limits.
// The function ensures that DML and DDL events with the same commitTs are processed together atomically.
//
// Logic:
// 1. If currentDML.commitTs != newCommitTs: Can interrupt (different transactions)
// 2. If currentDML.commitTs == newCommitTs: Check if there are DDL events at this commitTs
//   - If no DDL at this commitTs: Can interrupt
//   - If DDL exists at this commitTs: Cannot interrupt (must process together)
//
// Examples:
//
// Case 1 - Different commitTs, can interrupt:
// Event sequence:
//
//	DML1(x+1) -> DML2(x+2)
//	          ▲
//	          └── currentDML.commitTs=x+1, newCommitTs=x+2, can interrupt
//
// Case 2 - Same commitTs, no DDL, can interrupt:
// Event sequence:
//
//	DML1(x+1) -> DML2(x+2) -> DML3(x+2)
//	                       ▲
//	                       └── currentDML.commitTs=x+2, newCommitTs=x+2, no DDL at x+2, can interrupt
//
// Case 3 - Same commitTs, has DDL, cannot interrupt:
// Event sequence:
//
//	DML1(x+1) -> DML2(x+2) -> DDL(x+2) -> DML3(x+2)
//	                       ▲
//	                       └── currentDML.commitTs=x+2, newCommitTs=x+2, DDL exists at x+2, cannot interrupt
//	                           Must process DML2, DML3, DDL together atomically
func (m *eventMerger) canInterrupt(newCommitTs uint64, currentBatchDML *event.BatchDMLEvent) bool {
	currentDMLCommitTs := uint64(0)
	if len(currentBatchDML.DMLEvents) > 0 {
		currentDMLCommitTs = currentBatchDML.GetCommitTs()
	}

	if currentDMLCommitTs != newCommitTs {
		return true
	}

	// Check if there are any DDL events at the lastCommitTs
	// If there are, we cannot interrupt to ensure they are processed together
	return !m.hasDDLAtCommitTs(newCommitTs)
}
