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

// Package logpuller subscribes TiKV CDC events for table spans and feeds them
// into the log service.
//
// The main data path is:
//  1. A subscribedSpan is created by SubscriptionClient.Subscribe.
//  2. regionRequestScheduler splits the span into TiKV regions and locks each
//     region range.
//  3. A regionReq is admitted into one store-local regionRequestWorker.
//  4. regionRequestWorkerSession sends the request through the store gRPC
//     stream and tracks the corresponding regionFeedState.
//  5. The receive loop pushes ordered regionEvent values to dynamic stream.
//  6. regionEventHandler matches 2PC rows, advances resolved-ts, and hands
//     committed rows to the consumer.
//
// Request lifecycle:
//
//	regionReq: queued -> processing -> sent -> finished
//
// The request cache owns this lifecycle for admission control. A request is
// removed when the region initializes, when the request is canceled, or when a
// worker session fails before the request becomes an active feed.
//
// Region feed lifecycle:
//
//	regionFeedState: normal -> stopped -> removed
//
// Region failures are intentionally delivered through the same dynamic stream
// path as row and resolved-ts events. This keeps failure recovery ordered with
// events from the same subscription and prevents retry logic from racing ahead
// of older events that are still buffered.
//
// Failure flow:
//  1. TiKV event errors, RPC context misses, send failures and subscription
//     stops are normalized into regionFailureInfo.
//  2. If a request has already become an active regionFeedState, the failure is
//     marked on that state and emitted through dynamic stream so recovery stays
//     ordered with earlier region events.
//  3. If a request is still local to a worker session, the scheduler can handle
//     it directly because no TiKV event can still arrive for that request.
//  4. The scheduler unlocks the range, records metrics/runtime state, and then
//     either retries the same region or reloads the range from PD depending on
//     whether the old region metadata is still usable.
package logpuller
