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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
)

type regionFailureScope string

const (
	regionFailureScopeRegion       regionFailureScope = "region"
	regionFailureScopeStoreSession regionFailureScope = "store_session"
	regionFailureScopeSubscription regionFailureScope = "subscription"
)

func (s regionFailureScope) String() string {
	return string(s)
}

type regionFailureSource string

const (
	regionFailureSourceTiKVEvent        regionFailureSource = "tikv_event"
	regionFailureSourceWorkerSend       regionFailureSource = "worker_send"
	regionFailureSourceWorkerRecv       regionFailureSource = "worker_recv"
	regionFailureSourceWorkerSession    regionFailureSource = "worker_session"
	regionFailureSourceRouterRPCContext regionFailureSource = "router_rpc_ctx"
	regionFailureSourceSubscriptionStop regionFailureSource = "subscription_stop"
	regionFailureSourceDeregister       regionFailureSource = "deregister"
)

func (s regionFailureSource) String() string {
	return string(s)
}

type regionFailureKind string

const (
	regionFailureKindTiKVEvent           regionFailureKind = "tikv_event"
	regionFailureKindRPCCtxUnavailable   regionFailureKind = "rpc_ctx_unavailable"
	regionFailureKindGetStore            regionFailureKind = "get_store"
	regionFailureKindSendRequestToStore  regionFailureKind = "send_request_to_store"
	regionFailureKindRequestCancelled    regionFailureKind = "request_cancelled"
	regionFailureKindSubscriptionStopped regionFailureKind = "subscription_stopped"
)

func (k regionFailureKind) String() string {
	return string(k)
}

type regionFailureInfo struct {
	regionInfo
	scope  regionFailureScope
	source regionFailureSource
	kind   regionFailureKind
	err    error
}

func newRegionFailureInfo(
	region regionInfo,
	scope regionFailureScope,
	source regionFailureSource,
	kind regionFailureKind,
	err error,
) regionFailureInfo {
	return regionFailureInfo{
		regionInfo: region,
		scope:      scope,
		source:     source,
		kind:       kind,
		err:        err,
	}
}

func newEventRegionFailure(region regionInfo, err *cdcpb.Error) regionFailureInfo {
	return newRegionFailureInfo(
		region,
		regionFailureScopeRegion,
		regionFailureSourceTiKVEvent,
		regionFailureKindTiKVEvent,
		&eventError{err: err},
	)
}

func newRPCCtxUnavailableFailure(region regionInfo) regionFailureInfo {
	return newRegionFailureInfo(
		region,
		regionFailureScopeRegion,
		regionFailureSourceRouterRPCContext,
		regionFailureKindRPCCtxUnavailable,
		&rpcCtxUnavailableErr{verID: region.verID},
	)
}

func newGetStoreFailure(region regionInfo, source regionFailureSource, cause error) regionFailureInfo {
	return newRegionFailureInfo(
		region,
		regionFailureScopeStoreSession,
		source,
		regionFailureKindGetStore,
		normalizeRegionFailure(&getStoreErr{}, cause),
	)
}

func newSendRequestToStoreFailure(region regionInfo, source regionFailureSource, cause error) regionFailureInfo {
	return newRegionFailureInfo(
		region,
		regionFailureScopeStoreSession,
		source,
		regionFailureKindSendRequestToStore,
		normalizeRegionFailure(&sendRequestToStoreErr{}, cause),
	)
}

func newRequestCancelledFailure(region regionInfo, source regionFailureSource) regionFailureInfo {
	return newRegionFailureInfo(
		region,
		regionFailureScopeSubscription,
		source,
		regionFailureKindRequestCancelled,
		&requestCancelledErr{},
	)
}

func newSubscriptionStoppedFailure(region regionInfo) regionFailureInfo {
	return newRegionFailureInfo(
		region,
		regionFailureScopeSubscription,
		regionFailureSourceSubscriptionStop,
		regionFailureKindSubscriptionStopped,
		&subscriptionStoppedErr{},
	)
}

func normalizeRegionFailure(base error, cause error) error {
	if cause == nil {
		return base
	}
	return errors.Annotate(base, cause.Error())
}

type recoveryAction string

const (
	recoveryActionRetryRegion  recoveryAction = "retry_region"
	recoveryActionReloadRange  recoveryAction = "reload_range"
	recoveryActionRemoveRegion recoveryAction = "remove_region"
)

func (a recoveryAction) String() string {
	return string(a)
}

type recoveryPlan struct {
	action   recoveryAction
	priority TaskType
}

type failureBuffer struct {
	sync.Mutex
	pending []regionFailureInfo
	ch      chan regionFailureInfo
	notify  chan struct{}
}

func newFailureBuffer() *failureBuffer {
	return &failureBuffer{
		pending: make([]regionFailureInfo, 0, 1024),
		ch:      make(chan regionFailureInfo, 1024),
		notify:  make(chan struct{}, 1024),
	}
}

func (b *failureBuffer) enqueue(failure regionFailureInfo) {
	b.Lock()
	defer b.Unlock()
	b.pending = append(b.pending, failure)
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

func (b *failureBuffer) run(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	dispatchOne := func() {
		b.Lock()
		if len(b.pending) == 0 {
			b.Unlock()
			return
		}
		failure := b.pending[0]
		b.pending = b.pending[1:]
		b.Unlock()

		select {
		case <-ctx.Done():
			log.Info("subscription client dispatch failure buffer done")
		case b.ch <- failure:
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			dispatchOne()
		case <-b.notify:
			dispatchOne()
		}
	}
}
