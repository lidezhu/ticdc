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
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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

type workerSessionFailure struct {
	kind   regionFailureKind
	source regionFailureSource
	cause  error
}

func (f workerSessionFailure) toRegionFailure(region regionInfo) regionFailureInfo {
	switch f.kind {
	case regionFailureKindGetStore:
		return newGetStoreFailure(region, f.source, f.cause)
	case regionFailureKindSendRequestToStore:
		return newSendRequestToStoreFailure(region, f.source, f.cause)
	default:
		log.Panic("unknown worker session failure kind",
			zap.Stringer("failureKind", f.kind),
			zap.Stringer("failureSource", f.source),
			zap.Error(f.cause))
		return regionFailureInfo{}
	}
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

func normalizeWorkerSessionFailure(region regionInfo, sessionFailure workerSessionFailure) regionFailureInfo {
	if region.subscribedSpan != nil && region.subscribedSpan.stopped.Load() {
		return newSubscriptionStoppedFailure(region)
	}
	return sessionFailure.toRegionFailure(region)
}

func normalizeRegionFailure(base error, cause error) error {
	if cause == nil {
		return base
	}
	return errors.Annotate(base, cause.Error())
}
