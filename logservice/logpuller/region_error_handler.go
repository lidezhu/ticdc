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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type regionErrorHandler struct {
}

// Note: don't block the caller, otherwise there may be deadlock.
func (h regionErrorHandler) onRegionFail(client *subscriptionClient, errInfo regionErrorInfo) {
	client.recordRegionRuntimeError(errInfo.regionInfo, errInfo.err, time.Now())
	if errInfo.subscribedSpan.rangeLock.UnlockRange(
		errInfo.span.StartKey, errInfo.span.EndKey,
		errInfo.verID.GetID(), errInfo.verID.GetVer(), errInfo.resolvedTs()) {
		client.onTableDrained(errInfo.subscribedSpan)
		return
	}
	client.errCache.add(errInfo)
}

func (h regionErrorHandler) handleErrors(client *subscriptionClient, ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Info("subscription client handle errors and exit")
			return ctx.Err()
		case errInfo := <-client.errCache.errCh:
			if err := h.handleError(client, ctx, errInfo); err != nil {
				return err
			}
		}
	}
}

func (h regionErrorHandler) retryRegion(
	client *subscriptionClient,
	ctx context.Context,
	errInfo regionErrorInfo,
	priority TaskType,
) {
	client.markRegionRetryPending(errInfo.regionInfo, errInfo.err, time.Now())
	client.scheduler.scheduleRegionRequest(client, ctx, errInfo.regionInfo, priority)
}

func (h regionErrorHandler) reloadRegionRange(
	client *subscriptionClient,
	ctx context.Context,
	errInfo regionErrorInfo,
) {
	client.removeRegionRuntime(errInfo.regionInfo, time.Now())
	client.scheduler.scheduleRangeRequest(client, ctx, errInfo.span, errInfo.subscribedSpan, errInfo.filterLoop, TaskHighPrior)
}

func (h regionErrorHandler) handleEventError(
	client *subscriptionClient,
	ctx context.Context,
	errInfo regionErrorInfo,
	eerr *eventError,
) error {
	innerErr := eerr.err
	if notLeader := innerErr.GetNotLeader(); notLeader != nil {
		metricFeedNotLeaderCounter.Inc()
		client.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader(), errInfo.rpcCtx.AccessIdx)
		h.retryRegion(client, ctx, errInfo, TaskHighPrior)
		return nil
	}
	if innerErr.GetEpochNotMatch() != nil {
		metricFeedEpochNotMatchCounter.Inc()
		h.reloadRegionRange(client, ctx, errInfo)
		return nil
	}
	if innerErr.GetRegionNotFound() != nil {
		metricFeedRegionNotFoundCounter.Inc()
		h.reloadRegionRange(client, ctx, errInfo)
		return nil
	}
	if innerErr.GetCongested() != nil {
		metricKvCongestedCounter.Inc()
		h.retryRegion(client, ctx, errInfo, TaskLowPrior)
		return nil
	}
	if innerErr.GetServerIsBusy() != nil {
		metricKvIsBusyCounter.Inc()
		h.retryRegion(client, ctx, errInfo, TaskLowPrior)
		return nil
	}
	if duplicated := innerErr.GetDuplicateRequest(); duplicated != nil {
		// TODO(qupeng): It's better to add a new machanism to deregister one region.
		metricFeedDuplicateRequestCounter.Inc()
		return errors.New("duplicate request")
	}
	if compatibility := innerErr.GetCompatibility(); compatibility != nil {
		return cerror.ErrVersionIncompatible.GenWithStackByArgs(compatibility)
	}
	if mismatch := innerErr.GetClusterIdMismatch(); mismatch != nil {
		return cerror.ErrClusterIDMismatch.GenWithStackByArgs(mismatch.Current, mismatch.Request)
	}

	log.Warn("empty or unknown cdc error",
		zap.Uint64("subscriptionID", uint64(errInfo.subscribedSpan.subID)),
		zap.Stringer("error", innerErr))
	metricFeedUnknownErrorCounter.Inc()
	h.retryRegion(client, ctx, errInfo, TaskHighPrior)
	return nil
}

func (h regionErrorHandler) handleError(
	client *subscriptionClient,
	ctx context.Context,
	errInfo regionErrorInfo,
) error {
	err := errors.Cause(errInfo.err)
	log.Debug("cdc region error",
		zap.Uint64("subscriptionID", uint64(errInfo.subscribedSpan.subID)),
		zap.Uint64("regionID", errInfo.verID.GetID()),
		zap.Error(err))

	switch eerr := err.(type) {
	case *eventError:
		return h.handleEventError(client, ctx, errInfo, eerr)
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		h.reloadRegionRange(client, ctx, errInfo)
		return nil
	case *getStoreErr:
		metricGetStoreErr.Inc()
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		// Cannot get the store the region belongs to, so we need to reload the region.
		client.regionCache.OnSendFail(bo, errInfo.rpcCtx, true, err)
		h.reloadRegionRange(client, ctx, errInfo)
		return nil
	case *sendRequestToStoreErr:
		metricStoreSendRequestErr.Inc()
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		client.regionCache.OnSendFail(bo, errInfo.rpcCtx, regionScheduleReload, err)
		h.retryRegion(client, ctx, errInfo, TaskHighPrior)
		return nil
	case *requestCancelledErr:
		client.removeRegionRuntime(errInfo.regionInfo, time.Now())
		// The corresponding subscription has been unsubscribed, just ignore.
		return nil
	default:
		// TODO(qupeng): for some errors it's better to just deregister the region from TiKVs.
		log.Warn("subscription client meets an internal error, fail the changefeed",
			zap.Uint64("subscriptionID", uint64(errInfo.subscribedSpan.subID)),
			zap.Error(err))
		return err
	}
}
