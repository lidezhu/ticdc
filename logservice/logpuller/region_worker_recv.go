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

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	grpcstatus "google.golang.org/grpc/status"
)

// receiveAndDispatchChangeEvents receives events from the grpc stream and dispatches them to ds.
func (s *regionRequestWorkerSession) receiveAndDispatchChangeEvents(ctx context.Context) error {
	for {
		changeEvent, err := s.conn.Client.Recv()
		if err != nil {
			log.Info("region request worker receive from grpc stream failed",
				zap.Uint64("workerID", s.workerID),
				zap.String("addr", s.storeAddr),
				zap.String("code", grpcstatus.Code(err).String()),
				zap.Error(err))
			if ctx.Err() != nil && isCanceledByContext(ctx, err) {
				return err
			}
			if StatusIsEOF(grpcstatus.Convert(err)) {
				return errWorkerSessionReconnect
			}
			return errors.Trace(err)
		}
		if len(changeEvent.Events) > 0 {
			s.dispatchRegionChangeEvents(changeEvent.Events)
		}
		if changeEvent.ResolvedTs != nil {
			s.dispatchResolvedTsEvent(changeEvent.ResolvedTs)
		}
	}
}

func (s *regionRequestWorkerSession) submitOrderedStateFailure(state *regionFeedState, failure regionFailureInfo) {
	state.markStopped(failure)
	s.emitRegionEvent(SubscriptionID(state.requestID), regionEvent{
		// Keep ordered region-failure notifications on the same path.
		states: []*regionFeedState{state},
	})
}

func (s *regionRequestWorkerSession) cancelSubscriptionStates(subID SubscriptionID) {
	for _, state := range s.activeRegions.takeSubscription(subID) {
		s.submitOrderedStateFailure(
			state,
			newRequestCancelledFailure(state.getRegionInfo(), regionFailureSourceDeregister),
		)
	}
}

func (s *regionRequestWorkerSession) dispatchRegionChangeEvents(events []*cdcpb.Event) {
	for _, event := range events {
		subscriptionID := SubscriptionID(event.RequestId)
		if state := s.activeRegions.get(subscriptionID, event.RegionId); state != nil {
			s.handleTrackedRegionEvent(subscriptionID, state, event)
			continue
		}
		s.handleUntrackedRegionEvent(subscriptionID, event)
	}
}

func (s *regionRequestWorkerSession) handleTrackedRegionEvent(
	subscriptionID SubscriptionID,
	state *regionFeedState,
	event *cdcpb.Event,
) {
	regionEvent := regionEvent{
		states: []*regionFeedState{state},
	}
	switch eventData := event.Event.(type) {
	case *cdcpb.Event_Entries_:
		if eventData == nil {
			log.Warn("region request worker receives a region event with nil entries, ignore it",
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("subscriptionID", uint64(subscriptionID)),
				zap.Uint64("regionID", event.RegionId))
			return
		}
		regionEvent.entries = eventData
	case *cdcpb.Event_Admin_:
		return
	case *cdcpb.Event_Error:
		log.Debug("region request worker receives a region error",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subscriptionID)),
			zap.Uint64("regionID", event.RegionId),
			zap.Any("error", eventData.Error))
		s.submitOrderedStateFailure(state, newEventRegionFailure(state.getRegionInfo(), eventData.Error))
		return
	case *cdcpb.Event_ResolvedTs:
		regionEvent.resolvedTs = eventData.ResolvedTs
	case *cdcpb.Event_LongTxn_:
		return
	default:
		log.Panic("unknown event type", zap.Any("event", event))
	}
	s.emitRegionEvent(subscriptionID, regionEvent)
}

func (s *regionRequestWorkerSession) handleUntrackedRegionEvent(
	subscriptionID SubscriptionID,
	event *cdcpb.Event,
) {
	switch event.Event.(type) {
	case *cdcpb.Event_Error:
		log.Debug("region request worker receives an error for a stale region, ignore it",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subscriptionID)),
			zap.Uint64("regionID", event.RegionId))
	default:
		log.Warn("region request worker receives a region event for an untracked region",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subscriptionID)),
			zap.Uint64("regionID", event.RegionId))
	}
}

func (s *regionRequestWorkerSession) dispatchResolvedTsEvent(resolvedTsEvent *cdcpb.ResolvedTs) {
	subscriptionID := SubscriptionID(resolvedTsEvent.RequestId)
	metricsResolvedTsCount.Add(float64(len(resolvedTsEvent.Regions)))
	metricBatchResolvedEventSize.Observe(float64(len(resolvedTsEvent.Regions)))
	if resolvedTsEvent.Ts == 0 {
		log.Warn("region request worker receives a resolved ts event with zero value, ignore it",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", resolvedTsEvent.RequestId),
			zap.Any("regionIDs", resolvedTsEvent.Regions))
		return
	}

	const resolvedTsStateBatchSize = 1024
	capHint := len(resolvedTsEvent.Regions)
	if capHint > resolvedTsStateBatchSize {
		capHint = resolvedTsStateBatchSize
	}
	resolvedStates := make([]*regionFeedState, 0, capHint)
	flush := func() {
		if len(resolvedStates) == 0 {
			return
		}
		s.emitRegionEvent(subscriptionID, regionEvent{
			resolvedTs: resolvedTsEvent.Ts,
			states:     resolvedStates,
		})
		resolvedStates = nil
	}
	for i, regionID := range resolvedTsEvent.Regions {
		if state := s.activeRegions.get(subscriptionID, regionID); state != nil {
			resolvedStates = append(resolvedStates, state)
			if len(resolvedStates) >= resolvedTsStateBatchSize {
				flush()
				if i+1 < len(resolvedTsEvent.Regions) {
					capHint = len(resolvedTsEvent.Regions) - (i + 1)
					if capHint > resolvedTsStateBatchSize {
						capHint = resolvedTsStateBatchSize
					}
					resolvedStates = make([]*regionFeedState, 0, capHint)
				}
			}
			continue
		}
		log.Warn("region request worker receives a resolved ts event for an untracked region",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subscriptionID)),
			zap.Uint64("regionID", regionID),
			zap.Uint64("resolvedTs", resolvedTsEvent.Ts))
	}
	flush()
}

func (s *regionRequestWorkerSession) emitRegionEvent(subID SubscriptionID, event regionEvent) {
	s.pushRegionEvent(subID, event)
}
