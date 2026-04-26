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
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func (s *regionRequestWorkerSession) sendRequest(req *cdcpb.ChangeDataRequest) error {
	if err := s.conn.Client.Send(req); err != nil {
		log.Warn("region request worker send request to grpc stream failed",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", req.RequestId),
			zap.Uint64("regionID", req.RegionId),
			zap.String("addr", s.storeAddr),
			zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func (s *regionRequestWorkerSession) nextRegionRequest(ctx context.Context) (*regionReq, error) {
	if s.bootstrapRegion != nil {
		req := s.bootstrapRegion
		s.bootstrapRegion = nil
		return req, nil
	}
	return s.requestCache.pop(ctx)
}

func (s *regionRequestWorkerSession) handleStopTask(request *regionReq) error {
	region := request.regionInfo
	subID := region.subscribedSpan.subID
	req := &cdcpb.ChangeDataRequest{
		Header:    s.requestHeader(),
		RequestId: uint64(subID),
		Request: &cdcpb.ChangeDataRequest_Deregister_{
			Deregister: &cdcpb.ChangeDataRequest_Deregister{},
		},
		FilterLoop: region.filterLoop,
	}
	if err := s.sendRequest(req); err != nil {
		return err
	}
	request.finish()
	s.cancelSubscriptionStates(subID)
	return nil
}

func (s *regionRequestWorkerSession) handleStoppedSubscription(request *regionReq) {
	s.submitDirectFailure(newSubscriptionStoppedFailure(request.regionInfo))
	request.finish()
}

func (s *regionRequestWorkerSession) handleActiveRegionRequest(request *regionReq) error {
	region := request.regionInfo
	subID := region.subscribedSpan.subID
	state := s.newState(request)
	state.start()
	s.activeRegions.add(subID, region.verID.GetID(), state)

	// Mark the request as sent before sending it to keep active-state tracking
	// and request lifecycle tracking visible in the same order.
	request.markSent()
	s.markRegionSent(region, time.Now())
	if err := s.sendRequest(s.createRegionRequest(region)); err != nil {
		state.markStopped(newSendRequestToStoreFailure(region, regionFailureSourceWorkerSend, err))
		return err
	}
	return nil
}

func (s *regionRequestWorkerSession) handleRegionSendTask(request *regionReq) error {
	region := request.regionInfo
	switch {
	case region.isStopped():
		return s.handleStopTask(request)
	case region.subscribedSpan.stopped.Load():
		s.handleStoppedSubscription(request)
		return nil
	default:
		return s.handleActiveRegionRequest(request)
	}
}

// processRegionSendTask receives region requests and sends them to the remote store.
func (s *regionRequestWorkerSession) processRegionSendTask(ctx context.Context) error {
	for {
		request, err := s.nextRegionRequest(ctx)
		if err != nil {
			return err
		}

		region := request.regionInfo
		subID := region.subscribedSpan.subID
		log.Debug("region request worker gets a singleRegionInfo",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.String("addr", s.storeAddr),
			zap.Bool("bdrMode", region.filterLoop))

		if err := s.handleRegionSendTask(request); err != nil {
			return err
		}
	}
}

func (s *regionRequestWorkerSession) createRegionRequest(region regionInfo) *cdcpb.ChangeDataRequest {
	return &cdcpb.ChangeDataRequest{
		Header:       s.requestHeader(),
		RegionId:     region.verID.GetID(),
		RequestId:    uint64(region.subscribedSpan.subID),
		RegionEpoch:  region.rpcCtx.Meta.RegionEpoch,
		CheckpointTs: region.resolvedTs(),
		StartKey:     region.span.StartKey,
		EndKey:       region.span.EndKey,
		ExtraOp:      kvrpcpb.ExtraOp_ReadOldValue,
		FilterLoop:   region.filterLoop,
	}
}
