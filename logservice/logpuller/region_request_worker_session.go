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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/version"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	grpcstatus "google.golang.org/grpc/status"
)

type regionFeedStates map[uint64]*regionFeedState

type sessionRunResult struct {
	failure  workerSessionFailure
	canceled bool
}

type sessionLoopExit struct {
	source regionFailureSource
	err    error
}

var errWorkerSessionReconnect = errors.New("worker session reconnect")

// regionRequestWorkerSession owns one grpc stream session to one TiKV store.
// It is responsible for grpc send/recv, active region states and bootstrap request handling.
type regionRequestWorkerSession struct {
	workerID        uint64
	storeAddr       string
	pd              pd.Client
	credential      *security.Credential
	clusterID       uint64
	requestCache    *requestCache
	runtimeRegistry *regionRuntimeRegistry
	failures        *failureHandler
	pushRegionEvent func(SubscriptionID, regionEvent)

	conn            *ConnAndClient
	bootstrapRegion *regionReq

	requestedRegions struct {
		sync.RWMutex
		subscriptions map[SubscriptionID]regionFeedStates
	}
}

func newRegionRequestWorkerSession(
	workerID uint64,
	storeAddr string,
	pd pd.Client,
	credential *security.Credential,
	clusterID uint64,
	requestCache *requestCache,
	runtimeRegistry *regionRuntimeRegistry,
	failures *failureHandler,
	pushRegionEvent func(SubscriptionID, regionEvent),
) *regionRequestWorkerSession {
	session := &regionRequestWorkerSession{
		workerID:        workerID,
		storeAddr:       storeAddr,
		pd:              pd,
		credential:      credential,
		clusterID:       clusterID,
		requestCache:    requestCache,
		runtimeRegistry: runtimeRegistry,
		failures:        failures,
		pushRegionEvent: pushRegionEvent,
	}
	session.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)
	return session
}

func (s *regionRequestWorkerSession) close() {
	if s.conn != nil && s.conn.Conn != nil {
		_ = s.conn.Conn.Close()
	}
}

func defaultWorkerSessionFailure(source regionFailureSource, cause error) workerSessionFailure {
	return workerSessionFailure{
		kind:   regionFailureKindSendRequestToStore,
		source: source,
		cause:  cause,
	}
}

// isCanceledByContext only treats nil/context errors as cancellation,
// so a real worker error is not hidden by a later ctx cancellation.
func isCanceledByContext(ctx context.Context, err error) bool {
	if err == nil {
		return ctx.Err() != nil
	}
	switch errors.Cause(err) {
	case context.Canceled, context.DeadlineExceeded:
		return true
	}
	return false
}

func (s *regionRequestWorkerSession) startLoop(
	g *errgroup.Group,
	cancel context.CancelFunc,
	exitCh chan<- sessionLoopExit,
	source regionFailureSource,
	run func() error,
) {
	g.Go(func() error {
		err := run()
		exitCh <- sessionLoopExit{source: source, err: err}
		cancel()
		return err
	})
}

// pickPrimaryLoopExit chooses the session exit reason.
//
// errgroup.Wait only gives one non-nil error. That is not enough here because
// one loop can stop first and cancel the session, then the other loop returns
// context.Canceled as a follow-up effect.
//
// Example:
// 1. recv gets EOF, so this session should reconnect
// 2. recv exit cancels the session context
// 3. send then returns context.Canceled
//
// If we only use waitErr, we may report canceled instead of reconnect.
func pickPrimaryLoopExit(
	ctx context.Context,
	exits []sessionLoopExit,
	waitErr error,
) sessionLoopExit {
	for _, exit := range exits {
		if exit.err != nil && !isCanceledByContext(ctx, exit.err) {
			return exit
		}
	}
	if waitErr != nil && !isCanceledByContext(ctx, waitErr) {
		return sessionLoopExit{source: regionFailureSourceWorkerSession, err: waitErr}
	}
	if len(exits) == 0 {
		return sessionLoopExit{source: regionFailureSourceWorkerSession, err: waitErr}
	}
	return exits[0]
}

func (s *regionRequestWorkerSession) run(ctx context.Context) (sessionRunResult, error) {
	bootstrapRegion, err := s.waitBootstrapRegion(ctx)
	if err != nil {
		if isCanceledByContext(ctx, err) {
			return sessionRunResult{canceled: true}, nil
		}
		return sessionRunResult{}, err
	}
	req := newRegionReq(bootstrapRegion)
	s.bootstrapRegion = &req

	if err := s.checkStoreVersion(ctx); err != nil {
		if isCanceledByContext(ctx, err) {
			return sessionRunResult{canceled: true}, nil
		}
		return sessionRunResult{
			failure: s.checkStoreVersionFailure(err),
		}, nil
	}

	s.conn, err = s.connectStore(ctx)
	if err != nil {
		if isCanceledByContext(ctx, err) {
			return sessionRunResult{canceled: true}, nil
		}
		return sessionRunResult{
			failure: defaultWorkerSessionFailure(regionFailureSourceWorkerSession, err),
		}, nil
	}

	return s.runConnectedLoops(ctx)
}

func (s *regionRequestWorkerSession) runConnectedLoops(
	ctx context.Context,
) (sessionRunResult, error) {
	defer s.close()
	defer func() {
		log.Info("region request worker session exits",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.storeAddr),
			zap.Bool("canceled", ctx.Err() != nil))
	}()

	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gctx := errgroup.WithContext(loopCtx)
	loopCount := 2
	exitCh := make(chan sessionLoopExit, 3)
	s.startLoop(g, cancel, exitCh, regionFailureSourceWorkerRecv, func() error {
		return s.receiveAndDispatchChangeEvents(ctx)
	})
	s.startLoop(g, cancel, exitCh, regionFailureSourceWorkerSend, func() error {
		return s.processRegionSendTask(gctx)
	})

	failpoint.Inject("InjectForceReconnect", func() {
		loopCount++
		s.startLoop(g, cancel, exitCh, regionFailureSourceWorkerSession, func() error {
			timer := time.NewTimer(10 * time.Second)
			defer timer.Stop()

			select {
			case <-gctx.Done():
				return gctx.Err()
			case <-timer.C:
			}

			err := errors.New("inject force reconnect")
			log.Info("inject force reconnect", zap.Error(err))
			return err
		})
	})

	waitErr := g.Wait()
	exits := make([]sessionLoopExit, 0, loopCount)
	for i := 0; i < loopCount; i++ {
		exits = append(exits, <-exitCh)
	}
	exit := pickPrimaryLoopExit(ctx, exits, waitErr)
	if errors.Cause(exit.err) == errWorkerSessionReconnect {
		exit.err = nil
	}
	if isCanceledByContext(ctx, exit.err) {
		return sessionRunResult{canceled: true}, nil
	}
	return sessionRunResult{
		failure: defaultWorkerSessionFailure(exit.source, exit.err),
	}, nil
}

func (s *regionRequestWorkerSession) waitBootstrapRegion(ctx context.Context) (regionInfo, error) {
	for {
		req, err := s.requestCache.pop(ctx)
		if err != nil {
			return regionInfo{}, err
		}
		if req.regionInfo.isStopped() {
			s.requestCache.markDone()
			continue
		}
		return req.regionInfo, nil
	}
}

func (s *regionRequestWorkerSession) checkStoreVersionFailure(err error) workerSessionFailure {
	if cerror.Is(err, cerror.ErrGetAllStoresFailed) {
		return workerSessionFailure{
			kind:   regionFailureKindGetStore,
			source: regionFailureSourceWorkerSession,
			cause:  err,
		}
	}
	return defaultWorkerSessionFailure(regionFailureSourceWorkerSession, err)
}

func (s *regionRequestWorkerSession) checkStoreVersion(ctx context.Context) error {
	if err := version.CheckStoreVersion(ctx, s.pd); err != nil {
		if isCanceledByContext(ctx, err) {
			return err
		}
		log.Error("event feed check store version fails",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.storeAddr),
			zap.Error(err))
		return err
	}
	return nil
}

func (s *regionRequestWorkerSession) connectStore(
	ctx context.Context,
) (*ConnAndClient, error) {
	log.Info("region request worker going to create grpc stream",
		zap.Uint64("workerID", s.workerID),
		zap.String("addr", s.storeAddr))

	conn, err := Connect(ctx, s.credential, s.storeAddr)
	if err != nil {
		log.Warn("region request worker create grpc stream failed",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.storeAddr),
			zap.Error(err))
		if conn != nil && conn.Conn != nil {
			_ = conn.Conn.Close()
		}
		return nil, err
	}

	return conn, nil
}

func (s *regionRequestWorkerSession) markRegionSent(region regionInfo, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.runtimeRegistry.markWaitInitialized(region.runtimeKey, s.workerID, now)
}

func (s *regionRequestWorkerSession) requestHeader() *cdcpb.Header {
	return &cdcpb.Header{
		ClusterId:    s.clusterID,
		TicdcVersion: version.ReleaseSemver(),
	}
}

func (s *regionRequestWorkerSession) newState(region regionInfo) *regionFeedState {
	return newRegionFeedState(
		region,
		uint64(region.subscribedSpan.subID),
		s.workerID,
		s.requestCache,
		s.runtimeRegistry,
		s.takeRegionState,
	)
}

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
	for _, state := range s.takeRegionStates(subID) {
		s.submitOrderedStateFailure(
			state,
			newRequestCancelledFailure(state.getRegionInfo(), regionFailureSourceDeregister),
		)
	}
}

func (s *regionRequestWorkerSession) dispatchRegionChangeEvents(events []*cdcpb.Event) {
	for _, event := range events {
		regionID := event.RegionId
		subscriptionID := SubscriptionID(event.RequestId)
		state := s.getRegionState(subscriptionID, regionID)
		if state != nil {
			regionEvent := regionEvent{
				states: []*regionFeedState{state},
			}
			switch eventData := event.Event.(type) {
			case *cdcpb.Event_Entries_:
				if eventData == nil {
					log.Warn("region request worker receives a region event with nil entries, ignore it",
						zap.Uint64("workerID", s.workerID),
						zap.Uint64("subscriptionID", uint64(subscriptionID)),
						zap.Uint64("regionID", regionID))
					continue
				}
				regionEvent.entries = eventData
			case *cdcpb.Event_Admin_:
				continue
			case *cdcpb.Event_Error:
				log.Debug("region request worker receives a region error",
					zap.Uint64("workerID", s.workerID),
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Uint64("regionID", event.RegionId),
					zap.Any("error", eventData.Error))
				s.submitOrderedStateFailure(state, newEventRegionFailure(state.getRegionInfo(), eventData.Error))
				continue
			case *cdcpb.Event_ResolvedTs:
				regionEvent.resolvedTs = eventData.ResolvedTs
			case *cdcpb.Event_LongTxn_:
				continue
			default:
				log.Panic("unknown event type", zap.Any("event", event))
			}
			s.emitRegionEvent(subscriptionID, regionEvent)
		} else {
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
		if state := s.getRegionState(subscriptionID, regionID); state != nil {
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

func (s *regionRequestWorkerSession) nextRegionRequest(ctx context.Context) (regionReq, error) {
	if s.bootstrapRegion != nil {
		req := *s.bootstrapRegion
		s.bootstrapRegion = nil
		return req, nil
	}
	return s.requestCache.pop(ctx)
}

func (s *regionRequestWorkerSession) emitRegionEvent(subID SubscriptionID, event regionEvent) {
	s.pushRegionEvent(subID, event)
}

func (s *regionRequestWorkerSession) handleStopTask(region regionInfo) error {
	subID := region.subscribedSpan.subID
	req := &cdcpb.ChangeDataRequest{
		Header:    s.requestHeader(),
		RequestId: uint64(subID),
		Request: &cdcpb.ChangeDataRequest_Deregister_{
			Deregister: &cdcpb.ChangeDataRequest_Deregister{},
		},
		FilterLoop: region.filterLoop,
	}
	s.requestCache.markDone()
	if err := s.sendRequest(req); err != nil {
		return err
	}
	s.cancelSubscriptionStates(subID)
	return nil
}

func (s *regionRequestWorkerSession) handleStoppedSubscription(region regionInfo) {
	s.failures.submitDirectFailure(newSubscriptionStoppedFailure(region))
	s.requestCache.markDone()
}

func (s *regionRequestWorkerSession) handleActiveRegionRequest(regionReq regionReq) error {
	region := regionReq.regionInfo
	subID := region.subscribedSpan.subID
	state := s.newState(region)
	state.start()
	s.addRegionState(subID, region.verID.GetID(), state)

	// Mark the request as sent before sending it to keep active-state tracking
	// and request-cache accounting visible in the same order.
	s.requestCache.markSent(regionReq)
	s.markRegionSent(region, time.Now())
	if err := s.sendRequest(s.createRegionRequest(region)); err != nil {
		state.markStopped(newSendRequestToStoreFailure(region, regionFailureSourceWorkerSend, err))
		return err
	}
	return nil
}

func (s *regionRequestWorkerSession) handleRegionSendTask(regionReq regionReq) error {
	region := regionReq.regionInfo
	switch {
	case region.isStopped():
		return s.handleStopTask(region)
	case region.subscribedSpan.stopped.Load():
		s.handleStoppedSubscription(region)
		return nil
	default:
		return s.handleActiveRegionRequest(regionReq)
	}
}

// processRegionSendTask receives region requests and sends them to the remote store.
func (s *regionRequestWorkerSession) processRegionSendTask(ctx context.Context) error {
	for {
		regionReq, err := s.nextRegionRequest(ctx)
		if err != nil {
			return err
		}

		region := regionReq.regionInfo
		subID := region.subscribedSpan.subID
		log.Debug("region request worker gets a singleRegionInfo",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.String("addr", s.storeAddr),
			zap.Bool("bdrMode", region.filterLoop))

		if err := s.handleRegionSendTask(regionReq); err != nil {
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

func (s *regionRequestWorkerSession) addRegionState(subscriptionID SubscriptionID, regionID uint64, state *regionFeedState) {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	states := s.requestedRegions.subscriptions[subscriptionID]
	if states == nil {
		states = make(regionFeedStates)
		s.requestedRegions.subscriptions[subscriptionID] = states
	}
	states[regionID] = state
}

func (s *regionRequestWorkerSession) getRegionState(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	s.requestedRegions.RLock()
	defer s.requestedRegions.RUnlock()
	if states, ok := s.requestedRegions.subscriptions[subscriptionID]; ok {
		return states[regionID]
	}
	return nil
}

func (s *regionRequestWorkerSession) takeRegionState(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	if statesMap, ok := s.requestedRegions.subscriptions[subscriptionID]; ok {
		state := statesMap[regionID]
		delete(statesMap, regionID)
		if len(statesMap) == 0 {
			delete(s.requestedRegions.subscriptions, subscriptionID)
		}
		return state
	}
	return nil
}

func (s *regionRequestWorkerSession) takeRegionStates(subscriptionID SubscriptionID) regionFeedStates {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	states := s.requestedRegions.subscriptions[subscriptionID]
	delete(s.requestedRegions.subscriptions, subscriptionID)
	return states
}

func (s *regionRequestWorkerSession) clearRegionStates() map[SubscriptionID]regionFeedStates {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	subscriptions := s.requestedRegions.subscriptions
	s.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)
	return subscriptions
}

func (s *regionRequestWorkerSession) takeNotStartedRegions() []regionInfo {
	regions := s.requestCache.drainPendingQueue()
	if s.bootstrapRegion == nil {
		return regions
	}
	region := s.bootstrapRegion.regionInfo
	s.bootstrapRegion = nil
	s.requestCache.markDone()
	return append([]regionInfo{region}, regions...)
}
