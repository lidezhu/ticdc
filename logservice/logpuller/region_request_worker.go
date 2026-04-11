// Copyright 2023 PingCAP, Inc.
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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	grpcstatus "google.golang.org/grpc/status"
)

// To generate a workerID in `newRegionRequestWorker`.
var workerIDGen atomic.Uint64

type regionFeedStates map[uint64]*regionFeedState

// regionRequestWorker is responsible for sending region requests to a specific TiKV store.
// It owns the long-lived request cache and reconnection loop.
type regionRequestWorker struct {
	workerID uint64

	client *subscriptionClient

	store *requestedStore

	// We must always get a region to request before creating a grpc stream.
	// Only in this way can we avoid trying to connect to an offline store infinitely.
	preFetchForConnecting *regionInfo

	// request cache with flow control
	requestCache *requestCache
}

// regionWorkerSession owns a single grpc stream session to one TiKV store.
// All grpc send/recv logic and active region state tracking stay inside this object.
type regionWorkerSession struct {
	client       *subscriptionClient
	workerID     uint64
	storeAddr    string
	conn         *ConnAndClient
	requestCache *requestCache

	controller *regionStateController
	bootstrap  *regionReq

	requestedRegions struct {
		sync.RWMutex
		subscriptions map[SubscriptionID]regionFeedStates
	}

	failureOnce sync.Once
	failure     workerSessionFailure
}

func newRegionWorkerSession(
	client *subscriptionClient,
	workerID uint64,
	storeAddr string,
	conn *ConnAndClient,
	requestCache *requestCache,
	bootstrap regionInfo,
) *regionWorkerSession {
	session := &regionWorkerSession{
		client:       client,
		workerID:     workerID,
		storeAddr:    storeAddr,
		conn:         conn,
		requestCache: requestCache,
	}
	session.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)
	session.controller = newRegionStateController(workerID, client, requestCache, session.takeRegionState)

	req := newRegionReq(bootstrap)
	session.bootstrap = &req
	return session
}

func (s *regionWorkerSession) recordFailure(source regionFailureSource, err error) {
	if err == nil {
		return
	}
	s.failureOnce.Do(func() {
		s.failure = workerSessionFailure{
			kind:   regionFailureKindSendRequestToStore,
			source: source,
			cause:  err,
		}
	})
}

func (s *regionWorkerSession) runReceiveLoop() error {
	err := s.receiveAndDispatchChangeEvents()
	s.recordFailure(regionFailureSourceWorkerRecv, err)
	return err
}

func (s *regionWorkerSession) runSendLoop(ctx context.Context) error {
	err := s.processRegionSendTask(ctx)
	s.recordFailure(regionFailureSourceWorkerSend, err)
	return err
}

func (s *regionWorkerSession) failureOrDefault() workerSessionFailure {
	if s.failure.kind != "" {
		return s.failure
	}
	return workerSessionFailure{
		kind:   regionFailureKindSendRequestToStore,
		source: regionFailureSourceWorkerSession,
	}
}

func (s *regionWorkerSession) run(ctx context.Context) workerSessionFailure {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return s.runReceiveLoop() })
	g.Go(func() error { return s.runSendLoop(gctx) })

	failpoint.Inject("InjectForceReconnect", func() {
		timer := time.After(10 * time.Second)
		g.Go(func() error {
			<-timer
			err := errors.New("inject force reconnect")
			log.Info("inject force reconnect", zap.Error(err))
			s.recordFailure(regionFailureSourceWorkerSession, err)
			return err
		})
	})

	_ = g.Wait()
	return s.failureOrDefault()
}

func (s *regionWorkerSession) runtimeRegistry() *regionRuntimeRegistry {
	return s.controller.runtimeRegistry()
}

func (s *regionWorkerSession) markRegionRuntimeSent(region regionInfo, now time.Time) {
	if registry := s.runtimeRegistry(); registry != nil && region.runtimeKey.isValid() {
		registry.markWaitInitialized(region.runtimeKey, s.workerID, now)
	}
}

func (s *regionRequestWorker) runtimeRegistry() *regionRuntimeRegistry {
	if s.client == nil {
		return nil
	}
	return s.client.regionRuntimeRegistry
}

func (s *regionRequestWorker) markRegionRuntimeEnqueued(region regionInfo, now time.Time) {
	if registry := s.runtimeRegistry(); registry != nil && region.runtimeKey.isValid() {
		registry.setRequestEnqueueTime(region.runtimeKey, now)
	}
}

func (s *regionRequestWorker) waitPreFetchedRegion(ctx context.Context) error {
	if s.preFetchForConnecting != nil {
		log.Panic("preFetchForConnecting should be nil",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.store.storeAddr))
	}
	for {
		req, err := s.requestCache.pop(ctx)
		if err != nil {
			return err
		}
		if req.regionInfo.isStopped() {
			s.requestCache.markDone()
			continue
		}
		s.preFetchForConnecting = new(regionInfo)
		*s.preFetchForConnecting = req.regionInfo
		return nil
	}
}

func (s *regionRequestWorker) checkStoreVersion(ctx context.Context) (workerSessionFailure, bool) {
	if err := version.CheckStoreVersion(ctx, s.client.pd); err != nil {
		if errors.Cause(err) == context.Canceled {
			return workerSessionFailure{}, true
		}
		log.Error("event feed check store version fails",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.store.storeAddr),
			zap.Error(err))
		if cerror.Is(err, cerror.ErrGetAllStoresFailed) {
			return workerSessionFailure{
				kind:   regionFailureKindGetStore,
				source: regionFailureSourceWorkerSession,
				cause:  err,
			}, false
		}
		return workerSessionFailure{
			kind:   regionFailureKindSendRequestToStore,
			source: regionFailureSourceWorkerSession,
			cause:  err,
		}, false
	}
	return workerSessionFailure{}, false
}

func (s *regionRequestWorker) collectPendingRegions(session *regionWorkerSession) []regionInfo {
	pendingRegions := s.clearPendingRegions()
	if session == nil {
		return pendingRegions
	}
	return append(session.clearUnsentRegions(), pendingRegions...)
}

func (s *regionRequestWorker) handleSessionFailure(session *regionWorkerSession, sessionFailure workerSessionFailure) {
	// A store/session failure fans out into ordered failures for started regions
	// and direct failures for requests that never became active states.
	s.client.failures.submitWorkerSessionFailure(
		session,
		s.collectPendingRegions(session),
		sessionFailure,
	)
}

func (s *regionRequestWorker) runNextSession(
	ctx context.Context,
	credential *security.Credential,
) (*regionWorkerSession, workerSessionFailure, bool, error) {
	if err := s.waitPreFetchedRegion(ctx); err != nil {
		return nil, workerSessionFailure{}, false, err
	}

	sessionFailure, canceled := s.checkStoreVersion(ctx)
	if canceled || sessionFailure.kind != "" {
		return nil, sessionFailure, canceled, nil
	}

	session, sessionFailure, canceled := s.runSession(ctx, credential)
	return session, sessionFailure, canceled, nil
}

func (s *regionRequestWorker) runSessionLoop(
	ctx context.Context,
	credential *security.Credential,
) error {
	for {
		session, sessionFailure, canceled, err := s.runNextSession(ctx, credential)
		if err != nil {
			return err
		}
		if canceled {
			return nil
		}
		s.handleSessionFailure(session, sessionFailure)

		if err := util.Hang(ctx, time.Second); err != nil {
			return err
		}
	}
}

func newRegionRequestWorker(
	ctx context.Context,
	client *subscriptionClient,
	credential *security.Credential,
	g *errgroup.Group,
	store *requestedStore,
	requestCacheSize int,
) *regionRequestWorker {
	worker := &regionRequestWorker{
		workerID:     workerIDGen.Add(1),
		client:       client,
		store:        store,
		requestCache: newRequestCache(requestCacheSize),
	}

	g.Go(func() error {
		return worker.runSessionLoop(ctx, credential)
	})

	return worker
}

func (s *regionRequestWorker) runSession(
	ctx context.Context,
	credential *security.Credential,
) (session *regionWorkerSession, sessionFailure workerSessionFailure, canceled bool) {
	isCanceled := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	}

	log.Info("region request worker going to create grpc stream",
		zap.Uint64("workerID", s.workerID),
		zap.String("addr", s.store.storeAddr))

	defer func() {
		log.Info("region request worker exits",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.store.storeAddr),
			zap.Bool("canceled", canceled))
	}()

	conn, err := Connect(ctx, credential, s.store.storeAddr)
	if err != nil {
		log.Warn("region request worker create grpc stream failed",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.store.storeAddr),
			zap.Error(err))
		// Close the connection if it was partially created to prevent goroutine leaks
		if conn != nil && conn.Conn != nil {
			_ = conn.Conn.Close()
		}
		return nil, workerSessionFailure{
			kind:   regionFailureKindSendRequestToStore,
			source: regionFailureSourceWorkerSession,
			cause:  err,
		}, isCanceled()
	}
	defer func() {
		_ = conn.Conn.Close()
	}()

	if s.preFetchForConnecting == nil {
		log.Panic("preFetchForConnecting should not be nil",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.store.storeAddr))
	}

	session = newRegionWorkerSession(
		s.client,
		s.workerID,
		s.store.storeAddr,
		conn,
		s.requestCache,
		*s.preFetchForConnecting,
	)
	s.preFetchForConnecting = nil

	sessionFailure = session.run(ctx)
	return session, sessionFailure, isCanceled()
}

// receiveAndDispatchChangeEvents receives events from the grpc stream and dispatches them to ds.
func (s *regionWorkerSession) receiveAndDispatchChangeEvents() error {
	for {
		changeEvent, err := s.conn.Client.Recv()
		if err != nil {
			log.Info("region request worker receive from grpc stream failed",
				zap.Uint64("workerID", s.workerID),
				zap.String("addr", s.storeAddr),
				zap.String("code", grpcstatus.Code(err).String()),
				zap.Error(err))
			if StatusIsEOF(grpcstatus.Convert(err)) {
				return nil
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

func (s *regionWorkerSession) submitOrderedStateFailure(state *regionFeedState, failure regionFailureInfo) {
	state.markStopped(failure)
	s.client.pushRegionEventToDS(SubscriptionID(state.requestID), regionEvent{
		states: []*regionFeedState{state},
	})
}

func (s *regionWorkerSession) cancelSubscriptionStates(subID SubscriptionID) {
	for _, state := range s.takeRegionStates(subID) {
		s.submitOrderedStateFailure(
			state,
			newRequestCancelledFailure(state.getRegionInfo(), regionFailureSourceDeregister),
		)
	}
}

func (s *regionWorkerSession) dispatchRegionChangeEvents(events []*cdcpb.Event) {
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
			s.client.pushRegionEventToDS(subscriptionID, regionEvent)
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

func (s *regionWorkerSession) dispatchResolvedTsEvent(resolvedTsEvent *cdcpb.ResolvedTs) {
	subscriptionID := SubscriptionID(resolvedTsEvent.RequestId)
	metricsResolvedTsCount.Add(float64(len(resolvedTsEvent.Regions)))
	s.client.metrics.batchResolvedSize.Observe(float64(len(resolvedTsEvent.Regions)))
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
		s.client.pushRegionEventToDS(subscriptionID, regionEvent{
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

func (s *regionWorkerSession) sendRequest(req *cdcpb.ChangeDataRequest) error {
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

func (s *regionWorkerSession) nextRegionRequest(ctx context.Context) (regionReq, error) {
	if s.bootstrap != nil {
		req := *s.bootstrap
		s.bootstrap = nil
		return req, nil
	}
	return s.requestCache.pop(ctx)
}

func (s *regionWorkerSession) handleStopTask(region regionInfo) error {
	subID := region.subscribedSpan.subID
	req := &cdcpb.ChangeDataRequest{
		Header:    &cdcpb.Header{ClusterId: s.client.clusterID, TicdcVersion: version.ReleaseSemver()},
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

func (s *regionWorkerSession) handleStoppedSubscription(region regionInfo) {
	s.client.failures.submitDirectFailure(newSubscriptionStoppedFailure(region))
	s.requestCache.markDone()
}

func (s *regionWorkerSession) handleActiveRegionRequest(regionReq regionReq) error {
	region := regionReq.regionInfo
	subID := region.subscribedSpan.subID
	state := newRegionFeedState(region, uint64(subID), s.controller)
	state.start()
	s.addRegionState(subID, region.verID.GetID(), state)

	// Mark the request as sent before sending it to keep active-state tracking
	// and request-cache accounting visible in the same order.
	s.requestCache.markSent(regionReq)
	s.markRegionRuntimeSent(region, time.Now())
	if err := s.sendRequest(s.createRegionRequest(region)); err != nil {
		state.markStopped(newSendRequestToStoreFailure(region, regionFailureSourceWorkerSend, err))
		return err
	}
	return nil
}

func (s *regionWorkerSession) handleRegionSendTask(regionReq regionReq) error {
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
func (s *regionWorkerSession) processRegionSendTask(ctx context.Context) error {
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

func (s *regionWorkerSession) createRegionRequest(region regionInfo) *cdcpb.ChangeDataRequest {
	return &cdcpb.ChangeDataRequest{
		Header:       &cdcpb.Header{ClusterId: s.client.clusterID, TicdcVersion: version.ReleaseSemver()},
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

func (s *regionWorkerSession) addRegionState(subscriptionID SubscriptionID, regionID uint64, state *regionFeedState) {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	states := s.requestedRegions.subscriptions[subscriptionID]
	if states == nil {
		states = make(regionFeedStates)
		s.requestedRegions.subscriptions[subscriptionID] = states
	}
	states[regionID] = state
}

func (s *regionWorkerSession) getRegionState(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	s.requestedRegions.RLock()
	defer s.requestedRegions.RUnlock()
	if states, ok := s.requestedRegions.subscriptions[subscriptionID]; ok {
		return states[regionID]
	}
	return nil
}

func (s *regionWorkerSession) takeRegionState(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
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

func (s *regionWorkerSession) takeRegionStates(subscriptionID SubscriptionID) regionFeedStates {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	states := s.requestedRegions.subscriptions[subscriptionID]
	delete(s.requestedRegions.subscriptions, subscriptionID)
	return states
}

func (s *regionWorkerSession) clearRegionStates() map[SubscriptionID]regionFeedStates {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	subscriptions := s.requestedRegions.subscriptions
	s.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)
	return subscriptions
}

func (s *regionWorkerSession) clearUnsentRegions() []regionInfo {
	if s.bootstrap == nil {
		return nil
	}
	region := s.bootstrap.regionInfo
	s.bootstrap = nil
	return []regionInfo{region}
}

// add adds a region request to the worker's cache.
// It blocks if the cache is full until there's space or ctx is cancelled.
func (s *regionRequestWorker) add(ctx context.Context, region regionInfo, force bool) (bool, error) {
	ok, err := s.requestCache.add(ctx, region, force)
	if ok && err == nil {
		s.markRegionRuntimeEnqueued(region, time.Now())
	}
	return ok, err
}

func (s *regionRequestWorker) clearPendingRegions() []regionInfo {
	var regions []regionInfo

	// Clear pre-fetched region. This only happens before a session is created.
	if s.preFetchForConnecting != nil {
		region := *s.preFetchForConnecting
		s.preFetchForConnecting = nil
		regions = append(regions, region)
		// The pre-fetched region was popped from pendingQueue but hasn't been
		// marked as sent or done yet. Release its pendingCount slot to avoid
		// leaking flow control credits on worker failures.
		s.requestCache.markDone()
	}

	cacheRegions := s.requestCache.clear()
	regions = append(regions, cacheRegions...)
	return regions
}
