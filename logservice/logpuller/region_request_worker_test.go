// Copyright 2024 PingCAP, Inc.
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
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdopt "github.com/tikv/pd/client/opt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type mockEventFeedV2Client struct {
	sendErr error
	recvErr error
}

func (m *mockEventFeedV2Client) Send(*cdcpb.ChangeDataRequest) error   { return m.sendErr }
func (m *mockEventFeedV2Client) Recv() (*cdcpb.ChangeDataEvent, error) { return nil, m.recvErr }
func (m *mockEventFeedV2Client) Header() (metadata.MD, error)          { return metadata.MD{}, nil }
func (m *mockEventFeedV2Client) Trailer() metadata.MD                  { return metadata.MD{} }
func (m *mockEventFeedV2Client) CloseSend() error                      { return nil }
func (m *mockEventFeedV2Client) Context() context.Context              { return context.Background() }
func (m *mockEventFeedV2Client) SendMsg(any) error                     { return nil }
func (m *mockEventFeedV2Client) RecvMsg(any) error                     { return nil }

type mockSessionPDClient struct {
	pd.Client
	stores []*metapb.Store
	err    error
}

func (m *mockSessionPDClient) GetAllStores(context.Context, ...pdopt.GetStoreOption) ([]*metapb.Store, error) {
	return m.stores, m.err
}

func prepareRegionForSendTest(region regionInfo) regionInfo {
	region.rpcCtx = &tikv.RPCContext{
		Meta: &metapb.Region{
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		},
	}
	region.lockedRangeState = &regionlock.LockedRangeState{}
	region.lockedRangeState.ResolvedTs.Store(100)
	return region
}

func newTestRegionRequestWorker(requestCache *requestCache) *regionRequestWorker {
	if requestCache == nil {
		requestCache = newRequestCache(1)
	}
	client := &subscriptionClient{
		clusterID:             123,
		regionRuntimeRegistry: newRegionRuntimeRegistry(),
		ds:                    &mockRegionEventDynamicStream{},
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	client.cond = sync.NewCond(&client.mu)
	client.regionScheduler = newRegionRequestScheduler(client)
	return &regionRequestWorker{
		workerID:     1,
		client:       client,
		store:        &requestedStore{storeAddr: "store-1"},
		requestCache: requestCache,
	}
}

func newTestRegionRequestWorkerSession(
	worker *regionRequestWorker,
) *regionRequestWorkerSession {
	return newRegionRequestWorkerSession(
		worker.workerID,
		worker.store.storeAddr,
		worker.client,
		worker.requestCache,
	)
}

func TestRegionStatesOperation(t *testing.T) {
	session := &regionRequestWorkerSession{activeRegions: newActiveRegionStates()}

	require.Nil(t, session.activeRegions.get(1, 2))
	require.Nil(t, session.activeRegions.take(1, 2))

	session.activeRegions.add(1, 2, &regionFeedState{})
	require.NotNil(t, session.activeRegions.get(1, 2))
	require.NotNil(t, session.activeRegions.take(1, 2))
	require.Nil(t, session.activeRegions.get(1, 2))
	require.Equal(t, 0, len(session.activeRegions.subscriptions))

	session.activeRegions.add(1, 2, &regionFeedState{})
	require.NotNil(t, session.activeRegions.get(1, 2))
	require.NotNil(t, session.activeRegions.take(1, 2))
	require.Nil(t, session.activeRegions.get(1, 2))
	require.Equal(t, 0, len(session.activeRegions.subscriptions))
}

func TestTakeNotStartedRegionsReleaseSlotForBootstrapRegion(t *testing.T) {
	requestCache := newRequestCache(10)
	session := &regionRequestWorkerSession{
		requestCache: requestCache,
	}

	ctx := context.Background()
	region := createTestRegionInfo(1, 1)

	ok, err := requestCache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)

	req, err := requestCache.pop(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, requestCache.getPendingCount())

	session.bootstrapRegion = req
	snapshot := session.takeFailureSnapshot()
	require.Empty(t, snapshot.startedRegions)
	require.Len(t, snapshot.pendingRegions, 1)
	require.Equal(t, req.regionInfo.verID, snapshot.pendingRegions[0].verID)
	require.Equal(t, 0, requestCache.getPendingCount())
}

func TestSessionRunReturnsStartupFailureAndKeepsBootstrapRegion(t *testing.T) {
	requestCache := newRequestCache(10)
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))

	ok, err := requestCache.add(context.Background(), region, false)
	require.NoError(t, err)
	require.True(t, ok)

	worker := newTestRegionRequestWorker(requestCache)
	worker.client.pd = &mockSessionPDClient{
		stores: []*metapb.Store{{Id: 1, Address: "store-1", Version: "0.0.1"}},
	}
	session := newTestRegionRequestWorkerSession(worker)

	result, err := session.run(context.Background())
	require.NoError(t, err)
	require.False(t, result.canceled)
	require.Equal(t, regionFailureKindSendRequestToStore, result.failure.kind)
	require.Equal(t, regionFailureSourceWorkerSession, result.failure.source)
	require.Error(t, result.failure.cause)
	require.Nil(t, session.conn)

	snapshot := session.takeFailureSnapshot()
	require.Empty(t, snapshot.startedRegions)
	require.Len(t, snapshot.pendingRegions, 1)
	require.Equal(t, region.verID, snapshot.pendingRegions[0].verID)
	require.Equal(t, 0, requestCache.getPendingCount())
}

func TestWorkerAddDuplicateQueuedRequestRefreshesEnqueueTime(t *testing.T) {
	requestCache := newRequestCache(10)
	runtimeRegistry := newRegionRuntimeRegistry()
	worker := &regionRequestWorker{
		requestCache: requestCache,
		client: &subscriptionClient{
			regionRuntimeRegistry: runtimeRegistry,
		},
	}

	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))
	region.runtimeKey = runtimeRegistry.allocKey(region.subscribedSpan.subID, region.verID.GetID())
	runtimeRegistry.markDiscovered(region.runtimeKey, region, time.Now())

	ok, err := worker.add(context.Background(), region, false)
	require.NoError(t, err)
	require.True(t, ok)

	firstState, ok := runtimeRegistry.get(region.runtimeKey)
	require.True(t, ok)
	firstEnqueueTime := firstState.timeline.workerEnqueuedAt
	require.False(t, firstEnqueueTime.IsZero())

	time.Sleep(10 * time.Millisecond)

	ok, err = worker.add(context.Background(), region, false)
	require.NoError(t, err)
	require.True(t, ok)

	secondState, ok := runtimeRegistry.get(region.runtimeKey)
	require.True(t, ok)
	require.True(t, secondState.timeline.workerEnqueuedAt.After(firstEnqueueTime))
}

func TestWorkerAddDuplicateActiveRegionRequestsWarnAndKeepNewest(t *testing.T) {
	requestCache := newRequestCache(10)
	worker := &regionRequestWorker{
		workerID:     1,
		client:       &subscriptionClient{regionRuntimeRegistry: newRegionRuntimeRegistry()},
		store:        &requestedStore{storeAddr: "store-1"},
		requestCache: requestCache,
	}

	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))

	ok, err := requestCache.add(context.Background(), region, false)
	require.NoError(t, err)
	require.True(t, ok)

	req, err := requestCache.pop(context.Background())
	require.NoError(t, err)
	req.markSent()
	require.Equal(t, 1, requestCache.getPendingCount())

	ok, err = worker.add(context.Background(), region, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, requestCache.getPendingCount())

	latestReq, err := requestCache.pop(context.Background())
	require.NoError(t, err)
	latestReq.markSent()
	require.Equal(t, 1, requestCache.getPendingCount())
}

func TestRunConnectedLoopsReturnsReceiveFailure(t *testing.T) {
	session := newTestRegionRequestWorkerSession(newTestRegionRequestWorker(newRequestCache(1)))
	recvErr := errors.New("recv failed")
	session.conn = &ConnAndClient{
		Client: &mockEventFeedV2Client{recvErr: recvErr},
	}

	parentCtx := context.Background()
	sessionCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	result, err := session.runConnectedLoops(parentCtx, sessionCtx, cancel)
	require.NoError(t, err)
	require.False(t, result.canceled)
	require.Equal(t, regionFailureKindSendRequestToStore, result.failure.kind)
	require.Equal(t, regionFailureSourceWorkerRecv, result.failure.source)
	require.ErrorIs(t, result.failure.cause, recvErr)
}

func TestRunConnectedLoopsTreatsEOFAsReconnect(t *testing.T) {
	session := newTestRegionRequestWorkerSession(newTestRegionRequestWorker(newRequestCache(1)))
	session.conn = &ConnAndClient{
		Client: &mockEventFeedV2Client{recvErr: io.EOF},
	}

	parentCtx := context.Background()
	sessionCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	result, err := session.runConnectedLoops(parentCtx, sessionCtx, cancel)
	require.NoError(t, err)
	require.False(t, result.canceled)
	require.Equal(t, regionFailureKindSendRequestToStore, result.failure.kind)
	require.Equal(t, regionFailureSourceWorkerRecv, result.failure.source)
	require.NoError(t, result.failure.cause)
}

func TestRunConnectedLoopsPrefersSendFailureOverLoopCancellation(t *testing.T) {
	requestCache := newRequestCache(10)
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))
	req := newRegionReq(nil, region)

	session := newTestRegionRequestWorkerSession(newTestRegionRequestWorker(requestCache))
	session.bootstrapRegion = req
	sendErr := errors.New("send failed")
	session.conn = &ConnAndClient{
		Client: &mockEventFeedV2Client{
			sendErr: sendErr,
			recvErr: context.Canceled,
		},
	}

	parentCtx := context.Background()
	sessionCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	result, err := session.runConnectedLoops(parentCtx, sessionCtx, cancel)
	require.NoError(t, err)
	require.False(t, result.canceled)
	require.Equal(t, regionFailureKindSendRequestToStore, result.failure.kind)
	require.Equal(t, regionFailureSourceWorkerSend, result.failure.source)
	require.ErrorIs(t, result.failure.cause, sendErr)
}

type pushedResolvedEvent struct {
	subscriptionID SubscriptionID
	resolvedTs     uint64
	statesCount    int
}

type mockRegionEventDynamicStream struct {
	pushCount   int
	totalStates int
	pushed      []pushedResolvedEvent
}

func (m *mockRegionEventDynamicStream) Start() {}

func (m *mockRegionEventDynamicStream) Close() {}

func (m *mockRegionEventDynamicStream) Push(path SubscriptionID, event regionEvent) {
	m.pushCount++
	m.totalStates += len(event.states)
	m.pushed = append(m.pushed, pushedResolvedEvent{
		subscriptionID: path,
		resolvedTs:     event.resolvedTs,
		statesCount:    len(event.states),
	})
}

func (m *mockRegionEventDynamicStream) Wake(SubscriptionID) {}

func (m *mockRegionEventDynamicStream) Feedback() <-chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan] {
	return nil
}

func (m *mockRegionEventDynamicStream) AddPath(SubscriptionID, *subscribedSpan, ...dynstream.AreaSettings) error {
	return nil
}

func (m *mockRegionEventDynamicStream) RemovePath(SubscriptionID) error {
	return nil
}

func (m *mockRegionEventDynamicStream) Release(SubscriptionID) {}

func (m *mockRegionEventDynamicStream) SetAreaSettings(int, dynstream.AreaSettings) {}

func (m *mockRegionEventDynamicStream) GetMetrics() dynstream.Metrics[int, SubscriptionID] {
	return dynstream.Metrics[int, SubscriptionID]{}
}

func newDispatchResolvedTsTestSession(regionCount int) (*regionRequestWorkerSession, *mockRegionEventDynamicStream, *cdcpb.ResolvedTs) {
	ds := &mockRegionEventDynamicStream{}
	client := &subscriptionClient{
		ds: ds,
	}
	session := &regionRequestWorkerSession{
		client:        client,
		activeRegions: newActiveRegionStates(),
	}
	session.activeRegions.subscriptions = map[SubscriptionID]regionFeedStates{
		1: make(regionFeedStates, regionCount),
	}
	regions := make([]uint64, regionCount)
	for i := 0; i < regionCount; i++ {
		regionID := uint64(i + 1)
		regions[i] = regionID
		session.activeRegions.subscriptions[1][regionID] = &regionFeedState{
			requestID: 1,
		}
	}

	return session, ds, &cdcpb.ResolvedTs{
		RequestId: 1,
		Ts:        100,
		Regions:   regions,
	}
}

func dispatchResolvedTsEventLegacyForBenchmark(s *regionRequestWorkerSession, resolvedTsEvent *cdcpb.ResolvedTs) {
	subscriptionID := SubscriptionID(resolvedTsEvent.RequestId)
	const resolvedTsStateBatchSize = 1024
	resolvedStates := make([]*regionFeedState, 0, resolvedTsStateBatchSize)
	flush := func() {
		if len(resolvedStates) == 0 {
			return
		}
		states := resolvedStates
		s.emitRegionEvent(subscriptionID, regionEvent{
			resolvedTs: resolvedTsEvent.Ts,
			states:     states,
		})
		resolvedStates = make([]*regionFeedState, 0, resolvedTsStateBatchSize)
	}
	for _, regionID := range resolvedTsEvent.Regions {
		if state := s.activeRegions.get(subscriptionID, regionID); state != nil {
			resolvedStates = append(resolvedStates, state)
			if len(resolvedStates) >= resolvedTsStateBatchSize {
				flush()
			}
		}
	}
	flush()
}

func benchmarkDispatchResolvedTsEvent(b *testing.B, regionCount int, useLegacy bool) {
	session, ds, event := newDispatchResolvedTsTestSession(regionCount)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if useLegacy {
			dispatchResolvedTsEventLegacyForBenchmark(session, event)
		} else {
			session.dispatchResolvedTsEvent(event)
		}
	}
	b.StopTimer()
	if ds.pushCount == 0 {
		b.Fatal("expected at least one push")
	}
}

func TestDispatchResolvedTsEventSingleRegion(t *testing.T) {
	session, ds, event := newDispatchResolvedTsTestSession(1)
	session.dispatchResolvedTsEvent(event)

	require.Equal(t, 1, ds.pushCount)
	require.Equal(t, 1, ds.totalStates)
	require.Len(t, ds.pushed, 1)
	require.Equal(t, 1, ds.pushed[0].statesCount)
	require.Equal(t, uint64(100), ds.pushed[0].resolvedTs)
	require.Equal(t, SubscriptionID(1), ds.pushed[0].subscriptionID)
}

func TestDispatchResolvedTsEventBatchSplitForLargeRegions(t *testing.T) {
	session, ds, event := newDispatchResolvedTsTestSession(2050)
	session.dispatchResolvedTsEvent(event)

	require.Equal(t, 2050, ds.totalStates)
	require.Len(t, ds.pushed, 3)
	require.Equal(t, 1024, ds.pushed[0].statesCount)
	require.Equal(t, 1024, ds.pushed[1].statesCount)
	require.Equal(t, 2, ds.pushed[2].statesCount)
}

func BenchmarkDispatchResolvedTsEventSingleRegionLegacy(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 1, true)
}

func BenchmarkDispatchResolvedTsEventSingleRegionCurrent(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 1, false)
}

func BenchmarkDispatchResolvedTsEventLargeBatchLegacy(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 4096, true)
}

func BenchmarkDispatchResolvedTsEventLargeBatchCurrent(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 4096, false)
}

func BenchmarkDispatchResolvedTsEventSmallBatchLegacy(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 16, true)
}

func BenchmarkDispatchResolvedTsEventSmallBatchCurrent(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 16, false)
}

func TestTakeNotStartedRegionsDoesNotReturnStoppedSentRegion(t *testing.T) {
	requestCache := newRequestCache(10)
	session := &regionRequestWorkerSession{
		requestCache:  requestCache,
		activeRegions: newActiveRegionStates(),
	}

	ctx := context.Background()
	region := createTestRegionInfo(1, 1)

	ok, err := requestCache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)

	req, err := requestCache.pop(ctx)
	require.NoError(t, err)

	state := newRegionFeedState(
		req.regionInfo,
		uint64(req.regionInfo.subscribedSpan.subID),
		0,
		req,
		nil,
		session.activeRegions.take,
	)
	state.start()
	session.activeRegions.add(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID(), state)

	// Simulate the race we are fixing in processRegionSendTask:
	// once a request is visible as sent, a fast region error may mark the
	// region stopped before worker cleanup runs. In that case, markStopped should
	// remove the sent request immediately, so takeNotStartedRegions must not return
	// the stale region again during worker shutdown.
	req.markSent()
	state.markStopped(newSendRequestToStoreFailure(
		req.regionInfo,
		regionFailureSourceWorkerSend,
		errors.New("send request to store error"),
	))
	session.activeRegions.take(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID())

	require.Equal(t, 0, requestCache.getPendingCount())
	snapshot := session.takeFailureSnapshot()
	require.Empty(t, snapshot.startedRegions)
	require.Empty(t, snapshot.pendingRegions)
}

func TestTakeNotStartedRegionsDoesNotIncludeActiveSentRegion(t *testing.T) {
	requestCache := newRequestCache(10)
	session := &regionRequestWorkerSession{
		requestCache:  requestCache,
		activeRegions: newActiveRegionStates(),
	}

	ctx := context.Background()
	region := createTestRegionInfo(1, 1)

	ok, err := requestCache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)

	req, err := requestCache.pop(ctx)
	require.NoError(t, err)

	state := newRegionFeedState(
		req.regionInfo,
		uint64(req.regionInfo.subscribedSpan.subID),
		0,
		req,
		nil,
		session.activeRegions.take,
	)
	state.start()
	session.activeRegions.add(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID(), state)
	req.markSent()

	snapshot := session.takeFailureSnapshot()
	require.Empty(t, snapshot.pendingRegions)
	require.Equal(t, 1, len(snapshot.startedRegions[req.regionInfo.subscribedSpan.subID]))
	require.Equal(t, 1, requestCache.getPendingCount())
}

func TestProcessRegionSendTaskSendFailureCleansSentRequest(t *testing.T) {
	worker := &regionRequestWorker{
		requestCache: newRequestCache(10),
		store:        &requestedStore{storeAddr: "store-1"},
		client:       &subscriptionClient{regionRuntimeRegistry: newRegionRuntimeRegistry()},
	}

	ctx := context.Background()
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))

	ok, err := worker.requestCache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, worker.requestCache.getPendingCount())

	req, err := worker.requestCache.pop(ctx)
	require.NoError(t, err)

	sendErr := errors.New("send failed")
	conn := &ConnAndClient{
		Client: &mockEventFeedV2Client{sendErr: sendErr},
		Conn:   &grpc.ClientConn{},
	}

	session := newTestRegionRequestWorkerSession(worker)
	session.conn = conn
	session.bootstrapRegion = req
	err = session.processRegionSendTask(ctx)
	require.ErrorIs(t, err, sendErr)
	require.Equal(t, 0, worker.requestCache.getPendingCount())
	state := session.activeRegions.get(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID())
	require.True(t, state == nil || state.isStale(), "region state should be removed or marked stale after send failure")
	if state != nil {
		failure, ok := state.detachFailure()
		require.True(t, ok)
		require.Equal(t, regionFailureKindSendRequestToStore, failure.kind)
		require.Equal(t, regionFailureSourceWorkerSend, failure.source)
		require.Contains(t, failure.err.Error(), "send failed")
	}
}
