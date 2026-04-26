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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/version"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

type workerSessionFailureSnapshot struct {
	// startedRegions have been sent to TiKV and must recover through the
	// ordered dynamic-stream failure path.
	startedRegions map[SubscriptionID]regionFeedStates
	// pendingRegions were queued or processing locally but never became active
	// region states, so they can be retried directly.
	pendingRegions []regionInfo
}

var errWorkerSessionReconnect = errors.New("worker session reconnect")

// regionRequestWorkerSession owns one grpc stream session to one TiKV store.
//
// A session starts only after one bootstrap region request is available. It
// then checks store compatibility, opens the stream, and runs send/recv loops
// until either loop exits. Session failure recovery separates regions that were
// already active in TiKV from requests that were still local to the worker.
type regionRequestWorkerSession struct {
	workerID            uint64
	storeAddr           string
	pd                  pd.Client
	credential          *security.Credential
	clusterID           uint64
	requestCache        *requestCache
	runtimeRegistry     *regionRuntimeRegistry
	submitDirectFailure func(regionFailureInfo)
	pushRegionEvent     func(SubscriptionID, regionEvent)

	conn            *ConnAndClient
	bootstrapRegion *regionReq
	activeRegions   activeRegionStates
	stage           atomic.Uint32
}

func newRegionRequestWorkerSession(
	workerID uint64,
	storeAddr string,
	pd pd.Client,
	credential *security.Credential,
	clusterID uint64,
	requestCache *requestCache,
	runtimeRegistry *regionRuntimeRegistry,
	submitDirectFailure func(regionFailureInfo),
	pushRegionEvent func(SubscriptionID, regionEvent),
) *regionRequestWorkerSession {
	return &regionRequestWorkerSession{
		workerID:            workerID,
		storeAddr:           storeAddr,
		pd:                  pd,
		credential:          credential,
		clusterID:           clusterID,
		requestCache:        requestCache,
		runtimeRegistry:     runtimeRegistry,
		submitDirectFailure: submitDirectFailure,
		pushRegionEvent:     pushRegionEvent,
		activeRegions:       newActiveRegionStates(),
	}
}

func (s *regionRequestWorkerSession) close() {
	s.setStage(WorkerSessionStateDisconnected)
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

func stageToUint(stage WorkerSessionState) uint32 {
	switch stage {
	case WorkerSessionStateWaitingBootstrap:
		return 1
	case WorkerSessionStateCheckingStore:
		return 2
	case WorkerSessionStateConnecting:
		return 3
	case WorkerSessionStateRunning:
		return 4
	default:
		return 0
	}
}

func uintToStage(v uint32) WorkerSessionState {
	switch v {
	case 1:
		return WorkerSessionStateWaitingBootstrap
	case 2:
		return WorkerSessionStateCheckingStore
	case 3:
		return WorkerSessionStateConnecting
	case 4:
		return WorkerSessionStateRunning
	default:
		return WorkerSessionStateDisconnected
	}
}

func (s *regionRequestWorkerSession) setStage(stage WorkerSessionState) {
	s.stage.Store(stageToUint(stage))
}

func (s *regionRequestWorkerSession) sessionState() WorkerSessionState {
	return uintToStage(s.stage.Load())
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
	return exits[0]
}

func (s *regionRequestWorkerSession) run(ctx context.Context) (sessionRunResult, error) {
	s.setStage(WorkerSessionStateWaitingBootstrap)
	bootstrapRegion, err := s.waitBootstrapRegion(ctx)
	if err != nil {
		if isCanceledByContext(ctx, err) {
			return sessionRunResult{canceled: true}, nil
		}
		return sessionRunResult{}, err
	}
	s.bootstrapRegion = bootstrapRegion

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.setStage(WorkerSessionStateCheckingStore)
	if err := s.checkStoreVersion(sessionCtx); err != nil {
		if isCanceledByContext(ctx, err) {
			return sessionRunResult{canceled: true}, nil
		}
		return sessionRunResult{
			failure: s.checkStoreVersionFailure(err),
		}, nil
	}

	s.setStage(WorkerSessionStateConnecting)
	s.conn, err = s.connectStore(sessionCtx)
	if err != nil {
		if isCanceledByContext(ctx, err) {
			return sessionRunResult{canceled: true}, nil
		}
		return sessionRunResult{
			failure: defaultWorkerSessionFailure(regionFailureSourceWorkerSession, err),
		}, nil
	}

	s.setStage(WorkerSessionStateRunning)
	return s.runConnectedLoops(ctx, sessionCtx, cancel)
}

func (s *regionRequestWorkerSession) runConnectedLoops(
	parentCtx context.Context,
	sessionCtx context.Context,
	cancel context.CancelFunc,
) (sessionRunResult, error) {
	defer s.close()
	defer func() {
		log.Info("region request worker session exits",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.storeAddr),
			zap.Bool("canceled", parentCtx.Err() != nil))
	}()

	// sessionCtx owns the grpc stream lifetime, so canceling it will unblock
	// both send and recv loops consistently.
	g, gctx := errgroup.WithContext(sessionCtx)
	loopCount := 2
	exitCh := make(chan sessionLoopExit, 3)
	s.startLoop(g, cancel, exitCh, regionFailureSourceWorkerRecv, func() error {
		return s.receiveAndDispatchChangeEvents(gctx)
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
	exit := pickPrimaryLoopExit(parentCtx, exits, waitErr)
	if errors.Cause(exit.err) == errWorkerSessionReconnect {
		exit.err = nil
	}
	if isCanceledByContext(parentCtx, exit.err) {
		return sessionRunResult{canceled: true}, nil
	}
	return sessionRunResult{
		failure: defaultWorkerSessionFailure(exit.source, exit.err),
	}, nil
}

func (s *regionRequestWorkerSession) waitBootstrapRegion(ctx context.Context) (*regionReq, error) {
	for {
		req, err := s.requestCache.pop(ctx)
		if err != nil {
			return nil, err
		}
		if req.regionInfo.isStopped() {
			req.finish()
			continue
		}
		return req, nil
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
	s.runtimeRegistry.markRequestSent(region.runtimeKey, s.workerID, now)
}

func (s *regionRequestWorkerSession) requestHeader() *cdcpb.Header {
	return &cdcpb.Header{
		ClusterId:    s.clusterID,
		TicdcVersion: version.ReleaseSemver(),
	}
}

func (s *regionRequestWorkerSession) newState(request *regionReq) *regionFeedState {
	region := request.regionInfo
	return newRegionFeedState(
		region,
		uint64(region.subscribedSpan.subID),
		s.workerID,
		request,
		s.runtimeRegistry,
		s.activeRegions.take,
	)
}
