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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/logservice/txnutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/dynstream"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// Maximum total sleep time(in ms), 20 seconds.
	tikvRequestMaxBackoff = 20000

	// TiCDC always interacts with region leader, every time something goes wrong,
	// failed region will be reloaded via `BatchLoadRegionsWithKeyRange` API. So we
	// don't need to force reload region anymore.
	regionScheduleReload = false

	loadRegionRetryInterval time.Duration = 100 * time.Millisecond
	resolveLockMinInterval  time.Duration = 10 * time.Second
	resolveLockTickInterval time.Duration = 2 * time.Second
	resolveLockFence        time.Duration = 4 * time.Second

	// resolveLastRunGCThreshold is the size threshold to GC resolveLastRun and drop stale entries.
	resolveLastRunGCThreshold = 1024
)

var (
	metricFeedNotLeaderCounter        = metrics.EventFeedErrorCounter.WithLabelValues("NotLeader")
	metricFeedEpochNotMatchCounter    = metrics.EventFeedErrorCounter.WithLabelValues("EpochNotMatch")
	metricFeedRegionNotFoundCounter   = metrics.EventFeedErrorCounter.WithLabelValues("RegionNotFound")
	metricFeedDuplicateRequestCounter = metrics.EventFeedErrorCounter.WithLabelValues("DuplicateRequest")
	metricFeedUnknownErrorCounter     = metrics.EventFeedErrorCounter.WithLabelValues("Unknown")
	metricFeedRPCCtxUnavailable       = metrics.EventFeedErrorCounter.WithLabelValues("RPCCtxUnavailable")
	metricGetStoreErr                 = metrics.EventFeedErrorCounter.WithLabelValues("GetStoreErr")
	metricStoreSendRequestErr         = metrics.EventFeedErrorCounter.WithLabelValues("SendRequestToStore")
	metricKvIsBusyCounter             = metrics.EventFeedErrorCounter.WithLabelValues("KvIsBusy")
	metricKvCongestedCounter          = metrics.EventFeedErrorCounter.WithLabelValues("KvCongested")
	metricBatchResolvedEventSize      = metrics.BatchResolvedEventSize.WithLabelValues("event-store")

	metricSubscriptionClientDSChannelSize     = metrics.DynamicStreamEventChanSize.WithLabelValues("event-store")
	metricSubscriptionClientDSPendingQueueLen = metrics.DynamicStreamPendingQueueLen.WithLabelValues("event-store")
)

// To generate an ID for a new subscription.
var subscriptionIDGen atomic.Uint64

// subscriptionID is a unique identifier for a subscription.
// It is used as `RequestId` in region requests to remote store.
type SubscriptionID uint64

const InvalidSubscriptionID SubscriptionID = 0

type resolveLockTask struct {
	keyspaceID uint32
	regionID   uint64
	targetTs   uint64
	state      *regionlock.LockedRangeState
	create     time.Time
}

// rangeTask represents a task to subscribe a range span of a table.
// It can be a part of a table or a whole table, it also can be a part of a region.
type rangeTask struct {
	span           heartbeatpb.TableSpan
	subscribedSpan *subscribedSpan
	filterLoop     bool
	priority       TaskType
}

const kvEventsCacheMaxSize = 32

// subscribedSpan represents a span to subscribe.
// It contains a sub span of a table(or the total span of a table),
// the startTs of the table, and the output event channel.
type subscribedSpan struct {
	subID   SubscriptionID
	startTs uint64
	// Whether to filter out the value written by TiCDC itself.
	// It should be `true` in BDR mode.
	filterLoop bool

	// The target span
	span heartbeatpb.TableSpan
	// The range lock of the span,
	// it is used to prevent duplicate requests to the same region range,
	// and it also used to calculate this table's resolvedTs.
	rangeLock *regionlock.RangeLock

	consumeKVEvents func(events []common.RawKVEntry, wakeCallback func()) bool

	advanceResolvedTs func(ts uint64)

	advanceInterval int64

	kvEventsCache []common.RawKVEntry

	// To handle span removing.
	stopped atomic.Bool

	// To handle stale lock resolvings.
	tryResolveLock     func(regionID uint64, state *regionlock.LockedRangeState)
	staleLocksTargetTs atomic.Uint64

	lastAdvanceTime atomic.Int64

	initialized       atomic.Bool
	resolvedTsUpdated atomic.Int64
	resolvedTs        atomic.Uint64
}

func (span *subscribedSpan) clearKVEventsCache() {
	if cap(span.kvEventsCache) > kvEventsCacheMaxSize {
		span.kvEventsCache = nil
	} else {
		span.kvEventsCache = span.kvEventsCache[:0]
	}
}

type SubscriptionClientConfig struct {
	// The number of region request workers to send region task for every tikv store
	RegionRequestWorkerPerStore uint
}

// subscriptionClient is used to subscribe events of table ranges from TiKV.
// All exported Methods are thread-safe.
type SubscriptionClient interface {
	common.SubModule
	// allocate a unique id for the subscription
	AllocSubscriptionID() SubscriptionID
	// subscribe a table span
	Subscribe(
		subID SubscriptionID,
		span heartbeatpb.TableSpan,
		startTs uint64,
		consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
		advanceResolvedTs func(ts uint64),
		advanceInterval int64,
		bdrMode bool,
	)
	// unsubscribe a table span
	Unsubscribe(subID SubscriptionID)
}

type subscriptionClient struct {
	ctx       context.Context
	cancel    context.CancelFunc
	config    *SubscriptionClientConfig
	clusterID uint64

	regionRuntimeRegistry *regionRuntimeRegistry

	pd           pd.Client
	regionCache  *tikv.RegionCache
	pdClock      pdutil.Clock
	lockResolver txnutil.LockResolver

	stores sync.Map

	ds dynstream.DynamicStream[int, SubscriptionID, regionEvent, *subscribedSpan, *regionEventHandler]
	// the following three fields are used to manage feedback from ds and notify other goroutines
	mu     sync.Mutex
	cond   *sync.Cond
	paused atomic.Bool

	// the credential to connect tikv
	credential *security.Credential

	totalSpans struct {
		sync.RWMutex
		spanMap map[SubscriptionID]*subscribedSpan
	}

	// rangeTaskCh is used to receive range tasks.
	// The tasks will be handled in `handleRangeTasks` goroutine.
	rangeTaskCh chan rangeTask
	// regionTaskQueue is used to receive region tasks with priority.
	// The tasks will be handled in `handleRegions` goroutine.
	regionTaskQueue *PriorityQueue
	// resolveLockTaskCh is used to receive resolve lock tasks.
	// The tasks will be handled in `handleResolveLockTasks` goroutine.
	resolveLockTaskCh chan resolveLockTask
	// failureBuffer serializes region failures before recovery.
	failureBuffer *failureBuffer
}

func (s *subscriptionClient) ensureRegionRuntime(region *regionInfo, now time.Time) {
	if s.regionRuntimeRegistry == nil {
		return
	}
	if region == nil || region.subscribedSpan == nil {
		return
	}
	if region.verID.GetID() == 0 {
		return
	}
	if !region.runtimeKey.isValid() {
		region.runtimeKey = s.regionRuntimeRegistry.allocKey(region.subscribedSpan.subID, region.verID.GetID())
		s.regionRuntimeRegistry.registerRegion(region.runtimeKey, *region, now)
	}
}

func (s *subscriptionClient) updateRegionRuntimeInfo(region regionInfo) {
	if s.regionRuntimeRegistry == nil {
		return
	}
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.updateRegionInfo(region.runtimeKey, region)
}

func (s *subscriptionClient) transitionRegionRuntime(region regionInfo, phase regionPhase, now time.Time) {
	if s.regionRuntimeRegistry == nil {
		return
	}
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.transition(region.runtimeKey, phase, now)
}

func (s *subscriptionClient) markRegionRetryPending(region regionInfo, err error, now time.Time) {
	if s.regionRuntimeRegistry == nil || !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markRetryPending(region.runtimeKey, err, now)
}

func (s *subscriptionClient) markRegionRPCReady(region regionInfo, now time.Time) {
	if s.regionRuntimeRegistry == nil || !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markRPCReady(region.runtimeKey, now)
}

func (s *subscriptionClient) markRegionQueued(region regionInfo, acquiredTime, queuedTime time.Time) {
	if s.regionRuntimeRegistry == nil || !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markQueued(region.runtimeKey, acquiredTime, queuedTime)
}

func (s *subscriptionClient) recordRegionRuntimeError(region regionInfo, err error, now time.Time) {
	if s.regionRuntimeRegistry == nil || !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.recordError(region.runtimeKey, err, now)
}

func (s *subscriptionClient) regionRuntimePhaseCounts() map[regionPhase]int {
	if s.regionRuntimeRegistry == nil {
		return nil
	}
	return s.regionRuntimeRegistry.phaseCounts()
}

func (s *subscriptionClient) removeSubscriptionRuntime(subID SubscriptionID) {
	if s.regionRuntimeRegistry == nil {
		return
	}
	s.regionRuntimeRegistry.removeBySubscription(subID)
}

func (s *subscriptionClient) removeRegionRuntime(region regionInfo, now time.Time) {
	if s.regionRuntimeRegistry == nil || !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.transition(region.runtimeKey, regionPhaseRemoved, now)
	s.regionRuntimeRegistry.remove(region.runtimeKey)
}

// NewSubscriptionClient creates a client.
func NewSubscriptionClient(
	config *SubscriptionClientConfig,
	pd pd.Client,
	lockResolver txnutil.LockResolver,
	credential *security.Credential,
) SubscriptionClient {
	subClient := &subscriptionClient{
		config: config,

		pd:           pd,
		regionCache:  appcontext.GetService[*tikv.RegionCache](appcontext.RegionCache),
		pdClock:      appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		lockResolver: lockResolver,

		regionRuntimeRegistry: newRegionRuntimeRegistry(),

		credential: credential,

		rangeTaskCh:       make(chan rangeTask, 1024),
		regionTaskQueue:   NewPriorityQueue(),
		resolveLockTaskCh: make(chan resolveLockTask, 1024),
		failureBuffer:     newFailureBuffer(),
	}
	subClient.ctx, subClient.cancel = context.WithCancel(context.Background())
	subClient.totalSpans.spanMap = make(map[SubscriptionID]*subscribedSpan)

	option := dynstream.NewOption()
	// Note: it is max batch size of the kv sent from tikv(not committed rows)
	option.BatchCount = 1024
	// TODO: Set `UseBuffer` to true until we refactor the `regionEventHandler.Handle` method so that it doesn't call any method of the dynamic stream. Currently, if `UseBuffer` is set to false, there will be a deadlock:
	// 	ds.handleLoop fetch events from `ch` -> regionEventHandler.Handle -> ds.RemovePath -> send event to `ch`
	option.UseBuffer = true
	option.EnableMemoryControl = true
	ds := dynstream.NewParallelDynamicStream(
		"log-puller",
		&regionEventHandler{subClient: subClient},
		option,
	)
	ds.Start()
	subClient.ds = ds
	subClient.cond = sync.NewCond(&subClient.mu)
	return subClient
}

func (s *subscriptionClient) Name() string {
	return appcontext.SubscriptionClient
}

// AllocsubscriptionID gets an ID can be used in `Subscribe`.
func (s *subscriptionClient) AllocSubscriptionID() SubscriptionID {
	return SubscriptionID(subscriptionIDGen.Add(1))
}

func (s *subscriptionClient) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			resolvedTsLag := s.GetResolvedTsLag()
			if resolvedTsLag > 0 {
				metrics.LogPullerResolvedTsLag.Set(resolvedTsLag)
			}
			dsMetrics := s.ds.GetMetrics()
			metricSubscriptionClientDSChannelSize.Set(float64(dsMetrics.EventChanSize))
			metricSubscriptionClientDSPendingQueueLen.Set(float64(dsMetrics.PendingQueueLen))
			if len(dsMetrics.MemoryControl.AreaMemoryMetrics) > 1 {
				log.Panic("subscription client should have only one area")
			}
			if len(dsMetrics.MemoryControl.AreaMemoryMetrics) > 0 {
				areaMetric := dsMetrics.MemoryControl.AreaMemoryMetrics[0]
				metrics.DynamicStreamMemoryUsage.WithLabelValues(
					"log-puller",
					"max",
					"default",
					"default",
				).Set(float64(areaMetric.MaxMemory()))
				metrics.DynamicStreamMemoryUsage.WithLabelValues(
					"log-puller",
					"used",
					"default",
					"default",
				).Set(float64(areaMetric.MemoryUsage()))
			}

			pendingRegionReqCount := s.pendingRequestCount()
			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Set(float64(pendingRegionReqCount))

			if counts := s.regionRuntimePhaseCounts(); counts != nil {
				for _, phase := range regionRuntimePhases {
					metrics.SubscriptionClientRegionRuntimePhaseCount.WithLabelValues(string(phase)).Set(float64(counts[phase]))
				}
			}

			count := 0
			s.totalSpans.RLock()
			for _, rt := range s.totalSpans.spanMap {
				count += rt.rangeLock.Len()
			}
			s.totalSpans.RUnlock()
			metrics.SubscriptionClientSubscribedRegionCount.Set(float64(count))
		}
	}
}

// Subscribe the given table span.
// NOTE: `span.TableID` must be set correctly.
// It new a subscribedSpan and store it in `s.totalSpans`,
// and send a rangeTask to `s.rangeTaskCh`.
func (s *subscriptionClient) Subscribe(
	subID SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	bdrMode bool,
) {
	if span.TableID == 0 {
		log.Panic("subscription client subscribe with zero TableID")
		return
	}

	rt := s.newSubscribedSpan(subID, span, startTs, consumeKVEvents, advanceResolvedTs, advanceInterval, bdrMode)
	s.totalSpans.Lock()
	s.totalSpans.spanMap[subID] = rt
	s.totalSpans.Unlock()

	areaSetting := dynstream.NewAreaSettingsWithMaxPendingSize(1*1024*1024*1024, dynstream.MemoryControlForPuller, "logPuller") // 1GB
	s.ds.AddPath(rt.subID, rt, areaSetting)

	select {
	case <-s.ctx.Done():
		log.Warn("subscribes span failed, the subscription client has closed")
	case s.rangeTaskCh <- rangeTask{span: span, subscribedSpan: rt, filterLoop: rt.filterLoop, priority: TaskLowPrior}:
		log.Info("subscribes span done", zap.Uint64("subscriptionID", uint64(subID)),
			zap.Int64("tableID", span.TableID), zap.Uint64("startTs", startTs),
			zap.String("startKey", spanz.HexKey(span.StartKey)), zap.String("endKey", spanz.HexKey(span.EndKey)))
	}
}

// Unsubscribe the given table span. All covered regions will be deregistered asynchronously.
// NOTE: `span.TableID` must be set correctly.
func (s *subscriptionClient) Unsubscribe(subID SubscriptionID) {
	// NOTE: `subID` is cleared from `s.totalSpans` in `onTableDrained`.
	s.totalSpans.Lock()
	rt := s.totalSpans.spanMap[subID]
	s.totalSpans.Unlock()
	if rt == nil {
		log.Warn("unknown subscription", zap.Uint64("subscriptionID", uint64(subID)))
		return
	}
	s.setTableStopped(rt)

	log.Info("unsubscribe span success",
		zap.Uint64("subscriptionID", uint64(rt.subID)),
		zap.Bool("exists", rt != nil))
}

func (s *subscriptionClient) wakeSubscription(subID SubscriptionID) {
	s.ds.Wake(subID)
}

func (s *subscriptionClient) pushRegionEventToDS(subID SubscriptionID, event regionEvent) {
	// fast path
	if !s.paused.Load() {
		s.ds.Push(subID, event)
		return
	}
	// slow path: wait until paused is false
	s.mu.Lock()
	for s.paused.Load() {
		select {
		case <-s.ctx.Done():
			s.mu.Unlock()
			return
		default:
			s.cond.Wait()
		}
	}
	s.mu.Unlock()
	s.ds.Push(subID, event)
}

func (s *subscriptionClient) handleDSFeedBack(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case feedback := <-s.ds.Feedback():
			switch feedback.FeedbackType {
			case dynstream.PauseArea:
				s.mu.Lock()
				s.paused.Store(true)
				s.mu.Unlock()
				log.Info("subscription client pause push region event")
			case dynstream.ResumeArea:
				s.mu.Lock()
				s.paused.Store(false)
				s.cond.Broadcast()
				s.mu.Unlock()
				log.Info("subscription client resume push region event")
			case dynstream.ReleasePath, dynstream.ResumePath:
				// Ignore it, because it is no need to pause and resume a path in puller.
			}
		}
	}
}

func (s *subscriptionClient) Run(ctx context.Context) error {
	// s.consume = consume
	if s.pd == nil {
		log.Warn("subscription client should be in test mode, skip run")
		return nil
	}
	s.clusterID = s.pd.GetClusterID(ctx)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return s.updateMetrics(ctx) })
	g.Go(func() error { return s.handleDSFeedBack(ctx) })
	g.Go(func() error { return s.handleRangeTasks(ctx) })
	g.Go(func() error { return s.handleRegions(ctx, g) })
	g.Go(func() error { return s.handleFailures(ctx) })
	g.Go(func() error { return s.runResolveLockChecker(ctx) })
	g.Go(func() error { return s.handleResolveLockTasks(ctx) })
	g.Go(func() error { return s.logSlowRegions(ctx) })
	g.Go(func() error { return s.failureBuffer.run(ctx) })

	log.Info("subscription client starts")
	defer log.Info("subscription client exits")
	return g.Wait()
}

// Close closes the client. Must be called after `Run` returns.
func (s *subscriptionClient) Close(ctx context.Context) error {
	s.cancel()
	s.mu.Lock()
	s.paused.Store(false)
	s.cond.Broadcast()
	s.mu.Unlock()
	s.ds.Close()
	s.regionTaskQueue.Close()
	return nil
}

func (s *subscriptionClient) setTableStopped(rt *subscribedSpan) {
	log.Info("subscription client starts to stop table",
		zap.Uint64("subscriptionID", uint64(rt.subID)))

	// Set stopped to true so we can stop handling region events from the table.
	// Then send a special singleRegionInfo so every store worker deregisters the table.
	if rt.stopped.CompareAndSwap(false, true) {
		s.regionTaskQueue.Push(NewRegionPriorityTask(
			TaskHighPrior,
			regionInfo{subscribedSpan: rt, filterLoop: rt.filterLoop},
			s.pdClock.CurrentTS(),
		))
		if rt.rangeLock.Stop() {
			s.onTableDrained(rt)
		}
	}
}

func (s *subscriptionClient) onTableDrained(rt *subscribedSpan) {
	log.Info("subscription client stop span is finished",
		zap.Uint64("subscriptionID", uint64(rt.subID)))

	s.removeSubscriptionRuntime(rt.subID)

	err := s.ds.RemovePath(rt.subID)
	if err != nil {
		log.Warn("subscription client remove path failed",
			zap.Uint64("subscriptionID", uint64(rt.subID)),
			zap.Error(err))
	}
	s.totalSpans.Lock()
	defer s.totalSpans.Unlock()
	delete(s.totalSpans.spanMap, rt.subID)
}

func (s *subscriptionClient) pendingRequestCount() int {
	pendingRegionReqCount := 0
	s.stores.Range(func(_, value any) bool {
		store := value.(*requestedStore)
		for _, worker := range store.snapshotWorkers() {
			pendingRegionReqCount += worker.requestCache.getPendingCount()
		}
		return true
	})
	return pendingRegionReqCount
}

func (s *subscriptionClient) cleanupRequestedStores() {
	s.stores.Range(func(_, value any) bool {
		store := value.(*requestedStore)
		for _, worker := range store.snapshotWorkers() {
			worker.requestCache.clear()
		}
		return true
	})
}

func (s *subscriptionClient) perWorkerQueueSize() int {
	config := config.GetGlobalServerConfig()
	perWorkerQueueSize := config.Debug.Puller.PendingRegionRequestQueueSize / int(s.config.RegionRequestWorkerPerStore)
	if perWorkerQueueSize <= 0 {
		log.Warn("pending region request queue size is smaller than the number of workers, adjust per worker queue size to 1",
			zap.Int("pendingRegionRequestQueueSize", config.Debug.Puller.PendingRegionRequestQueueSize),
			zap.Uint("regionRequestWorkerPerStore", s.config.RegionRequestWorkerPerStore))
		return 1
	}
	return perWorkerQueueSize
}

func (s *subscriptionClient) getOrCreateRequestedStore(
	ctx context.Context,
	eg *errgroup.Group,
	storeAddr string,
) *requestedStore {
	if value, ok := s.stores.Load(storeAddr); ok {
		return value.(*requestedStore)
	}

	store := newRequestedStore(storeAddr)
	s.stores.Store(storeAddr, store)

	perWorkerQueueSize := s.perWorkerQueueSize()
	for i := uint(0); i < s.config.RegionRequestWorkerPerStore; i++ {
		store.addWorker(newRegionRequestWorker(ctx, s, s.credential, eg, store, perWorkerQueueSize))
	}
	return store
}

func (s *subscriptionClient) enqueueRegionToAllStores(ctx context.Context, region regionInfo) (bool, error) {
	enqueued := true
	var firstErr error
	s.stores.Range(func(_ any, value any) bool {
		store := value.(*requestedStore)
		for _, worker := range store.snapshotWorkers() {
			ok, err := worker.add(ctx, region, true)
			if err != nil {
				firstErr = err
				enqueued = false
				return false
			}
			if !ok {
				enqueued = false
				// It is likely the store is busy, no need to try other workers in this store now.
				break
			}
		}
		return true
	})
	return enqueued, firstErr
}

func (s *subscriptionClient) attachRPCContextForRegion(ctx context.Context, region regionInfo) (regionInfo, bool) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.regionCache.GetTiKVRPCContext(bo, region.verID, kvclientv2.ReplicaReadLeader, 0)
	if rpcCtx != nil {
		region.rpcCtx = rpcCtx
		return region, true
	}
	if err != nil {
		log.Debug("subscription client get rpc context fail",
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Error(err))
	}
	s.submitDirectFailure(newRPCCtxUnavailableFailure(region))
	return region, false
}

// handleRegions receives regionInfo from regionTaskQueue, attaches rpcCtx to them,
// then sends them to the corresponding requestedStore.
func (s *subscriptionClient) handleRegions(ctx context.Context, eg *errgroup.Group) error {
	defer s.cleanupRequestedStores()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		regionTask, err := s.regionTaskQueue.Pop(ctx)
		if err != nil {
			return err
		}

		region := regionTask.GetRegionInfo()
		if region.isStopped() {
			enqueued, err := s.enqueueRegionToAllStores(ctx, region)
			if err != nil {
				return err
			}
			if !enqueued {
				log.Debug("enqueue stop request failed, retry later",
					zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)))
				s.regionTaskQueue.Push(regionTask)
			}
			continue
		}

		region, ok := s.attachRPCContextForRegion(ctx, region)
		if !ok {
			continue
		}
		s.updateRegionRuntimeInfo(region)
		s.markRegionRPCReady(region, time.Now())

		store := s.getOrCreateRequestedStore(ctx, eg, region.rpcCtx.Addr)
		worker := store.getRequestWorker()
		force := regionTask.Priority() <= forcedPriorityBase

		ok, err = worker.add(ctx, region, force)
		if err != nil {
			log.Warn("subscription client add region request failed",
				zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
				zap.Uint64("regionID", region.verID.GetID()),
				zap.Error(err))
			return err
		}
		if !ok {
			s.regionTaskQueue.Push(regionTask)
			continue
		}

		log.Debug("subscription client will request a region",
			zap.Uint64("workID", worker.workerID),
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.String("addr", store.storeAddr))
	}
}

func (s *subscriptionClient) handleRangeTasks(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	// Limit the concurrent number of goroutines to convert range tasks to region tasks.
	g.SetLimit(1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-s.rangeTaskCh:
			g.Go(func() error {
				return s.divideSpanAndScheduleRegionRequests(ctx, task.span, task.subscribedSpan, task.filterLoop, task.priority)
			})
		}
	}
}

// divideSpanAndScheduleRegionRequests processes the specified span by dividing it into
// manageable regions and schedules requests to subscribe to these regions.
// 1. Load regions from PD.
// 2. Find the intersection of each region.span and the subscribedSpan.span.
// 3. Schedule a region request to subscribe the region.
func (s *subscriptionClient) divideSpanAndScheduleRegionRequests(
	ctx context.Context,
	span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	taskType TaskType,
) error {
	// Limit the number of regions loaded at a time to make the load more stable.
	limit := 1024
	nextSpan := span
	backoffBeforeLoad := false
	for {
		if backoffBeforeLoad {
			if err := util.Hang(ctx, loadRegionRetryInterval); err != nil {
				return err
			}
			backoffBeforeLoad = false
		}
		log.Debug("subscription client is going to load regions",
			zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
			zap.Any("span", common.FormatTableSpan(&nextSpan)))

		backoff := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		regions, err := s.regionCache.BatchLoadRegionsWithKeyRange(backoff, nextSpan.StartKey, nextSpan.EndKey, limit)
		if err != nil {
			log.Warn("subscription client load regions failed",
				zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
				zap.Any("span", common.FormatTableSpan(&nextSpan)),
				zap.Error(err))
			backoffBeforeLoad = true
			continue
		}
		regionMetas := make([]*metapb.Region, 0, len(regions))
		for _, region := range regions {
			if meta := region.GetMeta(); meta != nil {
				regionMetas = append(regionMetas, meta)
			}
		}
		regionMetas = regionlock.CutRegionsLeftCoverSpan(regionMetas, nextSpan)
		if len(regionMetas) == 0 {
			log.Warn("subscription client load regions with holes",
				zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
				zap.Any("span", common.FormatTableSpan(&nextSpan)))
			backoffBeforeLoad = true
			continue
		}

		for _, regionMeta := range regionMetas {
			regionSpan := heartbeatpb.TableSpan{
				StartKey:   regionMeta.StartKey,
				EndKey:     regionMeta.EndKey,
				KeyspaceID: subscribedSpan.span.KeyspaceID,
			}
			// NOTE: the End key return by the PD API will be nil to represent the biggest key.
			// So we need to fix it by calling spanz.HackSpan.
			regionSpan = common.HackTableSpan(regionSpan)

			// Find the intersection of the regionSpan returned by PD and the subscribedSpan.span.
			// The intersection is the span that needs to be subscribed.
			intersectSpan := common.GetIntersectSpan(subscribedSpan.span, regionSpan)
			if common.IsEmptySpan(intersectSpan) {
				log.Panic("subscription client check spans intersect shouldn't fail",
					zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)))
			}

			regionInfo := newRegionInfo(
				tikv.NewRegionVerID(regionMeta.Id, regionMeta.RegionEpoch.ConfVer, regionMeta.RegionEpoch.Version),
				intersectSpan,
				nil,
				subscribedSpan,
				filterLoop,
			)
			s.scheduleRegionRequest(ctx, regionInfo, taskType)

			nextSpan.StartKey = regionMeta.EndKey
			// If the nextSpan.StartKey is larger than the subscribedSpan.span.EndKey,
			// it means all span of the subscribedSpan have been requested. So we return.
			if common.EndCompare(nextSpan.StartKey, span.EndKey) >= 0 {
				return nil
			}
		}
	}
}

func (s *subscriptionClient) scheduleRegionRequest(ctx context.Context, region regionInfo, priority TaskType) {
	s.ensureRegionRuntime(&region, time.Now())
	lockRangeResult := region.subscribedSpan.rangeLock.LockRange(
		ctx, region.span.StartKey, region.span.EndKey, region.verID.GetID(), region.verID.GetVer())

	if lockRangeResult.Status == regionlock.LockRangeStatusWait {
		s.transitionRegionRuntime(region, regionPhaseRangeLockWait, time.Now())
		lockRangeResult = lockRangeResult.WaitFn()
	}

	switch lockRangeResult.Status {
	case regionlock.LockRangeStatusSuccess:
		region.lockedRangeState = lockRangeResult.LockedRangeState
		s.markRegionQueued(region, lockRangeResult.LockedRangeState.Created, time.Now())
		s.regionTaskQueue.Push(NewRegionPriorityTask(priority, region, s.pdClock.CurrentTS()))
	case regionlock.LockRangeStatusStale:
		s.removeRegionRuntime(region, time.Now())
		for _, retryRange := range lockRangeResult.RetryRanges {
			s.scheduleRangeRequest(ctx, retryRange, region.subscribedSpan, region.filterLoop, priority)
		}
	case regionlock.LockRangeStatusCancel:
		s.removeRegionRuntime(region, time.Now())
	default:
		return
	}
}

func (s *subscriptionClient) scheduleRangeRequest(
	ctx context.Context,
	span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	priority TaskType,
) {
	select {
	case <-ctx.Done():
	case s.rangeTaskCh <- rangeTask{
		span:           span,
		subscribedSpan: subscribedSpan,
		filterLoop:     filterLoop,
		priority:       priority,
	}:
	}
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

func (s *subscriptionClient) submitDirectFailure(failure regionFailureInfo) {
	s.recordRegionRuntimeError(failure.regionInfo, failure.err, time.Now())
	if failure.subscribedSpan.rangeLock.UnlockRange(
		failure.span.StartKey, failure.span.EndKey,
		failure.verID.GetID(), failure.verID.GetVer(), failure.resolvedTs()) {
		s.onTableDrained(failure.subscribedSpan)
		return
	}
	s.failureBuffer.enqueue(failure)
}

func (s *subscriptionClient) submitOrderedFailure(state *regionFeedState) (regionFailureInfo, bool) {
	failure, removed := state.detachFailure()
	if !removed {
		return regionFailureInfo{}, false
	}
	s.submitDirectFailure(failure)
	return failure, true
}

func (s *subscriptionClient) submitWorkerSessionFailure(
	session *regionRequestWorkerSession,
	pendingRegions []regionInfo,
	sessionFailure workerSessionFailure,
) {
	for subID, states := range session.clearRegionStates() {
		for _, state := range states {
			state.markStopped(normalizeWorkerSessionFailure(state.getRegionInfo(), sessionFailure))
			s.pushRegionEventToDS(subID, regionEvent{
				states: []*regionFeedState{state},
			})
		}
	}

	for _, region := range pendingRegions {
		if region.isStopped() {
			continue
		}
		s.submitDirectFailure(normalizeWorkerSessionFailure(region, sessionFailure))
	}
}

func (s *subscriptionClient) handleFailures(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Info("subscription client handle failures and exit")
			return ctx.Err()
		case failure := <-s.failureBuffer.ch:
			if err := s.handleFailure(ctx, failure); err != nil {
				return err
			}
		}
	}
}

func (s *subscriptionClient) retryRegion(ctx context.Context, failure regionFailureInfo, priority TaskType) {
	s.markRegionRetryPending(failure.regionInfo, failure.err, time.Now())
	s.scheduleRegionRequest(ctx, failure.regionInfo, priority)
}

func (s *subscriptionClient) reloadRegionRange(ctx context.Context, failure regionFailureInfo) {
	s.removeRegionRuntime(failure.regionInfo, time.Now())
	s.scheduleRangeRequest(ctx, failure.span, failure.subscribedSpan, failure.filterLoop, TaskHighPrior)
}

func (s *subscriptionClient) handleRegionFailure(ctx context.Context, failure regionFailureInfo) error {
	switch failure.kind {
	case regionFailureKindTiKVEvent:
		eventErr, ok := failure.err.(*eventError)
		if !ok || eventErr == nil {
			return errors.New("invalid tikv event failure")
		}

		innerErr := eventErr.err
		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			metricFeedNotLeaderCounter.Inc()
			if s.regionCache != nil {
				s.regionCache.UpdateLeader(failure.verID, notLeader.GetLeader(), failure.rpcCtx.AccessIdx)
			}
			s.retryRegion(ctx, failure, TaskHighPrior)
			return nil
		}
		if innerErr.GetEpochNotMatch() != nil {
			metricFeedEpochNotMatchCounter.Inc()
			s.reloadRegionRange(ctx, failure)
			return nil
		}
		if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			s.reloadRegionRange(ctx, failure)
			return nil
		}
		if innerErr.GetCongested() != nil {
			metricKvCongestedCounter.Inc()
			s.retryRegion(ctx, failure, TaskLowPrior)
			return nil
		}
		if innerErr.GetServerIsBusy() != nil {
			metricKvIsBusyCounter.Inc()
			s.retryRegion(ctx, failure, TaskLowPrior)
			return nil
		}
		if duplicated := innerErr.GetDuplicateRequest(); duplicated != nil {
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
			zap.Uint64("subscriptionID", uint64(failure.subscribedSpan.subID)),
			zap.Stringer("error", innerErr))
		metricFeedUnknownErrorCounter.Inc()
		s.retryRegion(ctx, failure, TaskHighPrior)
		return nil
	case regionFailureKindRPCCtxUnavailable:
		metricFeedRPCCtxUnavailable.Inc()
		s.reloadRegionRange(ctx, failure)
		return nil
	default:
		return errors.New("unexpected region failure kind")
	}
}

func (s *subscriptionClient) handleStoreSessionFailure(ctx context.Context, failure regionFailureInfo) error {
	switch failure.kind {
	case regionFailureKindGetStore:
		metricGetStoreErr.Inc()
		if s.regionCache != nil {
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			s.regionCache.OnSendFail(bo, failure.rpcCtx, true, errors.Cause(failure.err))
		}
		s.reloadRegionRange(ctx, failure)
		return nil
	case regionFailureKindSendRequestToStore:
		metricStoreSendRequestErr.Inc()
		if s.regionCache != nil {
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			s.regionCache.OnSendFail(bo, failure.rpcCtx, regionScheduleReload, errors.Cause(failure.err))
		}
		s.retryRegion(ctx, failure, TaskHighPrior)
		return nil
	default:
		return errors.New("unexpected store session failure kind")
	}
}

func (s *subscriptionClient) handleSubscriptionFailure(failure regionFailureInfo) error {
	switch failure.kind {
	case regionFailureKindRequestCancelled, regionFailureKindSubscriptionStopped:
		s.removeRegionRuntime(failure.regionInfo, time.Now())
		return nil
	default:
		return errors.New("unexpected subscription failure kind")
	}
}

func (s *subscriptionClient) handleFailure(ctx context.Context, failure regionFailureInfo) error {
	log.Debug("cdc region failure",
		zap.Uint64("subscriptionID", uint64(failure.subscribedSpan.subID)),
		zap.Uint64("regionID", failure.verID.GetID()),
		zap.Stringer("failureScope", failure.scope),
		zap.Stringer("failureSource", failure.source),
		zap.Stringer("failureKind", failure.kind),
		zap.Error(failure.err))

	var err error
	switch failure.scope {
	case regionFailureScopeRegion:
		err = s.handleRegionFailure(ctx, failure)
	case regionFailureScopeStoreSession:
		err = s.handleStoreSessionFailure(ctx, failure)
	case regionFailureScopeSubscription:
		err = s.handleSubscriptionFailure(failure)
	default:
		err = errors.New("unknown failure scope")
	}
	if err == nil {
		return nil
	}

	log.Warn("subscription client meets an internal error, fail the changefeed",
		zap.Uint64("subscriptionID", uint64(failure.subscribedSpan.subID)),
		zap.Stringer("failureScope", failure.scope),
		zap.Stringer("failureSource", failure.source),
		zap.Stringer("failureKind", failure.kind),
		zap.Error(err))
	return err
}

type subscriptionAndTargetTs struct {
	subSpan  *subscribedSpan
	targetTs uint64
}

func (s *subscriptionClient) runResolveLockChecker(ctx context.Context) error {
	resolveLockTicker := time.NewTicker(resolveLockTickInterval)
	defer resolveLockTicker.Stop()
	maxCacheSize := 1024
	subSpanAndTsCache := make([]subscriptionAndTargetTs, 0, maxCacheSize)
	// getResolvedTargetTs returns the targetTs to resolve stale locks. 0 means no need to resolve.
	getResolvedTargetTs := func(subSpan *subscribedSpan, currentTime time.Time) uint64 {
		resolvedTsUpdated := time.Unix(subSpan.resolvedTsUpdated.Load(), 0)
		if !subSpan.initialized.Load() || time.Since(resolvedTsUpdated) < resolveLockFence {
			return 0
		}
		resolvedTs := subSpan.resolvedTs.Load()
		resolvedTime := oracle.GetTimeFromTS(resolvedTs)
		if currentTime.Sub(resolvedTime) < resolveLockFence {
			return 0
		}
		return oracle.GoTimeToTS(resolvedTime.Add(resolveLockFence))
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resolveLockTicker.C:
		}
		currentTime := s.pdClock.CurrentTime()
		s.totalSpans.Lock()
		for _, subSpan := range s.totalSpans.spanMap {
			if subSpan != nil {
				targetTs := getResolvedTargetTs(subSpan, currentTime)
				if targetTs > 0 {
					subSpanAndTsCache = append(subSpanAndTsCache, subscriptionAndTargetTs{
						subSpan:  subSpan,
						targetTs: targetTs,
					})
				}
			}
		}
		s.totalSpans.Unlock()
		for _, subSpanAndTs := range subSpanAndTsCache {
			subSpanAndTs.subSpan.resolveStaleLocks(subSpanAndTs.targetTs)
		}
		subSpanAndTsCache = subSpanAndTsCache[:0]
		if cap(subSpanAndTsCache) > maxCacheSize {
			subSpanAndTsCache = make([]subscriptionAndTargetTs, 0, maxCacheSize)
		}
	}
}

func gcResolveLastRunMap(resolveLastRun map[uint64]time.Time, now time.Time) map[uint64]time.Time {
	if len(resolveLastRun) <= resolveLastRunGCThreshold {
		return resolveLastRun
	}

	copied := make(map[uint64]time.Time, len(resolveLastRun))
	for regionID, lastRun := range resolveLastRun {
		if now.Sub(lastRun) < resolveLockMinInterval {
			copied[regionID] = lastRun
		}
	}
	return copied
}

func (s *subscriptionClient) handleResolveLockTasks(ctx context.Context) error {
	resolveLastRun := make(map[uint64]time.Time)

	doResolve := func(keyspaceID uint32, regionID uint64, state *regionlock.LockedRangeState, targetTs uint64) {
		if state.ResolvedTs.Load() > targetTs || !state.Initialized.Load() {
			return
		}

		lastRun, ok := resolveLastRun[regionID]
		if ok {
			if time.Since(lastRun) < resolveLockMinInterval {
				return
			}
		}

		if err := s.lockResolver.Resolve(ctx, keyspaceID, regionID, targetTs); err != nil {
			log.Warn("subscription client resolve lock fail",
				zap.Uint32("keyspaceID", keyspaceID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("targetTs", targetTs),
				zap.Time("lastRun", lastRun),
				zap.Any("state", state),
				zap.Error(err))
		}
		resolveLastRun[regionID] = time.Now()
	}

	gcTicker := time.NewTicker(resolveLockMinInterval * 3 / 2)
	defer gcTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gcTicker.C:
			resolveLastRun = gcResolveLastRunMap(resolveLastRun, time.Now())
		case task := <-s.resolveLockTaskCh:
			doResolve(task.keyspaceID, task.regionID, task.state, task.targetTs)
		}
	}
}

func (s *subscriptionClient) logSlowRegions(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		currTime := s.pdClock.CurrentTime()
		s.totalSpans.RLock()
		slowInitializeRegion := 0
		for subscriptionID, rt := range s.totalSpans.spanMap {
			attr := rt.rangeLock.IterAll(nil)
			ckptTime := oracle.GetTimeFromTS(attr.SlowestRegion.ResolvedTs)
			if attr.SlowestRegion.Initialized {
				if currTime.Sub(ckptTime) > 6*resolveLockMinInterval {
					log.Info("subscription client finds a initialized slow region",
						zap.Uint64("subscriptionID", uint64(subscriptionID)),
						zap.Any("slowRegion", attr.SlowestRegion))
				}
			} else if currTime.Sub(attr.SlowestRegion.Created) > 10*time.Minute {
				slowInitializeRegion += 1
				log.Info("subscription client initializes a region too slow",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			} else if currTime.Sub(ckptTime) > 10*time.Minute {
				log.Info("subscription client finds a uninitialized slow region",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			}
			if len(attr.UnLockedRanges) > 0 {
				log.Info("subscription client holes exist",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("holes", attr.UnLockedRanges))
			}
		}
		s.totalSpans.RUnlock()
	}
}

func (s *subscriptionClient) newSubscribedSpan(
	subID SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	filterLoop bool,
) *subscribedSpan {
	rangeLock := regionlock.NewRangeLock(uint64(subID), span.StartKey, span.EndKey, startTs)

	rt := &subscribedSpan{
		subID:      subID,
		span:       span,
		startTs:    startTs,
		filterLoop: filterLoop,
		rangeLock:  rangeLock,

		consumeKVEvents:   consumeKVEvents,
		advanceResolvedTs: advanceResolvedTs,
		advanceInterval:   advanceInterval,
	}
	rt.initialized.Store(false)
	rt.resolvedTsUpdated.Store(time.Now().Unix())
	rt.resolvedTs.Store(startTs)

	rt.tryResolveLock = func(regionID uint64, state *regionlock.LockedRangeState) {
		targetTs := rt.staleLocksTargetTs.Load()
		if state.ResolvedTs.Load() < targetTs && state.Initialized.Load() {
			select {
			case <-s.ctx.Done():
			case s.resolveLockTaskCh <- resolveLockTask{
				keyspaceID: span.KeyspaceID,
				regionID:   regionID,
				targetTs:   targetTs,
				state:      state,
				create:     time.Now(),
			}:
			// it is ok to ignore resolve lock task when the channel is full
			default:
				metrics.SubscriptionClientResolveLockTaskDropCounter.Inc()
			}
		}
	}
	return rt
}

func (s *subscriptionClient) GetResolvedTsLag() float64 {
	pullerMinResolvedTs := uint64(0)
	s.totalSpans.RLock()
	for _, rt := range s.totalSpans.spanMap {
		resolvedTs := rt.resolvedTs.Load()
		if pullerMinResolvedTs == 0 || resolvedTs < pullerMinResolvedTs {
			pullerMinResolvedTs = resolvedTs
		}
	}
	s.totalSpans.RUnlock()
	if pullerMinResolvedTs == 0 {
		return 0
	}
	pdTime := s.pdClock.CurrentTime()
	phyResolvedTs := oracle.ExtractPhysical(pullerMinResolvedTs)
	lag := float64(oracle.GetPhysical(pdTime)-phyResolvedTs) / 1e3
	return lag
}

func (r *subscribedSpan) resolveStaleLocks(targetTs uint64) {
	util.MustCompareAndMonotonicIncrease(&r.staleLocksTargetTs, targetTs)
	res := r.rangeLock.IterAll(r.tryResolveLock)
	log.Debug("subscription client finds slow locked ranges",
		zap.Uint64("subscriptionID", uint64(r.subID)),
		zap.Any("ranges", res))
}
