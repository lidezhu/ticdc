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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/logservice/txnutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/ticdc/utils/dynstream"
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

	requestedStores   *requestedStoreSet
	regionScheduler   *regionRequestScheduler
	subscribedSpans   *subscribedSpanSet
	staleLockResolver *staleLockResolver

	ds dynstream.DynamicStream[int, SubscriptionID, regionEvent, *subscribedSpan, *regionEventHandler]
	// the following three fields are used to manage feedback from ds and notify other goroutines
	mu     sync.Mutex
	cond   *sync.Cond
	paused atomic.Bool

	// the credential to connect tikv
	credential *security.Credential
}

func (s *subscriptionClient) ensureHelpers() {
	if s.requestedStores == nil {
		s.requestedStores = newRequestedStoreSet(s)
	}
	if s.regionScheduler == nil {
		s.regionScheduler = newRegionRequestScheduler(s)
	}
	if s.subscribedSpans == nil {
		s.subscribedSpans = newSubscribedSpanSet(s)
	}
	if s.staleLockResolver == nil {
		s.staleLockResolver = newStaleLockResolver(s)
	}
	if s.regionRuntimeRegistry == nil {
		s.regionRuntimeRegistry = newRegionRuntimeRegistry()
	}
}

func (s *subscriptionClient) ensureRegionRuntime(region *regionInfo, now time.Time) {
	if region.verID.GetID() == 0 {
		return
	}
	if !region.runtimeKey.isValid() {
		region.runtimeKey = s.regionRuntimeRegistry.allocKey(region.subscribedSpan.subID, region.verID.GetID())
		s.regionRuntimeRegistry.registerRegion(region.runtimeKey, *region, now)
	}
}

func (s *subscriptionClient) updateRegionRuntimeInfo(region regionInfo) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.updateRegionInfo(region.runtimeKey, region)
}

func (s *subscriptionClient) transitionRegionRuntime(region regionInfo, phase regionPhase, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.transition(region.runtimeKey, phase, now)
}

func (s *subscriptionClient) markRegionRetryPending(region regionInfo, err error, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markRetryPending(region.runtimeKey, err, now)
}

func (s *subscriptionClient) markRegionRPCReady(region regionInfo, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markRPCReady(region.runtimeKey, now)
}

func (s *subscriptionClient) markRegionQueued(region regionInfo, acquiredTime, queuedTime time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markQueued(region.runtimeKey, acquiredTime, queuedTime)
}

func (s *subscriptionClient) recordRegionRuntimeError(region regionInfo, err error, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.recordError(region.runtimeKey, err, now)
}

func (s *subscriptionClient) regionRuntimePhaseCounts() map[regionPhase]int {
	return s.regionRuntimeRegistry.phaseCounts()
}

func (s *subscriptionClient) removeSubscriptionRuntime(subID SubscriptionID) {
	s.regionRuntimeRegistry.removeBySubscription(subID)
}

func (s *subscriptionClient) removeRegionRuntime(region regionInfo, now time.Time) {
	if !region.runtimeKey.isValid() {
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
	}
	subClient.ctx, subClient.cancel = context.WithCancel(context.Background())
	subClient.ensureHelpers()

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
			resolvedTsLag := s.subscribedSpans.getResolvedTsLag()
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

			pendingRegionReqCount := s.requestedStores.pendingRequestCount()
			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Set(float64(pendingRegionReqCount))

			counts := s.regionRuntimePhaseCounts()
			for _, phase := range regionRuntimePhases {
				metrics.SubscriptionClientRegionRuntimePhaseCount.WithLabelValues(string(phase)).Set(float64(counts[phase]))
			}

			metrics.SubscriptionClientSubscribedRegionCount.Set(float64(s.subscribedSpans.requestedRegionCount()))
		}
	}
}

// Subscribe the given table span.
// NOTE: `span.TableID` must be set correctly.
// It creates a subscribed span, registers it, and schedules the bootstrap range request.
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

	rt := s.subscribedSpans.newSubscribedSpan(subID, span, startTs, consumeKVEvents, advanceResolvedTs, advanceInterval, bdrMode)
	s.subscribedSpans.add(subID, rt)

	areaSetting := dynstream.NewAreaSettingsWithMaxPendingSize(1*1024*1024*1024, dynstream.MemoryControlForPuller, "logPuller") // 1GB
	s.ds.AddPath(rt.subID, rt, areaSetting)

	if !s.regionScheduler.scheduleRangeRequest(s.ctx, span, rt, rt.filterLoop, TaskLowPrior) {
		log.Warn("subscribes span failed, the subscription client has closed")
		return
	}
	log.Info("subscribes span done", zap.Uint64("subscriptionID", uint64(subID)),
		zap.Int64("tableID", span.TableID), zap.Uint64("startTs", startTs),
		zap.String("startKey", spanz.HexKey(span.StartKey)), zap.String("endKey", spanz.HexKey(span.EndKey)))
}

// Unsubscribe the given table span. All covered regions will be deregistered asynchronously.
// NOTE: `span.TableID` must be set correctly.
func (s *subscriptionClient) Unsubscribe(subID SubscriptionID) {
	rt := s.subscribedSpans.get(subID)
	if rt == nil {
		log.Warn("unknown subscription", zap.Uint64("subscriptionID", uint64(subID)))
		return
	}
	s.subscribedSpans.setTableStopped(rt)

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
	g.Go(func() error { return s.regionScheduler.handleRangeTasks(ctx) })
	g.Go(func() error { return s.regionScheduler.handleRegions(ctx, g) })
	g.Go(func() error { return s.regionScheduler.handleFailures(ctx) })
	g.Go(func() error { return s.staleLockResolver.runResolveLockChecker(ctx) })
	g.Go(func() error { return s.staleLockResolver.handleResolveLockTasks(ctx) })
	g.Go(func() error { return s.logSlowRegions(ctx) })
	g.Go(func() error { return s.regionScheduler.runFailureBuffer(ctx) })

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
	if s.regionScheduler != nil {
		s.regionScheduler.close()
	}
	return nil
}

func (s *subscriptionClient) setTableStopped(rt *subscribedSpan) {
	s.subscribedSpans.setTableStopped(rt)
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
	return s.subscribedSpans.newSubscribedSpan(
		subID, span, startTs, consumeKVEvents, advanceResolvedTs, advanceInterval, filterLoop)
}

func (s *subscriptionClient) GetResolvedTsLag() float64 {
	return s.subscribedSpans.getResolvedTsLag()
}

func (s *subscriptionClient) scheduleRegionRequest(ctx context.Context, region regionInfo, priority TaskType) {
	s.regionScheduler.scheduleRegionRequest(ctx, region, priority)
}

func (s *subscriptionClient) scheduleRangeRequest(
	ctx context.Context,
	span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	priority TaskType,
) {
	s.regionScheduler.scheduleRangeRequest(ctx, span, subscribedSpan, filterLoop, priority)
}

func (s *subscriptionClient) submitDirectFailure(failure regionFailureInfo) {
	s.regionScheduler.submitDirectFailure(failure)
}

func (s *subscriptionClient) submitOrderedFailure(state *regionFeedState) (regionFailureInfo, bool) {
	return s.regionScheduler.submitOrderedFailure(state)
}

func (s *subscriptionClient) submitWorkerSessionFailure(
	startedRegions map[SubscriptionID]regionFeedStates,
	pendingRegions []regionInfo,
	sessionFailure workerSessionFailure,
) {
	s.regionScheduler.submitWorkerSessionFailure(startedRegions, pendingRegions, sessionFailure)
}

func (s *subscriptionClient) handleFailure(ctx context.Context, failure regionFailureInfo) error {
	return s.regionScheduler.handleFailure(ctx, failure)
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
		slowInitializeRegion := 0
		for _, entry := range s.subscribedSpans.snapshot() {
			rt := entry.span
			attr := rt.rangeLock.IterAll(nil)
			ckptTime := oracle.GetTimeFromTS(attr.SlowestRegion.ResolvedTs)
			if attr.SlowestRegion.Initialized {
				if currTime.Sub(ckptTime) > 6*resolveLockMinInterval {
					log.Info("subscription client finds a initialized slow region",
						zap.Uint64("subscriptionID", uint64(entry.subID)),
						zap.Any("slowRegion", attr.SlowestRegion))
				}
			} else if currTime.Sub(attr.SlowestRegion.Created) > 10*time.Minute {
				slowInitializeRegion += 1
				log.Info("subscription client initializes a region too slow",
					zap.Uint64("subscriptionID", uint64(entry.subID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			} else if currTime.Sub(ckptTime) > 10*time.Minute {
				log.Info("subscription client finds a uninitialized slow region",
					zap.Uint64("subscriptionID", uint64(entry.subID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			}
			if len(attr.UnLockedRanges) > 0 {
				log.Info("subscription client holes exist",
					zap.Uint64("subscriptionID", uint64(entry.subID)),
					zap.Any("holes", attr.UnLockedRanges))
			}
		}
	}
}
