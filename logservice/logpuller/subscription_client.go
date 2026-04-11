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
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
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

type sharedClientMetrics struct {
	batchResolvedSize prometheus.Observer
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
	ctx    context.Context
	cancel context.CancelFunc
	config *SubscriptionClientConfig

	infra struct {
		pd           pd.Client
		regionCache  *tikv.RegionCache
		pdClock      pdutil.Clock
		lockResolver txnutil.LockResolver
		credential   *security.Credential
		metrics      sharedClientMetrics
		clusterID    uint64
	}

	events  *eventStreamController
	runtime *regionRuntimeTracker

	subscriptions struct {
		manager    *spanManager
		supervisor *spanSupervisor
	}

	pipeline struct {
		scheduler     *regionScheduler
		requestRouter *regionRequestRouter
		errorHandler  *regionErrorHandler
	}
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
	}
	subClient.ctx, subClient.cancel = context.WithCancel(context.Background())
	subClient.infra.pd = pd
	subClient.infra.regionCache = appcontext.GetService[*tikv.RegionCache](appcontext.RegionCache)
	subClient.infra.pdClock = appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock)
	subClient.infra.lockResolver = lockResolver
	subClient.infra.credential = credential
	subClient.runtime = newRegionRuntimeTracker()

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
	subClient.events = newEventStreamController(subClient.ctx, ds)

	subClient.pipeline.requestRouter = newRegionRequestRouter(subClient)
	subClient.pipeline.scheduler = newRegionScheduler(subClient.infra.regionCache, subClient.runtime, subClient.pipeline.requestRouter)
	subClient.subscriptions.manager = newSpanManager(subClient.ctx, subClient.infra.pdClock, subClient.events, subClient.runtime)
	subClient.subscriptions.supervisor = newSpanSupervisor(subClient.infra.pdClock, subClient.infra.lockResolver, subClient.subscriptions.manager)
	subClient.subscriptions.manager.setSupervisor(subClient.subscriptions.supervisor)
	subClient.subscriptions.manager.setPipeline(subClient.pipeline.scheduler, subClient.pipeline.requestRouter)
	subClient.pipeline.errorHandler = newRegionErrorHandler(
		subClient.runtime,
		subClient.pipeline.scheduler,
		subClient.subscriptions.manager,
		subClient.infra.regionCache,
	)
	subClient.pipeline.requestRouter.client = subClient

	subClient.initMetrics()
	return subClient
}

func (s *subscriptionClient) Name() string {
	return appcontext.SubscriptionClient
}

// AllocsubscriptionID gets an ID can be used in `Subscribe`.
func (s *subscriptionClient) AllocSubscriptionID() SubscriptionID {
	return SubscriptionID(subscriptionIDGen.Add(1))
}

func (s *subscriptionClient) initMetrics() {
	// TODO: fix metrics
	s.infra.metrics.batchResolvedSize = metrics.BatchResolvedEventSize.WithLabelValues("event-store")
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
			dsMetrics := s.events.metrics()
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

			pendingRegionReqCount := 0
			s.pipeline.requestRouter.stores.Range(func(key, value any) bool {
				store := value.(*requestedStore)
				store.requestWorkers.RLock()
				for _, worker := range store.requestWorkers.s {
					worker.requestCache.clearStaleRequest()
					pendingRegionReqCount += worker.requestCache.getPendingCount()
				}
				store.requestWorkers.RUnlock()
				return true
			})

			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Set(float64(pendingRegionReqCount))

			if counts := s.runtime.phaseCounts(); counts != nil {
				for _, phase := range regionRuntimePhases {
					metrics.SubscriptionClientRegionRuntimePhaseCount.WithLabelValues(string(phase)).Set(float64(counts[phase]))
				}
			}

			metrics.SubscriptionClientSubscribedRegionCount.Set(float64(s.subscriptions.manager.subscribedRegionCount()))
		}
	}
}

// Subscribe the given table span.
// NOTE: `span.TableID` must be set correctly.
// The subscription is registered in spanManager, then regionScheduler expands the
// span into region requests and hands them to regionRequestRouter.
func (s *subscriptionClient) Subscribe(
	subID SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	bdrMode bool,
) {
	s.subscriptions.manager.subscribe(subID, span, startTs, consumeKVEvents, advanceResolvedTs, advanceInterval, bdrMode)
}

// Unsubscribe the given table span. All covered regions will be deregistered asynchronously.
// NOTE: `span.TableID` must be set correctly.
func (s *subscriptionClient) Unsubscribe(subID SubscriptionID) {
	s.subscriptions.manager.unsubscribe(subID)
}

func (s *subscriptionClient) Run(ctx context.Context) error {
	// s.consume = consume
	if s.infra.pd == nil {
		log.Warn("subscription client should be in test mode, skip run")
		return nil
	}
	s.infra.clusterID = s.infra.pd.GetClusterID(ctx)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return s.updateMetrics(ctx) })
	g.Go(func() error { return s.events.runFeedback(ctx) })
	g.Go(func() error { return s.pipeline.scheduler.run(ctx) })
	g.Go(func() error { return s.pipeline.requestRouter.run(ctx, g) })
	g.Go(func() error { return s.pipeline.errorHandler.run(ctx) })
	g.Go(func() error { return s.subscriptions.supervisor.runResolveLockChecker(ctx) })
	g.Go(func() error { return s.subscriptions.supervisor.handleResolveLockTasks(ctx) })
	g.Go(func() error { return s.subscriptions.supervisor.logSlowRegions(ctx) })
	g.Go(func() error { return s.pipeline.errorHandler.errCache.dispatch(ctx) })

	log.Info("subscription client starts")
	defer log.Info("subscription client exits")
	return g.Wait()
}

// Close closes the client. Must be called after `Run` returns.
func (s *subscriptionClient) Close(ctx context.Context) error {
	s.cancel()
	s.events.close()
	s.pipeline.requestRouter.regionTaskQueue.Close()
	return nil
}

func (s *subscriptionClient) GetResolvedTsLag() float64 {
	return s.subscriptions.manager.resolvedTsLag()
}
