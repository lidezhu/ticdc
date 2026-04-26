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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/txnutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

// SubscriptionID is a unique identifier for a subscription.
// It is used as `RequestId` in region requests to remote store.
type SubscriptionID uint64

const InvalidSubscriptionID SubscriptionID = 0

type SubscriptionClientConfig struct {
	// The number of region request workers to send region task for every tikv store
	RegionRequestWorkerPerStore uint
}

// SubscriptionClient subscribes table-range events from TiKV.
// All exported methods are thread-safe.
type SubscriptionClient interface {
	common.SubModule
	// AllocSubscriptionID allocates a unique ID for Subscribe.
	AllocSubscriptionID() SubscriptionID
	// Subscribe subscribes a table span.
	Subscribe(
		subID SubscriptionID,
		span heartbeatpb.TableSpan,
		startTs uint64,
		consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
		advanceResolvedTs func(ts uint64),
		advanceInterval int64,
		bdrMode bool,
	)
	// Unsubscribe unsubscribes a table span.
	Unsubscribe(subID SubscriptionID)
	// GetObservabilitySnapshot returns a point-in-time snapshot for debugging.
	GetObservabilitySnapshot(sampleLimit int) ObservabilitySnapshot
}

type subscriptionClient struct {
	ctx       context.Context
	cancel    context.CancelFunc
	config    *SubscriptionClientConfig
	clusterID uint64

	regionRuntimeRegistry *regionRuntimeRegistry
	failureStats          *failureStats

	pd           pd.Client
	regionCache  *tikv.RegionCache
	pdClock      pdutil.Clock
	lockResolver txnutil.LockResolver

	requestedStores   *requestedStoreSet
	regionScheduler   *regionRequestScheduler
	subscribedSpans   *subscribedSpanSet
	staleLockResolver *staleLockResolver
	observability     *observabilityReporter

	ds dynstream.DynamicStream[int, SubscriptionID, regionEvent, *subscribedSpan, *regionEventHandler]
	// the following three fields are used to manage feedback from ds and notify other goroutines
	mu     sync.Mutex
	cond   *sync.Cond
	paused atomic.Bool

	// the credential to connect tikv
	credential *security.Credential
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

		clusterID:             pd.GetClusterID(context.Background()),
		pd:                    pd,
		regionCache:           appcontext.GetService[*tikv.RegionCache](appcontext.RegionCache),
		pdClock:               appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		lockResolver:          lockResolver,
		regionRuntimeRegistry: newRegionRuntimeRegistry(),
		failureStats:          newFailureStats(),

		credential: credential,
	}
	subClient.ctx, subClient.cancel = context.WithCancel(context.Background())
	subClient.requestedStores = newRequestedStoreSet(subClient)
	subClient.regionScheduler = newRegionRequestScheduler(subClient)
	subClient.subscribedSpans = newSubscribedSpanSet(subClient)
	subClient.staleLockResolver = newStaleLockResolver(subClient)
	subClient.observability = newObservabilityReporter(subClient)

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

// AllocSubscriptionID gets an ID that can be used in Subscribe.
func (s *subscriptionClient) AllocSubscriptionID() SubscriptionID {
	return SubscriptionID(subscriptionIDGen.Add(1))
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
		zap.Uint64("subscriptionID", uint64(rt.subID)))
}

func (s *subscriptionClient) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return s.handleDSFeedBack(ctx) })
	s.observability.run(ctx, g)
	s.regionScheduler.run(ctx, g)
	s.staleLockResolver.run(ctx, g)

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
	s.regionScheduler.close()
	return nil
}
