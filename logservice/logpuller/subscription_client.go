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
	"sort"
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

	stalledSpanThreshold      = 30 * time.Second
	stalledSpanLogSampleLimit = 8
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
	// get a point-in-time observability snapshot for debugging
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
	if s.failureStats == nil {
		s.failureStats = newFailureStats()
	}
}

func (s *subscriptionClient) markRegionDiscovered(region *regionInfo, now time.Time) {
	if region.verID.GetID() == 0 {
		return
	}
	if !region.runtimeKey.isValid() {
		region.runtimeKey = s.regionRuntimeRegistry.allocKey(region.subscribedSpan.subID, region.verID.GetID())
		s.regionRuntimeRegistry.markDiscovered(region.runtimeKey, *region, now)
	}
}

func (s *subscriptionClient) updateRegionRuntimeInfo(region regionInfo) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.updateRegionInfo(region.runtimeKey, region)
}

func (s *subscriptionClient) markRegionRangeLockWait(region regionInfo, now time.Time) {
	if !region.runtimeKey.isValid() {
		return
	}
	s.regionRuntimeRegistry.markRangeLockWait(region.runtimeKey, now)
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
	s.regionRuntimeRegistry.markQueued(region.runtimeKey, acquiredTime, queuedTime, region.resolvedTs())
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
	s.regionRuntimeRegistry.markRemoved(region.runtimeKey, now)
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
		failureStats:          newFailureStats(),

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

			requestStats := s.requestedStores.requestStats()
			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Set(float64(requestStats.Total))
			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("queued").Set(float64(requestStats.Queued))
			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("processing").Set(float64(requestStats.Processing))
			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("sent").Set(float64(requestStats.Sent))

			counts := s.regionRuntimePhaseCounts()
			for _, phase := range regionRuntimePhases {
				metrics.SubscriptionClientRegionRuntimePhaseCount.WithLabelValues(string(phase)).Set(float64(counts[phase]))
			}
			stalledSpanReport := s.collectStalledSpanReport(s.pdClock.CurrentTime(), 0, -1)
			metrics.SubscriptionClientStalledSpanCount.Set(float64(stalledSpanReport.stalledSpanCount))
			for _, blockerType := range resolvedTsBlockerTypes {
				metrics.SubscriptionClientStalledSpanCountByBlockerType.WithLabelValues(string(blockerType)).
					Set(float64(stalledSpanReport.blockerCounts[blockerType]))
			}
			metrics.SubscriptionClientStalledSpanMaxResolvedTsLag.Set(stalledSpanReport.maxResolvedTsLag.Seconds())
			metrics.SubscriptionClientStalledSpanMaxResolvedTsUpdatedAge.Set(stalledSpanReport.maxResolvedTsUpdatedAgo.Seconds())

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
	g.Go(func() error { return s.logStalledSpans(ctx) })
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

func (s *subscriptionClient) GetResolvedTsLag() float64 {
	return s.subscribedSpans.getResolvedTsLag()
}

func phaseCountsToStrings(counts map[regionPhase]int) map[string]int {
	result := make(map[string]int, len(counts))
	for phase, count := range counts {
		result[string(phase)] = count
	}
	return result
}

func trackedRegionCount(counts map[regionPhase]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}

var resolvedTsBlockerTypes = []regionlock.ResolvedTsBlockerType{
	regionlock.ResolvedTsBlockerUnlockedRange,
	regionlock.ResolvedTsBlockerUninitializedRegion,
	regionlock.ResolvedTsBlockerInitializedRegion,
}

func blockerCountsToStrings(counts map[regionlock.ResolvedTsBlockerType]int) map[string]int {
	result := make(map[string]int, len(resolvedTsBlockerTypes))
	for _, blockerType := range resolvedTsBlockerTypes {
		result[string(blockerType)] = counts[blockerType]
	}
	return result
}

type stalledSpanReport struct {
	stalledSpanCount        int
	blockerCounts           map[regionlock.ResolvedTsBlockerType]int
	maxResolvedTsLag        time.Duration
	maxResolvedTsUpdatedAgo time.Duration
	samples                 []stalledSpanSample
}

type stalledSpanSample struct {
	SubscriptionID       uint64
	TableID              int64
	Span                 string
	Initialized          bool
	ResolvedTs           uint64
	ResolvedTsLag        time.Duration
	ResolvedTsUpdatedAgo time.Duration
	LockedRegionCount    int
	UnlockedRangeCount   int
	BlockedBy            []SpanResolvedTsBlockerSnapshot
}

func spanStallDurations(
	now time.Time,
	span *subscribedSpan,
) (resolvedTsUpdatedAgo time.Duration, resolvedTsLag time.Duration, stalled bool) {
	resolvedTsUpdatedUnix := span.resolvedTsUpdated.Load()
	resolvedTs := span.resolvedTs.Load()
	if resolvedTsUpdatedUnix == 0 || resolvedTs == 0 {
		return 0, 0, false
	}

	resolvedTsUpdatedAgo = normalizeDuration(now.Sub(time.Unix(resolvedTsUpdatedUnix, 0)))
	resolvedTsLag = normalizeDuration(now.Sub(oracle.GetTimeFromTS(resolvedTs)))
	return resolvedTsUpdatedAgo, resolvedTsLag,
		resolvedTsUpdatedAgo >= stalledSpanThreshold && resolvedTsLag >= stalledSpanThreshold
}

func betterStalledSpanSample(left, right stalledSpanSample) bool {
	if left.ResolvedTsUpdatedAgo != right.ResolvedTsUpdatedAgo {
		return left.ResolvedTsUpdatedAgo > right.ResolvedTsUpdatedAgo
	}
	if left.ResolvedTsLag != right.ResolvedTsLag {
		return left.ResolvedTsLag > right.ResolvedTsLag
	}
	if left.SubscriptionID != right.SubscriptionID {
		return left.SubscriptionID < right.SubscriptionID
	}
	if left.TableID != right.TableID {
		return left.TableID < right.TableID
	}
	return left.Span < right.Span
}

func insertStalledSpanSample(
	samples []stalledSpanSample,
	sample stalledSpanSample,
	limit int,
) []stalledSpanSample {
	if limit <= 0 {
		return samples
	}
	originalLen := len(samples)
	if originalLen == limit && !betterStalledSpanSample(sample, samples[originalLen-1]) {
		return samples
	}

	index := sort.Search(originalLen, func(i int) bool {
		return betterStalledSpanSample(sample, samples[i])
	})
	if originalLen < limit {
		samples = append(samples, stalledSpanSample{})
	} else {
		if index >= limit {
			index = limit - 1
		}
	}
	copy(samples[index+1:], samples[index:])
	samples[index] = sample
	if len(samples) > limit {
		samples = samples[:limit]
	}
	return samples
}

func spanResolvedTsBlockerType(
	blockerType regionlock.ResolvedTsBlockerType,
) SpanResolvedTsBlockerType {
	switch blockerType {
	case regionlock.ResolvedTsBlockerUnlockedRange:
		return SpanResolvedTsBlockerUnlockedRange
	case regionlock.ResolvedTsBlockerUninitializedRegion:
		return SpanResolvedTsBlockerUninitializedRegion
	case regionlock.ResolvedTsBlockerInitializedRegion:
		return SpanResolvedTsBlockerInitializedRegion
	default:
		return SpanResolvedTsBlockerType(blockerType)
	}
}

func durationSinceString(now time.Time, timestamp time.Time) string {
	if timestamp.IsZero() {
		return ""
	}
	return normalizeDuration(now.Sub(timestamp)).String()
}

func convertRegionRuntimeBlocker(
	now time.Time,
	state regionRuntimeState,
) RegionRuntimeBlockerSnapshot {
	phase := state.phase
	if phase == "" {
		phase = regionPhaseUnknown
	}
	return RegionRuntimeBlockerSnapshot{
		Phase:        string(phase),
		PhaseAge:     state.phaseAge(now).String(),
		LastEventAgo: durationSinceString(now, state.lastEventTime),
		StoreAddr:    state.storeAddr,
		WorkerID:     state.workerID,
		LastError:    state.lastError,
	}
}

func (s *subscriptionClient) convertResolvedTsBlocker(
	now time.Time,
	subID SubscriptionID,
	parentSpan heartbeatpb.TableSpan,
	blocker regionlock.ResolvedTsBlocker,
) SpanResolvedTsBlockerSnapshot {
	blockerSpan := blocker.Span
	blockerSpan.KeyspaceID = parentSpan.KeyspaceID
	blockerSpan.TableID = parentSpan.TableID
	snapshot := SpanResolvedTsBlockerSnapshot{
		Type:       spanResolvedTsBlockerType(blocker.Type),
		RegionID:   blocker.RegionID,
		Span:       common.FormatTableSpan(&blockerSpan),
		ResolvedTs: blocker.ResolvedTs,
		CreatedAgo: durationSinceString(now, blocker.Created),
	}

	switch blocker.Type {
	case regionlock.ResolvedTsBlockerUninitializedRegion,
		regionlock.ResolvedTsBlockerInitializedRegion:
		initialized := blocker.Initialized
		snapshot.Initialized = &initialized
		if runtimeState, ok := s.regionRuntimeRegistry.getLatest(subID, blocker.RegionID); ok {
			runtimeSnapshot := convertRegionRuntimeBlocker(now, runtimeState)
			snapshot.Runtime = &runtimeSnapshot
		}
	}
	return snapshot
}

func (s *subscriptionClient) makeStalledSpanSample(
	now time.Time,
	entry subscribedSpanEntry,
	resolvedTsUpdatedAgo time.Duration,
	resolvedTsLag time.Duration,
	blockerStats regionlock.ResolvedTsBlockerStatistics,
) stalledSpanSample {
	blockers := make([]SpanResolvedTsBlockerSnapshot, 0, len(blockerStats.Blockers))
	for _, blocker := range blockerStats.Blockers {
		blockers = append(blockers,
			s.convertResolvedTsBlocker(now, entry.subID, entry.span.span, blocker))
	}

	span := entry.span.span
	return stalledSpanSample{
		SubscriptionID:       uint64(entry.subID),
		TableID:              span.TableID,
		Span:                 common.FormatTableSpan(&span),
		Initialized:          entry.span.initialized.Load(),
		ResolvedTs:           entry.span.resolvedTs.Load(),
		ResolvedTsLag:        resolvedTsLag,
		ResolvedTsUpdatedAgo: resolvedTsUpdatedAgo,
		LockedRegionCount:    blockerStats.LockedRegionCount,
		UnlockedRangeCount:   blockerStats.UnlockedRangeCount,
		BlockedBy:            blockers,
	}
}

func convertStalledSpanSamples(samples []stalledSpanSample) []StalledSpanSnapshot {
	snapshots := make([]StalledSpanSnapshot, 0, len(samples))
	for _, sample := range samples {
		snapshots = append(snapshots, StalledSpanSnapshot{
			SubscriptionID:       sample.SubscriptionID,
			TableID:              sample.TableID,
			Span:                 sample.Span,
			Initialized:          sample.Initialized,
			ResolvedTs:           sample.ResolvedTs,
			ResolvedTsLag:        sample.ResolvedTsLag.String(),
			ResolvedTsUpdatedAgo: sample.ResolvedTsUpdatedAgo.String(),
			LockedRegionCount:    sample.LockedRegionCount,
			UnlockedRangeCount:   sample.UnlockedRangeCount,
			BlockedBy:            sample.BlockedBy,
		})
	}
	return snapshots
}

func (s *subscriptionClient) collectStalledSpanReport(
	now time.Time,
	sampleLimit int,
	blockerLimit int,
) stalledSpanReport {
	report := stalledSpanReport{
		blockerCounts: make(map[regionlock.ResolvedTsBlockerType]int, len(resolvedTsBlockerTypes)),
	}
	for _, entry := range s.subscribedSpans.snapshot() {
		if entry.span == nil {
			continue
		}
		resolvedTsUpdatedAgo, resolvedTsLag, stalled := spanStallDurations(now, entry.span)
		if !stalled {
			continue
		}

		blockerStats := entry.span.rangeLock.CollectResolvedTsBlockers(blockerLimit)
		report.stalledSpanCount++
		for _, blockerType := range resolvedTsBlockerTypes {
			if blockerStats.BlockerTypeCounts[blockerType] > 0 {
				report.blockerCounts[blockerType]++
			}
		}
		if resolvedTsLag > report.maxResolvedTsLag {
			report.maxResolvedTsLag = resolvedTsLag
		}
		if resolvedTsUpdatedAgo > report.maxResolvedTsUpdatedAgo {
			report.maxResolvedTsUpdatedAgo = resolvedTsUpdatedAgo
		}
		if sampleLimit <= 0 {
			continue
		}

		report.samples = insertStalledSpanSample(
			report.samples,
			s.makeStalledSpanSample(now, entry, resolvedTsUpdatedAgo, resolvedTsLag, blockerStats),
			sampleLimit,
		)
	}
	return report
}

func (s *subscriptionClient) runtimeObservability(now time.Time, sampleLimit int) RuntimeObservability {
	phaseCounts := s.regionRuntimePhaseCounts()
	stalledSpanReport := s.collectStalledSpanReport(now, sampleLimit, sampleLimit)

	return RuntimeObservability{
		TrackedRegionCount: trackedRegionCount(phaseCounts),
		PhaseCounts:        phaseCountsToStrings(phaseCounts),
		StalledSpanCount:   stalledSpanReport.stalledSpanCount,
		StalledSpans:       convertStalledSpanSamples(stalledSpanReport.samples),
	}
}

func (s *subscriptionClient) GetObservabilitySnapshot(sampleLimit int) ObservabilitySnapshot {
	sampleLimit = normalizeObservabilitySampleLimit(sampleLimit)
	now := s.pdClock.CurrentTime()
	return ObservabilitySnapshot{
		GeneratedAt: now,
		Runtime:     s.runtimeObservability(now, sampleLimit),
		Stores:      s.requestedStores.snapshotStores(),
	}
}

func (s *subscriptionClient) logStalledSpans(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		now := s.pdClock.CurrentTime()
		report := s.collectStalledSpanReport(
			now,
			stalledSpanLogSampleLimit,
			stalledSpanLogSampleLimit,
		)
		if report.stalledSpanCount == 0 {
			continue
		}

		phaseCounts := s.regionRuntimePhaseCounts()
		log.Info("subscription client stalled span summary",
			zap.Int("trackedRegionCount", trackedRegionCount(phaseCounts)),
			zap.Any("phaseCounts", phaseCountsToStrings(phaseCounts)),
			zap.String("stalledSpanThreshold", stalledSpanThreshold.String()),
			zap.Int("stalledSpanCount", report.stalledSpanCount),
			zap.Duration("maxResolvedTsLag", report.maxResolvedTsLag),
			zap.Duration("maxResolvedTsUpdatedAgo", report.maxResolvedTsUpdatedAgo),
			zap.Any("blockerCounts", blockerCountsToStrings(report.blockerCounts)),
			zap.Any("samples", convertStalledSpanSamples(report.samples)))
	}
}
