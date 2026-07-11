// Copyright 2025 PingCAP, Inc.
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

package eventservice

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// scanAdmission owns a changefeed memory-quota reservation for one scan. The
// reservation is kept for emitted events and the unused portion is returned
// after the scanner finishes.
type scanAdmission struct {
	quota    *atomic.Uint64
	reserved uint64
	limit    scanLimit
}

func (a scanAdmission) releaseUnused(scannedBytes int64) {
	if scannedBytes <= 0 {
		a.releaseAll()
		return
	}
	if scannedBytes < int64(a.reserved) {
		releaseQuota(a.quota, a.reserved-uint64(scannedBytes))
	}
}

func (a scanAdmission) releaseAll() {
	releaseQuota(a.quota, a.reserved)
}

// getScanTaskDataRange determines the range that can be scanned without
// crossing the schema, scan-window, or sync-point barriers.
func (c *eventBroker) getScanTaskDataRange(task scanTask) (bool, common.DataRange) {
	dataRange, needScan := task.getDataRange()
	if !needScan {
		updateMetricEventServiceSkipResolvedTsCount(task.info.GetMode())
		return false, common.DataRange{}
	}

	keyspaceMeta := common.KeyspaceMeta{
		ID:   task.info.GetTableSpan().KeyspaceID,
		Name: task.changefeedStat.changefeedID.Keyspace(),
	}
	ddlState, err := c.schemaStore.GetTableDDLEventState(keyspaceMeta, task.info.GetTableSpan().TableID)
	if err != nil {
		log.Error("get table ddl event state failed",
			zap.Uint32("keyspaceID", task.info.GetTableSpan().KeyspaceID),
			zap.Int64("tableID", task.info.GetTableSpan().TableID), zap.Error(err))
		return false, common.DataRange{}
	}

	dataRange.CommitTsEnd = min(dataRange.CommitTsEnd, ddlState.ResolvedTs)
	endBeforeScanWindow := dataRange.CommitTsEnd
	hasPendingDDL := dataRange.CommitTsStart < ddlState.MaxEventCommitTs &&
		ddlState.MaxEventCommitTs <= endBeforeScanWindow
	nextSyncPointTs := task.nextSyncPoint.Load()
	hasPendingSyncPoint := task.enableSyncPoint && endBeforeScanWindow > nextSyncPointTs

	c.limitScanRangeByWindow(task, &dataRange, endBeforeScanWindow)
	c.advanceRangeForPendingBarrier(task, &dataRange, endBeforeScanWindow, ddlState.MaxEventCommitTs, hasPendingDDL, hasPendingSyncPoint, nextSyncPointTs)

	if dataRange.CommitTsEnd <= dataRange.CommitTsStart {
		updateMetricEventServiceSkipResolvedTsCount(task.info.GetMode())
		// The capped range made no progress. Keep the downstream responsive
		// without advancing its durable watermark.
		c.sendSignalResolvedTs(task)
		return false, common.DataRange{}
	}

	// The iterator is only needed when it may contain DML or when a DDL has to
	// be merged with DML at the same commit-ts boundary.
	noDMLEvent := dataRange.CommitTsStart > task.eventStoreCommitTs.Load()
	noDDLEvent := dataRange.CommitTsStart >= ddlState.MaxEventCommitTs
	if noDMLEvent && noDDLEvent {
		c.sendResolvedTs(task, dataRange.CommitTsEnd)
		return false, common.DataRange{}
	}
	return true, dataRange
}

func (c *eventBroker) limitScanRangeByWindow(task scanTask, dataRange *common.DataRange, endBeforeScanWindow uint64) {
	scanMaxTs := task.changefeedStat.getScanMaxTs()
	if scanMaxTs == 0 {
		return
	}

	dataRange.CommitTsEnd = min(dataRange.CommitTsEnd, scanMaxTs)
	if dataRange.CommitTsEnd >= endBeforeScanWindow {
		return
	}
	log.Debug("scan window capped",
		zap.Stringer("changefeedID", task.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", task.id),
		zap.Uint64("baseTs", task.changefeedStat.minSentTs.Load()),
		zap.Uint64("scanMaxTs", scanMaxTs),
		zap.Uint64("beforeEndTs", endBeforeScanWindow),
		zap.Uint64("afterEndTs", dataRange.CommitTsEnd),
		zap.Duration("scanInterval", time.Duration(task.changefeedStat.scanInterval.Load())))
}

func (c *eventBroker) advanceRangeForPendingBarrier(
	task scanTask,
	dataRange *common.DataRange,
	endBeforeScanWindow uint64,
	ddlCommitTs uint64,
	hasPendingDDL, hasPendingSyncPoint bool,
	nextSyncPointTs uint64,
) {
	if dataRange.CommitTsEnd > dataRange.CommitTsStart || (!hasPendingDDL && !hasPendingSyncPoint) {
		return
	}

	interval := time.Duration(task.changefeedStat.scanInterval.Load())
	if interval <= 0 {
		interval = defaultScanInterval
	}
	localScanMaxTs := oracle.GoTimeToTS(oracle.GetTimeFromTS(dataRange.CommitTsStart).Add(interval))
	if hasPendingSyncPoint && nextSyncPointTs >= dataRange.CommitTsStart && localScanMaxTs <= nextSyncPointTs {
		localScanMaxTs = nextSyncPointTs + 1
	}
	dataRange.CommitTsEnd = min(endBeforeScanWindow, localScanMaxTs)
	if dataRange.CommitTsEnd <= dataRange.CommitTsStart {
		return
	}

	log.Info("scan window local advance due to pending barrier event",
		zap.Stringer("changefeedID", task.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", task.id),
		zap.Uint64("startTs", dataRange.CommitTsStart),
		zap.Uint64("globalScanMaxTs", task.changefeedStat.getScanMaxTs()),
		zap.Uint64("localScanMaxTs", localScanMaxTs),
		zap.Bool("hasPendingDDL", hasPendingDDL),
		zap.Uint64("ddlCommitTs", ddlCommitTs),
		zap.Bool("hasPendingSyncPoint", hasPendingSyncPoint),
		zap.Uint64("nextSyncPointTs", nextSyncPointTs),
		zap.Uint64("newEndTs", dataRange.CommitTsEnd))
}

// scanReady performs the inexpensive eligibility checks before a task is
// enqueued. The range is checked again by doScan because notifications can
// advance state while a task waits in the worker queue.
func (c *eventBroker) scanReady(task scanTask) bool {
	span := task.info.GetTableSpan()
	if span.Equal(common.KeyspaceDDLSpan(span.KeyspaceID)) || task.isRemoved.Load() || task.isTaskScanning.Load() {
		return false
	}
	if !c.checkAndSendReady(task) {
		return false
	}
	c.sendHandshakeIfNeed(task)
	needScan, _ := c.getScanTaskDataRange(task)
	return needScan
}

func (c *eventBroker) checkAndSendReady(task scanTask) bool {
	if task.epoch != 0 {
		return true
	}

	now := time.Now().Unix()
	lastSendTime := task.lastReadySendTime.Load()
	interval := task.readyInterval.Load()
	if now-lastSendTime < interval {
		return false
	}

	remoteID := node.ID(task.info.GetServerID())
	c.getMessageCh(task.messageWorkerIndex, common.IsRedoMode(task.info.GetMode())) <- newWrapReadyEvent(remoteID, event.NewReadyEvent(task.info.GetID()))
	log.Debug("send ready event to dispatcher",
		zap.Stringer("changefeedID", task.changefeedStat.changefeedID), zap.Stringer("dispatcherID", task.id))
	task.lastReadySendTime.Store(now)
	task.readyInterval.Store(min(interval*2, int64(maxReadyEventIntervalSeconds)))
	updateMetricEventServiceSendCommandCount(task.info.GetMode())
	return false
}

func (c *eventBroker) sendHandshakeIfNeed(task scanTask) {
	if task.isHandshaked() {
		return
	}

	task.handshakeLock.Lock()
	defer task.handshakeLock.Unlock()
	if task.isHandshaked() {
		return
	}

	remoteID := node.ID(task.info.GetServerID())
	handshake := event.NewHandshakeEvent(task.id, task.startTs, task.epoch, task.startTableInfo)
	log.Info("send handshake event to dispatcher",
		zap.Stringer("changefeedID", task.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", task.id),
		zap.Int64("tableID", task.info.GetTableSpan().GetTableID()),
		zap.Uint64("commitTs", handshake.GetCommitTs()),
		zap.Uint64("epoch", handshake.GetEpoch()), zap.Uint64("seq", handshake.GetSeq()))
	c.getMessageCh(task.messageWorkerIndex, common.IsRedoMode(task.info.GetMode())) <- newWrapHandshakeEvent(remoteID, handshake)
	updateMetricEventServiceSendCommandCount(task.info.GetMode())
	// Queue the handshake before exposing the handshaked state so later events
	// cannot overtake it on the same message worker.
	task.setHandshaked()
}

// hasSyncPointEventsBeforeTs reports whether sending an event at ts must first
// emit a sync point. The corresponding emission is performed in the same
// message-worker queue, preserving dispatcher order.
func (c *eventBroker) hasSyncPointEventsBeforeTs(ts uint64, task scanTask) bool {
	return task.enableSyncPoint && ts > task.nextSyncPoint.Load()
}

func (c *eventBroker) emitSyncPointEventIfNeeded(ts uint64, task scanTask, remoteID node.ID) {
	for task.enableSyncPoint && ts > task.nextSyncPoint.Load() {
		commitTs := task.nextSyncPoint.Load()
		task.nextSyncPoint.Store(oracle.GoTimeToTS(oracle.GetTimeFromTS(commitTs).Add(task.syncPointInterval)))

		syncPoint := event.NewSyncPointEvent(task.id, commitTs, task.seq.Add(1), task.epoch)
		log.Debug("send sync point event to dispatcher",
			zap.Stringer("changefeedID", task.changefeedStat.changefeedID), zap.Stringer("dispatcherID", task.id),
			zap.Int64("tableID", task.info.GetTableSpan().GetTableID()), zap.Uint64("commitTs", syncPoint.GetCommitTs()),
			zap.Uint64("seq", syncPoint.GetSeq()))
		c.getMessageCh(task.messageWorkerIndex, common.IsRedoMode(task.info.GetMode())) <- newWrapSyncPointEvent(remoteID, syncPoint)
	}
}

func (c *eventBroker) calculateScanLimit(task scanTask) scanLimit {
	return scanLimit{maxDMLBytes: task.getCurrentScanLimitInBytes()}
}

func (c *eventBroker) admitScan(task scanTask, remoteID node.ID) (scanAdmission, bool) {
	changefeedID := task.info.GetChangefeedID()
	item, ok := c.changefeedMap.Load(changefeedID)
	if !ok || item != task.changefeedStat {
		log.Info("changefeed status is not found, skip scan",
			zap.Stringer("changefeedID", changefeedID), zap.Stringer("dispatcherID", task.id),
			zap.Int64("tableID", task.info.GetTableSpan().GetTableID()))
		return scanAdmission{}, false
	}

	item, ok = task.changefeedStat.availableMemoryQuota.Load(remoteID)
	if !ok {
		log.Info("available memory quota is not set, skip scan",
			zap.Stringer("changefeedID", changefeedID), zap.Stringer("nodeID", remoteID))
		return scanAdmission{}, false
	}

	quota := item.(*atomic.Uint64)
	if quota.Load() < c.scanLimitInBytes {
		task.resetScanLimit()
	}
	limit := c.calculateScanLimit(task)
	reserved := uint64(limit.maxDMLBytes)

	// Dispatcher quota is a current downstream capacity report. Check it before
	// changing the shared changefeed quota; otherwise a rejected scan leaks a
	// reservation until the next congestion-control report arrives.
	if reserved > task.availableMemoryQuota.Load() {
		log.Debug("dispatcher available memory quota is not enough, skip scan",
			zap.Stringer("dispatcherID", task.id), zap.Uint64("available", task.availableMemoryQuota.Load()),
			zap.Uint64("required", reserved))
		c.sendSignalResolvedTs(task)
		metrics.EventServiceSkipScanCount.WithLabelValues("dispatcher_quota").Inc()
		return scanAdmission{}, false
	}
	if !allocQuota(quota, reserved) {
		log.Debug("changefeed available memory quota is not enough, skip scan",
			zap.Stringer("changefeedID", changefeedID), zap.Stringer("nodeID", remoteID),
			zap.Uint64("available", quota.Load()), zap.Uint64("required", reserved))
		c.sendSignalResolvedTs(task)
		metrics.EventServiceSkipScanCount.WithLabelValues("changefeed_quota").Inc()
		return scanAdmission{}, false
	}
	return scanAdmission{quota: quota, reserved: reserved, limit: limit}, true
}

func (c *eventBroker) doScan(ctx context.Context, task scanTask) {
	var interrupted bool
	defer func() {
		task.isTaskScanning.Store(false)
		if interrupted {
			c.pushTask(task, false)
		}
	}()

	if task.isRemoved.Load() {
		return
	}
	remoteID := node.ID(task.info.GetServerID())
	if !c.msgSender.IsReadyToSend(remoteID) {
		log.Info("remote target is not ready, skip scan",
			zap.Stringer("changefeedID", task.info.GetChangefeedID()), zap.Stringer("dispatcherID", task.id),
			zap.Int64("tableID", task.info.GetTableSpan().GetTableID()), zap.Stringer("nodeID", remoteID))
		return
	}

	needScan, dataRange := c.getScanTaskDataRange(task)
	if !needScan {
		return
	}
	if !c.scanRateLimiter.AllowN(time.Now(), int(task.lastScanBytes.Load())) {
		log.Debug("scan rate limit exceeded", zap.Stringer("dispatcherID", task.id),
			zap.Int64("lastScanBytes", task.lastScanBytes.Load()), zap.Uint64("sentResolvedTs", task.sentResolvedTs.Load()))
		return
	}

	admission, ok := c.admitScan(task, remoteID)
	if !ok {
		return
	}
	scanner := newEventScanner(c.eventStore, c.schemaStore, c.mounter, task.info.GetMode())
	scannedBytes, events, interrupted, err := scanner.scan(ctx, task, dataRange, admission.limit)

	if interrupted {
		metrics.EventServiceInterruptScanCount.Inc()
	}
	if err != nil {
		// Scanner errors do not enqueue any returned events, so none of the
		// reservation remains owned by the downstream message queue.
		admission.releaseAll()
		log.Error("scan events failed",
			zap.Stringer("changefeedID", task.changefeedStat.changefeedID), zap.Stringer("dispatcherID", task.id),
			zap.Int64("tableID", task.info.GetTableSpan().GetTableID()), zap.Any("dataRange", dataRange),
			zap.Uint64("receivedResolvedTs", task.receivedResolvedTs.Load()), zap.Uint64("sentResolvedTs", task.sentResolvedTs.Load()), zap.Error(err))
		return
	}
	admission.releaseUnused(scannedBytes)

	if scannedBytes > int64(c.scanLimitInBytes) {
		log.Info("scan bytes exceeded the limit, there must be a big transaction",
			zap.Stringer("dispatcherID", task.id), zap.Int64("scannedBytes", scannedBytes), zap.Uint64("limit", c.scanLimitInBytes))
		scannedBytes = int64(c.scanLimitInBytes)
	}
	task.lastScanBytes.Store(scannedBytes)
	c.sendScannedEvents(ctx, task, remoteID, events)
	metricEventBrokerScanTaskCount.Inc()
}

func (c *eventBroker) sendScannedEvents(ctx context.Context, task scanTask, remoteID node.ID, events []event.Event) {
	for _, e := range events {
		if task.isRemoved.Load() || ctx.Err() != nil {
			return
		}
		switch e.GetType() {
		case event.TypeBatchDMLEvent:
			dmls, ok := e.(*event.BatchDMLEvent)
			if !ok {
				log.Panic("expect a DML event, but got", zap.Any("event", e))
			}
			c.sendDML(remoteID, dmls, task)
		case event.TypeDDLEvent:
			ddl, ok := e.(*event.DDLEvent)
			if !ok {
				log.Panic("expect a DDL event, but got", zap.Any("event", e))
			}
			c.sendDDL(ctx, remoteID, ddl, task)
		case event.TypeResolvedEvent:
			resolved, ok := e.(event.ResolvedEvent)
			if !ok {
				log.Panic("expect a resolved event, but got", zap.Any("event", e))
			}
			c.sendResolvedTs(task, resolved.ResolvedTs)
		default:
			log.Panic("unknown event type", zap.Any("event", e))
		}
	}
}

func allocQuota(quota *atomic.Uint64, nBytes uint64) bool {
	for {
		available := quota.Load()
		if available < nBytes {
			return false
		}
		if quota.CompareAndSwap(available, available-nBytes) {
			return true
		}
	}
}

func releaseQuota(quota *atomic.Uint64, nBytes uint64) {
	quota.Add(nBytes)
}
