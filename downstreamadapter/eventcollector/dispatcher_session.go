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

package eventcollector

import (
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type dispatcherConnState struct {
	sync.RWMutex
	// removed marks the session as terminal. Once set, control-plane state is no
	// longer mutated and late signal events are ignored.
	removed bool
	// currentEventServiceID is the event service currently used to receive data.
	// It is set only after a ready signal has been accepted.
	currentEventServiceID node.ID
	// localReadyPending is set after a local register request is sent and remains
	// true until local ready is accepted or the session is cleared.
	localReadyPending bool
	// pendingRemoteTarget tracks the remote event service currently being probed.
	// It is non-empty only while waiting for ready/not reusable from that remote.
	pendingRegisterTarget node.ID
	// the remote event services which may contain data this dispatcher needed
	remoteCandidates []string
}

type readyAcceptance struct {
	commitTarget   node.ID
	cleanupTargets []node.ID
}

// appendCleanupTarget keeps cleanup target collection local to connState
// transitions so session code can stay focused on side effects.
func appendCleanupTarget(cleanupTargets []node.ID, target node.ID, skip node.ID) []node.ID {
	if target.IsEmpty() || target == skip {
		return cleanupTargets
	}
	for _, existing := range cleanupTargets {
		if existing == target {
			return cleanupTargets
		}
	}
	return append(cleanupTargets, target)
}

// dispatcherConnState owns the control-plane state machine for one dispatcher.
// It does not send messages. Its job is to apply atomic state transitions and
// return the side-effect inputs that dispatcherSession should execute.
//
// The state machine tracks three orthogonal pieces of control-plane state:
// 1. currentEventServiceID: which event service is currently serving data.
// 2. localReadyPending: whether local register has been sent but local ready has
//    not been accepted yet.
// 3. pendingRegisterTarget: which remote candidate is currently being probed.
//
// This lets the collector represent the common startup path correctly:
// - after add: current="", localReadyPending=true, pendingRemote=""
// - after choosing a remote candidate: current="", localReadyPending=true,
//   pendingRemote="<remote>"
// - after remote ready first: current="<remote>", localReadyPending=true,
//   pendingRemote=""
// - after local ready later: current="<local>", localReadyPending=false,
//   pendingRemote=""
//
// In other words, waiting for local ready and waiting for remote ready are not
// mutually exclusive states.

// Registration state transitions.
func (d *dispatcherConnState) beginRegisterToLocal() {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return
	}
	d.localReadyPending = true
}

func (d *dispatcherConnState) beginRegisterToRemote(serverID node.ID) {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return
	}
	d.pendingRegisterTarget = serverID
}

// Signal-event state transitions.
//
// acceptReady enforces the ready acceptance rules:
//  1. once local is already serving, any later remote ready is stale and should
//     only trigger cleanup;
//  2. local ready can be accepted while local registration is still pending, and
//     local wins over any remote that started serving earlier;
//  3. remote ready is accepted only from the single remote candidate currently
//     being probed.
func (d *dispatcherConnState) acceptReady(from node.ID, localServerID node.ID) readyAcceptance {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return readyAcceptance{}
	}

	// Once local is serving, any later remote ready can only be stale and should
	// be cleaned up. Another local ready does not change state.
	if d.currentEventServiceID == localServerID {
		if from != localServerID {
			return readyAcceptance{
				cleanupTargets: []node.ID{from},
			}
		}
		return readyAcceptance{}
	}

	if from == localServerID {
		if !d.localReadyPending {
			return readyAcceptance{}
		}

		cleanupTargets := make([]node.ID, 0, 2)
		cleanupTargets = appendCleanupTarget(cleanupTargets, d.currentEventServiceID, localServerID)
		cleanupTargets = appendCleanupTarget(cleanupTargets, d.pendingRegisterTarget, localServerID)

		d.currentEventServiceID = localServerID
		d.localReadyPending = false
		d.pendingRegisterTarget = ""
		d.remoteCandidates = nil
		return readyAcceptance{
			commitTarget:   localServerID,
			cleanupTargets: cleanupTargets,
		}
	}

	if d.pendingRegisterTarget != from {
		return readyAcceptance{}
	}

	d.currentEventServiceID = from
	d.pendingRegisterTarget = ""
	return readyAcceptance{
		commitTarget: from,
	}
}

func (d *dispatcherConnState) beginRemove(localServerID node.ID) ([]node.ID, bool) {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return nil, true
	}
	cleanupTargets := make([]node.ID, 0, 3)
	cleanupTargets = appendCleanupTarget(cleanupTargets, localServerID, "")
	cleanupTargets = appendCleanupTarget(cleanupTargets, d.currentEventServiceID, "")
	cleanupTargets = appendCleanupTarget(cleanupTargets, d.pendingRegisterTarget, "")
	d.removed = true
	d.currentEventServiceID = ""
	d.localReadyPending = false
	d.pendingRegisterTarget = ""
	d.remoteCandidates = nil
	return cleanupTargets, false
}

// Read-only state queries used by session orchestration and other collector
// components.
func (d *dispatcherConnState) getCurrentEventServiceID() node.ID {
	d.RLock()
	defer d.RUnlock()
	return d.currentEventServiceID
}

func (d *dispatcherConnState) isReceivingDataEvent() bool {
	d.RLock()
	defer d.RUnlock()
	return !d.currentEventServiceID.IsEmpty()
}

func (d *dispatcherConnState) isRemoved() bool {
	d.RLock()
	defer d.RUnlock()
	return d.removed
}

// Remote-probing transitions.
func (d *dispatcherConnState) beginRemoteProbing(nodes []string) (node.ID, bool) {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return "", false
	}
	// reading from a event service or checking remotes already, ignore
	if !d.currentEventServiceID.IsEmpty() || !d.pendingRegisterTarget.IsEmpty() {
		return "", false
	}
	if len(nodes) == 0 {
		return "", false
	}
	candidate := node.ID(nodes[0])
	d.pendingRegisterTarget = candidate
	d.remoteCandidates = nodes[1:]
	return candidate, true
}

// advanceRemoteProbeAfterNotReusable accepts only the not reusable response
// from the active remote probe and advances the fallback chain one candidate at
// a time.
func (d *dispatcherConnState) advanceRemoteProbeAfterNotReusable(from node.ID) (node.ID, bool) {
	d.Lock()
	defer d.Unlock()
	if d.removed || d.pendingRegisterTarget != from {
		return "", false
	}
	if len(d.remoteCandidates) == 0 {
		d.pendingRegisterTarget = ""
		return "", true
	}
	candidate := node.ID(d.remoteCandidates[0])
	d.remoteCandidates = d.remoteCandidates[1:]
	d.pendingRegisterTarget = candidate
	return candidate, true
}

// dispatcherSession owns side-effect orchestration for one dispatcher. It asks
// dispatcherConnState to apply atomic state transitions, then turns the results
// into register/remove/reset requests.
type dispatcherSession struct {
	// target provides immutable dispatcher metadata used by control-plane requests.
	target dispatcher.DispatcherService
	// localServerID identifies the collector side when talking to EventService.
	localServerID node.ID
	// sendMessage delivers control-plane requests generated by this session.
	sendMessage func(*messaging.TargetMessage)
	// nextResetEpoch advances the dispatcher's epoch and returns the new value.
	nextResetEpoch func(resetTs uint64) uint64
	// readyCallback is only set during the initial local registration path.
	readyCallback func()

	// connState tracks which EventService this session is currently talking to.
	connState dispatcherConnState
}

func newDispatcherSession(
	target dispatcher.DispatcherService,
	localServerID node.ID,
	sendMessage func(*messaging.TargetMessage),
	nextResetEpoch func(resetTs uint64) uint64,
	readyCallback func(),
) *dispatcherSession {
	return &dispatcherSession{
		target:         target,
		localServerID:  localServerID,
		sendMessage:    sendMessage,
		nextResetEpoch: nextResetEpoch,
		readyCallback:  readyCallback,
	}
}

// Control-plane request entry points.

func (s *dispatcherSession) startLocalRegistration() {
	s.connState.beginRegisterToLocal()
	s.sendRegisterRequest(s.localServerID)
}

func (s *dispatcherSession) retryCurrentRegistration() {
	serverID := s.connState.getCurrentEventServiceID()
	if serverID.IsEmpty() {
		log.Panic("current event service should not be empty when retrying registration",
			zap.Stringer("changefeedID", s.target.GetChangefeedID()),
			zap.Stringer("dispatcher", s.target.GetId()))
	}
	s.beginRegister(serverID)
	s.sendRegisterRequest(serverID)
}

// sendRegisterRequest sends a REGISTER request to the target event service.
// For local registration, OnlyReuse=false means the target may initialize a new
// source if needed. For remote probing, OnlyReuse=true means the target should
// only accept the dispatcher if it can reuse an existing source.
func (s *dispatcherSession) sendRegisterRequest(serverID node.ID) {
	onlyReuse := serverID != s.localServerID
	msg := messaging.NewSingleTargetMessage(
		serverID,
		messaging.EventServiceTopic,
		s.newDispatcherRegisterRequest(s.localServerID.String(), onlyReuse),
	)
	s.sendMessage(msg)
}

// beginRegister records the in-flight registrations before the register request
// is sent. Local and remote registration are tracked independently because a
// dispatcher may wait for local ready and a remote reusable candidate at the
// same time.
func (s *dispatcherSession) beginRegister(serverID node.ID) {
	if serverID == s.localServerID {
		s.connState.beginRegisterToLocal()
		return
	}
	s.connState.beginRegisterToRemote(serverID)
}

// commitReady commits an accepted READY by sending RESET to the chosen
// event service. In the current protocol, READY only means the registration is
// accepted; RESET is the command that starts or restarts event delivery from
// the collector checkpoint.
func (s *dispatcherSession) commitReady(serverID node.ID) {
	s.doReset(serverID, s.target.GetCheckpointTs())
}

// reset is used to reset the dispatcher to the specified commitTs,
// it will remove the dispatcher from the dynamic stream and add it back.
func (s *dispatcherSession) reset(serverID node.ID) {
	s.doReset(serverID, s.target.GetCheckpointTs())
}

// doReset sends RESET to the target event service and advances the
// collector epoch for the new stream.
func (s *dispatcherSession) doReset(serverID node.ID, resetTs uint64) {
	epoch := s.nextResetEpoch(resetTs)
	resetRequest := s.newDispatcherResetRequest(
		s.localServerID.String(),
		resetTs,
		epoch,
	)
	msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, resetRequest)
	s.sendMessage(msg)
	log.Info("send reset dispatcher request to event service",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcher", s.target.GetId()),
		zap.Stringer("eventServiceID", serverID),
		zap.Uint64("epoch", epoch),
		zap.Uint64("resetTs", resetTs))
}

// remove marks the session as terminal, snapshots all cleanup targets, then
// sends remove requests outside the state lock.
func (s *dispatcherSession) remove() {
	cleanupTargets, alreadyRemoved := s.connState.beginRemove(s.localServerID)
	if alreadyRemoved {
		return
	}
	for _, target := range cleanupTargets {
		s.removeFrom(target)
	}
}

// removeFrom sends REMOVE to the target event service. The request may
// represent either terminal removal of the dispatcher session or best-effort
// cleanup of a stale registration on another event service.
func (s *dispatcherSession) removeFrom(serverID node.ID) {
	log.Info("send remove dispatcher request to event service",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcher", s.target.GetId()),
		zap.Stringer("eventServiceID", serverID))
	msg := messaging.NewSingleTargetMessage(
		serverID,
		messaging.EventServiceTopic,
		s.newDispatcherRemoveRequest(s.localServerID.String()),
	)
	s.sendMessage(msg)
}

// Signal-event orchestration.

// handleSignalEvent is the control-plane event entrypoint.
//
// Signal handling follows one rule throughout this file:
//  1. connState decides whether the incoming signal is relevant and returns the
//     resulting control-plane decision;
//  2. session applies the side effects for that decision in a fixed order.
//
// Keeping "state transition" and "side effects" separate makes it easier to
// audit whether a signal path forgot cleanup, retry, or commit work.
func (s *dispatcherSession) handleSignalEvent(event dispatcher.DispatcherEvent) {
	if s.connState.isRemoved() {
		return
	}
	from := *event.From
	switch event.GetType() {
	case commonEvent.TypeReadyEvent:
		s.handleReadyEvent(from)
	case commonEvent.TypeNotReusableEvent:
		s.handleNotReusableEvent(from)
	default:
		log.Panic("should not happen: unknown signal event type", zap.Int("eventType", event.GetType()))
	}
}

// handleReadyEvent always applies ready in two steps:
// 1. clean up stale registrations returned by connState;
// 2. commit the accepted target, if this ready won the race.
func (s *dispatcherSession) handleReadyEvent(from node.ID) {
	accepted := s.connState.acceptReady(from, s.localServerID)
	s.cleanupRegistrations(accepted.cleanupTargets)
	s.commitAcceptedReady(accepted.commitTarget)
}

func (s *dispatcherSession) cleanupRegistrations(targets []node.ID) {
	for _, target := range targets {
		s.removeFrom(target)
	}
}

func (s *dispatcherSession) commitAcceptedReady(serverID node.ID) {
	if serverID.IsEmpty() {
		return
	}
	if serverID == s.localServerID {
		s.handleAcceptedLocalReady()
		return
	}
	s.handleAcceptedRemoteReady(serverID)
}

func (s *dispatcherSession) handleAcceptedLocalReady() {
	if s.readyCallback != nil {
		// This path is used during the initial add flow before the dispatcher is
		// committed. Local is still authoritative, so any speculative remote
		// registration must already be canceled above.
		s.readyCallback()
		return
	}
	log.Info("received ready signal from local event service, prepare to reset the dispatcher",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcher", s.target.GetId()))
	s.commitReady(s.localServerID)
}

func (s *dispatcherSession) handleAcceptedRemoteReady(serverID node.ID) {
	if s.readyCallback != nil {
		log.Panic("ready callback should be nil when accepting remote ready",
			zap.Stringer("changefeedID", s.target.GetChangefeedID()),
			zap.Stringer("dispatcher", s.target.GetId()),
			zap.Stringer("eventServiceID", serverID))
	}
	log.Info("received ready signal from remote event service, prepare to reset the dispatcher",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcher", s.target.GetId()),
		zap.Stringer("eventServiceID", serverID))
	s.commitReady(serverID)
}

// handleNotReusableEvent only advances the active remote probe. Any stale not
// reusable signal is ignored by connState before side effects are considered.
func (s *dispatcherSession) handleNotReusableEvent(from node.ID) {
	if from == s.localServerID {
		log.Panic("should not happen: local event service should not send not reusable event")
	}
	nextCandidate, accepted := s.connState.advanceRemoteProbeAfterNotReusable(from)
	if !accepted {
		return
	}
	// Remote candidates are probed one by one. Once a candidate returns not
	// reusable, move to the next candidate if any; otherwise stop remote probing
	// and leave only the local pending path alive.
	if nextCandidate.IsEmpty() {
		return
	}
	s.sendRegisterRequest(nextCandidate)
}

// Dispatcher request builders.
func (s *dispatcherSession) newDispatcherRegisterRequest(serverID string, onlyReuse bool) *messaging.DispatcherRequest {
	startTs := s.target.GetStartTs()
	syncPointInterval := s.target.GetSyncPointInterval()
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: s.target.GetChangefeedID().ToPB(),
			DispatcherId: s.target.GetId().ToPB(),
			TableSpan:    s.target.GetTableSpan(),
			StartTs:      startTs,
			// ServerId is the id of the request sender.
			ServerId:             serverID,
			ActionType:           eventpb.ActionType_ACTION_TYPE_REGISTER,
			FilterConfig:         s.target.GetFilterConfig(),
			EnableSyncPoint:      s.target.EnableSyncPoint(),
			SyncPointInterval:    uint64(syncPointInterval.Seconds()),
			SyncPointTs:          syncpoint.CalculateStartSyncPointTs(startTs, syncPointInterval, s.target.GetSkipSyncpointAtStartTs()),
			OnlyReuse:            onlyReuse,
			BdrMode:              s.target.GetBDRMode(),
			Mode:                 s.target.GetMode(),
			Epoch:                0,
			Timezone:             s.target.GetTimezone(),
			Integrity:            s.target.GetIntegrityConfig(),
			OutputRawChangeEvent: s.target.IsOutputRawChangeEvent(),
			TxnAtomicity:         string(s.target.GetTxnAtomicity()),
		},
	}
}

func (s *dispatcherSession) newDispatcherResetRequest(serverID string, resetTs uint64, epoch uint64) *messaging.DispatcherRequest {
	syncPointInterval := s.target.GetSyncPointInterval()

	// after reset during normal run time, we can filter reduduant syncpoint at event collector side
	// so we just take care of the case that resetTs is same as startTs
	skipSyncpointSameAsResetTs := false
	if resetTs == s.target.GetStartTs() {
		skipSyncpointSameAsResetTs = s.target.GetSkipSyncpointAtStartTs()
	}
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: s.target.GetChangefeedID().ToPB(),
			DispatcherId: s.target.GetId().ToPB(),
			TableSpan:    s.target.GetTableSpan(),
			StartTs:      resetTs,
			// ServerId is the id of the request sender.
			ServerId:          serverID,
			ActionType:        eventpb.ActionType_ACTION_TYPE_RESET,
			FilterConfig:      s.target.GetFilterConfig(),
			EnableSyncPoint:   s.target.EnableSyncPoint(),
			SyncPointInterval: uint64(syncPointInterval.Seconds()),
			SyncPointTs:       syncpoint.CalculateStartSyncPointTs(resetTs, syncPointInterval, skipSyncpointSameAsResetTs),
			// OnlyReuse:         false,
			BdrMode:              s.target.GetBDRMode(),
			Mode:                 s.target.GetMode(),
			Epoch:                epoch,
			Timezone:             s.target.GetTimezone(),
			Integrity:            s.target.GetIntegrityConfig(),
			OutputRawChangeEvent: s.target.IsOutputRawChangeEvent(),
		},
	}
}

func (s *dispatcherSession) newDispatcherRemoveRequest(serverID string) *messaging.DispatcherRequest {
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: s.target.GetChangefeedID().ToPB(),
			DispatcherId: s.target.GetId().ToPB(),
			TableSpan:    s.target.GetTableSpan(),
			// ServerId is the id of the request sender.
			ServerId:   serverID,
			ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
			Mode:       s.target.GetMode(),
		},
	}
}

// startRemoteProbing begins probing reusable remote event services one by one.
func (s *dispatcherSession) startRemoteProbing(nodes []string) {
	candidate, ok := s.connState.beginRemoteProbing(nodes)
	if !ok {
		return
	}
	log.Info("set remote candidates",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcherID", s.target.GetId()),
		zap.Int64("tableID", s.target.GetTableSpan().TableID),
		zap.Strings("nodes", nodes))
	s.sendRegisterRequest(candidate)
}

// Read-only session queries.
func (s *dispatcherSession) getEventServiceID() node.ID {
	return s.connState.getCurrentEventServiceID()
}

func (s *dispatcherSession) isReceivingDataEvent() bool {
	return s.connState.isReceivingDataEvent()
}
