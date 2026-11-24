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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventcollector

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

const (
	dispatcherRequestBatchSize = 256
	dispatcherRequestBatchWait = time.Millisecond
	congestedRetryInterval     = time.Second
	defaultRetryInterval       = 10 * time.Millisecond
)

type dispatcherTargetSendState struct {
	queue    []DispatcherMessage
	ready    bool
	retrying bool
	retryAt  time.Time
}

// sendDispatcherRequests preserves FIFO order for each target. A congested
// target is delayed independently, so it cannot stall lifecycle requests to
// the other EventService nodes.
func (c *EventCollector) sendDispatcherRequests(ctx context.Context) error {
	states := make(map[node.ID]*dispatcherTargetSendState)
	readyTargets := make([]node.ID, 0)

	schedule := func(target node.ID, state *dispatcherTargetSendState, now time.Time) {
		if len(state.queue) == 0 || state.ready || !state.retryAt.IsZero() {
			return
		}
		if state.queue[0].Message.Type == messaging.TypeDispatcherRequest {
			state.retryAt = now.Add(dispatcherRequestBatchWait)
			return
		}
		state.ready = true
		readyTargets = append(readyTargets, target)
	}

	enqueue := func(req DispatcherMessage, now time.Time) {
		target := req.Message.To
		state, ok := states[target]
		if !ok {
			state = &dispatcherTargetSendState{}
			states[target] = state
		}
		wasEmpty := len(state.queue) == 0
		state.queue = append(state.queue, req)
		if wasEmpty {
			schedule(target, state, now)
			return
		}
		if !state.ready && !state.retrying && dispatcherBatchPayloadCount(state.queue) >= dispatcherRequestBatchSize {
			state.retryAt = time.Time{}
			state.ready = true
			readyTargets = append(readyTargets, target)
		}
	}

	for {
		// Bound each drain pass so continuous producers cannot starve sends.
		for range 1024 {
			select {
			case req := <-c.dispatcherMessageChan.Out():
				enqueue(req, time.Now())
			default:
				goto drained
			}
		}
	drained:

		now := time.Now()
		for target, state := range states {
			if !state.ready && !state.retryAt.IsZero() && !now.Before(state.retryAt) {
				state.retryAt = time.Time{}
				state.retrying = false
				state.ready = true
				readyTargets = append(readyTargets, target)
			}
		}

		if len(readyTargets) != 0 {
			target := readyTargets[0]
			readyTargets[0] = ""
			readyTargets = readyTargets[1:]
			state := states[target]
			state.ready = false

			req, consumed := batchDispatcherMessages(state.queue)
			c.filterDispatcherRequests(&req)
			if len(req.Message.Message) == 0 {
				state.queue = removeDispatcherMessages(state.queue, consumed)
				if len(state.queue) == 0 {
					delete(states, target)
				} else {
					schedule(target, state, now)
				}
				continue
			}

			err := c.mc.SendCommand(req.Message)
			if err == nil {
				state.queue = removeDispatcherMessages(state.queue, consumed)
				if len(state.queue) == 0 {
					delete(states, target)
				} else {
					schedule(target, state, now)
				}
				continue
			}

			retryInterval := defaultRetryInterval
			if appErr, ok := err.(errors.AppError); ok && appErr.Type == errors.ErrorTypeMessageCongested {
				retryInterval = congestedRetryInterval
			}
			if !req.decrAndCheckRetry() {
				log.Warn("dispatcher request retry limit exceeded",
					zap.Stringer("targetNodeID", target),
					zap.String("messageType", req.Message.Type.String()),
					zap.Int("payloadCount", len(req.Message.Message)),
					zap.Error(err))
				state.queue = removeDispatcherMessages(state.queue, consumed)
				if len(state.queue) == 0 {
					delete(states, target)
				} else {
					schedule(target, state, now)
				}
				continue
			}

			log.Warn("dispatcher request send delayed",
				zap.Stringer("targetNodeID", target),
				zap.String("messageType", req.Message.Type.String()),
				zap.Int("payloadCount", len(req.Message.Message)),
				zap.Duration("retryInterval", retryInterval),
				zap.Error(err))
			state.queue = replaceDispatcherMessages(state.queue, consumed, req)
			state.retrying = true
			state.retryAt = now.Add(retryInterval)
			continue
		}

		var wakeup <-chan time.Time
		var timer *time.Timer
		var earliest time.Time
		for _, state := range states {
			if state.retryAt.IsZero() || (!earliest.IsZero() && !state.retryAt.Before(earliest)) {
				continue
			}
			earliest = state.retryAt
		}
		if !earliest.IsZero() {
			delay := time.Until(earliest)
			if delay < 0 {
				delay = 0
			}
			timer = time.NewTimer(delay)
			wakeup = timer.C
		}

		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return context.Cause(ctx)
		case req := <-c.dispatcherMessageChan.Out():
			if timer != nil {
				timer.Stop()
			}
			enqueue(req, time.Now())
		case <-wakeup:
		}
	}
}

func (c *EventCollector) filterDispatcherRequests(req *DispatcherMessage) {
	if req.Message.Type != messaging.TypeDispatcherRequest {
		return
	}

	requests := req.Message.Message[:0]
	for _, payload := range req.Message.Message {
		request := payload.(*messaging.DispatcherRequest)
		if request.GetActionType() == eventpb.ActionType_ACTION_TYPE_REGISTER {
			stat := c.getDispatcherStatByID(request.GetID())
			if stat == nil || !stat.session.connState.hasRegistration(req.Message.To, c.serverId) {
				continue
			}
		}
		requests = append(requests, payload)
	}
	req.Message.Message = requests
}

func batchDispatcherMessages(queue []DispatcherMessage) (DispatcherMessage, int) {
	first := queue[0]
	cloned := *first.Message
	cloned.Message = append([]messaging.IOTypeT(nil), first.Message.Message...)
	first.Message = &cloned
	if cloned.Type != messaging.TypeDispatcherRequest {
		return first, 1
	}

	consumed := 1
	for consumed < len(queue) && len(cloned.Message) < dispatcherRequestBatchSize {
		next := queue[consumed]
		if !dispatcherMessagesBatchable(first, next) ||
			len(cloned.Message)+len(next.Message.Message) > dispatcherRequestBatchSize {
			break
		}
		cloned.Message = append(cloned.Message, next.Message.Message...)
		consumed++
	}
	return first, consumed
}

func dispatcherMessagesBatchable(first, next DispatcherMessage) bool {
	return next.Message.Type == messaging.TypeDispatcherRequest &&
		first.Message.To == next.Message.To &&
		first.Message.Topic == next.Message.Topic &&
		first.Message.Group == next.Message.Group &&
		first.Droppable == next.Droppable &&
		first.RetryQuota == next.RetryQuota
}

func dispatcherBatchPayloadCount(queue []DispatcherMessage) int {
	if len(queue) == 0 || queue[0].Message.Type != messaging.TypeDispatcherRequest {
		return 0
	}
	count := 0
	for index := range queue {
		if !dispatcherMessagesBatchable(queue[0], queue[index]) {
			break
		}
		count += len(queue[index].Message.Message)
		if count >= dispatcherRequestBatchSize {
			return count
		}
	}
	return count
}

func removeDispatcherMessages(queue []DispatcherMessage, count int) []DispatcherMessage {
	clear(queue[:count])
	return queue[count:]
}

func replaceDispatcherMessages(queue []DispatcherMessage, count int, req DispatcherMessage) []DispatcherMessage {
	queue[0] = req
	if count == 1 {
		return queue
	}
	copy(queue[1:], queue[count:])
	clear(queue[len(queue)-count+1:])
	return queue[:len(queue)-count+1]
}
