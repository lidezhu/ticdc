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

package eventservice

import (
	"sync"
)

// dispatcherRequestInbox decouples MessageCenter dispatching from dispatcher
// lifecycle processing. Submit only holds the mutex while appending, while Pop
// preserves the global request order expected by eventService.Run.
type dispatcherRequestInbox struct {
	mu     sync.Mutex
	queue  []DispatcherInfo
	head   int
	notify chan struct{}
}

func newDispatcherRequestInbox() *dispatcherRequestInbox {
	return &dispatcherRequestInbox{notify: make(chan struct{}, 1)}
}

func (i *dispatcherRequestInbox) Submit(infos []DispatcherInfo) {
	if len(infos) == 0 {
		return
	}

	i.mu.Lock()
	i.queue = append(i.queue, infos...)
	i.mu.Unlock()

	select {
	case i.notify <- struct{}{}:
	default:
	}
}

func (i *dispatcherRequestInbox) Pop() (DispatcherInfo, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.head == len(i.queue) {
		return nil, false
	}

	info := i.queue[i.head]
	i.queue[i.head] = nil
	i.head++
	if i.head == len(i.queue) {
		if cap(i.queue) > 4096 {
			i.queue = nil
		} else {
			i.queue = i.queue[:0]
		}
		i.head = 0
	} else {
		// Keep Run selectable so heartbeat and cleanup work remain fair between
		// lifecycle requests.
		select {
		case i.notify <- struct{}{}:
		default:
		}
		// Avoid retaining an arbitrarily large consumed prefix after a burst.
		if i.head >= 1024 && i.head*2 >= len(i.queue) {
			copy(i.queue, i.queue[i.head:])
			i.queue = i.queue[:len(i.queue)-i.head]
			i.head = 0
		}
	}
	return info, true
}

func (i *dispatcherRequestInbox) Len() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.queue) - i.head
}
