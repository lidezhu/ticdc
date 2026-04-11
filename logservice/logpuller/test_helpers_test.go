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

	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus"
)

type noopDynamicStream struct{}

func (*noopDynamicStream) Start() {}

func (*noopDynamicStream) Close() {}

func (*noopDynamicStream) Push(SubscriptionID, regionEvent) {}

func (*noopDynamicStream) Wake(SubscriptionID) {}

func (*noopDynamicStream) Feedback() <-chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan] {
	return nil
}

func (*noopDynamicStream) AddPath(SubscriptionID, *subscribedSpan, ...dynstream.AreaSettings) error {
	return nil
}

func (*noopDynamicStream) RemovePath(SubscriptionID) error {
	return nil
}

func (*noopDynamicStream) Release(SubscriptionID) {}

func (*noopDynamicStream) SetAreaSettings(int, dynstream.AreaSettings) {}

func (*noopDynamicStream) GetMetrics() dynstream.Metrics[int, SubscriptionID] {
	return dynstream.Metrics[int, SubscriptionID]{}
}

func newSubscriptionClientForTest() *subscriptionClient {
	ctx, cancel := context.WithCancel(context.Background())
	client := &subscriptionClient{
		ctx:    ctx,
		cancel: cancel,
		config: &SubscriptionClientConfig{RegionRequestWorkerPerStore: 1},
	}
	client.infra.pdClock = pdutil.NewClock4Test()
	client.infra.metrics.batchResolvedSize = prometheus.ObserverFunc(func(float64) {})
	client.runtime = newRegionRuntimeTracker()
	client.events = newEventStreamController(ctx, &noopDynamicStream{})
	client.pipeline.requestRouter = newRegionRequestRouter(client)
	client.pipeline.scheduler = newRegionScheduler(nil, client.runtime, client.pipeline.requestRouter)
	client.subscriptions.manager = newSpanManager(ctx, client.infra.pdClock, client.events, client.runtime)
	client.subscriptions.supervisor = newSpanSupervisor(client.infra.pdClock, nil, client.subscriptions.manager)
	client.subscriptions.manager.setSupervisor(client.subscriptions.supervisor)
	client.subscriptions.manager.setPipeline(client.pipeline.scheduler, client.pipeline.requestRouter)
	client.pipeline.errorHandler = newRegionErrorHandler(
		client.runtime,
		client.pipeline.scheduler,
		client.subscriptions.manager,
		nil,
	)
	client.pipeline.requestRouter.client = client
	return client
}
