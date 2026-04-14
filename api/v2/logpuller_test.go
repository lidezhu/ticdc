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

package v2

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/stretchr/testify/require"
)

type mockObservabilitySubscriptionClient struct {
	snapshot logpuller.ObservabilitySnapshot
}

func (m *mockObservabilitySubscriptionClient) Name() string {
	return "mock-observability-subscription-client"
}
func (m *mockObservabilitySubscriptionClient) Run(context.Context) error   { return nil }
func (m *mockObservabilitySubscriptionClient) Close(context.Context) error { return nil }
func (m *mockObservabilitySubscriptionClient) AllocSubscriptionID() logpuller.SubscriptionID {
	return 1
}
func (m *mockObservabilitySubscriptionClient) Subscribe(
	logpuller.SubscriptionID,
	heartbeatpb.TableSpan,
	uint64,
	func([]common.RawKVEntry, func()) bool,
	func(uint64),
	int64,
	bool,
) {
}
func (m *mockObservabilitySubscriptionClient) Unsubscribe(logpuller.SubscriptionID) {}
func (m *mockObservabilitySubscriptionClient) GetObservabilitySnapshot(sampleLimit int) logpuller.ObservabilitySnapshot {
	return m.snapshot
}

func TestLogPullerObservability(t *testing.T) {
	gin.SetMode(gin.TestMode)

	original, ok := appcontext.LookupService[logpuller.SubscriptionClient](appcontext.SubscriptionClient)
	if ok {
		defer appcontext.SetService(appcontext.SubscriptionClient, original)
	} else {
		defer appcontext.DeleteService(appcontext.SubscriptionClient)
	}

	appcontext.SetService(appcontext.SubscriptionClient, &mockObservabilitySubscriptionClient{
		snapshot: logpuller.ObservabilitySnapshot{
			Runtime: logpuller.RuntimeObservability{
				TrackedRegionCount: 1,
				SlowRegionCount:    1,
			},
		},
	})

	api := OpenAPIV2{}
	ctx, recorder := newFailpointContext(http.MethodGet, "/api/v2/debug/logpuller?sample_limit=3", "")
	api.LogPullerObservability(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)

	var snapshot logpuller.ObservabilitySnapshot
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &snapshot))
	require.Equal(t, 1, snapshot.Runtime.TrackedRegionCount)
	require.Equal(t, 1, snapshot.Runtime.SlowRegionCount)
}
