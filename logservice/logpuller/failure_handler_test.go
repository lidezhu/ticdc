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
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestNormalizeWorkerSessionFailureForStoppedSubscription(t *testing.T) {
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))
	region.subscribedSpan.stopped.Store(true)

	failure := normalizeWorkerSessionFailure(region, workerSessionFailure{
		kind:   regionFailureKindSendRequestToStore,
		source: regionFailureSourceWorkerSession,
		cause:  errors.New("store down"),
	})

	require.Equal(t, regionFailureKindSubscriptionStopped, failure.kind)
	require.Equal(t, regionFailureScopeSubscription, failure.scope)
	require.Equal(t, regionFailureSourceSubscriptionStop, failure.source)
}

func TestNormalizeWorkerSessionFailureForActiveSubscription(t *testing.T) {
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))

	failure := normalizeWorkerSessionFailure(region, workerSessionFailure{
		kind:   regionFailureKindSendRequestToStore,
		source: regionFailureSourceWorkerSession,
		cause:  errors.New("store down"),
	})

	require.Equal(t, regionFailureKindSendRequestToStore, failure.kind)
	require.Equal(t, regionFailureScopeStoreSession, failure.scope)
	require.Equal(t, regionFailureSourceWorkerSession, failure.source)
	require.ErrorContains(t, failure.err, "store down")
}
