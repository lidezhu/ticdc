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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventservice

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/metrics"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestTxnSizeMetricRecorderCountsSplitTxnOnce(t *testing.T) {
	beforeHistogramCount, beforeHistogramSum := readBigTxnSizeMetric(t)
	beforeCounter := readBigTxnCountMetric(t)

	recorder := txnSizeMetricRecorder{}
	recorder.addFragment(txnSizeSample{
		txn:       txnIdentity{startTs: 100, commitTs: 200},
		rawBytes:  70,
		threshold: 50,
	})

	histogramCount, histogramSum := readBigTxnSizeMetric(t)
	require.Equal(t, beforeHistogramCount, histogramCount)
	require.Equal(t, beforeHistogramSum, histogramSum)
	require.Equal(t, beforeCounter, readBigTxnCountMetric(t))

	recorder.complete(txnSizeSample{
		txn:       txnIdentity{startTs: 100, commitTs: 200},
		rawBytes:  30,
		threshold: 50,
	})

	histogramCount, histogramSum = readBigTxnSizeMetric(t)
	require.Equal(t, beforeHistogramCount+1, histogramCount)
	require.Equal(t, beforeHistogramSum+100, histogramSum)
	require.Equal(t, beforeCounter+1, readBigTxnCountMetric(t))
}

func TestTxnSizeMetricRecorderAdvancesToNextTxn(t *testing.T) {
	beforeHistogramCount, beforeHistogramSum := readBigTxnSizeMetric(t)
	beforeCounter := readBigTxnCountMetric(t)

	recorder := txnSizeMetricRecorder{}
	recorder.addFragment(txnSizeSample{
		txn:       txnIdentity{startTs: 100, commitTs: 200},
		rawBytes:  70,
		threshold: 50,
	})
	recorder.addFragment(txnSizeSample{
		txn:       txnIdentity{startTs: 101, commitTs: 201},
		rawBytes:  80,
		threshold: 50,
	})

	histogramCount, histogramSum := readBigTxnSizeMetric(t)
	require.Equal(t, beforeHistogramCount+1, histogramCount)
	require.Equal(t, beforeHistogramSum+70, histogramSum)
	require.Equal(t, beforeCounter+1, readBigTxnCountMetric(t))
	require.Equal(t, &txnSizeSample{
		txn:       txnIdentity{startTs: 101, commitTs: 201},
		rawBytes:  80,
		threshold: 50,
	}, recorder.pending)
}

func readBigTxnSizeMetric(t *testing.T) (uint64, float64) {
	t.Helper()

	metric := &dto.Metric{}
	require.NoError(t, metrics.EventServiceBigTxnSize.Write(metric))
	histogram := metric.GetHistogram()
	require.NotNil(t, histogram)
	return histogram.GetSampleCount(), histogram.GetSampleSum()
}

func readBigTxnCountMetric(t *testing.T) float64 {
	t.Helper()

	metric := &dto.Metric{}
	require.NoError(t, metrics.EventServiceBigTxnCount.Write(metric))
	counter := metric.GetCounter()
	require.NotNil(t, counter)
	return counter.GetValue()
}
