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

type txnIdentity struct {
	startTs  uint64
	commitTs uint64
}

type txnSizeSample struct {
	txn       txnIdentity
	rawBytes  int64
	threshold int64
}

func txnSizeSampleFromTxn(txn *TxnEvent) txnSizeSample {
	dml := txn.CurrentDMLEvent
	return txnSizeSample{
		txn: txnIdentity{
			startTs:  dml.GetStartTs(),
			commitTs: dml.GetCommitTs(),
		},
		rawBytes:  txn.rawKVBytes,
		threshold: txn.largeTxnThresholdInBytes,
	}
}

// txnSizeMetricRecorder aggregates transaction fragments across scan tasks so
// each large transaction is observed exactly once. It is used only by the
// serialized scan task of a dispatcher and therefore requires no lock.
// Its zero value is ready to use.
type txnSizeMetricRecorder struct {
	pending *txnSizeSample
}

func (r *txnSizeMetricRecorder) addFragment(sample txnSizeSample) {
	if sample.rawBytes <= sample.threshold {
		return
	}
	r.advanceTo(sample.txn)
	if r.pending == nil {
		r.pending = &sample
		return
	}
	r.pending.rawBytes += sample.rawBytes
}

func (r *txnSizeMetricRecorder) complete(sample txnSizeSample) {
	if r.pending != nil {
		if r.pending.txn == sample.txn {
			sample.rawBytes += r.pending.rawBytes
			r.pending = nil
		} else {
			r.flush()
		}
	}
	r.observe(sample)
}

func (r *txnSizeMetricRecorder) advanceTo(txn txnIdentity) {
	if r.pending == nil || r.pending.txn == txn {
		return
	}
	r.flush()
}

func (r *txnSizeMetricRecorder) flush() {
	if r.pending == nil {
		return
	}
	sample := *r.pending
	r.pending = nil
	r.observe(sample)
}

func (r *txnSizeMetricRecorder) observe(sample txnSizeSample) {
	if sample.rawBytes > sample.threshold {
		updateMetricEventServiceBigTxn(sample.rawBytes)
	}
}
