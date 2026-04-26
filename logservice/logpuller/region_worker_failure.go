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

// takeFailureSnapshot is only called after runConnectedLoops has returned.
// At that point the old session loops have stopped, so no request can still
// move from requestCache into activeRegions. We can therefore drain
// activeRegions as startedRegions, then take the remaining unsent requests
// from requestCache as pendingRegions, without classifying one request into
// both sets.
func (s *regionRequestWorkerSession) takeFailureSnapshot() workerSessionFailureSnapshot {
	return workerSessionFailureSnapshot{
		startedRegions: s.activeRegions.takeAll(),
		pendingRegions: s.requestCache.takeUnsentRegions(),
	}
}
