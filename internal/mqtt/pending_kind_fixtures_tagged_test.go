// Copyright 2026 The mqtt-nats-bridge Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build mqttport_fulltests

package mqtt

import "sync/atomic"

// testPendingEv is planted evidence for lineage lin in generation gen.
func testPendingEv(gen, lin string) mqttPendingEvidence {
	return mqttPendingEvidence{gen: gen, lin: lin}
}

// testDemoteInPlace demotes a fixture's entry exactly as mqttNewDemoted would, keeping the pointer
// the fixture holds (production replaces it; a fixture comparing pointers needs the same one).
func testDemoteInPlace(p *mqttPending) {
	*p = *mqttNewDemoted(p)
}

// mqttSetTestSeam installs h at one of the pending-entry closure's seams and returns its restore.
func mqttSetTestSeam(v *atomic.Pointer[func()], h func()) func() {
	prev := v.Load()
	if h == nil {
		v.Store(nil)
	} else {
		v.Store(&h)
	}
	return mqttTestRestoreOnce(func() { v.Store(prev) })
}
