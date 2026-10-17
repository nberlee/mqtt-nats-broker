// Copyright 2026 The mqtt-nats-broker Authors
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

package mqtt

// Test helpers for the shared-group lifecycle records (F-105, the shared-group lifecycle
// transfer). Untagged, so every tag set compiles against them.

// testSharedMembership builds the membership value a session record carries for one shared
// key at incarnation 1 of each group it joins — the incarnation a fresh test server's first
// join mounts — with fresh acquisition nonces. A multi-level key gets its level-up companion.
//
// A membership built here names acquisitions NO LEDGER LISTS: it is a record shape, not a
// join. A test that needs the release of these acquisitions to move a group's count acquires
// through the production path instead (testAcquireShared, shared_failed_filter_release_test.go).
func testSharedMembership(key string, qos byte) mqttSharedMembership {
	return testSharedMembershipAt(key, qos, 1)
}

// testSharedMembershipAt is testSharedMembership at an explicit incarnation.
func testSharedMembershipAt(key string, qos byte, inc uint64) mqttSharedMembership {
	m := mqttSharedMembership{QoS: qos, Main: mqttSharedAcquisition{Inc: inc, Acq: mqttNewInflightNonce()}}
	if _, subject, ok := mqttParseSharedKey(key); ok && mqttNeedSubForLevelUp(subject) {
		m.Up = &mqttSharedAcquisition{Inc: inc, Acq: mqttNewInflightNonce()}
	}
	return m
}
