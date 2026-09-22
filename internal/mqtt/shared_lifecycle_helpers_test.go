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

package mqtt

import "testing"

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

// testSharedSubs builds a SharedSubs map from (key, qos) pairs at incarnation 1.
func testSharedSubs(kv ...any) map[string]mqttSharedMembership {
	out := make(map[string]mqttSharedMembership, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		key := kv[i].(string)
		var qos byte
		switch v := kv[i+1].(type) {
		case int:
			qos = byte(v)
		case byte:
			qos = v
		}
		out[key] = testSharedMembership(key, qos)
	}
	return out
}

// testSharedHolderKey builds a shared-subscription session key from the production prefix, so
// a rename cannot leave a fixture asserting against a key nothing writes.
func testSharedHolderKey(group, subject string) string {
	return string(mqttSharedSubPrefix) + group + "/" + subject
}

// testPlantSessionRecord stores raw bytes on another session's $MQTT_sess subject, derived from
// a client-ID hash exactly as production does.
func testPlantSessionRecord(t *testing.T, asm *mqttAccountSessionManager, ownerID string, data []byte) {
	t.Helper()
	subject := mqttSessStreamSubjectPrefix + asm.domainTk + getHash(ownerID)
	if _, err := asm.jsa.storeMsg(subject, 0, data); err != nil {
		t.Fatalf("planting the session record of %q: %v", ownerID, err)
	}
}

// testPlantSessionRecordJSON is testPlantSessionRecord for a record that decodes.
func testPlantSessionRecordJSON(t *testing.T, asm *mqttAccountSessionManager, ownerID string, ps *mqttPersistedSession) {
	t.Helper()
	testPlantSessionRecord(t, asm, ownerID, testMustJSON(t, ps))
}

// testSharedConsRecord is a record of the one shape this broker never writes: a shared group
// consumer configuration in Cons. buildPersistedSessionLocked skips every entry whose
// DeliverGroup is non-empty, so this record decodes and is refused at every ingress that reads it.
func testSharedConsRecord(clientID, group, subject string) *mqttPersistedSession {
	_, deliver, durable := mqttSharedNames(group, subject, 1)
	return &mqttPersistedSession{
		ID: clientID,
		Cons: map[string]*ConsumerConfig{
			testSharedHolderKey(group, subject): {
				Durable:        durable,
				DeliverSubject: deliver,
				DeliverGroup:   group,
			},
		},
	}
}
