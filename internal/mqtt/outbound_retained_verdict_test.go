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

import (
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

// TestF105RetainedReplayVerdict (T24, acl-deny): the carrying connection's judgement of a retained
// PUBLISH before its output is acquired. A subscribe deny over the copy's topic refuses it as
// acl-deny, AHEAD of the size check; a frame over the Maximum Packet Size refuses it as
// packet-too-large; an unusable snapshot decides nothing. What follows a refusal is the shared
// disposition (withdraw or abandon), exercised end to end by the packet-too-large tests.
func TestF105RetainedReplayVerdict(t *testing.T) {
	e := testNewInflightEnv(t, "ret-verdict")
	e.s.dialClient = func(string, string, string) (*nats.Conn, error) { return nil, nil }
	setServiceAccount(e.s, "RV", true)
	setPerms := func(valid bool, deny ...string) {
		perms, err := mqttCompilePermissions(&SubjectPermission{Allow: []string{">"}, Deny: deny}, true)
		if err != nil {
			t.Fatal(err)
		}
		e.c.mu.Lock()
		e.c.origin, e.c.nrc = mqttOriginClient, e.s.nrc
		e.c.authzValid, e.c.authzSvcAccount, e.c.mperms = valid, "RV", perms
		e.c.mu.Unlock()
	}
	e.c.mqtt.proto = mqttProtoLevel5
	m := &mqttRetainedManifest{Topic: "rv/t"}
	f := mqttRetainedFrame{pi: 1, qos: 1, mat: &mqttRetainedMaterial{Topic: "rv/t", QoS: 1, Data: []byte(strings.Repeat("x", 100))}}
	for _, tc := range []struct {
		name    string
		valid   bool
		deny    []string
		mps     uint32
		verdict mqttRetainedReplayVerdict
		reason  string
	}{
		{name: "allowed and fits", valid: true, verdict: mqttRetainedReplayServable},
		{name: "denied", valid: true, deny: []string{"rv.t"}, verdict: mqttRetainedReplayRefused, reason: mqttAbandonReasonACLDeny},
		{name: "too large", valid: true, mps: 64, verdict: mqttRetainedReplayRefused, reason: mqttAbandonReasonPacketTooLarge},
		{name: "denied and too large: the deny decides", valid: true, deny: []string{"rv.>"}, mps: 64,
			verdict: mqttRetainedReplayRefused, reason: mqttAbandonReasonACLDeny},
		{name: "unusable snapshot", valid: false, deny: []string{"rv.t"}, verdict: mqttRetainedReplayIndeterminate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setPerms(tc.valid, tc.deny...)
			e.c.mqtt.maxPacketSize = tc.mps
			v, reason := e.c.retainedReplayVerdict(f, m)
			if v != tc.verdict || reason != tc.reason {
				t.Fatalf("verdict %v reason %q, want %v %q", v, reason, tc.verdict, tc.reason)
			}
		})
	}
}
