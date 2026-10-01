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

// T15 — THE ORDINARY-SOURCE SINKS REFUSE A RETAINED REGISTER fed to them directly (F-105 7c): each
// answers its fail-closed value — Unknown, no completion, no settlement, no restoration match, not
// cached — and counts the refusal. The behavioural tests assert the count stays zero on every
// assembled route; this pins that each sink would refuse were the routing ever wrong.
func TestF105RetainedRegisterIsRefusedAtEveryOrdinarySink(t *testing.T) {
	e := testNewInflightEnv(t, "ret-sinks")
	rec := &mqttInflightRecord{PI: 3, QoS: 1, State: mqttInflightPublish, Nonce: mqttNewInflightNonce(),
		Payload: mqttNewInflightNonce(), Digest: mqttMaterialDigest([]byte("x"))}
	rec.Lin = rec.Nonce
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, 3)
	refused := func(t *testing.T, name string, f func() bool) {
		t.Helper()
		before := mqttRetainedAtOrdinarySink.Load()
		if !f() {
			t.Fatalf("%s did not answer its fail-closed value for a retained register", name)
		}
		if mqttRetainedAtOrdinarySink.Load() != before+1 {
			t.Fatalf("%s refused a retained register without counting it", name)
		}
	}
	refused(t, "inflightSourceState", func() bool {
		return e.asm.inflightSourceState(rec) == mqttInflightSourceUnknown
	})
	refused(t, "completeSourceFor", func() bool {
		return e.sess.completeSourceFor(subject, 7, rec, true, nil) == mqttSourceCompletion{}
	})
	refused(t, "settleTerminalSource", func() bool {
		return !e.sess.settleTerminalSource(subject, 7, rec)
	})
	refused(t, "mqttRecoveryPendingMatches", func() bool {
		return !mqttRecoveryPendingMatches(&mqttPending{kind: mqttPendingRetained, regNonce: rec.Nonce}, rec, false)
	})
	refused(t, "the source cache", func() bool {
		sc := &mqttInflightSourceCache{as: e.asm, consumers: map[string]*ConsumerInfo{}, known: map[string]bool{}}
		sc.resolve([]*mqttInflightRecord{rec})
		return len(sc.known) == 0
	})
}
