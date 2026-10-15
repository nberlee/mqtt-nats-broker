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

// The same sinks refuse a record SYNTHESIZED from empty coordinates — no payload, no digest, so not
// the retained shape — and count it. finishTerminalSource built exactly such a record from a
// retained slot's (dur, sseq), and a shape-only guard let it through to a consumer lookup on an
// empty name, which a server with permissions never answers. A zero sequence alone is refused
// too: it would read as Complete against any ack floor. holdTerminalSourceLocked takes the
// coordinate form and installs no owner.
func TestF105SourcelessCoordinatesAreRefusedAtEveryOrdinarySink(t *testing.T) {
	e := testNewInflightEnv(t, "sourceless-sinks")
	const pi = 3
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	for _, tc := range []struct {
		name string
		dur  string
		seq  uint64
	}{
		{"no durable, no sequence", _EMPTY_, 0},
		{"no durable", _EMPTY_, 7},
		{"no sequence", "sourceless_dur", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &mqttInflightRecord{PI: pi, QoS: 1, State: mqttInflightPublish, Nonce: mqttNewInflightNonce(),
				Dur: tc.dur, Seq: tc.seq}
			rec.Lin = rec.Nonce
			// Errorf, not Fatalf: each sink is judged on its own, so one sink's failure does not
			// hide another's.
			refused := func(t *testing.T, name string, f func() bool) {
				t.Helper()
				before := mqttRetainedAtOrdinarySink.Load()
				if !f() {
					t.Errorf("%s did not answer its fail-closed value for (%q, %d)", name, tc.dur, tc.seq)
				}
				if got := mqttRetainedAtOrdinarySink.Load() - before; got != 1 {
					t.Errorf("%s counted %d refusals for (%q, %d), want exactly 1", name, got, tc.dur, tc.seq)
				}
			}
			refused(t, "inflightSourceState", func() bool {
				return e.asm.inflightSourceState(rec) == mqttInflightSourceUnknown
			})
			refused(t, "the source cache", func() bool {
				sc := &mqttInflightSourceCache{as: e.asm, consumers: map[string]*ConsumerInfo{}, known: map[string]bool{},
					resolved: map[string]bool{}, states: map[string]mqttInflightSourceState{},
					subjects: map[string]string{}}
				sc.resolve([]*mqttInflightRecord{rec})
				return len(sc.known) == 0 && len(sc.states) == 0
			})
			refused(t, "completeSourceFor", func() bool {
				return e.sess.completeSourceFor(subject, 11, rec, true, nil) == mqttSourceCompletion{}
			})
			refused(t, "settleTerminalSource", func() bool {
				return !e.sess.settleTerminalSource(subject, 11, rec)
			})
			refused(t, "mqttRecoveryPendingMatches", func() bool {
				return !mqttRecoveryPendingMatches(nil, rec, false)
			})
			refused(t, "computeDrainRegisterStateLocked", func() bool {
				// A drain claiming exactly these coordinates on this identifier: everything the
				// recomputation needs to call the register OWNED, but for the refusal.
				addTestDrain(e.sess, "sourceless-sid", tc.dur, "sourceless/f", 1,
					map[uint64]*drainSeq{tc.seq: {claimed: true, pi: pi}})
				defer func() {
					e.sess.mu.Lock()
					delete(e.sess.drains, "sourceless-sid")
					delete(e.sess.drainByDur, tc.dur)
					e.sess.mu.Unlock()
				}()
				e.sess.mu.Lock()
				defer e.sess.mu.Unlock()
				owned, ambiguous, obs := e.sess.computeDrainRegisterStateLocked(map[string][]mqttRecoveredInflightReg{
					"sourceless-key": {{rec: rec, seq: 11}},
				})
				return len(owned) == 0 && len(ambiguous) == 0 && len(obs) == 0
			})
			refused(t, "holdTerminalSourceLocked", func() bool {
				ref := mqttSlotRefFromRecord(subject, 11, rec)
				if ref == nil || !ref.slotOK() {
					t.Fatalf("the fixture's slot reference is not otherwise eligible: %+v", ref)
				}
				e.sess.mu.Lock()
				defer e.sess.mu.Unlock()
				w := e.sess.holdTerminalSourceLocked(*ref)
				for k := range e.sess.terminalSources {
					if k.subject == subject {
						return false
					}
				}
				for k := range e.sess.terminalRetries {
					if k.source.subject == subject {
						return false
					}
				}
				return w == nil
			})
		})
	}
}
