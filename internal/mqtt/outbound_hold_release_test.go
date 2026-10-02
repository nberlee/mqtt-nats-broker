//go:build mqttport_fulltests

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

import (
	"encoding/json"
	"strings"
	"testing"
)

// F-105 7c-lc: an ending releases its holds in its own batch, and nothing else releases them.

// testRequireHolds asserts, for every hold x names, whether its resource still carries the edge.
// An exchange with no holds proves nothing here, so it is refused as a fixture.
func testRequireHolds(t *testing.T, jsa *mqttJSA, x *mqttOutboundExchange, standing bool) {
	t.Helper()
	if x == nil || len(x.Holds) == 0 {
		t.Fatalf("fixture: the obligation holds nothing, so its release proves nothing: %+v", x)
	}
	for _, h := range x.Holds {
		r, _, err := jsa.loadOutboundResource(x.ID, x.Epoch, x.Gen, h.Inc)
		if err != nil || r == nil {
			t.Fatalf("resource %q of %q does not load: %v", h.Inc, x.Nonce, err)
		}
		has := false
		for _, d := range r.Deps {
			if d == (mqttResourceEdge{Nonce: x.Nonce, Role: h.Role}) {
				has = true
			}
		}
		if has != standing {
			t.Fatalf("obligation %q (phase %d): its %s hold on %q stands=%v, want %v (deps %v)",
				x.Nonce, x.Phase, h.Role, h.Inc, has, standing, r.Deps)
		}
	}
}

// T1 at the operation surface: every ending releases exactly its holds, a non-ending releases
// nothing, and the index names exactly what admission took.
func TestF105EndingReleasesItsHolds(t *testing.T) {
	type step struct {
		to mqttExchangePhase
	}
	for name, tc := range map[string]struct {
		qos   byte
		incs  []string
		steps []mqttExchangePhase
		ended bool
	}{
		"QoS 1 PUBACK":               {1, []string{"inc-src"}, []mqttExchangePhase{mqttExchangeTerminal}, true},
		"QoS 2 PUBREC then PUBCOMP":  {2, []string{"inc-src", "inc-rel"}, []mqttExchangePhase{mqttExchangePubRecObserved, mqttExchangeTerminal}, true},
		"QoS 2 refused PUBREC":       {2, []string{"inc-src", "inc-rel"}, []mqttExchangePhase{mqttExchangeRejected}, true},
		"QoS 2 PUBREC only":          {2, []string{"inc-src", "inc-rel"}, []mqttExchangePhase{mqttExchangePubRecObserved}, false},
		"QoS 2 pending drain":        {2, []string{"inc-src", "inc-rel"}, []mqttExchangePhase{mqttExchangePubRecObserved, mqttExchangeTerminalPendingDrain}, false},
		"QoS 2 pending drain, drain": {2, []string{"inc-src", "inc-rel"}, []mqttExchangePhase{mqttExchangePubRecObserved, mqttExchangeTerminalPendingDrain, mqttExchangeTerminal}, true},
	} {
		t.Run(name, func(t *testing.T) {
			e, done := testF105NewEnv(t, "f105-holds-"+strings.ReplaceAll(strings.ToLower(name), " ", "-"))
			defer done()
			e.bootstrap(t, "txn-1")
			testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
			testF105PlantResource(t, e, "inc-rel", mqttResourcePubRelConsumer)
			nonce, _, _, _ := testF105AdmitQoS(t, e, "txn-adm", 7, tc.qos, tc.incs...)

			x, _ := testF105LoadExchange(t, e, testF105Gen, nonce)
			want := []mqttExchangeHold{{Inc: "inc-src", Role: mqttRoleSourceReplay}}
			if tc.qos == 2 {
				want = append(want, mqttExchangeHold{Inc: "inc-rel", Role: mqttRoleCanonicalPubRel})
			}
			if !mqttHoldsEqual(x.Holds, want) {
				t.Fatalf("admission indexed holds %v, want %v", x.Holds, want)
			}
			testRequireHolds(t, &e.asm.jsa, x, true)

			for i, to := range tc.steps {
				if _, err := e.asm.jsa.opAdvanceExchange(e.id, e.epoch, "txn-step-"+string(rune('a'+i)),
					"op-step", testF105Gen, nonce, to); err != nil {
					t.Fatalf("advancing to %d was refused: %v", to, err)
				}
			}
			end, _ := testF105LoadExchange(t, e, testF105Gen, nonce)
			if !mqttHoldsEqual(end.Holds, want) {
				t.Fatalf("the holds index changed across the protocol: %v", end.Holds)
			}
			testRequireHolds(t, &e.asm.jsa, end, !tc.ended)
		})
	}
}

// T1, the acknowledgement arrow: a PUBACK releases the source hold in the batch that ends it.
func TestF105AckEndingReleasesItsHold(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-holds-ack")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, _, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")
	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
		Gen: auth.Gen, Nonce: nonce, To: mqttExchangeTerminal, Acq: reg.Acq,
		Register: reg, RegisterSeq: seq,
	}); err != nil {
		t.Fatalf("the PUBACK advance was refused: %v", err)
	}
	end, _ := testF105LoadExchange(t, e, auth.Gen, nonce)
	if end.Phase != mqttExchangeTerminal {
		t.Fatalf("the exchange stands at %d, want terminal", end.Phase)
	}
	testRequireHolds(t, &e.asm.jsa, end, false)
}

// T1, through a real session: a proven-unsent withdrawal releases the obligation's holds, and an
// abandonment — which is not an ending — releases nothing.
func TestF105WithdrawalReleasesAndAbandonmentKeepsTheHolds(t *testing.T) {
	t.Parallel()
	for _, qos := range []byte{1, 2} {
		f := testWithdrawAdmitted(t, "holds-withdraw-q"+string('0'+qos), qos)
		testRequireHolds(t, &f.e.asm.jsa, f.exchange(t), true)
		if removed := f.e.sess.dropProvenUnsentPublishEntry(f.pi, mqttTestLicence(f.e.sess, nil)); removed == nil {
			t.Fatalf("QoS %d: the disposal found no entry to remove", qos)
		}
		f.requireWithdrawnAndGone(t)
		testRequireHolds(t, &f.e.asm.jsa, f.exchange(t), false)
	}
	for _, qos := range []byte{1, 2} {
		f := testAbandonAdmitted(t, "holds-abandon-q"+string('0'+qos), qos)
		f.deny()
		f.requireAbandoned(t)
		// THE ABANDONMENT SUCCEEDED and the edges are untouched: a refusal alone would hide a
		// release on DeliveryAbandoned (M4).
		testRequireHolds(t, &f.e.asm.jsa, f.exchange(t), true)
	}
}

// testHoldsLegitEnding composes a legitimate terminal ending of a QoS 1 obligation holding
// inc-src, with its release, and returns it uncommitted with the stored exchange.
func testHoldsLegitEnding(t *testing.T, e *testF105Env, txn string) (*mqttAuthorityTransition, *mqttOutboundExchange) {
	t.Helper()
	nonce, _, _, _ := testF105AdmitQoS(t, e, "txn-adm-"+txn, 7, 1, "inc-src")
	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, nonce)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	tr, err := mqttAdvanceExchangeTransition(al.rec, al.seq, txn, "op-"+txn, cur, curSeq, mqttExchangeTerminal)
	if err != nil {
		t.Fatalf("composing the ending: %v", err)
	}
	return tr, cur
}

// testHoldsResourceMember returns the index of tr's member on resource inc, or -1.
func testHoldsResourceMember(e *testF105Env, tr *mqttAuthorityTransition, inc string) int {
	subj := mqttResourceSubject(getHash(e.id), testF105Gen, inc)
	for i, m := range tr.Extra {
		if m.subj == subj {
			return i
		}
	}
	return -1
}

// T2: the property, submitted DIRECTLY to the write boundary with every composer bypassed.
func TestF105EndingWithoutReleaseIsRefused(t *testing.T) {
	refused := func(t *testing.T, err error, why string) {
		t.Helper()
		if err == nil {
			t.Fatalf("the batch was ACCEPTED; want it refused by %q", why)
		}
		if !strings.Contains(err.Error(), why) {
			t.Fatalf("refused for another reason: %v (want %q)", err, why)
		}
	}

	t.Run("an ending without its release", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-holds-t2a")
		defer done()
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		tr, _ := testHoldsLegitEnding(t, e, "txn-bare")
		_, err := e.asm.jsa.commitAuthorityTransition(tr)
		refused(t, err, "without releasing its hold")
	})

	t.Run("the same ending with its release commits", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-holds-t2b")
		defer done()
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		tr, cur := testHoldsLegitEnding(t, e, "txn-ok")
		tr = testF105WithHoldReleases(t, e, tr, cur, mqttExchangeTerminal)
		if _, err := e.asm.jsa.commitAuthorityTransition(tr); err != nil {
			t.Fatalf("the ending with its release was refused: %v", err)
		}
		end, _ := testF105LoadExchange(t, e, testF105Gen, cur.Nonce)
		testRequireHolds(t, &e.asm.jsa, end, false)
	})

	t.Run("a release without an ending", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-holds-t2c")
		defer done()
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
		err := testF105ReleaseDependency(t, e, "inc-src", nonce, "txn-rel")
		refused(t, err, "does not move that obligation into an ending")
	})

	t.Run("a release against a stored ending", func(t *testing.T) {
		// The contradiction the atomic release makes unwritable: an ended exchange whose hold
		// still stands. Planted raw, and the standalone release is refused rather than offered
		// as a repair door.
		e, done := testF105NewEnv(t, "f105-holds-t2d")
		defer done()
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
		x, xSeq := testF105LoadExchange(t, e, testF105Gen, nonce)
		ended := *x
		ended.Phase = mqttExchangeRejected
		b, _ := json.Marshal(&ended)
		msg, hdr := mqttFrameBatchWrite(xSeq, b, _EMPTY_, 0, false)
		if _, err := e.asm.jsa.storeMsg(mqttExchangeSubject(getHash(e.id), testF105Gen, nonce), hdr, msg); err != nil {
			t.Fatalf("planting the contradiction: %v", err)
		}
		err := testF105ReleaseDependency(t, e, "inc-src", nonce, "txn-rel")
		refused(t, err, "does not move that obligation into an ending")
	})

	// UNDER AN ENDING KIND the resource member is a hold release and nothing else (P-4, M22).
	for name, edit := range map[string]func(t *testing.T, e *testF105Env, tr *mqttAuthorityTransition, i int){
		"an added edge": func(t *testing.T, e *testF105Env, tr *mqttAuthorityTransition, i int) {
			var r mqttOutboundResource
			_ = json.Unmarshal(tr.Extra[i].body, &r)
			r.Deps = append(r.Deps, mqttResourceEdge{Nonce: mqttNewInflightNonce(), Role: mqttRoleSourceReplay})
			tr.Extra[i].body, _ = json.Marshal(&r)
		},
		"a state change": func(t *testing.T, e *testF105Env, tr *mqttAuthorityTransition, i int) {
			var r mqttOutboundResource
			_ = json.Unmarshal(tr.Extra[i].body, &r)
			r.State, r.AuthTxn = mqttResourceDeleteAuthorized, tr.Attempt.Txn
			tr.Extra[i].body, _ = json.Marshal(&r)
		},
		"a provisioning change": func(t *testing.T, e *testF105Env, tr *mqttAuthorityTransition, i int) {
			var r mqttOutboundResource
			_ = json.Unmarshal(tr.Extra[i].body, &r)
			// TOGGLED, so the edit is a change whatever the fixture planted.
			if r.Prov == mqttProvisioningSealed {
				r.Prov = mqttProvisioningPending
			} else {
				r.Prov = mqttProvisioningSealed
			}
			tr.Extra[i].body, _ = json.Marshal(&r)
		},
		"a resource create": func(t *testing.T, e *testF105Env, tr *mqttAuthorityTransition, i int) {
			fresh := &mqttOutboundResource{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Inc: "inc-new",
				Kind: mqttResourceSourceConsumer, Name: "$MQTT_inc-new", State: mqttResourceLive,
				Prov: mqttProvisioningPending, Deps: []mqttResourceEdge{}}
			b, _ := json.Marshal(fresh)
			tr.Extra = append(tr.Extra, mqttTransitionMember{
				subj: mqttResourceSubject(getHash(e.id), testF105Gen, "inc-new"), expect: 0, body: b})
		},
		"a release of an obligation this batch does not end": func(t *testing.T, e *testF105Env, tr *mqttAuthorityTransition, i int) {
			// A second obligation on the same resource; drop ITS edge too, under this ending.
			other := testF105AdmitOn(t, e, "txn-adm-other", 8, "inc-src")
			r, seq := testF105LoadResource(t, e, "inc-src")
			var keep []mqttResourceEdge
			for _, d := range r.Deps {
				if d.Nonce != other {
					keep = append(keep, d)
				}
			}
			var mine mqttOutboundResource
			_ = json.Unmarshal(tr.Extra[i].body, &mine)
			out := []mqttResourceEdge{}
			for _, d := range keep {
				for _, m := range mine.Deps {
					if d == m {
						out = append(out, d)
					}
				}
			}
			r.Deps = out
			tr.Extra[i].body, _ = json.Marshal(r)
			tr.Extra[i].expect = seq
		},
	} {
		t.Run("ending kind refuses "+name, func(t *testing.T) {
			e, done := testF105NewEnv(t, "f105-holds-k-"+strings.ReplaceAll(name, " ", "-"))
			defer done()
			e.bootstrap(t, "txn-1")
			testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
			tr, cur := testHoldsLegitEnding(t, e, "txn-k")
			tr = testF105WithHoldReleases(t, e, tr, cur, mqttExchangeTerminal)
			i := testHoldsResourceMember(e, tr, "inc-src")
			if i < 0 {
				t.Fatal("fixture: the ending carries no release member")
			}
			edit(t, e, tr, i)
			if _, err := e.asm.jsa.commitAuthorityTransition(tr); err == nil {
				t.Fatalf("an ending kind carried %s", name)
			}
			// AND NOTHING LANDED.
			x, _ := testF105LoadExchange(t, e, testF105Gen, cur.Nonce)
			if x.Phase != mqttExchangePublish {
				t.Fatalf("a refused ending moved the exchange to %d", x.Phase)
			}
		})
	}
}

// T3: the index is derived from the admitted edges, in both directions.
func TestF105HoldsMatchAdmittedEdges(t *testing.T) {
	for name, edit := range map[string]func(x *mqttOutboundExchange){
		"missing": func(x *mqttOutboundExchange) { x.Holds = nil },
		"extra": func(x *mqttOutboundExchange) {
			x.Holds = append(x.Holds, mqttExchangeHold{Inc: "inc-ghost", Role: mqttRoleSourceReplay})
		},
		"wrong role": func(x *mqttOutboundExchange) {
			x.Holds = []mqttExchangeHold{{Inc: "inc-src", Role: mqttRoleCanonicalPubRel}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			e, done := testF105NewEnv(t, "f105-holds-t3-"+strings.ReplaceAll(name, " ", "-"))
			defer done()
			auth, authSeq := e.bootstrap(t, "txn-1")
			src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
			adm := testF105Admission(e, auth, authSeq, "txn-adm", 7,
				[]mqttResourceDependency{{Resource: src, Seq: srcSeq, Role: mqttRoleSourceReplay}})
			tr, err := mqttAdmitPublishTransition(adm)
			if err != nil {
				t.Fatalf("composing the admission: %v", err)
			}
			subj := mqttExchangeSubject(getHash(e.id), testF105Gen, adm.Exchange.Nonce)
			for i := range tr.Extra {
				if tr.Extra[i].subj != subj {
					continue
				}
				var x mqttOutboundExchange
				if err := json.Unmarshal(tr.Extra[i].body, &x); err != nil {
					t.Fatal(err)
				}
				edit(&x)
				tr.Extra[i].body, _ = json.Marshal(&x)
			}
			if _, err := e.asm.jsa.commitAuthorityTransition(tr); err == nil {
				t.Fatalf("an admission with a %s holds index was accepted", name)
			}
		})
	}

	t.Run("immutable after admission", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-holds-t3-immutable")
		defer done()
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		testF105PlantResource(t, e, "inc-rel", mqttResourcePubRelConsumer)
		nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src", "inc-rel")
		cur, curSeq := testF105LoadExchange(t, e, testF105Gen, nonce)
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		tr, err := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-pr", "op-pr", cur, curSeq, mqttExchangePubRecObserved)
		if err != nil {
			t.Fatal(err)
		}
		var x mqttOutboundExchange
		_ = json.Unmarshal(tr.Extra[0].body, &x)
		x.Holds = x.Holds[:1]
		tr.Extra[0].body, _ = json.Marshal(&x)
		if _, err := e.asm.jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatal("an advance rewrote the holds index")
		}
	})
}

// testSealMember builds a member sealing resource inc (LIVE, PENDING) at its standing sequence: a
// change with NO edge of its own, which only the ending-kind confinement refuses (H-2).
func testSealMember(t *testing.T, jsa *mqttJSA, id, epoch, gen, inc string) mqttTransitionMember {
	t.Helper()
	r, seq, err := jsa.loadOutboundResource(id, epoch, gen, inc)
	if err != nil || r == nil {
		t.Fatalf("loading %q: %v", inc, err)
	}
	if r.Prov == mqttProvisioningSealed {
		t.Fatalf("fixture: %q must be unsealed for the seal to be a change", inc)
	}
	sealed := *r
	sealed.Prov = mqttProvisioningSealed
	b, _ := json.Marshal(&sealed)
	return mqttTransitionMember{subj: mqttResourceSubject(getHash(id), gen, inc), expect: seq, body: b}
}

// H-2: an ending batch may not carry a SEPARATE resource member, here another resource's seal.
func TestF105EndingBatchCarriesNoUnrelatedResource(t *testing.T) {
	t.Run("advance", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-holds-h2-adv")
		defer done()
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		testF105PlantUnsealedResource(t, e, "inc-other", mqttResourceSourceConsumer)
		tr, cur := testHoldsLegitEnding(t, e, "txn-h2")
		tr = testF105WithHoldReleases(t, e, tr, cur, mqttExchangeTerminal)
		tr.Extra = append(tr.Extra, testSealMember(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, "inc-other"))
		_, err := e.asm.jsa.commitAuthorityTransition(tr)
		testRequireRefusal(t, err, "a resource member is an edge change")
		if r, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, "inc-other"); r.Prov == mqttProvisioningSealed {
			t.Fatal("an ending batch sealed an unrelated resource")
		}
	})
	t.Run("withdraw", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "holds-h2-withdraw", 1)
		jsa := &f.e.asm.jsa
		id, epoch := f.e.sess.id, f.e.asm.sessStreamEpoch
		other := &mqttOutboundResource{ID: id, Epoch: epoch, Gen: f.gen, Inc: "inc-other",
			Kind: mqttResourceSourceConsumer, Name: "$MQTT_inc-other", State: mqttResourceLive,
			Prov: mqttProvisioningPending, Deps: []mqttResourceEdge{}}
		b, _ := json.Marshal(other)
		msg, hdr := mqttFrameBatchWrite(0, b, _EMPTY_, 0, false)
		if _, err := jsa.storeMsg(mqttResourceSubject(getHash(id), f.gen, "inc-other"), hdr, msg); err != nil {
			t.Fatal(err)
		}
		var refusal error
		hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if tr.Attempt.Kind == mqttTransitionWithdrawUnsent {
				tr.Extra = append(tr.Extra, testSealMember(t, jsa, id, epoch, f.gen, "inc-other"))
				_, refusal = send()
				return 0, refusal
			}
			return send()
		}
		jsa.testTransitionCommit.Store(&hook)
		f.e.sess.dropProvenUnsentPublishEntry(f.pi, mqttTestLicence(f.e.sess, nil))
		jsa.testTransitionCommit.Store(nil)
		testRequireRefusal(t, refusal, "a resource member is an edge change")
		if r, _, _ := jsa.loadOutboundResource(id, epoch, f.gen, "inc-other"); r.Prov == mqttProvisioningSealed {
			t.Fatal("a withdrawal batch sealed an unrelated resource")
		}
	})
}

// H-3: an ADMISSION batch — ordinary, and a pair replacement — may not carry a separate resource
// member either: outside dispose-resource a resource overwrite is an edge change.
func TestF105AdmissionBatchCarriesNoUnrelatedResource(t *testing.T) {
	t.Run("ordinary admission", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-holds-h3-adm")
		defer done()
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		testF105PlantUnsealedResource(t, e, "inc-other", mqttResourceSourceConsumer)
		var refusal error
		hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if tr.Attempt.Kind == mqttTransitionAdmitExchange {
				tr.Extra = append(tr.Extra, testSealMember(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, "inc-other"))
				_, refusal = send()
				return 0, refusal
			}
			return send()
		}
		e.asm.jsa.testTransitionCommit.Store(&hook)
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		_, _ = e.asm.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
			ID: e.id, Epoch: e.epoch, Txn: "txn-adm", Op: "op-adm", Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			Register: freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src",
				Seq: 1, QoS: 1, Nonce: mqttNewInflightNonce()}),
			Deps: []mqttAdmitDependency{{Inc: "inc-src", Role: mqttRoleSourceReplay}},
		})
		e.asm.jsa.testTransitionCommit.Store(nil)
		testRequireRefusal(t, refusal, "a resource member is an edge change")
		if r, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, "inc-other"); r.Prov == mqttProvisioningSealed {
			t.Fatal("an admission sealed an unrelated resource")
		}
	})
	t.Run("pair replacement", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-holds-h3-pair")
		defer done()
		testF105PlantUnsealedResource(t, r.testF105Env, "inc-other", mqttResourceSourceConsumer)
		adm := r.replacingFor(t, mqttReplacePair, "txn-h3-pair")
		adm.Exchange = &mqttOutboundExchange{
			ID: r.id, Epoch: r.epoch, Gen: testF105Gen, Nonce: adm.Register.Lin,
			PI: r.pi, QoS: r.occ.QoS, Phase: mqttExchangePublish,
			AdmitTxn: "txn-h3-pair", Tenure: adm.Auth.Tenure,
		}
		pred := *r.occExchange(t)
		pred.Phase = mqttExchangeSupersededUnsent
		adm.Predecessor, adm.PredecessorSeq = &pred, r.exchangeSeq(t, r.occLin)
		tr, err := mqttReplacePublishTransition(adm)
		if err != nil {
			t.Fatal(err)
		}
		tr.Extra = append(tr.Extra, testSealMember(t, &r.asm.jsa, r.id, r.epoch, testF105Gen, "inc-other"))
		_, err = r.asm.jsa.commitAuthorityTransition(tr)
		testRequireRefusal(t, err, "a resource member is an edge change")
	})
}
