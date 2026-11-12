//go:build mqtt_fulltests

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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// testComposeCreation composes, and does not send, a first creation of client id, record member
// included, exactly as firstReservation does for the env's own client.
func testComposeCreation(t *testing.T, e *testOutboundEnv, id, txn string) *mqttAuthorityTransition {
	t.Helper()
	jsa := &e.asm.jsa
	al := jsa.loadOutboundAuthority(id, e.epoch)
	reg := jsa.loadLifecycleRegistryShard(e.epoch, mqttLifecycleRegistryShardFor(getHash(id)))
	ca, err := jsa.composeAcceptance(mqttAcceptRequest{
		ID: id, Epoch: e.epoch, Txn: txn, Tenure: "tenure-" + txn, Nonce: "intent-" + txn,
		At: time.Now().UnixNano(),
	}, al, reg)
	if err != nil {
		t.Fatalf("composing the creation %q: %v", txn, err)
	}
	ca.T.Record = &mqttTransitionMember{
		subj: mqttSessSubject(e.asm.domainTk, getHash(id)), expect: 0,
		body: []byte(`{"id":"` + id + `","gen":"` + ca.Gen + `"}`),
	}
	return ca.T
}

// T-creation-resolution (issue #133): a creation whose acknowledgement was lost is resolved from
// the authority it installs, row by row. Unlanded so far: the SAME frozen batch is re-sent and
// lands. Current: a lost acknowledgement of a landed creation is adopted with no second write.
// NotCurrent: a competing creator took the client, so the frozen batch can never land and the
// CONNECT recomposes against what stands. Gone: the authority it expected is absent but the
// registry it CASed moved, so every re-send is refused and the CONNECT is refused with nothing
// landed. Unreadable: no replay, the CONNECT refuses.
//
// (The plan's resolver read the registry for NotCurrent, Gone and Inconsistent; the implemented
// one reads the authority alone and lets the registry CAS refuse a resend, DECISIONS.md step 6.)
func TestACreationIsResolvedFromWhatItInstalls(t *testing.T) {
	e, done := testNewEnv(t, "creation-resolution")
	defer done()
	lost := errors.New("test: the acknowledgement was lost")
	allow := func() error { return nil }
	resolve := func(id string, tr *mqttAuthorityTransition, step func() error) (bool, bool, error) {
		sess, _, err := e.asm.createOrRestoreSession(id, e.asm.server().getOpts())
		if err != nil {
			t.Fatalf("a session object for %q: %v", id, err)
		}
		return sess.resolveAcceptance(&e.asm.jsa, mqttSessSaveDetail{acceptShape: tr, rawErr: lost},
			tr.Attempt.Intent, allow, step)
	}

	// UNLANDED SO FAR, then CURRENT.
	a := testComposeCreation(t, e, "res-unlanded", "txn-a")
	if landed, retry, err := resolve("res-unlanded", a, allow); !landed || retry || err != nil {
		t.Fatalf("an unlanded creation resolved landed=%v retry=%v err=%v, want the re-send landed", landed, retry, err)
	}
	before := e.asm.jsa.loadOutboundAuthority("res-unlanded", e.epoch)
	if before.status != mqttOutboundValid || before.rec.Life.Accept.Nonce != a.Attempt.Intent {
		t.Fatalf("the re-sent creation did not land: %v", before.status)
	}
	if landed, _, err := resolve("res-unlanded", a, allow); !landed || err != nil {
		t.Fatalf("a landed creation's lost acknowledgement resolved landed=%v err=%v", landed, err)
	}
	if after := e.asm.jsa.loadOutboundAuthority("res-unlanded", e.epoch); after.seq != before.seq {
		t.Fatalf("resolving a landed creation wrote again (%d -> %d)", before.seq, after.seq)
	}

	// NOT CURRENT: a competing creator's batch landed first.
	frozen := testComposeCreation(t, e, "res-competed", "txn-frozen")
	rival := testComposeCreation(t, e, "res-competed", "txn-rival")
	if _, err := e.asm.jsa.commitAuthorityTransition(rival); err != nil {
		t.Fatalf("the rival creation: %v", err)
	}
	if landed, retry, err := resolve("res-competed", frozen, allow); landed || !retry || err != nil {
		t.Fatalf("a creation beaten by a rival resolved landed=%v retry=%v err=%v, want a fresh composition", landed, retry, err)
	}

	// GONE: the authority it expected is absent, but the registry it CASed has moved on.
	gone := testComposeCreation(t, e, "res-gone", "txn-gone")
	other := testComposeCreation(t, e, "res-gone", "txn-took")
	if _, err := e.asm.jsa.commitAuthorityTransition(other); err != nil {
		t.Fatalf("the creation that took the shard: %v", err)
	}
	for _, subj := range []string{mqttOutboundAuthoritySubject(getHash("res-gone")), mqttSessSubject(e.asm.domainTk, getHash("res-gone"))} {
		if err := e.asm.jsa.purgeStream(mqttSessStreamName, subj, 0, false); err != nil {
			t.Fatalf("staging the retired client: %v", err)
		}
	}
	steps := 0
	budget := func() error {
		if steps++; steps > 2 {
			return errors.New("test: the re-send allowance is spent")
		}
		return nil
	}
	if landed, _, err := resolve("res-gone", gone, budget); landed || err == nil {
		t.Fatalf("a creation over a retired client resolved landed=%v err=%v, want refused with nothing landed", landed, err)
	}
	if al := e.asm.jsa.loadOutboundAuthority("res-gone", e.epoch); al.status != mqttOutboundAbsent {
		t.Fatalf("a refused re-send left an authority (%v)", al.status)
	}

	// UNREADABLE: no replay.
	bad := testComposeCreation(t, e, "res-unreadable", "txn-bad")
	if _, err := e.asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash("res-unreadable")), 0, []byte(`{"id":`)); err != nil {
		t.Fatalf("planting an unreadable authority: %v", err)
	}
	if landed, retry, err := resolve("res-unreadable", bad, allow); landed || retry || err == nil {
		t.Fatalf("a creation over an unreadable authority resolved landed=%v retry=%v err=%v, want unresolved", landed, retry, err)
	}
}

// testPendingAcceptance commits a creation of client id without its bind, leaving its acceptance
// pending, and returns a session object for it and the acceptance's nonce.
func testPendingAcceptance(t *testing.T, e *testOutboundEnv, id string) (*mqttSession, string) {
	t.Helper()
	tr := testComposeCreation(t, e, id, "txn-"+id)
	if _, err := e.asm.jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("committing the acceptance of %q: %v", id, err)
	}
	sess, _, err := e.asm.createOrRestoreSession(id, e.asm.server().getOpts())
	if err != nil {
		t.Fatalf("a session object for %q: %v", id, err)
	}
	return sess, tr.Attempt.Intent
}

// T-bind-resolver (issue #133): the bind before CONNACK, under each reading of plan 5.1's bind
// resolver. A lost acknowledgement is resolved from the bind's own outcome (served, no second
// bind). Same-subject contention (10164) decided nothing, so the same frozen bind is re-sent.
// Authority-only movement makes the frozen bind unlandable, so it is recomposed once. A bind
// whose acceptance was superseded refuses before CONNACK. Exhaustion refuses, leaving the
// acceptance pending for abandonment. (The plan's "10071 with the record moved" row has no
// bind counterpart: the bind writes the authority alone.) Mutation M26: authority-only
// movement routed into the refusal (the moved-authority row is refused instead of rebound).
func TestTheBindIsResolvedFromItsOwnOutcome(t *testing.T) {
	e, done := testNewEnv(t, "bind-resolver")
	defer done()
	budget := func() *mqttOwnershipBudget { return &mqttOwnershipBudget{Anchor: time.Now(), Total: time.Minute} }
	ok := func() error { return nil }
	type seen struct{ sends, binds int }
	arm := func(t *testing.T, id string, act func(n int, tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error)) *seen {
		s := &seen{}
		testArmTransitionSeam(t, &e.asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if tr.Attempt.ID != id || tr.Attempt.Kind != mqttTransitionBind {
				return send()
			}
			s.binds++
			return act(s.binds, tr, func() (uint64, error) { s.sends++; return send() })
		})
		return s
	}
	pending := func(id string) *mqttPendingAcceptance {
		return e.asm.jsa.loadOutboundAuthority(id, e.epoch).rec.Life.Pending
	}

	t.Run("lost acknowledgement", func(t *testing.T) {
		sess, nonce := testPendingAcceptance(t, e, "bind-lost-ack")
		s := arm(t, "bind-lost-ack", func(n int, tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if _, err := send(); err != nil {
				return 0, err
			}
			return 0, errors.New("test: the acknowledgement was lost")
		})
		if err := sess.bindAcceptance(budget(), nonce, ok); err != nil {
			t.Fatalf("a bind whose acknowledgement was lost was not resolved landed: %v", err)
		}
		if s.sends != 1 || pending("bind-lost-ack") != nil {
			t.Fatalf("the lost acknowledgement led to %d send(s), pending %+v", s.sends, pending("bind-lost-ack"))
		}
	})
	t.Run("contention re-sends the frozen bind", func(t *testing.T) {
		sess, nonce := testPendingAcceptance(t, e, "bind-contended")
		var first string
		s := arm(t, "bind-contended", func(n int, tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if n == 1 {
				first = tr.Attempt.Txn
				return 0, ApiErrors[JSStreamWrongLastSequenceConstantErr]
			}
			if tr.Attempt.Txn != first {
				t.Errorf("the contended bind was recomposed (%q -> %q), want the same frozen bind", first, tr.Attempt.Txn)
			}
			return send()
		})
		if err := sess.bindAcceptance(budget(), nonce, ok); err != nil || s.binds != 2 || pending("bind-contended") != nil {
			t.Fatalf("a contended bind resolved %v after %d bind(s), pending %+v", err, s.binds, pending("bind-contended"))
		}
	})
	t.Run("authority-only movement recomposes once", func(t *testing.T) {
		sess, nonce := testPendingAcceptance(t, e, "bind-moved")
		var first string
		s := arm(t, "bind-moved", func(n int, tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if n == 1 {
				first = tr.Attempt.Txn
				testAcceptSharedTenure(t, &e.asm.jsa, "bind-moved", e.epoch, testTenure(5)) // an authority-only write
			} else if tr.Attempt.Txn == first {
				t.Error("the bind frozen against a moved authority was re-sent instead of recomposed")
			}
			return send()
		})
		if err := sess.bindAcceptance(budget(), nonce, ok); err != nil || s.binds != 2 || pending("bind-moved") != nil {
			t.Fatalf("a bind over a moved authority resolved %v after %d bind(s)", err, s.binds)
		}
	})
	t.Run("a superseded acceptance refuses", func(t *testing.T) {
		sess, nonce := testPendingAcceptance(t, e, "bind-superseded")
		if err := sess.bindAcceptance(budget(), "intent-another", ok); !errors.Is(err, errMQTTAcceptanceLost) {
			t.Fatalf("a bind of an acceptance not pending returned %v, want errMQTTAcceptanceLost", err)
		}
		if p := pending("bind-superseded"); p == nil || p.Nonce != nonce {
			t.Fatalf("the refused bind moved the pending acceptance: %+v", p)
		}
	})
	t.Run("exhaustion refuses and leaves the acceptance pending", func(t *testing.T) {
		sess, nonce := testPendingAcceptance(t, e, "bind-exhausted")
		arm(t, "bind-exhausted", func(int, *mqttAuthorityTransition, func() (uint64, error)) (uint64, error) {
			return 0, ApiErrors[JSStreamWrongLastSequenceConstantErr]
		})
		if err := sess.bindAcceptance(budget(), nonce, ok); err == nil {
			t.Fatal("an exhausted bind was reported landed")
		}
		if p := pending("bind-exhausted"); p == nil || p.Nonce != nonce {
			t.Fatalf("an exhausted bind did not leave its acceptance pending: %+v", p)
		}
	})
}

// testPendingResume commits a resume acceptance of the env's client with a delay-capable Will and
// no bind: its acceptance pending, its anchor ARMED.
func testPendingResume(t *testing.T, e *testOutboundEnv, txn string) (string, *mqttWillAnchorRef) {
	t.Helper()
	jsa := &e.asm.jsa
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	reg := jsa.loadLifecycleRegistryShard(e.epoch, mqttLifecycleRegistryShardFor(getHash(e.id)))
	recSubj := mqttSessSubject(e.asm.domainTk, getHash(e.id))
	smsg, lerr := jsa.loadLastMsgFor(mqttSessStreamName, recSubj)
	if lerr != nil || smsg == nil {
		t.Fatalf("reading the env's record: %v", lerr)
	}
	ca, err := jsa.composeAcceptance(mqttAcceptRequest{
		ID: e.id, Epoch: e.epoch, Txn: txn, Tenure: "tenure-" + txn, Nonce: "intent-" + txn,
		At: time.Now().UnixNano(), RecordExists: true, WillDelayCapable: true,
	}, al, reg)
	if err != nil || ca.Anchor == nil {
		t.Fatalf("composing the resume %q: %v (anchor %v)", txn, err, ca.Anchor)
	}
	ca.T.Record = &mqttTransitionMember{subj: recSubj, expect: smsg.Sequence, body: smsg.Data}
	if _, cerr := jsa.commitAuthorityTransition(ca.T); cerr != nil {
		t.Fatalf("committing the resume %q: %v", txn, cerr)
	}
	return "intent-" + txn, &mqttWillAnchorRef{L: ca.L, Ord: ca.Anchor.Ord}
}

// T-refused-superseded (issue #133): acceptance A arms its anchor; acceptance B replaces A's
// pending acceptance and arms its own; A's bind loses and A is refused; A's first anchor removal
// fails and its retained removal retries until it lands; B's anchor is untouched and B binds,
// leaving B's the only anchor, so nothing of A pins the lifecycle.
// Mutation M35: the refusal's removal without its retry (A's anchor pins the lifecycle).
func TestASupersededAcceptanceRemovesOnlyItsOwnAnchor(t *testing.T) {
	e, done := testNewEnv(t, "refused-superseded")
	defer done()
	e.bootstrap(t, "txn-1")
	nonceA, refA := testPendingResume(t, e, "txn-a")
	nonceB, refB := testPendingResume(t, e, "txn-b")
	sess, _, err := e.asm.createOrRestoreSession(e.id, e.asm.server().getOpts())
	if err != nil {
		t.Fatal(err)
	}
	budget := &mqttOwnershipBudget{Anchor: time.Now(), Total: time.Minute}
	if berr := sess.bindAcceptance(budget, nonceA, func() error { return nil }); !errors.Is(berr, errMQTTAcceptanceLost) {
		t.Fatalf("the superseded acceptance's bind returned %v, want errMQTTAcceptanceLost", berr)
	}
	var failed atomic.Bool
	e.asm.mu.Lock()
	e.asm.testAnchorTransitionErr = func(_ string, op mqttAnchorOp) error {
		if op == mqttAnchorRemove && failed.CompareAndSwap(false, true) {
			return errors.New("test: the first removal failed")
		}
		return nil
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testAnchorTransitionErr = nil
		e.asm.mu.Unlock()
	})
	e.asm.dropWillAnchor(e.id, refA)
	standing := func(ref *mqttWillAnchorRef) bool {
		for _, a := range testWillAnchorsOf(t, e.asm, e.id) {
			if a.Ord == ref.Ord {
				return true
			}
		}
		return false
	}
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if standing(refA) {
			return fmt.Errorf("the refused acceptance's anchor %d still stands", refA.Ord)
		}
		return nil
	})
	if !failed.Load() {
		t.Fatal("fixture: the first removal was never failed, so its retry was not exercised")
	}
	if !standing(refB) {
		t.Fatal("the refused acceptance's cleanup removed the winner's anchor")
	}
	if berr := sess.bindAcceptance(budget, nonceB, func() error { return nil }); berr != nil {
		t.Fatalf("the winning acceptance did not bind: %v", berr)
	}
	// NOTHING OF A IS LEFT to pin the lifecycle: the serving connection's anchor is the only one,
	// so the lifecycle retires once that connection's Will is settled.
	if a := testWillAnchorsOf(t, e.asm, e.id); len(a) != 1 || a[0].Ord != refB.Ord {
		t.Fatalf("after the winner binds the lifecycle holds anchors %+v, want only the winner's %d", a, refB.Ord)
	}
}
