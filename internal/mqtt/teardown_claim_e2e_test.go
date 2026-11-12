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
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// The teardown claim batch, its release, and the shapes that meet a marked record (issue #133,
// plan 5.2 and 5.1), driven through real CONNECTs.

// testSessionGenOf is the generation the resident session of clientID serves.
func testSessionGenOf(t *testing.T, s *Server, clientID string) string {
	t.Helper()
	sess := testMQTTResidentSession(t, s, clientID)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.gen
}

// testOutstandingEntry returns gen's Outstanding entry in clientID's authority, and the authority.
func testOutstandingEntry(t *testing.T, asm *mqttAccountSessionManager, clientID, gen string) (*mqttOutstandingGen, *mqttOutboundAuthority) {
	t.Helper()
	al := asm.jsa.loadOutboundAuthority(clientID, asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil {
		t.Fatalf("the authority of %q reads %v", clientID, al.status)
	}
	_, ord, _ := mqttParseGenToken(gen)
	for _, o := range al.rec.Life.Outstanding {
		if o.Ord == ord {
			o := o
			return &o, al.rec
		}
	}
	return nil, al.rec
}

// T-marker-variants, the marker row: a session ending with no successor makes its generation
// noncurrent in one batch (closed authority, the generation outstanding with its marker and
// discarding closure), and the release closes the lifecycle.
func TestTeardownClaimEndsTheGenerationAndItsReleaseClosesTheLifecycle(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "claim-marker"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	gen := testSessionGenOf(t, s, id)
	testMQTTDisconnect(t, c, nil)
	c.Close()
	testMQTTWaitForRecordAbsent(t, asm, getHash(id))

	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		entry, auth := testOutstandingEntry(t, asm, id, gen)
		switch {
		case auth.State != mqttAuthorityClosed || auth.Gen != gen:
			return fmt.Errorf("the authority is in state %d at %q, want closed at %q", auth.State, auth.Gen, gen)
		case entry == nil:
			return fmt.Errorf("generation %q is not outstanding", gen)
		case entry.Teardown.Kind != mqttTeardownMarker || entry.Teardown.Nonce == _EMPTY_:
			return fmt.Errorf("the entry is %+v, want a marker teardown", entry)
		case entry.Teardown.Pending || entry.Teardown.Displaced:
			return fmt.Errorf("the release did not land: %+v", entry)
		case auth.Life.Phase != mqttLifecycleClosed:
			return fmt.Errorf("the released lifecycle is in phase %d, want closed", auth.Life.Phase)
		}
		return nil
	})
	if cl, _, cst := asm.jsa.loadGenClosure(id, asm.sessStreamEpoch, gen); cst != mqttOutboundValid {
		t.Fatalf("the discarding closure reads %v: %+v", cst, cl)
	}
}

// T-claim-contention, authority-only movement: the authority moves after the claim composed but
// before it lands, the record staying at its base. The frozen batch can never land, so the claim
// recomposes ONCE against the fresh authority and wins; the teardown completes.
//
// M34: routing authority-only movement into the record-moved (superseded) path leaves the record
// standing and the generation current, which this test refuses.
func TestTeardownClaimRecomposesOnceOverAuthorityOnlyMovement(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "claim-authority-moved"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	gen := testSessionGenOf(t, s, id)

	var claims, moved atomic.Int32
	txns := make(chan string, 4)
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Attempt == nil || tr.Attempt.Kind != mqttTransitionTeardownClaim || tr.Attempt.ID != id {
			return
		}
		claims.Add(1)
		txns <- tr.Attempt.Txn
		if !moved.CompareAndSwap(0, 1) {
			return
		}
		// The movement: the same authority at the next revision, written at its exact sequence.
		al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch)
		next := *al.rec
		next.Rev, next.Txn = next.Rev+1, mqttNewInflightNonce()
		body, _ := json.Marshal(&next)
		msg, hdr := mqttFrameBatchWrite(al.seq, body, _EMPTY_, 0, false)
		if _, err := asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(id)), hdr, msg); err != nil {
			t.Errorf("moving the authority: %v", err)
		}
	}
	asm.jsa.testTransitionPresend.Store(&pre)
	defer asm.jsa.testTransitionPresend.Store(nil)

	testMQTTDisconnect(t, c, nil)
	c.Close()
	testMQTTWaitForRecordAbsent(t, asm, getHash(id))

	if n := claims.Load(); n != 2 {
		t.Fatalf("%d claim batches were sent, want the frozen one and exactly one recomposition", n)
	}
	if first, second := <-txns, <-txns; first == second {
		t.Fatalf("the recomposition reused transition %q: it must be a new one against the fresh authority", first)
	}
	if entry, auth := testOutstandingEntry(t, asm, id, gen); entry == nil || auth.State != mqttAuthorityClosed {
		t.Fatalf("the recomposed claim did not end generation %q: entry %+v, state %d", gen, entry, auth.State)
	}
}

// T-displace: a successor CONNECT meets a record whose marker's release has not landed. It writes
// over the marker, discharging that release in its own batch (pending false, displaced), and the
// generation stays outstanding while the successor serves.
//
// M39: a displacement that does not clear pending leaves the generation unfenceable forever; this
// test reads pending false.
func TestTeardownClaimIsDisplacedByASuccessor(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "claim-displaced"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	gen := testSessionGenOf(t, s, id)

	// The first close stops at its gate release, after the claim and before the record's release:
	// the state a crash there leaves.
	var stopped atomic.Bool
	testSetGateReleasedHook(t, func(sess *mqttSession) error {
		if sess.id == id && stopped.CompareAndSwap(false, true) {
			return errTestContinuationAbandoned
		}
		return nil
	})
	testMQTTDisconnect(t, c, nil)
	c.Close()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if !stopped.Load() {
			return fmt.Errorf("the close has not reached its gate release")
		}
		return nil
	})
	if entry, _ := testOutstandingEntry(t, asm, id, gen); entry == nil || !entry.Teardown.Pending {
		t.Fatalf("staging: generation %q's release must still be pending, got %+v", gen, entry)
	}

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if present, rc, _ := testMQTTReadConnAckV5(t, r2); rc != mqttConnAckRCConnectionAccepted || present {
		t.Fatalf("the successor was answered rc=%#x sessionPresent=%v", rc, present)
	}
	entry, auth := testOutstandingEntry(t, asm, id, gen)
	if entry == nil || entry.Teardown.Pending || !entry.Teardown.Displaced {
		t.Fatalf("the displacement must discharge generation %q's release: %+v", gen, entry)
	}
	if auth.State != mqttAuthorityServing || auth.Gen == gen || auth.Life.Phase != mqttLifecycleServing {
		t.Fatalf("the successor must serve a fresh generation: state %d gen %q phase %d", auth.State, auth.Gen, auth.Life.Phase)
	}
}

// The restore's retirement fence: an UNMARKED record whose generation the authority no longer
// serves (what a revived limit-evicted record leaves, MAXMSGSPER-REVIVAL) is obsolete. The CONNECT
// does not resume it and does not tear it down again: the record alone is deleted and the session
// starts fresh, under a new generation.
func TestRestoreDeletesAnUnmarkedRecordOfANoncurrentGeneration(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "restore-inconsistent"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	gen := testSessionGenOf(t, s, id)
	testMQTTDisconnect(t, c, nil)
	c.Close()
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		sess := testMQTTResidentSessionOrNil(asm, id)
		if sess == nil {
			return nil
		}
		sess.mu.Lock()
		bound := sess.c != nil
		sess.mu.Unlock()
		if bound {
			return fmt.Errorf("the session is still bound")
		}
		asm.removeSession(sess, true)
		return nil
	})
	// The generation is made noncurrent with its record left unmarked.
	testRetireGeneration(t, asm, id, gen)

	sess, existed, err := asm.createOrRestoreSession(id, s.getOpts())
	if err != nil {
		t.Fatalf("the restore over an obsolete record must start fresh: %v", err)
	}
	if existed || sess.seq != 0 {
		t.Fatalf("an obsolete record was resumed: existed=%v seq=%d", existed, sess.seq)
	}
	if rec, _ := testCleanStartRecord(t, asm, getHash(id)); rec != nil {
		t.Fatalf("the obsolete record of generation %q still stands: %+v", gen, rec)
	}
}

// testAbandonBeforeBind arms the before-bind seam so that clientID's next CONNECT, paused between
// its acceptance and its bind, has its pending acceptance abandoned by the sweep's own step. It
// reports whether the abandonment ran.
func testAbandonBeforeBind(t *testing.T, asm *mqttAccountSessionManager, clientID string) *atomic.Bool {
	t.Helper()
	var ran atomic.Bool
	h := func(cid string) {
		if cid != clientID || ran.Load() {
			return
		}
		al := asm.jsa.loadOutboundAuthority(clientID, asm.sessStreamEpoch)
		if err := asm.abandonPendingAcceptance(clientID, al, time.Now(), 0, nil); err != nil {
			t.Errorf("abandoning the pending acceptance of %q: %v", clientID, err)
			return
		}
		ran.Store(true)
	}
	mqttTestBeforeBindHookV.Store(&h)
	t.Cleanup(func() { mqttTestBeforeBindHookV.Store(nil) })
	return &ran
}

// T-bind-vs-abandon: a CONNECT pauses between its acceptance and its bind, and the sweep abandons
// its fresh acceptance. The bind is refused, no success CONNACK is sent, and the generation the
// acceptance installed is torn down as an abandonment.
func TestBindLosesToAnAbandonment(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "bind-vs-abandon"
	ran := testAbandonBeforeBind(t, asm, id)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, rc, _ := testMQTTReadConnAckV5(t, r); rc == mqttConnAckRCConnectionAccepted {
		t.Fatal("a CONNECT whose acceptance was abandoned before its bind was answered with success")
	}
	if !ran.Load() {
		t.Fatal("the abandonment never ran, so the bind it races was not exercised")
	}
	al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityClosed || al.rec.Life.Pending != nil {
		t.Fatalf("the abandoned acceptance's authority reads %v: %+v", al.status, al.rec)
	}
	entry, _ := testOutstandingEntry(t, asm, id, al.rec.Gen)
	if entry == nil || entry.Teardown.Kind != mqttTeardownAbandon {
		t.Fatalf("the abandoned generation must be outstanding as an abandonment, got %+v", entry)
	}
}

// T-resume-abandon: a persistent session reconnects without Clean Start and pauses before its
// bind; the sweep abandons the resume. The generation stays current and the record says the
// session is disconnected with its prior lifetime, as after a CONNECT refused before CONNACK. The
// prior lifetime is the PAIR the CONNECT's own lifetime save replaced: a resume requesting expiry
// zero saved a clean session, and the abandonment restores it persistent, so the next resume
// finds the session and its pending QoS 1 message. Mutation: the abandonment restoring the
// expiry alone (the zero-expiry arm reads Clean and the next resume discards the session).
func TestResumeAbandonmentKeepsTheGeneration(t *testing.T) {
	for _, requested := range []uint32{600, 0} {
		t.Run(fmt.Sprintf("requested expiry %d", requested), func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			// The refused resume's restore may already have offered the owed message; it returns
			// at AckWait, as any refused connection's offered delivery does.
			o.MQTT.AckWait = time.Second
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)
			asm := testMQTTGetGlobalASM(t, s)
			id := fmt.Sprintf("resume-abandon-%d", requested)
			const topic = "resume/abandon"

			c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true,
				props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, r)
			testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
			gen := testSessionGenOf(t, s, id)
			testMQTTDisconnect(t, c, nil)
			c.Close()
			pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, pr)
			testMQTTPublishV5(t, pc, 1, 1, topic, []byte("owed"))
			testMQTTReadPubAckV5(t, pr, 1)
			pc.Close()

			ran := testAbandonBeforeBind(t, asm, id)
			c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: false,
				props: mqttV5ConnPropsSessionExpiry(requested)}, o.MQTT.Host, o.MQTT.Port)
			if _, rc, _ := testMQTTReadConnAckV5(t, r2); rc == mqttConnAckRCConnectionAccepted {
				t.Fatal("a resume whose acceptance was abandoned before its bind was answered with success")
			}
			c2.Close()
			if !ran.Load() {
				t.Fatal("the abandonment never ran, so the bind it races was not exercised")
			}
			mqttTestBeforeBindHookV.Store(nil)
			al := asm.jsa.loadOutboundAuthority(id, asm.sessStreamEpoch)
			if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityServing || al.rec.Gen != gen ||
				al.rec.Life.Pending != nil {
				t.Fatalf("an abandoned resume must keep generation %q current with nothing pending: %v %+v", gen, al.status, al.rec)
			}
			rec, _ := testCleanStartRecord(t, asm, getHash(id))
			if rec == nil || rec.Gen != gen || rec.ExpiryInterval != 300 || rec.Clean || rec.DisconnectedAt == 0 {
				t.Fatalf("the record must be the persistent session disconnected with its prior 300s lifetime, got %+v", rec)
			}

			// THE NEXT RESUME finds the session, and the message it was owed.
			checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
				if sess := testMQTTResidentSessionOrNil(asm, id); sess != nil {
					sess.mu.Lock()
					bound := sess.c != nil
					sess.mu.Unlock()
					if bound {
						return fmt.Errorf("the refused connection is still bound")
					}
				}
				return nil
			})
			c3, r3 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: false,
				props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
			defer c3.Close()
			present, rc, _ := testMQTTReadConnAckV5(t, r3)
			if rc != mqttConnAckRCConnectionAccepted || !present {
				t.Fatalf("the resume after the abandonment was answered %#x, session present %v", rc, present)
			}
			if g := testSessionGenOf(t, s, id); g != gen {
				t.Fatalf("the resume after the abandonment serves generation %q, want %q", g, gen)
			}
			testMQTTReadPublishV5(t, r3, topic, []byte("owed"))
		})
	}
}

// A resume whose bind fails with no abandonment (every attempt's authority invalidated, so the
// bind exhausts its budget) is refused before its lifetime grant (issue #133): the record keeps
// the prior lifetime, a requested expiry of zero never persists a clean session, the refused
// connection's close does not tear the session down, and the next resume finds it with the
// message it was owed. Mutation: the grant moved back before the bind (the zero-expiry grant
// persists, the close discards the session).
func TestARefusedBindLeavesThePriorLifetime(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const id = "refused-bind-lifetime"
	const topic = "refused/bind"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	gen := testSessionGenOf(t, s, id)
	testMQTTDisconnect(t, c, nil)
	c.Close()
	pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, pr)
	testMQTTPublishV5(t, pc, 1, 1, topic, []byte("owed"))
	testMQTTReadPubAckV5(t, pr, 1)
	pc.Close()

	var binds atomic.Int32
	h := func(cid, stage string) {
		if cid == id && stage == "bind" {
			binds.Add(1)
			testInvalidateAuthorityOf(t, asm, id)
		}
	}
	mqttAcceptTestReadToSendHookV.Store(&h)
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(0)}, o.MQTT.Host, o.MQTT.Port)
	if _, rc, _ := testMQTTReadConnAckV5(t, r2); rc == mqttConnAckRCConnectionAccepted {
		mqttAcceptTestReadToSendHookV.Store(nil)
		t.Fatal("a resume whose bind never landed was answered with success")
	}
	c2.Close()
	mqttAcceptTestReadToSendHookV.Store(nil)
	if binds.Load() == 0 {
		t.Fatal("the bind was never reached, so its refusal was not exercised")
	}
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		if sess := testMQTTResidentSessionOrNil(asm, id); sess != nil {
			sess.mu.Lock()
			bound := sess.c != nil
			sess.mu.Unlock()
			if bound {
				return fmt.Errorf("the refused connection is still bound")
			}
		}
		return nil
	})
	rec, _ := testCleanStartRecord(t, asm, getHash(id))
	if rec == nil || rec.Gen != gen || rec.ExpiryInterval != 300 || rec.Clean {
		t.Fatalf("the refused resume must leave the record on its prior 300s persistent lifetime, got %+v", rec)
	}
	c3, r3 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	present, rc, _ := testMQTTReadConnAckV5(t, r3)
	if rc != mqttConnAckRCConnectionAccepted || !present {
		t.Fatalf("the resume after the refused bind was answered %#x, session present %v", rc, present)
	}
	if g := testSessionGenOf(t, s, id); g != gen {
		t.Fatalf("the resume after the refused bind serves generation %q, want %q", g, gen)
	}
	testMQTTReadPublishV5(t, r3, topic, []byte("owed"))
}

// A teardown claim's closure and closed state are one fact, written for the generation it ends,
// naming the claim itself, and moving no ownership: every substitution is refused at the write
// boundary, and the claim as composed lands (the positive control, run last).
func TestTeardownClaimClosesItsOwnGenerationInOneBatch(t *testing.T) {
	e, done := testNewEnv(t, "claim-closure-rules")
	defer done()
	auth, seq := e.bootstrap(t, "txn-1")
	for name, mutate := range map[string]func(tr *mqttAuthorityTransition){
		"a closure naming another transition": func(tr *mqttAuthorityTransition) { tr.Closure.Txn = "txn-other" },
		"another client's closure":            func(tr *mqttAuthorityTransition) { tr.Closure.ID = "client-b" },
		"no closure":                          func(tr *mqttAuthorityTransition) { tr.Closure = nil },
		"another generation's closure":        func(tr *mqttAuthorityTransition) { tr.Closure.Gen = testGen2 },
		"an authority left serving":           func(tr *mqttAuthorityTransition) { tr.Auth.State = mqttAuthorityServing },
		"a moved tenure": func(tr *mqttAuthorityTransition) {
			moved := *tr.Auth
			moved.Tenure = "tenure-moved"
			tr.Auth = &moved
		},
	} {
		tr := testClosure(e, auth, seq, "txn-close")
		mutate(tr)
		if _, err := e.asm.jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a teardown claim carrying %s was committed", name)
		}
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(testClosure(e, auth, seq, "txn-close")); err != nil {
		t.Fatalf("the claim as composed was refused: %v", err)
	}
	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, auth.Gen); st != mqttOutboundValid {
		t.Fatalf("the claim left no closure of generation %q: %v", auth.Gen, st)
	}
}
