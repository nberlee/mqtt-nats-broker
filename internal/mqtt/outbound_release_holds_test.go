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

//go:build mqtt_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testEndedHolding is one QoS 2 obligation holding each of incs, driven by its acknowledgements
// to Terminal: its holds stand RELEASE-OWED (outbound_release_holds.go).
func testEndedHolding(t *testing.T, client string, incs ...string) (*testF105Env, func(), string) {
	t.Helper()
	e, done := testF105NewEnv(t, client)
	e.bootstrap(t, "txn-1")
	for _, inc := range incs {
		kind := mqttResourceSourceConsumer
		if strings.HasPrefix(inc, "inc-rel") {
			kind = mqttResourcePubRelConsumer
		}
		testF105PlantResource(t, e, inc, kind)
	}
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, incs...)
	jsa := &e.asm.jsa
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testF105Gen, nonce, mqttExchangePubRecObserved); err != nil {
		t.Fatalf("the PUBREC advance was refused: %v", err)
	}
	for i := 0; i < 3; i++ {
		x, _, _ := jsa.loadOutboundExchange(e.id, e.epoch, testF105Gen, nonce)
		if x != nil && x.Phase == mqttExchangeTerminal {
			return e, done, nonce
		}
		if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testF105Gen, nonce, mqttExchangeTerminal); err != nil {
			t.Fatalf("the PUBCOMP advance was refused: %v", err)
		}
	}
	t.Fatalf("fixture: obligation %q never reached Terminal", nonce)
	return nil, nil, _EMPTY_
}

// testReleaseOwed releases the holds an acknowledgement ending of nonce left owed.
func testReleaseOwed(t *testing.T, e *testF105Env, nonce string) {
	t.Helper()
	if _, err := e.asm.jsa.opReleaseHolds(e.id, e.epoch, testF105Gen, []string{nonce}, nil,
		mqttNewInflightNonce(), nil); err != nil {
		t.Fatalf("releasing obligation %q's owed holds: %v", nonce, err)
	}
}

// testHoldStands reports whether resource inc still lists obligation nonce's hold.
func testHoldStands(t *testing.T, e *testF105Env, inc, nonce string) bool {
	t.Helper()
	r, _, err := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, inc)
	if err != nil || r == nil {
		t.Fatalf("reading resource %q: %v", inc, err)
	}
	for _, d := range r.Deps {
		if d.Nonce == nonce {
			return true
		}
	}
	return false
}

// testDutyNaming finds the delivery duty predeclared for obligation nonce.
func testDutyNaming(t *testing.T, e *testF105Env, nonce string) mqttSweptDuty {
	t.Helper()
	led, _, err := e.asm.jsa.loadGenLedger(e.id, e.epoch, testF105Gen)
	if err != nil || led == nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	for ord := led.Lo; ord <= led.Hi; ord++ {
		d, _, derr := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, testF105Gen, ord)
		if derr == nil && d != nil && d.Kind == mqttDutyExchange && d.Ref == nonce {
			return mqttSweptDuty{Ord: ord, Entry: d}
		}
	}
	t.Fatalf("no duty names obligation %q", nonce)
	return mqttSweptDuty{}
}

// testReleaseBatch composes a raw release-holds batch: the given resources with obligation
// nonce's hold edge removed, then the given duties written terminal.
func testReleaseBatch(t *testing.T, e *testF105Env, nonce string, incs []string, duties ...mqttSweptDuty) *mqttAuthorityTransition {
	t.Helper()
	jsa := &e.asm.jsa
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	tr := &mqttAuthorityTransition{Attempt: &mqttTransitionAttempt{
		ID: e.id, Epoch: e.epoch, Txn: mqttNewInflightNonce(), Op: "op-release", Kind: mqttTransitionReleaseHolds,
		Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
	}}
	for _, inc := range incs {
		r, seq, err := jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, inc)
		if err != nil || r == nil {
			t.Fatalf("reading resource %q: %v", inc, err)
		}
		next := *r
		next.Deps = nil
		for _, d := range r.Deps {
			if d.Nonce != nonce {
				next.Deps = append(next.Deps, d)
			}
		}
		if next.Deps == nil {
			next.Deps = []mqttResourceEdge{}
		}
		b, _ := json.Marshal(&next)
		tr.Extra = append(tr.Extra, mqttTransitionMember{subj: mqttResourceSubject(getHash(e.id), testF105Gen, inc),
			expect: seq, body: b})
	}
	for _, d := range duties {
		cur, seq, err := jsa.loadDutyEntryAt(e.id, e.epoch, testF105Gen, d.Ord)
		if err != nil || cur == nil {
			t.Fatalf("reading duty %d: %v", d.Ord, err)
		}
		b, berr := mqttResolvedDutyBody(cur, mqttDutyOutcomeResolved)
		if berr != nil {
			t.Fatal(berr)
		}
		tr.Extra = append(tr.Extra, mqttTransitionMember{subj: mqttDutySubject(getHash(e.id), testF105Gen, d.Ord),
			expect: seq, body: b})
	}
	return tr
}

// testDutyTerminal reports whether duty d stands terminal.
func testDutyTerminal(t *testing.T, e *testF105Env, d mqttSweptDuty) bool {
	t.Helper()
	cur, _, err := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, testF105Gen, d.Ord)
	if err != nil || cur == nil {
		t.Fatalf("reading duty %d: %v", d.Ord, err)
	}
	return mqttDutyResolved(cur.State)
}

// THE ACKNOWLEDGEMENT ENDING LEAVES ITS HOLD RELEASE-OWED: Terminal exchange, edge listed, duty
// pending. A release-holds batch then removes the edge; the duty is resolved by whoever resolves
// duties (here the recovery executor's opResolveDuty), which the released edge now earns.
//
// Mutation M-L0a: the completion releases in its own batch again (mqttCompletionReleasesInBatch
// true) -> the edge is gone right after the PUBCOMP.
func TestCompletionLeavesHoldReleaseOwed(t *testing.T) {
	e, done, nonce := testEndedHolding(t, "release-owed", "inc-src")
	defer done()
	jsa := &e.asm.jsa
	duty := testDutyNaming(t, e, nonce)

	if !testHoldStands(t, e, "inc-src", nonce) {
		t.Fatal("the completion released its hold in its own batch: it is release-owed")
	}
	if testDutyTerminal(t, e, duty) {
		t.Fatal("the duty of a release-owed obligation stands terminal: the debt would be forgotten")
	}
	out, err := jsa.opReleaseHolds(e.id, e.epoch, testF105Gen, []string{nonce}, nil, mqttNewInflightNonce(), nil)
	if err != nil {
		t.Fatalf("releasing the owed hold: %v", err)
	}
	if len(out.cleared) != 1 || out.cleared[0] != nonce || out.resolved != 0 {
		t.Fatalf("the release reported %+v, want exactly %q cleared and nothing resolved", out, nonce)
	}
	if testHoldStands(t, e, "inc-src", nonce) {
		t.Fatal("the release-holds batch left the edge listed")
	}
	if err := jsa.opResolveDuty(&mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: testF105Gen}, duty,
		mqttNewInflightNonce()); err != nil {
		t.Fatalf("resolving the released obligation's duty: %v", err)
	}
	if !testDutyTerminal(t, e, duty) {
		t.Fatal("the duty did not resolve once its hold was released")
	}
}

// THE RELEASE IS DISCOVERED FROM THE INVENTORY, with no process memory: the recovery executor's
// resolution of the pending duty releases the hold and writes the duty terminal in ONE batch
// (opResolveDuty routes an exchange duty through opReleaseHolds).
//
// Mutation M-L0d: opResolveDuty keeps the authority-written path for an exchange duty -> refused
// while the hold is listed, and nothing ever releases it.
func TestReleaseOwedResolvedFromTheInventory(t *testing.T) {
	e, done, nonce := testEndedHolding(t, "release-discovered", "inc-src", "inc-rel")
	defer done()
	duty := testDutyNaming(t, e, nonce)
	if err := e.asm.jsa.opResolveDuty(&mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: testF105Gen}, duty,
		mqttNewInflightNonce()); err != nil {
		t.Fatalf("resolving a release-owed duty from the inventory: %v", err)
	}
	if testHoldStands(t, e, "inc-src", nonce) || testHoldStands(t, e, "inc-rel", nonce) {
		t.Fatal("the resolution left a hold listed")
	}
	if !testDutyTerminal(t, e, duty) {
		t.Fatal("the resolution did not write the duty terminal")
	}
}

// THE RELEASE-HOLDS BOUNDARY, judged on the state the batch leaves.
//
// Mutations: M-L0c (a release-holds removal accepted without a stored ending) -> "not ended"
// accepted; judge the duty on stored resources only -> "final removal" refused; judge it on the
// batch's resources only -> "another hold listed" accepted.
func TestReleaseHoldsBoundary(t *testing.T) {
	refused := func(t *testing.T, e *testF105Env, tr *mqttAuthorityTransition, want string) {
		t.Helper()
		_, err := e.asm.jsa.commitAuthorityTransition(tr)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("the batch was answered %v, want a refusal containing %q", err, want)
		}
	}
	landed := func(t *testing.T, e *testF105Env, tr *mqttAuthorityTransition) {
		t.Helper()
		if _, err := e.asm.jsa.commitAuthorityTransition(tr); err != nil {
			t.Fatalf("the batch was refused: %v", err)
		}
	}

	t.Run("not ended", func(t *testing.T) {
		e, done := testF105NewEnv(t, "release-not-ended")
		defer done()
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
		refused(t, e, testReleaseBatch(t, e, nonce, []string{"inc-src"}), "released by nothing else")
	})
	t.Run("pending drain", func(t *testing.T) {
		e, done := testF105NewEnv(t, "release-pending-drain")
		defer done()
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
		for _, to := range []mqttExchangePhase{mqttExchangePubRecObserved, mqttExchangeTerminalPendingDrain} {
			if _, err := testAdvanceLive(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, nonce, to); err != nil {
				t.Fatalf("advancing to %d: %v", to, err)
			}
		}
		refused(t, e, testReleaseBatch(t, e, nonce, []string{"inc-src"}), "released by nothing else")
	})
	t.Run("completion releasing in its own batch", func(t *testing.T) {
		e, done := testF105NewEnv(t, "release-in-completion")
		defer done()
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
		jsa := &e.asm.jsa
		x, _, _ := jsa.loadOutboundExchange(e.id, e.epoch, testF105Gen, nonce)
		// A refused PUBREC ENDS the QoS 2 obligation (Rejected): an acknowledgement ending, so
		// its hold is release-owed and a release beside it is refused.
		tr := testComposeAck(t, e, nonce, mqttExchangeRejected)
		if herr := jsa.appendHoldReleases(tr, x, mqttExchangeRejected); herr != nil {
			t.Fatalf("composing the in-batch release: %v", herr)
		}
		refused(t, e, tr, "released by nothing else")
	})
	t.Run("another hold listed", func(t *testing.T) {
		e, done, nonce := testEndedHolding(t, "release-partial-resolve", "inc-src", "inc-rel")
		defer done()
		refused(t, e, testReleaseBatch(t, e, nonce, []string{"inc-src"}, testDutyNaming(t, e, nonce)),
			"still lists obligation")
	})
	t.Run("partial release without resolution", func(t *testing.T) {
		e, done, nonce := testEndedHolding(t, "release-partial", "inc-src", "inc-rel")
		defer done()
		landed(t, e, testReleaseBatch(t, e, nonce, []string{"inc-src"}))
		if testHoldStands(t, e, "inc-src", nonce) || !testHoldStands(t, e, "inc-rel", nonce) {
			t.Fatal("the partial release did not remove exactly the source edge")
		}
	})
	t.Run("final removal resolves", func(t *testing.T) {
		e, done, nonce := testEndedHolding(t, "release-final", "inc-src", "inc-rel")
		defer done()
		landed(t, e, testReleaseBatch(t, e, nonce, []string{"inc-src"}))
		d := testDutyNaming(t, e, nonce)
		landed(t, e, testReleaseBatch(t, e, nonce, []string{"inc-rel"}, d))
		if !testDutyTerminal(t, e, d) {
			t.Fatal("the duty did not resolve beside its final removal")
		}
	})
	t.Run("resolution with no removal", func(t *testing.T) {
		e, done, nonce := testEndedHolding(t, "release-none-left", "inc-src")
		defer done()
		landed(t, e, testReleaseBatch(t, e, nonce, []string{"inc-src"}))
		d := testDutyNaming(t, e, nonce)
		landed(t, e, testReleaseBatch(t, e, nonce, nil, d))
		if !testDutyTerminal(t, e, d) {
			t.Fatal("the duty did not resolve once every hold was already gone")
		}
	})
	t.Run("an added edge", func(t *testing.T) {
		e, done, nonce := testEndedHolding(t, "release-adds", "inc-src")
		defer done()
		tr := testReleaseBatch(t, e, nonce, []string{"inc-src"})
		r, _ := mqttDecodeOutboundResource(tr.Extra[0].body, e.id, e.epoch, testF105Gen, "inc-src")
		r.Deps = append(r.Deps, mqttResourceEdge{Nonce: mqttNewInflightNonce(), Role: mqttRoleSourceReplay})
		tr.Extra[0].body, _ = json.Marshal(r)
		refused(t, e, tr, "")
	})
	t.Run("a foreign member", func(t *testing.T) {
		e, done, nonce := testEndedHolding(t, "release-foreign", "inc-src")
		defer done()
		tr := testReleaseBatch(t, e, nonce, []string{"inc-src"})
		x, xSeq, _ := e.asm.jsa.loadOutboundExchange(e.id, e.epoch, testF105Gen, nonce)
		b, _ := json.Marshal(x)
		tr.Extra = append(tr.Extra, mqttTransitionMember{subj: mqttExchangeSubject(getHash(e.id), testF105Gen, nonce),
			expect: xSeq, body: b})
		refused(t, e, tr, "neither a resource")
	})
	t.Run("an authority", func(t *testing.T) {
		e, done, nonce := testEndedHolding(t, "release-auth", "inc-src")
		defer done()
		tr := testReleaseBatch(t, e, nonce, []string{"inc-src"})
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		mqttFenceAdvanceOnAuthority(tr, al.rec, al.seq)
		refused(t, e, tr, "carries an authority")
	})
}

// testReleaseRig is one QoS 1 subscriber whose session's floor and (optionally) releaser are
// held, so only the act under test can release a hold.
type testReleaseRig struct {
	s        *Server
	sub      net.Conn
	subr     *mqttReader
	pub      net.Conn
	pubr     *mqttReader
	asm      *mqttAccountSessionManager
	sess     *mqttSession
	releaser atomic.Bool // true: the releaser is held
}

func testReleaseRigUp(t *testing.T, cid string, holdReleaser bool) *testReleaseRig {
	t.Helper()
	o := testMQTTDefaultOptions()
	g := &testReleaseRig{s: testMQTTRunServer(t, o)}
	t.Cleanup(func() { testMQTTShutdownServer(g.s) })
	g.sub, g.subr = testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { g.sub.Close() })
	testMQTTCheckConnAck(t, g.subr, mqttConnAckRCConnectionAccepted, false)
	g.asm = testMQTTGetAccountSessionManager(t, g.s, cid)
	g.asm.mu.RLock()
	g.sess = g.asm.sessions[cid]
	g.asm.mu.RUnlock()
	if g.sess == nil {
		t.Fatal("no resident session for the subscriber")
	}
	g.releaser.Store(holdReleaser)
	g.sess.mu.Lock()
	if g.sess.testCustodyDriver == nil {
		g.sess.testCustodyDriver = &mqttCustodyDriverSeams{}
	}
	g.sess.testCustodyDriver.floorHeld = func(string) bool { return true }
	g.sess.testCustodyDriver.releaseHeld = func(string) bool { return g.releaser.Load() }
	g.sess.mu.Unlock()
	testMQTTSub(t, 1, g.sub, g.subr, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, g.sub, nil, g.subr)
	g.pub, g.pubr = testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { g.pub.Close() })
	testMQTTCheckConnAck(t, g.pubr, mqttConnAckRCConnectionAccepted, false)
	return g
}

// deliverAcked publishes n QoS 1 messages and PUBACKs each as it arrives.
func (g *testReleaseRig) deliverAcked(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		testMQTTPublish(t, g.pub, g.pubr, 1, false, false, "foo", uint16(i+1), []byte("m"))
		pi := testMQTTCheckPubMsgNoAck(t, g.sub, g.subr, "foo", mqttPubQos1, []byte("m"))
		testMQTTSendPIPacket(mqttPacketPubAck, t, g.sub, pi)
	}
	testMQTTFlush(t, g.sub, nil, g.subr)
}

// source returns the subscription's source-consumer resource as it stands.
func (g *testReleaseRig) source(t *testing.T) *mqttOutboundResource {
	t.Helper()
	g.sess.mu.Lock()
	gen := g.sess.gen
	g.sess.mu.Unlock()
	subjs, _, err := g.asm.jsa.streamSubjectsComplete(mqttInflightStreamName, mqttResourceSubject(g.sess.idHash, gen, "*"))
	if err != nil {
		t.Fatalf("listing the resources: %v", err)
	}
	for subj := range subjs {
		inc := subj[strings.LastIndex(subj, ".")+1:]
		r, _, lerr := g.asm.jsa.loadOutboundResource(g.sess.id, g.asm.sessStreamEpoch, gen, inc)
		if lerr == nil && r != nil && r.Kind == mqttResourceSourceConsumer {
			return r
		}
	}
	t.Fatal("the subscription has no source-consumer resource")
	return nil
}

// THE RELEASER RELEASES WHILE THE CLIENT IS SERVED, with the floor held: acknowledged
// deliveries' holds leave the source resource within its pacing (16 owed or 200 ms), so its
// dependents list does not grow with the backlog.
//
// Mutation M-L0b: runReleaseUnit does nothing -> the edges stay listed.
func TestReleaserReleasesWhileDeliveriesContinue(t *testing.T) {
	g := testReleaseRigUp(t, "releaser-runs", false)
	g.deliverAcked(t, 20)
	deadline := time.Now().Add(5 * time.Second)
	for {
		r := g.source(t)
		if len(r.Deps) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d acknowledged deliveries still hold the source consumer with the floor held", len(r.Deps))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// AN UNSUBSCRIBE RETIRES THE SOURCE CONSUMER THROUGH RELEASE-OWED HOLDS, with the releaser and
// the floor both held: the retirement releases the ended holders it waits on itself.
//
// Mutation: the retirement step only waits on an ended holder -> the consumer is never retired.
func TestRetirementReleasesOwedHolds(t *testing.T) {
	g := testReleaseRigUp(t, "retire-owed", true)
	g.deliverAcked(t, 5)
	src := g.source(t)
	if len(src.Deps) == 0 {
		t.Fatal("fixture: the held releaser released the holds anyway, so nothing here is tested")
	}
	testMQTTUnsub(t, 2, g.sub, g.subr, []*mqttFilter{{filter: "foo"}})
	deadline := time.Now().Add(10 * time.Second)
	for {
		r, _, err := g.asm.jsa.loadOutboundResource(g.sess.id, g.asm.sessStreamEpoch, src.Gen, src.Inc)
		if err == nil && r != nil && r.State == mqttResourceCleanupComplete {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the source consumer was not retired: %+v (%v)", r, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A RELEASE OWED IN A GENERATION THAT IS THEN DISCARDED is released by the account-level
// executor, with no process memory of it: the session that completed the obligation never
// releases (no releaser runs in this environment), the generation is closed discarding by a
// Clean-Start handoff, and the recovery sweep releases the hold and resolves the duty before it
// certifies the generation.
//
// Mutation M-L0d (an exchange duty resolved on the authority path) -> the duty is refused while
// the hold is listed and the generation never certifies.
func TestReleaseOwedInADiscardedGenerationIsReleasedByTheExecutor(t *testing.T) {
	e, done, nonce := testEndedHolding(t, "release-discarded", "inc-src")
	defer done()
	if !testHoldStands(t, e, "inc-src", nonce) {
		t.Fatal("fixture: the hold is not owed")
	}
	duty := testDutyNaming(t, e, nonce)
	gen := testDiscardClose(t, e)
	if gen != testF105Gen {
		t.Fatalf("fixture: the discarded generation is %q, not the obligation's", gen)
	}
	testDiscardSweepUntilCertified(t, e, gen, func() error {
		if testHoldStands(t, e, "inc-src", nonce) {
			return errors.New("the owed hold still stands")
		}
		if !testDutyTerminal(t, e, duty) {
			return errors.New("the owed duty is not terminal")
		}
		return nil
	})
}

// TWO RELEASERS RACING on one resource: the one landing second loses its CAS and, recomposed from
// a fresh resource read inside the same call, finds nothing left to release. A RELEASE WHOSE
// RESPONSE IS LOST is settled by re-reading: the re-composition finds the edge gone and writes
// nothing.
//
// Mutation: no recomposition on a stale expectation -> the losing call surfaces the CAS loss.
func TestReleaseHoldsRacesAndLostResponses(t *testing.T) {
	t.Run("racing", func(t *testing.T) {
		e, done, nonce := testEndedHolding(t, "release-race", "inc-src")
		defer done()
		jsa := &e.asm.jsa
		var inner, raced atomic.Bool
		var outerSends atomic.Int32
		testArmTransitionSeam(t, jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if tr.Attempt.Kind != mqttTransitionReleaseHolds || inner.Load() {
				return send()
			}
			outerSends.Add(1)
			if raced.Load() {
				return send()
			}
			raced.Store(true)
			inner.Store(true)
			testReleaseOwed(t, e, nonce)
			inner.Store(false)
			return send()
		})
		out, err := jsa.opReleaseHolds(e.id, e.epoch, testF105Gen, []string{nonce}, nil, mqttNewInflightNonce(), nil)
		if !raced.Load() {
			t.Fatal("fixture: no race was staged")
		}
		if err != nil || len(out.cleared) != 1 {
			t.Fatalf("the release that lost the race answered %+v, %v: want it recomposed, nothing left "+
				"to write and the obligation cleared", out, err)
		}
		if n := outerSends.Load(); n != 1 {
			t.Fatalf("the losing release sent %d batches, want 1: its recomposition had nothing to write", n)
		}
		if testHoldStands(t, e, "inc-src", nonce) {
			t.Fatal("the hold stands after the race")
		}
	})
	t.Run("lost response", func(t *testing.T) {
		e, done, nonce := testEndedHolding(t, "release-lost", "inc-src")
		defer done()
		jsa := &e.asm.jsa
		testArmTransitionSeam(t, jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if tr.Attempt.Kind != mqttTransitionReleaseHolds {
				return send()
			}
			if _, err := send(); err != nil {
				t.Errorf("the release's send failed: %v", err)
			}
			return 0, errors.New("test: the release's response was lost")
		})
		if _, err := jsa.opReleaseHolds(e.id, e.epoch, testF105Gen, []string{nonce}, nil, mqttNewInflightNonce(), nil); err == nil {
			t.Fatal("fixture: the lost response must surface as an error")
		}
		jsa.testTransitionCommit.Store(nil)
		if testHoldStands(t, e, "inc-src", nonce) {
			t.Fatal("fixture: the release must have landed behind its lost response")
		}
		out, err := jsa.opReleaseHolds(e.id, e.epoch, testF105Gen, []string{nonce}, nil, mqttNewInflightNonce(), nil)
		if err != nil || len(out.cleared) != 1 || out.resolved != 0 {
			t.Fatalf("re-reading after the lost response answered %+v, %v", out, err)
		}
	})
}

// THE FLOOR YIELDS WITHOUT HOLDING THE DISPATCHER: while a foreground operation stays announced
// and the floor unit is due, the floor's step is refused admission and returns, and the releaser
// on the same dispatcher still releases the owed holds. Deterministic: the releaser is held until
// the floor has been refused once.
//
// Mutation: the floor waits for admission (admitBackgroundAuthority) -> the dispatcher is held for
// as long as the foreground lasts and the holds stay listed.
func TestFloorYieldDoesNotHoldTheReleaser(t *testing.T) {
	g := testReleaseRigUp(t, "floor-yield", true)
	var floorHeld atomic.Bool
	floorHeld.Store(true)
	g.sess.mu.Lock()
	g.sess.testCustodyDriver.floorHeld = func(string) bool { return floorHeld.Load() }
	g.sess.mu.Unlock()
	g.deliverAcked(t, 2*mqttFloorSlack)
	if len(g.source(t).Deps) == 0 {
		t.Fatal("fixture: the holds were released with the releaser held")
	}
	release := g.sess.announceForegroundAuthority()
	defer release()
	refused := make(chan struct{}, 1)
	wait := func() {
		select {
		case refused <- struct{}{}:
		default:
		}
	}
	g.sess.testBackgroundWait.Store(&wait)
	defer g.sess.testBackgroundWait.Store(nil)
	floorHeld.Store(false)
	g.sess.mu.Lock()
	g.sess.armFloorLocked(g.sess.gen, time.Time{})
	g.sess.mu.Unlock()
	go g.sess.startCustodyDispatcher()
	select {
	case <-refused:
	case <-time.After(10 * time.Second):
		t.Fatal("fixture: the floor never asked for admission while the foreground was announced")
	}
	g.releaser.Store(false)
	go g.sess.startCustodyDispatcher()
	deadline := time.Now().Add(10 * time.Second)
	for len(g.source(t).Deps) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d holds still listed: the releaser never ran behind the yielding floor", len(g.source(t).Deps))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A BELOW-THRESHOLD RELEASE ON A QUIESCENT SESSION RUNS: with the dispatcher idle and no tick
// armed, fewer than mqttReleaseOwedThreshold owed completions still reach the releaser within its
// delay, because the first owed completion starts the dispatcher, which arms its tick at the
// release deadline.
//
// Mutation: the first owed completion only arms the deadline -> nothing runs the releaser.
func TestBelowThresholdReleaseRunsOnAQuiescentSession(t *testing.T) {
	g := testReleaseRigUp(t, "release-quiescent", true)
	g.deliverAcked(t, 3)
	if len(g.source(t).Deps) == 0 {
		t.Fatal("fixture: the holds were released with the releaser held")
	}
	// QUIESCE: the floor has nothing to do at this span, so its hold is lifted; every owed
	// completion noted so far (a late acknowledgement included) is taken out of the set, and the
	// session is idle once no dispatcher runs and no tick is armed.
	g.sess.mu.Lock()
	g.sess.testCustodyDriver.floorHeld = nil
	gen := g.sess.gen
	g.sess.mu.Unlock()
	owed := map[string]struct{}{}
	quiet := time.Now().Add(10 * time.Second)
	for {
		g.sess.mu.Lock()
		for n := range g.sess.releaseOwed {
			owed[n] = struct{}{}
		}
		g.sess.releaseOwed, g.sess.releaseDueAt = nil, time.Time{}
		idle := !g.sess.custodyObserving && g.sess.custodyTickAt.IsZero()
		g.sess.mu.Unlock()
		if idle {
			break
		}
		if time.Now().After(quiet) {
			g.sess.mu.Lock()
			t.Fatalf("fixture: the session's dispatcher never went quiescent: observing %v, tick in %v, "+
				"floor %v, demand %v, hints %d, retry %d, withdraw %d, cont %d",
				g.sess.custodyObserving, time.Until(g.sess.custodyTickAt), g.sess.floorWanted, g.sess.custodyDemand,
				len(g.sess.custodyHints), len(g.sess.custodyRetry), len(g.sess.withdrawOwed),
				len(g.sess.admissionContinuity))
			g.sess.mu.Unlock()
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(owed) == 0 {
		t.Fatal("fixture: no completion was noted owed")
	}
	g.releaser.Store(false)
	for n := range owed {
		g.sess.noteReleaseOwed(gen, n)
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(g.source(t).Deps) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d holds still listed on a quiescent session: nothing ran the releaser", len(g.source(t).Deps))
		}
		time.Sleep(50 * time.Millisecond)
	}
}
