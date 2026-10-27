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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// F-105 7c-lc: the retirement owner's source step, and the routes that hand it decisions.

// testClaimedSourceConsumer claims a fresh source name through the production claim and creates
// its physical consumer, returning the claim.
func testClaimedSourceConsumer(t *testing.T, e *testInflightEnv, subject string) *mqttSourceClaim {
	t.Helper()
	name := e.sess.idHash + "_" + nuid.Next()
	cl, err := e.sess.claimSourceConsumerName(testServingEntitlement(t, e.sess), subject, name)
	if err != nil {
		t.Fatalf("claiming %q: %v", name, err)
	}
	if _, cerr := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName,
		Config: ConsumerConfig{Durable: name, DeliverSubject: mqttSubPrefix + nuid.Next(),
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, FilterSubject: mqttStreamSubjectPrefix + subject}}); cerr != nil {
		t.Fatalf("creating %q: %v", name, cerr)
	}
	return cl
}

func testRetireItem(cl *mqttSourceClaim) *mqttSourceRetirement {
	return &mqttSourceRetirement{gen: cl.gen, name: cl.name, inc: cl.inc,
		reason: mqttDisposalUnsubscribed, ent: cl.ent, txn: mqttNewInflightNonce()}
}

// testStepUntil drives retireSourceStep until it answers Done or the bound runs out.
func testStepUntil(t *testing.T, sess *mqttSession, it *mqttSourceRetirement, bound int) (mqttReclaimStep, error) {
	t.Helper()
	var step mqttReclaimStep
	var err error
	for i := 0; i < bound; i++ {
		step, err = sess.retireSourceStep(it)
		if step == mqttReclaimDone {
			return step, err
		}
	}
	return step, err
}

func testConsumerPresent(t *testing.T, e *testInflightEnv, name string) bool {
	t.Helper()
	ci, err := e.asm.jsa.lookupConsumer(mqttStreamName, name)
	if err != nil && isErrorOtherThan(err, JSConsumerNotFoundErr) {
		t.Fatalf("looking up %q: %v", name, err)
	}
	return ci != nil
}

// testRequireSourceRetired asserts the whole retirement: the name RETIRED for its owner, the
// resource CleanupComplete with a disposal decision, both duties terminal, the consumer gone.
func testRequireSourceRetired(t *testing.T, e *testInflightEnv, gen, inc, name string) {
	t.Helper()
	jsa := &e.asm.jsa
	own, _, oerr := jsa.loadNameOwnershipPhysical(e.asm.sessStreamEpoch, mqttOwnedStreamMsgs, name)
	if oerr != nil || own == nil || own.State != mqttNameOwnRetired || own.OwnerInc != inc {
		t.Fatalf("name %q is not RETIRED for %q: %+v (%v)", name, inc, own, oerr)
	}
	r, _, rerr := jsa.loadOutboundResource(e.sess.id, e.asm.sessStreamEpoch, gen, inc)
	if rerr != nil || r == nil || r.State != mqttResourceCleanupComplete || r.Disposal == nil {
		t.Fatalf("resource %q is not CleanupComplete with its decision: %+v (%v)", inc, r, rerr)
	}
	for _, kind := range []mqttDutyKind{mqttDutyProvision, mqttDutyCleanup} {
		ord, err := jsa.resourceDutyOrdinal(e.sess.id, e.asm.sessStreamEpoch, gen, inc, kind)
		if err != nil {
			t.Fatalf("the %s duty of %q: %v", kind, inc, err)
		}
		d, _, derr := jsa.loadDutyEntryAt(e.sess.id, e.asm.sessStreamEpoch, gen, ord)
		if derr != nil || d == nil || d.State != mqttDutyTerminal {
			t.Fatalf("the %s duty of %q is not terminal: %+v (%v)", kind, inc, d, derr)
		}
	}
	if testConsumerPresent(t, e, name) {
		t.Fatalf("the retired consumer %q still exists", name)
	}
}

// T8: an AMBIGUOUS delete records nothing; a delete whose response was lost repeats harmlessly.
func TestF105RetirementCrashWindows(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "retire-crash")
	cl := testClaimedSourceConsumer(t, e, "retire.crash")
	it := testRetireItem(cl)

	// The delete is answered ambiguously, after really landing: a lost response.
	var landed bool
	hook := func(stream, name string) error {
		if !landed {
			landed = true
			_, _ = e.asm.jsa.deleteConsumer(stream, name, false)
			return errors.New("test: the delete's response was lost")
		}
		_, err := e.asm.jsa.deleteConsumer(stream, name, false)
		return err
	}
	e.sess.testRetiredConsumerDelete.Store(&hook)
	defer e.sess.testRetiredConsumerDelete.Store(nil)

	// Disposal, authorization, then the ambiguous delete: the step must stop there.
	for i := 0; i < 6; i++ {
		if step, _ := e.sess.retireSourceStep(it); step == mqttReclaimWithheld && landed {
			break
		}
	}
	if !landed {
		t.Fatal("fixture: the delete was never reached")
	}
	r, _, _ := e.asm.jsa.loadOutboundResource(e.sess.id, e.asm.sessStreamEpoch, cl.gen, cl.inc)
	if r == nil || r.State != mqttResourceDeleteAuthorized {
		t.Fatalf("after an ambiguous delete the resource must stand DELETE_AUTHORIZED, got %+v", r)
	}
	ord, _ := e.asm.jsa.resourceDutyOrdinal(e.sess.id, e.asm.sessStreamEpoch, cl.gen, cl.inc, mqttDutyCleanup)
	if d, _, _ := e.asm.jsa.loadDutyEntryAt(e.sess.id, e.asm.sessStreamEpoch, cl.gen, ord); d == nil || d.State == mqttDutyTerminal {
		t.Fatalf("an ambiguous delete resolved the cleanup duty: %+v", d)
	}
	// The repeat finds the consumer gone, classifies it, and finishes without a second authorization.
	if step, err := testStepUntil(t, e.sess, it, 6); step != mqttReclaimDone {
		t.Fatalf("the retirement did not finish after the repeat: %v %v", step, err)
	}
	r2, _, _ := e.asm.jsa.loadOutboundResource(e.sess.id, e.asm.sessStreamEpoch, cl.gen, cl.inc)
	if r2.AuthTxn != r.AuthTxn || r2.Outcome != mqttCleanupGone {
		t.Fatalf("the completion re-authorized or misclassified: %+v (authorized by %q)", r2, r.AuthTxn)
	}
	testRequireSourceRetired(t, e, cl.gen, cl.inc, cl.name)
}

// T9: a delayed CREATE resurrecting a RETIRED name is swept, and nothing can adopt it.
func TestF105LateCreateAfterCleanupIsSwept(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "retire-late")
	cl := testClaimedSourceConsumer(t, e, "retire.late")
	if step, err := testStepUntil(t, e.sess, testRetireItem(cl), 10); step != mqttReclaimDone {
		t.Fatalf("the retirement did not finish: %v %v", step, err)
	}
	testRequireSourceRetired(t, e, cl.gen, cl.inc, cl.name)

	// The late CREATE lands under the retired name.
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName,
		Config: ConsumerConfig{Durable: cl.name, DeliverSubject: mqttSubPrefix + nuid.Next(),
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, FilterSubject: mqttStreamSubjectPrefix + "retire.late"}}); err != nil {
		t.Fatalf("the resurrection: %v", err)
	}
	// Nothing may depend on it.
	if err := testReadyForUse(&e.asm.jsa, e.sess.id, e.asm.sessStreamEpoch, cl.gen, cl.inc); err == nil {
		t.Fatal("a resurrected consumer under a RETIRED name was reported ready for use")
	}
	// And the plain sweep reclaims it.
	e.asm.sweepOrphanedConsumersBefore(e.s, time.Now().Add(time.Minute))
	if testConsumerPresent(t, e, cl.name) {
		t.Fatal("the plain sweep left a resurrected consumer whose name is RETIRED")
	}
}

// T10: a route naming a consumer with no ownership of this generation deletes NOTHING.
func TestF105RouteWithholdsWithoutOwnership(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "retire-noown")
	name := e.sess.idHash + "_" + nuid.Next()
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName,
		Config: ConsumerConfig{Durable: name, DeliverSubject: mqttSubPrefix + nuid.Next(),
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, FilterSubject: mqttStreamSubjectPrefix + "retire.noown"}}); err != nil {
		t.Fatal(err)
	}
	ent, _ := e.c.outboundEntitlement()
	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	it := &mqttSourceRetirement{gen: gen, name: name, reason: mqttDisposalUnsubscribed, ent: ent, txn: mqttNewInflightNonce()}
	if step, _ := testStepUntil(t, e.sess, it, 4); step != mqttReclaimDone {
		t.Fatalf("an unowned consumer's retirement did not end: %v", step)
	}
	if !testConsumerPresent(t, e, name) {
		t.Fatal("a consumer with no ownership record was deleted: a missing owner is never permission")
	}
}

// T7c: a disposal that loses the authority CAS, or whose response is lost, is re-attempted and lands
// once.
func TestF105DisposalRequestContention(t *testing.T) {
	t.Parallel()
	for _, lost := range []bool{false, true} {
		e := testNewInflightEnv(t, fmt.Sprintf("retire-cont-%v", lost))
		cl := testClaimedSourceConsumer(t, e, "retire.cont")
		it := testRetireItem(cl)
		var fired bool
		hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if !fired && strings.HasPrefix(tr.Attempt.Op, "op-dispose-") {
				fired = true
				if lost {
					if _, err := send(); err != nil {
						t.Errorf("the lost-response send itself failed: %v", err)
					}
					return 0, errors.New("test: the disposal's response was lost")
				}
				return 0, errors.New("test: another op won the authority CAS")
			}
			return send()
		}
		e.asm.jsa.testTransitionCommit.Store(&hook)
		if step, _ := e.sess.retireSourceStep(it); step != mqttReclaimWithheld || it.disposed {
			t.Fatalf("lost=%v: a failed disposal attempt was taken as landed (%v, disposed=%v)", lost, step, it.disposed)
		}
		if step, err := testStepUntil(t, e.sess, it, 10); step != mqttReclaimDone {
			t.Fatalf("lost=%v: the retirement did not finish after the re-attempt: %v %v", lost, step, err)
		}
		e.asm.jsa.testTransitionCommit.Store(nil)
		r, _, _ := e.asm.jsa.loadOutboundResource(e.sess.id, e.asm.sessStreamEpoch, cl.gen, cl.inc)
		if lost && r.Disposal.Txn != it.txn {
			t.Fatalf("the lost-response decision was re-made under another transaction: %+v", r.Disposal)
		}
		testRequireSourceRetired(t, e, cl.gen, cl.inc, cl.name)
	}
}

// T7d: a claim whose attempts all "failed" may still have landed — every handle is arbitrated.
func TestF105ClaimLandsAfterTimeout(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "retire-claim")
	// RESIDENT, as a CONNECTed session is: the retirement owner stops for a session object that
	// is not the client ID's resident one.
	e.asm.mu.Lock()
	e.asm.sessions[e.sess.id] = e.sess
	e.asm.mu.Unlock()
	name := e.sess.idHash + "_" + nuid.Next()
	var mu sync.Mutex
	attempts := 0
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Op != "op-provision-source" {
			return send()
		}
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		if n == 2 {
			// THIS one lands, and its response is lost.
			if _, err := send(); err != nil {
				t.Errorf("the landing claim's send failed: %v", err)
			}
			return 0, errors.New("test: the claim's response was lost")
		}
		// Every other attempt is never sent: it fails like a timeout before the store saw it.
		return 0, errors.New("test: the claim timed out")
	}
	e.asm.jsa.testTransitionCommit.Store(&hook)
	var site string
	siteHook := func(s, _ string) { site = s }
	e.sess.testSourceDisposalSite.Store(&siteHook)
	if _, err := e.sess.claimSourceConsumerName(testServingEntitlement(t, e.sess), "retire.claim", name); err == nil {
		t.Fatal("fixture: every attempt answered failure, so the claim must report failure")
	}
	e.asm.jsa.testTransitionCommit.Store(nil)
	if site != "claim-failed" {
		t.Fatalf("the failed claim was not taken into custody (site %q)", site)
	}
	// An ownership read says LIVE (attempt 2 landed): the negative-read shortcut is not what
	// decides; the owner's arbitration of every handle is.
	own, _, _ := e.asm.jsa.loadNameOwnershipPhysical(e.asm.sessStreamEpoch, mqttOwnedStreamMsgs, name)
	if own == nil || own.State != mqttNameOwnLive {
		t.Fatalf("fixture: the landed claim must stand LIVE, got %+v", own)
	}
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		own, _, _ := e.asm.jsa.loadNameOwnershipPhysical(e.asm.sessStreamEpoch, mqttOwnedStreamMsgs, name)
		if own == nil || own.State != mqttNameOwnRetired {
			return fmt.Errorf("the landed claim's name is not retired yet: %+v", own)
		}
		return nil
	})
	// The never-sent attempts were aborted: their outcome subjects are taken.
	if attempts < 3 {
		t.Fatalf("fixture: expected several submitted attempts, got %d", attempts)
	}
}

// T13: readiness refuses a consumer whose EFFECTIVE configuration carries an inactivity threshold.
func TestF105ReadinessRefusesEffectiveThreshold(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "ready-thr")
	cl := testClaimedSourceConsumer(t, e, "ready.thr")
	if _, ok := e.sess.readyDependencyInc(e.c, mqttOwnedStreamMsgs, mqttStreamName, cl.name, e.asm.sessStreamEpoch, cl.gen); !ok {
		t.Fatal("fixture: a safely configured consumer must be ready")
	}
	ci, err := e.asm.jsa.lookupConsumer(mqttStreamName, cl.name)
	if err != nil || ci == nil {
		t.Fatal(err)
	}
	cfg := *ci.Config
	cfg.InactiveThreshold = time.Hour
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName, Config: cfg}); err != nil {
		t.Fatalf("imposing a threshold out of band: %v", err)
	}
	if _, ok := e.sess.readyDependencyInc(e.c, mqttOwnedStreamMsgs, mqttStreamName, cl.name, e.asm.sessStreamEpoch, cl.gen); ok {
		t.Fatal("a consumer carrying an effective inactivity threshold was admitted against")
	}
}

// T14: the PUBREL ensure refuses when the session's generation and the serving authority's differ.
func TestF105PubRelGenerationMismatchRefused(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "pubrel-gen")
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	testConnIdentity(t, e, e.c)
	// Accepted under the serving authority, so the claim reaches the generation rule rather than
	// refusing a connection with no entitlement at all.
	testStampAcceptedConnection(t, e)
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.gen = mqttNewSessGen()
	e.sess.mu.Unlock()
	err := e.sess.ensurePubRelConsumerSubscription(e.c)
	if err == nil || !strings.Contains(err.Error(), "belongs to generation") {
		t.Fatalf("a PUBREL ensure across generations was not refused: %v", err)
	}
}

// T6 / T7, end to end: an UNSUBSCRIBE and a QoS 0 downgrade with nothing outstanding retire the
// consumer through the owner — the route was reached, and the whole retirement lands.
func TestF105RoutesRetireTheSource(t *testing.T) {
	for _, route := range []string{"unsubscribed", "downgraded"} {
		t.Run(route, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)
			id := "route-" + route
			c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id}, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "route/topic", qos: 1}}, []byte{1})
			testMQTTFlush(t, c, nil, r)
			sess := testMQTTResidentSession(t, s, id)
			sess.mu.Lock()
			var dur string
			for _, cc := range sess.cons {
				dur = cc.Durable
			}
			gen := sess.gen
			sess.mu.Unlock()
			var hit string
			siteHook := func(site, name string) {
				if name == dur {
					hit = site
				}
			}
			sess.testSourceDisposalSite.Store(&siteHook)
			if route == "unsubscribed" {
				testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "route/topic"}})
			} else {
				testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "route/topic", qos: 0}}, []byte{0})
			}
			testMQTTFlush(t, c, nil, r)
			checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
				own, _, _ := sess.jsa.loadNameOwnershipPhysical(sess.asm.sessStreamEpoch, mqttOwnedStreamMsgs, dur)
				if own == nil || own.State != mqttNameOwnRetired {
					return fmt.Errorf("%q not retired yet: %+v", dur, own)
				}
				if ci, _ := sess.jsa.lookupConsumer(mqttStreamName, dur); ci != nil {
					return fmt.Errorf("%q still exists", dur)
				}
				res, _, _ := sess.jsa.loadOutboundResource(sess.id, sess.asm.sessStreamEpoch, gen, own.OwnerInc)
				if res == nil || res.State != mqttResourceCleanupComplete {
					return fmt.Errorf("%q's cleanup is not complete yet: %+v", dur, res)
				}
				return nil
			})
			if hit != "deferred-delete:"+route {
				t.Fatalf("the %s route was not reached through its site (hit %q)", route, hit)
			}
			own, _, _ := sess.jsa.loadNameOwnershipPhysical(sess.asm.sessStreamEpoch, mqttOwnedStreamMsgs, dur)
			res, _, _ := sess.jsa.loadOutboundResource(sess.id, sess.asm.sessStreamEpoch, gen, own.OwnerInc)
			if res == nil || res.State != mqttResourceCleanupComplete || res.Disposal == nil || res.Disposal.Reason != route {
				t.Fatalf("the resource does not record the %s retirement: %+v", route, res)
			}
		})
	}
}

// T7: a fresh consumer whose CREATE fails after its claim is disposed of, and a SUBSCRIBE whose
// local subscription fails after its CREATE too.
func TestF105FailedProvisioningIsDisposed(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "prov-fail"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	sess := testMQTTResidentSession(t, s, "prov-fail")
	var mu sync.Mutex
	var failed string
	sess.asm.mu.Lock()
	sess.asm.testSourcePreCreate = func(_ *mqttSession, durable string) error {
		mu.Lock()
		failed = durable
		mu.Unlock()
		return errors.New("test: the source create failed")
	}
	sess.asm.mu.Unlock()
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "prov/topic", qos: 1}}, []byte{mqttSubAckFailure})
	sess.asm.mu.Lock()
	sess.asm.testSourcePreCreate = nil
	sess.asm.mu.Unlock()
	mu.Lock()
	name := failed
	mu.Unlock()
	if name == _EMPTY_ {
		t.Fatal("fixture: the create was never attempted")
	}
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		own, _, _ := sess.jsa.loadNameOwnershipPhysical(sess.asm.sessStreamEpoch, mqttOwnedStreamMsgs, name)
		if own == nil || own.State != mqttNameOwnRetired {
			return fmt.Errorf("the claimed name %q of a failed create is not retired: %+v", name, own)
		}
		return nil
	})
}

// testPlantPubRelOwnership plants a PUBREL name's ownership record in the given state.
func testPlantPubRelOwnership(t *testing.T, e *testOrphanEnv, name, gen string, state mqttNameOwnState) {
	t.Helper()
	o := &mqttNameOwnership{ID: "t12-owner", Epoch: e.asm.sessStreamEpoch, Stream: mqttOwnedStreamOut,
		Name: name, State: state, OwnerGen: gen, OwnerInc: "inc-" + gen}
	b, _ := json.Marshal(o)
	msg, hdr := mqttFrameBatchWrite(0, b, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttNameOwnershipSubject(mqttOwnedStreamOut, name), hdr, msg); err != nil {
		t.Fatalf("planting the ownership of %q: %v", name, err)
	}
}

// T12 (consult 14 finding 2): a RETIRED PUBREL name is reclaimed with NO retirement watermark — the
// single-slot proof a later generation overwrites, and a restart forgets — while a LIVE one is kept.
func TestF105RetiredPubRelSweptWithoutTheWatermark(t *testing.T) {
	e := testNewOrphanEnv(t, "t12-pubrel")
	e.setGraces(t, 0, 0)
	idHash := getHash("t12-pubrel-client")
	g1, g2 := mqttNewSessGen(), mqttNewSessGen()
	retired := testCreatePubRelConsumer(t, e, idHash, g1) // G1's, resurrected after its retirement
	live := testCreatePubRelConsumer(t, e, idHash, g2)
	testPlantPubRelOwnership(t, e, retired, g1, mqttNameOwnRetired)
	testPlantPubRelOwnership(t, e, live, g2, mqttNameOwnLive)
	// No watermark names G1 (nothing was ever written): the restarted broker's state.
	for i := 0; i < 3; i++ {
		e.sweepPubRelConsumers(t)
	}
	if ci, _ := e.asm.jsa.lookupConsumer(mqttOutStreamName, retired); ci != nil {
		t.Fatalf("the RETIRED PUBREL name %q was not reclaimed without its watermark", retired)
	}
	if ci, _ := e.asm.jsa.lookupConsumer(mqttOutStreamName, live); ci == nil {
		t.Fatalf("the LIVE PUBREL name %q was deleted", live)
	}
}

// T2b: an acknowledgement ending whose response is lost leaves the exchange ended and its hold
// release-owed, with the pending duty that records the debt: never an ended obligation whose
// release nothing durable remembers.
func TestF105EndingLostResponseLeavesTheReleaseOwed(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-holds-lost")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, _, _, _ := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Op == "op-ack" {
			if _, err := send(); err != nil {
				t.Errorf("the ending's send failed: %v", err)
			}
			return 0, errors.New("test: the ending's response was lost")
		}
		return send()
	}
	e.asm.jsa.testTransitionCommit.Store(&hook)
	_, err := testAdvanceLive(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, nonce, mqttExchangeTerminal)
	e.asm.jsa.testTransitionCommit.Store(nil)
	if err == nil {
		t.Fatal("fixture: the lost response must surface as an error")
	}
	x, _ := testF105LoadExchange(t, e, testF105Gen, nonce)
	if x.Phase != mqttExchangeTerminal {
		t.Fatalf("fixture: the ending must have landed, phase %d", x.Phase)
	}
	// RELEASE-OWED, and durably so: the hold stands and the duty that records the debt is still
	// pending, so the inventory's resolution releases it with no memory of the lost response.
	testRequireHolds(t, &e.asm.jsa, x, true)
	duty := testDutyNaming(t, e, nonce)
	if testDutyTerminal(t, e, duty) {
		t.Fatal("the duty of a release-owed obligation stands terminal")
	}
	if err := e.asm.jsa.opResolveDuty(&mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: testF105Gen}, duty,
		mqttNewInflightNonce()); err != nil {
		t.Fatalf("resolving the owed duty: %v", err)
	}
	testRequireHolds(t, &e.asm.jsa, x, false)
}

// T7, the drain route: a drained delivery's PUBACK clears the drain's last claim, the drain retires,
// and its consumer is retired through the owner — the route reached, the retirement whole.
func TestF105DrainRetirementRetiresTheSource(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	conn, sess, pi := testDrainSetup(t, s, o, "drain-retire", "drainretire/topic", 1)
	defer conn.c.Close()
	sess.mu.Lock()
	var dur string
	for _, cc := range sess.cons {
		dur = cc.Durable
	}
	gen := sess.gen
	sess.mu.Unlock()
	var hit string
	siteHook := func(site, name string) {
		if name == dur {
			hit = site
		}
	}
	sess.testSourceDisposalSite.Store(&siteHook)
	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "drainretire/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("fixture: expected a drain, got %d", n)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, pi)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		own, _, _ := sess.jsa.loadNameOwnershipPhysical(sess.asm.sessStreamEpoch, mqttOwnedStreamMsgs, dur)
		if own == nil || own.State != mqttNameOwnRetired {
			return fmt.Errorf("%q not retired yet", dur)
		}
		res, _, _ := sess.jsa.loadOutboundResource(sess.id, sess.asm.sessStreamEpoch, gen, own.OwnerInc)
		if res == nil || res.State != mqttResourceCleanupComplete {
			return fmt.Errorf("%q cleanup not complete", dur)
		}
		if ci, _ := sess.jsa.lookupConsumer(mqttStreamName, dur); ci != nil {
			return fmt.Errorf("%q still exists", dur)
		}
		return nil
	})
	if hit != "drain-retired" {
		t.Fatalf("the drain route was not reached through its site (hit %q)", hit)
	}
}

// T7d-2: a claim attempt that has NOT landed when custody starts — an ownership read answers absent
// — is aborted by the arbitration, so its delayed batch can never land afterwards. A custody that
// trusted the negative read (M20) would leave the late claim to land unowned.
func TestF105DelayedClaimIsAbortedNotTrusted(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "retire-delayed")
	e.asm.mu.Lock()
	e.asm.sessions[e.sess.id] = e.sess
	e.asm.mu.Unlock()
	name := e.sess.idHash + "_" + nuid.Next()
	var mu sync.Mutex
	var held []*mqttAuthorityTransition
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Op != "op-provision-source" {
			return send()
		}
		// Queued, not sent: the store has not seen it yet.
		mu.Lock()
		held = append(held, tr)
		mu.Unlock()
		return 0, errors.New("test: the claim is still queued")
	}
	e.asm.jsa.testTransitionCommit.Store(&hook)
	if _, err := e.sess.claimSourceConsumerName(testServingEntitlement(t, e.sess), "retire.delayed", name); err == nil {
		t.Fatal("fixture: the queued claim must report failure")
	}
	e.asm.jsa.testTransitionCommit.Store(nil)
	if own, _, _ := e.asm.jsa.loadNameOwnershipPhysical(e.asm.sessStreamEpoch, mqttOwnedStreamMsgs, name); own != nil {
		t.Fatalf("fixture: nothing may have landed yet, got %+v", own)
	}
	// The owner settles the custody...
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		if e.sess.retirementPendingFor(name) {
			return fmt.Errorf("the claim's custody is still open")
		}
		return nil
	})
	// ...and then the queued batches arrive at the store. Every one must be refused: its outcome
	// subject already carries the abort.
	mu.Lock()
	queued := append([]*mqttAuthorityTransition(nil), held...)
	mu.Unlock()
	if len(queued) == 0 {
		t.Fatal("fixture: no claim attempt was queued")
	}
	for _, tr := range queued {
		if _, err := e.asm.jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a delayed claim landed after its custody was settled on a negative read")
		}
	}
	if own, _, _ := e.asm.jsa.loadNameOwnershipPhysical(e.asm.sessStreamEpoch, mqttOwnedStreamMsgs, name); own != nil {
		t.Fatalf("a delayed claim's name stands: %+v", own)
	}
}

// The source retirement is a step of the reclaimer's owner, so it passes the foreground-priority
// handshake (consults 105, 112): an attempt it has NOT yet been admitted for WAITS at the gate while
// a foreground authority operation is announced — observed at the gate, not inferred from timing —
// and proceeds once it is released. (An attempt admitted just before an announcement may still
// commit after it; that is the protocol's stated allowance and is not this test's claim.)
func TestF105SourceRetirementWaitsForTheForeground(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "retire-fg")
	e.asm.mu.Lock()
	e.asm.sessions[e.sess.id] = e.sess
	e.asm.mu.Unlock()
	cl := testClaimedSourceConsumer(t, e, "retire.fg")
	var steps atomic.Int64
	stepHook := func(k mqttRetainedReclaimKey, _ mqttReclaimStep, _ error) {
		if k.src {
			steps.Add(1)
		}
	}
	e.sess.testReclaimAfterStep.Store(&stepHook)
	waited := make(chan struct{}, 1)
	waitHook := func() {
		select {
		case waited <- struct{}{}:
		default:
		}
	}
	e.sess.testBackgroundWait.Store(&waitHook)
	release := e.sess.announceForegroundAuthority()
	e.sess.handSourceDisposal(cl.gen, cl.name, cl.inc, mqttDisposalUnsubscribed, cl.ent, "test")
	select {
	case <-waited:
	case <-time.After(10 * time.Second):
		release()
		t.Fatal("the retirement owner never waited at the gate while the foreground was announced")
	}
	if n := steps.Load(); n != 0 {
		release()
		t.Fatalf("%d retirement step(s) ran while the owner was waiting at the gate", n)
	}
	release()
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		own, _, _ := e.asm.jsa.loadNameOwnershipPhysical(e.asm.sessStreamEpoch, mqttOwnedStreamMsgs, cl.name)
		if own == nil || own.State != mqttNameOwnRetired {
			return fmt.Errorf("not retired after the release")
		}
		return nil
	})
}
