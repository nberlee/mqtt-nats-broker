//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// testOpRoundsEnv is an F-105 world with a serving authority and the two consumer resources a QoS 2
// obligation depends on.
func testOpRoundsEnv(t *testing.T, name string) (*testF105Env, *mqttOutboundAuthority, *testJSAObserver) {
	t.Helper()
	e, done := testF105NewEnv(t, name)
	t.Cleanup(done)
	auth, _ := e.bootstrap(t, "txn-boot")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	testF105PlantResource(t, e, "inc-rel", mqttResourcePubRelConsumer)
	return e, auth, testObserveJSA(t, &e.asm.jsa)
}

func testOpAdmitRequest(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority, pi uint16, qos byte, gen string, incs ...string) *mqttAdmitPublishRequest {
	t.Helper()
	deps := make([]mqttAdmitDependency, 0, len(incs))
	for _, inc := range incs {
		deps = append(deps, mqttAdmitDependency{Inc: inc, Role: testF105AdmissionRole(t, e, inc)})
	}
	txn := mqttNewInflightNonce()
	return &mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn, Gen: gen, Tenure: auth.Tenure,
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: pi, Dur: "$MQTT_src", Seq: 1, QoS: qos, Nonce: mqttNewInflightNonce(),
		}),
		Deps: deps,
	}
}

// An admission's composition reads go out in one round: with the authority's reply held, both
// resources and the ledger are still sent.
func TestOpAdmitPublishComposesInOneRound(t *testing.T) {
	e, auth, o := testOpRoundsEnv(t, "op-rounds-admit")
	idHash := getHash(e.id)
	authSubj := mqttOutboundAuthoritySubject(idHash)
	rest := map[string]bool{
		mqttResourceSubject(idHash, auth.Gen, "inc-src"): true,
		mqttResourceSubject(idHash, auth.Gen, "inc-rel"): true,
		mqttGenLedgerSubject(idHash, auth.Gen):           true,
	}
	isRest := func(s string) bool { return rest[s] }
	r := testOpAdmitRequest(t, e, auth, 7, 2, auth.Gen, "inc-src", "inc-rel")
	h := o.hold(testSubjectIs(authSubj))
	m := o.mark()
	done := make(chan error, 1)
	go func() {
		_, err := e.asm.jsa.opAdmitPublish(r)
		done <- err
	}()
	if !o.waitSends(m, 3, isRest, 2*time.Second) {
		t.Fatalf("with the authority's reply held the admission sent %d of its other 3 composition reads: serial",
			o.sends(m, isRest))
	}
	o.release(h, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the admission was refused after its round was released: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the admission did not complete after its round was released")
	}
}

// An acknowledgement advance reads its authority and exchange in one round, and the resources its
// ending releases in a second: with two holds (a refused PUBREC releases both the source and the
// canonical PUBREL hold), both are sent while the first is held. A QoS 1 PUBACK releases one hold
// and reads one resource.
func TestOpAdvanceExchangeAckComposesInRounds(t *testing.T) {
	e, auth, o := testOpRoundsEnv(t, "op-rounds-advance")
	idHash := getHash(e.id)
	nonce, _, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 2, "inc-src", "inc-rel")
	authSubj := mqttOutboundAuthoritySubject(idHash)
	xSubj := mqttExchangeSubject(idHash, auth.Gen, nonce)
	res := map[string]bool{
		mqttResourceSubject(idHash, auth.Gen, "inc-src"): true,
		mqttResourceSubject(idHash, auth.Gen, "inc-rel"): true,
	}
	isRes := func(s string) bool { return res[s] }

	h1 := o.hold(testSubjectIs(authSubj))
	m := o.mark()
	done := make(chan error, 1)
	go func() {
		_, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
			ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
			Gen: auth.Gen, Nonce: nonce, To: mqttExchangeRejected, Acq: reg.Acq,
			Register: reg, RegisterSeq: seq,
		})
		done <- err
	}()
	if !o.waitSends(m, 1, testSubjectIs(xSubj), 2*time.Second) {
		t.Fatal("with the authority's reply held the advance never read its exchange: serial")
	}
	h2 := o.hold(isRes)
	o.release(h1, nil)
	if !o.waitSends(m, 2, isRes, 2*time.Second) {
		t.Fatalf("with the first held resource's reply held the advance sent %d of its 2 resource reads: serial",
			o.sends(m, isRes))
	}
	o.release(h2, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the refused-PUBREC advance failed after its rounds were released: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the advance did not complete after its rounds were released")
	}

	// QoS 1: one hold, read alone (the write boundary's own resource check is a different read).
	nonce1, _, _, _ := testF105AdmitQoS(t, e, "txn-adm-1", 8, 1, "inc-src")
	x1, _ := testF105LoadExchange(t, e, auth.Gen, nonce1)
	if len(x1.Holds) != 1 {
		t.Fatalf("fixture: a QoS 1 exchange holds %d resources, want 1", len(x1.Holds))
	}
	m1 := o.mark()
	loaded := e.asm.jsa.loadHeldResources(&mqttAuthorityTransition{}, x1)
	if n := o.sends(m1, isRes); n != 1 || len(loaded) != 1 {
		t.Fatalf("a QoS 1 ending read %d held resources (%d loaded), want 1", n, len(loaded))
	}
}

// An admission decided under a generation the authority no longer serves is refused as exactly
// that, before anything it read for that generation is judged.
func TestOpAdmitPublishRefusesMovedAuthority(t *testing.T) {
	e, auth, _ := testOpRoundsEnv(t, "op-rounds-moved")
	r := testOpAdmitRequest(t, e, auth, 7, 1, mqttGenSessEpoch(), "inc-src")
	_, err := e.asm.jsa.opAdmitPublish(r)
	if err == nil || !strings.Contains(err.Error(), "entitlement that has moved on") {
		t.Fatalf("an admission under a moved generation was not refused as one: %v", err)
	}
}

// The shared-dependency check is judged before the resources, as it ran before they were read: a
// shared dependency supplied for a non-shared source is refused as that, even when the round's
// resource read went unanswered.
func TestOpAdmitPublishSharedRefusalPrecedesResourceErrors(t *testing.T) {
	e, auth, _ := testOpRoundsEnv(t, "op-rounds-shared")
	r := testOpAdmitRequest(t, e, auth, 7, 1, auth.Gen, "inc-src")
	r.Shared = &mqttAdmitShared{}
	resSubj := mqttResourceSubject(getHash(e.id), auth.Gen, "inc-src")
	drop := func(subject string, body []byte) bool {
		var req JSApiMsgGetRequest
		return json.Unmarshal(body, &req) == nil && req.LastFor == resSubj
	}
	e.asm.jsa.testDropRequest.Store(&drop)
	t.Cleanup(func() { e.asm.jsa.testDropRequest.Store(nil) })
	_, err := e.asm.jsa.opAdmitPublish(r)
	if err == nil || !strings.Contains(err.Error(), "a shared dependency was supplied for a non-shared source") {
		t.Fatalf("the shared refusal did not win over the unanswered resource read: %v", err)
	}
}

// What a refused operation has read, by its rounds: an admission under a moved generation has
// read its whole composition round and nothing after; an advance whose first held resource is gone
// has read both held resources in its second round and is refused.
func TestOpRefusalCosts(t *testing.T) {
	e, auth, o := testOpRoundsEnv(t, "op-rounds-costs")
	idHash := getHash(e.id)
	moved := mqttGenSessEpoch()
	r := testOpAdmitRequest(t, e, auth, 7, 2, moved, "inc-src", "inc-rel")
	round := map[string]bool{
		mqttOutboundAuthoritySubject(idHash):          true,
		mqttResourceSubject(idHash, moved, "inc-src"): true,
		mqttResourceSubject(idHash, moved, "inc-rel"): true,
		mqttGenLedgerSubject(idHash, moved):           true,
	}
	f105 := testSubjectHasPrefix(mqttF105SubjectPrefix())
	m := o.mark()
	if _, err := e.asm.jsa.opAdmitPublish(r); err == nil {
		t.Fatal("an admission under a moved generation must be refused")
	}
	if n := o.sends(m, func(s string) bool { return round[s] }); n != 4 {
		t.Fatalf("the refused admission read %d of its composition round, want all 4", n)
	}
	if n := o.sends(m, f105); n != 4 {
		t.Fatalf("the refused admission made %d F-105 reads, want only its round's 4: %v", n, o.sentSubjects(m, f105))
	}

	nonce, _, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 9, 2, "inc-src", "inc-rel")
	src := mqttResourceSubject(idHash, auth.Gen, "inc-src")
	smsg, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, src)
	if lerr != nil {
		t.Fatalf("fixture: reading the source resource: %v", lerr)
	}
	if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true); err != nil {
		t.Fatalf("fixture: deleting the source resource: %v", err)
	}
	isRes := func(s string) bool { return s == src || s == mqttResourceSubject(idHash, auth.Gen, "inc-rel") }
	m2 := o.mark()
	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
		Gen: auth.Gen, Nonce: nonce, To: mqttExchangeRejected, Acq: reg.Acq,
		Register: reg, RegisterSeq: seq,
	}); err == nil {
		t.Fatal("an ending over a held resource that does not stand must be refused")
	}
	if n := o.sends(m2, isRes); n != 2 {
		t.Fatalf("the refused ending read %d held resources, want both: its second round", n)
	}
}

// An acquisition's authority and exchange go out in one round.
func TestOpAcquireActionComposesInOneRound(t *testing.T) {
	e, subject, tok, pi := testAcquireEnv(t, "op-rounds-acquire")
	cur, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil {
		t.Fatalf("fixture: the admitted slot: %v", err)
	}
	o := testObserveJSA(t, &e.asm.jsa)
	authSubj := mqttOutboundAuthoritySubject(e.sess.idHash)
	xSubj := mqttExchangeSubject(e.sess.idHash, testGenOf(e), cur.Lin)
	h := o.hold(testSubjectIs(authSubj))
	m := o.mark()
	done := make(chan bool, 1)
	go func() { done <- e.sess.acquirePublishAction(e.c, pi, &tok) }()
	if !o.waitSends(m, 1, testSubjectIs(xSubj), 2*time.Second) {
		t.Fatal("with the authority's reply held the acquisition never read its exchange: serial")
	}
	o.release(h, nil)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("the acquisition was refused after its round was released")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the acquisition did not complete after its round was released")
	}
}
