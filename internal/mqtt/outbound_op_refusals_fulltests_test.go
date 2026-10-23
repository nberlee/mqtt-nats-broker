//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"strings"
	"testing"
)

// testOpRefusalEnv is an F-105 world with a serving authority and the two consumer resources a QoS 2
// obligation depends on.
func testOpRefusalEnv(t *testing.T, name string) (*testF105Env, *mqttOutboundAuthority, *testJSAObserver) {
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

// An admission decided under a generation the authority no longer serves is refused as exactly
// that, before anything it read for that generation is judged.
func TestOpAdmitPublishRefusesMovedAuthority(t *testing.T) {
	e, auth, _ := testOpRefusalEnv(t, "op-refusal-moved")
	r := testOpAdmitRequest(t, e, auth, 7, 1, mqttGenSessEpoch(), "inc-src")
	_, err := e.asm.jsa.opAdmitPublish(r)
	if err == nil || !strings.Contains(err.Error(), "entitlement that has moved on") {
		t.Fatalf("an admission under a moved generation was not refused as one: %v", err)
	}
}

// The shared-dependency check is judged before the resources are read: a shared dependency
// supplied for a non-shared source is refused as that, even when the resource read would go
// unanswered.
func TestOpAdmitPublishSharedRefusalPrecedesResourceErrors(t *testing.T) {
	e, auth, _ := testOpRefusalEnv(t, "op-refusal-shared")
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

// What a refused operation reads: the reads are serial and stop at the refusal, so an admission
// under a moved generation has read only the authority, and an ending whose first held resource is
// gone has read only that resource.
func TestOpRefusalCosts(t *testing.T) {
	e, auth, o := testOpRefusalEnv(t, "op-refusal-costs")
	idHash := getHash(e.id)
	moved := mqttGenSessEpoch()
	r := testOpAdmitRequest(t, e, auth, 7, 2, moved, "inc-src", "inc-rel")
	f105 := testSubjectHasPrefix(mqttF105SubjectPrefix())
	m := o.mark()
	if _, err := e.asm.jsa.opAdmitPublish(r); err == nil {
		t.Fatal("an admission under a moved generation must be refused")
	}
	if n := o.sends(m, f105); n != 1 || o.sends(m, testSubjectIs(mqttOutboundAuthoritySubject(idHash))) != 1 {
		t.Fatalf("the refused admission made %d F-105 reads, want only the authority: %v", n, o.sentSubjects(m, f105))
	}

	nonce, _, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 9, 2, "inc-src", "inc-rel")
	x, _ := testF105LoadExchange(t, e, auth.Gen, nonce)
	if len(x.Holds) != 2 {
		t.Fatalf("fixture: a QoS 2 exchange holds %d resources, want 2", len(x.Holds))
	}
	first := mqttResourceSubject(idHash, auth.Gen, x.Holds[0].Inc)
	second := mqttResourceSubject(idHash, auth.Gen, x.Holds[1].Inc)
	smsg, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, first)
	if lerr != nil {
		t.Fatalf("fixture: reading the first held resource: %v", lerr)
	}
	if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true); err != nil {
		t.Fatalf("fixture: deleting the first held resource: %v", err)
	}
	m2 := o.mark()
	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
		Gen: auth.Gen, Nonce: nonce, To: mqttExchangeRejected, Acq: reg.Acq,
		Register: reg, RegisterSeq: seq,
	}); err == nil {
		t.Fatal("an ending over a held resource that does not stand must be refused")
	}
	if n := o.sends(m2, testSubjectIs(first)); n != 1 {
		t.Fatalf("the refused ending read its first held resource %d times, want once", n)
	}
	if n := o.sends(m2, testSubjectIs(second)); n != 0 {
		t.Fatalf("the refused ending still read its second held resource (%d): the refusal did not stop it", n)
	}
}
