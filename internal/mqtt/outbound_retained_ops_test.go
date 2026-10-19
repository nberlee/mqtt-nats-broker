//go:build mqttport_fulltests

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
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// The retained materialization substrate, through the operations and the real write boundary
// (F-105 7c). These are the boundary-level oracles; the SUBSCRIBE-route tests exercise the same
// operations through production.

// testRetainedMaterialize reserves, chunks and seals one copy, returning the READY manifest and
// its sequence.
func testRetainedMaterialize(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority, payload, inc string,
	data []byte, qos byte) (*mqttRetainedManifest, uint64) {
	t.Helper()
	mat := &mqttRetainedMaterial{Topic: "rt/" + payload, QoS: qos, Data: data}
	slices, err := mqttRetainedChunkPlan(data, 0)
	if err != nil {
		t.Fatalf("chunk plan: %v", err)
	}
	m, _, err := e.asm.jsa.opReserveRetainedPayload(e.id, e.epoch, "rsv-"+payload, "op-rsv-"+payload,
		auth.Gen, auth.Tenure, inc, payload, mat, uint64(len(slices)))
	if err != nil {
		t.Fatalf("reserving payload %q was refused: %v", payload, err)
	}
	_, mSeq, err := e.asm.jsa.loadRetainedManifest(e.id, e.epoch, auth.Gen, payload)
	if err != nil {
		t.Fatalf("the reserved manifest does not load back: %v", err)
	}
	if len(slices) > 0 {
		m, _, err = e.asm.jsa.opWriteRetainedChunks("chk-"+payload, "op-chk-"+payload, auth.Tenure, m, mSeq, slices)
		if err != nil {
			t.Fatalf("writing payload %q's chunks was refused: %v", payload, err)
		}
		_, mSeq, _ = e.asm.jsa.loadRetainedManifest(e.id, e.epoch, auth.Gen, payload)
	}
	_, _, err = e.asm.jsa.opSealRetainedPayload("seal-"+payload, "op-seal-"+payload, auth.Tenure, m, mSeq)
	if err != nil {
		t.Fatalf("sealing payload %q was refused: %v", payload, err)
	}
	m, mSeq, err = e.asm.jsa.loadRetainedManifest(e.id, e.epoch, auth.Gen, payload)
	if err != nil || m.State != mqttManifestReady {
		t.Fatalf("the sealed manifest is %+v (%v), want READY", m, err)
	}
	return m, mSeq
}

// testRetainedRegister is a retained register at its publish for one allocation.
func testRetainedRegister(pi uint16, m *mqttRetainedManifest) *mqttInflightRecord {
	return freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, QoS: m.QoS, Nonce: mqttNewInflightNonce(),
		Payload: m.Payload, Digest: m.Digest,
	})
}

func testRetainedAdmit(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority, pi uint16,
	m *mqttRetainedManifest, mSeq uint64) *mqttInflightRecord {
	t.Helper()
	reg := testRetainedRegister(pi, m)
	if _, err := e.asm.jsa.opAdmitRetained(&mqttAdmitRetainedRequest{
		ID: e.id, Epoch: e.epoch, Txn: "adm-" + m.Payload, Op: "op-adm-" + m.Payload,
		Gen: auth.Gen, Tenure: auth.Tenure, Register: reg, Manifest: m, ManifestSeq: mSeq,
	}); err != nil {
		t.Fatalf("admitting against payload %q was refused: %v", m.Payload, err)
	}
	return reg
}

// TestF105RetainedMaterializeAndAdmit drives the four acts and asserts what each leaves standing:
// a READY copy whose chunks reassemble to the snapshot, then an exchange, a retained register with
// its marker, the payload's retained-material edge, and the manifest naming the exchange.
func TestF105RetainedMaterializeAndAdmit(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ret-admit")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")

	data := bytes.Repeat([]byte("retained-payload-"), 9000) // spans several chunks
	m, mSeq := testRetainedMaterialize(t, e, auth, "pay-a", "inc-a", data, 2)
	if m.Chunks < 2 {
		t.Fatalf("a %d-byte payload was cut into %d chunk(s), want several", len(data), m.Chunks)
	}
	got, verr := e.asm.jsa.verifyMaterial(m)
	if verr != nil || !bytes.Equal(got, data) {
		t.Fatalf("the sealed material does not reassemble to the snapshot (%v)", verr)
	}

	reg := testRetainedAdmit(t, e, auth, 11, m, mSeq)
	idHash := getHash(e.id)
	x, _, xerr := e.asm.jsa.loadOutboundExchange(e.id, e.epoch, auth.Gen, reg.Lin)
	if xerr != nil || x == nil || x.Phase != mqttExchangePublish || x.PI != 11 || x.QoS != 2 {
		t.Fatalf("the admitted exchange is %+v (%v)", x, xerr)
	}
	stored, smsg, lerr := e.asm.loadInflightRecord(mqttInflightSubject(idHash, auth.Gen, 11))
	if lerr != nil || smsg == nil || stored == nil || !stored.retainedSource() || stored.Payload != "pay-a" ||
		stored.Dur != _EMPTY_ || stored.Seq != 0 {
		t.Fatalf("the retained register is %+v (%v)", stored, lerr)
	}
	if _, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttInflightSubject(idHash, auth.Gen, 11)+"."+mqttInflightMarkerSuffix); err != nil {
		t.Fatalf("the unsent marker does not load back: %v", err)
	}
	res, _, rerr := e.asm.jsa.loadOutboundResource(e.id, e.epoch, auth.Gen, "inc-a")
	if rerr != nil || len(res.Deps) != 1 || res.Deps[0].Role != mqttRoleRetainedMaterial || res.Deps[0].Nonce != reg.Lin {
		t.Fatalf("the payload owner carries %+v (%v), want one retained-material edge for %q", res, rerr, reg.Lin)
	}
	after, _, _ := e.asm.jsa.loadRetainedManifest(e.id, e.epoch, auth.Gen, "pay-a")
	if after.Admitted != reg.Lin || after.State != mqttManifestReady {
		t.Fatalf("the manifest after admission is %+v, want READY admitted to %q", after, reg.Lin)
	}
}

// TestF105RetainedAdmissionBoundaryRefusals submits malformed admissions DIRECTLY to the boundary:
// against a PROVISIONING copy, against an already admitted one, with a register naming another
// digest, and without the manifest member. Each must be refused.
func TestF105RetainedAdmissionBoundaryRefusals(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ret-adm-refuse")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")

	// A PROVISIONING copy is not admissible.
	mat := &mqttRetainedMaterial{Topic: "rt/p", QoS: 1, Data: []byte("x")}
	prov, _, err := e.asm.jsa.opReserveRetainedPayload(e.id, e.epoch, "rsv-p", "op-rsv-p", auth.Gen, auth.Tenure,
		"inc-p", "pay-p", mat, 1)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// Every chunk landed, the copy never sealed: only the state stands between it and an admission.
	_, provSeq, _ := e.asm.jsa.loadRetainedManifest(e.id, e.epoch, auth.Gen, "pay-p")
	if _, _, err = e.asm.jsa.opWriteRetainedChunks("chk-p", "op-chk-p", auth.Tenure, prov, provSeq,
		[][]byte{[]byte("x")}); err != nil {
		t.Fatalf("writing the chunk: %v", err)
	}
	prov, provSeq, _ = e.asm.jsa.loadRetainedManifest(e.id, e.epoch, auth.Gen, "pay-p")
	if prov.State != mqttManifestProvisioning || prov.Written != prov.Chunks {
		t.Fatalf("the fixture copy is %+v, want PROVISIONING with every chunk written", prov)
	}
	early := *prov
	early.State = mqttManifestReady // the op checks admissibility itself; lie to reach the boundary
	if _, err := e.asm.jsa.opAdmitRetained(&mqttAdmitRetainedRequest{ID: e.id, Epoch: e.epoch, Txn: "adm-p",
		Op: "op-adm-p", Gen: auth.Gen, Tenure: auth.Tenure, Register: testRetainedRegister(3, prov),
		Manifest: &early, ManifestSeq: provSeq}); err == nil ||
		!strings.Contains(err.Error(), "an admission names a READY payload") {
		t.Fatalf("an admission against a PROVISIONING copy was answered %v, want the admission's state refusal", err)
	}

	m, mSeq := testRetainedMaterialize(t, e, auth, "pay-b", "inc-b", []byte("hello"), 1)

	// A register naming another digest.
	bad := testRetainedRegister(4, m)
	bad.Digest = mqttMaterialDigest([]byte("other"))
	if _, err := e.asm.jsa.opAdmitRetained(&mqttAdmitRetainedRequest{ID: e.id, Epoch: e.epoch, Txn: "adm-d",
		Op: "op-adm-d", Gen: auth.Gen, Tenure: auth.Tenure, Register: bad, Manifest: m, ManifestSeq: mSeq}); err == nil ||
		!strings.Contains(err.Error(), "against manifest") {
		t.Fatalf("an admission whose register names another digest was not refused at the boundary: %v", err)
	}

	// Without the manifest member: compose opAdmitPublish's batch alone.
	led, ledSeq, _ := e.asm.jsa.loadGenLedger(e.id, e.epoch, auth.Gen)
	res, resSeq, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, auth.Gen, "inc-b")
	cur, curSeq, _ := e.asm.jsa.servingAuthority(e.id, e.epoch)
	reg := testRetainedRegister(5, m)
	tr, terr := mqttAdmitPublishTransition(&mqttPublishAdmission{
		Auth: cur, AuthSeq: curSeq, Txn: "adm-n", Op: "op-adm-n",
		Exchange: &mqttOutboundExchange{ID: e.id, Epoch: e.epoch, Gen: cur.Gen, Nonce: reg.Lin, PI: 5, QoS: 1,
			Phase: mqttExchangePublish, AdmitTxn: "adm-n", Tenure: cur.Tenure},
		Register: reg, RegSubj: mqttInflightSubject(getHash(e.id), cur.Gen, 5),
		Deps:   []mqttResourceDependency{{Resource: res, Seq: resSeq, Role: mqttRoleRetainedMaterial}},
		Ledger: led, LedgerSeq: ledSeq,
	})
	if terr != nil {
		t.Fatalf("composing: %v", terr)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(tr); err != nil {
		// Without `admitted` the copy does not name the exchange, so a later close could not see it:
		// the boundary refuses a retained-material edge taken by an admission that did not CAS the copy.
		t.Logf("refused as expected: %v", err)
	} else {
		t.Fatalf("a retained admission without the manifest member was accepted")
	}

	// A second admission against an admitted copy.
	testRetainedAdmit(t, e, auth, 6, m, mSeq)
	m2, mSeq2, _ := e.asm.jsa.loadRetainedManifest(e.id, e.epoch, auth.Gen, "pay-b")
	m2.Admitted = _EMPTY_ // the op refuses an admitted copy itself; reach the boundary
	if _, err := e.asm.jsa.opAdmitRetained(&mqttAdmitRetainedRequest{ID: e.id, Epoch: e.epoch, Txn: "adm-2",
		Op: "op-adm-2", Gen: auth.Gen, Tenure: auth.Tenure, Register: testRetainedRegister(7, m2),
		Manifest: m2, ManifestSeq: mSeq2}); err == nil {
		t.Fatalf("a second admission against one copy was accepted")
	}
}

// TestF105RetainedCloseArbitratesWithAdmission: an admitted copy cannot be closed while its
// exchange is at Publish; an unadmitted one can be closed by its producer's own entitlement; and a
// close landing first makes a later admission's CAS fail.
func TestF105RetainedCloseArbitratesWithAdmission(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ret-close")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")

	m, mSeq := testRetainedMaterialize(t, e, auth, "pay-c", "inc-c", []byte("c"), 1)
	testRetainedAdmit(t, e, auth, 8, m, mSeq)
	m, mSeq, _ = e.asm.jsa.loadRetainedManifest(e.id, e.epoch, auth.Gen, "pay-c")
	if _, err := e.asm.jsa.opCloseRetainedManifest("cls-c", "op-cls-c", m, mSeq); err == nil {
		t.Fatalf("an admitted copy whose exchange stands at Publish was closed")
	}

	// Close first, then admit: the admission's CAS is against a sequence that moved.
	n, nSeq := testRetainedMaterialize(t, e, auth, "pay-d", "inc-d", []byte("d"), 1)
	if _, err := e.asm.jsa.opCloseRetainedManifest("cls-d", "op-cls-d", n, nSeq); err != nil {
		t.Fatalf("the producer's own close of an unadmitted copy was refused: %v", err)
	}
	if _, err := e.asm.jsa.opAdmitRetained(&mqttAdmitRetainedRequest{ID: e.id, Epoch: e.epoch, Txn: "adm-d",
		Op: "op-adm-d", Gen: auth.Gen, Tenure: auth.Tenure, Register: testRetainedRegister(9, n),
		Manifest: n, ManifestSeq: nSeq}); err == nil {
		t.Fatalf("an admission against a copy closed in between was accepted")
	}
}

// TestF105RetainedPubRecCreatesItsCanonical: a successful PUBREC moves the exchange to
// PubRecObserved and the retained register straight to pubrel_ready, creating its canonical PUBREL
// on $MQTT_inflight in the same batch; a direct advance without the record is refused.
func TestF105RetainedPubRecCreatesItsCanonical(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ret-pubrec")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	m, mSeq := testRetainedMaterialize(t, e, auth, "pay-e", "inc-e", []byte("e"), 2)
	reg := testRetainedAdmit(t, e, auth, 12, m, mSeq)

	subj := mqttInflightSubject(getHash(e.id), auth.Gen, 12)
	stored, smsg, _ := e.asm.loadInflightRecord(subj)

	// Without the canonical: compose the ordinary arrow by hand, retained slot to pubrel_ready only.
	cur, curSeq, _ := e.asm.jsa.loadOutboundExchange(e.id, e.epoch, auth.Gen, reg.Lin)
	a, aSeq, _ := e.asm.jsa.requireAuthority(e.id, e.epoch)
	tr, err := mqttAdvanceExchangeTransition(a, aSeq, "adv-x", "op-adv-x", cur, curSeq, mqttExchangePubRecObserved)
	if err != nil {
		t.Fatalf("composing: %v", err)
	}
	bare := *stored
	bare.State = mqttInflightPubRelReady
	bare.RelSubj = mqttRetainedRelSubject(getHash(e.id), auth.Gen, reg.Lin)
	bare.RelID = mqttNewInflightNonce()
	bb, _ := json.Marshal(&bare)
	tr.Extra = append(tr.Extra, mqttTransitionMember{subj: subj, expect: smsg.Sequence, body: bb})
	if _, err := e.asm.jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a retained PUBREC arrow without its canonical PUBREL record was accepted")
	}

	// Without the manifest fence: the composed advance with its manifest member removed. A Publish-
	// origin retained arrow must CAS the copy unchanged, or a copy cleanup already closed could
	// still take a `rel`.
	{
		cm, cmSeq, _ := e.asm.jsa.loadRetainedManifest(e.id, e.epoch, auth.Gen, "pay-e")
		cur, curSeq, _ := e.asm.jsa.loadOutboundExchange(e.id, e.epoch, auth.Gen, reg.Lin)
		a, aSeq, _ := e.asm.jsa.requireAuthority(e.id, e.epoch)
		ftr, ferr := mqttAdvanceExchangeAckTransition(a, aSeq, &mqttAdvanceExchangeAckRequest{
			ID: e.id, Epoch: e.epoch, Txn: "adv-nf", Op: "op-adv-nf", Gen: auth.Gen, Nonce: reg.Lin,
			To: mqttExchangePubRecObserved, Acq: stored.Acq, Register: stored, RegisterSeq: smsg.Sequence,
			RelID: mqttNewInflightNonce(), Manifest: cm, ManifestSeq: cmSeq,
		}, cur, curSeq)
		if ferr != nil {
			t.Fatalf("composing the fenced advance: %v", ferr)
		}
		kept := ftr.Extra[:0:0]
		for _, mem := range ftr.Extra {
			if !mqttIsManifestSubject(mem.subj) {
				kept = append(kept, mem)
			}
		}
		if len(kept) == len(ftr.Extra) {
			t.Fatal("the composed advance carried no manifest member to remove")
		}
		ftr.Extra = kept
		const want = "without fencing on its copy's manifest"
		if _, err := e.asm.jsa.commitAuthorityTransition(ftr); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("a retained PUBREC advance without its manifest fence was answered %v, want a refusal naming %q", err, want)
		}
	}

	relID := mqttNewInflightNonce()
	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "adv-1", Op: "op-adv-1", Gen: auth.Gen, Nonce: reg.Lin,
		To: mqttExchangePubRecObserved, Acq: stored.Acq, Register: stored, RegisterSeq: smsg.Sequence, RelID: relID,
	}); err != nil {
		t.Fatalf("the retained PUBREC advance was refused: %v", err)
	}
	after, _, _ := e.asm.loadInflightRecord(subj)
	if after.State != mqttInflightPubRelReady || after.RelID != relID {
		t.Fatalf("the retained register after PUBREC is %+v", after)
	}
	rmsg, rerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, after.RelSubj)
	if rerr != nil {
		t.Fatalf("the canonical PUBREL does not load back: %v", rerr)
	}
	if rel, derr := mqttDecodeRetainedRel(rmsg.Data, e.id, e.epoch, auth.Gen, reg.Lin); derr != nil || rel.Identity != relID {
		t.Fatalf("the canonical PUBREL is %+v (%v)", rel, derr)
	}
}

// T9c — THE TENURE FENCE on closing an UNADMITTED copy, over the inputs the boundary sees: the
// authority predecessor the batch CASes, the batch's own attempt, the generation's closure. Each
// case names the arm that admitted it, or is refused: a closer presenting another tenure while the
// producer's still serves, a PREPARING authority (a preparation can be cancelled back), and an
// unreadable one are refused; a successor serving tenure, a closure, and the producer's own
// disposition admit. A DELAYED predecessor close after a successor took over is admitted only
// through the successor arm — its captured entitlement is not the standing one.
func TestF105RetainedCloseTenureFence(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ret-fence")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	m, _ := testRetainedMaterialize(t, e, auth, "pay-f", "inc-f", []byte("fence"), 1)
	attempt := func(tenure string) *mqttTransitionAttempt {
		return &mqttTransitionAttempt{ID: e.id, Epoch: e.epoch, Txn: "cls-" + tenure, Gen: m.Gen, Tenure: tenure}
	}
	with := func(f func(p *mqttOutboundAuthority)) *mqttOutboundAuthority {
		p := *auth
		f(&p)
		return &p
	}
	for _, tc := range []struct {
		name string
		a    *mqttTransitionAttempt
		prev *mqttOutboundAuthority
		arm  string // empty: refused
	}{
		{name: "another tenure while the producer's serves", a: attempt("other"), prev: auth},
		{name: "a preparing authority", a: attempt(m.Tenure),
			prev: with(func(p *mqttOutboundAuthority) { p.State = mqttAuthorityPreparing })},
		{name: "an unreadable authority", a: attempt(m.Tenure), prev: nil},
		{name: "a successor serving tenure", a: attempt("successor"),
			prev: with(func(p *mqttOutboundAuthority) { p.Tenure = "successor" }), arm: mqttRetainedCloseSuccessor},
		{name: "a delayed predecessor close after the successor", a: attempt(m.Tenure),
			prev: with(func(p *mqttOutboundAuthority) { p.Tenure = "successor" }), arm: mqttRetainedCloseSuccessor},
		{name: "the producer's own disposition", a: attempt(m.Tenure), prev: auth, arm: mqttRetainedCloseProducer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arm, err := e.asm.jsa.retainedCloseArm(tc.a, tc.prev, m)
			if tc.arm == _EMPTY_ {
				if err == nil {
					t.Fatalf("the close was admitted through %q, want a refusal", arm)
				}
				return
			}
			if err != nil || arm != tc.arm {
				t.Fatalf("the close was answered %q (%v), want the %q arm", arm, err, tc.arm)
			}
		})
	}
	t.Run("a closed generation", func(t *testing.T) {
		cur, curSeq, aerr := e.asm.jsa.servingAuthority(e.id, e.epoch)
		if aerr != nil {
			t.Fatalf("no serving authority: %v", aerr)
		}
		if _, err := e.asm.jsa.commitAuthorityTransition(testF105Closure(e, cur, curSeq, "txn-close")); err != nil {
			t.Fatalf("closing the generation: %v", err)
		}
		arm, err := e.asm.jsa.retainedCloseArm(attempt("anyone"), nil, m)
		if err != nil || arm != mqttRetainedCloseClosure {
			t.Fatalf("the close in a closed generation was answered %q (%v), want the %q arm", arm, err, mqttRetainedCloseClosure)
		}
	})
}
