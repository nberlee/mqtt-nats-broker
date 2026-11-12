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
	"strings"
	"sync/atomic"
	"testing"
)

// THE INVENTORY FLOOR (issue #114). A live generation's history is retired behind a proven contiguous
// floor: every position below Lo is reclaimed or relocated, every duty that still has work keeps
// exactly one representative inside [Lo, Hi], and every reader reads [Lo, Hi] only.

// testFloorEnv is a bootstrapped store-level environment: a serving authority, nothing admitted.
func testFloorEnv(t *testing.T, clientID string) (*testOutboundEnv, func()) {
	t.Helper()
	e, done := testNewEnv(t, clientID)
	e.bootstrap(t, "txn-boot")
	return e, done
}

// testFloorAdmit admits one QoS 1 obligation at identifier pi, with its register, through the
// production admission composer, and returns its nonce.
func testFloorAdmit(t *testing.T, e *testOutboundEnv, pi uint16) string {
	t.Helper()
	jsa := &e.asm.jsa
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	txn := mqttNewInflightNonce()
	x := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
		PI: pi, QoS: 1, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure,
	}
	tr, terr := testAdmitExchangeTr(t, e, al.rec, al.seq, txn, "op-"+txn, x)
	if terr != nil {
		t.Fatalf("composing the admission at %d: %v", pi, terr)
	}
	if _, err := jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("admitting at %d: %v", pi, err)
	}
	return x.Nonce
}

// testEndAndResolve ends a QoS 1 obligation at Terminal (its PUBACK), lets the terminal
// cleanup take its publish_terminal register, and resolves its duty. Once the register is gone
// nothing but the obligation's own records can name it: the shape that becomes reclaimable as
// soon as its duty resolves, and which frees the identifier for the next admission.
func testEndAndResolve(t *testing.T, e *testOutboundEnv, nonce string) {
	t.Helper()
	jsa := &e.asm.jsa
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testGen, nonce, mqttExchangeTerminal); err != nil {
		t.Fatalf("ending %q: %v", nonce, err)
	}
	testDropTerminalRegister(t, e, nonce)
	testResolve(t, e, nonce)
}

// testDropTerminalRegister clears an ended obligation's slot: testDropEndedSlot.
func testDropTerminalRegister(t *testing.T, e *testOutboundEnv, nonce string) {
	t.Helper()
	testDropEndedSlot(t, &e.asm.jsa, e.id, e.epoch, testGen, nonce)
}

// testDropEndedSlot removes what an ended QoS 1 obligation's identifier still carries, as
// production does by the time its PUBACK is recorded and cleaned up: the unsent marker, which the
// transmission deleted (the fixture never transmitted), and the publish_terminal register, which
// the terminal cleanup deletes by its exact sequence. After it nothing but the obligation's own
// records names it, and the identifier is free for the next admission.
func testDropEndedSlot(t *testing.T, jsa *mqttJSA, id, epoch, gen, nonce string) {
	t.Helper()
	x, _, xerr := jsa.loadOutboundExchange(id, epoch, gen, nonce)
	if xerr != nil {
		t.Fatalf("reading exchange %q: %v", nonce, xerr)
	}
	subj := mqttInflightSubject(getHash(id), gen, x.PI)
	cur, smsg, lerr := jsa.loadInflightRecord(subj)
	if lerr != nil || cur == nil || smsg == nil || cur.State != mqttInflightPublishTerminal || cur.Lin != nonce {
		t.Fatalf("the register of ended exchange %q is not its publish_terminal slot: %+v (%v)", nonce, cur, lerr)
	}
	if mmsg, merr := jsa.loadLastMsgFor(mqttSessStreamName, mqttInflightMarkerSubject(getHash(id), gen, x.PI)); merr == nil && mmsg != nil {
		if err := jsa.deleteMsg(mqttSessStreamName, mmsg.Sequence, true); err != nil {
			t.Fatalf("deleting %q's unsent marker: %v", nonce, err)
		}
	}
	if err := jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); err != nil {
		t.Fatalf("the terminal cleanup of %q's register: %v", nonce, err)
	}
}

// testResolve resolves the exchange duty naming nonce.
func testResolve(t *testing.T, e *testOutboundEnv, nonce string) {
	t.Helper()
	jsa := &e.asm.jsa
	ord, found, err := jsa.findDutyInInventory(e.id, e.epoch, testGen, func(d *mqttDutyEntry) bool {
		return d.Kind == mqttDutyExchange && d.Ref == nonce
	})
	if err != nil || !found {
		t.Fatalf("finding the duty of %q: found=%v %v", nonce, found, err)
	}
	d, _, derr := jsa.loadDutyEntryAt(e.id, e.epoch, testGen, ord)
	if derr != nil || d == nil {
		t.Fatalf("reading duty %d: %v", ord, derr)
	}
	root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: testGen}
	if err := jsa.opResolveDuty(root, mqttSweptDuty{Ord: ord, Entry: d}, mqttNewInflightNonce()); err != nil {
		t.Fatalf("resolving duty %d of %q: %v", ord, nonce, err)
	}
}

func testFloorLedger(t *testing.T, e *testOutboundEnv) (*mqttGenLedger, uint64) {
	t.Helper()
	led, seq, err := e.asm.jsa.loadGenLedger(e.id, e.epoch, testGen)
	if err != nil || led == nil {
		t.Fatalf("reading the ledger: %+v %v", led, err)
	}
	return led, seq
}

// testAdvanceTo retires every decidable position below end, in bounded batches, and then
// deletes what was retired. It is the floor unit's execution without its horizon heuristic.
func testAdvanceTo(t *testing.T, e *testOutboundEnv, end uint64) {
	t.Helper()
	jsa := &e.asm.jsa
	for i := 0; i < 64; i++ {
		f, err := jsa.composeFloorBatch(e.id, e.epoch, testGen, end)
		if err != nil {
			t.Fatalf("composing a floor batch: %v", err)
		}
		if f == nil || len(f.Positions) == 0 {
			break
		}
		if err := jsa.opAdvanceFloor(f, mqttNewInflightNonce()); err != nil {
			t.Fatalf("advancing the floor from %d: %v", f.Ledger.Lo, err)
		}
	}
	for i := 0; i < 64; i++ {
		more, err := jsa.deleteRetiredPositionsStep(e.id, e.epoch, testGen, nil)
		if err != nil {
			t.Fatalf("deleting retired positions: %v", err)
		}
		if !more {
			break
		}
	}
}

// T-A. The floor reclaims ended history and carries outstanding work. Three obligations admitted
// first (ordinals 1-3) stay outstanding; sixty more end and resolve. One advance over the whole
// inventory retires every position: the sixty are reclaimed and deleted, the three are carried to
// representatives above the old high-water, and every reader still sees them.
func TestFloorReclaimsEndedHistoryAndCarriesOutstandingWork(t *testing.T) {
	e, done := testFloorEnv(t, "f112-reclaim")
	defer done()
	jsa := &e.asm.jsa

	var outstanding []string
	for pi := uint16(1); pi <= 3; pi++ {
		outstanding = append(outstanding, testFloorAdmit(t, e, pi))
	}
	var ended []string
	for i := 0; i < 60; i++ {
		n := testFloorAdmit(t, e, 10)
		testEndAndResolve(t, e, n)
		ended = append(ended, n)
	}
	before, _ := testFloorLedger(t, e)
	if before.Lo != 1 || before.Hi != 63 {
		t.Fatalf("fixture ledger %+v, want Lo 1 Hi 63", before)
	}

	testAdvanceTo(t, e, before.Hi+1)

	after, _ := testFloorLedger(t, e)
	if after.Lo != before.Hi+1 || after.Hi != before.Hi+3 || after.Del != after.Lo {
		t.Fatalf("after the advance the ledger is %+v; want Lo %d, Hi %d, Del == Lo",
			after, before.Hi+1, before.Hi+3)
	}
	// THE CARRIED DUTIES ARE FOUND AT THEIR ONE REPRESENTATIVE, inside the floor.
	for _, n := range outstanding {
		ord, found, err := jsa.findDutyInInventory(e.id, e.epoch, testGen, func(d *mqttDutyEntry) bool {
			return d.Kind == mqttDutyExchange && d.Ref == n
		})
		if err != nil || !found || ord < after.Lo {
			t.Fatalf("outstanding obligation %q has no representative inside the floor: ord %d found %v %v",
				n, ord, found, err)
		}
		d, _, _ := jsa.loadDutyEntryAt(e.id, e.epoch, testGen, ord)
		if d == nil || d.State != mqttDutyPending || d.From == 0 || d.From >= after.Lo {
			t.Fatalf("representative of %q is %+v: a relocation carries the duty unchanged, with its provenance", n, d)
		}
	}
	// THE TRAVERSAL SEES EXACTLY THE OUTSTANDING WORK, completely.
	census, err := mqttTraverseGenerationInventory(jsa, nil, e.id, e.epoch, testGen, nil)
	if err != nil || !census.complete || len(census.uncertain) != 0 || len(census.live) != 3 {
		t.Fatalf("the traversal after the advance: err %v complete %v uncertain %v live %d",
			err, census != nil && census.complete, census.uncertain, len(census.live))
	}
	// THE RECLAIMED RECORDS ARE GONE, and their absence classifies as a reclamation for a reader
	// that proves the landing, and as nothing for one that cannot.
	for _, n := range ended {
		if _, _, xerr := jsa.loadOutboundExchange(e.id, e.epoch, testGen, n); xerr == nil || !mqttExchangeReadAbsent(xerr) {
			t.Fatalf("reclaimed exchange %q still reads: %v", n, xerr)
		}
	}
	if v := jsa.classifyExchangeAbsence(e.id, e.epoch, testGen, ended[0], mqttLandingDecodedRead); v != mqttAbsenceReclaimed {
		t.Fatalf("a reclaimed exchange with a landing proof classifies %d, want reclaimed", v)
	}
	if v := jsa.classifyExchangeAbsence(e.id, e.epoch, testGen, ended[0], mqttLandingUnproven); v != mqttAbsenceUnproven {
		t.Fatalf("an absence without a landing proof classifies %d, want unproven", v)
	}
	for ord := uint64(1); ord < after.Del; ord++ {
		if d, _, derr := jsa.loadDutyEntryAt(e.id, e.epoch, testGen, ord); derr != nil || d != nil {
			t.Fatalf("retired position %d below the watermark still stands: %+v %v", ord, d, derr)
		}
	}
	// EXCLUSIVITY READS THE INVENTORY: the carried obligation still holds its identifier, and the
	// identifier the reclaimed history used is free.
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	x := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
		PI: 2, QoS: 1, Phase: mqttExchangePublish, AdmitTxn: "txn-collide", Tenure: al.rec.Tenure,
	}
	tr, _ := testAdmitExchangeTr(t, e, al.rec, al.seq, "txn-collide", "op-collide", x)
	if _, err := jsa.commitAuthorityTransition(tr); err == nil || !strings.Contains(err.Error(), "still holds") {
		t.Fatalf("an identifier a carried obligation holds was claimed: %v", err)
	}
	testFloorAdmit(t, e, 10)

	// A CARRIED DUTY STILL COMPLETES (holistic round 1): two of the carried obligations end, one
	// resolved alone and one in bulk, at their representatives; the next advance reclaims both.
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testGen, outstanding[0], mqttExchangeTerminal); err != nil {
		t.Fatalf("ending a carried obligation: %v", err)
	}
	testDropTerminalRegister(t, e, outstanding[0])
	testResolve(t, e, outstanding[0])
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testGen, outstanding[1], mqttExchangeTerminal); err != nil {
		t.Fatalf("ending a carried obligation: %v", err)
	}
	testDropTerminalRegister(t, e, outstanding[1])
	ord1, _, _ := jsa.findDutyInInventory(e.id, e.epoch, testGen, func(d *mqttDutyEntry) bool { return d.Ref == outstanding[1] })
	d1, _, _ := jsa.loadDutyEntryAt(e.id, e.epoch, testGen, ord1)
	root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: testGen}
	if err := jsa.opResolveDuties(root, []mqttSweptDuty{{Ord: ord1, Entry: d1}}, mqttNewInflightNonce()); err != nil {
		t.Fatalf("resolving a carried duty in bulk: %v", err)
	}
	now, _ := testFloorLedger(t, e)
	testAdvanceTo(t, e, outstanding2Ord(t, e, outstanding[2]))
	for _, n := range outstanding[:2] {
		if _, _, xerr := jsa.loadOutboundExchange(e.id, e.epoch, testGen, n); xerr == nil || !mqttExchangeReadAbsent(xerr) {
			t.Fatalf("the completed carried obligation %q was not reclaimed after its resolution: %v (ledger before %+v)", n, xerr, now)
		}
	}
}

// outstanding2Ord is the ordinal of the representative naming nonce.
func outstanding2Ord(t *testing.T, e *testOutboundEnv, nonce string) uint64 {
	t.Helper()
	ord, found, err := e.asm.jsa.findDutyInInventory(e.id, e.epoch, testGen, func(d *mqttDutyEntry) bool { return d.Ref == nonce })
	if err != nil || !found {
		t.Fatalf("finding the representative of %q: %v", nonce, err)
	}
	return ord
}

// T-B. The floor advance's write boundary. Each case is a batch the composer would never build,
// submitted straight to the boundary.
func TestFloorAdvanceBoundaryRefusesUnprovenRetirement(t *testing.T) {
	e, done := testFloorEnv(t, "f112-boundary")
	defer done()
	jsa := &e.asm.jsa

	pinned := testFloorAdmit(t, e, 1)
	for i := 0; i < 4; i++ {
		testEndAndResolve(t, e, testFloorAdmit(t, e, 10))
	}
	led, ledSeq := testFloorLedger(t, e)
	read := func(ord uint64) mqttFloorPosition {
		d, seq, err := jsa.loadDutyEntryAt(e.id, e.epoch, testGen, ord)
		if err != nil || d == nil {
			t.Fatalf("reading position %d: %v", ord, err)
		}
		return mqttFloorPosition{Ord: ord, Entry: d, Seq: seq}
	}

	// (1) A NON-RECLAIMABLE POSITION RETIRED WITHOUT ITS REPRESENTATIVE (M1).
	p := read(1)
	p.Class = mqttFloorReclaim
	f := &mqttFloorAdvance{ID: e.id, Epoch: e.epoch, Gen: testGen, Ledger: led, LedgerSeq: ledSeq,
		Positions: []mqttFloorPosition{p}}
	if err := jsa.opAdvanceFloor(f, mqttNewInflightNonce()); err == nil || !strings.Contains(err.Error(), "not reclaimable") {
		t.Fatalf("an outstanding obligation's position was retired with no representative: %v", err)
	}

	// (2) A REPRESENTATIVE THAT DOES NOT CARRY THE DUTY UNCHANGED.
	p = read(1)
	p.Class = mqttFloorRelocate
	f.Positions = []mqttFloorPosition{p}
	members, merr := mqttComposeFloorMembers(f)
	if merr != nil {
		t.Fatalf("composing a relocation: %v", merr)
	}
	for i, m := range members {
		if m.expect == 0 && mqttIsDutySubject(m.subj) {
			var d mqttDutyEntry
			_ = json.Unmarshal(m.body, &d)
			d.State = mqttDutyNotYetActionable
			members[i].body, _ = json.Marshal(&d)
		}
	}
	if err := testCommitFloorMembers(t, e, members); err == nil || !strings.Contains(err.Error(), "different state") {
		t.Fatalf("a relocation altered the duty it carried: %v", err)
	}

	// (3) THE WATERMARK OVER A POSITION THAT STILL STANDS.
	f.Positions = []mqttFloorPosition{read(1)}
	f.Positions[0].Class = mqttFloorRelocate
	for ord := uint64(2); ord <= led.Hi; ord++ {
		q := read(ord)
		q.Class = mqttFloorReclaim
		f.Positions = append(f.Positions, q)
	}
	if err := jsa.opAdvanceFloor(f, mqttNewInflightNonce()); err != nil {
		t.Fatalf("a legal advance was refused: %v", err)
	}
	led2, led2Seq := testFloorLedger(t, e)
	g := &mqttFloorAdvance{ID: e.id, Epoch: e.epoch, Gen: testGen, Ledger: led2, LedgerSeq: led2Seq, DelTo: led2.Lo}
	if err := jsa.opAdvanceFloor(g, mqttNewInflightNonce()); err == nil || !strings.Contains(err.Error(), "still stands") {
		t.Fatalf("positions were recorded deleted while their entries stand: %v", err)
	}

	// (4) THE CARRIED OBLIGATION IS THE ONE REPRESENTATIVE: found, and still at Publish.
	if x, _, err := jsa.loadOutboundExchange(e.id, e.epoch, testGen, pinned); err != nil || x.Phase != mqttExchangePublish {
		t.Fatalf("the carried obligation does not stand: %+v %v", x, err)
	}

	// (5) A LEDGER WITHOUT ITS FLOOR OR WATERMARK IS AN UNSUPPORTED SHAPE, refused at decode.
	for _, body := range []string{
		`{"id":"` + e.id + `","epoch":"` + e.epoch + `","gen":"` + testGen + `","state":1,"hi":3}`,
		`{"id":"` + e.id + `","epoch":"` + e.epoch + `","gen":"` + testGen + `","state":1,"hi":3,"lo":5,"del":1}`,
		`{"id":"` + e.id + `","epoch":"` + e.epoch + `","gen":"` + testGen + `","state":1,"hi":3,"lo":2,"del":3}`,
	} {
		if _, err := mqttDecodeGenLedger([]byte(body), e.id, e.epoch, testGen); err == nil {
			t.Fatalf("an unsupported ledger shape decoded: %s", body)
		}
	}
	if mqttLedgerSpan(&mqttGenLedger{Lo: 8, Hi: 7}) != 0 || mqttLedgerSpan(&mqttGenLedger{Lo: 3, Hi: 7}) != 5 {
		t.Fatal("the span of an empty or populated inventory is miscounted")
	}
}

// testCommitFloorMembers submits hand-built floor members under the standing authority.
func testCommitFloorMembers(t *testing.T, e *testOutboundEnv, members []mqttTransitionMember) error {
	t.Helper()
	jsa := &e.asm.jsa
	auth, authSeq, err := jsa.requireAuthority(e.id, e.epoch)
	if err != nil || auth == nil {
		t.Fatalf("no authority: %v", err)
	}
	txn := mqttNewInflightNonce()
	_, cerr := jsa.commitAuthorityTransition(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Scope: auth.Gen, Op: "op-" + txn,
			Kind: mqttTransitionAdvanceFloor, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: authSeq,
		Auth: &mqttOutboundAuthority{
			ID: auth.ID, Epoch: auth.Epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: auth.State, Rev: auth.Rev + 1, Txn: txn, OwnerTxn: auth.OwnerTxn,
			Life: mqttLifeOf(auth),
		},
		Extra: members,
	})
	return cerr
}

// T-C. The register clause and the token rule. An ended obligation whose register still names it
// is carried, not reclaimed (M2); and no batch may end an obligation beside a register holding
// its output token (M12).
func TestARegisterNamingTheLineagePinsItAndATokenRefusesTheEnding(t *testing.T) {
	e, done := testFloorEnv(t, "f112-register")
	defer done()
	jsa := &e.asm.jsa

	n := testFloorAdmit(t, e, 7)
	regSubj := mqttInflightSubject(getHash(e.id), testGen, 7)
	// A transmission is in flight: its output token stands on the register.
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	reg, rmsg, _ := jsa.loadInflightRecord(regSubj)
	if _, _, err := jsa.opAcquireAction(&mqttAcquireActionRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-acq", Op: "op-acq",
		Gen: al.rec.Gen, Tenure: al.rec.Tenure, Nonce: n, Acq: reg.Acq + 1, Kind: mqttActionPublish,
		Binding: mqttBindConnection(4242), Register: reg, RegisterSeq: rmsg.Sequence, Token: mqttNewInflightNonce(),
	}); err != nil {
		t.Fatalf("acquiring the transmission: %v", err)
	}
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testGen, n, mqttExchangeTerminal); err == nil ||
		!strings.Contains(err.Error(), "live action token") {
		t.Fatalf("an obligation was ended beside a register holding its output token: %v", err)
	}
	// The holder's drain clears the token; the fixture clears it in place.
	held, hmsg, _ := jsa.loadInflightRecord(regSubj)
	cleared := *held
	cleared.ActionToken, cleared.ActionClaim, cleared.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
	if _, err := jsa.storeInflightCAS(regSubj, hmsg.Sequence, &cleared); err != nil {
		t.Fatalf("clearing the token: %v", err)
	}
	// Ended and resolved, with its register still standing (no terminal cleanup yet).
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testGen, n, mqttExchangeTerminal); err != nil {
		t.Fatalf("ending %q: %v", n, err)
	}
	testResolve(t, e, n)

	ord, _, _ := jsa.findDutyInInventory(e.id, e.epoch, testGen, func(d *mqttDutyEntry) bool { return d.Ref == n })
	d, _, _ := jsa.loadDutyEntryAt(e.id, e.epoch, testGen, ord)
	if cls, err := jsa.classifyFloorPosition(e.id, e.epoch, testGen, d); cls != mqttFloorRelocate {
		t.Fatalf("an ended obligation whose register still names it classifies %d (%v), want relocate", cls, err)
	}
	smsg, _ := jsa.loadLastMsgFor(mqttSessStreamName, regSubj)
	if err := jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); err != nil {
		t.Fatalf("deleting the register: %v", err)
	}
	if cls, err := jsa.classifyFloorPosition(e.id, e.epoch, testGen, d); cls != mqttFloorReclaim {
		t.Fatalf("an ended, resolved obligation with no register classifies %d (%v), want reclaim", cls, err)
	}
}

// T-D. An abandoned delivery keeps its identifier, so its position is carried and never
// reclaimed, whatever else has gone (M3): no register, no edges, the duty Terminal.
func TestAnAbandonedDeliveryIsNeverReclaimed(t *testing.T) {
	e, done := testFloorEnv(t, "f112-abandoned")
	defer done()
	jsa := &e.asm.jsa
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	n := testFloorAdmit(t, e, 9)
	// The abandoned phase is written by its own kind beside a register this fixture does not
	// build, so the record is planted directly: the classifier is what is under test.
	x := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: testGen, Nonce: n, PI: 9, QoS: 1,
		Phase: mqttExchangeDeliveryAbandoned, AdmitTxn: "txn-x", Tenure: al.rec.Tenure,
		Abandoned: &mqttExchangeAbandonment{Reason: mqttAbandonReasonACLDeny, Tenure: al.rec.Tenure, Alloc: n, Acq: 1,
			Dur: "$MQTT_src", Seq: 1, Reply: testInflightAckSubject("$MQTT_src", 1, 1)},
	}
	body, _ := json.Marshal(x)
	if _, err := jsa.storeMsg(mqttExchangeSubject(getHash(e.id), testGen, n), -1, body); err != nil {
		t.Fatalf("planting the abandoned exchange: %v", err)
	}
	if _, _, err := jsa.loadOutboundExchange(e.id, e.epoch, testGen, n); err != nil {
		t.Fatalf("the planted abandoned exchange does not decode: %v", err)
	}
	testResolve(t, e, n)
	ord, _, _ := jsa.findDutyInInventory(e.id, e.epoch, testGen, func(d *mqttDutyEntry) bool { return d.Ref == n })
	d, _, _ := jsa.loadDutyEntryAt(e.id, e.epoch, testGen, ord)
	if d.State != mqttDutyTerminal {
		t.Fatalf("the abandoned delivery's duty is %d, want terminal", d.State)
	}
	if cls, err := jsa.classifyFloorPosition(e.id, e.epoch, testGen, d); cls != mqttFloorRelocate {
		t.Fatalf("an abandoned delivery classifies %d (%v): it holds its identifier and is never reclaimed", cls, err)
	}
}

// T-E. The traversal restarts when, and only when, the floor crosses a position it could not read
// (M10). Unrelated ledger movement — an admission — and a genuine hole inside the floor are
// UNCERTAINTY, not restarts.
func TestTraversalRestartsOnlyWhenTheFloorCrossesIt(t *testing.T) {
	e, done := testFloorEnv(t, "f112-restart")
	defer done()
	jsa := &e.asm.jsa
	for i := 0; i < 6; i++ {
		testEndAndResolve(t, e, testFloorAdmit(t, e, 10))
	}
	keep := testFloorAdmit(t, e, 2)

	// A FLOOR CROSSING UNDER THE TRAVERSAL: the first batched read finds the positions retired and
	// deleted by an advance that ran after the traversal read the ledger.
	var reads atomic.Int32
	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		if reads.Add(1) == 1 {
			led, _ := testFloorLedger(t, e)
			testAdvanceTo(t, e, led.Hi)
		}
		return e.asm.jsa.loadLastMsgForMulti(mqttSessStreamName, subjects)
	}
	e.asm.mu.Unlock()
	census, err := mqttTraverseGenerationInventory(jsa, e.asm, e.id, e.epoch, testGen, nil)
	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = nil
	e.asm.mu.Unlock()
	if err != nil || !census.complete || len(census.uncertain) != 0 || len(census.live) != 1 || census.live[0].Nonce != keep {
		t.Fatalf("a floor crossing was not absorbed by a restart: err %v uncertain %v live %d",
			err, census.uncertain, len(census.live))
	}
	if reads.Load() < 2 {
		t.Fatalf("the traversal read once (%d): a crossed position must restart it", reads.Load())
	}

	// A GENUINE HOLE inside the floor, with only an unrelated ledger write moving its sequence
	// meanwhile (the floor unchanged): one pass, the hole uncertain.
	ord, _, _ := jsa.findDutyInInventory(e.id, e.epoch, testGen, func(d *mqttDutyEntry) bool { return d.Ref == keep })
	smsg, _ := jsa.loadLastMsgFor(mqttSessStreamName, mqttDutySubject(getHash(e.id), testGen, ord))
	if err := jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); err != nil {
		t.Fatalf("making the hole: %v", err)
	}
	reads.Store(0)
	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		if reads.Add(1) == 1 {
			smsg, lerr := jsa.loadLastMsgFor(mqttSessStreamName, mqttGenLedgerSubject(getHash(e.id), testGen))
			if lerr != nil {
				t.Errorf("re-reading the ledger: %v", lerr)
			} else if _, serr := jsa.storeMsg(mqttGenLedgerSubject(getHash(e.id), testGen), -1, smsg.Data); serr != nil {
				t.Errorf("rewriting the ledger: %v", serr)
			}
		}
		return e.asm.jsa.loadLastMsgForMulti(mqttSessStreamName, subjects)
	}
	e.asm.mu.Unlock()
	census, err = mqttTraverseGenerationInventory(jsa, e.asm, e.id, e.epoch, testGen, nil)
	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = nil
	e.asm.mu.Unlock()
	hole := mqttDutySubject(getHash(e.id), testGen, ord)
	if err != nil || reads.Load() != 1 || len(census.uncertain) != 1 || census.uncertain[0] != hole {
		t.Fatalf("a genuine hole with only an admission moving the ledger: err %v reads %d uncertain %v",
			err, reads.Load(), census.uncertain)
	}
}

// T-F. The landing proof (M11). An admission whose attempt was positively ABORTED leaves a
// permanent outcome and no exchange; its absence is NOT a reclamation. A committed attempt is a
// proof whether its revision still stands (LANDED) or has been passed (SUPERSEDED).
func TestALandingProofIsACommittedAttempt(t *testing.T) {
	e, done := testFloorEnv(t, "f112-landing")
	defer done()
	jsa := &e.asm.jsa

	compose := func(pi uint16) (*mqttAuthorityTransition, string) {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		txn := mqttNewInflightNonce()
		x := &mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
			PI: pi, QoS: 1, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		tr, err := testAdmitExchangeTr(t, e, al.rec, al.seq, txn, "op-"+txn, x)
		if err != nil {
			t.Fatalf("composing: %v", err)
		}
		return tr, x.Nonce
	}

	aborted, abortedLin := compose(4)
	h, herr := mqttNewTransitionHandle(aborted)
	if herr != nil {
		t.Fatalf("freezing the handle: %v", herr)
	}
	if err := jsa.abortTransition(&h.attempt, nil); err != nil {
		t.Fatalf("aborting: %v", err)
	}
	if p := testProof(jsa, []mqttCustodyAttempt{{handle: h, lin: abortedLin}}, abortedLin); p != mqttLandingUnproven {
		t.Fatalf("an ABORTED attempt was taken as a landing proof (%d)", p)
	}
	if v := jsa.classifyExchangeAbsence(e.id, e.epoch, testGen, abortedLin,
		testProof(jsa, []mqttCustodyAttempt{{handle: h, lin: abortedLin}}, abortedLin)); v != mqttAbsenceUnproven {
		t.Fatalf("a never-landed admission's absence classifies %d, want unproven (the NAK stands)", v)
	}

	landed, landedLin := compose(5)
	lh, _ := mqttNewTransitionHandle(landed)
	if _, err := jsa.commitAuthorityTransition(landed); err != nil {
		t.Fatalf("committing: %v", err)
	}
	if p := testProof(jsa, []mqttCustodyAttempt{{handle: lh, lin: landedLin}}, landedLin); p != mqttLandingCommitted {
		t.Fatalf("a LANDED attempt is not a landing proof (%d)", p)
	}
	// SUPERSEDED: another transition passes its revision; the attempt still happened.
	testFloorAdmit(t, e, 6)
	if v := jsa.resolveTransitionHandle(lh); v != mqttTransitionSuperseded {
		t.Fatalf("fixture: the attempt resolves %d, want superseded", v)
	}
	if p := testProof(jsa, []mqttCustodyAttempt{{handle: lh, lin: landedLin}}, landedLin); p != mqttLandingCommitted {
		t.Fatalf("a SUPERSEDED attempt is not a landing proof (%d)", p)
	}
	// A proof for another lineage proves nothing about this one.
	if p := testProof(jsa, []mqttCustodyAttempt{{handle: lh, lin: landedLin}}, abortedLin); p != mqttLandingUnproven {
		t.Fatalf("an attempt that wrote another lineage was taken as this one's proof (%d)", p)
	}
}

// T-G. Recovery never retires a root while authorized deletions stand below a sealed floor (M14).
func TestARootRetiresOnlyWithItsRetiredPositionsDeleted(t *testing.T) {
	root := &mqttRecoveryRoot{ID: "c", Epoch: "e", Gen: testGen, SweepStart: 1, Resume: 11, Bound: 10}
	led := &mqttGenLedger{State: mqttGenLedgerSealed, Hi: 10, SealedHi: 10, Lo: 6, Del: 3}
	if mqttRootRetires(root, led) {
		t.Fatal("a root retired with retired positions still standing below its floor")
	}
	led.Del = 6
	if !mqttRootRetires(root, led) {
		t.Fatal("a root with every retired position deleted did not retire")
	}
}

// testPlantAdmittedDuty writes the duty entry an admission writes for exchange nonce — at the
// next ordinal, with the ledger's high-water moved to it — for a fixture that plants an exchange
// record directly. Since issue #114 every exchange enumeration is the inventory, so a planted
// obligation the inventory does not name is a shape no admission writes (the coverage rule) and
// no reader enumerates.
func testPlantAdmittedDuty(t *testing.T, jsa *mqttJSA, id, epoch, gen, nonce string) {
	t.Helper()
	led, _, err := jsa.loadGenLedger(id, epoch, gen)
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	if led == nil {
		led = &mqttGenLedger{ID: id, Epoch: epoch, Gen: gen, State: mqttGenLedgerOpen, Lo: 1, Del: 1}
	}
	// The duty names the identifier of the exchange it is planted beside, as the admission's
	// boundary binds it; every caller plants the exchange first.
	x, _, xerr := jsa.loadOutboundExchange(id, epoch, gen, nonce)
	if xerr != nil || x == nil {
		t.Fatalf("planting the duty of exchange %q, which does not read: %v", nonce, xerr)
	}
	next := *led
	next.Hi++
	d := &mqttDutyEntry{ID: id, Epoch: epoch, Gen: gen, Ord: next.Hi, Kind: mqttDutyExchange, Ref: nonce,
		PI: x.PI, State: mqttDutyPending}
	dbody, _ := json.Marshal(d)
	if _, err := jsa.storeMsg(mqttDutySubject(getHash(id), gen, next.Hi), -1, dbody); err != nil {
		t.Fatalf("planting the duty: %v", err)
	}
	lbody, _ := json.Marshal(&next)
	if _, err := jsa.storeMsg(mqttGenLedgerSubject(getHash(id), gen), -1, lbody); err != nil {
		t.Fatalf("moving the ledger: %v", err)
	}
}

// testProof is the landing proof, whether or not it is decided.
func testProof(jsa *mqttJSA, attempts []mqttCustodyAttempt, lin string) mqttLandingProof {
	p, _ := jsa.mqttLandingProofDecided(attempts, lin)
	return p
}
