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
	"testing"
)

// T8, the write boundary (F-105). Every legal batch shape is accepted and ROUND-TRIPS through
// the production decoders, and every refusal row is refused with nothing stored. The round-trip
// is load-bearing: the two production defects the plan's nine clean rounds missed were both a
// legal-looking batch that the DECODER refused on read (an empty list marshalled as JSON null),
// so a boundary test that only checked the write's own return value would have missed them too.
//
// It drives commitSharedLifecycleBatch directly against a real $MQTT_shared stream, because
// the boundary compares the composer's priors with the STORE. Legal batches are built by the
// composers (the realistic path); refusal rows hand-build the illegal member, which is the only
// way to reach a rule the composer would never violate.

type testBoundaryEnv struct {
	e     *testOrphanEnv
	epoch string
	id    string
	gen   string
	qkey  string
}

func testNewBoundaryEnv(t *testing.T) *testBoundaryEnv {
	t.Helper()
	e := testNewOrphanEnv(t, "boundary-client")
	return &testBoundaryEnv{
		e:     e,
		epoch: e.asm.sessStreamEpoch,
		id:    "boundary-client",
		gen:   mqttNewSessGen(),
		qkey:  mqttSharedGroupKey("g", "foo"),
	}
}

func (x *testBoundaryEnv) commit(b *mqttSharedBatch) mqttSharedWriteResult {
	return x.e.asm.jsa.commitSharedLifecycleBatch(b)
}

func (x *testBoundaryEnv) group(t *testing.T) mqttSharedGroupLoad {
	t.Helper()
	return x.e.asm.jsa.loadSharedGroup(x.epoch, x.qkey)
}

func (x *testBoundaryEnv) ledger(t *testing.T) mqttSharedLedgerLoad {
	t.Helper()
	return x.e.asm.jsa.loadSharedLedger(x.id, x.epoch, x.gen)
}

// landed asserts a batch is accepted, and then that BOTH records it wrote read back VALID
// through the production decoders — the assertion that catches a member the store accepts but
// the decoder refuses.
func (x *testBoundaryEnv) landed(t *testing.T, what string, b *mqttSharedBatch) {
	t.Helper()
	res := x.commit(b)
	if res.Outcome != mqttSharedWriteLanded {
		t.Fatalf("%s: expected the batch to land, got %v (%v)", what, res.Outcome, res.Err)
	}
	if b.hasGroup {
		if g := x.group(t); g.status != mqttOutboundValid {
			t.Fatalf("%s: the group record it wrote reads back %v (%v)", what, g.status, g.err)
		}
	}
	if b.hasLedger {
		if l := x.ledger(t); l.status != mqttOutboundValid {
			t.Fatalf("%s: the ledger it wrote reads back %v (%v)", what, l.status, l.err)
		}
	}
}

// refused asserts a batch is rejected and stores nothing that changes the current reads.
func (x *testBoundaryEnv) refused(t *testing.T, what string, b *mqttSharedBatch) {
	t.Helper()
	res := x.commit(b)
	if res.Outcome == mqttSharedWriteLanded {
		t.Fatalf("%s: the boundary accepted a batch it must refuse", what)
	}
}

// install lands the tenure's installation over the absent ledger: since F-105 8b the ONE
// creator of a ledger, which every join then steps from.
func (x *testBoundaryEnv) install(t *testing.T, tenure mqttSharedTenure) {
	t.Helper()
	b, err := mqttComposeSharedInstall(x.epoch, x.id, x.gen, tenure, x.ledger(t))
	if err != nil {
		t.Fatalf("composing the installation: %v", err)
	}
	x.landed(t, "install", b)
}

func testTenure(seq uint64) mqttSharedTenure {
	return mqttSharedTenure{Seq: seq, ID: mqttNewInflightNonce()}
}

func TestSharedBoundaryLegalShapesRoundTrip(t *testing.T) {
	x := testNewBoundaryEnv(t)
	tenure := testTenure(10)
	max := mqttSharedLedgerMaxAcqs(mqttDefaultMaxAckPending)
	acqA := mqttNewInflightNonce()
	acqB := mqttNewInflightNonce()

	// The INSTALLATION creates the ledger (F-105 8b); the first JOIN creates the group at
	// incarnation 1. The retiring list is written as [] — the shape defect I1 turned into JSON
	// null.
	x.install(t, tenure)
	joinA, err := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, acqA, tenure,
		mqttSharedGroupLoad{status: mqttOutboundAbsent}, x.ledger(t), max)
	if err != nil {
		t.Fatalf("composing the first join: %v", err)
	}
	x.landed(t, "first join over an installed ledger", joinA)
	if g := x.group(t); g.rec.Live == nil || g.rec.Live.Inc != 1 || g.rec.Live.Holders != 1 || len(g.rec.Retiring) != 0 || g.rec.Next != 2 {
		t.Fatalf("the first join did not mount incarnation 1: %+v", g.rec)
	}

	// A SECOND JOIN onto the live incarnation: holders 1 -> 2, over an EMPTY retiring list
	// (the exact I1 shape — the copy of [] must not become null).
	g := x.group(t)
	l := x.ledger(t)
	joinB, err := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, acqB, tenure, g, l, max)
	if err != nil {
		t.Fatalf("composing the second join: %v", err)
	}
	x.landed(t, "second join over empty retiring", joinB)
	if g := x.group(t); g.rec.Live.Holders != 2 {
		t.Fatalf("the second join did not reach two holders: %+v", g.rec)
	}

	// CLOSE B (F-105 7e): the release fence's first step, the closed list gaining exactly B.
	l = x.ledger(t)
	closeB, err := mqttComposeSharedClose(x.epoch, x.id, x.gen, mqttSharedAcqRef{Group: x.qkey, Inc: 1, Acq: acqB}, l)
	if err != nil || closeB == nil {
		t.Fatalf("composing the close of B: %v", err)
	}
	x.landed(t, "close of B", closeB)
	if l := x.ledger(t); len(l.rec.Closed) != 1 || l.rec.Closed[0] != acqB || len(l.rec.Acqs) != 2 {
		t.Fatalf("the close did not close exactly B: %+v", l.rec)
	}

	// RELEASE B: holders 2 -> 1, the ledger back to one entry (emptying toward the I1 shape),
	// and B leaves the closed list with it.
	g = x.group(t)
	l = x.ledger(t)
	relB, err := mqttComposeSharedRelease(x.epoch, x.id, x.gen,
		mqttSharedAcqRef{Group: x.qkey, Inc: 1, Acq: acqB}, g, l)
	if err != nil {
		t.Fatalf("composing the release of B: %v", err)
	}
	x.landed(t, "release to one holder", relB)
	if g := x.group(t); g.rec.Live == nil || g.rec.Live.Holders != 1 {
		t.Fatalf("the release did not leave one holder: %+v", g.rec)
	}
	if l := x.ledger(t); len(l.rec.Closed) != 0 {
		t.Fatalf("the release left its acquisition in the closed list: %+v", l.rec)
	}

	// CLOSE A, then RELEASE A, the LAST holder: live -> null, retiring gains incarnation 1, the ledger's acqs
	// goes to [] (I1) and live goes to null (I2). Both are the shapes the decoders refuse if a
	// composer writes them as JSON null.
	closeA, err := mqttComposeSharedClose(x.epoch, x.id, x.gen, mqttSharedAcqRef{Group: x.qkey, Inc: 1, Acq: acqA}, x.ledger(t))
	if err != nil || closeA == nil {
		t.Fatalf("composing the close of A: %v", err)
	}
	x.landed(t, "close of A", closeA)
	g = x.group(t)
	l = x.ledger(t)
	relA, err := mqttComposeSharedRelease(x.epoch, x.id, x.gen,
		mqttSharedAcqRef{Group: x.qkey, Inc: 1, Acq: acqA}, g, l)
	if err != nil {
		t.Fatalf("composing the release of A: %v", err)
	}
	x.landed(t, "release of the last holder", relA)
	g = x.group(t)
	if g.rec.Live != nil {
		t.Fatalf("the last release left live non-null: %+v", g.rec)
	}
	if len(g.rec.Retiring) != 1 || g.rec.Retiring[0] != 1 {
		t.Fatalf("the last release did not move incarnation 1 into retiring: %+v", g.rec)
	}
	if l := x.ledger(t); len(l.rec.Acqs) != 0 {
		t.Fatalf("the last release left the ledger non-empty: %+v", l.rec)
	}

	// SEAL the emptied ledger: state sealed, acqs [] preserved (I1 on the seal path).
	l = x.ledger(t)
	seal, err := mqttComposeSharedSeal(x.epoch, x.id, x.gen, l)
	if err != nil {
		t.Fatalf("composing the seal: %v", err)
	}
	x.landed(t, "seal of the emptied ledger", seal)
	if l := x.ledger(t); l.rec.State != mqttSharedLedgerSealed || len(l.rec.Acqs) != 0 {
		t.Fatalf("the seal did not leave sealed([]): %+v", l.rec)
	}

	// RETIRE-DONE: remove incarnation 1 from retiring.
	g = x.group(t)
	rd, err := mqttComposeSharedRetireDone(x.epoch, x.qkey, 1, g)
	if err != nil {
		t.Fatalf("composing the retire-done: %v", err)
	}
	x.landed(t, "retire-done", rd)
	if g := x.group(t); len(g.rec.Retiring) != 0 {
		t.Fatalf("the retire-done did not clear the retiring entry: %+v", g.rec)
	}
}

func TestSharedBoundaryInstallAndSealOverAbsence(t *testing.T) {
	// INSTALL over absence (a resume of a generation that never acquired, and the abort init
	// fence): open([]) under the tenure.
	x := testNewBoundaryEnv(t)
	inst, err := mqttComposeSharedInstall(x.epoch, x.id, x.gen, testTenure(7),
		mqttSharedLedgerLoad{status: mqttOutboundAbsent})
	if err != nil {
		t.Fatalf("composing the installation: %v", err)
	}
	x.landed(t, "install over absence", inst)
	if l := x.ledger(t); l.rec.State != mqttSharedLedgerOpen || len(l.rec.Acqs) != 0 {
		t.Fatalf("the installation did not write open([]): %+v", l.rec)
	}

	// SEAL over absence (a teardown of a generation that never acquired): sealed([]) with no
	// tenure. Separate generation so the ledger is genuinely absent.
	y := testNewBoundaryEnv(t)
	seal, err := mqttComposeSharedSeal(y.epoch, y.id, y.gen, mqttSharedLedgerLoad{status: mqttOutboundAbsent})
	if err != nil {
		t.Fatalf("composing the empty seal: %v", err)
	}
	y.landed(t, "seal over absence", seal)
	if l := y.ledger(t); l.rec.State != mqttSharedLedgerSealed || !l.rec.Tenure.isZero() || len(l.rec.Acqs) != 0 {
		t.Fatalf("the empty seal did not write sealed([]) with no tenure: %+v", l.rec)
	}
}

// TestSharedBoundaryRefusesTheNullLists is the direct regression for I1 and I2: a group record
// with a nil retiring list or a ledger with a nil acqs list marshals as JSON null, which the
// decoder refuses; the boundary must refuse to SEND one, so a composer bug cannot reach the
// store. Mutation M-I1: delete the `g.Retiring == nil` / `l.Acqs == nil` rules in the
// validators, and these two rows land a record the next read cannot decode.
func TestSharedBoundaryRefusesTheNullLists(t *testing.T) {
	x := testNewBoundaryEnv(t)
	tenure := testTenure(3)

	// A group member with a NIL retiring list, otherwise well-formed, created over absence.
	nilRetiring := &mqttSharedBatch{
		Shape: mqttSharedShapeJoin, Epoch: x.epoch, Qkey: x.qkey, ID: x.id, Gen: x.gen, Tenure: tenure,
		hasGroup: true, hasLedger: true, LedgerMax: mqttSharedLedgerMaxAcqs(mqttDefaultMaxAckPending),
		Acq:        mqttSharedAcqRef{Group: x.qkey, Inc: 1, Acq: mqttNewInflightNonce()},
		GroupNext:  &mqttSharedGroupRecord{Epoch: x.epoch, Group: x.qkey, Live: &mqttSharedLive{Inc: 1, Holders: 1}, Retiring: nil, Next: 2},
		LedgerNext: &mqttSharedLedger{Epoch: x.epoch, ID: x.id, Gen: x.gen, State: mqttSharedLedgerOpen, Tenure: tenure, Acqs: []mqttSharedAcqRef{{Group: x.qkey, Inc: 1, Acq: "x"}}, Closed: []string{}},
	}
	nilRetiring.Acq = nilRetiring.LedgerNext.Acqs[0]
	x.refused(t, "a nil retiring list", nilRetiring)

	// A ledger member with a NIL acqs list.
	nilAcqs := &mqttSharedBatch{
		Shape: mqttSharedShapeSeal, Epoch: x.epoch, ID: x.id, Gen: x.gen, hasLedger: true,
		LedgerNext: &mqttSharedLedger{Epoch: x.epoch, ID: x.id, Gen: x.gen, State: mqttSharedLedgerSealed, Acqs: nil, Closed: []string{}},
	}
	x.refused(t, "a nil acqs list", nilAcqs)
}

// TestSharedBoundaryRefusesTheInvariantViolations is the refusal matrix: each row hand-builds an
// illegal step and asserts the boundary refuses it. Each is paired with a mutation that removes
// exactly its rule (mutation-ledger.md, rows M8-*).
func TestSharedBoundaryRefusesTheInvariantViolations(t *testing.T) {
	x := testNewBoundaryEnv(t)
	tenure := testTenure(5)
	max := mqttSharedLedgerMaxAcqs(mqttDefaultMaxAckPending)

	// Land a real join so there is a standing prior to step from illegally.
	acqA := mqttNewInflightNonce()
	x.install(t, tenure)
	joinA, err := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, acqA, tenure,
		mqttSharedGroupLoad{status: mqttOutboundAbsent}, x.ledger(t), max)
	if err != nil {
		t.Fatalf("composing the standing join: %v", err)
	}
	x.landed(t, "standing join", joinA)
	g := x.group(t)
	l := x.ledger(t)

	mkJoin := func(mut func(b *mqttSharedBatch)) *mqttSharedBatch {
		b, err := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, mqttNewInflightNonce(), tenure, g, l, max)
		if err != nil {
			t.Fatalf("composing a join to mutate: %v", err)
		}
		mut(b)
		return b
	}

	// COUNT DISCIPLINE: the group's holder delta must equal the ledger's entry delta. Add the
	// ledger entry but leave the holder count unchanged.
	x.refused(t, "count delta != entry delta", mkJoin(func(b *mqttSharedBatch) {
		b.GroupNext.Live.Holders = g.rec.Live.Holders
	}))

	// A join naming a tenure the prior ledger does not carry.
	x.refused(t, "join under a foreign tenure", func() *mqttSharedBatch {
		other := testTenure(6)
		b, cerr := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, mqttNewInflightNonce(), other, g, l, max)
		if cerr != nil {
			// The composer itself refuses; synthesize the illegal batch to reach the boundary.
			b = mkJoin(func(bb *mqttSharedBatch) {})
			b.Tenure = other
			b.LedgerNext.Tenure = other
		}
		return b
	}())

	// A join whose ledger member DROPS another entry (does not preserve the base).
	x.refused(t, "join dropping another entry", mkJoin(func(b *mqttSharedBatch) {
		b.LedgerNext.Acqs = []mqttSharedAcqRef{b.Acq}
	}))

	// A retiring append without moving live to null (retire while still live).
	x.refused(t, "retiring append while live", mkJoin(func(b *mqttSharedBatch) {
		b.GroupNext.Retiring = []uint64{1}
		b.GroupNext.Next = 2
	}))

	// A two-member batch that is neither a join nor a release (shape join, but the group loses
	// a holder while the ledger gains one — the shapes disagree).
	x.refused(t, "group and ledger disagree", mkJoin(func(b *mqttSharedBatch) {
		b.GroupNext.Live.Holders = g.rec.Live.Holders - 1
	}))

	// A release changing the tenure. Last, because it first lands the close the release fence
	// requires (F-105 7e), which moves the ledger past the prior every row above composed from;
	// closed, the release is otherwise legal, so the tenure rule alone refuses it.
	closeA, err := mqttComposeSharedClose(x.epoch, x.id, x.gen, mqttSharedAcqRef{Group: x.qkey, Inc: 1, Acq: acqA}, l)
	if err != nil || closeA == nil {
		t.Fatalf("composing the close of A: %v", err)
	}
	x.landed(t, "close of A", closeA)
	relTenure, err := mqttComposeSharedRelease(x.epoch, x.id, x.gen,
		mqttSharedAcqRef{Group: x.qkey, Inc: 1, Acq: acqA}, x.group(t), x.ledger(t))
	if err != nil {
		t.Fatalf("composing a release to mutate: %v", err)
	}
	relTenure.LedgerNext.Tenure = testTenure(99)
	x.refused(t, "release changing the tenure", relTenure)
}

// TestSharedBoundaryRefusesInstallationRegression covers the tenure table's refusals: an
// installation over a NEWER tenure and over a SEAL. Mutations M8-install-newer and
// M8-install-seal.
func TestSharedBoundaryRefusesInstallationRegression(t *testing.T) {
	x := testNewBoundaryEnv(t)

	// Stand up open(T=10, []).
	inst, err := mqttComposeSharedInstall(x.epoch, x.id, x.gen, testTenure(10),
		mqttSharedLedgerLoad{status: mqttOutboundAbsent})
	if err != nil {
		t.Fatalf("composing the first installation: %v", err)
	}
	x.landed(t, "install T=10", inst)

	// An installation of an OLDER tenure (7) over the standing 10 is refused (superseded).
	l := x.ledger(t)
	older, oerr := mqttComposeSharedInstall(x.epoch, x.id, x.gen, testTenure(7), l)
	if oerr == nil {
		x.refused(t, "install of an older tenure", older)
	}

	// Seal it, then an installation over the seal is refused (closing).
	l = x.ledger(t)
	seal, err := mqttComposeSharedSeal(x.epoch, x.id, x.gen, l)
	if err != nil {
		t.Fatalf("composing the seal: %v", err)
	}
	x.landed(t, "seal before install", seal)
	l = x.ledger(t)
	overSeal, serr := mqttComposeSharedInstall(x.epoch, x.id, x.gen, testTenure(20), l)
	if serr == nil {
		x.refused(t, "install over a seal", overSeal)
	}
}

// TestSharedBoundaryDecoderRefusesCorruptRecords isolates the DECODER rules — the last line of
// defense a record read must pass, and the sole guard against a stored body no write step
// vetted (a corrupt record, or one an older broker wrote). Each row plants a raw body and
// asserts the load reads CORRUPT. The write-boundary test above cannot isolate these: count
// discipline and the step rules refuse the same shapes first, so a mutation of a validator rule
// survives there. Mutations M8-live0 (live with zero holders), M8-retdup (a retiring incarnation
// twice), M8-ordinal (an ordinal outside [1,next)) each fail exactly the paired row here.
func TestSharedBoundaryDecoderRefusesCorruptRecords(t *testing.T) {
	x := testNewBoundaryEnv(t)
	plantGroup := func(t *testing.T, body []byte) mqttSharedGroupLoad {
		t.Helper()
		if _, err := x.e.asm.jsa.storeMsg(mqttSharedGroupSubject(x.qkey), 0, body); err != nil {
			t.Fatalf("planting the group body: %v", err)
		}
		return x.group(t)
	}

	// live with zero holders: the invariant live != null => holders >= 1.
	if g := plantGroup(t, []byte(`{"epoch":"`+x.epoch+`","group":"`+x.qkey+`","live":{"inc":1,"holders":0},"retiring":[],"next":2}`)); g.status != mqttOutboundCorrupt {
		t.Fatalf("a live incarnation with zero holders read %v, want corrupt", g.status)
	}
	// a retiring incarnation listed twice.
	if g := plantGroup(t, []byte(`{"epoch":"`+x.epoch+`","group":"`+x.qkey+`","live":null,"retiring":[1,1],"next":2}`)); g.status != mqttOutboundCorrupt {
		t.Fatalf("a duplicated retiring incarnation read %v, want corrupt", g.status)
	}
	// an ordinal outside [1, next): live.inc >= next.
	if g := plantGroup(t, []byte(`{"epoch":"`+x.epoch+`","group":"`+x.qkey+`","live":{"inc":5,"holders":1},"retiring":[],"next":2}`)); g.status != mqttOutboundCorrupt {
		t.Fatalf("a live incarnation outside the allocated ordinals read %v, want corrupt", g.status)
	}
	// a null retiring list (I1 shape) on read.
	if g := plantGroup(t, []byte(`{"epoch":"`+x.epoch+`","group":"`+x.qkey+`","live":null,"retiring":null,"next":2}`)); g.status != mqttOutboundCorrupt {
		t.Fatalf("a null retiring list read %v, want corrupt", g.status)
	}
	// an absent live member (mandatory even though it may be null).
	if g := plantGroup(t, []byte(`{"epoch":"`+x.epoch+`","group":"`+x.qkey+`","retiring":[],"next":2}`)); g.status != mqttOutboundCorrupt {
		t.Fatalf("an absent live member read %v, want corrupt", g.status)
	}

	// A ledger with a null acqs list (I1 shape), and one open with no tenure.
	plantLedger := func(t *testing.T, body []byte) mqttSharedLedgerLoad {
		t.Helper()
		if _, err := x.e.asm.jsa.storeMsg(mqttSharedLedgerSubject(getHash(x.id), x.gen), 0, body); err != nil {
			t.Fatalf("planting the ledger body: %v", err)
		}
		return x.ledger(t)
	}
	if l := plantLedger(t, []byte(`{"epoch":"`+x.epoch+`","id":"`+x.id+`","gen":"`+x.gen+`","state":1,"tenure":{"seq":1,"id":"`+mqttNewInflightNonce()+`"},"acqs":null,"closed":[]}`)); l.status != mqttOutboundCorrupt {
		t.Fatalf("a null acqs list read %v, want corrupt", l.status)
	}
	if l := plantLedger(t, []byte(`{"epoch":"`+x.epoch+`","id":"`+x.id+`","gen":"`+x.gen+`","state":1,"tenure":{"seq":0,"id":""},"acqs":[],"closed":[]}`)); l.status != mqttOutboundCorrupt {
		t.Fatalf("an open ledger with no tenure read %v, want corrupt", l.status)
	}

	// THE CLOSED LIST (F-105 7e): mandatory, never null, a subset of acqs, no duplicates.
	acq := mqttNewInflightNonce()
	base := `{"epoch":"` + x.epoch + `","id":"` + x.id + `","gen":"` + x.gen + `","state":1,"tenure":{"seq":1,"id":"` +
		mqttNewInflightNonce() + `"},"acqs":[{"group":"` + x.qkey + `","inc":1,"acq":"` + acq + `"}]`
	for name, body := range map[string]string{
		"a ledger with no closed member":  base + `}`,
		"a null closed list":              base + `,"closed":null}`,
		"a closed nonce it does not list": base + `,"closed":["` + mqttNewInflightNonce() + `"]}`,
		"a closed nonce listed twice":     base + `,"closed":["` + acq + `","` + acq + `"]}`,
	} {
		if l := plantLedger(t, []byte(body)); l.status != mqttOutboundCorrupt {
			t.Errorf("%s read %v, want corrupt", name, l.status)
		}
	}
	if l := plantLedger(t, []byte(base+`,"closed":["`+acq+`"]}`)); l.status != mqttOutboundValid {
		t.Fatalf("a ledger closing a listed acquisition read %v (%v), want valid", l.status, l.err)
	}
}

// T1 (F-105 7e): the close shape and the release's closed-or-sealed rule at the write boundary.
// Mutations M1 (the release rule removed), M11 (a release keeping its nonce in the closed list).
func TestSharedBoundaryCloseShape(t *testing.T) {
	x := testNewBoundaryEnv(t)
	tenure := testTenure(4)
	max := mqttSharedLedgerMaxAcqs(mqttDefaultMaxAckPending)
	acqA, acqB := mqttNewInflightNonce(), mqttNewInflightNonce()
	x.install(t, tenure)
	joinA, err := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, acqA, tenure,
		mqttSharedGroupLoad{status: mqttOutboundAbsent}, x.ledger(t), max)
	if err != nil {
		t.Fatal(err)
	}
	x.landed(t, "join A", joinA)
	joinB, err := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, acqB, tenure, x.group(t), x.ledger(t), max)
	if err != nil {
		t.Fatal(err)
	}
	x.landed(t, "join B", joinB)
	refA := mqttSharedAcqRef{Group: x.qkey, Inc: 1, Acq: acqA}
	refB := mqttSharedAcqRef{Group: x.qkey, Inc: 1, Acq: acqB}

	// A release from an OPEN ledger that has not closed the acquisition is refused.
	rel, err := mqttComposeSharedRelease(x.epoch, x.id, x.gen, refA, x.group(t), x.ledger(t))
	if err != nil {
		t.Fatal(err)
	}
	x.refused(t, "release of an unclosed acquisition from an open ledger", rel)

	mkClose := func(ref mqttSharedAcqRef, mut func(b *mqttSharedBatch)) *mqttSharedBatch {
		b, cerr := mqttComposeSharedClose(x.epoch, x.id, x.gen, ref, x.ledger(t))
		if cerr != nil || b == nil {
			t.Fatalf("composing a close: %v", cerr)
		}
		mut(b)
		return b
	}
	x.refused(t, "a close closing two acquisitions", mkClose(refA, func(b *mqttSharedBatch) {
		b.LedgerNext.Closed = []string{acqA, acqB}
	}))
	x.refused(t, "a close dropping an acquisition", mkClose(refA, func(b *mqttSharedBatch) {
		b.LedgerNext.Acqs = []mqttSharedAcqRef{refA}
	}))
	x.refused(t, "a close naming another incarnation", mkClose(refA, func(b *mqttSharedBatch) {
		b.Acq.Inc = 2
	}))
	x.landed(t, "close A", mkClose(refA, func(*mqttSharedBatch) {}))
	if again, cerr := mqttComposeSharedClose(x.epoch, x.id, x.gen, refA, x.ledger(t)); cerr != nil || again != nil {
		t.Fatalf("a close of an already-closed acquisition composes %v, %v; want nothing", again, cerr)
	}

	// A join neither closes nor reopens anything.
	acqC := mqttNewInflightNonce()
	joinC, err := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, acqC, tenure, x.group(t), x.ledger(t), max)
	if err != nil {
		t.Fatal(err)
	}
	reopen := *joinC
	reopenNext := *joinC.LedgerNext
	reopenNext.Closed = []string{}
	reopen.LedgerNext = &reopenNext
	x.refused(t, "a join reopening a closed acquisition", &reopen)

	// The release of the closed acquisition lands and leaves the closed list with it.
	rel, err = mqttComposeSharedRelease(x.epoch, x.id, x.gen, refA, x.group(t), x.ledger(t))
	if err != nil {
		t.Fatal(err)
	}
	keep := *rel
	keepNext := *rel.LedgerNext
	keepNext.Closed = []string{acqA}
	keep.LedgerNext = &keepNext
	x.refused(t, "a release keeping its nonce in the closed list", &keep)
	x.landed(t, "release of the closed acquisition", rel)
	if l := x.ledger(t); len(l.rec.Closed) != 0 || len(l.rec.Acqs) != 1 {
		t.Fatalf("after the release: %+v", l.rec)
	}

	// SEALED: closed generation-wide — a close is refused, and the release needs no close.
	seal, err := mqttComposeSharedSeal(x.epoch, x.id, x.gen, x.ledger(t))
	if err != nil {
		t.Fatal(err)
	}
	x.landed(t, "seal", seal)
	sealedClose := &mqttSharedBatch{Shape: mqttSharedShapeClose, Epoch: x.epoch, ID: x.id, Gen: x.gen, Acq: refB,
		hasLedger: true}
	l := x.ledger(t)
	sealedClose.LedgerPrior, sealedClose.LedgerSeq, sealedClose.ledgerExpect = l.rec, l.seq, l.seq
	sn := *l.rec
	sn.Acqs = mqttSharedCopyAcqs(l.rec.Acqs)
	sn.Closed = []string{acqB}
	sealedClose.LedgerNext = &sn
	x.refused(t, "a close over a sealed ledger", sealedClose)
	relB, err := mqttComposeSharedRelease(x.epoch, x.id, x.gen, refB, x.group(t), x.ledger(t))
	if err != nil {
		t.Fatal(err)
	}
	x.landed(t, "a sealed release with no close", relB)
}
