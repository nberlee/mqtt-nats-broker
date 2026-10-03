package mqtt

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Repairing the grant a recognised forked record may be missing: a CANDIDATE delta at
// mint time, filtered against the survivor at merge time.
//
// Every fixture here stages THREE records — the parent P, our child X, and a survivor Y
// that is X's sibling — because that is the only shape the repair is defined for, and
// because a two-record fixture cannot tell a three-way merge from a union. The union is the
// trap: with P = {a}, X = {a,b} and a survivor Y = {} carrying an acknowledged UNSUBSCRIBE
// of a, a union yields {a,b} and resurrects a. The counterexample is asserted verbatim
// below.
//
// The records are produced by the ordinary writer wherever a test can reach it. Where a
// unit test builds an mqttPersistedSession by hand it is a shape the writer demonstrably
// produces, and the paired comment says which writer produces it — a fixture that only
// exists by hand-editing stored JSON would enshrine a format the compatibility boundary
// refuses.

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// testForkRepairEnv is a broker that has reached a RECOGNISED fork with a capsule minted: the
// record chain is R1 -> R2 (the parent P) -> R3 (ours, X), a body claiming P has replaced X, and
// the local session has been disposed exactly as production disposes it.
type testForkRepairEnv struct {
	*testRejectionEnv
	// parentRec is P's content, kept so a survivor can be given the SAME grant — the one
	// shape the conflict predicate admits.
	parentRec mqttPersistedSession
	ackWID    string
	ackPWID   string
}

// testStageForkedGrant writes the three records and drives the definite-rejection path to a
// forked verdict, which is what mints.
//
// base mutates the session before P is written; ours mutates it before X is written.
// survivor rewrites the sibling's content and defaults to "identical to P".
func testStageForkedGrant(t *testing.T, clientID string, base, ours func(sess *mqttSession),
	survivor func(ps *mqttPersistedSession, parent *mqttPersistedSession)) *testForkRepairEnv {

	t.Helper()
	e := testNewRejectionEnv(t, clientID)
	e.dropPersistObservations(t, e.idHash)

	if base != nil {
		e.sess.mu.Lock()
		base(e.sess)
		e.sess.mu.Unlock()
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the PARENT record: %v", err)
	}
	e.sess.mu.Lock()
	parentRec := e.sess.buildPersistedSessionLocked()
	e.sess.mu.Unlock()

	if ours != nil {
		e.sess.mu.Lock()
		ours(e.sess)
		e.sess.mu.Unlock()
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist OUR record (the one the fork erases): %v", err)
	}
	e.sess.mu.Lock()
	ackWID, ackPWID := e.sess.wid, e.sess.pwid
	e.sess.mu.Unlock()
	if ackWID == _EMPTY_ || ackPWID == _EMPTY_ {
		t.Fatalf("fixture: the acknowledged pair must be complete, got (%q,%q)", ackWID, ackPWID)
	}

	// The survivor: a body CLAIMING P as its parent, so classifyRecordLineage reports it
	// as not demonstrably a successor of X. Its content defaults to P's, which is the
	// survivor whose RECORDED grant equals the retained baseline — the repairable side of
	// the conflict predicate, not a survivor proven to have decided nothing.
	testLandSibling(t, e.testInflightEnv, func(ps *mqttPersistedSession) {
		testCopyGrantInto(ps, &parentRec)
		if survivor != nil {
			survivor(ps, &parentRec)
		}
	})

	e.rejectSave(t, e.sess)
	if got := e.sess.settleRecordRejection(e.asm); got != mqttRejectionForked {
		t.Fatalf("the fixture must reach a FORKED verdict, got %v", got)
	}
	return &testForkRepairEnv{testRejectionEnv: e, parentRec: parentRec, ackWID: ackWID, ackPWID: ackPWID}
}

// testCopyGrantInto gives ps the grant of src, leaving its identity alone. Every member the
// snapshot reads, so a survivor built this way really is grant-equal to src.
func testCopyGrantInto(ps, src *mqttPersistedSession) {
	ps.Subs, ps.NoLocal, ps.RetainAsPublished, ps.SubIDs = src.Subs, src.NoLocal, src.RetainAsPublished, src.SubIDs
	ps.Cons, ps.Drains, ps.PubRel = src.Cons, src.Drains, src.PubRel
	ps.CancelledRegs, ps.SharedSubs = src.CancelledRegs, src.SharedSubs
}

// capsule returns the minted repair, failing if there is none.
func (e *testRejectionEnv) capsule(t *testing.T) *mqttForkedGrant {
	t.Helper()
	e.asm.mu.Lock()
	defer e.asm.mu.Unlock()
	g := e.asm.forkedGrants[e.clientID]
	if g == nil {
		t.Fatal("a forked verdict must mint a repair capsule")
	}
	return g
}

func (e *testRejectionEnv) capsuleOrNil() *mqttForkedGrant {
	e.asm.mu.Lock()
	defer e.asm.mu.Unlock()
	return e.asm.forkedGrants[e.clientID]
}

// reconnect restores the surviving record through the SAME account manager — the capsule is
// memory, so only this broker can apply it — and runs the reclaim behind a record
// transaction, exactly as the accept block does.
func (e *testForkRepairEnv) reconnect(t *testing.T) *mqttSession {
	t.Helper()
	sess, present, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("restore the surviving record: %v", err)
	}
	if !present {
		t.Fatal("the surviving record must be resumed, not replaced by a fresh session")
	}
	testClaimCapsuleDurables(t, e.testRejectionEnv, sess)
	tok := sess.beginRecordTx()
	if ferr := e.asm.reassertForkedGrantInto(sess, tok); ferr != nil {
		t.Fatalf("the fork repair refused: %v", ferr)
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	return sess
}

// testClaimCapsuleDurables gives every consumer our branch names what production gives it: a LIVE
// name-ownership and a LIVE, undecided source resource of the generation (F-105 7f). Every source
// consumer is claimed before it is created, and fork repair reinstates only a consumer that still
// serves; these fixtures predate the claim and name bare durables. A name that already stands is
// left as it is, so a test that decided a consumer keeps its decision.
func testClaimCapsuleDurables(t *testing.T, e *testRejectionEnv, sess *mqttSession) {
	t.Helper()
	g := e.capsuleOrNil()
	if g == nil {
		return
	}
	jsa := &e.asm.jsa
	ent := testServingEntitlement(t, sess)
	for _, ch := range g.changes {
		for sid := range mqttSidsOf(nil, ch.ours) {
			st := ch.ours.sidState(sid)
			for _, name := range []string{mqttForkConDurable(st), mqttDrainDurable(st.drain)} {
				if name == _EMPTY_ {
					continue
				}
				own, _, err := jsa.loadNameOwnershipPhysical(e.asm.sessStreamEpoch, mqttOwnedStreamMsgs, name)
				if err != nil {
					t.Fatalf("reading the ownership of %q: %v", name, err)
				}
				if own != nil {
					continue
				}
				if _, perr := jsa.opProvisionResource(sess.id, e.asm.sessStreamEpoch, mqttNewInflightNonce(),
					"op-provision-source", ent.Gen, ent.Tenure, mqttNewInflightNonce(), name,
					mqttResourceSourceConsumer); perr != nil {
					t.Fatalf("claiming %q for the fixture: %v", name, perr)
				}
			}
		}
	}
}

// testSubsOf reads a session's membership and options as one comparable value per filter.
func testSubsOf(t *testing.T, sess *mqttSession) map[string]mqttGrantTerms {
	t.Helper()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	out := map[string]mqttGrantTerms{}
	for f, qos := range sess.subs {
		_, nl := sess.noLocalSubs[f]
		_, rap := sess.rapSubs[f]
		id, has := sess.subIDs[f]
		out[f] = mqttGrantTerms{present: true, qos: qos, noLocal: nl, rap: rap, subID: id, hasSubID: has}
	}
	return out
}

// buildPersistedSessionLockedBodyForTest is the exact bytes a save would store, for a test
// that needs to assert about the record's SIZE.
//
// sess.mu must be held.
func (sess *mqttSession) buildPersistedSessionLockedBodyForTest() []byte {
	ps := sess.buildPersistedSessionLocked()
	b, _ := json.Marshal(&ps)
	return b
}

// testGrantOf materialises the grant of the bytes a session retains, which is what the
// production fork path does — the session holds record BYTES, not a built grant.
func testGrantOf(t *testing.T, sess *mqttSession) (*mqttGrantSnap, *mqttGrantSnap) {
	t.Helper()
	sess.mu.Lock()
	body, parent := sess.grantBody, sess.grantBodyParent
	sess.mu.Unlock()
	return mqttGrantFromBody(body), mqttGrantFromBody(parent)
}

func testConsumerConfig(durable string) *ConsumerConfig {
	return &ConsumerConfig{
		Durable:        durable,
		DeliverSubject: "$MQTT.sub." + durable,
		FilterSubject:  "$MQTT.msgs.>",
		AckPolicy:      AckExplicit,
		MaxAckPending:  1024,
	}
}

// ---------------------------------------------------------------------------
// 1-5a: the snapshots, and where they come from
// ---------------------------------------------------------------------------

// TestForkRepairSnapshotRotation is test 1: every adoption that can later mint rotates the
// pair, and clear() drops it.
//
// The rotation is what makes the delta meaningful, so each production adoption gets its own
// case: a site that moved the sequence and the lineage while leaving the grant stale would
// compute a fork's repair against a record two writes back.
func TestForkRepairSnapshotRotation(t *testing.T) {
	t.Parallel()

	t.Run("the resume restore adopts the record and NO parent", func(t *testing.T) {
		t.Parallel()
		e := testNewRejectionEnv(t, "rot-restore")
		e.sess.mu.Lock()
		e.sess.subs = map[string]byte{"rot/restore": 1}
		e.sess.mu.Unlock()
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("save: %v", err)
		}
		e.asm.removeSession(e.sess, true)

		sess, present, err := e.asm.createOrRestoreSession("rot-restore", e.s.getOpts())
		if err != nil || !present {
			t.Fatalf("restore: present=%v err=%v", present, err)
		}
		snap, parent := testGrantOf(t, sess)
		if snap == nil {
			t.Fatal("a restore must adopt the restored record's grant")
		}
		if _, ok := snap.filters["rot/restore"]; !ok {
			t.Fatalf("the restored grant must carry the record's membership, got %v", snap.filters)
		}
		if parent != nil {
			t.Fatal("a record this broker did not author has no known parent content here, " +
				"so nothing about it is repairable")
		}
	})

	t.Run("a confirmed save rotates", func(t *testing.T) {
		t.Parallel()
		e := testNewRejectionEnv(t, "rot-save")
		e.sess.mu.Lock()
		e.sess.subs = map[string]byte{"rot/one": 1}
		e.sess.mu.Unlock()
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("first save: %v", err)
		}
		e.sess.mu.Lock()
		e.sess.subs = map[string]byte{"rot/one": 1, "rot/two": 2}
		e.sess.mu.Unlock()
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("second save: %v", err)
		}
		snap, parent := testGrantOf(t, e.sess)
		if snap == nil || parent == nil {
			t.Fatal("two authored saves must leave a grant AND its parent")
		}
		if _, ok := snap.filters["rot/two"]; !ok {
			t.Fatal("the base must be the record just confirmed")
		}
		if _, ok := parent.filters["rot/two"]; ok {
			t.Fatal("the parent must be the record the confirmed one REPLACED")
		}
	})

	t.Run("clear() drops both", func(t *testing.T) {
		t.Parallel()
		e := testNewRejectionEnv(t, "rot-clear")
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("second save: %v", err)
		}
		snapBefore, parentBefore := testGrantOf(t, e.sess)
		hadBoth := snapBefore != nil && parentBefore != nil
		if !hadBoth {
			t.Fatal("fixture: two saves must leave both snapshots")
		}
		if err := e.sess.clear(false); err != nil {
			t.Fatalf("clear: %v", err)
		}
		snap, parent := testGrantOf(t, e.sess)
		if snap != nil || parent != nil {
			t.Fatal("a re-minted incarnation must inherit NO grant: re-asserting the retired " +
				"one is the resurrection this repair is forbidden to perform")
		}
	})
}

// TestForkRepairRestoreRotationFollowsHydration is test 2 (M13).
//
// mqttSessionCreate runs BEFORE the caller fills Subs, the option maps, Cons and the
// drains, so a rotation placed there records an empty grant — after which the first authored
// save's delta claims the whole session as an addition and a fork re-asserts membership the
// client never added.
func TestForkRepairRestoreRotationFollowsHydration(t *testing.T) {
	t.Parallel()
	e := testNewRejectionEnv(t, "rot-hydration")
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{"hydrate/a": 1, "hydrate/b": 2}
	e.sess.cons = map[string]*ConsumerConfig{"hydrate/a": testConsumerConfig("dur-a")}
	e.sess.mu.Unlock()
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("save: %v", err)
	}
	e.asm.removeSession(e.sess, true)

	sess, _, err := e.asm.createOrRestoreSession("rot-hydration", e.s.getOpts())
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	snap, _ := testGrantOf(t, sess)
	if snap == nil || len(snap.filters) != 2 {
		t.Fatalf("the restored snapshot must carry the hydrated grant, got %v", snap)
	}
	if got := snap.filters["hydrate/a"]; !got.present || len(got.sids) != 1 {
		t.Fatalf("membership AND the consumer must both be in the snapshot: %+v", got)
	}
}

// TestForkRepairNilParentMintsNothing is test 3 (M17).
//
// Without the nil-parent refusal every entry looks like an addition, so a fork on a session
// this broker merely RESTORED would re-assert the survivor's own grant back over it. The
// re-mint subtest below now reaches the same "nothing minted" conclusion one step earlier,
// because a parentless write is not classified as a sibling at all; it is kept because the
// refusal must still hold if lineage ever reaches that record again.
func TestForkRepairNilParentMintsNothing(t *testing.T) {
	t.Parallel()

	t.Run("restored but never re-saved", func(t *testing.T) {
		t.Parallel()
		e := testNewRejectionEnv(t, "nilparent-restored")
		e.sess.mu.Lock()
		e.sess.subs = map[string]byte{"nilparent/a": 1}
		e.sess.mu.Unlock()
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("save: %v", err)
		}
		e.asm.removeSession(e.sess, true)
		sess, _, err := e.asm.createOrRestoreSession("nilparent-restored", e.s.getOpts())
		if err != nil {
			t.Fatalf("restore: %v", err)
		}
		e.asm.addSession(sess, true)

		testLandSibling(t, &testInflightEnv{s: e.s, asm: e.asm, sess: sess, c: e.c}, testMutateSibling)
		e.rejectSave(t, sess)
		if got := sess.settleRecordRejection(e.asm); got != mqttRejectionForked {
			t.Fatalf("verdict = %v, want forked", got)
		}
		if g := e.capsuleOrNil(); g != nil {
			t.Fatalf("a restored record has no parent content here, so nothing is "+
				"repairable: minted %+v", g)
		}
	})

	t.Run("the first record after a teardown re-mint", func(t *testing.T) {
		t.Parallel()
		e := testNewRejectionEnv(t, "nilparent-remint")
		if err := e.sess.clear(false); err != nil {
			t.Fatalf("clear: %v", err)
		}
		e.asm.addSession(e.sess, true)
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("the first post-re-mint save: %v", err)
		}
		// The post-re-mint write is PARENTLESS since the write-lineage audit, so the
		// clean-start sibling is not even recognised as one: the verdict concedes rather
		// than forking, which is the stated residual. Both halves are asserted, because
		// this subtest's job is that NOTHING is minted, and a conceding verdict reaches
		// that conclusion by a different route than the nil-parent refusal does.
		e.sess.mu.Lock()
		pwid := e.sess.pwid
		e.sess.mu.Unlock()
		if pwid != _EMPTY_ {
			t.Fatalf("the first post-re-mint write descends from nothing: parent %q", pwid)
		}
		testLandSibling(t, e.testInflightEnv, testMutateSibling)
		e.rejectSave(t, e.sess)
		if got := e.sess.settleRecordRejection(e.asm); got != mqttRejectionForeign {
			t.Fatalf("verdict = %v, want foreign: a parentless write establishes no sibling-hood", got)
		}
		if g := e.capsuleOrNil(); g != nil {
			t.Fatalf("nothing is repairable without a parent: minted %+v", g)
		}
	})
}

// TestForkRepairSnapshotsAreIsolated is test 4 (M15, M16): run under -race.
//
// buildPersistedSessionLocked returns sess.subs and sess.subIDs BY REFERENCE and shares
// *ConsumerConfig pointers, so a shallow snapshot would alias state that keeps changing —
// and a capsule built from it would describe whatever memory held at merge time rather than
// what the record held.
func TestForkRepairSnapshotsAreIsolated(t *testing.T) {
	t.Parallel()
	e := testNewRejectionEnv(t, "snap-isolation")
	cc := testConsumerConfig("iso-dur")
	cc.Metadata = map[string]string{"k": "original"}
	cc.BackOff = []time.Duration{time.Second}
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{"iso/a": 1}
	e.sess.subIDs = map[string]int{"iso/a": 7}
	e.sess.cons = map[string]*ConsumerConfig{"iso/a": cc}
	e.sess.mu.Unlock()
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("save: %v", err)
	}

	snap, _ := testGrantOf(t, e.sess)
	e.sess.mu.Lock()
	e.sess.subs["iso/mutated"] = 2
	e.sess.subs["iso/a"] = 0
	e.sess.subIDs["iso/a"] = 99
	e.sess.mu.Unlock()
	cc.MaxAckPending = 1
	cc.Durable = "rewritten"
	// The REFERENCE fields too: a struct copy alone still shares these, so the snapshot would
	// track a live config's slices and maps.
	cc.Metadata["k"] = "mutated"
	cc.BackOff[0] = time.Hour

	if _, ok := snap.filters["iso/mutated"]; ok {
		t.Fatal("the snapshot must not alias sess.subs")
	}
	got := snap.filters["iso/a"]
	if got.qos != 1 || got.subID != 7 {
		t.Fatalf("the snapshot must hold the RECORD's terms, got %+v", got)
	}
	st := got.sids["iso/a"]
	if st.con == nil || st.con.Durable != "iso-dur" || st.con.MaxAckPending != 1024 {
		t.Fatalf("the snapshot must hold its own copy of the consumer config, got %+v", st.con)
	}
	if st.con.Metadata["k"] != "original" || st.con.BackOff[0] != time.Second {
		t.Fatalf("a shallow struct copy still aliases the config's slices and maps: got %v / %v",
			st.con.Metadata, st.con.BackOff)
	}
}

// TestForkRepairSnapshotsComeFromTheRecord is test 5 (M14).
//
// The divergence is created BEFORE the production rotation, which is the only way to tell a
// record-sourced snapshot from a memory-sourced one: the store-result seam fires after the
// bytes are on the stream and before the success arm adopts, so a mutation made there is in
// memory and not in the record.
// NOT parallel: it installs a process-global store-result hook.
func TestForkRepairSnapshotsComeFromTheRecord(t *testing.T) {
	e := testNewRejectionEnv(t, "snap-record-sourced")
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{"src/recorded": 1}
	e.sess.mu.Unlock()

	gate := newTestHookGate(t)
	restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if !gate.enter() {
			return resp, err
		}
		defer gate.leave()
		if sess == e.sess {
			// In memory, never in the bytes just stored.
			sess.mu.Lock()
			sess.subs["src/memory-only"] = 2
			sess.mu.Unlock()
		}
		return resp, err
	})
	t.Cleanup(restore)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("save: %v", err)
	}
	restore()

	snap, _ := testGrantOf(t, e.sess)
	if _, ok := snap.filters["src/memory-only"]; ok {
		t.Fatal("the snapshot must come from the RECORD this save marshalled, not from memory: " +
			"a memory-sourced one can capture a mutation that was never persisted")
	}
	if _, ok := snap.filters["src/recorded"]; !ok {
		t.Fatalf("the snapshot must carry what the record carries, got %v", snap.filters)
	}
}

// TestForkRepairMarkedDrainIsNeverSnapshotted is test 5a (M9e).
//
// buildPersistedSessionLocked omits a drain whose owned retirement transition has MARKED it,
// which is what licenses deleting the durable with no further write. A memory-sourced
// snapshot would carry that drain anyway, and the reclaim would then re-assert a reference
// to a durable whose deletion was already licensed.
//
// The mark, not the claim count: an empty but unmarked drain is still named by the record,
// so a snapshot carrying it agrees with the record and is correct. What must never be
// snapshotted is the state the record has already stopped naming.
func TestForkRepairMarkedDrainIsNeverSnapshotted(t *testing.T) {
	t.Parallel()
	e := testNewRejectionEnv(t, "snap-zero-claim")
	e.sess.mu.Lock()
	// Empty AND marked, exactly as the owned transition leaves it for the save that omits it.
	e.sess.restoreDrainLocked("zc/a", &mqttDrain{
		cc: testConsumerConfig("zc-dur"), seqs: map[uint64]*drainSeq{}, removalMarked: true,
	})
	inMemory := len(e.sess.drains)
	e.sess.mu.Unlock()
	if inMemory != 1 {
		t.Fatalf("fixture: the drain must be in memory, got %d", inMemory)
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("save: %v", err)
	}
	snap, _ := testGrantOf(t, e.sess)
	if e := snap.filters["zc/a"]; e.sids != nil {
		t.Fatalf("a MARKED drain is omitted from the record, so it must be absent from the "+
			"snapshot too: got %+v", e.sids)
	}
}

// ---------------------------------------------------------------------------
// 6-8: the three-way merge, and what it refuses
// ---------------------------------------------------------------------------

// TestForkRepairThreeWayMerge is test 6, including the counterexample that killed the union
// design (M4, M5).
func TestForkRepairThreeWayMerge(t *testing.T) {
	t.Parallel()

	t.Run("an addition is re-asserted", func(t *testing.T) {
		t.Parallel()
		e := testStageForkedGrant(t, "merge-add",
			func(s *mqttSession) { s.subs = map[string]byte{"m/a": 1} },
			func(s *mqttSession) { s.subs = map[string]byte{"m/a": 1, "m/b": 2} }, nil)
		sess := e.reconnect(t)
		got := testSubsOf(t, sess)
		if _, ok := got["m/b"]; !ok {
			t.Fatalf("the acknowledged SUBSCRIBE missing from the survivor must be re-asserted, got %v", got)
		}
		if _, ok := got["m/a"]; !ok {
			t.Fatalf("the survivor's own membership must survive, got %v", got)
		}
	})

	t.Run("a survivor-side removal is NOT resurrected", func(t *testing.T) {
		t.Parallel()
		// The counterexample verbatim: P = {a}, ours = {a,b}, survivor = {} because it
		// acknowledged an UNSUBSCRIBE of a. A union yields {a,b}; the correct answer is
		// {b}, with a staying removed.
		e := testStageForkedGrant(t, "merge-union-trap",
			func(s *mqttSession) { s.subs = map[string]byte{"u/a": 1} },
			func(s *mqttSession) { s.subs = map[string]byte{"u/a": 1, "u/b": 2} },
			func(ps, parent *mqttPersistedSession) {
				ps.Subs = nil // the survivor's acknowledged UNSUBSCRIBE of u/a
			})
		sess := e.reconnect(t)
		got := testSubsOf(t, sess)
		if _, ok := got["u/a"]; ok {
			t.Fatalf("a union resurrects an UNSUBSCRIBE the client was ACKNOWLEDGED for; "+
				"only the parent's content can tell that from a lost addition: got %v", got)
		}
		if _, ok := got["u/b"]; !ok {
			t.Fatalf("our own addition must still be re-asserted, got %v", got)
		}
	})

	t.Run("a removal is re-asserted", func(t *testing.T) {
		t.Parallel()
		e := testStageForkedGrant(t, "merge-remove",
			func(s *mqttSession) { s.subs = map[string]byte{"r/a": 1, "r/b": 1} },
			func(s *mqttSession) { s.subs = map[string]byte{"r/a": 1} }, nil)
		sess := e.reconnect(t)
		if got := testSubsOf(t, sess); len(got) != 1 || !got["r/a"].present {
			t.Fatalf("the acknowledged UNSUBSCRIBE must be re-asserted, got %v", got)
		}
	})

	t.Run("a filter the survivor decided about is left alone", func(t *testing.T) {
		t.Parallel()
		e := testStageForkedGrant(t, "merge-conflict",
			func(s *mqttSession) { s.subs = map[string]byte{"c/a": 1} },
			func(s *mqttSession) { s.subs = map[string]byte{"c/a": 2} },
			func(ps, parent *mqttPersistedSession) {
				ps.Subs = map[string]byte{"c/a": 0} // the survivor's own re-SUBSCRIBE
			})
		sess := e.reconnect(t)
		if got := testSubsOf(t, sess); got["c/a"].qos != 0 {
			t.Fatalf("two acknowledgements with no ordering between them: the record that is "+
				"actually durable wins, got %v", got)
		}
	})
}

// TestForkRepairCoupledOptionsAreOneValue is test 7 (M6).
//
// QoS, No Local, Retain As Published and the Subscription Identifier are ONE grant. Merging
// them independently would take our QoS and the survivor's No Local, producing a grant
// neither acknowledgement describes.
func TestForkRepairCoupledOptionsAreOneValue(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		survivor func(*mqttSession)
	}{
		{"No Local", func(s *mqttSession) { s.noLocalSubs = map[string]struct{}{"o/a": {}} }},
		{"Retain As Published", func(s *mqttSession) { s.rapSubs = map[string]struct{}{"o/a": {}} }},
		{"Subscription Identifier", func(s *mqttSession) { s.subIDs = map[string]int{"o/a": 42} }},
		{"QoS", func(s *mqttSession) { s.subs = map[string]byte{"o/a": 2} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// The survivor differs from the parent in exactly one option, so it made its own
			// grant decision about o/a and our whole entry is refused.
			var survivorRec mqttPersistedSession
			e := testStageForkedGrant(t, "opts-"+strings.ReplaceAll(tc.name, " ", "-"),
				func(s *mqttSession) { s.subs = map[string]byte{"o/a": 1} },
				func(s *mqttSession) { s.subs = map[string]byte{"o/a": 1}; s.subIDs = map[string]int{"o/a": 9} },
				func(ps, parent *mqttPersistedSession) {
					probe := &mqttSession{subs: map[string]byte{"o/a": 1}}
					tc.survivor(probe)
					ps.Subs = probe.subs
					ps.NoLocal = mqttFilterSetToList(probe.noLocalSubs)
					ps.RetainAsPublished = mqttFilterSetToList(probe.rapSubs)
					ps.SubIDs = probe.subIDs
					survivorRec = *ps
				})
			sess := e.reconnect(t)
			got := testSubsOf(t, sess)
			want := mqttGrantFromRecord(&survivorRec).filters["o/a"].mqttGrantTerms
			if got["o/a"] != want {
				t.Fatalf("the survivor's WHOLE entry must survive as one value: got %+v, want %+v",
					got["o/a"], want)
			}
		})
	}
}

// TestForkRepairConsumerIdentityIsNotAConflict is test 8 (M7): the live -> live row.
//
// The resume reconciliation recreates a reaped durable with no MQTT decision behind it, so
// comparing consumer identity would turn a server-side consumer repair into a subscription
// conflict and suppress a repair that is correct. The identity difference has to be on the
// SAME filter that carries the change, or the comparison is never reached for it.
func TestForkRepairConsumerIdentityIsNotAConflict(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "identity-not-conflict",
		func(s *mqttSession) {
			s.subs = map[string]byte{"i/a": 1}
			s.cons = map[string]*ConsumerConfig{"i/a": testConsumerConfig("i-dur-parent")}
		},
		func(s *mqttSession) {
			// A real grant change on i/a: the client re-SUBSCRIBEd with a Subscription
			// Identifier. The sid stays LIVE on both sides, so this is the live -> live row.
			s.subIDs = map[string]int{"i/a": 9}
		},
		func(ps, parent *mqttPersistedSession) {
			// Equal in every acknowledged field, and carrying a DIFFERENT consumer config for
			// the same sid — the shape a reconciliation-recreated durable has.
			ps.Cons = map[string]*ConsumerConfig{"i/a": testConsumerConfig("i-dur-recreated")}
		})
	sess := e.reconnect(t)
	if got := testSubsOf(t, sess)["i/a"]; !got.hasSubID || got.subID != 9 {
		t.Fatalf("a consumer identity that differs is not a grant decision, so the repair "+
			"applies: got %+v", got)
	}
	sess.mu.Lock()
	cc := sess.cons["i/a"]
	sess.mu.Unlock()
	if cc == nil || cc.Durable != "i-dur-recreated" {
		t.Fatalf("the survivor's own consumer config must be preserved, never replaced with "+
			"ours: got %+v", cc)
	}
}

// ---------------------------------------------------------------------------
// 9-10i: the per-sid transition table
// ---------------------------------------------------------------------------

// testPersistedDrain is one drained consumer as the record holds it: the surviving durable
// plus the started deliveries [MQTT-3.10.4-3] still owes.
func testPersistedDrain(durable string, started ...mqttPersistedDrainSeq) *mqttPersistedDrain {
	return &mqttPersistedDrain{
		Cons: testConsumerConfig(durable), Subject: "$MQTT.msgs.drained", QoS: 1, Started: started,
	}
}

// testInstallDrain puts a drain in memory the way a resume does, which is the shape
// buildPersistedSessionLocked then persists.
//
// sess.mu must be held.
func testInstallDrain(sess *mqttSession, sid string, pd *mqttPersistedDrain) {
	sess.restoreDrainLocked(sid, mqttDrainFromPersisted(pd))
}

// testDrainOf reads a session's drain for one sid, plus whether drainByDur agrees.
func testDrainOf(t *testing.T, sess *mqttSession, sid string) (*mqttDrain, bool) {
	t.Helper()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	d := sess.drains[sid]
	if d == nil {
		return nil, false
	}
	return d, sess.drainByDur[d.cc.Durable] == d
}

// TestForkRepairReassertedRemovalKeepsItsDrain is test 10a: the round-3 blocker as a test.
//
// An acknowledged UNSUBSCRIBE over a filter with a started delivery does not remove the
// consumer — it moves it into a DRAIN, because the client is still owed those deliveries
// ([MQTT-3.10.4-3]). A repair that re-asserted the membership removal while carrying no drain
// would leave the record naming neither membership, consumer nor drain, and the started
// delivery would be lost. Kills M9a and M9b.
func TestForkRepairReassertedRemovalKeepsItsDrain(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "drain-removal",
		func(s *mqttSession) {
			s.subs = map[string]byte{"d/a": 1}
			s.cons = map[string]*ConsumerConfig{"d/a": testConsumerConfig("d-a-dur")}
		},
		func(s *mqttSession) {
			// The UNSUBSCRIBE: membership gone, the consumer moved into a drain that still
			// claims one started delivery.
			s.subs = nil
			delete(s.cons, "d/a")
			testInstallDrain(s, "d/a", testPersistedDrain("d-a-dur",
				mqttPersistedDrainSeq{Seq: 11, PI: 3, RegSeq: 7}))
		}, nil)

	sess := e.reconnect(t)
	if got := testSubsOf(t, sess); len(got) != 0 {
		t.Fatalf("the acknowledged UNSUBSCRIBE must be re-asserted, got %v", got)
	}
	// Asserted BEFORE any resume, because restoreDrains repopulates drainByDur and would
	// mask a reclaim that only wrote sess.drains.
	d, indexed := testDrainOf(t, sess, "d/a")
	if d == nil {
		t.Fatal("the drain must be re-asserted with the removal, or the started delivery is lost")
	}
	if !indexed {
		t.Fatal("drainByDur must be written too: the delivery callback finds a drain through it, " +
			"and a drain missing from it looks absent to everything but a teardown")
	}
	if d.cc.Durable != "d-a-dur" {
		t.Fatalf("the drain must keep its own durable, got %q", d.cc.Durable)
	}
	if st := d.seqs[11]; st == nil || !st.claimed || st.regSeq != 7 {
		t.Fatalf("the started delivery and its in-flight register must survive, got %+v", st)
	}
	// buildPersistedSessionLocked persists a claiming drain regardless of membership, so the
	// accept save keeps the durable referenced.
	sess.mu.Lock()
	ps := sess.buildPersistedSessionLocked()
	sess.mu.Unlock()
	if ps.Drains["d/a"] == nil {
		t.Fatal("the record the accept save writes must name the drain")
	}
}

// TestForkRepairAdoptsASurvivorDrain is test 10b: the re-SUBSCRIBE-over-a-drain row.
//
// adoptDrainLocked is what moves drains[sid] to cons[sid] keeping the durable and its started
// deliveries. Written by hand instead, cons and drains end up BOTH populated, which breaks
// the exclusivity invariant the whole drain design rests on. Kills M9c.
func TestForkRepairAdoptsASurvivorDrain(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "drain-adopt",
		func(s *mqttSession) {
			s.subs = nil
			testInstallDrain(s, "d/b", testPersistedDrain("d-b-dur",
				mqttPersistedDrainSeq{Seq: 21, PI: 4}))
		},
		func(s *mqttSession) {
			// Our re-SUBSCRIBE adopted the drain: live again, same durable.
			s.adoptDrainLocked("d/b")
			s.subs = map[string]byte{"d/b": 1}
		}, nil)

	sess := e.reconnect(t)
	if _, ok := testSubsOf(t, sess)["d/b"]; !ok {
		t.Fatal("the re-SUBSCRIBE must be re-asserted")
	}
	sess.mu.Lock()
	cc, stillDraining := sess.cons["d/b"], sess.drains["d/b"]
	sess.mu.Unlock()
	if cc == nil || cc.Durable != "d-b-dur" {
		t.Fatalf("the adoption must keep the drain's durable, got %+v", cc)
	}
	if stillDraining != nil {
		t.Fatal("cons and drains must never both hold one sid (I7)")
	}
}

// TestForkRepairRefusesTwoDifferingDrains is test 10d (M7c, M9d).
//
// Two drains for one sid whose persisted content differs are two acknowledgements with no
// ordering between them, so the survivor — the record that is actually durable — wins.
// Refusing loses nothing: under the outer predicate the base was already draining, a
// committed drain adds no fresh claims, and completion only removes them.
func TestForkRepairRefusesTwoDifferingDrains(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		ours mqttPersistedDrainSeq
	}{
		// A PubRel intent carries a mandatory exchange identity, so the fixture builds a
		// SUPPORTED shape: the restore path refuses an intent without one, and this test is
		// about fork repair refusing to merge two differing drains, not about that refusal.
		{"the PUBREL intent", mqttPersistedDrainSeq{Seq: 31, PubRel: true, PI: 5,
			OwnerNonce: "xxxxxxxxxxxxxxxxxxxxxx"}},
		{"the packet identifier", mqttPersistedDrainSeq{Seq: 31, PI: 6}},
		{"the store attempt", mqttPersistedDrainSeq{Seq: 31, PI: 5, Attempt: 2}},
		{"the in-flight register", mqttPersistedDrainSeq{Seq: 31, PI: 5, RegSeq: 99}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := mqttPersistedDrainSeq{Seq: 31, PI: 5}
			e := testStageForkedGrant(t, "drain-differ-"+strings.ReplaceAll(tc.name, " ", "-"),
				func(s *mqttSession) {
					s.subs = nil
					testInstallDrain(s, "d/c", testPersistedDrain("d-c-dur", base))
				},
				func(s *mqttSession) {
					s.drains, s.drainByDur = nil, nil
					testInstallDrain(s, "d/c", testPersistedDrain("d-c-dur", tc.ours))
				}, nil)

			sess := e.reconnect(t)
			d, _ := testDrainOf(t, sess, "d/c")
			if d == nil {
				t.Fatal("the survivor's drain must be left in place")
			}
			st := d.seqs[31]
			if st == nil {
				t.Fatal("the survivor's started delivery must be untouched")
			}
			if st.intent != base.owesPubRel() || st.pi != base.PI ||
				st.attempt != base.Attempt || st.regSeq != base.RegSeq {
				t.Fatalf("comparing drains by sequence MEMBERSHIP alone would merge two "+
					"different drains: got intent=%v pi=%d attempt=%d reg=%d",
					st.intent, st.pi, st.attempt, st.regSeq)
			}
		})
	}
}

// TestForkRepairReassertsALastClaimCompletion is test 10e: the draining -> absent row, which
// revision 4 of the plan wrongly called unreachable.
//
// With the base holding a one-claim drain, our branch finalizing that last claim makes
// buildPersistedSessionLocked OMIT the now-empty drain — that omission IS the acknowledged
// completion — while a sibling survivor still equals the base. Kills M9g and M9h.
func TestForkRepairReassertsALastClaimCompletion(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "drain-completed",
		func(s *mqttSession) {
			s.subs = nil
			testInstallDrain(s, "d/e", testPersistedDrain("d-e-dur",
				mqttPersistedDrainSeq{Seq: 41, PI: 8}))
		},
		func(s *mqttSession) {
			// The last claim finalized AND its owned retirement transition marked the
			// object, which is what makes the record omit it. The mark is the authority
			// for omission — an empty but unmarked drain stays named — so without it this
			// fixture describes a record it never produces.
			s.drains["d/e"].seqs[41].claimed = false
			s.drains["d/e"].removalMarked = true
		}, nil)

	// A real consumer, so "not deleted" is a fact about JetStream rather than about a map.
	if _, err := e.sess.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			Durable: "d-e-dur", FilterSubject: mqttStreamSubjectPrefix + "d.e",
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, MaxAckPending: 64,
		},
	}); err != nil {
		t.Fatalf("create the probe consumer: %v", err)
	}
	sess := e.reconnect(t)
	sess.mu.Lock()
	d, byDur := sess.drains["d/e"], sess.drainByDur["d-e-dur"]
	sess.mu.Unlock()
	if _, err := e.sess.jsa.lookupConsumer(mqttStreamName, "d-e-dur"); err != nil {
		t.Fatalf("the reclaim deletes no durable on ANY row, this one included: %v", err)
	}
	if d != nil || byDur != nil {
		t.Fatalf("the acknowledged completion must be re-asserted in BOTH maps: drains=%v byDur=%v", d, byDur)
	}
	// The durable is not this reclaim's business either way: on our branch
	// finishDrainIfEmpty already deleted it after proving its own save.
	sess.mu.Lock()
	ps := sess.buildPersistedSessionLocked()
	sess.mu.Unlock()
	if len(ps.Drains) != 0 {
		t.Fatalf("the record must no longer name the completed drain, got %v", ps.Drains)
	}
}

// TestForkRepairDrainPresenceAloneRefusesAChange is test 10h, and it is M7a's discriminator.
//
// Every side is a shape the ordinary writer authors. The base holds a one-claim drain for a
// removed filter (an acknowledged UNSUBSCRIBE with a started delivery); OURS re-SUBSCRIBEs
// it, so our record has the filter live; the SURVIVOR instead completed that last claim, so
// its record omits the drain. Membership and options are equal on base and survivor — both
// absent for that filter — so drain presence is the ONLY discriminator, and the correct code
// refuses because the survivor made its own grant consequence.
//
// 10e cannot kill M7a: there the survivor is drain-equal to the base, so the mutant and the
// correct code both admit the change.
func TestForkRepairDrainPresenceAloneRefusesAChange(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "drain-shape-only",
		func(s *mqttSession) {
			s.subs = nil
			testInstallDrain(s, "d/h", testPersistedDrain("d-h-dur",
				mqttPersistedDrainSeq{Seq: 51, PI: 9}))
		},
		func(s *mqttSession) {
			s.adoptDrainLocked("d/h")
			s.subs = map[string]byte{"d/h": 1}
		},
		func(ps, parent *mqttPersistedSession) {
			// The survivor completed the last claim, so its record omits the drain. Its
			// membership and options are identical to the parent's: both absent.
			ps.Drains = nil
		})

	sess := e.reconnect(t)
	if got := testSubsOf(t, sess); len(got) != 0 {
		t.Fatalf("the survivor made its own grant consequence for this filter, so the change "+
			"must be refused: got %v", got)
	}
	sess.mu.Lock()
	cc := sess.cons["d/h"]
	sess.mu.Unlock()
	if cc != nil {
		t.Fatalf("applying ours here installs a consumer config for a durable the survivor's "+
			"own finishDrainIfEmpty has already deleted, so delivery interest would name a "+
			"consumer that does not exist: got %+v", cc)
	}
}

// TestForkRepairADifferingStartedSetDoesNotBlockAnAdopt is test 10g, and it is M7b's
// discriminator: putting the persisted drain in the CONFLICT predicate would refuse a repair
// that is correct.
//
// 10a cannot kill M7b, because there the base and the survivor are both live and no drain is
// ever compared.
func TestForkRepairADifferingStartedSetDoesNotBlockAnAdopt(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "drain-shape-equal",
		func(s *mqttSession) {
			s.subs = nil
			testInstallDrain(s, "d/g", testPersistedDrain("d-g-dur",
				mqttPersistedDrainSeq{Seq: 61, PI: 1}))
		},
		func(s *mqttSession) {
			s.adoptDrainLocked("d/g")
			s.subs = map[string]byte{"d/g": 1}
		},
		func(ps, parent *mqttPersistedSession) {
			// Still DRAINING, so the shape is equal to the parent's and no grant decision was
			// made — but the started set has moved on, which is in-flight accounting both
			// sides advance independently.
			ps.Drains = map[string]*mqttPersistedDrain{
				"d/g": testPersistedDrain("d-g-dur", mqttPersistedDrainSeq{Seq: 62, PI: 2}),
			}
		})

	sess := e.reconnect(t)
	if _, ok := testSubsOf(t, sess)["d/g"]; !ok {
		t.Fatal("a started set that differs is not a grant decision, so the re-SUBSCRIBE applies")
	}
	sess.mu.Lock()
	cc, stillDraining := sess.cons["d/g"], sess.drains["d/g"]
	sess.mu.Unlock()
	if cc == nil || stillDraining != nil {
		t.Fatalf("the survivor's drain must be ADOPTED, got cons=%+v drains=%v", cc, stillDraining)
	}
}

// TestForkRepairAReassertedDrainKeepsItsOwnDurable is test 10i, and it is the discriminator
// for re-pointing a drain's Cons at the survivor's live config (M9l).
//
// A drain's durable is not interchangeable with another's: drainByDur is keyed by it,
// in-flight registers correlate through the durable they were created under, drain recovery
// queries that durable's consumer state, and completion deletes it. Re-pointing would
// evaluate our started sequences against another consumer's ack state and eventually delete
// the wrong durable.
//
// 10a cannot expose this, because there the survivor equals the base in identity too.
func TestForkRepairAReassertedDrainKeepsItsOwnDurable(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "drain-own-durable",
		func(s *mqttSession) {
			s.subs = map[string]byte{"d/i": 1}
			s.cons = map[string]*ConsumerConfig{"d/i": testConsumerConfig("d-i-A")}
		},
		func(s *mqttSession) {
			s.subs = nil
			delete(s.cons, "d/i")
			testInstallDrain(s, "d/i", testPersistedDrain("d-i-A",
				mqttPersistedDrainSeq{Seq: 71, PI: 2, RegSeq: 13}))
		},
		func(ps, parent *mqttPersistedSession) {
			// The survivor holds a LIVE consumer for the same sid, on a different durable.
			// Identity is not a grant decision, so the change still applies.
			ps.Cons = map[string]*ConsumerConfig{"d/i": testConsumerConfig("d-i-B")}
		})

	sess := e.reconnect(t)
	d, indexed := testDrainOf(t, sess, "d/i")
	if d == nil {
		t.Fatal("the drain must be re-asserted")
	}
	if d.cc.Durable != "d-i-A" {
		t.Fatalf("a resulting drain keeps its OWN persisted durable, got %q", d.cc.Durable)
	}
	if !indexed {
		t.Fatal("drainByDur must be keyed by the drain's own durable")
	}
	if st := d.seqs[71]; st == nil || st.regSeq != 13 {
		t.Fatalf("the started sequences belong to that durable, got %+v", st)
	}
	sess.mu.Lock()
	byB, liveB := sess.drainByDur["d-i-B"], sess.cons["d/i"]
	sess.mu.Unlock()
	if byB != nil {
		t.Fatal("the survivor's live durable must never become the drain's key")
	}
	if liveB != nil {
		t.Fatal("the displaced live consumer is removed from cons and left to the orphan sweep")
	}
}

// TestForkRepairMultiLevelFilterCarriesBothSids is test 9 (M8).
//
// A "foo/#" filter has TWO independent consumer identities — the main sid and the level-up
// companion — and a removal that dropped only the main one leaves a stale cons entry: the
// next record omits it, so it is invisible durably, but processJSConsumer's
// existing-consumer arm then returns on a later re-SUBSCRIBE without rebuilding delivery
// interest, and that filter silently stops delivering.
func TestForkRepairMultiLevelFilterCarriesBothSids(t *testing.T) {
	t.Parallel()
	// Membership keys are the NATS-side subject a filter maps to, so a multi-level filter
	// ends in ".>" and its level-up companion sid is that key with the last two characters
	// replaced by mqttMultiLevelSidSuffix — the derivation mqttUnsubSids makes and the
	// reversal buildPersistedSessionLocked makes back.
	const filter = "ml.a.>"
	main := filter
	companion := "ml.a" + mqttMultiLevelSidSuffix

	t.Run("an addition installs both", func(t *testing.T) {
		t.Parallel()
		e := testStageForkedGrant(t, "ml-add", nil,
			func(s *mqttSession) {
				s.subs = map[string]byte{filter: 1}
				s.cons = map[string]*ConsumerConfig{
					main:      testConsumerConfig("ml-main"),
					companion: testConsumerConfig("ml-fwc"),
				}
			}, nil)
		sess := e.reconnect(t)
		sess.mu.Lock()
		gotMain, gotCompanion := sess.cons[main], sess.cons[companion]
		sess.mu.Unlock()
		if gotMain == nil || gotCompanion == nil {
			t.Fatalf("both sids must be installed, got main=%v companion=%v", gotMain, gotCompanion)
		}
	})

	t.Run("a removal deletes both", func(t *testing.T) {
		t.Parallel()
		e := testStageForkedGrant(t, "ml-remove",
			func(s *mqttSession) {
				s.subs = map[string]byte{filter: 1}
				s.cons = map[string]*ConsumerConfig{
					main:      testConsumerConfig("ml-main"),
					companion: testConsumerConfig("ml-fwc"),
				}
			},
			func(s *mqttSession) { s.subs = nil; s.cons = nil }, nil)
		sess := e.reconnect(t)
		sess.mu.Lock()
		gotMain, gotCompanion := sess.cons[main], sess.cons[companion]
		sess.mu.Unlock()
		if gotMain != nil || gotCompanion != nil {
			t.Fatalf("a removal must delete EVERY sid, got main=%v companion=%v", gotMain, gotCompanion)
		}
	})
}

// TestForkRepairDeletesNoDurable is test 10 (M9).
//
// An unreferenced consumer is the orphan sweep's business. Proving a delete here would be a
// late delete on a name this pass never established it owned.
func TestForkRepairDeletesNoDurable(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "no-durable-delete",
		func(s *mqttSession) {
			s.subs = map[string]byte{"nd/a": 1}
			s.cons = map[string]*ConsumerConfig{"nd/a": testConsumerConfig("nd-dur")}
		},
		func(s *mqttSession) { s.subs = nil; s.cons = nil }, nil)

	// A real consumer, so "still there" is a fact about JetStream rather than about a map.
	if _, err := e.sess.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			// A PULL consumer: no deliver subject, so nothing has to be subscribed for it
			// to stay alive, and "still there" is a fact about JetStream.
			Durable: "nd-dur", FilterSubject: mqttStreamSubjectPrefix + "nd.a",
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, MaxAckPending: 64,
		},
	}); err != nil {
		t.Fatalf("create the probe consumer: %v", err)
	}
	sess := e.reconnect(t)
	sess.mu.Lock()
	gone := sess.cons["nd/a"] == nil
	sess.mu.Unlock()
	if !gone {
		t.Fatal("fixture: the removal must have applied")
	}
	if _, err := e.sess.jsa.lookupConsumer(mqttStreamName, "nd-dur"); err != nil {
		t.Fatalf("the reclaim must delete NO durable: %v", err)
	}
}

// ---------------------------------------------------------------------------
// The mint sites, the anchor, and the capsule's life
// ---------------------------------------------------------------------------

// testForkEnvDeltaFilter is the membership testNewForkEnv adds on its second save, so every
// fork test's record has a real parent and a real delta. Named rather than inlined because
// the repair assertions attached to those tests all look for exactly this filter.
const testForkEnvDeltaFilter = "fork/env/delta"

// testAssertMinted checks that a forked verdict recorded the CANDIDATE delta. Whether the
// survivor lacks any of it is a merge-time question this mint-time assertion cannot ask.
//
// It also checks the capsule was stored EXACTLY ONCE, which the capsule itself cannot show:
// every store replaces the map entry with a fresh pointer, so a second mint leaves state
// indistinguishable from a single one. Callers arm the counter with
// testCountForkedGrantStores; a caller that does not is unaffected.
func testAssertMinted(t *testing.T, e *testRejectionEnv) *mqttForkedGrant {
	t.Helper()
	if n := e.asm.forkedGrantStores.Load(); n > 1 {
		t.Fatalf("the capsule was stored %d times; one verdict mints once, under one fence", n)
	}
	g := e.capsule(t)
	for _, ch := range g.changes {
		if ch.filter == testForkEnvDeltaFilter {
			return g
		}
	}
	t.Fatalf("the capsule must carry the grant this record added over its parent, got %+v", g.changes)
	return nil
}

// testAssertNotMinted checks that a verdict which established no fork recorded nothing.
func testAssertNotMinted(t *testing.T, e *testRejectionEnv) {
	t.Helper()
	if g := e.capsuleOrNil(); g != nil {
		t.Fatalf("only a body claiming our parent licenses a repair; this verdict minted %+v", g.changes)
	}
}

// TestForkRepairMintsOnThePostReattemptReadback is test 13's own case for the SECOND
// classification inside the resolver (M19b).
//
// The first readback answers at-base — contention, never an outcome — so the resolver
// re-attempts the exact bytes; the sibling lands in that window and the re-attempt's own
// rejection is what reaches the forked verdict. A resolver that minted only on the first
// readback would leave this session's repair unrecorded.
// NOT parallel: it installs a process-global re-attempt hook.
func TestForkRepairMintsOnThePostReattemptReadback(t *testing.T) {
	e := testNewForkEnv(t, "mint-reattempt")
	testAmbiguousSave(t, e.sess)

	gate := newTestHookGate(t)
	var landed bool
	restore := setMQTTTestResolveReattemptHook(func(sess *mqttSession) {
		if !gate.enter() {
			return
		}
		defer gate.leave()
		if sess != e.sess || landed {
			return
		}
		landed = true
		// Inside the window the re-attempt exists for: the sibling becomes current between
		// the at-base readback and the re-attempt, so the re-attempt is rejected and the
		// record that IS there is classified.
		testLandSibling(t, e.testInflightEnv, testMutateSibling)
	})
	t.Cleanup(restore)

	got, _ := e.sess.resolveUncertainWrite()
	restore()
	if got != mqttUncertainForked {
		t.Fatalf("resolveUncertainWrite = %v, want forked", got)
	}
	if !landed {
		t.Fatal("fixture: the sibling must land inside the re-attempt window")
	}
	testAssertMinted(t, e)
}

// TestForkRepairAnchorConjuncts is test 11 (M1, M2, M3).
//
// The anchor RE-EVALUATES the licence rather than remembering it. Both conjuncts are
// load-bearing and for different reasons: the lineage test says the record now current is
// still the sibling that erased us, and the generation test is needed because
// classifyRecordLineage deliberately ignores the generation. It no longer has the
// clean-start sibling to catch — that write is parentless since the write-lineage audit,
// so the pair concedes before it reaches here — but the conjunct is what keeps any future
// lineage match across a re-mint from re-asserting state into an incarnation a teardown
// destroyed.
func TestForkRepairAnchorConjuncts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		break_  func(sess *mqttSession, g *mqttForkedGrant)
		want    mqttForkAnchor
		explain string
	}{
		{"still the sibling, same incarnation", nil, mqttForkAnchorMerge, ""},
		{"a write since the verdict", func(sess *mqttSession, g *mqttForkedGrant) {
			// A child of the survivor: its parent is the survivor's own identity, so it is
			// no longer a sibling of the capsule's parent.
			sess.pwid = sess.wid
			sess.wid = mqttNewRecordWID()
		}, mqttForkAnchorDrop, "any intervening write makes the record a CHILD of the survivor"},
		{"a re-minted incarnation", func(sess *mqttSession, g *mqttForkedGrant) {
			sess.gen = mqttNewSessGen()
		}, mqttForkAnchorDrop, "a clean-start sibling classifies forked while belonging to an " +
			"incarnation a teardown destroyed"},
		{"still on the capsule's own base", func(sess *mqttSession, g *mqttForkedGrant) {
			sess.wid, sess.pwid = g.ackWID, g.ackPWID
		}, mqttForkAnchorKeep, "a resident object that has not re-read the record still carries " +
			"the capsule's own pair"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testStageForkedGrant(t, "anchor-"+strings.ReplaceAll(tc.name, " ", "-"),
				nil, func(s *mqttSession) { s.subs = map[string]byte{"an/a": 1} }, nil)
			g := e.capsule(t)

			sess, _, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
			if err != nil {
				t.Fatalf("restore: %v", err)
			}
			sess.mu.Lock()
			if tc.break_ != nil {
				tc.break_(sess, g)
			}
			got := sess.anchorForkedGrantLocked(g)
			sess.mu.Unlock()
			if got != tc.want {
				t.Fatalf("anchor = %v, want %v (%s)", got, tc.want, tc.explain)
			}

			// And the capsule is GONE after a refusal, kept otherwise.
			tok := sess.beginRecordTx()
			if ferr := e.asm.reassertForkedGrantInto(sess, tok); ferr != nil {
				t.Fatalf("the fork repair refused: %v", ferr)
			}
			if terr := sess.endRecordTx(tok); terr != nil {
				t.Fatalf("endRecordTx: %v", terr)
			}
			held := e.capsuleOrNil() != nil
			if tc.want == mqttForkAnchorDrop && held {
				t.Fatal("a refused capsule must be dropped: its anchor can never match again")
			}
			if tc.want != mqttForkAnchorDrop && !held {
				t.Fatal("a capsule is NOT consumed by a merge or a keep: a re-driven CONNECT " +
					"must be able to merge again")
			}
		})
	}
}

// TestForkRepairEmptyDeltaMintsNothing is test 12.
//
// This is the property that replaces the site-specific refusal an earlier design had at the
// CONNECT rollback: grant terms are not carried fields, so a rollback of a CONNECT that
// repaired nothing produces no capsule and no site has to know whether it might be undoing a
// repair.
func TestForkRepairEmptyDeltaMintsNothing(t *testing.T) {
	t.Parallel()
	e := testNewRejectionEnv(t, "empty-delta")
	e.dropPersistObservations(t, e.idHash)
	// Two saves with IDENTICAL content: a real parent, and nothing between them.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("second save: %v", err)
	}
	snapNow, parentNow := testGrantOf(t, e.sess)
	bothSnaps := snapNow != nil && parentNow != nil
	if !bothSnaps {
		t.Fatal("fixture: both snapshots must exist, or this proves nothing about the DELTA")
	}
	testLandSibling(t, e.testInflightEnv, testMutateSibling)
	e.rejectSave(t, e.sess)
	if got := e.sess.settleRecordRejection(e.asm); got != mqttRejectionForked {
		t.Fatalf("verdict = %v, want forked", got)
	}
	testAssertNotMinted(t, e)
}

// TestForkRepairCancelledRegVerdictsMergeByUnion is test 10f (M9f).
//
// The restore asserts the restored nonce set is already durable (voidedRegsRev = 0), so a
// merged nonce must be marked NOT durable — which only voidRegNonceLocked does, by bumping
// the revision and stamping the nonce with its own insertion revision. Assigning the map
// instead leaves the verdict unsaved forever, and on an unbound session nothing need ever
// look again.
//
// Sampled BEFORE the accept save, because that save credits the revision and a post-CONNECT
// assertion could not expect voidedRegsRev > voidedRegsSavedRev.
func TestForkRepairCancelledRegVerdictsMergeByUnion(t *testing.T) {
	t.Parallel()
	const nonce = "void-nonce-merged"
	e := testStageForkedGrant(t, "reg-union", nil,
		func(s *mqttSession) { s.voidRegNonceLocked(nonce) }, nil)

	g := e.capsule(t)
	if len(g.regNonces) != 1 || g.regNonces[0] != nonce {
		t.Fatalf("the capsule must carry the cancelled-register verdict, got %v", g.regNonces)
	}

	sess, _, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	sess.mu.Lock()
	restoredDurable := len(sess.cancelledRegNonces) == 0 &&
		sess.voidedRegsRev == 0 && sess.voidedRegsSavedRev == 0
	sess.mu.Unlock()
	if !restoredDurable {
		t.Fatal("fixture: the survivor's record carries no verdict, and what it does carry the " +
			"restore asserts is already durable")
	}

	tok := sess.beginRecordTx()
	if ferr := e.asm.reassertForkedGrantInto(sess, tok); ferr != nil {
		t.Fatalf("the fork repair refused: %v", ferr)
	}
	// Sampled INSIDE the transaction, before it ends: releasing the fence runs the
	// void-verdict handoff, and that save credits the revision — so a sample taken
	// afterwards could not tell "marked not durable" from "never marked at all".
	sess.mu.Lock()
	stamp, have := sess.cancelledRegNonces[nonce]
	rev, saved := sess.voidedRegsRev, sess.voidedRegsSavedRev
	sess.mu.Unlock()
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if !have {
		t.Fatal("the verdict must be re-asserted: a nonce is a monotonically accumulating fact")
	}
	if stamp == 0 || rev <= saved {
		t.Fatalf("a merged nonce must be marked NOT durable so the next landed save persists it: "+
			"stamp=%d rev=%d saved=%d", stamp, rev, saved)
	}

	// And the save that follows really does carry it, which is what "durable" has to mean.
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("the accept-equivalent save: %v", err)
	}
	sess.mu.Lock()
	ps := sess.buildPersistedSessionLocked()
	sess.mu.Unlock()
	if len(ps.CancelledRegs) != 1 || ps.CancelledRegs[0] != nonce {
		t.Fatalf("the record must carry the re-asserted verdict, got %v", ps.CancelledRegs)
	}
}

// TestForkRepairCarriesPubRelConsumer is test 9a (M9i-1).
//
// A QoS 2 SUBSCRIBE creates the PUBREL consumer and the record persists it, so a repaired
// QoS 2 membership without it produces a record carrying QoS 2 membership and no PUBREL
// infrastructure — a session that can never complete a QoS 2 exchange.
func TestForkRepairCarriesPubRelConsumer(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "pubrel-carry", nil,
		func(s *mqttSession) {
			s.subs = map[string]byte{"q2/a": 2}
			s.pubRelConsumer = testConsumerConfig("pubrel-ours")
		},
		func(ps, parent *mqttPersistedSession) {
			ps.PubRel = nil // the survivor's branch never provisioned one
		})

	sess := e.reconnect(t)
	sess.mu.Lock()
	ps := sess.buildPersistedSessionLocked()
	sess.mu.Unlock()
	if ps.Subs["q2/a"] != 2 {
		t.Fatalf("the QoS 2 membership must be re-asserted, got %v", ps.Subs)
	}
	if ps.PubRel == nil || ps.PubRel.Durable != "pubrel-ours" {
		t.Fatalf("the record must carry QoS 2 membership AND its PUBREL consumer, got %+v", ps.PubRel)
	}
}

// TestForkRepairNeverOverwritesSurvivorPubRel is test 9b (M9i-2).
//
// Survivor-wins-if-present is a rule about PRESENCE only, and it is never a comparison: the
// durable name is deterministic per generation but the config around it is not — AckWait,
// MaxAckPending, storage and inactivity come independently from server options — so two
// branches of one generation can hold different configs for one durable. The two fixtures
// here are therefore DISTINGUISHABLE, or equality would mask the overwrite.
func TestForkRepairNeverOverwritesSurvivorPubRel(t *testing.T) {
	t.Parallel()
	survivorPubRel := testConsumerConfig("pubrel-shared-name")
	survivorPubRel.MaxAckPending = 7
	e := testStageForkedGrant(t, "pubrel-keep", nil,
		func(s *mqttSession) {
			s.subs = map[string]byte{"q2/b": 2}
			ours := testConsumerConfig("pubrel-shared-name")
			ours.MaxAckPending = 4242
			s.pubRelConsumer = ours
		},
		func(ps, parent *mqttPersistedSession) { ps.PubRel = survivorPubRel })

	sess := e.reconnect(t)
	sess.mu.Lock()
	got := sess.pubRelConsumer
	sess.mu.Unlock()
	if got == nil || got.MaxAckPending != 7 {
		t.Fatalf("the survivor's own PUBREL consumer must survive verbatim, got %+v", got)
	}
}

// TestForkRepairMergesUnderTheRecordFence is test 9c (M9j).
//
// The merge mutates subs, the options, cons, the drains and the nonce set, so it must run
// inside the accept transaction: a transaction that starts after the mutation it protects
// cannot make that mutation invisible to another writer, and the sessLocked ownership gate is
// not a recordTx.
//
// The ordering is FIXED by a seam rather than raced: the hook fires immediately before the
// merge takes sess.mu, releases the try-acquirer there and waits for its verdict.
// NOT parallel: it installs a process-global merge hook.
func TestForkRepairMergesUnderTheRecordFence(t *testing.T) {
	e := testStageForkedGrant(t, "fence-held", nil,
		func(s *mqttSession) { s.subs = map[string]byte{"f/a": 1} }, nil)

	sess, _, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("restore: %v", err)
	}

	gate := newTestHookGate(t)
	var fired bool
	var acquired bool
	restore := setMQTTTestForkRepairMergeHook(func(s *mqttSession) {
		if !gate.enter() {
			return
		}
		defer gate.leave()
		if s != sess || fired {
			return
		}
		fired = true
		// The verdict is READ HERE, inside the hook. Reading it after the reclaim returns
		// would race the acquisition against endRecordTx and the test could pass with the
		// fence never held at all.
		acquired = <-testTryBeginAsync(t, sess)
	})
	t.Cleanup(restore)

	tok := sess.beginRecordTx()
	if ferr := e.asm.reassertForkedGrantInto(sess, tok); ferr != nil {
		t.Fatalf("the fork repair refused: %v", ferr)
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	restore()

	if !fired {
		t.Fatal("fixture: the merge hook must have fired")
	}
	if acquired {
		t.Fatal("another writer must be REFUSED the fence while the merge runs")
	}
	if _, ok := testSubsOf(t, sess)["f/a"]; !ok {
		t.Fatal("fixture: the merge must actually have applied")
	}
}

// TestForkRepairRefusesToMergeWithNoOpenTransaction is the other half of test 9c, and it is
// M9j's discriminator.
//
// Revisions 1-4 of the plan put the reclaim at the end of the restore, which returns long
// before the accept transaction opens — so there was no token to pass. That placement is
// expressed here as its only faithful signature: a token that identifies no open transaction.
// The merge must refuse and mutate NOTHING, because a transaction that begins after the
// mutation it protects cannot make that mutation invisible to another writer.
//
// The other half above cannot catch this: it opens a real transaction itself, so it proves
// what the CALLER does, not what the reclaim requires of it.
func TestForkRepairRefusesToMergeWithNoOpenTransaction(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "fence-absent", nil,
		func(s *mqttSession) { s.subs = map[string]byte{"fa/a": 1} }, nil)

	sess, _, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if ferr := e.asm.reassertForkedGrantInto(sess, recordTx{}); ferr != nil {
		t.Fatalf("the fork repair refused: %v", ferr)
	}

	if got := testSubsOf(t, sess)["fa/a"]; got.present {
		t.Fatal("the merge must refuse outside an open record transaction: it mutates subs, " +
			"the options, cons, the drains and the nonce set")
	}
	if e.capsuleOrNil() == nil {
		t.Fatal("a refused merge must not consume the capsule either: the repair is still owed")
	}
	// And with a real transaction it applies, so the refusal is about the FENCE and not about
	// something else in this fixture.
	tok := sess.beginRecordTx()
	if ferr := e.asm.reassertForkedGrantInto(sess, tok); ferr != nil {
		t.Fatalf("the fork repair refused: %v", ferr)
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if got := testSubsOf(t, sess)["fa/a"]; !got.present {
		t.Fatal("with the transaction open the same merge must apply")
	}
}

// TestForkRepairSurvivesAReDriveAndAFailedConnect is test 15 (M25).
//
// The capsule is not consumed on merge, so a CONNECT that merges and then fails forfeits
// nothing and a T0/T1a/T1b/T2 re-drive that re-enters the restore simply re-merges —
// idempotently, because the merge is a function of the survivor's content.
func TestForkRepairSurvivesAReDriveAndAFailedConnect(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "redrive-survives", nil,
		func(s *mqttSession) { s.subs = map[string]byte{"rd/a": 1} }, nil)

	first := e.reconnect(t)
	if _, ok := testSubsOf(t, first)["rd/a"]; !ok {
		t.Fatal("the first pass must merge")
	}
	// That CONNECT fails without its accept save landing, so the record has not moved.
	e.asm.removeSession(first, true)
	if e.capsuleOrNil() == nil {
		t.Fatal("a merge must not consume the capsule")
	}

	second := e.reconnect(t)
	if _, ok := testSubsOf(t, second)["rd/a"]; !ok {
		t.Fatal("the re-drive must merge again, against the same survivor")
	}
}

// TestForkRepairKeepArmDefersToTheAuthoritativeRestore is test 15a, and it is M9k's
// discriminator.
//
// A resident object that has not re-read the record still carries the capsule's own pair, so
// the forked test returns not-forked and a two-outcome anchor would DROP a capsule that is
// still valid. Keeping it costs nothing: that accept save CASes at a sequence the record has
// moved past, is definitely rejected, and T2 re-drives to the authoritative restore, whose
// pass merges. Test 15 cannot exercise this, because it starts from a merge.
func TestForkRepairKeepArmDefersToTheAuthoritativeRestore(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "keep-arm", nil,
		func(s *mqttSession) { s.subs = map[string]byte{"ka/a": 1} }, nil)

	// PASS ONE, on the object that still holds the capsule's own acknowledged pair.
	resident := e.sess
	resident.mu.Lock()
	stillOwnBase := resident.wid == e.capsule(t).ackWID
	resident.mu.Unlock()
	if !stillOwnBase {
		t.Fatal("fixture: the resident object must still carry the pair the verdict was reached against")
	}
	tok := resident.beginRecordTx()
	if ferr := e.asm.reassertForkedGrantInto(resident, tok); ferr != nil {
		t.Fatalf("the fork repair refused: %v", ferr)
	}
	if terr := resident.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if e.capsuleOrNil() == nil {
		t.Fatal("the keep arm must retain the capsule: without it the first pass drops a " +
			"still-valid repair and the T2 pass has nothing to merge")
	}

	// PASS TWO, after T2 has re-entered the authoritative restore.
	second := e.reconnect(t)
	if _, ok := testSubsOf(t, second)["ka/a"]; !ok {
		t.Fatal("the authoritative restore's pass must merge")
	}
}

// TestForkRepairCarriesNoWill is test 17 (M26).
//
// Asserted on the MINTED CAPSULE, not only on the reclaim's effect: mqttProcessConnect clears
// es.will before the accept save anyway, so an effect-only assertion would pass against an
// implementation that captured a Will and never used it. A Will is a per-connection term the
// reconnecting CONNECT establishes, and carrying one would resurrect a Will the client
// replaced.
func TestForkRepairCarriesNoWill(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "no-will", nil,
		func(s *mqttSession) {
			s.subs = map[string]byte{"w/a": 1}
			s.will = &mqttPersistedWill{Topic: "w/last", Msg: []byte("gone"), QoS: 1}
		}, nil)

	g := e.capsule(t)
	// Structural: the capsule has no Will-derived member at all, so there is nothing an
	// implementation could smuggle one into.
	if len(g.changes) == 0 {
		t.Fatal("fixture: the capsule must carry the membership change")
	}
	sess := e.reconnect(t)
	sess.mu.Lock()
	will := sess.will
	sess.mu.Unlock()
	if will != nil {
		t.Fatalf("a Will is a per-connection term the CONNECT establishes, never repaired: %+v", will)
	}
}

// TestForkRepairSharedMembershipIsNeverCarried is test 19 (M27).
//
// A per-filter shared restore can fail while the membership stays durable, so a re-asserted
// SharedSubs membership would be durable while that failure leaves no group join and no
// consumer — the accepted-but-dead state F-050 already records. Repairing into that ordering
// would extend an open finding rather than respect it.
func TestForkRepairSharedMembershipIsNeverCarried(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "shared-excluded", nil,
		func(s *mqttSession) {
			s.subs = map[string]byte{"sh/plain": 1}
			s.sharedSubs = map[string]mqttSharedMembership{"$share/g/sh/grp": testSharedMembership("$share/g/sh/grp", 2)}
		},
		func(ps, parent *mqttPersistedSession) { ps.SharedSubs = nil })

	g := e.capsule(t)
	for _, ch := range g.changes {
		if strings.Contains(ch.filter, "$share") {
			t.Fatalf("shared membership must be neither snapshotted nor merged, got %q", ch.filter)
		}
	}
	sess := e.reconnect(t)
	sess.mu.Lock()
	shared := len(sess.sharedSubs)
	sess.mu.Unlock()
	if shared != 0 {
		t.Fatalf("the survivor's shared membership must be left exactly as it is, got %d", shared)
	}
	if _, ok := testSubsOf(t, sess)["sh/plain"]; !ok {
		t.Fatal("the PLAIN half must still be repaired")
	}
}

// TestForkRepairCleanStartDiscardsTheMerge is test 18 ([MQTT-3.1.2-6]).
//
// A clean start needs no special case, and the placement is what makes that structural rather
// than incidental: clear() re-mints the generation well before the accept transaction opens,
// after which the capsule's generation cannot match and the reclaim drops it without ever
// merging.
func TestForkRepairCleanStartDiscardsTheMerge(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "clean-start", nil,
		func(s *mqttSession) { s.subs = map[string]byte{"cs/a": 1} }, nil)

	sess, _, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	// The clean-start discard, exactly where mqttProcessConnect runs it: before the accept
	// transaction opens.
	if err := sess.clear(false); err != nil {
		t.Fatalf("clear: %v", err)
	}
	tok := sess.beginRecordTx()
	if ferr := e.asm.reassertForkedGrantInto(sess, tok); ferr != nil {
		t.Fatalf("the fork repair refused: %v", ferr)
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if got := testSubsOf(t, sess); len(got) != 0 {
		t.Fatalf("Clean Start must discard the session state, repair included: got %v", got)
	}
	if e.capsuleOrNil() != nil {
		t.Fatal("the re-minted generation must make the anchor refuse, which drops the capsule")
	}
}

// TestForkRepairCapEvictsTheOldest is test 20 (M28).
//
// The anchor bounds HARM; the cap bounds MEMORY. Two ordinary outcomes leave a capsule
// behind — a client that never returns here, and one whose reconnect merged — so the
// population needs a bound even though every entry is correct.
func TestForkRepairCapEvictsTheOldest(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "cap-evict", nil,
		func(s *mqttSession) { s.subs = map[string]byte{"cap/a": 1} }, nil)
	real := e.capsule(t)

	e.asm.mu.Lock()
	// The real capsule is the OLDEST, so it is the one the bound discards.
	for i := 0; i < mqttForkedGrantMax; i++ {
		id := "cap-filler-" + strconv.Itoa(i)
		e.asm.storeForkedGrantLocked(&mqttSession{id: id, asm: e.asm},
			&mqttForkedGrant{gen: "g", ackWID: "w", ackPWID: "p",
				mintedAt: real.mintedAt.Add(time.Duration(i+1) * time.Second),
				changes:  []mqttGrantChange{{filter: "filler"}}})
	}
	held := len(e.asm.forkedGrants)
	e.asm.mu.Unlock()
	if held != mqttForkedGrantMax {
		t.Fatalf("the population must stay bounded, got %d", held)
	}
	if e.capsuleOrNil() != nil {
		t.Fatal("reaching the cap forfeits the OLDEST client's otherwise repairable state")
	}

	// Forfeited for that client only: a later reconnect simply resumes the survivor's record.
	sess, _, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	tok := sess.beginRecordTx()
	if ferr := e.asm.reassertForkedGrantInto(sess, tok); ferr != nil {
		t.Fatalf("the fork repair refused: %v", ferr)
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if _, ok := testSubsOf(t, sess)["cap/a"]; ok {
		t.Fatal("a forfeited repair must not be applied from somewhere else")
	}
}

// TestForkRepairAmbiguityResolvedLandedRotates is test 1's clearUncertaintyLocked case
// (M11).
//
// NOT parallel: it installs a process-global store-result hook, and a hook changes the
// behaviour of every test running beside it. Go runs the serial tests to completion before the
// parallel ones resume, which is what keeps them from overlapping.
func TestForkRepairAmbiguityResolvedLandedRotates(t *testing.T) {
	{
		e := testNewRejectionEnv(t, "rot-uncertain")
		e.sess.mu.Lock()
		e.sess.subs = map[string]byte{"rot/parent": 1}
		e.sess.mu.Unlock()
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("parent save: %v", err)
		}

		// A write that LANDS and whose response is lost: the record moves, memory does
		// not adopt it, and the resolver's readback proves the bytes durable.
		e.sess.mu.Lock()
		e.sess.subs = map[string]byte{"rot/parent": 1, "rot/landed": 2}
		e.sess.mu.Unlock()
		restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if sess != e.sess {
				return resp, err
			}
			return nil, errMQTTTestLostResponse
		})
		serr := testSessSave(t, e.sess)
		restore()
		if serr == nil {
			t.Fatal("the save must come back ambiguous")
		}
		if !e.sess.isSeqUncertain() {
			t.Fatal("an ambiguous save must arm the uncertainty")
		}
		if res, err := e.sess.resolveUncertainWrite(); res != mqttUncertainOurs {
			t.Fatalf("the landed bytes must resolve OURS: %v (%v)", res, err)
		}
		snap, parent := testGrantOf(t, e.sess)
		if snap == nil || parent == nil {
			t.Fatal("a resolution proving the bytes durable must rotate the pair")
		}
		if _, ok := snap.filters["rot/landed"]; !ok {
			t.Fatalf("the base must be the grant the PROVEN BYTES carry, got %v", snap.filters)
		}
		if _, ok := parent.filters["rot/landed"]; ok {
			t.Fatal("the parent must be the record those bytes replaced")
		}
	}
}

// TestForkRepairRepairSaveLandedRotates is test 1's mqttRepairSaveLanded case (M12).
//
// resolveContendedRecordSave proves a 10164-rejected write durable outside saveWithDetail's
// own success arm — by an exact-byte retry, or by finding its digest at a newer sequence —
// and every such arm adopts the sequence, the lineage AND the grant through
// adoptRecordLineageLocked. A site that moved the first two while leaving the grant stale
// would measure the next fork's delta against a record two writes back and re-assert state
// the client never held.
//
// NOT parallel: it installs process-global store hooks.
func TestForkRepairRepairSaveLandedRotates(t *testing.T) {
	e := testNewRejectionEnv(t, "rot-repair-landed")
	e.dropPersistObservations(t, e.idHash)
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{"rl/parent": 1}
	e.sess.mu.Unlock()
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("parent save: %v", err)
	}

	// A save the store reports as 10164-contended while the record has NOT moved, so
	// resolveContendedRecordSave takes its at-base arm and the exact-byte retry lands.
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{"rl/parent": 1, "rl/landed": 2}
	e.sess.mu.Unlock()
	stop := testForceContendedRejection(t, e.sess)
	_, detail, serr := testSaveClassified(t, e.sess)
	stop()
	if serr == nil {
		t.Fatal("the forced save must be rejected")
	}
	if got := e.sess.resolveContendedRecordSave(&detail); got != mqttRepairSaveLanded {
		t.Fatalf("the identical retry must LAND: got %v", got)
	}

	snap, parent := testGrantOf(t, e.sess)
	if snap == nil || parent == nil {
		t.Fatal("a repair-landed proof must rotate the pair")
	}
	if _, ok := snap.filters["rl/landed"]; !ok {
		t.Fatalf("the base must be the grant the PROVEN BYTES carry, got %v", snap.filters)
	}
	if _, ok := parent.filters["rl/landed"]; ok {
		t.Fatal("the parent must be the record those bytes replaced")
	}
}

// TestForkRepairMultiLevelDrainsTransitionIndependently is test 10c, and it is the
// discriminator for M7c and M9d.
//
// A "foo/#" filter drains BOTH sids, and each completes on its own. The refusal is therefore
// per sid: the main sid's drains differ, so it is refused, while the companion's acknowledged
// completion is still re-asserted. Comparing drains by sequence MEMBERSHIP alone (M7c), or
// applying the differing row instead of refusing it (M9d), both make the main sid's drain
// change hands — which 10d cannot see, because there "equal so do nothing" and "differing so
// refuse" leave the survivor's drain identical either way.
func TestForkRepairMultiLevelDrainsTransitionIndependently(t *testing.T) {
	t.Parallel()
	const filter = "mld.a.>"
	main := filter
	companion := "mld.a" + mqttMultiLevelSidSuffix

	e := testStageForkedGrant(t, "ml-drains",
		func(s *mqttSession) {
			s.subs = nil
			testInstallDrain(s, main, testPersistedDrain("mld-main",
				mqttPersistedDrainSeq{Seq: 91, PI: 7}))
			testInstallDrain(s, companion, testPersistedDrain("mld-fwc",
				mqttPersistedDrainSeq{Seq: 92, PI: 8}))
		},
		func(s *mqttSession) {
			// The main sid's exchange reached PUBREC here; the companion's last claim
			// completed AND its owned retirement transition marked it, which is what makes
			// our record omit that drain entirely. The mark is the authority for omission,
			// so clearing the claim alone would leave the companion still named and this
			// fixture would never reach the state it is describing.
			s.drains[main].seqs[91].intent = true
			s.drains[companion].seqs[92].claimed = false
			s.drains[companion].removalMarked = true
		}, nil)

	sess := e.reconnect(t)
	mainDrain, indexed := testDrainOf(t, sess, main)
	if mainDrain == nil || !indexed {
		t.Fatal("the main sid's drain differs, so it is refused and the survivor's stays")
	}
	if st := mainDrain.seqs[91]; st == nil || st.intent {
		t.Fatalf("a differing drain is left to the survivor VERBATIM, not merged: got %+v", st)
	}
	sess.mu.Lock()
	companionDrain := sess.drains[companion]
	sess.mu.Unlock()
	if companionDrain != nil {
		t.Fatal("the refusal is PER SID: the companion's acknowledged completion must still " +
			"be re-asserted, because a multi-level filter's two sids complete independently")
	}
}

// TestForkRepairALandedReconnectInvalidatesAnotherBrokersCapsule is test 22.
//
// A capsule is memory, so two brokers that both conceded to one survivor each hold their own.
// Only ONE of them can apply: the first reconnect whose accept CAS lands advances the record
// identity, after which every other broker's anchor sees a record that is a CHILD of the
// survivor rather than its sibling, and refuses permanently — record identities are fresh
// NUIDs and never recur, so no second mechanism is needed to expire the loser's capsule.
//
// Two real account managers over one $MQTT_sess, which is the cross-broker boundary.
func TestForkRepairALandedReconnectInvalidatesAnotherBrokersCapsule(t *testing.T) {
	t.Parallel()
	e := testStageForkedGrant(t, "two-losers", nil,
		func(s *mqttSession) { s.subs = map[string]byte{"tl/a": 1} }, nil)
	if e.capsuleOrNil() == nil {
		t.Fatal("fixture: this broker must hold a repair")
	}

	// Another broker resumes the surviving record and lands an accept save of its own.
	_, peerSess, _ := e.peer(t, "node2", e.clientID)
	if err := testSessSave(t, peerSess); err != nil {
		t.Fatalf("the peer's accept-equivalent save: %v", err)
	}

	// Our client now reconnects here. The record it restores is the PEER's, a child of the
	// survivor, so the anchor refuses and this client is not repaired.
	sess, _, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	tok := sess.beginRecordTx()
	if ferr := e.asm.reassertForkedGrantInto(sess, tok); ferr != nil {
		t.Fatalf("the fork repair refused: %v", ferr)
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if _, ok := testSubsOf(t, sess)["tl/a"]; ok {
		t.Fatal("a capsule whose survivor is no longer the current record must never be " +
			"applied: the record now descends from another broker's write")
	}
	if e.capsuleOrNil() != nil {
		t.Fatal("and it must be dropped, not retained: its anchor can never match again")
	}
}

// TestForkRepairRefusesAnOversizeRecord is the bound on retained bytes.
//
// Two record bodies per resident session is this feature's whole memory cost, and it is
// bounded rather than trusted: a session whose record exceeds mqttGrantBodyMax retains
// NOTHING and forgoes the repair, which is the same unrepairable state a
// restored-but-never-resaved session is in. Stated as a residual in docs/CONFORMANCE.md.
func TestForkRepairRefusesAnOversizeRecord(t *testing.T) {
	t.Parallel()
	e := testNewRejectionEnv(t, "oversize-record")
	e.dropPersistObservations(t, e.idHash)

	// A parent small enough to retain, so the refusal below is about the CHILD's size.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("parent save: %v", err)
	}
	if body, _ := testGrantOf(t, e.sess); body == nil {
		t.Fatal("fixture: an ordinary record must be retained")
	}

	// Now a record past the bound. Real membership, not padding: this is the shape a session
	// with a very large subscription set has.
	e.sess.mu.Lock()
	e.sess.subs = make(map[string]byte, 4000)
	for i := 0; i < 4000; i++ {
		e.sess.subs["oversize.filter.with.a.realistic.length."+strconv.Itoa(i)] = 1
	}
	e.sess.mu.Unlock()
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("oversize save: %v", err)
	}
	e.sess.mu.Lock()
	recLen := len(e.sess.buildPersistedSessionLockedBodyForTest())
	e.sess.mu.Unlock()
	if recLen <= mqttGrantBodyMax {
		t.Fatalf("fixture: the record must exceed the bound, got %d bytes", recLen)
	}

	snap, parent := testGrantOf(t, e.sess)
	if snap != nil || parent != nil {
		t.Fatal("a record past the bound must be retained NEITHER as the base nor as a parent: " +
			"keeping only one would leave a delta computed against a record two writes back")
	}

	// And therefore a recognised fork mints nothing rather than repairing from stale bytes.
	testLandSibling(t, e.testInflightEnv, testMutateSibling)
	e.rejectSave(t, e.sess)
	if got := e.sess.settleRecordRejection(e.asm); got != mqttRejectionForked {
		t.Fatalf("verdict = %v, want forked", got)
	}
	testAssertNotMinted(t, e)
}

// TestForkRepairRefusesToMergeOverAnUnreadableSurvivor is the fail-closed half of the
// retention bound, and the regression test for a review-caught HIGH: an OVERSIZE survivor.
//
// The restore retains no body for a record past mqttGrantBodyMax, but the anchor consults
// only lineage — so it still licenses a merge, and a survivor grant defaulted to empty would
// make every conflict invisible: filters the survivor decided about would read as
// absent-therefore-unconflicted, and the losing branch would overwrite grant decisions the
// survivor actually made. Survivor-wins inverted. The merge must instead be refused OUTRIGHT
// — no partial repair — and the capsule dies, because this CONNECT's accept save makes the
// record ours and the anchor would refuse it permanently anyway.
func TestForkRepairRefusesToMergeOverAnUnreadableSurvivor(t *testing.T) {
	t.Parallel()
	// Base lacks u/a; ours adds it at QoS 1. The survivor ALSO decides about u/a — QoS 2 —
	// which is exactly the conflict a nil survivor grant cannot see.
	e := testStageForkedGrant(t, "oversize-survivor",
		nil,
		func(s *mqttSession) { s.subs = map[string]byte{"u/a": 1} },
		func(ps, parent *mqttPersistedSession) {
			ps.Subs = map[string]byte{"u/a": 2}
			// And thousands of other filters, pushing the record past the retention bound —
			// the shape a large session legitimately has.
			for i := 0; i < 4000; i++ {
				ps.Subs["oversize.survivor.filter.with.a.realistic.length."+strconv.Itoa(i)] = 1
			}
		})

	sess, present, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
	if err != nil || !present {
		t.Fatalf("restore the surviving record: present=%v err=%v", present, err)
	}
	if body, _ := testGrantOf(t, sess); body != nil {
		t.Fatal("fixture: the oversize survivor must have been refused retention, or this " +
			"tests nothing")
	}
	tok := sess.beginRecordTx()
	if ferr := e.asm.reassertForkedGrantInto(sess, tok); ferr != nil {
		t.Fatalf("the fork repair refused: %v", ferr)
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}

	if got := testSubsOf(t, sess)["u/a"]; got.qos != 2 {
		t.Fatalf("the survivor's own grant decision must be untouched: a merge over an "+
			"unreadable survivor cannot SEE conflicts, so it must not run at all — got %+v", got)
	}
	if e.capsuleOrNil() == nil {
		t.Fatal("the capsule must be RETAINED, not discarded: this pass's accept save can be " +
			"rejected and T2-re-driven to a second, READABLE sibling of the same parent, " +
			"against which the repair still applies (review round 3)")
	}

	// The second-sibling re-drive, which is why retention matters: another stale broker
	// replaces the oversize survivor with a smaller sibling of the SAME parent — the same
	// leader-transition mechanism that forked the record in the first place. Our accept CAS
	// is rejected by that write and T2 re-enters the authoritative restore, which is what
	// this second pass performs. The new record is anchor-eligible AND materialisable, so
	// the merge that the oversize pass refused now applies.
	testLandSibling(t, e.testInflightEnv, func(ps *mqttPersistedSession) {
		testCopyGrantInto(ps, &e.parentRec)
	})
	second := e.reconnect(t)
	if got := testSubsOf(t, second)["u/a"]; !got.present || got.qos != 1 {
		t.Fatalf("the re-driven pass against a readable sibling must merge the retained "+
			"capsule: got %+v", got)
	}
}

// TestForkRepairRefusesAFilterWithAQuarantinedSid is round 18's second HIGH finding, and it
// is the SAME property as the processSubs preflight at the OTHER caller of
// adoptDrainLocked.
//
// Repair mutated sess.subs and the option maps first, then adopted per sid and DISCARDED
// adoptDrainLocked's refusal. For a wildcard with a usable main drain and a quarantined
// companion that reported the filter applied, having adopted the main drain while the
// companion refused — and the accept save then persists the live membership and consumer
// but no longer the adopted drain's Started obligations. Exactly the partial adoption the
// processSubs preflight exists to prevent, reached through the other door.
//
// The whole change is refused, which is the right disposition because a half-applied filter
// loses obligations PERMANENTLY while a refused one loses only the candidate change. It is
// NOT a convergence, and saying so was wrong: mergeForkedGrantLocked records the filter as a
// conflict and completes, so after the accept save the capsule no longer authorizes that
// merge and no resume retries it. See
// TestForkRepairRefusedFilterIsCountedAsAConflictAndTheMergeCompletes.
func TestForkRepairRefusesAFilterWithAQuarantinedSid(t *testing.T) {
	t.Parallel()
	const filter = "q.>"
	companion := filter[:len(filter)-2] + mqttMultiLevelSidSuffix

	// Built by hand rather than through the fork fixture, because the state under test is
	// one the fixture's own reconnect cannot leave behind: our record has to describe the
	// filter as LIVE (the adopt row) while the session still HOLDS both drains in memory.
	// A PUBLISH-phase claim, deliberately: F-094's widened hold makes every past-PUBREC
	// intent hold its sid, so the valid claimed intent this fixture used to carry would now
	// be held for its OWN reason and the companion would prove nothing. The subject is
	// wildcard atomicity, so the main drain must be genuinely unheld.
	usable := mqttDrainFromPersisted(testPersistedDrain("q-main-dur",
		mqttPersistedDrainSeq{Seq: 21, PI: 4}))
	quarantined := mqttDrainFromPersisted(testPersistedDrain("q-comp-dur",
		// Past PUBREC with NO identifier: quarantined on restore.
		mqttPersistedDrainSeq{Seq: 22, PubRel: true, PI: 0,
			OwnerNonce: "xxxxxxxxxxxxxxxxxxxxxx"}))
	if usable == nil || quarantined == nil {
		t.Fatal("both drains must restore")
	}
	sess := &mqttSession{
		drains:     map[string]*mqttDrain{filter: usable, companion: quarantined},
		drainByDur: map[string]*mqttDrain{"q-main-dur": usable, "q-comp-dur": quarantined},
		subs:       map[string]byte{},
	}

	// The premise both ways: the main sid is usable and adoptable, the companion is not.
	// Without this a refusal proves nothing about which sid caused it.
	sess.mu.Lock()
	mainUsable := sess.drainSidHoldLocked(filter) == mqttDrainNotHeld
	compQuarantined := sess.drainSidHoldLocked(companion) == mqttDrainHeldUnusableIdentity
	sess.mu.Unlock()
	if !mainUsable {
		t.Fatal("the main drain must be USABLE, or a refusal proves nothing about the companion")
	}
	if !compQuarantined {
		t.Fatal("the companion must be QUARANTINED")
	}

	// Our side says the filter is LIVE on both sids — the re-SUBSCRIBE-over-a-drain row
	// that adopts.
	ours := &mqttGrantEntry{
		mqttGrantTerms: mqttGrantTerms{present: true, qos: 1},
		sids: map[string]mqttSidState{
			filter:    {con: testConsumerConfig("q-main-dur")},
			companion: {con: testConsumerConfig("q-comp-dur")},
		},
	}
	survivor := &mqttGrantEntry{
		sids: map[string]mqttSidState{
			filter:    {drain: testPersistedDrain("q-main-dur")},
			companion: {drain: testPersistedDrain("q-comp-dur")},
		},
	}

	sess.mu.Lock()
	applied := sess.applyGrantChangeLocked(mqttGrantChange{filter: filter, ours: ours}, survivor)
	// Nothing moved: both drains still own their sids, no consumer was installed for
	// either, and the filter is not in the membership map. That last one is the assertion
	// the defect failed — repair mutated membership FIRST, so the accept save would then
	// persist a live filter whose drain obligations it no longer carries.
	stillMain, stillComp := sess.drains[filter], sess.drains[companion]
	_, mainLive := sess.cons[filter]
	_, compLive := sess.cons[companion]
	_, inSubs := sess.subs[filter]
	sess.mu.Unlock()

	if applied {
		t.Error("a filter with a quarantined sid must be REFUSED as a whole")
	}
	if stillMain != usable {
		t.Errorf("the refused repair adopted the main drain away: %v", stillMain)
	}
	if stillComp != quarantined {
		t.Errorf("the refused repair moved the companion drain: %v", stillComp)
	}
	if mainLive || compLive {
		t.Errorf("a refused repair must install NO consumer, got main=%v comp=%v", mainLive, compLive)
	}
	if inSubs {
		t.Error("a refused repair must not leave the filter in the membership map")
	}
}

// TestForkRepairAppliesADestroyedQoS0Downgrade is T12 of the consumer-release item.
//
// Fork repair's per-sid state comes from the RECORD's Cons and Drains presence, independent
// of QoS (mqttGrantFromRecord). Before the release moved to the final-update boundary, a
// destroyed downgrade left its consumer in the record, so the sid read `live` on BOTH sides
// and applyGrantSidLocked took its live x live row — "the survivor's consumer config is
// PRESERVED" — and the repaired session ended holding a cons entry for a QoS 0 membership.
// That is the state sess_fork_repair.go:1035 names as the harm in the mirror direction: a
// later re-SUBSCRIBE returns through processJSConsumer's existing-consumer arm without
// rebuilding delivery interest, and the filter silently stops delivering.
//
// So the assertion is the TRANSITION — the merged session ends with the membership at QoS 0
// and NO consumer for that sid — not the repair tally, which was already 1 either way.
//
// OUR record is produced through the production boundary helper rather than by poking the
// map, because a fixture that writes the absence itself proves nothing about the boundary.
func TestForkRepairAppliesADestroyedQoS0Downgrade(t *testing.T) {
	t.Parallel()
	const sid = "forkdg/a"
	cc := &ConsumerConfig{Durable: "forkdg_durable", DeliverSubject: mqttSubPrefix + "forkdg"}

	var releasedOK bool
	e := testStageForkedGrant(t, "fork-qos0-downgrade",
		func(s *mqttSession) {
			// BASE: an ordinary QoS 1 subscription with its consumer.
			s.subs = map[string]byte{sid: 1}
			s.cons = map[string]*ConsumerConfig{sid: cc}
		},
		func(s *mqttSession) {
			// OURS: the downgrade, driven through the PRODUCTION boundary helper — the
			// membership goes to QoS 0 and releaseDowngradedConsumersLocked gives the entry
			// up, exactly as the packet's final update does. Called with sess.mu held.
			//
			// FRESH MAPS, not in-place mutation. buildPersistedSessionLocked returns Subs
			// and Cons BY REFERENCE, and the fixture kept the parent record it built from
			// this same session — so mutating either map in place rewrites the parent
			// snapshot too, and the "survivor" then carries our downgrade and reads as
			// having decided the grant itself. The filter is counted a conflict and the
			// repair never runs.
			s.subs = map[string]byte{sid: 0}
			s.cons = map[string]*ConsumerConfig{sid: cc}
			releasedOK = s.releaseDowngradedConsumersLocked([]*mqttDeferredDel{{sid: sid, cc: cc}})
		},
		nil)
	if !releasedOK {
		t.Fatal("the fixture never reached the route it names: the production release must " +
			"have taken the entry, or OUR record is not a boundary-produced one")
	}

	sess := e.reconnect(t)

	terms := testSubsOf(t, sess)
	got, present := terms[sid]
	if !present || got.qos != 0 {
		t.Fatalf("the merged session must carry the membership at QoS 0, got %+v (present %v)", got, present)
	}
	sess.mu.Lock()
	live := sess.cons[sid]
	ps := sess.buildPersistedSessionLocked()
	sess.mu.Unlock()
	if live != nil {
		t.Fatalf("the repair must remove the survivor's in-memory consumer reference for a "+
			"membership our branch downgraded to QoS 0, got %+v", live)
	}
	if rec := ps.Cons[sid]; rec != nil {
		t.Fatalf("and the next record must not name it either, got %+v", rec)
	}
}

// TestForkRepairConflictsWhenTheSurvivorDowngraded is T13, and it exists so a future change
// cannot quietly turn a conflict into an applied repair on the strength of the new sid
// shape.
//
// Two arms, and both must be CONFLICTS. The reason is the TERMS, not the sid:
// mqttSurvivorDecidedGrant compares survivor.terms() against base.terms() FIRST and returns
// there, and QoS 0 differs from QoS 1. The sid comparison could not decide it either way —
// mqttSidShape has two values and deliberately does not distinguish "live consumer" from
// "no consumer", so moving a sid between live and absent is invisible to the conflict
// predicate by design.
//
// The second arm — both branches downgraded — is the one an earlier revision of this
// item's plan got wrong, calling it a no-op. Equality between ours and the survivor does not
// bypass the survivor-versus-base comparison.
func TestForkRepairConflictsWhenTheSurvivorDowngraded(t *testing.T) {
	t.Parallel()
	const sid = "forkdgs/a"
	cc := &ConsumerConfig{Durable: "forkdgs_durable", DeliverSubject: mqttSubPrefix + "forkdgs"}

	run := func(t *testing.T, oursDowngrades bool) {
		t.Helper()
		e := testStageForkedGrant(t, "fork-qos0-survivor-"+map[bool]string{true: "both", false: "one"}[oursDowngrades],
			func(s *mqttSession) {
				s.subs = map[string]byte{sid: 1}
				s.cons = map[string]*ConsumerConfig{sid: cc}
			},
			func(s *mqttSession) {
				// Fresh maps: the builder hands them out by reference and the fixture kept
				// the parent record built from this session.
				if oursDowngrades {
					s.subs = map[string]byte{sid: 0}
					s.cons = map[string]*ConsumerConfig{sid: cc}
					s.releaseDowngradedConsumersLocked([]*mqttDeferredDel{{sid: sid, cc: cc}})
					return
				}
				// Ours changes something ELSE about the filter, so a change is minted at
				// all: without a delta there is nothing for the survivor to conflict with.
				s.subs = map[string]byte{sid: 2}
				s.cons = map[string]*ConsumerConfig{sid: cc}
			},
			func(ps *mqttPersistedSession, parent *mqttPersistedSession) {
				// THE SURVIVOR downgraded: fresh maps again, and its consumer is released
				// the way the boundary releases one.
				ps.Subs = map[string]byte{sid: 0}
				ps.Cons = nil
			})

		sess := e.reconnect(t)
		terms := testSubsOf(t, sess)
		got, present := terms[sid]
		if !present || got.qos != 0 {
			t.Fatalf("the SURVIVOR's grant must stand: it decided QoS 0 for this filter and "+
				"nothing orders the two acknowledgements, got %+v (present %v)", got, present)
		}
		sess.mu.Lock()
		live := sess.cons[sid]
		sess.mu.Unlock()
		if live != nil {
			t.Fatalf("the survivor released its own consumer, so the repair must not put one "+
				"back, got %+v", live)
		}
	}

	t.Run("only the survivor downgraded", func(t *testing.T) { t.Parallel(); run(t, false) })
	t.Run("both branches downgraded", func(t *testing.T) { t.Parallel(); run(t, true) })
}
