package mqtt

import (
	"encoding/json"
	"testing"
)

// Recognising a session record that FORKED from ours.
//
// The Raft window itself is unreachable from a test: it needs a proposal accepted under
// one leader, an election, and a second proposal admitted against the stale applied state
// the transition leaves behind. What IS reproducible, and what the broker actually has to
// act on, is the DURABLE OUTCOME that window produces — two records derived from one
// parent, the later surviving under MaxMsgsPer:1. That is staged here with an
// unconditional store, which needs no CAS and no cluster, and it is the same
// "pin the consequence, not the window" shape docs/CONFORMANCE.md already uses.
//
// What these tests therefore do NOT reproduce, stated so the coverage is not read as more
// than it is: pre-proposal admission against stale applied state, the cleared in-process
// map, two success PubAcks to distinct brokers, and a second election.

// testLineageRecord builds a record fixture with an explicit identity. Everything the
// classifier reads is stated at the call site — no fixture here relies on a zero value it
// did not name, EXCEPT where an absent field is the whole subject of the test.
func testLineageRecord(gen, wid, pwid string) *mqttPersistedSession {
	return &mqttPersistedSession{ID: "lineage", Gen: gen, WID: wid, PWID: pwid}
}

// TestSessLineageRuleArms is the classifier's whole truth table, driven directly.
//
// The rule is consulted only where a caller has ALREADY concluded foreign, so every row
// that is not a proven sibling must come back not-forked — that is the concession the
// change promises, and it is what keeps this a pure narrowing of an existing verdict.
func TestSessLineageRuleArms(t *testing.T) {
	const (
		ourGen  = "gen-ours"
		ackWID  = "a"
		ackPWID = "p"
	)
	for _, tc := range []struct {
		name  string
		found *mqttPersistedSession
		want  mqttRecordLineage
	}{
		// The one shape the rule exists for: same parent, different identity.
		{"proven sibling", testLineageRecord(ourGen, "b", "p"), mqttLineageForked},

		// 5c: a CHILD of our acknowledged record is a legitimate successor and a lost
		// race, not a fork. It must not become forked now that the sibling arm is
		// defined against the acknowledged pair.
		{"child of the acknowledged record", testLineageRecord(ourGen, "d", "a"), mqttLineageNotForked},

		// 4: our own in-flight attempt landing late. Its parent is the acknowledged
		// WID, not the acknowledged PWID, so it concedes without needing an arm of its
		// own.
		{"our own attempt", testLineageRecord(ourGen, "c", "a"), mqttLineageNotForked},

		// 3: Codex round 1's counterexample — a competing branch that never read our
		// record. Unrelated lineage must reach the default arm.
		{"unrelated lineage", testLineageRecord(ourGen, "d", "c"), mqttLineageNotForked},

		// 5f / M23: our acknowledged record's OWN identity, seen again. storeFramedSessionMsg
		// replays identical bytes deliberately and a clustered duplicate proposal can land
		// them at a different sequence, so this shape is reachable — and without the
		// found.WID != ackWID conjunct it is reported as a fork of itself.
		{"verbatim replay of the acknowledged record", testLineageRecord(ourGen, "a", "p"), mqttLineageNotForked},

		// 6 / M4: otherwise a proven sibling, so ONLY the generation arm can concede it.
		{"proven sibling of another incarnation", testLineageRecord("gen-other", "b", "p"), mqttLineageNotForked},

		// 5a / M2a: an absent identity with a matching parent. Without the
		// found.WID != "" guard, "" != ackWID satisfies the last conjunct and this
		// accuses a record that never carried an identity at all.
		{"absent found WID, matching parent", testLineageRecord(ourGen, _EMPTY_, "p"), mqttLineageNotForked},

		{"nil record", nil, mqttLineageNotForked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyRecordLineage(tc.found, ourGen, ackWID, ackPWID); got != tc.want {
				t.Fatalf("classifyRecordLineage(%+v) = %v, want %v", tc.found, got, tc.want)
			}
		})
	}
}

// TestSessAbsentAcknowledgedWIDIsNotASiblingBase is arm 2's ackWID != "" guard (M20).
//
// A correct writer cannot produce an acknowledged pair of {"", p} — the store boundary
// always mints — but createOrRestoreSession validates only Gen and adopts whatever lineage
// the JSON carries, so a hand-edited or future-broker record can. Without the guard, every
// survivor with parent p becomes a sibling of a record that has no identity of its own.
func TestSessAbsentAcknowledgedWIDIsNotASiblingBase(t *testing.T) {
	got := classifyRecordLineage(testLineageRecord("g", "b", "p"), "g", _EMPTY_, "p")
	if got != mqttLineageNotForked {
		t.Fatalf("an acknowledged pair with no identity of its own is no sibling base: got %v", got)
	}
}

// TestSessEmptyParentsAreNotSiblings is arm 2's ackPWID != "" guard (M3): two unrelated
// FIRST CREATES both carry an empty parent, and without the guard they read as siblings of
// each other.
func TestSessEmptyParentsAreNotSiblings(t *testing.T) {
	got := classifyRecordLineage(testLineageRecord("g", "b", _EMPTY_), "g", "a", _EMPTY_)
	if got != mqttLineageNotForked {
		t.Fatalf("two empty parents are not a shared ancestry: got %v", got)
	}
}

// TestSessTwoAbsentWIDsAreNotForked is the pre-change shape: neither record carries an
// identity, the bodies differ, and the verdict must be the one the broker reached before
// this change existed. A regression guard, with no paired mutation — several guards
// already cover the individual conjuncts, and this pins that they compose.
func TestSessTwoAbsentWIDsAreNotForked(t *testing.T) {
	got := classifyRecordLineage(testLineageRecord("g", _EMPTY_, _EMPTY_), "g", _EMPTY_, _EMPTY_)
	if got != mqttLineageNotForked {
		t.Fatalf("a record written before lineage existed must concede: got %v", got)
	}
}

// ---------------------------------------------------------------------------
// Stamping and provenance
// ---------------------------------------------------------------------------

// TestSessBuilderDoesNotMintLineage is M17's paired test, and the observation has to be
// made on the BUILDER rather than on any stored record.
//
// buildPersistedSessionLocked is also clear()'s pre-teardown snapshot, so minting there
// would write a parent pointer naming a record that was never stored. Every stored marker
// has its lineage overwritten by claimTeardown before anything can inspect it, which is
// why asserting on a marker cannot discriminate — two earlier revisions of this test
// paired M17 with the teardown test and it passed against the broken code both times.
func TestSessBuilderDoesNotMintLineage(t *testing.T) {
	e := testNewInflightEnv(t, "builder-lineage")
	e.sess.mu.Lock()
	ps := e.sess.buildPersistedSessionLocked()
	e.sess.mu.Unlock()
	if ps.WID != _EMPTY_ || ps.PWID != _EMPTY_ {
		t.Fatalf("the builder must mint no lineage: WID=%q PWID=%q", ps.WID, ps.PWID)
	}
}

// TestSessSaveStampsAndAdoptsLineage pins the store boundary's half: a save mints a fresh
// identity, parents it to the record the session was acknowledged for, and adopts the pair
// only once the write is CONFIRMED.
func TestSessSaveStampsAndAdoptsLineage(t *testing.T) {
	e := testNewInflightEnv(t, "save-lineage")

	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("first save: %v", err)
	}
	first := testLoadRecordLineage(t, e)
	if first.WID == _EMPTY_ {
		t.Fatal("a stored record must carry a minted identity")
	}
	if first.PWID != _EMPTY_ {
		t.Fatalf("a FIRST create has no parent: PWID=%q", first.PWID)
	}
	e.sess.mu.Lock()
	adoptedWID, adoptedPWID := e.sess.wid, e.sess.pwid
	e.sess.mu.Unlock()
	if adoptedWID != first.WID || adoptedPWID != first.PWID {
		t.Fatalf("a confirmed save must adopt the lineage it stamped: sess=(%q,%q) record=(%q,%q)",
			adoptedWID, adoptedPWID, first.WID, first.PWID)
	}

	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("second save: %v", err)
	}
	second := testLoadRecordLineage(t, e)
	if second.PWID != first.WID {
		t.Fatalf("the second save must parent itself to the first: PWID=%q want %q", second.PWID, first.WID)
	}
	if second.WID == first.WID {
		t.Fatal("every write mints its OWN identity; reusing the parent's makes a record indistinguishable from its parent")
	}
}

// TestSessRestoreAdoptsLoadedLineage is M16's paired test (12a).
//
// Without adoption at the restore, the first save after ANY restart is stamped with an
// empty parent — after which arm 2's ackPWID guard makes every sibling of that base
// unprovable, so detection would be inert after every restart even on a fully upgraded
// cluster. The assertion is deliberately BOTH halves: that the parent is the restored
// record's identity, and that the save's own identity is freshly minted, because the
// parent assertion alone does not pin the mint.
func TestSessRestoreAdoptsLoadedLineage(t *testing.T) {
	e := testNewInflightEnv(t, "restore-lineage")
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("seed the record: %v", err)
	}
	stored := testLoadRecordLineage(t, e)

	_, peerSess, _ := e.peer(t, "node2", "restore-lineage")
	peerSess.mu.Lock()
	gotWID, gotPWID := peerSess.wid, peerSess.pwid
	peerSess.mu.Unlock()
	if gotWID != stored.WID || gotPWID != stored.PWID {
		t.Fatalf("a restore must adopt the record's lineage: got (%q,%q), want (%q,%q)",
			gotWID, gotPWID, stored.WID, stored.PWID)
	}

	if err := testSessSave(t, peerSess); err != nil {
		t.Fatalf("save after restore: %v", err)
	}
	after := testLoadRecordLineage(t, e)
	if after.PWID != stored.WID {
		t.Fatalf("the first save after a restore parents itself to the restored record: PWID=%q want %q",
			after.PWID, stored.WID)
	}
	if after.WID == _EMPTY_ || after.WID == stored.WID {
		t.Fatalf("that save must mint its own identity: WID=%q (parent %q)", after.WID, stored.WID)
	}
}

// TestSessTeardownClaimParentsTheLoadedRecord is M11's and M12's paired test (9).
//
// The marker is a record write like any other, so it carries a lineage — a fresh identity
// of its own, parented to the record being retired. Two ways to get that wrong and both
// have a mutation: reusing the snapshot's identity makes the marker indistinguishable from
// its own parent, and resetting sess.wid before the capture leaves the parent empty.
func TestSessTeardownClaimParentsTheLoadedRecord(t *testing.T) {
	e := testNewInflightEnv(t, "teardown-lineage")
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("seed the record: %v", err)
	}
	retiring := testLoadRecordLineage(t, e)

	e.sess.mu.Lock()
	ps := e.sess.buildPersistedSessionLocked()
	seq, retiringWID := e.sess.seq, e.sess.wid
	e.sess.mu.Unlock()
	if retiringWID != retiring.WID {
		t.Fatalf("fixture: the session must hold the stored identity, got %q want %q", retiringWID, retiring.WID)
	}

	tok := e.sess.beginRecordTx()
	_, _, claim, err := e.sess.claimTeardown(tok, &ps, seq, retiringWID)
	if terr := e.sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if err != nil || claim != mqttTeardownClaimWon {
		t.Fatalf("claim the teardown: claim=%v err=%v", claim, err)
	}

	marker := testLoadRecordLineage(t, e)
	if marker.Teardown == nil {
		t.Fatal("the stored record must carry the teardown marker")
	}
	if marker.PWID != retiring.WID {
		t.Fatalf("the marker must parent the record it retires: PWID=%q want %q", marker.PWID, retiring.WID)
	}
	if marker.WID == _EMPTY_ || marker.WID == marker.PWID {
		t.Fatalf("the marker must mint its own identity: WID=%q PWID=%q", marker.WID, marker.PWID)
	}
}

// TestSessClearZeroesLineageAfterCapture pins the other half of clear(): once the record
// is gone the session must hold no parent pointer at it, or a REUSED object's first write
// would claim descent from a record that no longer exists.
func TestSessClearZeroesLineageAfterCapture(t *testing.T) {
	e := testNewInflightEnv(t, "clear-lineage")
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("seed the record: %v", err)
	}
	if err := e.sess.clear(true); err != nil {
		t.Fatalf("clear: %v", err)
	}
	e.sess.mu.Lock()
	wid, pwid, seq := e.sess.wid, e.sess.pwid, e.sess.seq
	e.sess.mu.Unlock()
	if wid != _EMPTY_ || pwid != _EMPTY_ {
		t.Fatalf("clear() must zero the lineage with the sequence: WID=%q PWID=%q seq=%d", wid, pwid, seq)
	}
}

// TestSessExactByteResendKeepsIdentity is M14's paired test (11).
//
// storeFramedSessionMsg replays the attempt's bytes VERBATIM, which is what makes the
// re-attempt an arbitration at the same CAS base and what keeps the digest a readback
// compares against intact. Re-stamping the identity on the way out would change the bytes
// and make the original landing unrecognisable as ours.
func TestSessExactByteResendKeepsIdentity(t *testing.T) {
	e := testNewInflightEnv(t, "resend-lineage")
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("seed the record: %v", err)
	}

	e.sess.mu.Lock()
	base := e.sess.seq
	ps := e.sess.buildPersistedSessionLocked()
	ps.WID, ps.PWID = mqttNewRecordWID(), e.sess.wid
	domainTk, idHash := e.sess.domainTk, e.sess.idHash
	e.sess.mu.Unlock()
	body, _ := json.Marshal(&ps)
	framed, hdr := mqttFrameRecordWrite(base, body)

	resp, err := e.sess.storeFramedSessionMsg(domainTk, idHash, hdr, framed)
	if err != nil {
		t.Fatalf("re-send the framed bytes: %v", err)
	}
	stored := testLoadRecordLineage(t, e)
	if stored.WID != ps.WID || stored.PWID != ps.PWID {
		t.Fatalf("a verbatim re-send must store the bytes UNCHANGED: got (%q,%q), want (%q,%q)",
			stored.WID, stored.PWID, ps.WID, ps.PWID)
	}
	if mqttRecordDigest(testLoadRecordBody(t, e)) != mqttRecordDigest(body) {
		t.Fatal("the re-sent record's digest must equal the attempt's, or a readback can never recognise it")
	}
	if resp.Sequence <= base {
		t.Fatalf("the re-send must land past its base: %d <= %d", resp.Sequence, base)
	}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// testLoadRecordLineage returns the session record as it currently stands.
func testLoadRecordLineage(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
	t.Helper()
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(testLoadRecordBody(t, e), ps); err != nil {
		t.Fatalf("decode the session record: %v", err)
	}
	return ps
}

// testLoadRecordBody returns the raw stored record bytes.
func testLoadRecordBody(t *testing.T, e *testInflightEnv) []byte {
	t.Helper()
	smsg, err := e.sess.jsa.loadSessionMsg(e.sess.domainTk, e.sess.idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	return smsg.Data
}

// testLandSibling stores a record derived from the SAME parent as the session's
// acknowledged one, UNCONDITIONALLY — no CAS header, so it lands whatever the subject's
// current sequence is.
//
// That is the durable end state a leader transition produces, staged without one: two
// records off a single base, the later surviving under MaxMsgsPer:1. It is deliberately
// not a CAS, because a CAS is exactly what cannot produce this outcome inside a single
// leader term — which is the whole finding.
//
// mutate is applied to the sibling's content so its digest differs from anything of ours;
// without that the record would classify as a byte-identical landing.
func testLandSibling(t *testing.T, e *testInflightEnv, mutate func(*mqttPersistedSession)) *mqttPersistedSession {
	t.Helper()
	e.sess.mu.Lock()
	sibling := e.sess.buildPersistedSessionLocked()
	sibling.Gen = e.sess.gen
	sibling.PWID = e.sess.pwid // the SAME parent our acknowledged record has
	sibling.WID = mqttNewRecordWID()
	domainTk, idHash := e.sess.domainTk, e.sess.idHash
	e.sess.mu.Unlock()
	if mutate != nil {
		mutate(&sibling)
	}
	body, _ := json.Marshal(&sibling)
	// hdr 0: an unconditional store. See the doc comment.
	if _, err := e.sess.jsa.storeSessionMsg(domainTk, idHash, 0, body); err != nil {
		t.Fatalf("land the sibling record: %v", err)
	}
	return &sibling
}

// testLandSuccessor is testLandSibling's counterpart: a record that is a legitimate CHILD
// of our acknowledged one. Every forked test has a paired successor test, because a change
// that reported forked for any moved record would pass the first and fail this.
func testLandSuccessor(t *testing.T, e *testInflightEnv, mutate func(*mqttPersistedSession)) *mqttPersistedSession {
	t.Helper()
	e.sess.mu.Lock()
	child := e.sess.buildPersistedSessionLocked()
	child.Gen = e.sess.gen
	child.PWID = e.sess.wid // derived FROM our acknowledged record
	child.WID = mqttNewRecordWID()
	domainTk, idHash := e.sess.domainTk, e.sess.idHash
	e.sess.mu.Unlock()
	if mutate != nil {
		mutate(&child)
	}
	body, _ := json.Marshal(&child)
	if _, err := e.sess.jsa.storeSessionMsg(domainTk, idHash, 0, body); err != nil {
		t.Fatalf("land the successor record: %v", err)
	}
	return &child
}

// TestSessRawWritersStampLineage covers the two record writers that do not go through
// saveWithDetail: the expiry sweep's disconnect stamp and the CONNECT rollback re-save.
//
// Both build their bytes from a record they already hold, so both must set PWID from THAT
// record's identity before minting their own. Getting the order wrong makes the write
// parent ITSELF, which is indistinguishable from a first create and silently ends the
// chain.
func TestSessRawWritersStampLineage(t *testing.T) {
	t.Run("the expiry sweep's disconnect stamp", func(t *testing.T) {
		e := testNewInflightEnv(t, "sweep-lineage")
		e.sess.mu.Lock()
		e.sess.expiryInterval = 3600
		e.sess.disconnectedAt = 0 // the record claims a CONNECTED client
		e.sess.mu.Unlock()
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("give the record a parent: %v", err)
		}
		before := testLoadRecordLineage(t, e)

		// The sweep only stamps a record it owns with NO session in memory: that is the
		// "we went down while the client was connected" shape it exists for.
		e.asm.mu.Lock()
		delete(e.asm.sessByHash, e.sess.idHash)
		e.asm.mu.Unlock()

		e.asm.sweepSessionExpiries(e.s)

		after := testLoadRecordLineage(t, e)
		if after.DisconnectedAt == 0 {
			t.Fatal("the sweep must stamp the disconnect, or this asserts nothing")
		}
		if after.PWID != before.WID {
			t.Fatalf("the stamp must parent the record it rewrites: PWID=%q want %q",
				after.PWID, before.WID)
		}
		if after.WID == _EMPTY_ || after.WID == after.PWID {
			t.Fatalf("the stamp must mint its own identity: WID=%q PWID=%q", after.WID, after.PWID)
		}
	})

	t.Run("the CONNECT rollback re-save", func(t *testing.T) {
		e := testNewInflightEnv(t, "rollback-lineage")
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("give the record a parent: %v", err)
		}
		e.asm.addSession(e.sess, true)
		e.sess.mu.Lock()
		candidate, candidateWID := e.sess.seq, e.sess.wid
		e.sess.mu.Unlock()

		// The record is untouched, so the rollback's re-save LANDS and its bytes are what
		// the assertion reads. A positive expiry selects the re-save over the delete.
		e.asm.rollbackAcceptedSession(e.s, e.sess, e.sess.idHash, candidate, 3600)

		after := testLoadRecordLineage(t, e)
		if after.DisconnectedAt == 0 {
			t.Fatal("the rollback must have re-saved the record as disconnected")
		}
		if after.PWID != candidateWID {
			t.Fatalf("the rollback re-save must parent the candidate record: PWID=%q want %q",
				after.PWID, candidateWID)
		}
		if after.WID == _EMPTY_ || after.WID == after.PWID {
			t.Fatalf("the rollback re-save must mint its own identity: WID=%q PWID=%q",
				after.WID, after.PWID)
		}
	})
}

// TestSessClearParentsTheTeardownMarker is M12's paired test, and the observation has to
// be made through clear() rather than through claimTeardown directly.
//
// claimTeardown takes the retiring identity as a PARAMETER, so a test that passes it
// explicitly proves nothing about the one production site that has to supply it: clear()
// captures it beside retiringGen and seq, BEFORE the reset that zeroes sess.wid a few
// lines later. Move the capture after the reset and the marker's parent comes out empty —
// invisible to every test that calls claimTeardown itself.
//
// The claimed hook is the seam: it fires with the marker stored and before the deletions
// that remove the record, which is the only window in which the marker is readable.
func TestSessClearParentsTheTeardownMarker(t *testing.T) {
	e := testNewInflightEnv(t, "clear-marker-lineage")
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("seed the record: %v", err)
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("give the record a parent: %v", err)
	}
	retiring := testLoadRecordLineage(t, e)

	var markerWID, markerPWID string
	var sawMarker bool
	restore := setMQTTTestTeardownClaimedHook(func(sess *mqttSession, nonce string, markerSeq uint64) {
		if sess != e.sess {
			return
		}
		ps := testLoadRecordLineage(t, e)
		if ps.Teardown == nil || ps.Teardown.Nonce != nonce {
			t.Errorf("the claimed hook must fire with OUR marker stored: %+v", ps.Teardown)
			return
		}
		markerWID, markerPWID, sawMarker = ps.WID, ps.PWID, true
	})
	defer restore()

	if err := e.sess.clear(true); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if !sawMarker {
		t.Fatal("the teardown claim must have been observed, or this asserts nothing")
	}
	if markerPWID != retiring.WID {
		t.Fatalf("clear() must capture the retiring identity BEFORE resetting it: marker "+
			"PWID=%q, want the retired record's WID %q", markerPWID, retiring.WID)
	}
	if markerWID == _EMPTY_ || markerWID == markerPWID {
		t.Fatalf("the marker must mint its own identity: WID=%q PWID=%q", markerWID, markerPWID)
	}
}
