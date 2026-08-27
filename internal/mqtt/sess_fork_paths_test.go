package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
)

// The four paths that reported a takeover on evidence that could not support one. F-084
// has since withdrawn that claim from every record-derived route; what these paths now
// separate is a proven SIBLING (which mints a repair grant) from everything else.
//
// Each is driven with a REAL rejection against a REAL record that a real sibling moved,
// and each has a paired CHILD-SHAPED case: a change that reported forked for any moved
// record would satisfy the sibling half and fail the child-shaped half. Nothing here
// asserts only "not taken over" — every case asserts the exact verdict, because "not
// superseded" and "not 0x8E" are both satisfied by outcomes that are themselves wrong.

// testNewForkEnv is testNewRejectionEnv with the record saved a SECOND time, so the
// acknowledged pair has a non-empty parent.
//
// That is not incidental. A first create carries no parent, arm 2's ackPWID guard
// concedes, and every fork test built on a first create would pass against a deleted
// lineage rule. Production reaches this state on the second write of any session's life.
func testNewForkEnv(t *testing.T, clientID string) *testRejectionEnv {
	t.Helper()
	e := testNewRejectionEnv(t, clientID)
	// The second save also carries a GRANT CHANGE, so the record it replaces is a real
	// parent rather than a duplicate. Without it every fork test would have an empty delta,
	// and the repair assertions at each mint site would be vacuous: an empty delta mints
	// nothing by design. See sess_fork_repair.go.
	e.sess.mu.Lock()
	if e.sess.subs == nil {
		e.sess.subs = map[string]byte{}
	}
	e.sess.subs[testForkEnvDeltaFilter] = 1
	e.sess.mu.Unlock()
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("second save (the one that gives the record a parent): %v", err)
	}
	e.sess.mu.Lock()
	wid, pwid := e.sess.wid, e.sess.pwid
	e.sess.mu.Unlock()
	if wid == _EMPTY_ || pwid == _EMPTY_ {
		t.Fatalf("fixture: the acknowledged pair must be complete, got (%q,%q)", wid, pwid)
	}
	e.dropPersistObservations(t, e.idHash)
	return e
}

// testMutateSibling gives a staged record content of its own, so its digest can never
// match anything we attempted.
func testMutateSibling(ps *mqttPersistedSession) {
	ps.Subs = map[string]byte{"forked/marker": 1}
}

// ---------------------------------------------------------------------------
// Path 1: the definite rejection probe
// ---------------------------------------------------------------------------

// TestSessRejectionProbeClassifiesLineage is the definite-rejection path's verdict, both
// ways round.
//
// The sibling row is the finding: the probe's CAS is rejected, the record moved, and until
// now that alone was read as "another owner holds this session". The child-shaped row is
// what stops the fix from being a blanket collapse — the two classes must stay
// distinguishable, in the Reason String and in the mint (M5, M2b). Since F-084 they no
// longer differ in the reason CODE: neither proves a successor, so neither claims one.
func TestSessRejectionProbeClassifiesLineage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		stage func(*testing.T, *testInflightEnv) *mqttPersistedSession
		want  mqttRejectionVerdict
	}{
		{"claims our parent", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSibling(t, e, testMutateSibling)
		}, mqttRejectionForked},
		{"child-shaped body", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandChildRecord(t, e, testMutateSibling)
		}, mqttRejectionForeign},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewForkEnv(t, "rejfork-"+tc.name)
			tc.stage(t, e.testInflightEnv)

			// A real save, rejected by the stream because the record moved.
			e.rejectSave(t, e.sess)

			if got := e.sess.settleRecordRejection(e.asm); got != tc.want {
				t.Fatalf("settleRecordRejection = %v, want %v", got, tc.want)
			}
			// The verdict is reached inside the probe's own record transaction, and that is
			// where the repair is minted — covering both the unbound settlement here and the
			// bound one, which never disposes at all. See sess_fork_repair.go.
			if tc.want == mqttRejectionForked {
				testAssertMinted(t, e)
			} else {
				testAssertNotMinted(t, e)
			}
			// Both verdicts dispose: the surviving record is authoritative whoever wrote
			// it, and this object may not write. Only the CLAIM differs.
			if e.resident(e.sess) {
				t.Fatal("a disposed session must not stay resident")
			}
		})
	}
}

// TestSessForkedUnboundDisposesOrdinarily is test 2, and its point is that NOTHING
// observable differs on this path.
//
// The unbound settlement passes nil for the client, so disposeForeignSessionLocked returns
// before mqttNotifyForeignDisposal: no packet, and no log line. Revision 6 of the plan
// claimed this arm could avoid the foreign disposition altogether, which is impossible —
// settleRecordRejection has no other removal path, and adding one is the new disposition
// mechanism this item refuses to add. The arm exists so the classifier has ONE meaning at
// every call site, not because the distinction is visible here.
func TestSessForkedUnboundDisposesOrdinarily(t *testing.T) {
	t.Parallel()
	e := testNewForkEnv(t, "rejfork-unbound")
	testLandSibling(t, e.testInflightEnv, testMutateSibling)
	e.rejectSave(t, e.sess)

	e.sess.mu.Lock()
	bound := e.sess.c
	e.sess.mu.Unlock()
	if bound != nil {
		t.Fatal("fixture: this settlement is unbound-only")
	}

	if got := e.sess.settleRecordRejection(e.asm); got != mqttRejectionForked {
		t.Fatalf("verdict = %v, want forked", got)
	}
	if e.resident(e.sess) {
		t.Fatal("the session must be disposed through the ordinary foreign disposition")
	}
}

// TestSessForkedBoundIsUnsettledNotDisposed is tests 1 and 1a at the settlement level.
//
// A bound connection whose record forked is told 0x89 and torn down by the ORDINARY close
// bookkeeping — the Will by its own rules, the expiry save attempted, shared pending
// purged. That is what mqttBoundUnsettled already means, so the arm routes there rather
// than to a fifth settlement.
//
// The assertions that discriminate are the ones about what did NOT happen:
// disposeForeignStateLocked must not have run: the session is still bound and resident, so
// close bookkeeping still has something to do. Copying the foreign arm's shape — which
// DISPOSES — would evict a session on evidence that established no other owner (M6).
//
// The Will half of this contract is gone, and that is F-084: the foreign arm used to call
// mqttDiscardWillOnTakeover and now consumes, so "the Will must not have been discarded" no
// longer distinguishes the arms. What distinguishes them is the disposal.
func TestSessForkedBoundIsUnsettledNotDisposed(t *testing.T) {
	t.Parallel()
	e := testNewForkEnv(t, "rejfork-bound")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	testLandSibling(t, e.testInflightEnv, testMutateSibling)
	e.rejectSave(t, e.sess)

	outcome, stale := b.bc.settleBoundRejectionGated(e.asm, e.sess)
	if outcome != mqttBoundUnsettled {
		t.Fatalf("a forked record leaves ownership UNSETTLED: got %v", outcome)
	}
	if stale != nil {
		t.Fatalf("nothing was disposed, so there is no stale client to notify: %v", stale)
	}
	e.sess.mu.Lock()
	stillBound := e.sess.c == b.bc
	e.sess.mu.Unlock()
	if !stillBound {
		t.Fatal("the forked arm must leave the connection BOUND: clearing sess.c sends the " +
			"close through mqttHandleClosedClient's rebound arm, which skips the expiry save " +
			"and the shared-pending purge")
	}
	if !e.resident(e.sess) {
		t.Fatal("the forked arm must not evict the session; the foreign arm does")
	}
}

// TestSessForkedBoundPacketAnswers0x89 drives the same settlement through the real
// per-packet gate and reads the wire, which is where the reason byte is decided (M7).
func TestSessForkedBoundPacketAnswers0x89(t *testing.T) {
	t.Parallel()
	e := testNewForkEnv(t, "rejfork-wire")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	testLandSibling(t, e.testInflightEnv, testMutateSibling)
	e.rejectSave(t, e.sess)

	if err := b.deliver(t, []byte{mqttPacketPing, 0}); err == nil {
		t.Fatal("a packet on a forked session must not be served")
	}
	reasons := b.disconnectAttempts(t)
	if len(reasons) != 1 || reasons[0] != mqttReasonServerBusy {
		t.Fatalf("a forked record answers 0x89 'server busy', never 0x8E: got %x (wire %x)",
			reasons, b.out.bytes())
	}
	if bytes.Contains(b.out.bytes(), []byte(mqttReasonStrSessionTakenOver)) {
		t.Fatal("the takeover Reason String must never accompany a forked verdict")
	}
}

// ---------------------------------------------------------------------------
// Path 2: the ambiguity resolver
// ---------------------------------------------------------------------------

// testAmbiguousSave leaves the session with an unresolved write whose bytes never reached
// the stream — the "issued but did not land" half of ambiguity, which is the half a
// surviving sibling can be classified against.
func testAmbiguousSave(t *testing.T, sess *mqttSession) {
	t.Helper()
	mqttTestSessStoreSuppressV.Store(true)
	err := testSessSave(t, sess)
	mqttTestSessStoreSuppressV.Store(false)
	if err == nil {
		t.Fatal("the suppressed save must fail")
	}
	if !sess.isSeqUncertain() {
		t.Fatal("a suppressed save must leave the session unresolved")
	}
}

// TestSessForkOnAmbiguousResolution is test 8: the resolver's own verdict, both ways, plus
// the pending write carrying the ATTEMPT's identity rather than memory's (M15).
func TestSessForkOnAmbiguousResolution(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage func(*testing.T, *testInflightEnv) *mqttPersistedSession
		want  mqttUncertainResolution
	}{
		{"claims our parent", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSibling(t, e, testMutateSibling)
		}, mqttUncertainForked},
		{"child-shaped body", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandChildRecord(t, e, testMutateSibling)
		}, mqttUncertainForeign},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewForkEnv(t, "ambfork-"+tc.name)

			e.sess.mu.Lock()
			ackWID := e.sess.wid
			e.sess.mu.Unlock()

			testAmbiguousSave(t, e.sess)

			// The attempt's identity, not memory's: the pending write is what a readback
			// is compared against, and it must name the bytes that were sent.
			e.sess.mu.Lock()
			pw := e.sess.pendingWrite
			e.sess.mu.Unlock()
			if pw == nil {
				t.Fatal("an ambiguous write must record its identity")
			}
			if pw.pwid != ackWID {
				t.Fatalf("the attempt must be parented to the acknowledged record: pw.pwid=%q want %q",
					pw.pwid, ackWID)
			}
			if pw.wid == _EMPTY_ || pw.wid == ackWID {
				t.Fatalf("the attempt must carry its OWN minted identity: pw.wid=%q", pw.wid)
			}

			tc.stage(t, e.testInflightEnv)

			got, _ := e.sess.resolveUncertainWrite()
			if got != tc.want {
				t.Fatalf("resolveUncertainWrite = %v, want %v", got, tc.want)
			}
			// The resolver's first readback is a mint site: only a body claiming our parent licenses a
			// repair, so the child-shaped row must record nothing. See sess_fork_repair.go.
			if tc.want == mqttUncertainForked {
				testAssertMinted(t, e)
			} else {
				testAssertNotMinted(t, e)
			}
		})
	}
}

// TestSessForkedAmbiguityReportsUnsettledNotTakenOver is test 8a: the whole reported
// vocabulary, not only the byte.
//
// Checking the reason code alone would pass an implementation that tells a v5 client 0x89
// and "session taken over by a new connection" in the same packet, which is precisely the
// claim this item exists to withhold (M8, M8a).
func TestSessForkedAmbiguityReportsUnsettledNotTakenOver(t *testing.T) {
	e := testNewForkEnv(t, "ambfork-vocab")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	testAmbiguousSave(t, e.sess)
	testLandSibling(t, e.testInflightEnv, testMutateSibling)

	if got := e.sess.resolveUncertainNow(b.bc); got != mqttUncertainForked {
		t.Fatalf("resolveUncertainNow = %v, want forked", got)
	}
	if e.resident(e.sess) {
		t.Fatal("the forked ambiguity takes the same disposition a foreign one does")
	}

	reasons := b.disconnectAttempts(t)
	if len(reasons) != 1 || reasons[0] != mqttReasonServerBusy {
		t.Fatalf("reason codes %x, want exactly one 0x89 (wire %x)", reasons, b.out.bytes())
	}
	wire := b.out.bytes()
	if !bytes.Contains(wire, []byte(mqttReasonStrOwnershipUnsettled)) {
		t.Fatalf("the forked Reason String must be on the wire: %x", wire)
	}
	if bytes.Contains(wire, []byte(mqttReasonStrSessionTakenOver)) {
		t.Fatal("0x89 must never carry the takeover Reason String")
	}
}

// TestSessForkedNotificationKeepsBothGates pins that generalizing the notification tail did
// not lose either gate it carries (M9): a connection that was never told its CONNECT
// succeeded gets no packet ([MQTT-3.2.0-1]), and a 3.1.1 connection has no server
// DISCONNECT at all.
func TestSessForkedNotificationKeepsBothGates(t *testing.T) {
	t.Run("pre-CONNACK is silent", func(t *testing.T) {
		e := testNewForkEnv(t, "ambfork-gate-connack")
		b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
		b.forgetConnAck()
		testAmbiguousSave(t, e.sess)
		testLandSibling(t, e.testInflightEnv, testMutateSibling)

		if got := e.sess.resolveUncertainNow(b.bc); got != mqttUncertainForked {
			t.Fatalf("resolution = %v, want forked", got)
		}
		if reasons := b.disconnectAttempts(t); len(reasons) != 0 {
			t.Fatalf("CONNACK must be the first packet a client sees: got %x", reasons)
		}
	})

	t.Run("3.1.1 gets no DISCONNECT", func(t *testing.T) {
		e := testNewForkEnv(t, "ambfork-gate-v311")
		b := e.bindSocketAttemptLog(t, mqttProtoLevel)
		testAmbiguousSave(t, e.sess)
		testLandSibling(t, e.testInflightEnv, testMutateSibling)

		if got := e.sess.resolveUncertainNow(b.bc); got != mqttUncertainForked {
			t.Fatalf("resolution = %v, want forked", got)
		}
		if reasons := b.disconnectAttempts(t); len(reasons) != 0 {
			t.Fatalf("MQTT 3.1.1 has no server DISCONNECT: got %x", reasons)
		}
	})

	t.Run("a spent claim is not spoken over", func(t *testing.T) {
		e := testNewForkEnv(t, "ambfork-gate-claim")
		b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
		// Somebody else already owns this connection's one DISCONNECT.
		b.bc.mu.Lock()
		claimed := b.bc.mqttClaimDisconnectLocked()
		b.bc.mu.Unlock()
		if !claimed {
			t.Fatal("fixture: the first claim must succeed")
		}
		testAmbiguousSave(t, e.sess)
		testLandSibling(t, e.testInflightEnv, testMutateSibling)

		if got := e.sess.resolveUncertainNow(b.bc); got != mqttUncertainForked {
			t.Fatalf("resolution = %v, want forked", got)
		}
		if reasons := b.disconnectAttempts(t); len(reasons) != 0 {
			t.Fatalf("at most one server DISCONNECT per connection [MQTT-3.14.4-1]: got %x", reasons)
		}
	})
}

// TestSessUncertaintyResolutionAdoptsLineage is M13's paired test (10): resolving an
// ambiguity as LANDED must adopt the lineage the bytes proven DURABLE carry alongside the
// sequence, or the next save parents itself to a record that is no longer the base.
func TestSessUncertaintyResolutionAdoptsLineage(t *testing.T) {
	e := testNewForkEnv(t, "amb-adopt")

	// An ambiguity whose bytes DID land: the store succeeds and only the response is
	// lost, which is what the result seam models.
	restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess != e.sess {
			return resp, err
		}
		return nil, errors.New("mqtt test: the store landed but its response was lost")
	})
	err := testSessSave(t, e.sess)
	restore()
	if err == nil {
		t.Fatal("the save must come back unresolved")
	}
	e.sess.mu.Lock()
	pw := e.sess.pendingWrite
	e.sess.mu.Unlock()
	if pw == nil {
		t.Fatal("the ambiguous write must record its identity")
	}
	attemptWID, attemptPWID := pw.wid, pw.pwid

	if got, rerr := e.sess.resolveUncertainWrite(); got != mqttUncertainOurs {
		t.Fatalf("the bytes are durable, so the resolution is ours: got %v (%v)", got, rerr)
	}
	e.sess.mu.Lock()
	gotWID, gotPWID := e.sess.wid, e.sess.pwid
	e.sess.mu.Unlock()
	if gotWID != attemptWID || gotPWID != attemptPWID {
		t.Fatalf("resolution must adopt the ATTEMPT's lineage: got (%q,%q), want (%q,%q)",
			gotWID, gotPWID, attemptWID, attemptPWID)
	}

	// And the next ordinary save parents itself to what was adopted, which is the
	// consequence a stale pair would break.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("save after resolution: %v", err)
	}
	if after := testLoadRecordLineage(t, e.testInflightEnv); after.PWID != attemptWID {
		t.Fatalf("the next save must parent itself to the adopted identity: PWID=%q want %q",
			after.PWID, attemptWID)
	}
}

// ---------------------------------------------------------------------------
// The sequence fail-safe
// ---------------------------------------------------------------------------

// TestSessLowerSequenceSiblingIsReportedForeign is test 5e (M21).
//
// The lineage rule takes no sequence and must NOT replace the sequence fail-safe: every
// caller keeps its `> base` gate ahead of the lineage arms. A record BELOW the base means
// a reset or re-provisioned store, and adopting or even naming it would move state
// backwards — so it stays foreign however its identity compares.
//
// Driven at the classifier level because production cannot produce a lower sequence on a
// live stream; that is what makes it a fail-safe rather than a reachable path. Asserting
// only "nothing was adopted" would not discriminate: with lineage restricted to
// forked/not-forked, dropping the gate misreports the record as forked while still
// adopting nothing. The VERDICT is the only thing that catches it.
func TestSessLowerSequenceSiblingIsReportedForeign(t *testing.T) {
	t.Parallel()
	const (
		ourGen  = "gen-ours"
		ackWID  = "a"
		ackPWID = "p"
	)
	// A record that DOES claim our parent by identity, sitting below the base.
	sibling := testLineageRecord(ourGen, "b", ackPWID)
	body, err := json.Marshal(sibling)
	if err != nil {
		t.Fatalf("marshal the sibling fixture: %v", err)
	}
	below := &StoredMsg{Sequence: 9, Data: body}
	const base = 10

	t.Run("classifyRejectedProbe", func(t *testing.T) {
		d := mqttSessSaveDetail{baseSeq: base, rawErr: ApiErrors[JSStreamWrongLastSequenceConstantErr]}
		got := classifyRejectedProbe(saveDefinitelyFailed, d, below, nil, ackWID, ackPWID)
		if got != mqttRejectionForeign {
			t.Fatalf("a sibling BELOW the base stays foreign fail-safe: got %v", got)
		}
	})

	t.Run("classifyAttemptReadback", func(t *testing.T) {
		pw := &mqttPendingWrite{baseSeq: base, digest: mqttRecordDigest([]byte("other"))}
		got, seq := classifyAttemptReadback(below, nil, pw, ackWID, ackPWID)
		if got != mqttAttemptForeign || seq != 0 {
			t.Fatalf("a sibling BELOW the base stays foreign fail-safe: got (%v,%d)", got, seq)
		}
	})

	// The same fixture ABOVE the base is forked at both classifiers, which is what makes
	// the two rows above assertions about the GATE and not about the fixture.
	above := &StoredMsg{Sequence: 11, Data: body}
	t.Run("above the base, both classifiers report forked", func(t *testing.T) {
		d := mqttSessSaveDetail{baseSeq: base, rawErr: ApiErrors[JSStreamWrongLastSequenceConstantErr]}
		if got := classifyRejectedProbe(saveDefinitelyFailed, d, above, nil, ackWID, ackPWID); got != mqttRejectionForked {
			t.Fatalf("classifyRejectedProbe above the base = %v, want forked", got)
		}
		pw := &mqttPendingWrite{baseSeq: base, digest: mqttRecordDigest([]byte("other"))}
		if got, _ := classifyAttemptReadback(above, nil, pw, ackWID, ackPWID); got != mqttAttemptForked {
			t.Fatalf("classifyAttemptReadback above the base = %v, want forked", got)
		}
	})
}

// TestSessLowerSequenceSiblingAtEveryOtherGate is the rest of test 5e's matrix.
//
// The two classifiers above are not the only independent `> base` gates: two more
// production sites reach the lineage rule by their own route —
// disposalForDirectRejection (the CONNECT-time 10071 arms) and
// resolveContendedRecordSave's movedVerdict (the 10164 arms). Removing either one is
// invisible to a test that drives only the other, and each removal downgrades the
// documented reset/re-provisioned fail-safe: a lower-sequence record would take forked
// vocabulary on a store that was rebuilt underneath us.
//
// Driven against a real record, with the ATTEMPT's base placed ABOVE it — which is the
// only way to produce "record below the base" on a live stream, and is exactly the shape
// a re-provisioned store presents.
func TestSessLowerSequenceSiblingAtEveryOtherGate(t *testing.T) {
	t.Run("disposalForDirectRejection", func(t *testing.T) {
		e := testNewForkEnv(t, "gate-direct")
		testLandSibling(t, e.testInflightEnv, testMutateSibling)
		recSeq := testCurrentRecordSeq(t, e.testInflightEnv)

		// EVERY claim-bearing field, not just the byte. A mixed result — 0x8E with the
		// forked Reason String, or the forked log line — is the contradictory vocabulary
		// this change treats as the defect, so an assertion on `reason` alone would
		// accept exactly what the disposal struct exists to prevent. The close state is
		// excluded on purpose: nothing reads it.
		below := mqttSessSaveDetail{baseSeq: recSeq + 5, domainTk: e.sess.domainTk, cidHash: e.idHash}
		// UNSETTLED, not taken-over. The gate refuses to read a below-base record's identity
		// at all, so what survives is an unclassifiable observation — which says the subject
		// is not ours and says nothing about who holds it. Asserting the takeover vocabulary
		// here was asserting F-084's defect. The fork BIT must be false with it, because the
		// bit is what mints.
		belowVoc, belowForked := e.sess.disposalForDirectRejection(&below)
		testAssertVocabulary(t, "a sibling BELOW the base", belowVoc, mqttDisposalOwnershipUnsettled)
		if belowForked {
			t.Fatal("a sibling below the base must not report the fork bit: it would mint a grant")
		}
		// The same record, with the base beneath it, IS forked — so the row above is
		// about the gate and not about the fixture.
		at := mqttSessSaveDetail{baseSeq: recSeq - 1, domainTk: e.sess.domainTk, cidHash: e.idHash}
		atVoc, atForked := e.sess.disposalForDirectRejection(&at)
		testAssertVocabulary(t, "the same record above the base", atVoc, mqttDisposalForked)
		if !atForked {
			t.Fatal("a sibling above the base must report the fork bit")
		}
	})

	t.Run("resolveContendedRecordSave", func(t *testing.T) {
		e := testNewForkEnv(t, "gate-contended")

		stop := testForceContendedRejection(t, e.sess)
		_, detail, serr := testSaveClassified(t, e.sess)
		stop()
		if serr == nil {
			t.Fatal("the forced save must be rejected")
		}
		testLandSibling(t, e.testInflightEnv, testMutateSibling)
		recSeq := testCurrentRecordSeq(t, e.testInflightEnv)

		below := detail
		below.baseSeq = recSeq + 5
		if got := e.sess.resolveContendedRecordSave(&below); got != mqttRepairSaveForeign {
			t.Fatalf("a sibling BELOW the base stays foreign fail-safe: got %v", got)
		}
		if got := e.sess.resolveContendedRecordSave(&detail); got != mqttRepairSaveForked {
			t.Fatalf("above its own base the same record is forked: got %v", got)
		}
	})

}

// testCurrentRecordSeq returns the sequence the session record currently sits at.
func testCurrentRecordSeq(t *testing.T, e *testInflightEnv) uint64 {
	t.Helper()
	smsg, err := e.sess.jsa.loadSessionMsg(e.sess.domainTk, e.sess.idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	return smsg.Sequence
}

// ---------------------------------------------------------------------------
// Path 4: a live competing owner
// ---------------------------------------------------------------------------

// TestSessForkedWithALiveCompetingOwnerIsStillUnsettled pins the VERDICT on the shape that
// produces this change's sharpest residual.
//
// A body claiming our parent does not prove the survivor is an ORPHAN. A live competitor
// whose write reached the subject by a route no CAS excludes produces exactly this shape
// with a real owner on the other side — a genuine takeover — and the fork verdict cannot
// tell that apart from a write whose author is gone. The bound arm therefore treats both as unsettled rather than
// as a takeover, which is what this test asserts: the competing writer is real and resident,
// its record is the one that survives, and the verdict is still mqttBoundUnsettled with
// nothing disposed.
//
// The CONSEQUENCE of that verdict — the Will published where a demonstrated takeover would
// have suppressed it — is asserted end to end by
// TestMQTTForkedRecordE2EWithALiveCompetingOwnerPublishesTheWill, which is where a Will
// publication can actually be observed on the wire. This test deliberately claims only
// what it checks.
func TestSessForkedWithALiveCompetingOwnerIsStillUnsettled(t *testing.T) {
	t.Parallel()
	const cid = "forklive"
	e := testNewRejectionEnv(t, cid)
	e.dropPersistObservations(t, e.idHash)

	// The competing broker restores the record as it stands. Its acknowledged pair is
	// therefore the SAME record ours is, which is what makes the two writes below
	// siblings rather than parent and child.
	peerASM, peerSess, _ := e.peer(t, "node2", cid)
	if peerASM == nil {
		t.Fatal("the peer must have its own account session manager")
	}
	// The peer's OWN persist observation has to be dropped as well, or our save below
	// evicts its session through the peer's persist-observation worker — and the
	// fixture would then have no live competing owner at all. That eviction is
	// correct broker behavior; it is simply not the path under test.
	peerASM.mu.Lock()
	peerASM.testSessPersistDrop = func(h string) bool { return h == e.idHash }
	peerASM.mu.Unlock()
	peerASM.addSession(peerSess, true)

	// Our own second save: {a, p}. The peer still holds p as its acknowledged record.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("our save: %v", err)
	}
	e.sess.mu.Lock()
	ourWID, ourPWID := e.sess.wid, e.sess.pwid
	e.sess.mu.Unlock()

	// The peer's own write, derived from the record IT was acknowledged for, landed
	// unconditionally: the durable outcome of the transition. {b, p} — a sibling of ours,
	// written by a broker that is live and resident.
	peerSess.mu.Lock()
	peerRec := peerSess.buildPersistedSessionLocked()
	peerRec.Gen = peerSess.gen
	peerRec.WID, peerRec.PWID = mqttNewRecordWID(), peerSess.wid
	peerRec.Subs = map[string]byte{"forklive/peer": 1}
	peerSess.mu.Unlock()
	if peerRec.PWID != ourPWID {
		t.Fatalf("fixture: the peer must share OUR parent, got %q want %q", peerRec.PWID, ourPWID)
	}
	if peerRec.WID == ourWID {
		t.Fatal("fixture: the two writes must have distinct identities")
	}
	body, err := json.Marshal(&peerRec)
	if err != nil {
		t.Fatalf("marshal the peer record: %v", err)
	}
	if _, err := e.sess.jsa.storeSessionMsg(e.sess.domainTk, e.idHash, 0, body); err != nil {
		t.Fatalf("land the peer's record: %v", err)
	}

	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	e.rejectSave(t, e.sess)

	outcome, stale := b.bc.settleBoundRejectionGated(e.asm, e.sess)
	if outcome != mqttBoundUnsettled || stale != nil {
		t.Fatalf("a body claiming our parent is UNSETTLED even with a live competing owner: got %v (stale=%v)",
			outcome, stale)
	}
	// Not disposed, so the connection ends through mqttHandleClosedClient's ordinary
	// path — the one that marks the Will owed and publishes it.
	if !e.resident(e.sess) {
		t.Fatal("the forked arm must not dispose, or the Will consequence does not follow")
	}
	// The competing owner is still there: this body claiming our parent has a LIVE author, not an
	// orphan, which is the whole point of the fixture.
	peerASM.mu.RLock()
	_, peerResident := peerASM.sessions[cid]
	peerASM.mu.RUnlock()
	if !peerResident {
		t.Fatal("fixture: the competing owner must still be resident on its own broker")
	}
	e.sess.mu.Lock()
	stillBound := e.sess.c == b.bc
	e.sess.mu.Unlock()
	if !stillBound {
		t.Fatal("the connection must still be bound for ordinary close bookkeeping to run")
	}
}

// testAssertVocabulary checks every field a displacement CLAIMS, which is the whole reason
// the four travel as one value: a reason code paired with the other side's Reason String
// tells a v5 client two contradictory things in one packet.
func testAssertVocabulary(t *testing.T, what string, got, want mqttDisposalVocabulary) {
	t.Helper()
	if got.reason != want.reason {
		t.Fatalf("%s: reason code %x, want %x", what, got.reason, want.reason)
	}
	if got.reasonStr != want.reasonStr {
		t.Fatalf("%s: Reason String %q, want %q", what, got.reasonStr, want.reasonStr)
	}
	if got.logReason != want.logReason {
		t.Fatalf("%s: log line %q, want %q", what, got.logReason, want.logReason)
	}
}
