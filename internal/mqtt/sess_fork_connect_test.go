package mqtt

import (
	"encoding/json"
	"sync/atomic"
	"testing"
)

// Path 3: the CONNECT-time classified saves.
//
// reconcileResumedConsumers and grantRequestedLifetime do not wait for the rejection
// settlement: they classify it themselves — directly on 10071, and through
// resolveContendedRecordSave on 10164. They no longer DISPOSE, though, which is what the
// pre-CONNACK-signal item changed: each returns an outcome and a vocabulary, and
// mqttProcessConnect's refuseDisposing aborts, signals the client and only then hands the
// session over. Disposing inside the helper is what left a CONNACK racing the socket close
// its own disposal scheduled. Two revisions of the plan enumerated "the two paths" and both
// were wrong; this is the third.
//
// The CHRONOLOGY here is not the obvious one and revision 8 got it wrong. mqttProcessConnect
// performs and CONFIRMS the accept save before calling either helper, so by the time one
// runs, the accept save has already advanced the acknowledged pair. A fixture built on the
// pre-accept pair is unreachable at these call sites, so every case below states all three
// identities explicitly.

// testConnectForkEnv stages the reachable CONNECT-time shape: the acknowledged pair is the
// ACCEPT SAVE's, and the survivor claims THAT record's parent rather than anything
// older.
type testConnectForkEnv struct {
	*testRejectionEnv
	survivor *mqttPersistedSession
}

// testNewConnectForkEnv builds it. stage decides whether the survivor claims our parent
// of the accept save or a child-SHAPED body claiming descent from it.
func testNewConnectForkEnv(t *testing.T, clientID string,
	stage func(*testing.T, *testInflightEnv) *mqttPersistedSession) *testConnectForkEnv {

	t.Helper()
	// testNewForkEnv's second save stands in for the accept save: it is the write whose
	// identity the helpers below are parented to.
	e := testNewForkEnv(t, clientID)
	return &testConnectForkEnv{testRejectionEnv: e, survivor: stage(t, e.testInflightEnv)}
}

// testGrantLifetime drives grantRequestedLifetime with a real record transaction, as the
// CONNECT does. The expiry differs from the session's so the helper does not take its
// no-change early return.
//
// The grant no longer disposes the session itself, so this driver performs the caller's
// own sequence for a disposing verdict — the same abort-signal-dispose order
// mqttProcessConnect's refuseDisposing runs — or the assertions below would be asserting
// a disposal nobody performed.
func testGrantLifetime(t *testing.T, e *testRejectionEnv, c *client) mqttLifetimeGrantResult {
	t.Helper()
	tok := e.sess.beginRecordTx()
	g := e.sess.grantRequestedLifetime(tok, 600, false)
	if terr := e.sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if g.outcome.disposes() {
		e.sess.disposeForeignSessionLocked(e.asm, c, g.voc)
	}
	return g
}

// TestSessForkedAtConnectTimeDirectRejection is the 10071 half of tests 8c-8f (M19a).
//
// This arm disposed with no readback at all, so it asserted a takeover on nothing but the
// fact that the CAS failed — the weakest evidence any of the five classified paths acts on.
// It now reads the record it is about to concede to. The child-shaped row is what keeps
// that from being a blanket downgrade.
func TestSessForkedAtConnectTimeDirectRejection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		stage func(*testing.T, *testInflightEnv) *mqttPersistedSession
		want  mqttDisposalVocabulary
	}{
		{"claims our parent", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSibling(t, e, testMutateSibling)
		}, mqttDisposalForked},
		// NOT "legitimate successor" any more, and not a takeover. Restore adopts WID/PWID
		// without validating provenance and valid-shaped bodies arrive through unconditional
		// publication, so a child-SHAPED body is a claim of descent, not proof of one.
		// F-084: it concedes to the unsettled vocabulary, which asserts only what is true.
		{"child-shaped body", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandChildRecord(t, e, testMutateSibling)
		}, mqttDisposalOwnershipUnsettled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewConnectForkEnv(t, "cxfork10071-"+tc.name, tc.stage)
			b := e.bindSocketAttemptLog(t, mqttProtoLevel5)

			// The record has moved, so this save's CAS is rejected by the single-writer
			// check: a real 10071, not a seam.
			g := testGrantLifetime(t, e.testRejectionEnv, b.bc)
			if !g.outcome.disposes() {
				t.Fatalf("the grant's save must be rejected by the moved record: outcome %v (%v)",
					g.outcome, g.err)
			}
			// The SPLIT verdict, which the merged one could not express: a sibling body is
			// its own outcome, and the caller's projection reads it rather than the
			// vocabulary. A helper that returns one disposing outcome for both fails here.
			wantOutcome := mqttPreConnackDisposingNonForked
			if tc.want == mqttDisposalForked {
				wantOutcome = mqttPreConnackDisposingForked
			}
			if g.outcome != wantOutcome {
				t.Fatalf("outcome %v, want %v", g.outcome, wantOutcome)
			}
			// The WHOLE vocabulary. Both of these carry 0x89 since F-084, so a reason-code
			// assertion would pass against a broker that mixed them up — and mixing them up
			// is precisely a false fork claim or a lost mint.
			testAssertVocabulary(t, tc.name, g.voc, tc.want)
			if e.resident(e.sess) {
				t.Fatal("both verdicts dispose; only the claim differs")
			}
			claims := b.disconnectClaims(t)
			if len(claims) != 1 || !claims[0].matches(tc.want) {
				t.Fatalf("disconnect claims %+v, want exactly one %+v (wire %x)",
					claims, tc.want, b.out.bytes())
			}
			// disposalForDirectRejection reaches its verdict inside the caller's record
			// transaction, which is where the repair is minted. See sess_fork_repair.go.
			if tc.want == mqttDisposalForked {
				testAssertMinted(t, e.testRejectionEnv)
				// EXACTLY once. The capsule alone cannot say so — a second mint replaces
				// the map entry with an equivalent one — so the store counter is the only
				// observable that distinguishes them (M9).
				if n := e.asm.forkedGrantStores.Load(); n != 1 {
					t.Fatalf("the forked verdict stored %d capsules, want exactly 1", n)
				}
			} else {
				testAssertNotMinted(t, e.testRejectionEnv)
				if n := e.asm.forkedGrantStores.Load(); n != 0 {
					t.Fatalf("a verdict that established no fork stored %d capsules, want 0", n)
				}
			}
		})
	}
}

// TestSessUnrelatedLineageAtConnectTimeStaysForeign is test 8f2 (M19b).
//
// Without it, an implementation can label every direct-10071 rejection forked without
// proving any lineage at all and still pass the sibling cases above. The survivor here
// shares neither our identity nor our parent.
func TestSessUnrelatedLineageAtConnectTimeStaysForeign(t *testing.T) {
	t.Parallel()
	e := testNewConnectForkEnv(t, "cxfork-unrelated",
		func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandUnrelated(t, e, testMutateSibling)
		})
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)

	if g := testGrantLifetime(t, e.testRejectionEnv, b.bc); !g.outcome.disposes() {
		t.Fatalf("the grant's save must be rejected: outcome %v (%v)", g.outcome, g.err)
	}
	// An unrelated lineage concedes to UNSETTLED. It used to concede to the takeover
	// vocabulary, which claimed a successor the record positively contradicts: this body
	// shares neither our identity nor our parent, so it establishes nothing about who holds
	// the session. F-084.
	claims := b.disconnectClaims(t)
	if len(claims) != 1 || !claims[0].matches(mqttDisposalOwnershipUnsettled) {
		t.Fatalf("an UNRELATED lineage concedes to ownership-unsettled: got %+v (wire %x)",
			claims, b.out.bytes())
	}
}

// TestSessForkedAtConnectTimeContendedInLoop is the 10164 half through
// resolveContendedRecordSave's IN-LOOP moved arm (M19).
func TestSessForkedAtConnectTimeContendedInLoop(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage func(*testing.T, *testInflightEnv) *mqttPersistedSession
		want  mqttRepairSaveVerdict
	}{
		{"claims our parent", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSibling(t, e, testMutateSibling)
		}, mqttRepairSaveForked},
		{"child-shaped body", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandChildRecord(t, e, testMutateSibling)
		}, mqttRepairSaveForeign},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewConnectForkEnv(t, "cxfork10164-"+tc.name, tc.stage)

			// A 10164-rejected save: the record has ALREADY moved, so the very first
			// re-read inside resolveContendedRecordSave takes the moved arm, before the
			// retry budget is touched.
			stop := testForceContendedRejection(t, e.sess)
			_, detail, release, serr := testSaveClassifiedHeld(t, e.sess)
			stop()
			if serr == nil {
				release()
				t.Fatal("the forced save must be rejected")
			}

			// The fence is still HELD, as the CONNECT holds it: this is where the fork is
			// classified and therefore where its repair is minted.
			got := e.sess.resolveContendedRecordSave(&detail)
			release()
			if got != tc.want {
				t.Fatalf("resolveContendedRecordSave (in-loop arm) = %v, want %v", got, tc.want)
			}
			if tc.want == mqttRepairSaveForked {
				testAssertMinted(t, e.testRejectionEnv)
			} else {
				testAssertNotMinted(t, e.testRejectionEnv)
			}
		})
	}
}

// TestSessForkedAtConnectTimeContendedAfterExhaustion drives the SAME classification
// through resolveContendedRecordSave's OTHER moved return: the post-exhaustion read
// (M19c).
//
// It is a separate branch, and with only the in-loop arm driven, reverting just this one to
// foreign passes every other pairing in the suite. Getting here needs the record to stay at
// the base for the whole retry budget — so each retry is CAS-rejected — and to move only
// once the budget is spent, which the store seam stages exactly.
func TestSessForkedAtConnectTimeContendedAfterExhaustion(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage func(*testing.T, *testInflightEnv) *mqttPersistedSession
		want  mqttRepairSaveVerdict
	}{
		{"claims our parent", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSibling(t, e, testMutateSibling)
		}, mqttRepairSaveForked},
		{"child-shaped body", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandChildRecord(t, e, testMutateSibling)
		}, mqttRepairSaveForeign},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewForkEnv(t, "cxexhaust-"+tc.name)

			stop := testForceContendedRejection(t, e.sess)
			_, detail, serr := testSaveClassified(t, e.sess)
			stop()
			if serr == nil {
				t.Fatal("the forced save must be rejected")
			}

			// Every retry is rejected while the record still reads AT BASE, so the loop
			// spends its whole budget in the at-base arm. The LAST retry moves the record
			// on its way out, so the post-exhaustion read is the first thing to see it.
			//
			// The suppression is what makes each retry a rejection rather than a landing:
			// the result seam fires AFTER the store, so without it the retry's bytes would
			// be durable before the hook rewrote the answer. It is lifted around the
			// staging store and restored by a cleanup, because a flag left set by a failing
			// subtest breaks the NEXT test's fixture rather than this one's assertion.
			var retries atomic.Int32
			restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
				if sess != e.sess {
					return resp, err
				}
				if n := retries.Add(1); int(n) == mqttRepairSaveAttempts {
					mqttTestSessStoreSuppressV.Store(false)
					tc.stage(t, e.testInflightEnv)
					mqttTestSessStoreSuppressV.Store(true)
				}
				return nil, ApiErrors[JSStreamWrongLastSequenceConstantErr]
			})
			t.Cleanup(func() { mqttTestSessStoreSuppressV.Store(false) })
			mqttTestSessStoreSuppressV.Store(true)
			got := e.sess.resolveContendedRecordSave(&detail)
			mqttTestSessStoreSuppressV.Store(false)
			restore()

			if n := retries.Load(); int(n) != mqttRepairSaveAttempts {
				t.Fatalf("the retry budget must be EXHAUSTED, or this is the in-loop arm again: %d retries", n)
			}
			if got != tc.want {
				t.Fatalf("resolveContendedRecordSave (post-exhaustion arm) = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSessRepairSaveLandedAdoptsLineage is M19d's paired test (10a).
//
// resolveContendedRecordSave proves a write durable in three places that never pass through
// saveWithDetail's success arm, and each of them assigns sess.seq. An implementation that
// moves the sequence and credits the void-register revision while leaving the lineage stale
// satisfies every other test in this suite — and then the NEXT save parents itself to a
// record that is no longer the base, after which every genuine sibling of the real base
// classifies as unrelated and concedes.
//
// The final assertion is that consequence, not just the field: a subsequent ordinary save
// must parent itself to the adopted identity.
func TestSessRepairSaveLandedAdoptsLineage(t *testing.T) {
	t.Run("retry succeeds at base", func(t *testing.T) {
		e := testNewForkEnv(t, "repairadopt-retry")

		stop := testForceContendedRejection(t, e.sess)
		_, detail, serr := testSaveClassified(t, e.sess)
		stop()
		if serr == nil {
			t.Fatal("the forced save must be rejected")
		}
		// The record is untouched, so the re-read finds it at base and the retry — a
		// verbatim re-send of the attempt's bytes — succeeds.
		if got := e.sess.resolveContendedRecordSave(&detail); got != mqttRepairSaveLanded {
			t.Fatalf("resolveContendedRecordSave = %v, want landed", got)
		}
		testAssertAdoptedLineage(t, e, detail)
	})

	t.Run("digest found at a newer sequence after the retries are exhausted", func(t *testing.T) {
		// The THIRD landed exit, and a separate branch from the two below: the
		// post-exhaustion read at sess_resume_reconcile.go's tail. Removing only its
		// adoption call leaves every other test in this suite green while the next save
		// parents itself to stale ancestry — after which genuine siblings of the real
		// base classify as unrelated and the false 0x8E behaviour comes back.
		e := testNewForkEnv(t, "repairadopt-exhausted")

		stop := testForceContendedRejection(t, e.sess)
		_, detail, serr := testSaveClassified(t, e.sess)
		stop()
		if serr == nil {
			t.Fatal("the forced save must be rejected")
		}

		// Every retry is rejected while the record reads AT BASE, so the loop spends its
		// whole budget in the at-base arm; the LAST one lands the attempt's own bytes on
		// its way out, so the post-exhaustion read is the first thing to see them and
		// recognises them by digest.
		var retries atomic.Int32
		restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if sess != e.sess {
				return resp, err
			}
			if n := retries.Add(1); int(n) == mqttRepairSaveAttempts {
				mqttTestSessStoreSuppressV.Store(false)
				if _, serr := e.sess.jsa.storeSessionMsg(detail.domainTk, detail.cidHash, 0, detail.body); serr != nil {
					t.Errorf("land the attempt's own bytes: %v", serr)
				}
				mqttTestSessStoreSuppressV.Store(true)
			}
			return nil, ApiErrors[JSStreamWrongLastSequenceConstantErr]
		})
		t.Cleanup(func() { mqttTestSessStoreSuppressV.Store(false) })
		mqttTestSessStoreSuppressV.Store(true)
		got := e.sess.resolveContendedRecordSave(&detail)
		mqttTestSessStoreSuppressV.Store(false)
		restore()

		if n := retries.Load(); int(n) != mqttRepairSaveAttempts {
			t.Fatalf("the retry budget must be EXHAUSTED, or this is one of the in-loop arms: %d retries", n)
		}
		if got != mqttRepairSaveLanded {
			t.Fatalf("resolveContendedRecordSave = %v, want landed", got)
		}
		testAssertAdoptedLineage(t, e, detail)
	})

	t.Run("digest found at a newer sequence", func(t *testing.T) {
		e := testNewForkEnv(t, "repairadopt-digest")

		stop := testForceContendedRejection(t, e.sess)
		_, detail, serr := testSaveClassified(t, e.sess)
		stop()
		if serr == nil {
			t.Fatal("the forced save must be rejected")
		}
		// The attempt's EXACT bytes land at a newer sequence, which is the shape a
		// landed-original-with-a-lost-response leaves: the re-read recognises them by
		// digest without any retry.
		if _, err := e.sess.jsa.storeSessionMsg(detail.domainTk, detail.cidHash, 0, detail.body); err != nil {
			t.Fatalf("land the attempt's own bytes: %v", err)
		}
		if got := e.sess.resolveContendedRecordSave(&detail); got != mqttRepairSaveLanded {
			t.Fatalf("resolveContendedRecordSave = %v, want landed", got)
		}
		testAssertAdoptedLineage(t, e, detail)
	})
}

// testAssertAdoptedLineage checks both halves: the session holds the attempt's identity,
// and the next ordinary save parents itself to it.
func testAssertAdoptedLineage(t *testing.T, e *testRejectionEnv, detail mqttSessSaveDetail) {
	t.Helper()
	if detail.wid == _EMPTY_ {
		t.Fatal("fixture: the attempt must carry a minted identity")
	}
	e.sess.mu.Lock()
	gotWID, gotPWID := e.sess.wid, e.sess.pwid
	e.sess.mu.Unlock()
	if gotWID != detail.wid || gotPWID != detail.pwid {
		t.Fatalf("a landed repair save must adopt the attempt's lineage: got (%q,%q), want (%q,%q)",
			gotWID, gotPWID, detail.wid, detail.pwid)
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("save after the repair: %v", err)
	}
	if after := testLoadRecordLineage(t, e.testInflightEnv); after.PWID != detail.wid {
		t.Fatalf("the next save must parent itself to the adopted identity: PWID=%q want %q",
			after.PWID, detail.wid)
	}
}

// testSaveClassified runs saveClassifiedDetail inside a record transaction.
func testSaveClassified(t *testing.T, sess *mqttSession) (saveOutcome, mqttSessSaveDetail, error) {
	t.Helper()
	tok := sess.beginRecordTx()
	outcome, detail, err := sess.saveClassifiedDetail(tok)
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	return outcome, detail, err
}

// testSaveClassifiedHeld is testSaveClassified with the transaction LEFT OPEN, and it returns
// the release.
//
// resolveContendedRecordSave runs inside the CONNECT's own transaction in production, and it
// classifies a fork there — so a test that closed the fence first could not show the mint
// happening under it, and would pass against an implementation that minted from a released
// fence. See sess_fork_repair.go.
func testSaveClassifiedHeld(t *testing.T, sess *mqttSession) (saveOutcome, mqttSessSaveDetail, func(), error) {
	t.Helper()
	tok := sess.beginRecordTx()
	outcome, detail, err := sess.saveClassifiedDetail(tok)
	release := func() {
		if terr := sess.endRecordTx(tok); terr != nil {
			t.Fatalf("endRecordTx: %v", terr)
		}
	}
	return outcome, detail, release, err
}

// testLandUnrelated stages a record from a lineage that shares nothing with ours — neither
// our identity nor our parent — which is Codex round 1's competing-branch counterexample.
func testLandUnrelated(t *testing.T, e *testInflightEnv, mutate func(*mqttPersistedSession)) *mqttPersistedSession {
	t.Helper()
	e.sess.mu.Lock()
	other := e.sess.buildPersistedSessionLocked()
	other.Gen = e.sess.gen
	other.PWID = mqttNewRecordWID() // a parent we never had
	other.WID = mqttNewRecordWID()
	domainTk, idHash := e.sess.domainTk, e.sess.idHash
	e.sess.mu.Unlock()
	if mutate != nil {
		mutate(&other)
	}
	body, _ := json.Marshal(&other)
	if _, err := e.sess.jsa.storeSessionMsg(domainTk, idHash, 0, body); err != nil {
		t.Fatalf("land the unrelated record: %v", err)
	}
	return &other
}

// testReconcileResumedConsumers drives the OTHER CONNECT-time helper. A QoS-1 filter with
// no consumer config makes step 3 repair, which is what produces the one record write this
// phase makes — and therefore the rejection the classification is about.
//
// Three things it now does that the earlier driver did not, each because the phase REPORTS
// its disposing verdict instead of acting on it:
//
//   - it takes the OWNERSHIP GATE around the classification, which the phase documents as
//     its precondition (asm.sessLocked) and which the old driver simply did not honour;
//   - it puts the client into PRE-CONNACK state, which is the state the real site is in:
//     the fixture binds a socket as if the connection were already accepted, so leaving
//     connAckWritten set would test a connection that cannot exist here;
//   - it performs the caller's own abort-signal-dispose sequence for a disposing verdict,
//     because after the move nothing else does.
func testReconcileResumedConsumers(t *testing.T, e *testRejectionEnv, c *client) mqttResumeReconcileResult {
	t.Helper()
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{"forked/reconcile": 1}
	e.sess.cons = nil
	e.sess.mu.Unlock()

	// Pre-CONNACK: no success CONNACK has been written at this point in a real CONNECT,
	// which is exactly why its refusal has to carry the reason itself.
	c.connAckWritten.Store(false)

	if err := e.asm.lockSession(e.sess, c); err != nil {
		t.Fatalf("taking the ownership gate the phase requires: %v", err)
	}
	tok := e.sess.beginRecordTx()
	r := e.asm.reconcileResumedConsumers(e.sess, c, tok)
	if terr := e.sess.endRecordTx(tok); terr != nil {
		e.asm.unlockSession(e.sess)
		t.Fatalf("endRecordTx: %v", terr)
	}
	if r.outcome.disposes() {
		e.sess.disposeForeignSessionLocked(e.asm, c, r.voc)
	}
	e.asm.unlockSession(e.sess)
	return r
}

// TestSessForkedAtReconcileDirectRejection is the second cell of M19a's pairing.
//
// The two helpers have SEPARATE direct-10071 arms — reconcileResumedConsumers does not call
// resolveContendedRecordSave on that branch at all — so a mutation reverting one of them is
// invisible to a test that only drives the other.
func TestSessForkedAtReconcileDirectRejection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		stage func(*testing.T, *testInflightEnv) *mqttPersistedSession
		want  mqttDisposalVocabulary
	}{
		{"claims our parent", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSibling(t, e, testMutateSibling)
		}, mqttDisposalForked},
		// See the connect-time table above: a child-SHAPED body proves no successor.
		{"child-shaped body", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandChildRecord(t, e, testMutateSibling)
		}, mqttDisposalOwnershipUnsettled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewForkEnv(t, "rcfork-"+tc.name)
			b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
			tc.stage(t, e.testInflightEnv)

			r := testReconcileResumedConsumers(t, e, b.bc)
			if !r.outcome.disposes() {
				t.Fatalf("the phase's save is rejected by the moved record: outcome %v (%v)",
					r.outcome, r.err)
			}
			// THE SPLIT. The merged verdict could not express this, which is why the
			// caller could not project the right CONNACK code: a sibling body is its own
			// outcome, and the projection reads the OUTCOME rather than the vocabulary.
			// A phase that returns one disposing outcome for both fails here — and only
			// here, because the caller groups the two arms and the wire tests would still
			// see whatever the unchanged vocabulary projects to.
			wantOutcome := mqttPreConnackDisposingNonForked
			if tc.want == mqttDisposalForked {
				wantOutcome = mqttPreConnackDisposingForked
			}
			if r.outcome != wantOutcome {
				t.Fatalf("outcome %v, want %v", r.outcome, wantOutcome)
			}
			// The WHOLE vocabulary: both arms carry 0x89 since F-084, so the reason code no
			// longer separates them and the Reason String is what does.
			testAssertVocabulary(t, tc.name, r.voc, tc.want)
			// The disposal happened, at the CALLER, and the session is gone from the maps.
			if e.resident(e.sess) {
				t.Fatal("both disposing outcomes evict; only the claim differs")
			}
			// And NO DISCONNECT, positively. This replaces the reason-code assertion this
			// test used to make on the wire, and it is a correction rather than a
			// weakening: the fixture had connAckWritten set as for an ACCEPTED connection,
			// so it was asserting a packet the real pre-CONNACK site cannot send —
			// mqttNotifyDisplaced is gated on that flag. Being unable to speak here is
			// exactly why the refusal CONNACK exists, and the codes themselves are pinned
			// on the wire by the end-to-end fixtures.
			if reasons := b.disconnectAttempts(t); len(reasons) != 0 {
				t.Fatalf("a pre-CONNACK refusal must send no DISCONNECT, got %x (wire %x)",
					reasons, b.out.bytes())
			}
			// disposalForDirectRejection reaches its verdict inside the caller's record
			// transaction, which is where the repair is minted. See sess_fork_repair.go.
			if tc.want == mqttDisposalForked {
				testAssertMinted(t, e)
				if n := e.asm.forkedGrantStores.Load(); n != 1 {
					t.Fatalf("the forked verdict stored %d capsules, want exactly 1", n)
				}
			} else {
				testAssertNotMinted(t, e)
				if n := e.asm.forkedGrantStores.Load(); n != 0 {
					t.Fatalf("a verdict that established no fork stored %d capsules, want 0", n)
				}
			}
		})
	}
}
