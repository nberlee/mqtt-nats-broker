package mqtt

import (
	"encoding/json"
	"sync/atomic"
	"testing"
)

// Path 3: the CONNECT-time classified saves.
//
// reconcileResumedConsumers and grantRequestedLifetime do not wait for the rejection
// settlement. They dispose the session themselves — directly on 10071, and through
// resolveContendedRecordSave on 10164 — and both branches close the connection claiming
// the session "is now owned elsewhere". Two revisions of the plan enumerated "the two
// paths" and both were wrong; this is the third.
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
// of the accept save or a legitimate successor of it.
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
func testGrantLifetime(t *testing.T, e *testRejectionEnv, c *client) error {
	t.Helper()
	tok := e.sess.beginRecordTx()
	err := e.sess.grantRequestedLifetime(e.asm, c, tok, 600, false)
	if terr := e.sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	return err
}

// TestSessForkedAtConnectTimeDirectRejection is the 10071 half of tests 8c-8f (M19a).
//
// This arm disposed with no readback at all, so it asserted a takeover on nothing but the
// fact that the CAS failed — the weakest evidence any of the five classified paths acts on.
// It now reads the record it is about to concede to. The successor row is what keeps that
// from being a blanket downgrade.
func TestSessForkedAtConnectTimeDirectRejection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		stage func(*testing.T, *testInflightEnv) *mqttPersistedSession
		want  byte
	}{
		{"claims our parent", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSibling(t, e, testMutateSibling)
		}, mqttReasonServerBusy},
		{"legitimate successor", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSuccessor(t, e, testMutateSibling)
		}, mqttReasonSessionTakenOver},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewConnectForkEnv(t, "cxfork10071-"+tc.name, tc.stage)
			b := e.bindSocketAttemptLog(t, mqttProtoLevel5)

			// The record has moved, so this save's CAS is rejected by the single-writer
			// check: a real 10071, not a seam.
			if err := testGrantLifetime(t, e.testRejectionEnv, b.bc); err == nil {
				t.Fatal("the grant's save must be rejected by the moved record")
			}
			if e.resident(e.sess) {
				t.Fatal("both verdicts dispose; only the claim differs")
			}
			if reasons := b.disconnectAttempts(t); len(reasons) != 1 || reasons[0] != tc.want {
				t.Fatalf("reason codes %x, want exactly one %x (wire %x)",
					reasons, tc.want, b.out.bytes())
			}
			// disposalForDirectRejection reaches its verdict inside the caller's record
			// transaction, which is where the repair is minted. See sess_fork_repair.go.
			if tc.want == mqttReasonServerBusy {
				testAssertMinted(t, e.testRejectionEnv)
			} else {
				testAssertNotMinted(t, e.testRejectionEnv)
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

	if err := testGrantLifetime(t, e.testRejectionEnv, b.bc); err == nil {
		t.Fatal("the grant's save must be rejected")
	}
	if reasons := b.disconnectAttempts(t); len(reasons) != 1 || reasons[0] != mqttReasonSessionTakenOver {
		t.Fatalf("an UNRELATED lineage is conceded exactly as before: got %x (wire %x)",
			reasons, b.out.bytes())
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
		{"legitimate successor", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSuccessor(t, e, testMutateSibling)
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
		{"legitimate successor", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSuccessor(t, e, testMutateSibling)
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
func testReconcileResumedConsumers(t *testing.T, e *testRejectionEnv, c *client) (mqttResumeReconcileVerdict, error) {
	t.Helper()
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{"forked/reconcile": 1}
	e.sess.cons = nil
	e.sess.mu.Unlock()

	tok := e.sess.beginRecordTx()
	_, verdict, err := e.asm.reconcileResumedConsumers(e.sess, c, tok)
	if terr := e.sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	return verdict, err
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
		want  byte
	}{
		{"claims our parent", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSibling(t, e, testMutateSibling)
		}, mqttReasonServerBusy},
		{"legitimate successor", func(t *testing.T, e *testInflightEnv) *mqttPersistedSession {
			return testLandSuccessor(t, e, testMutateSibling)
		}, mqttReasonSessionTakenOver},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewForkEnv(t, "rcfork-"+tc.name)
			b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
			tc.stage(t, e.testInflightEnv)

			verdict, err := testReconcileResumedConsumers(t, e, b.bc)
			if verdict != mqttReconcileRefuseForeign {
				t.Fatalf("the phase's save is rejected by the moved record: verdict %v (%v)", verdict, err)
			}
			if reasons := b.disconnectAttempts(t); len(reasons) != 1 || reasons[0] != tc.want {
				t.Fatalf("reason codes %x, want exactly one %x (wire %x)",
					reasons, tc.want, b.out.bytes())
			}
			// disposalForDirectRejection reaches its verdict inside the caller's record
			// transaction, which is where the repair is minted. See sess_fork_repair.go.
			if tc.want == mqttReasonServerBusy {
				testAssertMinted(t, e)
			} else {
				testAssertNotMinted(t, e)
			}
		})
	}
}
