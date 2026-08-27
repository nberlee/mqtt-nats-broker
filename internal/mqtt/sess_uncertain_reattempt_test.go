package mqtt

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
)

// The settlement contract for an ambiguous session-record write: a record found at
// the attempt's CAS base settles NOTHING — JetStream reports contention while a write
// for the subject is still in process upstream — so the resolver arbitrates with a
// CAS, re-sending the attempt's exact bytes, instead of reading the base sequence as
// proof that the write died.
//
// The defect these tests are about: clearing the uncertainty there discarded the
// digest that identifies a late-committing proposal as OURS, so the same proposal was
// later read as NOT OURS and the session was disposed of as FOREIGN —
// evicted, and on a bound settlement closed with 0x8E and its Will discarded, with no
// takeover anywhere — and since F-084 nothing on this path claims one at all.

// testUncertainEnv is one broker with a provisioned, saved session and its record's
// base sequence — the state every arm of the contract starts from.
type testUncertainEnv struct {
	a, b *testTeardownEnv
	sess *mqttSession
	base uint64
	body []byte
}

// testNewUncertainEnv provisions a session, marks a write ambiguous against the
// current record (body = the state the attempt would have stored) and returns
// everything the arms need.
func testNewUncertainEnv(t *testing.T, id string) *testUncertainEnv {
	t.Helper()
	a, b := testNewTeardownEnv(t)
	sess := testProvisionSession(t, a, id)
	_, base := testLoadSessRecord(t, a, sess.idHash)
	// The attempted state: one more subscription than the record holds.
	sess.mu.Lock()
	sess.subs["attempted"] = 1
	sess.mu.Unlock()
	body := testRecordBody(t, sess)
	testMarkUncertain(sess, base, body)
	// RESIDENT, so the periodic pass walks it: reconcileResidentRecordState is what
	// carries the old arm's disposal chain, and a session no map points at would make
	// that half of the causal pair vacuous.
	a.asm.addSession(sess, true)
	return &testUncertainEnv{a: a, b: b, sess: sess, base: base, body: body}
}

// resident reports whether the session object is still the account's.
func (e *testUncertainEnv) resident() bool {
	e.a.asm.mu.RLock()
	defer e.a.asm.mu.RUnlock()
	return e.a.asm.sessions[e.sess.id] == e.sess
}

// TestUncertainLateProposalIsNotForeign is the defect itself, as a causal pair: the
// arbitration that stops a late proposal, and the disposal chain it used to feed.
//
// The positive half keeps the negative from being vacuous. Under the old arm the
// resolver cleared at the base sequence, the late proposal then committed, the
// session's next save was CAS-rejected, its probe found a record whose bytes matched
// nothing it knew, and the pass disposed of the session as FOREIGN. Every step of
// that chain is driven here.
//
// Mutation M1: revert the at-base arm to clearing the uncertainty. The late store
// then commits and the pass disposes of the session over its own write.
func TestUncertainLateProposalIsNotForeign(t *testing.T) {
	t.Parallel()
	e := testNewUncertainEnv(t, "late-proposal")

	res, err := e.sess.resolveUncertainWrite()
	if err != nil {
		t.Fatalf("resolveUncertainWrite: %v", err)
	}
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours", res)
	}

	// The proposal that was in process upstream commits NOW. It CAS's against the
	// same base the resolver just re-attempted at, so the exclusion holds and this one
	// loses — the re-attempt already landed. That exclusion is now unqualified for two
	// bridge-authored conditional writes (the same-base boundary, docs/CONFORMANCE.md);
	// TestUncertainDuplicateCommitCollapses covers the both-landed outcome as a generic
	// MaxMsgsPer:1 property — NOT as coverage of the unconditional route, which supplies
	// no acknowledgement for this broker to adopt.
	lateSeq, lateErr := mqttStoreSessionRecordCASBody(&e.a.asm.jsa, e.a.asm.domainTk,
		e.body, e.sess.idHash, e.base)
	if lateErr == nil {
		t.Errorf("the late proposal must LOSE the arbitration, but it committed at %d: "+
			"the resolver settled the ambiguity by reading rather than by a CAS", lateSeq)
	}

	// The rest of the old chain, driven whatever happened above: the session's next
	// save, and the pass that settles any rejection it leaves behind.
	tok := e.sess.beginRecordTx()
	saveErr := e.sess.save(tok)
	if terr := e.sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	e.a.asm.reconcileResidentRecordState(e.a.s)

	if !e.resident() {
		t.Fatalf("the session was disposed of as FOREIGN over its OWN write "+
			"(next save: %v): a live client would have been closed and evicted", saveErr)
	}
	if saveErr != nil {
		t.Fatalf("a settled session must be writable again: %v", saveErr)
	}
	if e.sess.isSeqUncertain() {
		t.Fatal("the settled session must not be left unresolved")
	}
}

// TestUncertainReattemptWindow drives what happens when the record MOVES between the
// readback and the re-attempt — the window the whole arm exists for, opened
// deterministically by the resolver's own test seam rather than by a sleep.
//
// A rejected re-attempt says the subject moved, never by whom, so exactly one more
// read decides. Each arm below is a different answer to that question.
func TestUncertainReattemptWindow(t *testing.T) {
	t.Run("the original commits in the window: ours", func(t *testing.T) {
		e := testNewUncertainEnv(t, "window-original")
		var landed uint64
		restore := setMQTTTestResolveReattemptHook(func(sess *mqttSession) {
			if sess != e.sess || landed != 0 {
				return
			}
			seq, err := mqttStoreSessionRecordCASBody(&e.a.asm.jsa, e.a.asm.domainTk,
				e.body, e.sess.idHash, e.base)
			if err != nil {
				t.Errorf("simulate the original committing in the window: %v", err)
				return
			}
			landed = seq
		})
		defer restore()

		res, err := e.sess.resolveUncertainWrite()
		restore()
		if err != nil {
			t.Fatalf("resolveUncertainWrite: %v", err)
		}
		if landed == 0 {
			t.Fatal("the seam never fired; the test proved nothing")
		}
		if res != mqttUncertainOurs {
			t.Fatalf("resolution = %v, want ours: the rejected re-attempt must be classified by content, not by the rejection", res)
		}
		e.sess.mu.Lock()
		adopted := e.sess.seq
		e.sess.mu.Unlock()
		if adopted != landed {
			t.Fatalf("sess.seq = %d, want the sequence the original landed at (%d)", adopted, landed)
		}
		if !e.resident() {
			t.Fatal("the session must not be disposed of over its own landed write")
		}
	})

	t.Run("a foreign write takes the window: foreign, untouched", func(t *testing.T) {
		e := testNewUncertainEnv(t, "window-foreign")
		foreignBody := []byte(`{"origin":"nodeB","subs":{"theirs":2}}`)
		var foreignSeq uint64
		restore := setMQTTTestResolveReattemptHook(func(sess *mqttSession) {
			if sess != e.sess || foreignSeq != 0 {
				return
			}
			seq, err := mqttStoreSessionRecordCASBody(&e.b.asm.jsa, e.b.asm.domainTk,
				foreignBody, e.sess.idHash, e.base)
			if err != nil {
				t.Errorf("simulate the competing write: %v", err)
				return
			}
			foreignSeq = seq
		})
		defer restore()

		res, err := e.sess.resolveUncertainWrite()
		restore()
		if err != nil {
			t.Fatalf("resolveUncertainWrite: %v", err)
		}
		if foreignSeq == 0 {
			t.Fatal("the seam never fired; the test proved nothing")
		}
		if res != mqttUncertainForeign {
			t.Fatalf("resolution = %v, want foreign", res)
		}
		// The re-attempt carries a real expected-last-subject-sequence header, so it
		// was REJECTED rather than overwriting the record that moved.
		stored := testRecordBodyAt(t, e.b, e.sess.idHash)
		if mqttRecordDigest(stored) != mqttRecordDigest(foreignBody) {
			t.Fatalf("the foreign record was overwritten by the re-attempt: %q", stored)
		}
		e.sess.mu.Lock()
		adopted := e.sess.seq
		e.sess.mu.Unlock()
		if adopted == foreignSeq {
			t.Fatal("the foreign sequence was adopted; the next save would overwrite another broker's record")
		}
	})

	// Released with NOTHING in process: the re-attempt is refused with 10071, which is
	// the best evidence available that the original went with the record. Fail-safe,
	// not a proof — see the in-process arm below and docs/CONFORMANCE.md.
	t.Run("the record is released in the window: foreign", func(t *testing.T) {
		e := testNewUncertainEnv(t, "window-gone")
		var deleted bool
		restore := setMQTTTestResolveReattemptHook(func(sess *mqttSession) {
			if sess != e.sess || deleted {
				return
			}
			deleted = true
			subject := mqttSessStreamSubjectPrefix + e.a.asm.domainTk + e.sess.idHash
			smsg, lerr := e.a.asm.jsa.loadLastMsgFor(mqttSessStreamName, subject)
			if lerr != nil {
				t.Errorf("load the record to release it: %v", lerr)
				return
			}
			if derr := e.a.asm.jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); derr != nil {
				t.Errorf("release the record in the window: %v", derr)
			}
		})
		defer restore()

		res, err := e.sess.resolveUncertainWrite()
		restore()
		if err != nil {
			t.Fatalf("resolveUncertainWrite: %v", err)
		}
		if !deleted {
			t.Fatal("the seam never fired; the test proved nothing")
		}
		if res != mqttUncertainForeign {
			t.Fatalf("resolution = %v, want foreign: an absent record cannot be this attempt's", res)
		}
	})

	t.Run("gone while a write is in process: unresolved", func(t *testing.T) {
		e := testNewUncertainEnv(t, "window-gone-inprocess")
		e.sess.mu.Lock()
		pw := e.sess.pendingWrite
		e.sess.mu.Unlock()

		// The record is released, and the re-attempt is refused with 10164: a write
		// for this subject is IN PROCESS upstream. That may be our own original —
		// accepted into the log before the delete, and applying after it, because the
		// clustered path checks the expected sequence only before proposing. Disposing
		// here is the false-foreign shape all over again.
		var deleted bool
		restore := setMQTTTestResolveReattemptHook(func(sess *mqttSession) {
			if sess != e.sess || deleted {
				return
			}
			deleted = true
			subject := mqttSessStreamSubjectPrefix + e.a.asm.domainTk + e.sess.idHash
			smsg, lerr := e.a.asm.jsa.loadLastMsgFor(mqttSessStreamName, subject)
			if lerr != nil {
				t.Errorf("load the record to release it: %v", lerr)
				return
			}
			if derr := e.a.asm.jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); derr != nil {
				t.Errorf("release the record in the window: %v", derr)
			}
		})
		defer restore()
		restoreStore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if sess != e.sess {
				return resp, err
			}
			return nil, ApiErrors[JSStreamWrongLastSequenceConstantErr]
		})
		defer restoreStore()
		mqttTestSessStoreSuppressV.Store(true)

		res, _ := e.sess.resolveUncertainWrite()
		mqttTestSessStoreSuppressV.Store(false)
		restoreStore()
		restore()
		if !deleted {
			t.Fatal("the seam never fired; the test proved nothing")
		}
		if res != mqttUncertainForeign && res != mqttUncertainUnresolved {
			t.Fatalf("resolution = %v, want unresolved", res)
		}
		if res == mqttUncertainForeign {
			t.Fatal("an absent record plus an IN-PROCESS write must not be disposed as foreign: " +
				"a proposal already accepted into the log applies whatever happened to the subject, " +
				"so this is the late-landing false-foreign shape the settlement exists to prevent")
		}
		e.sess.mu.Lock()
		stillOurs := e.sess.seqUncertain && e.sess.pendingWrite == pw
		e.sess.mu.Unlock()
		if !stillOurs {
			t.Fatal("an unsettled gone verdict must leave the attempt's identity intact")
		}
	})

	t.Run("the re-read is unavailable: unresolved", func(t *testing.T) {
		e := testNewUncertainEnv(t, "window-unreadable")
		e.sess.mu.Lock()
		pw := e.sess.pendingWrite
		e.sess.mu.Unlock()

		// The record moves in the window (so the re-attempt is rejected) and the
		// re-read that would classify it cannot be taken: nothing is established, in
		// either direction.
		var loads atomic.Int32
		restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
			if stream != mqttSessStreamName {
				return nil
			}
			if loads.Add(1) >= 2 {
				return errors.New("test: the classifying re-read is unavailable")
			}
			return nil
		})
		defer restoreRead()
		var moved bool
		restore := setMQTTTestResolveReattemptHook(func(sess *mqttSession) {
			if sess != e.sess || moved {
				return
			}
			moved = true
			if _, err := mqttStoreSessionRecordCASBody(&e.b.asm.jsa, e.b.asm.domainTk,
				[]byte(`{"origin":"nodeB"}`), e.sess.idHash, e.base); err != nil {
				t.Errorf("move the record in the window: %v", err)
			}
		})
		defer restore()

		res, _ := e.sess.resolveUncertainWrite()
		restore()
		restoreRead()
		if !moved {
			t.Fatal("the seam never fired; the test proved nothing")
		}
		if res != mqttUncertainUnresolved {
			t.Fatalf("resolution = %v, want unresolved: an unreadable re-read establishes nothing", res)
		}
		e.sess.mu.Lock()
		stillOurs := e.sess.seqUncertain && e.sess.pendingWrite == pw
		e.sess.mu.Unlock()
		if !stillOurs {
			t.Fatal("an unclassifiable outcome must leave the attempt's identity exactly as it was")
		}
	})
}

// TestUncertainReattemptContendedStaysUnresolved covers the rejection that is NOT a
// verdict: 10164 with the record still at the base sequence.
//
// That is contention — a write for the subject is in process upstream, possibly our
// own original — so nothing may be concluded from it. Reading it as "somebody else
// owns this" is the same mistake as reading the base sequence as "our write died".
func TestUncertainReattemptContendedStaysUnresolved(t *testing.T) {
	e := testNewUncertainEnv(t, "reattempt-contended")
	e.sess.mu.Lock()
	pw := e.sess.pendingWrite
	e.sess.mu.Unlock()

	// A GENUINE rejection: the write must not reach the stream (or it would land and
	// move the record, which is a different arm entirely), and the error the caller
	// sees must carry the CAS kind. Suppression supplies the first, the result seam
	// the second.
	var stores int
	restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess != e.sess {
			return resp, err
		}
		stores++
		return nil, ApiErrors[JSStreamWrongLastSequenceConstantErr]
	})
	defer restore()
	mqttTestSessStoreSuppressV.Store(true)

	res, _ := e.sess.resolveUncertainWrite()
	mqttTestSessStoreSuppressV.Store(false)
	restore()

	if _, seq := testLoadSessRecord(t, e.a, e.sess.idHash); seq != e.base {
		t.Fatalf("the fixture must leave the record at its base sequence, got %d (base %d)", seq, e.base)
	}
	if stores != 1 {
		t.Fatalf("the resolver must make exactly one re-attempt per pass, got %d", stores)
	}
	if res != mqttUncertainUnresolved {
		t.Fatalf("resolution = %v, want unresolved: contention is a rejection, never a verdict", res)
	}
	e.sess.mu.Lock()
	stillOurs := e.sess.seqUncertain && e.sess.pendingWrite == pw
	e.sess.mu.Unlock()
	if !stillOurs {
		t.Fatal("a contended re-attempt must leave the attempt's identity intact for the next pass")
	}
	// And the session is still refused, which is what "unresolved" is for.
	tok := e.sess.beginRecordTx()
	serr := e.sess.save(tok)
	if terr := e.sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if serr == nil {
		t.Fatal("an unresolved session must refuse its writes")
	}
}

// TestUncertainReattemptSendsTheAttemptsOwnBytes pins the identity the whole
// mechanism rests on.
//
// An uncertain caller KEPT its mutation, so memory can move on while the write is
// outstanding. Re-marshalling current memory would store content whose digest no
// longer matches what a later read compares against — and the original landing would
// then be unrecognizable as ours, which is the defect one layer down.
//
// The staged change is a subscription entry deliberately, NOT a void-register nonce:
// a nonce's release handoff can perform a second save of current memory the moment
// the fence releases, which would mask the mutation.
//
// Mutation M13: re-marshal current memory in the re-attempt and the stored digest
// diverges from the attempt's.
func TestUncertainReattemptSendsTheAttemptsOwnBytes(t *testing.T) {
	t.Parallel()
	e := testNewUncertainEnv(t, "reattempt-bytes")

	// Staged AFTER the attempt was marked uncertain: it is not in the attempted
	// bytes, and nothing may put it there.
	e.sess.mu.Lock()
	e.sess.subs["staged-after"] = 2
	e.sess.mu.Unlock()

	if res, err := e.sess.resolveUncertainWrite(); res != mqttUncertainOurs {
		t.Fatalf("resolution = %v (err=%v), want ours", res, err)
	}

	stored := testRecordBodyAt(t, e.a, e.sess.idHash)
	if mqttRecordDigest(stored) != mqttRecordDigest(e.body) {
		t.Fatalf("the re-attempt stored a re-marshal of current memory, not the attempt's own bytes: %q", stored)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(stored, ps); err != nil {
		t.Fatalf("unmarshal the stored record: %v", err)
	}
	if _, leaked := ps.Subs["staged-after"]; leaked {
		t.Fatal("the re-attempt carried state the attempt never marshalled")
	}
}

// TestUncertainDuplicateCommitCollapses pins what makes the ours-landed invariant hold
// WITHOUT relying on "only one of the two proposals can commit".
//
// For two BRIDGE-AUTHORED CONDITIONAL writes that exclusion is now a guarantee — see the
// same-base boundary in docs/CONFORMANCE.md — so this test no longer pins a reachable
// interleaving of the original and its re-attempt.
//
// What it is NOT: coverage of the unconditional foreign-write route. This test performs
// two sequential CONDITIONAL writes and then assigns the second sequence to the session
// itself. On the unconditional route the broker is acknowledged for its own re-attempt
// and the later foreign write supplies NO acknowledgement to adopt, so nothing here
// exercises that disposition. Claiming otherwise would contradict sess_uncertain.go's own
// statement of the reachable order.
//
// What it still pins is the generic MaxMsgsPer:1 property the invariant leans on: two
// identical framed writes both landing leave one record, with the attempt's bytes, at the
// adopted sequence — indistinguishable from a single landing.
//
// The property rests on $MQTT_sess being MaxMsgsPer:1, which is a validated startup
// prerequisite (validateSessStream) rather than an assumption made here.
func TestUncertainDuplicateCommitCollapses(t *testing.T) {
	t.Parallel()
	a, _ := testNewTeardownEnv(t)
	sess := testProvisionSession(t, a, "duplicate-commit")
	_, base := testLoadSessRecord(t, a, sess.idHash)
	sess.mu.Lock()
	sess.subs["attempted"] = 1
	sess.mu.Unlock()
	body := testRecordBody(t, sess)

	// Both proposals land, in log order, carrying the same bytes.
	firstSeq, err := mqttStoreSessionRecordCASBody(&a.asm.jsa, a.asm.domainTk, body, sess.idHash, base)
	if err != nil {
		t.Fatalf("the original's landing: %v", err)
	}
	secondSeq, err := mqttStoreSessionRecordCASBody(&a.asm.jsa, a.asm.domainTk, body, sess.idHash, firstSeq)
	if err != nil {
		t.Fatalf("the re-attempt's landing: %v", err)
	}
	if secondSeq <= firstSeq {
		t.Fatalf("log order must put the re-attempt last: %d then %d", firstSeq, secondSeq)
	}

	// One record survives — the later one — and it holds the ATTEMPT's bytes.
	smsg, err := a.asm.jsa.loadSessionMsg(a.asm.domainTk, sess.idHash)
	if err != nil {
		t.Fatalf("loadSessionMsg: %v", err)
	}
	if smsg.Sequence != secondSeq {
		t.Fatalf("the surviving record is at %d, want the later landing %d", smsg.Sequence, secondSeq)
	}
	if mqttRecordDigest(smsg.Data) != mqttRecordDigest(body) {
		t.Fatal("the surviving record does not hold the attempt's bytes")
	}
	// The duplicate is GONE: MaxMsgsPer:1 discarded it on arrival of the second, so
	// nothing downstream can observe two records for this session. The positive probe
	// first, so "absent" means discarded rather than "this load never finds anything".
	if got, lerr := a.asm.jsa.loadMsg(mqttSessStreamName, secondSeq); lerr != nil || got == nil {
		t.Fatalf("the surviving landing must be loadable at %d: %v", secondSeq, lerr)
	}
	if got, lerr := a.asm.jsa.loadMsg(mqttSessStreamName, firstSeq); lerr == nil && got != nil {
		t.Fatalf("the earlier duplicate is still present at %d: two records for one session", firstSeq)
	}

	// And a session that adopted the later sequence is writable, which is the state a
	// settlement leaves behind.
	sess.mu.Lock()
	sess.seq = secondSeq
	sess.mu.Unlock()
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("a session adopting the surviving sequence must be writable: %v", err)
	}
}

// TestUncertainBaseZeroIsReAttempted replaces TestUncertainBaseZeroNeverReAttempts,
// which asserted the exclusion F-029 removed (test 11, mutation M10).
//
// The old test pinned a real property of the old framing: a first-ever write carried no
// CAS header, so re-sending it would have been an unconditional store that could
// overwrite whatever was there, and the resolver was right to refuse. What changed is
// the framing, not the reasoning — a base-0 create now CASes on "the subject must be
// empty", so re-sending it arbitrates instead of stomping, and the absent-subject
// readback is exactly the case that needs arbitrating.
//
// The two halves are asserted separately so a mutation cannot satisfy one by accident:
// the attempt must be framed WITH a header, and an absent subject must drive a store.
func TestUncertainBaseZeroIsReAttempted(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	sess := testProvisionSession(t, a, "base-zero")

	// An ambiguous first-ever create: base 0, and conditional like every other write.
	testMarkUncertain(sess, 0, []byte(`{"attempted":"conditional-create"}`))
	sess.mu.Lock()
	hdr := sess.pendingWrite.hdr
	sess.mu.Unlock()
	if hdr == 0 {
		t.Fatal("a base-0 attempt must carry a CAS header: an unconditional create is what F-029 closed")
	}

	// The subject must be EMPTY for the "gone" readback to be what the resolver sees.
	// testProvisionSession leaves a record behind, so remove it — and load first, so
	// "absent" below means removed rather than never there.
	smsg, lerr := a.asm.jsa.loadSessionMsg(a.asm.domainTk, sess.idHash)
	if lerr != nil || smsg == nil {
		t.Fatalf("the provisioned record must be loadable before it is removed: %v", lerr)
	}
	if derr := a.asm.jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); derr != nil {
		t.Fatalf("removing the session record: %v", derr)
	}

	var stores int
	restore := setMQTTTestSessStoreResultHook(func(s *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if s == sess {
			stores++
		}
		return resp, err
	})
	defer restore()

	res, rerr := sess.resolveUncertainWrite()
	restore()
	if stores != 1 {
		t.Fatalf("a base-0 ambiguity over an absent subject must be settled by RE-ATTEMPTING it, "+
			"got %d store(s) (err=%v): a readback never settles a rejection", stores, rerr)
	}
	// The re-attempt was accepted against an empty subject, which proves the create is
	// ours: that is the verdict the arbitration produces, and the one the old exclusion
	// could never reach.
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours: the re-attempt landed against an empty subject", res)
	}
}

// TestClassifyAttemptReadbackShapes is the shared classification helper on its own,
// including the shapes a live monotonic stream cannot fabricate.
//
// Both reads the resolver takes go through this function, so the `Sequence > baseSeq`
// conjunct is stated once and cannot drift between them. A record BELOW the base is
// impossible within a stream incarnation — sequences are monotonic, so a subject
// deleted and re-created lands HIGHER — and a lower one implies a reset or
// re-provisioned store. It is classified foreign FAIL-SAFE, never adopted, however
// its bytes compare.
//
// Mutation M11: drop the `Sequence > baseSeq` conjunct and the byte-identical
// below-base record is adopted as ours.
func TestClassifyAttemptReadbackShapes(t *testing.T) {
	t.Parallel()
	body := []byte(`{"id":"c","subs":{"foo":1}}`)
	pw := &mqttPendingWrite{baseSeq: 10, digest: mqttRecordDigest(body)}
	other := []byte(`{"id":"c","subs":{"theirs":2}}`)

	for _, tc := range []struct {
		name string
		smsg *StoredMsg
		lerr error
		want mqttAttemptReadback
		seq  uint64
	}{
		{"unreadable", nil, errors.New("boom"), mqttAttemptUnreadable, 0},
		{"gone at a non-zero base", nil, ApiErrors[JSNoMessageFoundErr], mqttAttemptGone, 0},
		{"at base", &StoredMsg{Sequence: 10, Data: body}, nil, mqttAttemptAtBase, 0},
		{"past base, byte-identical", &StoredMsg{Sequence: 11, Data: body}, nil, mqttAttemptOurs, 11},
		{"past base, different content", &StoredMsg{Sequence: 11, Data: other}, nil, mqttAttemptForeign, 0},
		{"below base, byte-identical", &StoredMsg{Sequence: 9, Data: body}, nil, mqttAttemptForeign, 0},
		{"below base, different content", &StoredMsg{Sequence: 9, Data: other}, nil, mqttAttemptForeign, 0},
		{"no error and no message", nil, nil, mqttAttemptUnreadable, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, seq := classifyAttemptReadback(tc.smsg, tc.lerr, pw, _EMPTY_, _EMPTY_)
			if got != tc.want || seq != tc.seq {
				t.Fatalf("classifyAttemptReadback = (%v, %d), want (%v, %d)", got, seq, tc.want, tc.seq)
			}
		})
	}

	// Gone at a base of ZERO reaches the ordinary gone arm since F-029 (M10). The
	// special case that returned foreign here existed because an unconditional create
	// could not be re-attempted; the create is conditional now, so the absent subject is
	// arbitrated by re-sending it rather than guessed at from a read.
	zero := &mqttPendingWrite{baseSeq: 0, digest: mqttRecordDigest(body)}
	if got, _ := classifyAttemptReadback(nil, ApiErrors[JSNoMessageFoundErr], zero, _EMPTY_, _EMPTY_); got != mqttAttemptGone {
		t.Fatalf("classifyAttemptReadback(gone, base 0) = %v, want gone", got)
	}

	// The digest is over the record BODY, which is what a load returns — the CAS
	// header is a message header and never part of the stored data.
	if mqttRecordDigest(body) != sha256.Sum256(body) {
		t.Fatal("the identity must be the digest of the stored body")
	}
}

// TestUncertainBelowBaseRecordIsNeverAdopted is the below-base fail-safe end to end,
// through a session rather than the helper: a record whose bytes match the attempt
// exactly but whose sequence is BELOW its base must not be adopted, because adopting
// it would turn a stale snapshot into a valid CAS over whatever that record is.
func TestUncertainBelowBaseRecordIsNeverAdopted(t *testing.T) {
	t.Parallel()
	a, _ := testNewTeardownEnv(t)
	sess := testProvisionSession(t, a, "below-base")
	body := testRecordBody(t, sess)
	_, seq := testLoadSessRecord(t, a, sess.idHash)

	// The attempt claims a base ABOVE the record's own sequence, which is the shape a
	// reset or re-provisioned store leaves behind.
	testMarkUncertain(sess, seq+5, body)

	res, err := sess.resolveUncertainWrite()
	if err != nil {
		t.Fatalf("resolveUncertainWrite: %v", err)
	}
	if res != mqttUncertainForeign {
		t.Fatalf("resolution = %v, want foreign: a record below the attempt's base is never its landing", res)
	}
	if !sess.isSeqUncertain() {
		t.Fatal("a foreign verdict must leave the local write unresolved, not resolved as ours")
	}
	sess.mu.Lock()
	pw := sess.pendingWrite
	sess.mu.Unlock()
	if pw == nil || pw.baseSeq != seq+5 {
		t.Fatalf("the attempt's identity must be untouched by a foreign verdict, got %+v", pw)
	}
	// And nothing was written: adopting a below-base record would have turned this
	// stale snapshot into a valid CAS over it.
	if _, cur := testLoadSessRecord(t, a, sess.idHash); cur != seq {
		t.Fatalf("the record moved to %d: a below-base classification must write nothing", cur)
	}
}
