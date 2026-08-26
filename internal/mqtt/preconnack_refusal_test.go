package mqtt

import (
	"errors"
	"testing"
)

// The pre-CONNACK refusal signal's projection, and the one thing it must never do.
//
// A refusal that DISPOSES is told to the client by preConnackRefusalFor, and that
// projection is deliberately NOT the disposal vocabulary: the vocabulary's takeover text
// asserts a successor most arms reaching it never established, which is F-084's class.
// So the projection has its own small vocabulary, and the tests below pin both halves —
// what each outcome says, and that no outcome can acquire an ownership claim by default.

// TestPreConnackRefusalProjectionIsExhaustive is T8.
//
// It exists because no wire-level fixture can reach this helper: preConnackRefusalFor is
// called only from refuseDisposing, and the Local and Unresolved arms refuse through
// refusePreConnack directly, carrying the step's own error. So M13 — giving the switch a
// permissive default — is invisible end to end and only this test kills it.
//
// The out-of-range row is the whole point. A `default` arm returning an ownership refusal
// would be indistinguishable from correct behaviour on all five defined outcomes and
// would silently give a future outcome an ownership claim it has no evidence for.
func TestPreConnackRefusalProjectionIsExhaustive(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		outcome mqttPreConnackOutcome
		wantOK  bool
		want    mqttPreConnackRefusal
	}{
		{"forked claims a sibling and invites a retry", mqttPreConnackDisposingForked, true,
			mqttPreConnackRefusal{mqttReasonServerBusy, mqttReasonStrOwnershipUnsettled}},
		{"non-forked claims only that ownership is unconfirmed", mqttPreConnackDisposingNonForked, true,
			mqttPreConnackRefusal{mqttReasonImplementationSpecificError, mqttReasonStrOwnershipUnconfirmed}},
		// The three non-disposing outcomes are listed in the switch rather than assumed
		// unreachable, and each must report false so a caller that arrives here with one
		// sends NOTHING rather than a 0x00 CONNACK.
		{"proceed is not a refusal at all", mqttPreConnackProceed, false, mqttPreConnackRefusal{}},
		{"local is not an ownership statement", mqttPreConnackLocal, false, mqttPreConnackRefusal{}},
		{"unresolved is not an ownership statement", mqttPreConnackUnresolved, false, mqttPreConnackRefusal{}},
		// M13's row.
		{"an outcome nobody answered gets no claim", mqttPreConnackOutcome(99), false, mqttPreConnackRefusal{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := preConnackRefusalFor(tc.outcome)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if got != tc.want {
				t.Fatalf("refusal = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestPreConnackRefusalNeverCarriesTheTakeoverText pins the boundary Move A rests on, as a
// property rather than as a list of call sites.
//
// The disposal vocabulary and the client-facing projection are different vocabularies on
// purpose. If a future edit routes the vocabulary's Reason String to the CONNACK — M8a —
// a client whose record was merely unreadable, absent, at-or-below base or simply
// different is told "session taken over by a new connection", which is the claim this
// item was careful not to make and which F-084 owns.
func TestPreConnackRefusalNeverCarriesTheTakeoverText(t *testing.T) {
	t.Parallel()
	for _, o := range []mqttPreConnackOutcome{
		mqttPreConnackDisposingForked, mqttPreConnackDisposingNonForked,
	} {
		r, ok := preConnackRefusalFor(o)
		if !ok {
			t.Fatalf("outcome %v disposes and must have an answer", o)
		}
		if r.reasonStr == mqttReasonStrSessionTakenOver {
			t.Fatalf("outcome %v tells the client %q; no pre-CONNACK arm establishes a takeover",
				o, r.reasonStr)
		}
		// And the code is one [3.2.2.2] permits. 0x8E is DISCONNECT-only, so it can never
		// appear here whatever the vocabulary says.
		if r.code != mqttReasonServerBusy && r.code != mqttReasonImplementationSpecificError {
			t.Fatalf("outcome %v projects to %#x, which is not a legal CONNACK reason code", o, r.code)
		}
	}
}

// TestReconcileNonDisposingArmsCarryNoVocabulary is T4a.
//
// The returned vocabulary is what the CALLER hands to the disposal, so a Local or
// Unresolved arm that carried one would have the caller dispose a session nothing proved
// gone — and, if the projection ever read the vocabulary instead of the outcome, tell the
// client an ownership story about a failed consumer create. M7 (defaulting the Local arm's
// vocabulary to the takeover one) is invisible on the wire, because a Local refusal's
// 0x83 is unchanged by it, so this is the test that kills it.
//
// Driven through the PRODUCTION phase rather than by constructing result values: a table
// of literals would pass against any implementation at all.
// NOT t.Parallel(): its third row installs setMQTTTestSessSaveHook, a package-level atomic,
// so a concurrent test would observe this one's injected save failure and vice versa.
// parallel_safety_test.go enforces exactly that, and caught this.
func TestReconcileNonDisposingArmsCarryNoVocabulary(t *testing.T) {
	for _, tc := range []struct {
		name string
		arm  func(t *testing.T, e *testRejectionEnv) func()
		want mqttPreConnackOutcome
	}{
		{"an indeterminate verification", func(t *testing.T, e *testRejectionEnv) func() {
			e.asm.mu.Lock()
			e.asm.testResumeReconcileLookup = func([]string) ([]*ConsumerInfo, []bool, error) {
				return nil, nil, errors.New("simulated verification outage")
			}
			e.asm.mu.Unlock()
			return func() {
				e.asm.mu.Lock()
				e.asm.testResumeReconcileLookup = nil
				e.asm.mu.Unlock()
			}
		}, mqttPreConnackLocal},
		{"a refused consumer create", func(t *testing.T, e *testRejectionEnv) func() {
			e.asm.mu.Lock()
			e.asm.testResumeReconcilePreCreate = func(*mqttSession, string) error {
				return errors.New("simulated consumer create rejection")
			}
			e.asm.mu.Unlock()
			return func() {
				e.asm.mu.Lock()
				e.asm.testResumeReconcilePreCreate = nil
				e.asm.mu.Unlock()
			}
		}, mqttPreConnackLocal},
		// The POST-save Local arm, which the two above never reach: they refuse before the
		// phase's record write is even attempted. Without this row a mutation that defaults
		// the post-save arm's vocabulary SURVIVES — measured, not assumed.
		{"a definitely failed record write", func(t *testing.T, e *testRejectionEnv) func() {
			return setMQTTTestSessSaveHook(func(sess *mqttSession) error {
				if sess.id != e.sess.id {
					return nil
				}
				return errors.New("simulated definite non-CAS save failure")
			})
		}, mqttPreConnackLocal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewForkEnv(t, "t4a-"+getHash(tc.name))
			b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
			cleanup := tc.arm(t, e)
			defer cleanup()

			r := testReconcileWithRecordedConsumer(t, e, b.bc)
			if r.outcome != tc.want {
				t.Fatalf("outcome %v, want %v (%v)", r.outcome, tc.want, r.err)
			}
			if r.outcome.disposes() {
				t.Fatalf("outcome %v must not dispose", r.outcome)
			}
			if r.voc != (mqttDisposalVocabulary{}) {
				t.Fatalf("a non-disposing outcome carried vocabulary %+v; the caller would "+
					"dispose a session nothing proved gone", r.voc)
			}
			// The session is still OURS: a local refusal leaves it resident and detaches
			// through ordinary close bookkeeping.
			if !e.resident(e.sess) {
				t.Fatal("a local refusal must leave the session resident")
			}
			if r.err == nil {
				t.Fatal("a refusal must carry its own diagnostic; it is what the client is told")
			}
		})
	}
}

// testReconcileWithRecordedConsumer drives the phase with a filter that DOES carry a
// recorded consumer identity, which is what makes the verification step reachable at all:
// a session whose only entry has no Cons record contributes no durable to look up, so the
// batched lookup loop never runs and an injected lookup failure fires against nothing.
//
// The durable name is one that does not exist, so the lookup — when it is allowed to
// answer — reports it absent and the phase goes on to repair it. That is what puts the
// create seam on the path too, from the same fixture.
func testReconcileWithRecordedConsumer(t *testing.T, e *testRejectionEnv, c *client) mqttResumeReconcileResult {
	t.Helper()
	const subject = "t4a/filter"
	e.sess.mu.Lock()
	e.sess.subs = map[string]byte{subject: 1}
	e.sess.cons = map[string]*ConsumerConfig{subject: {
		Durable:        e.sess.idHash + "_t4aAbsent",
		DeliverSubject: mqttSubPrefix + "t4aDeliver",
		FilterSubject:  mqttStreamSubjectPrefix + subject,
		AckPolicy:      AckExplicit,
	}}
	e.sess.mu.Unlock()

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
