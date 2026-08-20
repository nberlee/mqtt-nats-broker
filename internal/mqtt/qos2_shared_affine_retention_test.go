//go:build mqttport_fulltests

// Retention proof for session-affine shared-subscription QoS 2 (F-063).
//
// The property under test: the group source is released only against evidence — a
// copy PROVEN PRESENT by a fresh lookup, a terminal fact on the claim, or a
// SourceAcked already recorded. A store RESPONSE is not evidence in either
// direction, and neither is the `materialized` phase: $MQTT_out is
// interest-retained, so the copy a materialized claim names can be dropped with its
// consumer afterwards, while the source ack is irreversible and releases the message
// from the shared group forever.
//
// Three routes reach that decision and each gets its own fixture, because a remedy
// on one says nothing about the others: the winner's OWN-CLAIM path, a contender's
// ON-BEHALF path, and the MATERIALIZED OBSERVATION a contender makes about a foreign
// claim. The two callers of materializeQ2AffCopy are also mutated independently, so
// each carries its own terminal-fact fixture.
//
// Where a fixture stages durable state it does so through the election seam rather
// than by predicting $MQTT_msgs' next sequence: the interceptor is handed the claim
// the mover actually computed, so the group durable, the source sequence and the
// claim hash are the real ones and no run is skippable on unrelated stream activity.

package mqtt

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Observing a decision that is invisible on the wire
// ---------------------------------------------------------------------------

// testQ2AffMoveLog records the resolved disposition of every move on one account
// manager. A refusal sends nothing at all, so "refused" and "still running" are
// indistinguishable from the wire and from the claim record alike — only this seam
// tells them apart, which is what makes a refusal fixture positive evidence rather
// than an assertion about silence.
type testQ2AffMoveLog struct {
	mu   sync.Mutex
	seen map[string][]mqttQ2AffMoveResult
}

// testQ2AffWatchMoves installs the disposition seam for the rest of the test. The
// closure touches no *testing.T, so it needs no hook gate: everything it does is
// guarded by its own mutex and remains safe if a move lands after the test's
// cleanup has restored the seam.
func testQ2AffWatchMoves(t *testing.T, b *testQ2AffBroker) *testQ2AffMoveLog {
	t.Helper()
	l := &testQ2AffMoveLog{seen: map[string][]mqttQ2AffMoveResult{}}
	b.asm.mu.Lock()
	b.asm.testQ2AffMoveDone = func(subject string, res mqttQ2AffMoveResult) {
		l.mu.Lock()
		l.seen[subject] = append(l.seen[subject], res)
		l.mu.Unlock()
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQ2AffMoveDone = nil
		b.asm.mu.Unlock()
	})
	return l
}

func (l *testQ2AffMoveLog) results(subject string) []mqttQ2AffMoveResult {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]mqttQ2AffMoveResult(nil), l.seen[subject]...)
}

// await blocks until a move on subject resolved as want. The deadline is generous
// on purpose: the mover runs on a JetStream delivery callback, and under a loaded
// full-suite run it can be scheduled well after the publish that triggers it. The
// poll exits the instant the disposition appears, so a passing run pays nothing.
func (l *testQ2AffMoveLog) await(t *testing.T, subject string, want mqttQ2AffMoveResult, what string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, got := range l.results(subject) {
			if got == want {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no move on %q ever resolved as %v (%s); dispositions recorded: %v",
		subject, want, what, l.results(subject))
}

// never fails if any move on subject resolved as one of banned. Used alongside
// await: "it refused" and "it never acked" are two different assertions, and a
// fixture that checks only the first would pass against a route that acked once and
// then refused forever.
func (l *testQ2AffMoveLog) never(t *testing.T, subject string, banned ...mqttQ2AffMoveResult) {
	t.Helper()
	for _, got := range l.results(subject) {
		for _, bad := range banned {
			if got == bad {
				t.Fatalf("a move on %q resolved as %v, which releases the group source; dispositions: %v",
					subject, got, l.results(subject))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Staging durable state on the subject the mover really computed
// ---------------------------------------------------------------------------

// testQ2AffSeedElectedClaim intercepts the FIRST election on this account manager,
// writes mutate's version of the claim the mover was about to create, and then lets
// the real election run and be genuinely rejected by that record. The mover's own
// load-and-honor path then picks the seeded claim up and takes whichever arm the
// fixture is about.
//
// Two things make this the right seam rather than seeding a claim by hand. The
// record carries the REAL group durable, source sequence and claim hash, so nothing
// about the key is fabricated; and the rejection is an authentic
// wrong-last-sequence error produced by JetStream, not a manufactured one. mutate
// may also stage side state (a copy on the claim's copy subject, for instance)
// because it runs before the record is written.
//
// Returns an accessor for the claim subject the interception happened on.
func testQ2AffSeedElectedClaim(t *testing.T, b *testQ2AffBroker, mutate func(*mqttQ2AffClaim)) func() (string, bool) {
	t.Helper()
	// The gate is registered BEFORE the seam's restore so cleanups, which run
	// last-registered-first, stop new calls and only then wait out one already
	// inside. The closure below reports through t, so an ungated late call would
	// panic the whole binary rather than fail this test.
	gate := newTestHookGate(t)
	var (
		mu      sync.Mutex
		subject string
		seeded  bool
	)
	b.asm.mu.Lock()
	b.asm.testQ2AffClaimStore = func(subj string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error) {
		// expectSeq 0 is the create-only election and nothing else: every transition
		// CASes at the sequence it loaded, which is never 0.
		if expectSeq != 0 || !gate.enter() {
			return b.asm.jsa.storeQ2AffClaimCAS(subj, expectSeq, rec)
		}
		defer gate.leave()
		mu.Lock()
		first := !seeded
		if first {
			seeded, subject = true, subj
		}
		mu.Unlock()
		if first {
			seed := *rec
			mutate(&seed)
			if _, err := b.asm.jsa.storeQ2AffClaimCAS(subj, 0, &seed); err != nil {
				t.Errorf("seeding the claim under test on %q: %v", subj, err)
			}
		}
		return b.asm.jsa.storeQ2AffClaimCAS(subj, expectSeq, rec)
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQ2AffClaimStore = nil
		b.asm.mu.Unlock()
	})
	return func() (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		return subject, seeded
	}
}

// testQ2AffAwaitSeeded returns the subject the election interceptor fired on.
func testQ2AffAwaitSeeded(t *testing.T, seeded func() (string, bool)) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if subject, ok := seeded(); ok {
			return subject
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the election was never intercepted, so this run staged nothing and proves nothing")
	return _EMPTY_
}

// testQ2AffSyntheticWinner provisions a winner that never had a CLIENT on this broker: a
// valid session record naming an affine generation, plus that generation's affine consumer
// on $MQTT_out. Those two things are exactly what materializeQ2AffOnBehalf checks in its
// steps (1) and (2), and nothing else about a session is needed to be materialized onto.
//
// **This removes a class of fixture race rather than fencing it off.** A winner with a
// client competes for group deliveries, so the mover can turn out to be the winner itself
// and take completeQ2AffOwnClaim — which consumes the one-shot hooks these fixtures stage
// and lets them pass with their window never entered. Three attempts to fence that off all
// failed for the same reason: they tried to prove a negative about an asynchronous teardown
// the server gives no synchronization point for. A session with a non-zero expiry stays
// resident by design, and closeConnection removes the client from `s.clients` BEFORE
// unsubscribing its subs. With no client there is no teardown, and no racer.
//
// It is also a state a real deployment reaches, not a contrivance: a record plus a consumer
// with no local client is F-064's stranded-owner shape — the winning session serving on
// another broker, or gone while its delivery was in flight.
func testQ2AffSyntheticWinner(t *testing.T, b *testQ2AffBroker, id string) (idHash, gen string) {
	t.Helper()
	return testQ2AffSyntheticWinnerWith(t, b, id, nil, nil)
}

// testQ2AffSyntheticWinnerWith is testQ2AffSyntheticWinner with two hooks for
// fixtures that need the staged shape to DEVIATE: record adjusts the persisted
// record (including its Q2Aff) after the canonical config is built, and consumer
// adjusts the durable that is actually created. Either may be nil.
//
// They are separate on purpose. A record that no longer matches the durable is
// precisely what several of F-064's refusals are about, so a helper that kept the
// two in step could not stage them.
func testQ2AffSyntheticWinnerWith(t *testing.T, b *testQ2AffBroker, id string,
	record func(*mqttPersistedSession), consumer func(*ConsumerConfig)) (idHash, gen string) {
	t.Helper()
	idHash, gen = getHash(id), mqttNewQ2AffGen()
	// The record carries the consumer CONFIG, not just the generation, because a
	// session-provisioned record does: ensureQ2AffineConsumerSubscription persists
	// the request it sent. A generation with no config is a real but DIFFERENT
	// shape — restoreQ2AffineUnwind's names-only stub — and on-behalf restoration
	// refuses it, because its zero values are not merely incomplete but wrong
	// (AckPolicy zero is AckNone, DeliverPolicy zero is DeliverAll). Staging one
	// here would make every fixture built on this helper observe THAT refusal
	// instead of the window it is trying to enter. Built through the single
	// constructor for the same reason the production writers are.
	cfg := buildQ2AffineConsumerConfig(idHash, gen, time.Minute, mqttDefaultMaxAckPending, false)
	testQ2AffForeignStoreWith(t, testQ2AffPeerASM(t, b, "synthetic-"+id), id,
		func(ps *mqttPersistedSession) {
			ps.Q2AffGen = gen
			q2aff := cfg
			ps.Q2Aff = &q2aff
			if record != nil {
				record(ps)
			}
		})

	durable := cfg.Durable
	staged := cfg
	if consumer != nil {
		consumer(&staged)
	}
	if _, err := b.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: staged,
	}); err != nil {
		t.Fatalf("creating the synthetic winner's affine consumer: %v", err)
	}
	// The on-behalf lookup validates the durable against these exact names, so a consumer
	// this test created must satisfy the same rule a session-provisioned one does. Asserted
	// rather than assumed: if it did not, step (2) would refuse and every fixture built on
	// this helper would be observing the wrong refusal.
	ci, err := b.asm.jsa.lookupConsumer(mqttOutStreamName, durable)
	if err != nil || ci == nil || ci.Config == nil {
		t.Fatalf("looking up the synthetic winner's affine consumer: ci=%v err=%v", ci, err)
	}
	if consumer == nil {
		if verr := mqttValidateQ2AffineNames(ci.Config, idHash, gen); verr != nil {
			t.Fatalf("the synthetic winner's consumer fails the on-behalf identity check: %v", verr)
		}
	}
	return idHash, gen
}

// testQ2AffAssertSourcePresent fails unless the claim's SOURCE message loads successfully
// at exactly its recorded sequence.
//
// Deliberately NOT `!q2AffSourceGone(claim)`. That production predicate is fail-CLOSED: it
// returns false both when the source is present and when the stream or the message could
// not be read (see its body). Negating a fail-closed predicate is not positive evidence, so
// an indeterminate read would let a refusal fixture succeed even though confirmAck had
// already committed — the ack-versus-record distinction again, one level up. Here every
// error fails, and only a message that loads counts as present.
// Takes an account manager rather than a broker so the cluster fixture can hold every
// broker to the same standard: "the source is still there" has to mean the same thing on
// one node as on three.
func testQ2AffAssertSourcePresent(t *testing.T, asm *mqttAccountSessionManager, claim *mqttQ2AffClaim, when string) {
	t.Helper()
	if claim.SourceSeq == 0 {
		t.Fatalf("%s: the claim records no source sequence, so its source cannot be verified: %+v", when, *claim)
	}
	si, err := asm.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		t.Fatalf("%s: looking up %s: si=%v err=%v", when, mqttStreamName, si, err)
	}
	if claim.SourceSeq < si.State.FirstSeq {
		t.Fatalf("%s: the source at %d is below %s's first sequence %d, so it is gone",
			when, claim.SourceSeq, mqttStreamName, si.State.FirstSeq)
	}
	if _, err := asm.jsa.loadMsg(mqttStreamName, claim.SourceSeq); err != nil {
		t.Fatalf("%s: the source at %d could not be loaded from %s: %v", when, claim.SourceSeq, mqttStreamName, err)
	}
}

// testQ2AffStoreCopyFor puts a real copy on the claim's copy subject, encoded by the
// production encoder. A fixture that needs "the copy is present" must not fabricate
// a record: mqttNewQ2AffineRecord is what the delivery gate and the presence check
// are written against.
func testQ2AffStoreCopyFor(t *testing.T, b *testQ2AffBroker, claimSubject string, claim *mqttQ2AffClaim, payload string) {
	t.Helper()
	env := &mqttQ2AffineEnvelope{
		natsSubject:  b.topic,
		msg:          []byte(payload),
		claimSubject: claimSubject,
		gen:          claim.Gen,
		msgID:        mqttQ2AffMsgId(claim.GroupDurable, claim.SourceSeq),
	}
	msg, hdrLen := mqttNewQ2AffineRecord(env)
	if _, err := b.asm.jsa.storeMsg(claim.CopySubject, hdrLen, msg); err != nil {
		t.Fatalf("storing the copy on %q: %v", claim.CopySubject, err)
	}
}

// testQ2AffRealCopyStore performs the store the copy seam replaces, in the seam's own
// shape. A fixture that wants JetStream's genuine answer — a no-interest success, a
// duplicate, a rejected CAS — must go through the real store rather than fabricate one.
func testQ2AffRealCopyStore(b *testQ2AffBroker, subject string, hdrLen int, msg []byte) (uint64, error) {
	resp, err := b.asm.jsa.storeMsg(subject, hdrLen, msg)
	if err != nil {
		return 0, err
	}
	return resp.Sequence, nil
}

// testQ2AffLoadClaim reads one claim, treating an indeterminate load as a failure:
// every assertion here is about durable state, and a failed read is not a state.
func testQ2AffLoadClaim(t *testing.T, b *testQ2AffBroker, subject string) *mqttQ2AffClaim {
	t.Helper()
	claim, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("loading the claim on %q: %v", subject, err)
	}
	return claim
}

// testQ2AffAwaitClaim polls until the claim on subject satisfies pred.
func testQ2AffAwaitClaim(t *testing.T, b *testQ2AffBroker, subject, what string,
	pred func(*mqttQ2AffClaim) bool) *mqttQ2AffClaim {

	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last *mqttQ2AffClaim
	for time.Now().Before(deadline) {
		last = testQ2AffLoadClaim(t, b, subject)
		if pred(last) {
			return last
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the claim on %q never became %s; last read: %+v", subject, what, last)
	return nil
}

// testQ2AffAssertNotReleased asserts that the group source behind subject stays
// unreleased across FURTHER re-offers, and that the claim stays exactly as refused: no
// SourceAcked, no terminal fact, no phase promotion.
//
// It waits for `extraRefusals` ADDITIONAL deferred dispositions rather than for a
// stretch of wall-clock time, and that is the whole point. An elapsed window only shows
// that nothing changed while nobody was looking: under scheduling delay it can expire
// without a single further delivery callback, so a regression that refuses the first
// attempt and releases the source on a RETRY would pass it vacuously. That is not a
// hypothetical difference in this protocol — the first own attempt takes the
// fresh-election path and every retry takes the resumed-own-claim path, which is a
// different arm of the mover.
//
// Every poll re-checks the durable state, so a release at any point in the window fails
// immediately rather than being averaged away.
func testQ2AffAssertNotReleased(t *testing.T, b *testQ2AffBroker, moves *testQ2AffMoveLog,
	subject string, want mqttQ2AffPhase, extraRefusals int) {

	t.Helper()
	before := len(moves.results(subject))
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		claim := testQ2AffLoadClaim(t, b, subject)
		if claim == nil {
			t.Fatalf("the claim on %q was reaped; nothing may reap a claim whose source was never released", subject)
		}
		if claim.SourceAcked {
			t.Fatalf("the group source behind %q was released with no proof the copy was retained: %+v", subject, *claim)
		}
		if claim.terminal() {
			t.Fatalf("the claim on %q acquired a terminal fact (Discarded=%v AffineDone=%v Lost=%v); nothing delivered or disposed of this message",
				subject, claim.Discarded, claim.AffineDone, claim.Lost)
		}
		if claim.Phase != want {
			t.Fatalf("the claim on %q moved to phase %v, want %v", subject, claim.Phase, want)
		}
		// The SOURCE itself, on every poll, and by a POSITIVE load. confirmSourceAckAndRecord
		// calls confirmAck BEFORE writing SourceAcked, so a claim reading
		// `SourceAcked == false` does not prove the irreversible ack never committed — and
		// if that CAS then fails, the foreign-materialized and on-behalf paths fall back to
		// their default `deferred` result, so the disposition does not prove it either.
		testQ2AffAssertSourcePresent(t, b.asm, claim, "during the refusal window")
		got := moves.results(subject)
		for _, res := range got[min(before, len(got)):] {
			if res != mqttQ2AffMoveDeferred {
				t.Fatalf("a re-offer of %q resolved as %v, which releases the group source; dispositions: %v",
					subject, res, got)
			}
		}
		if len(got)-before >= extraRefusals {
			// Once more immediately before succeeding, so the last observed re-offer is
			// covered by the same invariant as every earlier one.
			fresh := testQ2AffLoadClaim(t, b, subject)
			if fresh == nil || fresh.SourceAcked || fresh.terminal() {
				t.Fatalf("the source behind %q was released on the final re-offer: claim=%+v", subject, fresh)
			}
			testQ2AffAssertSourcePresent(t, b.asm, fresh, "after the final re-offer")
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("only %d further move(s) on %q were observed, want %d: AckWait did not re-offer the source, so this run never exercised the retry path",
		len(moves.results(subject))-before, subject, extraRefusals)
}

// testQ2AffAssertCopyAbsent asserts the copy subject is AUTHORITATIVELY absent. An
// unreadable copy is not an absent one, and a fixture resting on a failed lookup
// would assert nothing about interest retention.
func testQ2AffAssertCopyAbsent(t *testing.T, b *testQ2AffBroker, copySubject, why string) {
	t.Helper()
	if st, _ := b.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyAbsent {
		t.Fatalf("the copy on %q reads %v, want absent: %s", copySubject, st, why)
	}
}

// ---------------------------------------------------------------------------
// R2: the on-behalf path gathers the evidence and cannot bind it
// ---------------------------------------------------------------------------

// The contender verifies the winner's affine consumer BEFORE storing the copy
// (materializeQ2AffOnBehalf step 2), and that lookup is not what authorizes the release:
// the consumer can be deleted in between, the interest-retained store then succeeds
// keeping nothing, and the source would be acked for a copy that does not exist.
//
// **The own-claim path is structurally unreachable here, not merely fenced off.** The
// claim is seeded to name the OTHER member, so the mover can never own it and the
// `default` arm — materializeQ2AffOnBehalf — is the only route it has. Two earlier
// versions tried instead to wait until the winner could no longer take a re-offer, and
// both were wrong: a session with a non-zero expiry stays resident by design, and
// closeConnection deletes the client from `s.clients` BEFORE unsubscribing its subs
// (shim_engine.go), so neither absence proves the old subscription is done. If the
// winner's own path had reached the store it would have consumed the one-shot hook
// below, and this fixture would have passed with the TOCTOU never staged.
//
// The winner's session record and affine consumer both survive its disconnect, which is
// exactly what an on-behalf materializer verifies before storing.
//
// Discriminating mutation: verify only on a rejected CAS (mutation 1). The presence check
// after the successful store disappears, the claim advances to materialized and the source
// is acked, so this fixture fails on the released source.
func TestQ2AffOnBehalfRefusesSourceAckWhenConsumerVanishesAfterLookup(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "obtoctou")
	ca, _ := b.member(t, "obtoctouA", 300)
	defer ca.Close()
	b.bind(t, "obtoctouA")
	moves := testQ2AffWatchMoves(t, b)

	// ONE connected member, and a winner that never had a client here. So the mover is
	// always the contender, the own-claim path has no session that could take it, and
	// materializeQ2AffOnBehalf is the only route to the store.
	wHash, wGen := testQ2AffSyntheticWinner(t, b, "obtoctouW")
	wDurable := affDurableFor(wHash, wGen)

	gate := newTestHookGate(t)
	var (
		vanish   atomic.Bool  // the on-behalf store ran, so the window was entered
		vanished atomic.Value // and the claim it ran for, read from the copy's own header
	)
	b.asm.mu.Lock()
	b.asm.testQ2AffCopyStore = func(subject string, hdrLen int, msg []byte) (uint64, error) {
		if !gate.enter() {
			return testQ2AffRealCopyStore(b, subject, hdrLen, msg)
		}
		defer gate.leave()
		if vanish.CompareAndSwap(false, true) {
			// Recorded from the Nmqtt-Q2Claim header so the fixture can prove the window was
			// entered for THE claim under test, not for some other store.
			vanished.Store(string(getHeader(mqttQ2AffClaimHeader, msg[:hdrLen])))
			// Between step (2)'s lookup and step (3)'s store, which IS the finding.
			if _, err := b.asm.jsa.deleteConsumer(mqttOutStreamName, wDurable, false); err != nil {
				t.Errorf("deleting the winner's affine consumer between the lookup and the store: %v", err)
			}
		}
		// The genuine store, now with no interest behind it: JetStream's own retention
		// drops it rather than the seam pretending to.
		return testQ2AffRealCopyStore(b, subject, hdrLen, msg)
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyStore = nil
		b.asm.mu.Unlock()
	})

	var copySubject atomic.Value
	seeded := testQ2AffSeedElectedClaim(t, b, func(seed *mqttQ2AffClaim) {
		// Tripwire: the only session that can elect here is the connected member. If any
		// other mover ever does, this fixture is no longer staging what it claims.
		if seed.Winner != getHash("obtoctouA") {
			t.Errorf("the election came from %q, want the sole connected member: the own-claim path is reachable after all", seed.Winner)
		}
		seed.Winner, seed.Gen = wHash, wGen
		seed.CopySubject = mqttQ2AffCopySubject(wHash, wGen, seed.ClaimHash)
		seed.AffDurable = wDurable
		copySubject.Store(seed.CopySubject)
	})

	b.publish(t, 1, "obtoctou")
	subject := testQ2AffAwaitSeeded(t, seeded)

	// The window is entered: the consumer existed for the lookup and is gone for the
	// store. Without this the fixture would assert a refusal that an absent record or a
	// failed lookup would also produce.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && !vanish.Load() {
		time.Sleep(20 * time.Millisecond)
	}
	if !vanish.Load() {
		t.Fatal("the on-behalf store never ran, so the lookup-then-store window was never entered and this run proves nothing")
	}
	if got, _ := vanished.Load().(string); got != subject {
		t.Fatalf("the lookup-then-store window was entered for claim %q, not the one under test %q", got, subject)
	}

	moves.never(t, subject, mqttQ2AffMoveOnBehalf, mqttQ2AffMoveForeign, mqttQ2AffMoveTerminal, mqttQ2AffMoveOwned)
	cs, _ := copySubject.Load().(string)
	testQ2AffAssertCopyAbsent(t, b, cs, "the store landed on a deleted consumer and must have kept nothing")
	testQ2AffAssertNotReleased(t, b, moves, subject, mqttQ2AffClaimed, 2)
}

// ---------------------------------------------------------------------------
// R3: a materialized claim is a record of the past
// ---------------------------------------------------------------------------

// A contender that finds a foreign claim already `materialized` used to release the
// group source on the phase alone. The phase says a copy was proven at some earlier
// instant; interest retention can have dropped it since, and the ack is
// irreversible. So the reading is taken by the decision that acts on it.
//
// The foreign winner here is a generation no session owns, so nothing holds interest
// on its copy prefix and the copy is authoritatively absent. That is the whole
// staging: this arm consults neither the session record nor the consumer, only the
// claim and the copy.
//
// Discriminating mutation: remove the decision-point copy read in the contender's
// materialized arm (mutation 3). It acks unconditionally again and this fixture
// fails on the released source.
func TestQ2AffMaterializedNoCopyRefusesSourceAck(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "matnocopy")
	c, r := b.member(t, "matnocopyA", 300)
	defer c.Close()
	b.bind(t, "matnocopyA")
	moves := testQ2AffWatchMoves(t, b)

	foreign, foreignGen := getHash("matnocopy-elsewhere"), mqttNewQ2AffGen()
	var copySubject atomic.Value
	seeded := testQ2AffSeedElectedClaim(t, b, func(seed *mqttQ2AffClaim) {
		seed.Winner, seed.Gen = foreign, foreignGen
		seed.CopySubject = mqttQ2AffCopySubject(foreign, foreignGen, seed.ClaimHash)
		_, _, seed.AffDurable = mqttQ2AffineNames(foreign, foreignGen)
		seed.Phase = mqttQ2AffMaterialized
		copySubject.Store(seed.CopySubject)
	})

	b.publish(t, 1, "matnocopy")
	subject := testQ2AffAwaitSeeded(t, seeded)

	moves.await(t, subject, mqttQ2AffMoveDeferred, "the contender refused to release a source for a copy it could not find")
	moves.never(t, subject, mqttQ2AffMoveForeign, mqttQ2AffMoveOnBehalf, mqttQ2AffMoveTerminal)
	cs, _ := copySubject.Load().(string)
	testQ2AffAssertCopyAbsent(t, b, cs, "no session owns the winning generation, so nothing retains its copies")
	testQ2AffAssertNotReleased(t, b, moves, subject, mqttQ2AffMaterialized, 2)

	// And the contender never delivers a message assigned to another session — the
	// conformance property the whole file exists for, re-checked on this route.
	if _, pl, _, _, got := testMQTTTryReadPublishV5(r, 1500*time.Millisecond); got {
		t.Fatalf("the contender delivered a message assigned to another session (payload %q)", pl)
	}
}

// ---------------------------------------------------------------------------
// The own-claim route: verify, and never re-store
// ---------------------------------------------------------------------------

// materializeQ2AffCopy's contract on an ALREADY-materialized claim is present-now,
// not present-once. Both directions are asserted, because a remedy that simply
// refuses everything would satisfy the absent half on its own.
//
// Discriminating mutation: restore the phase skip so a materialized claim is not
// verified (mutation 2). The absent subtest then acks on the phase alone and fails.
func TestQ2AffMaterializedCopyIsVerifiedNotAssumed(t *testing.T) {
	t.Parallel()

	t.Run("absent: the phase is not the proof", func(t *testing.T) {
		t.Parallel()
		b := testQ2AffSetup(t, "verifyabsent")
		c, _ := b.member(t, "verifyabsentA", 300)
		defer c.Close()
		b.bind(t, "verifyabsentA")
		moves := testQ2AffWatchMoves(t, b)

		// The winner's own claim, promoted to materialized with no copy behind it —
		// the state a deleted-and-recreated consumer leaves, and the state the phase
		// alone cannot tell from a live copy.
		seeded := testQ2AffSeedElectedClaim(t, b, func(seed *mqttQ2AffClaim) {
			seed.Phase = mqttQ2AffMaterialized
		})
		b.publish(t, 1, "verifyabsent")
		subject := testQ2AffAwaitSeeded(t, seeded)

		moves.await(t, subject, mqttQ2AffMoveDeferred, "the owner refused to release a source for a copy that is no longer there")
		moves.never(t, subject, mqttQ2AffMoveOwned, mqttQ2AffMoveTerminal)
		testQ2AffAssertNotReleased(t, b, moves, subject, mqttQ2AffMaterialized, 2)
	})

	t.Run("present: the proof is taken and the source released", func(t *testing.T) {
		t.Parallel()
		b := testQ2AffSetup(t, "verifypresent")
		c, _ := b.member(t, "verifypresentA", 300)
		defer c.Close()
		b.bind(t, "verifypresentA")
		moves := testQ2AffWatchMoves(t, b)

		// Same claim, but with a real copy on its copy subject. The owner's affine
		// consumer holds interest on its whole generation prefix, so it is retained.
		seeded := testQ2AffSeedElectedClaim(t, b, func(seed *mqttQ2AffClaim) {
			seed.Phase = mqttQ2AffMaterialized
			testQ2AffStoreCopyFor(t, b, mqttQ2AffClaimSubject(seed.GroupDurable, seed.SourceSeq), seed, "verifypresent")
		})
		b.publish(t, 1, "verifypresent")
		subject := testQ2AffAwaitSeeded(t, seeded)

		moves.await(t, subject, mqttQ2AffMoveOwned, "a copy proven present authorizes the release")
		testQ2AffAwaitClaim(t, b, subject, "source-acked", func(c *mqttQ2AffClaim) bool {
			return c == nil || c.SourceAcked
		})
	})
}

// A materialized claim whose copy is gone must not get a replacement. This is the
// defect an earlier revision of the design would have shipped: the copy under an
// AffineDone claim was consumed by its own delivery, and a replacement is a copy no
// delivery may publish — the gate disposes of it under the terminal branch, and once
// the claim is reaped it NAKs forever.
//
// Asserted on the STORE ATTEMPT, not on the copy subject afterwards, and that is the
// whole point of the fixture. A replacement copy under an `AffineDone` claim is
// disposed of by the delivery gate's terminal branch within milliseconds of landing —
// which is exactly what `AffineDone` means — so a fixture that read the copy subject
// after the move would find it absent whether or not the store ran, and discriminate
// nothing. The first version of this test did that and the mutation SURVIVED it.
//
// Discriminating mutation: let the store path run for a materialized input (mutation
// 10). The store then attempts for a claim that must never get a replacement, and this
// fixture fails on the attempt.
func TestQ2AffMaterializedAffineDoneIsNotRestored(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "norestore")
	c, _ := b.member(t, "norestoreA", 300)
	defer c.Close()
	b.bind(t, "norestoreA")
	moves := testQ2AffWatchMoves(t, b)

	// Keyed by the claim the copy names, read from its own Nmqtt-Q2Claim header, so a
	// store for some unrelated claim cannot be mistaken for a replacement of this one.
	var (
		mu     sync.Mutex
		stores = map[string]int{}
	)
	b.asm.mu.Lock()
	b.asm.testQ2AffCopyStore = func(subject string, hdrLen int, msg []byte) (uint64, error) {
		mu.Lock()
		stores[string(getHeader(mqttQ2AffClaimHeader, msg[:hdrLen]))]++
		mu.Unlock()
		return testQ2AffRealCopyStore(b, subject, hdrLen, msg)
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyStore = nil
		b.asm.mu.Unlock()
	})

	seeded := testQ2AffSeedElectedClaim(t, b, func(seed *mqttQ2AffClaim) {
		seed.Phase = mqttQ2AffMaterialized
		seed.AffineDone = true
	})
	b.publish(t, 1, "norestore")
	subject := testQ2AffAwaitSeeded(t, seeded)

	// AffineDone authorizes the release, so the move runs to completion — which is what
	// makes the absent store evidence about the STORE PATH rather than about a refusal
	// that never reached it.
	moves.await(t, subject, mqttQ2AffMoveOwned, "AffineDone authorizes releasing the source")
	// And the claim is reaped, which proves the confirmed ack committed. That matters
	// here beyond bookkeeping: an unacked source would be re-offered, a fresh election
	// would legitimately store a copy, and the count below would rise for a reason that
	// has nothing to do with the guard under test.
	testQ2AffAwaitClaim(t, b, subject, "reaped", func(cl *mqttQ2AffClaim) bool { return cl == nil })

	mu.Lock()
	n := stores[subject]
	mu.Unlock()
	if n != 0 {
		t.Fatalf("%d copy stores were attempted for the completed exchange on %q; a replacement is a copy no delivery may publish, and one whose claim has been reaped NAKs forever",
			n, subject)
	}
}

// ---------------------------------------------------------------------------
// The authorization predicate, one fixture per disjunct
// ---------------------------------------------------------------------------

// testQ2AffOwnTerminalMove stages the winner's own claim as materialized with the
// given terminal fact and no copy, then drives one real move over it. The own-claim
// route is where the predicate is consulted for a terminal fact: a foreign terminal
// claim is answered by the mover's own terminal arm and never reaches the predicate,
// and a successful materialization does not consult it at all.
func testQ2AffOwnTerminalMove(t *testing.T, name string, fact func(*mqttQ2AffClaim)) (*testQ2AffBroker, *testQ2AffMoveLog, string, string) {
	t.Helper()
	b := testQ2AffSetup(t, name)
	c, _ := b.member(t, name+"A", 300)
	t.Cleanup(func() { c.Close() })
	b.bind(t, name+"A")
	moves := testQ2AffWatchMoves(t, b)

	var copySubject atomic.Value
	seeded := testQ2AffSeedElectedClaim(t, b, func(seed *mqttQ2AffClaim) {
		seed.Phase = mqttQ2AffMaterialized
		fact(seed)
		copySubject.Store(seed.CopySubject)
	})
	b.publish(t, 1, name)
	subject := testQ2AffAwaitSeeded(t, seeded)
	cs, _ := copySubject.Load().(string)
	return b, moves, subject, cs
}

// copyPresent. Its ONLY decision point is the contender's materialized arm: on the
// own-claim route a successful materialization goes straight to the ack without
// consulting the predicate, so an own-route fixture could not fail the mutation.
//
// The winning generation belongs to a member that has left with its session intact,
// so its consumer still retains the copy — the shape a real absent winner has.
//
// Discriminating mutation: drop copyPresent from the predicate (mutation 5). The
// contender then refuses a copy it has proven present and this fixture fails.
func TestQ2AffAcksOnCopyPresent(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "ackpresent")
	ca, _ := b.member(t, "ackpresentA", 300)
	defer ca.Close()
	cb, _ := b.member(t, "ackpresentB", 300)
	b.bind(t, "ackpresentA")
	moves := testQ2AffWatchMoves(t, b)

	// B's real generation, taken from its record: a fabricated one would have no
	// consumer behind it and the copy would not be retained, so the fixture would
	// pass or fail for a reason that has nothing to do with the predicate.
	bHash := getHash("ackpresentB")
	bGen := testQ2AffAwaitStableRecord(t, b, bHash).Q2AffGen
	if bGen == _EMPTY_ {
		t.Fatal("the second member has no affine generation, so there is no consumer to retain a copy")
	}
	// B leaves; its session, record and consumer all survive. From here only A takes
	// group deliveries, so the contender arm is deterministic.
	cb.Close()

	seeded := testQ2AffSeedElectedClaim(t, b, func(seed *mqttQ2AffClaim) {
		seed.Winner, seed.Gen = bHash, bGen
		seed.CopySubject = mqttQ2AffCopySubject(bHash, bGen, seed.ClaimHash)
		_, _, seed.AffDurable = mqttQ2AffineNames(bHash, bGen)
		seed.Phase = mqttQ2AffMaterialized
		testQ2AffStoreCopyFor(t, b, mqttQ2AffClaimSubject(seed.GroupDurable, seed.SourceSeq), seed, "ackpresent")
	})
	b.publish(t, 1, "ackpresent")
	subject := testQ2AffAwaitSeeded(t, seeded)

	moves.await(t, subject, mqttQ2AffMoveForeign, "a foreign copy proven present authorizes releasing the source")
	testQ2AffAwaitClaim(t, b, subject, "source-acked", func(c *mqttQ2AffClaim) bool {
		return c == nil || c.SourceAcked
	})
}

// AffineDone. Recorded BEFORE the copy is acked, so an absent copy under it was
// consumed by its own delivery and the source is owed its release.
//
// Discriminating mutations: drop AffineDone from the predicate (mutation 6), or
// restore the early return in completeQ2AffOwnClaim (mutation 4a) — which makes
// every terminal fact unreachable on this route. Either way the source is never
// released and the claim is never reaped.
func TestQ2AffAcksOnAffineDone(t *testing.T) {
	t.Parallel()
	b, moves, subject, cs := testQ2AffOwnTerminalMove(t, "ackdone", func(seed *mqttQ2AffClaim) {
		seed.AffineDone = true
	})
	moves.await(t, subject, mqttQ2AffMoveOwned, "AffineDone authorizes releasing the source")
	// Reaped, which is stronger than SourceAcked alone: the reap requires the source
	// side PROVEN and the copy authoritatively absent, so a vanished claim is positive
	// evidence that this move's confirmed ack committed.
	testQ2AffAwaitClaim(t, b, subject, "reaped", func(c *mqttQ2AffClaim) bool { return c == nil })
	testQ2AffAssertCopyAbsent(t, b, cs, "a completed exchange's copy is gone and stays gone")
}

// Discarded. The winner's session ended and the delivery dies with it
// ([MQTT-4.8.2-5]). Required rather than merely permitted: reapable refuses without
// SourceAcked, so withholding the ack here would make every discarded claim
// permanently unreapable — the accumulation half of F-064, manufactured on purpose.
//
// Discriminating mutation: drop Discarded from the predicate (mutation 7). The claim
// is then never source-acked, so it is never reaped and this fixture fails.
func TestQ2AffAcksOnDiscarded(t *testing.T) {
	t.Parallel()
	b, moves, subject, _ := testQ2AffOwnTerminalMove(t, "ackdisc", func(seed *mqttQ2AffClaim) {
		seed.Discarded = true
	})
	moves.await(t, subject, mqttQ2AffMoveOwned, "a discarded claim's source is still released")
	testQ2AffAwaitClaim(t, b, subject, "reaped", func(c *mqttQ2AffClaim) bool { return c == nil })
}

// Lost. A QoS 2 message the broker accepted and could not prove it delivered.
// Withholding the group ack does not undo that, and the claim is terminal, so the
// source is released — while the record itself is RETAINED, because it is the only
// durable evidence that it happened.
//
// Not "never delivered": the client's PUBREC is answered before the affine side is
// recorded, so a Lost claim can name a message that in fact arrived (F-064's
// residual 1). Nothing this fixture asserts depends on which it was — the release
// and the retention are right either way — but the label is not proof of loss.
//
// Both halves are asserted. The retention half is not decoration: it is what
// separates releasing the source from laundering a compliance failure into a clean
// reap, and reapable refuses a Lost claim unconditionally for exactly that reason.
//
// Discriminating mutation: drop Lost from the predicate (mutation 8). The source is
// then never released for a legitimately lost claim and this fixture fails.
func TestQ2AffAcksOnLost(t *testing.T) {
	t.Parallel()
	b, moves, subject, _ := testQ2AffOwnTerminalMove(t, "acklost", func(seed *mqttQ2AffClaim) {
		seed.Lost = true
	})
	moves.await(t, subject, mqttQ2AffMoveOwned, "a lost claim's source is still released")
	claim := testQ2AffAwaitClaim(t, b, subject, "source-acked", func(c *mqttQ2AffClaim) bool {
		return c != nil && c.SourceAcked
	})
	if !claim.Lost || claim.AffineDone {
		t.Fatalf("the claim on %q no longer records the loss: %+v", subject, *claim)
	}
	// Retained, and stays retained: nothing may erase the evidence.
	time.Sleep(time.Second)
	if got := testQ2AffLoadClaim(t, b, subject); got == nil {
		t.Fatalf("the claim on %q was reaped; a lost delivery's only durable record must survive", subject)
	}
}

// ---------------------------------------------------------------------------
// The two callers are restructured independently, so each has its own fixture
// ---------------------------------------------------------------------------

// testQ2AffDiscardOnStore installs a copy-store seam that, once armed, writes
// Discarded to the storing claim and then returns a store that keeps nothing.
//
// This stages a terminal fact that arrives DURING the call, which is the case the
// caller's re-read exists for: the fact is not in the snapshot materializeQ2AffCopy
// was handed, so a caller that decided from its snapshot alone would refuse. The
// claim subject is read from the copy's own Nmqtt-Q2Claim header — the same header
// the delivery gate resolves the claim through — so nothing has to be threaded in.
//
// It also cannot be staged by seeding a terminal claim up front on the on-behalf
// route: a claim that is already terminal is answered by the mover's terminal arm
// and never reaches materializeQ2AffOnBehalf at all.
func testQ2AffDiscardOnStore(t *testing.T, b *testQ2AffBroker, armed *atomic.Bool) (*atomic.Bool, *atomic.Value) {
	t.Helper()
	gate := newTestHookGate(t)
	fired := &atomic.Bool{}
	captured := &atomic.Value{}
	b.asm.mu.Lock()
	b.asm.testQ2AffCopyStore = func(subject string, hdrLen int, msg []byte) (uint64, error) {
		if !armed.Load() {
			return 0, nil // keeps nothing: the claim stays at `claimed` with no copy
		}
		if !gate.enter() {
			return 0, nil
		}
		defer gate.leave()
		if fired.CompareAndSwap(false, true) {
			claimSubject := string(getHeader(mqttQ2AffClaimHeader, msg[:hdrLen]))
			if claimSubject == _EMPTY_ {
				t.Errorf("the copy on %q carries no claim header, so the fixture cannot terminalize its claim", subject)
			} else {
				captured.Store(claimSubject)
				if _, _, err := b.asm.advanceQ2AffClaim(claimSubject, _EMPTY_, _EMPTY_,
					func(c *mqttQ2AffClaim) { c.Discarded = true }); err != nil {
					t.Errorf("discarding the claim on %q mid-store: %v", claimSubject, err)
				}
			}
		}
		return 0, nil
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyStore = nil
		b.asm.mu.Unlock()
	})
	return fired, captured
}

func testQ2AffAwaitFired(t *testing.T, fired *atomic.Bool, captured *atomic.Value, what string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if fired.Load() {
			if subject, _ := captured.Load().(string); subject != _EMPTY_ {
				return subject
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s never happened, so this run staged nothing and proves nothing", what)
	return _EMPTY_
}

// The own-claim caller. A terminal fact that lands while the copy store is in flight
// must still release the source: the winner's own teardown commits Discarded before
// deleting its consumer, so this ordering is the ordinary one, not a contrivance.
//
// Discriminating mutation: restore the early return in completeQ2AffOwnClaim
// (mutation 4a). The re-read is then unreachable, the source is never released and
// the discarded claim is never reaped.
func TestQ2AffOwnClaimAcksOnTerminalFactWhenCopyGone(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "ownterm")
	c, _ := b.member(t, "owntermA", 300)
	defer c.Close()
	b.bind(t, "owntermA")
	moves := testQ2AffWatchMoves(t, b)

	armed := &atomic.Bool{}
	armed.Store(true) // the own route is reached on the first attempt
	fired, captured := testQ2AffDiscardOnStore(t, b, armed)

	b.publish(t, 1, "ownterm")
	subject := testQ2AffAwaitFired(t, fired, captured, "the mid-store discard")

	moves.await(t, subject, mqttQ2AffMoveOwned, "a terminal fact that arrived during the store still releases the source")
	testQ2AffAwaitClaim(t, b, subject, "reaped", func(cl *mqttQ2AffClaim) bool { return cl == nil })
}

// The on-behalf caller, mutated independently of the own one — which is why an
// own-claim fixture cannot stand in for this. Phase one keeps the winner's claim at
// `claimed` with no copy; phase two lets the contender enter the on-behalf store and
// terminalizes the claim inside it.
//
// Discriminating mutation: restore the early return in materializeQ2AffOnBehalf
// (mutation 4b). The contender then refuses, the source is never released and the
// discarded claim is never reaped.
func TestQ2AffOnBehalfAcksOnTerminalFactWhenCopyGone(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "obterm")
	ca, _ := b.member(t, "obtermA", 300)
	defer ca.Close()
	b.bind(t, "obtermA")
	moves := testQ2AffWatchMoves(t, b)

	// Same topology as the R2 fixture above, and for the same reason: with the winner
	// having no client here, completeQ2AffOwnClaim cannot consume the one-shot discard and
	// materializeQ2AffOnBehalf is the only route. This is mutation 4b's only pairing, so
	// the route has to be certain rather than merely likely.
	wHash, wGen := testQ2AffSyntheticWinner(t, b, "obtermW")

	armed := &atomic.Bool{}
	armed.Store(true)
	fired, captured := testQ2AffDiscardOnStore(t, b, armed)

	seeded := testQ2AffSeedElectedClaim(t, b, func(seed *mqttQ2AffClaim) {
		if seed.Winner != getHash("obtermA") {
			t.Errorf("the election came from %q, want the sole connected member: the own-claim path is reachable after all", seed.Winner)
		}
		seed.Winner, seed.Gen = wHash, wGen
		seed.CopySubject = mqttQ2AffCopySubject(wHash, wGen, seed.ClaimHash)
		seed.AffDurable = affDurableFor(wHash, wGen)
	})

	b.publish(t, 1, "obterm")
	subject := testQ2AffAwaitSeeded(t, seeded)

	if got := testQ2AffAwaitFired(t, fired, captured, "the mid-store discard on the on-behalf route"); got != subject {
		t.Fatalf("the discard landed on claim %q, not the one under test %q", got, subject)
	}
	moves.await(t, subject, mqttQ2AffMoveOnBehalf, "a terminal fact that arrived during the on-behalf store still releases the source")
	// Never the OWN route: it is unreachable by construction, so observing it would mean
	// the fixture is no longer staging what its name says.
	moves.never(t, subject, mqttQ2AffMoveOwned)
	testQ2AffAwaitClaim(t, b, subject, "reaped", func(cl *mqttQ2AffClaim) bool { return cl == nil })
}

// ---------------------------------------------------------------------------
// Fail closed on an unreadable copy
// ---------------------------------------------------------------------------

// An indeterminate copy lookup is not an absent copy and it is certainly not a
// present one. Nothing may be proven from it, so nothing is acked.
//
// Discriminating mutation: treat mqttQ2AffCopyUnknown as present (mutation 9). The
// claim then advances to materialized on an unreadable copy and the source is acked.
func TestQ2AffCopyLoadErrorRefusesSourceAck(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "copyunknown")
	c, _ := b.member(t, "copyunknownA", 300)
	defer c.Close()
	b.bind(t, "copyunknownA")
	moves := testQ2AffWatchMoves(t, b)

	// Indeterminate, not "no message found": the latter is an authoritative absence
	// and would exercise the store path instead of the fail-closed one.
	b.asm.mu.Lock()
	b.asm.testQ2AffCopyLoad = func(string) (*StoredMsg, error) {
		return nil, errors.New("the copy lookup is indeterminate")
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyLoad = nil
		b.asm.mu.Unlock()
	})

	b.publish(t, 1, "copyunknown")
	subject, _ := testQ2AffOnlyClaim(t, b)

	moves.await(t, subject, mqttQ2AffMoveDeferred, "an unreadable copy proves nothing")
	moves.never(t, subject, mqttQ2AffMoveOwned, mqttQ2AffMoveForeign, mqttQ2AffMoveOnBehalf, mqttQ2AffMoveTerminal)
	testQ2AffAssertNotReleased(t, b, moves, subject, mqttQ2AffClaimed, 2)
}

// ---------------------------------------------------------------------------
// The dependency behaviors the retry bound rests on, measured
// ---------------------------------------------------------------------------

// The two nats-server behaviors the retry analysis depends on, committed as a test
// so a dependency bump cannot silently invalidate the premise.
//
// M1, benign: the create-only expectation is checked against the current last
// message on the SUBJECT, so an absent subject accepts expected 0 however many
// no-interest stores preceded it. A retry is never rejected as a CAS conflict.
//
// M2, real: the deduplication entry outlives the message it was recorded for. A
// retry carrying the same stable msg-id inside the duplicate window is accepted and
// stores nothing — even with consumer interest present — so the presence check
// refuses again. That is the bound stated in docs/ARCHITECTURE.md: suppression lasts
// at most the stream's CONFIGURED duplicate window, and where interest never returns
// there is no bound at all.
func TestQ2AffCopyStoreWithNoInterestSequenceAndDedupe(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "measure")
	c, _ := b.member(t, "measureA", 300)
	defer c.Close()
	b.bind(t, "measureA")

	idHash := getHash("measureA")
	liveGen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if liveGen == _EMPTY_ {
		t.Fatal("the member has no affine generation, so there is no interest to measure against")
	}
	groupDur := b.groupDurable(t)

	record := func(gen string, sseq uint64, hash string) (string, []byte, int) {
		copySubject := mqttQ2AffCopySubject(idHash, gen, hash)
		env := &mqttQ2AffineEnvelope{
			natsSubject:  b.topic,
			msg:          []byte("measured"),
			claimSubject: mqttQ2AffClaimSubject(groupDur, sseq),
			gen:          gen,
			msgID:        mqttQ2AffMsgId(groupDur, sseq),
		}
		msg, hdrLen := mqttNewQ2AffineRecord(env)
		return copySubject, msg, hdrLen
	}

	t.Run("no interest: the store succeeds and keeps nothing", func(t *testing.T) {
		// A generation no session owns, so no consumer filters its prefix. This is the
		// exact condition a deleted affine consumer leaves behind.
		orphanGen := mqttNewQ2AffGen()
		copySubject, msg, hdrLen := record(orphanGen, 90001, "1111111111111111")

		seq, err := b.asm.jsa.storeMsg(copySubject, hdrLen, msg)
		if err != nil {
			t.Fatalf("an interest-retained store with no interest must SUCCEED, got: %v", err)
		}
		if seq == nil || seq.Sequence == 0 {
			t.Fatalf("the store returned no sequence (%v); the response carries one even when nothing is kept", seq)
		}
		if st, _ := b.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyAbsent {
			t.Fatalf("the copy on %q reads %v, want absent: M1's premise is that a no-interest store keeps nothing", copySubject, st)
		}
		// M1: the expectation is against the SUBJECT, which is still empty, so a retry
		// is accepted rather than rejected as a CAS conflict.
		if _, err := b.asm.jsa.storeMsg(copySubject, hdrLen, msg); err != nil {
			t.Fatalf("the retry was rejected (%v); expected-last-subject 0 must still hold on an empty subject", err)
		}
		if st, _ := b.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyAbsent {
			t.Fatalf("the copy on %q reads %v after the retry, want absent", copySubject, st)
		}
	})

	t.Run("dedupe outlives the message, so a same-id retry keeps nothing", func(t *testing.T) {
		// The LIVE generation, so interest exists and remains: this subtest is about
		// deduplication, not about retention.
		copySubject, msg, hdrLen := record(liveGen, 90002, "2222222222222222")
		if _, err := b.asm.jsa.storeMsg(copySubject, hdrLen, msg); err != nil {
			t.Fatalf("storing the copy with interest present: %v", err)
		}
		st, smsg := b.asm.loadQ2AffCopy(copySubject)
		if st != mqttQ2AffCopyPresent || smsg == nil {
			t.Fatalf("the copy on %q reads %v with interest present; this subtest cannot measure what it claims", copySubject, st)
		}
		// The message goes, the dedupe entry stays. Deleting it is the observable stand-in
		// for the two things that really remove it — a delivery's ack, or the consumer
		// being destroyed — neither of which touches the dedupe table either.
		if err := b.asm.jsa.deleteMsg(mqttOutStreamName, smsg.Sequence, true); err != nil {
			t.Fatalf("removing the copy from %s: %v", mqttOutStreamName, err)
		}
		if state, _ := b.asm.loadQ2AffCopy(copySubject); state != mqttQ2AffCopyAbsent {
			t.Fatalf("the copy on %q reads %v after the delete, want absent", copySubject, state)
		}
		// M2: the same stable msg-id, inside the window. Accepted, and nothing kept.
		if _, err := b.asm.jsa.storeMsg(copySubject, hdrLen, msg); err != nil {
			t.Fatalf("the same-id retry was rejected (%v); a duplicate is ACCEPTED, which is what makes it silent", err)
		}
		if state, _ := b.asm.loadQ2AffCopy(copySubject); state != mqttQ2AffCopyAbsent {
			t.Fatalf("the copy on %q reads %v, want absent: M2's premise is that a duplicate inside the window stores nothing", copySubject, state)
		}
		// And a DIFFERENT msg-id on the same subject is stored, which is what proves the
		// suppression above came from deduplication rather than from anything else.
		_, other, otherLen := record(liveGen, 90003, "2222222222222222")
		if _, err := b.asm.jsa.storeMsg(copySubject, otherLen, other); err != nil {
			t.Fatalf("storing a distinct-id record on the same subject: %v", err)
		}
		if state, _ := b.asm.loadQ2AffCopy(copySubject); state != mqttQ2AffCopyPresent {
			t.Fatalf("the copy on %q reads %v for a distinct msg-id, want present: the suppression was not deduplication", copySubject, state)
		}
	})
}

// ---------------------------------------------------------------------------
// Shared helper
// ---------------------------------------------------------------------------

// testQ2AffOnlyClaim waits for exactly one assignment claim to exist and returns it.
// Every fixture here publishes exactly one message, so "the claim" is unambiguous —
// and asserting the count is what keeps a fixture from silently observing a claim
// left by unrelated activity.
func testQ2AffOnlyClaim(t *testing.T, b *testQ2AffBroker) (string, *mqttQ2AffClaim) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last map[string]*mqttQ2AffClaim
	for time.Now().Before(deadline) {
		last = b.claims(t)
		if len(last) == 1 {
			for subject, claim := range last {
				return subject, claim
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected exactly one assignment claim, got %d: %s", len(last), b.dumpClaims(t))
	return _EMPTY_, nil
}
