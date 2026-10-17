// Copyright 2025 The mqtt-nats-broker Authors

package mqtt

import (
	"errors"
	"testing"
	"time"
)

// A delayed-Will ENQUEUE refused before it had any effect is RETAINED and
// RE-ENQUEUED, never published.
//
// Why a second queue rather than reusing the immediate one, which already retains a
// Will across a service-connection blip: their terminal actions are opposites. The
// immediate queue PUBLISHES on recovery, and for a delayed Will that is exactly the
// bypass the discharge path exists to close — a due one would skip the
// acceptance-fact arbitration, so a reconnect that genuinely occurred during the Will
// Delay is ignored, and a still-FUTURE one would go out before its FireAt.
//
// These fixtures need auth passthrough and a NONEMPTY service account: with the
// default configuration serviceAccountForWill returns ("", true) — it short-circuits
// when there is no dial client — so no default-account fixture can reach the account
// fence at all, and one that tried would pass against a broker with no fence.

// testDelayedRetryEnv is a real auth-passthrough broker with a nonempty service
// account, plus the account manager the enqueue writes through.
func testDelayedRetryEnv(t *testing.T) (*Server, *mqttAccountSessionManager, string) {
	t.Helper()
	_, _, _, srv := authzBrokerTrackingDials(t, authzConfOneAccount)
	account, ok := srv.serviceAccountForWill()
	if !ok || account == _EMPTY_ {
		t.Fatalf("fixture: the service account must be established and nonempty (got %q, ok=%v); "+
			"with an empty one the account fence is vacuous and this test would prove nothing", account, ok)
	}
	asm, err := srv.getOrCreateMQTTAccountSessionManager(srv.createInternalAccountClient())
	if err != nil {
		t.Fatalf("account manager: %v", err)
	}
	return srv, asm, account
}

// setServiceAccount forces what the enqueue's fence will compare against.
func setServiceAccount(srv *Server, account string, valid bool) {
	srv.svcID.mu.Lock()
	srv.svcID.account, srv.svcID.valid = account, valid
	srv.svcID.mu.Unlock()
}

func testDelayedWill(topic string) *mqttWill {
	return &mqttWill{
		topic:   []byte(topic),
		subject: []byte(topic),
		message: []byte("last words"),
		props:   &mqttProperties{willDelay: 600},
	}
}

// testWillProducerClient is a connection whose Will the ORDINARY producer
// (mqttHandleWill) will consume. Auth passthrough is on in this env, so the
// authorization decision and the destination account are the captured ones, exactly
// as a real CONNECT would leave them.
func testWillProducerClient(t *testing.T, srv *Server, asm *mqttAccountSessionManager,
	account, clientID, topic string, delaySecs uint32) *client {

	t.Helper()
	c := &client{srv: srv, acc: srv.globalAccount(), nrc: srv.nrc, mpay: 1024 * 1024}
	c.mqtt = &mqtt{asm: asm, proto: mqttProtoLevel5, pp: &mqttPublish{},
		cp: &mqttConnectProto{}, cid: clientID}
	c.mqtt.cp.will = &mqttWill{
		topic: []byte(topic), subject: []byte(topic), message: []byte("last words"),
		props: &mqttProperties{willDelay: delaySecs},
	}
	c.mqtt.willDisposition = mqttWillArmed
	c.mqtt.sessionExpiry = 3600
	// The CONNECT capture: allowed, and bound to this account.
	c.willAuthorized = true
	c.authzSvcAccount = account
	return c
}

// T9a. An unavailable destination RETAINS the enqueue THROUGH THE ORDINARY PRODUCER,
// and the ASYNCHRONOUS recovery kick re-enqueues it at the ORIGINAL FireAt.
//
// Driven end to end deliberately. An earlier version of this test called
// enqueueDelayedWill directly, built the queue entry itself and invoked the discharge
// by hand — so dropping retention from the producer, or removing the reconnect kick,
// left it passing. A fixture that constructs the state it is meant to prove the
// production path constructs is not testing that path
// (feedback_fixture_must_reach_its_route).
func TestAnUnavailableAccountRetainsADelayedWillAndReEnqueuesIt(t *testing.T) {
	srv, asm, account := testDelayedRetryEnv(t)

	// Captured at the ENQUEUE, not reloaded afterwards: a due record arms a fire timer
	// and a legitimate publication can delete it before any reload.
	enqueued := make(chan *mqttWillOutbox, 4)
	asm.mu.Lock()
	asm.testEnqueueStore = func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
		enqueued <- rec
		return asm.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
	}
	asm.mu.Unlock()

	c := testWillProducerClient(t, srv, asm, account, "retain-future", "retain/future", 600)

	// Held before the producer consumes it: mqttTakeWillLocked clears cp.will, so the
	// expected deadline has to be computed from a reference taken beforehand.
	will := c.mqtt.cp.will

	// The destination is away when the close is observed and the Will consumed.
	setServiceAccount(srv, _EMPTY_, false)
	c.mqttOweWill(3600)
	closeAt := c.mqtt.closeObservedAt
	if closeAt == 0 {
		t.Fatal("fixture: the close was not observed, so the deadline this test asserts is not anchored")
	}
	srv.mqttHandleWill(c, nil, mqttNewSessGen(), 1, asm.sessStreamEpoch)

	select {
	case rec := <-enqueued:
		t.Fatalf("the enqueue reached the store while the destination was away: %q", rec.Topic)
	default:
	}

	// THE PRODUCTION HANDOFF. Nothing in this test put it here.
	srv.owedDelayedWills.mu.Lock()
	count, held := len(srv.owedDelayedWills.owed), srv.owedDelayedWills.bytes
	var retained *mqttOwedDelayedWill
	if count == 1 {
		retained = srv.owedDelayedWills.owed[0]
	}
	srv.owedDelayedWills.mu.Unlock()
	if count != 1 || held == 0 {
		t.Fatalf("the queue holds %d entries / %d bytes after an unavailable destination, want 1 and a "+
			"nonzero charge: the producer must RETAIN rather than lose an authorized Will", count, held)
	}
	wantFireAt := mqttWillFireAt(will, 3600, closeAt, time.Now())
	if retained.fireAt != wantFireAt {
		t.Fatalf("the retained entry carries FireAt %v, want the close-anchored %v",
			time.Unix(0, retained.fireAt), time.Unix(0, wantFireAt))
	}
	if retained.account != account {
		t.Fatalf("the retained entry carries account %q, want the captured %q", retained.account, account)
	}

	// Recovery through the ASYNCHRONOUS kick, not a hand-called discharge: that is the
	// production wake-up, and removing it must be visible here.
	setServiceAccount(srv, account, true)
	srv.kickOwedDelayedWills()

	select {
	case rec := <-enqueued:
		// A vehicle that PUBLISHED would never reach the enqueue store at all, and one
		// that recomputed the deadline would arrive with a different one.
		if rec.FireAt != wantFireAt {
			t.Fatalf("the re-enqueued record carries FireAt %v, want the ORIGINAL %v: the deadline is the "+
				"arbitration input, so a retention that recomputes it reintroduces the defect anchoring it fixed",
				time.Unix(0, rec.FireAt), time.Unix(0, wantFireAt))
		}
		if rec.WillID != retained.identity.WillID {
			t.Fatalf("the record carries WillID %q, want the retained %q: a re-attempt must be the SAME "+
				"create-only CAS", rec.WillID, retained.identity.WillID)
		}
		if rec.Account != account {
			t.Fatalf("the record carries account %q, want %q", rec.Account, account)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the reconnect kick never re-enqueued the retained Will: a lost wake-up parks an " +
			"authorized Will indefinitely")
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		srv.owedDelayedWills.mu.Lock()
		left, bytes := len(srv.owedDelayedWills.owed), srv.owedDelayedWills.bytes
		srv.owedDelayedWills.mu.Unlock()
		if left == 0 && bytes == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the retained entry never left the queue after a confirmed re-enqueue: a terminal outcome " +
		"must leave and uncharge")
}

// T9b. An account MISMATCH is refused before persistence and retained under the
// AUTHORIZED account, not the current one.
func TestAnAccountMismatchBeforePersistenceRetainsUnderTheAuthorizedAccount(t *testing.T) {
	srv, asm, accountA := testDelayedRetryEnv(t)
	will := testDelayedWill("retain/mismatch")
	fireAt := time.Now().Add(10 * time.Minute).UnixNano()

	// Established, but NOT the account this Will was authorized against. The existing
	// "some account is established" check passes here, which is exactly why it is not
	// sufficient on its own: without the comparison the record would persist under B.
	setServiceAccount(srv, "some-other-account", true)
	identity := &mqttDelayedWillIdentity{}
	outcome, err := asm.enqueueDelayedWill(srv, "retain-mismatch", mqttNewSessGen(), will, nil, "alice",
		fireAt, 1, asm.sessStreamEpoch, accountA, identity)
	if outcome != mqttWillEnqueueUnavailable {
		t.Fatalf("outcome %v (err %v), want unavailable: an established account that is not the "+
			"authorized one is a destination that is unavailable TO THIS WILL", outcome, err)
	}

	// Nothing was persisted, under either account.
	if rec, _, _ := asm.jsa.loadWillOutbox(asm.domainTk,
		mqttWillQClientFilter(asm.domainTk, identity.CIDHash)); rec != nil {
		t.Fatalf("a record was persisted under account %q despite the mismatch: the fence must refuse "+
			"BEFORE any enqueue effect, or retention is unsafe", rec.Account)
	}

	o := &mqttOwedDelayedWill{
		as: asm, identity: *identity, clientID: "retain-mismatch", sessGen: mqttNewSessGen(),
		will: will, user: "alice", fireAt: fireAt, account: accountA,
		originSessSeq: 1, originSessEpoch: asm.sessStreamEpoch,
	}
	if !srv.retainDelayedWillEnqueue(o) {
		t.Fatal("retention must be admitted")
	}
	if o.account != accountA {
		t.Fatalf("the retained entry carries account %q, want the AUTHORIZED %q", o.account, accountA)
	}

	// Recovery under A discharges it.
	setServiceAccount(srv, accountA, true)
	srv.dischargeOwedDelayedWills()
	rec, _, lerr := asm.jsa.loadWillOutbox(asm.domainTk,
		mqttWillQClientFilter(asm.domainTk, identity.CIDHash))
	if lerr != nil || rec == nil {
		t.Fatalf("no record after recovery under the authorized account (err %v)", lerr)
	}
	if rec.Account != accountA {
		t.Fatalf("the record carries account %q, want %q", rec.Account, accountA)
	}
}

// T9c. Recovery under the WRONG account discharges nothing, and the entry stays
// charged — the original account may yet return, so each entry is asked
// individually rather than the pass giving up or giving in.
func TestRecoveryUnderTheWrongAccountDoesNotDischargeARetainedWill(t *testing.T) {
	srv, asm, accountA := testDelayedRetryEnv(t)
	will := testDelayedWill("retain/wrong")
	fireAt := time.Now().Add(10 * time.Minute).UnixNano()
	const accountB = "the-authorized-one"

	// Authorized for B, retained while the current account is A.
	setServiceAccount(srv, accountA, true)
	identity := &mqttDelayedWillIdentity{WillID: "wrongacct-1", CIDHash: getHash("retain-wrong")}
	outcome, _ := asm.enqueueDelayedWill(srv, "retain-wrong", mqttNewSessGen(), will, nil, "alice",
		fireAt, 1, asm.sessStreamEpoch, accountB, identity)
	if outcome != mqttWillEnqueueUnavailable {
		t.Fatalf("outcome %v, want unavailable for a Will authorized against %q while %q is current",
			outcome, accountB, accountA)
	}
	o := &mqttOwedDelayedWill{
		as: asm, identity: *identity, clientID: "retain-wrong", sessGen: mqttNewSessGen(),
		will: will, user: "alice", fireAt: fireAt, account: accountB,
		originSessSeq: 1, originSessEpoch: asm.sessStreamEpoch,
	}
	if !srv.retainDelayedWillEnqueue(o) {
		t.Fatal("retention must be admitted")
	}
	srv.owedDelayedWills.mu.Lock()
	chargedBefore := srv.owedDelayedWills.bytes
	srv.owedDelayedWills.mu.Unlock()

	// A pass while A — not B — is current. This is the "return" that is not this
	// entry's return.
	setServiceAccount(srv, accountA, true)
	srv.dischargeOwedDelayedWills()

	srv.owedDelayedWills.mu.Lock()
	count, charged := len(srv.owedDelayedWills.owed), srv.owedDelayedWills.bytes
	srv.owedDelayedWills.mu.Unlock()
	if count != 1 {
		t.Fatalf("the queue holds %d entries, want 1: a pass under a DIFFERENT account must discharge "+
			"nothing, because the authorized one may yet return", count)
	}
	if charged != chargedBefore {
		t.Fatalf("the charge moved from %d to %d without the entry leaving", chargedBefore, charged)
	}
	if rec, _, _ := asm.jsa.loadWillOutbox(asm.domainTk,
		mqttWillQClientFilter(asm.domainTk, identity.CIDHash)); rec != nil {
		t.Fatalf("a record was written under %q for a Will authorized against %q", rec.Account, accountB)
	}
}

// The capacity bound is real, separate from the immediate queue's, and charged
// exactly — a refusal is a reported loss rather than a silent one.
func TestDelayedWillRetentionBoundIsSeparateAndCharged(t *testing.T) {
	srv, asm, account := testDelayedRetryEnv(t)
	setServiceAccount(srv, _EMPTY_, false)

	// Separate budgets so a delayed backlog cannot consume the immediate queue's
	// admission capacity, which would let a Will Delay outage starve the Wills that
	// are due now.
	srv.owedWills.mu.Lock()
	immediateBefore := srv.owedWills.bytes
	srv.owedWills.mu.Unlock()

	payload := make([]byte, 512<<10)
	admitted, offered := 0, 0
	for i := 0; i < 16; i++ {
		will := testDelayedWill("bound/will")
		will.message = payload
		offered += len(payload) + len(will.topic)
		o := &mqttOwedDelayedWill{
			as: asm, identity: mqttDelayedWillIdentity{WillID: mqttNewSessGen(), CIDHash: getHash("bound")},
			clientID: "bound", will: will, account: account,
		}
		if srv.retainDelayedWillEnqueue(o) {
			admitted++
		}
	}

	srv.owedDelayedWills.mu.Lock()
	held, count, dropped := srv.owedDelayedWills.bytes, len(srv.owedDelayedWills.owed), srv.owedDelayedWills.dropped
	srv.owedDelayedWills.mu.Unlock()

	if held > mqttDelayedWillMaxBytes {
		t.Fatalf("the queue holds %d bytes, past its %d bound: capacity was handed out against entries "+
			"still occupying it", held, mqttDelayedWillMaxBytes)
	}
	if dropped == 0 {
		t.Fatalf("the fixture offered %d bytes against a %d bound and nothing was refused, so the "+
			"bound was never exercised", offered, mqttDelayedWillMaxBytes)
	}
	if count != admitted {
		t.Fatalf("the queue holds %d entries but %d retentions reported success: the charge and the "+
			"contents disagree", count, admitted)
	}

	srv.owedWills.mu.Lock()
	immediateAfter := srv.owedWills.bytes
	srv.owedWills.mu.Unlock()
	if immediateAfter != immediateBefore {
		t.Fatalf("the immediate queue's charge moved from %d to %d while only delayed Wills were "+
			"retained: the budgets must be independent", immediateBefore, immediateAfter)
	}
}

// THE CENTRAL ORDERING OF THE PER-EXIT CONTRACT, and the reason it is a precedence
// table over exits rather than a taxonomy of error values.
//
// Schedule: the create-only CAS keeps failing, so the attempts are exhausted; the
// record may or may not have landed, because a create can succeed while its
// acknowledgement is lost; and the fail-safe publication then refuses because the
// destination account has gone away, returning errMQTTWillDestinationUnavailable.
//
// That is the SAME sentinel a pre-effect refusal returns. Classifying on it would
// send this obligation back into retention — and a re-attempt would be a fresh
// create against a subject whose record may already exist, which is two records for
// one Will and, if both survive, two publications. The exit decides instead: this
// one is TERMINAL, no retry and no second publication, whatever the error says.
func TestAnExhaustedEnqueueWhoseFailSafeRefusesIsTerminalNotRetained(t *testing.T) {
	srv, asm, account := testDelayedRetryEnv(t)
	setServiceAccount(srv, account, true)

	// Every enqueue CAS fails, so the attempts exhaust and the fail-safe runs.
	asm.mu.Lock()
	asm.testEnqueueStore = func(string, uint64, *mqttWillOutbox) (uint64, error) {
		return 0, errMQTTWillDestinationUnavailable
	}
	// And the fail-safe publication refuses with the DESTINATION sentinel — the
	// value a retainable refusal also carries.
	asm.testPublishWill = func(*mqttWillOutbox) error { return errMQTTWillDestinationUnavailable }
	asm.mu.Unlock()

	identity := &mqttDelayedWillIdentity{}
	outcome, err := asm.enqueueDelayedWill(srv, "exhausted", mqttNewSessGen(), testDelayedWill("exhaust/will"),
		nil, "alice", time.Now().Add(10*time.Minute).UnixNano(), 1, asm.sessStreamEpoch, account, identity)

	if outcome != mqttWillEnqueueUnconfirmedAbandoned {
		t.Fatalf("outcome %v (err %v), want unconfirmed-abandoned: this is the exhaustion exit whose "+
			"fail-safe also failed", outcome, err)
	}
	if err == nil {
		t.Fatal("the abandoned exit must report why")
	}
	// The assertion this test exists for. The error IS the retainable one; the exit
	// is not.
	if !errorsIsWillDestinationUnavailable(err) {
		t.Fatalf("fixture: the error %v is not the destination sentinel, so this test does not "+
			"exercise the collision between a retainable ERROR and a terminal EXIT", err)
	}
	if outcome.retainable() {
		t.Fatal("an exhausted enqueue whose fail-safe refused the destination must NOT be retainable: " +
			"a create may already have landed, so re-attempting it would write a second record for one Will")
	}
}

func errorsIsWillDestinationUnavailable(err error) bool {
	return err != nil && errors.Is(err, errMQTTWillDestinationUnavailable)
}

// A DUE delayed Will is ARBITRATED through the outbox, not published directly.
//
// The direct-publish arm used to take any Will that was due, on the reasoning that a
// reconnect arriving after the deadline must not suppress an already-owed Will. That
// is right about the rule and wrong about who enforces it: an elapsed delay does not
// extinguish a reconnect's entitlement, it decides WHICH reconnects qualify — and
// only the acceptance-fact scan can tell, because the qualifying reconnect may have
// happened on a broker this one never observed. Publishing directly skipped that scan
// entirely.
//
// Anchoring the deadline to the close makes the bypass MORE reachable, since more
// deadlines are already past by the time a parked action discharges, which is why the
// two changes belong to one item.
func TestADueDeferredDelayedWillIsEnqueuedNotPublishedDirectly(t *testing.T) {
	srv, asm, account := testDelayedRetryEnv(t)
	setServiceAccount(srv, account, true)

	sess := &mqttSession{id: "due-deferred", idHash: getHash("due-deferred")}
	will := testDelayedWill("due/deferred")
	// OBSERVED AT THE ENQUEUE, and only there.
	//
	// A due record arms an immediate fire timer, so a LEGITIMATE publication by the
	// Will worker follows within milliseconds — and it is indistinguishable, at a
	// publish hook, from the direct publication this test is hunting. An earlier
	// version watched testPublishWill for "any publication" and would have failed
	// correct code whenever the timer won that race; it also claimed to hold the
	// claimant and did not. Reloading the record afterwards is no better: the same
	// publication DELETES it, so absence proves neither a bypass nor a loss.
	//
	// The enqueue store is the one observation the machinery cannot race, and it is
	// sufficient on its own: under the removed due-term the direct arm runs and
	// NOTHING reaches the store, so its absence is the failure.
	enqueued := make(chan *mqttWillOutbox, 4)
	asm.mu.Lock()
	asm.testEnqueueStore = func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
		enqueued <- rec
		return asm.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
	}
	asm.mu.Unlock()

	action := &mqttDeferredDisconnect{
		will: will, willUser: "alice", willAccount: account, willDelayed: true,
		// ALREADY PAST. This is the population the removed term diverted.
		willDeadline:   time.Now().Add(-1 * time.Minute).UnixNano(),
		willGen:        mqttNewSessGen(),
		willSessEpoch:  asm.sessStreamEpoch,
		willSessionSeq: 1,
	}
	srv.dischargeDeferredWill(asm, sess, action)

	select {
	case rec := <-enqueued:
		if rec.FireAt != action.willDeadline {
			t.Fatalf("the enqueued record carries FireAt %v, want the action's ORIGINAL %v",
				time.Unix(0, rec.FireAt), time.Unix(0, action.willDeadline))
		}
		if rec.Account != account {
			t.Fatalf("the enqueued record carries account %q, want the captured %q", rec.Account, account)
		}
	default:
		t.Fatal("a due delayed Will never reached the outbox: it was published DIRECTLY, which skips the " +
			"acceptance-fact scan, so a reconnect during the Will Delay on another broker is ignored")
	}
}

// THE LOST-WAKEUP WINDOW, forced with a BARRIER rather than raced.
//
// The window is inside one retention call: the reconnect that would discharge this
// obligation can complete BETWEEN the refusal that produced it and its insertion into
// the queue, and a discharge running in that window sees an EMPTY queue. Asking again
// after inserting is what closes it.
//
// Ordering, and every step is load-bearing: the destination is away when the enqueue
// is refused, so an obligation exists; it comes BACK and a full discharge pass runs to
// completion against an empty queue, which is the wake-up this entry misses; only then
// is the entry inserted. Nothing kicks afterwards. If the insertion does not ask again,
// nothing ever will, and the Will is parked indefinitely.
func TestRetentionInsertedAfterRecoveryIsStillDischarged(t *testing.T) {
	srv, asm, account := testDelayedRetryEnv(t)

	enqueued := make(chan *mqttWillOutbox, 4)
	asm.mu.Lock()
	asm.testEnqueueStore = func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
		enqueued <- rec
		return asm.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
	}
	asm.mu.Unlock()

	// The refusal that produces the obligation, while the destination is away.
	setServiceAccount(srv, _EMPTY_, false)
	identity := &mqttDelayedWillIdentity{}
	will := testDelayedWill("lostwake/will")
	fireAt := time.Now().Add(10 * time.Minute).UnixNano()
	outcome, _ := asm.enqueueDelayedWill(srv, "lostwake", mqttNewSessGen(), will, nil, "alice",
		fireAt, 1, asm.sessStreamEpoch, account, identity)
	if outcome != mqttWillEnqueueUnavailable {
		t.Fatalf("outcome %v, want unavailable: this test needs a retainable refusal", outcome)
	}

	// THE BARRIER. The destination returns and a whole discharge pass completes before
	// the insertion — against an empty queue, so it discharges nothing. This is the
	// wake-up the entry below will never receive.
	setServiceAccount(srv, account, true)
	srv.dischargeOwedDelayedWills()

	// Inserted only now, and nothing kicks after it.
	if !srv.retainDelayedWillEnqueue(&mqttOwedDelayedWill{
		as: asm, identity: *identity, clientID: "lostwake", sessGen: mqttNewSessGen(),
		will: will, user: "alice", fireAt: fireAt, account: account,
		originSessSeq: 1, originSessEpoch: asm.sessStreamEpoch,
	}) {
		t.Fatal("retention must be admitted")
	}

	select {
	case rec := <-enqueued:
		if rec.WillID != identity.WillID {
			t.Fatalf("re-enqueued %q, want the retained %q", rec.WillID, identity.WillID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the entry was never discharged: the recovery pass completed before the insertion, so " +
			"only the insertion's own recheck-and-kick could have closed the window — and it did not")
	}
}

// The OWED-ZERO disposition, driven through the ORDINARY PRODUCER.
//
// An owed Will that reaches a producer with no close observation is an invariant
// failure, and the producer enqueues it DUE NOW — not now + D. It is already owed, so
// granting a fresh full delay would hand back a suppression window it has already
// spent, on top of the one already lost.
//
// Driven through mqttHandleWill rather than through the arithmetic helper, because
// the helper's zero means "armed, so now is the anchor" and ADDS the delay — right for
// an armed Will, wrong for this one. Only the producer can be caught confusing them.
func TestAnOwedWillWithNoAnchorIsEnqueuedDueNowByTheProducer(t *testing.T) {
	srv, asm, account := testDelayedRetryEnv(t)
	setServiceAccount(srv, account, true)

	enqueued := make(chan *mqttWillOutbox, 4)
	asm.mu.Lock()
	asm.testEnqueueStore = func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
		enqueued <- rec
		return asm.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
	}
	asm.mu.Unlock()

	const delaySecs = 600
	c := testWillProducerClient(t, srv, asm, account, "owed-zero", "owed/zero", delaySecs)

	// OWED, with the observation missing: the invariant this disposition answers.
	c.mqttOweWill(3600)
	c.mqtt.closeObservedAt = 0

	lo := time.Now().UnixNano()
	srv.mqttHandleWill(c, nil, mqttNewSessGen(), 1, asm.sessStreamEpoch)
	hi := time.Now().UnixNano()

	select {
	case rec := <-enqueued:
		if rec.FireAt < lo || rec.FireAt > hi {
			t.Fatalf("FireAt %v is outside the producer call [%v, %v]: an owed Will with no anchor must "+
				"be enqueued DUE NOW", time.Unix(0, rec.FireAt), time.Unix(0, lo), time.Unix(0, hi))
		}
		// And decisively NOT a restarted delay, which is the mistake this asserts against.
		if restarted := time.Unix(0, lo).Add(delaySecs * time.Second).UnixNano(); rec.FireAt >= restarted {
			t.Fatalf("FireAt %v is at or past a RESTARTED delay (%v): the producer granted a fresh Will "+
				"Delay to a Will that was already owed", time.Unix(0, rec.FireAt), time.Unix(0, restarted))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the owed Will never reached the outbox")
	}
}

// The DEFERRED producer anchors its deadline to the close too.
//
// This is the path with the most lateness in it — a parked action waits out an
// ambiguous record resolution before it discharges — so it is the one where measuring
// from the consumption is worst. The ordinary producer's fixture cannot cover it: the
// two compute their deadlines in different functions.
func TestADeferredParkAnchorsItsWillDeadlineToTheClose(t *testing.T) {
	srv, asm, account := testDelayedRetryEnv(t)
	setServiceAccount(srv, account, true)

	sess := &mqttSession{id: "parked-anchor", idHash: getHash("parked-anchor"),
		gen: mqttNewSessGen(), seq: 1, expiryInterval: 3600}
	// UNCERTAIN, so the park is accepted and the action is retained for inspection.
	sess.seqUncertain = true

	const delaySecs = 600
	c := testWillProducerClient(t, srv, asm, account, "parked-anchor", "parked/will", delaySecs)
	c.mqtt.sess = sess
	// BOUND to the closing connection, as every production ungated park is (the teardown
	// reaches it only while sess.c == c): the park's commit refuses, taking nothing, once
	// the binding has moved.
	sess.c = c
	c.mqttOweWill(3600)
	closeAt := c.mqtt.closeObservedAt
	if closeAt == 0 {
		t.Fatal("fixture: no close observation, so there is no anchor to assert")
	}

	// A gap between the close and the park, which is what the two candidate anchors
	// disagree about. Without it the assertion would hold for either.
	time.Sleep(30 * time.Millisecond)

	if parked := srv.mqttDeferDisconnectEx(c, asm, sess, true); !parked {
		t.Fatal("fixture: the park was refused, so no action was built")
	}

	sess.mu.Lock()
	action := sess.deferredDisconnect
	sess.mu.Unlock()
	if action == nil || !action.willDelayed {
		t.Fatalf("fixture: parked action %v delayed=%v", action != nil, action != nil && action.willDelayed)
	}

	want := time.Unix(0, closeAt).Add(delaySecs * time.Second).UnixNano()
	if action.willDeadline != want {
		t.Fatalf("the parked action carries willDeadline %v, want the close-anchored %v: the deferred "+
			"producer is the LATEST-consuming route there is, so measuring from its own consumption "+
			"restarts the delay by the whole park interval",
			time.Unix(0, action.willDeadline), time.Unix(0, want))
	}
	if action.willAccount != account {
		t.Fatalf("the parked action carries account %q, want the captured %q", action.willAccount, account)
	}
}

// A REFUSED deferred-disconnect park leaves the Will, its disposition and its anchor
// exactly where they were, THROUGH mqttDeferDisconnectEx.
//
// The park used to take the Will first and roll all three back on refusal; the take now
// happens only at the park's commit (parkClose), so a refusal takes nothing. Either way
// the ordinary teardown that follows must find an owed-delayed Will WITH its
// observation: without it no later mqttOweWill can repair the anchor, because the owed
// disposition makes that call a no-op, and the teardown would measure the delay from its
// own consumption — a silent reversion to the defect, on the path that already has the
// most lateness.
//
// Driven through the production function (feedback_fixture_must_reach_its_route).
func TestARefusedParkRestoresTheCloseObservation(t *testing.T) {
	srv, asm, account := testDelayedRetryEnv(t)
	setServiceAccount(srv, account, true)

	sess := &mqttSession{id: "refused-park", idHash: getHash("refused-park"),
		gen: mqttNewSessGen(), seq: 1, expiryInterval: 3600}
	// NOT uncertain, so the ungated park's commit refuses: the "resolved while the action
	// was being built" case.
	sess.seqUncertain = false

	c := testWillProducerClient(t, srv, asm, account, "refused-park", "park/will", 600)
	c.mqtt.sess = sess
	// Bound, so the refusal is the RESOLVED one this test is about rather than the
	// binding refusal.
	sess.c = c
	c.mqttOweWill(3600)
	stamped := c.mqtt.closeObservedAt
	if stamped == 0 || c.mqtt.willDisposition != mqttWillOwedDelayed {
		t.Fatalf("fixture: disposition %v anchor %d; this test needs an owed-delayed Will with an anchor",
			c.mqtt.willDisposition, stamped)
	}

	// The last argument is passed straight through as deferDisconnectIfUncertain's
	// requireUncertain, so true plus a resolved session is the refusal branch.
	if parked := srv.mqttDeferDisconnectEx(c, asm, sess, true); parked {
		t.Fatal("fixture: the park was ACCEPTED, so the refusal this test exists for never ran")
	}

	c.mu.Lock()
	afterWill, afterDisp, afterAnchor := c.mqtt.cp.will, c.mqtt.willDisposition, c.mqtt.closeObservedAt
	c.mu.Unlock()

	if afterWill == nil || afterDisp != mqttWillOwedDelayed {
		t.Fatalf("after the refusal will=%v disposition=%v, want the Will in place and owed-delayed",
			afterWill != nil, afterDisp)
	}
	if afterAnchor != stamped {
		t.Fatalf("after the refusal the anchor is %d, want the original %d: without it the teardown that "+
			"follows measures the Will Delay from its OWN consumption, which is the defect this item fixed",
			afterAnchor, stamped)
	}

	// And the repair a lost anchor would need is NOT available: the disposition is owed,
	// so mqttOweWill is a no-op. That is why a refusal must leave the anchor in place
	// rather than leave it to a later observation.
	c.mu.Lock()
	c.mqtt.closeObservedAt = 0
	c.mu.Unlock()
	c.mqttOweWill(3600)
	if c.mqtt.closeObservedAt != 0 {
		t.Fatal("mqttOweWill re-stamped an OWED Will, so a lost anchor would be self-healing and " +
			"this property would not be load-bearing — check the no-op-unless-armed guard")
	}
}

// THE ITEM'S HEADLINE PROPERTY, arbitrated against a CONTROLLED acceptance timestamp
// on both sides of the close-anchored deadline.
//
// The suppression predicate is `At < FireAt` (mqttAcceptFactQualifies). Anchoring
// FireAt to the close is what makes that comparison mean what MQTT says: a reconnect
// DURING the Will Delay suppresses [MQTT-3.1.3-9]; one after the delay has elapsed
// does not, because the Will is already owed [MQTT-3.1.2-8]. Under the old
// consumption anchor the deadline was inflated by the whole teardown gap, so an
// acceptance in that extra slice — one that arrived after the true delay had run out —
// still qualified and cancelled a Will the client was owed.
//
// The fact's timestamp is PLANTED rather than produced by a real CONNECT. A barrier
// orders events; it cannot guarantee a CONNECT's acceptance lands inside a future
// wall-clock window, and an earlier draft of this fixture depended on exactly that.
// Both inequalities here hold by construction.
func TestSuppressionIsDatedByTheCloseAnchoredDeadline(t *testing.T) {
	const delaySecs = 600

	run := func(t *testing.T, name string, offset time.Duration, wantSuppressed bool) {
		t.Helper()
		srv, asm, account := testDelayedRetryEnv(t)
		setServiceAccount(srv, account, true)

		enqueued := make(chan *mqttWillOutbox, 4)
		asm.mu.Lock()
		asm.testEnqueueStore = func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
			enqueued <- rec
			return asm.jsa.storeWillOutboxCAS(subject, expectSeq, rec)
		}
		asm.mu.Unlock()

		clientID := "dated-" + name
		c := testWillProducerClient(t, srv, asm, account, clientID, "dated/"+name, delaySecs)
		c.mqttOweWill(3600)
		closeAt := c.mqtt.closeObservedAt
		if closeAt == 0 {
			t.Fatal("fixture: no close observation")
		}
		// The acceptance is planted FIRST, at a chosen instant relative to the deadline
		// the producer is about to derive, and the producer is then given an origin
		// sequence strictly BELOW the fact's — so every other term of the predicate
		// qualifies and only the DATE decides. Ordering it the other way put the fact
		// at or below the origin and the sequence term did the disqualifying, which the
		// fixture's own guard caught.
		wantFireAt := time.Unix(0, closeAt).Add(delaySecs * time.Second).UnixNano()

		// A throwaway fact for ANOTHER client first, purely to advance the stream so
		// the real one lands above sequence 1. The producer's origin sequence must be
		// NONZERO -- the enqueue refuses a zero one and fail-safes instead -- and it
		// must also be strictly below the real fact's, so there has to be room between.
		testPlantAcceptFact(t, asm, "sequence-filler", 1)

		at := wantFireAt + int64(offset)
		_, factSeq := testPlantAcceptFact(t, asm, clientID, at)
		if factSeq < 2 {
			t.Fatalf("fixture: the planted fact sits at %d, leaving no nonzero origin below it", factSeq)
		}

		srv.mqttHandleWill(c, nil, mqttNewSessGen(), factSeq-1, asm.sessStreamEpoch)

		var rec *mqttWillOutbox
		select {
		case rec = <-enqueued:
		case <-time.After(10 * time.Second):
			t.Fatal("the Will never reached the outbox")
		}

		// The deadline the PRODUCER derived, not one the test computed: the point is
		// that arbitration is dated by what the broker actually wrote.
		if rec.FireAt != wantFireAt {
			t.Fatalf("the record carries FireAt %v, want the close-anchored %v",
				time.Unix(0, rec.FireAt), time.Unix(0, wantFireAt))
		}
		if factSeq <= rec.OriginSessionSeq {
			t.Fatalf("fixture: the fact sits at %d, not above the record's origin %d, so the sequence "+
				"term would disqualify it and the DATE would not be what decides",
				factSeq, rec.OriginSessionSeq)
		}

		got := mqttAcceptFactQualifies(&mqttAcceptFact{
			ClientID: clientID, SessEpoch: asm.sessStreamEpoch, AcceptID: mqttNewAcceptID(), At: at,
		}, factSeq, asm.sessStreamEpoch, rec)
		if got != wantSuppressed {
			t.Fatalf("an acceptance at %v (offset %v from FireAt %v) qualifies=%v, want %v",
				time.Unix(0, at), offset, time.Unix(0, rec.FireAt), got, wantSuppressed)
		}
	}

	t.Run("a reconnect DURING the delay suppresses", func(t *testing.T) {
		// [MQTT-3.1.3-9]. Strictly before the close-anchored deadline.
		run(t, "during", -time.Second, true)
	})

	t.Run("a reconnect AFTER the delay does not", func(t *testing.T) {
		// [MQTT-3.1.2-8]: the Will was already owed. Under the OLD consumption anchor
		// this same acceptance could still fall below an inflated deadline and wrongly
		// cancel the Will -- which is the defect this item fixes.
		run(t, "after", time.Second, false)
	})
}

// The DEFERRED producer retains an unavailable destination too, under its CAPTURED
// account.
//
// Its handoff is a different one from the ordinary producer's: the action carries
// willAccount from the park, and the enqueue must be given that rather than
// resampling the current service account. A fixture that only ever ran with a
// matching, available account could not tell a producer that dropped retention, or
// one that re-bound the Will to whatever account the broker points at now.
func TestTheDeferredProducerRetainsAnUnavailableDestinationUnderItsCapturedAccount(t *testing.T) {
	srv, asm, account := testDelayedRetryEnv(t)

	sess := &mqttSession{id: "deferred-retain", idHash: getHash("deferred-retain")}
	will := testDelayedWill("deferred/retain")
	fireAt := time.Now().Add(10 * time.Minute).UnixNano()
	action := &mqttDeferredDisconnect{
		will: will, willUser: "alice", willAccount: account, willDelayed: true,
		willDeadline: fireAt, willGen: mqttNewSessGen(),
		willSessEpoch: asm.sessStreamEpoch, willSessionSeq: 1,
	}

	// Away at discharge.
	setServiceAccount(srv, _EMPTY_, false)
	srv.dischargeDeferredWill(asm, sess, action)

	srv.owedDelayedWills.mu.Lock()
	count := len(srv.owedDelayedWills.owed)
	var retained *mqttOwedDelayedWill
	if count == 1 {
		retained = srv.owedDelayedWills.owed[0]
	}
	srv.owedDelayedWills.mu.Unlock()
	if count != 1 {
		t.Fatalf("the queue holds %d entries after an unavailable destination, want 1: the DEFERRED "+
			"producer must retain rather than lose an authorized Will", count)
	}
	if retained.account != account {
		t.Fatalf("the retained entry carries account %q, want the action's CAPTURED %q: resampling the "+
			"current account re-binds the Will to whatever the broker points at now",
			retained.account, account)
	}
	if retained.fireAt != fireAt {
		t.Fatalf("the retained entry carries FireAt %v, want the action's %v",
			time.Unix(0, retained.fireAt), time.Unix(0, fireAt))
	}
}

// The ORDINARY producer retains an account MISMATCH too, not just an unestablished
// account. The established-but-wrong case is the one a "some account is established"
// check lets through.
func TestTheOrdinaryProducerRetainsAnAccountMismatch(t *testing.T) {
	srv, asm, account := testDelayedRetryEnv(t)

	c := testWillProducerClient(t, srv, asm, account, "producer-mismatch", "mismatch/will", 600)

	// Established, and NOT the captured one.
	setServiceAccount(srv, "a-different-account", true)
	c.mqttOweWill(3600)
	srv.mqttHandleWill(c, nil, mqttNewSessGen(), 1, asm.sessStreamEpoch)

	srv.owedDelayedWills.mu.Lock()
	count := len(srv.owedDelayedWills.owed)
	var retained *mqttOwedDelayedWill
	if count == 1 {
		retained = srv.owedDelayedWills.owed[0]
	}
	srv.owedDelayedWills.mu.Unlock()
	if count != 1 {
		t.Fatalf("the queue holds %d entries after an account MISMATCH, want 1: an established account "+
			"that is not the authorized one is a destination unavailable TO THIS WILL", count)
	}
	if retained.account != account {
		t.Fatalf("the retained entry carries account %q, want the captured %q", retained.account, account)
	}
	if rec, _, _ := asm.jsa.loadWillOutbox(asm.domainTk,
		mqttWillQClientFilter(asm.domainTk, retained.identity.CIDHash)); rec != nil {
		t.Fatalf("a record was persisted under %q despite the mismatch: the fence must refuse BEFORE "+
			"any enqueue effect, or retention is unsafe", rec.Account)
	}
}
