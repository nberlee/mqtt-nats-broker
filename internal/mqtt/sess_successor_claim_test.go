// Copyright 2024 The mqtt-nats-bridge Authors
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// F-084's fixtures: the broker may claim a takeover only where it holds the successor,
// and every record-derived disposal owes the displaced session's Will.
//
// The claim half is covered where each producer already lives (sess_fork_connect_test.go,
// sess_persist_evict_test.go, shared_group_arbitrate_test.go, sess_rejected_write_test.go).
// What lives here is the WILL half — the five routes by which a Will used to be lost, and
// the licence that separates the one disposal allowed to suppress one from the rest.

// testPlantDeferredAction parks a deferred disconnect carrying a Will, which is the state a
// connection leaves behind when it closes while its record write is unresolved.
//
// Every field the discharge reads is set explicitly, because a zero value in any of them
// changes which branch the discharge takes: willDeadline in the past is "already due" and
// publishes directly, willDeadline in the future enqueues on the outbox.
func testPlantDeferredAction(t *testing.T, e *testRejectionEnv, delayed bool) *mqttDeferredDisconnect {
	t.Helper()
	e.sess.mu.Lock()
	gen, seq := e.sess.gen, e.sess.seq
	e.sess.mu.Unlock()

	// UnixNANO, which is what willDue compares against. Seconds here would read as a
	// deadline decades in the past and send every row down the already-due branch.
	deadline := time.Now().Add(-time.Minute).UnixNano()
	if delayed {
		deadline = time.Now().Add(time.Hour).UnixNano()
	}
	action := &mqttDeferredDisconnect{
		disconnectedAt: time.Now().Add(-time.Minute).Unix(),
		expiryInterval: mqttSessionNeverExpire,
		willDeadline:   deadline,
		willDelayed:    delayed,
		willSessionSeq: seq,
		willSessEpoch:  e.asm.sessStreamEpoch,
		willGen:        gen,
		will: &mqttWill{
			topic:   []byte("will/topic"),
			subject: []byte("will.topic"),
			message: []byte("parked-will"),
		},
	}
	e.sess.mu.Lock()
	e.sess.deferredDisconnect = action
	e.sess.mu.Unlock()
	return action
}

// TestF084ForeignDisposalDischargesParkedWill is R2.
//
// disposeForeignStateLocked used to call takeDeferredDisconnect() and DROP the action on
// the floor: a connection that closed while its record write was unresolved parked its
// Will there, and a foreign disposal arriving afterwards destroyed it without publishing
// it and without any record of having done so.
//
// The assertion is POSITIVE — the Will reaches the wire — because "no publication" is what
// the broken behavior also produces.
func TestF084ForeignDisposalDischargesParkedWill(t *testing.T) {
	e := testNewRejectionEnv(t, "f084r2")
	watch := e.watchDelivery(t, "will/topic")
	action := testPlantDeferredAction(t, e, false)

	// No bound client: this is the unbound foreign disposal, which is exactly the shape
	// that used to lose the parked action.
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.mu.Unlock()

	e.sess.disposeForeignSession(nil, mqttDisposalOwnershipUnsettled)

	if msg := e.deliveryBarrier(t, watch, "will/topic"); string(msg.Data) != "parked-will" {
		t.Fatalf("a foreign disposal must DISCHARGE the parked action's Will, not drop it: "+
			"the watcher saw %q first", msg.Data)
	}
	// And the action is consumed exactly once: leaving it parked on a deresidented object
	// would strand it where no pass can reach it.
	e.sess.mu.Lock()
	still := e.sess.deferredDisconnect
	e.sess.mu.Unlock()
	if still != nil {
		t.Fatalf("the discharged action must be consumed, still parked: %+v", still)
	}
	if action.will == nil {
		t.Fatal("fixture: the planted action must carry a Will")
	}
}

// TestF084ForeignDisposalDischargeWritesNoRecord pins the WILL-ONLY property of
// dischargeDeferredWill, which is why applyDeferredDisconnect could not be reused.
//
// That helper applies the whole deferred teardown — clear or save, expiry scheduling,
// shared-pending purge — and takes the record fence. From a foreign disposal all three are
// wrong: the record has just been classified not-ours so there is nothing to write, the
// session is deresidented so its re-park would strand the action, and callers hold the
// fence already so acquiring it self-deadlocks.
//
// Kills M16's substitution independently of the ordering half.
func TestF084ForeignDisposalDischargeWritesNoRecord(t *testing.T) {
	e := testNewRejectionEnv(t, "f084r2nw")
	watch := e.watchDelivery(t, "will/topic")
	testPlantDeferredAction(t, e, false)

	e.sess.mu.Lock()
	e.sess.c = nil
	before := e.sess.seq
	e.sess.mu.Unlock()

	e.sess.disposeForeignSession(nil, mqttDisposalOwnershipUnsettled)

	if msg := e.deliveryBarrier(t, watch, "will/topic"); string(msg.Data) != "parked-will" {
		t.Fatalf("the discharge must publish: the watcher saw %q first", msg.Data)
	}
	// The record must not have moved. applyDeferredDisconnect's surviving branch persists a
	// disconnect time and its discard branch runs clear(); either would advance or destroy
	// the record this broker no longer owns.
	e.sess.mu.Lock()
	after := e.sess.seq
	e.sess.mu.Unlock()
	if after != before {
		t.Fatalf("a foreign disposal may write NOTHING to a record it does not own: "+
			"seq %d -> %d", before, after)
	}
}

// TestF084ForeignDisposalUnderOpenTransaction is the deadlock regression.
//
// The disposal must be able to run from a caller that already holds a record transaction on
// this goroutine — which mqttProcessSubs and the CONNECT paths do. dischargeDeferredWill
// takes no fence, so it completes; applyDeferredDisconnect would acquire the same fence and
// hang, because a fence cannot be acquired twice on one goroutine.
//
// It carries a PARKED ACTION deliberately: without one the discharge is never reached and
// the mutant survives through a no-op.
func TestF084ForeignDisposalUnderOpenTransaction(t *testing.T) {
	e := testNewRejectionEnv(t, "f084deadlock")
	watch := e.watchDelivery(t, "will/topic")
	testPlantDeferredAction(t, e, false)
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.mu.Unlock()

	tok := e.sess.beginRecordTx()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.sess.disposeForeignSession(nil, mqttDisposalOwnershipUnsettled)
	}()
	select {
	case <-done:
	case <-time.After(testRejectionWait):
		t.Fatal("the disposal DEADLOCKED against the caller's own record transaction: " +
			"the Will discharge must not take the record fence")
	}
	if terr := e.sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if msg := e.deliveryBarrier(t, watch, "will/topic"); string(msg.Data) != "parked-will" {
		t.Fatalf("the discharge must still publish: the watcher saw %q first", msg.Data)
	}
}

// TestF084RefusedDisposalTouchesNoWill is M14's paired fixture.
//
// The identity check must precede anything irreversible. When the session was rebound while
// this verdict was being reached, the disposal is not ours — and neither is the Will, which
// belongs to whoever holds the session now.
//
// The oracle is the Will's OWN STATE, not `applied == false`: a fixture asserting only the
// return value would pass against a broker that consumed the Will before checking.
func TestF084RefusedDisposalTouchesNoWill(t *testing.T) {
	e := testNewRejectionEnv(t, "f084refused")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	b.armWill()

	// A DIFFERENT client is bound now, so the identity check refuses for the stale caller.
	// ITS Will is armed too, and asserted below: a refused disposal must touch NOBODY's
	// Will, and watching only the stale caller's would miss a consume aimed at the current
	// holder — which is exactly what acting before the identity check does.
	other := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	other.armWill()

	d := e.sess.disposeForeignStateLocked(e.asm, b.bc, false)
	if d.applied {
		t.Fatal("the identity check must refuse a caller whose binding moved")
	}
	if d.will != mqttWillActionNone {
		t.Fatalf("a refused disposal owes NO Will action, got %v", d.will)
	}
	d.applyWill(e.sess, e.asm)

	// The Will is still exactly where it was: armed on the connection that owns it.
	b.bc.mu.Lock()
	disp, will := b.bc.mqtt.willDisposition, b.bc.mqtt.cp.will
	b.bc.mu.Unlock()
	if will == nil || disp != mqttWillArmed {
		t.Fatalf("a refused disposal must leave the stale caller's Will armed: disposition=%v will=%v",
			disp, will)
	}
	// And the CURRENT holder's, which belongs to whoever owns the session now.
	other.bc.mu.Lock()
	odisp, owill := other.bc.mqtt.willDisposition, other.bc.mqtt.cp.will
	other.bc.mu.Unlock()
	if owill == nil || odisp != mqttWillArmed {
		t.Fatalf("a refused disposal must not touch the CURRENT holder's Will either: "+
			"disposition=%v will=%v", odisp, owill)
	}
}

// TestF084LicenceSeparatesDiscardFromConsume is the licence table, and the pair is what
// makes it discriminating.
//
// A broker that simply never discards a Will passes the consume row; one that always
// discards passes the discard row. Only both together pin the rule that the LICENCE — not
// the vocabulary, and not the verdict's name — decides.
func TestF084LicenceSeparatesDiscardFromConsume(t *testing.T) {
	for _, tc := range []struct {
		name     string
		licensed bool
		want     mqttWillAction
	}{
		{"a proven in-process replacement discards", true, mqttWillActionDiscard},
		{"a record-derived disposal consumes", false, mqttWillActionConsumeLive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRejectionEnv(t, "f084lic")
			b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
			b.armWill()

			d := e.sess.disposeForeignStateLocked(e.asm, b.bc, tc.licensed)
			if !d.applied {
				t.Fatal("the disposal must apply: this caller is the bound one")
			}
			if d.will != tc.want {
				t.Fatalf("Will action = %v, want %v", d.will, tc.want)
			}
		})
	}
}

// TestF084ParkedActionOutranksLiveWill pins the precedence, which is not arbitrary: parking
// is what MOVED the Will off the connection, so the two cannot both be owed. Acting on the
// live connection instead would publish nothing (its Will is already gone) and strand the
// action.
func TestF084ParkedActionOutranksLiveWill(t *testing.T) {
	e := testNewRejectionEnv(t, "f084rank")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	testPlantDeferredAction(t, e, false)

	d := e.sess.disposeForeignStateLocked(e.asm, b.bc, false)
	if !d.applied {
		t.Fatal("the disposal must apply")
	}
	if d.will != mqttWillActionDischargeDeferred {
		t.Fatalf("a parked action outranks a live Will: got %v", d.will)
	}
	if d.action == nil {
		t.Fatal("the disposition must carry the action it decided to discharge")
	}
}

// TestF084DelayedParkedWillReachesTheOutbox is the delayed half of R2, and the reason the
// negatives assert an OWNERSHIP TRANSITION rather than a publication.
//
// "Consume normally" does not mean "the Will appears on the wire": a delayed Will is
// enqueued against its ORIGINAL deadline and a qualifying acceptance fact may later suppress
// it. What the broker owes is the enqueue.
func TestF084DelayedParkedWillReachesTheOutbox(t *testing.T) {
	e := testNewRejectionEnv(t, "f084delayed")
	action := testPlantDeferredAction(t, e, true)
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.mu.Unlock()

	e.sess.disposeForeignSession(nil, mqttDisposalOwnershipUnsettled)

	// The record's subject carries a freshly minted WillID, so it is found by FILTER rather
	// than predicted. One record, PENDING, is the ownership transition this route owes.
	filter := mqttWillQSubjectPrefix + e.asm.domainTk + e.idHash + ".*"
	deadline := time.Now().Add(testRejectionWait)
	for {
		subjects, _, err := e.asm.jsa.streamSubjects(mqttWillQStreamName, filter)
		if err == nil && len(subjects) == 1 {
			for subject := range subjects {
				if st := e.willOutboxState(t, subject); st != mqttWillPending {
					t.Fatalf("the enqueued Will must be pending, got %v", st)
				}
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a delayed parked Will must be ENQUEUED on the outbox against its "+
				"original deadline, not published and not dropped: subjects=%v err=%v",
				subjects, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if action.willDeadline <= time.Now().UnixNano() {
		t.Fatal("fixture: the planted deadline must still be in the future, or the " +
			"discharge publishes directly and this test proves nothing about the outbox")
	}
}

// TestF084RemotePersistEvictionDischargesWill is R4, and it fails against the pre-F-084
// broker outright.
//
// The remote-persist eviction detaches the client inside its as.mu section and then closes
// it. A DETACHED client's close publishes only an IMMEDIATE Will — mqttPublishOwedWill
// returns unless the disposition is mqttWillOwedNow — so an ARMED Will on that connection
// was never published and never enqueued. The tail performed no Will action at all.
func TestF084RemotePersistEvictionDischargesWill(t *testing.T) {
	e := testNewEvictEnv(t, "f084r4")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	b.armWill()
	watch := e.watchDelivery(t, "will/topic")
	done := e.armEvictObserver()

	e.remoteRecordWrite(t, "node2", "f084r4")
	if !testAwaitEvictDone(t, done) {
		t.Fatal("the persist observation must evict the displaced session")
	}

	// POSITIVE: the Will reaches the wire. The pre-change broker produced silence here, and
	// silence is what a barrier-only assertion would also accept.
	if msg := e.deliveryBarrier(t, watch, "will/topic"); string(msg.Data) != "bye" {
		t.Fatalf("a record-derived eviction must discharge the displaced connection's Will: "+
			"the watcher saw %q first", msg.Data)
	}
}

// TestF084BoundRejectionNilRebindOwesNothing is M7's paired fixture, and the nil case is
// the ONLY one where the two predicates differ.
//
// settleBoundRejectionGated's spin used to ask `sess.c != c`, which is also true when
// sess.c is NIL — and nil is what a resolver or a record-derived disposal leaves behind,
// neither of which demonstrates a successor. Discarding there suppressed a Will on the
// strength of somebody else's disposal.
func TestF084BoundRejectionNilRebindOwesNothing(t *testing.T) {
	e := testNewRejectionEnv(t, "f084nil")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	b.armWill()

	// A record-derived disposal already detached the connection: sess.c is nil, and no
	// replacement client exists anywhere.
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.mu.Unlock()

	// The gate is held by somebody else, so the spin refuses and classifies the rebind.
	if err := e.asm.lockSession(e.sess, nil); err != nil {
		t.Fatalf("stage the contended gate: %v", err)
	}
	outcome, stale := b.bc.settleBoundRejectionGated(e.asm, e.sess)
	e.asm.unlockSession(e.sess)

	if outcome != mqttBoundDisplaced {
		t.Fatalf("a moved binding reports displaced, got %v", outcome)
	}
	if stale != nil {
		t.Fatalf("a displaced arm notifies nobody, got %p", stale)
	}
	// The Will is UNTOUCHED. A nil binding is not a takeover, so this arm may not consume it
	// either — the disposal that set sess.c to nil owns it, and acting here would leave two
	// paths believing they owed the same Will.
	b.bc.mu.Lock()
	disp, will := b.bc.mqtt.willDisposition, b.bc.mqtt.cp.will
	b.bc.mu.Unlock()
	if will == nil || disp != mqttWillArmed {
		t.Fatalf("a nil rebind owes no Will action: disposition=%v will=%v", disp, will)
	}
}

// TestF084BoundRejectionLocalReplacementDiscards is the other half of the pair, and without
// it a broker that simply never discards passes the fixture above.
//
// A DIFFERENT, NON-NIL local client bound to this session is the licensed evidence: a real
// in-process replacement holds the session, and its takeover owns the Will's fate.
func TestF084BoundRejectionLocalReplacementDiscards(t *testing.T) {
	e := testNewRejectionEnv(t, "f084local")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	b.armWill()

	// The replacement rebinds, exactly as a local takeover's CONNECT does.
	other := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	if other.bc == b.bc {
		t.Fatal("fixture: the replacement must be a different connection")
	}

	// The gate is taken as the CURRENT owner, which is the replacement — that is what makes
	// the stale connection's own attempt fail fast and reach the rebind classification.
	if err := e.asm.lockSession(e.sess, other.bc); err != nil {
		t.Fatalf("stage the contended gate: %v", err)
	}
	outcome, _ := b.bc.settleBoundRejectionGated(e.asm, e.sess)
	e.asm.unlockSession(e.sess)

	if outcome != mqttBoundDisplaced {
		t.Fatalf("a moved binding reports displaced, got %v", outcome)
	}
	// DISCARDED: mqttTakeWillLocked leaves the disposition discharged.
	b.bc.mu.Lock()
	disp := b.bc.mqtt.willDisposition
	b.bc.mu.Unlock()
	if disp != mqttWillDischarged {
		t.Fatalf("a proven local replacement discards the displaced Will, disposition=%v", disp)
	}
}

// testTakeoverClaimSites returns every PRODUCTION line mentioning name, with its file and
// line number. Test files are excluded: a fixture naming the symbol is not a route to it.
func testTakeoverClaimSites(t *testing.T, name string) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	var sites []string
	for _, e := range entries {
		fn := e.Name()
		if e.IsDir() || !strings.HasSuffix(fn, ".go") || strings.HasSuffix(fn, "_test.go") {
			continue
		}
		body, rerr := os.ReadFile(fn)
		if rerr != nil {
			t.Fatalf("read %s: %v", fn, rerr)
		}
		for i, line := range strings.Split(string(body), "\n") {
			code := line
			if idx := strings.Index(code, "//"); idx >= 0 {
				code = code[:idx]
			}
			if strings.Contains(code, name) {
				sites = append(sites, fmt.Sprintf("%s:%d: %s", fn, i+1, strings.TrimSpace(line)))
			}
		}
	}
	return sites
}

// TestF084VocabulariesSayDifferentThings pins the CLAIM TEXTS as literals.
//
// Every other assertion in this change compares a claim against a vocabulary VALUE, which
// is the right shape for a call-site test and is unfalsifiable about the text itself: a
// mutation that rewrites the constant moves the expectation and the observation together.
// The literals live here, once, so a broker cannot quietly start telling an unsettled
// client that its session was taken over.
func TestF084VocabulariesSayDifferentThings(t *testing.T) {
	if mqttDisposalTakenOver.reasonStr != "session taken over by a new connection" {
		t.Fatalf("the takeover claim's text changed: %q", mqttDisposalTakenOver.reasonStr)
	}
	if mqttDisposalTakenOver.reason != 0x8e {
		t.Fatalf("the takeover claim's code changed: %#x", mqttDisposalTakenOver.reason)
	}
	if mqttDisposalOwnershipUnsettled.reasonStr != "session ownership could not be established; try again" {
		t.Fatalf("the unsettled claim's text changed: %q", mqttDisposalOwnershipUnsettled.reasonStr)
	}
	if mqttDisposalOwnershipUnsettled.reason != 0x89 {
		t.Fatalf("the unsettled claim's code changed: %#x", mqttDisposalOwnershipUnsettled.reason)
	}
	// The two 0x89 vocabularies share a code and a Reason String, so the LOG LINE is the
	// only thing separating "a sibling erased our record" from "nothing was established".
	if mqttDisposalForked.logReason == mqttDisposalOwnershipUnsettled.logReason {
		t.Fatalf("forked and unsettled must stay distinguishable in the log: both say %q",
			mqttDisposalForked.logReason)
	}
	// And no 0x89 vocabulary may borrow the takeover text: pairing "try again later" with
	// "session taken over by a new connection" is the contradictory claim the disposal
	// struct exists to prevent.
	for _, voc := range []mqttDisposalVocabulary{
		mqttDisposalForked, mqttDisposalOwnershipUnsettled, mqttDisposalTeardownIncomplete,
	} {
		if voc.reasonStr == mqttReasonStrSessionTakenOver {
			t.Fatalf("a 0x89 vocabulary claims a takeover in its text: %+v", voc)
		}
	}
}

// TestF084OnlyOneReaderOfTheTakeoverClaim is the invariant F-084 rests on, asserted where a
// reader can see it: the takeover vocabulary is reachable from exactly one notifier, and
// that notifier from exactly one tail.
//
// A source-level guard rather than a behavioral one, and deliberately so: the behavioral
// fixtures can only cover the routes they know about, and a NEW route added later would
// pass every one of them while still claiming a takeover it cannot prove. That is the
// enumeration defect this finding was filed for; this is the property that replaces it.
func TestF084OnlyOneReaderOfTheTakeoverClaim(t *testing.T) {
	readers := testTakeoverClaimSites(t, "mqttDisposalTakenOver")
	// Its definition in sess_fork_lineage.go, and mqttNotifyTakenOver. Nothing else.
	if len(readers) != 2 {
		t.Fatalf("mqttDisposalTakenOver must have exactly ONE reader beside its definition; "+
			"found %d sites:\n%s\nA new one is a takeover claim on unproven evidence (F-084).",
			len(readers), strings.Join(readers, "\n"))
	}
	callers := testTakeoverClaimSites(t, "mqttNotifyTakenOver")
	// Its definition, and discardTakenOverClient's call.
	if len(callers) != 2 {
		t.Fatalf("mqttNotifyTakenOver must have exactly ONE caller beside its definition "+
			"(discardTakenOverClient); found %d sites:\n%s",
			len(callers), strings.Join(callers, "\n"))
	}
}

// TestF084RemotePersistEvictionEnqueuesDelayedWill is R4's delayed half, and the only place
// the capsule's EPOCH is observable.
//
// An immediate Will is published straight through and carries no epoch, so the fixture
// above cannot tell a correct epoch from an empty one. A DELAYED Will lands on the outbox
// with its fence baseline attached, and that record is what pins it. The expiry likewise
// has to come from the CONNECTION rather than the session: this route holds no ownership
// gate and can detach between the two updates a v5 DISCONNECT makes.
func TestF084RemotePersistEvictionEnqueuesDelayedWill(t *testing.T) {
	e := testNewEvictEnv(t, "f084r4d")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)

	// A DELAYED Will on a surviving session: a Will Delay Interval, and an expiry that
	// outlives the connection so mqttWillIsDelayed answers yes.
	b.bc.mu.Lock()
	b.bc.mqtt.cp.will = &mqttWill{
		topic:   []byte("will/topic"),
		subject: []byte("will.topic"),
		message: []byte("delayed-bye"),
		props:   &mqttProperties{willDelay: 3600},
	}
	b.bc.mqtt.willDisposition = mqttWillArmed
	// DELIBERATELY DIFFERENT from the session's. testNewRejectionEnv sets
	// sess.expiryInterval to mqttSessionNeverExpire, so a fixture that used the same value
	// here could not tell the connection-side sample from the session-side one — and the
	// session-side one is exactly the detach race the design rejects. mqttHandleWill caps
	// the Will delay at the effective expiry, so a SHORTER connection expiry is visible in
	// the record's FireAt and a session-side read is not.
	b.bc.mqtt.sessionExpiry = 60
	b.bc.mu.Unlock()

	done := e.armEvictObserver()
	e.remoteRecordWrite(t, "node2", "f084r4d")
	if !testAwaitEvictDone(t, done) {
		t.Fatal("the persist observation must evict the displaced session")
	}

	filter := mqttWillQSubjectPrefix + e.asm.domainTk + e.idHash + ".*"
	deadline := time.Now().Add(testRejectionWait)
	for {
		subjects, _, err := e.asm.jsa.streamSubjects(mqttWillQStreamName, filter)
		if err == nil && len(subjects) == 1 {
			for subject := range subjects {
				rec, _, lerr := e.asm.jsa.loadWillOutbox(e.asm.domainTk, subject)
				if lerr != nil || rec == nil {
					t.Fatalf("load the enqueued Will: %v", lerr)
				}
				// The EXACT manager epoch. An empty one would leave the record unable to
				// fence itself against the incarnation that actually disconnected.
				if rec.OriginSessEpoch != e.asm.sessStreamEpoch {
					t.Fatalf("the enqueued Will must carry the manager's stream epoch %q, got %q",
						e.asm.sessStreamEpoch, rec.OriginSessEpoch)
				}
				if rec.State != mqttWillPending {
					t.Fatalf("the enqueued Will must be pending, got %v", rec.State)
				}
				// THE EXPIRY, read from the FireAt it capped. The Will Delay is 3600 s and
				// this connection's expiry is 60 s, so a correct sample fires in ~60 s;
				// reading sess.expiryInterval (never-expire) would leave the full 3600 s
				// delay uncapped. Bounded generously — this asserts WHICH value was used,
				// not scheduler precision.
				fireIn := time.Until(time.Unix(0, rec.FireAt))
				if fireIn > 10*time.Minute {
					t.Fatalf("the Will delay must be capped by the CONNECTION's expiry (60s), "+
						"not by the session's never-expire: fires in %v", fireIn)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a record-derived eviction must ENQUEUE the displaced connection's "+
				"delayed Will: subjects=%v err=%v", subjects, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestF084DeferredWillImmediatePublishFailureIsLoggedNotReparked pins residual W9.
//
// On the already-due branch the discharge publishes through the outbox's client-free
// publisher, and a failure there is LOGGED with the action already consumed — nothing
// retries it and nothing puts it back. Re-parking would be worse, not better: phase 1 has
// already deresidented the session, so a re-parked action sits where no pass can reach it.
//
// The fixture exists so that residual is a pinned behavior rather than an unexamined one.
func TestF084DeferredWillImmediatePublishFailureIsLoggedNotReparked(t *testing.T) {
	e := testNewRejectionEnv(t, "f084w9")
	testPlantDeferredAction(t, e, false)
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.mu.Unlock()

	// A Will whose topic cannot be published. deliverWillRecord's own guard warns and
	// returns without publishing, which is the failure this residual is about: the action
	// has already been consumed by the time anything goes wrong.
	e.sess.mu.Lock()
	e.sess.deferredDisconnect.will.topic = []byte("will/+/undeliverable")
	e.sess.mu.Unlock()

	// The LOG is half the residual. "Lost, and only logged" is a promise about the
	// diagnostic as much as about the loss, and without capturing it this fixture would
	// stay green if both warning paths were deleted — turning a logged loss into a silent
	// one, which is precisely what the residual says does NOT happen.
	logs := testCaptureServerWarnings(t, e.s)

	e.sess.disposeForeignSession(nil, mqttDisposalOwnershipUnsettled)

	if got := logs.text(); !strings.Contains(got, "will/+/undeliverable") {
		t.Fatalf("a Will lost to a failed immediate discharge must be LOGGED, naming what "+
			"was lost; captured warnings:\n%s", got)
	}

	// The action is CONSUMED, not re-parked: a re-park onto a deresidented object is
	// unreachable, so it would convert a logged loss into a silent one.
	e.sess.mu.Lock()
	still := e.sess.deferredDisconnect
	e.sess.mu.Unlock()
	if still != nil {
		t.Fatalf("a failed immediate discharge must NOT re-park the action onto a "+
			"deresidented session, where no pass can reach it: %+v", still)
	}
}

// TestF084ConnectDisposalsAllUseThePhasedHelper is the ordering regression the holistic
// review asked for, and it is a SOURCE assertion because the ordering it protects is a
// property of where code sits, not of what any single run observes.
//
// Round 1 found the three post-CONNACK forced-leave arms calling the plain
// disposeForeignSessionLocked wrapper while acceptTok was still open. That wrapper runs
// phase 2 and the notification INLINE, so the Will action happened inside the record
// transaction and the notification before the ownership gate released — the exact ordering
// the plan, docs/ARCHITECTURE.md and the roadmap all claim landed. A behavioral fixture for
// it would have to observe a fence held across an enqueue's retries, which is a timing
// property; the durable fact is that every disposing arm of mqttProcessConnect goes through
// the one helper that hands phase 2 to the defers.
func TestF084ConnectDisposalsAllUseThePhasedHelper(t *testing.T) {
	body, err := os.ReadFile("mqtt.go")
	if err != nil {
		t.Fatalf("read mqtt.go: %v", err)
	}
	lines := strings.Split(string(body), "\n")

	start, end := -1, -1
	for i, line := range lines {
		if strings.HasPrefix(line, "func (s *Server) mqttProcessConnect(") {
			start = i
			continue
		}
		if start >= 0 && strings.HasPrefix(line, "func ") && i > start {
			end = i
			break
		}
	}
	if start < 0 {
		t.Fatal("mqttProcessConnect not found; this guard cannot check what it claims to")
	}
	if end < 0 {
		end = len(lines)
	}

	// EVERY primitive that can begin a disposal, not just the two the first draft knew
	// about. disposeForeignStateFenced is the one the helper itself calls, so a guard blind
	// to it would let an arm bypass the helper through exactly the function the helper uses
	// — green, and wrong.
	primitives := []string{
		"disposeForeignSessionLocked",
		"disposeForeignStateLocked",
		"disposeForeignStateFenced",
		// The retirement path's phase 1 (F-036 unit 1): it builds the disposition from
		// the capsule instead of the session, and a bypass through it would run the Will
		// inside the accept transaction exactly as one through the others would.
		"asm.disposeCaptured(",
	}
	var stray []string
	helperCalls := 0
	for i := start; i < end; i++ {
		code := lines[i]
		if idx := strings.Index(code, "//"); idx >= 0 {
			code = code[:idx]
		}
		// Both phased helpers count: they hand phase 2 to the same connWillPhase /
		// connTail defers, which is the property this guard is about.
		if strings.Contains(code, "disposeThroughPhases(") ||
			strings.Contains(code, "disposeCapturedThroughPhases(") {
			helperCalls++
		}
		hit := false
		for _, pr := range primitives {
			if strings.Contains(code, pr) {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		// Each helper's own body is a permitted caller of its phase 1 here.
		if strings.Contains(code, "d := target.disposeForeignStateFenced(asm, owner, false, fence)") ||
			strings.Contains(code, "d := asm.disposeCaptured(target, capsule, false)") {
			continue
		}
		stray = append(stray, fmt.Sprintf("mqtt.go:%d: %s", i+1, strings.TrimSpace(lines[i])))
	}
	if len(stray) != 0 {
		t.Fatalf("every disposing arm of mqttProcessConnect must go through "+
			"disposeThroughPhases, which hands phase 2 to the defers so the Will runs "+
			"AFTER endAcceptTx and BEFORE the gate release. These bypass it and would run "+
			"the Will inside the accept transaction:\n%s", strings.Join(stray, "\n"))
	}

	// EXACT, not a minimum. A loose floor lets an arm be replaced by a bypass while the
	// count still passes, which is how a guard stops guarding. The five are: the
	// teardown-incomplete arm (which since F-036 unit 1 is the CAPSULE-consuming
	// helper, because by then the displaced client has been detached and es.c is nil),
	// refuseDisposing, and the three forced-leave arms. (Neither helper's own
	// definition is a call, and neither is counted.)
	const wantHelperCalls = 5
	if helperCalls != wantHelperCalls {
		t.Fatalf("expected exactly %d phased-disposal call sites in mqttProcessConnect "+
			"(teardown-incomplete, refuseDisposing, and the three forced-leave arms); found "+
			"%d. If an arm was added or removed deliberately, update this count and say why.",
			wantHelperCalls, helperCalls)
	}
}

// TestF084TeardownIncompleteConsumesAgainstTheRetiringIncarnation pins the Will fence.
//
// The post-reset teardown-incomplete arm disposes an object whose clear() has already
// re-minted its generation and set its sequence to 0. Consuming a DELAYED Will against
// that object hands enqueueDelayedWill a zero origin sequence, which its validation
// refuses — so the delay is sacrificed and the Will is published immediately, for a session
// that had merely failed to tear itself down.
//
// The oracle is the outbox record's own fields: a NONZERO origin sequence and the generation
// the Will was armed under. Asserting only "a record exists" would pass against the zero
// baseline, and asserting only "no immediate publication" would pass against no Will at all.
func TestF084TeardownIncompleteConsumesAgainstTheRetiringIncarnation(t *testing.T) {
	e := testNewRejectionEnv(t, "f084fence")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)

	b.bc.mu.Lock()
	b.bc.mqtt.cp.will = &mqttWill{
		topic:   []byte("will/topic"),
		subject: []byte("will.topic"),
		message: []byte("fenced-bye"),
		props:   &mqttProperties{willDelay: 3600},
	}
	b.bc.mqtt.willDisposition = mqttWillArmed
	b.bc.mqtt.sessionExpiry = mqttSessionNeverExpire
	b.bc.mu.Unlock()

	// The fence, captured while the object still names the incarnation the Will belongs to.
	fence := e.sess.mqttCaptureWillFence()
	if fence.seq == 0 || fence.gen == _EMPTY_ {
		t.Fatalf("fixture: the pre-reset fence must carry a real incarnation, got %+v", fence)
	}

	// Now RESET the object exactly as a post-reset clear() failure leaves it: sequence 0,
	// generation re-minted. This is the state the disposal actually runs against.
	e.sess.mu.Lock()
	e.sess.seq, e.sess.gen = 0, mqttNewSessGen()
	e.sess.mu.Unlock()

	d := e.sess.disposeForeignStateFenced(e.asm, nil, false, fence)
	if !d.applied || d.will != mqttWillActionConsumeLive {
		t.Fatalf("the teardown-incomplete arm consumes the live Will: applied=%v will=%v",
			d.applied, d.will)
	}
	d.applyWill(e.sess, e.asm)

	filter := mqttWillQSubjectPrefix + e.asm.domainTk + e.idHash + ".*"
	deadline := time.Now().Add(testRejectionWait)
	for {
		subjects, _, err := e.asm.jsa.streamSubjects(mqttWillQStreamName, filter)
		if err == nil && len(subjects) == 1 {
			for subject := range subjects {
				rec, _, lerr := e.asm.jsa.loadWillOutbox(e.asm.domainTk, subject)
				if lerr != nil || rec == nil {
					t.Fatalf("load the enqueued Will: %v", lerr)
				}
				// The RETIRING incarnation's identity, not the wiped object's.
				if rec.OriginSessionSeq != fence.seq {
					t.Fatalf("the delayed Will must be fenced against the retiring "+
						"incarnation's sequence %d, got %d — a zero baseline is refused by "+
						"the outbox and sacrifices the delay", fence.seq, rec.OriginSessionSeq)
				}
				if rec.Gen != fence.gen {
					t.Fatalf("the delayed Will must carry the retiring generation %q, got %q",
						fence.gen, rec.Gen)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a teardown-incomplete disposal must ENQUEUE the delayed Will against "+
				"the retiring incarnation: subjects=%v err=%v", subjects, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// testWarningCapture collects everything the server logged at WARN or above.
type testWarningCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *testWarningCapture) text() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// testCaptureServerWarnings swaps in a capturing logger for the duration of the test.
//
// It exists because several F-084 residuals are "the Will is lost, and the loss is logged":
// the log IS the guarantee there, so a fixture that does not read it cannot tell a
// documented loss from a silent one.
func testCaptureServerWarnings(t *testing.T, s *Server) *testWarningCapture {
	t.Helper()
	cap := &testWarningCapture{}
	prev := s.log.Load()
	s.log.Store(slog.New(slog.NewTextHandler(&syncWriter{c: cap}, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})))
	t.Cleanup(func() { s.log.Store(prev) })
	return cap
}

type syncWriter struct{ c *testWarningCapture }

func (w *syncWriter) Write(p []byte) (int, error) {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	return w.c.buf.Write(p)
}

// TestF084WillExpiryIsReadAtConsumptionNotSnapshotted pins the interval's ownership.
//
// The Session Expiry Interval is CONNECTION-owned and stays mutable through an MQTT 5
// DISCONNECT override. A disposal that snapshots it in phase 1 and consumes in phase 2 —
// with a clear(), a record probe or an as.mu section in between — can therefore consume
// against an interval the client has since changed. That decides whether the Will is due
// now or after its delay, and caps the delay, so a stale read holds a due Will back for the
// full Will Delay.
//
// The fixture moves the value BETWEEN the phases and asserts the outbox FireAt follows the
// NEW one. A snapshot-based implementation caps at the old interval and fails.
func TestF084WillExpiryIsReadAtConsumptionNotSnapshotted(t *testing.T) {
	e := testNewRejectionEnv(t, "f084expiry")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)

	b.bc.mu.Lock()
	b.bc.mqtt.cp.will = &mqttWill{
		topic:   []byte("will/topic"),
		subject: []byte("will.topic"),
		message: []byte("expiry-bye"),
		props:   &mqttProperties{willDelay: 3600},
	}
	b.bc.mqtt.willDisposition = mqttWillArmed
	// Phase 1 sees never-expire, which would leave the 3600 s delay uncapped.
	b.bc.mqtt.sessionExpiry = mqttSessionNeverExpire
	b.bc.mu.Unlock()

	d := e.sess.disposeForeignStateLocked(e.asm, b.bc, false)
	if !d.applied || d.will != mqttWillActionConsumeLive {
		t.Fatalf("the disposal must prepare a live consume: applied=%v will=%v", d.applied, d.will)
	}

	// The client's DISCONNECT override lands BETWEEN the phases.
	b.bc.mu.Lock()
	b.bc.mqtt.sessionExpiry = 60
	b.bc.mu.Unlock()

	d.applyWill(e.sess, e.asm)

	filter := mqttWillQSubjectPrefix + e.asm.domainTk + e.idHash + ".*"
	deadline := time.Now().Add(testRejectionWait)
	for {
		subjects, _, err := e.asm.jsa.streamSubjects(mqttWillQStreamName, filter)
		if err == nil && len(subjects) == 1 {
			for subject := range subjects {
				rec, _, lerr := e.asm.jsa.loadWillOutbox(e.asm.domainTk, subject)
				if lerr != nil || rec == nil {
					t.Fatalf("load the enqueued Will: %v", lerr)
				}
				// Capped by the CURRENT 60 s, not the 3600 s delay a never-expire snapshot
				// would have left uncapped.
				if fireIn := time.Until(time.Unix(0, rec.FireAt)); fireIn > 10*time.Minute {
					t.Fatalf("the Will delay must be capped by the expiry as it stands when "+
						"the Will is CONSUMED (60s), not by the value phase 1 saw: "+
						"fires in %v", fireIn)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the consume must enqueue the delayed Will: subjects=%v err=%v", subjects, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestF084WillPhaseOrderingIsObserved is the BEHAVIOURAL oracle for the phase-2 ordering,
// and it is what the source guard above cannot be.
//
// That guard recognises three primitive names inside one function. A renamed wrapper, an
// inlined detach-and-extract, or a fourth primitive all evade the text — and none of them
// evades this, because this observes the ordering at the moment it matters: immediately
// before the irreversible Will action, on whatever path reached it.
//
// Two facts are probed there, and each is probed by ATTEMPTING the thing the ordering
// promises is possible or impossible:
//
//   - the record transaction must be CLOSED, so `tryBeginRecordTx` must SUCCEED. The
//     consume does JetStream work and the discharge would self-deadlock on a fence its
//     caller holds; a fence still held here is the deadlock, observed before it happens.
//   - the ownership gate must still be HELD, so `lockSession` must FAIL. A released gate
//     means something could rebind between the disposal and the Will it owes.
//
// The probe fires from mqttForeignDisposition.applyWill, so every disposal that goes
// THROUGH the prepared disposition is instrumented the moment it disposes.
//
// That is not the same as "every disposal path", and an earlier draft of this comment
// claimed it was. FOUR routes reach a Will action WITHOUT applyWill, and each is covered
// by its own oracle or excluded on the record:
//
//   - R4, the remote-persist eviction, calls dischargeDeferredWill/mqttHandleWill directly
//     from its tail. Its CONTRACT is the inverse of this one — no ownership gate is held
//     there at all — so it fires the same probe and is asserted by
//     TestF084RemotePersistWillPhaseIsUngated;
//   - the LICENSED takeover discards through discardTakenOverClient, which is not a
//     record-derived disposal and is pinned by the positive oracle;
//   - B2, settleBoundRejectionGated's rebound arm, calls mqttDiscardWillOnTakeover on the
//     DISPLACED connection directly. It is not a disposal at all: it applies the licensed
//     takeover's own rule early, so that whichever side notices the displacement first
//     applies it and the other's call is a no-op. It reaches no prepared disposition and
//     so cannot fire this probe; it is asserted by
//     TestF084BoundRejectionLocalReplacementDiscards;
//   - R5 discards the ARRIVING connection's Will in mqttAbortAcceptedConnect, which is
//     excluded by design (no session was granted) and pinned by the two-Will fixture.
func TestF084WillPhaseOrderingIsObserved(t *testing.T) {
	e := testNewRejectionEnv(t, "f084order")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	b.armWill()

	var fired, fenceFree, gateHeld bool
	restore := setMQTTTestWillPhaseProbe(func(sess *mqttSession, asm *mqttAccountSessionManager) {
		fired = true
		// The FENCE: free means the caller's record transaction is closed.
		if tok, ok := sess.tryBeginRecordTx(); ok {
			fenceFree = true
			if terr := sess.endRecordTx(tok); terr != nil {
				t.Errorf("release the probe's fence: %v", terr)
			}
		}
		// The GATE: an acquisition that FAILS means it is still held by the disposer.
		if err := asm.lockSession(sess, nil); err != nil {
			gateHeld = true
		} else {
			asm.unlockSession(sess)
		}
	})
	defer restore()

	// Drive a real disposal through the gated wrapper, which is the shape every
	// record-derived caller uses.
	e.sess.disposeForeignSession(b.bc, mqttDisposalOwnershipUnsettled)

	if !fired {
		t.Fatal("the Will phase never ran, so this fixture asserted nothing about its ordering")
	}
	if !fenceFree {
		t.Fatal("the record transaction was STILL OPEN at the Will phase: the consume does " +
			"JetStream work and the deferred discharge would self-deadlock on it")
	}
	if !gateHeld {
		t.Fatal("the ownership gate was ALREADY RELEASED at the Will phase: something could " +
			"rebind between the disposal and the Will it owes")
	}
}

// TestF084RemotePersistWillPhaseIsUngated is R4's half of the ordering oracle, and its
// expectations are the INVERSE of the gated paths'.
//
// That inversion is the point. R4 decides under as.mu, detaches inside that section, and
// runs its Will action in a tail that executes after the record token and every lock are
// released — so asserting "the gate is held" there would assert something false, and a
// single universal oracle would have had to be wrong for one route or the other.
//
// What it shares with the gated paths is the fence: the record transaction must be CLOSED
// before the Will action, because the discharge and the consume both do JetStream work.
func TestF084RemotePersistWillPhaseIsUngated(t *testing.T) {
	e := testNewEvictEnv(t, "f084r4ord")
	b := e.bindSocketAttemptLog(t, mqttProtoLevel5)
	b.armWill()

	var fired, fenceFree, gateFree bool
	restore := setMQTTTestWillPhaseProbe(func(sess *mqttSession, asm *mqttAccountSessionManager) {
		fired = true
		if tok, ok := sess.tryBeginRecordTx(); ok {
			fenceFree = true
			if terr := sess.endRecordTx(tok); terr != nil {
				t.Errorf("release the probe's fence: %v", terr)
			}
		}
		// UNGATED here, unlike the phase-2 paths: an acquisition that SUCCEEDS is the
		// contract, and holding one across this tail would be the bug.
		if err := asm.lockSession(sess, nil); err == nil {
			gateFree = true
			asm.unlockSession(sess)
		}
	})
	defer restore()

	done := e.armEvictObserver()
	e.remoteRecordWrite(t, "node2", "f084r4ord")
	if !testAwaitEvictDone(t, done) {
		t.Fatal("the persist observation must evict the displaced session")
	}

	if !fired {
		t.Fatal("R4's Will phase never ran, so this fixture asserted nothing — and R4 is " +
			"exactly the route that does NOT go through applyWill")
	}
	if !fenceFree {
		t.Fatal("R4 ran its Will action with the record transaction still OPEN: the " +
			"discharge does JetStream work and cannot hold the fence across it")
	}
	if !gateFree {
		t.Fatal("R4 ran its Will action while holding an ownership gate: this route " +
			"decides under as.mu and its tail runs after every lock is released")
	}
}

// TestF084RemotePersistEvictionDischargesParkedAction covers R4's PARKED-ACTION arm, which
// the armed-Will fixtures above do not reach — and which a mutation disabling it therefore
// survived until this existed.
//
// The two arms are different code and different consequences. With a parked deferred close,
// the eviction owes that action's Will against its ORIGINAL deadline, not the connection's
// live one; before F-084 it owed nothing and the action was dropped.
func TestF084RemotePersistEvictionDischargesParkedAction(t *testing.T) {
	e := testNewEvictEnv(t, "f084r4park")
	watch := e.watchDelivery(t, "will/topic")
	testPlantDeferredAction(t, e, false)

	done := e.armEvictObserver()
	e.remoteRecordWrite(t, "node2", "f084r4park")
	if !testAwaitEvictDone(t, done) {
		t.Fatal("the persist observation must evict the displaced session")
	}

	if msg := e.deliveryBarrier(t, watch, "will/topic"); string(msg.Data) != "parked-will" {
		t.Fatalf("a record-derived eviction must DISCHARGE the parked action's Will, not "+
			"drop it: the watcher saw %q first", msg.Data)
	}
	e.sess.mu.Lock()
	still := e.sess.deferredDisconnect
	e.sess.mu.Unlock()
	if still != nil {
		t.Fatalf("the discharged action must be consumed, still parked: %+v", still)
	}
}
