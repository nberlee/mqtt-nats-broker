//go:build mqtt_fulltests

package mqtt

import (
	"errors"
	"testing"
)

// End-to-end coverage for the retirement classifier (qos2_retire_classify.go), issue #40
// site 2.
//
// These are behavioral pins for the same reason the release-claim e2e tests are: the
// property at stake is "was this acknowledged message delivered onward for a session
// that no longer exists", which no record inspection settles. Every case drives a real
// PUBLISH/PUBREL exchange and asserts on what the WITNESS SUBSCRIBER received and on the
// WIRE answer the publisher got — 0x92 with the hold released versus no PUBCOMP at all
// are two different refusals, and a test that only checked "not 0x00" could not tell them
// apart.
//
// What the classifier catches that neither existing fence can: the discard's enumeration
// is a one-shot snapshot, so a hold on a subject it never named is never competed for at
// all, and issue #66's cutoff deliberately SPARES a hold that landed above the watermark. In
// both cases no claim is ever written for this exchange, so the claim succeeds, fence 2
// finds our own record at our own sequence, and without this classifier the release fans
// out and answers 0x00.

// setMQTTTestQoS2RetireSampledHook installs (or, with nil, clears) the classifier's
// post-sample hook and returns a function that restores the previous value.
//
// It lives beside its only caller rather than next to the hook variable in production:
// the ordering test that uses it is behind this build tag, and a setter with no caller in
// the default build is dead code the linter rejects.
func setMQTTTestQoS2RetireSampledHook(h func(verdict mqttQoS2RetireVerdict)) func() {
	prev := mqttTestQoS2RetireSampledHookV.Load()
	if h == nil {
		mqttTestQoS2RetireSampledHookV.Store(nil)
	} else {
		mqttTestQoS2RetireSampledHookV.Store(&h)
	}
	return func() { mqttTestQoS2RetireSampledHookV.Store(prev) }
}

// testPlantPubRelRetirement makes the publisher's generation gen noncurrent in its authority
// (testRetireGeneration); later has a later generation installed after it and torn down as
// well, so the closed authority's last generation is not ours. The classifier reads the
// standing through the same authority read every other reader uses.
func testPlantPubRelRetirement(t *testing.T, b *testQoS2Broker, gen string, later bool) {
	t.Helper()
	if later {
		testRetireGenerationUnderLater(t, b.asm, b.pubID, gen)
		return
	}
	testRetireGeneration(t, b.asm, b.pubID, gen)
}

// testPubRelSessionFacts returns the publisher session's incarnation generation and its
// acknowledged record sequence.
func testPubRelSessionFacts(t *testing.T, b *testQoS2Broker) (gen string, seq uint64) {
	t.Helper()
	sess := testMQTTGetClient(t, b.s, b.pubID).mqtt.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.gen, sess.seq
}

// TestPubRelRefusesUnderAMatchingRetirement: THIS incarnation's generation reads noncurrent,
// and the closed authority still names it as its last generation (consult 10's bare-Gen case).
//
// The hold is one the discard never enumerated, so nothing competes for the claim and both
// existing fences pass. The only thing that can stop the fan-out is the classification.
//
// Mutations that must fail this: classifying "current" on Gen equality alone without the
// serving state; using the exchange's rec.gen instead of the session generation; moving the
// classifier after the onward store.
func TestPubRelRefusesUnderAMatchingRetirement(t *testing.T) {
	t.Parallel()
	b := testQoS2Setup(t, "f036-retire-match")
	pi := uint16(41)
	holdSubject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("retired-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	// The peer's teardown claimed the generation and then died before deleting anything —
	// or its enumeration never named this subject. Either way the fact is durable and the
	// hold is still here, which is exactly the population the claim cannot see.
	gen, _ := testPubRelSessionFacts(t, b)
	testPlantPubRelRetirement(t, b, gen, false)

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x%x for a release whose incarnation was retired",
			reason, mqttReasonPacketIDNotFound)
	}
	testQoS2ExpectNoDelivery(t, b)

	// The hold is RELEASED, and that is the half that distinguishes this refusal from
	// the unresolved one: PUBCOMP frees the identifier, so leaving the record would let
	// the client's next PUBLISH on it be matched against a survivor and silently lost.
	rec, _, err := b.asm.loadQoS2Record(holdSubject)
	if err != nil {
		t.Fatalf("load the hold record: %v", err)
	}
	if rec != nil {
		t.Fatalf("the hold record survived a refused release: %+v", rec)
	}
}

// TestPubRelRefusesOnceALaterGenerationEndedOurs: a successor's Clean Start made our
// generation outstanding and its own generation was torn down after it, so the authority's
// last generation is not ours. Our generation's standing still refuses.
//
// Mutation that must fail this: classifying on the authority's last generation instead of the
// session's own.
func TestPubRelRefusesOnceALaterGenerationEndedOurs(t *testing.T) {
	t.Parallel()
	b := testQoS2Setup(t, "f036-retire-superseded")
	pi := uint16(42)
	holdSubject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("superseded-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	gen, _ := testPubRelSessionFacts(t, b)
	testPlantPubRelRetirement(t, b, gen, true)

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x%x for a release whose record subject advanced past it",
			reason, mqttReasonPacketIDNotFound)
	}
	testQoS2ExpectNoDelivery(t, b)
	if rec, _, err := b.asm.loadQoS2Record(holdSubject); err != nil || rec != nil {
		t.Fatalf("the hold record survived a refused release: rec=%+v err=%v", rec, err)
	}
}

// TestPubRelProceedsUnderAnotherGenerationsRetirement is the POSITIVE CONTROL, and without it
// every mutation above could be "killed" by a classifier that simply refuses everything.
//
// Another generation of the lifecycle reading retired says nothing about ours, which the
// authority still serves. The release must deliver.
func TestPubRelProceedsUnderAnotherGenerationsRetirement(t *testing.T) {
	t.Parallel()
	b := testQoS2Setup(t, "f036-retire-outlived")
	pi := uint16(43)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("outlived-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	// A LATER generation of the lifecycle reads retired: the allocator moved past it.
	gen, _ := testPubRelSessionFacts(t, b)
	l, ord, _ := mqttParseGenToken(gen)
	testPlantPubRelRetirement(t, b, testLifecycleGen(t, l, ord+5), false)

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x00: another generation's retirement authorizes no refusal", reason)
	}
	b.consumeOne(t, "outlived-payload")
}

// TestPubRelProceedsWithNoRetirement is the other positive control: the ordinary case, where
// nothing retired.
func TestPubRelProceedsWithNoRetirement(t *testing.T) {
	t.Parallel()
	b := testQoS2Setup(t, "f036-retire-absent")
	pi := uint16(44)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("ordinary-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x00 with no retirement at all", reason)
	}
	b.consumeOne(t, "ordinary-payload")
}

// TestPubRelFailsClosedOnAnUnreadableRetirement is the THIRD outcome, and it is
// wire-distinguishable from the other refusal: no PUBCOMP at all, and BOTH records
// retained.
//
// That distinction is the whole point of testing it separately. A classifier that
// answered 0x92 here would free the packet identifier over a question it never decided,
// and the client's next PUBLISH on that identifier would meet the surviving hold.
//
// Mutations that must fail this: making the corrupt arm PROCEED (the witness receives
// the message); making it answer 0x92 instead of suppressing the PUBCOMP (this test's
// no-PUBCOMP assertion fails while the delivery assertion still passes, which is what
// makes the two mutations separately detectable).
func TestPubRelFailsClosedOnAnUnreadableRetirement(t *testing.T) {
	// NOT t.Parallel(): setMQTTTestRetirementLoadHook is a process-global atomic, so a
	// parallel sibling's classifier would read THIS test's corrupt watermark and refuse
	// a release it was asserting proceeds. A hook must not outlive — or reach outside —
	// its test.
	b := testQoS2Setup(t, "f036-retire-corrupt")
	pi := uint16(45)
	holdSubject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("undecidable-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	// An authority this broker cannot read. It is never read leniently, because it may say
	// the generation is no longer served. Scoped to the publisher's authority subject: the
	// hook is process-global.
	authSubj := mqttOutboundAuthoritySubject(getHash(b.pubID))
	restore := setMQTTTestLoadLastMsgHook(func(_, subject string) error {
		if subject == authSubj {
			return errors.New("injected: the authority read is indeterminate")
		}
		return nil
	})
	defer restore()

	testQoS2SendPubRel(t, b.cp, pi)
	b.expectNoPubComp(t)
	testQoS2ExpectNoDelivery(t, b)

	// BOTH records stay: the exchange is unfinished, so the client's retry must find
	// its payload and the claim must still fence a discard.
	restore()
	if rec, _, err := b.asm.loadQoS2Record(holdSubject); err != nil || rec == nil || rec.state != mqttQoS2Held {
		t.Fatalf("the hold must survive an undecided release: rec=%+v err=%v", rec, err)
	}
	claim, _, err := b.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(holdSubject))
	if err != nil {
		t.Fatalf("load the release claim: %v", err)
	}
	if claim == nil || claim.State != mqttQoS2RelStateReleasing {
		t.Fatalf("the release claim must survive an undecided release, got %+v", claim)
	}
}

// TestPubRelFailsClosedWhileOurOwnWriteIsUnresolved is the third input to the
// unresolved arm, and it is a distinct fact from an unreadable authority.
//
// While a write of ours is unresolved this session cannot say which record it is acknowledged
// for, so it does not answer for its generation either. The release fails closed, exactly as
// an unreadable authority does, and both records stay.
//
// Mutation: permitting seqUncertain to proceed must fail this test. Without it that
// mutation SURVIVES, because every other case has a resolved session.
func TestPubRelFailsClosedWhileOurOwnWriteIsUnresolved(t *testing.T) {
	t.Parallel()
	b := testQoS2Setup(t, "f036-retire-uncertain")
	pi := uint16(50)
	holdSubject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("uncertain-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	// A retirement IS recorded and it names this incarnation, so the durable state
	// alone would refuse with 0x92. The point is that an unresolved sequence must
	// suppress the answer entirely rather than produce the more specific one: the
	// broker cannot decide, and 0x92 would free the packet identifier over an
	// undecided question.
	gen, _ := testPubRelSessionFacts(t, b)
	testPlantPubRelRetirement(t, b, gen, false)

	sess := testMQTTGetClient(t, b.s, b.pubID).mqtt.sess
	sess.mu.Lock()
	sess.seqUncertain = true
	sess.mu.Unlock()
	defer func() {
		sess.mu.Lock()
		sess.seqUncertain = false
		sess.mu.Unlock()
	}()

	testQoS2SendPubRel(t, b.cp, pi)
	b.expectNoPubComp(t)
	testQoS2ExpectNoDelivery(t, b)

	if rec, _, err := b.asm.loadQoS2Record(holdSubject); err != nil || rec == nil || rec.state != mqttQoS2Held {
		t.Fatalf("the hold must survive a release whose own sequence is unresolved: rec=%+v err=%v", rec, err)
	}
	claim, _, err := b.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(holdSubject))
	if err != nil {
		t.Fatalf("load the release claim: %v", err)
	}
	if claim == nil || claim.State != mqttQoS2RelStateReleasing {
		t.Fatalf("the release claim must survive an undecided release, got %+v", claim)
	}
}

// TestPubRelClassifiesBeforeTheOnwardStore proves the ORDERING rather than the
// outcome, and it asserts REACH as well as order — a bare "did they happen in this
// sequence" check whose flags both default to false would pass against a classifier that
// never ran at all.
//
// Mutations that must fail this: moving the classifier before the release claim; moving
// it after the onward store.
func TestPubRelClassifiesBeforeTheOnwardStore(t *testing.T) {
	// NOT t.Parallel(): setMQTTTestQoS2RetireSampledHook is a process-global atomic, so
	// a parallel sibling's classification would set this test's ordering flags and this
	// test's callback would fire inside that sibling. Same reason as the corrupt case.
	b := testQoS2Setup(t, "f036-retire-order")
	pi := uint16(46)

	var claimed, classified, stored bool
	var classifiedAfterClaim, storedAfterClassify bool
	testQoS2SetPreClaim(t, b, func(string, string, uint64) error {
		claimed = true
		return nil
	})
	restore := setMQTTTestQoS2RetireSampledHook(func(mqttQoS2RetireVerdict) {
		classified = true
		classifiedAfterClaim = claimed
	})
	defer restore()
	b.setPreFinalize(t, func(string, string, uint64) error {
		stored = true
		storedAfterClassify = classified
		return nil
	})

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("ordered-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x00", reason)
	}
	b.consumeOne(t, "ordered-payload")

	// REACH first: each stage must actually have been entered. Without these three the
	// two ordering booleans below are satisfied by a run in which nothing happened.
	if !claimed {
		t.Fatal("the release claim was never taken, so this test proves nothing about ordering")
	}
	if !classified {
		t.Fatal("the retirement classifier never ran, so this test proves nothing about ordering")
	}
	if !stored {
		t.Fatal("the onward store was never reached, so this test proves nothing about ordering")
	}
	if !classifiedAfterClaim {
		t.Fatal("the retirement classifier ran BEFORE the release claim: it would then decide a window the claim has not fenced")
	}
	if !storedAfterClassify {
		t.Fatal("the onward store ran BEFORE the retirement classifier: the fan-out cannot be taken back")
	}
}

// TestPubRelMemoizesOnlyTheMatchingRetirement pins the cache boundary.
//
// A valid MATCHING observation is monotone — a generation never becomes rightful again,
// and the single slot can lose the fact's discoverability but not the fact — so it may be
// remembered. The negative outcomes may not: each can be invalidated by a retirement
// landing afterwards, and a finite window never arbitrates ownership.
//
// The test drives both directions on one connection: an ABSENT observation followed by a
// matching retirement must be re-read (the second release refuses), and once matched the
// fact survives the watermark being replaced by a later generation's (the third release
// still refuses, on a slot that no longer names us).
//
// Mutations that must fail this: memoizing a negative result (the second release
// proceeds and the witness receives the message).
func TestPubRelMemoizesOnlyTheMatchingRetirement(t *testing.T) {
	// NOT t.Parallel(): its last step installs the process-global load hook.
	b := testQoS2Setup(t, "f036-retire-memo")

	// 1. No watermark: the release proceeds, and this is the NEGATIVE observation that
	//    must not be cached.
	pi := uint16(47)
	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("memo-first"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("first PUBCOMP reason = 0x%x, want 0x00", reason)
	}
	b.consumeOne(t, "memo-first")

	// 2. The retirement lands. A cached "absent" would make this release proceed.
	gen, _ := testPubRelSessionFacts(t, b)
	testPlantPubRelRetirement(t, b, gen, false)
	pi = uint16(48)
	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("memo-second"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("second PUBCOMP reason = 0x%x, want 0x%x: a negative retirement observation must be RE-READ",
			reason, mqttReasonPacketIDNotFound)
	}
	testQoS2ExpectNoDelivery(t, b)

	// 3. The authority can no longer be read, so the durable state cannot speak at all. The
	//    remembered positive fact is what must still refuse.
	authSubj := mqttOutboundAuthoritySubject(getHash(b.pubID))
	restore := setMQTTTestLoadLastMsgHook(func(_, subject string) error {
		if subject == authSubj {
			return errors.New("injected: the authority read is indeterminate")
		}
		return nil
	})
	defer restore()
	pi = uint16(49)
	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("memo-third"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("third PUBCOMP reason = 0x%x, want 0x%x: a proven retirement is remembered past an unreadable authority",
			reason, mqttReasonPacketIDNotFound)
	}
	testQoS2ExpectNoDelivery(t, b)
}

// T-qos2-retired (issue #133): the bound session's whole lifecycle is RETIRED, its authority
// gone: the classification reads the registry and the release is refused, the hold released.
// Mutation M11: a missing authority read as Proceed.
func TestPubRelRefusesUnderARetiredLifecycle(t *testing.T) {
	t.Parallel()
	b := testQoS2Setup(t, "retired-lifecycle-pubrel")
	pi := uint16(43)
	holdSubject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("retired-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	gen, _ := testPubRelSessionFacts(t, b)
	// The lifecycle's authority is purged and the registry records the lifecycle RETIRED.
	if err := b.asm.jsa.purgeStream(mqttSessStreamName, mqttOutboundAuthoritySubject(getHash(b.pubID)), 0, false); err != nil {
		t.Fatalf("purging the authority: %v", err)
	}
	testRetireGeneration(t, b.asm, b.pubID, gen)
	if s, _ := b.asm.genStanding(b.pubID, gen); s != mqttGenRetired {
		t.Fatalf("fixture: generation %q reads %v, want retired by its lifecycle", gen, s)
	}

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x%x for a release whose lifecycle retired",
			reason, mqttReasonPacketIDNotFound)
	}
	testQoS2ExpectNoDelivery(t, b)
	if rec, _, err := b.asm.loadQoS2Record(holdSubject); err != nil || rec != nil {
		t.Fatalf("the hold survived a refused release: %+v (%v)", rec, err)
	}
}
