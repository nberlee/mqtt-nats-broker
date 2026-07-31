//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// End-to-end coverage for the two fences that serialize an inbound QoS 2 release
// against a session discard (qos2_release_claim.go).
//
// These are the behavioral pins: each one drives a real PUBLISH/PUBREL exchange
// and asserts on what a WITNESS SUBSCRIBER received, because the property at stake
// is "was this message delivered", which no amount of record inspection can settle.
// The unit-level tables live in qos2_release_claim_test.go.

// setPreClaim installs the load..claim seam, which is the only way to park a
// releaser exactly where a real race lands.
func testQoS2SetPreClaim(t *testing.T, b *testQoS2Broker, fn func(holdSubject, gen string, holdSeq uint64) error) {
	t.Helper()
	b.asm.mu.Lock()
	b.asm.testQoS2PreClaim = fn
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQoS2PreClaim = nil
		b.asm.mu.Unlock()
	})
}

// testQoS2ExpectNoDelivery asserts the witness subscriber received nothing. It is
// the assertion the whole feature exists to make true, so it must not be able to
// pass merely because the broker was slow: the caller has already observed the
// PUBCOMP, so a delivery — which the broker fans out BEFORE it answers — would
// already have arrived.
func testQoS2ExpectNoDelivery(t *testing.T, b *testQoS2Broker) {
	t.Helper()
	testMQTTExpectNothingFor(t, b.rs, 250*time.Millisecond)
}

// TestQoS2ReleaseDiscardedMidClaimDeliversNothing is THE behavioral pin for this
// feature. A PUBREL whose session is discarded inside the load..claim window must
// answer 0x92 and deliver nothing.
//
// Before the claim existed, the discard's blanket purge competed with nothing: the
// PUBREL had already read the held record, so it went on to fan out, and its
// finalize then read the record's absence as mqttQoS2FinalizeGone — "the exchange
// completed". The message was delivered out of a session already reported
// discarded, and the broker recorded that as success.
//
// Mutation: remove the claim (or let a lost claim proceed) and the witness receives
// the message.
func TestQoS2ReleaseDiscardedMidClaimDeliversNothing(t *testing.T) {
	b := testQoS2Setup(t, "relclaim-discard")
	pi := uint16(31)
	holdSubject := b.holdSubject(t, pi)

	// A peer broker discards this session while our PUBREL sits between its load and
	// its claim: it writes the discard marker and destroys the hold, in that order.
	var discarded bool
	testQoS2SetPreClaim(t, b, func(subject, _ string, _ uint64) error {
		if subject != holdSubject || discarded {
			return nil
		}
		discarded = true
		if _, err := b.jsa.storeQoS2RelClaimCAS(mqttQoS2ClaimSubjectFor(subject), 0,
			mqttNewQoS2RelClaim(mqttQoS2RelStateDiscarded, _EMPTY_, "peer")); err != nil {
			t.Errorf("peer discard claim: %v", err)
		}
		if err := b.jsa.purgeStream(mqttQoS2IncomingMsgsStreamName, subject, false); err != nil {
			t.Errorf("peer discard purge: %v", err)
		}
		return nil
	})

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("discarded-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)

	// 0x92: the identifier's state is gone, which is the honest answer for a session
	// that has been discarded.
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x%x", reason, mqttReasonPacketIDNotFound)
	}
	if !discarded {
		t.Fatal("the seam never fired: the race this test exists for was not exercised")
	}
	testQoS2ExpectNoDelivery(t, b)

	// The discard marker must survive. It is the only durable proof that a late
	// releaser must lose, so reaping it would re-open the window.
	cur, _, err := b.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(holdSubject))
	if err != nil {
		t.Fatalf("load claim: %v", err)
	}
	if cur == nil || cur.State != mqttQoS2RelStateDiscarded {
		t.Fatalf("claim = %+v, want the discard marker to survive", cur)
	}
}

// TestQoS2ReleaseStalledLoaderDeliversNothing pins fence 2, which the claim alone
// cannot provide.
//
// A releaser that stalls after loading the held record, while a PEER completes the
// entire exchange and reaps both the tombstone and the claim, finds an EMPTY claim
// subject when it resumes — so its create-only claim SUCCEEDS. Without the
// authoritative re-read it would then fan out a second time from the payload it
// still holds in memory, and this is not a new hazard: it is one the pre-change
// code had too, since its finalize read the absence as Gone.
//
// Mutation: drop the re-read (or accept its Gone/Delivered rows without checking
// the generation) and the witness receives the message twice.
func TestQoS2ReleaseStalledLoaderDeliversNothing(t *testing.T) {
	b := testQoS2Setup(t, "relclaim-stalled")
	pi := uint16(32)
	holdSubject := b.holdSubject(t, pi)

	var completed bool
	testQoS2SetPreClaim(t, b, func(subject, gen string, holdSeq uint64) error {
		if subject != holdSubject || completed {
			return nil
		}
		completed = true
		// A peer completes the whole exchange: commit the terminal tombstone at the
		// held record's sequence, then reap it. It leaves no claim behind, because a
		// completed release removes its own.
		doneSeq, err := b.jsa.storeQoS2DoneCAS(subject, gen, holdSeq)
		if err != nil {
			t.Errorf("peer terminal commit: %v", err)
			return nil
		}
		if err := b.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, doneSeq, true); err != nil {
			t.Errorf("peer tombstone reap: %v", err)
		}
		return nil
	})

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("stalled-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)
	testQoS2SendPubRel(t, b.cp, pi)

	// The peer completed the exchange, so this releaser owes the client a completion
	// and owes the topic nothing.
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x%x", reason, mqttReasonSuccess)
	}
	if !completed {
		t.Fatal("the seam never fired: the race this test exists for was not exercised")
	}
	testQoS2ExpectNoDelivery(t, b)
}

// TestQoS2ReleaseAdoptsItsOwnClaimAcrossRetries pins that the claim is
// DISCARD-exclusive and not RELEASER-exclusive. The generation identifies the
// exchange, not the broker, so a second releaser (or a retry) that finds its own
// generation must proceed rather than treat the claim as a conflict — otherwise a
// PUBREL retry after any failure could never complete.
func TestQoS2ReleaseAdoptsItsOwnClaimAcrossRetries(t *testing.T) {
	b := testQoS2Setup(t, "relclaim-adopt")
	pi := uint16(33)
	holdSubject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("adopted-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	// Plant a claim carrying the exchange's OWN generation, as a peer mid-release (or
	// this broker's own crashed attempt) would have left.
	rec, _, err := b.jsa.loadQoS2Record(holdSubject)
	if err != nil || rec == nil {
		t.Fatalf("load hold record: rec=%v err=%v", rec, err)
	}
	if _, err := b.jsa.storeQoS2RelClaimCAS(mqttQoS2ClaimSubjectFor(holdSubject), 0,
		mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, rec.gen, "peer")); err != nil {
		t.Fatalf("plant our own generation's claim: %v", err)
	}

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x%x", reason, mqttReasonSuccess)
	}

	// The message IS delivered: an adopted claim is not a lost race.
	b.consumeOne(t, "adopted-payload")

	// And the completed release removed the claim it adopted, so nothing is left to
	// confuse a later exchange on this identifier.
	cur, smsg, err := b.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(holdSubject))
	if err != nil {
		t.Fatalf("load claim: %v", err)
	}
	if cur != nil || smsg != nil {
		t.Fatalf("a completed release must remove its claim, found %+v", cur)
	}
}

// TestQoS2ReleaseForeignClaimIsReapedAndRetried pins the leftover case: a claim
// from an EARLIER exchange on the same packet identifier, whose releaser died,
// must not block the current one forever. It is reaped with a confirmed delete and
// the claim is retried.
func TestQoS2ReleaseForeignClaimIsReapedAndRetried(t *testing.T) {
	b := testQoS2Setup(t, "relclaim-foreign")
	pi := uint16(34)
	holdSubject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("foreign-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	if _, err := b.jsa.storeQoS2RelClaimCAS(mqttQoS2ClaimSubjectFor(holdSubject), 0,
		mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "dead-broker")); err != nil {
		t.Fatalf("plant a dead releaser's claim: %v", err)
	}

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x%x", reason, mqttReasonSuccess)
	}
	b.consumeOne(t, "foreign-payload")
}

// TestQoS2ReleaseUnreadableClaimAnswersGoneAndKeepsTheRecord pins the disposition
// of a claim we cannot attribute: deliver nothing, answer 0x92, and LEAVE the
// record. It may be a corrupted discard decision, and reaping it would let a peer
// win a fresh claim over a message this broker just declined to deliver.
//
// The hold record itself is released with a confirmed delete before the client is
// answered, because a PUBCOMP frees the packet identifier (invariant 3).
func TestQoS2ReleaseUnreadableClaimAnswersGoneAndKeepsTheRecord(t *testing.T) {
	b := testQoS2Setup(t, "relclaim-unreadable")
	pi := uint16(35)
	holdSubject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("unreadable-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	if _, err := b.jsa.storeMsg(mqttQoS2ClaimSubjectFor(holdSubject), 0, []byte("{not a claim")); err != nil {
		t.Fatalf("plant an unreadable claim: %v", err)
	}

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x%x", reason, mqttReasonPacketIDNotFound)
	}
	testQoS2ExpectNoDelivery(t, b)

	if _, smsg, err := b.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(holdSubject)); err != nil || smsg == nil {
		t.Fatalf("the unreadable claim must be kept, not reaped: smsg=%v err=%v", smsg, err)
	}
	// The hold record is gone, so the identifier is reusable — which is what makes
	// answering the client safe.
	if rec, _, err := b.jsa.loadQoS2Record(holdSubject); err != nil || rec != nil {
		t.Fatalf("the hold record must be released before the client is answered, got rec=%v err=%v", rec, err)
	}
}

// TestQoS2ReleaseIndeterminateClaimSuppressesPubComp pins that a claim we cannot
// resolve neither answers the client nor delivers: the exchange is unfinished, the
// hold record is intact, and the client retries against session state that is
// still whole. MQTT 5 has no PUBCOMP reason for "the broker failed".
func TestQoS2ReleaseIndeterminateClaimSuppressesPubComp(t *testing.T) {
	b := testQoS2Setup(t, "relclaim-indeterminate")
	pi := uint16(36)
	holdSubject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("indeterminate-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	b.asm.mu.Lock()
	b.asm.testQoS2ClaimStore = func(string, uint64, *mqttQoS2RelClaim) (uint64, error) {
		return 0, errors.New("claim store unavailable")
	}
	b.asm.mu.Unlock()

	testQoS2SendPubRel(t, b.cp, pi)
	b.expectNoPubComp(t)
	testQoS2ExpectNoDelivery(t, b)

	b.asm.mu.Lock()
	b.asm.testQoS2ClaimStore = nil
	b.asm.mu.Unlock()

	// The exchange is still whole, so the client's retry completes it normally.
	if rec, _, err := b.jsa.loadQoS2Record(holdSubject); err != nil || rec == nil || rec.state != mqttQoS2Held {
		t.Fatalf("the hold record must survive an unresolved claim, got rec=%v err=%v", rec, err)
	}
	cp, rp := b.reconnectPublisher(t)
	testQoS2SendPubRel(t, cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x%x", reason, mqttReasonSuccess)
	}
	b.consumeOne(t, "indeterminate-payload")
}

// TestQoS2DupPublishWhileClaimedStillAnswersPubRec pins that an outstanding claim
// does not disturb the PUBLISH path: a DUP PUBLISH is still answered PUBREC from
// the existing held record, and neither record is rewritten. The claim is a
// sibling register, so the hold record's payload, properties and store time — the
// last of which ages the Message Expiry Interval — stay exactly as the original
// PUBLISH left them.
func TestQoS2DupPublishWhileClaimedStillAnswersPubRec(t *testing.T) {
	b := testQoS2Setup(t, "relclaim-dup")
	pi := uint16(37)
	holdSubject := b.holdSubject(t, pi)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("dup-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	before, beforeSeq, err := b.jsa.loadQoS2Record(holdSubject)
	if err != nil || before == nil {
		t.Fatalf("load hold record: rec=%v err=%v", before, err)
	}
	claimSeq, err := b.jsa.storeQoS2RelClaimCAS(mqttQoS2ClaimSubjectFor(holdSubject), 0,
		mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, before.gen, "peer"))
	if err != nil {
		t.Fatalf("plant a claim: %v", err)
	}

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("dup-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	after, afterSeq, err := b.jsa.loadQoS2Record(holdSubject)
	if err != nil || after == nil {
		t.Fatalf("reload hold record: rec=%v err=%v", after, err)
	}
	if afterSeq != beforeSeq || after.gen != before.gen || after.state != mqttQoS2Held {
		t.Fatalf("the hold record changed under a DUP PUBLISH: seq %d->%d, gen %q->%q, state %v",
			beforeSeq, afterSeq, before.gen, after.gen, after.state)
	}
	if !before.stored.Time.Equal(after.stored.Time) {
		t.Fatalf("the hold record's store time moved (%v -> %v); the Message Expiry Interval would restart",
			before.stored.Time, after.stored.Time)
	}
	cur, smsg, err := b.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(holdSubject))
	if err != nil || cur == nil || smsg.Sequence != claimSeq {
		t.Fatalf("the claim must be untouched by a DUP PUBLISH: cur=%+v err=%v", cur, err)
	}
}

// TestQoS2ClearRefusesReleaseOnTheSameBroker pins the in-process fence. Across
// brokers the durable claim is the only arbiter, but a discard on THIS broker can
// refuse to admit a release rather than race it — so no delivery can straddle the
// discard locally.
//
// Mutation: remove the gate from mqttProcessPubRel and the message is delivered
// after the session was discarded.
func TestQoS2ClearRefusesReleaseOnTheSameBroker(t *testing.T) {
	b := testQoS2Setup(t, "relclaim-fence")
	pi := uint16(38)

	testMQTTPublishV5(t, b.cp, 2, pi, b.topic, []byte("fenced-payload"))
	testMQTTReadPubRecV5(t, b.rp, pi)

	sess := testMQTTGetClient(t, b.s, b.pubID).mqtt.sess
	sess.closeQoS2ReleaseAndDrain()
	t.Cleanup(sess.reopenQoS2ReleaseFence)

	testQoS2SendPubRel(t, b.cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, b.rp, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("PUBCOMP reason = 0x%x, want 0x%x", reason, mqttReasonPacketIDNotFound)
	}
	testQoS2ExpectNoDelivery(t, b)
}
