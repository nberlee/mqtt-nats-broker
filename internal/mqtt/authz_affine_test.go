//go:build mqttport_fulltests

// The affine shared-QoS-2 authorization fixture. It lives behind the fulltests
// tag because the raw MQTT 5 helpers it needs do, and it needs them: the
// interleaving it drives -- take the move, leave the exchange uncompleted, resume
// the same client ID under a different identity -- cannot be arranged through a
// high-level client.

package mqtt

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// authzConfAffine gives two identities in ONE account with DIFFERENT subscribe
// rights, which is what makes the affine deny arm reachable: the security
// principal is account plus client ID, so `bob` may resume a session `alice`
// created, and the copy already moved to that session's affine consumer is then
// redelivered under `bob`'s stricter ACL.
const authzConfAffine = `
port: -1
host: "127.0.0.1"
jetstream: enabled
accounts {
  APP: {
    jetstream: enabled
    users: [
      { user: svc, password: svcpw, permissions: { publish: ">", subscribe: ">" } }
      { user: alice, password: pw, permissions: { publish: ">", subscribe: ">" } }
      { user: bob, password: pw, permissions: {
          publish:   { allow: [">"] }
          subscribe: { allow: [">"], deny: ["aff.topic", "shared.msg"] }
      } }
      # carol's deny is QUEUE-SCOPED, and only the affine guard's queue source can
      # honour it: its own subscription carries a nil queue, so a guard reading
      # sub.queue sees nothing to match and lets the copy through.
      { user: carol, password: pw, permissions: {
          publish:   { allow: [">"] }
          subscribe: { allow: [">"], deny: ["aff.topic $mqttshare.>"] }
      } }
    ]
  }
}
`

// affineFlow parameterises one run of the interleaving.
type affineFlow struct {
	// firstUser resumes the owning session; thenUser resumes it again afterwards
	// (empty for a single resume).
	firstUser, thenUser string
	// gate, when set, is installed at the affine delivery guard for the FIRST
	// resume only and removed before the second. It is how the indeterminate arm
	// is reached; nil for every other arm.
	gate func(*client)
}

// affineOutcome is what one run observed. The claim subject is captured while the
// copy is still owed, because the disposal arms need to ask what happened to a
// record the disposal itself may legitimately delete.
type affineOutcome struct {
	firstDelivered, thenDelivered bool
	asm                           *mqttAccountSessionManager
	claimSubject                  string
}

// affineMovedCopyRedeliveredTo runs the whole interleaving once and reports
// whether the affine copy reached the identity that RESUMED the owning session.
//
// The flow: two members join the shared group as `alice`; a QoS 2 message is
// published and moved to whichever member the group chose; that member vanishes
// WITHOUT completing the exchange, so the copy is still owed; the same client ID
// then resumes as resumeUser, and its affine consumer redelivers.
func affineMovedCopyRedeliveredTo(t *testing.T, resumeUser string) bool {
	return runAffineFlow(t, affineFlow{firstUser: resumeUser}).firstDelivered
}

// affineMovedCopyRedelivered is the same flow, optionally resumed TWICE: once as
// firstUser and then as thenUser. The second resume is what makes the deny arm
// attributable, because it observes the DISPOSAL rather than the delivery. The
// guard's deny arm is terminal - it records AffineDone and acknowledges the copy -
// so a second resume by an ALLOWED identity must find nothing. Anything that
// merely withholds the copy leaves it owed, and that second resume receives it.
func affineMovedCopyRedelivered(t *testing.T, firstUser, thenUser string) bool {
	return runAffineFlow(t, affineFlow{firstUser: firstUser, thenUser: thenUser}).thenDelivered
}

func runAffineFlow(t *testing.T, f affineFlow) affineOutcome {
	t.Helper()
	_, srv := authzBroker(t, authzConfAffine)
	addr := srv.Addr().(*net.TCPAddr)
	host, port := addr.IP.String(), addr.Port

	// The WILDCARD is the point. F-081 is about a filter that OVERLAPS a narrower
	// deny, not one that matches it: bob is denied "aff.topic" but "aff/#" is
	// admissible, so his resume SUCCEEDS and the moved copy reaches the affine
	// delivery path with his snapshot. Subscribing to the exact denied subject
	// instead makes the resume fail at restore, upstream of the guard, and the test
	// then asserts a non-delivery with another cause -- which an earlier version of
	// this fixture did, and both guard mutations survived it.
	topic, filter, group := "aff/topic", "aff/#", "affgrp"
	join := func(id string) (net.Conn, *mqttReader) {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
			clientID: id, cleanStart: false, user: "alice", pass: "pw",
			props: mqttV5ConnPropsSessionExpiry(300),
		}, host, port)
		testMQTTReadConnAckV5(t, r)
		codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/" + group + "/" + filter, opts: 2}})
		if len(codes) != 1 || codes[0] != 2 {
			t.Fatalf("SUBACK for %s: %v", id, codes)
		}
		testMQTTFlush(t, c, nil, r)
		return c, r
	}

	ca, ra := join("affA")
	cb, rb := join("affB")
	defer cb.Close()

	pubc, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "affpub", cleanStart: true, user: "alice", pass: "pw",
	}, host, port)
	defer pubc.Close()
	testMQTTReadConnAckV5(t, pubr)
	// The FULL QoS 2 handshake: nothing is released to subscribers until PUBREL.
	testMQTTPublishV5(t, pubc, 2, 1, topic, []byte("affine"))
	if reason, _ := testMQTTReadPubRecV5(t, pubr, 1); reason != mqttReasonSuccess {
		t.Fatalf("PUBREC reason 0x%x", reason)
	}
	testQoS2SendPubRel(t, pubc, 1)
	if reason, _ := testMQTTReadPubCompV5(t, pubr, 1); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason 0x%x", reason)
	}

	var ownerID string
	var ownerConn net.Conn
	if _, _, _, _, ok := testMQTTTryReadPublishV5(ra, 3*time.Second); ok {
		ownerID, ownerConn = "affA", ca
	} else if _, _, _, _, ok := testMQTTTryReadPublishV5(rb, 3*time.Second); ok {
		ownerID, ownerConn = "affB", cb
	} else {
		ca.Close()
		t.Fatal("fixture: neither member received the shared QoS 2 delivery, so no affine copy exists")
	}
	// No PUBREC: the exchange is still owed, so the copy survives the disconnect
	// and is redelivered to whoever resumes this session.
	ownerConn.Close()

	// Captured while the exchange is still owed, which is the only moment the
	// claim is guaranteed to exist: a terminal disposal is entitled to reap it, so
	// asking afterwards cannot distinguish "reaped after AffineDone" from "never
	// created". Exactly one assignment exists at this point.
	out := affineOutcome{asm: asmOf(t, srv), claimSubject: soleQ2AffClaimSubject(t, asmOf(t, srv))}

	if f.gate != nil {
		// Installed for the FIRST resume only. Cleared before the second, and again
		// from Cleanup, because a hook that outlives its test fires inside another
		// one -- and this one mutates a live client.
		gate := f.gate
		mqttTestAffineGateV.Store(&gate)
		t.Cleanup(func() { mqttTestAffineGateV.Store(nil) })
	}

	cr, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: ownerID, cleanStart: false, user: f.firstUser, pass: "pw",
		props: mqttV5ConnPropsSessionExpiry(300),
	}, host, port)
	testMQTTReadConnAckV5(t, rr)
	_, _, _, _, out.firstDelivered = testMQTTTryReadPublishV5(rr, 5*time.Second)
	cr.Close()
	// After the close, so a delivery attempt still in flight on that connection
	// cannot slip past the gate the arm installed.
	mqttTestAffineGateV.Store(nil)
	if f.thenUser == _EMPTY_ {
		return out
	}

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: ownerID, cleanStart: false, user: f.thenUser, pass: "pw",
		props: mqttV5ConnPropsSessionExpiry(300),
	}, host, port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	_, _, _, _, out.thenDelivered = testMQTTTryReadPublishV5(r2, 5*time.Second)
	return out
}

// asmOf returns the broker's account session manager.
func asmOf(t *testing.T, s *Server) *mqttAccountSessionManager {
	t.Helper()
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatalf("account manager: %v", err)
	}
	return asm
}

// soleQ2AffClaimSubject returns the one assignment claim the flow created.
//
// It insists on exactly one rather than picking the first: with two the later
// assertions would be about an arbitrary claim, and with none the arm that follows
// would assert a disposition of a record that was never made.
func soleQ2AffClaimSubject(t *testing.T, asm *mqttAccountSessionManager) string {
	t.Helper()
	var subjects []string
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		inv, err := asm.collectQ2AffInventory()
		if err != nil {
			t.Fatalf("collect affine claims: %v", err)
		}
		subjects = subjects[:0]
		for _, e := range inv {
			subjects = append(subjects, e.subject)
		}
		if len(subjects) == 1 {
			return subjects[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("the flow must leave exactly ONE affine assignment claim, found %d: %v",
				len(subjects), subjects)
		}
	}
}

// assertAffineDispositionTerminal pins what a TERMINAL affine disposal wrote.
//
// Non-delivery is not the property: a guard that acknowledged the copy raw, without
// recording AffineDone, also delivers nothing to a later resume. It leaves the claim
// standing over an absent copy, which recovery reads as Lost -- and Lost is
// deliberately unreapable, so the claim accumulates forever while presenting itself
// as evidence that a message was lost. AffineDone is written BEFORE the copy is
// acked precisely so that cannot happen, and the reap that follows is licensed by
// it. So the record must be found either carrying AffineDone or already gone, and
// never Lost.
func assertAffineDispositionTerminal(t *testing.T, asm *mqttAccountSessionManager, claimSubject string) {
	t.Helper()
	var last string
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		claim, _, err := asm.loadQ2AffClaim(claimSubject)
		switch {
		case err != nil:
			last = fmt.Sprintf("load error: %v", err)
		case claim == nil:
			return // reaped, which only a recorded AffineDone licenses
		case claim.Lost:
			t.Fatalf("the denied copy was recorded LOST on %q: the guard acknowledged it "+
				"without writing AffineDone first, so recovery cannot tell a disposal from "+
				"a loss, and a Lost claim is never reaped", claimSubject)
		case claim.AffineDone:
			return
		default:
			last = fmt.Sprintf("AffineDone=false Lost=false Discarded=%v SourceAcked=%v",
				claim.Discarded, claim.SourceAcked)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the affine guard's DENY arm must record AffineDone before it acks the "+
				"copy; claim %q is still %s", claimSubject, last)
		}
	}
}

// assertAffineNothingRecorded pins what an INDETERMINATE affine verdict wrote:
// nothing. It is a fact about us, not about the message, so neither terminal state
// may appear -- AffineDone would assert a delivery that never happened, and Lost
// would launder a copy that is still sitting there into evidence of loss.
func assertAffineNothingRecorded(t *testing.T, asm *mqttAccountSessionManager, claimSubject string) {
	t.Helper()
	claim, _, err := asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("load claim %q: %v", claimSubject, err)
	}
	if claim == nil {
		t.Fatalf("the claim on %q was REAPED after an indeterminate verdict: an unusable "+
			"snapshot says nothing about the delivery, so nothing licenses reaping it",
			claimSubject)
	}
	if claim.AffineDone {
		t.Fatalf("an INDETERMINATE affine verdict recorded AffineDone on %q: it asserts a "+
			"delivery outcome that was never established, and the session-owned copy is "+
			"then acknowledged away from the resume that was entitled to it", claimSubject)
	}
	if claim.Lost {
		t.Fatalf("an INDETERMINATE affine verdict recorded Lost on %q: the copy is still "+
			"there and still owed", claimSubject)
	}
}

// TestAffineSharedQoS2HonoursASubscribeDenyOnResume drives the third delivery
// guard's DENY arm end to end.
//
// F-081 is about a filter that OVERLAPS a narrower deny, and that is what makes
// this arm reachable at all. `bob` is denied `aff.topic` but `$share/g/aff/#` is
// admissible, so he CAN resume a session `alice` created - the principal is
// account plus client ID - and the copy already moved to that session's affine
// consumer is then redelivered under his stricter ACL. Subscribing to the exact
// denied subject instead makes the resume fail at restore, upstream of the guard.
//
// Three arms, and each exists because the previous one is insufficient on its
// own. The CONTROL proves the copy is really owed and really redelivered. The
// DENY arm proves it does not reach a denied identity. The DISPOSAL arm is what
// makes the deny attributable to this guard: its deny arm is terminal - it records
// AffineDone and acknowledges - so a later resume by an allowed identity must find
// nothing, whereas anything that merely withheld the copy leaves it owed.
func TestAffineSharedQoS2HonoursASubscribeDenyOnResume(t *testing.T) {
	testRequireExperimentalSharedQoS2(t)

	t.Run("control: an allowed identity resuming the session receives the moved copy", func(t *testing.T) {
		if !affineMovedCopyRedeliveredTo(t, "alice") {
			t.Fatal("the affine copy must be redelivered to an ALLOWED identity resuming the " +
				"session; without this the deny arm asserts an absence with another cause")
		}
	})
	t.Run("an identity denied the subject does not receive it", func(t *testing.T) {
		if affineMovedCopyRedeliveredTo(t, "bob") {
			t.Fatal("the affine copy must not be delivered to an identity whose account denies " +
				"its subject through an overlapping wildcard: the ordinary guard ran before " +
				"the move, so this delivery is the affine guard's alone")
		}
	})
	t.Run("a QUEUE-SCOPED deny is honoured, which only the claim's queue can do", func(t *testing.T) {
		// The arm that pins the queue SOURCE. The affine subscription is created with
		// a nil queue, so a guard reading sub.queue would see no queue to match and
		// would let a queue-scoped deny through; recovering the group from the
		// claim's durable is what makes it enforceable. A subject-only deny cannot
		// show this - both sources deny alike - which is why carol exists.
		if affineMovedCopyRedeliveredTo(t, "carol") {
			t.Fatal("a queue-scoped subscribe deny must be honoured on the affine route: " +
				"its queue comes from the claim's group durable, because its own " +
				"subscription no longer names the group")
		}
	})
	t.Run("the denied copy is disposed of TERMINALLY, not merely withheld", func(t *testing.T) {
		out := runAffineFlow(t, affineFlow{firstUser: "bob", thenUser: "alice"})
		if out.thenDelivered {
			t.Fatal("a copy the affine guard denied must be terminally disposed of, not left " +
				"owed: it reappeared to an allowed identity resuming the same session")
		}
		// Absence is only half of the disposal. The other half is what was WRITTEN,
		// and no delivery assertion can see it: a raw ack with no AffineDone also
		// delivers nothing to the second resume, and leaves behind exactly the
		// unreapable false-loss record this asserts against.
		assertAffineDispositionTerminal(t, out.asm, out.claimSubject)
	})
	t.Run("an INDETERMINATE verdict withholds the copy and keeps it owed", func(t *testing.T) {
		// The third guard's other arm, and the one no external manipulation reaches:
		// a resuming client's snapshot is fresh by construction, and every way of
		// revoking it from outside also closes the connection the copy would have
		// arrived on. The seam makes the verdict indeterminate for the FIRST resume
		// only, so the second resume - an ordinary, fully authorized `alice` - is
		// what says whether the copy survived.
		//
		// The identity is `alice` on BOTH resumes deliberately. A denied identity
		// here would let a guard that ignored the indeterminate case entirely pass
		// through its deny arm and reach the same absence.
		invalidate := func(cc *client) {
			cc.mu.Lock()
			cc.authzValid = false
			cc.mu.Unlock()
		}
		out := runAffineFlow(t, affineFlow{firstUser: "alice", thenUser: "alice", gate: invalidate})
		if out.firstDelivered {
			t.Fatal("a copy whose authorization verdict is INDETERMINATE must be withheld: " +
				"an unusable snapshot cannot establish that this identity may receive it")
		}
		// Withheld, not consumed. This is the assertion the mutation dies on: replacing
		// the paced refusal with the deny arm's terminal disposal also delivers nothing
		// above, and only the surviving copy tells the two apart.
		if !out.thenDelivered {
			t.Fatal("a copy withheld on an INDETERMINATE verdict must still be owed: an " +
				"allowed identity resuming the same session did not receive it, so the " +
				"guard consumed a session-owned QoS 2 exchange it never judged")
		}
	})
	t.Run("a terminal deny frees the identifier it was holding", func(t *testing.T) {
		// The copy was TRANSMITTED before the disconnect, so the resume restores its
		// pendingPublish entry and its durable in-flight register from the register
		// stream. The guard then denies it and disposes of it terminally -- but the
		// guard runs BEFORE trackPublish, deliberately, so a refusal costs no NEW
		// identifier. That placement says nothing about the identifier the resume
		// already restored, and nothing else on the deny path is looking at it.
		//
		// Receive Maximum 1 is what makes the leak observable rather than theoretical:
		// one stranded entry is the whole quota, so the next ALLOWED delivery is
		// withheld for the life of the session. Non-delivery of the DENIED message
		// cannot see this at all, which is why the existing arms miss it.
		affineDenyFreesTheIdentifier(t)
	})
	t.Run("an INDETERMINATE verdict records neither terminal state", func(t *testing.T) {
		// Separate from the arm above because it observes a different moment: what the
		// claim holds WHILE the copy is still owed. Once the second resume completes,
		// AffineDone is legitimately there and says nothing about who wrote it.
		invalidate := func(cc *client) {
			cc.mu.Lock()
			cc.authzValid = false
			cc.mu.Unlock()
		}
		out := runAffineFlow(t, affineFlow{firstUser: "alice", gate: invalidate})
		if out.firstDelivered {
			t.Fatal("the seam did not make the verdict indeterminate; the rest of this arm " +
				"would then be inspecting an ordinary successful delivery")
		}
		assertAffineNothingRecorded(t, out.asm, out.claimSubject)
	})
}

// TestSharedDenyDoesNotConsumeForAllowedMembers is round 13's finding: a shared
// subscription whose selected member is denied the subject must not have the
// group's message ACKed away from members who ARE allowed it.
//
// The group consumer picks a member WITHOUT consulting its ACL, so a terminal ACK
// on a per-member deny turns one member's restriction into loss for the whole
// group. To make the offer land on the DENIED member deterministically -- rather
// than hoping the group picks it -- bob is the ONLY member when the message is
// stored; alice joins afterwards. Two observations then each make M35 (terminal ACK
// for shared) die: the consumer still holds the message unacknowledged after bob's
// refusals, and alice, an allowed member, is eventually delivered it.
//
// The second used to be a coin flip, and that is what made this test fail about one
// run in eight. nats-server routes each re-offer to a queue member chosen at random,
// and by the time alice joins the paced refusal backoff has grown to seconds per
// offer, so a fixed window after her join held about three offers, each going to bob
// with probability one half. Nothing was lost in those runs -- the message was still
// owed and reached alice on a later offer -- but the assertion said otherwise. So bob
// now LEAVES after alice has joined: with alice the only member every remaining offer
// is hers, and the receive window has to cover only one pending refusal delay.
func TestSharedDenyDoesNotConsumeForAllowedMembers(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 700 * time.Millisecond
	ns, srv := authzBrokerWithOpts(t, authzConfAffine, o)
	addr := srv.Addr().(*net.TCPAddr)
	host, port := addr.IP.String(), addr.Port

	group, topic, payload := "sharegrp", "shared/msg", []byte("m")
	sharedFilter := []mqttV5SubFilter{{topic: "$share/" + group + "/shared/#", opts: 1}}

	// bob is the SOLE member when the message is stored, so the group has nobody
	// else to offer it to and the denied path is certain to run. Joining alice
	// first would let the consumer satisfy the group without ever consulting an
	// ACL, which is the shape that let a terminal ACK survive the mutation.
	cb, rb := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "share-bob", cleanStart: true, user: "bob", pass: "pw",
	}, host, port)
	defer cb.Close()
	testMQTTReadConnAckV5(t, rb)
	if codes := testMQTTSubV5(t, cb, rb, 1, sharedFilter); len(codes) != 1 || codes[0] > 2 {
		t.Fatalf("SUBACK for share-bob: %v", codes)
	}
	testMQTTFlush(t, cb, nil, rb)

	pubc, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "share-pub", cleanStart: true, user: "alice", pass: "pw"}, host, port)
	defer pubc.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPublishV5(t, pubc, 1, 1, topic, payload)
	if reason, _ := testMQTTReadPubAckV5(t, pubr, 1); reason != mqttReasonSuccess {
		t.Fatalf("PUBACK reason 0x%x", reason)
	}

	// Nothing reaches bob, and the offer has to have been made to him -- he is the
	// only member -- so the disposition his guard chose is now the message's whole
	// fate. Wait past several AckWait periods so a terminal ACK has every chance to
	// consume it before alice can be offered anything.
	if _, _, _, _, ok := testMQTTTryReadPublishV5(rb, 3*time.Second); ok {
		t.Fatal("the denied member must be delivered nothing")
	}

	// How OFTEN it was re-offered, and this is the half no delivery assertion can
	// see. bob is the sole member, so there is nobody to hand the message to and no
	// event that resolves the refusal; a fixed hand-back delay is therefore not a
	// retry but an unbounded loop against MaxDeliver -1, driven by any client
	// permitted to hold such a subscription. Over the three seconds just spent, an
	// immediate NAK re-offers hundreds of times and the 50ms fixed delay this route
	// originally shipped with re-offers about sixty; the paced backoff re-offers at
	// 100ms, 200ms, 400ms, 800ms, 1600ms and then settles at its cap.
	if n := sharedGroupDeliveryCount(t, srv); n > 8 {
		t.Fatalf("the sole denied member was re-offered the group message %d times in three "+
			"seconds: the hand-back is not paced, so a permitted client can hold this "+
			"subscription and turn one denied subject into a sustained JetStream retry loop", n)
	}

	// The message is still OWED, read from the consumer itself. This is the
	// deterministic half of the M35 kill: an acknowledgement -- the terminal disposal
	// this route must not make -- clears the ack-pending entry and advances the ack
	// floor, and nothing else on this route does either. "shared/#" maps to two NATS
	// subjects ("shared" and "shared.>"), so the group has two durables, and only one
	// of them holds the message.
	consumers := sharedGroupConsumers(t, srv)
	if len(consumers) != 2 {
		t.Fatalf("expected the group's two durables (\"shared\" and \"shared.>\"), found %d", len(consumers))
	}
	var delivered uint64
	var ackPending int
	var deliverSubjects []string
	for _, ci := range consumers {
		delivered += ci.Delivered.Consumer
		ackPending += ci.NumAckPending
		if ci.AckFloor.Stream != 0 {
			t.Fatalf("durable %s advanced its ack floor to %d: the denied member's disposal "+
				"acknowledged the group's message", ci.Name, ci.AckFloor.Stream)
		}
		deliverSubjects = append(deliverSubjects, ci.Config.DeliverSubject)
	}
	if delivered == 0 {
		t.Fatal("the group never offered the message, so nothing below would be about bob's deny")
	}
	if ackPending != 1 {
		t.Fatalf("the group holds %d unacknowledged messages, want 1: the denied member's "+
			"disposal consumed the message instead of handing it back", ackPending)
	}

	// alice joins the SAME group. The SAME filter as bob, which is what puts them in
	// one group: the group durable is derived from the group name AND the filter, so
	// two members subscribing to different filters get two independent consumers and
	// each receives its own copy -- a shape in which bob's disposition could not
	// possibly affect alice, and the reason an earlier form of this test passed
	// against a terminal ACK.
	//
	// A re-offer can reach her before her SUBACK, so the join reads through any
	// PUBLISH and keeps it rather than assuming packet order.
	ca, ra := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "share-alice", cleanStart: true, user: "alice", pass: "pw",
	}, host, port)
	defer ca.Close()
	testMQTTReadConnAckV5(t, ra)
	testMQTTSubV5NoSubAck(t, ca, 1, 0, sharedFilter)
	received := false
	for subAcked := false; !subAcked; {
		b, pl, ok := testMQTTReadPacketReady(ra, 10*time.Second)
		if !ok {
			t.Fatal("share-alice: no SUBACK")
		}
		switch b & mqttPacketMask {
		case mqttPacketSubAck:
			start := ra.pos
			if pi, err := ra.readUint16("suback pi"); err != nil || pi != 1 {
				t.Fatalf("share-alice: SUBACK pi %d (%v)", pi, err)
			}
			if _, err := ra.readProperties(mqttPacketSubAck); err != nil {
				t.Fatalf("share-alice: SUBACK properties: %v", err)
			}
			if code, err := ra.readByte("suback reason code"); err != nil || code > 2 || ra.pos-start != pl {
				t.Fatalf("share-alice: SUBACK reason 0x%x (%v)", code, err)
			}
			subAcked = true
		case mqttPacketPub:
			ra.pos = ra.pstart
			gotTopic, gotPayload, _, _, ok := testMQTTTryReadPublishV5(ra, time.Second)
			if !ok || gotTopic != topic || string(gotPayload) != string(payload) {
				t.Fatalf("share-alice: unexpected PUBLISH %q %q", gotTopic, gotPayload)
			}
			received = true
		default:
			t.Fatalf("share-alice: unexpected packet 0x%x before SUBACK", b)
		}
	}

	// bob leaves, and the leave has to be a fact on the server, not a closed socket:
	// the broker forgets the client before it unsubscribes its queue members. Two
	// members first, which is what makes the second wait observe a departure rather
	// than an interest that was never there; and alice must be in before bob goes,
	// because the last holder leaving retires the group's consumer -- a different
	// mechanism from the one under test.
	acc, err := ns.LookupAccount("APP")
	if err != nil {
		t.Fatalf("lookup account: %v", err)
	}
	membersAre := func(want int) func() error {
		return func() error {
			for _, deliver := range deliverSubjects {
				if n := acc.Interest(deliver); n != want {
					return fmt.Errorf("%d members on %s, want %d", n, deliver, want)
				}
			}
			return nil
		}
	}
	checkFor(t, 10*time.Second, 20*time.Millisecond, membersAre(2))
	testMQTTDisconnectEx(t, cb, nil, false)
	cb.Close()
	checkFor(t, 10*time.Second, 20*time.Millisecond, membersAre(1))

	// alice is now the only member, so every remaining offer is hers. She can only
	// be offered a message that still exists, so receiving it proves bob's guard
	// handed it back rather than acknowledging it away -- the end-to-end half of the
	// M35 kill. The window has to cover one refusal delay scheduled before bob left,
	// whichever delivery count it was paced at, and that delay is at most the cap.
	if !received {
		if gotTopic, gotPayload, _, _, ok := testMQTTTryReadPublishV5(ra, mqttPacedRefusalMaxDelay+5*time.Second); !ok {
			t.Fatal("an allowed shared member joining after a DENIED member was offered the group " +
				"message must still receive it: a terminal ACK on the deny destroyed it")
		} else if gotTopic != topic || string(gotPayload) != string(payload) {
			t.Fatalf("share-alice: unexpected PUBLISH %q %q", gotTopic, gotPayload)
		}
	}
}

// authzBrokerWithOpts is authzBroker with caller-supplied MQTT options, for the
// few fixtures that need a short AckWait to force a shared-group redelivery. It
// returns the embedded nats-server too, because queue membership lives there: a
// shared member's departure is a fact only once the server's interest drops.
func authzBrokerWithOpts(t *testing.T, conf string, o *Options) (ns *natsserver.Server, s *Server) {
	t.Helper()
	path := t.TempDir() + "/authz.conf"
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	opts, err := natsserver.ProcessConfigFile(path)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	opts.NoSigs = true
	opts.StoreDir = t.TempDir()
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	url := srv.ClientURL()
	svc, err := nats.Connect(url, nats.UserInfo("svc", "svcpw"))
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}
	t.Cleanup(svc.Close)
	o.MQTT.Host, o.MQTT.Port = "127.0.0.1", -1
	s = NewServer(svc, o, "authz-opts-node")
	s.SetClientDialer(func(_, user, pass string) (*nats.Conn, error) {
		return nats.Connect(url, nats.UserInfo(user, pass))
	})
	if err := s.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(s.Shutdown)
	return srv, s
}

// sharedGroupConsumers returns the CONSUMER's own accounting for every shared durable on
// the message stream.
//
// It reads the consumer rather than counting what arrived on a connection, because the
// thing being measured is precisely the traffic that reaches no connection at all: a
// refused offer is re-delivered and re-refused entirely between the broker and JetStream.
// A subscriber-side assertion is blind to it, which is how a retry storm hides behind a
// passing non-delivery test -- and how a terminal ACK hides behind a late delivery.
//
// The listing is the error-reporting one. The legacy ConsumersInfo channel swallows a
// listing failure, which would read as "no shared consumer delivered anything" and pass a
// pacing bound vacuously.
func sharedGroupConsumers(t *testing.T, s *Server) []*jetstream.ConsumerInfo {
	t.Helper()
	js, err := jetstream.New(s.nrc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, mqttStreamName)
	if err != nil {
		t.Fatalf("stream %s: %v", mqttStreamName, err)
	}
	lister := stream.ListConsumers(ctx)
	var shared []*jetstream.ConsumerInfo
	for ci := range lister.Info() {
		if strings.HasPrefix(ci.Name, mqttSharedConsumerDurablePrefix) {
			shared = append(shared, ci)
		}
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("list consumers of %s: %v", mqttStreamName, err)
	}
	return shared
}

// sharedGroupDeliveryCount is how many times the shared group's consumers have delivered,
// summed over every shared durable on the message stream.
func sharedGroupDeliveryCount(t *testing.T, s *Server) uint64 {
	t.Helper()
	var total uint64
	for _, ci := range sharedGroupConsumers(t, s) {
		total += ci.Delivered.Consumer
	}
	return total
}

// affineDenyFreesTheIdentifier is round 18's finding, driven end to end.
//
// The setup is the ordinary affine interleaving, with one difference that is the
// whole point: the resumed connection STAYS OPEN and is asked to carry an ordinary
// allowed delivery afterwards. Every other arm closes it as soon as it has observed
// the denied copy's absence, and an absence is exactly what a wedged quota also
// produces -- so those arms cannot tell "denied and disposed of cleanly" from
// "denied and left holding the session's only identifier".
func affineDenyFreesTheIdentifier(t *testing.T) {
	t.Helper()
	_, srv := authzBroker(t, authzConfAffine)
	addr := srv.Addr().(*net.TCPAddr)
	host, port := addr.IP.String(), addr.Port

	topic, filter, group := "aff/topic", "aff/#", "affgrp"
	join := func(id string) (net.Conn, *mqttReader) {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
			clientID: id, cleanStart: false, user: "alice", pass: "pw",
			props: mqttV5ConnPropsSessionExpiry(300),
		}, host, port)
		testMQTTReadConnAckV5(t, r)
		codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/" + group + "/" + filter, opts: 2}})
		if len(codes) != 1 || codes[0] != 2 {
			t.Fatalf("SUBACK for %s: %v", id, codes)
		}
		testMQTTFlush(t, c, nil, r)
		return c, r
	}
	ca, ra := join("wedgeA")
	cb, rb := join("wedgeB")
	defer cb.Close()

	pubc, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "wedgepub", cleanStart: true, user: "alice", pass: "pw",
	}, host, port)
	defer pubc.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPublishV5(t, pubc, 2, 1, topic, []byte("affine"))
	if reason, _ := testMQTTReadPubRecV5(t, pubr, 1); reason != mqttReasonSuccess {
		t.Fatalf("PUBREC reason 0x%x", reason)
	}
	testQoS2SendPubRel(t, pubc, 1)
	if reason, _ := testMQTTReadPubCompV5(t, pubr, 1); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason 0x%x", reason)
	}

	var ownerID string
	var ownerConn net.Conn
	if _, _, _, _, ok := testMQTTTryReadPublishV5(ra, 3*time.Second); ok {
		ownerID, ownerConn = "wedgeA", ca
	} else if _, _, _, _, ok := testMQTTTryReadPublishV5(rb, 3*time.Second); ok {
		ownerID, ownerConn = "wedgeB", cb
	} else {
		ca.Close()
		t.Fatal("fixture: neither member received the shared QoS 2 delivery, so no affine copy exists")
	}
	// TRANSMITTED and unacknowledged. That is what makes the resume restore a
	// pendingPublish entry and an in-flight register for this identifier, which is
	// the state the denial has to clean up and the reason a never-transmitted copy
	// would prove nothing here.
	ownerConn.Close()

	// Receive Maximum 1: the restored identifier is the entire quota.
	props := append(mqttV5ConnPropsReceiveMax(1), mqttV5ConnPropsSessionExpiry(300)...)
	cr, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: ownerID, cleanStart: false, user: "bob", pass: "pw", props: props,
	}, host, port)
	defer cr.Close()
	testMQTTReadConnAckV5(t, rr)

	// An ORDINARY allowed subscription on the same connection. bob's deny covers
	// aff.topic and shared.msg only, so this one is his to receive.
	codes := testMQTTSubV5(t, cr, rr, 9, []mqttV5SubFilter{{topic: "other/#", opts: 1}})
	if len(codes) != 1 || codes[0] > 2 {
		t.Fatalf("SUBACK for the allowed filter: %v", codes)
	}
	testMQTTFlush(t, cr, nil, rr)

	// The denied copy is withheld, as the other arms already establish.
	if _, _, _, _, ok := testMQTTTryReadPublishV5(rr, 3*time.Second); ok {
		t.Fatal("the denied affine copy must not be delivered to bob")
	}

	// The assertion this arm exists for. If the terminal denial left the restored
	// entry in pendingPublish, qos12InflightForCap is still 1 of 1 and this delivery
	// is withheld forever -- with no error, no log the client sees, and nothing that
	// another disconnect would not have hidden.
	testMQTTPublishV5(t, pubc, 1, 2, "other/msg", []byte("allowed"))
	if reason, _ := testMQTTReadPubAckV5(t, pubr, 2); reason != mqttReasonSuccess {
		t.Fatalf("PUBACK for the allowed publish: 0x%x", reason)
	}
	if _, _, _, _, ok := testMQTTTryReadPublishV5(rr, 10*time.Second); !ok {
		t.Fatal("an ALLOWED QoS 1 delivery was withheld from the resumed session: the " +
			"terminal denial disposed of the copy but kept the packet identifier the " +
			"resume had restored for it, so Receive Maximum stays exhausted for the " +
			"life of the session and every later delivery is silently withheld")
	}
}

// TestPlainDenyOnResumeFreesTheRestoredIdentifier is the same leak as the affine
// arm, on the ORDINARY QoS 1/2 route.
//
// Both subscribe-deny guards sit before trackPublish, deliberately, so that a
// refusal costs no new packet identifier. Neither placement says anything about an
// identifier a PREVIOUS transmission already took: a message sent before a
// disconnect leaves a durable in-flight register, the resume restores it into the
// tracking maps before any guard runs, and a guard that only disposed of the
// message would leave the session counting it against Receive Maximum forever.
//
// It gets its own fixture rather than a parameter on the affine one because the two
// routes reach their guard by entirely different paths -- the affine copy through
// its claim on the service connection, this one through the ordinary delivery
// callback -- and a shared fixture would have proved whichever route it happened to
// take.
func TestPlainDenyOnResumeFreesTheRestoredIdentifier(t *testing.T) {
	_, srv := authzBroker(t, authzConfAffine)
	addr := srv.Addr().(*net.TCPAddr)
	host, port := addr.IP.String(), addr.Port

	// A PLAIN subscription, no shared group. The wildcard is what makes the deny
	// reachable on resume: bob is denied aff.topic, but "aff/#" is admissible, so
	// his CONNECT and his restored subscription both succeed and the message reaches
	// the delivery guard under his snapshot.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "plainwedge", cleanStart: false, user: "alice", pass: "pw",
		props: mqttV5ConnPropsSessionExpiry(300),
	}, host, port)
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "aff/#", opts: 1}}); len(codes) != 1 || codes[0] > 2 {
		t.Fatalf("SUBACK: %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	pubc, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "plainwedgepub", cleanStart: true, user: "alice", pass: "pw",
	}, host, port)
	defer pubc.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPublishV5(t, pubc, 1, 1, "aff/topic", []byte("first"))
	if reason, _ := testMQTTReadPubAckV5(t, pubr, 1); reason != mqttReasonSuccess {
		t.Fatalf("PUBACK: 0x%x", reason)
	}

	// Transmitted and NOT acknowledged, which is what leaves the durable register
	// the resume will restore. A message that never reached the wire proves nothing
	// here: its register carries a cancellation tombstone and recovery frees the
	// identifier outright.
	if _, _, _, _, ok := testMQTTTryReadPublishV5(r, 5*time.Second); !ok {
		t.Fatal("fixture: the allowed identity must receive the message before the disconnect, " +
			"or there is no restored identifier for the deny to strand")
	}
	c.Close()

	props := append(mqttV5ConnPropsReceiveMax(1), mqttV5ConnPropsSessionExpiry(300)...)
	cr, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "plainwedge", cleanStart: false, user: "bob", pass: "pw", props: props,
	}, host, port)
	defer cr.Close()
	testMQTTReadConnAckV5(t, rr)
	if codes := testMQTTSubV5(t, cr, rr, 9, []mqttV5SubFilter{{topic: "other/#", opts: 1}}); len(codes) != 1 || codes[0] > 2 {
		t.Fatalf("SUBACK for the allowed filter: %v", codes)
	}
	testMQTTFlush(t, cr, nil, rr)

	if _, _, _, _, ok := testMQTTTryReadPublishV5(rr, 3*time.Second); ok {
		t.Fatal("the denied message must not be redelivered to bob")
	}

	testMQTTPublishV5(t, pubc, 1, 2, "other/msg", []byte("allowed"))
	if reason, _ := testMQTTReadPubAckV5(t, pubr, 2); reason != mqttReasonSuccess {
		t.Fatalf("PUBACK for the allowed publish: 0x%x", reason)
	}
	if _, _, _, _, ok := testMQTTTryReadPublishV5(rr, 10*time.Second); !ok {
		t.Fatal("an ALLOWED QoS 1 delivery was withheld from the resumed session: the deny " +
			"acked the message away but kept the packet identifier the resume had restored " +
			"for it, so Receive Maximum stays exhausted and every later delivery is " +
			"silently withheld for the life of the session")
	}
}

// TestDeniedWillNeverReachesTheDurableOutbox and its sibling below cover the two
// discharge sites the immediate-Will fixture in authz_test.go does not reach.
//
// A per-site mutation census established that it reaches exactly ONE of the four:
// forcing the immediate site's verdict true fails it, while forcing the
// delayed-outbox, takeover and uncertainty-parked sites' verdicts true all survived.
// That is what these arms are for -- "the decision is read at every discharge site"
// is four claims, and a fixture that reaches one of them proves one of them.
//
// This arm is the DELAYED one, and its observable is durable rather than delivered:
// a denied Will must never be written to $MQTT_willq at all. Asserting non-delivery
// instead would pass against a broker that enqueued the record and refused it later,
// which is a different guarantee -- the record is read by every replica's claim
// loop, so a denied Will parked there is a denied Will that any other broker may
// still decide to publish.
func TestDeniedWillNeverReachesTheDurableOutbox(t *testing.T) {
	_, srv := authzBroker(t, authzConfOneAccount)
	addr := srv.Addr().(*net.TCPAddr)
	host, port := addr.IP.String(), addr.Port

	// A live client, purely so the counter can reach the account manager.
	live, liveR := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "willq-live", cleanStart: true, user: "alice", pass: "pw",
	}, host, port)
	defer live.Close()
	testMQTTReadConnAckV5(t, liveR)

	connectDelayedWill := func(id, topic string) {
		t.Helper()
		will := &mqttWill{topic: []byte(topic), message: []byte("bye"), qos: 0}
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
			clientID: id, cleanStart: false, user: "alice", pass: "pw",
			will: will, willProps: mqttV5WillDelayProps(30),
			props: mqttV5ConnPropsSessionExpiry(120),
		}, host, port)
		testMQTTReadConnAckV5(t, r)
		// Ungraceful: no DISCONNECT, so the Will is owed.
		c.Close()
	}

	// CONTROL first, and it is not optional: the delayed path writes the record from
	// a teardown goroutine, so "no record" is also what a broker that never enqueues
	// anything produces, and what a fixture whose Will was never armed produces.
	connectDelayedWill("willq-allowed", "wq/allowed")
	deadline := time.Now().Add(10 * time.Second)
	for testMQTTNumPendingWills(t, srv, "willq-live") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("control: an AUTHORIZED delayed Will must be recorded in $MQTT_willq; " +
				"without it the absence asserted below has another cause")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// THE DEFECT: same flow, a subject the account denies publish on.
	connectDelayedWill("willq-denied", "denied/will")
	// The would-be write races this check, so the absence has to hold for a settle
	// period rather than at one instant.
	for waitUntil := time.Now().Add(3 * time.Second); time.Now().Before(waitUntil); time.Sleep(100 * time.Millisecond) {
		if n := testMQTTNumPendingWills(t, srv, "willq-live"); n != 1 {
			t.Fatalf("a DENIED delayed Will was written to $MQTT_willq (pending count %d, want the "+
				"control's 1): the outbox record is read by every replica's claim loop, so parking "+
				"a denied Will there hands the publication to whichever broker fires it", n)
		}
	}
}

// TestQueueScopedDenyOnOrdinarySharedDelivery pins the QUEUE argument on ordinary
// shared QoS 1/2 delivery.
//
// A queue-scoped deny is serialised by $SYS.REQ.USER.INFO as "<subject> <queue>",
// so a guard that passes an empty queue silently downgrades it to a subject-only
// check -- and a subject-only check does not match, so the message is DELIVERED to
// a member the account denied. That is an authorization bypass, not a coverage gap.
// Until these existed the only end-to-end queue-scoped case exercised the affine
// guard, whose queue comes from a different source entirely (the claim's group
// durable), so it could not speak for either of these.
//
// carol is the identity for both: her deny is `aff.topic $mqttshare.>`, which is
// queue-scoped, so a subject-only evaluation of it denies nothing at all.
func TestQueueScopedDenyOnOrdinarySharedDelivery(t *testing.T) {
	_, srv := authzBroker(t, authzConfAffine)
	addr := srv.Addr().(*net.TCPAddr)
	host, port := addr.IP.String(), addr.Port

	group := "qsgrp"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "qs-carol", cleanStart: true, user: "carol", pass: "pw",
	}, host, port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	// QoS 1, so this is the ORDINARY shared route: the affine machinery only takes
	// over a QoS 2 exchange, and using QoS 2 here would test the guard that is
	// already covered.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{
		{topic: "$share/" + group + "/aff/#", opts: 1},
	}); len(codes) != 1 || codes[0] > 2 {
		t.Fatalf("SUBACK: %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	pubc, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "qs-pub", cleanStart: true, user: "alice", pass: "pw",
	}, host, port)
	defer pubc.Close()
	testMQTTReadConnAckV5(t, pubr)
	publish := func(pi uint16, topic string) {
		t.Helper()
		testMQTTPublishV5(t, pubc, 1, pi, topic, []byte("m"))
		if reason, _ := testMQTTReadPubAckV5(t, pubr, pi); reason != mqttReasonSuccess {
			t.Fatalf("PUBACK for %s: 0x%x", topic, reason)
		}
	}

	// CONTROL, and it is sent FIRST so a denied message left pending cannot occupy
	// the group consumer ahead of it. carol's deny names aff.topic specifically, so
	// this sibling subject under the same filter and the same group must arrive --
	// which is what proves the group, the membership and the delivery path are live
	// before anything is asserted absent.
	publish(1, "aff/other")
	if topic, _, _, _, ok := testMQTTTryReadPublishV5(r, 10*time.Second); !ok || topic != "aff/other" {
		t.Fatalf("control: an undenied subject on the same shared filter must be delivered "+
			"(ok=%v topic=%q); without it the absence below has another cause", ok, topic)
	}

	// THE DEFECT: the queue-scoped deny must be honoured on this route too.
	publish(2, "aff/topic")
	if topic, _, _, _, ok := testMQTTTryReadPublishV5(r, 3*time.Second); ok {
		t.Fatalf("a QUEUE-SCOPED subscribe deny was not honoured on ordinary shared delivery "+
			"(delivered %q): the guard must pass the subscription's queue, because "+
			"$SYS.REQ.USER.INFO serialises such a deny as \"subject queue\" and a "+
			"subject-only evaluation of it matches nothing and admits the message", topic)
	}
}
