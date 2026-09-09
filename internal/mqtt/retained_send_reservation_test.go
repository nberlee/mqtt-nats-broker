// Copyright 2025 The mqtt-nats-bridge Authors
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"
)

// F-091: a retained QoS 1/2 send reserves its packet identifier.
//
// A retained send is the one identifier-bearing transmission with no JetStream backing
// and therefore no in-flight register (R6): it allocates in serializeRetainedMsgsForSub,
// is buffered on sub.mqtt.prm, and is written by a LATER queueOutbound. Across that span
// its only other reservation is the pendingPublish entry, and clear() destroys exactly
// that while resetting last_pi — so without a hold in sendsInFlight the identifier is
// handed straight back out while the displaced connection's write is still in the socket.

// testRetainedEnv is an inflight env whose client can actually own a session: the env's
// own client comes from createInternalAccountClient, which never calls initClient, so it
// carries neither an MQTT state nor a nonzero cid.
type testRetainedEnv struct {
	*testInflightEnv
}

func testNewRetainedEnv(t *testing.T, clientID string) *testRetainedEnv {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	testConnIdentity(t, e, e.c)
	e.sess.c = e.c
	return &testRetainedEnv{e}
}

// buffer drives the PRODUCTION allocation route — serializeRetainedMsgsForSub — rather
// than calling the primitives, so what it proves is about that function's own wiring.
// Returns the identifier the retained frame was framed under.
func (e *testRetainedEnv) buffer(t *testing.T, topic string, qos byte) (*subscription, uint16) {
	t.Helper()
	subject, _ := mqttToNATSSubjectConversion([]byte(topic), false)
	rm := &mqttRetainedMsg{
		Subject: string(subject),
		Topic:   topic,
		Msg:     []byte("payload"),
		Flags:   qos << 1,
	}
	e.asm.addRetainedMsg(string(subject), 1, rm)

	sub := &subscription{client: e.c, subject: subject, sid: subject, mqtt: &mqttSub{qos: qos}}
	rms := map[string]*mqttRetainedMsg{string(subject): rm}

	e.asm.mu.Lock()
	e.sess.mu.Lock()
	e.asm.serializeRetainedMsgsForSub(rms, e.sess, e.c, sub, false)
	e.sess.mu.Unlock()
	e.asm.mu.Unlock()

	e.c.mu.Lock()
	frames := sub.mqtt.prm
	e.c.mu.Unlock()
	if len(frames) != 1 {
		t.Fatalf("the production serializer must buffer exactly one retained frame, got %d", len(frames))
	}
	if frames[0].pi == 0 {
		t.Fatalf("a QoS %d retained frame must carry an allocated identifier", qos)
	}
	return sub, frames[0].pi
}

// reserved asks the one question every allocator asks.
func (e *testRetainedEnv) reserved(pi uint16) bool {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	return e.sess.sendReservedLocked(pi)
}

func (e *testRetainedEnv) holdPresent(pi uint16) bool {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	_, ok := e.sess.sendsInFlight[pi]
	return ok
}

// dropEntry removes the retained pendingPublish entry WITHOUT re-minting the incarnation,
// which is what a PUBACK for that frame does. Used where a test needs bumpPI's answer to
// depend on the hold alone and clear()'s re-mint would change the question.
func (e *testRetainedEnv) dropEntry(pi uint16) {
	e.sess.mu.Lock()
	delete(e.sess.pendingPublish, pi)
	e.sess.mu.Unlock()
}

// mints reports whether bumpPI will hand pi back out. It asks the real allocator, because
// "is this identifier reserved" is exactly the question the allocator answers and a test
// reading fields instead would not notice a predicate that stopped being consulted.
func (e *testRetainedEnv) mints(t *testing.T, want uint16) bool {
	t.Helper()
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	saved := e.sess.last_pi
	e.sess.last_pi = want - 1
	got := e.sess.bumpPI()
	e.sess.last_pi = saved
	return got == want
}

// The defect. A buffered retained frame's identifier must survive the discard that
// destroys its pendingPublish entry, and — the half revision 3 of the plan got wrong —
// must survive the WRITE that follows the discard too, because past a clear() the hold is
// the identifier's only reservation.
//
// The seam is what makes the interleaving reachable at all: queueOutbound returns far too
// fast to be raced from a fixture, so a test without it would have to call the primitives
// and would prove nothing about the flusher's wiring.
//
// Deliberately NOT t.Parallel(): the seam is a process-global atomic and
// TestParallelTestsTouchNoProcessGlobalState enforces that.
func TestMQTTRetainedSendHoldsItsIdentifierAcrossADiscard(t *testing.T) {
	e := testNewRetainedEnv(t, "retained-discard")
	sub, pi := e.buffer(t, "foo/bar", 1)

	if !e.reserved(pi) {
		t.Fatal("a buffered retained frame must reserve its identifier the moment it is allocated")
	}

	// That the seam fires BEFORE any write is asserted STRUCTURALLY, by
	// TestMQTTRetainedFlushSeamPrecedesEveryWrite, not from inside the hook. An earlier
	// version tried to observe it here with a counter the hook read and the test assigned
	// afterwards — which observes nothing, and made a mutation that moves the seam below the
	// first write appear to be caught when it was not. Holistic round 7 found that false
	// kill.
	blocked := make(chan struct{})
	release := make(chan struct{})
	restore := setMQTTTestRetainedFlushBlock(func(subs []*subscription) {
		close(blocked)
		<-release
	})
	defer restore()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.c.mqttSendRetainedMsgsToNewSubs([]*subscription{sub})
	}()
	// BOUNDED, and that is the point rather than caution: a seam that never fires would
	// otherwise leave this fixture blocked until go test's own panic timeout, and a hung
	// test reports "failed" without ever having evaluated an assertion. A mutation that
	// disables the seam would then look exactly like a mutation the oracle caught.
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("the retained flush seam never fired, so nothing below was evaluated: the hook " +
			"has moved out of mqttSendRetainedMsgsToNewSubs or is no longer consulted")
	}

	// The discard: pendingPublish is nilled and last_pi reset, while sess.c is still this
	// connection. From here the hold is the identifier's ONLY reservation.
	// A client is BOUND here, so the discard runs the production two-phase
	// retirement (detach before the re-mint) rather than a bare clear().
	if err := mqttTestRetireBound(t, e.asm, e.sess, true); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if !e.reserved(pi) {
		t.Fatal("clear() must not free a retained identifier whose frame is still unwritten: " +
			"the write is in the socket and bumpPI would mint a second PUBLISH under it")
	}
	if e.mints(t, pi) {
		t.Fatal("bumpPI handed out the identifier of an unwritten retained frame across a discard")
	}

	close(release)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the retained flush did not return after the seam was released")
	}

	// The half a write-return release would get wrong. The write has returned, but the
	// incarnation that allocated the frame is gone with the entry that would have carried
	// custody on to PUBACK, so the hold stays.
	if !e.reserved(pi) {
		t.Fatal("a retained frame written AFTER a clear() must keep its identifier reserved: " +
			"the pendingPublish entry that would otherwise hold it was destroyed, and this " +
			"connection's own delivery callbacks can still re-mint and write to its socket")
	}

	// It becomes free when ownership moves, and the rebind's sweep collects the entry.
	succ := &client{srv: e.s}
	testConnIdentity(t, e.testInflightEnv, succ)
	e.sess.mu.Lock()
	e.sess.c = succ
	e.sess.sweepDeadSendHoldsLocked()
	e.sess.mu.Unlock()

	if e.reserved(pi) {
		t.Fatal("once the connection is no longer the session's, no additional PUBLISH under that " +
			"identifier can reach its socket, so the hold must reserve nothing")
	}
	if e.holdPresent(pi) {
		t.Fatal("the rebind's sweep must collect an inert retained hold")
	}
}

// The flush seam must precede EVERY write in the flusher, asserted over the syntax tree
// because it is a property of the code's shape and cannot be observed from inside the hook.
//
// This is what makes the seam's position load-bearing rather than incidental: the window
// TestMQTTRetainedSendHoldsItsIdentifierAcrossADiscard holds open is "reserved, framed,
// nothing written yet", and a seam that had drifted below the first queueOutbound would hold
// open a different, uninteresting window while still letting that fixture pass.
func TestMQTTRetainedFlushSeamPrecedesEveryWrite(t *testing.T) {
	t.Parallel()
	const flusher = "mqttSendRetainedMsgsToNewSubs"
	found := false
	for _, file := range testMQTTProductionFiles(t) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range f.Decls {
			fn, _ := decl.(*ast.FuncDecl)
			if fn == nil || fn.Body == nil || fn.Name.Name != flusher {
				continue
			}
			found = true
			var seam, firstWrite token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, _ := n.(*ast.CallExpr)
				if call == nil {
					return true
				}
				if sel, _ := call.Fun.(*ast.SelectorExpr); sel != nil {
					if sel.Sel.Name == "Load" {
						if id, _ := sel.X.(*ast.Ident); id != nil &&
							id.Name == "mqttTestRetainedFlushBlockV" && seam == token.NoPos {
							seam = call.Pos()
						}
					}
					if sel.Sel.Name == "queueOutbound" && firstWrite == token.NoPos {
						firstWrite = call.Pos()
					}
				}
				return true
			})
			if seam == token.NoPos {
				t.Errorf("%s no longer consults mqttTestRetainedFlushBlockV, so the interleaving "+
					"test's window is unreachable and it passes vacuously", flusher)
				continue
			}
			if firstWrite == token.NoPos {
				t.Errorf("%s performs no queueOutbound, so this guard checks nothing", flusher)
				continue
			}
			if seam > firstWrite {
				t.Errorf("%s: the flush seam is consulted AFTER the first queueOutbound: the "+
					"interleaving test would then hold open a window in which bytes have already "+
					"reached the client, which is not the window it asserts about",
					fset.Position(seam))
			}
		}
	}
	if !found {
		t.Errorf("%s was not found, so this guard checks nothing", flusher)
	}
}

// The ordinary path still releases on the write's return, so the generation fence is a
// fence rather than "never release". Without this companion, M16 and M17 would be
// indistinguishable from correct code.
func TestMQTTRetainedSendReleasesOnTheWriteWithoutADiscard(t *testing.T) {
	e := testNewRetainedEnv(t, "retained-ordinary")
	sub, pi := e.buffer(t, "foo/bar", 1)

	e.c.mqttSendRetainedMsgsToNewSubs([]*subscription{sub})

	if e.holdPresent(pi) {
		t.Fatal("with the allocating incarnation still current, the written frame's hold has " +
			"nothing left to do: pendingPublish carries custody on to the PUBACK")
	}
	// Custody did not vanish with the hold — the entry still has it, which is the
	// pre-F-091 behaviour and the correct one here.
	if e.mints(t, pi) {
		t.Fatal("the pendingPublish entry must still reserve the identifier until the client " +
			"acknowledges: releasing the hold is not releasing the identifier")
	}
	// And once the entry goes the way a PUBACK takes it, the identifier is allocatable —
	// so the released hold really is released and not merely invisible.
	e.dropEntry(pi)
	if !e.mints(t, pi) {
		t.Fatal("with the hold released and the entry acknowledged away, the identifier must be " +
			"allocatable: a hold that never releases is a permanently stranded identifier")
	}
}

// The buffered span alone, with no flush at all. A reservation taken at the flush instead
// of at the allocation would leave this window open, and it is a real window: the SUBACK
// tail runs after the SUBACK write, and a concurrent CONNECT can discard in between.
func TestMQTTRetainedSendHoldSurvivesTheBufferedSpan(t *testing.T) {
	t.Parallel()
	e := testNewRetainedEnv(t, "retained-buffered")
	_, pi := e.buffer(t, "foo/bar", 2)

	// A client is BOUND here, so the discard runs the production two-phase
	// retirement (detach before the re-mint) rather than a bare clear().
	if err := mqttTestRetireBound(t, e.asm, e.sess, true); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if !e.reserved(pi) {
		t.Fatal("a retained frame that has not been flushed at all must still reserve its identifier")
	}
	if e.mints(t, pi) {
		t.Fatal("bumpPI handed out a buffered retained frame's identifier after a discard")
	}
}

// beginRetainedSendLocked's clauses, tested directly. The busy-identifier clauses are
// unreachable from the serializer — bumpPI already skips every live hold and the
// allocation and the reservation share one sess.mu acquisition — so an end-to-end fixture
// could not establish them, and a defensive primitive still owes its own test.
func TestMQTTRetainedSendReservationRefusesABusyIdentifier(t *testing.T) {
	t.Parallel()
	e := testNewRetainedEnv(t, "retained-refuse")

	begin := func(cc *client, pi uint16) bool {
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		return e.sess.beginRetainedSendLocked(cc, pi)
	}

	if begin(nil, 7) {
		t.Error("a nil connection must be refused")
	}
	if begin(e.c, 0) {
		t.Error("identifier zero must be refused")
	}
	zero := &client{srv: e.s, mqtt: &mqtt{asm: e.asm, sess: e.sess}}
	if zero.cid != 0 {
		t.Fatal("this case needs an uninitialized connection identity")
	}
	if begin(zero, 7) {
		t.Error("a ZERO connection identity must be refused, not recorded: two of them would be " +
			"indistinguishable to every ownership comparison")
	}
	foreign := &client{srv: e.s}
	testConnIdentity(t, e.testInflightEnv, foreign)
	if begin(foreign, 7) {
		t.Error("a connection that is not the session's must be refused")
	}

	// A live write token.
	e.sess.mu.Lock()
	e.sess.sendsInFlight = map[uint16]*mqttSendHold{8: {token: "live"}}
	e.sess.mu.Unlock()
	if begin(e.c, 8) {
		t.Error("an identifier with a transmission still out must be refused")
	}

	// A live PUBCOMP debt on this very connection.
	e.sess.mu.Lock()
	e.sess.sendsInFlight[9] = &mqttSendHold{pubCompOwed: 1, owner: e.c.cid}
	e.sess.mu.Unlock()
	if begin(e.c, 9) {
		t.Error("an identifier still owing a PUBCOMP must be refused")
	}

	// An ALREADY-LIVE retained reservation. One identifier per retained message, so a
	// second live one is an invariant violation rather than a case to serve.
	if !begin(e.c, 10) {
		t.Fatal("a free identifier must be reservable")
	}
	if begin(e.c, 10) {
		t.Error("a second retained reservation on one identifier must be refused, not accepted: " +
			"each retained PUBLISH allocates its own")
	}
}

// The oversize discard is the one place a RESERVED retained frame is provably never even
// buffered, so it releases unconditionally — the generation fence does not apply, because
// no bytes reached the client.
func TestMQTTRetainedOversizeFrameReleasesItsReservation(t *testing.T) {
	t.Parallel()
	e := testNewRetainedEnv(t, "retained-oversize")
	e.c.mqtt.maxPacketSize = 4

	subject, _ := mqttToNATSSubjectConversion([]byte("foo/bar"), false)
	rm := &mqttRetainedMsg{
		Subject: string(subject), Topic: "foo/bar",
		Msg: []byte("a payload far beyond four bytes"), Flags: 1 << 1,
	}
	e.asm.addRetainedMsg(string(subject), 1, rm)
	sub := &subscription{client: e.c, subject: subject, sid: subject, mqtt: &mqttSub{qos: 1}}

	e.asm.mu.Lock()
	e.sess.mu.Lock()
	e.asm.serializeRetainedMsgsForSub(map[string]*mqttRetainedMsg{string(subject): rm}, e.sess, e.c, sub, false)
	e.sess.mu.Unlock()
	e.asm.mu.Unlock()

	e.c.mu.Lock()
	frames := len(sub.mqtt.prm)
	e.c.mu.Unlock()
	if frames != 0 {
		t.Fatalf("an oversize retained frame must be discarded, got %d buffered", frames)
	}
	e.sess.mu.Lock()
	entries, holds := len(e.sess.pendingPublish), len(e.sess.sendsInFlight)
	e.sess.mu.Unlock()
	if entries != 0 {
		t.Errorf("the oversize discard must release the pending entry, %d left", entries)
	}
	if holds != 0 {
		t.Errorf("the oversize discard must release the RESERVATION too, %d hold(s) left: "+
			"a frame that was never buffered will never be written, so nothing else ever releases it",
			holds)
	}
}

// A late release from a departed connection must not release its SUCCESSOR's reservation
// on the same identifier. The same identifier is the point: a test using an unrelated one
// is never visited by the release loop at all and would pass against an owner-blind
// release.
func TestMQTTRetainedReleaseRespectsTheOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		release func(e *testRetainedEnv, a *client, pi uint16)
	}{
		{"written", func(e *testRetainedEnv, a *client, pi uint16) {
			e.sess.endRetainedSends(a, []uint16{pi})
		}},
		// The abandoned verb needs its own case: the written one is fenced by the
		// generation and would refuse here for that reason instead of the owner's, so a
		// single case cannot tell the two guards apart.
		{"abandoned", func(e *testRetainedEnv, a *client, pi uint16) {
			e.sess.abandonRetainedSends(a, []uint16{pi})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewRetainedEnv(t, "retained-owner-"+tc.name)
			a := e.c
			_, pi := e.buffer(t, "foo/bar", 1)

			// A departs; B binds and takes the now-inert hold over with its own retained
			// allocation on that very identifier.
			b := &client{srv: e.s}
			testConnIdentity(t, e.testInflightEnv, b)
			e.sess.mu.Lock()
			e.sess.c = b
			ok := e.sess.beginRetainedSendLocked(b, pi)
			e.sess.mu.Unlock()
			if !ok {
				t.Fatal("the successor must be able to take an inert hold over")
			}

			tc.release(e, a, pi)

			if !e.reserved(pi) {
				t.Fatal("a departed connection's late release consumed its successor's reservation")
			}
		})
	}
}

// An inert hold is TAKEN OVER, and the takeover records the NEW owner. Inheriting the
// predecessor's owner instead leaves the fresh reservation inert on arrival, so the
// identifier is allocatable while the new frame is still buffered — which is worse than
// the strand it was meant to avoid.
func TestMQTTRetainedSendTakesOverAnInertHoldExactly(t *testing.T) {
	t.Parallel()
	e := testNewRetainedEnv(t, "retained-takeover")
	a := e.c
	_, pi := e.buffer(t, "foo/bar", 1)

	b := &client{srv: e.s}
	testConnIdentity(t, e.testInflightEnv, b)
	e.sess.mu.Lock()
	e.sess.c = b
	inert := e.sess.sendReservedLocked(pi)
	ok := e.sess.beginRetainedSendLocked(b, pi)
	owner := e.sess.sendsInFlight[pi].owner
	e.sess.mu.Unlock()

	if inert {
		t.Fatal("the predecessor's hold must be inert once ownership moved")
	}
	if !ok {
		t.Fatal("an inert hold must be replaceable")
	}
	if owner != b.cid {
		t.Fatalf("the takeover must record the NEW owner, got %d want %d", owner, b.cid)
	}
	if !e.reserved(pi) {
		t.Fatal("the successor's own retained reservation must be LIVE: a takeover that inherited " +
			"the predecessor's owner would leave it inert and the identifier allocatable")
	}
	_ = a
}

// A PUBCOMP must not destroy a live retained reservation.
//
// This is the surviving half of what round 1 found: a new term in sendHoldLiveLocked changes
// what every reader and mutator of sendsInFlight MEANS, and consumePubRelAckLocked re-spelled
// the condition instead of asking the predicate — so it deleted the whole hold on
// `token == "" && pubCompOwed == 0`, taking a buffered retained frame's only reservation with
// it. Asking the shared predicate is the fix, and this is its oracle.
//
// The other half of round 1's finding — a retained-term REFUSAL in beginPubRelSendLocked —
// was implemented, then REFUTED across holistic rounds 8, 9 and 10 and DELETED (lane
// DECISIONS.md, D4). F-092 later put durable admission in the delivery callback rather than
// this primitive, so a PUBREL token/action and PUBCOMP debt may still legitimately sit
// beside a retained term on one hold; what this test asserts is that discharging one does
// not discharge the other.
func TestMQTTPubCompDoesNotDestroyARetainedReservation(t *testing.T) {
	t.Parallel()
	e := testNewRetainedEnv(t, "retained-pubcomp")
	_, pi := e.buffer(t, "foo/bar", 1)

	e.sess.mu.Lock()
	h := e.sess.sendsInFlight[pi]
	if h == nil || h.retainedGen == _EMPTY_ {
		e.sess.mu.Unlock()
		t.Fatal("the buffered frame must hold a retained reservation")
	}
	// A PUBREL transmitted under this identifier, then answered.
	h.pubCompOwed = 1
	e.sess.consumePubRelAckLocked(e.c, pi)
	_, present := e.sess.sendsInFlight[pi]
	owed := 0
	if hh := e.sess.sendsInFlight[pi]; hh != nil {
		owed = hh.pubCompOwed
	}
	e.sess.mu.Unlock()

	if !present {
		t.Fatal("a PUBCOMP consumed the whole hold and with it a buffered retained frame's only " +
			"reservation: the delete must ask the shared liveness predicate, not re-spell it")
	}
	if owed != 0 {
		t.Fatalf("the PUBCOMP must still consume its own debt, got %d owed", owed)
	}
	if !e.reserved(pi) {
		t.Fatal("the retained reservation must still be live after the PUBCOMP")
	}
}

// A frame that will never be written is ABANDONED at a TEARDOWN departure, releasing its
// reservation, because otherwise the identifier stays reserved against reallocation for the
// session's life while nothing will ever write it. (The other departure, a same-sid
// replacement, TRANSFERS instead and releases nothing —
// TestMQTTRetainedFramesDetachOnSameSidReplacement and the deadlock test cover that.)
func TestMQTTAbandonedRetainedFrameReleasesItsHold(t *testing.T) {
	t.Parallel()
	e := testNewRetainedEnv(t, "retained-abandon")
	sub, pi := e.buffer(t, "foo/bar", 1)

	e.c.mu.Lock()
	if e.c.subs == nil {
		e.c.subs = make(map[string]*subscription)
	}
	e.c.subs[string(sub.sid)] = sub
	e.c.mu.Unlock()

	if err := e.c.processUnsub(sub.sid); err != nil {
		t.Fatalf("processUnsub: %v", err)
	}

	e.c.mu.Lock()
	frames := len(sub.mqtt.prm)
	e.c.mu.Unlock()
	if frames != 0 {
		t.Fatalf("a departing subscription's retained frames must be detached, %d left", frames)
	}
	if e.reserved(pi) {
		t.Fatal("an abandoned retained frame must release its reservation: nothing will ever write it")
	}

	// Nothing is asserted about PUBREL admission here, and that is deliberate: the
	// retained-term refusal this test used to exercise was REFUTED and deleted (D4), so a
	// PUBREL under this identifier is admitted exactly as it was before F-091. What
	// abandonment is FOR is that the identifier stops being reserved against reallocation
	// once nothing will ever write the frame.
	if e.mints(t, pi) {
		t.Fatal("this fixture's pre-existing empty pendingPublish entry should still reserve the " +
			"identifier, so the assertion above is about the HOLD alone")
	}
	e.dropEntry(pi)
	if !e.mints(t, pi) {
		t.Fatal("with the reservation abandoned and the entry gone the identifier must be " +
			"allocatable: a hold that never releases is a permanently stranded identifier")
	}
}

// The same-sid replacement THROUGH ITS REAL ROUTE, with the session lock held exactly as
// production holds it. This is the test that was missing, and its absence let a
// deterministic self-deadlock through six plan-review rounds and a green suite.
//
// mqttProcessSubs takes as.mu AND sess.mu and holds both across sess.processSub
// (mqtt.go:8167), whose own contract says "Session lock held on entry", and that reaches
// client.processSub. A release issued there through the LOCKING wrapper re-enters a
// non-reentrant sess.mu and wedges the read loop while it still holds the account lock —
// so the identifiers are handed back to the lock-aware caller instead.
//
// BOUNDED, and that is the whole design of the assertion: a deadlock makes a test hang,
// and a hang reports "failed" without ever evaluating an oracle. Failing on a timeout with
// a message that names the deadlock is what makes this a real check rather than a wedge.
func TestMQTTSameSidResubscribeUnderTheSessionLockDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	e := testNewRetainedEnv(t, "retained-resub-lock")

	subject, _ := mqttToNATSSubjectConversion([]byte("foo/bar"), false)
	rm := &mqttRetainedMsg{
		Subject: string(subject), Topic: "foo/bar", Msg: []byte("payload"), Flags: 1 << 1,
	}
	e.asm.addRetainedMsg(string(subject), 1, rm)
	rms := map[string]*mqttRetainedMsg{string(subject): rm}

	// Both filters of one SUBSCRIBE packet, mapping to the same sid — which
	// mqttParseSubsOrUnsubs permits, since it appends every filter with no dedup. The
	// first buffers a retained frame; the second REPLACES the subscription carrying it.
	sub := func() *subscription {
		e.asm.mu.Lock()
		e.sess.mu.Lock()
		defer func() {
			e.sess.mu.Unlock()
			e.asm.mu.Unlock()
		}()
		s, err := e.sess.processSub(e.c, subject, subject, nil, false, 1, false, false, false, 0,
			_EMPTY_, mqttDeliverMsgCbQoS0, false, rms, false, e.asm)
		if err != nil {
			t.Fatalf("processSub: %v", err)
		}
		return s
	}

	done := make(chan *subscription, 2)
	go func() {
		done <- sub()
		done <- sub()
	}()

	for i := range 2 {
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatalf("sess.processSub call %d never returned: the retained-frame release re-entered "+
				"sess.mu, which mqttProcessSubs already holds across this call, and a Go mutex is "+
				"not reentrant — the read loop is wedged holding as.mu too", i+1)
		}
	}

	// And the displaced frame was TRANSFERRED, not discarded. This is the other half of the
	// round-4 finding: a v5 SUBSCRIBE may legitimately carry one filter twice with the
	// first entry asking for the replay, so releasing the displaced frames would silently
	// drop a retained delivery the client is owed.
	e.c.mu.Lock()
	surviving := e.c.subs[string(subject)]
	var frames int
	if surviving != nil && surviving.mqtt != nil {
		frames = len(surviving.mqtt.prm)
	}
	e.c.mu.Unlock()
	if frames != 2 {
		t.Fatalf("the replacement must carry BOTH the transferred frame and its own, got %d: "+
			"releasing the displaced frame instead discards a retained replay the client is owed", frames)
	}

	e.sess.mu.Lock()
	held, entries := 0, len(e.sess.pendingPublish)
	for _, h := range e.sess.sendsInFlight {
		if h.retainedGen != _EMPTY_ {
			held++
		}
	}
	e.sess.mu.Unlock()
	if held != 2 || entries != 2 {
		t.Fatalf("both surviving frames must still reserve their identifiers and hold their entries: "+
			"holds=%d entries=%d want 2 and 2", held, entries)
	}
	_ = sub
}

// The detach-and-ABANDON helper pair, which is what the two teardown sites use.
//
// Note what this is NOT: it is not a model of the same-sid replacement, which TRANSFERS the
// displaced frames onto the replacement instead of abandoning them — see
// TestMQTTSameSidResubscribeUnderTheSessionLockDoesNotDeadlock for that. Abandoning at a
// replacement would discard a retained replay the client is owed, which was a holistic
// round-4 finding. Here the frames genuinely have nowhere to go.
func TestMQTTDetachedRetainedFramesCanBeAbandoned(t *testing.T) {
	t.Parallel()
	e := testNewRetainedEnv(t, "retained-replace")
	sub, pi := e.buffer(t, "foo/bar", 1)

	e.c.mu.Lock()
	if e.c.subs == nil {
		e.c.subs = make(map[string]*subscription)
	}
	e.c.subs[string(sub.sid)] = sub
	e.c.mu.Unlock()

	replacement := &subscription{client: e.c, subject: sub.subject, sid: sub.sid, mqtt: &mqttSub{qos: 1}}
	e.c.mu.Lock()
	displaced := e.c.detachRetainedFramesLocked(e.c.subs[string(sub.sid)])
	e.c.subs[string(sub.sid)] = replacement
	e.c.mu.Unlock()
	e.c.releaseDetachedRetainedFrames(displaced)

	if len(displaced) != 1 || displaced[0].pi != pi {
		t.Fatalf("the replacement must detach the displaced FRAME, got %d frame(s) want one carrying %d",
			len(displaced), pi)
	}
	e.c.mu.Lock()
	frames := len(sub.mqtt.prm)
	e.c.mu.Unlock()
	if frames != 0 {
		t.Fatalf("the displaced subscription's frames must be detached, %d left", frames)
	}
	if e.reserved(pi) {
		t.Fatal("a displaced subscription's retained frame must release its reservation")
	}
}

// The detach runs from processUnsub, which is GENERIC: it is called on the internal
// account client during session-manager startup rollback, and that client is built
// without any MQTT state at all. Both the helper's guards and the CALLER's are exercised,
// because a caller-side mutation survives a test that only covers the helper.
func TestMQTTRetainedFramesSurviveNoMQTTClient(t *testing.T) {
	t.Parallel()
	e := testNewRetainedEnv(t, "retained-nil-guards")

	bare := e.s.createInternalAccountClient()
	if bare.mqtt != nil {
		t.Fatal("this test needs the shape processUnsub actually meets at startup: mqtt == nil")
	}
	bare.mu.Lock()
	bare.subs = map[string]*subscription{"deliver": {client: bare, sid: []byte("deliver")}}
	bare.mu.Unlock()
	if err := bare.processUnsub([]byte("deliver")); err != nil {
		t.Fatalf("processUnsub on a client with no MQTT state: %v", err)
	}

	// A subscription with no MQTT metadata: the deliver-subject shape, which is the
	// overwhelming majority of this broker's unsubscribes.
	e.c.mu.Lock()
	e.c.subs = map[string]*subscription{"plain": {client: e.c, sid: []byte("plain")}}
	e.c.mu.Unlock()
	if err := e.c.processUnsub([]byte("plain")); err != nil {
		t.Fatalf("processUnsub on a subscription with no MQTT metadata: %v", err)
	}

	// A NONEMPTY detachment on a client with no session. Impossible in production, and
	// refused rather than dereferenced precisely because the detach is reachable from a
	// generic path.
	withFrames := &subscription{client: bare, sid: []byte("framed"), mqtt: &mqttSub{
		prm: []mqttRetainedFrame{{header: []byte{0}, msg: []byte("x"), pi: 3}},
	}}
	bare.mu.Lock()
	pis := bare.detachRetainedFramesLocked(withFrames)
	bare.mu.Unlock()
	if len(pis) != 1 || pis[0].pi != 3 {
		t.Fatalf("the detach must report the frame it took, got %d", len(pis))
	}
	bare.releaseDetachedRetainedFrames(pis) // must not panic: bare.mqtt is nil

	noSess := &client{srv: e.s, mqtt: &mqtt{asm: e.asm}}
	noSess.releaseDetachedRetainedFrames([]mqttRetainedFrame{{pi: 3}}) // must not panic: mqtt.sess is nil
}

// The DELIVERY-side flusher, and the assertion that needs c and cc to be DIFFERENT
// clients. mqttEnqueuePublishMsgTo drains a leftover prm under the SUBSCRIBER's connection
// lock while its own receiver can be a publishing or synthetic delivery client, so a
// completion issued on the receiver's session would land on the wrong object or on none. A
// fixture where c == cc passes against exactly that bug.
func TestMQTTDeliverySideFlushCompletesOnTheSubscribersSession(t *testing.T) {
	t.Parallel()
	e := testNewRetainedEnv(t, "retained-delivery-flush")
	sub, pi := e.buffer(t, "foo/bar", 1)

	// The receiver: a DIFFERENT client, with its own session, standing in for the
	// publishing or synthetic delivery client this path really runs on.
	other := testNewRetainedEnv(t, "retained-delivery-other")
	if other.c == e.c || other.sess == e.sess {
		t.Fatal("this test is only meaningful with a receiver distinct from the subscriber")
	}

	if !other.c.mqttEnqueueQoS0PublishMsgTo(e.c, sub, false, 0,
		[]byte("foo/bar"), []byte("live"), nil) {
		t.Fatal("the live enqueue must succeed")
	}

	e.c.mu.Lock()
	frames := len(sub.mqtt.prm)
	e.c.mu.Unlock()
	if frames != 0 {
		t.Fatalf("the delivery-side flusher must drain the buffered retained frames, %d left", frames)
	}
	if e.holdPresent(pi) {
		t.Fatal("the delivery-side flusher must discharge the SUBSCRIBER's reservation: " +
			"a completion issued on the receiver's own session would release nothing here")
	}
}

// PRODUCTION INVARIANT: removing or replacing a subscription in c.subs must not take an
// owed retained frame with it. Its frames are detached while c.mu is held and are then
// either TRANSFERRED to a reachable replacement or ABANDONED with their reservations
// released.
//
// THIS TEST IS A BOUNDED PURE-go/ast REGRESSION TRIPWIRE, NOT A PROOF. It checks that
// invariant only for the frozen direct mutation forms and signature-declared *client
// identifiers below. It does not resolve local aliases and does not prove that every
// semantic departure is covered.
//
// The boundary is frozen deliberately, and the reason is recorded because three consecutive
// holistic rounds each found another way to evade an earlier version of this guard — the
// receiver's spelling, then per-function booleans, then lock ordering and aliases. Chasing
// closure here means writing a whole-program analyzer inside a test: go/types would settle a
// local alias's type but not receiver identity, control flow, lock state, helper-mediated
// mutation, or aliases of the map itself. So the scope was fixed by a consult and is binding
// (lane DECISIONS.md, D2).
//
// THE FROZEN PREDICATE:
//  1. every production FuncDecl is scanned;
//  2. only a direct identifier declared syntactically as *client in that function's receiver
//     or parameter list is recognised as naming a client — so a differently spelled receiver
//     is still covered;
//  3. matched destructive operations are exactly: any assignment to r.subs[...], any
//     delete(r.subs, ...), and any whole-map assignment to r.subs;
//  4. a whole-map assignment is exempt ONLY in the exact nil-initialisation shape
//     `if r.subs == nil { r.subs = make(...) }`; any other r.subs = make(...) is destructive;
//  5. each destructive node requires, in the same enclosing block: a direct r.mu.Lock() at an
//     earlier index, an UNCONSUMED r.detachRetainedFramesLocked(...) strictly between that
//     lock and the destructive statement, and no r.mu.Unlock() in between — one detach
//     consumed per destructive node;
//  6. the extensional membership below is compared for equality.
//
// RES JUDICATA. This guard is intentionally bounded to the frozen direct-AST predicate above.
// It does not resolve local *client aliases, aliases of the subs map, indirect receiver
// expressions, helper-mediated mutations, or control-flow equivalence. A later review may
// block on (a) an operation satisfying the frozen predicate that the guard misses or accepts
// without the required ordering, (b) a named mutation that survives, or (c) an actual
// production path that loses or abandons owed retained frames. It may NOT require this guard
// to grow merely by exhibiting source code outside the frozen predicate that could evade it.
// Such an evasion is a settled limitation of this regression tripwire, not a reopening of
// F-091's mechanism.
func TestMQTTDirectClientSubsMutationsDetachRetainedFramesUnderLock(t *testing.T) {
	t.Parallel()
	const detach = "detachRetainedFramesLocked"
	want := map[string]string{
		"client.processUnsub":    "the delete, followed by abandonment",
		"client.processSub":      "the same-sid replacement, followed by TRANSFER to the replacement",
		"client.closeConnection": "the whole-map swap, followed by abandonment",
	}
	got := map[string]struct{}{}

	// The CONNECTION's subscription map, not the session's: mqttSession.subs is the
	// persisted filter set and a different thing entirely.
	//
	// Told apart by the identifier's DECLARED TYPE, not by its spelling. An earlier
	// revision matched the names `c` and `cc`, and the holistic review was right that this
	// turned the closure claim into a sample: a fourth destructive method written
	// `func (cl *client) ... { cl.subs = nil }` was invisible while the three known
	// members still satisfied the equality below, so the guard would have passed while the
	// property was broken.
	//
	// go/ast is enough for this without go/types, because every identifier that can hold
	// a *client here is DECLARED in the function's own signature — as the receiver or as a
	// parameter — and the declared type is syntactically present there.
	clientIdents := func(fn *ast.FuncDecl) map[string]bool {
		isClientPtr := func(e ast.Expr) bool {
			star, _ := e.(*ast.StarExpr)
			if star == nil {
				return false
			}
			id, _ := star.X.(*ast.Ident)
			return id != nil && id.Name == "client"
		}
		names := map[string]bool{}
		add := func(fl *ast.FieldList) {
			if fl == nil {
				return
			}
			for _, f := range fl.List {
				if !isClientPtr(f.Type) {
					continue
				}
				for _, n := range f.Names {
					names[n.Name] = true
				}
			}
		}
		add(fn.Recv)
		if fn.Type != nil {
			add(fn.Type.Params)
		}
		return names
	}
	var inScope map[string]bool
	// Reports the RECEIVER whose subs map this is, or "" for none. The identity matters and
	// not merely the fact: clause 5 of the frozen predicate requires the lock and the detach
	// to be on the SAME receiver as the mutation, and a yes/no answer let a function with two
	// client parameters lock and detach one while destroying the other's map.
	subsReceiver := func(x ast.Expr) string {
		sel, _ := x.(*ast.SelectorExpr)
		if sel == nil || sel.Sel.Name != "subs" {
			return ""
		}
		recv, _ := sel.X.(*ast.Ident)
		if recv == nil || !inScope[recv.Name] {
			return ""
		}
		return recv.Name
	}

	for _, file := range testMQTTProductionFiles(t) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range f.Decls {
			fn, _ := decl.(*ast.FuncDecl)
			if fn == nil || fn.Body == nil {
				continue
			}
			qualified := testQualifiedFuncName(fn)
			inScope = clientIdents(fn)
			if len(inScope) == 0 {
				continue // nothing here can name a *client's subs map
			}
			// PER DESTRUCTIVE NODE, and per the frozen predicate's clauses 4 and 5. Earlier
			// revisions collapsed the facts into one boolean each (so a second destructive
			// statement rode the first one's detach), and accepted any detach earlier in the
			// block (so moving the detach ABOVE the lock still passed while making the
			// detachment race the flusher).
			//
			// Same block is what "the same critical section" means for all three members:
			// each takes the lock, mutates, and unlocks inside one block.
			// Clause 4: the ONLY exempt whole-map assignment is the EXACT nil-init shape
			// `if r.subs == nil { r.subs = make(...) }` — same receiver on both sides, and a
			// make(...) right-hand side. Exempting any assignment inside the nil branch was
			// broader than the frozen predicate and would have let `r.subs = someOtherMap`
			// through.
			nilInit := map[ast.Node]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				ifs, _ := n.(*ast.IfStmt)
				if ifs == nil || ifs.Body == nil {
					return true
				}
				bin, _ := ifs.Cond.(*ast.BinaryExpr)
				if bin == nil || bin.Op != token.EQL {
					return true
				}
				guarded := subsReceiver(bin.X)
				if guarded == "" {
					return true
				}
				if id, _ := bin.Y.(*ast.Ident); id == nil || id.Name != "nil" {
					return true
				}
				for _, st := range ifs.Body.List {
					as, _ := st.(*ast.AssignStmt)
					if as == nil || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
						continue
					}
					if subsReceiver(as.Lhs[0]) != guarded {
						continue
					}
					call, _ := as.Rhs[0].(*ast.CallExpr)
					if call == nil {
						continue
					}
					if id, _ := call.Fun.(*ast.Ident); id != nil && id.Name == "make" {
						nilInit[as] = true
					}
				}
				return true
			})
			// Clause 5, PER RECEIVER. Reports which receiver a Lock/Unlock statement names.
			lockReceiver := func(st ast.Stmt, method string) string {
				expr, _ := st.(*ast.ExprStmt)
				if expr == nil {
					return ""
				}
				call, _ := expr.X.(*ast.CallExpr)
				if call == nil {
					return ""
				}
				sel, _ := call.Fun.(*ast.SelectorExpr)
				if sel == nil || sel.Sel.Name != method {
					return ""
				}
				mu, _ := sel.X.(*ast.SelectorExpr)
				if mu == nil || mu.Sel.Name != "mu" {
					return ""
				}
				recv, _ := mu.X.(*ast.Ident)
				if recv == nil || !inScope[recv.Name] {
					return ""
				}
				return recv.Name
			}
			// And which receiver a detach call names, which the callee name alone does not say.
			detachReceiver := func(call *ast.CallExpr) string {
				sel, _ := call.Fun.(*ast.SelectorExpr)
				if sel == nil || sel.Sel.Name != detach {
					return ""
				}
				recv, _ := sel.X.(*ast.Ident)
				if recv == nil || !inScope[recv.Name] {
					return ""
				}
				return recv.Name
			}

			var destructiveNodes int
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				block, _ := n.(*ast.BlockStmt)
				if block == nil {
					return true
				}
				// Per RECEIVER, so a function holding two client parameters cannot lock and
				// detach one while destroying the other's map — which the single-state
				// version allowed, and which is inside the frozen predicate rather than
				// res judicata.
				locked := map[string]bool{}
				detached := map[string]int{}
				for _, st := range block.List {
					if r := lockReceiver(st, "Lock"); r != "" {
						locked[r], detached[r] = true, 0
						continue
					}
					if r := lockReceiver(st, "Unlock"); r != "" {
						locked[r], detached[r] = false, 0
						continue
					}
					target := ""
					ast.Inspect(st, func(in ast.Node) bool {
						switch v := in.(type) {
						case *ast.CallExpr:
							if r := detachReceiver(v); r != "" {
								detached[r]++
							}
							if id, _ := v.Fun.(*ast.Ident); id != nil && id.Name == "delete" &&
								len(v.Args) > 0 {
								if r := subsReceiver(v.Args[0]); r != "" {
									target = r
								}
							}
						case *ast.AssignStmt:
							if nilInit[v] {
								return true
							}
							for _, lhs := range v.Lhs {
								if ix, _ := lhs.(*ast.IndexExpr); ix != nil {
									if r := subsReceiver(ix.X); r != "" {
										target = r
									}
									continue
								}
								// Every whole-map assignment, make(...) included, unless it
								// was the exact nil-init shape excluded above.
								if r := subsReceiver(lhs); r != "" {
									target = r
								}
							}
						}
						return true
					})
					if target == "" {
						continue
					}
					destructiveNodes++
					switch {
					case !locked[target]:
						t.Errorf("%s: %s mutates %s.subs destructively with no preceding %s.mu.Lock() "+
							"in the same block: the detachment would race the retained flusher",
							fset.Position(st.Pos()), qualified, target, target)
					case detached[target] == 0:
						t.Errorf("%s: %s removes a subscription from %s.subs with no unconsumed "+
							"%s.%s call between that receiver's lock and the mutation: its buffered "+
							"retained frames keep their packet identifiers reserved against "+
							"reallocation for the rest of the session, while nothing will ever "+
							"write them",
							fset.Position(st.Pos()), qualified, target, target, detach)
					default:
						// Consumed, so a second destructive statement on this receiver needs
						// its OWN detach.
						detached[target]--
					}
				}
				return true
			})
			if destructiveNodes == 0 {
				continue
			}
			got[qualified] = struct{}{}
		}
	}

	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s is a NEW destructive write to c.subs and is not in the frozen set: add it "+
				"with what it destroys, having checked it detaches retained frames", name)
		}
	}
	for name, what := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("%s no longer performs %s, so this frozen set describes code that has moved: "+
				"remove the entry", name, what)
		}
	}
}

// The rebind's sweep is asserted STRUCTURALLY as well as behaviourally, because a fixture
// can reproduce the rebind with `sess.c = successor; sess.sweepDeadSendHoldsLocked()` and
// would then pass with the production call deleted. What matters is that the sweep sits
// immediately after the assignment that MOVES ownership, since that is the instant the
// predecessor's holds become inert.
func TestMQTTRebindSweepsInertSendHolds(t *testing.T) {
	t.Parallel()
	const sweep = "sweepDeadSendHoldsLocked"
	found := false

	for _, file := range testMQTTProductionFiles(t) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			body, _ := n.(*ast.BlockStmt)
			if body == nil {
				return true
			}
			for i, st := range body.List {
				as, _ := st.(*ast.AssignStmt)
				if as == nil || len(as.Lhs) != 1 {
					continue
				}
				sel, _ := as.Lhs[0].(*ast.SelectorExpr)
				if sel == nil || sel.Sel.Name != "c" {
					continue
				}
				recv, _ := sel.X.(*ast.Ident)
				if recv == nil || recv.Name != "es" {
					continue
				}
				// Only an assignment that hands ownership to a NEW connection. `es.c = nil`
				// is a DETACH — ownership moves to nothing, and retainedHeldLocked and
				// pubCompHeldLocked already read false against a nil sess.c, so there is
				// nothing a sweep there would change about any allocation.
				if rhs, _ := as.Rhs[0].(*ast.Ident); rhs == nil || rhs.Name == "nil" {
					continue
				}
				found = true
				var next ast.Stmt
				if i+1 < len(body.List) {
					next = body.List[i+1]
				}
				expr, _ := next.(*ast.ExprStmt)
				if expr == nil || testCalleeName(exprCall(expr)) != sweep {
					t.Errorf("%s: the CONNECT rebind moves session ownership and must sweep the "+
						"predecessor's now-inert holds immediately afterwards, or a never-flushed "+
						"retained frame's entry survives every rebind", fset.Position(as.Pos()))
				}
			}
			return true
		})
	}
	if !found {
		t.Error("no session rebind assignment was found, so this guard checks nothing: the CONNECT " +
			"path's `es.c = c` has moved or been renamed")
	}
}

func exprCall(e *ast.ExprStmt) *ast.CallExpr {
	call, _ := e.X.(*ast.CallExpr)
	return call
}
