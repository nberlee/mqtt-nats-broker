// Copyright 2026 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build mqtt_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// THE FOREGROUND AUTHORITY LOOPS F-114's CLASSIFICATION LEFT UNSPACED, converted
// (outbound_foreground_priority.go, bounded_reattempt_census_test.go). Each re-attempted an
// authority transition three times with no spacing, and all but the PUBCOMP terminalization did so
// without announcing itself, so this session's own background writers could defeat every attempt.
// The witnesses here contend through the store with a real admitted writer
// (authority_contender_test.go); the budget and spacing counts force the losses at the commit seam.

// THE PUBCOMP TERMINALIZATION was announced but had three attempts against the five background
// writers that may each hold one attempt admitted before the announcement. Five such commits, one
// in each of its first five windows, exhausted it: the completion went to the 2 s worker with the
// Receive Maximum unit, the terminal reservation and the canonical PUBREL's acknowledgement held.
// Now the completion is authority-free (mqttAttemptAuthorityFree): a writer committing the authority
// inside its window is no contender, and it lands on its FIRST attempt with no wait. Mutation
// M-L1a (the advance writes the authority again): it loses that window and waits.
func TestMQTTPubCompOutlastsThePreAdmittedWriters(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr, sess, _, pi, nonce, _ := testF105PubRelReady(t, o, s, "fgloops-pubcomp")
	seams := testF105ObservePubComp(t, sess)
	waits := testCountContentionWaits(t, sess)
	w := testNewAuthorityContender(t, sess)
	w.preAdmit(mqttBackgroundAuthorityWriters)
	var windows atomic.Int32
	testPresendFor(t, sess, "op-ack-advance", testF105SlotSubject(sess, pi), func(*mqttAuthorityTransition) {
		windows.Add(1)
		w.fire()
	})

	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)

	if _, _, enrolled, _ := seams.counts(); enrolled != 0 {
		t.Fatalf("the PUBCOMP was handed to the completion worker after %d attempts (%d contending commits): "+
			"the pre-admitted writers exhausted it", windows.Load(), w.commits.Load())
	}
	testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminal)
	if n := windows.Load(); n != 1 {
		t.Fatalf("the PUBCOMP made %d attempts, want 1: a pre-admitted authority writer is no contender", n)
	}
	if c := w.commits.Load(); c != 1 {
		t.Fatalf("%d authority writers committed inside the PUBCOMP's window, want 1: nothing contended", c)
	}
	if n := waits.Load(); n != 0 {
		t.Fatalf("%d contention waits for a completion nothing contends with", n)
	}
	testF105SlotGone(t, sess, pi, time.Second)
}

// ROUTE B's RECOVERED PUBREC was unannounced, so a background writer admitted in each of its
// windows defeated all three attempts. Its exhaustion is session-wide: the slot is quarantined
// opaque and the resume marks the identifier space incomplete, so bumpPI allocates nothing until a
// later resume completes. Announced, the writer is held and the PUBREC lands.
func TestMQTTRouteBRecoveredPubRecHoldsTheBackgroundWriters(t *testing.T) {
	e, pi, subject, lin := testAdmittedPublishWorld(t, "fgloops-routeb", testRealPubRelSrcSeq)
	testStoreCanonical(t, e, pi, e.nonceOf(pi), testRealPubRelSrcSeq)
	w := testNewAuthorityContender(t, e.sess)
	var windows, committed atomic.Int32
	testPresendFor(t, e.sess, "op-ack-advance", subject, func(*mqttAuthorityTransition) {
		windows.Add(1)
		if w.live() {
			committed.Add(1)
		}
	})

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if incomplete {
		t.Fatalf("route B's PUBREC lost all %d attempts to a background writer admitted in each window "+
			"(%d commits): the identifier space is incomplete and bumpPI allocates nothing (%d)",
			windows.Load(), committed.Load(), e.sess.bumpPI())
	}
	if committed.Load() != 0 {
		t.Fatalf("%d background commits landed inside the announced PUBREC", committed.Load())
	}
	if slot := testSlotAt(t, e, subject); slot == nil || slot.State != mqttInflightPubRelReady {
		t.Fatalf("route B left the slot at %+v, want pubrel_ready", slot)
	}
	if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePubRecObserved {
		t.Fatalf("route B left the exchange at phase %d, want PUBREC-observed", got)
	}
}

// THE PUBREL CALLBACK's acquisition and its redispatch were unannounced: a background writer
// admitted in each window defeated every one of the outer loop's three inner acquisitions, and the
// canonical PUBREL waited out AckWait (and lost again on every redelivery). Announced, the writer
// is held and the PUBREL is emitted on its first offer. This is the combined witness of the
// acquisition (its own announcement holds the writer during each inner call) and the redispatch;
// the redispatch's own announcement is pinned by TestMQTTPubRelRedispatchAnnouncesBetweenAcquisitions.
func TestMQTTPubRelCallbackHoldsTheBackgroundWriters(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr, sess, pi, _ := testF105QoS2Delivered(t, o, s, "fgloops-pubrel")
	w := testNewAuthorityContender(t, sess)
	var windows, committed atomic.Int32
	testPresendFor(t, sess, "op-acquire-pubrel", testF105SlotSubject(sess, pi), func(*mqttAuthorityTransition) {
		windows.Add(1)
		if w.live() {
			committed.Add(1)
		}
	})

	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	b, pl, ok := testMQTTReadPacketReady(subr, 5*time.Second)
	if !ok || b&mqttPacketMask != mqttPacketPubRel {
		t.Fatalf("no PUBREL within 5 s (packet %x, ready %v) after %d acquisition attempts, %d lost to a background "+
			"writer admitted in their window: it waits out AckWait", b, ok, windows.Load(), committed.Load())
	}
	subr.pos += pl
	if committed.Load() != 0 {
		t.Fatalf("%d background commits landed inside the announced PUBREL acquisition", committed.Load())
	}
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)
}

// THE RETAINED PUBREL's FOREGROUND ARM (the PUBREC handler's own emission) acquired unannounced, so
// a background writer admitted in each of its three windows handed the PUBREL to the retained
// continuation, which then had to land it on its own backed-off schedule. Announced, the handler's
// first attempt lands and nothing is owed. The contender is SCOPED to the handler's budget at HEAD
// (its first three windows): a later window is the continuation's, which is admitted and must not
// be contended here, and it is where the hand-off is observed.
func TestMQTTRetainedPubRelHandlerArmHoldsTheBackgroundWriters(t *testing.T) {
	r := testRetainedRigUp(t, "fgloops-retpubrel", "rt/fgl", 2, []byte("q2"))
	defer r.done()
	ci := &mqttConnInfo{clientID: r.id, cleanSess: false}
	sub, subr := testMQTTConnect(t, ci, r.o.MQTT.Host, r.o.MQTT.Port)
	defer func() { sub.Close() }()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/fgl", qos: 2}}, []byte{2})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/fgl", mqttPubQoS2|mqttPubFlagRetain, []byte("q2"))
	r.attach(t)

	w := testNewAuthorityContender(t, r.sess)
	var windows, committed atomic.Int32
	var handedOff atomic.Bool
	subject := mqttInflightSubject(r.idHash, r.gen, pi)
	testPresendFor(t, r.sess, "op-acquire-pubrel", subject, func(*mqttAuthorityTransition) {
		if int(windows.Add(1)) <= mqttInflightStoreAttempts {
			if w.live() {
				committed.Add(1)
			}
			return
		}
		d := &r.sess.retainedDriver
		d.mu.Lock()
		for _, o := range d.owed {
			if o.pi == pi && o.kind == mqttRetainedOwedPubRel {
				handedOff.Store(true)
			}
		}
		d.mu.Unlock()
	})

	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	if handedOff.Load() {
		t.Fatalf("the handler's acquisition lost its attempts to a background writer admitted in each window "+
			"(%d commits) and handed the PUBREL to the retained continuation", committed.Load())
	}
	if committed.Load() != 0 {
		t.Fatalf("%d background commits landed inside the handler's announced acquisition", committed.Load())
	}
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)
}

// THE CANCELLATION's WITHDRAWAL, under Receive Maximum 1. A consumer teardown (the UNSUBSCRIBE's
// deferred consumer delete, a SUBSCRIBE rollback, the forced leave) voids an admitted, unsent
// delivery by withdrawing its exchange: first through the destructive door's own withdrawal (two
// attempts), then, when that refuses, through markInflightRegisterCancelled. That loop was
// unannounced, so a background writer admitted in each window defeated it too; the exchange then
// stayed at Publish holding the session's only Receive Maximum unit, and no new QoS 1/2 delivery
// could be admitted until a later resume. Announced, the writer is held, the withdrawal lands and
// the unit is free. The door's attempts are told apart from the loop's by the evidence seam the
// door passes and the loop does not.
func TestMQTTCancellationWithdrawalFreesTheOnlyUnit(t *testing.T) {
	f := testWithdrawAdmitted(t, "fgloops-cancel", 1)
	sess := f.e.sess
	sess.mu.Lock()
	sess.rmax = 1
	before := sess.capacityAvailableLocked()
	sess.mu.Unlock()
	if before != 0 {
		t.Fatalf("fixture: the admitted delivery leaves capacity %d under Receive Maximum 1, want 0", before)
	}

	var door atomic.Bool
	var doorWindows, loopWindows, committed atomic.Int32
	as := sess.asm
	as.mu.Lock()
	as.testWithdrawAfterEvidence = func() {}
	as.testWithdrawUnsent = func(req *mqttWithdrawUnsentRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		door.Store(req.afterEvidence != nil)
		return write()
	}
	as.mu.Unlock()
	t.Cleanup(func() {
		as.mu.Lock()
		as.testWithdrawAfterEvidence, as.testWithdrawUnsent = nil, nil
		as.mu.Unlock()
	})
	w := testNewAuthorityContender(t, sess)
	testPresendFor(t, sess, "op-withdraw-unsent", f.subject, func(*mqttAuthorityTransition) {
		if door.Load() {
			doorWindows.Add(1)
		} else {
			loopWindows.Add(1)
		}
		if w.live() {
			committed.Add(1)
		}
	})

	sess.mu.Lock()
	regs, _ := sess.purgeConsumerPendingLocked(testRealPubRelSrcDur, mqttTestLicenceLocked(sess, nil))
	sess.mu.Unlock()
	if len(regs) != 1 || regs[0].pi != f.pi {
		t.Fatalf("fixture: the teardown must void exactly the never-sent delivery, got %+v", regs)
	}
	sess.voidRegistersOnStream(f.e.c, regs, mqttLicenceOf(f.e.c))
	if doorWindows.Load() == 0 || loopWindows.Load() == 0 {
		t.Fatalf("fixture: the door made %d attempts and the cancellation loop %d; the witness needs the door "+
			"to refuse into the loop", doorWindows.Load(), loopWindows.Load())
	}

	if x := f.exchange(t); x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
		t.Fatalf("the cancellation's withdrawal lost its %d attempts (after the door's %d) to a background writer "+
			"admitted in each window (%d commits): the exchange stands at %+v", loopWindows.Load(),
			doorWindows.Load(), committed.Load(), x)
	}
	testCustodyEventually(t, "the withdrawn delivery's Receive Maximum unit is released", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.capacityAvailableLocked() == 1
	})
}

// testFgArm is one converted loop driven directly from the test goroutine, so nothing announces
// around it: run makes the call once and reports whether it succeeded.
type testFgArm struct {
	sess    *mqttSession
	op      string // the operation its batches carry
	subject string // the register member its batches carry
	run     func() bool
	// completion: the loop's batches are authority-free completions (mqttAttemptAuthorityFree), so
	// a background AUTHORITY writer committing in their windows is no contender at all.
	completion bool
	// refuseUncommitted makes every attempt fail BEFORE its batch reaches the commit (no handle),
	// and counts the attempts.
	refuseUncommitted func(t *testing.T) *atomic.Int32
}

type testFgArmCase struct {
	name string
	up   func(t *testing.T) *testFgArm
}

func testFgArms() []testFgArmCase {
	return []testFgArmCase{
		{"cancellation withdrawal", func(t *testing.T) *testFgArm {
			f := testWithdrawAdmitted(t, "fgarm-withdraw", 1)
			return testFgCancelArm(t, f, "op-withdraw-unsent", func(t *testing.T) *atomic.Int32 {
				var n atomic.Int32
				as := f.e.sess.asm
				as.mu.Lock()
				as.testWithdrawUnsent = func(*mqttWithdrawUnsentRequest, func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
					n.Add(1)
					return mqttOpOutcome{}, errors.New("test: refused before the batch")
				}
				as.mu.Unlock()
				t.Cleanup(func() { as.mu.Lock(); as.testWithdrawUnsent = nil; as.mu.Unlock() })
				return &n
			})
		}},
		{"cancellation tombstone", func(t *testing.T) *testFgArm {
			f := testWithdrawAdmitted(t, "fgarm-tombstone", 1)
			// No marker naming the allocation: the withdrawal has no evidence and the loop writes the
			// standalone tombstone.
			ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
			if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
				t.Fatalf("deleting the marker: %v", err)
			}
			return testFgCancelArm(t, f, "op-tombstone-register", func(t *testing.T) *atomic.Int32 {
				var n atomic.Int32
				as := f.e.sess.asm
				as.mu.Lock()
				as.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
					n.Add(1)
					return 0, errors.New("test: refused before the batch")
				}
				as.mu.Unlock()
				t.Cleanup(func() { as.mu.Lock(); as.testInflightStore = nil; as.mu.Unlock() })
				return &n
			})
		}},
		{"recovered PUBREC", func(t *testing.T) *testFgArm {
			e, pi, subject, lin := testAdmittedPublishWorld(t, "fgarm-routeb", testRealPubRelSrcSeq)
			testStoreCanonical(t, e, pi, e.nonceOf(pi), testRealPubRelSrcSeq)
			return &testFgArm{sess: e.sess, op: "op-ack-advance", subject: subject, completion: true,
				run: func() bool {
					if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
						t.Fatalf("recoverInflightRegisters: %v", err)
					}
					e.sess.mu.Lock()
					incomplete := e.sess.piSpaceIncomplete
					e.sess.mu.Unlock()
					slot := testSlotAt(t, e, subject)
					return !incomplete && slot != nil && slot.State == mqttInflightPubRelReady
				},
				refuseUncommitted: func(t *testing.T) *atomic.Int32 {
					var n atomic.Int32
					testF105SetAckAdvance(t, e.sess, func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
						if req.To != mqttExchangePubRecObserved || req.Nonce != lin {
							return write()
						}
						n.Add(1)
						return mqttOpOutcome{}, errors.New("test: refused before the batch")
					})
					return &n
				}}
		}},
		{"PUBREL acquisition", func(t *testing.T) *testFgArm {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			t.Cleanup(func() { testMQTTShutdownServer(s) })
			_, _, sess, c, pi, _, _ := testF105PubRelReady(t, o, s, "fgarm-pubrel")
			ref := testF105SlotRefAt(t, sess, pi, mqttInflightPubRelReady, false)
			return &testFgArm{sess: sess, op: "op-acquire-pubrel", subject: ref.name.subject,
				run: func() bool {
					_, _, ok, _ := sess.acquirePubRelThroughAuthority(c, ref, mqttInflightPubRelReady, mqttNewInflightNonce(), nil, 0)
					return ok
				},
				refuseUncommitted: func(t *testing.T) *atomic.Int32 {
					// The connection's certificate context names another generation, so the binding
					// refuses the acquisition before its batch (registerAcquisition) on every attempt.
					// The attempts are not counted: nothing observable precedes the refusal.
					b := c.outBind
					b.mu.Lock()
					prev := b.persist
					b.persist = &mqttCertPersistCtx{jsa: sess.jsa, id: sess.id, epoch: "fgarm-other", gen: "fgarm-other"}
					b.mu.Unlock()
					t.Cleanup(func() { b.mu.Lock(); b.persist = prev; b.mu.Unlock() })
					return nil
				}}
		}},
		{"PUBCOMP", func(t *testing.T) *testFgArm {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			t.Cleanup(func() { testMQTTShutdownServer(s) })
			_, _, sess, c, _, nonce, ref := testF105PubRelReady(t, o, s, "fgarm-pubcomp")
			return &testFgArm{sess: sess, op: "op-ack-advance", subject: ref.name.subject, completion: true,
				run: func() bool { return sess.mqttTerminalizeOnPubComp(c, ref).recorded },
				refuseUncommitted: func(t *testing.T) *atomic.Int32 {
					var n atomic.Int32
					testF105SetAckAdvance(t, sess, func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
						if !testF105IsPubCompAdvance(req, nonce) {
							return write()
						}
						n.Add(1)
						return mqttOpOutcome{}, errors.New("test: refused before the batch")
					})
					return &n
				}}
		}},
	}
}

func testFgCancelArm(t *testing.T, f *testWithdrawFix, op string, refuse func(*testing.T) *atomic.Int32) *testFgArm {
	return &testFgArm{sess: f.e.sess, op: op, subject: f.subject,
		run: func() bool {
			_, seq := f.register(t)
			out, err := f.e.sess.markInflightRegisterCancelled(f.e.c, f.pi, testRealPubRelSrcDur, f.sseq, seq, f.nonce,
				mqttTestLicence(f.e.sess, nil))
			return err == nil && out == mqttRegCancelStored
		},
		refuseUncommitted: refuse}
}

// THE BUDGET: one lost attempt per counted background writer still succeeds, each re-attempt after
// a commit-stage loss spaced by one contention wait; one loss more exhausts the foreground budget.
// The losses are the production CAS rejection at the commit seam, after the batch's handle froze.
func TestMQTTForegroundLoopsOutlastTheBackgroundWriters(t *testing.T) {
	for _, arm := range testFgArms() {
		for _, lose := range []int{mqttBackgroundAuthorityWriters, mqttBackgroundAuthorityWriters + 1} {
			t.Run(fmt.Sprintf("%s/lose %d", arm.name, lose), func(t *testing.T) {
				a := arm.up(t)
				waits := testCountContentionWaits(t, a.sess)
				attempts := testLoseAtCommit(t, a.sess, a.op, a.subject, lose)
				ok := a.run()
				if lose < mqttForegroundAcquireAttempts {
					if !ok {
						t.Fatalf("the loop did not survive %d commit-stage losses (%d attempts)", lose, attempts.Load())
					}
					if n := attempts.Load(); int(n) != lose+1 || int(waits.Load()) != lose {
						t.Fatalf("%d attempts and %d waits, want %d and %d", n, waits.Load(), lose+1, lose)
					}
					return
				}
				if ok {
					t.Fatalf("the loop succeeded after %d losses: its budget exceeds %d", lose, mqttForegroundAcquireAttempts)
				}
				if n := attempts.Load(); int(n) != mqttForegroundAcquireAttempts || int(waits.Load()) != mqttForegroundAcquireAttempts-1 {
					t.Fatalf("exhausted after %d attempts and %d waits, want %d and %d",
						n, waits.Load(), mqttForegroundAcquireAttempts, mqttForegroundAcquireAttempts-1)
				}
			})
		}
	}
}

// THE SPACING DISCRIMINATOR: an attempt refused before its batch reached the commit carries no
// handle, and a fresh read can settle it at once, so it is re-attempted without a wait.
func TestMQTTForegroundLoopsSpaceOnlyCommittedAttempts(t *testing.T) {
	for _, arm := range testFgArms() {
		t.Run(arm.name, func(t *testing.T) {
			a := arm.up(t)
			waits := testCountContentionWaits(t, a.sess)
			refused := a.refuseUncommitted(t)
			if a.run() {
				t.Fatal("fixture: the loop succeeded although every attempt was refused before its batch")
			}
			if refused != nil && int(refused.Load()) != mqttForegroundAcquireAttempts {
				t.Fatalf("fixture: %d refused attempts, want the whole budget of %d", refused.Load(), mqttForegroundAcquireAttempts)
			}
			if n := waits.Load(); n != 0 {
				t.Fatalf("%d contention waits after refusals that never reached the commit, want none", n)
			}
		})
	}
}

// THE BOUND, WITH REAL COMMITS AND THE ANNOUNCEMENT: five writers admitted before the loop starts
// each commit in one of its windows, which is the whole allowance the handshake concedes; a sixth,
// admitted during the loop, is held until it returns, so the sixth attempt lands. An unannounced
// loop admits the sixth and loses its last attempt too. A completion loop writes no authority, so
// the same commits never defeat it: it lands on its first attempt.
func TestMQTTForegroundLoopsHoldWritersAdmittedDuringThem(t *testing.T) {
	for _, arm := range testFgArms() {
		t.Run(arm.name, func(t *testing.T) {
			a := arm.up(t)
			waits := testCountContentionWaits(t, a.sess)
			w := testNewAuthorityContender(t, a.sess)
			w.preAdmit(mqttBackgroundAuthorityWriters)
			var windows, held atomic.Int32
			testPresendFor(t, a.sess, a.op, a.subject, func(*mqttAuthorityTransition) {
				windows.Add(1)
				if !w.fire() && !w.live() {
					held.Add(1)
				}
			})
			if !a.run() {
				t.Fatalf("the loop failed after %d attempts (%d contending commits, %d held)",
					windows.Load(), w.commits.Load(), held.Load())
			}
			if a.completion {
				// Mutation M-L1a (the advance writes the authority again): it loses the first window.
				if n := windows.Load(); n != 1 || waits.Load() != 0 {
					t.Fatalf("%d attempts and %d waits for a completion no authority writer contends with, "+
						"want 1 and 0", n, waits.Load())
				}
				return
			}
			if n := windows.Load(); int(n) != mqttBackgroundAuthorityWriters+1 || int(waits.Load()) != mqttBackgroundAuthorityWriters {
				t.Fatalf("%d attempts and %d waits, want %d and %d", n, waits.Load(),
					mqttBackgroundAuthorityWriters+1, mqttBackgroundAuthorityWriters)
			}
			if held.Load() != 1 {
				t.Fatalf("the writer admitted during the loop was not held (held %d)", held.Load())
			}
		})
	}
}

// THE REDISPATCH IS ANNOUNCED BETWEEN ITS ACQUISITIONS, not only inside them. With every
// acquisition refused at its commit, the callback's outer loop goes round; at the first inner
// release (the acquisition's own), the session must still be announced by the callback, so a
// background admission started there waits. The probe runs inside the release hook, on the
// callback's goroutine, so no next acquisition can announce in between.
func TestMQTTPubRelRedispatchAnnouncesBetweenAcquisitions(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, _, sess, pi, _ := testF105QoS2Delivered(t, o, s, "fgloops-redispatch")
	testLoseAtCommit(t, sess, "op-acquire-pubrel", testF105SlotSubject(sess, pi), 1<<20)

	var armed, probed atomic.Bool
	arm := func() { armed.Store(true) }
	mqttTestPubRelBeforeAdmissionV.Store(&arm)
	defer mqttTestPubRelBeforeAdmissionV.Store(nil)
	waiting := make(chan struct{}, 1)
	bw := func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}
	sess.testBackgroundWait.Store(&bw)
	defer sess.testBackgroundWait.Store(nil)
	result := make(chan string, 1)
	granted := make(chan struct{})
	ann := func(announced bool) {
		if announced || !armed.Load() || !probed.CompareAndSwap(false, true) {
			return
		}
		go func() {
			sess.admitBackgroundAuthority()
			close(granted)
		}()
		select {
		case <-waiting:
			result <- "held"
		case <-granted:
			result <- "granted"
		case <-time.After(5 * time.Second):
			result <- "neither waited nor granted"
		}
	}
	sess.testForegroundAnnounce.Store(&ann)
	defer sess.testForegroundAnnounce.Store(nil)

	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	select {
	case r := <-result:
		if r != "held" {
			t.Fatalf("a background admission started between two of the callback's acquisitions was %s: the "+
				"redispatch is not announced across its loop", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the callback's first acquisition never released")
	}
	select {
	case <-granted:
	case <-time.After(10 * time.Second):
		t.Fatal("the held admission was never granted after the callback returned")
	}
}

// AN ADMITTING CALLER NEVER ANNOUNCES. The cancellation under a licence that admits each attempt,
// and the PUBREL acquisition given an admission, are background calls: announcing would make their
// own admission wait on it until shutdown. Each must complete, unannounced.
func TestMQTTForegroundLoopsLeaveAdmittingCallersUnannounced(t *testing.T) {
	run := func(t *testing.T, sess *mqttSession, call func() bool) {
		t.Helper()
		var announced atomic.Int32
		ann := func(a bool) {
			if a {
				announced.Add(1)
			}
		}
		sess.testForegroundAnnounce.Store(&ann)
		defer sess.testForegroundAnnounce.Store(nil)
		done := make(chan bool, 1)
		go func() { done <- call() }()
		select {
		case ok := <-done:
			if !ok {
				t.Fatal("the admitting call did not succeed")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the admitting call never returned: it waits on its own announcement")
		}
		if n := announced.Load(); n != 0 {
			t.Fatalf("an admitting call announced %d foreground operation(s)", n)
		}
	}
	t.Run("cancellation", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "fgneg-cancel", 1)
		sess := f.e.sess
		_, seq := f.register(t)
		lic := mqttTestLicence(sess, nil).withAdmission(sess.admitBackgroundAuthority)
		run(t, sess, func() bool {
			out, err := sess.markInflightRegisterCancelled(f.e.c, f.pi, testRealPubRelSrcDur, f.sseq, seq, f.nonce, lic)
			return err == nil && out == mqttRegCancelStored
		})
	})
	t.Run("PUBREL acquisition", func(t *testing.T) {
		o := testMQTTDefaultOptions()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)
		_, _, sess, c, pi, _, _ := testF105PubRelReady(t, o, s, "fgneg-pubrel")
		ref := testF105SlotRefAt(t, sess, pi, mqttInflightPubRelReady, false)
		run(t, sess, func() bool {
			_, _, ok, _ := sess.acquirePubRelThroughAuthority(c, ref, mqttInflightPubRelReady, mqttNewInflightNonce(),
				nil, 0, sess.admitBackgroundAuthority)
			return ok
		})
	})
}
