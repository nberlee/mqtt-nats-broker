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
	"testing"
	"time"
)

// The behavioural half of the pending-entry closure (F-105 cutover checkpoint; plan §6.1). The
// structural half is pending_entry_structure_test.go.

// testPendingPubRel is the resident PUBREL entry at pi.
func testPendingPubRel(e *testInflightEnv, pi uint16) *mqttPending {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	return e.sess.pendingPubRel[pi]
}

// testPendingPublish is the resident PUBLISH entry at pi.
func testPendingPublish(e *testInflightEnv, pi uint16) *mqttPending {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	return e.sess.pendingPublish[pi]
}

// testMappedTo reports whether any source mapping names pi.
func testMappedTo(e *testInflightEnv, pi uint16) bool {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	for _, m := range e.sess.cpending {
		for _, p := range m {
			if p == pi {
				return true
			}
		}
	}
	return false
}

// testPlantPlaceholder installs the scan's lineage-less PUBREL placeholder for the seeded exchange
// through production's constructor and writer, as the resume's PUBREL scan does.
func testPlantPlaceholder(t *testing.T, e *testInflightEnv, pi uint16, nonce string, sseq uint64) {
	t.Helper()
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if !e.sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectEmpty(),
		mqttNewPubRelPlaceholder(testRealPubRelSrcDur, sseq, nonce)) {
		t.Fatal("fixture: the placeholder was not installed")
	}
}

// testCompleteRemotely records the exchange's PUBCOMP durably the way another broker that captured
// it would: the owner-independent authority batch alone (op-ack-advance to the terminal phase),
// with none of this session's local finalization, so nothing here observes the ending until the
// route under test does.
func testCompleteRemotely(t *testing.T, e *testInflightEnv, ref mqttInflightSlotRef, lin string) {
	t.Helper()
	cur, smsg, err := e.asm.loadInflightRecord(ref.name.subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("fixture: the slot does not load: %+v (%v)", cur, err)
	}
	x, _, err := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, ref.name.gen, lin)
	if err != nil || x == nil {
		t.Fatalf("fixture: the exchange does not load: %+v (%v)", x, err)
	}
	to, perr := mqttPhaseForAck(mqttAckArrival{Packet: mqttPacketPubComp, Reason: mqttReasonSuccess,
		OutstandingOutput: cur.ActionToken != _EMPTY_}, x.QoS)
	if perr != nil {
		t.Fatalf("fixture: %v", perr)
	}
	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-ack-advance",
		Gen: ref.name.gen, Nonce: lin, To: to, Acq: cur.Acq, Register: cur, RegisterSeq: smsg.Sequence,
	}); err != nil {
		t.Fatalf("fixture: the remote completion was not recorded: %v", err)
	}
	x, _, err = e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, ref.name.gen, lin)
	if err != nil || x == nil || !mqttExchangePhaseEnded(x.Phase) {
		t.Fatalf("fixture: the exchange must have ended: %+v (%v)", x, err)
	}
}

// T1 — the round-5 schedule (P7b-5), both acquisition routes. An ordinary PUBREL whose resident
// entry is the scan's LINEAGE-LESS placeholder is delivered; the acquired action must become an
// Ordinary entry carrying the ACTION's evidence, and when the exchange completes elsewhere and the
// ending is observed here, that entry, its mapping, its identifier and its Receive Maximum charge
// must all go. At f6e7feb the entry kept an empty lineage and survived the release.
//
// Mutations M2 (the callback's in-place attach restored) and M2b (the promotion takes the
// resident's evidence): the "Ordinary with the record's lineage" assertion.
func TestPendingClosureDeliveredPubRelGoesWithItsLineage(t *testing.T) {
	// Not parallel: it drives a process-wide test seam, which any concurrent test would reach.
	for _, route := range []struct {
		name    string
		observe func(e *testInflightEnv, gen, lin string)
	}{
		{"target observation", func(e *testInflightEnv, gen, lin string) {
			e.sess.observeCustodyTarget(gen, mqttCustodyTarget{kind: mqttCustodyTargetExchange, key: lin})
		}},
		{"census", func(e *testInflightEnv, gen, lin string) {
			e.sess.rebuildCustodyForResume(gen)
		}},
	} {
		for _, acq := range []string{"ready acquisition", "direct arrow"} {
			route, acq := route, acq
			t.Run(acq+"/"+route.name, func(t *testing.T) {
				e := testNewInflightEnv(t, "pending-t1")
				testReadyForRealPubRelDelivery(t, e)
				testWireInflightEnv(t, e) // its PUBREL is transmitted, so its PUBCOMP is owed (F-116)
				var pi uint16
				var nonce, lin string
				var canon uint64
				if acq == "ready acquisition" {
					seed := testSeedRealPubRelReady(t, e)
					pi, nonce, lin, canon = seed.pi, seed.nonce, seed.lin, seed.canon
				} else {
					// The direct arrow: the slot is still at `publish` and a canonical PUBREL
					// exists (a PUBREC whose phase CAS lost).
					pi = e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, 2)
					e.sess.mu.Lock()
					subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
					e.sess.mu.Unlock()
					cur, _, err := e.asm.loadInflightRecord(subject)
					if err != nil || cur == nil || cur.State != mqttInflightPublish {
						t.Fatalf("fixture: the admitted slot is %+v (%v), want publish", cur, err)
					}
					lin = cur.Lin
					nonce = e.nonceOf(pi)
					canon = testStoreCanonical(t, e, pi, nonce, testRealPubRelSrcSeq)
				}
				// The resume's scan leaves only the lineage-less placeholder (the in-memory state
				// is forgotten; custody keeps the lineage, as the inventory rebuild would).
				e.forgetInMemory()
				testPlantPlaceholder(t, e, pi, nonce, testRealPubRelSrcSeq)
				testCustodyRM1(t, e.sess)
				if rel := testPendingPubRel(e, pi); rel == nil || rel.kind != mqttPendingHold || rel.ev.lin != _EMPTY_ {
					t.Fatalf("fixture: the resident must be the lineage-less placeholder: %+v", rel)
				}
				if !testCustodyHasExchange(e.sess, lin) {
					t.Fatal("fixture: custody must hold the exchange's lineage")
				}
				writes := testCountPubRelWrites(t, e, func() {
					testDeliverPubRelAs(t, e, pi, testInflightAckSubject(e.sess.pubRelDurable(), canon, 1), nonce)
				})
				rel := testPendingPubRel(e, pi)
				if rel == nil || rel.kind != mqttPendingOrdinary || rel.ev.lin != lin {
					t.Fatalf("the acquired action must become an Ordinary entry with the record's lineage %q: %+v", lin, rel)
				}
				if writes != 1 {
					t.Fatalf("the PUBREL must be emitted exactly once, got %d", writes)
				}
				// The delivery gives its durable action back once the write returns; the client's
				// PUBCOMP, captured by another broker, arrives after that.
				testActionDrained(t, e, pi)
				e.sess.mu.Lock()
				gen, ref := e.sess.gen, rel.slot
				e.sess.mu.Unlock()
				testCompleteRemotely(t, e, ref, lin)
				if testPendingPubRel(e, pi) == nil {
					t.Fatal("fixture: nothing local may observe the remote ending before the route under test")
				}
				route.observe(e, gen, lin)
				// The ending REVOKES the entry's authority: what remains is the released
				// delivery, a Hold kept only because this connection transmitted the PUBREL and
				// still owes the client's PUBCOMP (consult 97). It acts for nothing, and the
				// ledger charge it keeps means Receive Maximum reopens at once.
				rel = testPendingPubRel(e, pi)
				if rel == nil || rel.kind != mqttPendingHold || rel.awaitingPubCompOwner == 0 ||
					rel.jsAckSubject == _EMPTY_ || !rel.custody {
					t.Fatalf("the %s must leave a released-delivery Hold keeping its ACK subject and charge: %+v", route.name, rel)
				}
				if got := testCustodyCapacity(e.sess); got != 1 {
					t.Fatalf("Receive Maximum 1 must reopen once the exchange ended, capacity %d", got)
				}
				e.sess.mu.Lock()
				held := e.sess.custody.holdsPI(pi)
				e.sess.mu.Unlock()
				if held {
					t.Fatal("custody must not still hold the ended exchange's identifier")
				}
				// The IDENTIFIER is this connection's to hold until its answer can no longer
				// come: once the connection is no longer the session's, the released delivery is
				// retired (at the next allocation) and no pending entry, mapping or custody hold
				// names the identifier. What still reserves it is exactly the durable slot's
				// exclusion — the terminal slot stands until its cleanup, which the completing
				// broker owns — and nothing the closure owns.
				e.sess.mu.Lock()
				e.sess.c = &client{cid: e.c.cid + 1000}
				e.sess.mu.Unlock()
				testCustodyMints(e.sess, pi)
				e.sess.mu.Lock()
				_, inPub := e.sess.pendingPublish[pi]
				_, inRel := e.sess.pendingPubRel[pi]
				heldNow := e.sess.custody.holdsPI(pi)
				h := e.sess.sendsInFlight[pi]
				onlyExclusion := h == nil || (h.token == _EMPTY_ && !e.sess.pubCompHeldLocked(h) && !actionHeldLocked(h) &&
					h.terminalOwed == _EMPTY_ && !e.sess.retainedHeldLocked(h))
				e.sess.c = e.c
				e.sess.mu.Unlock()
				if inPub || inRel || heldNow || testMappedTo(e, pi) || !onlyExclusion {
					t.Fatalf("with its owner gone nothing the closure owns may name the identifier: publish %v pubrel %v custody %v mapped %v, only the slot exclusion %v",
						inPub, inRel, heldNow, testMappedTo(e, pi), onlyExclusion)
				}
			})
		}
	}
}

// T1's constructor-level oracle (M2b, reported separately from T1's behavioural kill).
func TestPendingClosurePromotionTakesTheActionsEvidence(t *testing.T) {
	t.Parallel()
	placeholder := mqttNewPubRelPlaceholder("SRC", 41, "nonce-1")
	act := &mqttSendAction{lin: "lin-1", slot: mqttInflightSlotRef{
		name: mqttInflightName{gen: "g1", pi: 3}, nonce: "nonce-1", dur: "SRC", sseq: 41, seq: 7,
	}}
	next := mqttPromotePubRelAction(placeholder, act, "PUBREL", 9, "reply")
	if next.kind != mqttPendingOrdinary || next.ev != (mqttPendingEvidence{gen: "g1", lin: "lin-1", nonce: "nonce-1"}) {
		t.Fatalf("the promotion must carry the action's evidence, got kind %d ev %+v", next.kind, next.ev)
	}
	if next.regNonce != "nonce-1" || next.slot.seq != 7 || next.jsAckSubject != "reply" {
		t.Fatalf("the promotion must carry the action's slot and the delivery: %+v", next)
	}
}

// testPubRelWorld builds, for acquisition route acq, an ordinary exchange whose canonical PUBREL
// is stored and whose only resident entry is the scan's lineage-less placeholder, with custody
// holding the lineage and Receive Maximum 1.
func testPubRelWorld(t *testing.T, name, acq string) (e *testInflightEnv, pi uint16, nonce, lin string, canon uint64) {
	t.Helper()
	e = testNewInflightEnv(t, name)
	testReadyForRealPubRelDelivery(t, e)
	if acq == "ready acquisition" {
		seed := testSeedRealPubRelReady(t, e)
		pi, nonce, lin, canon = seed.pi, seed.nonce, seed.lin, seed.canon
	} else {
		pi = e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, 2)
		e.sess.mu.Lock()
		subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
		e.sess.mu.Unlock()
		cur, _, err := e.asm.loadInflightRecord(subject)
		if err != nil || cur == nil || cur.State != mqttInflightPublish {
			t.Fatalf("fixture: the admitted slot is %+v (%v), want publish", cur, err)
		}
		lin, nonce = cur.Lin, e.nonceOf(pi)
		canon = testStoreCanonical(t, e, pi, nonce, testRealPubRelSrcSeq)
	}
	e.forgetInMemory()
	testPlantPlaceholder(t, e, pi, nonce, testRealPubRelSrcSeq)
	testCustodyRM1(t, e.sess)
	if !testCustodyHasExchange(e.sess, lin) {
		t.Fatal("fixture: custody must hold the exchange's lineage")
	}
	return e, pi, nonce, lin, canon
}

// testReleaseLineageLocked is an accepted ending of lin as acceptExchangeObservationLocked applies
// it: the ledger releases the holder and every entry carrying the lineage's evidence goes.
//
// Lock held on entry.
func testReleaseLineageLocked(sess *mqttSession, lin string) {
	if x := sess.custody.heldExchanges[lin]; x != nil {
		sess.custody.releaseExchange(lin, x)
	}
	for _, l := range sess.custody.drainReleased() {
		sess.removeLineageProjectionsLocked(sess.gen, l)
	}
}

// testActionDrained waits for the durable action the delivery acquired to be given back.
func testActionDrained(t *testing.T, e *testInflightEnv, pi uint16) {
	t.Helper()
	e.sess.mu.Lock()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	e.sess.mu.Unlock()
	testCustodyEventually(t, "the acquired action was drained", func() bool {
		rec, smsg, err := e.asm.loadInflightRecord(subject)
		return err == nil && smsg != nil && rec != nil && rec.ActionToken == _EMPTY_
	})
}

// T2 — a promotion the session may not make changes nothing: three residents, both acquisition
// routes. The seam sits between the unlocked acquisition and the locked promotion.
//
// Mutations M1 (the custody condition removed) and M6 (any resident accepted).
func TestPendingClosureRefusedPromotionChangesNothing(t *testing.T) {
	// Not parallel: it drives a process-wide test seam, which any concurrent test would reach.
	for _, acq := range []string{"ready acquisition", "direct arrow"} {
		for _, resident := range []string{"absent, released", "incompatible", "compatible, released"} {
			acq, resident := acq, resident
			t.Run(acq+"/"+resident, func(t *testing.T) {
				e, pi, nonce, lin, canon := testPubRelWorld(t, "pending-t2", acq)
				var planted *mqttPending
				restore := mqttSetTestSeam(&mqttTestBeforePubRelPromotionV, func() {
					e.sess.mu.Lock()
					defer e.sess.mu.Unlock()
					switch resident {
					case "absent, released":
						e.sess.untrackPubRel(pi)
						testReleaseLineageLocked(e.sess, lin)
					case "incompatible":
						cur := e.sess.pendingPubRel[pi]
						planted = mqttNewPubRelPlaceholder(testRealPubRelSrcDur, testRealPubRelSrcSeq, "another-exchange")
						e.sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectExactly(cur), planted)
					case "compatible, released":
						planted = e.sess.pendingPubRel[pi]
						testReleaseLineageLocked(e.sess, lin)
					}
				})
				t.Cleanup(restore)
				mappedBefore := testMappedTo(e, pi)
				writes := testCountPubRelWrites(t, e, func() {
					testDeliverPubRelAs(t, e, pi, testInflightAckSubject(e.sess.pubRelDurable(), canon, 1), nonce)
				})
				restore()
				if writes != 0 {
					t.Fatalf("a refused promotion must write nothing, got %d PUBREL writes", writes)
				}
				rel := testPendingPubRel(e, pi)
				switch resident {
				case "absent, released":
					if rel != nil || testMappedTo(e, pi) {
						t.Fatalf("no entry and no mapping may appear: entry %+v, mapped %v", rel, testMappedTo(e, pi))
					}
				default:
					if rel != planted {
						t.Fatalf("the resident must be the same pointer, untouched: got %+v want %+v", rel, planted)
					}
					if rel.kind != mqttPendingHold || rel.jsAckSubject != _EMPTY_ || rel.slot.slotOK() {
						t.Fatalf("the resident's delivery facts must be unchanged: %+v", rel)
					}
					if testMappedTo(e, pi) != mappedBefore {
						t.Fatal("the resident's mapping changed")
					}
				}
				testActionDrained(t, e, pi)
			})
		}
	}
}

// testHoldCopy plants, in place of the resident entry, a copy that is identical except that it
// ACTS FOR NOTHING. Every other predicate a selector checks passes on it, so only the kind decides.
// (A fixture's claim, deliberately made outside the constructors: production never builds this.)
func testHoldCopy(t *testing.T, e *testInflightEnv, which mqttPendingMapID, pi uint16) *mqttPending {
	t.Helper()
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	cur := e.sess.pendingMapLocked(which)[pi]
	if cur == nil || !cur.acts() {
		t.Fatalf("fixture: the resident must be an acting entry: %+v", cur)
	}
	h := *cur
	h.kind = mqttPendingHold
	e.sess.pendingMapLocked(which)[pi] = &h
	return &h
}

// T3 — a Hold is refused at every acts() selector, each with its acting positive control.
//
// Mutations M4.1-M4.6 (acts() removed from each selector).
func TestPendingClosureAHoldActsForNothing(t *testing.T) {
	t.Parallel()
	admitted := func(t *testing.T, name string) (*testInflightEnv, uint16, uint64) {
		e := testNewInflightEnv(t, name)
		testReadyForRealPubRelDelivery(t, e)
		pi := e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, 2)
		return e, pi, testRealPubRelSrcSeq
	}
	for _, hold := range []bool{false, true} {
		hold := hold
		label := map[bool]string{false: "acting control", true: "hold"}[hold]
		t.Run("beginSendLocked/"+label, func(t *testing.T) {
			t.Parallel()
			e, pi, sseq := admitted(t, "pending-t3-send")
			if hold {
				testHoldCopy(t, e, mqttPendingPublishMap, pi)
			}
			e.sess.mu.Lock()
			tok, ok := e.sess.beginSendLocked(e.c, pi, testRealPubRelSrcDur, sseq)
			if ok {
				e.sess.mu.Unlock()
				e.sess.endSend(e.c, pi, tok.token)
			} else {
				e.sess.mu.Unlock()
			}
			if ok == hold {
				t.Fatalf("beginSendLocked opened=%v for a %s", ok, label)
			}
		})
		t.Run("trackPublish resident arm/"+label, func(t *testing.T) {
			t.Parallel()
			e, pi, sseq := admitted(t, "pending-t3-track")
			if hold {
				testHoldCopy(t, e, mqttPendingPublishMap, pi)
			}
			e.sess.mu.Lock()
			got, isNew := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, sseq, 2))
			e.sess.mu.Unlock()
			if isNew || (got == pi) == hold {
				t.Fatalf("a redelivery onto a %s tracked pi %d (new %v)", label, got, isNew)
			}
		})
		t.Run("mqttPendingRedeliverable/"+label, func(t *testing.T) {
			t.Parallel()
			e, pi, _ := admitted(t, "pending-t3-redeliverable")
			if hold {
				testHoldCopy(t, e, mqttPendingPublishMap, pi)
			}
			if got := mqttPendingRedeliverable(testPendingPublish(e, pi)); got == hold {
				t.Fatalf("mqttPendingRedeliverable=%v for a %s", got, label)
			}
		})
		t.Run("drainClaimEligibleLocked/"+label, func(t *testing.T) {
			t.Parallel()
			e, pi, _ := admitted(t, "pending-t3-drain")
			if hold {
				testHoldCopy(t, e, mqttPendingPublishMap, pi)
			}
			e.sess.mu.Lock()
			_, eligible, _ := e.sess.drainClaimEligibleLocked(pi)
			e.sess.mu.Unlock()
			if eligible == hold {
				t.Fatalf("drainClaimEligibleLocked eligible=%v for a %s", eligible, label)
			}
		})
		t.Run("PUBREL resume loop/"+label, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "pending-t3-resume")
			testReadyForRealPubRelDelivery(t, e)
			seed := testSeedRealPubRelReady(t, e)
			reply := testInflightAckSubject(seed.pubRel, seed.canon, 1)
			testDeliverPubRelAs(t, e, seed.pi, reply, seed.nonce)
			rel := testPendingPubRel(e, seed.pi)
			if rel == nil || rel.jsAckSubject != reply {
				t.Fatalf("fixture: the delivered entry must carry its ACK subject: %+v", rel)
			}
			if hold {
				testHoldCopy(t, e, mqttPendingPubRelMap, seed.pi)
			}
			sub, err := e.s.nrc.SubscribeSync(reply)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }() // cleanup; the connection closes with the test
			e.sess.resendPendingRedelivery(e.c, nil,
				map[mqttPendingRef]struct{}{{seed.pi, rel.sseq, rel.jsDur}: {}})
			_, nerr := sub.NextMsg(testPendingNakWait)
			if (nerr == nil) == hold {
				t.Fatalf("the resume NAKed=%v the PUBREL of a %s", nerr == nil, label)
			}
		})
		t.Run("duplicate-PUBREC NAK/"+label, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "pending-t3-dupnak")
			testReadyForRealPubRelDelivery(t, e)
			seed := testSeedRealPubRelReady(t, e)
			reply := testInflightAckSubject(seed.pubRel, seed.canon, 1)
			testDeliverPubRelAs(t, e, seed.pi, reply, seed.nonce)
			if hold {
				testHoldCopy(t, e, mqttPendingPubRelMap, seed.pi)
			}
			sub, err := e.s.nrc.SubscribeSync(reply)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }() // cleanup; the connection closes with the test
			_ = e.c.mqttProcessPublishReceived(seed.pi, true, mqttReasonSuccess)
			_, nerr := sub.NextMsg(testPendingNakWait)
			if (nerr == nil) == hold {
				t.Fatalf("a duplicate PUBREC NAKed=%v the PUBREL of a %s", nerr == nil, label)
			}
		})
	}
}

// testPendingNakWait bounds how long a test waits to see a NAK that must, or must not, be sent.
const testPendingNakWait = 500 * time.Millisecond
