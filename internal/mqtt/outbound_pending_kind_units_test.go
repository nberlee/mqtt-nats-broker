// Copyright 2026 The mqtt-nats-bridge Authors
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

//go:build mqttport_fulltests

package mqtt

import (
	"testing"
)

// The pending-entry closure's transition contracts, one per writer (plan §6.1 T4-T21). Each drives
// the production writer against the resident and custody state its contract names, with the
// positive control beside the refusal.

// testAllocation takes a fresh allocation for (SRC, sseq) through trackPublish, as a delivery does.
func testAllocation(t *testing.T, e *testInflightEnv, sseq uint64) uint16 {
	t.Helper()
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	pi, isNew := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, sseq, 1))
	if pi == 0 || !isNew {
		t.Fatalf("fixture: no fresh allocation (pi %d, new %v)", pi, isNew)
	}
	return pi
}

// T4 — retained authority is a KIND. An entry of the retained SHAPE (no register, this
// generation's PUBREL durable, the owner's nonce) is admitted only when it is Retained.
//
// Mutation M5 (the shape inference restored).
func TestPendingClosureRetainedAdmissionIsTheKind(t *testing.T) {
	t.Parallel()
	for _, kind := range []mqttPendingKind{mqttPendingRetained, mqttPendingOrdinary, mqttPendingHold} {
		kind := kind
		t.Run(map[mqttPendingKind]string{mqttPendingRetained: "retained", mqttPendingOrdinary: "ordinary", mqttPendingHold: "hold"}[kind], func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "pending-t4")
			testReadyForRealPubRelDelivery(t, e)
			const pi = uint16(9)
			nonce := mqttNewInflightNonce()
			e.sess.mu.Lock()
			e.sess.pendingMapLocked(mqttPendingPubRelMap)[pi] = &mqttPending{kind: kind, regNonce: nonce,
				jsDur: e.sess.pubRelDurable(), qos: 2}
			e.sess.mu.Unlock()
			got := e.sess.retainedPubRelAdmittedLocked_(e.c, pi, mqttPubRelOwner{Nonce: nonce})
			if got != (kind == mqttPendingRetained) {
				t.Fatalf("retained admission=%v for kind %d", got, kind)
			}
		})
	}
}

// T5 — a landing whose lineage custody no longer holds promotes nothing: the allocation and its
// mapping go. Positive control: custody holds it, and the allocation becomes Ordinary.
func TestPendingClosureConfirmationNeedsCustody(t *testing.T) {
	t.Parallel()
	for _, held := range []bool{true, false} {
		held := held
		t.Run(map[bool]string{true: "held", false: "released"}[held], func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "pending-t5")
			testReadyForRealPubRelDelivery(t, e)
			pi := testAllocation(t, e, 71)
			nonce, lin := e.nonceOf(pi), mqttNewInflightNonce()
			e.sess.mu.Lock()
			gen := e.sess.gen
			if held {
				testCustodyEnsureExchange(e.sess.custody, gen, lin, pi)
			}
			e.sess.mu.Unlock()
			ok := e.sess.confirmInflightRegister(e.c, mqttLandedRegister{gen: gen, pi: pi, jsDur: testRealPubRelSrcDur,
				sseq: 71, regSeq: 5, nonce: nonce, lin: lin, qos: 2}, 3, false)
			p := testPendingPublish(e, pi)
			if held {
				if !ok || p == nil || p.kind != mqttPendingOrdinary || p.ev.lin != lin {
					t.Fatalf("a held landing must promote to Ordinary with its lineage: ok %v entry %+v", ok, p)
				}
				return
			}
			if ok || p != nil || testMappedTo(e, pi) {
				t.Fatalf("a released landing must leave no entry and no mapping: ok %v entry %+v mapped %v", ok, p, testMappedTo(e, pi))
			}
		})
	}
}

// testOrdinaryResident lands an ordinary allocation and returns it with its lineage.
func testOrdinaryResident(t *testing.T, e *testInflightEnv, sseq uint64) (uint16, string) {
	t.Helper()
	pi := testAllocation(t, e, sseq)
	nonce, lin := e.nonceOf(pi), mqttNewInflightNonce()
	e.sess.mu.Lock()
	gen := e.sess.gen
	testCustodyEnsureExchange(e.sess.custody, gen, lin, pi)
	e.sess.mu.Unlock()
	if !e.sess.confirmInflightRegister(e.c, mqttLandedRegister{gen: gen, pi: pi, jsDur: testRealPubRelSrcDur,
		sseq: sseq, regSeq: 5, nonce: nonce, lin: lin, qos: 2}, 3, false) {
		t.Fatal("fixture: the ordinary landing was refused")
	}
	return pi, lin
}

// T6 — a rollback puts back the exact entry its caller removed, through the writer's gate: while
// custody holds the lineage it is reinstated; after a release nothing is. The resolver's
// reinstallation takes a VALUE COPY through the same function.
//
// Mutation M1.
func TestPendingClosureRollbackReinstallsOnlyWhileHeld(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		release  bool
		valueCpy bool
	}{{"held", false, false}, {"released", true, false}, {"resolver copy, held", false, true}, {"resolver copy, released", true, true}} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "pending-t6")
			testReadyForRealPubRelDelivery(t, e)
			pi, lin := testOrdinaryResident(t, e, 72)
			e.sess.mu.Lock()
			removed := e.sess.untrackPublishEntry(pi)
			if tc.release {
				testReleaseLineageLocked(e.sess, lin)
			}
			back := removed
			if tc.valueCpy {
				cp := *removed
				back = &cp
			}
			e.sess.retrackPublishLocked(pi, back)
			got := e.sess.pendingPublish[pi]
			e.sess.mu.Unlock()
			if tc.release && got != nil {
				t.Fatalf("a released lineage's entry must not be reinstated: %+v", got)
			}
			if !tc.release && got != back {
				t.Fatalf("a held lineage's entry must be reinstated as the same value: %+v", got)
			}
		})
	}
}

// T7 / T16 — a reactivation replaces exactly the captured demoted entry, through the gate: a
// released lineage stays demoted, a capture no longer resident is never brought back.
//
// Mutations M1 and M13 (the EXACTLY check removed).
func TestPendingClosureReactivationIsGatedAndExact(t *testing.T) {
	t.Parallel()
	for _, tc := range []string{"held", "released", "removed"} {
		tc := tc
		t.Run(tc, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "pending-t7")
			testReadyForRealPubRelDelivery(t, e)
			pi, lin := testOrdinaryResident(t, e, 73)
			e.sess.mu.Lock()
			cur := e.sess.pendingPublish[pi]
			e.sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectExactly(cur), mqttNewDemoted(cur))
			demoted := e.sess.pendingPublish[pi]
			rec := &mqttInflightRecord{Nonce: demoted.ev.nonce, Lin: lin, Dur: testRealPubRelSrcDur}
			switch tc {
			case "released":
				testCustodyEnsureExchange(e.sess.custody, e.sess.gen, lin, pi)
				x := e.sess.custody.heldExchanges[lin]
				e.sess.custody.releaseExchange(lin, x)
				e.sess.custody.drainReleased()
			case "removed":
				e.sess.untrackPublishEntry(pi)
			}
			ok := e.sess.reactivateDemotedLocked(mqttPendingPublishMap, pi, demoted, rec)
			got := e.sess.pendingPublish[pi]
			e.sess.mu.Unlock()
			switch tc {
			case "held":
				if !ok || got == nil || got.kind != mqttPendingOrdinary || got.regRecoveryDemoted {
					t.Fatalf("a held reactivation must restore the Ordinary entry: ok %v %+v", ok, got)
				}
			case "released":
				if ok || got != demoted {
					t.Fatalf("a released lineage must stay demoted: ok %v %+v", ok, got)
				}
			case "removed":
				if ok || got != nil {
					t.Fatalf("a capture no longer resident must never come back: ok %v %+v", ok, got)
				}
			}
		})
	}
}

// T8 — the PUBREC route advances only its own exchange's PUBREL entry: a resident of another
// nonce, swapped in after the phase CAS and before the locked update, keeps its slot.
//
// Mutation M8.
func TestPendingClosurePubRecPhaseAdvanceIsItsOwn(t *testing.T) {
	// Not parallel: it drives a process-wide test seam, which any concurrent test would reach.
	e := testNewInflightEnv(t, "pending-t8")
	testReadyForRealPubRelDelivery(t, e)
	pi := e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, 2)
	var stranger *mqttPending
	restore := mqttSetTestSeam(&mqttTestAfterPubRelPhaseCASV, func() {
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		stranger = mqttNewPubRelPlaceholder(testRealPubRelSrcDur, testRealPubRelSrcSeq, "another-exchange")
		e.sess.pendingMapLocked(mqttPendingPubRelMap)[pi] = stranger
	})
	defer restore()
	if err := e.c.mqttProcessPublishReceived(pi, true, mqttReasonSuccess); err != nil {
		t.Fatalf("PUBREC: %v", err)
	}
	restore()
	if stranger == nil {
		t.Fatal("fixture: the seam after the phase CAS never ran")
	}
	rel := testPendingPubRel(e, pi)
	if rel != stranger || rel.slot.slotOK() || rel.regSeq != 0 || rel.regNonce != "another-exchange" {
		t.Fatalf("another exchange's entry must keep its identity: %+v", rel)
	}
}

// T9 — a Hold naming a lineage goes with it (the landed-route marker-opaque reservation).
//
// Mutation M3 (the release restricted to Ordinary).
func TestPendingClosureNamingHoldGoesWithItsLineage(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "pending-t9")
	testReadyForRealPubRelDelivery(t, e)
	pi := testAllocation(t, e, 74)
	nonce, lin := e.nonceOf(pi), mqttNewInflightNonce()
	e.sess.mu.Lock()
	gen := e.sess.gen
	testCustodyEnsureExchange(e.sess.custody, gen, lin, pi)
	subject := mqttInflightSubject(e.sess.idHash, gen, pi)
	e.sess.mu.Unlock()
	installed, _ := e.sess.installMarkerOpaqueReservation(e.c, pi, testRealPubRelSrcDur, 74, nonce,
		mqttPendingEvidence{gen: gen, lin: lin, nonce: nonce}, 2, 5, subject, "test", false)
	p := testPendingPublish(e, pi)
	if !installed || p == nil || p.kind != mqttPendingHold || !p.markerOpaque || p.ev.lin != lin {
		t.Fatalf("fixture: the landed marker-opaque Hold must name its lineage: %+v", p)
	}
	e.sess.mu.Lock()
	testReleaseLineageLocked(e.sess, lin)
	e.sess.piSpaceIncomplete = false // the resume that read the pair whole
	e.sess.mu.Unlock()
	if p := testPendingPublish(e, pi); p != nil {
		t.Fatalf("the released lineage's Hold must go: %+v", p)
	}
	if got := testCustodyMints(e.sess, pi); got != pi {
		t.Fatalf("its identifier must be re-mintable, got %d", got)
	}
}

// T10 — a PUBREL successor inherits its predecessor's kind and evidence and never mints authority.
//
// Mutation M7.
func TestPendingClosureSuccessorInheritsItsKind(t *testing.T) {
	t.Parallel()
	ev := mqttPendingEvidence{gen: "g", lin: "l", nonce: "n"}
	for _, tc := range []struct {
		name     string
		rel, pub *mqttPending
		want     mqttPendingKind
	}{
		{"hold placeholder", mqttNewPubRelPlaceholder("SRC", 1, "n"), nil, mqttPendingHold},
		{"ordinary publish", nil, &mqttPending{kind: mqttPendingOrdinary, ev: ev, regNonce: "n"}, mqttPendingOrdinary},
		{"retained publish", nil, mqttNewRetained("n", 2), mqttPendingRetained},
		{"nothing", nil, nil, mqttPendingHold},
	} {
		next := mqttNewPubRelSuccessor(tc.rel, tc.pub, "PUBREL", 1, "reply")
		if next.kind != tc.want {
			t.Errorf("%s: successor kind %d, want %d", tc.name, next.kind, tc.want)
		}
		src := tc.rel
		if src == nil {
			src = tc.pub
		}
		if src != nil && next.ev != src.ev {
			t.Errorf("%s: successor evidence %+v, want %+v", tc.name, next.ev, src.ev)
		}
	}
}

// T11 — a Hold settles nothing from its resident fields: a PUBACK, or a failed PUBREC, for the
// unminted Hold's identifier acknowledges no source and removes no reservation.
//
// Mutation M4.7 (the ACK guard's acts() removed).
func TestPendingClosureAHoldSettlesNoSource(t *testing.T) {
	t.Parallel()
	for _, pubrec := range []bool{false, true} {
		pubrec := pubrec
		t.Run(map[bool]string{false: "PUBACK", true: "failed PUBREC"}[pubrec], func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "pending-t11")
			testReadyForRealPubRelDelivery(t, e)
			const pi = uint16(11)
			reply := testInflightAckSubject(testRealPubRelSrcDur, 75, 1)
			e.sess.mu.Lock()
			hold := mqttNewUnmintedHold(testRealPubRelSrcDur, 75, reply)
			e.sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectEmpty(), hold)
			e.sess.mu.Unlock()
			sub, err := e.s.nrc.SubscribeSync(reply)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()
			if pubrec {
				_ = e.c.mqttProcessPublishReceived(pi, true, 0x80)
			} else {
				_ = e.c.mqttProcessPubAck(pi)
			}
			if _, err := sub.NextMsg(testPendingNakWait); err == nil {
				t.Fatal("a Hold's ACK subject must not be acknowledged")
			}
			if p := testPendingPublish(e, pi); p != hold {
				t.Fatalf("the Hold must still be resident: %+v", p)
			}
		})
	}
}

// T12 — the PUBREL callback re-checks the exact kind where it enters the write: an entry swapped to
// a Hold after the commit writes nothing. Positive control: the seam alone, and the PUBREL goes out.
//
// Mutation M4.8.
func TestPendingClosurePubRelWriteNeedsAnActingEntry(t *testing.T) {
	// Not parallel: it drives a process-wide test seam, which any concurrent test would reach.
	for _, swap := range []bool{false, true} {
		swap := swap
		t.Run(map[bool]string{false: "control", true: "swapped to a hold"}[swap], func(t *testing.T) {
			e := testNewInflightEnv(t, "pending-t12")
			testReadyForRealPubRelDelivery(t, e)
			seed := testSeedRealPubRelReady(t, e)
			restore := mqttSetTestSeam(&mqttTestAfterPubRelCommitV, func() {
				if !swap {
					return
				}
				e.sess.mu.Lock()
				defer e.sess.mu.Unlock()
				cur := e.sess.pendingPubRel[seed.pi]
				h := *cur
				h.kind = mqttPendingHold
				e.sess.pendingMapLocked(mqttPendingPubRelMap)[seed.pi] = &h
			})
			defer restore()
			writes := testCountPubRelWrites(t, e, func() {
				testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
			})
			restore()
			if swap && writes != 0 {
				t.Fatalf("a Hold at write entry must write nothing, got %d", writes)
			}
			if !swap && writes != 1 {
				t.Fatalf("the control must write the PUBREL once, got %d", writes)
			}
			if swap {
				testActionDrained(t, e, seed.pi)
			}
		})
	}
}

// T13 — the abandoned-ACK completion snapshot carries the binding's evidence and is a Hold.
// (The handler's advance through it is covered by the abandoned-ACK suite, which this snapshot
// feeds; mutation M10 is run against that suite.)
func TestPendingClosureAckSnapshotCarriesTheBindingsEvidence(t *testing.T) {
	t.Parallel()
	b := &mqttAbandonedAckBinding{name: mqttInflightName{gen: "g", pi: 4}, nonce: "n", dur: "SRC", seq: 9, qos: 2,
		ev: mqttPendingEvidence{gen: "g", lin: "l", nonce: "n"}}
	s := mqttAckSnapshot(b)
	if s.kind != mqttPendingHold || s.ev != b.ev || s.slot.abandonedAck != b {
		t.Fatalf("the snapshot must be a Hold carrying the binding's evidence: %+v", s)
	}
}

// T14 — terminal conversions carry the obligation's evidence and ledger charge and no ACK subject;
// every terminal reservation, carried or fresh, survives its lineage's release and ends with its
// nonce-keyed release.
//
// Mutations M11, M15, M16, M17.
func TestPendingClosureTerminalConversions(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T) (*testInflightEnv, uint16, mqttInflightSlotRef, *mqttPending, string) {
		e := testNewInflightEnv(t, "pending-t14")
		testReadyForRealPubRelDelivery(t, e)
		seed := testSeedRealPubRelReady(t, e)
		rel := testPendingPubRel(e, seed.pi)
		if rel == nil || rel.kind != mqttPendingOrdinary {
			t.Fatalf("fixture: the PUBREC must leave an Ordinary successor: %+v", rel)
		}
		return e, seed.pi, rel.slot, rel, seed.lin
	}
	carried := func(t *testing.T, e *testInflightEnv, pi uint16, want *mqttPending) *mqttPending {
		t.Helper()
		got := testPendingPubRel(e, pi)
		if got == nil || got.kind != mqttPendingHold || got.ev != want.ev || got.custody != want.custody || got.jsAckSubject != _EMPTY_ {
			t.Fatalf("the terminal reservation must carry the evidence and charge, no ACK subject: %+v (from %+v)", got, want)
		}
		return got
	}
	t.Run("reconcile over an undelivered successor", func(t *testing.T) {
		t.Parallel()
		e, pi, ref, rel, lin := setup(t)
		e.sess.retainAndReconcileTerminal(e.c, ref)
		got := carried(t, e, pi, rel)
		if !got.terminalReconcile {
			t.Fatal("the reconcile must be marked on the reservation")
		}
		// A terminal reservation holds its identifier through the slot's cleanup, which the
		// exchange's ending PRECEDES: the lineage's release leaves it (consult 97).
		e.sess.mu.Lock()
		testReleaseLineageLocked(e.sess, lin)
		e.sess.mu.Unlock()
		if testPendingPubRel(e, pi) != got {
			t.Fatal("a terminal reservation must survive its lineage's release")
		}
	})
	t.Run("recovery installer over a same-nonce resident", func(t *testing.T) {
		t.Parallel()
		e, pi, ref, rel, _ := setup(t)
		e.sess.mu.Lock()
		ok := e.sess.installTerminalReservationLocked(ref)
		e.sess.mu.Unlock()
		if !ok {
			t.Fatal("the installer must replace this exchange's undelivered resident")
		}
		carried(t, e, pi, rel)
	})
	t.Run("recovery installer refuses a delivered resident", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "pending-t14-delivered")
		testReadyForRealPubRelDelivery(t, e)
		seed := testSeedRealPubRelReady(t, e)
		testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
		before := testPendingPubRel(e, seed.pi)
		e.sess.mu.Lock()
		ok := e.sess.installTerminalReservationLocked(before.slot)
		e.sess.mu.Unlock()
		if ok || testPendingPubRel(e, seed.pi) != before {
			t.Fatal("a delivered resident is not the installer's to replace")
		}
	})
	t.Run("PUBCOMP conversion, ordinary", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "pending-t14-pubcomp")
		testReadyForRealPubRelDelivery(t, e)
		seed := testSeedRealPubRelReady(t, e)
		testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
		// A redelivery refreshed the ACK subject before the conversion ran.
		testDeliverPubRelAs(t, e, seed.pi, seed.reply(2), seed.nonce)
		rel := testPendingPubRel(e, seed.pi)
		e.sess.mu.Lock()
		subj, reserved := e.sess.pubCompConvertLocked(seed.pi, rel, rel.slot)
		mapped := false
		for _, m := range e.sess.cpending {
			if p, ok := m[rel.sseq]; ok && p == seed.pi {
				mapped = true
			}
		}
		e.sess.mu.Unlock()
		if !reserved || subj != seed.reply(2) {
			t.Fatalf("the conversion must reserve and return the latest ACK subject: %q reserved %v", subj, reserved)
		}
		if mapped {
			t.Fatal("the delivered entry's exact mapping must go")
		}
		carried(t, e, seed.pi, rel)
	})
	t.Run("PUBCOMP conversion, retained", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "pending-t14-retained")
		testReadyForRealPubRelDelivery(t, e)
		const pi = uint16(14)
		e.sess.mu.Lock()
		ret := mqttNewPubRelSuccessor(nil, mqttNewRetained("rn", 2), e.sess.pubRelDurable(), 3, "retained-reply")
		e.sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectEmpty(), ret)
		subj, reserved := e.sess.pubCompConvertLocked(pi, ret, ret.slot)
		got := e.sess.pendingPubRel[pi]
		e.sess.mu.Unlock()
		if reserved || subj != "retained-reply" || got != nil {
			t.Fatalf("a registerless retained exchange is plainly removed: %q reserved %v entry %+v", subj, reserved, got)
		}
	})
	t.Run("fresh ref-only reservation", func(t *testing.T) {
		t.Parallel()
		e, pi, ref, _, lin := setup(t)
		e.sess.mu.Lock()
		e.sess.untrackPubRel(pi)
		ok := e.sess.installTerminalReservationLocked(ref)
		testReleaseLineageLocked(e.sess, lin)
		got := e.sess.pendingPubRel[pi]
		e.sess.mu.Unlock()
		if !ok || got == nil || got.ev.lin != _EMPTY_ {
			t.Fatalf("a fresh reservation names only its nonce and survives the lineage's release: ok %v %+v", ok, got)
		}
		e.sess.releaseTerminalReservation(ref)
		if testPendingPubRel(e, pi) != nil {
			t.Fatal("the nonce-keyed release removes it")
		}
	})
}

// T15 — an affine PUBREL action has no exchange and no custody: it becomes an Affine entry, and a
// demoted slot-only entry reactivates as Affine, with no custody gate.
//
// Mutation M12.
func TestPendingClosureAffineActionNeedsNoCustody(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "pending-t15")
	testReadyForRealPubRelDelivery(t, e)
	const pi = uint16(15)
	dur := mqttQ2AffineConsumerDurablePrefix + "test"
	e.sess.mu.Lock()
	act := &mqttSendAction{slot: mqttInflightSlotRef{name: mqttInflightName{gen: e.sess.gen, pi: pi}, nonce: "an", dur: dur, sseq: 3, seq: 4}}
	ok := e.sess.bindActionToPubRelLocked(pi, act, testInflightAckSubject(e.sess.pubRelDurable(), 8, 1), mqttPubRelAffineRef{})
	got := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if !ok || got == nil || got.kind != mqttPendingAffine {
		t.Fatalf("an affine action must bind without custody: ok %v %+v", ok, got)
	}
	demoted := mqttNewDemoted(got)
	back := mqttReactivate(demoted, e.sess.gen, &mqttInflightRecord{Dur: dur, Nonce: "an"})
	if back == nil || back.kind != mqttPendingAffine {
		t.Fatalf("a demoted slot-only entry reactivates as Affine: %+v", back)
	}
}

// T17 — the occupant-route marker-opaque conversion is fenced on custody's holder history since the
// allocation: installed over an occupant the ledger never held; refused, with local cleanup only,
// once any holder was created or the connection changed.
//
// Mutations M21 (the epoch comparison removed), M22 (the adoption path does not advance the epoch).
func TestPendingClosureOccupantReservationIsFenced(t *testing.T) {
	t.Parallel()
	for _, tc := range []string{"unheld", "adopt then release", "adopted by reconstruction then released", "held", "rebind", "unrelated holder"} {
		tc := tc
		t.Run(tc, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "pending-t17")
			testReadyForRealPubRelDelivery(t, e)
			pi := testAllocation(t, e, 76)
			nonce := e.nonceOf(pi)
			occupant := mqttPendingEvidence{lin: mqttNewInflightNonce(), nonce: mqttNewInflightNonce()}
			e.sess.mu.Lock()
			gen := e.sess.gen
			occupant.gen = gen
			subject := mqttInflightSubject(e.sess.idHash, gen, pi)
			switch tc {
			case "adopt then release":
				testCustodyEnsureExchange(e.sess.custody, gen, occupant.lin, pi)
				testReleaseLineageLocked(e.sess, occupant.lin)
			case "adopted by reconstruction then released":
				e.sess.custody.adoptReadExchange(&mqttOutboundExchange{Gen: gen, Nonce: occupant.lin, PI: pi, Phase: mqttExchangePublish})
				testReleaseLineageLocked(e.sess, occupant.lin)
			case "held":
				testCustodyEnsureExchange(e.sess.custody, gen, occupant.lin, pi)
			case "unrelated holder":
				testCustodyEnsureExchange(e.sess.custody, gen, mqttNewInflightNonce(), pi+1)
			case "rebind":
				e.sess.c = &client{}
			}
			e.sess.mu.Unlock()
			installed, refused := e.sess.installMarkerOpaqueReservation(e.c, pi, testRealPubRelSrcDur, 76, nonce,
				occupant, 2, 5, subject, "test", true)
			e.sess.mu.Lock()
			p := e.sess.pendingPublish[pi]
			incomplete := e.sess.piSpaceIncomplete
			e.sess.c = e.c
			e.sess.mu.Unlock()
			if tc == "unheld" {
				if !installed || p == nil || p.ev != occupant || p.kind != mqttPendingHold || !incomplete {
					t.Fatalf("the unheld occupant's reservation must stand, naming the occupant: installed %v %+v incomplete %v", installed, p, incomplete)
				}
				return
			}
			if installed || !refused || p != nil || incomplete || testMappedTo(e, pi) {
				t.Fatalf("a moved fence refuses with local cleanup: installed %v refused %v entry %+v incomplete %v mapped %v",
					installed, refused, p, incomplete, testMappedTo(e, pi))
			}
		})
	}
}

// T18 — 7b's recovery gate still refuses a recovered ordinary record's install when custody no
// longer holds its lineage, whatever kind the entry would have (a marker-opaque or past-PUBREC Hold).
//
// Mutation M18.
func TestPendingClosureRecoveryGateCoversHolds(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "pending-t18")
	testReadyForRealPubRelDelivery(t, e)
	lin := mqttNewInflightNonce()
	rec := &mqttInflightRecord{PI: 18, Nonce: mqttNewInflightNonce(), Lin: lin, Dur: testRealPubRelSrcDur, State: mqttInflightPubRelReady}
	for _, held := range []bool{true, false} {
		e.sess.mu.Lock()
		if held {
			testCustodyEnsureExchange(e.sess.custody, e.sess.gen, lin, 18)
		} else {
			testReleaseLineageLocked(e.sess, lin)
		}
		hold := &mqttPending{regRecoveryPlaceholder: true, ev: mqttEvidenceFromRecord(e.sess.gen, rec)}
		refused := e.sess.recoveryInstallRefusedLocked(rec, hold)
		e.sess.mu.Unlock()
		if refused == held {
			t.Fatalf("recovery gate refused=%v with custody held=%v for a Hold", refused, held)
		}
	}
}

// T19 — the landed-route marker-opaque conversion refuses a lineage released between the unlocked
// marker read and the locked conversion, with local cleanup and no incomplete mark.
//
// Mutation M19.
func TestPendingClosureLandedReservationNeedsCustody(t *testing.T) {
	// Not parallel: it drives a process-wide test seam, which any concurrent test would reach.
	for _, release := range []bool{false, true} {
		release := release
		t.Run(map[bool]string{false: "control", true: "released before conversion"}[release], func(t *testing.T) {
			e := testNewInflightEnv(t, "pending-t19")
			testReadyForRealPubRelDelivery(t, e)
			pi := testAllocation(t, e, 77)
			nonce, lin := e.nonceOf(pi), mqttNewInflightNonce()
			e.sess.mu.Lock()
			gen := e.sess.gen
			testCustodyEnsureExchange(e.sess.custody, gen, lin, pi)
			subject := mqttInflightSubject(e.sess.idHash, gen, pi)
			e.sess.mu.Unlock()
			restore := mqttSetTestSeam(&mqttTestBeforeMarkerOpaqueConversionV, func() {
				if release {
					e.sess.mu.Lock()
					testReleaseLineageLocked(e.sess, lin)
					e.sess.mu.Unlock()
				}
			})
			defer restore()
			installed, refused := e.sess.installMarkerOpaqueReservation(e.c, pi, testRealPubRelSrcDur, 77, nonce,
				mqttPendingEvidence{gen: gen, lin: lin, nonce: nonce}, 2, 5, subject, "test", false)
			restore()
			e.sess.mu.Lock()
			p, incomplete := e.sess.pendingPublish[pi], e.sess.piSpaceIncomplete
			e.sess.mu.Unlock()
			if !release {
				if !installed || p == nil || !p.markerOpaque || !incomplete {
					t.Fatalf("the control installs the reservation: installed %v %+v incomplete %v", installed, p, incomplete)
				}
				return
			}
			if installed || !refused || p != nil || incomplete || testMappedTo(e, pi) {
				t.Fatalf("a released lineage reserves nothing: installed %v refused %v %+v incomplete %v", installed, refused, p, incomplete)
			}
		})
	}
}

// T20 — an affine allocation takes HEAD's marker-opaque reservation on either route with no
// exchange custody anywhere.
//
// Mutation M20.
func TestPendingClosureAffineReservationNeedsNoCustody(t *testing.T) {
	t.Parallel()
	for _, occupant := range []bool{false, true} {
		occupant := occupant
		t.Run(map[bool]string{false: "landed", true: "occupant"}[occupant], func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "pending-t20")
			testReadyForRealPubRelDelivery(t, e)
			dur := mqttQ2AffineConsumerDurablePrefix + "test"
			e.sess.mu.Lock()
			pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 78, 1))
			gen := e.sess.gen
			subject := mqttInflightSubject(e.sess.idHash, gen, pi)
			e.sess.mu.Unlock()
			if pi == 0 || !isNew {
				t.Fatal("fixture: no affine allocation")
			}
			installed, _ := e.sess.installMarkerOpaqueReservation(e.c, pi, dur, 78, e.nonceOf(pi),
				mqttPendingEvidence{gen: gen, lin: mqttNewInflightNonce(), nonce: e.nonceOf(pi)}, 2, 5, subject, "test", occupant)
			e.sess.mu.Lock()
			p, incomplete := e.sess.pendingPublish[pi], e.sess.piSpaceIncomplete
			e.sess.mu.Unlock()
			if !installed || p == nil || !p.markerOpaque || !incomplete {
				t.Fatalf("the affine reservation stands without custody: installed %v %+v incomplete %v", installed, p, incomplete)
			}
		})
	}
}

// T21 — the incomplete mark is committed with the reservation, so it cannot land after a resume
// that already read the pair whole and cleared it.
//
// Mutation M23 (the mark after the unlock).
func TestPendingClosureIncompleteMarkCannotOutliveItsResume(t *testing.T) {
	// Not parallel: it drives a process-wide test seam, which any concurrent test would reach.
	e := testNewInflightEnv(t, "pending-t21")
	testReadyForRealPubRelDelivery(t, e)
	pi := testAllocation(t, e, 79)
	nonce := e.nonceOf(pi)
	e.sess.mu.Lock()
	gen := e.sess.gen
	subject := mqttInflightSubject(e.sess.idHash, gen, pi)
	e.sess.mu.Unlock()
	ran := false
	restore := mqttSetTestSeam(&mqttTestAfterMarkerOpaqueConversionV, func() {
		ran = true
		e.sess.mu.Lock()
		e.sess.piSpaceIncomplete = false // a resume completed the scan during the pause
		e.sess.mu.Unlock()
	})
	defer restore()
	installed, _ := e.sess.installMarkerOpaqueReservation(e.c, pi, testRealPubRelSrcDur, 79, nonce,
		mqttPendingEvidence{gen: gen, lin: mqttNewInflightNonce(), nonce: mqttNewInflightNonce()}, 2, 5, subject, "test", true)
	restore()
	if !installed || !ran {
		t.Fatalf("fixture: the conversion must succeed and pause (installed %v, seam %v)", installed, ran)
	}
	e.sess.mu.Lock()
	incomplete := e.sess.piSpaceIncomplete
	e.sess.mu.Unlock()
	if incomplete {
		t.Fatal("the incomplete mark landed after the resume that cleared it")
	}
}

// T22 — a released delivery ends with its owner. While the connection that transmitted the PUBREL
// is the session's, the Hold stays for its PUBCOMP; once another connection is bound it is
// retired: removed, or — when the completion worker still owns the slot's cleanup — handed to the
// worker's terminal reservation with no ACK subject.
func TestPendingClosureReleasedDeliveryEndsWithItsOwner(t *testing.T) {
	t.Parallel()
	for _, workerOwns := range []bool{false, true} {
		workerOwns := workerOwns
		t.Run(map[bool]string{false: "no cleanup owner", true: "worker owns the cleanup"}[workerOwns], func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "pending-t22")
			testReadyForRealPubRelDelivery(t, e)
			seed := testSeedRealPubRelReady(t, e)
			testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
			testActionDrained(t, e, seed.pi)
			e.sess.mu.Lock()
			rel := e.sess.pendingPubRel[seed.pi]
			if !e.sess.pubCompHeldLocked(e.sess.sendsInFlight[seed.pi]) {
				e.sess.mu.Unlock()
				t.Fatal("fixture: this connection must owe the PUBCOMP")
			}
			testReleaseLineageLocked(e.sess, seed.lin)
			released := e.sess.pendingPubRel[seed.pi]
			if workerOwns {
				if e.sess.pubCompOwed == nil {
					e.sess.pubCompOwed = make(map[mqttPubCompKey]*mqttPubCompOwed)
				}
				e.sess.pubCompOwed[mqttPubCompKey{pi: seed.pi, nonce: rel.regNonce}] = &mqttPubCompOwed{ref: rel.slot}
			}
			e.sess.retireReleasedDeliveriesLocked()
			kept := e.sess.pendingPubRel[seed.pi]
			e.sess.c = &client{cid: e.c.cid + 1000} // another connection is bound
			e.sess.retireReleasedDeliveriesLocked()
			after := e.sess.pendingPubRel[seed.pi]
			e.sess.c = e.c
			e.sess.mu.Unlock()
			if released == nil || released.awaitingPubCompOwner == 0 || kept != released {
				t.Fatalf("while its owner is live the released delivery stays: released %+v kept %+v", released, kept)
			}
			if workerOwns {
				if after == nil || after.jsAckSubject != _EMPTY_ || after.awaitingPubCompOwner != 0 || after.ev != rel.ev {
					t.Fatalf("the worker's cleanup keeps the identifier through a terminal reservation: %+v", after)
				}
				return
			}
			if after != nil || testMappedTo(e, seed.pi) {
				t.Fatalf("with no cleanup owner the retired delivery and its mapping go: %+v", after)
			}
		})
	}
}

// T23 — PUBCOMP settles a released delivery exactly as it settles a delivered entry: the
// conversion to the terminal reservation, the latest ACK subject returned.
func TestPendingClosurePubCompSettlesAReleasedDelivery(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "pending-t23")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
	testActionDrained(t, e, seed.pi)
	e.sess.mu.Lock()
	testReleaseLineageLocked(e.sess, seed.lin)
	rel := e.sess.pendingPubRel[seed.pi]
	subj, reserved := e.sess.pubCompConvertLocked(seed.pi, rel, rel.slot)
	after := e.sess.pendingPubRel[seed.pi]
	e.sess.mu.Unlock()
	if rel.awaitingPubCompOwner == 0 || !reserved || subj != seed.reply(1) || after == nil || after.jsAckSubject != _EMPTY_ {
		t.Fatalf("PUBCOMP must convert the released delivery: released %+v subj %q reserved %v after %+v", rel, subj, reserved, after)
	}
}

// T24 — the admission-custody clause (consult 97, G1): an allocation's own unresolved admission
// licenses its landing's projection, but not after an accepted ending of the lineage its attempt
// carries, and never for another nonce or generation.
func TestPendingClosureAdmissionCustodyEndsWithItsLineage(t *testing.T) {
	t.Parallel()
	const gen, nonce, lin = "g1", "alloc-1", "lin-1"
	for _, tc := range []struct {
		name  string
		ended bool
		ev    mqttPendingEvidence
		want  bool
	}{
		{"unresolved admission", false, mqttPendingEvidence{gen: gen, lin: lin, nonce: nonce}, true},
		{"after an accepted ending", true, mqttPendingEvidence{gen: gen, lin: lin, nonce: nonce}, false},
		{"another nonce", false, mqttPendingEvidence{gen: gen, lin: lin, nonce: "alloc-2"}, false},
		{"another generation", false, mqttPendingEvidence{gen: "g0", lin: lin, nonce: nonce}, false},
	} {
		sess := &mqttSession{gen: gen, custody: newMQTTCustodyLedger(gen)}
		sess.custody.openAdmission(gen, nonce, 3)
		sess.custody.recordAttempt(gen, nonce, mqttCustodyAttempt{handle: &mqttTransitionHandle{}, lin: lin})
		if tc.ended {
			sess.custody.adoptReadExchange(&mqttOutboundExchange{Gen: gen, Nonce: lin, PI: 3, Phase: mqttExchangeTerminal})
		}
		if got := sess.ordinaryHeldLocked(tc.ev); got != tc.want {
			t.Errorf("%s: ordinaryHeldLocked=%v, want %v", tc.name, got, tc.want)
		}
	}
}

// T25 — an acquisition refreshes the PHASE of this exchange's own resident PUBREL entry, and of no
// other: a resident whose slot names another subject or nonce is untouched.
func TestPendingClosureAcquisitionRefreshesOnlyItsOwnPhase(t *testing.T) {
	t.Parallel()
	idHash, gen := getHash("pending-t25"), mqttNewInflightNonce()
	parsed, err := mqttParseInflightSubject(mqttInflightSubject(idHash, gen, 5))
	if err != nil {
		t.Fatal(err)
	}
	name := *parsed
	own := mqttInflightSlotRef{name: name, nonce: mqttNewInflightNonce(), seq: 3, acq: 1, state: mqttInflightPubRelReady}
	acquired := own
	acquired.seq, acquired.acq, acquired.actionLive, acquired.actionToken = 9, 2, true, "tok"
	for _, tc := range []struct {
		name   string
		slot   mqttInflightSlotRef
		change bool
	}{
		{"own slot", own, true},
		{"another nonce", func() mqttInflightSlotRef { s := own; s.nonce = mqttNewInflightNonce(); return s }(), false},
		{"another subject", func() mqttInflightSlotRef {
			s := own
			s.name.subject = mqttInflightSubject(idHash, gen, 6)
			return s
		}(), false},
	} {
		sess := &mqttSession{pendingPubRel: map[uint16]*mqttPending{5: {kind: mqttPendingOrdinary, regNonce: tc.slot.nonce, slot: tc.slot}}}
		sess.advanceResidentPubRelPhaseLocked(5, acquired)
		got := sess.pendingPubRel[5]
		if refreshed := got.slot.acq == 2 && got.slot.seq == 9 && got.regSeq == 9; refreshed != tc.change {
			t.Errorf("%s: refreshed=%v, want %v (%+v)", tc.name, refreshed, tc.change, got.slot)
		}
		if got.kind != mqttPendingOrdinary || got.slot.nonce != tc.slot.nonce || got.slot.name != tc.slot.name {
			t.Errorf("%s: the refresh rewrote identity: %+v", tc.name, got)
		}
	}
}

// T26 — a projection the admission-custody clause licensed goes when its lineage ends, though no
// exchange holder exists: the ending of an admission-backed lineage is reported released like a
// holder's, through each observation route that adopts an exchange read (holistic round 1).
func TestPendingClosureAdmissionBackedProjectionGoesWithItsEnding(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"census", "uncertain re-read"} {
		route := route
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			const nonce, lin = "alloc-26", "lin-26"
			const pi = uint16(26)
			sess := &mqttSession{gen: "g26", pendingPublish: map[uint16]*mqttPending{},
				cpending: map[string]map[uint64]uint16{"SRC": {41: pi}}}
			sess.custody = newMQTTCustodyLedger(sess.gen)
			sess.custody.openAdmission(sess.gen, nonce, pi)
			sess.custody.recordAttempt(sess.gen, nonce, mqttCustodyAttempt{handle: &mqttTransitionHandle{}, lin: lin})
			entry := &mqttPending{kind: mqttPendingOrdinary, jsDur: "SRC", sseq: 41, regNonce: nonce,
				ev: mqttPendingEvidence{gen: sess.gen, lin: lin, nonce: nonce}}
			if !sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectEmpty(), entry) {
				t.Fatal("fixture: the admission-backed landing must install")
			}
			ended := &mqttOutboundExchange{Gen: sess.gen, Nonce: lin, PI: pi, Phase: mqttExchangeTerminal}
			sess.acceptExchangeObservationLocked(sess.gen, []*mqttOutboundExchange{ended}, func() bool {
				if route == "census" {
					sess.custody.mergeCensus(sess.gen, []*mqttOutboundExchange{ended}, nil, true)
				} else {
					subj := mqttExchangeSubject("idh", sess.gen, lin)
					sess.custody.uncertainSubjects[subj] = sess.custody.nextTick()
					sess.custody.resolveUncertain(sess.gen, subj, ended)
				}
				return true
			})
			if p := sess.pendingPublish[pi]; p != nil {
				t.Fatalf("the ended admission-backed projection must go: %+v", p)
			}
			if sess.cpending["SRC"][41] == pi {
				t.Fatal("its mapping must go with it")
			}
		})
	}
}
