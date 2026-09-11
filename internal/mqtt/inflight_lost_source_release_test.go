// F-102: a recovery reservation over a Lost source has no release edge.
//
// Recovery consumed a PUBLISH-phase pair only when its source classified Complete. A pair
// whose source classified LOST — the consumer authoritatively gone, which on an
// interest-retained stream took its messages with it — was restored instead, with its
// marker, its source mapping, and NO ack subject. The resume reap only ever scans entries
// that have one, no consumer remained to offer the source, and the client had never
// received the PUBLISH, so nothing could ever release the identifier or its Receive
// Maximum slot.
//
// The fix routes on "the source can never be re-offered" — Complete OR Lost — and lets the
// MARKER decide the outcome, which is what these tests pin. The two halves are separate
// claims and are tested separately: what is released, and what is emphatically not.
package mqtt

import (
	"errors"
	"testing"

	"github.com/nats-io/nuid"
)

// lostSourceHook forces both source classifiers (mqttInflightSourceCache.resolve
// short-circuits through the same hook) to one answer, so the matrix below can reach
// states a real stream cannot be driven into reliably.
func lostSourceHook(e *testInflightEnv, st mqttInflightSourceState) {
	e.asm.mu.Lock()
	e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return st }
	e.asm.mu.Unlock()
}

// TestMQTTInflightRecoveryConsumesAProvenUnsentRegisterOverALostSource is the matrix, and
// it is deliberately whole rather than a single Lost row.
//
// Three properties have to hold together, and a test of one of them alone passes for the
// wrong reason. The LOST/present row is the finding. The LOST/absent row is the safety
// half — a possibly-attempted exchange may be in the client's hands and is never released
// on the source alone, which is the inference F-100 and F-101 both rejected. And the
// UNKNOWN and LIVE rows pin the predicate to mqttInflightSourceMayReoffer rather than to
// "anything that is not Complete": an unreadable lookup concludes nothing, and a live
// source is a delivery that will come round again.
//
// The cpending column is not decoration either. A Complete source drops its mapping and a
// Lost one KEEPS it, and those two answer different questions — see the reserveSource
// install in recoverInflightRegistersOnce.
func TestMQTTInflightRecoveryConsumesAProvenUnsentRegisterOverALostSource(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		src  mqttInflightSourceState
		// transmitted removes the marker, which is what makes the exchange
		// possibly-attempted rather than proven-unsent.
		transmitted bool
		wantDropped bool
		wantMapping bool
	}{
		{"complete, marker present", mqttInflightSourceComplete, false, true, false},
		{"complete, marker absent", mqttInflightSourceComplete, true, false, false},
		{"lost, marker present", mqttInflightSourceLost, false, true, false},
		{"lost, marker absent", mqttInflightSourceLost, true, false, true},
		{"unknown, marker present", mqttInflightSourceUnknown, false, false, true},
		{"unknown, marker absent", mqttInflightSourceUnknown, true, false, true},
		{"live, marker present", mqttInflightSourceLive, false, false, true},
		{"live, marker absent", mqttInflightSourceLive, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "client-f102-"+nuid.Next())
			dur := e.sess.idHash + "_" + nuid.Next()

			// An ordinary QoS 1 SINGLETON: not shared, not affine, not cancelled, not
			// retired, no drain. Every one of those is diverted before the routing site,
			// so a fixture carrying one would never reach the arm under test.
			sseq := e.publishSource(t, dur)
			pi := e.track(t, dur, sseq, 1)
			if tc.transmitted {
				e.transmit(t, pi, dur, sseq, true)
				e.waitMarkerAbsent(t, pi)
			}
			if _, mk := e.loadMarker(t, pi); (mk != nil) == tc.transmitted {
				t.Fatalf("fixture: marker present=%v, want %v", mk != nil, !tc.transmitted)
			}

			lostSourceHook(e, tc.src)
			e.forgetInMemory()
			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}

			e.sess.mu.Lock()
			_, entry := e.sess.pendingPublish[pi]
			_, mapped := e.sess.cpending[dur][sseq]
			incomplete := e.sess.piSpaceIncomplete
			e.sess.mu.Unlock()

			// Not decoration: a walk that failed early also installs nothing, so without
			// this the dropped rows would pass for the wrong reason.
			if incomplete {
				t.Fatal("fixture: this walk withheld the identifier space, so it decided " +
					"nothing about the register under test")
			}

			gone := !e.registerPresent(t, e.regSubject(pi))
			if gone != tc.wantDropped {
				t.Fatalf("register deleted=%v, want %v", gone, tc.wantDropped)
			}
			if entry == tc.wantDropped {
				t.Fatalf("pendingPublish[%d] present=%v, want %v", pi, entry, !tc.wantDropped)
			}
			if mapped != tc.wantMapping {
				t.Fatalf("cpending[%s][%d] present=%v, want %v", dur, sseq, mapped, tc.wantMapping)
			}
		})
	}
}

// TestMQTTInflightDrainOwnedRegisterIsNeverConsumedByTheSourceArm is the regression for
// the defect this item nearly shipped, and it is the reason the predicate reads a ROUTED
// flag rather than the source cache.
//
// A drain-owned record appends to `live` and continues BEFORE the routing site, so it
// arrives at the reserve loop as an ordinary singleton with nothing routed. That falseness
// is the entire exclusion — there is no test for drain ownership in the consume arm at
// all. An earlier revision of this change consulted srcCache.state() there instead, which
// preserved every row of the matrix above and silently made drain-owned records
// droppable, for COMPLETE sources as well as Lost ones.
//
// So both rows are here. The Complete row is not redundant: it is the half that shows the
// regression was never specific to F-102's own classification.
func TestMQTTInflightDrainOwnedRegisterIsNeverConsumedByTheSourceArm(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		src  mqttInflightSourceState
	}{
		{"complete source", mqttInflightSourceComplete},
		{"lost source", mqttInflightSourceLost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "client-f102-drain-"+nuid.Next())
			dur := e.sess.idHash + "_" + nuid.Next()

			// Proven-unsent — marker present — which is exactly the shape the source arm
			// would otherwise consume.
			sseq := e.publishSource(t, dur)
			pi := e.track(t, dur, sseq, 1)

			// The drain OWNS this delivery: claimed, and addressing this same identifier.
			// recomputeDrainRegisterStateLocked requires both, and rejects neither a
			// Complete nor a Lost source, so ownership is what has to keep the register.
			addTestDrain(e.sess, "sid-f102", dur, "foo.bar", 1,
				map[uint64]*drainSeq{sseq: {claimed: true, pi: pi}})

			lostSourceHook(e, tc.src)
			e.sess.mu.Lock()
			e.sess.pendingPublish = nil
			e.sess.pendingPubRel = nil
			e.sess.cpending = nil
			e.sess.last_pi = 0
			e.sess.mu.Unlock()

			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}

			e.sess.mu.Lock()
			claimed := e.sess.drainByDur[dur] != nil && e.sess.drainByDur[dur].seqs[sseq] != nil &&
				e.sess.drainByDur[dur].seqs[sseq].claimed
			e.sess.mu.Unlock()
			if !claimed {
				t.Fatal("fixture: the drain must still own this delivery, or the exclusion " +
					"under test was never engaged")
			}
			if !e.registerPresent(t, e.regSubject(pi)) {
				t.Fatalf("a DRAIN-OWNED register was consumed by the source arm over a %v "+
					"source: the drain owns the record and recovery may neither rewrite nor "+
					"delete it", tc.src)
			}
		})
	}
}

// TestMQTTInflightLostSourceConsumeReleasesTheReceiveMaximumSlot states the release as a
// NUMBER rather than as the absence of an entry.
//
// The durable delete alone frees nothing the client is spending: qos12InflightForCap
// counts len(pendingPublish), so the capacity only comes back when the apply phase retires
// the live entry the consumed register backed. A fixture with empty memory would pass
// whether or not that retirement happened, which is why a matching same-object entry is
// seeded first.
//
// Quiescent deliberately: an entry whose transmission has not returned is CONDEMNED rather
// than deleted and keeps its charge until endSendWith settles it, so an open send section
// here would be measuring a different rule.
func TestMQTTInflightLostSourceConsumeReleasesTheReceiveMaximumSlot(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-f102-cap")
	dur := e.sess.idHash + "_" + nuid.Next()

	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)

	e.sess.mu.Lock()
	e.sess.rmax = 1
	before := e.sess.qos12InflightForCap()
	e.sess.mu.Unlock()
	if before != 1 {
		t.Fatalf("fixture: the exchange must occupy the only slot, got %d", before)
	}

	// The same-object entry and mapping the apply phase has to retire. Left in place
	// rather than forgotten, which is what distinguishes this from the matrix above.
	lostSourceHook(e, mqttInflightSourceLost)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	e.sess.mu.Lock()
	after := e.sess.qos12InflightForCap()
	_, entry := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()

	if entry {
		t.Fatalf("the live entry for packet identifier %d survived its register's "+
			"consumption, so it counts against Receive Maximum forever", pi)
	}
	if after != 0 {
		t.Fatalf("qos12InflightForCap = %d, want 0: a proven-unsent exchange over a source "+
			"nothing can re-offer holds neither the identifier nor the slot", after)
	}
	if e.registerPresent(t, e.regSubject(pi)) {
		t.Fatal("the durable register survived the consume arm")
	}
}

// TestMQTTInflightLostSourcePossiblyAttemptedKeepsItsSourceMapping is the asymmetry in
// §3.3 as behaviour rather than as a map assertion.
//
// A Lost classification is one completed consumer lookup. It does not retract a payload
// already dispatched to a callback, and if that callback reaches trackPublish with no
// mapping it takes the new-delivery branch and mints a SECOND identifier for one source —
// the client then holds one message under two identifiers. Keeping the mapping is what
// makes it take the redelivery branch instead.
//
// Receive Maximum 3 deliberately: at 1 the second allocation is refused by capacity and
// the oracle would pass without the mapping doing anything.
func TestMQTTInflightLostSourcePossiblyAttemptedKeepsItsSourceMapping(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-f102-mapping")
	dur := e.sess.idHash + "_" + nuid.Next()

	// Possibly-attempted: transmitted, so the marker is gone.
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	e.transmit(t, pi, dur, sseq, true)
	e.waitMarkerAbsent(t, pi)

	lostSourceHook(e, mqttInflightSourceLost)
	e.forgetInMemory()
	e.sess.mu.Lock()
	e.sess.rmax = 3
	e.sess.mu.Unlock()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	e.sess.mu.Lock()
	_, mapped := e.sess.cpending[dur][sseq]
	e.sess.mu.Unlock()
	if !mapped {
		t.Fatal("fixture: a possibly-attempted entry over a Lost source keeps its source " +
			"mapping; without it the drive below proves nothing")
	}

	// The dispatched callback's own call, at the same (durable, sequence).
	e.sess.mu.Lock()
	got, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	e.sess.mu.Unlock()

	if isNew || got != pi {
		t.Fatalf("trackPublish(%s/%d) = pi %d, isNew %v; want the REDELIVERY branch on the "+
			"restored identifier %d. A second identifier for one source is the client "+
			"holding one message twice", dur, sseq, got, isNew, pi)
	}
}

// TestMQTTInflightMarkerProvenDropDoesNotInitiateMarkerCleanup pins the DISPOSITION, and
// it exists because the obvious oracles for it do not discriminate.
//
// The two dispositions share a phase gate — both admit `publish` and nothing else — so
// asserting on the gate proves nothing. Their final states coincide too: on a
// same-exchange rewrite both preserve the register, and mqttSlotDeleteComplete's marker
// reap then keeps the marker anyway, because it finds the matching sibling. And regDrop is
// function-local, so no test can read the argument the production call passed.
//
// What separates them is whether marker cleanup is INITIATED at all.
// reapRegisterWithVerdictGated returns false for mqttSlotDeleteNeverTransmitted when the
// record has moved under the classify snapshot, and that false is what gates the
// reapMarkerCaptureFirst call. So the discriminating observation is the marker LOAD: it
// must never happen. Recovery's own marker phase reads through loadInflightRecordsMulti,
// not this hook, so a load arriving here came from the cleanup and nothing else.
//
// The rule being pinned is narrow, and deliberately so: a same-exchange, legal-phase
// register rewritten since the snapshot must not trigger marker cleanup through this drop
// continuation. An ABSENT or FOREIGN register still returns true under either disposition
// and still initiates cleanup, which is what the orphan-marker rules want.
func TestMQTTInflightMarkerProvenDropDoesNotInitiateMarkerCleanup(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-f102-disp")
	dur := e.sess.idHash + "_" + nuid.Next()

	// Proven-unsent over a Lost source: the marker-proven drop.
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	subject := e.regSubject(pi)

	var markerLoads int
	markerSubject := subject + "." + mqttInflightMarkerSuffix
	e.asm.mu.Lock()
	e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceLost
	}
	// THE REWRITE, staged for the gated delete's own read. The record keeps its nonce and
	// its phase — so identity and the phase gate both pass — and moves to a sequence the
	// classify snapshot never saw, which is exactly the freshness fence's question.
	jsa := &e.asm.jsa
	e.asm.testInflightLoad = func(subj string) (*mqttInflightRecord, *StoredMsg, error) {
		cur, smsg, err := jsa.loadInflightRecord(subj)
		if err == nil && smsg != nil && subj == subject {
			moved := *smsg
			moved.Sequence = smsg.Sequence + 1000
			return cur, &moved, nil
		}
		return cur, smsg, err
	}
	// The discriminating observation. Its RETURN is immaterial — the oracle is the count —
	// and reapMarkerCaptureFirst is the only thing in this fixture that can reach it.
	e.asm.testInflightMarkerLoad = func(subj string) (*StoredMsg, error) {
		if subj == markerSubject {
			markerLoads++
		}
		return nil, nil
	}
	e.asm.mu.Unlock()

	e.forgetInMemory()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	if !e.registerPresent(t, subject) {
		t.Fatal("fixture: a same-exchange rewrite must PRESERVE the register, or the " +
			"freshness fence was never reached and this test measures nothing")
	}
	if markerLoads != 0 {
		t.Fatalf("the drop was declined on a same-exchange rewrite but still initiated "+
			"marker cleanup (%d marker load(s)): a register this pass refused to delete "+
			"must keep its marker beside it", markerLoads)
	}
}

// TestMQTTInflightLostSourceDropSurvivesTheClaimContest covers the two rebuild sites that
// can carry the routed verdict away.
//
// A plain QoS 2 PUBLISH-phase register on a non-shared durable is a CUSTODY candidate, so
// it goes through claimRecoveredTransferableInflight and comes back as a REBUILT
// mqttRecoveredInflightReg. The routing verdict was decided before that contest, so a
// rebuild that forgets to carry it turns a droppable pair back into a reservation — and
// nothing else in the suite would notice, because every other F-102 fixture is QoS 1 and
// never enters the contest at all.
//
// Two arms, because the rebuilds are not interchangeable. The successful CAS and the
// moved-and-ours reread each produce an eligible PUBLISH register that returns to the
// marker-proven arm. The THIRD rebuild — the action/terminal reread — yields
// mqttInflightClaimWithheld and is routed to pastPubRec, which never comes back here, so
// no F-102 oracle covers it and the plan says so rather than pretending otherwise.
func TestMQTTInflightLostSourceDropSurvivesTheClaimContest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// faultFirstStore makes the custody CAS LAND and then report failure, which is the
		// lost-response shape: the reread finds this session's own claim at a moved
		// sequence and adopts the body. That is the second rebuild site.
		faultFirstStore bool
	}{
		{name: "custody CAS succeeds"},
		{name: "moved and ours", faultFirstStore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "client-f102-contest-"+nuid.Next())
			dur := e.sess.idHash + "_" + nuid.Next()

			// QoS 2, non-shared, publish, not retired, not cancelled: a custody candidate.
			// Proven-unsent over a Lost source, so the drop is what must survive.
			sseq := e.publishSource(t, dur)
			pi := e.track(t, dur, sseq, 2)
			if _, mk := e.loadMarker(t, pi); mk == nil {
				t.Fatal("fixture: the marker must be present, or this is not the drop arm")
			}

			e.asm.mu.Lock()
			e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
				return mqttInflightSourceLost
			}
			if tc.faultFirstStore {
				jsa := &e.asm.jsa
				faulted := false
				e.asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
					seq, err := jsa.storeInflightCAS(subject, expectSeq, r)
					if !faulted && err == nil {
						// LANDED, then reported lost. Faulted once only: the contest's
						// reread has to meet the write, not a second fault.
						faulted = true
						return 0, errors.New("test: the custody CAS response was lost")
					}
					return seq, err
				}
			}
			e.asm.mu.Unlock()

			e.forgetInMemory()
			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}

			e.sess.mu.Lock()
			_, entry := e.sess.pendingPublish[pi]
			incomplete := e.sess.piSpaceIncomplete
			e.sess.mu.Unlock()
			if incomplete {
				t.Fatal("fixture: the identifier space was withheld, so nothing was decided " +
					"about the register under test")
			}
			if entry {
				t.Fatalf("packet identifier %d was reserved after the claim contest: the "+
					"routed source verdict did not survive the rebuild, so a proven-unsent "+
					"pair over a source nothing can re-offer was restored instead of consumed", pi)
			}
			if e.registerPresent(t, e.regSubject(pi)) {
				t.Fatal("the durable register survived the drop")
			}
		})
	}
}
