// Tests for the fourth route that lifts an identifier exclusion.
//
// The property under test is ONE-SIDED and that asymmetry is the whole point: a MISSED lift
// costs an identifier, which is an availability cost, while a FALSE lift hands an identifier
// out while an exchange still owns it, which is a lost or duplicated message. So the
// negative fixtures outnumber the positive one, and five of the mutations below exist only
// to make a false lift impossible.
//
// Every negative fixture must do four things, because each one has masked a defect before:
// establish that the exclusion is the ONLY thing blocking the identifier (bumpPI also
// consults pendingPublish, pendingPubRel and the send registry), force allocation to
// consider the target identifier, OBSERVE THAT THE PASS COMPLETED — an assertion made
// before the worker runs proves nothing — and assert the exclusion itself still stands.

package mqtt

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// exclusionReclaimRig is a session wired far enough to run a real reclamation pass: a
// resident session object, a server to admit the worker against, and a seamed subject
// loader standing in for JetStream.
type exclusionReclaimRig struct {
	t    *testing.T
	as   *mqttAccountSessionManager
	sess *mqttSession
	srv  *Server

	mu sync.Mutex
	// loads counts subject reads, which is how a fixture observes that a pass actually ran
	// rather than assuming it did.
	loads map[string]int
	load  func(subject string) (*mqttInflightRecord, *StoredMsg, error)
}

func newExclusionReclaimRig(t *testing.T) *exclusionReclaimRig {
	t.Helper()
	as := &mqttAccountSessionManager{
		sessions:   make(map[string]*mqttSession),
		sessByHash: make(map[string]*mqttSession),
	}
	srv := &Server{}
	sess := &mqttSession{
		asm:    as,
		jsa:    &mqttJSA{mqttJSACore: &mqttJSACore{c: &client{srv: srv}}},
		id:     "reclaim-client",
		idHash: "abcdefgh",
		gen:    "gggggggggggggggggggggg",
	}
	rig := &exclusionReclaimRig{t: t, as: as, sess: sess, srv: srv, loads: map[string]int{}}
	// Resident by construction: the pass refuses to run for an object that is not the live
	// session, so a rig that skipped this would test the residency exit and nothing else.
	as.mu.Lock()
	as.sessions[sess.id] = sess
	as.sessByHash[sess.idHash] = sess
	as.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		rig.mu.Lock()
		rig.loads[subject]++
		fn := rig.load
		rig.mu.Unlock()
		if fn == nil {
			return nil, nil, nil
		}
		return fn(subject)
	}
	as.mu.Unlock()
	return rig
}

func (r *exclusionReclaimRig) setLoad(fn func(string) (*mqttInflightRecord, *StoredMsg, error)) {
	r.mu.Lock()
	r.load = fn
	r.mu.Unlock()
}

func (r *exclusionReclaimRig) loadCount(subject string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.loads[subject]
}

// subject is this session's slot subject for pi, in the CURRENT generation.
func (r *exclusionReclaimRig) subject(pi uint16) string {
	return mqttInflightSubject(r.sess.idHash, r.sess.gen, pi)
}

// exclude installs an exclusion through the production install function, which is also what
// arms the continuation.
func (r *exclusionReclaimRig) exclude(pi uint16, subject, nonce string) {
	r.sess.mu.Lock()
	r.sess.excludeIdentifierLocked(pi, subject, nonce)
	r.sess.mu.Unlock()
}

func (r *exclusionReclaimRig) armed() bool {
	r.sess.mu.Lock()
	defer r.sess.mu.Unlock()
	return r.sess.exclReclaimArmed
}

func (r *exclusionReclaimRig) excluded(pi uint16) bool {
	r.sess.mu.Lock()
	defer r.sess.mu.Unlock()
	return slotExclusionHeldLocked(r.sess.sendsInFlight[pi])
}

// allocatable reports whether bumpPI will hand pi out. THE ONLY BLOCKER CHECK lives here:
// the registry, not the pending maps, is what the fixtures manipulate, so a fixture that
// asserted on bumpPI alone could pass because some other term happened to reserve the
// identifier.
func (r *exclusionReclaimRig) allocatable(pi uint16) bool {
	r.sess.mu.Lock()
	defer r.sess.mu.Unlock()
	if _, ok := r.sess.pendingPublish[pi]; ok {
		r.t.Fatalf("fixture error: packet identifier %d is blocked by a pendingPublish entry, so this test would pass for the wrong reason", pi)
	}
	if _, ok := r.sess.pendingPubRel[pi]; ok {
		r.t.Fatalf("fixture error: packet identifier %d is blocked by a pendingPubRel entry, so this test would pass for the wrong reason", pi)
	}
	// Force the allocator to consider exactly this identifier next.
	r.sess.last_pi = pi - 1
	return r.sess.bumpPI() == pi
}

// runPass drives one real pass and waits for it, so every assertion below is made AFTER the
// worker has run. The pass is invoked directly rather than waited on through its timer:
// what the timer proves is the ARMING, which fixture 10 tests separately.
func (r *exclusionReclaimRig) runPass() {
	r.sess.runExclusionReclaim(mqttExclusionReclaimDelay)
}

// awaitArmedCallback waits for the armed continuation to fire at least once, which is how a
// fixture distinguishes "a flag was set" from "the worker ran".
func (r *exclusionReclaimRig) awaitLoad(subject string, atLeast int, why string) {
	r.t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if r.loadCount(subject) >= atLeast {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.t.Fatalf("the reclamation pass never read %q %d time(s): %s", subject, atLeast, why)
}

func absentLoad(string) (*mqttInflightRecord, *StoredMsg, error) { return nil, nil, nil }

func presentRecord(pi uint16, nonce string) *mqttInflightRecord {
	return freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, Dur: "$MQTT_abc", Seq: 41, QoS: 2, Nonce: nonce,
	})
}

// 1. THE POSITIVE CASE: a confirmed absence lifts, and the identifier comes back.
//
// This is the class CONFORMANCE calls "a landed delete whose confirmation failed" — the
// register is gone and only the confirming read was lost — and before this pass nothing
// re-derived the proof, because recovery reconciles through records it OBSERVES and a
// vanished register contributes no row.
func TestMQTTExclusionReclaimLiftsOnConfirmedAbsence(t *testing.T) {
	t.Parallel()
	rig := newExclusionReclaimRig(t)
	const pi = uint16(7)
	nonce := mqttNewInflightNonce()
	subject := rig.subject(pi)

	// The confirming read fails, exactly as the stranding class does.
	rig.setLoad(func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, errors.New("test: the confirmation was lost")
	})
	rig.exclude(pi, subject, nonce)
	if rig.allocatable(pi) {
		t.Fatal("the identifier was allocatable while its exclusion stood")
	}
	rig.runPass()
	if !rig.excluded(pi) {
		t.Fatal("a failed confirming read lifted the exclusion; only a proven absence may")
	}

	// The register really is gone, and the next pass proves it.
	rig.setLoad(absentLoad)
	rig.runPass()
	if rig.excluded(pi) {
		t.Fatal("a confirmed absence did not lift the exclusion")
	}
	if !rig.allocatable(pi) {
		t.Fatal("the identifier was not returned to the allocator after its exclusion lifted")
	}
}

// 2-5. THE FOUR NON-ABSENCE ANSWERS, each of which must HOLD.
//
// Fixtures 4 and 5 are separate on purpose. A load error and a present-but-unreadable record
// are different tuples from the loader, and an implementation that collapsed them would pass
// a test that only exercised one. The mutation this pairs with survived an entire package
// once, because nothing distinguished "the read failed" from "the subject is provably
// empty".
func TestMQTTExclusionReclaimHoldsOnEveryNonAbsence(t *testing.T) {
	t.Parallel()
	nonce := mqttNewInflightNonce()
	for _, tc := range []struct {
		name string
		load func(uint16, string) func(string) (*mqttInflightRecord, *StoredMsg, error)
		why  string
	}{
		{
			name: "our own record still stands",
			load: func(pi uint16, sub string) func(string) (*mqttInflightRecord, *StoredMsg, error) {
				return func(string) (*mqttInflightRecord, *StoredMsg, error) {
					return presentRecord(pi, nonce), &StoredMsg{Sequence: 6, Subject: sub}, nil
				}
			},
			why: "the exchange is still there, so no absence is provable",
		},
		{
			name: "a foreign successor holds the subject",
			load: func(pi uint16, sub string) func(string) (*mqttInflightRecord, *StoredMsg, error) {
				return func(string) (*mqttInflightRecord, *StoredMsg, error) {
					return presentRecord(pi, mqttNewInflightNonce()), &StoredMsg{Sequence: 9, Subject: sub}, nil
				}
			},
			why: "a successor occupying the subject is exactly the case the exclusion exists for",
		},
		{
			name: "the read failed",
			load: func(uint16, string) func(string) (*mqttInflightRecord, *StoredMsg, error) {
				return func(string) (*mqttInflightRecord, *StoredMsg, error) {
					return nil, nil, errors.New("test: indeterminate")
				}
			},
			why: "a read that failed establishes nothing; it is not an absence",
		},
		{
			name: "the record is present but unreadable",
			load: func(_ uint16, sub string) func(string) (*mqttInflightRecord, *StoredMsg, error) {
				return func(string) (*mqttInflightRecord, *StoredMsg, error) {
					return nil, &StoredMsg{Sequence: 5, Subject: sub}, nil
				}
			},
			why: "a record this broker cannot interpret is unsupported state, refused rather than read as empty",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := newExclusionReclaimRig(t)
			const pi = uint16(11)
			subject := rig.subject(pi)
			rig.setLoad(tc.load(pi, subject))
			rig.exclude(pi, subject, nonce)
			rig.runPass()
			if rig.loadCount(subject) == 0 {
				t.Fatal("the pass never read the subject, so this assertion proves nothing")
			}
			if !rig.excluded(pi) {
				t.Fatalf("the exclusion was lifted: %s", tc.why)
			}
			if rig.allocatable(pi) {
				t.Fatalf("the identifier was handed out: %s", tc.why)
			}
		})
	}
}

// 6. A CONJUNCTION IS NOT DISCHARGED BY ONE CONSTITUENT.
//
// Two DISTINCT SUBJECTS, not two nonces on one subject: the lift is keyed on subjects, so a
// same-subject pair would be satisfied by the single absence and the fixture would pass
// against the mutation.
func TestMQTTExclusionReclaimRequiresEveryBindAbsent(t *testing.T) {
	t.Parallel()
	rig := newExclusionReclaimRig(t)
	const pi = uint16(13)
	here := rig.subject(pi)
	// A bind retained from an older generation is a genuinely different subject.
	there := mqttInflightSubject(rig.sess.idHash, "oooooooooooooooooooooo", pi)
	rig.setLoad(func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if subject == there {
			return presentRecord(pi, mqttNewInflightNonce()), &StoredMsg{Sequence: 4, Subject: there}, nil
		}
		return nil, nil, nil // `here` is absent
	})
	rig.exclude(pi, here, mqttNewInflightNonce())
	rig.exclude(pi, there, mqttNewInflightNonce())
	rig.runPass()
	if rig.loadCount(here) == 0 || rig.loadCount(there) == 0 {
		t.Fatal("the pass did not read both bound subjects")
	}
	if !rig.excluded(pi) {
		t.Fatal("one constituent's absence discharged the conjunction")
	}
	if rig.allocatable(pi) {
		t.Fatal("the identifier was handed out while one bound subject was still occupied")
	}
}

// 7. THE BOUND SUBJECT, NOT THE CURRENT GENERATION'S.
//
// The subject carries the generation, so an absence in the current generation says nothing
// about an exclusion retained for an older one. The mutation this pairs with reads the
// current-generation subject and credits its answer to the bound one.
func TestMQTTExclusionReclaimConfirmsTheBoundSubjectNotTheCurrentGeneration(t *testing.T) {
	t.Parallel()
	rig := newExclusionReclaimRig(t)
	const pi = uint16(17)
	old := mqttInflightSubject(rig.sess.idHash, "oooooooooooooooooooooo", pi)
	current := rig.subject(pi)
	rig.setLoad(func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if subject == old {
			// STILL OCCUPIED: the older incarnation's exchange stands.
			return presentRecord(pi, mqttNewInflightNonce()), &StoredMsg{Sequence: 3, Subject: old}, nil
		}
		return nil, nil, nil // the current generation's subject is empty
	})
	rig.exclude(pi, old, mqttNewInflightNonce())
	rig.runPass()
	if rig.loadCount(old) == 0 {
		t.Fatal("the pass never read the BOUND subject")
	}
	if rig.loadCount(current) != 0 {
		t.Fatal("the pass read the current generation's subject, which this exclusion is not about")
	}
	if !rig.excluded(pi) {
		t.Fatal("an absence in the current generation lifted an older generation's exclusion")
	}
}

// 8. A RESTRICTION INSTALLED WHILE THE READ WAS OUT INVALIDATES THE ANSWER.
//
// The barrier is what makes this deterministic: the reassertion lands strictly between the
// observation and the lift, which is the schedule the instance counter exists for. An
// IDENTICAL bind is used deliberately — deduplicating the storage is safe, deduplicating the
// EVENT is not.
func TestMQTTExclusionReclaimRefusesAStaleInstance(t *testing.T) {
	t.Parallel()
	rig := newExclusionReclaimRig(t)
	const pi = uint16(19)
	subject := rig.subject(pi)
	nonce := mqttNewInflightNonce()
	var once sync.Once
	rig.setLoad(func(string) (*mqttInflightRecord, *StoredMsg, error) {
		once.Do(func() {
			// Between the observation and the lift: the same bind, reasserted.
			rig.exclude(pi, subject, nonce)
		})
		return nil, nil, nil // absent, honestly
	})
	rig.exclude(pi, subject, nonce)
	rig.runPass()
	if !rig.excluded(pi) {
		t.Fatal("a stale absence lifted an exclusion that had been reasserted under it")
	}
	if rig.allocatable(pi) {
		t.Fatal("the identifier was handed out on an observation taken before the restriction was restated")
	}
}

// 10. THE CALL-SITE TRIPWIRE. The install IS the arm, stated as a property over the one
// function rather than as a list of its 27 callers — the enumeration is what goes stale.
//
// It asserts the CALLBACK RAN, not that a flag was set, and the duplicate row establishes
// ISOLATED CALLBACK OWNERSHIP: the first install's continuation is driven to completion and
// the flag observed clear before the identical bind is reasserted, so the callback this row
// observes can only be the reassertion's.
func TestMQTTEveryExclusionInstallArmsTheReclaim(t *testing.T) {
	t.Parallel()
	t.Run("a fresh install arms a continuation that runs", func(t *testing.T) {
		t.Parallel()
		rig := newExclusionReclaimRig(t)
		const pi = uint16(29)
		subject := rig.subject(pi)
		rig.setLoad(func(string) (*mqttInflightRecord, *StoredMsg, error) {
			return presentRecord(pi, mqttNewInflightNonce()), &StoredMsg{Sequence: 8, Subject: subject}, nil
		})
		rig.exclude(pi, subject, mqttNewInflightNonce())
		if !rig.armed() {
			t.Fatal("the install did not arm the reclamation continuation")
		}
		rig.awaitLoad(subject, 1, "the armed continuation never ran")
	})

	t.Run("a duplicate assertion arms its own continuation", func(t *testing.T) {
		t.Parallel()
		rig := newExclusionReclaimRig(t)
		const pi = uint16(31)
		subject := rig.subject(pi)
		nonce := mqttNewInflightNonce()
		// The subject is ABSENT, so the first continuation lifts everything and disarms.
		rig.setLoad(absentLoad)
		rig.exclude(pi, subject, nonce)
		rig.runPass()
		if rig.armed() {
			t.Fatal("fixture error: the first continuation did not disarm, so a later callback could not be attributed")
		}
		if rig.excluded(pi) {
			t.Fatal("fixture error: the first pass did not lift, so the reassertion below is not a duplicate of a cleared state")
		}
		before := rig.loadCount(subject)

		// Now the IDENTICAL bind again. With the timer quiescent and the flag clear, any
		// callback from here is this assertion's.
		rig.exclude(pi, subject, nonce)
		if !rig.armed() {
			t.Fatal("the duplicate assertion did not arm a continuation")
		}
		rig.awaitLoad(subject, before+1, "the duplicate assertion's own continuation never ran")
	})
}

// 11. ONE BIND, ONE ADVANCE. The dedup restructure's own property.
//
// A reassertion must leave the storage deduplicated AND still advance the instance: the
// first is what keeps the set bounded, the second is what invalidates absence observations
// taken before it. The naive single-exit rewrite gets both wrong at once — it appends a
// duplicate and advances twice — and the stale-instance fixture above cannot see either.
func TestMQTTExclusionReassertionStoresOneBindAndAdvancesOnce(t *testing.T) {
	t.Parallel()
	rig := newExclusionReclaimRig(t)
	const pi = uint16(37)
	subject := rig.subject(pi)
	nonce := mqttNewInflightNonce()

	rig.exclude(pi, subject, nonce)
	rig.sess.mu.Lock()
	firstInst := rig.sess.slotExclusionInst
	rig.sess.mu.Unlock()

	rig.exclude(pi, subject, nonce)
	rig.sess.mu.Lock()
	binds := len(rig.sess.sendsInFlight[pi].slotOwed.binds)
	inst := rig.sess.slotExclusionInst
	entryInst := rig.sess.sendsInFlight[pi].slotOwed.inst
	rig.sess.mu.Unlock()

	if binds != 1 {
		t.Fatalf("a reassertion stored %d binds, want 1: an equivalent restriction adds no protection and accumulates without bound", binds)
	}
	if inst != firstInst+1 {
		t.Fatalf("the instance advanced by %d, want exactly 1: the counter is an invalidation token, not a bind count", inst-firstInst)
	}
	if entryInst != inst {
		t.Fatalf("the entry's instance is %d but the session counter is %d; a lift would compare against the wrong token", entryInst, inst)
	}
}

// 12. SATURATION REFUSES RATHER THAN WRAPS, on BOTH paths.
//
// The unique-bind path used to increment unguarded while only the duplicate path refused, so
// the installer could wrap where the lift could not. One guard over one increment is what
// makes the rule true everywhere rather than per branch.
func TestMQTTExclusionInstallRefusesToWrapAtSaturation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		twice bool
	}{
		{name: "a fresh bind", twice: false},
		{name: "a reasserted bind", twice: true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := newExclusionReclaimRig(t)
			const pi = uint16(41)
			subject := rig.subject(pi)
			nonce := mqttNewInflightNonce()
			if tc.twice {
				rig.exclude(pi, subject, nonce)
			}
			rig.sess.mu.Lock()
			rig.sess.slotExclusionInst = math.MaxUint64
			rig.sess.mu.Unlock()

			rig.exclude(pi, subject, nonce)

			rig.sess.mu.Lock()
			inst := rig.sess.slotExclusionInst
			held := slotExclusionHeldLocked(rig.sess.sendsInFlight[pi])
			rig.sess.mu.Unlock()
			if inst != math.MaxUint64 {
				t.Fatalf("the instance counter moved to %d at saturation; wrapping would let a stale absence match a fresh restriction", inst)
			}
			if !held {
				t.Fatal("saturation dropped the restriction itself; refusing to advance must never cost the exclusion")
			}
		})
	}
}

// 13/14. DISARMING AND RE-ARMING ARE DIFFERENT PROPERTIES.
//
// A continuation that never disarms polls an empty set forever; one that disarms while
// exclusions stand loses reclamation silently. They fail in opposite directions, so one
// fixture cannot cover both.
func TestMQTTExclusionReclaimDisarmsOnlyWhenNothingIsHeld(t *testing.T) {
	t.Parallel()
	t.Run("disarms on an empty pass", func(t *testing.T) {
		t.Parallel()
		rig := newExclusionReclaimRig(t)
		const pi = uint16(43)
		rig.setLoad(absentLoad)
		rig.exclude(pi, rig.subject(pi), mqttNewInflightNonce())
		rig.runPass()
		if rig.excluded(pi) {
			t.Fatal("fixture error: the exclusion did not lift, so this is not an empty pass")
		}
		if rig.armed() {
			t.Fatal("the continuation stayed armed with nothing held; it would poll an empty set forever")
		}
	})

	t.Run("stays armed while any exclusion stands", func(t *testing.T) {
		t.Parallel()
		rig := newExclusionReclaimRig(t)
		const pi = uint16(47)
		subject := rig.subject(pi)
		rig.setLoad(func(string) (*mqttInflightRecord, *StoredMsg, error) {
			return presentRecord(pi, mqttNewInflightNonce()), &StoredMsg{Sequence: 2, Subject: subject}, nil
		})
		rig.exclude(pi, subject, mqttNewInflightNonce())
		rig.runPass()
		if !rig.excluded(pi) {
			t.Fatal("fixture error: the exclusion lifted, so there is nothing left to re-arm for")
		}
		if !rig.armed() {
			t.Fatal("the continuation disarmed while an exclusion stood; nothing would ever reclaim it")
		}
	})
}

// 15. THE READ BUDGET BOUNDS A PASS, and it is counted in READS.
//
// Both shapes matter. Many single-bind identifiers must be spread across passes, and ONE
// identifier whose bind count exceeds the whole budget must still be taken ENTIRE — partial
// evidence is invalid the moment the instance can move, so a split entry could never lift.
func TestMQTTExclusionReclaimPacesALargeExclusionSet(t *testing.T) {
	t.Parallel()
	t.Run("many single-bind identifiers are spread across passes", func(t *testing.T) {
		t.Parallel()
		rig := newExclusionReclaimRig(t)
		rig.setLoad(func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
			return presentRecord(1, mqttNewInflightNonce()), &StoredMsg{Sequence: 2, Subject: subject}, nil
		})
		const total = mqttExclusionReclaimReadBudget * 2
		for pi := uint16(1); pi <= total; pi++ {
			rig.exclude(pi, rig.subject(pi), mqttNewInflightNonce())
		}
		cands, budgetMet := rig.sess.snapshotExclusionReclaimLocked_()
		if !budgetMet {
			t.Fatal("a set twice the budget did not meet it")
		}
		if len(cands) > mqttExclusionReclaimReadBudget {
			t.Fatalf("one pass took %d entries against a %d-read budget", len(cands), mqttExclusionReclaimReadBudget)
		}
	})

	t.Run("one oversized entry is taken entire", func(t *testing.T) {
		t.Parallel()
		rig := newExclusionReclaimRig(t)
		const pi = uint16(53)
		const binds = mqttExclusionReclaimReadBudget + 5
		for i := 0; i < binds; i++ {
			// Distinct subjects, so these are distinct restrictions rather than one.
			rig.exclude(pi, mqttInflightSubject(rig.sess.idHash, rig.sess.gen, uint16(1000+i)), mqttNewInflightNonce())
		}
		cands, budgetMet := rig.sess.snapshotExclusionReclaimLocked_()
		if len(cands) != 1 {
			t.Fatalf("took %d entries, want 1", len(cands))
		}
		if len(cands[0].binds) != binds {
			t.Fatalf("took %d of %d binds: an entry split across passes can never lift, because partial evidence is invalid once the instance moves",
				len(cands[0].binds), binds)
		}
		if !budgetMet {
			t.Fatal("an oversized entry did not report the budget met, so the pass would back off instead of continuing")
		}
	})
}

// 16. THE CURSOR IDENTIFIES AN ENTRY, NOT A POSITION IN THE SET.
//
// A rank cursor is not stable under mutation, and the failure is a STARVATION: inserting
// identifiers below the last visited one shifts every rank right, so a pass resuming "at
// rank N" resumes BEFORE the entry it already visited, re-reads the same prefix, and never
// advances to the tail.
//
// The discriminating assertion is therefore about WHERE THE NEXT PASS RESUMES, not about
// whether some entry is eventually seen — a set smaller than the read budget is visited
// entirely in one pass whatever the cursor means, which is exactly how the first version of
// this fixture let the rank mutation through. The budget has to bite for the question to
// exist at all, so the set is deliberately larger than it.
func TestMQTTExclusionReclaimCursorResumesByIdentifierNotByRank(t *testing.T) {
	t.Parallel()
	rig := newExclusionReclaimRig(t)
	rig.setLoad(func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		// Always occupied, so nothing lifts and the set only grows the way the fixture
		// says it does.
		return presentRecord(1, mqttNewInflightNonce()), &StoredMsg{Sequence: 2, Subject: subject}, nil
	})
	// Comfortably more entries than one pass may read, so a pass takes a PREFIX and the
	// cursor's meaning becomes observable.
	const high = uint16(40000)
	for i := 0; i < mqttExclusionReclaimReadBudget*2; i++ {
		pi := high + uint16(i)
		rig.exclude(pi, rig.subject(pi), mqttNewInflightNonce())
	}
	first, budgetMet := rig.sess.snapshotExclusionReclaimLocked_()
	if !budgetMet || len(first) == 0 {
		t.Fatal("fixture error: the first pass did not meet the read budget, so it took the whole set and the cursor is untested")
	}
	lastVisited := first[len(first)-1].pi

	// CHURN BELOW THE CURSOR. Every one of these sorts before the entry just visited, so a
	// rank cursor now points at an entry that was already read while a value cursor is
	// unaffected.
	for i := 0; i < mqttExclusionReclaimReadBudget; i++ {
		pi := uint16(100 + i)
		rig.exclude(pi, rig.subject(pi), mqttNewInflightNonce())
	}

	second, _ := rig.sess.snapshotExclusionReclaimLocked_()
	if len(second) == 0 {
		t.Fatal("the second pass took nothing")
	}
	for _, cand := range second {
		if cand.pi <= lastVisited {
			t.Fatalf("the second pass resumed at packet identifier %d, which is not past the last visited identifier %d: "+
				"inserting entries below the cursor shifted its meaning, so the tail of the set can be starved",
				cand.pi, lastVisited)
		}
	}
}

// 17/18. RESIDENCY IS NOT TERMINAL.
//
// A self-rescheduling timer holds the session object, so a nonresident object must stop
// polling. But the SAME object comes back — the expiry fire restores it on a drain timeout,
// and so do the clean-disconnect and refused-discard paths — and a restoration installs no
// new exclusion. Without re-enrolment the standing exclusions return resident with no timer
// and are never reclaimed again.
//
// Fixture 17 keeps a STANDING exclusion, so an empty-pass exit cannot masquerade as the
// residency check doing its job.
func TestMQTTExclusionReclaimStopsWhenNonresidentAndResumesOnRestoration(t *testing.T) {
	t.Parallel()
	rig := newExclusionReclaimRig(t)
	const pi = uint16(59)
	subject := rig.subject(pi)
	rig.setLoad(func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return presentRecord(pi, mqttNewInflightNonce()), &StoredMsg{Sequence: 2, Subject: subject}, nil
	})
	rig.exclude(pi, subject, mqttNewInflightNonce())

	// Nonresident, with the exclusion still standing.
	rig.as.mu.Lock()
	delete(rig.as.sessions, rig.sess.id)
	rig.as.mu.Unlock()

	before := rig.loadCount(subject)
	rig.runPass()
	if rig.loadCount(subject) != before {
		t.Fatal("a nonresident object still read subjects; the residency check did not stop the pass")
	}
	if rig.armed() {
		t.Fatal("a nonresident object stayed armed, pinning a session that is no longer live")
	}
	if !rig.excluded(pi) {
		t.Fatal("fixture error: the exclusion is gone, so nothing remains to re-enrol")
	}

	// Restoration of the SAME object, through the one function every such path calls.
	rig.as.addSession(rig.sess, true)
	if !rig.armed() {
		t.Fatal("residency was restored with an exclusion standing and nothing re-armed; its identifier would never be reclaimed")
	}
	rig.awaitLoad(subject, before+1, "the re-enrolled continuation never ran")
}

// 19/20. UNIT B: what each cancellation outcome LICENSES.
//
// Two questions, kept apart because they diverge: is this nonce's verdict retirable, and is
// the identifier releasable. NO outcome releases an identifier — that needs a captured
// instance and an all-bind absence proof this function has neither of.
func TestMQTTRegisterCancelOutcomeLicences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		outcome mqttRegCancelOutcome
		retires bool
		why     string
	}{
		{"refused is the zero value", mqttRegCancelRefused, false,
			"an arm that forgets to classify itself must fail closed"},
		{"stored", mqttRegCancelStored, true,
			"the tombstone is on the record and survives a restart"},
		{"already set", mqttRegCancelAlreadySet, true,
			"the same tombstone, written by an earlier attempt"},
		{"absent", mqttRegCancelAbsent, true,
			"the register is gone, so the verdict describes nothing"},
		{"superseded", mqttRegCancelSuperseded, true,
			"a readable foreign exchange proves ours is gone"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := mqttRegCancelRetiresVerdict(tc.outcome); got != tc.retires {
				t.Fatalf("mqttRegCancelRetiresVerdict(%v) = %v, want %v: %s", tc.outcome, got, tc.retires, tc.why)
			}
		})
	}
}

// EACH ARM RETURNS ITS OWN OUTCOME, driven through the real function.
//
// The predicate table above is a helper test, and a helper test cannot show that anything
// produces the values it classifies. This is the half that catches an arm returning the
// wrong constant — Stored from the absent arm, say, which would claim a tombstone that was
// never written and which the predicate test cannot see, because both happen to retire.
func TestMQTTMarkInflightRegisterCancelledClassifiesEveryArm(t *testing.T) {
	t.Parallel()
	const pi = uint16(67)
	ourNonce := mqttNewInflightNonce()
	for _, tc := range []struct {
		name  string
		load  func(sub string) (*mqttInflightRecord, *StoredMsg, error)
		store func(string, uint64, *mqttInflightRecord) (uint64, error)
		want  mqttRegCancelOutcome
		why   string
	}{
		{
			name: "an empty subject is ABSENT, never a tombstone",
			load: func(string) (*mqttInflightRecord, *StoredMsg, error) { return nil, nil, nil },
			want: mqttRegCancelAbsent,
			why:  "the register is gone; claiming Stored would assert a tombstone nothing wrote",
		},
		{
			name: "an already-cancelled record of ours is ALREADY SET",
			load: func(sub string) (*mqttInflightRecord, *StoredMsg, error) {
				r := presentRecord(pi, ourNonce)
				r.Dur, r.Seq, r.Cancelled = "$MQTT_abc", 500, true
				testRegisterSourceReply(r)
				return r, &StoredMsg{Sequence: 6, Subject: sub}, nil
			},
			want: mqttRegCancelAlreadySet,
			why:  "the tombstone is already on the record",
		},
		{
			name: "a readable foreign exchange is SUPERSEDED",
			load: func(sub string) (*mqttInflightRecord, *StoredMsg, error) {
				r := presentRecord(pi, mqttNewInflightNonce())
				r.Dur, r.Seq = "$MQTT_abc", 500
				testRegisterSourceReply(r)
				return r, &StoredMsg{Sequence: 7, Subject: sub}, nil
			},
			want: mqttRegCancelSuperseded,
			why:  "matchesRecord compares identity, so a mismatch is a different exchange",
		},
		{
			name: "a landed CAS is STORED",
			load: func(sub string) (*mqttInflightRecord, *StoredMsg, error) {
				r := presentRecord(pi, ourNonce)
				r.Dur, r.Seq = "$MQTT_abc", 500
				testRegisterSourceReply(r)
				return r, &StoredMsg{Sequence: 8, Subject: sub}, nil
			},
			store: func(string, uint64, *mqttInflightRecord) (uint64, error) { return 9, nil },
			want:  mqttRegCancelStored,
			why:   "this is the one arm where a tombstone really was written",
		},
		{
			name: "an unreadable occupant is REFUSED",
			load: func(sub string) (*mqttInflightRecord, *StoredMsg, error) {
				return nil, &StoredMsg{Sequence: 5, Subject: sub}, nil
			},
			want: mqttRegCancelRefused,
			why:  "a positive claim drawn from a read that failed is what F-034 found here",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := newExclusionReclaimRig(t)
			rig.as.mu.Lock()
			rig.as.testInflightLoad = tc.load
			rig.as.testInflightStore = tc.store
			rig.as.mu.Unlock()
			// NO STORE BEHIND THE RIG. Its JetStream handle is a placeholder that cannot answer a
			// read, and since the unsent ending a cancellation of an ORDINARY obligation first reads
			// the exchange, the placeholder is withdrawn: with no store the cancellation classifies
			// exactly as before, which is what these arms pin. The ordinary route with a store is
			// TestF105WithdrawCancellationTombstoneEndsTheObligation (evidence standing) and
			// TestF105CancellationWithoutEvidenceStoresTheStandaloneTombstone (none).
			rig.sess.jsa = nil
			got, _ := rig.sess.markInflightRegisterCancelled(nil, pi, "$MQTT_abc", 500, 8, ourNonce, mqttTestLicence(rig.sess, nil))
			if got != tc.want {
				t.Fatalf("outcome = %v, want %v: %s", got, tc.want, tc.why)
			}
		})
	}
}

// THE RESTORATION RACE, deterministically.
//
// The nonresident observation and the disarm must be ONE critical section under as.mu. Split
// them and a restoration lands in the gap: addSession sees the flag still armed and
// schedules nothing, the callback then clears it, and the session is resident with
// exclusions standing and no continuation. Nothing runs again until an unrelated install
// happens to arm one — and a session that is only holding exclusions is not allocating, so
// none is coming.
//
// The interleaving is forced rather than hoped for: the restoration runs while the callback
// is inside its residency section, so if that section does not hold as.mu the two overlap
// exactly as the race requires.
func TestMQTTExclusionReclaimRestorationCannotRaceTheDisarm(t *testing.T) {
	// NOT parallel: it installs a package-level seam.
	rig := newExclusionReclaimRig(t)
	const pi = uint16(71)
	subject := rig.subject(pi)
	rig.setLoad(func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return presentRecord(pi, mqttNewInflightNonce()), &StoredMsg{Sequence: 2, Subject: subject}, nil
	})
	rig.exclude(pi, subject, mqttNewInflightNonce())

	// Nonresident, with the exclusion standing.
	rig.as.mu.Lock()
	delete(rig.as.sessions, rig.sess.id)
	rig.as.mu.Unlock()

	// THE RESTORATION IS ATTEMPTED FROM INSIDE THE WINDOW, so the interleaving is forced
	// rather than hoped for. Holding as.mu across the disarm makes it block here; releasing
	// the lock lets it complete, observe an armed flag, schedule nothing, and lose the
	// continuation when the disarm below then clears that flag.
	restored := make(chan struct{})
	mqttTestExclusionReclaimDisarmHook = func() {
		go func() { defer close(restored); rig.as.addSession(rig.sess, true) }()
		// Long enough for an unsynchronised restoration to get all the way through.
		time.Sleep(100 * time.Millisecond)
	}
	t.Cleanup(func() { mqttTestExclusionReclaimDisarmHook = nil })

	rig.sess.runExclusionReclaim(mqttExclusionReclaimDelay)
	<-restored

	rig.as.mu.RLock()
	resident := rig.as.sessions[rig.sess.id] == rig.sess
	rig.as.mu.RUnlock()
	if !resident {
		t.Fatal("fixture error: the session was not restored, so the race window was not entered")
	}
	if !rig.excluded(pi) {
		t.Fatal("fixture error: the exclusion is gone, so there is nothing left to reclaim")
	}
	if !rig.armed() {
		t.Fatal("a restoration interleaved with the nonresident exit left a resident session holding an exclusion " +
			"with no continuation armed; its identifier would never be reclaimed")
	}
}

// SHUTDOWN STOPS THE PASS MID-FLIGHT, and the evidence it had gathered is discarded.
//
// A pass holds its worker token across every read it makes, and shutdown waits on that group
// before closing the request path. Against an unresponsive backend a full budget of reads
// would hold graceful shutdown open for minutes — charged to terminal drain work that cannot
// be abandoned, while this work can be: nothing durable is owed.
//
// The fixture closes the gate from inside the first read, so the remaining entries are
// reached only if the check is absent.
func TestMQTTExclusionReclaimStopsWhenShutdownBegins(t *testing.T) {
	t.Parallel()
	rig := newExclusionReclaimRig(t)
	var reads int32
	rig.setLoad(func(string) (*mqttInflightRecord, *StoredMsg, error) {
		if atomic.AddInt32(&reads, 1) == 1 {
			// Shutdown begins while the first read is out.
			rig.srv.willDispatchMu.Lock()
			rig.srv.willDispatchClosed = true
			rig.srv.willDispatchMu.Unlock()
		}
		return nil, nil, nil // absent: without the gate check these would all lift
	})
	// ONE identifier carrying MANY binds, plus several single-bind ones. The multi-bind
	// entry is the case a per-ENTRY check misses: an entry's bind count is unbounded, so
	// once its reads begin a check that only runs between entries lets it hold the worker
	// token for as long as the whole bind set takes.
	const entries = 6
	const fatBinds = 12
	for i := 0; i < fatBinds; i++ {
		rig.exclude(1, mqttInflightSubject(rig.sess.idHash, rig.sess.gen, uint16(2000+i)), mqttNewInflightNonce())
	}
	for pi := uint16(2); pi <= entries; pi++ {
		rig.exclude(pi, rig.subject(pi), mqttNewInflightNonce())
	}

	rig.sess.runExclusionReclaim(mqttExclusionReclaimDelay)

	// Strictly fewer reads than the FAT ENTRY alone requires: the first read closes the
	// gate, so a correct pass stops inside that entry's bind loop rather than finishing it.
	if got := atomic.LoadInt32(&reads); got >= fatBinds {
		t.Fatalf("the pass made %d reads after shutdown began, and one identifier alone holds %d binds; "+
			"the gate must be checked before every subject read, not only between identifiers", got, fatBinds)
	}
	// The exclusions that were never read must still stand: partial evidence is not a proof.
	stillHeld := 0
	for pi := uint16(1); pi <= entries; pi++ {
		if rig.excluded(pi) {
			stillHeld++
		}
	}
	if stillHeld == 0 {
		t.Fatal("every exclusion was lifted after shutdown began; the pass did not stop")
	}
	if rig.armed() {
		t.Fatal("the continuation stayed armed through shutdown, so it would keep rescheduling against a closed gate")
	}
}
