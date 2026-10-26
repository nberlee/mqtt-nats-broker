// Copyright 2024 The NATS Authors
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

package mqtt

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// F-041's deferred half. A deferred discharge IS the close it stands in for, so
// on every branch it must apply the action's lifetime, derive clean from it, and
// make the pair durable before doing anything fallible.
//
// Rounds 3, 4 and 5 of this item's plan review each found ONE branch that read
// the obligation without applying it -- the surviving branch, then the discard
// branch, then a single missing field on the discard branch. The rule is stated
// over every branch rather than patched per arm, and these tests are per branch
// for the same reason: applyDeferredDisconnect has exactly two operational
// branches and both are here.

// testOverrideAction returns a parked action carrying a DISCONNECT override that
// DIFFERS from the session's confirmed terms, which is what makes the assertions
// discriminate: a discharge that saved the session's own lifetime instead would
// write the confirmed value and look plausible.
//
// The stamp is deliberately in the past. A deferred discharge stands in for a
// close that already happened, so re-sampling the clock at discharge time is
// itself a defect, and an old stamp is what makes that visible.
func testOverrideAction(interval uint32) *mqttDeferredDisconnect {
	return testOverrideActionAged(interval, 600)
}

// testOverrideActionAged is testOverrideAction with the stamp's age chosen, which
// matters more than it looks: expiredBy(now) compares disconnectedAt+interval
// against now, so an age LARGER than the interval selects the DISCARD branch even
// for a finite non-zero override. A surviving-branch fixture therefore has to
// pick an age smaller than its interval, and one that does not is not a weaker
// test -- it is a test of the other branch wearing this one's name.
func testOverrideActionAged(interval uint32, ageSecs int64) *mqttDeferredDisconnect {
	return &mqttDeferredDisconnect{
		disconnectedAt: time.Now().Unix() - ageSecs,
		expiryInterval: interval,
		// Carries a DISCONNECT OVERRIDE, which is the only thing the discharge
		// writes for: these stand in for a park from a connection that carried one.
		// Without it the action is either a synthetic discard or an ordinary park on
		// the session's own confirmed terms, and the discharge deliberately leaves
		// both alone -- see
		// TestMQTTDeferredDischargeLeavesASyntheticDiscardsLifetimeAlone.
		lifetimeOverridden: true,
	}
}

// The SURVIVING branch must persist the override, not the session's retained
// terms.
//
// Never-expire over a confirmed finite interval is the arm that shows why
// carrying the override in the action is not enough on its own: the discharge
// arms no timer (the action says never-expire) while the RECORD still said 60,
// so a restart or a record reader expired a session the client asked to keep
// forever.
func TestMQTTDeferredDischargeAppliesOverrideOnSurvivingBranch(t *testing.T) {
	e := testNewDeferredEnv(t, "f041dsurv", "f/surv")

	e.sess.mu.Lock()
	confirmed := e.sess.expiryInterval
	e.sess.mu.Unlock()
	if confirmed == mqttSessionNeverExpire {
		t.Fatalf("this fixture needs a confirmed FINITE interval to differ from the override, got %d", confirmed)
	}

	action := testOverrideAction(mqttSessionNeverExpire)
	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	e.release()

	// Memory first: the session is what every later teardown decision reads.
	e.sess.mu.Lock()
	gotExpiry, gotClean, gotStamp := e.sess.expiryInterval, e.sess.clean, e.sess.disconnectedAt
	e.sess.mu.Unlock()
	if gotExpiry != mqttSessionNeverExpire {
		t.Errorf("session expiryInterval = %d after the discharge, want the action's %d",
			gotExpiry, uint32(mqttSessionNeverExpire))
	}
	if gotClean {
		t.Error("session clean = true for a never-expire lifetime, want false: clean is DERIVED from the interval")
	}
	if gotStamp != action.disconnectedAt {
		t.Errorf("session disconnectedAt = %d, want the action's %d: a discharge must not re-sample the clock",
			gotStamp, action.disconnectedAt)
	}

	// Then the record, which is the half that outlives this process.
	ps := e.loadRecord(t)
	if ps.ExpiryInterval != mqttSessionNeverExpire {
		t.Errorf("record ExpiryInterval = %d, want %d: a restart or the expiry sweep will delete a session the client asked to keep forever",
			ps.ExpiryInterval, uint32(mqttSessionNeverExpire))
	}
	if ps.Clean {
		t.Error("record Clean = true for a never-expire session, want false")
	}
	if ps.DisconnectedAt != action.disconnectedAt {
		t.Errorf("record DisconnectedAt = %d, want the action's %d", ps.DisconnectedAt, action.disconnectedAt)
	}
}

// The surviving branch, shrink direction: 3600 -> 60. The discharge arms an early
// timer from the action, and expireSession's own deadline recheck reads the
// SESSION -- so leaving the session on 3600 made that recheck refuse the expiry
// and the session outlived its instruction.
func TestMQTTDeferredDischargeAppliesShrunkOverrideSoTheRecheckAgrees(t *testing.T) {
	e := testNewDeferredEnv(t, "f041dshrink", "f/shrink")

	// Age 5s against a 60s interval: NOT elapsed, so this reaches the surviving
	// branch. Aged at all, so a discharge that re-sampled the clock is still
	// visible. An age past the interval would take the discard branch instead and
	// this test would silently be testing that one.
	action := testOverrideActionAged(60, 5)
	if action.expiredBy(time.Now()) {
		t.Fatal("the action's deadline has elapsed, so this fixture reaches the discard branch rather than the surviving one it names")
	}
	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	e.release()

	e.sess.mu.Lock()
	gotExpiry, gotClean, gotStamp := e.sess.expiryInterval, e.sess.clean, e.sess.disconnectedAt
	e.sess.mu.Unlock()
	if gotExpiry != 60 {
		t.Errorf("session expiryInterval = %d, want the action's 60", gotExpiry)
	}
	if gotClean {
		t.Error("session clean = true for a finite non-zero lifetime, want false")
	}
	if gotStamp != action.disconnectedAt {
		t.Errorf("session disconnectedAt = %d, want the action's %d", gotStamp, action.disconnectedAt)
	}

	// The point of the arm: the SESSION now carries 60, so expireSession's own
	// deadline recheck -- which reads the session, not the action -- agrees with
	// the early timer the discharge armed from the action. Left on the confirmed
	// 3600 that recheck refused the expiry and the session outlived its
	// instruction.
	ps := e.loadRecord(t)
	if ps.ExpiryInterval != 60 {
		t.Errorf("record ExpiryInterval = %d, want 60", ps.ExpiryInterval)
	}
	if ps.DisconnectedAt != action.disconnectedAt {
		t.Errorf("record DisconnectedAt = %d, want the action's %d", ps.DisconnectedAt, action.disconnectedAt)
	}
}

// The DISCARD branch: the ended lifetime must be durable BEFORE the fallible
// clear(), because clear() can fail without deleting anything.
//
// The schedule needs no failed lifetime write and no foreign settlement, which
// is why the close's own backstop never covered it: an unrelated write goes
// ambiguous, a DISCONNECT overrides to zero and parks before the close's update
// ever runs, the unrelated write settles as ours, the discharge selects discard,
// and clear() then refuses. Without the pre-discard write the record still says
// Clean=false with the old interval, and a CONNECT resumes a session the
// DISCONNECT ended.
func TestMQTTDeferredDischargePersistsTheEndedLifetimeBeforeARefusedDiscard(t *testing.T) {
	e := testNewDeferredEnv(t, "f041ddisc", "f/disc")

	// Force clear() to refuse, so nothing is deleted and the record this test
	// reads is the one the backstop wrote.
	armed := testArmAmbiguityAtClearPreFenceUnit(t, e.sess)

	// expiryInterval 0 selects the discard branch through endsAtDisconnect().
	action := testOverrideAction(0)
	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	e.release()

	if !armed() {
		t.Fatal("clear()'s pre-fence window was never reached, so the discard was not refused and this fixture did not exercise its route")
	}

	ps := e.loadRecord(t)
	if !ps.Clean {
		t.Error("record Clean = false after a discharge that ended the session: a CONNECT will resume a session the DISCONNECT ended")
	}
	if ps.ExpiryInterval != 0 {
		t.Errorf("record ExpiryInterval = %d, want 0", ps.ExpiryInterval)
	}
	// The stamp is what makes the expiredBy half of this same case clause work:
	// both resume checks need a POSITIVE timestamp to recognise expiry, so a
	// backstop that writes the lifetime and leaves this at zero leaves an
	// already-expired finite session resumable. Exact equality, not merely
	// positive -- "positive and eventually overdue" also passes against a
	// discharge that re-sampled the clock.
	if ps.DisconnectedAt != action.disconnectedAt {
		t.Errorf("record DisconnectedAt = %d, want the action's %d exactly", ps.DisconnectedAt, action.disconnectedAt)
	}
}

// The expiredBy half of the same case clause: a FINITE override whose deadline
// elapsed while the record was unresolved. Clean is false here, so the stamp is
// the only thing that makes the record non-resumable -- which is why the
// previous test's stamp assertion is not redundant with this one.
func TestMQTTDeferredDischargePersistsTheStampForAnElapsedFiniteOverride(t *testing.T) {
	e := testNewDeferredEnv(t, "f041delapsed", "f/elapsed")

	armed := testArmAmbiguityAtClearPreFenceUnit(t, e.sess)

	// Interval 1 with a stamp 600s old: endsAtDisconnect() is false, so this
	// reaches the discard through expiredBy(now) instead.
	action := testOverrideAction(1)
	if action.endsAtDisconnect() {
		t.Fatal("this fixture must reach the discard through expiredBy, not endsAtDisconnect")
	}
	if !action.expiredBy(time.Now()) {
		t.Fatal("the action's deadline has not elapsed, so the discard branch is not selected")
	}
	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	e.release()

	if !armed() {
		t.Fatal("clear()'s pre-fence window was never reached, so the discard was not refused")
	}

	ps := e.loadRecord(t)
	if ps.ExpiryInterval != 1 {
		t.Errorf("record ExpiryInterval = %d, want the action's 1", ps.ExpiryInterval)
	}
	if ps.Clean {
		t.Error("record Clean = true for a finite non-zero lifetime, want false")
	}
	if ps.DisconnectedAt != action.disconnectedAt {
		t.Errorf("record DisconnectedAt = %d, want the action's %d exactly: with Clean=false, the stamp is the ONLY thing that makes this record non-resumable",
			ps.DisconnectedAt, action.disconnectedAt)
	}
}

// testArmAmbiguityAtClearPreFenceUnit is the unit-harness twin of the e2e
// helper: it arms ONE ambiguous write inside clear()'s pre-fence window so the
// discard refuses with errMQTTClearRefusedUnresolved and deletes nothing.
//
// Separate from the e2e one because that lives behind the mqtt_fulltests
// tag and takes *testing.T from a client-level fixture. The reporter is a
// closure over an atomic because the hook runs on whatever goroutine drives the
// discharge while the reporter is read from the test goroutine.
func testArmAmbiguityAtClearPreFenceUnit(t *testing.T, sess *mqttSession) func() bool {
	t.Helper()
	var armed atomic.Bool
	restore := setMQTTTestClearWindowHook(func(cur *mqttSession, window string) {
		if cur != sess || window != mqttClearWindowPreFence || !armed.CompareAndSwap(false, true) {
			return
		}
		cur.mu.Lock()
		testMarkUncertainLocked(cur, cur.seq, testAmbiguousBodyLocked(cur, "f041-unrelated"))
		cur.mu.Unlock()
	})
	t.Cleanup(restore)
	return armed.Load
}

// A refused discard must still be recognisable as refused rather than silently
// treated as done, and the sentinel is what the discharge keys on. Asserted
// directly so the tests above rest on a checked premise rather than on the
// helper's name.
func TestMQTTDeferredDischargeRefusalUsesTheUnresolvedSentinel(t *testing.T) {
	e := testNewDeferredEnv(t, "f041dsent", "f/sent")

	testArmAmbiguityAtClearPreFenceUnit(t, e.sess)
	if err := e.sess.clear(true); !errors.Is(err, errMQTTClearRefusedUnresolved) {
		t.Fatalf("clear() = %v, want errMQTTClearRefusedUnresolved: the discharge's re-park is keyed on that sentinel", err)
	}
}

// The FINALITY policy, which is the one thing the close's caller needs that the
// CONNECT's does not.
//
// Both arms in one test, because the difference between them IS the assertion: a
// definite write failure leaves a CONNECT's speculative terms restored and an
// accepted connection's DISCONNECT override STANDING. Testing only one arm would
// pass against a helper that ignored finality entirely.
//
// NOT parallel: setMQTTTestSessSaveHook is a process-global setter.
func TestMQTTLifetimeFinalityDecidesADefiniteFailure(t *testing.T) {
	for _, tc := range []struct {
		name     string
		finality mqttLifetimeFinality
		// wantKept is whether the requested terms survive the failed write.
		wantKept bool
		why      string
	}{
		{"speculative restores", mqttLifetimeSpeculative, false,
			"a CONNECT can still be refused, so terms that did not prove themselves must not decide a teardown"},
		{"final keeps", mqttLifetimeFinal, true,
			"a DISCONNECT is unrefusable and the client has been told nothing further, so restoring would silently ignore its instruction"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewDeferredEnv(t, "f041fin"+tc.name[:4], "f/fin")

			e.sess.mu.Lock()
			confirmed := e.sess.expiryInterval
			e.sess.mu.Unlock()
			// Non-zero and different from the confirmed terms: equal values take the
			// helper's no-change early return, which reaches no classifier at all.
			const requested = uint32(600)
			if confirmed == requested {
				t.Fatalf("the requested interval must differ from the confirmed %d, or the helper takes its no-change early return", confirmed)
			}

			// A DEFINITE failure, before the store: nothing reached the stream, so
			// the classifier reports a local failure rather than an ambiguity.
			restore := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
				if sess == e.sess {
					return errors.New("f041: definite pre-store failure")
				}
				return nil
			})
			defer restore()

			tok := e.sess.beginRecordTx()
			g := e.sess.applyRequestedLifetime(tok, requested, mqttSessionIsCleanForExpiry(requested), tc.finality)
			if terr := e.sess.endRecordTx(tok); terr != nil {
				t.Fatalf("endRecordTx: %v", terr)
			}
			if g.outcome != mqttPreConnackLocal {
				t.Fatalf("outcome = %v, want mqttPreConnackLocal: this fixture must reach the definite-failure arm, not another one", g.outcome)
			}

			e.sess.mu.Lock()
			gotExpiry, gotClean := e.sess.expiryInterval, e.sess.clean
			e.sess.mu.Unlock()

			if tc.wantKept {
				if gotExpiry != requested || gotClean != mqttSessionIsCleanForExpiry(requested) {
					t.Errorf("under Final the terms were restored to expiry=%d clean=%v, want the requested %d/%v: %s",
						gotExpiry, gotClean, requested, mqttSessionIsCleanForExpiry(requested), tc.why)
				}
			} else {
				if gotExpiry != confirmed {
					t.Errorf("under Speculative the unproven terms were kept as expiry=%d, want the confirmed %d: %s",
						gotExpiry, confirmed, tc.why)
				}
			}
		})
	}
}

// The landed continuation must not RETARGET a parked action under Final.
//
// Under Final the action was built from the override in the first place, so
// there is nothing stale to fix and a retarget would rewrite it. The plan for
// this item recorded the mutant as "behaviourally equivalent", which is true
// only when the continuation's captured expiry happens to equal the action's --
// so this fixture deliberately makes them DIFFER, which is what turns a call
// policy into an observable value.
//
// NOT parallel: mqttTestSessStoreSuppressV is process-global.
func TestMQTTLifetimeFinalDoesNotRetargetTheParkedAction(t *testing.T) {
	for _, tc := range []struct {
		name       string
		finality   mqttLifetimeFinality
		wantAction uint32
	}{
		// Speculative retargets: the action snapshotted the confirmed terms while
		// the write was unresolved, so the proven grant is the newer fact.
		{"speculative retargets", mqttLifetimeSpeculative, 600},
		// Final does not: the action already carries the client's instruction.
		{"final leaves it alone", mqttLifetimeFinal, 77},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewDeferredEnv(t, "f041rt"+tc.name[:4], "f/rt")

			// AMBIGUOUS, so the landed continuation is registered at all: an
			// ordinary successful write registers none and makes zero calls under
			// either implementation.
			// Cleared through t.Cleanup, NOT by a trailing Store(false): the
			// t.Fatalf below would otherwise leave this process-global flag set for
			// the rest of the package run, suppressing every later session-record
			// store and failing unrelated tests on state that needed a write to
			// land. A leaked global is indistinguishable from a flake from the
			// outside, which is what makes the trailing form the wrong one even
			// when the fatal path is never taken.
			mqttTestSessStoreSuppressV.Store(true)
			t.Cleanup(func() { mqttTestSessStoreSuppressV.Store(false) })
			tok := e.sess.beginRecordTx()
			g := e.sess.applyRequestedLifetime(tok, 600, false, tc.finality)
			if terr := e.sess.endRecordTx(tok); terr != nil {
				t.Fatalf("endRecordTx: %v", terr)
			}
			mqttTestSessStoreSuppressV.Store(false)

			// Parked AFTER the grant captured priorDeferred, which is the real
			// sequence: the close parks while the write is unresolved. Installing it
			// beforehand makes prior == cur, and the retarget then correctly declines
			// to rewrite what it reads as an EARLIER connection's action -- so that
			// ordering would test the pointer-identity guard rather than finality.
			//
			// Its interval is neither the confirmed terms nor the grant's, so
			// whichever value ends up there names its writer unambiguously.
			action := testOverrideActionAged(77, 5)
			e.sess.mu.Lock()
			e.sess.deferredDisconnect = action
			e.sess.mu.Unlock()
			if g.outcome != mqttPreConnackUnresolved {
				t.Fatalf("outcome = %v, want mqttPreConnackUnresolved: without an unresolved write the continuation never runs and this fixture proves nothing", g.outcome)
			}

			// Settle it as OURS, which is what runs the landed continuation.
			if res := e.sess.resolveUncertainNow(e.c); res != mqttUncertainOurs {
				t.Fatalf("resolveUncertainNow = %v, want ours", res)
			}

			e.sess.mu.Lock()
			got := action.expiryInterval
			e.sess.mu.Unlock()
			if got != tc.wantAction {
				t.Errorf("parked action expiryInterval = %d, want %d", got, tc.wantAction)
			}
		})
	}
}

// A SYNTHETIC discard action must not have its zero interval persisted as the
// session's lifetime.
//
// Found by the holistic review of this change, and it is the same defect class
// the item spent three plan rounds on: one representation carrying two facts.
// `action.expiryInterval` is a LIFETIME when the action was built from a
// connection, and a discard INSTRUCTION when reparkRefusedDiscard synthesised
// it -- and that path retries ordinary expiry recovery for a session whose real
// lifetime is FINITE. Persisting the zero would write Clean=true with
// ExpiryInterval=0 over that finite record, and sweepSessionExpiries skips
// interval-zero records, so a subsequently refused clear() leaves the session
// unreapable where before it had a deadline the sweep could retry. Strictly
// worse than not writing at all.
//
// NOT parallel: setMQTTTestClearWindowHook is a process-global setter.
func TestMQTTDeferredDischargeLeavesASyntheticDiscardsLifetimeAlone(t *testing.T) {
	e := testNewDeferredEnv(t, "f041dsyn", "f/syn")

	// A FINITE lifetime, which is what makes the record reapable and what the
	// synthetic zero would destroy. The harness session starts at 0, so this is
	// set explicitly and persisted, exactly as a finite-expiry session's own close
	// would have left it.
	const finite = uint32(3600)
	e.sess.mu.Lock()
	e.sess.expiryInterval = finite
	e.sess.clean = mqttSessionIsCleanForExpiry(finite)
	e.sess.disconnectedAt = time.Now().Unix() - 7200 // long overdue, as expiry recovery finds it
	e.sess.mu.Unlock()
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the finite lifetime: %v", err)
	}
	if ps := e.loadRecord(t); ps.ExpiryInterval != finite {
		t.Fatalf("record ExpiryInterval = %d, want the finite %d before the discharge runs", ps.ExpiryInterval, finite)
	}

	// The synthetic action reparkRefusedDiscard builds: interval 0 meaning
	// "discard now", with lifetimeOverridden left false.
	action := &mqttDeferredDisconnect{disconnectedAt: time.Now().Unix(), expiryInterval: 0}
	if action.lifetimeOverridden {
		t.Fatal("a synthetic discard action must not be marked as carrying an override, or this test asserts nothing")
	}
	if !action.endsAtDisconnect() {
		t.Fatal("this fixture must reach the discard branch")
	}

	// Refuse the discard, so a record survives for the assertion.
	armed := testArmAmbiguityAtClearPreFenceUnit(t, e.sess)
	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	e.release()
	if !armed() {
		t.Fatal("clear()'s pre-fence window was never reached, so the discard was not refused")
	}

	ps := e.loadRecord(t)
	if ps.ExpiryInterval != finite {
		t.Errorf("the discharge persisted the synthetic discard interval as the lifetime: record ExpiryInterval = %d, want the finite %d. "+
			"sweepSessionExpiries skips interval-zero records, so this session is now unreapable without a reconnect",
			ps.ExpiryInterval, finite)
	}
	if ps.Clean {
		t.Error("the discharge persisted Clean=true for a finite session from a synthetic discard action")
	}
}

// An ordinary park with NO override must add no record write at all.
//
// This is the boundary the gate draws, and it needs its own test because nothing
// else in the suite catches widening it: an action carrying the session's own
// confirmed terms would be written back identically, so no state assertion can
// tell the two implementations apart. What distinguishes them is whether a STORE
// happened, which is why this counts stores rather than inspecting the record.
//
// Widening matters for two reasons beyond the wasted write. Every parked close
// would pay it, and an AMBIGUOUS backstop store reparks a discard that is
// otherwise simply attempted -- so the widened form changes the outcome of a
// path F-041 does not own.
//
// NOT parallel: setMQTTTestSessStoreResultHook and setMQTTTestClearWindowHook
// are process-global setters.
func TestMQTTDeferredDischargeWritesNothingForANoOverridePark(t *testing.T) {
	e := testNewDeferredEnv(t, "f041dnoov", "f/noov")

	// The harness session's confirmed interval is 0, so an ordinary park carries
	// interval 0 and reaches the discard branch -- the same branch the override
	// backstop writes on. That is what makes this the discriminating shape rather
	// than a different branch avoiding the question.
	e.sess.mu.Lock()
	confirmed := e.sess.expiryInterval
	e.sess.mu.Unlock()
	if confirmed != 0 {
		t.Fatalf("this fixture needs a confirmed interval of 0 to reach the discard branch, got %d", confirmed)
	}

	action := &mqttDeferredDisconnect{
		disconnectedAt: time.Now().Unix() - 5,
		expiryInterval: confirmed,
	}
	if action.lifetimeOverridden {
		t.Fatal("an ordinary park carries no override, or this test asserts nothing")
	}
	if !action.endsAtDisconnect() {
		t.Fatal("this fixture must reach the discard branch, where the backstop would run")
	}

	// Refuse the discard, so the discharge cannot delete the record and the only
	// store that could be counted is the backstop's own.
	testArmAmbiguityAtClearPreFenceUnit(t, e.sess)

	stores, restore := testSessRecordStoreCounter(e.sess)
	defer restore()

	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	e.release()

	if *stores != 0 {
		t.Errorf("an ordinary no-override park performed %d session-record store(s) during its discharge, want 0: "+
			"the backstop is for a DISCONNECT override, and running it here adds a write to every parked close "+
			"and lets an ambiguous store repark a discard that is otherwise attempted", *stores)
	}
}
