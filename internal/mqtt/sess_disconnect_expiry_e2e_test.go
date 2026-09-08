//go:build mqttport_fulltests

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
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// F-041: a v5 DISCONNECT may replace the Session Expiry Interval (Spec5
// [3.14.2.2.2]), and until this change that replacement reached memory only.
// Two defects followed from one cause, in OPPOSITE directions, which is why
// nothing here is conditioned on the override being zero:
//
//   - an override LOWERING the interval to 0 left a record still saying
//     Clean=false with the pre-override interval, so a discard that did not
//     complete let a later CONNECT resume a session the DISCONNECT ended and
//     report Session Present true, against Spec5 [3.1.2.11];
//   - an override RAISING it to mqttSessionNeverExpire left a FINITE record with
//     DisconnectedAt=0, which sweepSessionExpiries stamps and arms a timer for --
//     deleting a session the client asked to keep forever.
//
// Clean and the interval are ONE fact (mqttSessionIsCleanForExpiry). Persisting
// the interval alone would be strictly worse than persisting nothing: the resume
// decision reads Clean, `overdue` needs a positive interval, and the expiry sweep
// skips zero, so the record would be both resumable and unreapable.

// testMQTTExpectSessionClientGone waits until the broker holds no client for
// clientID, i.e. the connection really closed and its teardown ran, rather than
// the test merely having stopped writing to it.
//
// Every assertion in this file that inspects a record waits on THIS rather than
// on a sleep: the DISCONNECT is processed on the readLoop and the teardown runs
// on it too, so the client being gone is the observable that orders the record
// write before the read.
func testMQTTExpectSessionClientGone(t testing.TB, s *Server, clientID string) {
	t.Helper()
	checkFor(t, testMQTTTimeout, 20*time.Millisecond, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, c := range s.clients {
			c.mu.Lock()
			match := c.isMqtt() && c.mqtt.cid == clientID
			c.mu.Unlock()
			if match {
				return fmt.Errorf("client %q is still connected", clientID)
			}
		}
		return nil
	})
}

// testMQTTOverrideRecord connects a v5 client with a CONNECT Session Expiry
// Interval, subscribes it so the session has restorable state, sends a
// DISCONNECT carrying the override, closes, and returns the persisted record.
//
// The probe client is not incidental: loading a record needs a live client to
// borrow an account session manager from, and it must NOT share the client ID
// under test, or its own CONNECT would be the thing that resumes or discards the
// record the assertions are about.
func testMQTTOverrideRecord(t testing.TB, s *Server, o *Options, clientID string,
	connectSEI, overrideSEI uint32) (*mqttPersistedSession, bool) {

	t.Helper()
	probe, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "probe", cleanStart: true},
		o.MQTT.Host, o.MQTT.Port)
	defer probe.Close()
	testMQTTReadConnAckV5(t, pr)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(connectSEI)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: clientID + "/t", opts: 1}})

	if _, err := testMQTTWrite(c, mqttV5DisconnectSessionExpiry(mqttReasonSuccess, overrideSEI)); err != nil {
		t.Fatalf("Error writing DISCONNECT with Session Expiry %d: %v", overrideSEI, err)
	}
	c.Close()
	testMQTTExpectSessionClientGone(t, s, clientID)

	ps, _, ok := testMQTTLoadSessionRecord(t, s, clientID+"probe", clientID)
	return ps, ok
}

// The record itself, in both override directions.
//
// This is the assertion the defect is actually about. A reconnect-level test can
// pass against a broker that simply deleted the record, and the never-expire
// direction has no reconnect-visible symptom at all until a sweep runs -- so
// both arms read the persisted bytes.
func TestMQTTv5DisconnectOverridePersistsLifetimePair(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	t.Run("override to never-expire persists the raised interval", func(t *testing.T) {
		ps, ok := testMQTTOverrideRecord(t, s, o, "f041n", 60, mqttSessionNeverExpire)
		if !ok {
			t.Fatal("a never-expire session's record must survive its close")
		}
		if ps.ExpiryInterval != mqttSessionNeverExpire {
			t.Errorf("record says ExpiryInterval=%d after a DISCONNECT override to never-expire, want %d: "+
				"the expiry sweep will stamp this record and delete a session the client asked to keep forever",
				ps.ExpiryInterval, uint32(mqttSessionNeverExpire))
		}
		if ps.Clean {
			t.Error("record says Clean=true for a never-expire session, want false")
		}
	})

	t.Run("a raise from zero is refused and reaches nothing", func(t *testing.T) {
		// Spec5 [3.14.2.2.2]: raising a zero interval is a Protocol Error. The
		// override is ignored, so no obligation is recorded and the close behaves
		// exactly as with no property at all -- which for interval 0 means the
		// session ends here.
		ps, ok := testMQTTOverrideRecord(t, s, o, "f041g", 0, 30)
		if ok && (ps.ExpiryInterval != 0 || !ps.Clean) {
			t.Errorf("a refused 0->non-zero override reached the record: got ExpiryInterval=%d Clean=%v, want 0/true",
				ps.ExpiryInterval, ps.Clean)
		}
	})
}

// Defect B end to end: the sweep must not expire a session whose DISCONNECT
// raised the interval to never-expire.
//
// Driven with NO resident object, because that is the population the defect
// needs -- sweepSessionExpiries reads records and skips a resident session, so a
// fixture that leaves the object resident passes against the unfixed broker.
func TestMQTTv5DisconnectRaiseToNeverExpireSurvivesTheSweep(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	probe, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f041swprobe", cleanStart: true},
		o.MQTT.Host, o.MQTT.Port)
	defer probe.Close()
	testMQTTReadConnAckV5(t, pr)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f041sw", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "f041sw/t", opts: 1}})

	if _, err := testMQTTWrite(c, mqttV5DisconnectSessionExpiry(mqttReasonSuccess, mqttSessionNeverExpire)); err != nil {
		t.Fatalf("Error writing DISCONNECT: %v", err)
	}
	c.Close()
	testMQTTExpectSessionClientGone(t, s, "f041sw")

	asm := testMQTTGetClient(t, s, "f041swprobe").mqtt.asm

	// Evict the resident object so the sweep takes the RECORD path.
	asm.mu.Lock()
	resident := asm.sessions["f041sw"]
	if resident != nil {
		delete(asm.sessions, "f041sw")
		delete(asm.sessByHash, getHash("f041sw"))
	}
	asm.mu.Unlock()
	if resident == nil {
		t.Fatal("expected a resident session object to evict; without eviction the sweep skips it and this fixture proves nothing")
	}

	asm.sweepSessionExpiries(s)

	ps, _, ok := testMQTTLoadSessionRecord(t, s, "f041swprobe", "f041sw")
	if !ok {
		t.Fatal("the sweep deleted a never-expire session's record")
	}
	if ps.DisconnectedAt != 0 {
		t.Errorf("the sweep stamped DisconnectedAt=%d on a never-expire record: it has armed an expiry for a session the client asked to keep forever",
			ps.DisconnectedAt)
	}
	if ps.ExpiryInterval != mqttSessionNeverExpire {
		t.Errorf("record ExpiryInterval=%d, want %d", ps.ExpiryInterval, uint32(mqttSessionNeverExpire))
	}

	// Restore residency so the reconnect below exercises the ordinary resume
	// rather than a record restore this test did not set up.
	asm.mu.Lock()
	asm.sessions["f041sw"] = resident
	asm.sessByHash[getHash("f041sw")] = resident
	asm.mu.Unlock()

	c2, r2 := testMQTTConnectRetryV5(t, &mqttV5ConnInfo{clientID: "f041sw",
		props: mqttV5ConnPropsSessionExpiry(mqttSessionNeverExpire)}, o.MQTT.Host, o.MQTT.Port, 3)
	defer c2.Close()
	if sp, _, _ := testMQTTReadConnAckV5(t, r2); !sp {
		t.Fatal("expected session-present=true: a never-expire override must keep the session")
	}
}

// The control. Without it the tests above pass against a broker that discards
// every session it sees: they would read "no record" or "Clean=true" for the
// right reason and the wrong cause.
func TestMQTTv5DisconnectWithoutOverrideStillResumes(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f041ctl", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "f041ctl/t", opts: 1}})

	// A DISCONNECT with NO Session Expiry property: nothing is overridden, so the
	// session keeps its granted 3600 and must resume.
	testMQTTDisconnect(t, c, nil)
	c.Close()
	testMQTTExpectSessionClientGone(t, s, "f041ctl")

	c2, r2 := testMQTTConnectRetryV5(t, &mqttV5ConnInfo{clientID: "f041ctl", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port, 3)
	defer c2.Close()
	if sp, _, _ := testMQTTReadConnAckV5(t, r2); !sp {
		t.Fatal("expected session-present=true: a DISCONNECT carrying no override must not end the session")
	}
	// The subscription has to still be there, or "session present" is a bare bit.
	testMQTTSubV5(t, c2, r2, 1, []mqttV5SubFilter{{topic: "f041ctl/t", opts: 1}})
}

// The packet records an OBLIGATION of the connection and no longer mutates the
// session.
//
// Two properties, tested where each is DETERMINISTIC rather than where it is
// convenient. Asserting "the session's terms did not move" on the owning
// connection is a race against its own teardown, which legitimately applies the
// override moments later -- so that half is tested on a DISPLACED connection,
// where the live session must never be touched at all. That is also the case
// the deleted `sess.c == c` guard existed for, so it is the one worth pinning.
func TestMQTTv5DisconnectOverrideIsAConnectionObligation(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	t.Run("the connection records it", func(t *testing.T) {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f041ob", cleanStart: true,
			props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTReadConnAckV5(t, r)

		// Captured BEFORE the DISCONNECT: the packet closes the connection, so
		// looking the client up afterwards finds nothing.
		cl := testMQTTGetClient(t, s, "f041ob")
		if ov := cl.mqttLifetimeOverride(); ov != nil {
			t.Fatalf("a connection with no DISCONNECT override carries one: %d", *ov)
		}

		if _, err := testMQTTWrite(c, mqttV5DisconnectSessionExpiry(mqttReasonSuccess, 30)); err != nil {
			t.Fatalf("Error writing DISCONNECT: %v", err)
		}
		// The packet is processed on the readLoop; poll the captured client's own
		// copy. Nothing clears it, so this is stable once set.
		checkFor(t, testMQTTTimeout, 15*time.Millisecond, func() error {
			ov := cl.mqttLifetimeOverride()
			if ov == nil {
				return fmt.Errorf("the DISCONNECT override has not been recorded yet")
			}
			if *ov != 30 {
				t.Fatalf("connection records lifetimeOverride=%d, want 30", *ov)
			}
			return nil
		})
	})
}

// WHY THERE IS NO DISPLACED-CONNECTION TEST HERE.
//
// The obvious companion to the arm above would send a DISCONNECT override on a
// connection that has already been displaced and assert the live session's terms
// do not move -- the case the deleted `sess.c == c` guard used to protect. That
// route is not reachable AS A TEST CAN DRIVE IT: a DISCONNECT sent after the
// replacement's CONNACK never reaches the packet handler, because the takeover
// closes the displaced connection before its readLoop reads again. Measured 5/5
// with a probe that polled the displaced client's own lifetimeOverride for three
// seconds after a successful socket write; it was never recorded.
//
// Stated that precisely on purpose. The probe covers the post-takeover send
// route only. It does NOT rule out a DISCONNECT already buffered on the
// connection racing the takeover -- which no fixture can schedule
// deterministically either, and which is why the property below is established
// by construction and by mutation rather than by a test.
//
// An earlier version of this file asserted the property anyway, waiting for the
// displaced client to leave the server as its barrier. Round 2 of the holistic
// review caught it: `closeConnection` removes that client during the takeover
// regardless, so the barrier was satisfied by the takeover alone and the arm
// passed without the DISCONNECT ever being processed.
//
// What covers the property instead, and does so on a route that IS reached: the
// packet handler has no session write at all any more, so there is no path by
// which any connection -- displaced or owning -- can move the session's terms
// from the packet. TestMQTTv5DisconnectOverrideParksWithTheObligation is what
// makes that a checked fact rather than an assertion about the source: it holds
// the close in an ambiguous write and requires the session's CONFIRMED terms to
// be unchanged while the parked action carries the override, so re-adding a
// session write to the packet fails it.

// An unresolved lifetime write must PARK, and the parked action must carry the
// override rather than the session's confirmed terms.
//
// The close returning is not the discriminator -- a fall-through returns too --
// so this reads the action off the session. It also pins the deliberate
// disagreement: the session runs on its confirmed terms while the write is
// unproven, and the action records what the client instructed.
func TestMQTTv5DisconnectOverrideParksWithTheObligation(t *testing.T) {
	// NOT parallel: mqttTestSessStoreSuppressV is process-global, so a parallel
	// test suppresses every other session store in the run. parallel_safety_test.go
	// enforces this for the setMQTTTest* setters; the same reasoning applies to a
	// direct write of the flag they guard.
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f041pk", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "f041pk/t", opts: 1}})

	sess := testMQTTGetClient(t, s, "f041pk").mqtt.sess

	// Make the lifetime write AMBIGUOUS: the store is suppressed, so the save
	// neither lands nor definitely fails and the close must park.
	mqttTestSessStoreSuppressV.Store(true)
	// t.Cleanup rather than defer: a t.Fatalf inside checkFor below runs on a
	// different goroutine, where a defer in THIS frame does not fire -- and a
	// leaked suppression flag fails unrelated later tests in ways that read as
	// flakes.
	t.Cleanup(func() { mqttTestSessStoreSuppressV.Store(false) })

	if _, err := testMQTTWrite(c, mqttV5DisconnectSessionExpiry(mqttReasonSuccess, 0)); err != nil {
		t.Fatalf("Error writing DISCONNECT: %v", err)
	}
	c.Close()

	checkFor(t, testMQTTTimeout, 20*time.Millisecond, func() error {
		sess.mu.Lock()
		action := sess.deferredDisconnect
		confirmed := sess.expiryInterval
		sess.mu.Unlock()
		if action == nil {
			return fmt.Errorf("no deferred action parked yet")
		}
		if action.expiryInterval != 0 {
			t.Fatalf("the parked action carries expiryInterval=%d, want the override 0: an early park has silently retained a session the client ended",
				action.expiryInterval)
		}
		if !action.lifetimeOverridden {
			t.Fatal("the parked action is not marked as carrying an override, so the discharge will not make the client's instruction durable")
		}
		if confirmed != 3600 {
			t.Fatalf("the session's confirmed terms moved to %d on an unproven write, want 3600", confirmed)
		}
		return nil
	})
}

// The producer's other arm: a park with NO DISCONNECT override must NOT be
// marked as carrying one.
//
// This is the half a hand-built action cannot test. The discharge tests pin what
// applyDeferredDisconnect DOES with the flag; only driving the real producer
// through a real connection observes who SETS it -- and a mutation marking every
// park override-carrying otherwise passes the entire suite. It matters because
// the discharge would then add a record store to every parked close and let an
// AMBIGUOUS store repark a discard that is otherwise simply attempted.
//
// NOT parallel: it arms the session's uncertainty directly, and the harness's
// server is shared with whatever else is running.
func TestMQTTv5ParkWithoutAnOverrideCarriesNoObligation(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f041noov", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "f041noov/t", opts: 1}})

	sess := testMQTTGetClient(t, s, "f041noov").mqtt.sess

	// An UNRELATED ambiguity, armed directly: with no DISCONNECT override the close
	// issues no lifetime write of its own, so there is nothing to go ambiguous and
	// nothing would park. Arming it is what reaches the entry-uncertainty park --
	// the same route an unrelated in-flight record write would take.
	sess.mu.Lock()
	testMarkUncertainLocked(sess, sess.seq, testAmbiguousBodyLocked(sess, "f041-unrelated-noov"))
	sess.mu.Unlock()
	if !sess.isSeqUncertain() {
		t.Fatal("the session must be unresolved for the close to park, or this fixture produces no action")
	}

	c.Close()

	checkFor(t, testMQTTTimeout, 20*time.Millisecond, func() error {
		sess.mu.Lock()
		action := sess.deferredDisconnect
		confirmed := sess.expiryInterval
		sess.mu.Unlock()
		if action == nil {
			return fmt.Errorf("no deferred action parked yet")
		}
		if action.lifetimeOverridden {
			t.Fatal("a park with no DISCONNECT override is marked as carrying one: the discharge will then write the session's own confirmed terms back, " +
				"adding a store to every parked close and letting an ambiguous store repark a discard that is otherwise attempted")
		}
		// It still carries the session's confirmed terms, which is what sizes its
		// timer and caps its Will -- being unmarked is about the OBLIGATION, not
		// about the interval being absent.
		if action.expiryInterval != confirmed {
			t.Fatalf("the parked action carries expiryInterval=%d, want the confirmed %d", action.expiryInterval, confirmed)
		}
		return nil
	})
}

// The hot path takes no extra store: a close with no override must not open the
// lifetime transaction at all.
//
// It establishes STORE cost only. Proving "no fence wait" needs a contention
// observation this does not attempt, and an unfixed broker satisfies it either
// way -- so this is a regression guard, not evidence for the fix.
func TestMQTTv5CloseWithoutOverrideAddsNoRecordWrite(t *testing.T) {
	// NOT parallel: setMQTTTestSessStoreResultHook installs a process-global hook
	// (see parallel_safety_test.go's isProcessGlobalSetter).
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var stores atomic.Int64
	restore := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess != nil && sess.id == "f041hot" {
			stores.Add(1)
		}
		return resp, err
	})
	defer restore()

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f041hot", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "f041hot/t", opts: 1}})
	afterSetup := stores.Load()

	testMQTTDisconnect(t, c, nil)
	c.Close()
	testMQTTExpectSessionClientGone(t, s, "f041hot")

	// The finite branch still persists its disconnect stamp -- HEAD's own write,
	// expected. What must not appear is a SECOND write before it.
	if got := stores.Load() - afterSetup; got > 1 {
		t.Errorf("a close with no DISCONNECT override performed %d record writes, want at most 1 (the finite disconnect stamp)", got)
	}
}

// DEFECT A, and this is the assertion the whole item is for: an override to zero
// must leave a record a later CONNECT REFUSES to resume.
//
// It needs the discard to fail, and that is not a contrivance -- it is the
// population the defect lives in. docs/CONFORMANCE.md enumerates three paths
// that lose the clean-session discard, and on every one of them the record
// survives the close. With the discard succeeding there is no record left to be
// wrong about, which is why a fixture that lets it succeed can only SKIP: the
// first version of this test did exactly that and proved nothing.
//
// NOT parallel: setMQTTTestClearWindowHook is a process-global setter (see
// parallel_safety_test.go's isProcessGlobalSetter).
func TestMQTTv5DisconnectOverrideToZeroSurvivesARefusedDiscard(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	probe, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f041zprobe", cleanStart: true},
		o.MQTT.Host, o.MQTT.Port)
	defer probe.Close()
	testMQTTReadConnAckV5(t, pr)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f041z", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "f041z/t", opts: 1}})

	sess := testMQTTGetClient(t, s, "f041z").mqtt.sess
	if sess == nil {
		t.Fatal("no session for f041z")
	}

	// Arm ONE ambiguity inside clear()'s pre-fence window, so the discard refuses
	// with errMQTTClearRefusedUnresolved and deletes nothing. The lifetime write
	// this test is about has already happened by then: it runs before the
	// never-expire block and long before the discard.
	armed := testArmAmbiguityAtClearPreFence(t, sess)

	if _, err := testMQTTWrite(c, mqttV5DisconnectSessionExpiry(mqttReasonSuccess, 0)); err != nil {
		t.Fatalf("Error writing DISCONNECT: %v", err)
	}
	c.Close()
	testMQTTExpectSessionClientGone(t, s, "f041z")

	if !armed() {
		t.Fatal("the clear() pre-fence window was never reached, so the discard was not refused and this fixture did not exercise its route")
	}

	ps, _, ok := testMQTTLoadSessionRecord(t, s, "f041zprobe", "f041z")
	if !ok {
		t.Fatal("the refused discard deleted the record anyway; there is nothing left to assert on")
	}
	// The pair, together. Clean is what the resume decision reads; the interval
	// alone would leave the record resumable AND unreapable.
	if !ps.Clean {
		t.Errorf("record says Clean=false after a DISCONNECT override to 0: a later CONNECT resumes a session the DISCONNECT ended (Spec5 [3.1.2.11])")
	}
	if ps.ExpiryInterval != 0 {
		t.Errorf("record says ExpiryInterval=%d after a DISCONNECT override to 0, want 0", ps.ExpiryInterval)
	}

	// And the behaviour, not just the bytes: a reconnect must not resume it.
	c2, r2 := testMQTTConnectRetryV5(t, &mqttV5ConnInfo{clientID: "f041z"}, o.MQTT.Host, o.MQTT.Port, 3)
	defer c2.Close()
	if sp, _, _ := testMQTTReadConnAckV5(t, r2); sp {
		t.Fatal("expected session-present=false: the DISCONNECT ended this session, and a refused discard must not make it resumable")
	}
}

// An ORDERING fact, and nothing more: the lifetime update must run BEFORE the
// never-expire stamp block, because for a finite -> never-expire override the
// update is precisely what makes that block apply at all.
//
// The block reads sess.expiryInterval to decide whether this session makes no
// disconnect save and therefore needs an in-memory stamp of its own. Run the
// update after it and the block sees the pre-override FINITE interval, skips,
// and the session is left with no stamp -- so a later record write publishes
// DisconnectedAt=0, which the delayed-Will record fence reads as a live newer
// connection.
//
// No Will-fence consequence is claimed here. The comment justifying that stamp
// cites a mechanism mqttConsumeWillUnderGate says it does not use, and this item
// deliberately rests nothing on it (see the roadmap bullet); the assertion is the
// stamp's presence and the order that produces it.
func TestMQTTv5DisconnectRaiseToNeverExpireStampsInMemory(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f041ord", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "f041ord/t", opts: 1}})

	// Captured before the close: the session object survives (never-expire), but
	// the client does not, so it cannot be looked up afterwards.
	sess := testMQTTGetClient(t, s, "f041ord").mqtt.sess
	if sess == nil {
		t.Fatal("no session for f041ord")
	}
	sess.mu.Lock()
	before := sess.disconnectedAt
	sess.mu.Unlock()
	if before != 0 {
		t.Fatalf("session already carries disconnectedAt=%d before its close, so this fixture cannot attribute the stamp", before)
	}

	if _, err := testMQTTWrite(c, mqttV5DisconnectSessionExpiry(mqttReasonSuccess, mqttSessionNeverExpire)); err != nil {
		t.Fatalf("Error writing DISCONNECT: %v", err)
	}
	c.Close()
	testMQTTExpectSessionClientGone(t, s, "f041ord")

	sess.mu.Lock()
	stamp, interval := sess.disconnectedAt, sess.expiryInterval
	sess.mu.Unlock()
	if interval != mqttSessionNeverExpire {
		t.Fatalf("session expiryInterval = %d, want never-expire: without the update applied this test is not about ordering", interval)
	}
	if stamp == 0 {
		t.Error("the never-expire stamp block left no in-memory disconnect time: the lifetime update ran AFTER it, so the block saw the pre-override finite interval and skipped")
	}
}
