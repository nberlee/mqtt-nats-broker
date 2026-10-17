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

//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// The PUBCOMP arm across a restart, on a real server: what recovery concludes about an ordinary
// exchange from its durable records, measured where it matters — a Receive Maximum 1 client either
// receives its next message or, when the identifier must stay reserved, does not.

type testF105RestartWorld struct {
	o       *Options
	s       *Server
	ci      *mqttV5ConnInfo
	sub     net.Conn
	subr    *mqttReader
	asm     *mqttAccountSessionManager
	sess    *mqttSession
	pi      uint16
	lin     string
	subject string
	pubN    int
}

// testF105RestartWorldAtPubRelReady connects a Receive Maximum 1 v5 subscriber, delivers one
// ordinary QoS 2 message, answers PUBREC, reads the PUBREL, and waits for the slot to stand at
// pubrel_ready with no action.
func testF105RestartWorldAtPubRelReady(t *testing.T, clientID string) *testF105RestartWorld {
	t.Helper()
	w := &testF105RestartWorld{o: testMQTTDefaultOptionsV5()}
	w.s = testMQTTRunServer(t, w.o)
	props := append(mqttV5ConnPropsReceiveMax(1), mqttV5ConnPropsSessionExpiry(3600)...)
	w.ci = &mqttV5ConnInfo{clientID: clientID, props: props}
	w.sub, w.subr = testMQTTConnectV5(t, w.ci, w.o.MQTT.Host, w.o.MQTT.Port)
	testMQTTReadConnAckV5(t, w.subr)
	testMQTTSubV5(t, w.sub, w.subr, 1, []mqttV5SubFilter{{topic: "foo", opts: 2}})
	w.bind(t)
	w.publish(t, "first")
	_, pi := testMQTTReadPublishV5(t, w.subr, "foo", []byte("first"))
	w.pi = pi
	testMQTTSendPIPacket(mqttPacketPubRec, t, w.sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, w.subr, pi)
	w.sess.mu.Lock()
	w.subject = mqttInflightSubject(w.sess.idHash, w.sess.gen, pi)
	w.sess.mu.Unlock()
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		cur, _, err := w.asm.loadInflightRecord(w.subject)
		if err != nil || cur == nil || cur.State != mqttInflightPubRelReady || cur.ActionToken != _EMPTY_ {
			return fmt.Errorf("slot %+v (%v)", cur, err)
		}
		w.lin = cur.Lin
		return nil
	})
	return w
}

func (w *testF105RestartWorld) bind(t *testing.T) {
	t.Helper()
	w.asm = testMQTTGetAccountSessionManager(t, w.s, w.ci.clientID)
	w.asm.mu.RLock()
	w.sess = w.asm.sessions[w.ci.clientID]
	w.asm.mu.RUnlock()
	if w.sess == nil {
		t.Fatal("no resident session")
	}
}

func (w *testF105RestartWorld) publish(t *testing.T, payload string) {
	t.Helper()
	w.pubN++
	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: fmt.Sprintf("%s-pub-%d", w.ci.clientID, w.pubN), cleanStart: true},
		w.o.MQTT.Host, w.o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 2, false, 1, "foo", []byte(payload), nil)
}

// restart shuts the server down cleanly, restarts it on the same store, and resumes the session.
func (w *testF105RestartWorld) restart(t *testing.T) {
	t.Helper()
	w.sub.Close()
	w.s = testInflightRestart(t, w.s, w.o)
	t.Cleanup(func() { testMQTTShutdownServer(w.s) })
	w.sub, w.subr = testMQTTConnectV5(t, w.ci, w.o.MQTT.Host, w.o.MQTT.Port)
	t.Cleanup(func() { w.sub.Close() })
	if present, _, _ := testMQTTReadConnAckV5(t, w.subr); !present {
		t.Fatal("the session must be resumed")
	}
	w.bind(t)
}

func (w *testF105RestartWorld) slot(t *testing.T) *mqttInflightRecord {
	t.Helper()
	cur, smsg, err := w.asm.loadInflightRecord(w.subject)
	if err != nil {
		t.Fatalf("reading the slot: %v", err)
	}
	if smsg == nil {
		return nil
	}
	return cur
}

func (w *testF105RestartWorld) phase(t *testing.T) mqttExchangePhase {
	t.Helper()
	w.sess.mu.Lock()
	gen := w.sess.gen
	w.sess.mu.Unlock()
	x, _, err := w.asm.jsa.loadOutboundExchange(w.sess.id, w.asm.sessStreamEpoch, gen, w.lin)
	if err != nil || x == nil {
		t.Fatalf("reading the exchange: %+v (%v)", x, err)
	}
	return x.Phase
}

// nextDelivered publishes a fresh message and reports whether the Receive Maximum 1 client
// receives it within the window.
func (w *testF105RestartWorld) nextDelivered(t *testing.T) bool {
	t.Helper()
	w.publish(t, "next")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b, pl, ok := testMQTTReadPacketReady(w.subr, time.Until(deadline))
		if !ok {
			return false
		}
		// The only message published in this window is "next", so any PUBLISH is it (a v5
		// PUBLISH carries properties the v3 parser does not read, so the payload is not compared).
		if b&mqttPacketMask == mqttPacketPub {
			w.subr.pos += pl
			return true
		}
		w.subr.pos += pl
	}
	return false
}

// T10. An ordinary pubrel_ready slot whose canonical record vanished is NOT completed by recovery:
// "no record" is no proof of a PUBCOMP. The identifier stays reserved and the exchange still owes
// its release (the stated residual).
//
// Mutation M8: the absence arm left in force for ordinary slots -> the slot is finalized and the
// next message is delivered. M29: every ordinary ready slot treated as finished -> the same.
func TestF105RecoveryAbsenceDoesNotEndAnOrdinaryExchange(t *testing.T) {
	t.Parallel()
	w := testF105RestartWorldAtPubRelReady(t, "f105-absence")
	rec := w.slot(t)
	if err := w.asm.jsa.deleteMsg(mqttOutStreamName, rec.PubRelStreamSeq, true); err != nil {
		t.Fatalf("deleting the canonical PUBREL: %v", err)
	}
	w.restart(t)
	if slot := w.slot(t); slot == nil || slot.State != mqttInflightPubRelReady {
		t.Fatalf("recovery completed the exchange on an absence: slot %+v", slot)
	}
	if got := w.phase(t); got != mqttExchangePubRecObserved {
		t.Fatalf("the exchange stands at phase %d, want PUBREC-observed (release owed)", got)
	}
	if w.nextDelivered(t) {
		t.Fatal("the identifier was released on an absence: the next message reached the Receive Maximum 1 client")
	}
}

// T16. A completed ordinary exchange whose pre-PUBCOMP slot version the store revives
// (MAXMSGSPER-REVIVAL) is finished by recovery on its exchange's word, restoring capacity.
// T16b/T16c: the same revival beside a pending-drain exchange, or with no matching exchange,
// stays reserved.
//
// Mutation M28: accept TerminalPendingDrain as completion -> T16b's next message is delivered.
func TestF105RecoveryFinishesARevivedSlotBesideATerminalExchange(t *testing.T) {
	t.Run("beside Terminal: finished", func(t *testing.T) {
		t.Parallel()
		w := testF105RestartWorldAtPubRelReady(t, "f105-revived-terminal")
		revived := *w.slot(t)
		testMQTTSendPIPacket(mqttPacketPubComp, t, w.sub, w.pi)
		testMQTTFlush(t, w.sub, nil, w.subr)
		checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
			if slot := w.slot(t); slot != nil {
				return fmt.Errorf("the slot still stands")
			}
			return nil
		})
		if got := w.phase(t); got != mqttExchangeTerminal {
			t.Fatalf("the completed exchange stands at phase %d", got)
		}
		// THE REVIVAL, in the witnessed shape: the pre-PUBCOMP pubrel_ready version is back.
		if _, err := w.asm.jsa.storeInflightCAS(w.subject, 0, &revived); err != nil {
			t.Fatalf("reviving the slot: %v", err)
		}
		w.restart(t)
		checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
			if slot := w.slot(t); slot != nil {
				return fmt.Errorf("the revived slot stands at %q", slot.State)
			}
			return nil
		})
		if !w.nextDelivered(t) {
			t.Fatal("the Receive Maximum 1 client did not receive its next message after the revived slot was finished")
		}
	})
	t.Run("beside terminal-pending-drain: reserved", func(t *testing.T) {
		t.Parallel()
		w := testF105RestartWorldAtPubRelReady(t, "f105-revived-pending")
		revived := *w.slot(t)
		cur, smsg, _ := w.asm.loadInflightRecord(w.subject)
		w.sess.mu.Lock()
		c := w.sess.c
		w.sess.mu.Unlock()
		if w.sess.testAcquirePubRel(c, *mqttSlotRefFromRecord(w.subject, smsg.Sequence, cur)) == nil {
			t.Fatal("no PUBREL action")
		}
		testMQTTSendPIPacket(mqttPacketPubComp, t, w.sub, w.pi)
		testMQTTFlush(t, w.sub, nil, w.subr)
		checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
			if got := w.phase(t); got != mqttExchangeTerminalPendingDrain {
				return fmt.Errorf("phase %d", got)
			}
			return nil
		})
		cur, smsg, _ = w.asm.loadInflightRecord(w.subject)
		if _, err := w.asm.jsa.storeInflightCAS(w.subject, smsg.Sequence, &revived); err != nil {
			t.Fatalf("reviving the pre-acquisition slot: %v", err)
		}
		w.restart(t)
		if slot := w.slot(t); slot == nil || slot.State != mqttInflightPubRelReady {
			t.Fatalf("a revived slot beside a pending drain was finished: %+v", slot)
		}
		if w.nextDelivered(t) {
			t.Fatal("the identifier was released beside a pending drain")
		}
	})
	t.Run("no matching exchange: reserved", func(t *testing.T) {
		t.Parallel()
		w := testF105RestartWorldAtPubRelReady(t, "f105-revived-foreign")
		foreign := *w.slot(t)
		foreign.Lin = mqttNewInflightNonce()
		cur, smsg, _ := w.asm.loadInflightRecord(w.subject)
		_ = cur
		if _, err := w.asm.jsa.storeInflightCAS(w.subject, smsg.Sequence, &foreign); err != nil {
			t.Fatalf("installing the foreign-lineage slot: %v", err)
		}
		w.restart(t)
		if slot := w.slot(t); slot == nil || slot.Lin != foreign.Lin {
			t.Fatalf("a slot with no matching exchange was finished or rewritten: %+v", slot)
		}
	})
}

// T5c / T5g(c). A PUBCOMP whose batch LANDED behind a lost final response and a failed resolving
// read is unconfirmed: the ack is withheld. After a restart recovery finds the terminal slot beside
// its Terminal exchange, finalizes it, disposes of the unacked record, and re-sends nothing.
func TestF105UnconfirmedButLandedPubCompIsFinishedByRecovery(t *testing.T) {
	t.Parallel()
	w := testF105RestartWorldAtPubRelReady(t, "f105-landed-unconfirmed")
	var calls atomic.Int32
	var failLoad, loadFailed atomic.Bool
	var acks atomic.Int32
	w.asm.mu.Lock()
	w.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if !testF105IsPubCompAdvance(req, w.lin) {
			return write()
		}
		switch calls.Add(1) {
		case 1, 2:
			return mqttOpOutcome{}, errors.New("test: refused")
		case 3:
			if _, err := write(); err != nil {
				t.Errorf("the final attempt's write failed: %v", err)
			}
			failLoad.Store(true)
			return mqttOpOutcome{}, errors.New("test: response lost")
		}
		return mqttOpOutcome{}, errors.New("test: refused")
	}
	w.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if subject == w.subject && failLoad.CompareAndSwap(true, false) {
			loadFailed.Store(true)
			return nil, nil, errors.New("test: the resolving read fails")
		}
		return w.asm.jsa.loadInflightRecord(subject)
	}
	w.asm.testPubCompAck = func(string, bool) { acks.Add(1) }
	w.asm.mu.Unlock()

	testMQTTSendPIPacket(mqttPacketPubComp, t, w.sub, w.pi)
	testMQTTFlush(t, w.sub, nil, w.subr)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if !loadFailed.Load() {
			return fmt.Errorf("the resolving read has not run")
		}
		return nil
	})
	if n := acks.Load(); n != 0 {
		t.Fatalf("an unconfirmed completion sent %d acks", n)
	}
	if slot := w.slot(t); slot == nil || slot.State != mqttInflightPubRelTerminal {
		t.Fatalf("the landed batch did not leave the slot terminal: %+v", slot)
	}
	w.restart(t)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if slot := w.slot(t); slot != nil {
			return fmt.Errorf("the terminal slot stands")
		}
		return nil
	})
	if !w.nextDelivered(t) {
		t.Fatal("the Receive Maximum 1 client did not receive its next message")
	}
}

// T5g(b). A RECORDED completion whose cleanup its worker still owns at shutdown is finished by the
// next recovery from the terminal slot.
func TestF105RecordedCleanupOwnedAtShutdownIsFinishedByRecovery(t *testing.T) {
	t.Parallel()
	w := testF105RestartWorldAtPubRelReady(t, "f105-recorded-at-shutdown")
	var enrolled atomic.Int32
	w.asm.mu.Lock()
	w.asm.testInflightPubRelScan = func(string) ([]mqttStoredPubRel, int, bool) { return nil, 0, false }
	w.asm.testPubCompEnrolled = func(uint16, bool) { enrolled.Add(1) }
	w.asm.mu.Unlock()
	testMQTTSendPIPacket(mqttPacketPubComp, t, w.sub, w.pi)
	testMQTTFlush(t, w.sub, nil, w.subr)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if enrolled.Load() == 0 {
			return fmt.Errorf("no cleanup owner enrolled")
		}
		return nil
	})
	if got := w.phase(t); got != mqttExchangeTerminal {
		t.Fatalf("the completion was not recorded: phase %d", got)
	}
	w.restart(t)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if slot := w.slot(t); slot != nil {
			return fmt.Errorf("the terminal slot stands")
		}
		return nil
	})
	if !w.nextDelivered(t) {
		t.Fatal("the Receive Maximum 1 client did not receive its next message")
	}
}

// T7. The stated residual, NARROWED by F-105 8c to a holder whose connection left no usable
// durable certificate: a PUBREL holder that never drains (its process gone) leaves its exchange
// terminal-pending-drain and its identifier held; recovery neither finishes nor steals it. The
// certificate the restart's teardown would persist is suppressed, which is the crash before
// persistence; the certified population is T7b.
//
// Not parallel: the persistence observer is process-wide.
func TestF105DeadPubRelHolderLeavesItsDrainOwed(t *testing.T) {
	w := testF105RestartWorldAtPubRelReady(t, "f105-dead-holder")
	w.sess.mu.Lock()
	holderBind := w.sess.c.outBind
	w.sess.mu.Unlock()
	testObserveCertPersist(t, holderBind, false)
	cur, smsg, _ := w.asm.loadInflightRecord(w.subject)
	w.sess.mu.Lock()
	c := w.sess.c
	w.sess.mu.Unlock()
	act := w.sess.testAcquirePubRel(c, *mqttSlotRefFromRecord(w.subject, smsg.Sequence, cur))
	if act == nil {
		t.Fatal("no PUBREL action")
	}
	testMQTTSendPIPacket(mqttPacketPubComp, t, w.sub, w.pi)
	testMQTTFlush(t, w.sub, nil, w.subr)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if got := w.phase(t); got != mqttExchangeTerminalPendingDrain {
			return fmt.Errorf("phase %d", got)
		}
		return nil
	})
	// The holder dies without draining: its local hold is abandoned as a process loss would.
	w.sess.mu.Lock()
	act.drained = true
	w.sess.mu.Unlock()
	w.restart(t)
	if got := w.phase(t); got != mqttExchangeTerminalPendingDrain {
		t.Fatalf("recovery moved a dead holder's exchange to phase %d", got)
	}
	if slot := w.slot(t); slot == nil || slot.ActionToken != act.token {
		t.Fatalf("recovery took the dead holder's slot: %+v", slot)
	}
	w.sess.mu.Lock()
	gen := w.sess.gen
	w.sess.mu.Unlock()
	if v := w.sess.jsa.classifyOutstandingDuty(w.sess.id, w.asm.sessStreamEpoch, gen,
		&mqttDutyEntry{Kind: mqttDutyExchange, Ref: w.lin}); v.Disposition != mqttDutyOwesWork || v.Work != mqttDutyWorkDrain {
		t.Fatalf("the verdict is %+v, want the drain owed", v)
	}
}

// T7b. A dead PUBREL holder whose connection WAS certified, and whose certificate is durable,
// leaves nothing owed: the successor's resume clears the token on the certificate's licence and
// completes the pending drain in the same batch (F-105 8c).
func TestF105CertifiedDeadPubRelHolderIsFinishedByRecovery(t *testing.T) {
	w := testF105RestartWorldAtPubRelReady(t, "f105-dead-certified")
	cur, smsg, _ := w.asm.loadInflightRecord(w.subject)
	w.sess.mu.Lock()
	c := w.sess.c
	holderBind := c.outBind
	w.sess.mu.Unlock()
	act := w.sess.testAcquirePubRel(c, *mqttSlotRefFromRecord(w.subject, smsg.Sequence, cur))
	if act == nil {
		t.Fatal("no PUBREL action")
	}
	testMQTTSendPIPacket(mqttPacketPubComp, t, w.sub, w.pi)
	testMQTTFlush(t, w.sub, nil, w.subr)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if got := w.phase(t); got != mqttExchangeTerminalPendingDrain {
			return fmt.Errorf("phase %d", got)
		}
		return nil
	})
	got := testObserveCertPersist(t, holderBind, true)
	w.sess.mu.Lock()
	act.drained = true
	w.sess.mu.Unlock()
	w.restart(t)
	testAwaitCertPersist(t, got, "the restart's teardown of the PUBREL holder's connection")
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if got := w.phase(t); got != mqttExchangeTerminal {
			return fmt.Errorf("phase %d, want terminal", got)
		}
		if slot := w.slot(t); slot != nil && slot.ActionToken == act.token {
			return fmt.Errorf("the certified holder's token still stands")
		}
		return nil
	})
}
