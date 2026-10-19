//go:build mqttport_fulltests

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

package mqtt

import (
	"encoding/json"
	"errors"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Retained QoS 1/2 through the manifest, end to end through real clients (F-105 7c).

// testRetainedSubscriber publishes one retained message, connects a persistent subscriber and
// returns everything a test needs to inspect its retained exchange.
type testRetainedRig struct {
	s       *Server
	o       *Options
	sess    *mqttSession
	asm     *mqttAccountSessionManager
	epoch   string
	idHash  string
	gen     string
	id      string
	cleanup []func()
}

func (r *testRetainedRig) done() {
	for i := len(r.cleanup) - 1; i >= 0; i-- {
		r.cleanup[i]()
	}
}

func testRetainedRigUp(t *testing.T, id, topic string, pubQoS byte, payload []byte) *testRetainedRig {
	t.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	r := &testRetainedRig{s: s, o: o, id: id}
	r.cleanup = append(r.cleanup, func() { testMQTTShutdownServer(s) })
	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: id + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	r.cleanup = append(r.cleanup, func() { pub.Close() })
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, pubQoS, false, true, topic, 1, payload)
	testMQTTFlush(t, pub, nil, pubr)
	return r
}

func (r *testRetainedRig) attach(t *testing.T) {
	t.Helper()
	r.asm = testMQTTGetAccountSessionManager(t, r.s, r.id)
	r.asm.mu.RLock()
	r.sess = r.asm.sessions[r.id]
	r.epoch = r.asm.sessStreamEpoch
	r.asm.mu.RUnlock()
	if r.sess == nil {
		t.Fatalf("no resident session for %q", r.id)
	}
	r.sess.mu.Lock()
	r.idHash, r.gen = r.sess.idHash, r.sess.gen
	r.sess.mu.Unlock()
}

// TestF105RetainedQoS1PubAckReclaimsTheCopy: after the subscriber acknowledges a retained QoS 1
// delivery, the exchange is Terminal, the register goes, the identifier and the ledger charge are
// released, and the copy is reclaimed — chunks and manifest removed, the owner CleanupComplete and
// both of its duties terminal.
func TestF105RetainedQoS1PubAckReclaimsTheCopy(t *testing.T) {
	r := testRetainedRigUp(t, "ret-puback", "rt/a", 1, []byte("retained-a"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/a", qos: 1}}, []byte{1})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/a", mqttPubQos1|mqttPubFlagRetain, []byte("retained-a"))
	r.attach(t)
	subject := mqttInflightSubject(r.idHash, r.gen, pi)
	reg, _, err := r.asm.loadInflightRecord(subject)
	if err != nil || reg == nil {
		t.Fatalf("no retained register before the PUBACK: %v", err)
	}
	m, _, merr := r.asm.jsa.loadRetainedManifest(r.id, r.epoch, r.gen, reg.Payload)
	if merr != nil {
		t.Fatalf("no copy before the PUBACK: %v", merr)
	}

	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)

	deadline := time.Now().Add(10 * time.Second)
	for {
		x, _, _ := r.asm.jsa.loadOutboundExchange(r.id, r.epoch, r.gen, reg.Lin)
		res, _, _ := r.asm.jsa.loadOutboundResource(r.id, r.epoch, r.gen, m.Inc)
		_, gone := r.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttManifestSubject(r.idHash, r.gen, reg.Payload))
		cur, _, _ := r.asm.loadInflightRecord(subject)
		r.sess.mu.Lock()
		_, held := r.sess.pendingPublish[pi]
		charged := r.sess.custody.holdsPI(pi)
		r.sess.mu.Unlock()
		done := x != nil && x.Phase == mqttExchangeTerminal && cur == nil && !held && !charged &&
			res != nil && res.State == mqttResourceCleanupComplete && gone != nil
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the PUBACK: exchange %+v, register %+v, entry held %v, charged %v, owner %+v, manifest gone %v",
				x, cur, held, charged, res, gone != nil)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, kind := range []mqttDutyKind{mqttDutyProvision, mqttDutyCleanup} {
		ord, oerr := r.asm.jsa.resourceDutyOrdinal(r.id, r.epoch, r.gen, m.Inc, kind)
		if oerr != nil {
			t.Fatalf("the copy's %q duty: %v", kind, oerr)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			d, _, _ := r.asm.jsa.loadDutyEntryAt(r.id, r.epoch, r.gen, ord)
			if d != nil && d.State == mqttDutyTerminal {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the copy's %q duty %d is %+v, want terminal", kind, ord, d)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if n := mqttRetainedAtOrdinarySink.Load(); n != 0 {
		t.Fatalf("%d retained register(s) reached an ordinary-source sink", n)
	}
}

// testAnswerSourcelessAsUnknown installs asm's source-state seam and returns a count of the
// classifications it was asked for with no source coordinates. Every answer is Unknown — what a
// server that never replies to an empty-name consumer lookup yields — so whatever the test
// server would answer, a stray source owner cannot settle and leave the test blind to it. The
// sessions under test hold no ordinary deliveries, so no legitimate classification is starved of
// a real answer.
func testAnswerSourcelessAsUnknown(t *testing.T, as *mqttAccountSessionManager) *atomic.Int64 {
	t.Helper()
	var sourceless atomic.Int64
	as.mu.Lock()
	as.testInflightSourceState = func(r *mqttInflightRecord) mqttInflightSourceState {
		if r.Dur == _EMPTY_ || r.Seq == 0 {
			sourceless.Add(1)
		}
		return mqttInflightSourceUnknown
	}
	as.mu.Unlock()
	t.Cleanup(func() {
		as.mu.Lock()
		as.testInflightSourceState = nil
		as.mu.Unlock()
	})
	return &sourceless
}

// testAwaitNoSourceOwner waits for the acknowledged retained exchange on pi to leave nothing
// behind in the session's terminal machinery: no source owner and no retry for its subject, and
// no terminal hold on its identifier (sendsInFlight, distinct from the custody charge).
func testAwaitNoSourceOwner(t *testing.T, sess *mqttSession, subject string, pi uint16) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		sess.mu.Lock()
		var owners, retries int
		for k := range sess.terminalSources {
			if k.subject == subject {
				owners++
			}
		}
		for k := range sess.terminalRetries {
			if k.source.subject == subject {
				retries++
			}
		}
		h := sess.sendsInFlight[pi]
		holdOwed := h != nil && h.terminalOwed != _EMPTY_
		sess.mu.Unlock()
		if owners == 0 && retries == 0 && !holdOwed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the acknowledgement of retained %q: %d source owner(s), %d terminal retr(ies), identifier %d terminal hold owed %v",
				subject, owners, retries, pi, holdOwed)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestF105RetainedPubAckInstallsNoSourceOwner: a retained QoS 1 exchange's PUBACK settles no
// source (F-105 7c), so it installs no ordinary source owner. One that it did install asked a
// consumer lookup with an empty name, which a server with permissions never answers: the owner
// never settled, its retry announced foreground authority for the full API timeout on every
// attempt, the identifier stayed held, and the session's retained replay starved behind it — the
// Paho test_subscribe_options failure. Re-SUBSCRIBEs must keep replaying the retained message.
func TestF105RetainedPubAckInstallsNoSourceOwner(t *testing.T) {
	r := testRetainedRigUp(t, "ret-puback-noowner", "rt/n", 1, []byte("retained-n"))
	defer r.done()
	before := mqttRetainedAtOrdinarySink.Load()
	// The account's manager, reached through the publisher: the seam is installed before the
	// subscriber exists, so no classification of its exchanges escapes it.
	sourceless := testAnswerSourcelessAsUnknown(t, testMQTTGetAccountSessionManager(t, r.s, r.id+"-pub"))

	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	for round := 0; round < 4; round++ {
		testMQTTSub(t, uint16(round+1), sub, subr, []*mqttFilter{{filter: "rt/n", qos: 1}}, []byte{1})
		pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/n", mqttPubQos1|mqttPubFlagRetain, []byte("retained-n"))
		if round == 0 {
			r.attach(t)
		}
		subject := mqttInflightSubject(r.idHash, r.gen, pi)
		reg, _, err := r.asm.loadInflightRecord(subject)
		if err != nil || reg == nil {
			t.Fatalf("round %d: no retained register before the PUBACK: %v", round, err)
		}
		testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
		// The PUBACK must have been PROCESSED before its absence of an owner means anything: any
		// owner it installs is installed with the untrack, before the exchange ends and the
		// register goes.
		deadline := time.Now().Add(10 * time.Second)
		for {
			x, _, _ := r.asm.jsa.loadOutboundExchange(r.id, r.epoch, r.gen, reg.Lin)
			cur, _, _ := r.asm.loadInflightRecord(subject)
			if x != nil && x.Phase == mqttExchangeTerminal && cur == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("round %d: the PUBACK did not end the exchange: %+v, register %+v", round, x, cur)
			}
			time.Sleep(50 * time.Millisecond)
		}
		testAwaitNoSourceOwner(t, r.sess, subject, pi)
	}
	if n := sourceless.Load(); n != 0 {
		t.Fatalf("%d source classification(s) were asked with no source coordinates", n)
	}
	if n := mqttRetainedAtOrdinarySink.Load() - before; n != 0 {
		t.Fatalf("%d sourceless input(s) reached an ordinary-source sink", n)
	}
}

// TestF105RetainedReclaimStoreFailureKeepsItsOwner (consult 106): a reclamation step that fails
// on the STORE — here the purge's chunk delete, refused past the withheld bound — says nothing
// about the copy, so the reclaimer keeps the copy and finishes it once the store answers; only
// the close boundary's positive "the exchange may still need it" lets a key go.
func TestF105RetainedReclaimStoreFailureKeepsItsOwner(t *testing.T) {
	r := testRetainedRigUp(t, "ret-reclaim-fail", "rt/rf", 1, []byte("retained-rf"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/rf", qos: 1}}, []byte{1})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/rf", mqttPubQos1|mqttPubFlagRetain, []byte("retained-rf"))
	r.attach(t)
	reg, _, err := r.asm.loadInflightRecord(mqttInflightSubject(r.idHash, r.gen, pi))
	if err != nil || reg == nil {
		t.Fatalf("no retained register: %v", err)
	}
	m, _, merr := r.asm.jsa.loadRetainedManifest(r.id, r.epoch, r.gen, reg.Payload)
	if merr != nil {
		t.Fatalf("no copy: %v", merr)
	}
	chunk, cerr := r.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttChunkSubject(r.idHash, r.gen, reg.Payload, 1))
	if cerr != nil || chunk == nil {
		t.Fatalf("no chunk: %v", cerr)
	}
	// One more refusal than the withheld bound: a key dropped at the bound is never finished.
	var refused atomic.Int32
	restore := setMQTTTestDeleteMsgHook(func(stream string, seq uint64) error {
		if stream == mqttInflightStreamName && seq == chunk.Sequence && refused.Add(1) <= mqttRetainedStepAttempts+1 {
			return errors.New("the chunk delete was refused")
		}
		return nil
	})
	defer restore()
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
	deadline := time.Now().Add(60 * time.Second)
	for {
		if refused.Load() > 0 && !testRetainedMaterialOwned(r, m.Inc) {
			t.Fatal("a copy whose reclamation failed on the store has no owner")
		}
		res, _, _ := r.asm.jsa.loadOutboundResource(r.id, r.epoch, r.gen, m.Inc)
		if res != nil && res.State == mqttResourceCleanupComplete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the copy was never reclaimed after %d refused deletes: owner %+v", refused.Load(), res)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := refused.Load(); n <= mqttRetainedStepAttempts {
		t.Fatalf("only %d chunk deletes were refused: the fixture never crossed the withheld bound", n)
	}
}

// TestF105RetainedRecoveryReadFailureRefusesTheResume (holistic H-4 f1, consult 106): a resume whose
// read of a retained exchange's canonical PUBREL FAILS (the store did not answer) must not
// quarantine the exchange — a quarantine owns nothing, and the connected client would be owed its
// PUBREL forever. The resume is refused before CONNACK instead, and the reconnect classifies again
// with the records intact and replays the PUBREL.
func TestF105RetainedRecoveryReadFailureRefusesTheResume(t *testing.T) {
	for _, tc := range []struct {
		name string
		qos  byte
	}{{"canonical-read", 2}, {"chunk-read", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			defer func() { testMQTTShutdownServer(s) }()
			cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-rfail-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, rp)
			testMQTTPubV5Props(t, cp, rp, tc.qos, true, 1, "rf/t", []byte("owed"), nil)
			cp.Close()
			ci := &mqttV5ConnInfo{clientID: "ret-rfail", cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
			c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, r)
			testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "rf/t", opts: tc.qos}})
			_, pi := testMQTTReadPublishV5(t, r, "rf/t", []byte("owed"))
			if tc.qos == 2 {
				testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
				testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
			}
			rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
			rig.attach(t)
			reg, _, _ := rig.asm.loadInflightRecord(mqttInflightSubject(rig.idHash, rig.gen, pi))
			if reg == nil {
				t.Fatal("no retained register")
			}
			target := mqttChunkSubject(rig.idHash, rig.gen, reg.Payload, 1)
			if tc.qos == 2 {
				if reg.State != mqttInflightPubRelReady || reg.RelSubj == _EMPTY_ {
					t.Fatalf("the exchange is not at pubrel_ready with its canonical: %+v", reg)
				}
				target = reg.RelSubj
			}
			c.Close()

			var failed atomic.Int32
			restore := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
				if subject == target && failed.Add(1) == 1 {
					return errors.New("the read failed")
				}
				return nil
			})
			defer restore()
			ci.cleanStart = false
			checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
			c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			if _, rc, _ := testMQTTReadConnAckV5(t, r); rc == mqttConnAckRCConnectionAccepted {
				c.Close()
				t.Fatal("the resume was accepted over a read that failed: the exchange was withheld unowned")
			}
			c.Close()
			if failed.Load() < 1 {
				t.Fatal("the fixture never failed the read")
			}
			checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
			c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
				t.Fatalf("the reconnect: rc=%x present=%v", rc, present)
			}
			if tc.qos == 2 {
				testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
				testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
				return
			}
			_, got := testMQTTReadPublishV5(t, r, "rf/t", []byte("owed"))
			if got != pi {
				t.Fatalf("the reconnect replayed pi %d, want %d", got, pi)
			}
			testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
		})
	}
}

// TestF105RetainedRecoveryRereadFailureRefusesTheResume (holistic H-5): the classification's
// consistency re-reads decide nothing when they go unanswered, and a walk whose passes run out
// with a retained exchange outstanding installed nothing — so both refuse the resume before
// CONNACK rather than accept it with the exchange owned by nothing; the reconnect replays it.
// (a) the register re-read fails on every pass; (b) the exchange keeps moving (re-stored under a
// new sequence on every pass) until the passes are exhausted.
func TestF105RetainedRecoveryRereadFailureRefusesTheResume(t *testing.T) {
	for _, tc := range []struct {
		name  string
		moves bool
	}{{"reread-unanswered", false}, {"passes-exhausted", true}} {
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			defer func() { testMQTTShutdownServer(s) }()
			cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-reread-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, rp)
			testMQTTPubV5Props(t, cp, rp, 1, true, 1, "rr/t", []byte("owed"), nil)
			cp.Close()
			ci := &mqttV5ConnInfo{clientID: "ret-reread", cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
			c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, r)
			testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "rr/t", opts: 1}})
			_, pi := testMQTTReadPublishV5(t, r, "rr/t", []byte("owed"))
			rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
			rig.attach(t)
			subject := mqttInflightSubject(rig.idHash, rig.gen, pi)
			reg, _, _ := rig.asm.loadInflightRecord(subject)
			if reg == nil {
				t.Fatal("no retained register")
			}
			c.Close()

			var passes atomic.Int32
			var failNext atomic.Bool
			as := rig.asm
			xsubj := mqttExchangeSubject(rig.idHash, rig.gen, reg.Lin)
			classify := func(subj string) {
				if subj != subject {
					return
				}
				passes.Add(1)
				if tc.moves {
					raw, err := as.jsa.loadLastMsgFor(mqttInflightStreamName, xsubj)
					if err == nil && raw != nil {
						as.jsa.storeMsg(xsubj, -1, raw.Data)
					}
					return
				}
				failNext.Store(true)
			}
			rig.sess.testRetainedClassify.Store(&classify)
			rig.asm.mu.Lock()
			rig.asm.testInflightLoad = func(subj string) (*mqttInflightRecord, *StoredMsg, error) {
				if subj == subject && failNext.CompareAndSwap(true, false) {
					return nil, nil, errors.New("the register re-read failed")
				}
				return as.jsa.loadInflightRecord(subj)
			}
			rig.asm.mu.Unlock()
			ci.cleanStart = false
			checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
			c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			if _, rc, _ := testMQTTReadConnAckV5(t, r); rc == mqttConnAckRCConnectionAccepted {
				c.Close()
				t.Fatalf("the resume was accepted after %d classification pass(es): the retained exchange was left unowned",
					passes.Load())
			}
			c.Close()
			if n := passes.Load(); n < 1 || (tc.moves && n < mqttInflightStoreAttempts) {
				t.Fatalf("the fixture reached the classification %d time(s)", n)
			}
			rig.sess.testRetainedClassify.Store(nil)
			rig.asm.mu.Lock()
			rig.asm.testInflightLoad = nil
			rig.asm.mu.Unlock()
			checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
			c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
				t.Fatalf("the reconnect: rc=%x present=%v", rc, present)
			}
			_, got := testMQTTReadPublishV5(t, r, "rr/t", []byte("owed"))
			if got != pi {
				t.Fatalf("the reconnect replayed pi %d, want %d", got, pi)
			}
			testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
		})
	}
}

// TestF105RetainedPreClassificationFailureRefusesTheResume (holistic H-6): a walk that stops before
// classifying — here its batch register load fails — takes the conservative success, withholding
// the identifier space, which installs nothing. Sound for a source-backed delivery, whose consumer
// re-offers it; for a RETAINED exchange nothing would own it, so the resume is refused before
// CONNACK and the reconnect replays it. A session whose inventory rules a retained exchange out
// keeps the conservative success (the control).
func TestF105RetainedPreClassificationFailureRefusesTheResume(t *testing.T) {
	for _, tc := range []struct {
		name               string
		retained, noCensus bool
	}{
		{"retained-outstanding-refused", true, false},
		{"retained-census-unreadable-refused", true, true},
		{"no-retained-accepted", false, false},
	} {
		retained := tc.retained
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			defer func() { testMQTTShutdownServer(s) }()
			if retained {
				cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-precls-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTReadConnAckV5(t, rp)
				testMQTTPubV5Props(t, cp, rp, 1, true, 1, "pc/t", []byte("owed"), nil)
				cp.Close()
			}
			ci := &mqttV5ConnInfo{clientID: "ret-precls", cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
			c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, r)
			testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "pc/t", opts: 1}})
			var pi uint16
			if retained {
				_, pi = testMQTTReadPublishV5(t, r, "pc/t", []byte("owed"))
			} else {
				// The control's walk must reach the same exit: an ORDINARY delivery in flight.
				cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-precls-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTReadConnAckV5(t, rp)
				testMQTTPubV5Props(t, cp, rp, 1, false, 1, "pc/t", []byte("live"), nil)
				cp.Close()
				_, pi = testMQTTReadPublishV5(t, r, "pc/t", []byte("live"))
			}
			rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
			rig.attach(t)
			c.Close()

			var failed atomic.Int32
			as := rig.asm
			// The walk's own batch: the one carrying this session's register subjects (the custody
			// census batches its duties through the same door).
			registers := mqttInflightSubject(rig.idHash, rig.gen, 0)
			registers = registers[:strings.LastIndexByte(registers, '.')+1]
			rig.asm.mu.Lock()
			rig.asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
				for _, subj := range subjects {
					if strings.HasPrefix(subj, registers) && failed.Add(1) == 1 {
						return nil, errors.New("the register batch could not be loaded")
					}
				}
				return as.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
			}
			if tc.noCensus {
				// The inventory itself is unreadable: nothing rules a retained exchange out.
				rig.asm.testCustodyCensus = func() error { return errors.New("the inventory could not be read") }
			}
			rig.asm.mu.Unlock()
			defer func() {
				rig.asm.mu.Lock()
				rig.asm.testInflightLoadMulti = nil
				rig.asm.testCustodyCensus = nil
				rig.asm.mu.Unlock()
			}()
			ci.cleanStart = false
			checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
			c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			_, rc, _ := testMQTTReadConnAckV5(t, r)
			rig.asm.mu.Lock()
			rig.asm.testCustodyCensus = nil
			rig.asm.mu.Unlock()
			c.Close()
			if !retained {
				if rc != mqttConnAckRCConnectionAccepted {
					t.Fatalf("a session with no retained exchange was refused: rc=%x", rc)
				}
				if failed.Load() < 1 {
					t.Fatal("the control never reached the walk's batch load: it proves nothing")
				}
				return
			}
			if rc == mqttConnAckRCConnectionAccepted {
				t.Fatal("the resume was accepted though its walk never classified the retained exchange")
			}
			if failed.Load() < 1 {
				t.Fatal("the fixture never failed the register batch load")
			}
			checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
			c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
				t.Fatalf("the reconnect: rc=%x present=%v", rc, present)
			}
			_, got := testMQTTReadPublishV5(t, r, "pc/t", []byte("owed"))
			if got != pi {
				t.Fatalf("the reconnect replayed pi %d, want %d", got, pi)
			}
			testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
		})
	}
}

// TestF105RetainedRefusedRestorationRefusesTheResume (holistic H-7 b): an exchange that reached the
// resume's installation is this generation's, live and restorable, so an installation REFUSED
// there (a conflicting resident or binding, a custody refusal) leaves it owned by nothing. The
// resume is refused before CONNACK, and the reconnect replays it.
func TestF105RetainedRefusedRestorationRefusesTheResume(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-instref-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 1, true, 1, "ir/t", []byte("owed"), nil)
	cp.Close()
	ci := &mqttV5ConnInfo{clientID: "ret-instref", cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ir/t", opts: 1}})
	_, pi := testMQTTReadPublishV5(t, r, "ir/t", []byte("owed"))
	rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
	rig.attach(t)
	subject := mqttInflightSubject(rig.idHash, rig.gen, pi)
	c.Close()
	// The identifier's resident entry would keep it (a same-object resume): drop it, so the walk
	// installs from the register, then refuse that installation.
	rig.sess.mu.Lock()
	rig.sess.untrackPublishEntry(pi, testUntrackNoBinding)
	rig.sess.mu.Unlock()
	var refused atomic.Int32
	refuse := func(subj string) bool { return subj == subject && refused.Add(1) > 0 }
	rig.sess.testRecoveredRetainedRefuse.Store(&refuse)
	ci.cleanStart = false
	checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
	c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	_, rc, _ := testMQTTReadConnAckV5(t, r)
	c.Close()
	rig.sess.testRecoveredRetainedRefuse.Store(nil)
	if refused.Load() < 1 {
		t.Fatal("the fixture never reached the retained installation")
	}
	if rc == mqttConnAckRCConnectionAccepted {
		t.Fatal("the resume was accepted though its retained exchange's installation was refused")
	}
	checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
	c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
		t.Fatalf("the reconnect: rc=%x present=%v", rc, present)
	}
	_, got := testMQTTReadPublishV5(t, r, "ir/t", []byte("owed"))
	if got != pi {
		t.Fatalf("the reconnect replayed pi %d, want %d", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
}

// TestF105RetainedQuarantineMatrix (consult 107): the positive-store-answer recovery quarantine, the
// ONE exception to the ownership property, over each population it admits — the store ANSWERED,
// and what it answered cannot be restored from. Each row corrupts one record of a transmitted,
// unanswered retained exchange (the victim) beside a healthy one, restarts the broker and resumes:
// the resume is ACCEPTED, the healthy exchange is replayed, nothing is sent for the victim, and the
// combined oracle holds — every standing register owned, except the victim's, which must be a
// recorded quarantine of that exact allocation with its reason, its identifier reserved and
// nothing acting for it.
func TestF105RetainedQuarantineMatrix(t *testing.T) {
	type corrupt func(t *testing.T, r *testRetainedRig, pi uint16, reg *mqttInflightRecord)
	rewrite := func(t *testing.T, r *testRetainedRig, subject, field string, value any) {
		t.Helper()
		raw, err := r.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
		if err != nil || raw == nil {
			t.Fatalf("loading %q: %v", subject, err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw.Data, &m); err != nil {
			t.Fatalf("decoding %q: %v", subject, err)
		}
		m[field] = value
		body, _ := json.Marshal(m)
		if _, err := r.asm.jsa.storeMsg(subject, -1, body); err != nil {
			t.Fatalf("rewriting %q: %v", subject, err)
		}
	}
	remove := func(t *testing.T, r *testRetainedRig, subject string) {
		t.Helper()
		raw, err := r.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
		if err != nil || raw == nil {
			t.Fatalf("loading %q: %v", subject, err)
		}
		if err := r.asm.jsa.deleteMsg(mqttInflightStreamName, raw.Sequence, true); err != nil {
			t.Fatalf("deleting %q: %v", subject, err)
		}
	}
	for _, tc := range []struct {
		name   string
		qos    byte
		reason string
		apply  corrupt
	}{
		{"exchange-absent", 1, "its exchange is absent", func(t *testing.T, r *testRetainedRig, pi uint16, reg *mqttInflightRecord) {
			remove(t, r, mqttExchangeSubject(r.idHash, r.gen, reg.Lin))
		}},
		{"phase-contradicts-register", 1, "its register is at PUBLISH while its exchange is not", func(t *testing.T, r *testRetainedRig, pi uint16, reg *mqttInflightRecord) {
			// A SUPPORTED shape that contradicts the register: the QoS 1 exchange already Terminal.
			rewrite(t, r, mqttExchangeSubject(r.idHash, r.gen, reg.Lin), "phase", int(mqttExchangeTerminal))
		}},
		{"copy-corrupt", 1, "its copy is absent, contradicts the register, or does not verify", func(t *testing.T, r *testRetainedRig, pi uint16, reg *mqttInflightRecord) {
			m, _, merr := r.asm.jsa.loadRetainedManifest(r.id, r.epoch, r.gen, reg.Payload)
			if merr != nil {
				t.Fatalf("no copy: %v", merr)
			}
			chunk, _ := json.Marshal(&mqttRetainedChunk{Ord: 1, Inc: m.Inc, Data: []byte("tamper")})
			if _, err := r.asm.jsa.storeMsg(mqttChunkSubject(r.idHash, r.gen, reg.Payload, 1), 0, chunk); err != nil {
				t.Fatalf("corrupting the chunk: %v", err)
			}
		}},
		{"marker-of-another-allocation", 1, "its marker belongs to another allocation", func(t *testing.T, r *testRetainedRig, pi uint16, reg *mqttInflightRecord) {
			body, _ := mqttBuildInflightMarker(mqttNewInflightNonce())
			if _, err := r.asm.jsa.storeMsg(mqttInflightMarkerSubject(r.idHash, r.gen, pi), -1, body); err != nil {
				t.Fatalf("forging the marker: %v", err)
			}
		}},
		{"canonical-absent", 2, "its canonical PUBREL is absent", func(t *testing.T, r *testRetainedRig, pi uint16, reg *mqttInflightRecord) {
			remove(t, r, reg.RelSubj)
		}},
		// The register's own shape refused (a retained register at pubrel_intended): the walk's
		// unreadable-register reservation holds it before classification — the exception's second
		// form; no retained quarantine reason is recorded, the oracle checks the reservation.
		{"register-shape-refused", 1, _EMPTY_, func(t *testing.T, r *testRetainedRig, pi uint16, reg *mqttInflightRecord) {
			rewrite(t, r, mqttInflightSubject(r.idHash, r.gen, pi), "state", string(mqttInflightPubRelIntended))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			defer func() { testMQTTShutdownServer(s) }()
			cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-qm-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, rp)
			testMQTTPubV5Props(t, cp, rp, tc.qos, true, 1, "qm/victim", []byte("victim"), nil)
			testMQTTPubV5Props(t, cp, rp, 1, true, 2, "qm/healthy", []byte("healthy"), nil)
			cp.Close()
			ci := &mqttV5ConnInfo{clientID: "ret-qm", cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
			c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, r)
			testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "qm/victim", opts: tc.qos}})
			_, victim := testMQTTReadPublishV5(t, r, "qm/victim", []byte("victim"))
			if tc.qos == 2 {
				testMQTTSendPIPacket(mqttPacketPubRec, t, c, victim)
				testMQTTReadPIPacket(mqttPacketPubRel, t, r, victim)
			}
			testMQTTSubV5(t, c, r, 2, []mqttV5SubFilter{{topic: "qm/healthy", opts: 1}})
			_, healthy := testMQTTReadPublishV5(t, r, "qm/healthy", []byte("healthy"))
			rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
			rig.attach(t)
			reg, _, _ := rig.asm.loadInflightRecord(mqttInflightSubject(rig.idHash, rig.gen, victim))
			if reg == nil {
				t.Fatal("no victim register")
			}
			c.Close()
			tc.apply(t, rig, victim, reg)

			dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
			s.Shutdown()
			o.MQTT.Port = -1
			testSetStoreDir(o, dir)
			s = testMQTTRunServer(t, o)
			ci.cleanStart = false
			checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
			c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
				t.Fatalf("a positive-store-answer quarantine refused the resume: rc=%x present=%v", rc, present)
			}
			flags, got, _ := testMQTTReadPubV5Raw(t, r, "qm/healthy", []byte("healthy"))
			if got != healthy || flags&mqttPubFlagDup == 0 {
				t.Fatalf("the healthy exchange replayed pi %d flags %x, want pi %d with DUP", got, flags, healthy)
			}
			testMQTTExpectNothing(t, r)
			rig.s = s
			rig.attach(t)
			rig.sess.mu.Lock()
			var reason string
			for _, q := range rig.sess.retainedQuarantined {
				if q.subject == mqttInflightSubject(rig.idHash, rig.gen, victim) {
					reason = q.reason
				}
			}
			rig.sess.mu.Unlock()
			if tc.reason != _EMPTY_ && !strings.HasPrefix(reason, tc.reason) {
				t.Fatalf("the victim's quarantine reason is %q, want %q", reason, tc.reason)
			}
			testRetainedOwnershipHolds(t, rig, false, 4, 2*time.Second)
			// NON-ACTION COVERS THE MATERIAL: whatever reclamation the resume's discovery schedules,
			// the quarantined copy's manifest stays, so a corrected record can still restore it.
			rig.sess.wakeRetainedReclaimForPayload(rig.gen, reg.Payload)
			time.Sleep(time.Second)
			if _, _, merr := rig.asm.jsa.loadRetainedManifest(rig.id, rig.epoch, rig.gen, reg.Payload); merr != nil {
				t.Fatalf("the quarantined exchange's copy was reclaimed: %v", merr)
			}
			testMQTTSendPIPacket(mqttPacketPubAck, t, c, healthy)
		})
	}
}

// TestF105RetainedConcurrentPubCompIsNotQuarantined (holistic H-8): the quarantine is admitted only
// on evidence, never on an answer a SUPPORTED concurrent advance produced. A PUBCOMP landing on the
// previous broker after the resume's exchange read terminalizes the pair and removes the canonical;
// the canonical then reads absent, which must be a reclassification (the pair moved), not a
// quarantine. The resume is accepted, nothing is quarantined, and allocation is not withheld.
func TestF105RetainedConcurrentPubCompIsNotQuarantined(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-cpc-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 2, true, 1, "cpc/t", []byte("done"), nil)
	cp.Close()
	ci := &mqttV5ConnInfo{clientID: "ret-cpc", cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "cpc/t", opts: 2}})
	_, pi := testMQTTReadPublishV5(t, r, "cpc/t", []byte("done"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
	rig.attach(t)
	subject := mqttInflightSubject(rig.idHash, rig.gen, pi)
	reg, _, _ := rig.asm.loadInflightRecord(subject)
	if reg == nil || reg.State != mqttInflightPubRelReady || reg.RelSubj == _EMPTY_ {
		t.Fatalf("the exchange is not at pubrel_ready: %+v", reg)
	}
	c.Close()
	jsa := &rig.asm.jsa
	rewrite := func(subj, field string, value any) {
		raw, err := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
		if err != nil || raw == nil {
			t.Errorf("loading %q: %v", subj, err)
			return
		}
		var m map[string]any
		json.Unmarshal(raw.Data, &m)
		m[field] = value
		body, _ := json.Marshal(m)
		if _, err := jsa.storeMsg(subj, -1, body); err != nil {
			t.Errorf("rewriting %q: %v", subj, err)
		}
	}
	var fired atomic.Bool
	pubcomp := func(subj string) {
		if subj != subject || !fired.CompareAndSwap(false, true) {
			return
		}
		// THE PREVIOUS BROKER'S PUBCOMP, between the exchange read and the canonical read.
		rewrite(mqttExchangeSubject(rig.idHash, rig.gen, reg.Lin), "phase", int(mqttExchangeTerminal))
		rewrite(subject, "state", string(mqttInflightPubRelTerminal))
		raw, err := jsa.loadLastMsgFor(mqttInflightStreamName, reg.RelSubj)
		if err == nil && raw != nil {
			jsa.deleteMsg(mqttInflightStreamName, raw.Sequence, true)
		}
	}
	rig.sess.testRetainedClassify.Store(&pubcomp)
	defer rig.sess.testRetainedClassify.Store(nil)
	ci.cleanStart = false
	checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
	c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
		t.Fatalf("the resume after a concurrent PUBCOMP: rc=%x present=%v", rc, present)
	}
	if !fired.Load() {
		t.Fatal("the fixture never interleaved the PUBCOMP")
	}
	testMQTTExpectNothing(t, r)
	rig.sess.mu.Lock()
	quarantined := len(rig.sess.retainedQuarantined)
	withheld := rig.sess.piSpaceIncomplete
	rig.sess.mu.Unlock()
	if quarantined != 0 || withheld {
		t.Fatalf("a concurrent PUBCOMP was quarantined (%d) or withheld allocation (%v)", quarantined, withheld)
	}
	testRetainedOwnershipHolds(t, rig, false, 4, 2*time.Second)
}

// TestF105RetainedSameObjectRefusedShapeDemotesTheResident (holistic H-9): the quarantine's
// non-action holds on a SAME-OBJECT resume too. A retained QoS 2 exchange at pubrel_ready leaves a
// resident PUBREL entry on the session object; its register then reads as a shape this broker
// refuses. The resume must demote the resident entry — no PUBREL replayed, nothing owed, the
// connection not closed past the retry budget — and the identifier stays reserved.
func TestF105RetainedSameObjectRefusedShapeDemotesTheResident(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-sobj-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 2, true, 1, "so/t", []byte("held"), nil)
	cp.Close()
	ci := &mqttV5ConnInfo{clientID: "ret-sobj", cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "so/t", opts: 2}})
	_, pi := testMQTTReadPublishV5(t, r, "so/t", []byte("held"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
	rig.attach(t)
	subject := mqttInflightSubject(rig.idHash, rig.gen, pi)
	c.Close()
	rig.sess.mu.Lock()
	resident := rig.sess.pendingPubRel[pi]
	rig.sess.mu.Unlock()
	if resident == nil || resident.kind != mqttPendingRetained {
		t.Fatalf("no resident retained PUBREL entry: %+v", resident)
	}
	raw, err := rig.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	if err != nil || raw == nil {
		t.Fatalf("loading the register: %v", err)
	}
	var m map[string]any
	json.Unmarshal(raw.Data, &m)
	m["state"] = string(mqttInflightPubRelIntended) // a retained register at pubrel_intended: refused
	body, _ := json.Marshal(m)
	if _, err := rig.asm.jsa.storeMsg(subject, -1, body); err != nil {
		t.Fatalf("rewriting the register: %v", err)
	}
	ci.cleanStart = false
	checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
	c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
		t.Fatalf("the same-object resume: rc=%x present=%v", rc, present)
	}
	// Past the owed retry budget (eight backoffs, ~6s): nothing sent, and the connection still open.
	c.SetReadDeadline(time.Now().Add(9 * time.Second))
	if n, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatalf("%d byte(s) were sent for a quarantined exchange", n)
	} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("the connection was closed over a quarantined exchange: %v", err)
	}
	c.SetReadDeadline(time.Time{})
	rig.sess.mu.Lock()
	rel := rig.sess.pendingPubRel[pi]
	rig.sess.mu.Unlock()
	if rel != nil && rel.acts() {
		t.Fatalf("the resident PUBREL entry still acts for a quarantined exchange: %+v", rel)
	}
	testRetainedOwnershipHolds(t, rig, false, 4, 2*time.Second)
}

// TestF105RetainedReclaimWakeDuringAWithheldStepIsKept (holistic H-4 f2): a copy withheld because
// its exchange still needs it is dropped after the bound — but a WAKE that arrives while that last
// attempt is out (the exchange's ending, here the client's PUBACK) is newer than the attempt's
// answer, and must keep the key: the ending's wake is the copy's only one.
func TestF105RetainedReclaimWakeDuringAWithheldStepIsKept(t *testing.T) {
	r := testRetainedRigUp(t, "ret-reclaim-wake", "rt/rw", 1, []byte("retained-rw"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/rw", qos: 1}}, []byte{1})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/rw", mqttPubQos1|mqttPubFlagRetain, []byte("retained-rw"))
	r.attach(t)
	reg, _, _ := r.asm.loadInflightRecord(mqttInflightSubject(r.idHash, r.gen, pi))
	if reg == nil {
		t.Fatal("no retained register")
	}
	m, _, merr := r.asm.jsa.loadRetainedManifest(r.id, r.epoch, r.gen, reg.Payload)
	if merr != nil {
		t.Fatalf("no copy: %v", merr)
	}
	// The LAST withheld attempt is held open after it answered "still needed"; the PUBACK lands
	// and wakes the copy while it is out.
	var steps atomic.Int32
	held, release := make(chan struct{}), make(chan struct{})
	after := func(k mqttRetainedReclaimKey, step mqttReclaimStep, err error) {
		if k.inc != m.Inc || !errors.Is(err, errMQTTRetainedMaterialNeeded) {
			return
		}
		if steps.Add(1) == mqttRetainedStepAttempts {
			close(held)
			<-release
		}
	}
	r.sess.testReclaimAfterStep.Store(&after)
	defer r.sess.testReclaimAfterStep.Store(nil)
	r.sess.wakeRetainedReclaim(r.gen, m.Inc)
	select {
	case <-held:
	case <-time.After(30 * time.Second):
		t.Fatalf("the copy was withheld only %d time(s)", steps.Load())
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
	for deadline := time.Now().Add(10 * time.Second); ; {
		x, _, _ := r.asm.jsa.loadOutboundExchange(r.id, r.epoch, r.gen, reg.Lin)
		if x != nil && x.Phase == mqttExchangeTerminal {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the PUBACK never ended the exchange: %+v", x)
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // the ending's wake has been taken
	close(release)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if !testRetainedMaterialOwned(r, m.Inc) {
			t.Fatal("the ended exchange's copy has no owner: the ending's wake was erased by an older answer")
		}
		res, _, _ := r.asm.jsa.loadOutboundResource(r.id, r.epoch, r.gen, m.Inc)
		if res != nil && res.State == mqttResourceCleanupComplete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the copy was never reclaimed: the ending's wake was erased by an older answer: owner %+v", res)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestF105RetainedQoS2HandshakeThroughTheCanonical: a retained QoS 2 delivery's PUBREC creates its
// canonical PUBREL on $MQTT_inflight in the advance and the PUBREL is emitted directly — no
// $MQTT_out record exists for it; PUBCOMP then terminalizes the exchange and the cleanup removes
// the canonical, the register and the copy.
func TestF105RetainedQoS2HandshakeThroughTheCanonical(t *testing.T) {
	r := testRetainedRigUp(t, "ret-qos2", "rt/b", 2, []byte("retained-b"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/b", qos: 2}}, []byte{2})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/b", mqttPubQoS2|mqttPubFlagRetain, []byte("retained-b"))
	r.attach(t)
	subject := mqttInflightSubject(r.idHash, r.gen, pi)
	reg, _, _ := r.asm.loadInflightRecord(subject)
	if reg == nil || !reg.retainedSource() {
		t.Fatalf("no retained register before PUBREC: %+v", reg)
	}
	m, _, _ := r.asm.jsa.loadRetainedManifest(r.id, r.epoch, r.gen, reg.Payload)

	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)

	after, _, _ := r.asm.loadInflightRecord(subject)
	if after == nil || after.State != mqttInflightPubRelReady || after.RelSubj == _EMPTY_ {
		t.Fatalf("the retained register after PUBREC is %+v", after)
	}
	if _, err := r.asm.jsa.loadLastMsgFor(mqttInflightStreamName, after.RelSubj); err != nil {
		t.Fatalf("the canonical PUBREL is not on $MQTT_inflight: %v", err)
	}
	if _, err := r.asm.jsa.loadLastMsgFor(mqttOutStreamName, r.sess.pubRelSubject); err == nil {
		t.Fatalf("a retained PUBREC stored a PUBREL on $MQTT_out")
	}

	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	deadline := time.Now().Add(10 * time.Second)
	for {
		x, _, _ := r.asm.jsa.loadOutboundExchange(r.id, r.epoch, r.gen, reg.Lin)
		cur, _, _ := r.asm.loadInflightRecord(subject)
		_, relGone := r.asm.jsa.loadLastMsgFor(mqttInflightStreamName, after.RelSubj)
		res, _, _ := r.asm.jsa.loadOutboundResource(r.id, r.epoch, r.gen, m.Inc)
		if x != nil && x.Phase == mqttExchangeTerminal && cur == nil && relGone != nil &&
			res != nil && res.State == mqttResourceCleanupComplete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after PUBCOMP: exchange %+v, register %+v, rel gone %v, owner %+v", x, cur, relGone != nil, res)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := mqttRetainedAtOrdinarySink.Load(); n != 0 {
		t.Fatalf("%d retained register(s) reached an ordinary-source sink", n)
	}
}

// T1 — IMMUTABLE REPLAY. A retained QoS 1 delivery left unacknowledged is re-sent on reconnect with
// DUP from its OWN copy — the original bytes — even after the retained topic was rewritten.
func TestF105RetainedReplayIsTheOriginalCopy(t *testing.T) {
	r := testRetainedRigUp(t, "ret-replay", "rt/c", 1, []byte("original"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/c", qos: 1}}, []byte{1})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/c", mqttPubQos1|mqttPubFlagRetain, []byte("original"))

	// The retained topic is rewritten while the delivery is unacknowledged.
	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id + "-pub2", cleanSess: true}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 1, false, true, "rt/c", 1, []byte("rewritten"))
	testMQTTFlush(t, pub, nil, pubr)
	sub.Close()

	sub2, subr2 := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub2.Close()
	testMQTTCheckConnAck(t, subr2, mqttConnAckRCConnectionAccepted, true)
	got := testMQTTCheckPubMsgNoAck(t, sub2, subr2, "rt/c", mqttPubQos1|mqttPubFlagRetain|mqttPubFlagDup, []byte("original"))
	if got != pi {
		t.Fatalf("the replay went out under identifier %d, want the original %d", got, pi)
	}
}

// T2 / T4 — RESTART REPLAY. A retained QoS 2 delivery whose PUBLISH went unacknowledged is replayed
// from its copy after a broker restart (a fresh session object); after a PUBREC and a second restart
// its PUBREL is replayed from the canonical; PUBCOMP completes it.
func TestF105RetainedReplaySurvivesARestart(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "ret-restart-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 2, false, true, "rt/d", 1, []byte("persisted"))
	testMQTTFlush(t, pub, nil, pubr)
	pub.Close()
	ci := &mqttConnInfo{clientID: "ret-restart", cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "rt/d", qos: 2}}, []byte{2})
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "rt/d", mqttPubQoS2|mqttPubFlagRetain, []byte("persisted"))

	restart := func() {
		dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
		s.Shutdown()
		c.Close()
		o.MQTT.Port = -1
		testSetStoreDir(o, dir)
		s = testMQTTRunServer(t, o)
		c, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	}
	restart()
	defer func() { c.Close(); testMQTTShutdownServer(s) }()
	if got := testMQTTCheckPubMsgNoAck(t, c, r, "rt/d", mqttPubQoS2|mqttPubFlagRetain|mqttPubFlagDup, []byte("persisted")); got != pi {
		t.Fatalf("the restart replay went out under identifier %d, want %d", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)

	restart()
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
	testMQTTFlush(t, c, nil, r)
}

// TestMQTTRetainedSendReleasesOnTheWriteWithoutADiscard, REWRITTEN for F-105 7c with its guarantee
// kept: a written retained frame's identifier stays reserved until the client acknowledges it, and
// is allocatable afterwards. Since 7c the reservation after the write is the EXCHANGE's — the
// ledger's charge and the register-backed entry — not a retained hold released on the write.
func TestMQTTRetainedSendReleasesOnTheWriteWithoutADiscard(t *testing.T) {
	r := testRetainedRigUp(t, "retained-ordinary", "foo/bar", 1, []byte("retained"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "foo/bar", qos: 1}}, []byte{1})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo/bar", mqttPubQos1|mqttPubFlagRetain, []byte("retained"))
	r.attach(t)
	reserved := func() bool {
		r.sess.mu.Lock()
		defer r.sess.mu.Unlock()
		_, held := r.sess.pendingPublish[pi]
		return held || r.sess.custody.holdsPI(pi) || r.sess.sendReservedLocked(pi)
	}
	if !reserved() {
		t.Fatal("a written retained frame's identifier must stay reserved until the client acknowledges it")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
	deadline := time.Now().Add(5 * time.Second)
	for reserved() {
		if time.Now().After(deadline) {
			t.Fatal("once acknowledged, the identifier must be allocatable: a reservation that never " +
				"releases is a permanently stranded identifier")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// T10 — DISCARD AT PUBLISH. An admitted retained delivery left unacknowledged, then a clean-start
// CONNECT discards the generation: the closed generation's recovery pass reclaims the copy — chunks
// and manifest gone, the owner CleanupComplete, both payload duties terminal — without waiting for
// a PUBACK that will never come.
func TestF105RetainedDiscardReclaimsTheCopy(t *testing.T) {
	// The recovery pass rides the session-expiry sweep's cadence.
	defer setMQTTSessionExpirySweepInterval(100 * time.Millisecond)()
	r := testRetainedRigUp(t, "ret-discard", "rt/x", 1, []byte("discarded"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/x", qos: 1}}, []byte{1})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/x", mqttPubQos1|mqttPubFlagRetain, []byte("discarded"))
	r.attach(t)
	gen := r.gen
	reg, _, _ := r.asm.loadInflightRecord(mqttInflightSubject(r.idHash, gen, pi))
	if reg == nil || !reg.retainedSource() {
		t.Fatalf("no retained register: %+v", reg)
	}
	m, _, merr := r.asm.jsa.loadRetainedManifest(r.id, r.epoch, gen, reg.Payload)
	if merr != nil {
		t.Fatalf("no copy: %v", merr)
	}
	sub.Close()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: true}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	deadline := time.Now().Add(20 * time.Second)
	for {
		res, _, _ := r.asm.jsa.loadOutboundResource(r.id, r.epoch, gen, m.Inc)
		_, gone := r.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttManifestSubject(r.idHash, gen, reg.Payload))
		if res != nil && res.State == mqttResourceCleanupComplete && gone != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the Discard: owner %+v, manifest gone %v", res, gone != nil)
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, kind := range []mqttDutyKind{mqttDutyProvision, mqttDutyCleanup} {
		ord, oerr := r.asm.jsa.resourceDutyOrdinal(r.id, r.epoch, gen, m.Inc, kind)
		if oerr != nil {
			t.Fatalf("the copy's %q duty: %v", kind, oerr)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			d, _, _ := r.asm.jsa.loadDutyEntryAt(r.id, r.epoch, gen, ord)
			if d != nil && d.State == mqttDutyTerminal {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the copy's %q duty %d is %+v, want terminal", kind, ord, d)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// T14 — A FAILED PUBREL ACQUISITION IS RETRIED BY THE DRIVER, the client silent. The PUBREC handler's
// own acquisition attempts are refused before sending; the driver re-attempts and the PUBREL
// arrives. And when the driver's attempts are exhausted too, the connection is closed and the
// reconnect replays the PUBREL from the canonical record.
func TestF105RetainedPubRelAcquisitionIsRetriedByTheDriver(t *testing.T) {
	for _, exhaust := range []bool{false, true} {
		t.Run(map[bool]string{false: "retried", true: "exhausted"}[exhaust], func(t *testing.T) {
			r := testRetainedRigUp(t, "ret-pubrel-retry-"+map[bool]string{false: "r", true: "x"}[exhaust], "rt/q2", 2, []byte("q2"))
			defer r.done()
			ci := &mqttConnInfo{clientID: r.id, cleanSess: false}
			sub, subr := testMQTTConnect(t, ci, r.o.MQTT.Host, r.o.MQTT.Port)
			defer func() { sub.Close() }()
			testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/q2", qos: 2}}, []byte{2})
			pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/q2", mqttPubQoS2|mqttPubFlagRetain, []byte("q2"))
			r.attach(t)
			jsa := &r.asm.jsa
			var refused atomic.Int32
			commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
				if at.Attempt != nil && at.Attempt.ID == r.id && at.Attempt.Op == "op-acquire-pubrel" {
					if n := refused.Add(1); exhaust || n <= int32(mqttInflightStoreAttempts) {
						return 0, errors.New("refused before it was sent")
					}
				}
				return send()
			}
			jsa.testTransitionCommit.Store(&commit)
			defer jsa.testTransitionCommit.Store(nil)
			testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
			if !exhaust {
				testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
				if n := refused.Load(); n <= int32(mqttInflightStoreAttempts) {
					t.Fatalf("the PUBREL went out after %d acquisition attempts: the handler's own attempts were not refused", n)
				}
				testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
				testMQTTFlush(t, sub, nil, subr)
				return
			}
			// Eight backed-off driver attempts, then the close.
			sub.SetReadDeadline(time.Now().Add(30 * time.Second))
			if _, err := sub.Read(make([]byte, 1)); err == nil {
				t.Fatal("expected the connection to be closed after the driver's attempts were exhausted")
			} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatal("the connection was never closed after the driver's attempts were exhausted")
			}
			if n := refused.Load(); n < int32(mqttInflightStoreAttempts) {
				t.Fatalf("only %d acquisition attempts were refused before the close", n)
			}
			jsa.testTransitionCommit.Store(nil)
			sub, subr = testMQTTConnect(t, ci, r.o.MQTT.Host, r.o.MQTT.Port)
			testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, true)
			testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
			testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
			testMQTTFlush(t, sub, nil, subr)
		})
	}
}

// T9 — A CRASH MID-MATERIALIZATION, then a CONNECT of the same session. The broker stops while the
// copy's chunk batches are being written (the first batch is held at its commit), leaving it
// PROVISIONING under the old tenure; the resume's inventory traversal collects the payload from
// its duties, and the reclaimer closes it under the successor-tenure arm, seals the provisioning,
// purges the material and completes the owner with both duties terminal.
func TestF105RetainedCrashMidChunksIsReclaimedAtTheNextConnect(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MaxPayload = 4 * 1024 * 1024
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()
	big := []byte(strings.Repeat("c", 9*mqttChunkMaxBytes+5))
	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "ret-crash-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 1, false, true, "rt/crash", 1, big)
	testMQTTFlush(t, pub, nil, pubr)
	pub.Close()

	ci := &mqttConnInfo{clientID: "ret-crash", cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
	rig.attach(t)
	jsa := &rig.asm.jsa
	held := make(chan *mqttAuthorityTransition, 1)
	hold := make(chan struct{})
	var once sync.Once
	presend := func(at *mqttAuthorityTransition) {
		if at.Attempt != nil && at.Attempt.ID == ci.clientID && at.Attempt.Op == "op-retained-chunks" {
			once.Do(func() {
				held <- at
				<-hold
			})
		}
	}
	jsa.testTransitionPresend.Store(&presend)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "rt/crash", qos: 1}}, []byte{1})
	var payload string
	select {
	case at := <-held:
		for _, e := range at.Extra {
			if mqttIsManifestSubject(e.subj) {
				var m mqttRetainedManifest
				if json.Unmarshal(e.body, &m) == nil {
					payload = m.Payload
				}
			}
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the copy's first chunk batch was never reached")
	}
	if payload == _EMPTY_ {
		t.Fatal("the held chunk batch named no manifest")
	}
	gen := rig.gen
	m, _, merr := jsa.loadRetainedManifest(rig.id, rig.epoch, gen, payload)
	if merr != nil || m.State != mqttManifestProvisioning {
		t.Fatalf("before the crash the copy is %+v (%v), want PROVISIONING", m, merr)
	}

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	close(hold)
	jsa.testTransitionPresend.Store(nil)
	c.Close()
	o.MQTT.Port = -1
	testSetStoreDir(o, dir)
	s = testMQTTRunServer(t, o)
	c, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	rig.s = s
	rig.attach(t)
	jsa = &rig.asm.jsa
	deadline := time.Now().Add(20 * time.Second)
	for {
		res, _, _ := jsa.loadOutboundResource(rig.id, rig.epoch, gen, m.Inc)
		_, gone := jsa.loadLastMsgFor(mqttInflightStreamName, mqttManifestSubject(rig.idHash, gen, payload))
		if res != nil && res.State == mqttResourceCleanupComplete && gone != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the CONNECT the crashed copy's owner is %+v, manifest gone %v", res, gone != nil)
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, kind := range []mqttDutyKind{mqttDutyProvision, mqttDutyCleanup} {
		ord, oerr := jsa.resourceDutyOrdinal(rig.id, rig.epoch, gen, m.Inc, kind)
		if oerr != nil {
			t.Fatalf("the copy's %q duty: %v", kind, oerr)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			d, _, _ := jsa.loadDutyEntryAt(rig.id, rig.epoch, gen, ord)
			if d != nil && d.State == mqttDutyTerminal {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the copy's %q duty %d is %+v, want terminal", kind, ord, d)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// T5 — ORDERING. While a subscription's retained replay is held (its copy's reservation parked at
// its batch), a live QoS 0 message published to the topic is NOT delivered ahead of it: the raw
// subscription's gate holds it, and on release the retained frame arrives first, then the live one.
func TestF105RetainedReplayPrecedesLiveQoS0(t *testing.T) {
	r := testRetainedRigUp(t, "ret-order", "rt/o", 1, []byte("retained"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	r.attach(t)
	jsa := &r.asm.jsa
	parked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	presend := func(at *mqttAuthorityTransition) {
		if at.Attempt != nil && at.Attempt.ID == r.id && at.Attempt.Op == "op-retained-reserve" {
			once.Do(func() {
				close(parked)
				<-release
			})
		}
	}
	jsa.testTransitionPresend.Store(&presend)
	defer jsa.testTransitionPresend.Store(nil)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/o", qos: 1}}, []byte{1})
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the replay never reached its reservation")
	}
	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id + "-live", cleanSess: true}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 0, false, false, "rt/o", 0, []byte("live"))
	testMQTTFlush(t, pub, nil, pubr)
	testMQTTExpectNothing(t, subr)
	close(release)
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/o", mqttPubQos1|mqttPubFlagRetain, []byte("retained"))
	testMQTTCheckPubMsg(t, sub, subr, "rt/o", 0, []byte("live"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
}

// T7c — DEPARTURE AFTER THE DURABLE ADMISSION, BEFORE ACQUISITION. The subscription departs while
// its admitted copy waits (held after the admission): the driver withdraws the proven-unsent
// exchange — WithdrawnUnsent, its register gone, its identifier released — and no PUBLISH is sent.
func TestF105RetainedDepartureAfterAdmissionIsWithdrawn(t *testing.T) {
	r := testRetainedRigUp(t, "ret-depart", "rt/dp", 1, []byte("departing"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	r.attach(t)
	admitted := make(chan uint16, 1)
	release := make(chan struct{})
	r.sess.mu.Lock()
	r.sess.testRetainedAfterAdmit = func(pi uint16) {
		admitted <- pi
		<-release
	}
	r.sess.mu.Unlock()
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/dp", qos: 1}}, []byte{1})
	var pi uint16
	select {
	case pi = <-admitted:
	case <-time.After(10 * time.Second):
		t.Fatal("the copy was never admitted")
	}
	reg, _, _ := r.asm.loadInflightRecord(mqttInflightSubject(r.idHash, r.gen, pi))
	if reg == nil {
		t.Fatal("no admitted register")
	}
	testMQTTUnsub(t, 2, sub, subr, []*mqttFilter{{filter: "rt/dp"}})
	close(release)
	deadline := time.Now().Add(10 * time.Second)
	for {
		x, _, _ := r.asm.jsa.loadOutboundExchange(r.id, r.epoch, r.gen, reg.Lin)
		cur, _, _ := r.asm.loadInflightRecord(mqttInflightSubject(r.idHash, r.gen, pi))
		r.sess.mu.Lock()
		_, entry := r.sess.pendingPublish[pi]
		held := r.sess.custody != nil && r.sess.custody.holdsPI(pi)
		r.sess.mu.Unlock()
		if x != nil && x.Phase == mqttExchangeWithdrawnUnsent && cur == nil && !entry && !held {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the departure: exchange %+v, register %+v, entry %v, identifier held %v", x, cur, entry, held)
		}
		time.Sleep(50 * time.Millisecond)
	}
	testMQTTExpectNothing(t, subr)
}

// T7d — A WITHDRAWAL WHOSE REGISTER DELETE FAILS STAYS OWNED (holistic H-3 f1, consult 106). The
// departure's withdrawal lands, then the register's delete is refused: the entry stays TRACKED (the
// identifier reserved, the exchange this connection's) across the refusal, never untracked before
// the delete is confirmed. (a) A transient refusal is retried by the continuation worker until the
// tombstone is consumed and the identifier released. (b) A persistent one closes the connection,
// and the resume consumes the tombstone; nothing is ever sent.
func TestF105RetainedFailedWithdrawalStaysOwned(t *testing.T) {
	for _, tc := range []struct {
		name       string
		persistent bool
	}{{"transient", false}, {"persistent-closes", true}} {
		t.Run(tc.name, func(t *testing.T) {
			r := testRetainedRigUp(t, "ret-wfail", "rt/wf", 1, []byte("departing"))
			defer r.done()
			ci := &mqttConnInfo{clientID: r.id, cleanSess: false}
			sub, subr := testMQTTConnect(t, ci, r.o.MQTT.Host, r.o.MQTT.Port)
			defer sub.Close()
			testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
			r.attach(t)
			admitted := make(chan uint16, 1)
			release := make(chan struct{})
			r.sess.mu.Lock()
			r.sess.testRetainedAfterAdmit = func(pi uint16) {
				admitted <- pi
				<-release
			}
			r.sess.mu.Unlock()
			testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/wf", qos: 1}}, []byte{1})
			var pi uint16
			select {
			case pi = <-admitted:
			case <-time.After(10 * time.Second):
				t.Fatal("the copy was never admitted")
			}
			subject := mqttInflightSubject(r.idHash, r.gen, pi)
			reg, _, _ := r.asm.loadInflightRecord(subject)
			if reg == nil {
				t.Fatal("no admitted register")
			}
			var refused atomic.Int32
			refusing := make(chan struct{})
			var once sync.Once
			r.asm.mu.Lock()
			as := r.asm
			r.asm.testInflightDelete = func(seq uint64) error {
				if tc.persistent || refused.Load() < mqttInflightStoreAttempts {
					if refused.Add(1) == mqttInflightStoreAttempts {
						once.Do(func() { close(refusing) })
					}
					return errors.New("the register delete was refused")
				}
				return as.jsa.deleteMsg(mqttInflightStreamName, seq, true)
			}
			r.asm.mu.Unlock()
			defer func() {
				r.asm.mu.Lock()
				r.asm.testInflightDelete = nil
				r.asm.mu.Unlock()
			}()
			testMQTTUnsub(t, 2, sub, subr, []*mqttFilter{{filter: "rt/wf"}})
			close(release)
			select {
			case <-refusing:
			case <-time.After(10 * time.Second):
				t.Fatal("the withdrawal's register delete was never attempted")
			}
			// THE REFUSAL LEFT THE IDENTIFIER RESERVED OVER THE STANDING REGISTER, AND OWNED: the entry
			// (until the landed withdrawal's ending removes it) or the exclusion handed over before the
			// first attempt holds it, and the continuation worker owns the register's withdrawal.
			if cur, _, _ := r.asm.loadInflightRecord(subject); cur == nil {
				t.Fatal("the refused delete removed the register")
			}
			r.sess.mu.Lock()
			p := r.sess.pendingPublish[pi]
			excluded := r.sess.slotExclusionInstLocked(pi) != 0
			r.sess.mu.Unlock()
			if (p == nil || p.regNonce != reg.Nonce) && !excluded {
				t.Fatalf("a refused withdrawal left the identifier unreserved over its register: entry %+v", p)
			}
			owned := false
			for deadline := time.Now().Add(5 * time.Second); !owned && time.Now().Before(deadline); {
				r.sess.retainedDriver.mu.Lock()
				for _, o := range r.sess.retainedDriver.owed {
					owned = owned || (o.pi == pi && o.nonce == reg.Nonce && o.kind == mqttRetainedOwedWithdraw)
				}
				r.sess.retainedDriver.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
			if !owned {
				t.Fatal("a refused withdrawal has no owner")
			}
			testRetainedOwnershipHolds(t, r, false, 4, 2*time.Second)
			if !tc.persistent {
				deadline := time.Now().Add(15 * time.Second)
				for {
					x, _, _ := r.asm.jsa.loadOutboundExchange(r.id, r.epoch, r.gen, reg.Lin)
					cur, _, _ := r.asm.loadInflightRecord(subject)
					r.sess.mu.Lock()
					_, entry := r.sess.pendingPublish[pi]
					held := r.sess.custody != nil && r.sess.custody.holdsPI(pi)
					excluded := r.sess.slotExclusionInstLocked(pi) != 0
					r.sess.mu.Unlock()
					if x != nil && x.Phase == mqttExchangeWithdrawnUnsent && cur == nil && !entry && !held && !excluded {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("the refused withdrawal was never retried to completion: exchange %+v, register %+v, "+
							"entry %v, held %v, excluded %v (refusals %d)", x, cur, entry, held, excluded, refused.Load())
					}
					time.Sleep(50 * time.Millisecond)
				}
				testMQTTExpectNothing(t, subr)
				return
			}
			sub.SetReadDeadline(time.Now().Add(40 * time.Second))
			if _, err := sub.Read(make([]byte, 1)); err == nil {
				t.Fatal("a withdrawn exchange's PUBLISH was sent")
			} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatal("the connection was never closed: the refused withdrawal was stranded on it")
			}
			r.asm.mu.Lock()
			r.asm.testInflightDelete = nil
			r.asm.mu.Unlock()
			sub, subr = testMQTTConnect(t, ci, r.o.MQTT.Host, r.o.MQTT.Port)
			defer sub.Close()
			testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, true)
			deadline := time.Now().Add(15 * time.Second)
			for {
				cur, _, _ := r.asm.loadInflightRecord(subject)
				if cur == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the resume never consumed the withdrawn exchange's register: %+v", cur)
				}
				time.Sleep(50 * time.Millisecond)
			}
			testMQTTExpectNothing(t, subr)
		})
	}
}

// T22 — A TAKEOVER WHILE A LIVE QoS 0 CALLBACK WAITS ON THE GATE. The replay is held, a live QoS 0
// delivery parks on the raw subscription's gate, and a takeover CONNECT arrives: the closing
// signal releases the waiter (the QoS 0 message is dropped, as QoS 0 permits), so the displaced
// incarnation drains and the takeover is accepted promptly.
func TestF105RetainedGateWaiterDoesNotHoldATakeover(t *testing.T) {
	r := testRetainedRigUp(t, "ret-gate-takeover", "rt/g", 1, []byte("retained"))
	defer r.done()
	ci := &mqttConnInfo{clientID: r.id, cleanSess: false}
	sub, subr := testMQTTConnect(t, ci, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	r.attach(t)
	jsa := &r.asm.jsa
	parked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	presend := func(at *mqttAuthorityTransition) {
		if at.Attempt != nil && at.Attempt.ID == r.id && at.Attempt.Op == "op-retained-reserve" {
			once.Do(func() {
				close(parked)
				<-release
			})
		}
	}
	jsa.testTransitionPresend.Store(&presend)
	defer jsa.testTransitionPresend.Store(nil)
	defer close(release)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/g", qos: 1}}, []byte{1})
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the replay never reached its reservation")
	}
	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id + "-live", cleanSess: true}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 0, false, false, "rt/g", 0, []byte("live"))
	testMQTTFlush(t, pub, nil, pubr)
	waiters := func() int {
		buf := make([]byte, 1<<22)
		return strings.Count(string(buf[:runtime.Stack(buf, true)]), "awaitRetainedReplayGate(")
	}
	deadline := time.Now().Add(5 * time.Second)
	for waiters() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the live QoS 0 delivery never parked on the gate: the fixture does not reach the waiter")
		}
		time.Sleep(20 * time.Millisecond)
	}

	start := time.Now()
	c2, r2 := testMQTTConnect(t, ci, r.o.MQTT.Host, r.o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("the takeover took %v behind a gate waiter", d)
	}
	// The displaced connection's waiter is RELEASED, not left parked until the held replay ends.
	deadline = time.Now().Add(3 * time.Second)
	for waiters() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the displaced connection's gate waiter is still parked after the takeover")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// T7 — AMBIGUITY AT EACH STEP. Each step's first batch LANDS but its response is lost; the driver
// resolves the frozen attempt rather than treating it as refused, and the copy is delivered, the
// client silent, exactly once.
func TestF105RetainedLostResponsesAreResolvedAndDelivered(t *testing.T) {
	for _, op := range []string{"op-retained-reserve", "op-retained-chunks", "op-retained-seal",
		"op-admit-retained", "op-acquire-publish"} {
		t.Run(op, func(t *testing.T) {
			r := testRetainedRigUp(t, "ret-lost-"+strings.TrimPrefix(op, "op-"), "rt/lost", 1, []byte("lost-and-found"))
			defer r.done()
			sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
			defer sub.Close()
			testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
			r.attach(t)
			jsa := &r.asm.jsa
			var lost atomic.Bool
			commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
				seq, err := send()
				if err == nil && at.Attempt != nil && at.Attempt.ID == r.id && at.Attempt.Op == op && lost.CompareAndSwap(false, true) {
					return 0, errors.New("the response was lost")
				}
				return seq, err
			}
			jsa.testTransitionCommit.Store(&commit)
			defer jsa.testTransitionCommit.Store(nil)
			testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/lost", qos: 1}}, []byte{1})
			pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/lost", mqttPubQos1|mqttPubFlagRetain, []byte("lost-and-found"))
			if !lost.Load() {
				t.Fatalf("the fixture never lost %s's response", op)
			}
			testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
			testMQTTExpectNothing(t, subr)
		})
	}
}

// T23 — FAIRNESS. The replay worker is held at a reservation (the one active materialization). While
// it is held, an owed PUBREL (its acquisition refused on the PUBREC handler's own attempts) is still
// emitted by the continuation worker, the completed exchange's copy is still reclaimed by the
// reclaimer, and a second subscription's replay waits behind the held one; released, both replays
// arrive in SUBSCRIBE order.
func TestF105RetainedWorkersDoNotWaitOnAHeldReplay(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "ret-fair-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	for i, topic := range []string{"fair/q2", "fair/held", "fair/next"} {
		qos := byte(1)
		if topic == "fair/q2" {
			qos = 2
		}
		testMQTTPublish(t, pub, pubr, qos, false, true, topic, uint16(i+1), []byte(topic))
	}
	testMQTTFlush(t, pub, nil, pubr)

	ci := &mqttConnInfo{clientID: "ret-fair", cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
	rig.attach(t)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "fair/q2", qos: 2}}, []byte{2})
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "fair/q2", mqttPubQoS2|mqttPubFlagRetain, []byte("fair/q2"))
	reg, _, _ := rig.asm.loadInflightRecord(mqttInflightSubject(rig.idHash, rig.gen, pi))
	if reg == nil {
		t.Fatal("no retained register for fair/q2")
	}

	jsa := &rig.asm.jsa
	var refusedPubRel atomic.Int32
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if at.Attempt != nil && at.Attempt.ID == ci.clientID && at.Attempt.Op == "op-acquire-pubrel" &&
			refusedPubRel.Add(1) <= int32(mqttInflightStoreAttempts) {
			return 0, errors.New("refused before it was sent")
		}
		return send()
	}
	jsa.testTransitionCommit.Store(&commit)
	defer jsa.testTransitionCommit.Store(nil)
	parked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	presend := func(at *mqttAuthorityTransition) {
		if at.Attempt != nil && at.Attempt.ID == ci.clientID && at.Attempt.Op == "op-retained-reserve" {
			once.Do(func() {
				close(parked)
				<-release
			})
		}
	}
	jsa.testTransitionPresend.Store(&presend)
	defer jsa.testTransitionPresend.Store(nil)

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "fair/held", qos: 1}}, []byte{1})
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the replay never reached its reservation")
	}
	testMQTTSub(t, 3, c, r, []*mqttFilter{{filter: "fair/next", qos: 1}}, []byte{1})

	// The PUBREC's own acquisitions are refused; the continuation worker emits the PUBREL anyway.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
	// The finished copy is reclaimed while the replay is still held.
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, gone := jsa.loadLastMsgFor(mqttInflightStreamName, mqttManifestSubject(rig.idHash, rig.gen, reg.Payload))
		if gone != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the completed copy was not reclaimed while the replay was held")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// One active materialization: the second subscription's replay has not overtaken the held one.
	testMQTTExpectNothing(t, r)
	close(release)
	p1 := testMQTTCheckPubMsgNoAck(t, c, r, "fair/held", mqttPubQos1|mqttPubFlagRetain, []byte("fair/held"))
	p2 := testMQTTCheckPubMsgNoAck(t, c, r, "fair/next", mqttPubQos1|mqttPubFlagRetain, []byte("fair/next"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, p1)
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, p2)
}

// T7b — THE READLOOP IS NOT BLOCKED by a replay: with the driver held at its first reservation, a
// PINGREQ sent right after the SUBSCRIBE is answered.
func TestF105RetainedReplayDoesNotBlockTheReadLoop(t *testing.T) {
	r := testRetainedRigUp(t, "ret-readloop", "rt/rl", 1, []byte("held"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	r.attach(t)
	jsa := &r.asm.jsa
	parked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	presend := func(at *mqttAuthorityTransition) {
		if at.Attempt != nil && at.Attempt.ID == r.id && at.Attempt.Op == "op-retained-reserve" {
			once.Do(func() {
				close(parked)
				<-release
			})
		}
	}
	jsa.testTransitionPresend.Store(&presend)
	defer jsa.testTransitionPresend.Store(nil)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/rl", qos: 1}}, []byte{1})
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the replay never reached its reservation")
	}
	testMQTTFlush(t, sub, nil, subr)
	close(release)
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/rl", mqttPubQos1|mqttPubFlagRetain, []byte("held"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
}

// T8 (R5) — A REPLAY VERIFIES ITS COPY. A transmitted, unanswered retained delivery's chunk is
// corrupted in the store, then the broker restarts: the resume reads and verifies the copy before
// any acquisition, finds the digest wrong with both records unchanged, and WITHHOLDS the exchange —
// nothing is emitted, the identifier stays reserved.
func TestF105RetainedCorruptCopyIsWithheldAtTheResume(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()
	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "ret-corrupt-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 1, false, true, "rt/corrupt", 1, []byte("intact"))
	testMQTTFlush(t, pub, nil, pubr)
	pub.Close()
	ci := &mqttConnInfo{clientID: "ret-corrupt", cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "rt/corrupt", qos: 1}}, []byte{1})
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "rt/corrupt", mqttPubQos1|mqttPubFlagRetain, []byte("intact"))
	rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
	rig.attach(t)
	reg, _, _ := rig.asm.loadInflightRecord(mqttInflightSubject(rig.idHash, rig.gen, pi))
	if reg == nil {
		t.Fatal("no retained register")
	}
	m, _, merr := rig.asm.jsa.loadRetainedManifest(rig.id, rig.epoch, rig.gen, reg.Payload)
	if merr != nil {
		t.Fatalf("no copy: %v", merr)
	}
	chunk, _ := json.Marshal(&mqttRetainedChunk{Ord: 1, Inc: m.Inc, Data: []byte("tamper")})
	if _, err := rig.asm.jsa.storeMsg(mqttChunkSubject(rig.idHash, rig.gen, reg.Payload, 1), 0, chunk); err != nil {
		t.Fatalf("corrupting the chunk: %v", err)
	}
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	c.Close()
	o.MQTT.Port = -1
	testSetStoreDir(o, dir)
	s = testMQTTRunServer(t, o)
	c, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTExpectNothing(t, r)
	rig.s = s
	rig.attach(t)
	rig.sess.mu.Lock()
	p := rig.sess.pendingPublish[pi]
	reserved := rig.sess.sendReservedLocked(pi) || (rig.sess.custody != nil && rig.sess.custody.holdsPI(pi)) ||
		(p != nil && !p.acts())
	rig.sess.mu.Unlock()
	if !reserved {
		t.Fatalf("the corrupt copy's identifier %d is not reserved after the resume (entry %+v)", pi, p)
	}
	// The exception on its own terms (consult 107): a recorded quarantine of this allocation.
	testRetainedOwnershipHolds(t, rig, false, 4, 2*time.Second)
}

// T25 — ANOTHER BROKER'S ANSWER LANDS BETWEEN THE RESUME'S READS AND ITS VERIFICATION. The other
// broker's act is committed at the classification seam, after the fresh exchange read, through the
// same operations it would run (they are broker-agnostic). Base: a delayed PUBACK ends the exchange
// and the copy is reclaimed, so verification fails on a MOVED pair — the pass re-runs and finds the
// exchange ended: nothing is replayed, nothing stays reserved, a fresh delivery flows. (a): a delayed
// PUBREC moves a QoS 2 exchange past PUBLISH with its copy intact — the pass re-runs on the fresh
// phase and the PUBREL is delivered after CONNACK, the client silent.
func TestF105RetainedResumeReclassifiesOnAConcurrentAnswer(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(map[byte]string{1: "ended and reclaimed", 2: "pubrec"}[qos], func(t *testing.T) {
			r := testRetainedRigUp(t, "ret-t25-"+string('0'+qos), "rt/t25", qos, []byte("raced"))
			defer r.done()
			ci := &mqttConnInfo{clientID: r.id, cleanSess: false}
			c, cr := testMQTTConnect(t, ci, r.o.MQTT.Host, r.o.MQTT.Port)
			testMQTTCheckConnAck(t, cr, mqttConnAckRCConnectionAccepted, false)
			flags := mqttPubQos1
			if qos == 2 {
				flags = mqttPubQoS2
			}
			testMQTTSub(t, 1, c, cr, []*mqttFilter{{filter: "rt/t25", qos: qos}}, []byte{qos})
			pi := testMQTTCheckPubMsgNoAck(t, c, cr, "rt/t25", flags|mqttPubFlagRetain, []byte("raced"))
			r.attach(t)
			subject := mqttInflightSubject(r.idHash, r.gen, pi)
			var reg *mqttInflightRecord
			deadline := time.Now().Add(5 * time.Second)
			for {
				cur, _, _ := r.asm.loadInflightRecord(subject)
				if cur != nil && cur.ActionToken == _EMPTY_ {
					reg = cur
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the register never settled tokenless: %+v", cur)
				}
				time.Sleep(20 * time.Millisecond)
			}
			m, _, _ := r.asm.jsa.loadRetainedManifest(r.id, r.epoch, r.gen, reg.Payload)
			c.Close()

			jsa := &r.asm.jsa
			var fired atomic.Bool
			passes := atomic.Int32{}
			hook := func(subj string) {
				if subj != subject {
					return
				}
				passes.Add(1)
				if !fired.CompareAndSwap(false, true) {
					return
				}
				cur, smsg, _ := r.asm.loadInflightRecord(subject)
				to := mqttExchangeTerminal
				relID := _EMPTY_
				if qos == 2 {
					to, relID = mqttExchangePubRecObserved, mqttNewInflightNonce()
				}
				if _, err := jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
					ID: r.id, Epoch: r.epoch, Txn: mqttNewInflightNonce(), Op: "op-ack-advance", Gen: r.gen,
					Nonce: cur.Lin, To: to, Acq: cur.Acq, Register: cur, RegisterSeq: smsg.Sequence, RelID: relID,
				}); err != nil {
					t.Errorf("the other broker's answer: %v", err)
					return
				}
				if qos == 1 {
					for i := 0; i < 40; i++ {
						if step, _ := jsa.reclaimRetainedPayloadStep(r.asm, r.id, r.epoch, r.gen, m.Inc); step == mqttReclaimDone {
							break
						}
					}
				}
			}
			r.sess.testRetainedClassify.Store(&hook)
			defer r.sess.testRetainedClassify.Store(nil)

			c, cr = testMQTTConnect(t, ci, r.o.MQTT.Host, r.o.MQTT.Port)
			defer c.Close()
			testMQTTCheckConnAck(t, cr, mqttConnAckRCConnectionAccepted, true)
			if !fired.Load() {
				t.Fatal("the resume never classified the retained register: the fixture does not reach the route")
			}
			if passes.Load() < 2 {
				t.Fatalf("the resume classified the register %d time(s): a moved pair must re-run the pass", passes.Load())
			}
			if qos == 2 {
				testMQTTReadPIPacket(mqttPacketPubRel, t, cr, pi)
				testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
				testMQTTFlush(t, c, nil, cr)
				return
			}
			testMQTTExpectNothing(t, cr)
			r.sess.mu.Lock()
			incomplete := r.sess.piSpaceIncomplete
			r.sess.mu.Unlock()
			if incomplete {
				t.Fatal("an ended exchange left the identifier space incomplete")
			}
			pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id + "-live", cleanSess: true}, r.o.MQTT.Host, r.o.MQTT.Port)
			defer pub.Close()
			testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
			testMQTTPublish(t, pub, pubr, 1, false, false, "rt/t25", 2, []byte("fresh"))
			lp := testMQTTCheckPubMsgNoAck(t, c, cr, "rt/t25", mqttPubQos1, []byte("fresh"))
			testMQTTSendPIPacket(mqttPacketPubAck, t, c, lp)
		})
	}
}

// Holistic round 1, finding 2: the admission LANDS but its register can never be read back to
// promote the allocation. A committed admission is never routed through the pre-admission
// abandonment: the connection is closed, and the reconnect's resume restores and sends the copy.
func TestF105RetainedCommittedAdmissionSurvivesReadbackFailures(t *testing.T) {
	r := testRetainedRigUp(t, "ret-adm-readback", "rt/arb", 1, []byte("committed"))
	defer r.done()
	ci := &mqttConnInfo{clientID: r.id, cleanSess: false}
	c, cr := testMQTTConnect(t, ci, r.o.MQTT.Host, r.o.MQTT.Port)
	testMQTTCheckConnAck(t, cr, mqttConnAckRCConnectionAccepted, false)
	r.attach(t)
	var landed atomic.Bool
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if err == nil && at.Attempt != nil && at.Attempt.ID == r.id && at.Attempt.Op == "op-admit-retained" {
			landed.Store(true)
		}
		return seq, err
	}
	r.asm.jsa.testTransitionCommit.Store(&commit)
	defer r.asm.jsa.testTransitionCommit.Store(nil)
	as := r.asm
	r.asm.mu.Lock()
	r.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if landed.Load() && strings.HasSuffix(subject, ".1") {
			return nil, nil, errors.New("the register read failed")
		}
		return as.jsa.loadInflightRecord(subject)
	}
	r.asm.mu.Unlock()
	testMQTTSub(t, 1, c, cr, []*mqttFilter{{filter: "rt/arb", qos: 1}}, []byte{1})
	c.SetReadDeadline(time.Now().Add(30 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("a PUBLISH was sent although the admission's register could not be read")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("the connection was never closed: the committed admission was stranded")
	}
	r.asm.mu.Lock()
	r.asm.testInflightLoad = nil
	r.asm.mu.Unlock()
	c, cr = testMQTTConnect(t, ci, r.o.MQTT.Host, r.o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, cr, mqttConnAckRCConnectionAccepted, true)
	pi := testMQTTCheckPubMsgNoAck(t, c, cr, "rt/arb", mqttPubQos1|mqttPubFlagRetain, []byte("committed"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
}

// Holistic round 2, finding 1: the retained PUBLISH's section has drained its durable action but
// not yet released its local token when the client's PUBREC arrives. The PUBREL's acquisition
// succeeds and its send section is refused by that live local token — which is not a settlement:
// the PUBREL stays owed, and once the PUBLISH section ends it is emitted with no further client
// packet.
func TestF105RetainedPubRelOwedAcrossALivePublishSection(t *testing.T) {
	r := testRetainedRigUp(t, "ret-pubrel-section", "rt/sec", 2, []byte("sec"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	paused := make(chan uint16, 1)
	release := make(chan struct{})
	var once sync.Once
	defer setMQTTTestPublishDrainedPause(func(pi uint16) {
		once.Do(func() {
			paused <- pi
			<-release
		})
	})()
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/sec", qos: 2}}, []byte{2})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/sec", mqttPubQoS2|mqttPubFlagRetain, []byte("sec"))
	select {
	case <-paused:
	case <-time.After(10 * time.Second):
		t.Fatal("the PUBLISH section never reached its drained pause")
	}
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	time.Sleep(300 * time.Millisecond) // the PUBREC's own emission meets the live section
	close(release)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)
}

// Holistic round 2, finding 2: the admission LANDS, its response is lost, and the custody
// settlement cannot resolve it; the next resolution confirms it. Every verdict is classified, so
// the confirmed landing is promoted and delivered — never abandoned as unadmitted.
func TestF105RetainedLateConfirmedAdmissionIsDelivered(t *testing.T) {
	r := testRetainedRigUp(t, "ret-adm-late", "rt/late", 1, []byte("late"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	r.attach(t)
	var lost atomic.Bool
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if err == nil && at.Attempt != nil && at.Attempt.ID == r.id && at.Attempt.Op == "op-admit-retained" &&
			lost.CompareAndSwap(false, true) {
			return 0, errors.New("the response was lost")
		}
		return seq, err
	}
	r.asm.jsa.testTransitionCommit.Store(&commit)
	defer r.asm.jsa.testTransitionCommit.Store(nil)
	var masked atomic.Int32
	r.asm.mu.Lock()
	r.asm.testCustodyResolve = func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool) {
		if h.attempt.Op == "op-admit-retained" && masked.Add(1) <= 2 {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	r.asm.mu.Unlock()
	defer func() {
		r.asm.mu.Lock()
		r.asm.testCustodyResolve = nil
		r.asm.mu.Unlock()
	}()
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/late", qos: 1}}, []byte{1})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/late", mqttPubQos1|mqttPubFlagRetain, []byte("late"))
	if !lost.Load() || masked.Load() < 2 {
		t.Fatalf("the fixture did not reach the window: lost %v, masked settlements %d", lost.Load(), masked.Load())
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)
}
