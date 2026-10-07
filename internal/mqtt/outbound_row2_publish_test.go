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
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The PUBLISH half of the row-2 output boundary (F-105 row 2, plan-row2.md §6): an ordinary QoS
// 1/2 PUBLISH is the execution of the acquisition that authorized it, consumed once and only for
// its live section, and an acquisition given up before it wrote is local evidence of finish.

// T3 (PUBLISH, immediate adoption). A lost acquisition response is NOT an acquisition: the section
// transmits nothing, and once its own drain proves the token its acquisition is recorded ABANDONED
// at the counter the record carries — never finished by execution, and never unrecorded.
//
// Mutations: M10 (adoption reports acquired), M7c/M7b (abandonment not recorded on discovery).
func TestF105Row2PublishLostResponseIsAbandonedAtTheRecordsCounter(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "row2-pub-lost", 2)
	lost := testLoseNextAcquisitionResponse(t, s.e)
	if s.e.sess.acquirePublishAction(s.e.c, s.pi, &s.tok) {
		t.Fatal("an acquisition whose response was lost reported acquired: nothing may be transmitted under it")
	}
	if !lost.Load() {
		t.Fatal("the fixture never lost an acquisition response, so nothing here is tested")
	}
	if s.tok.out.action != nil {
		t.Fatalf("an adopted acquisition was published as the section's output: %+v", s.tok.out.action)
	}
	cur, _, err := s.e.asm.loadInflightRecord(s.subject)
	if err != nil || cur == nil || cur.ActionToken != s.tok.token {
		t.Fatalf("the landed acquisition is not on the record: %+v (%v)", cur, err)
	}
	if _, seen := s.state(cur.Acq); seen {
		t.Fatal("the acquisition was recorded in the guard before any read proved it this section's")
	}
	s.e.sess.endPublishSend(s.e.c, s.pi, s.tok, mqttIntentNone, false)
	if st, ok := s.state(cur.Acq); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the adopted acquisition at counter %d reads %v (present=%v), want ABANDONED", cur.Acq, st, ok)
	}
	if len(s.tc.written) != 0 {
		t.Fatalf("the lost acquisition put %q on the wire", s.tc.written)
	}
}

// T3 (PUBLISH, unreadable resolution, later reconcile). The resolving read and the section's own
// drain cannot read the register, so nothing is recorded; the shutdown's final pass, with storage
// back, proves the token and records the acquisition ABANDONED at the record's counter.
//
// Mutation: M7c (abandonment not recorded on late discovery).
func TestF105Row2PublishUnresolvedLostResponseIsAbandonedWhenReconciled(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "row2-pub-unresolved", 2)
	lost := testLoseNextAcquisitionResponse(t, s.e)
	var reading atomic.Bool
	reading.Store(true)
	s.e.asm.mu.Lock()
	load := s.e.asm.testInflightLoad
	s.e.asm.testInflightLoad = func(subj string) (*mqttInflightRecord, *StoredMsg, error) {
		if lost.Load() && reading.Load() {
			return nil, nil, errors.New("test: the register is unreadable")
		}
		return load(subj)
	}
	s.e.asm.mu.Unlock()
	if s.e.sess.acquirePublishAction(s.e.c, s.pi, &s.tok) {
		t.Fatal("an unresolved acquisition reported acquired")
	}
	if !lost.Load() {
		t.Fatal("the fixture never lost an acquisition response, so nothing here is tested")
	}
	s.e.sess.endPublishSend(s.e.c, s.pi, s.tok, mqttIntentNone, false)
	cur, _, err := s.e.asm.jsa.loadInflightRecord(s.subject)
	if err != nil || cur == nil || cur.ActionToken != s.tok.token {
		t.Fatalf("the unresolved token must still stand: %+v (%v)", cur, err)
	}
	if _, seen := s.state(cur.Acq); seen {
		t.Fatal("abandonment was recorded without a read proving the token this section's")
	}
	s.e.s.owedDrainsMu.Lock()
	owed := len(s.e.s.owedDrains)
	s.e.s.owedDrainsMu.Unlock()
	if owed != 1 {
		t.Fatalf("the unresolved drain must be owed to the final pass, got %d", owed)
	}
	reading.Store(false)
	s.e.s.finishOwedDrains()
	if st, ok := s.state(cur.Acq); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the reconciled acquisition at counter %d reads %v (present=%v), want ABANDONED", cur.Acq, st, ok)
	}
	if after, _, _ := s.e.asm.jsa.loadInflightRecord(s.subject); after == nil || after.ActionToken != _EMPTY_ {
		t.Fatalf("the final pass did not drain the token: %+v", after)
	}
}

// T4 (PUBLISH). A connection whose accepted tenure no longer serves acquires nothing: no token,
// no action record, no frame — and it asks ONCE, since re-attempting can only ask again under a
// tenure that is not its own.
//
// Mutation: M9 (the acquisition reads the serving authority instead of the connection's own).
func TestF105Row2PublishAcquisitionEndsWithTheConnectionsEntitlement(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "row2-pub-ended", 2)
	ent, ok := s.e.c.outboundEntitlement()
	if !ok {
		t.Fatal("the fixture connection must start accepted")
	}
	// A successor accepted over this connection: its session object still believes it serves,
	// and the store's serving tenure is no longer the one it was accepted under.
	s.e.c.mu.Lock()
	s.e.c.mqtt.outEnt = mqttOutboundEntitlement{Gen: ent.Gen, Tenure: mqttNewInflightNonce()}
	s.e.c.mu.Unlock()
	var loads, commits atomic.Int32
	s.e.asm.mu.Lock()
	load := s.e.asm.testInflightLoad
	s.e.asm.testInflightLoad = func(subj string) (*mqttInflightRecord, *StoredMsg, error) {
		if subj == s.subject {
			loads.Add(1)
		}
		return load(subj)
	}
	s.e.asm.mu.Unlock()
	testSetTransitionCommit(t, s.e, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if testAcquisitionTokenOf(tr) != _EMPTY_ {
			commits.Add(1)
		}
		return send()
	})
	before, _, _ := s.e.asm.jsa.loadInflightRecord(s.subject)
	if s.e.sess.acquirePublishAction(s.e.c, s.pi, &s.tok) {
		t.Fatal("a displaced connection acquired output under a tenure that is not its own")
	}
	if n := commits.Load(); n != 0 {
		t.Fatalf("%d acquisition batch(es) reached the store for a displaced connection", n)
	}
	if n := loads.Load(); n > 2 {
		t.Fatalf("the acquisition re-read the register %d times: an ended entitlement is terminal", n)
	}
	cur, _, _ := s.e.asm.jsa.loadInflightRecord(s.subject)
	if cur == nil || cur.ActionToken != _EMPTY_ || before == nil || cur.Acq != before.Acq {
		t.Fatalf("the refused acquisition moved the register: before %+v after %+v", before, cur)
	}
	if _, err := s.e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttActionSubject(getHash(s.e.sess.id), s.tok.slot.name.gen, cur.Lin, cur.Acq+1)); err == nil {
		t.Fatal("the refused acquisition wrote an action record")
	}
	if s.tok.out.action != nil {
		t.Fatal("the refused acquisition published an output")
	}
	if s.e.c.mqttEnqueuePreparedPublishMsgTo(s.e.c, testRow2Sub(), s.prepare(t, 2)) || len(s.tc.written) != 0 {
		t.Fatalf("a section with no acquisition wrote %q", s.tc.written)
	}
}

// T5. A shared (and, where built, an affine) delivery is slot-only until 7a: it completes end to
// end through the slot machine's own CAS and writes NO action record.
//
// Mutation: M3b (the class gate answers ordinary for shared/affine — the delivery then never
// arrives, since a slot without an exchange acquires nothing through the authority).
func TestF105Row2SlotOnlyDeliveriesWriteNoActionRecord(t *testing.T) {
	noActions := func(t *testing.T, sess *mqttSession, gen, lin string) {
		t.Helper()
		subs, whole, err := sess.jsa.streamSubjectsComplete(mqttInflightStreamName,
			mqttActionWalkFilter(getHash(sess.id), gen, lin))
		if err != nil || !whole {
			t.Fatalf("enumerating the action subjects: whole=%v err=%v", whole, err)
		}
		if len(subs) != 0 {
			t.Fatalf("a slot-only delivery wrote action records: %v", subs)
		}
	}
	register := func(t *testing.T, s *Server, clientID string, pi uint16) (*mqttSession, string, *mqttInflightRecord) {
		t.Helper()
		asm := testMQTTGetAccountSessionManager(t, s, clientID)
		asm.mu.RLock()
		sess := asm.sessions[clientID]
		asm.mu.RUnlock()
		if sess == nil {
			t.Fatal("the subscriber's session must be resident")
		}
		sess.mu.Lock()
		gen := sess.gen
		sess.mu.Unlock()
		reg, _, err := sess.jsa.loadInflightRecord(mqttInflightSubject(getHash(sess.id), gen, pi))
		if err != nil || reg == nil || !mqttDurIsSlotOnlyClass(reg.Dur) {
			t.Fatalf("the fixture must deliver through a slot-only durable: %+v (%v)", reg, err)
		}
		return sess, gen, reg
	}

	// The shared QoS 1 leg is REPLACED BY NAME by TestF105Row2SharedQoS1PublishIsAnAcquiredAction:
	// since 7a a shared delivery is ordinary, so the slot-only class is the experimental affine one.
	t.Run("affine QoS 2", func(t *testing.T) {
		testRequireExperimentalSharedQoS2(t)
		t.Parallel()
		o := testMQTTDefaultOptionsV5()
		o.MQTT.ExperimentalSharedQoS2 = true
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)
		const clientID = "row2-affine-q2"
		sub, subr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer sub.Close()
		testMQTTReadConnAckV5(t, subr)
		testMQTTSubV5(t, sub, subr, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 2}})
		pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer pub.Close()
		testMQTTReadConnAckV5(t, pubr)
		testMQTTPubV5Props(t, pub, pubr, 2, false, 1, "foo", []byte("payload"), nil)
		_, pi := testMQTTReadPublishV5(t, subr, "foo", []byte("payload"))
		sess, gen, reg := register(t, s, clientID, pi)
		testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
		testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
		testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
		testMQTTFlush(t, sub, nil, subr)
		noActions(t, sess, gen, reg.Lin)
	})
}

// A SHARED QoS 1 delivery is ordinary since 7a (it replaces the slot-only test's shared leg): it
// is admitted beside an exchange naming its exact acquisition, its PUBLISH is an acquired action,
// and its PUBACK ends the exchange Terminal — end to end through a real subscription.
//
// Mutations: M1 (the shared branch admits through the pre-F-105 writer: no exchange), M9 (shared
// stays slot-only: no action record, the exchange never reaches Terminal).
func TestF105Row2SharedQoS1PublishIsAnAcquiredAction(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const clientID = "row2-shared-q1"
	sub, subr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer sub.Close()
	testMQTTReadConnAckV5(t, subr)
	testMQTTSubV5(t, sub, subr, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})
	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 1, false, 1, "foo", []byte("payload"), nil)
	_, pi := testMQTTReadPublishV5(t, subr, "foo", []byte("payload"))

	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess := asm.sessions[clientID]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the subscriber's session must be resident")
	}
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	reg, _, err := sess.jsa.loadInflightRecord(mqttInflightSubject(getHash(sess.id), gen, pi))
	if err != nil || reg == nil || !mqttIsSharedDurable(reg.Dur) || mqttDurIsSlotOnlyClass(reg.Dur) {
		t.Fatalf("the fixture must deliver through an ORDINARY shared durable: %+v (%v)", reg, err)
	}
	x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, asm.sessStreamEpoch, gen, reg.Lin)
	if xerr != nil || x == nil || x.SharedAcq == nil || mqttSharedDurableOf(*x.SharedAcq) != reg.Dur {
		t.Fatalf("the shared delivery's exchange does not name its acquisition: %+v (%v)", x, xerr)
	}
	subs, whole, serr := sess.jsa.streamSubjectsComplete(mqttInflightStreamName,
		mqttActionWalkFilter(getHash(sess.id), gen, reg.Lin))
	if serr != nil || !whole || len(subs) == 0 {
		t.Fatalf("the shared PUBLISH wrote no action record: %v whole=%v err=%v", subs, whole, serr)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)
	deadline := time.Now().Add(5 * time.Second)
	for {
		x, _, _ = sess.jsa.loadOutboundExchange(sess.id, asm.sessStreamEpoch, gen, reg.Lin)
		if x != nil && x.Phase == mqttExchangeTerminal {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the shared exchange did not end Terminal on its PUBACK: %+v", x)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// T6 / T8. An ordinary QoS 1 and QoS 2 PUBLISH is ONE execution of its acquisition: the bytes are
// exactly the frame HEAD's raw emitter puts on the wire, the guard then holds that acquisition
// FINISHED, a second execution of it is refused by consumption with nothing further written, and
// the retransmission is a NEW acquisition (Acq+1) consumed in its own right.
//
// Mutations: M8 (ordinary QoS 1/2 written raw — the guard never holds the acquisition), M5
// (consume not consulted — the duplicate is written).
func TestF105Row2OrdinaryPublishExecutesItsAcquisitionOnce(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		qos := qos
		t.Run(fmt.Sprintf("QoS %d", qos), func(t *testing.T) {
			t.Parallel()
			s := testRow2OpenSection(t, fmt.Sprintf("row2-pub-once-%d", qos), qos)
			s.acquire(t)
			prep := s.prepare(t, qos)
			want := append(append([]byte(nil), prep.header...), prep.msg...)
			if !s.e.c.mqttEnqueuePreparedPublishMsgTo(s.e.c, testRow2Sub(), prep) {
				t.Fatal("the authorized PUBLISH was not written")
			}
			if !bytes.Equal(s.tc.written, want) {
				t.Fatalf("the wire carries %x, want the raw emitter's frame %x", s.tc.written, want)
			}
			first := s.tok.out.acq
			if st, ok := s.state(first); !ok || st != mqttFrameFinished {
				t.Fatalf("acquisition %d reads %v (present=%v) after its frame, want FINISHED", first, st, ok)
			}

			// T8: a second execution of the SAME acquisition, from a holder that admits it: only
			// consumption stands in the way, and it must.
			n := len(s.tc.written)
			res, err := s.e.c.writeAuthorizedFrame(mqttFrameRequest{Action: s.tok.out.action, Binding: s.e.c.outBind,
				Holder: mqttTestAdmitHolder{cell: s.tok.out.exec}, Parts: [][]byte{prep.header, prep.msg}})
			if res != mqttOutputPrevented || err == nil || !strings.Contains(err.Error(), "has already executed") {
				t.Fatalf("a second execution of one acquisition gave %v %v, want refused by consumption", res, err)
			}
			if len(s.tc.written) != n {
				t.Fatalf("the refused execution wrote %q", s.tc.written[n:])
			}

			// THE RETRANSMISSION: the section ends (its drain), a new one opens, and it acquires
			// the next counter and consumes that one.
			s.e.sess.endPublishSend(s.e.c, s.pi, s.tok, mqttIntentNone, true)
			s.e.sess.mu.Lock()
			tok2, ok := s.e.sess.beginSendLocked(s.e.c, s.pi, testRealPubRelSrcDur, testRealPubRelSrcSeq)
			s.e.sess.mu.Unlock()
			if !ok {
				t.Fatal("the retransmission's section did not open")
			}
			s.tok = tok2
			s.acquire(t)
			if s.tok.out.acq != first+1 {
				t.Fatalf("the retransmission acquired counter %d, want %d", s.tok.out.acq, first+1)
			}
			prep2 := s.prepare(t, qos)
			if prep2.flags&mqttPubFlagDup == 0 {
				t.Fatal("a retransmission of a transmitted PUBLISH must carry DUP")
			}
			want2 := append(append([]byte(nil), prep2.header...), prep2.msg...)
			if !s.e.c.mqttEnqueuePreparedPublishMsgTo(s.e.c, testRow2Sub(), prep2) {
				t.Fatal("the retransmission was not written")
			}
			if !bytes.Equal(s.tc.written[n:], want2) {
				t.Fatalf("the retransmission wrote %x, want %x", s.tc.written[n:], want2)
			}
			if st, ok := s.state(first + 1); !ok || st != mqttFrameFinished {
				t.Fatalf("the retransmission's acquisition reads %v (present=%v), want FINISHED", st, ok)
			}
		})
	}
}

// T6 (alias on). With an outbound Topic Alias the frame is built under the connection lock at
// transmission, so the reference is HEAD's raw emitter on a twin connection with the same alias
// state: the authorized write must put the identical bytes on the wire, and commit the alias.
func TestF105Row2OrdinaryAliasedPublishIsTheRawEmittersFrame(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "row2-pub-alias", 2)
	s.e.c.mqtt.proto, s.e.c.mqtt.outTopicAliasMax = mqttProtoLevel5, 10
	s.acquire(t)
	prep := s.prepare(t, 2)
	if !prep.useAlias {
		t.Fatal("the fixture must frame with an outbound topic alias")
	}
	if !s.e.c.mqttEnqueuePreparedPublishMsgTo(s.e.c, testRow2Sub(), prep) {
		t.Fatal("the authorized aliased PUBLISH was not written")
	}
	if st, ok := s.state(s.tok.out.acq); !ok || st != mqttFrameFinished {
		t.Fatalf("the aliased frame did not execute its acquisition: %v (present=%v)", st, ok)
	}
	tc2 := &testFrameConn{}
	twin := &client{nc: tc2, rawConn: tc2, outBind: mqttBindConnection(99),
		mqtt: &mqtt{proto: mqttProtoLevel5, outTopicAliasMax: 10}}
	tprep, ok := twin.mqttPreparePublishMsgTo(twin, s.pi, 2, s.tok.dup, false, 0, []byte("foo"), []byte("payload"), nil)
	if !ok || tprep.requireAction {
		t.Fatalf("the reference frame must take the raw emitter: ok=%v", ok)
	}
	if !twin.mqttEnqueuePreparedPublishMsgTo(twin, testRow2Sub(), tprep) {
		t.Fatal("the reference raw frame was not written")
	}
	if !bytes.Equal(s.tc.written, tc2.written) {
		t.Fatalf("the authorized aliased frame %x differs from the raw emitter's %x", s.tc.written, tc2.written)
	}
	s.e.c.mu.Lock()
	alias := s.e.c.mqtt.outTopicAliases["foo"]
	s.e.c.mu.Unlock()
	if alias == 0 {
		t.Fatal("the written aliased frame did not commit its alias binding")
	}
}

// T9 (PUBLISH). A gate sealed before the authorized write prevents it: nothing on the wire, the
// acquisition never consumed — and the section's end then records it ABANDONED.
//
// Mutation: M6 (the gate check removed — the frame is written past the seal).
func TestF105Row2SealedGatePreventsTheAuthorizedPublish(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "row2-pub-sealed", 2)
	s.acquire(t)
	prep := s.prepare(t, 2)
	sealOutput(s.e.c.outBind)
	if s.e.c.mqttEnqueuePreparedPublishMsgTo(s.e.c, testRow2Sub(), prep) {
		t.Fatal("a PUBLISH past a sealed gate reported written")
	}
	if len(s.tc.written) != 0 {
		t.Fatalf("a sealed gate let %q onto the wire", s.tc.written)
	}
	if _, seen := s.state(s.tok.out.acq); seen {
		t.Fatal("a prevented execution consumed its acquisition")
	}
	s.e.sess.endPublishSend(s.e.c, s.pi, s.tok, mqttIntentNone, false)
	if st, ok := s.state(s.tok.out.acq); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the unwritten acquisition reads %v (present=%v) after its section ended, want ABANDONED", st, ok)
	}
}

// T10b (PUBLISH). An ordinary QoS 1/2 frame without its acquisition writes NOTHING — not the frame,
// and not the retained frames buffered ahead of it, which stay buffered for the next write.
//
// Mutation: M13b (missing-action ordinary traffic routed to the raw writer).
func TestF105Row2OrdinaryPublishWithoutItsAcquisitionWritesNothing(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "row2-pub-noaction", 2)
	sub := testRow2Sub()
	sub.mqtt.prm = []mqttRetainedFrame{{header: []byte("RH"), msg: []byte("RM")}}
	prep := s.prepare(t, 2)
	if prep.out == nil || prep.out.action != nil {
		t.Fatal("the fixture must bind a section that holds no acquisition")
	}
	if s.e.c.mqttEnqueuePreparedPublishMsgTo(s.e.c, sub, prep) {
		t.Fatal("an ordinary frame without its acquisition reported written")
	}
	if len(s.tc.written) != 0 {
		t.Fatalf("an unauthorized ordinary frame wrote %q", s.tc.written)
	}
	if len(sub.mqtt.prm) != 1 {
		t.Fatalf("the buffered retained frames were consumed by a refused write: %d left", len(sub.mqtt.prm))
	}
}

// T11. A section whose intent failed never writes; its end is its FIRST drain, and that drain,
// proving the token, records the acquisition ABANDONED in the connection's frame guard, so no saved
// request can later execute it. Before it, the same acquisition is
// outstanding to both.
//
// Mutation: M7b (abandonment not recorded at the section's end).
func TestF105Row2IntentFailedPublishIsAbandonedInTheGuard(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "row2-pub-intent", 2)
	s.acquire(t)
	acq := s.tok.out.acq
	if st, ok := s.state(acq); ok {
		t.Fatalf("an acquired, unwritten acquisition reads %v before anything gave it up", st)
	}
	s.e.sess.endPublishSend(s.e.c, s.pi, s.tok, mqttIntentDefiniteFailure, false)
	if st, ok := s.state(acq); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the intent-failed acquisition reads %v (present=%v), want ABANDONED", st, ok)
	}
	if len(s.tc.written) != 0 {
		t.Fatalf("an intent-failed section wrote %q", s.tc.written)
	}
}

// T11b. The oversize disposal drains in-section, before any output: that drain records the
// acquisition ABANDONED in the frame guard.
//
// Mutation: M7b' (abandonment not recorded on the in-section drain).
func TestF105Row2OversizeDisposedPublishIsAbandonedInTheGuard(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "row2-pub-oversize", 2)
	s.e.c.mqtt.maxPacketSize = 5
	s.acquire(t)
	if _, admitted := s.e.c.mqttPreparePublishMsgTo(s.e.c, s.pi, 2, false, false, 0, []byte("foo"),
		[]byte("payload"), nil); admitted {
		t.Fatal("the fixture must be refused for size, or the oversize route is not reached")
	}
	s.e.sess.drainPublishAction(s.e.c, s.tok)
	if st, ok := s.state(s.tok.out.acq); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the oversize-disposed acquisition reads %v (present=%v), want ABANDONED", st, ok)
	}
}

// T13b. The section's output object is SHARED by every copy of its token, so a drain run on a
// copy closes it for the writer: a saved request for the acquisition is refused by the holder
// alone — the guard is fresh, so consumption would have admitted it — and nothing is written.
//
// Mutation: M16 (drainPublishAction does not close the output).
func TestF105Row2DrainedPublishSectionAdmitsNoSavedRequest(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "row2-pub-closed", 2)
	s.acquire(t)
	copied := s.tok
	s.e.sess.drainPublishAction(s.e.c, copied)
	// A FRESH GUARD: whatever the drain recorded is forgotten, so only the holder can refuse.
	testRow2ResetCell(s.tok.out.exec)
	prep := s.prepare(t, 2)
	res, err := s.e.c.writeAuthorizedFrame(mqttFrameRequest{Action: s.tok.out.action, Binding: s.e.c.outBind,
		Holder: s.tok.out, Parts: [][]byte{prep.header, prep.msg}})
	if res != mqttOutputPrevented || err == nil || !strings.Contains(err.Error(), "not held") {
		t.Fatalf("a saved request after the drain gave %v %v, want refused by its holder", res, err)
	}
	if len(s.tc.written) != 0 {
		t.Fatalf("a drained section's saved request wrote %q", s.tc.written)
	}
	if !s.tok.out.closed.Load() {
		t.Fatal("the drain on a copy did not close the section's shared output")
	}
}

// T11 (7a). A shared QoS 1 delivery survives its member's disconnect and resume as an ordinary
// obligation: the resumed session restores the exchange's custody and identifier, the group source's
// redelivery is retransmitted to it under the SAME identifier with DUP set, and its PUBACK ends the
// exchange Terminal with its acquisition dependency intact and the register deleted.
//
// Single-member by construction: with a second member connected, the group may legitimately
// redeliver the source to it instead, which is not the property this test pins.
func TestSharedQoS1DeliverySurvivesResume(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const clientID = "shared-resume-a"
	props := mqttV5ConnPropsSessionExpiry(60)
	a, ar := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true, props: props}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, ar)
	testMQTTSubV5(t, a, ar, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})
	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 1, false, 1, "foo", []byte("held"), nil)
	_, pi := testMQTTReadPublishV5(t, ar, "foo", []byte("held"))

	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess := asm.sessions[clientID]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the member's session must be resident")
	}
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	reg, _, err := sess.jsa.loadInflightRecord(mqttInflightSubject(getHash(clientID), gen, pi))
	if err != nil || reg == nil || !mqttIsSharedDurable(reg.Dur) {
		t.Fatalf("the shared delivery's register: %+v (%v)", reg, err)
	}
	lin := reg.Lin
	// Disconnect WITHOUT acknowledging: the exchange is possibly transmitted and must survive.
	a.Close()

	a2, ar2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: false, props: props}, o.MQTT.Host, o.MQTT.Port)
	defer a2.Close()
	testMQTTReadConnAckV5(t, ar2)
	flags, rpi := testReadPublishHeaderV5(t, ar2, "foo")
	if rpi != pi {
		t.Fatalf("the resumed retransmission carries identifier %d, want the original %d", rpi, pi)
	}
	if flags&mqttPubFlagDup == 0 {
		t.Fatal("the resumed retransmission of a transmitted shared delivery is not marked DUP")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, a2, pi)
	testMQTTFlush(t, a2, nil, ar2)

	asm = testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess = asm.sessions[clientID]
	asm.mu.RUnlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		x, _, _ := sess.jsa.loadOutboundExchange(sess.id, asm.sessStreamEpoch, gen, lin)
		r, _, _ := sess.jsa.loadInflightRecord(mqttInflightSubject(getHash(clientID), gen, pi))
		if x != nil && x.Phase == mqttExchangeTerminal && x.SharedAcq != nil && r == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the resumed PUBACK: exchange=%+v register=%+v", x, r)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// testReadPublishHeaderV5 reads one v5 PUBLISH and returns its fixed-header byte (DUP, QoS and RETAIN
// flags included) and its packet identifier.
func testReadPublishHeaderV5(t *testing.T, r *mqttReader, expTopic string) (byte, uint16) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketPub {
		t.Fatalf("Expected PUBLISH (%x), got %x", mqttPacketPub, pt)
	}
	start := r.pos
	defer func() { r.pos = start + pl }()
	topic, err := r.readBytes("topic", false)
	if err != nil || string(topic) != expTopic {
		t.Fatalf("PUBLISH topic %q (%v), want %q", topic, err, expTopic)
	}
	var pi uint16
	if mqttGetQoS(b&mqttPacketFlagMask) > 0 {
		if pi, err = r.readUint16("pi"); err != nil {
			t.Fatalf("Error reading pi: %v", err)
		}
	}
	return b, pi
}

// Consult 85. At Receive Maximum 1 the resumed retransmission must still progress: the original
// exchange's charge occupies the only slot, and the redelivery is that exchange's retransmission —
// not a new admission, which would have no slot to take.
func TestSharedQoS1ResumeRetransmitsAtReceiveMaximumOne(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const clientID = "shared-resume-rm1"
	pw := newMQTTWriter(0)
	pw.WriteByte(mqttPropSessionExpiry)
	pw.WriteUint32(60)
	pw.WriteByte(mqttPropReceiveMaximum)
	pw.WriteUint16(1)
	props := pw.Bytes()
	a, ar := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true, props: props}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, ar)
	testMQTTSubV5(t, a, ar, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})
	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 1, false, 1, "foo", []byte("held"), nil)
	_, pi := testMQTTReadPublishV5(t, ar, "foo", []byte("held"))
	a.Close()
	a2, ar2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: false, props: props}, o.MQTT.Host, o.MQTT.Port)
	defer a2.Close()
	testMQTTReadConnAckV5(t, ar2)
	flags, rpi := testReadPublishHeaderV5(t, ar2, "foo")
	if rpi != pi || flags&mqttPubFlagDup == 0 {
		t.Fatalf("at Receive Maximum 1 the resumed retransmission is pi %d dup=%v, want pi %d dup=true",
			rpi, flags&mqttPubFlagDup != 0, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, a2, pi)
}

// Consult 85, the redistribution witness: what 7a does NOT fix. While a member is away, the group
// may redeliver its outstanding source to ANOTHER member, which acknowledges it. That acknowledgement
// is a fact about the source, not about the away member's exchange: its exchange stays at Publish,
// charged and reserved — the stated gap (docs/CONFORMANCE.md) — and is never completed by B's ACK.
func TestSharedRedistributionLeavesTheAwayMembersExchangeOutstanding(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 300 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const clientID = "shared-redistribute-a"
	props := mqttV5ConnPropsSessionExpiry(60)
	a, ar := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true, props: props}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, ar)
	testMQTTSubV5(t, a, ar, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})
	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 1, false, 1, "foo", []byte("held"), nil)
	_, pi := testMQTTReadPublishV5(t, ar, "foo", []byte("held"))
	asm := testMQTTGetAccountSessionManager(t, s, clientID+"-pub")
	asm.mu.RLock()
	sessA := asm.sessions[clientID]
	asm.mu.RUnlock()
	sessA.mu.Lock()
	gen := sessA.gen
	sessA.mu.Unlock()
	reg, _, err := sessA.jsa.loadInflightRecord(mqttInflightSubject(getHash(clientID), gen, pi))
	if err != nil || reg == nil {
		t.Fatalf("A's register: %+v %v", reg, err)
	}
	lin := reg.Lin
	a.Close()

	b, br := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "-b", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer b.Close()
	testMQTTReadConnAckV5(t, br)
	testMQTTSubV5(t, b, br, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})
	_, _, _, bpi, ok := testMQTTTryReadPublishV5(br, 5*time.Second)
	if !ok {
		t.Fatal("the group did not redeliver A's outstanding source to B")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, b, bpi)
	testMQTTFlush(t, b, nil, br)
	time.Sleep(300 * time.Millisecond)

	x, _, xerr := sessA.jsa.loadOutboundExchange(sessA.id, asm.sessStreamEpoch, gen, lin)
	if xerr != nil || x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("A's exchange after B acknowledged the source: %+v (%v); B's ACK must not complete it", x, xerr)
	}
	sessA.mu.Lock()
	held := sessA.pendingPublish[pi] != nil
	sessA.mu.Unlock()
	if !held {
		t.Fatal("A's outstanding identifier was released by another member's acknowledgement")
	}
}
