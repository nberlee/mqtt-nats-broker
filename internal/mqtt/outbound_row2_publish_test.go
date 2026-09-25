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

	t.Run("shared QoS 1", func(t *testing.T) {
		t.Parallel()
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
		sess, gen, reg := register(t, s, clientID, pi)
		noActions(t, sess, gen, reg.Lin)
		testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
		testMQTTFlush(t, sub, nil, subr)
		noActions(t, sess, gen, reg.Lin)
	})

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
				Holder: mqttTestAdmitHolder{}, Parts: [][]byte{prep.header, prep.msg}})
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
// proving the token, records the acquisition ABANDONED — which the connection's local drain oracle
// and the whole-exchange discharge proof then read as finished. Before it, the same acquisition is
// outstanding to both.
//
// Mutation: M7b (abandonment not recorded at the section's end).
func TestF105Row2IntentFailedPublishIsFinishedToTheOracle(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "row2-pub-intent", 2)
	s.acquire(t)
	gen, lin, acq := s.tok.slot.name.gen, s.tok.out.lin, s.tok.out.acq
	oracle := s.e.c.localDrainOracle()
	prove := func() mqttDischargeProof {
		t.Helper()
		p, err := s.e.asm.proveExchangeDischarged(s.e.sess.id, s.e.asm.sessStreamEpoch, gen, lin, oracle)
		if err != nil {
			t.Fatalf("proving discharge: %v", err)
		}
		return p
	}
	if oracle(gen, lin, acq) || prove().Discharged() {
		t.Fatal("an acquired, unwritten acquisition read as finished before anything gave it up")
	}
	s.e.sess.endPublishSend(s.e.c, s.pi, s.tok, mqttIntentDefiniteFailure, false)
	if st, ok := s.state(acq); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the intent-failed acquisition reads %v (present=%v), want ABANDONED", st, ok)
	}
	if !oracle(gen, lin, acq) {
		t.Fatal("the local drain oracle does not read the abandoned acquisition as finished")
	}
	if p := prove(); !p.Complete || !p.Discharged() {
		t.Fatalf("the discharge proof does not count the abandoned acquisition finished: %+v", p)
	}
	if len(s.tc.written) != 0 {
		t.Fatalf("an intent-failed section wrote %q", s.tc.written)
	}
}

// T11b. The oversize disposal drains in-section, before any output: that drain records the
// acquisition ABANDONED, and the oracle reads it finished.
//
// Mutation: M7b' (abandonment not recorded on the in-section drain).
func TestF105Row2OversizeDisposedPublishIsFinishedToTheOracle(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "row2-pub-oversize", 2)
	s.e.c.mqtt.maxPacketSize = 5
	s.acquire(t)
	if _, admitted := s.e.c.mqttPreparePublishMsgTo(s.e.c, s.pi, 2, false, false, 0, []byte("foo"),
		[]byte("payload"), nil); admitted {
		t.Fatal("the fixture must be refused for size, or the oversize route is not reached")
	}
	s.e.sess.drainPublishAction(s.e.c, s.tok)
	gen, lin, acq := s.tok.slot.name.gen, s.tok.out.lin, s.tok.out.acq
	if st, ok := s.state(acq); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the oversize-disposed acquisition reads %v (present=%v), want ABANDONED", st, ok)
	}
	if !s.e.c.localDrainOracle()(gen, lin, acq) {
		t.Fatal("the local drain oracle does not read the oversize-disposed acquisition as finished")
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
	s.e.c.outFrames.mu.Lock()
	s.e.c.outFrames.state = nil
	s.e.c.outFrames.mu.Unlock()
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
