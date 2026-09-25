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

import "testing"

// Fixtures for the row-2 PUBLISH output tests (F-105 row 2): an ORDINARY send section opened the
// way the delivery callback opens it, on a connection whose transport records every byte.

// testRow2PubSection is one ordinary send section on the real store.
type testRow2PubSection struct {
	e       *testInflightEnv
	tc      *testFrameConn
	pi      uint16
	subject string
	tok     mqttSendToken
}

// testRow2OpenSection admits one ordinary delivery at qos through the authority, accepts the
// fixture connection under the serving entitlement, wires a recording transport, and opens the
// send section with beginSendLocked — so the token carries the section's own output object.
func testRow2OpenSection(t *testing.T, clientID string, qos byte) *testRow2PubSection {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	testReadyForRealPubRelDelivery(t, e)
	pi := e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, qos)
	testInstallPassThroughSeams(e)
	testStampAcceptedConnection(t, e)
	tc := &testFrameConn{}
	e.c.nc, e.c.rawConn = tc, tc
	e.sess.mu.Lock()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	tok, ok := e.sess.beginSendLocked(e.c, pi, testRealPubRelSrcDur, testRealPubRelSrcSeq)
	e.sess.mu.Unlock()
	if !ok || tok.out == nil {
		t.Fatalf("the section did not open with its output object: ok=%v out=%v", ok, tok.out)
	}
	cur, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || cur.State != mqttInflightPublish || mqttDurIsSlotOnlyClass(cur.Dur) {
		t.Fatalf("the fixture must be an ordinary publish slot: %+v (%v)", cur, err)
	}
	return &testRow2PubSection{e: e, tc: tc, pi: pi, subject: subject, tok: tok}
}

// acquire runs the section's durable acquisition and requires it to have landed through the
// authority: an ordinary acquisition carries the exact record its frame will consume.
func (s *testRow2PubSection) acquire(t *testing.T) {
	t.Helper()
	if !s.e.sess.acquirePublishAction(s.e.c, s.pi, &s.tok) {
		t.Fatalf("the ordinary acquisition did not land")
	}
	if s.tok.out.action == nil || s.tok.out.action.Kind != mqttActionPublish || s.tok.out.acq != s.tok.slot.acq {
		t.Fatalf("the landed acquisition is not the section's output: %+v", s.tok.out)
	}
}

// key is the frame-guard key of the section's acquisition at acq.
func (s *testRow2PubSection) key(acq uint64) mqttActionKey {
	return mqttActionKey{gen: s.tok.slot.name.gen, nonce: s.tok.out.lin, acq: acq}
}

// state reads the connection's frame guard for one acquisition.
func (s *testRow2PubSection) state(acq uint64) (mqttFrameState, bool) {
	g := &s.e.c.outFrames
	g.mu.Lock()
	defer g.mu.Unlock()
	st, ok := g.state[s.key(acq)]
	return st, ok
}

// prepare frames a PUBLISH for the section the way the delivery callback does, bound to it.
func (s *testRow2PubSection) prepare(t *testing.T, qos byte) mqttPreparedPublish {
	t.Helper()
	prep, ok := s.e.c.mqttPreparePublishMsgTo(s.e.c, s.pi, qos, s.tok.dup, false, 0, []byte("foo"), []byte("payload"), nil)
	if !ok {
		t.Fatalf("the PUBLISH was not admitted for framing")
	}
	prep.bindSection(s.tok)
	if !prep.requireAction {
		t.Fatalf("an ordinary QoS %d frame must require its acquisition", qos)
	}
	return prep
}

// testRow2Sub is a delivery subscription with nothing buffered.
func testRow2Sub() *subscription { return &subscription{mqtt: &mqttSub{}} }
