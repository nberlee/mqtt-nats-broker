//go:build mqtt_fulltests

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
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// T-late-frame-carryover (issue #133, restated by consult 13b): an action acquired on broker A
// before a Clean Start on broker B discards its generation may still execute afterwards, on its
// original connection with its original packet identifier (docs/CONFORMANCE.md, "A displaced
// connection may receive a late packet"); the record-derived displacement may close that
// connection first. Either outcome is permitted, a read timeout is neither, and in both the old
// action delivers nothing to the successor and moves none of its state. The reclamation adds no
// barrier: the discarded generation is noncurrent and Outstanding open, not retired, while the
// action is out. No mutation: it pins carried-over behavior and generation isolation.
func TestAnActionAcquiredBeforeARemoteCleanStartStillWritesItsFrame(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	defer bA.cleanup()
	bB := c.startBroker(t, 1)
	defer bB.cleanup()
	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	const cid = "late-frame-carryover"
	const topic = "late/frame"
	payload := []byte("acquired-before-the-discard")

	old, rOld := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(3600)}, bA.opts.MQTT.Host, bA.opts.MQTT.Port)
	defer old.Close()
	testMQTTReadConnAckV5(t, rOld)
	testMQTTSubV5(t, old, rOld, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})

	asmA := testMQTTGetGlobalASM(t, bA.s)
	epoch := testAccountEpoch(asmA)
	before := asmA.jsa.loadOutboundAuthority(cid, epoch)
	if before.status != mqttOutboundValid || before.rec == nil || before.rec.Life == nil ||
		before.rec.State != mqttAuthorityServing {
		t.Fatalf("fixture: the subscriber's authority reads %v, want serving", before.status)
	}
	gen := before.rec.Gen
	_, ord, ok := mqttParseGenToken(gen)
	if !ok {
		t.Fatalf("fixture: generation %q names no lifecycle", gen)
	}

	// The delivery is parked at RESERVED: its durable action is acquired and confirmed,
	// nothing is recorded as intent and nothing is written.
	var once sync.Once
	parked := make(chan uint16, 1)
	release := make(chan struct{})
	releaseOnce := func() { once.Do(func() { close(release) }) }
	defer releaseOnce()
	hook := func(sess *mqttSession, pi uint16, window string) bool {
		if sess.id == cid && window == mqttMarkerSendReserved {
			select {
			case parked <- pi:
				<-release
			default:
			}
		}
		return true
	}
	mqttTestMarkerSendWindowHookV.Store(&hook)
	defer mqttTestMarkerSendWindowHookV.Store(nil)

	pub, rPub := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid + "-pub", cleanStart: true},
		bA.opts.MQTT.Host, bA.opts.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, rPub)
	testMQTTPublishV5(t, pub, 1, 1, topic, payload)
	testMQTTReadPubAckV5(t, rPub, 1)

	var pi uint16
	select {
	case pi = <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the delivery never reached its send section")
	}

	// The Clean Start on broker B, while the action is out.
	succ, rSucc := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true},
		bB.opts.MQTT.Host, bB.opts.MQTT.Port)
	defer succ.Close()
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		al := asmA.jsa.loadOutboundAuthority(cid, epoch)
		if al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil {
			return fmt.Errorf("the authority reads %v", al.status)
		}
		if al.rec.Gen == gen && al.rec.State == mqttAuthorityServing {
			return errors.New("the successor has not displaced the generation yet")
		}
		return nil
	})

	// AT RESUME the discarded generation is noncurrent, Outstanding open and not retired.
	al := asmA.jsa.loadOutboundAuthority(cid, epoch)
	i := mqttOutstandingIndex(al.rec.Life, ord)
	if i < 0 {
		t.Fatalf("the discarded generation %q is not Outstanding after the Clean Start", gen)
	}
	if ph := al.rec.Life.Outstanding[i].Phase; ph != mqttOutstandingOpen {
		t.Fatalf("the discarded generation %q stands at phase %d while its action is out, want open", gen, ph)
	}
	if s, _ := asmA.genStanding(cid, gen); s != mqttGenOutstanding {
		t.Fatalf("the discarded generation %q reads %v while its action is out, want outstanding", gen, s)
	}

	// The successor is accepted while the action is still out, and its state is what the old
	// action must not touch.
	if _, rc, _ := testMQTTReadConnAckV5(t, rSucc); rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("the successor's Clean Start was answered %#x", rc)
	}
	testMQTTSubV5(t, succ, rSucc, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	idHash := getHash(cid)
	successor := asmA.jsa.loadOutboundAuthority(cid, epoch)
	if successor.status != mqttOutboundValid || successor.rec.State != mqttAuthorityServing || successor.rec.Gen == gen {
		t.Fatalf("fixture: the successor's authority reads %v %+v", successor.status, successor.rec)
	}
	succRecords := func() map[string]uint64 {
		out := map[string]uint64{}
		for _, f := range mqttGenerationPurgeFilters(idHash, successor.rec.Gen) {
			subjects, _, err := asmA.jsa.streamSubjects(mqttSessStreamName, f)
			if err != nil {
				t.Fatalf("listing %q: %v", f, err)
			}
			for subj, n := range subjects {
				out[subj] = n
			}
		}
		return out
	}
	succBefore := succRecords()

	releaseOnce()
	// EITHER OUTCOME IS PERMITTED: the late frame on the original connection under its original
	// identifier, or the displacement's close winning first. A read timeout is neither.
	received, closed := testReadLateFrameOrClose(t, old, topic)
	switch {
	case received != nil:
		if received.qos != 1 || received.pi != pi {
			t.Fatalf("the late frame arrived as QoS %d identifier %d, want QoS 1 under its original identifier %d",
				received.qos, received.pi, pi)
		}
	case !closed:
		t.Fatal("the old connection neither received the late frame nor was closed")
	}

	// GENERATION ISOLATION: the old action delivers nothing to the successor and moves none of
	// its state.
	_ = succ.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if n, err := succ.Read(one[:]); n != 0 || !testIsTimeout(err) {
		t.Fatalf("the successor received a packet (first byte %#x, %v) from the old generation's action", one[0], err)
	}
	after := asmA.jsa.loadOutboundAuthority(cid, epoch)
	if after.status != mqttOutboundValid || after.rec.Gen != successor.rec.Gen || after.rec.State != mqttAuthorityServing ||
		after.rec.Life.Accept != successor.rec.Life.Accept {
		t.Fatalf("the old action moved the successor's authority: %+v -> %+v", successor.rec, after.rec)
	}
	if got := succRecords(); fmt.Sprint(got) != fmt.Sprint(succBefore) {
		t.Fatalf("the old action changed the successor's records: %v -> %v", succBefore, got)
	}
}

// testLateFrame is one PUBLISH read off the wire.
type testLateFrame struct {
	qos byte
	pi  uint16
}

// testReadLateFrameOrClose reads the old connection's next packet: a PUBLISH on topic is the late
// frame; a DISCONNECT or the connection ending is positive evidence of the displacement's close.
// A timeout fails the test.
func testReadLateFrameOrClose(t *testing.T, conn net.Conn, topic string) (*testLateFrame, bool) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)
	b, err := br.ReadByte()
	switch {
	case err != nil && testIsTimeout(err):
		t.Fatalf("no packet and no close on the old connection: %v", err)
	case err != nil:
		return nil, true // the connection ended
	case b&mqttPacketMask == mqttPacketDisconnect:
		return nil, true
	case b&mqttPacketMask != mqttPacketPub:
		t.Fatalf("the old connection read packet %#x, want a PUBLISH or the close", b)
	}
	rl, mult := 0, 1
	for {
		c, err := br.ReadByte()
		if err != nil {
			t.Fatalf("reading the PUBLISH length: %v", err)
		}
		rl += int(c&0x7f) * mult
		if c&0x80 == 0 {
			break
		}
		mult *= 128
	}
	body := make([]byte, rl)
	if _, err := io.ReadFull(br, body); err != nil {
		t.Fatalf("reading the PUBLISH body: %v", err)
	}
	tl := int(binary.BigEndian.Uint16(body))
	if got := string(body[2 : 2+tl]); got != topic {
		t.Fatalf("the late PUBLISH is on %q, want %q", got, topic)
	}
	f := &testLateFrame{qos: (b >> 1) & 3}
	if f.qos > 0 {
		f.pi = binary.BigEndian.Uint16(body[2+tl:])
	}
	return f, false
}
