//go:build mqttport_fulltests

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

package mqtt

import (
	"errors"
	"net"
	"testing"
	"time"
)

// The action-consuming output boundary (F-105, row 2).
//
// Every case here pairs a VALID POSITIVE CONTROL with the illegal variation it is testing, and
// asserts what actually reached the transport rather than only that an error came back — the
// discipline Codex set after four mutations in row 1 pointed at my fixtures instead of my code.

// testFrameConn is a net.Conn that records what was written and can be made to fail at a chosen
// byte, which is how a PARTIAL frame is produced deterministically.
type testFrameConn struct {
	net.Conn
	written  []byte
	failAt   int // fail once this many bytes have been accepted; 0 means never
	closed   bool
	writeErr error
	onWrite  func()
}

func (c *testFrameConn) Write(b []byte) (int, error) {
	if c.onWrite != nil {
		c.onWrite()
	}
	if c.failAt > 0 && len(c.written) >= c.failAt {
		c.writeErr = errors.New("test: the transport refused the rest of the frame")
		return 0, c.writeErr
	}
	n := len(b)
	if c.failAt > 0 && len(c.written)+n > c.failAt {
		n = c.failAt - len(c.written)
	}
	c.written = append(c.written, b[:n]...)
	if n < len(b) {
		c.writeErr = errors.New("test: the transport refused the rest of the frame")
		return n, c.writeErr
	}
	return n, nil
}

// onWrite, when set, runs inside the transport write — which is the only place an execution
// can be observed MID-FLIGHT. Without it "outstanding" is only ever sampled before and after,
// and a guard that marked an acquisition finished the moment it was consumed would pass.
func (c *testFrameConn) Close() error                     { c.closed = true; return nil }
func (c *testFrameConn) SetWriteDeadline(time.Time) error { return nil }

// testFrameClient is a client wired to a recording transport, with an output binding.
func testFrameClient(t *testing.T, failAt int) (*client, *testFrameConn, *mqttConnBinding) {
	t.Helper()
	tc := &testFrameConn{failAt: failAt}
	c := &client{nc: tc, rawConn: tc}
	c.outBind = mqttBindConnection(7)
	return c, tc, c.outBind
}

// testFrameAction is an action bound to the given binding, which is what the boundary requires.
func testFrameAction(b *mqttConnBinding, acq uint64) *mqttOutboundAction {
	return &mqttOutboundAction{
		ID: "client-frame", Epoch: "epoch-1", Gen: testF105Gen, Nonce: "exchange-1", Acq: acq,
		Kind: mqttActionPublish, PI: 7, Run: b.run, Conn: b.conn, Tenure: "tenure-1",
		AcqTxn: "txn-acq",
	}
}

func TestF105FrameWriteConsumesItsActionOnce(t *testing.T) {
	c, tc, bind := testFrameClient(t, 0)
	act := testFrameAction(bind, 1)

	// THE POSITIVE CONTROL: a header and a payload are ONE execution, so both reach the wire
	// under a single consumption. Consuming per write would have consumed on the header and
	// rejected the body, which is why this is asserted on the BYTES and not on the result.
	res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
		Action: act, Binding: bind, Parts: [][]byte{[]byte("HDR"), []byte("PAYLOAD")},
	})
	if err != nil || res != mqttOutputCompleted {
		t.Fatalf("the authorized frame did not complete: %v %v", res, err)
	}
	if string(tc.written) != "HDRPAYLOAD" {
		t.Fatalf("the transport received %q, want the whole frame", string(tc.written))
	}

	// The SAME acquisition a second time is refused, and PUTS NOTHING on the wire. One
	// acquired action is one frame; a second execution is how one acquisition would send two
	// copies.
	before := len(tc.written)
	res, err = c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
		Action: act, Binding: bind, Parts: [][]byte{[]byte("AGAIN")},
	})
	if err == nil || res != mqttOutputPrevented {
		t.Fatalf("a second execution of one acquisition was allowed: %v %v", res, err)
	}
	if len(tc.written) != before {
		t.Fatalf("the refused second execution put %q on the wire", string(tc.written[before:]))
	}

	// A DIFFERENT ACQUISITION for the same exchange is a retransmission and MUST emit. The
	// key is the acquisition, not the exchange; keying on the nonce alone would silently
	// disable every retransmission.
	res, err = c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
		Action: testFrameAction(bind, 2), Binding: bind, Parts: [][]byte{[]byte("RETRY")},
	})
	if err != nil || res != mqttOutputCompleted {
		t.Fatalf("a retransmission under a new acquisition was refused: %v %v", res, err)
	}
	if string(tc.written) != "HDRPAYLOADRETRY" {
		t.Fatalf("the retransmission did not reach the wire: %q", string(tc.written))
	}
}

func TestF105FrameWriteDistinguishesItsThreeOutcomes(t *testing.T) {
	t.Run("a sealed gate PREVENTS, and is not an error", func(t *testing.T) {
		c, tc, bind := testFrameClient(t, 0)
		bind.gate.seal()
		res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
			Action: testFrameAction(bind, 1), Binding: bind, Parts: [][]byte{[]byte("HDR")},
		})
		// PREVENTED is not a failure: nothing was emitted, so the obligation is untouched and
		// a newly acquired action may execute it later. Reporting it as an error would make a
		// torn-down connection look like a lost message.
		if res != mqttOutputPrevented || err != nil {
			t.Fatalf("a sealed gate gave %v %v, want prevented with no error", res, err)
		}
		if len(tc.written) != 0 {
			t.Fatalf("a sealed gate still put %q on the wire", string(tc.written))
		}
	})

	t.Run("a partial write FAILS, seals and closes", func(t *testing.T) {
		// The transport accepts three bytes and then refuses, so the frame is genuinely torn
		// rather than merely rejected.
		c, tc, bind := testFrameClient(t, 3)
		res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
			Action: testFrameAction(bind, 1), Binding: bind,
			Parts: [][]byte{[]byte("HDR"), []byte("PAYLOAD")},
		})
		if res != mqttOutputFailed || err == nil {
			t.Fatalf("a torn frame gave %v %v, want failed with an error", res, err)
		}
		// SEALED BEFORE THE LOCK WAS RELEASED. queueOutbound closes after releasing wmu,
		// which would let another writer begin a second frame on a transport already carrying
		// half of one.
		if bind.mayWrite() {
			t.Fatalf("a torn frame left the output gate open")
		}
		if !tc.closed {
			t.Fatalf("a torn frame left the socket open")
		}
		// AND THE ACTION IS NOT RESET. Bytes may have reached the peer, so this is never read
		// as "never transmitted"; a retransmission acquires a NEW action.
		res, _ = c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
			Action: testFrameAction(bind, 1), Binding: bind, Parts: [][]byte{[]byte("X")},
		})
		if res != mqttOutputPrevented {
			t.Fatalf("the torn frame's action was reusable: %v", res)
		}
	})

	t.Run("a completed frame is not an acknowledgement", func(t *testing.T) {
		c, tc, bind := testFrameClient(t, 0)
		res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
			Action: testFrameAction(bind, 1), Binding: bind, Parts: [][]byte{[]byte("HDR")},
		})
		if res != mqttOutputCompleted || err != nil {
			t.Fatalf("a whole frame gave %v %v", res, err)
		}
		// The gate stays OPEN and the socket stays up: completion is about this frame
		// reaching the transport, and says nothing about the peer.
		if !bind.mayWrite() || tc.closed {
			t.Fatalf("a completed frame disturbed the transport")
		}
	})
}

func TestF105FrameWriteRefusesUnauthorizedOutput(t *testing.T) {
	t.Run("no action at all", func(t *testing.T) {
		c, tc, bind := testFrameClient(t, 0)
		// THERE IS NO MISSING-AUTHORITY FALLBACK. A caller without an action is refused,
		// because a fallback is how a path quietly keeps emitting work the authority never
		// authorized.
		res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{}, Binding: bind, Parts: [][]byte{[]byte("HDR")}})
		if err == nil || res != mqttOutputPrevented {
			t.Fatalf("a QoS 1/2 frame with no action was allowed: %v %v", res, err)
		}
		if len(tc.written) != 0 {
			t.Fatalf("an unauthorized frame reached the wire: %q", string(tc.written))
		}
	})

	t.Run("an action bound to another connection", func(t *testing.T) {
		c, tc, bind := testFrameClient(t, 0)
		other := mqttBindConnection(99)
		// The action is never redirected through a rebound connection: it executes on the one
		// that acquired it or not at all.
		res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
			Action: testFrameAction(other, 1), Binding: bind, Parts: [][]byte{[]byte("HDR")},
		})
		if err == nil || res != mqttOutputPrevented {
			t.Fatalf("an action bound elsewhere executed here: %v %v", res, err)
		}
		if len(tc.written) != 0 {
			t.Fatalf("a foreign action's frame reached the wire: %q", string(tc.written))
		}
	})

	// THE CASE THE SUBTEST ABOVE DOES NOT REACH, and the one that was a real defect.
	//
	// Above, the REQUEST's binding is this client's own and only the action is foreign, so
	// mqttActionBoundTo refuses it. Here the action and the request's binding AGREE with each
	// other and both belong to a different connection — a dead predecessor's matching pair,
	// handed to a successor client. Every check that compares the action to the supplied
	// binding passes, and the only thing standing between that pair and the successor's socket
	// is the destination test.
	//
	// What it must NOT do is as load-bearing as the refusal: not write, not record the
	// consumption in this client's guard (which would make the real owner's later execution
	// look like a duplicate), and not seal the supplied gate (which would tear down a
	// connection nobody was writing through).
	t.Run("a matching pair belonging to another connection", func(t *testing.T) {
		c, tc, _ := testFrameClient(t, 0)
		other := mqttBindConnection(99)
		act := testFrameAction(other, 1)

		res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
			Action: act, Binding: other, Parts: [][]byte{[]byte("HDR"), []byte("PAYLOAD")},
		})
		if err == nil || res != mqttOutputPrevented {
			t.Fatalf("a frame for another connection executed here: %v %v", res, err)
		}
		if len(tc.written) != 0 {
			t.Fatalf("another connection's frame reached this socket: %q", string(tc.written))
		}
		if tc.closed {
			t.Fatal("a refused frame closed this connection's transport")
		}
		if !other.mayWrite() {
			t.Fatal("a refused frame sealed the gate of the connection it named")
		}
		// The owning connection must still be able to execute it. If the refusal had consumed
		// the action into this client's guard the fact would be unobservable there, so the
		// check is made where it is observable: on the real owner.
		owner := &client{nc: &testFrameConn{}, rawConn: &testFrameConn{}}
		owner.outBind = other
		if ores, oerr := owner.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
			Action: act, Binding: other, Parts: [][]byte{[]byte("HDR")},
		}); ores != mqttOutputCompleted || oerr != nil {
			t.Fatalf("the owning connection could not execute its own action after the "+
				"refusal: %v %v", ores, oerr)
		}
	})
}

// TestF105FrameGuardAnswersOutstandingLocally pins the one fact a local caller genuinely knows.
//
// Outstanding is the caller's obligation and a caller that cannot answer must say TRUE, because
// an unknown action is an outstanding one. What this connection CAN establish is narrower and
// is the whole value of tracking execution: an acquisition consumed here whose execution has
// returned will emit nothing more.
//
// Consumption and drain are separate, and this is where that separation is visible: the action
// is consumed the moment the frame starts, and only FINISHED makes it non-outstanding.
func TestF105FrameGuardAnswersOutstandingLocally(t *testing.T) {
	c, _, bind := testFrameClient(t, 0)
	act := testFrameAction(bind, 1)

	// Before anything is acquired here, this connection knows of no execution. That is NOT
	// "not outstanding" in the protocol sense — it is this connection having nothing to say,
	// and the caller is the one that must turn silence into TRUE.
	if st, ok := testRow2GuardState(&c.outFrames, act.Gen, act.Nonce, act.Acq); ok {
		t.Fatalf("an unexecuted acquisition already has a frame state %v here", st)
	}

	res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
		Action: act, Binding: bind, Parts: [][]byte{[]byte("HDR")},
	})
	if err != nil || res != mqttOutputCompleted {
		t.Fatalf("the control frame did not complete: %v %v", res, err)
	}
	// The execution has RETURNED, so this acquisition can emit nothing more.
	if st, ok := testRow2GuardState(&c.outFrames, act.Gen, act.Nonce, act.Acq); !ok || st != mqttFrameFinished {
		t.Fatalf("a finished execution reads %v (present=%v), want FINISHED", st, ok)
	}

	// A TORN frame also ENDS the execution: it may have put bytes on the wire, but it will
	// put no more, and the transport is sealed. Outstanding is about whether output can still
	// execute, which is a different question from whether the obligation is discharged.
	c2, _, bind2 := testFrameClient(t, 3)
	act2 := testFrameAction(bind2, 1)
	if res, _ := c2.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
		Action: act2, Binding: bind2, Parts: [][]byte{[]byte("HDR"), []byte("PAYLOAD")},
	}); res != mqttOutputFailed {
		t.Fatalf("the fixture did not tear the frame: %v", res)
	}
	if st, ok := testRow2GuardState(&c2.outFrames, act2.Gen, act2.Nonce, act2.Acq); !ok || st != mqttFrameFinished {
		t.Fatalf("a torn execution reads %v (present=%v), want FINISHED: it will emit no more", st, ok)
	}
}

// TestF105FrameGuardReportsOutstandingDuringExecution is MUT-F7's oracle.
//
// Consumption and drain are separate facts, and every other test here samples outstanding only
// BEFORE and AFTER a frame — where a guard that marked an acquisition finished the moment it
// was consumed gives exactly the same answers. The difference is visible only from inside the
// execution, so that is where this looks.
func TestF105FrameGuardReportsOutstandingDuringExecution(t *testing.T) {
	c, tc, bind := testFrameClient(t, 0)
	act := testFrameAction(bind, 1)

	var sawOutstanding, sampled bool
	tc.onWrite = func() {
		// Sampled from inside the transport write, so the execution is genuinely in flight:
		// consumed, bytes going out, not yet returned.
		sampled = true
		if st, ok := testRow2GuardState(&c.outFrames, act.Gen, act.Nonce, act.Acq); ok && st == mqttFrameExecuting {
			sawOutstanding = true
		}
	}

	res, err := c.writeAuthorizedFrame(mqttFrameRequest{Holder: mqttTestAdmitHolder{},
		Action: act, Binding: bind, Parts: [][]byte{[]byte("HDR"), []byte("PAYLOAD")},
	})
	if err != nil || res != mqttOutputCompleted {
		t.Fatalf("the control frame did not complete: %v %v", res, err)
	}
	if !sampled {
		t.Fatalf("the fixture never reached the transport, so it sampled nothing")
	}
	if !sawOutstanding {
		t.Fatalf("an execution in flight did not read as outstanding: consumption was folded into drain")
	}
	// And it is no longer outstanding once the execution has returned.
	if st, ok := testRow2GuardState(&c.outFrames, act.Gen, act.Nonce, act.Acq); !ok || st != mqttFrameFinished {
		t.Fatalf("a finished execution reads %v (present=%v), want FINISHED", st, ok)
	}
}
