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
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// mqttTestWriterPreLockHookV runs on a writer immediately before it takes wmu, in both output
// writers (writeAuthorizedFrame and writeSocket), so a test can establish that a writer has passed
// every check that precedes the lock before it seals the gate. nil in production.
var mqttTestWriterPreLockHookV atomic.Pointer[func(c *client)]

// mqttTestTornWriterUnlockedHookV runs on a torn writer immediately after it releases wmu, so a
// test can let a queued writer run in the interval between that release and the close. nil in
// production.
var mqttTestTornWriterUnlockedHookV atomic.Pointer[func(c *client)]

// The action-consuming output boundary (F-105, row 2).
//
// The conversion's structural property is not about a deleted symbol, because four of the
// production emitters never called the one that was deleted — the retained flush inside
// mqttEnqueuePreparedPublishMsgTo, reachable even from a QoS 0 delivery; the SUBACK tail; and
// both isNew==false retransmission branches. The property is about EFFECTS:
//
//	Every QoS 1/2 emission consumes its own acquired action, bound to its exchange and its
//	destination.
//
// Asserting that the emitters call this API is the wrong direction and was refused: a permitted
// emitter can call it AND call queueOutbound directly, leaving the caller set unchanged. So the
// enforcement is at the LOWER boundary — a QoS 1/2 frame cannot reach the raw writer without
// its action — and actionless output (control packets and QoS 0) takes an explicitly
// constrained path of its own.
//
// # What "consumed once" means, and what it does not
//
// ONE LOGICAL FRAME, not one writeSocket call and not one net.Conn.Write. The existing emitters
// send a PUBLISH header and its body separately, and the retained flush does the same, so
// consuming per invocation would consume on the header and reject the body. Header, payload and
// any continuation of remaining bytes are one execution.
//
// CONSUMPTION IS NOT DRAIN. The action stays outstanding while its execution can still emit;
// what consumption excludes is a SECOND execution of the same acquisition, which is how one
// acquired action would otherwise put two copies on the wire.
//
// # Why the three-way result exists
//
// writeSocket returns nil both for a missing socket and for a sealed gate, and an error only
// for an attempted write, so it cannot distinguish the three outcomes a completion needs:
// PREVENTED (nothing was emitted and the obligation is untouched), COMPLETED (the whole frame
// reached the transport, which is still not peer acknowledgement), and FAILED (bytes may have
// gone out and the transport is no longer trustworthy).
//
// On a failure the frame is not retried with the same action: the transport is sealed so no
// second frame can begin on a socket carrying half of one, then closed. A later protocol
// retransmission acquires a NEW action, because an action is one acquisition and a
// retransmission is a different one.

// mqttOutputResult is the three-way answer a frame execution gives its caller.
type mqttOutputResult uint8

const (
	// mqttOutputPrevented: nothing was emitted. The gate was sealed, the socket is gone, or
	// the action is not bound to this connection. The obligation is untouched and may be
	// executed later by a newly acquired action.
	mqttOutputPrevented mqttOutputResult = iota + 1
	// mqttOutputCompleted: every byte of the frame reached the transport. NOT an
	// acknowledgement, and not proof the peer saw it.
	mqttOutputCompleted
	// mqttOutputFailed: a write was attempted and did not complete. Bytes MAY have reached
	// the peer, so this is never read as "never transmitted"; the transport is sealed and
	// closed, and the obligation stays outstanding.
	mqttOutputFailed
)

func (r mqttOutputResult) String() string {
	switch r {
	case mqttOutputPrevented:
		return "prevented"
	case mqttOutputCompleted:
		return "completed"
	case mqttOutputFailed:
		return "failed"
	}
	return "unknown"
}

// mqttActionKey identifies one ACQUISITION, which is what may be executed once.
//
// The exchange nonce alone is not it: a retransmission is a distinct action for the same
// exchange, and must be allowed to emit. Acq is what separates them.
type mqttActionKey struct {
	gen   string
	nonce string
	acq   uint64
}

func mqttActionKeyOf(a *mqttOutboundAction) mqttActionKey {
	if a == nil {
		return mqttActionKey{}
	}
	return mqttActionKey{gen: a.Gen, nonce: a.Nonce, acq: a.Acq}
}

// mqttExecCell records how far ONE acquisition's execution has got: the consume-once fact.
//
// PER HOLDER, not per connection. Each acquisition has exactly one holder object — a PUBLISH send
// section's output, or a PUBREL action holder — which is never reused for another acquisition, and
// every value copy of that holder and every write ticket it issues points to this one cell. So the
// fact lives exactly as long as anything can still present the acquisition for execution: when the
// holder is gone, nothing can execute under it and the cell goes with it. A connection-wide record
// would instead outlive every acquisition it ever saw (the integrated gate's finding: one entry per
// transmission, retained for the life of the connection). This bounds execution history only; a
// prevented PUBREL's PUBCOMP debt on its send hold is F-116.
//
// conn is the connection the holder was acquired for, fixed at creation — an action is never
// redirected through another one. The cell is process-local and deliberately not durable: an
// immutable action record cannot say whether its execution finished.
type mqttExecCell struct {
	conn *client

	mu sync.Mutex
	// st is never reset: a consumed or abandoned acquisition stays so, which is what refuses a saved
	// request presented again.
	st mqttFrameState
	// key is the acquisition the transition recorded. Inspection only — the holder's admission has
	// already pinned the identity.
	key mqttActionKey
}

// mqttTestExecCellCreatedHookV observes every execution cell as it is created, so a test can read an
// acquisition's cell after its holder has gone. nil in production.
var mqttTestExecCellCreatedHookV atomic.Pointer[func(x *mqttExecCell)]

func mqttNewExecCell(c *client) *mqttExecCell {
	if c == nil {
		return nil
	}
	x := &mqttExecCell{conn: c}
	if h := mqttTestExecCellCreatedHookV.Load(); h != nil {
		(*h)(x)
	}
	return x
}

// mqttFrameState is how far one acquisition's execution has got on its connection.
type mqttFrameState uint8

const (
	// mqttFrameExecuting: consumed, and bytes may still be going out.
	mqttFrameExecuting mqttFrameState = iota + 1
	// mqttFrameFinished: the execution returned. Output for this acquisition can no longer
	// execute — whether it completed or tore, both of which end the execution.
	mqttFrameFinished
	// mqttFrameAbandoned: the acquisition was given up before it executed, and it never will —
	// proven this holder's by a read that found its exact token, or by an entry that ended without
	// consuming it. Output for it can no longer execute, exactly as for a finished one, and consume
	// refuses it.
	mqttFrameAbandoned
)

// consume claims the single execution of the acquisition, reporting whether this caller won it.
func (x *mqttExecCell) consume(k mqttActionKey) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.st != 0 {
		return false
	}
	x.st, x.key = mqttFrameExecuting, k
	return true
}

// abandon records that the acquisition will never execute, and reports whether it did. Only an
// untouched cell is abandoned: an executing or finished one is NOT, and overwriting it would declare
// output finished that may still be going out.
func (x *mqttExecCell) abandon(k mqttActionKey) bool {
	if x == nil {
		return false
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.st != 0 {
		return false
	}
	x.st, x.key = mqttFrameAbandoned, k
	return true
}

// finish records that the execution has returned. Called only by the consumer that won it.
func (x *mqttExecCell) finish() {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.st = mqttFrameFinished
}

// mqttFrameRequest is one QoS 1/2 frame execution.
type mqttFrameRequest struct {
	// Action is the acquisition being executed. Required: a QoS 1/2 frame with no action is
	// output nothing authorized, and there is deliberately no fallback — a fallback is how a
	// path quietly keeps emitting work the authority never authorized.
	Action *mqttOutboundAction
	// Binding is the connection the action was acquired on. The action is never redirected
	// through a rebound one.
	Binding *mqttConnBinding
	// Holder is the acquisition's live holder, asked under wmu whether this execution is still its
	// to perform (outbound_output_holder.go). Required: consuming the record alone would let a saved
	// request execute an acquisition its holder already gave up.
	Holder mqttOutputHolder
	// Parts are the frame's bytes in order — header, then payload. They are ONE execution.
	Parts [][]byte
	// Cap is an optional absolute write deadline, for the shutdown drain.
	Cap time.Time
}

// writeAuthorizedFrame executes one acquired action's frame.
//
// The sequencing is the mechanism, and it is the same pairing the output gate already relies
// on: the consumption and the gate check happen under wmu, immediately before the first byte,
// so a duplicate attempt and the closure barrier are both serialized against this execution.
// Testing either outside that lock would establish neither.
func (c *client) writeAuthorizedFrame(r mqttFrameRequest) (mqttOutputResult, error) {
	if r.Action == nil {
		return mqttOutputPrevented, fmt.Errorf("a QoS 1/2 frame was offered with no acquired action")
	}
	if r.Binding == nil {
		return mqttOutputPrevented, fmt.Errorf("action %q/%d was offered with no connection binding",
			r.Action.Nonce, r.Action.Acq)
	}
	// THE BINDING MUST BE THIS CONNECTION'S OWN, and it is checked FIRST because it is the
	// only thing that makes the check below mean anything.
	//
	// The bytes go to c.nc, the consumption is recorded in a cell bound to c, and a torn frame
	// seals r.Binding.gate — three facts about THIS client. An earlier revision validated
	// only that the action matched the SUPPLIED binding, so a caller handing a successor
	// client a matching (action, binding) pair from a dead predecessor passed validation and
	// wrote the successor's socket: past its own sealed gate, recorded in the wrong guard,
	// and sealing a gate nobody was writing through. Pointer identity is the right test
	// because outBind is assigned once at construction and never replaced (shim_engine.go),
	// so the binding a connection owns IS one object.
	//
	// A client with no outBind REFUSES rather than writes. Such a client is built outside
	// createMQTTClient, so it has no output identity at all, and an authorized frame cannot
	// be authorized against nothing. There is deliberately no fallback: a fallback here is
	// how a path quietly keeps emitting work the authority never authorized.
	if c.outBind == nil || r.Binding != c.outBind {
		return mqttOutputPrevented, fmt.Errorf("action %q/%d was offered with a binding that is "+
			"not this connection's output identity: a frame is written to the connection that "+
			"acquired it, never to whichever one it was handed to",
			r.Action.Nonce, r.Action.Acq)
	}
	// THE BINDING IS CHECKED, NOT THE AUTHORITY. An action acquired before a retirement still
	// executes, on its ORIGINAL connection, with its original exchange and identifier binding.
	// Requiring a serving tenure here would make transfer and closure destroy the completion
	// route of everything admitted before them, which is the defect this mechanism removes.
	if !mqttActionBoundTo(r.Action, r.Binding) {
		return mqttOutputPrevented, fmt.Errorf("action %q/%d is bound to another connection",
			r.Action.Nonce, r.Action.Acq)
	}

	if h := mqttTestWriterPreLockHookV.Load(); h != nil {
		(*h)(c)
	}
	c.wmu.Lock()
	// A SEALED GATE LEGITIMATELY PREVENTS EXECUTION, and generation retirement alone does
	// not. Checked under wmu because certification seals the gate and then crosses this lock:
	// a writer arriving after the seal declines, and one that arrived before it has returned
	// before certification completes.
	if !r.Binding.mayWrite() || c.nc == nil {
		c.wmu.Unlock()
		return mqttOutputPrevented, nil
	}
	// THE HOLDER STILL AUTHORIZES THIS EXECUTION, asked after the gate and before consumption. It
	// reads only execution-private state (the PUBREL entry ticket, the PUBLISH section's output),
	// never shared holder fields, so nothing here reaches back for the session lock.
	// Admission FIRST, then the holder's execution cell: a holder that does not admit this
	// execution — a nil one included — is refused before anything else is asked of it.
	var cell *mqttExecCell
	if r.Holder != nil && r.Holder.enterWrite(r.Action, c) {
		cell = r.Holder.execCell()
	}
	if cell == nil || cell.conn != c {
		c.wmu.Unlock()
		return mqttOutputPrevented, fmt.Errorf("action %q/%d is not held for this execution",
			r.Action.Nonce, r.Action.Acq)
	}
	// CONSUMED ONCE, for the WHOLE frame. Taken here rather than per write so a header and
	// its payload are one execution; taken before the first byte so a duplicate attempt
	// cannot interleave between them.
	if !cell.consume(mqttActionKeyOf(r.Action)) {
		c.wmu.Unlock()
		return mqttOutputPrevented, fmt.Errorf("action %q/%d has already executed",
			r.Action.Nonce, r.Action.Acq)
	}

	var werr error
	for _, part := range r.Parts {
		if werr = c.writeFramePartLocked(part, r.Cap); werr != nil {
			break
		}
	}
	// FINISHED ON BOTH EXITS, and under the lock the execution held, so no window shows an
	// acquisition that has returned still reading as executing.
	cell.finish()
	if werr == nil {
		c.wmu.Unlock()
		return mqttOutputCompleted, nil
	}

	// AN INCOMPLETE FRAME SEALS BEFORE IT RELEASES THE LOCK. queueOutbound closes the
	// socket AFTER wmu is released, which is fine for a whole-message write and wrong here:
	// between the release and the close another writer could begin a second frame on a
	// transport already carrying half of one. Sealing first makes every later writer decline.
	// This is the writer half of certification: it holds wmu, so it CANNOT cross the writer
	// barrier here; it seals now, releases wmu, then crosses the barrier and completes.
	sealOutput(r.Binding)
	c.wmu.Unlock()
	if h := mqttTestTornWriterUnlockedHookV.Load(); h != nil {
		(*h)(c)
	}
	if c.rawConn != nil {
		// Closed OUTSIDE wmu, so a writer blocked in the kernel returns rather than holding
		// the lock the closer would need.
		_ = c.rawConn.Close()
	}
	// COMPLETE certification now that wmu is free to be re-taken as the barrier. Owner-free:
	// if a teardown certifier is racing this torn writer, whichever crosses the barrier first
	// records the certificate and the other adopts it — neither blocks on the other. Leaving
	// the gate sealed with no completed certificate is exactly the deadlock this replaces.
	mqttCrossWriterBarrier(c)
	completeCertification(r.Binding, "outbound frame torn mid-write")
	// The action is NOT released back to unused and the frame is NOT retried with it. A later
	// protocol retransmission acquires a new action; this one's outcome is that bytes may
	// have gone out and nothing may assume otherwise.
	return mqttOutputFailed, werr
}

// writeFramePartLocked writes one part of a frame. c.wmu held by the caller, for the whole
// frame rather than for each part.
func (c *client) writeFramePartLocked(data []byte, cap time.Time) error {
	if c.nc == nil {
		return fmt.Errorf("the socket went away mid-frame")
	}
	d := time.Now().Add(mqttWriteDeadline)
	if !cap.IsZero() && cap.Before(d) {
		d = cap
	}
	_ = c.nc.SetWriteDeadline(d)
	defer func() { _ = c.nc.SetWriteDeadline(time.Time{}) }()
	for len(data) > 0 {
		n, err := c.nc.Write(data)
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return errMQTTShortFrameWrite
		}
	}
	return nil
}

var errMQTTShortFrameWrite = fmt.Errorf("the transport accepted no bytes of a frame")
