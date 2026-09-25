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

import "sync/atomic"

// The live holder an authorized frame is executed for (F-105 row 2, consult 28).
//
// Consuming an action record is not enough to authorize its frame: an acquisition given up before
// output still has an immutable, unconsumed record, and a saved request for it would pass
// consumption. So writeAuthorizedFrame also asks the acquisition's HOLDER whether this execution is
// still its to perform.
//
// # Two atomicities, two locks
//
// The holder's decision must be atomic against the DRAIN that gives the acquisition up; that is the
// session lock's job, and for a PUBREL it is taken by enterPubRelWriteLocked before the writer runs.
// Consumption and the gate must be atomic against the SOCKET; that is wmu's. Nothing needs both in
// one critical section, because no first drain of a holder can run while its frame is being written:
// every drain entry is the acquiring goroutine before hand-off, the delivery callback's deferred
// drain after the synchronous write returned, or a reconcile/shutdown pass enrolled only by a drain
// that already ran (TestF105DrainEntriesCannotRunDuringAWrite records the set). Under wmu the writer
// therefore reads only state private to the execution: the PUBREL entry ticket, or the PUBLISH
// section's own output object.

// mqttOutputHolder is what writeAuthorizedFrame asks, under wmu, before it consumes an acquisition.
//
// enterWrite reports whether THIS execution of THIS requested action on THIS writing connection is
// the one the holder authorized. It must read nothing another goroutine can be writing.
type mqttOutputHolder interface {
	enterWrite(a *mqttOutboundAction, c *client) bool
}

// mqttPublishOutput is a PUBLISH send section's output identity, shared by every copy of its token.
//
// The section's token travels BY VALUE — into endPublishSend, drainPublishAction, the owed-drain
// reconcile and the shutdown pass — so a flag stored on the token itself would be set on a copy
// and never seen by the writer. This object is created once by beginSendLocked and pointed to by
// every copy.
//
// Its identity tiers have two initialization points. guard, gen, pi and token are fixed at creation;
// lin is set by acquirePublishAction from the fresh read, before its first batch is sent. action and
// acq are published only by a CLEAN landing — never by an adoption, which does not emit. All of them
// are written by the section's goroutine before any other goroutine can hold a copy, and never
// rewritten.
type mqttPublishOutput struct {
	guard *mqttFrameGuard
	gen   string
	pi    uint16
	token string
	lin   string

	action *mqttOutboundAction
	acq    uint64

	// closed is set by the first drain of the section's durable action, before its first store
	// call: once the lease is being given up, no execution may begin under it. The section's own
	// control flow already never writes after a drain; this makes the boundary refuse it too.
	closed atomic.Bool
}

func (o *mqttPublishOutput) enterWrite(a *mqttOutboundAction, c *client) bool {
	if o == nil || a == nil || c == nil || o.action == nil || o.closed.Load() {
		return false
	}
	if a != o.action || o.guard != &c.outFrames {
		return false
	}
	return a.Kind == mqttActionPublish && a.Gen == o.gen && a.Nonce == o.lin && a.Acq == o.acq && a.PI == o.pi
}

// abandonProven records that the acquisition (gen, lin, acq) — proven this section's by a read that
// found its exact token — will not execute here. A consumed acquisition is left alone.
func (o *mqttPublishOutput) abandonProven(acq uint64) {
	if o == nil || o.guard == nil || o.lin == _EMPTY_ || acq == 0 {
		return
	}
	o.guard.abandon(mqttActionKey{gen: o.gen, nonce: o.lin, acq: acq})
}

// mqttWriteEntry is the PUBREL write ENTRY TICKET: the proof that enterPubRelWriteLocked, under the
// session lock, found this exact holder live and moved it to writing, for ONE synchronous
// execution.
//
// It lives on the delivery callback's stack and is handed by pointer to the writer; it is never
// stored in a field, channel, closure or map, and it cannot be retried once used. A saved request
// therefore carries no ticket of its own — it must enter again, and entry refuses a holder that is
// already writing, released or drained.
type mqttWriteEntry struct {
	action *mqttOutboundAction
	conn   *client
	guard  *mqttFrameGuard
	key    mqttActionKey
	used   bool
}

// enterWrite compares the REQUEST's action and the WRITER's connection with the ones entry captured.
func (t *mqttWriteEntry) enterWrite(a *mqttOutboundAction, c *client) bool {
	if t == nil || t.used || a == nil || c == nil || a != t.action || c != t.conn {
		return false
	}
	t.used = true
	return true
}

// relinquish is armed by the callback IMMEDIATELY after a successful entry and runs on every exit
// after it — a writer refusal, a sealed gate, an early return, a recovered panic, runtime.Goexit.
// Either the execution consumed the acquisition, or it never will: then the acquisition is recorded
// ABANDONED, so a local drain oracle can call it finished. A consumed key is never overwritten.
func (t *mqttWriteEntry) relinquish() {
	if t == nil || t.guard == nil || t.key.acq == 0 {
		return
	}
	t.guard.abandon(t.key)
}
