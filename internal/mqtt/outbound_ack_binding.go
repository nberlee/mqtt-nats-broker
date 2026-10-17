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
	"fmt"
	"sync"
)

// Which obligation an acknowledgement answers (F-105, row 3).
//
// Today an arriving PUBACK, PUBREC or PUBCOMP is matched by asking whether it came from the
// session's CURRENT connection and then looking the identifier up in the in-memory scheduling
// maps. That is a stale-connection guard plus a lookup, and neither is the question the
// acknowledgement actually asks, which is: *which obligation does this answer?*
//
// # Two readings were tried and refused, and both are recorded so they are not re-derived
//
// "The action's original connection" is WRONG. A resumed client acknowledging on a NEW
// connection an exchange admitted before the resume is legitimate MQTT — the session carries
// in-flight state across connections — and gating acknowledgement on the acquiring connection
// would break QoS 1/2 resumption outright.
//
// "The exchange the identifier is allocated to NOW, in the serving generation" is ALSO wrong,
// and more subtly: a handler captures the acknowledgement for E1 at identifier 7, pauses, and
// resumes after 7 names E2. A lookup taken *now* selects E2, and adding a generation test
// changes nothing because both are in the same generation. It also breaks completion after
// displacement, since an already-captured handler for G1 must still complete E1 while G2
// serves — "completion does not require serving authority" is the design's own rule.
//
// # So the binding is CAPTURED AT DISPATCH and retained
//
// When a frame goes out for an exchange under an identifier on a connection, that triple is
// recorded here. When an acknowledgement arrives, the obligation it answers is read from the
// binding as it stood when the packet was dispatched, and that identity is carried through
// every wait and retry the handler performs.
//
// A resume ESTABLISHES bindings on the new connection for the exchanges that survived it, which
// is what keeps resumed acknowledgement working without consulting the current allocation.
//
// The identifier may be reused afterwards; the captured binding is unaffected, because it is a
// value taken at dispatch and not a lookup performed later. That is the whole mechanism.

// mqttAckBinding is the obligation an identifier named on one connection when a frame for it
// was dispatched.
type mqttAckBinding struct {
	// Gen and Nonce identify the EXCHANGE — the obligation — not the allocation. The
	// identifier is an allocation belonging to it and may later belong to another.
	Gen   string
	Nonce string
	// PI is the identifier the client sees, frozen with the rest.
	PI uint16
	// Acq is the acquisition whose frame established this binding, so a completion can tell
	// which transmission is being answered when an exchange has been retransmitted.
	Acq uint64
	// QoS is the EXCHANGE's, captured with the rest at dispatch.
	//
	// It lives here rather than being supplied with the acknowledgement because the phase is
	// DERIVED from the packet and the QoS together, and a caller free to state the QoS could
	// make a PUBACK derive a legal ending for a QoS 2 exchange — which is contract 3's defect
	// wearing the new API. The dispatching PUBLISH knows the QoS; an arriving packet does not
	// carry one at all.
	QoS byte
}

// mqttAckBindings is one connection's captured identifier bindings.
//
// PER CONNECTION, because an acknowledgement arrives on a connection and the question it
// answers is "what did THIS peer last hear under this identifier". A session-wide map would
// reintroduce the reuse hazard the capture exists to remove.
type mqttAckBindings struct {
	mu sync.RWMutex
	by map[uint16]mqttAckBinding
}

// capture records the binding a dispatched frame establishes.
//
// Called at DISPATCH, not at admission: admission decides which obligation holds an identifier
// durably, while this records what the peer was actually told, and the two differ exactly in
// the window a replacement moves the allocation.
func (b *mqttAckBindings) capture(bind mqttAckBinding) error {
	// QoS 0 HAS NOTHING TO ACKNOWLEDGE, so a binding for one is a record of an obligation
	// that does not exist — and it would let an arriving PUBACK derive a phase for it.
	if bind.QoS != 1 && bind.QoS != 2 {
		return fmt.Errorf("capturing identifier %d for exchange %q at QoS %d: only 1 and 2 have "+
			"an acknowledgement to answer", bind.PI, bind.Nonce, bind.QoS)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.by == nil {
		b.by = map[uint16]mqttAckBinding{}
	}
	b.by[bind.PI] = bind
	return nil
}

// resolve reports the obligation an acknowledgement for this identifier answers on this
// connection.
//
// The second return is false when this connection never told the peer anything under that
// identifier, which is a REFUSAL and never a licence to guess: an acknowledgement nobody can
// attribute must not complete an obligation chosen by proximity.
func (b *mqttAckBindings) resolve(pi uint16) (mqttAckBinding, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	bind, ok := b.by[pi]
	return bind, ok
}

// release forgets the binding for one identifier, once its obligation has ended.
//
// Only on a TERMINAL outcome for the bound exchange. Releasing on the acknowledgement alone
// would drop the binding a retransmission still needs, and releasing on allocation reuse would
// silently retarget it — which is the defect the capture exists to prevent.
func (b *mqttAckBindings) release(pi uint16, nonce string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur, ok := b.by[pi]; ok && cur.Nonce == nonce {
		delete(b.by, pi)
	}
}

// adopt installs the bindings a resume inherits, for exchanges that survived onto this
// connection.
//
// A resumed client may acknowledge an exchange admitted before the resume, so the new
// connection needs the bindings for what the PREVIOUS one told the peer. Nothing is invented
// here: the caller supplies bindings derived from durable exchanges, and an exchange with no
// surviving record contributes none.
func (b *mqttAckBindings) adopt(binds []mqttAckBinding) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.by == nil {
		b.by = map[uint16]mqttAckBinding{}
	}
	for _, bind := range binds {
		b.by[bind.PI] = bind
	}
}

// mqttCapturedAckResolution is what an arriving acknowledgement resolves to.
//
// Named for the CAPTURE rather than for the acknowledgement because mqttAckResolution already
// exists in inflight_register.go for the source-side ack ladder, and two types called the same
// thing in one package is how a reader ends up reasoning about the wrong one.
type mqttCapturedAckResolution struct {
	// Binding is the captured obligation the packet answers.
	Binding mqttAckBinding
	// Phase is what the RECEIVED PACKET establishes. DERIVED from the arrival by
	// mqttPhaseForAck rather than supplied: see mqttResolveAck.
	Phase mqttExchangePhase
	// Resolved reports that this acknowledgement answered a captured obligation at all. The
	// zero value is "it did not", so a caller that ignores it reads a zero phase rather than
	// a plausible one.
	Resolved bool
}

// mqttResolveAck resolves one arriving acknowledgement against the connection it arrived on.
//
// It takes the ARRIVAL and derives the phase, rather than taking a phase. That is the whole
// correction a review required here: the boundary downstream checks that the proposed move is
// legal from where the exchange stands, and a fabricated phase can be perfectly legal and still
// answer the wrong protocol event. A caller cannot pass one in any more.
//
// The exchange's QoS comes from its own record, so a packet of the wrong kind is refused as
// what it is — an answer to a different obligation — rather than resolved and checked later.
func mqttResolveAck(binds *mqttAckBindings, pi uint16, in mqttAckArrival) (mqttCapturedAckResolution, error) {
	if binds == nil {
		return mqttCapturedAckResolution{}, fmt.Errorf("resolving an acknowledgement requires " +
			"the captured bindings of the connection it arrived on")
	}
	bind, ok := binds.resolve(pi)
	if !ok {
		// NOT AN ERROR. An identifier this connection captured nothing for is an
		// acknowledgement of something else — a source-side ladder, or a packet the client
		// should not have sent — and this function's answer is that it resolves to no
		// captured obligation.
		return mqttCapturedAckResolution{}, nil
	}
	// THE QoS COMES FROM THE BINDING, never from the caller. That is what makes the
	// derivation a boundary rather than a better-shaped caller contract.
	phase, perr := mqttPhaseForAck(in, bind.QoS)
	if perr != nil {
		return mqttCapturedAckResolution{}, fmt.Errorf("acknowledgement for identifier %d on "+
			"exchange %q: %w", pi, bind.Nonce, perr)
	}
	return mqttCapturedAckResolution{Binding: bind, Phase: phase, Resolved: true}, nil
}

// dischargedFor reports that NO captured binding on this connection still names this exchange.
//
// This is the fact mqttIdentifierReleaseAllowed calls bindingsDischarged, and the reason it is
// a precondition on releasing an identifier: the charge may be releasable in the accounting
// while a captured acknowledgement handler can still name the exchange on its original
// connection. Reusing the identifier there would let a late acknowledgement complete an
// obligation that no longer holds it.
//
// Connection-local, and so is a HALF of the answer. A caller that cannot reach every connection
// an exchange was dispatched on has not established discharge and must not claim it — the same
// shape as Outstanding, and for the same reason: absence of knowledge is not evidence.
func (b *mqttAckBindings) dischargedFor(gen, nonce string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, bind := range b.by {
		if bind.Gen == gen && bind.Nonce == nonce {
			return false
		}
	}
	return true
}
