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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// THE POSITIVE SOURCE-ACK GATE (F-105 cutover, unit 7a; consult 83).
//
// Invariant: for a valid $MQTT_msgs source reply naming a shared-group durable and stream
// sequence, this session never SUBMITS a positive acknowledgement while it holds a HandBack for
// that source. An attempted positive acknowledgement under HandBack submits a paced NAK instead
// and reports mqttSourceAckHandedBack.
//
// Why a gate rather than a policy at each route: a shared delivery abandoned on an ACL deny must
// be HANDED BACK to the group — acknowledging it consumes the group's message on behalf of members
// who ARE allowed it (publish_deliver.go, the deny guard in mqttDeliverMsgCbQoS12). Every route that settles, cleans up, suppresses
// or discards a source can reach a positive acknowledgement, and three review rounds each found
// one more; the enumeration was the defect. So the class is closed where the effect happens: every
// positive source acknowledgement passes through here (TestEverySourceAckIsGated pins that), and
// the routes are witnesses of its reach, not the definition of correctness.
//
// "Holds a HandBack" has ONE definition: the abandoned-ACK binding set (outbound_abandoned_ack.go)
// holds a binding for exactly this {durable, sequence}, or a disposal owner for the source is
// still unresolved. There is no second map. The predicate applies only to shared-group durables:
// an ordinary abandonment keeps positive settlement, and a genuine shared PUBACK with nothing held
// keeps it too.
//
// The check and the submission are one critical section under sess.mu, serialized with binding
// installation and removal. A confirmed acknowledgement is split: prepared outside the lock,
// ENQUEUED under it together with the check, and waited for outside it.

// mqttSourceAckResult is what one gated submission did.
type mqttSourceAckResult uint8

const (
	// mqttSourceAckSubmitted: a fire-and-forget positive acknowledgement was enqueued.
	mqttSourceAckSubmitted mqttSourceAckResult = iota + 1
	// mqttSourceAckConfirmed: a confirmed positive acknowledgement was applied by JetStream.
	mqttSourceAckConfirmed
	// mqttSourceAckHandedBack: a HandBack was held; a paced NAK was submitted instead. Never a
	// completion: the group still owns the message.
	mqttSourceAckHandedBack
	// mqttSourceAckStale: the continuation that asked is no longer the installed owner; nothing
	// was submitted — the owner that superseded it owns the source.
	mqttSourceAckStale
	// mqttSourceAckFailed: a confirmed acknowledgement was enqueued but not confirmed, or the
	// reply could not be classified.
	mqttSourceAckFailed
)

// positive reports whether the result is a positive acknowledgement of the source.
func (r mqttSourceAckResult) positive() bool {
	return r == mqttSourceAckSubmitted || r == mqttSourceAckConfirmed
}

// errMQTTSourceHandedBack is the error form of mqttSourceAckHandedBack, for the callers whose
// contract is an error: it must never read as a confirmation.
var errMQTTSourceHandedBack = errors.New("the source is handed back to its group, not acknowledged")

// mqttSourceAckOwner revalidates, under sess.mu, that the continuation submitting is still the
// installed owner of the work it acts for. Nil for a callback-originated submission.
type mqttSourceAckOwner func() bool

// sourceHandBackHeldLocked is the ONE predicate: this session holds a HandBack for shared source
// {dur, seq}.
//
// Lock held on entry.
func (sess *mqttSession) sourceHandBackHeldLocked(dur string, seq uint64) bool {
	if !mqttIsSharedDurable(dur) || seq == 0 {
		return false
	}
	for _, b := range sess.abandonedAcks {
		// HandBack ONLY (F-088 WP2): an AckRoute routes a late acknowledgement and holds no
		// source gate — its source is acknowledged by its own completion like any delivery's.
		if b.handBack() && b.belongsTo(sess) && b.dur == dur && b.seq == seq {
			return true
		}
	}
	// AN UNRESOLVED DISPOSAL IS NOT PERMISSION. Between a denial's installation and its binding
	// there is no binding yet, and that interval must not read as "nothing held".
	return len(sess.disposalsBySource[mqttDeferredKey{dur, seq}]) > 0
}

// mqttSourceAckScope classifies a reply for the gate: whether it is a $MQTT_msgs acknowledgement on
// a shared-group durable (the predicate's scope) and the source it names. A reply that cannot be
// parsed is out of the predicate's scope only if it could not be a source reply at all; one that
// starts like an acknowledgement but does not parse is refused rather than acknowledged.
func mqttSourceAckScope(reply string) (dur string, seq uint64, shared, malformed bool) {
	stream, d, s, ok := mqttParseSourceAckReply(reply)
	if !ok {
		return _EMPTY_, 0, false, strings.HasPrefix(reply, "$JS.ACK.")
	}
	if stream != mqttStreamName || !mqttIsSharedDurable(d) {
		return d, s, false, false
	}
	return d, s, true, false
}

// handBackLocked submits the paced NAK a HandBack owes, on the same backoff the deny guard uses.
//
// Lock held on entry.
func (sess *mqttSession) handBackLocked(reply string) mqttSourceAckResult {
	_, _, dc, _, _ := ackReplyInfo(reply)
	mqttObserveSourceAck(reply, false)
	sess.jsa.sendMsg(reply, mqttPacedRefusalNak(dc))
	return mqttSourceAckHandedBack
}

// mqttTestSourceAckObserver sees every submission the gate makes: positive (an acknowledgement) or
// not (a hand-back NAK). Nil in production.
var mqttTestSourceAckObserver atomic.Pointer[func(reply string, positive bool)]

func mqttObserveSourceAck(reply string, positive bool) {
	if o := mqttTestSourceAckObserver.Load(); o != nil {
		(*o)(reply, positive)
	}
}

// handBackRefusesLocked applies the predicate WITHOUT a positive submission: when a HandBack is held
// it submits the paced NAK and reports true. For a caller whose positive acknowledgement is a test
// seam standing in for JetStream.
//
// Lock held on entry.
func (sess *mqttSession) handBackRefusesLocked(reply string) bool {
	dur, seq, shared, _ := mqttSourceAckScope(reply)
	if shared && sess.sourceHandBackHeldLocked(dur, seq) {
		sess.handBackLocked(reply)
		return true
	}
	return false
}

// submitSourceAckLocked is the gated FIRE-AND-FORGET positive acknowledgement, for a caller that
// already holds sess.mu.
//
// Lock held on entry.
func (sess *mqttSession) submitSourceAckLocked(reply string, owner mqttSourceAckOwner) mqttSourceAckResult {
	if reply == _EMPTY_ {
		return mqttSourceAckFailed
	}
	if owner != nil && !owner() {
		return mqttSourceAckStale
	}
	dur, seq, shared, malformed := mqttSourceAckScope(reply)
	if malformed {
		return mqttSourceAckFailed
	}
	if shared && sess.sourceHandBackHeldLocked(dur, seq) {
		return sess.handBackLocked(reply)
	}
	mqttObserveSourceAck(reply, true)
	sess.jsa.sendAck(reply)
	return mqttSourceAckSubmitted
}

// submitSourceAck is the gated positive acknowledgement for a caller NOT holding sess.mu:
// fire-and-forget, or confirmed (prepare outside the lock, enqueue under it with the check, wait
// outside it).
//
// Session lock NOT held on entry.
func (sess *mqttSession) submitSourceAck(reply string, confirm bool, owner mqttSourceAckOwner) (mqttSourceAckResult, error) {
	if !confirm {
		sess.mu.Lock()
		r := sess.submitSourceAckLocked(reply, owner)
		sess.mu.Unlock()
		if r == mqttSourceAckFailed {
			return r, fmt.Errorf("the acknowledgement subject %q could not be classified", reply)
		}
		return r, nil
	}
	if reply == _EMPTY_ {
		return mqttSourceAckFailed, fmt.Errorf("no ack subject")
	}
	dur, seq, shared, malformed := mqttSourceAckScope(reply)
	if malformed {
		return mqttSourceAckFailed, fmt.Errorf("the acknowledgement subject %q could not be classified", reply)
	}
	p := sess.jsa.prepareAckConfirm(reply)
	sess.mu.Lock()
	if owner != nil && !owner() {
		sess.mu.Unlock()
		p.abandon()
		return mqttSourceAckStale, nil
	}
	if shared && sess.sourceHandBackHeldLocked(dur, seq) {
		r := sess.handBackLocked(reply)
		sess.mu.Unlock()
		p.abandon()
		return r, errMQTTSourceHandedBack
	}
	mqttObserveSourceAck(reply, true)
	p.enqueue()
	sess.mu.Unlock()
	if hook := mqttTestSourceAckWaitHook.Get(); hook != nil {
		hook(sess)
	}
	if err := p.wait(); err != nil {
		return mqttSourceAckFailed, err
	}
	return mqttSourceAckConfirmed, nil
}

// mqttTestSourceAckWaitHook observes a confirmed acknowledgement's wait, which runs with sess.mu
// released. Nil in production.
var mqttTestSourceAckWaitHook = new(mqttKnob[func(sess *mqttSession)])

// mqttPreparedAck is one confirmed acknowledgement split into its three steps, so the submission
// can be linearized under a lock while the wait is not.
type mqttPreparedAck struct {
	jsa   *mqttJSA
	subj  string
	reply string
	ch    chan *mqttJSAResponse
}

// prepareAckConfirm builds a confirmed acknowledgement and registers its reply, without
// submitting it.
func (jsa *mqttJSA) prepareAckConfirm(ackSubject string) *mqttPreparedAck {
	var sb strings.Builder
	jsa.mu.Lock()
	uid := jsa.nuid.Next()
	sb.WriteString(jsa.rplyr)
	jsa.mu.Unlock()
	sb.WriteString(mqttJSAAckConfirm)
	sb.WriteByte(btsep)
	sb.WriteString(uid)
	p := &mqttPreparedAck{jsa: jsa, subj: ackSubject, reply: sb.String(), ch: make(chan *mqttJSAResponse, 1)}
	jsa.replies.Store(p.reply, p.ch)
	return p
}

// enqueue submits the prepared acknowledgement. A queue push: safe under sess.mu.
func (p *mqttPreparedAck) enqueue() {
	p.jsa.sendMsgReply(p.subj, p.reply, AckAck)
}

// abandon drops a prepared acknowledgement that was never submitted.
func (p *mqttPreparedAck) abandon() { p.jsa.replies.Delete(p.reply) }

// wait blocks for JetStream's confirmation, with the request timeout every JS API call uses. A
// reply that is a transport status (processJSAPIReplies delivers it as an error) is no
// confirmation: the acknowledgement reached no consumer leader.
func (p *mqttPreparedAck) wait() error {
	t := time.NewTimer(p.jsa.timeout)
	defer t.Stop()
	select {
	case r := <-p.ch:
		if err, _ := r.value.(error); err != nil {
			return err
		}
		return nil
	case <-p.jsa.quitCh:
		return ErrServerNotRunning
	case <-t.C:
		p.jsa.replies.Delete(p.reply)
		return fmt.Errorf("timeout after %v: acknowledgement confirmation on %q", p.jsa.timeout, p.subj)
	}
}

// mqttHandBackTerminalProof authorizes the deletion of ONE shared terminal register whose source was
// handed back (7a; P7a-5). The terminal destruction boundary otherwise requires a source
// completion, which for a handed-back group message can never exist: the member no longer holds
// it. So for exactly that population — a shared terminal carrier whose exchange, read now and
// pair-validated, carries the abandonment fact of this allocation and reads Terminal — this typed
// capability replaces ONLY the source-completion requirement. It is bound to the observed register
// version like a completion is, so the gate's version fence refuses it if the register moved, and
// every other destruction guard still applies. It is never a source completion.
type mqttHandBackTerminalProof struct {
	subject string
	seq     uint64
	record  mqttInflightRecord
}

func (p mqttHandBackTerminalProof) permits(subject string, seq uint64, r *mqttInflightRecord) bool {
	return r != nil && p.subject == subject && subject != _EMPTY_ &&
		p.seq == seq && seq != 0 && p.record == *r
}

// handBackTerminalProof mints the capability from a FRESH exact-pair read.
//
// Session lock NOT held on entry.
func (sess *mqttSession) handBackTerminalProof(subject string, seq uint64, r *mqttInflightRecord) mqttHandBackTerminalProof {
	if sess.asm == nil || seq == 0 || !mqttTerminalSourceCarrier(r) || !mqttIsSharedDurable(r.Dur) ||
		mqttValidateInflightShape(r) != nil || !mqttInflightRecordMatchesSubject(r, subject) {
		return mqttHandBackTerminalProof{}
	}
	ref := mqttSlotRefFromRecord(subject, seq, r)
	if ref == nil {
		return mqttHandBackTerminalProof{}
	}
	class, x := sess.ackExchangeFor(r, ref.name.gen)
	if class != mqttAckExchange || x.Phase != mqttExchangeTerminal || x.QoS != r.QoS || x.Abandoned == nil ||
		x.ID != sess.id || x.Epoch != sess.asm.sessStreamEpoch || x.Gen != ref.name.gen {
		return mqttHandBackTerminalProof{}
	}
	if f := x.Abandoned; f.Alloc != r.Nonce || f.Acq != r.Acq || f.Dur != r.Dur || f.Seq != r.Seq ||
		f.Reason != r.AbandonReason || f.Tenure != r.AbandonTenure {
		return mqttHandBackTerminalProof{}
	}
	return mqttHandBackTerminalProof{subject: subject, seq: seq, record: *r}
}
