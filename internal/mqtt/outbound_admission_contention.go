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

// A delivery's admission that loses only to stale expectations keeps its place in the
// subscription's order.

package mqtt

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// errMQTTStaleExpectation marks a refusal whose only reason is that a record this batch expected
// at one sequence has since moved: another writer committed first.
//
// ONE RULE DECIDES IT: a write-boundary refusal is marked exactly when the store's
// expected-last-subject-sequence CAS on the same member would have refused the batch — the
// member's expectation differs from the sequence a POSITIVE read observes at its subject, a
// positively absent subject standing at 0. The boundary refusal is the CAS rejection taken early,
// which is why mqttIsStaleExpectation classifies both alike.
//
// AND THE EXPECTATION IS COMPARED FIRST among everything decided from the standing record: before
// its body is decoded or judged, and before any rule conditioned on what was observed. A record
// that moved is refused as moved whatever it now holds; an undecodable body, a content violation
// or a disposition violation at exactly the expected sequence is not marked. Rules decided from
// the batch alone may run before it, and refuse unmarked whatever stands. An indeterminate read
// is never marked.
//
// Marking is safe to re-attempt on for a connection's life only because every composer whose
// refusals reach a marker consumer recomposes each attempt from fresh reads and conditions each
// member on the sequence it read: a mismatch proves the CAS would fail, not who moved the record.
var errMQTTStaleExpectation = errors.New("a record the batch expected has moved")

// mqttStaleExpectationf is a boundary refusal the store's CAS on the same member would have made
// (errMQTTStaleExpectation).
func mqttStaleExpectationf(format string, args ...any) error {
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), errMQTTStaleExpectation)
}

// mqttReadIsAbsence reports whether a read's error is the store's positive "no message" answer,
// seen through wrapping: IsNatsErr does a bare type assertion, and several loaders wrap.
func mqttReadIsAbsence(err error) bool {
	ae := jsAPIError(err)
	return ae != nil && ae.ErrCode == ApiErrors[JSNoMessageFoundErr].ErrCode
}

// readAtExpectation reads what stands at a batch member's subject and refuses, marked stale,
// unless it stands exactly where the member expects (0: nothing stands). An indeterminate read
// refuses unmarked. It returns the standing message, nil when positively absent.
//
// what names the member for the refusal ("transition %q replaces the register on %q").
func (jsa *mqttJSA) readAtExpectation(subj string, expect uint64, what string) (*StoredMsg, error) {
	smsg, err := jsa.loadLastMsgFor(mqttSessStreamName, subj)
	if err != nil && !mqttReadIsAbsence(err) {
		return nil, fmt.Errorf("%s, which could not be read: %w", what, err)
	}
	var standing uint64
	if err == nil && smsg != nil {
		standing = smsg.Sequence
	} else {
		smsg = nil
	}
	if standing != expect {
		return nil, mqttStaleExpectationf("%s at sequence %d, but it stands at %d", what, expect, standing)
	}
	return smsg, nil
}

// mqttIsStaleExpectation reports whether err is POSITIVELY a stale expectation: the boundary's own
// stale-sequence refusal, or the store's rejection of an expected-last-subject-sequence CAS. A read
// failure, a transport failure, an ambiguous outcome or any semantic refusal is not, whatever
// contended the ladder recorded for it.
func mqttIsStaleExpectation(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errMQTTStaleExpectation) {
		return true
	}
	var ae *ApiError
	return errors.As(err, &ae) && isQoS2CASRejection(ae)
}

// mqttDeliveryContentionExtension is the delivery entrance's extension of the admission ladder.
//
// A SUBSCRIPTION'S OFFERS ARE PROCESSED ON ONE DISPATCH GOROUTINE, IN THE ORDER JETSTREAM OFFERS
// THEM, their fresh deliveries admitted singly or several per batch and sent in that order, and
// that is the whole of what keeps a non-shared subscription's same-topic messages in order
// (MQTT-4.6.0-6). An admission that gives up abandons its offer, and JetStream's re-offer queues
// behind everything already pushed to the subscription: the message reaches the client after
// messages published later. Under a backlog an admission can lose attempt after attempt to
// whatever else of the session writes the records it CASes.
//
// So an admission (or a batch of them, attemptAdmissionBatch) that has lost ONLY to stale
// expectations keeps its offer and attempts again, with the same spacing before each attempt's
// fresh reads, for as long as its delivery lifecycle lasts (mqttDeliveryLifecycle). Each attempt is
// the ladder's ordinary one: the same allocation, custody reservation and settlement, and the write
// compares the standing authority with the entitlement this connection was accepted under, so a
// displaced tenure is refused and that refusal ends the extension. It ends too on any failure that
// is not positively a stale expectation (attempts that did not all fail that way never qualify);
// the lifecycle ending is decided at the head of every attempt after the first, inside the budget
// and past it alike, and the ladder then gives up exactly as it always did.
//
// Bounded by the incarnation, not by time: a periodic give-up while the connection lives would
// reorder exactly as before. A subscription can therefore stall for as long as its admissions keep
// losing, and this session's background authority writers stay yielded meanwhile
// (docs/CONFORMANCE.md).
type mqttDeliveryContentionExtension struct {
	life  *mqttDeliveryLifecycle
	stale int // attempts whose write failed positively stale
}

// newDeliveryContentionExtension is the extension for an admission run on behalf of life. A caller
// outside a delivery callback passes nil and gets a lifecycle of its own, captured now, so the
// extension still ends with the incarnation and the connection as it always has; only the
// callback's own lifecycle STOPS work at attempt heads (mqttDeliveryLifecycle).
func newDeliveryContentionExtension(sess *mqttSession, c *client, life *mqttDeliveryLifecycle) *mqttDeliveryContentionExtension {
	if life == nil {
		life = newDeliveryLifecycle(sess, c)
	}
	return &mqttDeliveryContentionExtension{life: life}
}

// mqttDeliveryLifecycle is whether a subscription callback's delivery work may still START: the
// connection's incarnation has not begun retiring in the closing cycle the callback started under,
// and the connection is open and still the session's.
//
// A RETIREMENT WAITS FOR THE CALLBACK'S LEASE, so everything the callback starts after the
// retirement began is time the takeover waits for (a whole batched flush, an admission losing
// attempt after attempt to its sibling subscriptions). The callback creates one lifecycle and every
// step it reaches is handed the same one; each stops where the code before batching would have
// been between two callbacks — before an offer, before an admission, before an acquisition,
// before a transmission, and at the head of every retry — and leaves exactly the state that
// step's existing refusal or exhaustion leaves. A started attempt always finishes and settles.
//
// Monotone without a latch: the closing channel is captured once, and a closed channel stays
// closed, so a reopen (an aborted takeover) never revives work the callback already stopped; a
// connection that stopped serving never serves again. The callback's later offers come back as
// re-offers, a delay and never a loss or a duplicate.
type mqttDeliveryLifecycle struct {
	sess *mqttSession
	c    *client
	// tr is the connection's tracker, whose lease the callback holds: it cannot retire, and the
	// session cannot be re-minted under it, while the callback runs.
	tr      *mqttIncarnationTracker
	closing <-chan struct{}
}

// newDeliveryLifecycle captures c's tracker and its current closing cycle.
func newDeliveryLifecycle(sess *mqttSession, c *client) *mqttDeliveryLifecycle {
	l := &mqttDeliveryLifecycle{sess: sess, c: c}
	if c != nil && c.mqtt != nil {
		l.tr = c.mqtt.tracker
		l.closing = l.tr.closingSignal()
	}
	return l
}

// over reports whether the lifecycle has ended. A nil lifecycle (work outside a delivery callback)
// never ends.
func (l *mqttDeliveryLifecycle) over() bool {
	if l == nil {
		return false
	}
	select {
	case <-l.closing:
		return true
	default:
	}
	return !l.connectionServes()
}

// noteWrite records one attempt's write failure.
func (x *mqttDeliveryContentionExtension) noteWrite(err error) {
	if mqttIsStaleExpectation(err) {
		x.stale++
	}
}

// extend is the ladder's mqttAdmissionExtension.
//
// Eligibility and spacing only: whether the lifecycle still lasts after the wait is decided at the
// loop head that follows, before the next write.
func (x *mqttDeliveryContentionExtension) extend(attempts int, lastErr error) bool {
	if x.stale != attempts || !mqttIsStaleExpectation(lastErr) || x.life.over() {
		return false
	}
	if h := mqttTestAdmissionExtendedHook.Get(); h != nil {
		h(x.life.sess, attempts)
	}
	return x.life.sess.authorityContentionWaitUnless(x.life.closing)
}

// connectionServes reports whether the delivery's connection is still open and still the
// session's. A persistent session's connection can drop without its incarnation retiring, so the
// closing signal alone would let delivery work outlive the connection it delivers for; its offer
// is then the next connection's to receive, as a re-offer.
func (l *mqttDeliveryLifecycle) connectionServes() bool {
	if l.c == nil {
		return false
	}
	if l.c.isClosed() {
		return false
	}
	l.sess.mu.Lock()
	serves := l.sess.c == l.c
	l.sess.mu.Unlock()
	return serves
}

// authorityContentionWaitUnless is authorityContentionWait, abandoned when closing fires. It
// reports whether the wait completed with closing still open.
//
// Session lock NOT held on entry, and none is taken across the wait.
func (sess *mqttSession) authorityContentionWaitUnless(closing <-chan struct{}) bool {
	select {
	case <-closing:
		return false
	default:
	}
	d := mqttAuthorityContentionBackoff(rand.Float64())
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testAuthorityContentionWait
		as.mu.RUnlock()
		if hook != nil {
			hook(d)
			select {
			case <-closing:
				return false
			default:
				return true
			}
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-closing:
		return false
	case <-t.C:
		return true
	}
}

// mqttTestAdmissionExtendedHook observes the delivery entrance extending an admission past its
// budget. Nil in production.
var mqttTestAdmissionExtendedHook = new(mqttKnob[func(sess *mqttSession, attempts int)])
