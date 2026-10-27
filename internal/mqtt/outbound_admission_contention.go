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
	"math/rand"
	"time"
)

// errMQTTStaleExpectation marks a refusal whose only reason is that a record this batch expected
// at one sequence has since moved: another writer committed first. It is wrapped by the write
// boundary's stale-sequence checks that an admission reaches; every other refusal is not it.
var errMQTTStaleExpectation = errors.New("a record the batch expected has moved")

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
// A SUBSCRIPTION'S DELIVERIES ARE ADMITTED ONE AT A TIME, ON ONE CALLBACK, IN THE ORDER JETSTREAM
// OFFERS THEM, and that is the whole of what keeps a non-shared subscription's same-topic messages
// in order (MQTT-4.6.0-6). An admission that gives up abandons its offer, and JetStream's re-offer
// queues behind everything already pushed to the subscription: the message reaches the client
// after messages published later. Every admission and every acknowledgement of a session CASes its
// one authority record, so under a backlog an admission can lose every attempt of its budget to
// the session's own acknowledgements.
//
// So an admission that has lost ONLY to stale expectations keeps its offer and attempts again,
// with the same spacing before each attempt's fresh reads, for as long as this connection's
// incarnation lives and its connection stays open. Each attempt is the ladder's ordinary one: the same allocation, custody
// reservation and settlement, and the write compares the standing authority with the entitlement
// this connection was accepted under, so a displaced tenure is refused and that refusal ends the
// extension. It ends too on any failure that is not positively a stale expectation (attempts that
// did not all fail that way never qualify), on the incarnation's closing signal, and when the
// connection closes or stops being the session's, each checked before every further attempt; the ladder then gives up exactly as it always did.
//
// Bounded by the incarnation, not by time: a periodic give-up while the connection lives would
// reorder exactly as before. A subscription can therefore stall for as long as its admissions keep
// losing, and this session's background authority writers stay yielded meanwhile
// (docs/CONFORMANCE.md).
type mqttDeliveryContentionExtension struct {
	sess    *mqttSession
	c       *client
	closing <-chan struct{}
	stale   int // attempts whose write failed positively stale
}

// noteWrite records one attempt's write failure.
func (x *mqttDeliveryContentionExtension) noteWrite(err error) {
	if mqttIsStaleExpectation(err) {
		x.stale++
	}
}

// extend is the ladder's mqttAdmissionExtension.
func (x *mqttDeliveryContentionExtension) extend(attempts int, lastErr error) bool {
	if x.stale != attempts || !mqttIsStaleExpectation(lastErr) || !x.connectionServes() {
		return false
	}
	if h := mqttTestAdmissionExtendedHook.Get(); h != nil {
		h(x.sess, attempts)
	}
	return x.sess.authorityContentionWaitUnless(x.closing) && x.connectionServes()
}

// connectionServes reports whether the delivery's connection is still open and still the
// session's. A persistent session's connection can drop without its incarnation retiring, so the
// closing signal alone would let the extension outlive the connection it delivers for; its offer
// is then the next connection's to receive, as a re-offer.
func (x *mqttDeliveryContentionExtension) connectionServes() bool {
	if x.c == nil {
		return false
	}
	if x.c.isClosed() {
		return false
	}
	x.sess.mu.Lock()
	serves := x.sess.c == x.c
	x.sess.mu.Unlock()
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
