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
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

// A SUBSCRIBE filter refused while JetStream did not answer closes the connection (issue #187).
//
// A filter that cannot be provisioned is answered SUBACK 0x80 and the connection stays open. That
// is right for a refusal the store DECIDED — a permission, a ceiling, a sealed tenure — and wrong
// for one it never answered: a client that does not retry a refused filter stays deaf on it for as
// long as the connection lives. [MQTT-4.8.0-2] closes the connection on a transient error while an
// inbound packet is processed, which is what the CONNECT restore and an unresolved SUBSCRIBE save
// already do. The client then reconnects and, unless it resumes without resubscribing, subscribes
// again (docs/CONFORMANCE.md).
//
// HOW A REFUSAL IS KNOWN TO BE ONE JETSTREAM DID NOT ANSWER. Not by its error: between the request
// and the refusal sit status-only load results, formatted wrappers and retry loops that keep only
// their last error, and two review rounds each found more of them dropping the cause. The class is
// closed where it originates instead. The account's assistant advances one sequence whenever a
// request it made goes unanswered — a wait that ran out (requestTimeout), the double-ack
// confirmation's own timer, a no-responders reply, an offline asset — and processSubs reads it
// around each filter: a filter refused while the sequence moved was refused while JetStream was
// not answering. A core-NATS flush that got no PONG (the raw subscription's, and the delivery
// subscription processSub installs for a JetStream consumer) is observed by no assistant, so its
// sentinel is read from the refusal's error directly.
//
// WHAT THAT PROVES, AND WHAT IT DOES NOT. The assistant is the account's, shared by every session
// and by background work, so a moved sequence proves that SOME request went unanswered while this
// filter was being provisioned, not that this filter's did. A definite refusal overlapping one is
// closed rather than answered 0x80 — a deliberate, conservative policy, stated with its
// consequences in docs/CONFORMANCE.md. A filter that SUCCEEDED is never affected, whatever its
// window saw.

// errMQTTSubscribeJetStreamUnanswered ends a SUBSCRIBE that refused a filter while JetStream was
// not answering: no SUBACK, and the connection closes (v5: DISCONNECT 0x89, see
// mqttDisconnectReasonFromErr).
var errMQTTSubscribeJetStreamUnanswered = errors.New("a filter was refused while JetStream was not answering")

// mqttTestSubPacketFactsHookV observes a live SUBSCRIBE's facts and error as processSubs returns
// them, so a test can tell this packet's classification from a close another arm caused. nil in
// production.
var mqttTestSubPacketFactsHookV atomic.Pointer[func(facts mqttSubPacketFacts, err error)]

// observeUnanswered records that a request of this account's assistant went unanswered.
func (jsa *mqttJSACore) observeUnanswered() {
	jsa.unanswered.Add(1)
}

// unansweredMark is the assistant's unanswered sequence now: the start of a filter's window.
func (jsa *mqttJSACore) unansweredMark() uint64 {
	return jsa.unanswered.Load()
}

// unansweredSince reports whether a request went unanswered since mark was taken.
func (jsa *mqttJSACore) unansweredSince(mark uint64) bool {
	return jsa.unanswered.Load() != mark
}

// mqttCoreFlushUnanswered reports a core-NATS flush that got no PONG in time, or ran during a
// reconnect. No assistant observes it, so it is read from the error.
func mqttCoreFlushUnanswered(err error) bool {
	return errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrConnectionReconnecting)
}

// mqttUnansweredRefusal is the filter that made a packet owe a close, and the refusal it got.
type mqttUnansweredRefusal struct {
	filter string
	err    error
}

// noteRefusal records a filter's refusal as one JetStream did not answer when the filter's window
// saw the assistant's sequence move, or when the refusal is a core-NATS flush with no answer. Call
// it at the refusal, BEFORE that site's compensation runs: a compensation that itself goes
// unanswered must not reclassify a refusal the store had already decided. The first qualifying
// refusal is kept.
func (facts *mqttSubPacketFacts) noteRefusal(f *mqttFilter, err error, windowMoved bool) {
	if facts.jsUnanswered != nil || err == nil {
		return
	}
	if windowMoved || mqttCoreFlushUnanswered(err) {
		facts.jsUnanswered = &mqttUnansweredRefusal{filter: f.filter, err: err}
	}
}

// packetError is the error that ends a packet one of whose filters JetStream did not answer.
func (r *mqttUnansweredRefusal) packetError() error {
	return fmt.Errorf("%w: filter %q: %v", errMQTTSubscribeJetStreamUnanswered, r.filter, r.err)
}
