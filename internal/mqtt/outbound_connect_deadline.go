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
	"time"
)

// The ownership protocol's absolute deadline, carried by every request it makes (F-105 8a).
//
// A budget checked only before each step bounds when a step STARTS, never how long its
// requests run: one step is several requests, each waiting up to the per-request timeout, and
// the nested resolutions a failed step triggers wait again. The deadline is therefore a
// property of the assistant VIEW the protocol runs on (see mqttJSA), so every request reached
// through it — reads, writes, receipt resolutions, aborts, re-attempts — honours the same
// instant. One instant, never a duration handed to each call: N requests each given "the
// remaining budget" would spend N times it.

// errMQTTOwnershipDeadline marks a request the ownership deadline cut off.
//
// Two shapes, and they mean opposite things about the store. Wrapped in a request that was
// never queued it is CERTAIN: nothing was sent. Wrapped in a timeout after the request was
// queued it is AMBIGUOUS exactly like the per-request timeout it replaced — the write may still
// land — and every caller already resolves that ambiguity from durable state.
var errMQTTOwnershipDeadline = errors.New("the ownership protocol's deadline has passed")

// errMQTTRequestNotSent marks a request refused BEFORE it was queued, so nothing reached the
// store. Distinct from a timeout, which says nothing about whether the request landed.
var errMQTTRequestNotSent = errors.New("request not sent")

// withDeadline returns a view of this assistant whose every request is capped at t. The view
// shares the core — replies, send queue, test seams — so it is the same assistant, bounded.
// A zero t returns a view with no cap beyond the per-request timeout.
func (jsa *mqttJSA) withDeadline(t time.Time) *mqttJSA {
	return &mqttJSA{mqttJSACore: jsa.mqttJSACore, deadline: t, gate: jsa.gate}
}

// withRequestGate returns a view of jsa whose every request is admitted by gate first, keeping
// jsa's deadline: a background caller that owes per-request shutdown admission and accounting
// (the exclusion-reclaim pass, F-119) runs its multi-request operations — commit, resolve, abort
// — on this view, so no request inside them escapes the gate.
func (jsa *mqttJSA) withRequestGate(gate func() bool) *mqttJSA {
	return &mqttJSA{mqttJSACore: jsa.mqttJSACore, deadline: jsa.deadline, gate: gate}
}

// mqttTestRequestGateRefusals and mqttTestRequestGateAdmits count the requests a view's gate refused
// and admitted. Read only by tests.
var mqttTestRequestGateRefusals, mqttTestRequestGateAdmits atomic.Int64

// admitRequests asks the view's gate once for each of n requests about to be queued. A refusal
// returns before anything is queued, so the caller learns nothing was sent.
func (jsa *mqttJSA) admitRequests(n int) error {
	if jsa.gate == nil {
		return nil
	}
	for range n {
		if !jsa.gate() {
			mqttTestRequestGateRefusals.Add(1)
			return fmt.Errorf("%w: the caller's request gate is closed", errMQTTRequestNotSent)
		}
		mqttTestRequestGateAdmits.Add(1)
	}
	return nil
}

// requestWait is how long the next request may wait for its reply: the per-request timeout,
// capped by the view's deadline. A deadline already passed refuses BEFORE anything is queued,
// which is the certain shape: the caller learns nothing was sent.
func (jsa *mqttJSA) requestWait() (time.Duration, error) {
	if jsa.deadline.IsZero() {
		return jsa.timeout, nil
	}
	left := time.Until(jsa.deadline)
	if left <= 0 {
		return 0, fmt.Errorf("%w: %w", errMQTTRequestNotSent, errMQTTOwnershipDeadline)
	}
	if left < jsa.timeout {
		return left, nil
	}
	return jsa.timeout, nil
}

// timeoutError marks a reply wait that ended on the view's deadline rather than on the
// per-request timeout, so the protocol can tell an expiry from an ordinary store stall. The
// request was queued, so the outcome stays ambiguous either way.
func (jsa *mqttJSA) timeoutError(err error) error {
	if !jsa.deadline.IsZero() && !time.Now().Before(jsa.deadline) {
		return fmt.Errorf("%w (%w)", err, errMQTTOwnershipDeadline)
	}
	return err
}

// deadline returns the instant the whole ownership protocol of one CONNECT must finish by.
func (b *mqttOwnershipBudget) deadline() time.Time {
	if b == nil {
		return time.Time{}
	}
	return b.Anchor.Add(b.Total)
}
