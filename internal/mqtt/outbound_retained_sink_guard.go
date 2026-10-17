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

import "sync/atomic"

// The ordinary-source sinks refuse anything that names no ordinary source (F-105 7c).
//
// A retained register has no source consumer and no sequence, and the readers that ask a consumer
// about a delivery would read its EMPTY coordinates as a consumer that is gone — LOST — and LOST
// licenses terminal cleanup, source retirement and grouping by an empty key. A missing value must
// refuse, never be believed, so every sink whose question is about a source DELIVERY refuses at its
// entry rather than trusting that its callers routed the input away.
//
// "Names no ordinary source" is judged by the COORDINATES, not only by the retained shape: a
// caller that synthesizes a record from a slot's (dur, sseq) carries no payload or digest, and a
// shape-only test let exactly that through to a consumer lookup on an empty name, whose reply never
// came (the Paho test_subscribe_options stall). A register is a union of the retained arm and the
// source arm, so a non-retained record with an empty durable or a zero sequence is malformed, and
// a zero sequence would otherwise read as Complete against any ack floor.
//
// The sinks this guard claims, and no others: inflightSourceState, the source cache's resolve,
// completeSourceFor, settleTerminalSource, mqttRecoveryPendingMatches and
// computeDrainRegisterStateLocked take the record form; holdTerminalSourceLocked and the recovery
// walk's reserveSource take the coordinate form. Functions outside that list that already reject
// empty coordinates (retireSourceLocked) do so silently and are not claimed here.
//
// The refusal is the fail-closed answer of each sink (Unknown, no completion, not grouped, no
// owner), and it is COUNTED: the dispatch upstream is supposed to route every sourceless input away
// before any of these is reached, so a non-zero count is a routing defect the behavioural tests
// assert never happens.

// mqttRetainedAtOrdinarySink counts sourceless inputs that reached an ordinary-source sink.
// Tests read it; production only increments it.
var mqttRetainedAtOrdinarySink atomic.Int64

// mqttRefuseSourcelessRecordAtOrdinarySink reports whether r names no ordinary source — the
// retained arm, or empty coordinates in any shape — counting the arrival.
func mqttRefuseSourcelessRecordAtOrdinarySink(r *mqttInflightRecord) bool {
	if r == nil {
		return false
	}
	if r.retainedSource() {
		mqttRetainedAtOrdinarySink.Add(1)
		return true
	}
	return mqttRefuseSourcelessAtOrdinarySink(r.Dur, r.Seq)
}

// mqttRefuseSourcelessAtOrdinarySink is the coordinate form, for the sinks that take a source as
// (durable, sequence) rather than as a register: an empty durable is the retained arm's
// coordinates, and no ordinary source has one.
func mqttRefuseSourcelessAtOrdinarySink(dur string, seq uint64) bool {
	if dur == _EMPTY_ || seq == 0 {
		mqttRetainedAtOrdinarySink.Add(1)
		return true
	}
	return false
}
