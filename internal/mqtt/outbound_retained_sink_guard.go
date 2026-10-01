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

// The ordinary-source sinks refuse a retained register (F-105 7c).
//
// A retained register has no source consumer and no sequence, and the readers that ask a consumer
// about a delivery would read its EMPTY coordinates as a consumer that is gone — LOST — and LOST
// licenses terminal cleanup, source retirement and grouping by an empty key. A missing value must
// refuse, never be believed, so every sink whose question is about a source DELIVERY refuses the
// retained arm at its entry rather than trusting that its callers routed the record away.
//
// The refusal is the fail-closed answer of each sink (Unknown, no completion, not grouped), and it
// is COUNTED: the dispatch upstream is supposed to route every retained register away before any
// of these is reached, so a non-zero count is a routing defect the behavioural tests assert never
// happens. The census of register-field readers (the 7c plan's audit) names the sinks; this guard
// is the enforcement, not the census.

// mqttRetainedAtOrdinarySink counts retained registers that reached an ordinary-source sink.
// Tests read it; production only increments it.
var mqttRetainedAtOrdinarySink atomic.Int64

// mqttRefuseRetainedAtOrdinarySink reports whether r is a retained register, counting the arrival.
func mqttRefuseRetainedAtOrdinarySink(r *mqttInflightRecord) bool {
	if r != nil && r.retainedSource() {
		mqttRetainedAtOrdinarySink.Add(1)
		return true
	}
	return false
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
