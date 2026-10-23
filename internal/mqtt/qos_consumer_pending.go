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

// mqttConsumerPending maps a JetStream source delivery (durable, stream sequence) to the packet
// identifier it occupies: the reverse index of a session's pending QoS 1/2 PUBLISH and PUBREL
// entries.
//
// A BUCKET EXISTS ONLY WHILE IT HOLDS AN ENTRY. Every consumer create mints a fresh durable name,
// so a bucket left behind once its last entry went is never reused: on a persistent session that
// subscribes and unsubscribes it accumulated one empty map per durable for the session's life.
// That is why every mutation goes through these methods, which create a bucket on its first entry
// and drop it with its last, and why no method hands out a bucket itself: a caller holding the
// inner map could delete from it and leave it empty again. Reads return values or copies.
//
// The session lock protects it, as it protects the entries it indexes.
type mqttConsumerPending map[string]map[uint64]uint16

// lookup returns the identifier recorded for (dur, seq).
func (p mqttConsumerPending) lookup(dur string, seq uint64) (uint16, bool) {
	pi, ok := p[dur][seq]
	return pi, ok
}

// records reports whether (dur, seq) is recorded as occupying pi.
func (p mqttConsumerPending) records(dur string, seq uint64, pi uint16) bool {
	cur, ok := p[dur][seq]
	return ok && cur == pi
}

// put records that (dur, seq) occupies pi, creating the durable's bucket on its first entry.
func (p *mqttConsumerPending) put(dur string, seq uint64, pi uint16) {
	if *p == nil {
		*p = make(mqttConsumerPending)
	}
	b := (*p)[dur]
	if b == nil {
		b = make(map[uint64]uint16)
		(*p)[dur] = b
	}
	b[seq] = pi
}

// deleteSeq removes (dur, seq) whatever identifier it records, and the bucket with its last entry.
func (p mqttConsumerPending) deleteSeq(dur string, seq uint64) {
	b := p[dur]
	if b == nil {
		return
	}
	delete(b, seq)
	if len(b) == 0 {
		delete(p, dur)
	}
}

// deleteIf removes (dur, seq) only while it still records pi — an entry since re-bound to another
// identifier is not this caller's to remove — and reports whether it did.
func (p mqttConsumerPending) deleteIf(dur string, seq uint64, pi uint16) bool {
	if cur, ok := p[dur][seq]; !ok || cur != pi {
		return false
	}
	p.deleteSeq(dur, seq)
	return true
}

// purge drops every entry of dur.
func (p mqttConsumerPending) purge(dur string) {
	delete(p, dur)
}

// entries returns a copy of dur's entries, seq -> pi; nil when it has none.
func (p mqttConsumerPending) entries(dur string) map[uint64]uint16 {
	b := p[dur]
	if len(b) == 0 {
		return nil
	}
	out := make(map[uint64]uint16, len(b))
	for seq, pi := range b {
		out[seq] = pi
	}
	return out
}

// durables returns the durables that have entries.
func (p mqttConsumerPending) durables() []string {
	if len(p) == 0 {
		return nil
	}
	out := make([]string, 0, len(p))
	for dur := range p {
		out = append(out, dur)
	}
	return out
}
