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

// The sources this session has acknowledged: what lets a re-offer JetStream dispatched before it
// processed one of this session's acknowledgements be recognised as one.

package mqtt

// mqttAckedSourcesPerDurable bounds the sequences remembered per source durable: the default
// MaxAckPending, the most a consumer can have dispatched and not yet seen acknowledged.
const mqttAckedSourcesPerDurable = 1024

// mqttAckedSourcesDurables bounds the durables remembered per session. A source durable's name is
// fresh per subscription and never reused, so a durable this session no longer consumes from can
// never match again; evicting the one recorded longest ago is all the forgetting it needs.
const mqttAckedSourcesDurables = 64

// mqttAckedSources records the (durable, stream sequence) pairs whose positive acknowledgement this
// session submitted through the source-acknowledgement gate. A submission is the broker's decision
// about that source's fate — completed, finalized, withdrawn, disposed or handed to another
// obligation — so a later offer of the same pair is not a delivery the client is owed: JetStream
// dispatched it before it processed the acknowledgement (a dispatched redelivery is not recalled),
// or re-sent it because the acknowledgement was lost.
//
// It records a SUBMISSION, not a confirmation, and nothing absent from it is a fact: a sequence
// acknowledged by another session or broker, or evicted, is simply not here.
//
// Guarded by sess.mu.
type mqttAckedSources struct {
	durs  map[string]*mqttAckedDurable
	order []string // durables, oldest first record
}

type mqttAckedDurable struct {
	seqs map[uint64]struct{}
	fifo []uint64
}

// record remembers (dur, seq).
func (a *mqttAckedSources) record(dur string, seq uint64) {
	if dur == _EMPTY_ || seq == 0 {
		return
	}
	if a.durs == nil {
		a.durs = map[string]*mqttAckedDurable{}
	}
	d := a.durs[dur]
	if d == nil {
		if len(a.order) >= mqttAckedSourcesDurables {
			delete(a.durs, a.order[0])
			a.order = a.order[1:]
		}
		d = &mqttAckedDurable{seqs: map[uint64]struct{}{}}
		a.durs[dur] = d
		a.order = append(a.order, dur)
	}
	if _, ok := d.seqs[seq]; ok {
		return
	}
	if len(d.fifo) >= mqttAckedSourcesPerDurable {
		delete(d.seqs, d.fifo[0])
		d.fifo = d.fifo[1:]
	}
	d.seqs[seq] = struct{}{}
	d.fifo = append(d.fifo, seq)
}

// has reports whether (dur, seq) was recorded and not since evicted.
func (a *mqttAckedSources) has(dur string, seq uint64) bool {
	d := a.durs[dur]
	if d == nil {
		return false
	}
	_, ok := d.seqs[seq]
	return ok
}

// mqttTestAckedReofferDeclinedHook observes a re-offer declined because this session had
// acknowledged its source. Nil in production.
var mqttTestAckedReofferDeclinedHook = new(mqttKnob[func(dur string, seq uint64)])
