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

import "testing"

// TestF115SourceRepresentedByAPubRelEntryIsNotAdmitted pins the predicate behind F-115's
// shape B, entry shape by entry shape: a source delivery whose exchange is represented by a
// PUBREL-phase entry is declined by trackPublish on the unmapped path, and nothing is
// allocated for it. Each shape is the one a real route installs — the PUBREC successor, the
// promoted PUBREL action entry, the recovered placeholder, PUBCOMP's terminal reservation —
// and each is staged ALONE, so a predicate that reads only some of them fails here.
//
// The negative cases are the ones the predicate must not swallow: a sourceless (retained)
// exchange, and a source on the experimental slot-only class, which keeps its own order.
func TestF115SourceRepresentedByAPubRelEntryIsNotAdmitted(t *testing.T) {
	const (
		srcDur  = "f115-src"
		relDur  = "f115-pubrel"
		srcSeq  = uint64(41)
		otherSq = uint64(42)
		pi      = uint16(7)
	)
	slot := func(dur string, sseq uint64) mqttInflightSlotRef {
		return mqttInflightSlotRef{
			name:  mqttInflightName{pi: pi},
			nonce: mqttNewInflightNonce(),
			state: mqttInflightPubRelReady,
			dur:   dur,
			sseq:  sseq,
		}
	}
	successor := func() *mqttPending {
		pub := &mqttPending{jsDur: srcDur, sseq: srcSeq, slot: slot(srcDur, srcSeq)}
		return mqttNewPubRelSuccessor(nil, pub, relDur, 0, _EMPTY_)
	}
	promoted := func() *mqttPending {
		act := &mqttSendAction{slot: slot(srcDur, srcSeq)}
		return mqttPromotePubRelAction(nil, act, relDur, 3, testInflightAckSubject(relDur, 3, 1))
	}
	placeholder := func() *mqttPending {
		return mqttNewPubRelPlaceholder(srcDur, srcSeq, mqttNewInflightNonce())
	}
	reservation := func() *mqttPending {
		return mqttNewTerminalReservation(slot(srcDur, srcSeq), nil)
	}

	// The staging must be able to ALLOCATE, or a decline proves nothing: a session with no
	// capacity also returns zero, through the in-flight cap's deferral. So the positive control
	// first — the same session shape with no PUBREL entry admits the offer.
	t.Run("control: an unrepresented source is admitted", func(t *testing.T) {
		sess := &mqttSession{maxp: 10, pendingPubRel: map[uint16]*mqttPending{}}
		got, isNew := sess.trackPublish(srcDur, testInflightAckSubject(srcDur, srcSeq, 2))
		if got == 0 || !isNew {
			t.Fatalf("fixture: the staged session cannot allocate (pi=%d new=%v), so no decline here would discriminate", got, isNew)
		}
	})

	for _, tc := range []struct {
		name  string
		entry func() *mqttPending
	}{
		{"pubrec successor", successor},
		{"promoted pubrel action entry", promoted},
		{"recovered placeholder", placeholder},
		{"terminal reservation", reservation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := &mqttSession{maxp: 10, pendingPubRel: map[uint16]*mqttPending{pi: tc.entry()}}
			if !sess.sourcePendingPubRelLocked(srcDur, srcSeq) {
				t.Fatalf("the %s does not represent its source", tc.name)
			}
			if sess.sourcePendingPubRelLocked(srcDur, otherSq) {
				t.Fatalf("the %s represents a source it was not admitted for", tc.name)
			}
			got, isNew := sess.trackPublish(srcDur, testInflightAckSubject(srcDur, srcSeq, 2))
			if got != 0 || isNew {
				t.Fatalf("a re-offer of a source past PUBREC was admitted: pi=%d new=%v", got, isNew)
			}
			if n := len(sess.pendingPublish); n != 0 {
				t.Fatalf("the declined re-offer left %d PUBLISH entries", n)
			}
			if m := sess.cpending[srcDur]; len(m) != 0 {
				t.Fatalf("the declined re-offer mapped its source: %v", m)
			}
			if n := len(sess.deferred); n != 0 {
				t.Fatalf("the re-offer was deferred (%d) rather than declined: a deferral NAKs it forward when capacity frees", n)
			}
		})
	}

	t.Run("sourceless retained exchange", func(t *testing.T) {
		ret := &mqttPending{kind: mqttPendingRetained, slot: slot(_EMPTY_, 0)}
		sess := &mqttSession{pendingPubRel: map[uint16]*mqttPending{pi: ret}}
		if dur, seq, ok := mqttPendingExchangeSource(ret); ok {
			t.Fatalf("a retained exchange reports a source %s/%d", dur, seq)
		}
		if sess.sourcePendingPubRelLocked(srcDur, srcSeq) {
			t.Fatal("a sourceless exchange spoke for a source")
		}
	})

	t.Run("slot-only source", func(t *testing.T) {
		aff := mqttQ2AffineConsumerDurablePrefix + "f115"
		pub := &mqttPending{jsDur: aff, sseq: srcSeq, slot: slot(aff, srcSeq)}
		sess := &mqttSession{pendingPubRel: map[uint16]*mqttPending{
			pi: mqttNewPubRelSuccessor(nil, pub, relDur, 0, _EMPTY_),
		}}
		if sess.sourcePendingPubRelLocked(aff, srcSeq) {
			t.Fatal("the slot-only class is declined by the ordinary predicate; it keeps its own order")
		}
	})
}
