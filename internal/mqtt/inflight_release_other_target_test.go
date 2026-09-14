// Copyright 2025 The mqtt-nats-bridge Authors
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
	"reflect"
	"sync"
	"testing"

	"github.com/nats-io/nuid"
)

var errTestIndeterminate = errors.New("test: the read was indeterminate")

// testScriptedRead is one scripted load of a subject: an occupant, or an error.
type testScriptedRead struct {
	occ *testInflightOccupant
	err error
}

// testReleaseScript plants a read script PER SUBJECT: each load of a subject returns that
// subject's next scripted read, and a subject whose script has run out reads as empty.
func testReleaseScript(t *testing.T, as *mqttAccountSessionManager,
	script map[string][]testScriptedRead) *[]uint64 {

	t.Helper()
	var mu sync.Mutex
	reads := map[string]int{}
	deleted := &[]uint64{}
	as.mu.Lock()
	as.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		mu.Lock()
		defer mu.Unlock()
		i := reads[subject]
		reads[subject]++
		if i >= len(script[subject]) {
			return nil, nil, nil
		}
		r := script[subject][i]
		if r.err != nil {
			return nil, nil, r.err
		}
		return r.occ.rec, &StoredMsg{Sequence: r.occ.seq, Subject: subject}, nil
	}
	as.testInflightDelete = func(seq uint64) error {
		mu.Lock()
		defer mu.Unlock()
		*deleted = append(*deleted, seq)
		return nil
	}
	as.mu.Unlock()
	// As in testReleaseSequence: no marker was written beside these registers, so the
	// marker reads ABSENT rather than leaving the fixture with no evidence backend at all.
	testPlantAbsentMarkers(as)
	return deleted
}

// A REQUEST FOR ANOTHER TARGET IS SETTLED BEFORE THE CURRENT SUBJECT SPEAKS (F-098 L4, holistic
// review round 2, findings 1 and 2). The drained delivery was re-offered under a new identifier
// after an earlier attempt recorded Rel(A); the finalize now addresses B. Whatever B says —
// empty, another exchange, our own register — is evidence about B only, so Rel(A) must be
// settled against A's own identity first, and through the same bound release that follows a
// same-identity rewrite.
//
// Mutations: M112 (settle only after the current subject's shortcuts, as round 2 found it) and
// M113 (settle with a single exact-sequence delete, without the rewrite loop).
func TestInflightIdentityReleaseSettlesTheOtherTargetFirst(t *testing.T) {
	t.Parallel()
	const (
		subject    = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.7"
		oldSubject = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.6"
		ourDur     = "$MQTT_abc"
		ourSeq     = uint64(41)
	)
	nonceA := mqttNewInflightNonce()
	recA := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 6, Dur: ourDur, Seq: ourSeq, QoS: 2, Nonce: nonceA})
	heldA := *recA
	heldA.ActionToken = nuid.Next() // a live action: no delete is admitted under it
	relA := &mqttPersistedDrainRelease{Subject: oldSubject, Nonce: nonceA, Acq: recA.Acq}
	ours := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: ourDur, Seq: ourSeq, QoS: 2, Nonce: mqttNewInflightNonce()})
	other := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 7, Dur: "$MQTT_other", Seq: 99, QoS: 2, Nonce: mqttNewInflightNonce()})
	at := func(rec *mqttInflightRecord, seq uint64) testScriptedRead {
		return testScriptedRead{occ: &testInflightOccupant{rec: rec, seq: seq}}
	}

	for _, tc := range []struct {
		name        string
		script      map[string][]testScriptedRead
		want        mqttInflightIdentityOutcome
		wantDel     []uint64
		wantStands  bool // Rel(A) must still be the drain's request
		whyDeletion string
	}{
		{
			name:        "the old target is released even when the current subject is empty",
			script:      map[string][]testScriptedRead{oldSubject: {at(recA, 50)}},
			want:        mqttInflightAlreadyGone,
			wantDel:     []uint64{50},
			whyDeletion: "A still held the drained exchange; an empty B says nothing about it",
		},
		{
			name:        "the old target is released even when another exchange holds the current subject",
			script:      map[string][]testScriptedRead{oldSubject: {at(recA, 50)}, subject: {at(other, 70)}},
			want:        mqttInflightAlreadyGone,
			wantDel:     []uint64{50},
			whyDeletion: "A must be released and B's occupant, another exchange, preserved",
		},
		{
			name:        "an old target that cannot be released keeps its request, whatever the current subject says",
			script:      map[string][]testScriptedRead{oldSubject: {at(&heldA, 50)}},
			want:        mqttInflightUndecided,
			wantStands:  true,
			whyDeletion: "nothing is admitted under a live action",
		},
		{
			name: "a same-identity rewrite of the old target is followed before the request retires",
			script: map[string][]testScriptedRead{
				oldSubject: {at(recA, 50), at(recA, 60)},
				subject:    {at(ours, 300)},
			},
			want:        mqttInflightReleased,
			wantDel:     []uint64{50, 60, 300},
			whyDeletion: "sequence 50 already gone is what a rewrite to 60 looks like; 60 must go before Rel(A) retires",
		},
		{
			name: "an indeterminate confirmation read of the old target keeps its request",
			script: map[string][]testScriptedRead{
				oldSubject: {at(recA, 50), {err: errTestIndeterminate}},
				subject:    {at(ours, 300)},
			},
			want:        mqttInflightUndecided,
			wantDel:     []uint64{50},
			wantStands:  true,
			whyDeletion: "the current target is not touched while the old request is unsettled",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			as := &mqttAccountSessionManager{}
			deleted := testReleaseScript(t, as, tc.script)
			// EVIDENCE PER TARGET, PER ALLOCATION. A and B each have their own marker
			// naming their own nonce, which is the shape of the real store — and the
			// reason the mapping is per subject is that B's evidence must never license
			// a deletion at A. Neither marker helps the live-action row: an action vetoes
			// a deletion that is otherwise fully proven, which is what makes that row
			// discriminating rather than merely unlicensed.
			testPlantUnsentMarkers(as, map[string]string{
				oldSubject: nonceA,
				subject:    ours.Nonce,
			})
			st := &drainSeq{claimed: true, rel: relA}
			sess := &mqttSession{asm: as, drainByDur: map[string]*mqttDrain{
				ourDur: {cc: &ConsumerConfig{Durable: ourDur}, seqs: map[uint64]*drainSeq{ourSeq: st}},
			}}
			if got := as.releaseInflightRegisterByIdentity(nil, sess, subject, ourDur, ourSeq); got != tc.want {
				t.Fatalf("outcome = %d, want %d", got, tc.want)
			}
			if !reflect.DeepEqual(append([]uint64{}, *deleted...), append([]uint64{}, tc.wantDel...)) {
				t.Fatalf("deleted %v, want %v: %s", *deleted, tc.wantDel, tc.whyDeletion)
			}
			sess.mu.Lock()
			stands := st.rel == relA
			sess.mu.Unlock()
			if stands != tc.wantStands {
				t.Fatalf("Rel(%s) standing = %v, want %v", oldSubject, stands, tc.wantStands)
			}
		})
	}
}
