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

import (
	"encoding/json"
	"fmt"
	"strings"
)

// A retained exchange's CANONICAL PUBREL (F-105 7c).
//
// An ordinary QoS 2 exchange's release is a record on $MQTT_out delivered by the PUBREL consumer;
// storing it there and re-phasing the slot on $MQTT_inflight can never be one write, which is why
// the ordinary path has pubrel_intended and its owed materialization. A retained exchange has no
// consumer and every other record of it is on $MQTT_inflight, so its release is a record THERE, at
// an exchange-specific subject, CREATED in the same advance batch that moves the exchange to
// PubRecObserved and the register to pubrel_ready. There is no window in which the client has
// answered PUBREC and nothing durable says the release is owed.
//
// The record carries an IDENTITY beside its address (mqttCanonicalRef's in-flight arm): a subject
// can be rewritten, and only the identity says the record standing there is this exchange's.
// It is never rewritten. It goes with the exchange — removed by exact sequence by the exchange's
// terminal cleanup after PUBCOMP — and is never touched by payload reclamation.

const mqttRelClassTk = "rel"

// mqttRetainedRelSubject is one retained exchange's canonical PUBREL subject.
func mqttRetainedRelSubject(idHash, gen, nonce string) string {
	return mqttF105SubjectPrefix() + mqttRelClassTk + "." + idHash + "." + gen + "." + nonce
}

func mqttIsRetainedRelSubject(subject string) bool {
	return strings.HasPrefix(subject, mqttF105SubjectPrefix()+mqttRelClassTk+".")
}

// mqttRetainedRel is the record standing at that subject.
type mqttRetainedRel struct {
	ID       string `json:"id"`
	Epoch    string `json:"epoch"`
	Gen      string `json:"gen"`
	Nonce    string `json:"nonce"`
	PI       uint16 `json:"pi"`
	Identity string `json:"identity"`
}

var mqttRetainedRelMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "nonce": {}, "pi": {}, "identity": {},
}

// mqttDecodeRetainedRel reads a canonical PUBREL the caller knows belongs to a named exchange.
func mqttDecodeRetainedRel(data []byte, wantID, wantEpoch, wantGen, wantNonce string) (*mqttRetainedRel, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttRetainedRelMembers, "a retained canonical PUBREL"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttRetainedRelMembers, "a retained canonical PUBREL"); err != nil {
		return nil, err
	}
	r := &mqttRetainedRel{}
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}
	switch {
	case r.ID != wantID || r.Epoch != wantEpoch || r.Gen != wantGen || r.Nonce != wantNonce:
		return nil, fmt.Errorf("retained canonical PUBREL names %q/%q/%q/%q, not %q/%q/%q/%q",
			r.ID, r.Epoch, r.Gen, r.Nonce, wantID, wantEpoch, wantGen, wantNonce)
	case r.PI == 0:
		return nil, fmt.Errorf("retained canonical PUBREL names no packet identifier")
	case !mqttF105IdentTk(r.Identity):
		return nil, fmt.Errorf("retained canonical PUBREL carries a malformed identity %q", r.Identity)
	}
	return r, nil
}

// mqttCheckRetainedRelMember is the per-member rule: an owned, well-formed, CREATE-ONLY record,
// carried only by the advance that installs it.
func mqttCheckRetainedRelMember(a *mqttTransitionAttempt, idHash string, e mqttTransitionMember) error {
	toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
	if len(toks) != 4 {
		return fmt.Errorf("transition %q carries a malformed canonical PUBREL subject %q", a.Txn, e.subj)
	}
	if toks[1] != idHash {
		return fmt.Errorf("transition %q carries canonical PUBREL %q, which belongs to another client", a.Txn, e.subj)
	}
	if !mqttValidSessGen(toks[2]) {
		return fmt.Errorf("transition %q carries canonical PUBREL %q with a malformed generation token", a.Txn, e.subj)
	}
	if a.Kind != mqttTransitionAdvanceExchange {
		return fmt.Errorf("transition %q of kind %q carries canonical PUBREL %q: it is created only by the "+
			"retained PUBREC advance", a.Txn, a.Kind, e.subj)
	}
	if e.expect != 0 {
		return fmt.Errorf("transition %q rewrites canonical PUBREL %q, which is never rewritten", a.Txn, e.subj)
	}
	if _, derr := mqttDecodeRetainedRel(e.body, a.ID, a.Epoch, toks[2], toks[3]); derr != nil {
		return fmt.Errorf("transition %q carries an unsupported canonical PUBREL on %q: %w", a.Txn, e.subj, derr)
	}
	return nil
}

// checkRetainedRelMembers binds the batch's canonical PUBRELs to its retained PUBREC arrows, in
// BOTH directions: every arrow into pubrel_ready creates exactly its record, and every record is
// created by exactly such an arrow, agreeing on generation, exchange, identifier and identity. The
// Publish-origin arrow additionally carries its copy's manifest (the fence closure arbitrates on);
// the abandoned-origin arrow needs no material, and is fenced instead by the generation's closure —
// a generation closed with Discard takes no new canonical (an unreadable closure refuses).
func (jsa *mqttJSA) checkRetainedRelMembers(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	idHash := getHash(a.ID)
	rels := map[string]*mqttRetainedRel{}
	for _, e := range extra {
		if !mqttIsRetainedRelSubject(e.subj) {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			return fmt.Errorf("transition %q carries a malformed canonical PUBREL subject %q", a.Txn, e.subj)
		}
		r, derr := mqttDecodeRetainedRel(e.body, a.ID, a.Epoch, toks[2], toks[3])
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported canonical PUBREL on %q: %w", a.Txn, e.subj, derr)
		}
		rels[e.subj] = r
	}
	used := map[string]bool{}
	for _, e := range extra {
		if mqttIsF105Subject(e.subj) || e.expect == 0 {
			continue
		}
		name, perr := mqttParseInflightSubject(e.subj)
		if perr != nil {
			continue
		}
		rec, derr := mqttDecodeInflightRecord(e.body)
		if derr != nil || !rec.retainedSource() || rec.State != mqttInflightPubRelReady {
			continue
		}
		cur, smsg, lerr := jsa.loadInflightRecord(e.subj)
		if lerr != nil || smsg == nil || cur == nil {
			return fmt.Errorf("transition %q could not read the retained slot it advances on %q (%v)", a.Txn, e.subj, lerr)
		}
		if cur.State == mqttInflightPubRelReady {
			continue // not an arrival: a token acquisition or other same-phase write
		}
		want := mqttRetainedRelSubject(idHash, name.gen, rec.Lin)
		rel := rels[want]
		switch {
		case rec.RelSubj != want || rel == nil:
			return fmt.Errorf("transition %q advances retained slot %q to pubrel_ready without creating its "+
				"canonical PUBREL at %q", a.Txn, e.subj, want)
		case rel.Identity != rec.RelID || rel.PI != rec.PI || rel.Gen != name.gen:
			return fmt.Errorf("transition %q creates canonical PUBREL %q disagreeing with the slot it answers",
				a.Txn, want)
		}
		used[want] = true
		switch cur.State {
		case mqttInflightPublish:
			if !mqttBatchCarriesManifest(extra, idHash, name.gen, rec.Payload) {
				return fmt.Errorf("transition %q advances retained slot %q past PUBREC without fencing on its "+
					"copy's manifest", a.Txn, e.subj)
			}
		case mqttInflightDeliveryAbandoned:
			closure, _, cst := jsa.loadGenClosure(a.ID, a.Epoch, name.gen)
			if cst != mqttOutboundValid && cst != mqttOutboundAbsent {
				return fmt.Errorf("transition %q advances abandoned retained slot %q while generation %q's "+
					"closure cannot be read (status %v)", a.Txn, e.subj, name.gen, cst)
			}
			if closure != nil && closure.Discard {
				return fmt.Errorf("transition %q advances abandoned retained slot %q in discarded generation %q: "+
					"a discarded generation takes no new canonical PUBREL", a.Txn, e.subj, name.gen)
			}
		default:
			return fmt.Errorf("transition %q advances retained slot %q from %q to pubrel_ready", a.Txn, e.subj,
				string(cur.State))
		}
	}
	for subj := range rels {
		if !used[subj] {
			return fmt.Errorf("transition %q creates canonical PUBREL %q with no retained PUBREC arrow beside it",
				a.Txn, subj)
		}
	}
	return nil
}

// mqttBatchCarriesManifest reports whether the batch carries the manifest of that copy.
func mqttBatchCarriesManifest(extra []mqttTransitionMember, idHash, gen, payload string) bool {
	want := mqttManifestSubject(idHash, gen, payload)
	for _, e := range extra {
		if e.subj == want && e.expect != 0 {
			return true
		}
	}
	return false
}
