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
	"encoding/json"
	"fmt"
)

// The affine cleanup's readiness assertion (F-105, row 8).
//
// # What this record is, and what it is NOT
//
// It asserts that one retiring generation's two affine prerequisites are done: the PUBREC proofs
// its PUBREL consumer carried are secured into their assignments, and the assignments' discard
// disposition is established. Deleting the affine consumer before either is what loses the only
// evidence that a client answered PUBREC, so the seal — and through the seal, the deletion — is
// refused until this record stands.
//
// It is a TRUSTED ASSERTION, not a fence, and the distinction is load-bearing enough to state
// twice. The write boundary establishes WHOM the record is about — client, account recovery epoch,
// session generation, affine generation and durable, all matched exactly — and that a seal cannot
// be composed without one. It cannot establish that the acts actually ran: matching identifiers
// are not evidence of work. A premature record written by a producer that skipped the acts would
// satisfy the seal rule, which is why the producers are enumerated and audited rather than
// described (see "The trusted producer set" in docs/ARCHITECTURE.md).
//
// # It does not prevent a delayed create either
//
// An earlier design had the affine create path read this record and refuse. That was never a
// fence: a creator that read it as absent and then paused can still land its create afterwards,
// because a read cannot invalidate a request already in flight. The read is deliberately NOT
// there. What a late create leaves is an affine consumer for an ENDED generation, which nothing
// reaps — the same accumulation F-036 course E accepted, reached through a narrower window.
//
// # Create-only, so a second writer converges rather than competing
//
// Two producers may reach it: the teardown, which is the fast path, and the recovery pass, which
// is the finisher for the window where no teardown ever runs. Whichever arrives second finds an
// identical record and treats it as done. A CONFLICTING one — a different affine generation or
// durable for the same session generation — is an error rather than an overwrite, because the two
// cannot both be true of one generation.

// mqttReadinessClassTk is the readiness record's class token.
const mqttReadinessClassTk = "rdy"

// mqttAffineReadiness asserts that one generation's affine prerequisites are complete.
type mqttAffineReadiness struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	// AffGen and Durable name the affine consumer this readiness is about. BOTH are carried,
	// because the durable is derived from the affine generation and a record naming one without
	// the other could be believed about a consumer it was never written for.
	AffGen  string `json:"aff_gen"`
	Durable string `json:"durable"`
	// PubRelDur is the PUBREL consumer whose records the proofs were secured from. Recorded so
	// the assertion says WHAT it covered, in the same way the completion certificate carries the
	// bound it covered rather than a bare flag.
	PubRelDur string `json:"pubrel_dur"`
}

var mqttReadinessMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "aff_gen": {}, "durable": {}, "pubrel_dur": {},
}

// mqttReadinessSubject returns one generation's readiness subject.
func mqttReadinessSubject(idHash, gen string) string {
	return mqttF105SubjectPrefix() + mqttReadinessClassTk + "." + idHash + "." + gen
}

// mqttValidateAffineReadiness refuses a readiness body this broker does not support.
func mqttValidateAffineReadiness(r *mqttAffineReadiness, wantID, wantEpoch, wantGen string) error {
	if r == nil {
		return fmt.Errorf("affine readiness is absent")
	}
	if r.ID != wantID || r.Epoch != wantEpoch || r.Gen != wantGen {
		return fmt.Errorf("affine readiness names %q/%q/%q, not %q/%q/%q",
			r.ID, r.Epoch, r.Gen, wantID, wantEpoch, wantGen)
	}
	if !mqttValidSessGen(r.Gen) {
		return fmt.Errorf("affine readiness carries a malformed generation %q", r.Gen)
	}
	if r.AffGen == _EMPTY_ || r.Durable == _EMPTY_ || r.PubRelDur == _EMPTY_ {
		return fmt.Errorf("affine readiness for generation %q names no affine generation, no "+
			"durable or no PUBREL consumer, so it does not say what it covered", r.Gen)
	}
	// THE SAME DESCRIPTOR DEFINITION the resource and the retirement request are held to.
	//
	// A record may otherwise keep this client, epoch and generation and substitute another
	// well-formed durable or PUBREL consumer — and the readers compare only what they happen to
	// compare, so the seal would rest on an assertion about a different consumer or about proofs
	// secured somewhere else. The names are computable from the identity the record already
	// carries, so the substitution is refused on READ.
	if derr := mqttValidateAffineDescriptor(getHash(r.ID), r.Gen, r.AffGen, r.Durable,
		r.PubRelDur); derr != nil {
		return fmt.Errorf("affine readiness for generation %q: %w", r.Gen, derr)
	}
	return nil
}

// mqttDecodeAffineReadiness decodes and fully validates one readiness body.
func mqttDecodeAffineReadiness(data []byte, wantID, wantEpoch, wantGen string) (*mqttAffineReadiness, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttReadinessMembers, "affine readiness"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttReadinessMembers, "affine readiness"); err != nil {
		return nil, err
	}
	r := &mqttAffineReadiness{}
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}
	if err := mqttValidateAffineReadiness(r, wantID, wantEpoch, wantGen); err != nil {
		return nil, err
	}
	return r, nil
}

// loadAffineReadiness reads one generation's readiness record, nil when none stands.
//
// UNREADABLE IS AN ERROR, never an absence: a record this broker cannot interpret may be the one
// asserting the prerequisites of the very generation a seal is being composed for, and reading it
// as missing would refuse a legal seal — or, at the boundary, admit nothing at all.
func (jsa *mqttJSA) loadAffineReadiness(id, epoch, gen string) (*mqttAffineReadiness, error) {
	subj := mqttReadinessSubject(getHash(id), gen)
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, fmt.Errorf("reading the affine readiness of generation %q: %w", gen, err)
		}
		return nil, nil
	}
	if smsg == nil {
		return nil, nil
	}
	r, derr := mqttDecodeAffineReadiness(smsg.Data, id, epoch, gen)
	if derr != nil {
		return nil, fmt.Errorf("the affine readiness of generation %q is unsupported: %w", gen, derr)
	}
	return r, nil
}

// publishAffineReadiness writes the assertion, create-only.
//
// An occupied subject is NOT a failure: it means another producer got there first, so the standing
// record is read back and compared. Identical is DONE — both producers assert the same fact about
// the same generation. Conflicting is an error, because one generation cannot have two different
// affine consumers, and a writer that found one has been told something it cannot reconcile.
//
// An AMBIGUOUS publication is settled by that same read-back rather than by assuming absence: the
// subject is create-only, so whatever stands there is the answer.
func (jsa *mqttJSA) publishAffineReadiness(r *mqttAffineReadiness) error {
	if err := mqttValidateAffineReadiness(r, r.ID, r.Epoch, r.Gen); err != nil {
		return fmt.Errorf("refusing to publish an unsupported affine readiness: %w", err)
	}
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, serr := jsa.storeMsg(mqttReadinessSubject(getHash(r.ID), r.Gen), hdr, msg); serr != nil {
		stood, lerr := jsa.loadAffineReadiness(r.ID, r.Epoch, r.Gen)
		if lerr != nil {
			return fmt.Errorf("publishing the affine readiness of generation %q was unresolved "+
				"(%v) and the standing record could not be read: %w", r.Gen, serr, lerr)
		}
		if stood == nil {
			return fmt.Errorf("publishing the affine readiness of generation %q: %w", r.Gen, serr)
		}
		if stood.AffGen != r.AffGen || stood.Durable != r.Durable || stood.PubRelDur != r.PubRelDur {
			return fmt.Errorf("the affine readiness standing for generation %q covers %q/%q/%q, "+
				"and this producer asserts %q/%q/%q: one generation has one affine consumer, so "+
				"these cannot both be true", r.Gen, stood.AffGen, stood.Durable, stood.PubRelDur,
				r.AffGen, r.Durable, r.PubRelDur)
		}
	}
	return nil
}
