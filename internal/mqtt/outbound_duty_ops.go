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
)

// The transitions a recovery pass composes (F-105, row 6).

// mqttDutyOutcomeResolved is what a duty says when the thing it is about reached a durable ending
// of its own — an obligation that terminated, or a resource whose provisioning sealed. A CLEANUP
// duty says something more specific: the cleanup's own classified outcome.
const mqttDutyOutcomeResolved = "resolved"

// loadDutyEntryAt reads one duty entry with the sequence it stands at.
func (jsa *mqttJSA) loadDutyEntryAt(id, epoch, gen string, ord uint64) (*mqttDutyEntry, uint64, error) {
	subj := mqttDutySubject(getHash(id), gen, ord)
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, fmt.Errorf("reading duty %d of generation %q: %w", ord, gen, err)
		}
		return nil, 0, nil
	}
	if smsg == nil {
		return nil, 0, nil
	}
	d, derr := mqttDecodeDutyEntry(smsg.Data, id, epoch, gen, ord)
	if derr != nil {
		return nil, 0, fmt.Errorf("duty %d of generation %q cannot be interpreted: %w", ord, gen, derr)
	}
	return d, smsg.Sequence, nil
}

// mqttResolvedDutyBody is the terminal form of an entry, with its identity preserved exactly.
//
// IDENTITY NEVER MOVES: an entry is predeclared for one thing and continued as that thing, so the
// kind and the reference are copied from the STANDING record rather than restated by the caller.
func mqttResolvedDutyBody(cur *mqttDutyEntry, outcome string) ([]byte, error) {
	next := *cur
	next.State, next.Outcome = mqttDutyTerminal, outcome
	if verr := mqttValidateDutyEntry(&next, cur.ID, cur.Epoch, cur.Gen, cur.Ord); verr != nil {
		return nil, fmt.Errorf("refusing to compose an unsupported terminal duty: %w", verr)
	}
	return json.Marshal(&next)
}

// opResolveDuty continues one duty entry to terminal, and carries nothing else.
//
// The boundary re-reads the standing entry and judges the ending against the records the duty is
// about: a duty whose obligation has not ended is refused here whatever the caller believes, and a
// terminal entry is immutable, so this cannot reopen one.
func (jsa *mqttJSA) opResolveDuty(root *mqttRecoveryRoot, d mqttSweptDuty, txn string) error {
	auth, authSeq, err := jsa.requireAuthority(root.ID, root.Epoch)
	if err != nil {
		return err
	}
	if auth == nil {
		// AN ABSENT AUTHORITY WITHHOLDS, and is never read as permission.
		return fmt.Errorf("client %q has no outbound authority, so no duty of its may be resolved",
			root.ID)
	}
	cur, curSeq, lerr := jsa.loadDutyEntryAt(root.ID, root.Epoch, root.Gen, d.Ord)
	if lerr != nil {
		return lerr
	}
	if cur == nil {
		return fmt.Errorf("duty %d of generation %q no longer stands, so there is nothing to "+
			"continue: a continuation over an absent entry would invent the duty it claims to "+
			"advance", d.Ord, root.Gen)
	}
	// THE OUTCOME IS WHAT MAKES IT TERMINAL, so an ending says what it was. Here the duty ended
	// because the thing it is about reached a durable ending of its own — which the boundary
	// re-derives from that thing rather than from this word.
	body, berr := mqttResolvedDutyBody(cur, mqttDutyOutcomeResolved)
	if berr != nil {
		return berr
	}
	_, cerr := jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: root.ID, Epoch: root.Epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionDisposeResource, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: authSeq,
		Auth: &mqttOutboundAuthority{
			ID: auth.ID, Epoch: auth.Epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: auth.State, Rev: auth.Rev + 1, Txn: txn, OwnerTxn: auth.OwnerTxn,
			Intent: auth.Intent, Creation: auth.Creation,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttDutySubject(getHash(root.ID), root.Gen, d.Ord),
			expect: curSeq,
			body:   body,
		}},
	})
	return cerr
}

// completeCleanupAndResolve records the classified physical outcome AND ends the duty, in ONE
// batch.
//
// Two members rather than two transitions, deliberately: completing a resource does not complete
// its inventory entry, and a crash between the two would leave a cleanup that is finished and a
// duty that says otherwise — which the next pass would re-execute. One batch removes the window
// rather than reasoning about it.
//
// The boundary judges the duty's ending against the state THIS BATCH RESULTS IN, so the completion
// member it carries is what earns the ending. That is the S-xor-Delta property this project
// already names: a batch is atomic, so members that could never coexist may be written together,
// and a rule reading only the STORED record would refuse a batch that is correct.
func (jsa *mqttJSA) completeCleanupAndResolve(root *mqttRecoveryRoot, r *mqttOutboundResource,
	resSeq uint64, d mqttSweptDuty, txn string, outcome mqttCleanupOutcome) error {

	auth, authSeq, err := jsa.requireAuthority(root.ID, root.Epoch)
	if err != nil {
		return err
	}
	if auth == nil {
		return fmt.Errorf("client %q has no outbound authority, so no cleanup of its may be completed",
			root.ID)
	}
	if r.State != mqttResourceDeleteAuthorized {
		return fmt.Errorf("resource %q is in state %d, so there is no authorized cleanup to complete",
			r.Inc, r.State)
	}
	if !mqttValidCleanupOutcome(outcome) {
		return fmt.Errorf("resource %q was offered an unsupported cleanup outcome %q", r.Inc, outcome)
	}
	next := *r
	next.State, next.Outcome = mqttResourceCleanupComplete, outcome
	rbody, merr := json.Marshal(&next)
	if merr != nil {
		return merr
	}
	cur, curSeq, lerr := jsa.loadDutyEntryAt(root.ID, root.Epoch, root.Gen, d.Ord)
	if lerr != nil {
		return lerr
	}
	if cur == nil {
		return fmt.Errorf("duty %d of generation %q no longer stands", d.Ord, root.Gen)
	}
	// THE DUTY'S OUTCOME IS THE CLEANUP'S OWN, so the entry says what ended it rather than
	// carrying a word of its own that could disagree with the resource beside it.
	dbody, berr := mqttResolvedDutyBody(cur, string(outcome))
	if berr != nil {
		return berr
	}
	idHash := getHash(root.ID)
	_, cerr := jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: root.ID, Epoch: root.Epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionDisposeResource, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: authSeq,
		Auth: &mqttOutboundAuthority{
			ID: auth.ID, Epoch: auth.Epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: auth.State, Rev: auth.Rev + 1, Txn: txn, OwnerTxn: auth.OwnerTxn,
			Intent: auth.Intent, Creation: auth.Creation,
		},
		Extra: []mqttTransitionMember{
			{subj: mqttResourceSubject(idHash, r.Gen, r.Inc), expect: resSeq, body: rbody},
			{subj: mqttDutySubject(idHash, root.Gen, d.Ord), expect: curSeq, body: dbody},
		},
	})
	return cerr
}

// opSealAffineProvisioning seals a ready affine resource's provisioning.
//
// The boundary admits PENDING -> SEALED for an affine resource ONLY against a standing readiness
// record naming exactly this generation, affine generation and durable. What that establishes is
// WHOM the assertion is about; that the prerequisites actually ran is the producer's assertion,
// audited rather than enforced.
func (jsa *mqttJSA) opSealAffineProvisioning(root *mqttRecoveryRoot, r *mqttOutboundResource,
	resSeq uint64, txn string) error {

	auth, authSeq, err := jsa.requireAuthority(root.ID, root.Epoch)
	if err != nil {
		return err
	}
	if auth == nil {
		return fmt.Errorf("client %q has no outbound authority, so no resource of its may be sealed",
			root.ID)
	}
	tr, terr := mqttSealProvisioningTransition(auth, authSeq, txn, "op-"+txn, r, resSeq)
	if terr != nil {
		return terr
	}
	_, cerr := jsa.commitOp(tr)
	return cerr
}

// publishGenCompletionCert writes one generation's completion certificate, create-only.
//
// # An absent read after a timeout is NOT proof it never landed
//
// The original publication may still commit, so absence is UNRESOLVED and the caller retries the
// IDENTICAL certificate; only a present, validated, matching record settles success. That is why
// this reports an error on an unresolved publication rather than reporting failure: the root
// deletion that follows must wait for a certificate that is known to stand.
//
// # A standing certificate is VALIDATED, never overwritten
//
// Two brokers may retire one generation, and they will write the same facts — the same client,
// epoch, generation and sealed bound. An identical record is DONE. A conflicting one is an error
// and retirement stops: two different bounds cannot both describe one sealed inventory, and
// overwriting would destroy the evidence the release gate reads after the root is gone.
func (jsa *mqttJSA) publishGenCompletionCert(c *mqttGenCompletionCertificate) error {
	if verr := mqttValidateGenCert(c, c.ID, c.Epoch, c.Gen); verr != nil {
		return fmt.Errorf("refusing to publish an unsupported completion certificate: %w", verr)
	}
	body, err := json.Marshal(c)
	if err != nil {
		return err
	}
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, serr := jsa.storeMsg(mqttGenCertSubject(getHash(c.ID), c.Gen), hdr, msg); serr != nil {
		stood, lerr := jsa.loadGenCompletionCert(c.ID, c.Epoch, c.Gen)
		if lerr != nil {
			return fmt.Errorf("certifying generation %q was unresolved (%v) and the standing "+
				"certificate could not be read: %w", c.Gen, serr, lerr)
		}
		if stood == nil {
			// UNRESOLVED, not failed: the publication may still commit. The caller retries the
			// identical certificate and the root stays until one is seen to stand.
			return fmt.Errorf("certifying generation %q is unresolved: %w", c.Gen, serr)
		}
		if stood.SealedHi != c.SealedHi {
			return fmt.Errorf("the certificate standing for generation %q covers bound %d and "+
				"this one covers %d: one sealed inventory has one bound, so these cannot both be "+
				"true", c.Gen, stood.SealedHi, c.SealedHi)
		}
	}
	return nil
}

// cleanupDutyStandsFor reports whether a CLEANUP duty entry names this resource incarnation.
//
// Walked from the ledger's own bound rather than guessed at: an enrolment allocates its entry
// inside the batch that seals, so every entry that can name this incarnation is at an ordinal at
// or below the sealed high-water. An unreadable ledger or entry is an ERROR — this answer
// authorizes a teardown to STOP deleting something, so "I could not read it" must not read as
// "it is enrolled".
func (jsa *mqttJSA) cleanupDutyStandsFor(id, epoch, gen, inc string) (bool, error) {
	led, _, lerr := jsa.loadGenLedger(id, epoch, gen)
	if lerr != nil {
		return false, lerr
	}
	if led == nil {
		return false, nil
	}
	bound := led.Hi
	if led.State == mqttGenLedgerSealed {
		bound = led.SealedHi
	}
	for ord := uint64(1); ord <= bound; ord++ {
		d, _, derr := jsa.loadDutyEntryAt(id, epoch, gen, ord)
		if derr != nil {
			return false, derr
		}
		if d == nil {
			// A MISSING ENTRY IS AN INCONSISTENCY, not an answer: the allocation that took this
			// ordinal wrote one, so a caller about to stop deleting on the strength of this walk
			// may not treat the gap as "not here".
			return false, fmt.Errorf("duty %d of generation %q is missing, so this walk cannot "+
				"say which duties the generation carries", ord, gen)
		}
		if d.Kind == mqttDutyCleanup && d.Ref == inc {
			return true, nil
		}
	}
	return false, nil
}
