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
	"errors"
	"fmt"
	"strings"
)

// The retained manifest ARBITRATES (F-105 7c).
//
// Three acts touch a READY copy and must exclude each other, and they do it by all CASing the one
// record rather than by observing each other:
//
//   - ADMISSION writes `admitted` READY -> READY in the batch that creates the exchange, the
//     register and the material edge;
//   - the retained PUBREC advance rewrites it UNCHANGED, so a copy whose cleanup already closed it
//     cannot have a canonical PUBREL created against it afterwards;
//   - CLOSE (the first act of reclamation) moves it to CLOSED, and is admitted only on an
//     eligibility the boundary re-reads inside the batch — never on the closer's earlier view.
//
// A cleanup that observed "no dependency" and then closed would be an observation, not a fence: an
// admission landing in between would leave an obligation whose material is being removed. With
// both acts on one sequence, whichever lands first makes the other's CAS fail.

// checkRetainedManifestArbitration applies those rules to one manifest step. prev is the authority
// predecessor the batch CASes; cur is the stored manifest and next the batch's.
func (jsa *mqttJSA) checkRetainedManifestArbitration(a *mqttTransitionAttempt, prev *mqttOutboundAuthority,
	extra []mqttTransitionMember, cur, next *mqttRetainedManifest) error {

	switch {
	case cur.Admitted == _EMPTY_ && next.Admitted != _EMPTY_:
		return jsa.checkRetainedAdmission(a, extra, next)
	case next.State == mqttManifestClosed && cur.State != mqttManifestClosed:
		return jsa.checkRetainedClose(a, prev, cur)
	case cur.State == mqttManifestReady && mqttRetainedManifestUnchanged(cur, next):
		// AN UNCHANGED REWRITE IS A FENCE, and only the retained PUBREC advance takes one: it is
		// how a canonical PUBREL is forbidden once cleanup has closed the copy. Any other kind
		// rewriting a manifest to itself is a write with nothing to say.
		if a.Kind != mqttTransitionAdvanceExchange {
			return fmt.Errorf("transition %q of kind %q rewrites manifest %q unchanged: only the retained "+
				"PUBREC advance fences on the copy that way", a.Txn, a.Kind, cur.Payload)
		}
		if cur.Admitted == _EMPTY_ {
			return fmt.Errorf("transition %q fences an advance on manifest %q, which no exchange was "+
				"admitted against", a.Txn, cur.Payload)
		}
		if !mqttBatchCarriesExchange(a, extra, cur.Gen, cur.Admitted) {
			return fmt.Errorf("transition %q fences on manifest %q, admitted for exchange %q, without "+
				"advancing that exchange", a.Txn, cur.Payload, cur.Admitted)
		}
	}
	return nil
}

// mqttRetainedManifestUnchanged reports whether next restates cur exactly.
func mqttRetainedManifestUnchanged(cur, next *mqttRetainedManifest) bool {
	return cur.State == next.State && cur.Written == next.Written && cur.Announced == next.Announced &&
		cur.Purged == next.Purged && cur.Admitted == next.Admitted && cur.AdmittedPI == next.AdmittedPI
}

// mqttBatchCarriesExchange reports whether the batch carries a member for that exact exchange.
func mqttBatchCarriesExchange(a *mqttTransitionAttempt, extra []mqttTransitionMember, gen, nonce string) bool {
	want := mqttExchangeSubject(getHash(a.ID), gen, nonce)
	for _, e := range extra {
		if e.subj == want {
			return true
		}
	}
	return false
}

// checkRetainedAdmission requires the admission batch to be exactly the obligation the copy names:
// the create-only exchange `admitted` at the copy's QoS, its RETAINED register agreeing on payload,
// digest and lineage, and the copy's own owner taking the retained-material edge for it. Stated for
// the BATCH, so a direct submission that writes `admitted` without the obligation — or the
// obligation without `admitted` — is refused here and not only by a careful producer.
func (jsa *mqttJSA) checkRetainedAdmission(a *mqttTransitionAttempt, extra []mqttTransitionMember,
	m *mqttRetainedManifest) error {

	if a.Kind != mqttTransitionAdmitExchange {
		return fmt.Errorf("transition %q of kind %q admits against manifest %q: an admission is the "+
			"batch that creates the exchange", a.Txn, a.Kind, m.Payload)
	}
	idHash := getHash(a.ID)
	xsubj := mqttExchangeSubject(idHash, m.Gen, m.Admitted)
	rsubj := mqttResourceSubject(idHash, m.Gen, m.Inc)
	var x *mqttOutboundExchange
	var reg *mqttInflightRecord
	var regSubj string
	edge := false
	for _, e := range extra {
		switch e.subj {
		case xsubj:
			if e.expect != 0 {
				return fmt.Errorf("transition %q admits against manifest %q over a standing exchange", a.Txn, m.Payload)
			}
			got, derr := mqttDecodeOutboundExchange(e.body, a.ID, a.Epoch, m.Gen, m.Admitted)
			if derr != nil {
				return fmt.Errorf("transition %q admits an unsupported exchange against manifest %q: %w", a.Txn, m.Payload, derr)
			}
			x = got
		case rsubj:
			r, derr := mqttDecodeOutboundResource(e.body, a.ID, a.Epoch, m.Gen, m.Inc)
			if derr != nil {
				return fmt.Errorf("transition %q carries the owner of manifest %q in an unsupported shape: %w", a.Txn, m.Payload, derr)
			}
			for _, d := range r.Deps {
				if d.Nonce == m.Admitted && d.Role == mqttRoleRetainedMaterial {
					edge = true
				}
			}
		default:
			if mqttIsF105Subject(e.subj) {
				continue
			}
			if r, derr := mqttDecodeInflightRecord(e.body); derr == nil && r.retainedSource() && r.Lin == m.Admitted {
				reg, regSubj = r, e.subj
			}
		}
	}
	switch {
	case x == nil:
		return fmt.Errorf("transition %q names exchange %q admitted against manifest %q without creating it",
			a.Txn, m.Admitted, m.Payload)
	case x.QoS != m.QoS:
		return fmt.Errorf("transition %q admits a QoS %d exchange against a QoS %d copy", a.Txn, x.QoS, m.QoS)
	case x.RetainedOwner != m.Inc:
		// THE OWNER ADDRESS is the copy's own owner (F-112): the floor reads it to prove the
		// material edge released, so an exchange naming another incarnation, or none, would
		// point that proof at a resource this obligation never held.
		return fmt.Errorf("transition %q admits exchange %q against manifest %q owned by %q while the "+
			"exchange names owner %q", a.Txn, m.Admitted, m.Payload, m.Inc, x.RetainedOwner)
	case reg == nil:
		return fmt.Errorf("transition %q admits exchange %q against manifest %q with no retained register for it",
			a.Txn, m.Admitted, m.Payload)
	case reg.Payload != m.Payload || reg.Digest != m.Digest || reg.QoS != m.QoS:
		return fmt.Errorf("transition %q admits a register naming %q/%q at QoS %d against manifest %q/%q at QoS %d",
			a.Txn, reg.Payload, reg.Digest, reg.QoS, m.Payload, m.Digest, m.QoS)
	case !edge:
		return fmt.Errorf("transition %q admits exchange %q against manifest %q without the copy's owner %q "+
			"taking its retained-material edge", a.Txn, m.Admitted, m.Payload, m.Inc)
	case m.AdmittedPI == 0 || x.PI != m.AdmittedPI || reg.PI != m.AdmittedPI ||
		regSubj != mqttInflightSubject(idHash, m.Gen, m.AdmittedPI):
		// THE ADMISSION ADDRESS is bound to the exchange, the register and the register's subject in
		// the batch that writes all three (consult 109).
		return fmt.Errorf("transition %q admits exchange %q against manifest %q at identifier %d, while the exchange "+
			"names %d and its register %d on %q", a.Txn, m.Admitted, m.Payload, m.AdmittedPI, x.PI, reg.PI, regSubj)
	}
	return nil
}

// checkRetainedClose admits a manifest's CLOSE only on eligibility read inside the batch.
//
// An ADMITTED copy may close once its exchange can no longer need it: ended, carrying the retained
// abandonment fact for this payload (in any successor phase — a late PUBREC advances the phase and
// keeps the fact), or its generation closed with Discard. No drain proof: no acquired action reads
// material (a first send emits the bytes it holds; a replay verifies before it acquires).
//
// An UNADMITTED copy may close only when its producer can write no further batch, POSITIVELY: a
// successor tenure serves the generation, or the generation's closure stands — or the closer IS the
// producer, presenting the entitlement the copy was reserved under while it still serves (its own
// definitive failure or its client's departure). An unreadable authority or closure refuses;
// "not serving" alone refuses, because a Preparing authority can be cancelled back to the producer.
func (jsa *mqttJSA) checkRetainedClose(a *mqttTransitionAttempt, prev *mqttOutboundAuthority, m *mqttRetainedManifest) error {
	_, err := jsa.retainedCloseArm(a, prev, m)
	return err
}

// errMQTTRetainedMaterialNeeded is the close boundary's POSITIVE refusal: the copy's admitted
// exchange is live and may still replay it. Not a store failure — the exchange's own ending is what
// makes the copy closable, and every ending wakes its reclamation.
var errMQTTRetainedMaterialNeeded = errors.New("its exchange may still need its material")

// The arms that admit a retained close, by name, so a test can say WHICH one admitted it.
const (
	mqttRetainedCloseDiscarded = "discarded-generation" // admitted copy: its generation was discarded
	mqttRetainedCloseEnded     = "exchange-ended"       // admitted copy: its exchange ended
	mqttRetainedCloseAbandoned = "abandoned"            // admitted copy: the retained abandonment fact
	mqttRetainedCloseClosure   = "closure"              // unadmitted copy: the generation's closure stands
	mqttRetainedCloseSuccessor = "successor-tenure"     // unadmitted copy: a successor tenure serves
	mqttRetainedCloseProducer  = "producer"             // unadmitted copy: the producer's own entitlement
)

// retainedCloseArm decides a retained close and names the arm that admitted it.
func (jsa *mqttJSA) retainedCloseArm(a *mqttTransitionAttempt, prev *mqttOutboundAuthority, m *mqttRetainedManifest) (string, error) {
	closure, _, cst := jsa.loadGenClosure(a.ID, a.Epoch, m.Gen)
	if cst != mqttOutboundValid && cst != mqttOutboundAbsent {
		return _EMPTY_, fmt.Errorf("transition %q closes manifest %q while generation %q's closure cannot be read "+
			"(status %v)", a.Txn, m.Payload, m.Gen, cst)
	}
	if m.Admitted != _EMPTY_ {
		if closure != nil && closure.Discard {
			return mqttRetainedCloseDiscarded, nil
		}
		x, _, xerr := jsa.loadOutboundExchange(a.ID, a.Epoch, m.Gen, m.Admitted)
		if xerr != nil || x == nil {
			return _EMPTY_, fmt.Errorf("transition %q closes manifest %q while its exchange %q cannot be read (%v)",
				a.Txn, m.Payload, m.Admitted, xerr)
		}
		arm := _EMPTY_
		switch {
		case mqttExchangePhaseEnded(x.Phase):
			arm = mqttRetainedCloseEnded
		case x.Abandoned.retained() && x.Abandoned.Payload == m.Payload:
			arm = mqttRetainedCloseAbandoned
		}
		if arm != _EMPTY_ {
			// THE EXCHANGE'S OWN REGISTER MUST AGREE (consult 107): an ending or an abandonment the
			// register contradicts — the register of this allocation still standing at PUBLISH, or at
			// pubrel_ready for an ending — is a record the store got wrong, which a resume quarantines
			// and which nothing may act on, so the material it may yet be restored from is kept. An
			// unreadable register decides nothing and refuses.
			if err := jsa.retainedRegisterAgrees(a, m, x, arm); err != nil {
				return _EMPTY_, err
			}
			return arm, nil
		}
		return _EMPTY_, fmt.Errorf("transition %q closes manifest %q while exchange %q at phase %d: %w",
			a.Txn, m.Payload, m.Admitted, int(x.Phase), errMQTTRetainedMaterialNeeded)
	}
	if closure != nil {
		return mqttRetainedCloseClosure, nil
	}
	if prev == nil || prev.State != mqttAuthorityServing || prev.Gen != m.Gen {
		return _EMPTY_, fmt.Errorf("transition %q closes unadmitted manifest %q without a positively established "+
			"successor or closure", a.Txn, m.Payload)
	}
	if prev.Tenure != m.Tenure {
		return mqttRetainedCloseSuccessor, nil
	}
	if a.Gen == m.Gen && a.Tenure == m.Tenure {
		return mqttRetainedCloseProducer, nil
	}
	return _EMPTY_, fmt.Errorf("transition %q closes unadmitted manifest %q while its producing tenure %q still "+
		"serves and the closer does not present it", a.Txn, m.Payload, m.Tenure)
}

// retainedRegisterAgrees checks an ended or abandoned exchange against its allocation's register,
// read from the store inside the closing batch, by the ONE consistency definition recovery restores
// pairs with (retainedPairVerdict, consult 109): anything it does not admit is a contradiction a
// resume quarantines and nothing may act on, so the close refuses as material still needed. The
// register is located by the copy's own ADMISSION ADDRESS, never by the exchange's PI, and the two
// must agree before an absence or another occupant may count for anything.
func (jsa *mqttJSA) retainedRegisterAgrees(a *mqttTransitionAttempt, m *mqttRetainedManifest,
	x *mqttOutboundExchange, arm string) error {
	refuse := func(why string) error {
		return fmt.Errorf("transition %q closes manifest %q while exchange %q reads %s but %s: %w",
			a.Txn, m.Payload, m.Admitted, arm, why, errMQTTRetainedMaterialNeeded)
	}
	if m.AdmittedPI == 0 || x.PI != m.AdmittedPI {
		return refuse(fmt.Sprintf("its identifier %d contradicts the copy's admission address %d", x.PI, m.AdmittedPI))
	}
	subject := mqttInflightSubject(getHash(a.ID), m.Gen, m.AdmittedPI)
	reg, smsg, err := jsa.loadInflightRecord(subject)
	if err != nil {
		return fmt.Errorf("transition %q closes manifest %q while the register of exchange %q cannot be read (%v)",
			a.Txn, m.Payload, m.Admitted, err)
	}
	switch {
	case smsg != nil && reg == nil:
		return refuse("its register is a shape this broker refuses")
	case smsg == nil || reg.Nonce != m.Admitted:
		// GONE, OR THE IDENTIFIER REUSED: legitimate only for an ENDED exchange, whose terminal cleanup
		// deletes the register. An abandoned exchange keeps its register until an answer or a discard.
		if arm == mqttRetainedCloseEnded {
			return nil
		}
		return refuse("its register is gone or another allocation's")
	}
	agrees, reason, verr := jsa.retainedPairVerdict(a.ID, a.Epoch, m.Gen, reg, x, mqttCanonicalRequired)
	if verr != nil {
		return fmt.Errorf("transition %q closes manifest %q while the canonical PUBREL of exchange %q cannot be read (%v)",
			a.Txn, m.Payload, m.Admitted, verr)
	}
	if !agrees || reg.Payload != m.Payload {
		return refuse(reason)
	}
	return nil
}

// retainedMaterialNoLongerNeeded is the retained-material edge's release floor beyond an ending:
// the exchange carries the retained abandonment fact for this payload, or its generation's closure
// stands with Discard. Read from the store inside the releasing batch; an unreadable read refuses.
func (jsa *mqttJSA) retainedMaterialNoLongerNeeded(a *mqttTransitionAttempt, r *mqttOutboundResource, nonce string) error {
	closure, _, cst := jsa.loadGenClosure(a.ID, a.Epoch, r.Gen)
	if cst == mqttOutboundValid && closure != nil && closure.Discard {
		return nil
	}
	x, _, xerr := jsa.loadOutboundExchange(a.ID, a.Epoch, r.Gen, nonce)
	if xerr != nil || x == nil {
		return fmt.Errorf("its exchange record could not be read (%v)", xerr)
	}
	if f := x.Abandoned; f.retained() && f.Payload == r.Name {
		return nil
	}
	return fmt.Errorf("it carries no retained abandonment fact and its generation was not discarded")
}

// loadRetainedManifest reads one copy's manifest and the sequence it stands at. Absence is an
// error: every caller is acting on a copy it was told exists.
func (jsa *mqttJSA) loadRetainedManifest(id, epoch, gen, payload string) (*mqttRetainedManifest, uint64, error) {
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, mqttManifestSubject(getHash(id), gen, payload))
	if err != nil {
		if !mqttStoreAnswered(err) {
			err = fmt.Errorf("%w: %w", errMQTTStoreUnreadable, err)
		}
		return nil, 0, fmt.Errorf("the manifest of payload %q could not be read: %w", payload, err)
	}
	m, derr := mqttDecodeRetainedManifest(smsg.Data, id, epoch, gen, payload)
	if derr != nil {
		return nil, 0, fmt.Errorf("the manifest of payload %q is unsupported: %w", payload, derr)
	}
	return m, smsg.Sequence, nil
}

// checkRetainedAdmissionCoverage is the OTHER direction of the admission rule: a batch that creates
// a retained register, or takes a retained-material edge, must carry the copy's manifest naming
// that exchange as `admitted`. checkRetainedAdmission only runs when the manifest member is present,
// so without this an admission that simply omitted the manifest would escape it — leaving a copy
// that does not know its exchange, which a later close would then not see.
func checkRetainedAdmissionCoverage(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	idHash := getHash(a.ID)
	admittedBy := func(gen, payload, nonce string) bool {
		want := mqttManifestSubject(idHash, gen, payload)
		for _, e := range extra {
			if e.subj != want || e.expect == 0 {
				continue
			}
			var m struct {
				Admitted string `json:"admitted"`
			}
			if json.Unmarshal(e.body, &m) == nil && m.Admitted == nonce {
				return true
			}
		}
		return false
	}
	// THE OTHER DIRECTION OF THE OWNER ADDRESS (F-112): an exchange created naming a retained owner
	// is a retained admission, so the batch must carry that owner's copy naming it admitted.
	ownedAdmission := func(gen, owner, nonce string) bool {
		for _, e := range extra {
			if e.expect == 0 {
				continue
			}
			mgen, _, perr := mqttParseManifestSubject(e.subj, idHash)
			if perr != nil || mgen != gen {
				continue
			}
			var m struct {
				Inc      string `json:"inc"`
				Admitted string `json:"admitted"`
			}
			if json.Unmarshal(e.body, &m) == nil && m.Inc == owner && m.Admitted == nonce {
				return true
			}
		}
		return false
	}
	for _, e := range extra {
		switch {
		case e.expect == 0 && mqttIsExchangeMember(e.subj):
			var x struct {
				Gen           string `json:"gen"`
				Nonce         string `json:"nonce"`
				RetainedOwner string `json:"retained_owner"`
			}
			if json.Unmarshal(e.body, &x) != nil || x.RetainedOwner == _EMPTY_ {
				continue
			}
			if !ownedAdmission(x.Gen, x.RetainedOwner, x.Nonce) {
				return fmt.Errorf("transition %q creates exchange %q naming retained owner %q without that "+
					"owner's copy naming it admitted", a.Txn, x.Nonce, x.RetainedOwner)
			}
		case !mqttIsF105Subject(e.subj) && e.expect == 0:
			name, perr := mqttParseInflightSubject(e.subj)
			if perr != nil {
				continue
			}
			rec, derr := mqttDecodeInflightRecord(e.body)
			if derr != nil || !rec.retainedSource() {
				continue
			}
			if !admittedBy(name.gen, rec.Payload, rec.Lin) {
				return fmt.Errorf("transition %q creates retained register %q without its copy %q naming exchange %q as admitted",
					a.Txn, e.subj, rec.Payload, rec.Lin)
			}
		case mqttResourceIsF105Class(e.subj):
			var r struct {
				Gen  string             `json:"gen"`
				Name string             `json:"name"`
				Kind mqttResourceKind   `json:"kind"`
				Deps []mqttResourceEdge `json:"deps"`
			}
			if json.Unmarshal(e.body, &r) != nil || r.Kind != mqttResourceRetainedPayload {
				continue
			}
			for _, d := range r.Deps {
				if d.Role == mqttRoleRetainedMaterial && !admittedBy(r.Gen, r.Name, d.Nonce) &&
					!mqttRetainedEdgeStood(extra, e, d) {
					return fmt.Errorf("transition %q gives payload %q a retained-material edge for %q without the copy "+
						"naming that exchange as admitted", a.Txn, r.Name, d.Nonce)
				}
			}
		}
	}
	return nil
}

// mqttIsExchangeMember reports whether a member subject is an F-105 exchange record.
func mqttIsExchangeMember(subj string) bool {
	cls, ok := mqttF105ObligationClass(subj)
	return ok && cls == mqttExchangeClassTk
}

// mqttRetainedEdgeStood reports whether an edge in a resource member is one the stored record
// already carried — a later rewrite of the resource (a release of another edge, a seal) is not an
// admission and owes no manifest. Only an ADDED edge does; the boundary's one-delta rule means an
// overwrite adds at most one, and a create-only payload resource starts with none.
func mqttRetainedEdgeStood(extra []mqttTransitionMember, e mqttTransitionMember, d mqttResourceEdge) bool {
	// An overwrite (expect != 0) of a resource whose edge set already contained d is exactly the
	// case; distinguishing it would need the stored record, which checkResourceOverwrites reads.
	// A create-only resource carries no edge at all (opProvisionResource writes `[]`), so any edge
	// on an overwrite that is not this batch's admission must have stood before.
	if e.expect == 0 {
		return false
	}
	for _, x := range extra {
		if cls, ok := mqttF105ObligationClass(x.subj); ok && cls == mqttExchangeClassTk && x.expect == 0 &&
			strings.HasSuffix(x.subj, "."+d.Nonce) {
			return false // this batch creates the exchange: the edge is its admission's
		}
	}
	return true
}
