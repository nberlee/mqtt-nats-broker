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

// Composing a REPLACING admission (F-105, row 1).
//
// mqttAdmitPublishTransition composes the create-only case: a free identifier, a fresh
// obligation, a pair written create-only. This composes the other one — an identifier an
// occupant already holds — and the two modes it must implement are not variations of each
// other, they are different transactions:
//
//   - PAIR replacement, over a PROVEN-UNSENT occupant. Nothing ever went out under it, so the
//     identifier goes to a FRESH obligation: a new exchange is admitted, the occupant's marker
//     is displaced at its observed sequence by the new allocation's, and the predecessor's
//     obligation must already be ended or be ended by this same batch — overwriting a register
//     does not remove its exchange.
//   - REGISTER-ONLY replacement, over a POSSIBLY-ATTEMPTED occupant. The client may already
//     hold it, so the obligation CONTINUES: the register alone is rewritten, inheriting the
//     occupant's lineage AND its QoS, no exchange is admitted, no marker is minted, and no
//     dependency is taken — the obligation already has its edges.
//
// The caller does not choose. mqttClassifyInflightSlot says whether this is a replacement at
// all, mqttDecideMarker says which mode, and this function composes what that decision means.
// Everything it produces is re-derived at the write boundary by checkRegisterReplacements,
// which reads the occupant itself — so a composition bug cannot land, and a boundary that
// trusted the composer would be the single-layer design four review rounds dismantled.

// mqttReplacingAdmission is one replacement, assembled from the classifier's verdict and the
// marker boundary's mode.
type mqttReplacingAdmission struct {
	// Auth is the authority this replacement is made under, and AuthSeq its read sequence.
	Auth    *mqttOutboundAuthority
	AuthSeq uint64

	Txn string
	Op  string

	// Mode is mqttDecideMarker's answer, converted by the caller. It is not a preference.
	Mode mqttReplacementMode

	// RegSubj is the slot, and Occupant/OccupantSeq the exact record being displaced, as the
	// classifier observed them. The register CASes at OccupantSeq and nothing else.
	RegSubj     string
	Occupant    *mqttInflightRecord
	OccupantSeq uint64

	// MarkerSeq is the occupant's marker sequence, required for a PAIR replacement and unused
	// for a register-only one.
	MarkerSeq uint64

	// Register is the displacing allocation. Its lineage and QoS are DERIVED here from the
	// mode and the occupant, never taken from the caller — those two fields are exactly what
	// the two modes disagree about.
	Register *mqttInflightRecord

	// Exchange is the fresh obligation a PAIR replacement admits. Nil for register-only, and
	// an error if supplied there.
	Exchange *mqttOutboundExchange
	// Deps are the fresh obligation's dependencies, for a PAIR replacement only.
	Deps []mqttResourceDependency

	// Predecessor, when non-nil, is the predecessor obligation's ENDED successor, carried in
	// this batch at PredecessorSeq. Nil when it is already ended.
	Predecessor    *mqttOutboundExchange
	PredecessorSeq uint64

	// Ledger is the generation's inventory ledger as it stands, and LedgerSeq the sequence it
	// was read at. Used only by a PAIR replacement, which admits a fresh obligation and so owes
	// it a duty ordinal like any other admission; a register-only replacement CONTINUES its
	// predecessor's obligation, which already has its entry.
	Ledger    *mqttGenLedger
	LedgerSeq uint64
}

// mqttReplacePublishTransition assembles the whole replacement.
func mqttReplacePublishTransition(r *mqttReplacingAdmission) (*mqttAuthorityTransition, error) {
	if r == nil || r.Auth == nil {
		return nil, fmt.Errorf("replacing a register requires the authority it is made under")
	}
	if r.AuthSeq == 0 {
		return nil, fmt.Errorf("replacing a register requires the sequence its authority was read at")
	}
	if r.Auth.State != mqttAuthorityServing {
		return nil, fmt.Errorf("an authority in state %d admits no replacement", r.Auth.State)
	}
	if r.Occupant == nil || r.OccupantSeq == 0 || r.RegSubj == _EMPTY_ {
		return nil, fmt.Errorf("replacing a register requires the exact occupant it displaces")
	}
	if r.Register == nil {
		return nil, fmt.Errorf("replacing a register requires the allocation displacing it")
	}
	// SAME DELIVERY. Every replacement arm is scoped to an occupant naming the same
	// (Dur, Seq); another delivery's occupant is quarantined, reaped or abandoned.
	if r.Occupant.Dur != r.Register.Dur || r.Occupant.Seq != r.Register.Seq {
		return nil, fmt.Errorf("a replacement answers the same delivery, but the occupant names "+
			"%s/%d and the allocation names %s/%d",
			r.Occupant.Dur, r.Occupant.Seq, r.Register.Dur, r.Register.Seq)
	}

	// THE OCCUPANT'S QoS IS PRESERVED IN BOTH MODES. This writer cannot prove the occupant
	// was never transmitted, and its QoS is the only durable trace of what the client may be
	// holding; taking the caller's instead destroys that trace, and for 2 -> 1 it licenses
	// the slot deletion that abandons the exactly-once handshake.
	rec := *r.Register
	rec.QoS = r.Occupant.QoS

	members := make([]mqttTransitionMember, 0, 4+len(r.Deps))
	idHash := getHash(r.Auth.ID)

	switch r.Mode {
	case mqttReplacePair:
		if r.MarkerSeq == 0 {
			return nil, fmt.Errorf("a pair replacement displaces the occupant's marker and so " +
				"requires its observed sequence")
		}
		if r.Exchange == nil {
			return nil, fmt.Errorf("a pair replacement admits a fresh obligation and so requires one")
		}
		// A FRESH OBLIGATION, so the lineage is the allocation's own. The occupant was proven
		// unsent, so there is no client obligation to inherit and no durable fact can have
		// been recorded about one.
		rec.Lin = rec.Nonce
		if r.Exchange.Nonce != rec.Lin {
			return nil, fmt.Errorf("the fresh exchange is %q but the displacing register's "+
				"lineage is %q: the exchange nonce IS the obligation lineage",
				r.Exchange.Nonce, rec.Lin)
		}
		if r.Exchange.QoS != rec.QoS {
			return nil, fmt.Errorf("the fresh exchange is QoS %d but the register it is admitted "+
				"with carries the occupant's QoS %d", r.Exchange.QoS, rec.QoS)
		}

	case mqttReplaceRegisterOnly:
		if r.Exchange != nil || len(r.Deps) > 0 {
			return nil, fmt.Errorf("a register-only replacement CONTINUES the occupant's " +
				"obligation, so it admits no exchange and takes no dependency: the obligation " +
				"already holds its edges, and a second set would duplicate its charge")
		}
		if r.Predecessor != nil {
			return nil, fmt.Errorf("a register-only replacement continues its predecessor and " +
				"so never ends it")
		}
		// INHERITED, and the marker's ABSENCE is what licenses it — absence means POSSIBLY
		// ATTEMPTED, not transmitted, because the send intent deletes the marker BEFORE the
		// bytes leave. The obligation continues and its lineage continues with it.
		rec.Lin = r.Occupant.Lin

	default:
		return nil, fmt.Errorf("a replacement carries no mode, and this mechanism does not guess one")
	}

	if verr := mqttValidateInflightShape(&rec); verr != nil {
		return nil, fmt.Errorf("refusing to compose an unsupported replacing register: %w", verr)
	}
	regBody, err := json.Marshal(&rec)
	if err != nil {
		return nil, err
	}
	// THE REGISTER CASes AT THE OBSERVED OCCUPANT'S SEQUENCE and nothing else. That is what
	// makes the decision sound: if the occupant gained a retirement or a cancellation after
	// the classifier read it, this rejects and the caller re-observes.
	members = append(members, mqttTransitionMember{
		subj: r.RegSubj, expect: r.OccupantSeq, body: regBody,
	})

	if r.Mode == mqttReplacePair {
		markerBody, merr := mqttBuildInflightMarker(rec.Nonce)
		if merr != nil {
			return nil, fmt.Errorf("refusing to compose a replacement whose marker cannot be built: %w", merr)
		}
		// The occupant's marker is displaced AT ITS OBSERVED SEQUENCE, in the same batch, so
		// no interval shows a bare register or a marker naming a record that is gone.
		members = append(members, mqttTransitionMember{
			subj: r.RegSubj + "." + mqttInflightMarkerSuffix, expect: r.MarkerSeq, body: markerBody,
		})
		xchBody, xerr := json.Marshal(r.Exchange)
		if xerr != nil {
			return nil, xerr
		}
		members = append(members, mqttTransitionMember{
			subj:   mqttExchangeSubject(idHash, r.Exchange.Gen, r.Exchange.Nonce),
			expect: 0,
			body:   xchBody,
		})
		for _, d := range r.Deps {
			if d.Resource == nil || d.Seq == 0 {
				return nil, fmt.Errorf("a replacement's dependency needs its record and its read sequence")
			}
			next, derr := mqttResourceTakeDependency(d.Resource, d.edgeFor(r.Exchange.Nonce))
			if derr != nil {
				return nil, fmt.Errorf("replacing a register: %w", derr)
			}
			body, berr := json.Marshal(next)
			if berr != nil {
				return nil, berr
			}
			members = append(members, mqttTransitionMember{
				subj: mqttResourceSubject(idHash, next.Gen, next.Inc), expect: d.Seq, body: body,
			})
		}
		// THE PREDECESSOR'S ENDING, when this batch is the one that ends it. Overwriting a
		// register does not remove its exchange, and a predecessor left standing holds this
		// identifier — checkIdentifierExclusivity refuses exactly that, including for both
		// pending-drain phases, which are NOT ended.
		if r.Predecessor != nil {
			if r.PredecessorSeq == 0 {
				return nil, fmt.Errorf("ending the predecessor requires the sequence it was read at")
			}
			if !mqttExchangePhaseEnded(r.Predecessor.Phase) {
				return nil, fmt.Errorf("the predecessor is carried at phase %d, which is not an "+
					"ending: a pending-drain phase still holds the identifier",
					int(r.Predecessor.Phase))
			}
			pBody, perr := json.Marshal(r.Predecessor)
			if perr != nil {
				return nil, perr
			}
			members = append(members, mqttTransitionMember{
				subj:   mqttExchangeSubject(idHash, r.Predecessor.Gen, r.Predecessor.Nonce),
				expect: r.PredecessorSeq,
				body:   pBody,
			})
		}
		// THE FRESH OBLIGATION'S DUTY ORDINAL. A pair replacement admits a new exchange, so it
		// owes an entry exactly as an ordinary admission does. A REGISTER-ONLY replacement
		// reaches none of this: it continues its predecessor's obligation, which already has
		// its entry, and allocating a second one would make the inventory count allocations
		// rather than obligations.
		inv, ierr := mqttAllocateInventoryMembers(r.Auth.ID, r.Auth.Epoch, r.Auth.Gen,
			r.Ledger, r.LedgerSeq, []mqttDutyDeclaration{
				{Kind: mqttDutyExchange, Ref: r.Exchange.Nonce, State: mqttDutyPending},
			})
		if ierr != nil {
			return nil, fmt.Errorf("replacing a register: %w", ierr)
		}
		members = append(members, inv...)
	}

	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: r.Auth.ID, Epoch: r.Auth.Epoch, Txn: r.Txn, Op: r.Op,
			Kind: mqttTransitionAdmitExchange, PrevTxn: r.Auth.OwnerTxn, Rev: r.Auth.Rev + 1,
			Gen: r.Auth.Gen, Tenure: r.Auth.Tenure, OwnerTxn: r.Auth.OwnerTxn,
		},
		Prev: r.Auth, PrevSeq: r.AuthSeq,
		Auth: &mqttOutboundAuthority{
			ID: r.Auth.ID, Epoch: r.Auth.Epoch, Gen: r.Auth.Gen, Tenure: r.Auth.Tenure,
			State: mqttAuthorityServing, Rev: r.Auth.Rev + 1, Txn: r.Txn, OwnerTxn: r.Auth.OwnerTxn,
		},
		Extra: members,
	}, nil
}

// mqttReplacementModeFromMarker converts the marker boundary's disposition into the mode.
//
// A separate function so the conversion has ONE home: the decision is mqttDecideMarker's, and a
// caller that mapped dispositions itself would be a second table — which is exactly what an
// earlier revision of the write boundary did and had to have deleted.
func mqttReplacementModeFromMarker(d mqttMarkerDisposition) (mqttReplacementMode, bool) {
	switch d {
	case mqttMarkerReplacePair:
		return mqttReplacePair, true
	case mqttMarkerReplaceRegisterOnly:
		return mqttReplaceRegisterOnly, true
	}
	return 0, false
}
