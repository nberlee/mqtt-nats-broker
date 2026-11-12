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
	"errors"
	"fmt"
)

// The clean start's displaced-record proof (issue #107 8b): which targets the predecessor
// generation's retirement names, read from the record the acceptance batch displaces. The
// acceptance itself is one batch (accept_shapes.go, accept_drive.go, issue #133).

// mqttCleanStartRetireTargets carries a clean start's predecessor backing-resource names —
// captured from the resident session BEFORE it is reset — so the discarding handoff's retire
// request can enrol them for recovery in the closing batch. Empty where the predecessor holds
// none; the shared-group durables are deliberately absent, since a generation's shared
// acquisitions are released from its own ledger by the closing, never named by a retirement.
type mqttCleanStartRetireTargets struct {
	AffineDurable string
	AffineGen     string
	PubRelDurable string
}

// errMQTTDisplacedRecordUnproven refuses a clean start, before anything is detached, whose
// displaced record could not be proven: unreadable, or undecodable or naming another identity at
// the very sequence the object holds. Falling through to the discard there would let its claim
// WIN at that sequence and retire what the object's memory names.
var errMQTTDisplacedRecordUnproven = errors.New("the session record a clean start would displace could not be proven")

// displacedRetireTargets is the clean start's ONE retirement disposition (issue #107 8b, P2;
// consult 123), run as the retirement's pre-detach proof from the discard boundary's CARRIED
// load. A clean start that hands off from a SERVING predecessor must name the targets of the
// record its accepted batch DISPLACES, never the object's memory; and a clean start must never
// take the discard arm where its memory-built marker could claim a generation that may still
// serve. So, defaulting to REFUSE:
//
//	no record (seq 0)                                     -> the discard arm (local resources)
//	load not read, indeterminate, corrupt, malformed      -> refuse
//	ABSENT, CLOSED at any generation, SERVING at another  -> the discard arm (no claim can be
//	                                                         composed for this generation)
//	SERVING at this object's generation                   -> prove the record:
//	  no write identity                                   -> refuse
//	  unreadable; undecodable or another identity AT seq  -> refuse
//	  gone, or moved past seq                             -> the discard arm (its claim is
//	                                                         superseded; the re-drive runs)
//	  this object's record at seq                         -> hand off, naming ITS targets
//
// ok reports a handoff; an error refuses the CONNECT with the incumbent intact (the proof's
// unwind). The batch's CAS at the proven sequence is what makes the record read the displaced
// one. Other discards — disconnect, expiry, gone-retirement — mark from the owning
// incarnation's own state, which is theirs to snapshot; this disposition is the CONNECT's.
func (sess *mqttSession) displacedRetireTargets(p *mqttPreparedClear) (mqttCleanStartRetireTargets, bool, error) {
	sess.mu.Lock()
	wid, seq, gen, domainTk, idHash := sess.wid, sess.seq, sess.gen, sess.domainTk, sess.idHash
	sess.mu.Unlock()
	if seq == 0 {
		return mqttCleanStartRetireTargets{}, false, nil
	}
	al := p.auth
	switch {
	case !p.authRead:
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: the discard boundary read no authority", errMQTTDisplacedRecordUnproven)
	case al.status == mqttOutboundAbsent:
		return mqttCleanStartRetireTargets{}, false, nil
	case al.status != mqttOutboundValid || al.rec == nil:
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: the authority reads %v", errMQTTDisplacedRecordUnproven, al.status)
	}
	switch al.rec.State {
	case mqttAuthorityClosed:
		return mqttCleanStartRetireTargets{}, false, nil
	case mqttAuthorityServing:
		if al.rec.Gen != gen {
			return mqttCleanStartRetireTargets{}, false, nil
		}
	default:
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: the authority is in state %d", errMQTTDisplacedRecordUnproven, al.rec.State)
	}
	if wid == _EMPTY_ {
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: the record at %d carries no write identity", errMQTTDisplacedRecordUnproven, seq)
	}
	smsg, lerr := sess.jsa.loadSessionMsg(domainTk, idHash)
	switch {
	case lerr != nil && isErrorOtherThan(lerr, JSNoMessageFoundErr):
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: %v", errMQTTDisplacedRecordUnproven, lerr)
	case lerr != nil || smsg == nil || smsg.Sequence != seq:
		return mqttCleanStartRetireTargets{}, false, nil
	}
	ps := &mqttPersistedSession{}
	if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: %v", errMQTTDisplacedRecordUnproven, uerr)
	}
	if ps.WID != wid || ps.Gen != gen || ps.Teardown != nil {
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: at sequence %d it names write identity %q generation %q, not %q %q",
			errMQTTDisplacedRecordUnproven, seq, ps.WID, ps.Gen, wid, gen)
	}
	t := mqttTeardownTargetFromRecord(ps, idHash)
	return mqttCleanStartRetireTargets{AffineDurable: t.q2AffDur, AffineGen: t.q2AffGen, PubRelDurable: t.pubRelDur}, true, nil
}

// mqttIsMetadataContention reports whether an ownership metadata-commit error is a same-subject
// CAS contention (JetStream 10164). Such a rejection decided NOTHING: the three-member metadata
// batch is atomic, so a rejected send stored no member, and the SAME frozen attempt may be
// re-sent (Codex consult 5 step 4, consult 10). Any other error settles the attempt.
//
// The store error travels %w-wrapped out of persistRecord ("unable to persist session %q
// ownership metadata ... %w") and raw out of a retry's commitMetadataAttempt, so it is
// classified from the UNWRAPPED ApiError — the same wrap-aware discipline the accept loop's CAS
// arms use, because IsNatsErr's bare type assertion cannot see a code through a %w wrap.
func mqttIsMetadataContention(err error) bool {
	ce := jsAPIError(err)
	return ce != nil && IsNatsErr(ce, JSStreamWrongLastSequenceConstantErr)
}
