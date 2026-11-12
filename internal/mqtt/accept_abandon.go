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
	"time"
)

// Abandoning an acceptance that never bound (issue #133, plan 5.1).
//
// An accepted CONNECT whose broker dies, or whose CONNECT fails, between its acceptance and its
// bind leaves the acceptance PENDING. Any broker's sweep may abandon one older than the CONNECT
// deadline plus a margin; the deadline only triggers the attempt. The licence is winning the
// authority CAS against the bind: both name the same pending acceptance with mutually exclusive
// outcomes, so a paused CONNECT that loses can never bind or serve, and one that wins was never
// abandoned. An early abandonment costs a refused CONNECT, never a second owner.
//
// The form follows the acceptance:
//
//   - fresh: its generation was installed by this acceptance and holds nothing acknowledged
//     (no SUBSCRIBE precedes CONNACK). It is torn down: the teardown claim batch with an
//     abandon entry (teardown_claim.go) marks the record and makes the generation noncurrent,
//     and the ordinary marked-record adoption finishes it.
//   - resume: the generation is an established persistent session nothing authorized
//     discarding. accept-abandon clears the pending acceptance and rewrites the session record
//     at its sequence with the prior lifetime, disconnected at the acceptance time, so the
//     session is owned by its expiry exactly as after a CONNECT refused before CONNACK.
//
// Both remove the anchors the abandoned acceptance allocated: that connection never served.

// mqttTransitionAcceptAbandon abandons a pending RESUME acceptance.
const mqttTransitionAcceptAbandon mqttTransitionKind = "accept-abandon"

// mqttRetireAbandoned is the retirement reason of a fresh acceptance's generation torn down by
// its abandonment: discarded, because it never held anything acknowledged.
const mqttRetireAbandoned mqttRetirementReason = "abandoned"

// pendingAcceptanceMargin is what the sweep waits beyond the CONNECT deadline before it
// abandons: the bind's own in-flight requests, which the deadline does not cancel.
func (as *mqttAccountSessionManager) pendingAcceptanceMargin() time.Duration {
	if as.jsa.timeout == 0 {
		return 4 * mqttDefaultJSAPITimeout
	}
	return 4 * as.jsa.timeout
}

// mqttAbandonedLife is the lifecycle an abandonment of the pending acceptance installs over prev:
// no pending acceptance, and none of its anchors.
func mqttAbandonedLife(prev *mqttLifecycleState) mqttLifecycleState {
	nl := *prev
	if p := prev.Pending; p != nil {
		nl.Anchors = nil
		for _, a := range prev.Anchors {
			if a.AcceptOrd != p.AcceptOrd {
				nl.Anchors = append(nl.Anchors, a)
			}
		}
	}
	nl.Pending = nil
	return nl
}

// mqttComposeResumeAbandon abandons the pending resume acceptance of prev, with the session
// record rewritten at recSeq from rec.
func mqttComposeResumeAbandon(prev *mqttOutboundAuthority, prevSeq uint64, txn, recSubj string, recSeq uint64,
	rec *mqttPersistedSession) (*mqttAuthorityTransition, error) {

	if prev == nil || prev.Life == nil || prev.State != mqttAuthorityServing ||
		prev.Life.Pending == nil || prev.Life.Pending.Kind != mqttPendingResume || prev.Life.Pending.Prior == nil {
		return nil, fmt.Errorf("no pending resume acceptance stands to abandon")
	}
	if rec == nil || rec.Gen != prev.Gen || rec.Teardown != nil {
		return nil, fmt.Errorf("the record of %q is not the serving generation's", prev.ID)
	}
	p := prev.Life.Pending
	body := *rec
	body.WID, body.PWID = mqttNewRecordWID(), rec.WID
	body.ExpiryInterval, body.Clean, body.DisconnectedAt = p.Prior.ExpiryInterval, p.Prior.Clean, time.Unix(0, p.At).Unix()
	b, err := json.Marshal(&body)
	if err != nil {
		return nil, err
	}
	nl := mqttAbandonedLife(prev.Life)
	next := *prev
	next.Life, next.Rev, next.Txn = &nl, prev.Rev+1, txn
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Scope: mqttLifeScope(prev.Life), Op: "op-" + txn,
			Kind:    mqttTransitionAcceptAbandon,
			PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1, Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
			Intent: p.Nonce,
		},
		Prev: prev, PrevSeq: prevSeq, Auth: &next,
		Record: &mqttTransitionMember{subj: recSubj, expect: recSeq, body: b},
	}, nil
}

// mqttCheckResumeAbandonRules is the boundary's rule for accept-abandon: the pending resume
// acceptance its attempt names is cleared with its anchors and nothing else changes, and the
// record it writes is the serving generation's, disconnected with the prior lifetime.
func mqttCheckResumeAbandonRules(t *mqttAuthorityTransition) error {
	prev, next, a := t.Prev, t.Auth, t.Attempt
	if prev == nil || prev.Life == nil || next == nil || next.Life == nil || prev.State != mqttAuthorityServing {
		return fmt.Errorf("an abandonment has no serving lifecycle to abandon in")
	}
	p := prev.Life.Pending
	if p == nil || p.Kind != mqttPendingResume || p.Prior == nil || p.Nonce != a.Intent {
		return fmt.Errorf("an abandonment names acceptance %q, which is not the pending resume", a.Intent)
	}
	if t.Closure != nil || len(t.Extra) != 0 || t.Registry != nil || t.Record == nil {
		return fmt.Errorf("an abandonment carries members beyond its session record")
	}
	nl := mqttAbandonedLife(prev.Life)
	want := *prev
	want.Life, want.Rev, want.Txn = &nl, a.Rev, a.Txn
	got, _ := json.Marshal(next)
	exp, _ := json.Marshal(&want)
	if string(got) != string(exp) {
		return fmt.Errorf("an abandonment changes more than its pending acceptance")
	}
	var rec mqttPersistedSession
	if err := json.Unmarshal(t.Record.body, &rec); err != nil || rec.Gen != prev.Gen || rec.Teardown != nil ||
		rec.ExpiryInterval != p.Prior.ExpiryInterval || rec.Clean != p.Prior.Clean ||
		rec.DisconnectedAt != time.Unix(0, p.At).Unix() {
		return fmt.Errorf("an abandonment writes a record that is not the session disconnected with its prior lifetime")
	}
	return nil
}

// abandonPendingAcceptance abandons the pending acceptance of the authority al if it is still
// the one read and older than threshold, in the form its kind calls for. A lost CAS means the
// bind or another abandonment won, and nothing more is owed.
//
// admit is asked immediately before the one authority attempt: an abandonment landing while the
// pending acceptance's own CONNECT is binding (announced foreground work) would abandon a
// connection about to serve, so the sweep yields to it.
func (as *mqttAccountSessionManager) abandonPendingAcceptance(clientID string, al mqttAuthorityLoad, now time.Time, threshold time.Duration, admit func() bool) error {
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil || al.rec.Life.Pending == nil ||
		al.rec.State != mqttAuthorityServing {
		return nil
	}
	p := al.rec.Life.Pending
	if now.Sub(time.Unix(0, p.At)) < threshold {
		return nil
	}
	recSubj := mqttSessSubject(as.domainTk, getHash(clientID))
	smsg, err := as.jsa.loadLastMsgFor(mqttSessStreamName, recSubj)
	if err != nil {
		return fmt.Errorf("reading the record of %q to abandon its acceptance: %w", clientID, err)
	}
	rec := &mqttPersistedSession{}
	if uerr := json.Unmarshal(smsg.Data, rec); uerr != nil || rec.Gen != al.rec.Gen || rec.Teardown != nil {
		return fmt.Errorf("the record of %q is not its serving generation's, so its acceptance is not abandoned", clientID)
	}
	var t *mqttAuthorityTransition
	switch p.Kind {
	case mqttPendingResume:
		t, err = mqttComposeResumeAbandon(al.rec, al.seq, mqttNewInflightNonce(), recSubj, smsg.Sequence, rec)
	case mqttPendingFresh:
		nonce := mqttNewInflightNonce()
		marked := *rec
		marked.WID, marked.PWID = mqttNewRecordWID(), rec.WID
		marked.Teardown = &mqttSessTeardown{Origin: as.jsa.id, Nonce: nonce, StartedAt: now.Unix()}
		body, merr := json.Marshal(&marked)
		if merr != nil {
			return merr
		}
		ret := mqttRetirementRequest{
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
			SessWID: rec.WID, SessSeq: smsg.Sequence, Reason: mqttRetireAbandoned,
		}
		var affDur, pubRelDur string
		if rec.Q2Aff != nil {
			affDur = rec.Q2Aff.Durable
		}
		if rec.PubRel != nil {
			pubRelDur = rec.PubRel.Durable
		}
		ret.AffineDurable, ret.AffineGen, ret.PubRelDurable = mqttAffineRetireDescriptor(affDur, rec.Q2AffGen, pubRelDur)
		t, err = as.jsa.composeTeardownClaim(mqttTeardownClaimRequest{
			ID: clientID, Epoch: as.sessStreamEpoch, Txn: mqttNewInflightNonce(), Prev: al.rec, PrevSeq: al.seq,
			Retire: ret, RecSubj: recSubj, RecSeq: smsg.Sequence, Marker: body, Nonce: nonce,
			MarkerWID: marked.WID, Kind: mqttTeardownAbandon,
		})
	default:
		return fmt.Errorf("the pending acceptance of %q carries unsupported kind %d", clientID, p.Kind)
	}
	if err != nil {
		return fmt.Errorf("composing the abandonment of %q: %w", clientID, err)
	}
	if admit != nil && !admit() {
		return fmt.Errorf("abandoning the pending acceptance of %q: %w", clientID, errMQTTSweepNotAdmitted)
	}
	if _, cerr := as.jsa.commitAuthorityTransition(t); cerr != nil {
		return fmt.Errorf("abandoning the pending acceptance of %q: %w", clientID, cerr)
	}
	if as.jsa.c != nil {
		as.jsa.c.Warnf("MQTT: abandoned the acceptance of %q that never bound (%v pending)", clientID, now.Sub(time.Unix(0, p.At)))
	}
	return nil
}
