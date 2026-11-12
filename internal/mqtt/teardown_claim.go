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

// The teardown claim batch (issue #133, plan 5.2).
//
// A session that ends with no displacing successor makes its generation noncurrent in ONE batch:
// the authority (closed, the generation added to Outstanding with its marker reference), the
// generation's closing members (the discarding closure, the sealed ledger, the shard and the
// recovery root, the affine enrolment), the outcome, and the marker on the session record, which commits. Claiming the record and discarding the
// generation are one act, so no reader ever sees a marker whose discard has not been decided, or
// a discard whose record is not marked.
//
// The generation keeps its name on the closed authority: "current" is State serving with Gen ==
// G, never Gen alone (consult 10).

// mqttTransitionTeardownClaim makes the serving generation noncurrent and marks its record.
const mqttTransitionTeardownClaim mqttTransitionKind = "teardown-claim"

// mqttTeardownClaimRequest is what one claim composes from: the authority it displaces, the
// retirement it authorizes, and the marker record it writes at the record's sequence.
type mqttTeardownClaimRequest struct {
	ID, Epoch, Txn string
	Prev           *mqttOutboundAuthority
	PrevSeq        uint64
	Retire         mqttRetirementRequest
	// RecSubj and RecSeq are the session record and the sequence the marker CASes against;
	// Marker is the marker record's body, minted once so every attempt re-sends one identity.
	RecSubj   string
	RecSeq    uint64
	Marker    []byte
	Nonce     string
	MarkerWID string
	// Kind is the teardown's kind: a marker (a session ending), or an abandonment of a fresh
	// acceptance that never bound (plan 5.1), which requires that acceptance still pending.
	Kind mqttTeardownKind
}

// mqttClaimedLife is the lifecycle a claim of kind installs over prev: the generation added to
// Outstanding with its marker reference and its release pending, the pending acceptance cleared
// with the anchors it allocated (that connection can never serve, and a crashed CONNECT leaves no
// removal continuation), everything else the predecessor's.
func mqttClaimedLife(prev *mqttOutboundAuthority, kind mqttTeardownKind, nonce, markerWID string) mqttLifecycleState {
	_, ord, _ := mqttParseGenToken(prev.Gen)
	nl := *prev.Life
	nl.Outstanding = append(append([]mqttOutstandingGen(nil), prev.Life.Outstanding...), mqttOutstandingGen{
		Ord: ord, Phase: mqttOutstandingOpen, Acqs: prev.Life.GenAcqs,
		Teardown: mqttOutstandingTeardown{Kind: kind, Nonce: nonce, MarkerWID: markerWID, Pending: true},
	})
	nl.GenAcqs = false
	if p := prev.Life.Pending; p != nil {
		nl.Anchors = nil
		for _, a := range prev.Life.Anchors {
			if a.AcceptOrd != p.AcceptOrd {
				nl.Anchors = append(nl.Anchors, a)
			}
		}
	}
	nl.Pending = nil
	return nl
}

// composeTeardownClaim composes the claim batch over a SERVING authority at the retired
// generation. The closing members are read here, so a recomposition reads them afresh.
func (jsa *mqttJSA) composeTeardownClaim(r mqttTeardownClaimRequest) (*mqttAuthorityTransition, error) {
	prev, gen := r.Prev, r.Retire.Gen
	if prev == nil || prev.Life == nil || prev.State != mqttAuthorityServing || prev.Gen != gen {
		return nil, fmt.Errorf("the teardown of %q discards %q, which the authority does not serve", r.ID, gen)
	}
	if r.Retire.Tenure != prev.Tenure || r.Retire.OwnerTxn != prev.OwnerTxn {
		return nil, fmt.Errorf("the teardown of %q names an ownership the authority does not hold", r.ID)
	}
	if _, _, ok := mqttParseGenToken(gen); !ok {
		return nil, fmt.Errorf("the teardown of %q discards %q, which no lifecycle allocated", r.ID, gen)
	}
	kind := r.Kind
	if kind == 0 {
		kind = mqttTeardownMarker
	}
	if kind == mqttTeardownAbandon && (prev.Life.Pending == nil || prev.Life.Pending.Kind != mqttPendingFresh) {
		return nil, fmt.Errorf("the abandonment of %q finds no fresh pending acceptance", r.ID)
	}
	// A pending acceptance of this generation can never bind once it is noncurrent: clearing it
	// is what its bind resolver reads as Lost (plan 5.1).
	nl := mqttClaimedLife(prev, kind, r.Nonce, r.MarkerWID)
	if err := mqttCheckGenerationSlots(len(nl.Outstanding), false); err != nil {
		return nil, err
	}

	targets := mqttClosingTargets{
		AffineDurable: r.Retire.AffineDurable, AffineGen: r.Retire.AffineGen, PubRelDurable: r.Retire.PubRelDurable,
	}
	cur, curSeq, lerr := jsa.loadGenLedger(r.ID, r.Epoch, gen)
	if lerr != nil {
		return nil, lerr
	}
	hdr, hdrSeq, herr := jsa.loadDirShardAt(mqttDirShardFor(getHash(r.ID)))
	if herr != nil {
		return nil, herr
	}
	enr, cerr := mqttComposeClosingEnrolment(r.ID, r.Epoch, gen, targets, cur, curSeq, hdr, hdrSeq)
	if cerr != nil {
		return nil, cerr
	}
	ret := r.Retire
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: r.ID, Epoch: r.Epoch, Txn: r.Txn, Scope: mqttLifeScope(prev.Life), Op: "op-" + r.Txn,
			Kind: mqttTransitionTeardownClaim, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: r.PrevSeq,
		Auth: &mqttOutboundAuthority{
			ID: r.ID, Epoch: r.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityClosed, Rev: prev.Rev + 1, Txn: r.Txn, OwnerTxn: prev.OwnerTxn,
			Life: &nl,
		},
		Closure: &mqttGenClosure{
			ID: r.ID, Epoch: r.Epoch, Gen: gen, Txn: r.Txn,
			RootShard: enr.RootShard, RootOrd: enr.RootOrd,
		},
		Retire: &ret,
		Extra:  enr.Members,
		Record: &mqttTransitionMember{subj: r.RecSubj, expect: r.RecSeq, body: r.Marker},
	}, nil
}

// mqttCheckTeardownClaimRules is the boundary's rule for the claim: it discards exactly the
// serving generation, under the standing retirement, with its marker in the batch, and the
// authority's only changes are the noncurrent state and that generation's Outstanding entry,
// which is the durable record of the teardown: its kind and the marker it binds.
func mqttCheckTeardownClaimRules(t *mqttAuthorityTransition) error {
	prev, next := t.Prev, t.Auth
	if prev == nil || prev.Life == nil || next == nil || next.Life == nil {
		return fmt.Errorf("a teardown claim has no lifecycle on both sides")
	}
	if prev.State != mqttAuthorityServing || next.State != mqttAuthorityClosed {
		return fmt.Errorf("a teardown claim moves the authority from state %d to %d, not serving to closed",
			prev.State, next.State)
	}
	if next.Gen != prev.Gen || next.Tenure != prev.Tenure || next.OwnerTxn != prev.OwnerTxn {
		return fmt.Errorf("a teardown claim moves ownership")
	}
	if t.Closure == nil || t.Closure.Gen != prev.Gen {
		return fmt.Errorf("a teardown claim writes no closure of the generation it ends")
	}
	ret := t.Retire
	if ret == nil || ret.Gen != prev.Gen || !mqttValidRetirementReason(ret.Reason) ||
		ret.Tenure != prev.Tenure || ret.OwnerTxn != prev.OwnerTxn {
		return fmt.Errorf("a teardown claim of %q carries no retirement of the standing ownership", prev.Gen)
	}
	// THE ENTRY THE CLAIM ADDS names the teardown: it is the last Outstanding entry, of this
	// generation, and the exact-lifecycle comparison below proves it is the only change.
	_, ord, _ := mqttParseGenToken(prev.Gen)
	if len(next.Life.Outstanding) == 0 || next.Life.Outstanding[len(next.Life.Outstanding)-1].Ord != ord {
		return fmt.Errorf("a teardown claim of %q enrols no Outstanding entry of it", prev.Gen)
	}
	b := next.Life.Outstanding[len(next.Life.Outstanding)-1].Teardown
	if (b.Kind != mqttTeardownMarker && b.Kind != mqttTeardownAbandon) ||
		!mqttOutboundIdentTk(b.Nonce) || !mqttOutboundIdentTk(b.MarkerWID) {
		return fmt.Errorf("a teardown claim of %q enrols no marker teardown: %+v", prev.Gen, b)
	}
	if b.Kind == mqttTeardownAbandon && (prev.Life.Pending == nil || prev.Life.Pending.Kind != mqttPendingFresh) {
		return fmt.Errorf("an abandonment of %q finds no fresh pending acceptance", prev.Gen)
	}
	if t.Record == nil {
		return fmt.Errorf("a teardown claim of %q writes no marker", prev.Gen)
	}
	var marker struct {
		Gen      string            `json:"gen"`
		WID      string            `json:"wid"`
		Teardown *mqttSessTeardown `json:"teardown"`
	}
	if err := json.Unmarshal(t.Record.body, &marker); err != nil || marker.Teardown == nil ||
		marker.Gen != prev.Gen || marker.Teardown.Nonce != b.Nonce || marker.WID != b.MarkerWID {
		return fmt.Errorf("a teardown claim of %q writes a record that is not its bound marker", prev.Gen)
	}
	want := mqttClaimedLife(prev, b.Kind, b.Nonce, b.MarkerWID)
	got, _ := json.Marshal(next.Life)
	exp, _ := json.Marshal(&want)
	if string(got) != string(exp) {
		return fmt.Errorf("a teardown claim of %q changes the lifecycle beyond its generation's entry", prev.Gen)
	}
	return nil
}

// mqttTransitionTeardownRelease records that a marked generation's record is released: the
// lifecycle-scoped half of the release (plan 5.2), after the confirmed delete. It changes one
// Outstanding entry's pending flag, and closes the lifecycle when no generation is current.
const mqttTransitionTeardownRelease mqttTransitionKind = "teardown-release"

// mqttComposeTeardownRelease releases generation ord's marker named by nonce over prev.
func mqttComposeTeardownRelease(prev *mqttOutboundAuthority, prevSeq uint64, txn string, ord uint64, nonce string) (*mqttAuthorityTransition, error) {
	if prev == nil || prev.Life == nil {
		return nil, fmt.Errorf("no lifecycle holds generation %d", ord)
	}
	nl := *prev.Life
	nl.Outstanding = append([]mqttOutstandingGen(nil), prev.Life.Outstanding...)
	i := -1
	for k, o := range nl.Outstanding {
		if o.Ord == ord {
			i = k
		}
	}
	if i < 0 || nl.Outstanding[i].Phase != mqttOutstandingOpen || !nl.Outstanding[i].Teardown.Pending ||
		nl.Outstanding[i].Teardown.Nonce != nonce {
		return nil, fmt.Errorf("generation %d carries no pending release of marker %q", ord, nonce)
	}
	nl.Outstanding[i].Teardown.Pending = false
	if prev.State == mqttAuthorityClosed {
		nl.Phase = mqttLifecycleClosed
	}
	next := *prev
	next.Life, next.Rev, next.Txn = &nl, prev.Rev+1, txn
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Scope: mqttLifeScope(prev.Life), Op: "op-" + txn,
			Kind:    mqttTransitionTeardownRelease,
			PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1, Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq, Auth: &next,
	}, nil
}

// mqttCheckTeardownReleaseRules is the boundary's rule for the release: everything but the
// revision, the producing transition, one entry's pending flag and (with no current generation)
// the lifecycle phase is the predecessor's.
func mqttCheckTeardownReleaseRules(t *mqttAuthorityTransition) error {
	prev, next := t.Prev, t.Auth
	if prev == nil || prev.Life == nil || next == nil || next.Life == nil {
		return fmt.Errorf("a teardown release has no lifecycle on both sides")
	}
	if t.Closure != nil || len(t.Extra) != 0 || t.Record != nil || t.Registry != nil {
		return fmt.Errorf("a teardown release carries members")
	}
	if len(next.Life.Outstanding) != len(prev.Life.Outstanding) {
		return fmt.Errorf("a teardown release changes the outstanding generations")
	}
	released := -1
	for i, p := range prev.Life.Outstanding {
		n := next.Life.Outstanding[i]
		if n == p {
			continue
		}
		q := p
		q.Teardown.Pending = false
		if released != -1 || n != q || !p.Teardown.Pending || p.Phase != mqttOutstandingOpen {
			return fmt.Errorf("a teardown release changes generation %d beyond its pending release", p.Ord)
		}
		released = i
	}
	if released == -1 {
		return fmt.Errorf("a teardown release releases nothing")
	}
	want := *prev.Life
	want.Outstanding = next.Life.Outstanding
	if prev.State == mqttAuthorityClosed {
		want.Phase = mqttLifecycleClosed
	}
	wantAuth := *prev
	wantAuth.Life, wantAuth.Rev, wantAuth.Txn = &want, t.Attempt.Rev, t.Attempt.Txn
	got, _ := json.Marshal(next)
	exp, _ := json.Marshal(&wantAuth)
	if string(got) != string(exp) {
		return fmt.Errorf("a teardown release changes more than its generation's release")
	}
	return nil
}

// mqttTeardownReleaseCASAttempts bounds the release CAS. Each attempt follows a read showing the
// release still pending.
const mqttTeardownReleaseCASAttempts = 4

// releaseTeardownEntry records the release of generation gen's marker nonce, idempotently: it
// ends as soon as the entry reads released, displaced, or gone (retired). admit, when set, is
// asked before every attempt (the lifecycle sweep's); the teardown's own release passes nil.
func (as *mqttAccountSessionManager) releaseTeardownEntry(clientID, gen, nonce string, admit func() bool) error {
	_, ord, ok := mqttParseGenToken(gen)
	if !ok {
		return fmt.Errorf("the released generation %q is not a lifecycle's", gen)
	}
	var last error
	for attempt := 0; attempt < mqttTeardownReleaseCASAttempts; attempt++ {
		if admit != nil && !admit() {
			return errMQTTSweepNotAdmitted
		}
		al := as.jsa.loadOutboundAuthority(clientID, as.sessStreamEpoch)
		if al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil {
			last = fmt.Errorf("the authority of %q reads %v", clientID, al.status)
			continue
		}
		pending := false
		for _, o := range al.rec.Life.Outstanding {
			if o.Ord == ord && o.Teardown.Nonce == nonce && o.Teardown.Pending {
				pending = true
			}
		}
		if !pending {
			return nil
		}
		t, cerr := mqttComposeTeardownRelease(al.rec, al.seq, mqttNewInflightNonce(), ord, nonce)
		if cerr != nil {
			return cerr
		}
		if _, err := as.jsa.commitAuthorityTransition(t); err != nil {
			last = err
			continue
		}
		return nil
	}
	return fmt.Errorf("the release of generation %q of %q was not recorded within %d attempts: %v",
		gen, clientID, mqttTeardownReleaseCASAttempts, last)
}

// mqttOutstandingNamesMarker reports whether the lifecycle carries gen as an outstanding
// generation whose teardown is the marker nonce names.
func mqttOutstandingNamesMarker(life *mqttLifecycleState, gen, nonce string) bool {
	l, ord, ok := mqttParseGenToken(gen)
	if !ok || life == nil || life.L != l {
		return false
	}
	for _, o := range life.Outstanding {
		if o.Ord == ord && o.Teardown.Nonce == nonce &&
			(o.Teardown.Kind == mqttTeardownMarker || o.Teardown.Kind == mqttTeardownAbandon) {
			return true
		}
	}
	return false
}
