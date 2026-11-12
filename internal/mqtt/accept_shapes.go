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
	"strings"
)

// The acceptance shapes (issue #133, plan 5.1).
//
// An accepted CONNECT changes the client's ownership and its session record in ONE atomic
// batch on the metadata stream, so there is no proposal standing between two writes: no
// reservation to finalize or abandon, no handoff receipt recording which half won. Each shape
// is a distinct transition kind with its own rule, because each displaces a different
// predecessor:
//
//   - accept-create: no authority. The registry shard allocates the lifecycle in the same
//     batch, so two creators of one client cannot both land.
//   - accept-handoff: a serving authority with the record present. A resume keeps the
//     generation and installs a new tenure; a Clean Start installs the lifecycle's next
//     generation and makes the predecessor outstanding, closing it in the same batch.
//   - accept-reopen: the lifecycle's generation was closed and its record released; the same
//     client reconnects before the lifecycle retires.
//
// Every shape installs the acceptance as PENDING: it is bound to a live connection only by
// the bind transition, before CONNACK, so a CONNECT that never binds leaves an owner for its
// acceptance rather than a serving authority nobody holds.

const (
	mqttTransitionAcceptCreate  mqttTransitionKind = "accept-create"
	mqttTransitionAcceptHandoff mqttTransitionKind = "accept-handoff"
	mqttTransitionAcceptReopen  mqttTransitionKind = "accept-reopen"
	// mqttTransitionAcceptDisplace accepts over a MARKED record whose generation's release has not
	// landed (plan 5.1 shape 4): it CASes the record at the marker's sequence and discharges that
	// release in the same batch, since the subject will never again hold the marker.
	mqttTransitionAcceptDisplace mqttTransitionKind = "accept-displace"
	// mqttTransitionBind binds the pending acceptance to the connection that made it, with the
	// grant point's session save. It moves no ownership.
	mqttTransitionBind mqttTransitionKind = "bind"
)

// mqttIsAcceptShape reports the kinds that accept a CONNECT.
func mqttIsAcceptShape(k mqttTransitionKind) bool {
	return k == mqttTransitionAcceptCreate || k == mqttTransitionAcceptHandoff || k == mqttTransitionAcceptReopen ||
		k == mqttTransitionAcceptDisplace
}

// mqttCheckAcceptanceSlots confines each acceptance slot to the kinds that write it, and each
// slot's subject to the one record it may name. Every other kind carries none.
func mqttCheckAcceptanceSlots(t *mqttAuthorityTransition) error {
	a := t.Attempt
	k := a.Kind
	idHash := getHash(a.ID)
	if t.Registry != nil {
		if k != mqttTransitionAcceptCreate && k != mqttTransitionRetireLifecycle {
			return fmt.Errorf("a %s transition carries a lifecycle registry member", k)
		}
		if t.Registry.subj != mqttLifecycleRegistrySubject(mqttLifecycleRegistryShardFor(idHash)) {
			return fmt.Errorf("a creation of %q allocates from %q, which is not its registry shard", a.ID, t.Registry.subj)
		}
	} else if k == mqttTransitionAcceptCreate {
		return fmt.Errorf("a creation of %q carries no lifecycle registry member", a.ID)
	}
	if t.Record != nil {
		if !mqttIsAcceptShape(k) && k != mqttTransitionBind && k != mqttTransitionTeardownClaim &&
			k != mqttTransitionAcceptAbandon {
			return fmt.Errorf("a %s transition carries a session record", k)
		}
		if !strings.HasPrefix(t.Record.subj, mqttSessStreamSubjectPrefix) || !strings.HasSuffix(t.Record.subj, idHash) {
			return fmt.Errorf("a %s transition of %q writes %q, which is not its session record", k, a.ID, t.Record.subj)
		}
	} else if mqttIsAcceptShape(k) {
		return fmt.Errorf("a %s transition of %q carries no session record", k, a.ID)
	}
	return nil
}

// mqttCheckAcceptShapeRules is the boundary's rule for each acceptance kind and for bind.
func mqttCheckAcceptShapeRules(t *mqttAuthorityTransition) error {
	a, next, prev := t.Attempt, t.Auth, t.Prev
	if next == nil || next.Life == nil {
		return fmt.Errorf("a %s transition installs no lifecycle section", a.Kind)
	}
	nl := next.Life
	if a.Kind != mqttTransitionBind {
		// EVERY ACCEPTANCE IS PENDING and is the lifecycle's next one, installed serving under a
		// new owning transition.
		if next.State != mqttAuthorityServing || next.OwnerTxn != a.Txn {
			return fmt.Errorf("a %s transition installs state %d under owner %q", a.Kind, next.State, next.OwnerTxn)
		}
		if nl.Phase != mqttLifecycleServing || nl.Pending == nil {
			return fmt.Errorf("a %s transition installs no pending acceptance", a.Kind)
		}
		if nl.Accept.Nonce != a.Intent {
			return fmt.Errorf("a %s transition installs acceptance %q but its attempt names %q", a.Kind, nl.Accept.Nonce, a.Intent)
		}
	}
	if mqttIsAcceptShape(a.Kind) {
		var pl *mqttLifecycleState
		if prev != nil {
			pl = prev.Life
		}
		if err := mqttCheckAcceptAnchors(pl, nl); err != nil {
			return err
		}
	}
	switch a.Kind {
	case mqttTransitionAcceptCreate:
		if prev != nil || a.Rev != 1 {
			return fmt.Errorf("a creation of %q displaces a predecessor", a.ID)
		}
		want, err := mqttGenToken(nl.L, 0)
		if err != nil || next.Gen != want || nl.NextGenOrd != 1 || nl.Accept.Ord != 0 || nl.NextAcceptOrd != 1 ||
			len(nl.Outstanding) != 0 || nl.Pending.Kind != mqttPendingFresh {
			return fmt.Errorf("a creation of %q does not install its lifecycle's first generation and acceptance", a.ID)
		}
		var reg mqttLifecycleRegistryShard
		if err := json.Unmarshal(t.Registry.body, &reg); err != nil {
			return fmt.Errorf("a creation's registry member is not a registry shard: %w", err)
		}
		if len(reg.Active)+len(reg.Retiring) > mqttLifecycleRegistryShardMaxEntries {
			return fmt.Errorf("a creation of %q takes its registry shard past its %d entries", a.ID, mqttLifecycleRegistryShardMaxEntries)
		}
		if reg.Active[getHash(a.ID)] != nl.L || reg.Next != nl.L+1 {
			return fmt.Errorf("a creation of %q installs lifecycle %d but its registry member allocates %d (next %d)",
				a.ID, nl.L, reg.Active[getHash(a.ID)], reg.Next)
		}
		if t.Closure != nil || len(t.Extra) != 0 {
			return fmt.Errorf("a creation of %q closes something", a.ID)
		}

	case mqttTransitionAcceptHandoff, mqttTransitionAcceptReopen, mqttTransitionAcceptDisplace:
		if prev == nil || prev.Life == nil {
			return fmt.Errorf("a %s transition of %q has no lifecycle to accept into", a.Kind, a.ID)
		}
		pl := prev.Life
		if pl.Phase == mqttLifecycleRetiring {
			return fmt.Errorf("a %s transition of %q accepts into a retiring lifecycle", a.Kind, a.ID)
		}
		if nl.L != pl.L || nl.Accept.Ord != pl.NextAcceptOrd || nl.NextAcceptOrd != pl.NextAcceptOrd+1 {
			return fmt.Errorf("a %s transition of %q is not its lifecycle's next acceptance", a.Kind, a.ID)
		}
		if a.Kind == mqttTransitionAcceptReopen {
			if prev.State != mqttAuthorityClosed || pl.Phase == mqttLifecycleRetiring {
				return fmt.Errorf("a reopen of %q over an authority in state %d, phase %d", a.ID, prev.State, pl.Phase)
			}
			if err := mqttCheckInstallsNextGeneration(next, pl); err != nil {
				return err
			}
			// The release deleted the record, so the reopen CREATES it; the outstanding set is the
			// predecessor's.
			if nl.Pending.Kind != mqttPendingFresh || t.Closure != nil || len(t.Extra) != 0 ||
				t.Record.expect != 0 || !mqttSameOutstanding(nl.Outstanding, mqttReleaseAllPending(pl.Outstanding)) {
				return fmt.Errorf("a reopen of %q is not a fresh acceptance creating its record", a.ID)
			}
			break
		}
		if a.Kind == mqttTransitionAcceptDisplace {
			// Over a MARKED record whose release has not landed: the lifecycle never closed.
			if prev.State != mqttAuthorityClosed || pl.Phase != mqttLifecycleServing {
				return fmt.Errorf("a displacement of %q over an authority in state %d, phase %d", a.ID, prev.State, pl.Phase)
			}
			if err := mqttCheckInstallsNextGeneration(next, pl); err != nil {
				return err
			}
			if nl.Pending.Kind != mqttPendingFresh || t.Closure != nil || len(t.Extra) != 0 ||
				t.Record.expect == 0 {
				return fmt.Errorf("a displacement of %q is not a fresh acceptance over its marker", a.ID)
			}
			if err := mqttCheckDisplacedRelease(pl.Outstanding, nl.Outstanding); err != nil {
				return fmt.Errorf("a displacement of %q: %w", a.ID, err)
			}
			break
		}
		// A PENDING PREDECESSOR IS SUPERSEDED, not waited for: its CONNECT's bind can no longer
		// win, so it refuses before CONNACK, and this acceptance replaces it (plan 5.3).
		if prev.State != mqttAuthorityServing {
			return fmt.Errorf("a handoff of %q over an authority in state %d", a.ID, prev.State)
		}
		if next.Gen == prev.Gen {
			// A RESUME: the generation, its outstanding set and its shared acquisitions carry over.
			if nl.Pending.Kind != mqttPendingResume || nl.NextGenOrd != pl.NextGenOrd || t.Closure != nil ||
				len(nl.Outstanding) != len(pl.Outstanding) || nl.GenAcqs != pl.GenAcqs {
				return fmt.Errorf("a resume of %q changes more than its tenure and acceptance", a.ID)
			}
			break
		}
		// A CLEAN START: the predecessor becomes outstanding, discarded, in the same batch.
		if err := mqttCheckInstallsNextGeneration(next, pl); err != nil {
			return err
		}
		_, pord, _ := mqttParseGenToken(prev.Gen)
		if nl.Pending.Kind != mqttPendingFresh || nl.GenAcqs || len(nl.Outstanding) != len(pl.Outstanding)+1 {
			return fmt.Errorf("a clean start of %q does not enrol its predecessor as outstanding", a.ID)
		}
		o := nl.Outstanding[len(nl.Outstanding)-1]
		if o.Ord != pord || o.Phase != mqttOutstandingOpen || o.Acqs != pl.GenAcqs || o.Teardown.Kind != mqttTeardownHandoffDiscard {
			return fmt.Errorf("a clean start of %q enrols its predecessor as %+v", a.ID, o)
		}
		if t.Closure == nil || t.Closure.Gen != prev.Gen {
			return fmt.Errorf("a clean start of %q does not close its predecessor %q", a.ID, prev.Gen)
		}
		if ret := t.Retire; ret == nil || ret.Gen != prev.Gen || ret.Tenure != prev.Tenure ||
			ret.OwnerTxn != prev.OwnerTxn || ret.Reason != mqttRetireCleanStart ||
			mqttValidateRetireTargets(getHash(a.ID), prev.Gen, mqttClosingTargets{AffineDurable: ret.AffineDurable,
				AffineGen: ret.AffineGen, PubRelDurable: ret.PubRelDurable}) != nil {
			return fmt.Errorf("a clean start of %q carries no retirement of its predecessor's ownership", a.ID)
		}

	case mqttTransitionBind:
		if prev == nil || prev.Life == nil || prev.Life.Pending == nil || prev.State != mqttAuthorityServing {
			return fmt.Errorf("a bind of %q finds no pending acceptance", a.ID)
		}
		if prev.Life.Pending.Nonce != a.Intent {
			return fmt.Errorf("a bind of %q names acceptance %q, but %q is pending", a.ID, a.Intent, prev.Life.Pending.Nonce)
		}
		// THE BIND CLEARS THE PENDING ACCEPTANCE AND NOTHING ELSE.
		want := *prev
		wl := *prev.Life
		wl.Pending = nil
		want.Life, want.Rev, want.Txn = &wl, a.Rev, a.Txn
		got, _ := json.Marshal(next)
		exp, _ := json.Marshal(&want)
		if string(got) != string(exp) {
			return fmt.Errorf("a bind of %q changes more than its pending acceptance", a.ID)
		}
		if t.Closure != nil || len(t.Extra) != 0 {
			return fmt.Errorf("a bind of %q closes something", a.ID)
		}
	}
	return nil
}

// mqttCheckInstallsNextGeneration is the arithmetic freshness of an installed generation: it
// is exactly the lifecycle's next one, and the allocator moves past it.
func mqttCheckInstallsNextGeneration(next *mqttOutboundAuthority, pl *mqttLifecycleState) error {
	want, err := mqttGenToken(pl.L, pl.NextGenOrd)
	if err != nil {
		return err
	}
	if next.Gen != want || next.Life.NextGenOrd != pl.NextGenOrd+1 {
		return fmt.Errorf("the installed generation %q is not lifecycle %d's next one %q", next.Gen, pl.L, want)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Composing an acceptance
// ---------------------------------------------------------------------------

// mqttAcceptRequest is what a CONNECT brings to its acceptance: who it is, what it asks for,
// and the fresh identities it minted.
type mqttAcceptRequest struct {
	ID, Epoch    string
	Txn          string
	Tenure       string
	Nonce        string
	At           int64
	CleanStart   bool
	PriorExpiry  uint32
	PriorClean   bool
	Retire       mqttClosingTargets
	RecordExists bool
	// DisplacedMarker is the nonce of the teardown marker the CONNECT read on the record it is
	// about to write over, or empty: a displacement names the release it discharges.
	DisplacedMarker string
	// WillDelayCapable: the connection's Will may be delayed (MQTT 5.0, Will Delay > 0, session
	// expiry > 0), so its acceptance allocates the Will's anchor.
	WillDelayCapable bool
}

// mqttComposedAcceptance is an acceptance ready for its record member: the transition minus
// the record, and the generation the record must carry.
type mqttComposedAcceptance struct {
	T   *mqttAuthorityTransition
	Gen string
	// Anchor is the Will anchor this acceptance allocated, or nil.
	Anchor *mqttWillAnchor
	L      uint64
}

// composeAcceptance decides the shape from what stands and composes it. reg is read only for
// a creation. The record member is supplied later, by the store boundary that marshals it.
func (jsa *mqttJSA) composeAcceptance(r mqttAcceptRequest, al mqttAuthorityLoad, reg mqttRegistryLoad) (*mqttComposedAcceptance, error) {
	accept := mqttAcceptance{Tenure: r.Tenure, Nonce: r.Nonce, At: r.At}
	switch {
	case al.status == mqttOutboundAbsent:
		if reg.status != mqttOutboundValid && reg.status != mqttOutboundAbsent {
			return nil, fmt.Errorf("the lifecycle registry of %q reads %v: %v", r.ID, reg.status, reg.err)
		}
		h := getHash(r.ID)
		if _, ok := reg.rec.Active[h]; ok {
			// The registry lists a lifecycle whose authority is absent: a wiped stream or the
			// upstream revival defect, never a state this broker writes.
			return nil, fmt.Errorf("the registry lists an active lifecycle of %q but its authority is absent", r.ID)
		}
		if _, ok := reg.rec.Retiring[h]; ok {
			return nil, fmt.Errorf("a lifecycle of %q is still retiring", r.ID)
		}
		if !mqttRegistryShardHasRoom(reg.rec) {
			return nil, fmt.Errorf("creating %q: %w", r.ID, errMQTTRegistryShardFull)
		}
		nreg := *reg.rec
		nreg.Active = make(map[string]uint64, len(reg.rec.Active)+1)
		for k, v := range reg.rec.Active {
			nreg.Active[k] = v
		}
		l := reg.rec.Next
		nreg.Active[h], nreg.Next = l, l+1
		gen, err := mqttGenToken(l, 0)
		if err != nil {
			return nil, err
		}
		regBody, _ := json.Marshal(&nreg)
		accept.Ord = 0
		anchors, nextAnchor, own, aerr := mqttAcceptAnchors(nil, 0, r.At, r.WillDelayCapable)
		if aerr != nil {
			return nil, aerr
		}
		auth := &mqttOutboundAuthority{
			ID: r.ID, Epoch: r.Epoch, Gen: gen, Tenure: r.Tenure, State: mqttAuthorityServing,
			Rev: 1, Txn: r.Txn, OwnerTxn: r.Txn,
			Life: &mqttLifecycleState{
				L: l, Phase: mqttLifecycleServing, NextGenOrd: 1, Accept: accept, NextAcceptOrd: 1,
				Pending:       &mqttPendingAcceptance{Kind: mqttPendingFresh, AcceptOrd: 0, Nonce: r.Nonce, At: r.At},
				Anchors:       anchors,
				NextAnchorOrd: nextAnchor,
			},
		}
		return &mqttComposedAcceptance{Gen: gen, Anchor: own, L: l, T: &mqttAuthorityTransition{
			Attempt: &mqttTransitionAttempt{
				ID: r.ID, Epoch: r.Epoch, Txn: r.Txn, Scope: mqttLifeScope(auth.Life), Op: "op-" + r.Txn,
				Kind: mqttTransitionAcceptCreate, Rev: 1, Gen: gen, Tenure: r.Tenure, OwnerTxn: r.Txn, Intent: r.Nonce,
			},
			Auth:     auth,
			Registry: &mqttTransitionMember{subj: mqttLifecycleRegistrySubject(reg.rec.Shard), expect: reg.seq, body: regBody},
		}}, nil

	case al.status != mqttOutboundValid || al.rec == nil:
		return nil, fmt.Errorf("the authority of %q reads %v", r.ID, al.status)
	}

	prev := al.rec
	if prev.Life == nil {
		return nil, fmt.Errorf("the authority of %q carries no lifecycle section", r.ID)
	}
	pl := prev.Life
	if pl.Phase == mqttLifecycleRetiring {
		return nil, fmt.Errorf("the lifecycle of %q is retiring", r.ID)
	}
	nl := *pl
	nl.Outstanding = append([]mqttOutstandingGen(nil), pl.Outstanding...)
	nl.Anchors = append([]mqttWillAnchor(nil), pl.Anchors...)
	accept.Ord = pl.NextAcceptOrd
	nl.Accept, nl.NextAcceptOrd = accept, pl.NextAcceptOrd+1
	anchors, nextAnchor, own, aerr := mqttAcceptAnchors(pl, accept.Ord, r.At, r.WillDelayCapable)
	if aerr != nil {
		return nil, aerr
	}
	nl.Anchors, nl.NextAnchorOrd = anchors, nextAnchor
	t := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: r.ID, Epoch: r.Epoch, Txn: r.Txn, Scope: mqttLifeScope(pl), Op: "op-" + r.Txn,
			PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1, Tenure: r.Tenure, OwnerTxn: r.Txn, Intent: r.Nonce,
		},
		Prev: prev, PrevSeq: al.seq,
	}
	next := &mqttOutboundAuthority{
		ID: r.ID, Epoch: r.Epoch, Tenure: r.Tenure, State: mqttAuthorityServing,
		Rev: prev.Rev + 1, Txn: r.Txn, OwnerTxn: r.Txn, Life: &nl,
	}
	installNext := func() error {
		gen, err := mqttGenToken(pl.L, pl.NextGenOrd)
		if err != nil {
			return err
		}
		next.Gen, nl.NextGenOrd = gen, pl.NextGenOrd+1
		return nil
	}
	switch {
	case prev.State == mqttAuthorityClosed && r.DisplacedMarker != _EMPTY_:
		// A DISPLACEMENT: the record still holds a teardown marker whose release has not
		// landed. The successor writes over the marker, which discharges that release.
		// A marker whose release landed since the CONNECT read it is gone from the record: the
		// record moved, and the CONNECT reads it again.
		if pl.Phase != mqttLifecycleServing {
			return nil, fmt.Errorf("the marker on the record of %q was released (lifecycle phase %d): %w",
				r.ID, pl.Phase, errMQTTAcceptRecordMoved)
		}
		out, err := mqttDisplaceRelease(pl.Outstanding, r.DisplacedMarker)
		if err != nil {
			return nil, fmt.Errorf("the marker on the record of %q cannot be displaced (%v): %w", r.ID, err, errMQTTAcceptRecordMoved)
		}
		if err := installNext(); err != nil {
			return nil, err
		}
		t.Attempt.Kind = mqttTransitionAcceptDisplace
		nl.Outstanding = out
		nl.Pending = &mqttPendingAcceptance{Kind: mqttPendingFresh, AcceptOrd: accept.Ord, Nonce: r.Nonce, At: r.At}
		// The new current generation takes a slot beside every outstanding one: refused here,
		// with its reason, rather than at the write boundary.
		if err := mqttCheckGenerationSlots(len(nl.Outstanding), true); err != nil {
			return nil, err
		}

	case prev.State == mqttAuthorityClosed:
		// A REOPEN: the record is gone and the lifecycle has not retired. The record is created
		// afresh, create-only, which is what proves its absence in this very batch: so a
		// release whose delete landed and whose lifecycle-scoped half has not yet is discharged
		// here too, rather than refusing the client until a sweep finishes it. If a marker still
		// stands, the create fails and the CONNECT re-reads and displaces it.
		if r.RecordExists {
			return nil, fmt.Errorf("the generation of %q is closed but its record stands", r.ID)
		}
		if err := installNext(); err != nil {
			return nil, err
		}
		t.Attempt.Kind = mqttTransitionAcceptReopen
		nl.Outstanding = mqttReleaseAllPending(pl.Outstanding)
		nl.Phase = mqttLifecycleServing
		nl.Pending = &mqttPendingAcceptance{Kind: mqttPendingFresh, AcceptOrd: accept.Ord, Nonce: r.Nonce, At: r.At}
		if err := mqttCheckGenerationSlots(len(nl.Outstanding), true); err != nil {
			return nil, err
		}

	case prev.State == mqttAuthorityServing && !r.RecordExists:
		// Every serving generation's record was written by the acceptance that installed it, and
		// only its own teardown, which ends the generation first, removes it: no writer leaves a
		// serving authority with no record. Inconsistent; the CONNECT is refused.
		return nil, fmt.Errorf("the authority of %q serves %q, but its record is absent", r.ID, prev.Gen)

	case prev.State == mqttAuthorityServing && !r.CleanStart:
		t.Attempt.Kind = mqttTransitionAcceptHandoff
		next.Gen = prev.Gen
		nl.Pending = &mqttPendingAcceptance{Kind: mqttPendingResume, AcceptOrd: accept.Ord, Nonce: r.Nonce, At: r.At,
			Prior: &mqttPriorLifetime{ExpiryInterval: r.PriorExpiry, Clean: r.PriorClean}}

	case prev.State == mqttAuthorityServing:
		t.Attempt.Kind = mqttTransitionAcceptHandoff
		if err := installNext(); err != nil {
			return nil, err
		}
		if err := mqttValidateRetireTargets(getHash(r.ID), prev.Gen, r.Retire); err != nil {
			return nil, fmt.Errorf("the clean start of %q names an unsupported retirement target: %w", r.ID, err)
		}
		_, pord, _ := mqttParseGenToken(prev.Gen)
		nl.Outstanding = append(nl.Outstanding, mqttOutstandingGen{
			Ord: pord, Phase: mqttOutstandingOpen, Acqs: pl.GenAcqs,
			Teardown: mqttOutstandingTeardown{Kind: mqttTeardownHandoffDiscard},
		})
		nl.GenAcqs = false
		nl.Pending = &mqttPendingAcceptance{Kind: mqttPendingFresh, AcceptOrd: accept.Ord, Nonce: r.Nonce, At: r.At}
		if err := mqttCheckGenerationSlots(len(nl.Outstanding), true); err != nil {
			return nil, err
		}
		cur, curSeq, lerr := jsa.loadGenLedger(r.ID, r.Epoch, prev.Gen)
		if lerr != nil {
			return nil, lerr
		}
		hdr, hdrSeq, herr := jsa.loadDirShardAt(mqttDirShardFor(getHash(r.ID)))
		if herr != nil {
			return nil, herr
		}
		enr, cerr := mqttComposeClosingEnrolment(r.ID, r.Epoch, prev.Gen, r.Retire, cur, curSeq, hdr, hdrSeq)
		if cerr != nil {
			return nil, cerr
		}
		t.Closure = &mqttGenClosure{
			ID: r.ID, Epoch: r.Epoch, Gen: prev.Gen, Txn: r.Txn,
			RootShard: enr.RootShard, RootOrd: enr.RootOrd,
		}
		t.Extra = enr.Members
		t.Retire = &mqttRetirementRequest{
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
			Reason:        mqttRetireCleanStart,
			AffineDurable: r.Retire.AffineDurable, AffineGen: r.Retire.AffineGen, PubRelDurable: r.Retire.PubRelDurable,
		}

	default:
		return nil, fmt.Errorf("the authority of %q is in state %d", r.ID, prev.State)
	}
	t.Attempt.Gen = next.Gen
	t.Auth = next
	return &mqttComposedAcceptance{Gen: next.Gen, T: t, Anchor: own, L: pl.L}, nil
}

// composeBind composes the bind of this CONNECT's pending acceptance, with the grant point's
// record member supplied later.
func mqttComposeBind(prev *mqttOutboundAuthority, prevSeq uint64, txn, nonce string) (*mqttAuthorityTransition, error) {
	if prev == nil || prev.Life == nil || prev.Life.Pending == nil || prev.Life.Pending.Nonce != nonce {
		return nil, fmt.Errorf("no acceptance %q is pending", nonce)
	}
	nl := *prev.Life
	nl.Pending = nil
	next := *prev
	next.Life, next.Rev, next.Txn = &nl, prev.Rev+1, txn
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Scope: mqttLifeScope(prev.Life), Op: "op-" + txn,
			Kind:    mqttTransitionBind,
			PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1, Gen: prev.Gen, Tenure: prev.Tenure,
			OwnerTxn: prev.OwnerTxn, Intent: nonce,
		},
		Prev: prev, PrevSeq: prevSeq, Auth: &next,
	}, nil
}

// mqttValidateRetireTargets refuses a Clean Start's retirement targets that are not a whole affine
// descriptor or nothing: the closing enrols them, so a PUBREL consumer named alone would be
// enrolled without the affine target it belongs to.
func mqttValidateRetireTargets(idHash, gen string, r mqttClosingTargets) error {
	if r.AffineDurable == _EMPTY_ && r.AffineGen == _EMPTY_ && r.PubRelDurable == _EMPTY_ {
		return nil
	}
	return mqttValidateAffineDescriptor(idHash, gen, r.AffineGen, r.AffineDurable, r.PubRelDurable)
}

// mqttSameOutstanding reports whether two outstanding lists are equal.
func mqttSameOutstanding(a, b []mqttOutstandingGen) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// mqttDisplaceRelease is the outstanding list a displacement of the marker nonce installs: that
// marker's pending release discharged as displaced, everything else unchanged.
func mqttDisplaceRelease(out []mqttOutstandingGen, nonce string) ([]mqttOutstandingGen, error) {
	next := append([]mqttOutstandingGen(nil), out...)
	for i, o := range next {
		if o.Teardown.Nonce == nonce && o.Teardown.Pending &&
			(o.Teardown.Kind == mqttTeardownMarker || o.Teardown.Kind == mqttTeardownAbandon) {
			next[i].Teardown.Pending, next[i].Teardown.Displaced = false, true
			return next, nil
		}
	}
	return nil, fmt.Errorf("no outstanding generation carries a pending release of marker %q", nonce)
}

// mqttCheckDisplacedRelease is the boundary's half of mqttDisplaceRelease: exactly one pending
// marker release moved to displaced, and nothing else changed.
func mqttCheckDisplacedRelease(prev, next []mqttOutstandingGen) error {
	if len(prev) != len(next) {
		return fmt.Errorf("the outstanding generations change")
	}
	moved := 0
	for i, p := range prev {
		n := next[i]
		if n == p {
			continue
		}
		q := p
		q.Teardown.Pending, q.Teardown.Displaced = false, true
		if n != q || !p.Teardown.Pending || (p.Teardown.Kind != mqttTeardownMarker && p.Teardown.Kind != mqttTeardownAbandon) {
			return fmt.Errorf("generation %d changes beyond its displaced release", p.Ord)
		}
		moved++
	}
	if moved != 1 {
		return fmt.Errorf("%d marker releases are displaced, not one", moved)
	}
	return nil
}

// mqttReleaseAllPending is the outstanding list with every pending marker release discharged: what
// a reopen installs, whose create-only record member proves the record those markers stood on is
// gone.
func mqttReleaseAllPending(out []mqttOutstandingGen) []mqttOutstandingGen {
	next := append([]mqttOutstandingGen(nil), out...)
	for i := range next {
		next[i].Teardown.Pending = false
	}
	if len(next) == 0 {
		return nil
	}
	return next
}
