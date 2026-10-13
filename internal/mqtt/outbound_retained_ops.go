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

// The retained materialization operations (F-105 7c).
//
// A retained QoS 1/2 delivery becomes durable in four acts, each one authority batch on
// $MQTT_inflight, each fenced by the producing connection's CAPTURED entitlement (compared with the
// standing authority, never adopted from it):
//
//  1. RESERVE: the payload resource, its provision and cleanup duties, and the PROVISIONING
//     manifest naming it, create-only, in one provision-resource batch — so a crash after this
//     leaves material a reclaimer can find and own.
//  2. CHUNKS: manifest CAS + create-only chunks, as many batches as the payload needs.
//  3. SEAL: PROVISIONING -> READY, the boundary verifying length and digest over the stored chunks.
//  4. ADMIT: the exchange, the retained register and its unsent marker, the payload's
//     retained-material edge, the exchange duty, and the manifest's `admitted` — one batch, so an
//     admission and a cleanup arbitrate on the copy.
//
// Then the delivery is an exchange like any other: acquisition, output, acknowledgement.

// mqttRetainedMaterial is one snapshot's material, as the producer hands it to the operations.
type mqttRetainedMaterial struct {
	Topic       string
	QoS         byte
	Props       []byte
	ExpiresAt   int64
	SessionHash string
	SubID       uint32
	Data        []byte
}

// mqttRetainedChunkPlan is how a payload is cut: the slices, each within mqttChunkMaxBytes and
// within the server's publish limit once ENCODED (the chunk is JSON with base64 data, carried in a
// batch with its headers), so a chunk that fits its slice bound still fits the wire.
func mqttRetainedChunkPlan(data []byte, maxPayload int) ([][]byte, error) {
	limit := mqttChunkMaxBytes
	if maxPayload > 0 {
		// base64 expands by 4/3, the JSON envelope and the batch headers by a bounded overhead.
		const overhead = 1024
		fit := (maxPayload - overhead) * 3 / 4
		if fit < limit {
			limit = fit
		}
	}
	if limit <= 0 {
		return nil, fmt.Errorf("the server's publish limit %d leaves no room for a chunk", maxPayload)
	}
	var out [][]byte
	for off := 0; off < len(data); off += limit {
		end := off + limit
		if end > len(data) {
			end = len(data)
		}
		out = append(out, data[off:end])
	}
	return out, nil
}

// mqttMaterializeTransition is the materialize-payload batch shape: serving, ownership unchanged.
func mqttMaterializeTransition(auth *mqttOutboundAuthority, authSeq uint64, txn, op string,
	kind mqttTransitionKind, members []mqttTransitionMember) *mqttAuthorityTransition {
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: auth.ID, Epoch: auth.Epoch, Txn: txn, Op: op,
			Kind: kind, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: authSeq,
		Auth: &mqttOutboundAuthority{
			ID: auth.ID, Epoch: auth.Epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: mqttAuthorityServing, Rev: auth.Rev + 1, Txn: txn, OwnerTxn: auth.OwnerTxn,
			Intent: auth.Intent, Creation: auth.Creation,
		},
		Extra: members,
	}
}

// entitledServingAuthority reads the serving authority and compares it with the captured
// entitlement.
func (jsa *mqttJSA) entitledServingAuthority(id, epoch, gen, tenure string) (*mqttOutboundAuthority, uint64, error) {
	auth, authSeq, err := jsa.servingAuthority(id, epoch)
	if err != nil {
		return nil, 0, err
	}
	if auth.Gen != gen || auth.Tenure != tenure {
		return nil, 0, fmt.Errorf("retained material for %q under %q/%q: the standing authority carries "+
			"%q/%q, so the entitlement that decided it has moved on: %w", id, gen, tenure, auth.Gen,
			auth.Tenure, errMQTTEntitlementEnded)
	}
	return auth, authSeq, nil
}

// opReserveRetainedPayload is act 1. It returns the manifest it reserved.
func (jsa *mqttJSA) opReserveRetainedPayload(id, epoch, txn, op, gen, tenure, inc, payload string,
	mat *mqttRetainedMaterial, chunks uint64) (*mqttRetainedManifest, mqttOpOutcome, error) {

	auth, authSeq, err := jsa.entitledServingAuthority(id, epoch, gen, tenure)
	if err != nil {
		return nil, mqttOpOutcome{}, err
	}
	r := &mqttOutboundResource{
		ID: id, Epoch: epoch, Gen: auth.Gen, Inc: inc,
		Kind: mqttResourceRetainedPayload, Name: payload, State: mqttResourceLive,
		Prov: mqttProvisioningPending, Deps: []mqttResourceEdge{},
	}
	if verr := mqttValidateOutboundResource(r, id, epoch, auth.Gen, inc); verr != nil {
		return nil, mqttOpOutcome{}, fmt.Errorf("refusing to reserve an unsupported payload resource: %w", verr)
	}
	m := &mqttRetainedManifest{
		ID: id, Epoch: epoch, Gen: auth.Gen, Payload: payload, State: mqttManifestProvisioning,
		Topic: mat.Topic, QoS: mat.QoS, Props: mat.Props, ExpiresAt: mat.ExpiresAt,
		SessionHash: mat.SessionHash, Bytes: uint64(len(mat.Data)), Chunks: chunks,
		Digest: mqttMaterialDigest(mat.Data), Inc: inc, SubID: mat.SubID, Tenure: tenure,
	}
	if verr := mqttValidateRetainedManifest(m, id, epoch, auth.Gen, payload); verr != nil {
		return nil, mqttOpOutcome{}, fmt.Errorf("refusing to reserve an unsupported manifest: %w", verr)
	}
	rb, err := json.Marshal(r)
	if err != nil {
		return nil, mqttOpOutcome{}, err
	}
	mb, err := json.Marshal(m)
	if err != nil {
		return nil, mqttOpOutcome{}, err
	}
	led, ledSeq, lerr := jsa.loadGenLedger(id, epoch, auth.Gen)
	if lerr != nil {
		return nil, mqttOpOutcome{}, lerr
	}
	inv, ierr := mqttAllocateInventoryMembers(id, epoch, auth.Gen, led, ledSeq, []mqttDutyDeclaration{
		{Kind: mqttDutyProvision, Ref: inc, State: mqttDutyPending},
		{Kind: mqttDutyCleanup, Ref: inc, State: mqttDutyNotYetActionable},
	})
	if ierr != nil {
		return nil, mqttOpOutcome{}, ierr
	}
	idHash := getHash(id)
	members := append([]mqttTransitionMember{
		{subj: mqttResourceSubject(idHash, auth.Gen, inc), expect: 0, body: rb},
		{subj: mqttManifestSubject(idHash, auth.Gen, payload), expect: 0, body: mb},
	}, inv...)
	out, cerr := jsa.commitOp(mqttMaterializeTransition(auth, authSeq, txn, op, mqttTransitionProvisionResource, members))
	return m, out, cerr
}

// opWriteRetainedChunks is act 2 for one batch of chunks, ordinals from m.Written+1.
func (jsa *mqttJSA) opWriteRetainedChunks(txn, op, tenure string, m *mqttRetainedManifest, mSeq uint64,
	slices [][]byte) (*mqttRetainedManifest, mqttOpOutcome, error) {

	auth, authSeq, err := jsa.entitledServingAuthority(m.ID, m.Epoch, m.Gen, tenure)
	if err != nil {
		return nil, mqttOpOutcome{}, err
	}
	next := *m
	idHash := getHash(m.ID)
	var members []mqttTransitionMember
	for i, s := range slices {
		ord := m.Written + uint64(i) + 1
		cb, err := json.Marshal(&mqttRetainedChunk{Ord: ord, Inc: m.Inc, Data: s})
		if err != nil {
			return nil, mqttOpOutcome{}, err
		}
		members = append(members, mqttTransitionMember{subj: mqttChunkSubject(idHash, m.Gen, m.Payload, ord), body: cb})
	}
	next.Written = m.Written + uint64(len(slices))
	mb, err := json.Marshal(&next)
	if err != nil {
		return nil, mqttOpOutcome{}, err
	}
	members = append([]mqttTransitionMember{{subj: mqttManifestSubject(idHash, m.Gen, m.Payload), expect: mSeq, body: mb}}, members...)
	out, cerr := jsa.commitOp(mqttMaterializeTransition(auth, authSeq, txn, op, mqttTransitionMaterializePayload, members))
	return &next, out, cerr
}

// opSealRetainedPayload is act 3.
func (jsa *mqttJSA) opSealRetainedPayload(txn, op, tenure string, m *mqttRetainedManifest, mSeq uint64) (*mqttRetainedManifest, mqttOpOutcome, error) {
	auth, authSeq, err := jsa.entitledServingAuthority(m.ID, m.Epoch, m.Gen, tenure)
	if err != nil {
		return nil, mqttOpOutcome{}, err
	}
	next := *m
	next.State = mqttManifestReady
	mb, err := json.Marshal(&next)
	if err != nil {
		return nil, mqttOpOutcome{}, err
	}
	members := []mqttTransitionMember{{subj: mqttManifestSubject(getHash(m.ID), m.Gen, m.Payload), expect: mSeq, body: mb}}
	out, cerr := jsa.commitOp(mqttMaterializeTransition(auth, authSeq, txn, op, mqttTransitionMaterializePayload, members))
	return &next, out, cerr
}

// mqttAdmitRetainedRequest is act 4, as its caller states it.
type mqttAdmitRetainedRequest struct {
	ID, Epoch, Txn, Op string
	// Gen and Tenure are the captured entitlement.
	Gen, Tenure string
	// Register is the retained register (publish, retained arm, custody) for this allocation.
	Register *mqttInflightRecord
	// Manifest is the READY copy, read at ManifestSeq; it names its owner incarnation.
	Manifest    *mqttRetainedManifest
	ManifestSeq uint64
}

// opAdmitRetained is act 4: opAdmitPublish's batch plus the copy's `admitted`, with the payload's
// retained-material edge as the obligation's one dependency.
func (jsa *mqttJSA) opAdmitRetained(r *mqttAdmitRetainedRequest) (mqttOpOutcome, error) {
	if r == nil || r.Register == nil || r.Manifest == nil || r.ManifestSeq == 0 {
		return mqttOpOutcome{}, fmt.Errorf("admitting a retained delivery requires its register and its READY copy")
	}
	if !r.Register.retainedSource() || r.Register.State != mqttInflightPublish {
		return mqttOpOutcome{}, fmt.Errorf("admitting a retained delivery requires a retained register at its publish")
	}
	if !mqttManifestAdmits(r.Manifest) || r.Manifest.Admitted != _EMPTY_ {
		return mqttOpOutcome{}, fmt.Errorf("retained payload %q is not admissible: an exchange is admitted only "+
			"against a READY copy nothing was admitted against", r.Manifest.Payload)
	}
	auth, authSeq, err := jsa.entitledServingAuthority(r.ID, r.Epoch, r.Gen, r.Tenure)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	x := &mqttOutboundExchange{
		ID: r.ID, Epoch: r.Epoch, Gen: auth.Gen, Nonce: r.Register.Lin,
		PI: r.Register.PI, QoS: r.Register.QoS, Phase: mqttExchangePublish,
		AdmitTxn: r.Txn, Tenure: auth.Tenure, RetainedOwner: r.Manifest.Inc,
	}
	res, resSeq, lerr := jsa.loadOutboundResource(r.ID, r.Epoch, auth.Gen, r.Manifest.Inc)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	led, ledSeq, lerr := jsa.loadGenLedger(r.ID, r.Epoch, auth.Gen)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	tr, terr := mqttAdmitPublishTransition(&mqttPublishAdmission{
		Auth: auth, AuthSeq: authSeq, Txn: r.Txn, Op: r.Op,
		Exchange: x,
		Register: r.Register,
		RegSubj:  mqttInflightSubject(getHash(r.ID), auth.Gen, r.Register.PI),
		Deps:     []mqttResourceDependency{{Resource: res, Seq: resSeq, Role: mqttRoleRetainedMaterial}},
		Ledger:   led, LedgerSeq: ledSeq,
	})
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	admitted := *r.Manifest
	admitted.Admitted, admitted.AdmittedPI = x.Nonce, x.PI
	mb, merr := json.Marshal(&admitted)
	if merr != nil {
		return mqttOpOutcome{}, merr
	}
	tr.Extra = append(tr.Extra, mqttTransitionMember{
		subj: mqttManifestSubject(getHash(r.ID), r.Manifest.Gen, r.Manifest.Payload), expect: r.ManifestSeq, body: mb})
	return jsa.commitOp(tr)
}

// mqttCloseManifestTransition closes one copy: PROVISIONING|READY -> CLOSED, a disposal under the
// standing authority (cleanup outlives closure). Whether it MAY close is the boundary's question
// (checkRetainedClose), answered from the store inside this batch.
func mqttCloseManifestTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op string,
	m *mqttRetainedManifest, mSeq uint64) (*mqttAuthorityTransition, error) {
	if prev == nil || m == nil || mSeq == 0 {
		return nil, fmt.Errorf("closing a retained copy requires the authority, the manifest and its sequence")
	}
	next := *m
	next.State = mqttManifestClosed
	body, err := json.Marshal(&next)
	if err != nil {
		return nil, err
	}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionDisposeResource, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: prev.State, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
			Intent: prev.Intent, Creation: prev.Creation,
		},
		Extra: []mqttTransitionMember{{subj: mqttManifestSubject(getHash(prev.ID), m.Gen, m.Payload), expect: mSeq, body: body}},
	}, nil
}

// opCloseRetainedManifest closes one copy under the standing authority.
func (jsa *mqttJSA) opCloseRetainedManifest(txn, op string, m *mqttRetainedManifest, mSeq uint64) (mqttOpOutcome, error) {
	auth, authSeq, err := jsa.requireAuthority(m.ID, m.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no copy of its may be closed", m.ID)
	}
	tr, terr := mqttCloseManifestTransition(auth, authSeq, txn, op, m, mSeq)
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	return jsa.commitOp(tr)
}
