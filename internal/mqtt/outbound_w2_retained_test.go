//go:build mqtt_fulltests

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
	"sync/atomic"
	"testing"
)

// A DELETION AUTHORIZATION IS CONDITIONED ON THE RECORDS IT READ (F-105 WP-F W2, row M12).
//
// Authorizing a retained payload's deletion rests on "nothing depends on it" as of the resource
// sequence and the authority revision the authorization was composed against. If an edge lands on
// the resource between that read and the batch's commit, the batch's store comparisons - the
// authority's revision and the resource's own sequence - refuse it: the resource stays LIVE with
// the edge, and no physical delete is licensed.
//
// CONTROL / WITNESS, NOT A REACHABLE SCHEDULE. On the production route no admission can take an
// edge on a SEALED payload: the seal requires the copy's manifest CLOSED (the boundary's sealable
// rule), and a retained admission requires a READY manifest. So the competing write is PLANTED in
// the authorization's presend window - an authority revision and a raw resource CAS adding a
// retained-material edge - which is exactly what a racing admission would have changed. Mutation
// (M12): the authorization's authority and resource expectations both dropped (two-site).
func TestF105RetainedAuthorizationLosesToAnEdge(t *testing.T) {
	e, done := testF105NewEnv(t, "w2-retained-auth-edge")
	defer done()
	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")

	// A retained payload owner: LIVE, provisioning sealed (its manifest closed first, as the
	// recovery order requires), zero dependents.
	m, mSeq := testRetainedMaterialize(t, e, auth, "pay-auth", "inc-auth", []byte("auth"), 1)
	if _, err := jsa.opCloseRetainedManifest("cls-auth", "op-cls-auth", m, mSeq); err != nil {
		t.Fatalf("closing the copy: %v", err)
	}
	if _, err := jsa.opSealProvisioning(e.id, e.epoch, "seal-auth", "op-seal-auth", auth.Gen, "inc-auth"); err != nil {
		t.Fatalf("sealing the owner's provisioning: %v", err)
	}
	r0, _, err := jsa.loadOutboundResource(e.id, e.epoch, auth.Gen, "inc-auth")
	if err != nil || r0.Kind != mqttResourceRetainedPayload || r0.State != mqttResourceLive ||
		r0.Prov != mqttProvisioningSealed || len(r0.Deps) != 0 {
		t.Fatalf("fixture: the owner is not a LIVE, sealed, unneeded retained payload: %+v (%v)", r0, err)
	}

	edge := mqttResourceEdge{Nonce: mqttNewInflightNonce(), Role: mqttRoleRetainedMaterial}
	var fired atomic.Bool
	var plantErr error
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Attempt == nil || tr.Attempt.Kind != mqttTransitionDisposeResource || !fired.CompareAndSwap(false, true) {
			return
		}
		// What a racing admission changes: the authority's revision, and the resource's edges.
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		if al.status != mqttOutboundValid {
			plantErr = errors.New("no authority stands to revise")
			return
		}
		if _, plantErr = jsa.commitAuthorityTransition(testF105AdmitNothing(e, al.rec, al.seq, "txn-racer")); plantErr != nil {
			return
		}
		cur, curSeq, lerr := jsa.loadOutboundResource(e.id, e.epoch, auth.Gen, "inc-auth")
		if lerr != nil {
			plantErr = lerr
			return
		}
		next := *cur
		next.Deps = append(append([]mqttResourceEdge(nil), cur.Deps...), edge)
		body, merr := json.Marshal(&next)
		if merr != nil {
			plantErr = merr
			return
		}
		msg, hdr := mqttFrameBatchWrite(curSeq, body, _EMPTY_, 0, false)
		_, plantErr = jsa.storeMsg(mqttResourceSubject(getHash(e.id), auth.Gen, "inc-auth"), hdr, msg)
	}
	jsa.testTransitionPresend.Store(&pre)
	t.Cleanup(func() { jsa.testTransitionPresend.Store(nil) })
	var sendErr error
	var observed atomic.Bool
	commit := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, serr := send()
		if tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionDisposeResource && tr.Attempt.Txn == "authz-auth" &&
			observed.CompareAndSwap(false, true) {
			sendErr = serr
		}
		return seq, serr
	}
	jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { jsa.testTransitionCommit.Store(nil) })

	_, aerr := jsa.opAuthorizeDeletion(e.id, e.epoch, "authz-auth", "op-authz-auth", auth.Gen, "inc-auth")
	// ROUTE WITNESS: the window was reached and the competing write landed in it.
	if !fired.Load() {
		t.Fatal("fixture: the authorization never reached its presend window")
	}
	if plantErr != nil {
		t.Fatalf("fixture: the competing write did not land in the authorization's window: %v", plantErr)
	}
	// TARGET: the authorization lost, at the store's comparisons.
	if aerr == nil {
		t.Fatal("the deletion authorization committed over an edge that landed after its read")
	}
	if !observed.Load() {
		t.Fatalf("the authorization was refused before its batch reached the store: %v", aerr)
	}
	if ae := jsAPIError(sendErr); ae == nil || !isQoS2CASRejection(ae) {
		t.Fatalf("the authorization was refused by something other than the store's sequence CAS: %v", sendErr)
	}
	r1, _, err := jsa.loadOutboundResource(e.id, e.epoch, auth.Gen, "inc-auth")
	if err != nil || r1.State != mqttResourceLive || r1.AuthTxn != _EMPTY_ || len(r1.Deps) != 1 || r1.Deps[0] != edge {
		t.Fatalf("the owner must stand LIVE with the edge and no authorization: %+v (%v)", r1, err)
	}
	// No physical delete was licensed: the copy's manifest still stands.
	if mm, _, merr := jsa.loadRetainedManifest(e.id, e.epoch, auth.Gen, "pay-auth"); merr != nil || mm == nil ||
		mm.State != mqttManifestClosed {
		t.Fatalf("the copy's material moved under a refused authorization: %+v (%v)", mm, merr)
	}
}
