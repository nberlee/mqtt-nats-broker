//go:build mqttport_fulltests

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
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// TestF105MintOutboundAuthorityCreatesAServingAuthority drives the production minting executor
// end to end against the store: from an absent authority, reserve -> commit-metadata ->
// finalize must leave a SERVING authority at the generation and tenure the mint chose. This is
// the reusable core the accept flip will call in place of saveAcceptWithDetail; here it is
// exercised standalone (no accept path), which is the whole point of building it unwired first.
func TestF105MintOutboundAuthorityCreatesAServingAuthority(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-mint")
	defer done()
	jsa := &e.asm.jsa

	// The metadata half the mint commits: the session body and acceptance fact the accept
	// flip will supply from its own context. Gen/WID here are overwritten by opCommitMetadata
	// from the proposal and SessWID, exactly as the accept path's would be.
	meta := mqttOwnershipMetadataInputs{
		DomainTk: _EMPTY_,
		Record:   &mqttPersistedSession{ID: e.id, Gen: testF105GenInvented, WID: nuid.Next(), Origin: "o"},
		SessWID:  nuid.Next(),
		AcceptID: mqttNewAcceptID(),
		AcceptAt: time.Now().UnixNano(),
	}
	b, _ := mqttNewOwnershipBudget(time.Now(), 5*time.Second)

	gen, tenure, res, err := jsa.mintOutboundAuthority(e.id, e.epoch, meta, b, time.Now)
	if err != nil {
		t.Fatalf("minting the outbound authority: %v", err)
	}
	if res.Outcome != mqttOwnershipServable {
		t.Fatalf("minting drove to outcome %v (%s), want a servable authority", res.Outcome, res.Reason)
	}

	// The authority now STANDS and is SERVING, at the mint's own generation and tenure.
	a, _, aerr := jsa.servingAuthority(e.id, e.epoch)
	if aerr != nil {
		t.Fatalf("no serving authority after a servable mint: %v", aerr)
	}
	if a.Gen != gen || a.Tenure != tenure {
		t.Fatalf("the serving authority carries %q/%q, but the mint installed %q/%q",
			a.Gen, a.Tenure, gen, tenure)
	}

	// And an admission now COMPOSES (the reservation-refusal is gone): a serving authority is
	// exactly what row-1 admission needs, which is why minting is the flip's prerequisite.
	if _, admitErr := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-after-mint", Op: "op-after-mint",
		Gen: gen, Tenure: tenure,
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		}),
	}); admitErr != nil {
		t.Fatalf("a publish could not be admitted under the freshly minted serving authority: %v", admitErr)
	}
}

// TestF105MintOrHandoffResumesAndCleanStarts drives the resume/handoff arm against the store.
// From a SERVING authority, a RESUME hands off to a serving authority at the SAME generation with
// a NEW tenure (a resume preserves the generation) and an admission then composes under it; a
// CLEAN START installs a FRESH generation. The predecessor session record's write identity and
// sequence — which the accept path threads from its own session object — are read here from the
// store, since a handoff's metadata commit CASes against them.
func TestF105MintOrHandoffResumesAndCleanStarts(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-handoff")
	defer done()
	jsa := &e.asm.jsa
	meta := func() mqttOwnershipMetadataInputs {
		return mqttOwnershipMetadataInputs{
			DomainTk: e.asm.domainTk,
			Record:   &mqttPersistedSession{ID: e.id, Gen: testF105GenInvented, WID: nuid.Next(), Origin: "o"},
			SessWID:  nuid.Next(),
			AcceptID: mqttNewAcceptID(),
			AcceptAt: time.Now().UnixNano(),
		}
	}
	budget := func() *mqttOwnershipBudget { b, _ := mqttNewOwnershipBudget(time.Now(), 5*time.Second); return b }

	// The standing session record's write identity and sequence (the CONNECT that testF105NewEnv
	// ran wrote it; each metadata commit below rewrites it, so re-read before each handoff).
	prevSess := func() (string, uint64) {
		smsg, lerr := jsa.loadSessionMsg(e.asm.domainTk, getHash(e.id))
		if lerr != nil {
			t.Fatalf("loading the standing session record: %v", lerr)
		}
		var ps mqttPersistedSession
		if uerr := json.Unmarshal(smsg.Data, &ps); uerr != nil {
			t.Fatalf("decoding the standing session record: %v", uerr)
		}
		return ps.WID, smsg.Sequence
	}

	// A serving authority first (the creation branch: no live authority stands).
	gen0, tenure0, res0, err := jsa.mqttMintOrHandoffAuthority(e.id, e.epoch, false, _EMPTY_, 0, meta(), budget(), time.Now)
	if err != nil || res0.Outcome != mqttOwnershipServable {
		t.Fatalf("initial mint: err=%v outcome=%v (%s)", err, res0.Outcome, res0.Reason)
	}

	// A RESUME (cleanStart=false): preserve gen0, install a NEW tenure.
	pwid, pseq := prevSess()
	gen1, tenure1, res1, err := jsa.mqttMintOrHandoffAuthority(e.id, e.epoch, false, pwid, pseq, meta(), budget(), time.Now)
	if err != nil || res1.Outcome != mqttOwnershipServable {
		t.Fatalf("resume handoff: err=%v outcome=%v (%s)", err, res1.Outcome, res1.Reason)
	}
	if gen1 != gen0 {
		t.Fatalf("a resume changed the generation %q -> %q; a resume preserves it", gen0, gen1)
	}
	if tenure1 == tenure0 {
		t.Fatalf("a resume reused tenure %q; a handoff installs a new tenure", tenure0)
	}
	a, _, aerr := jsa.servingAuthority(e.id, e.epoch)
	if aerr != nil || a.Gen != gen1 || a.Tenure != tenure1 {
		t.Fatalf("after the resume the serving authority is %v (%q/%q), want %q/%q", aerr, a.Gen, a.Tenure, gen1, tenure1)
	}
	if _, admitErr := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-after-resume", Op: "op-after-resume", Gen: gen1, Tenure: tenure1,
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 9, Dur: "$MQTT_src", Seq: 2, QoS: 2, Nonce: mqttNewInflightNonce(),
		}),
	}); admitErr != nil {
		t.Fatalf("a publish could not be admitted under the resumed serving authority: %v", admitErr)
	}

	// A CLEAN START (cleanStart=true): install a FRESH generation.
	pwid, pseq = prevSess()
	gen2, _, res2, err := jsa.mqttMintOrHandoffAuthority(e.id, e.epoch, true, pwid, pseq, meta(), budget(), time.Now)
	if err != nil || res2.Outcome != mqttOwnershipServable {
		t.Fatalf("clean-start handoff: err=%v outcome=%v (%s)", err, res2.Outcome, res2.Reason)
	}
	if gen2 == gen1 {
		t.Fatalf("a clean start reused generation %q; it must mint a fresh one", gen1)
	}
}
