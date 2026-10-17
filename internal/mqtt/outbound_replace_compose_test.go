//go:build mqttport_fulltests

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
	"strings"
	"testing"
)

// Composing a replacing admission (F-105, row 1).
//
// These drive the composer through the REAL write boundary, because a composer whose output the
// boundary refuses has composed nothing — and because the boundary re-derives everything, a
// batch that commits is a batch both layers agree about.

// replacingFor builds the composer input for the occupant standing in r, in the given mode.
//
// The register carries the lineage the admission ladder would have frozen for that mode
// (mqttLineageForWrite), because that is what the composer is handed on the production route and
// what it requires to agree with its own derivation. A mode with no derivation keeps the
// allocation's nonce, so only the missing mode is wrong with it.
func (r *testF105ReplEnv) replacingFor(t *testing.T, mode mqttReplacementMode, txn string) *mqttReplacingAdmission {
	t.Helper()
	al := r.asm.jsa.loadOutboundAuthority(r.id, r.epoch)
	reg := &mqttInflightRecord{
		State: mqttInflightPublish, PI: r.pi, Dur: r.occ.Dur, Seq: r.occ.Seq, QoS: r.occ.QoS,
		Nonce: mqttNewInflightNonce(), Claim: r.occ.Claim, ClaimSeq: r.occ.ClaimSeq,
		SourceAckReply: r.occ.SourceAckReply,
	}
	reg.Lin, _ = mqttLineageForWrite(mode.lineageWrite(), r.occ, reg.Nonce)
	if reg.Lin == _EMPTY_ {
		reg.Lin = reg.Nonce
	}
	// THE LEDGER, read as it stands. A pair replacement admits a FRESH obligation, so it
	// allocates a duty ordinal like any other admission; a register-only replacement continues
	// its predecessor's obligation and allocates nothing, which is why the composer is given
	// the ledger in both cases and uses it in only one.
	led, ledSeq, lerr := r.asm.jsa.loadGenLedger(r.id, r.epoch, testF105Gen)
	if lerr != nil {
		t.Fatalf("reading the generation ledger: %v", lerr)
	}
	return &mqttReplacingAdmission{
		Auth: al.rec, AuthSeq: al.seq, Txn: txn, Op: "op-" + txn, Mode: mode,
		RegSubj: r.regSubj, Occupant: r.occ, OccupantSeq: r.occSeq,
		MarkerSeq: r.markerSeq(t), Register: reg,
		Ledger: led, LedgerSeq: ledSeq,
	}
}

func TestF105ComposedRegisterOnlyReplacementCommits(t *testing.T) {
	r, done := testF105NewReplEnv(t, "f105-comp-inherit")
	defer done()
	r.deleteMarker(t)

	// A caller offering a FRESH lineage is REFUSED, not corrected: the lineage the caller holds is
	// the one custody charged for the attempt, so overwriting it would write one obligation while
	// custody charges another.
	fresh := r.replacingFor(t, mqttReplaceRegisterOnly, "txn-comp-inherit-fresh")
	fresh.Register.Lin = fresh.Register.Nonce
	if _, err := mqttReplacePublishTransition(fresh); err == nil ||
		!strings.Contains(err.Error(), "a replacement that disagrees is never submitted") {
		t.Fatalf("a register-only replacement offered a fresh lineage was answered %v, want the "+
			"lineage agreement refusal", err)
	}

	adm := r.replacingFor(t, mqttReplaceRegisterOnly, "txn-comp-inherit")
	// The caller offers the wrong QoS; the composer derives it from the occupant, because it is
	// what the modes disagree about and a caller's opinion of it is not an input.
	adm.Register.QoS = 1
	tr, err := mqttReplacePublishTransition(adm)
	if err != nil {
		t.Fatalf("composing a register-only replacement failed: %v", err)
	}
	if _, err := r.asm.jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("the composed register-only replacement was refused by the boundary: %v", err)
	}

	cur, _, lerr := r.asm.jsa.loadInflightRecord(r.regSubj)
	if lerr != nil || cur == nil {
		t.Fatalf("the replacing register does not load back: %v", lerr)
	}
	if cur.Lin != r.occLin {
		t.Fatalf("the composer did not inherit the occupant's lineage: %q", cur.Lin)
	}
	if cur.QoS != r.occ.QoS {
		t.Fatalf("the composer did not preserve the occupant's QoS: %d", cur.QoS)
	}
	// NO MARKER, because minting one would describe an allocation the client may already hold
	// as never sent.
	if seq := r.markerSeq(t); seq != 0 {
		t.Fatalf("a register-only replacement minted a marker at %d", seq)
	}
}

func TestF105ComposedPairReplacementCommits(t *testing.T) {
	r, done := testF105NewReplEnv(t, "f105-comp-pair")
	defer done()

	adm := r.replacingFor(t, mqttReplacePair, "txn-comp-pair")
	adm.Exchange = &mqttOutboundExchange{
		ID: r.id, Epoch: r.epoch, Gen: testF105Gen, Nonce: adm.Register.Lin,
		PI: r.pi, QoS: r.occ.QoS, Phase: mqttExchangePublish,
		AdmitTxn: "txn-comp-pair", Tenure: adm.Auth.Tenure,
	}
	// The predecessor is LIVE, so this batch is the one that ends it. Overwriting a register
	// does not remove its exchange, and a predecessor left standing holds this identifier.
	pred := *r.occExchange(t)
	pred.Phase = mqttExchangeRejected
	adm.Predecessor, adm.PredecessorSeq = &pred, r.exchangeSeq(t, r.occLin)

	tr, err := mqttReplacePublishTransition(adm)
	if err != nil {
		t.Fatalf("composing a pair replacement failed: %v", err)
	}
	if _, err := r.asm.jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("the composed pair replacement was refused by the boundary: %v", err)
	}
	cur, _, lerr := r.asm.jsa.loadInflightRecord(r.regSubj)
	if lerr != nil || cur == nil || cur.Lin != adm.Register.Lin {
		t.Fatalf("the pair replacement did not install its fresh lineage")
	}
	// A FRESH marker, for the fresh allocation, so nothing has been transmitted under it.
	if seq := r.markerSeq(t); seq == 0 {
		t.Fatalf("a pair replacement left no unsent marker for its fresh obligation")
	}
}

func TestF105ComposerRefusesAMixedReplacement(t *testing.T) {
	t.Run("register-only carrying an exchange", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-comp-mixed")
		defer done()
		r.deleteMarker(t)
		adm := r.replacingFor(t, mqttReplaceRegisterOnly, "txn-comp-mixed")
		adm.Exchange = &mqttOutboundExchange{
			ID: r.id, Epoch: r.epoch, Gen: testF105Gen, Nonce: adm.Register.Lin,
			PI: r.pi, QoS: 2, Phase: mqttExchangePublish, AdmitTxn: "x", Tenure: adm.Auth.Tenure,
		}
		// A continued obligation already holds its edges; admitting a second would duplicate
		// its charge and its dependencies. The refusal must be THAT rule, not the lineage
		// agreement a mis-built fixture would trip first.
		if _, err := mqttReplacePublishTransition(adm); err == nil ||
			!strings.Contains(err.Error(), "admits no exchange and takes no dependency") {
			t.Fatalf("a continuing replacement carrying an exchange was answered %v", err)
		}
	})

	t.Run("pair carrying a pending-drain predecessor", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-comp-pd")
		defer done()
		adm := r.replacingFor(t, mqttReplacePair, "txn-comp-pd")
		adm.Exchange = &mqttOutboundExchange{
			ID: r.id, Epoch: r.epoch, Gen: testF105Gen, Nonce: adm.Register.Lin,
			PI: r.pi, QoS: r.occ.QoS, Phase: mqttExchangePublish,
			AdmitTxn: "txn-comp-pd", Tenure: adm.Auth.Tenure,
		}
		pred := *r.occExchange(t)
		pred.Phase = mqttExchangeTerminalPendingDrain
		adm.Predecessor, adm.PredecessorSeq = &pred, r.exchangeSeq(t, r.occLin)
		// PENDING DRAIN IS NOT AN ENDING. Output acquired before it may still execute under
		// this identifier, so the obligation still holds it.
		if _, err := mqttReplacePublishTransition(adm); err == nil {
			t.Fatalf("a pending-drain predecessor was accepted as an ending")
		}
	})

	t.Run("no mode at all", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-comp-nomode")
		defer done()
		adm := r.replacingFor(t, 0, "txn-comp-nomode")
		// The mode is the marker boundary's answer. This mechanism does not guess one.
		if _, err := mqttReplacePublishTransition(adm); err == nil {
			t.Fatalf("a replacement composed with no mode")
		}
	})
}
