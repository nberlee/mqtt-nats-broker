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

//go:build mqtt_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"testing"
)

// issue #107 8b, the unit-level half: the one creator of a ledger, the installation order at both
// layers, the closing driver's licence, and the close-optimization audit. The CONNECT-level half
// is outbound_tenure_per_acceptance_e2e_test.go. Mutation ledger: mutation-ledger-8b.md.

// T4u: the composer refuses to install an OLDER tenure over a newer one. Mutation M-comp-order.
func Test8bComposerRefusesAnOlderInstallation(t *testing.T) {
	x := testNewBoundaryEnv(t)
	x.install(t, testTenure(10))
	b, err := mqttComposeSharedInstall(x.epoch, x.id, x.gen, testTenure(7), x.ledger(t))
	if !errors.Is(err, errMQTTSharedTenureSuperseded) {
		t.Fatalf("installing tenure 7 over 10 composed %+v, %v: want errMQTTSharedTenureSuperseded", b, err)
	}
}

// T4v: the write boundary refuses an installation that does not ADVANCE the tenure, isolated on
// EQUAL sequences (the general decreasing-sequence rule cannot fire) with a different nonce, so
// only the install-specific ordering rule can refuse it. Mutation M-val-order.
func Test8bValidatorRefusesANonAdvancingInstallation(t *testing.T) {
	x := testNewBoundaryEnv(t)
	x.install(t, testTenure(10))
	b, err := mqttComposeSharedInstall(x.epoch, x.id, x.gen, testTenure(11), x.ledger(t))
	if err != nil || b == nil {
		t.Fatalf("composing a legal installation to mutate: %+v, %v", b, err)
	}
	same := mqttSharedTenure{Seq: 10, ID: mqttNewInflightNonce()}
	b.Tenure, b.LedgerNext.Tenure = same, same
	x.refused(t, "an installation at the standing sequence under another nonce", b)
	if l := x.ledger(t); l.status != mqttOutboundValid || l.rec.Tenure.Seq != 10 || l.rec.Tenure.ID == same.ID {
		t.Fatalf("the refused installation changed the ledger: %+v", l.rec)
	}
}

// T5c: a join never creates a ledger — the composer refuses one over absence. Mutation
// M-join-comp.
func Test8bComposerRefusesAJoinOverAbsence(t *testing.T) {
	x := testNewBoundaryEnv(t)
	b, err := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, mqttNewInflightNonce(), testTenure(3),
		mqttSharedGroupLoad{status: mqttOutboundAbsent}, mqttSharedLedgerLoad{status: mqttOutboundAbsent},
		mqttSharedLedgerMaxAcqs(mqttDefaultMaxAckPending))
	if !errors.Is(err, errMQTTSharedJoinWithoutLedger) {
		t.Fatalf("a join over an absent ledger composed %+v, %v: want errMQTTSharedJoinWithoutLedger", b, err)
	}
}

// T5v: the write boundary refuses a join whose ledger prior is absent, whatever composed it:
// a join legal against generation G's installed ledger is re-targeted at a generation with no
// ledger and handed in directly. Mutation M-join-val.
func Test8bValidatorRefusesAJoinOverAbsence(t *testing.T) {
	x := testNewBoundaryEnv(t)
	tenure := testTenure(3)
	x.install(t, tenure)
	b, err := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, mqttNewInflightNonce(), tenure,
		x.group(t), x.ledger(t), mqttSharedLedgerMaxAcqs(mqttDefaultMaxAckPending))
	if err != nil {
		t.Fatalf("composing the legal join to re-target: %v", err)
	}
	other := mqttNewSessGen()
	b.Gen = other
	b.LedgerPrior, b.LedgerSeq, b.ledgerExpect = nil, 0, 0
	b.LedgerNext = &mqttSharedLedger{Epoch: x.epoch, ID: x.id, Gen: other, State: mqttSharedLedgerOpen,
		Tenure: tenure, Acqs: []mqttSharedAcqRef{b.Acq}, Closed: []string{}}
	x.refused(t, "a join creating a ledger", b)
	if l := x.e.asm.jsa.loadSharedLedger(x.id, x.epoch, other); l.status != mqttOutboundAbsent {
		t.Fatalf("a refused join left generation %q's ledger reading %v", other, l.status)
	}
}

// T5a: an exhausted acquisition meeting an absent ledger is UNRESOLVED and writes nothing — the
// abort never recreates a ledger. Mutation M-abort-init.
func Test8bAbortOverAbsenceWritesNothing(t *testing.T) {
	e := testNewInflightEnv(t, "f105-8b-abort-absent")
	e.sess.mu.Lock()
	id, gen, epoch := e.sess.id, e.sess.gen, e.sess.sharedEpoch()
	e.sess.mu.Unlock()
	tenure := testTenure(4)
	_, out, err := e.sess.abortSharedAcquisition(id, gen, epoch, tenure, mqttNewInflightNonce(), 0)
	if out != mqttSharedAcquireFailed || err == nil {
		t.Fatalf("the abort over absence reported %v, %v", out, err)
	}
	if l := e.sess.jsa.loadSharedLedger(id, epoch, gen); l.status != mqttOutboundAbsent {
		t.Fatalf("the abort over absence wrote the ledger: it reads %v", l.status)
	}
	_, out, err = e.sess.abortSharedAcquisition(id, gen, epoch, tenure, mqttNewInflightNonce(), 7)
	if out != mqttSharedAcquireFailed || !errors.Is(err, errMQTTSharedAcquireExhausted) {
		t.Fatalf("an abort of attempts composed against a sequence, meeting absence, reported %v, %v: want exhausted (unresolved)", out, err)
	}
	if l := e.sess.jsa.loadSharedLedger(id, epoch, gen); l.status != mqttOutboundAbsent {
		t.Fatalf("the abort recreated an absent ledger: it reads %v", l.status)
	}
}

// test8bInstallEmpty creates the generation's ledger the one legal way, empty.
func test8bInstallEmpty(t *testing.T, e *testOutboundEnv, gen string) {
	t.Helper()
	tenure := testTenure(2)
	testAcceptSharedTenure(t, &e.asm.jsa, e.id, e.epoch, tenure)
	if _, err := e.asm.jsa.installSharedTenure(e.id, e.epoch, gen, tenure); err != nil {
		t.Fatalf("installing an empty ledger: %v", err)
	}
}

func test8bLedgerState(e *testOutboundEnv, gen string) (mqttOutboundLoadStatus, bool, int) {
	l := e.asm.jsa.loadSharedLedger(e.id, e.epoch, gen)
	if l.status != mqttOutboundValid {
		return l.status, false, 0
	}
	return l.status, l.rec.State == mqttSharedLedgerSealed, len(l.rec.Acqs)
}

// T13: the closing driver's licence, row by row (consult 122). Mutations M-preserve-licence (the
// emptiness requirement dropped for a never-authorized generation) and M-sealed-resume.
func Test8bClosingLicenceMatrix(t *testing.T) {
	type row struct {
		name     string
		setup    func(t *testing.T, e *testOutboundEnv, auth *mqttOutboundAuthority) bool // returns neverAuthorized
		licensed bool
	}
	rows := []row{
		{"discarding closure over a listed acquisition", func(t *testing.T, e *testOutboundEnv, auth *mqttOutboundAuthority) bool {
			testPlantSharedAcq(t, e, auth.Gen, "g8b-disc")
			testDiscardClose(t, e)
			return false
		}, true},
		{"serving with no closure, empty ledger", func(t *testing.T, e *testOutboundEnv, auth *mqttOutboundAuthority) bool {
			test8bInstallEmpty(t, e, auth.Gen)
			return false
		}, false},
		{"authority read absent, absent ledger", func(t *testing.T, e *testOutboundEnv, auth *mqttOutboundAuthority) bool {
			return true
		}, true},
		{"authority read absent, listed acquisition", func(t *testing.T, e *testOutboundEnv, auth *mqttOutboundAuthority) bool {
			testPlantSharedAcq(t, e, auth.Gen, "g8b-abs")
			return true
		}, false},
		{"corrupt ledger behind a discarding closure", func(t *testing.T, e *testOutboundEnv, auth *mqttOutboundAuthority) bool {
			testDiscardClose(t, e)
			if _, err := e.asm.jsa.storeMsg(mqttSharedLedgerSubject(getHash(e.id), auth.Gen), 0, []byte(`{"epoch":`)); err != nil {
				t.Fatalf("planting a corrupt ledger: %v", err)
			}
			return false
		}, false},
	}
	for i, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			e, done := testNewEnv(t, "f105-8b-licence-"+string(rune('a'+i)))
			defer done()
			auth, _ := e.bootstrap(t, "txn-1")
			gen := auth.Gen
			absent := r.setup(t, e, auth)
			stBefore, sealedBefore, _ := test8bLedgerState(e, gen)
			err := e.asm.jsa.closeRetiringGeneration(e.id, e.epoch, gen, absent)
			st, sealed, n := test8bLedgerState(e, gen)
			if r.licensed {
				if err != nil {
					t.Fatalf("a licensed closing withheld: %v", err)
				}
				// A GENERATION THAT NEVER ACQUIRED HAS NO LEDGER, before or after its closing (issue
				// #133): there is nothing to seal. One that acquired is left sealed and empty.
				if stBefore == mqttOutboundAbsent && st == mqttOutboundAbsent {
					return
				}
				if st != mqttOutboundValid || !sealed || n != 0 {
					t.Fatalf("a licensed closing left the ledger %v sealed=%v listing %d", st, sealed, n)
				}
				return
			}
			if !errors.Is(err, errMQTTClosingUnlicensed) {
				t.Fatalf("an unlicensed closing returned %v: want errMQTTClosingUnlicensed", err)
			}
			if sealed && !sealedBefore {
				t.Fatalf("an unlicensed closing SEALED the ledger")
			}
		})
	}
}

// T13d: a closing that fails after its seal is finished by the next driver, which resumes from
// the sealed ledger and releases its acquisitions. (T13c's late join, landing after the driver's
// empty observation, no longer exists: a join is fenced by the authority to the generation it
// serves, so none can land once the generation is discarded, issue #133.) Mutation
// M-sealed-resume.
func Test8bAClosingResumesFromItsSeal(t *testing.T) {
	e, done := testNewEnv(t, "f105-8b-resume")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	gen := auth.Gen
	testPlantSharedAcq(t, e, gen, "g8b-held")
	testDiscardClose(t, e)
	// The state a driver leaves when it crashes after its seal.
	if _, err := e.asm.jsa.sealSharedLedger(e.id, e.epoch, gen, nil); err != nil {
		t.Fatalf("sealing as the crashed driver did: %v", err)
	}
	if _, sealed, n := test8bLedgerState(e, gen); !sealed || n != 1 {
		t.Fatalf("the crashed driver's ledger must be sealed listing the acquisition: sealed=%v n=%d", sealed, n)
	}
	if err := e.asm.jsa.closeRetiringGeneration(e.id, e.epoch, gen, false); err != nil {
		t.Fatalf("the closing withheld: %v", err)
	}
	if err := testSharedLedgerClosedEmpty(e, gen); err != nil {
		t.Fatalf("the sealed acquisition was not released: %v", err)
	}
}

// testComposeClaimOver composes a teardown claim over the env's serving authority, with a
// minimal marker body naming its generation.
func testComposeClaimOver(e *testOutboundEnv, auth *mqttOutboundAuthority, seq uint64, ret mqttRetirementRequest) (*mqttAuthorityTransition, error) {
	marker, _ := json.Marshal(&mqttPersistedSession{
		ID: e.id, Gen: auth.Gen, WID: "marker-wid-x", PWID: "wid-x",
		Teardown: &mqttSessTeardown{Origin: "test", Nonce: "nonce-x", StartedAt: 1},
	})
	return e.asm.jsa.composeTeardownClaim(mqttTeardownClaimRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-claim", Prev: auth, PrevSeq: seq, Retire: ret,
		RecSubj: mqttSessSubject(e.asm.domainTk, getHash(e.id)), RecSeq: 1, Marker: marker,
		Nonce: "nonce-x", MarkerWID: "marker-wid-x",
	})
}

// A teardown claim binds its retirement to the standing ownership: a retirement naming another
// tenure composes nothing.
func TestTeardownClaimRefusesAnotherOwnership(t *testing.T) {
	e, done := testNewEnv(t, "f105-8b-fastbind")
	defer done()
	auth, seq := e.bootstrap(t, "txn-1")
	ret := mqttRetirementRequest{
		Gen: auth.Gen, Tenure: "tenure-other", OwnerTxn: auth.OwnerTxn,
		SessWID: "wid-x", SessSeq: 1, Reason: mqttRetireSessionEnd,
	}
	if _, err := testComposeClaimOver(e, auth, seq, ret); err == nil {
		t.Fatal("a teardown claim under a retirement naming another tenure was composed")
	}
}
