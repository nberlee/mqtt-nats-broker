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
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// F-105 8a unit properties: own versus foreign identity, the settlement continuation, the
// verdict latch, and the writer-lock rule certification rests on.

// TestF105DecisionSettlesAForeignProposalByItsVerdict: a standing proposal that is not this
// acceptance's current one is settled — never finalized or abandoned as though it were its own —
// and once bound, its slot's verdict selects the continuation. Mutations M1, M2.
func TestF105DecisionSettlesAForeignProposalByItsVerdict(t *testing.T) {
	// A RESUME handoff: it preserves the generation and installs a new tenure.
	g0 := mqttNewSessGen()
	foreign := &mqttHandoffIntent{ID: "foreign", Gen: g0, Tenure: "t1", PrevGen: g0, PrevTenure: "t0",
		PrevOwnerTxn: "o0", PrevSessWID: "w0", PrevSessSeq: 3}
	prep := mqttAuthorityLoad{status: mqttOutboundValid, seq: 9, rec: &mqttOutboundAuthority{
		State: mqttAuthorityPreparing, Gen: g0, Tenure: "t0", Intent: foreign}}
	p, err := mqttProposalFromHandoff(foreign)
	if err != nil {
		t.Fatalf("the fixture intent was refused: %v", err)
	}
	bound := &mqttForeignRecovery{Proposal: p, AuthSeq: 9, AuthState: mqttAuthorityPreparing}

	for _, tc := range []struct {
		name string
		in   mqttConnectOwnershipInput
		want mqttConnectOwnershipAction
	}{
		{"unbound foreign is settled", mqttConnectOwnershipInput{Load: prep}, mqttOwnershipRecoverForeign},
		{"unbound foreign, even with a decided slot, is settled first",
			mqttConnectOwnershipInput{Load: prep, StandingVerdict: mqttMetadataAborted}, mqttOwnershipRecoverForeign},
		{"bound, slot unresolved: settle again",
			mqttConnectOwnershipInput{Load: prep, Recovering: bound}, mqttOwnershipRecoverForeign},
		{"bound, slot COMMITTED: finalize as proposed",
			mqttConnectOwnershipInput{Load: prep, Recovering: bound, StandingVerdict: mqttMetadataCommitted},
			mqttOwnershipFinalizeHandoff},
		{"bound, slot ABORTED: cancel back to the predecessor",
			mqttConnectOwnershipInput{Load: prep, Recovering: bound, StandingVerdict: mqttMetadataAborted},
			mqttOwnershipCancelHandoff},
		{"own proposal, COMMITTED: finalize own",
			mqttConnectOwnershipInput{Load: prep, IntentID: "foreign", Metadata: mqttMetadataCommitted},
			mqttOwnershipFinalizeHandoff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := mqttDecideConnectOwnership(tc.in)
			if d.Action != tc.want {
				t.Fatalf("decided %v (%s), want %v", d.Action, d.Reason, tc.want)
			}
			if tc.in.IntentID == _EMPTY_ && d.Action != mqttOwnershipRefuse && d.Recovery == nil {
				t.Fatalf("an act on a proposal this acceptance did not make must NAME it")
			}
		})
	}
}

// TestF105DriverLatchesAVerdictReadOnTheExhaustingReload: an ABORTED verdict first read on the
// reload that exhausts the budget is the one the expiry disposes — latched before the budget is
// asked. Mutation M15.
func TestF105DriverLatchesAVerdictReadOnTheExhaustingReload(t *testing.T) {
	b, _ := mqttNewOwnershipBudget(time.Unix(1000, 0), time.Second)
	clock := time.Unix(1000, 0)
	now := func() time.Time { return clock }
	g := mqttNewSessGen()
	own := &mqttOutboundAuthority{State: mqttAuthorityReserving, Gen: g, Tenure: "t",
		Creation: &mqttCreationIntent{ID: "own", Gen: g, Tenure: "t"}}
	reserving := mqttAuthorityLoad{status: mqttOutboundValid, rec: own}
	exec := func(mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
		clock = clock.Add(2 * time.Second) // the budget is gone when this reload lands
		return mqttConnectOwnershipInput{Load: reserving, IntentID: "own", Metadata: mqttMetadataAborted}, nil
	}
	res, err := mqttDriveOwnership(mqttConnectOwnershipInput{Load: mqttAuthorityLoad{status: mqttOutboundAbsent}},
		"t", b, now, exec)
	if err != nil {
		t.Fatalf("driving: %v", err)
	}
	if res.Outcome != mqttOwnershipExpired || !res.Disposition.AbandonOwn {
		t.Fatalf("an ABORTED verdict read on the exhausting reload must be disposed ABORTED, got %+v", res)
	}
}

// TestF105DriverCarriesAnObservedVerdictThroughAnExecutorError: a verdict the executor
// established before failing at the deadline is disposed by that verdict, not as UNRESOLVED.
// Mutation M20.
func TestF105DriverCarriesAnObservedVerdictThroughAnExecutorError(t *testing.T) {
	b, _ := mqttNewOwnershipBudget(time.Now(), time.Hour)
	obs := &mqttOwnObservation{}
	exec := func(mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
		obs.noteOwn(mqttOwnershipProposal{Kind: mqttProposalCreation, ID: "own"}, mqttAuthorityReserving)
		obs.noteVerdict(mqttMetadataAborted)
		return mqttConnectOwnershipInput{}, errMQTTOwnershipDeadline
	}
	res, err := mqttDriveOwnershipRun(mqttConnectOwnershipInput{Load: mqttAuthorityLoad{status: mqttOutboundAbsent}},
		"t", b, time.Now, exec, nil, obs, false)
	if err != nil {
		t.Fatalf("an executor cut off by the deadline is an expiry, not an error: %v", err)
	}
	if res.Outcome != mqttOwnershipExpired || !res.Disposition.AbandonOwn || res.Obs.Own == nil {
		t.Fatalf("the observed ABORTED verdict must survive the executor error, got %+v", res)
	}
}

// TestF105ForeignCloseDoesNotMakeTheAcceptanceALoser: closing a FOREIGN aborted reservation is
// a settlement act, after which the acceptance reserves over the closure. Mutation M6.
func TestF105ForeignCloseDoesNotMakeTheAcceptanceALoser(t *testing.T) {
	b, _ := mqttNewOwnershipBudget(time.Now(), time.Hour)
	gf := mqttNewSessGen()
	foreign := &mqttOutboundAuthority{State: mqttAuthorityReserving, Gen: gf, Tenure: "tf",
		Creation: &mqttCreationIntent{ID: "foreign", Gen: gf, Tenure: "tf"}}
	var acts []mqttConnectOwnershipAction
	exec := func(s mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
		acts = append(acts, s.Action)
		switch s.Action {
		case mqttOwnershipRecoverForeign:
			return mqttConnectOwnershipInput{Load: mqttAuthorityLoad{status: mqttOutboundValid, rec: foreign},
				Recovering: s.Recovery, StandingVerdict: mqttMetadataAborted}, nil
		case mqttOwnershipCloseAbandoned:
			return mqttConnectOwnershipInput{Load: mqttAuthorityLoad{status: mqttOutboundValid,
				rec: &mqttOutboundAuthority{State: mqttAuthorityClosed, Gen: gf}}}, nil
		}
		return mqttConnectOwnershipInput{}, errors.New("stop")
	}
	_, _ = mqttDriveOwnership(mqttConnectOwnershipInput{Load: mqttAuthorityLoad{status: mqttOutboundValid, rec: foreign}},
		"t", b, time.Now, exec)
	want := []mqttConnectOwnershipAction{mqttOwnershipRecoverForeign, mqttOwnershipCloseAbandoned, mqttOwnershipReserveOverClosed}
	if len(acts) < len(want) {
		t.Fatalf("the acceptance stopped after %v: closing a foreign reservation must not make it a loser", acts)
	}
	for i, a := range want {
		if acts[i] != a {
			t.Fatalf("acts %v, want the prefix %v", acts, want)
		}
	}
}

// TestCloseConnectionNeverRunsUnderWriteLock: certification crosses the writer barrier by
// taking wmu, so a caller holding wmu would deadlock. No function that takes wmu may reach
// closeConnection or mqttCertifyClosed directly (T14).
func TestCloseConnectionNeverRunsUnderWriteLock(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, rerr := os.ReadFile(f)
		if rerr != nil {
			t.Fatal(rerr)
		}
		file, perr := parser.ParseFile(fset, f, src, 0)
		if perr != nil {
			t.Fatal(perr)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name == "mqttCrossWriterBarrier" {
				continue
			}
			var locks, closes bool
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					if fun.Sel.Name == "Lock" {
						if inner, ok := fun.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "wmu" {
							locks = true
						}
					}
					if fun.Sel.Name == "closeConnection" {
						closes = true
					}
				case *ast.Ident:
					if fun.Name == "mqttCertifyClosed" {
						closes = true
					}
				}
				return true
			})
			if locks && closes {
				t.Errorf("%s: %s takes wmu and reaches certification, which takes wmu again",
					fset.Position(fn.Pos()), fn.Name.Name)
			}
		}
	}
}

// TestF105DisposalPhasesOnlyAppend: every disposing arm of mqttProcessConnect hands its Will
// and its close to the CONNECT's defers, and since F-105 8a one CONNECT can run more than one
// disposal (a stale resident at its top, then a disposing arm later). So the deferred phases
// may only be APPENDED to, never replaced — a replaced phase is a Will that is never
// discharged, already extracted from an object no teardown will find again (H-5). The one
// writer is registerDisposalPhases; a SOURCE guard, as for F-084, because the property is
// where the assignments sit, not what any single run observes. Mutation M35.
func TestF105DisposalPhasesOnlyAppend(t *testing.T) {
	body, err := os.ReadFile("mqtt.go")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(body), "\n")
	start, end := -1, -1
	for i, line := range lines {
		if strings.HasPrefix(line, "func (s *Server) mqttProcessConnect(") {
			start = i
			continue
		}
		if start >= 0 && i > start && strings.HasPrefix(line, "func ") {
			end = i
			break
		}
	}
	if start < 0 || end < 0 {
		t.Fatal("mqttProcessConnect not found; this guard cannot check what it claims to")
	}
	regStart, regEnd := -1, -1
	for i := start; i < end; i++ {
		if strings.Contains(lines[i], "registerDisposalPhases := func(") {
			regStart = i
		}
		if regStart >= 0 && regEnd < 0 && i > regStart && lines[i] == "\t}" {
			regEnd = i
		}
	}
	if regStart < 0 || regEnd < 0 {
		t.Fatal("registerDisposalPhases not found in mqttProcessConnect")
	}
	var stray []string
	for i := start; i < end; i++ {
		code := lines[i]
		if idx := strings.Index(code, "//"); idx >= 0 {
			code = code[:idx]
		}
		if !strings.Contains(code, "connWillPhase =") && !strings.Contains(code, "connTail =") {
			continue
		}
		if i > regStart && i < regEnd {
			continue
		}
		stray = append(stray, fmt.Sprintf("mqtt.go:%d: %s", i+1, strings.TrimSpace(lines[i])))
	}
	if len(stray) != 0 {
		t.Fatalf("the CONNECT's deferred disposal phases may only be appended to, through "+
			"registerDisposalPhases; these assign them directly and can drop an earlier "+
			"disposal's Will or close:\n%s", strings.Join(stray, "\n"))
	}
}
