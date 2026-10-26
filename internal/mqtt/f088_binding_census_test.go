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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// T13c (F-088 WP2, plan rev 20, "Binding removal census"): EVERY removal of an ACK-only binding
// states the proof it rests on, and every reader that dispatches on the binding's ROLE is known.
//
// A binding is removed only through removeAbandonedAckBindingLocked, which dispatches the proof on
// the role (an ending suffices for an AckRoute and an ordinary HandBack; a shared HandBack needs
// ended AND deleted; HANDOFF is AckRoute-only), and that boundary alone reaches the exact
// compare-delete. A generation discard drops the whole index. A new call site, a new proof
// argument, a new direct write to the index or a new role reader fails this census until it is
// classified here, with the behavioural tests that back its class.

// f088BindingReason is the fact a binding write or removal rests on.
type f088BindingReason string

const (
	// The binding's own exchange positively ENDED, matched by its role's matcher (endedBy).
	f088BindingEndedObserved f088BindingReason = "ended-observed"
	// The PUBREL intent resolver positively found the exchange Ended, or displaced (Other).
	f088BindingIntentEndedOrOther f088BindingReason = "pubrel-intent-ended-or-other"
	// The terminal-source continuation: Ended, EndedAndDeleted, or Discard (its proof variable
	// takes only those three constants).
	f088BindingTerminalSource f088BindingReason = "terminal-source"
	// Recovery restored an ACTING entry of the same allocation, which now owns its ACK.
	f088BindingRecoveryHandoff f088BindingReason = "recovery-handoff"
	// The role-dispatched removal boundary reaching the exact compare-delete.
	f088BindingBoundary f088BindingReason = "boundary"
	// The exact compare-delete itself.
	f088BindingCompareDelete f088BindingReason = "compare-delete"
	// The install boundary: a fresh install, or a HandBack upgrading its own allocation's
	// AckRoute (REPLACED). Nothing else is overwritten.
	f088BindingInstallOrReplaced f088BindingReason = "install-or-replaced"
	// The generation was DISCARDED: its bindings are unreachable from the fresh generation.
	f088BindingDiscard f088BindingReason = "discard"
	// The binding's lineage was RECLAIMED behind the F-112 floor: ended AND its register positively
	// gone, the strongest proof a binding has (classifyExchangeAbsence).
	f088BindingReclaimed f088BindingReason = "reclaimed"
)

// f088BindingBacking names the behavioural tests that witness each reason. Each must exist.
var f088BindingBacking = map[f088BindingReason][]string{
	f088BindingEndedObserved:      {"TestF088AckRouteEndsByLineage", "TestF088UntrackedTransmittedDeliveryLatePubAckEndsTheExchange"},
	f088BindingIntentEndedOrOther: {"TestUnitCAbandonedIntentClassification"},
	f088BindingTerminalSource: {"TestUnitCSourceCompletionPreservesUnansweredBinding",
		"TestSharedTerminalOwnerCapturedBeforeTheBindingReleasesTheHandBack"},
	f088BindingRecoveryHandoff:   {"TestF088RecoveryHandoffRetiresTheAckRoute"},
	f088BindingBoundary:          {"TestF088TerminalHandBackStillReconstructs"},
	f088BindingCompareDelete:     {"TestF088TerminalHandBackStillReconstructs"},
	f088BindingInstallOrReplaced: {"TestF088AbandonmentUpgradesAnAckRoute", "TestF088BindingRefusesAnotherGeneration"},
	f088BindingDiscard:           {"TestUnitCAbandonedAckBindingsGoWithTheGeneration"},
	f088BindingReclaimed:         {"TestF112AReclaimedLineageReleasesItsBindingAndItsCustody"},
}

// f088BindingSite is one discovered write or removal: its enclosing function and its form.
type f088BindingSite struct {
	fn   string
	what string
}

// f088FrozenBindingSites is the FROZEN census of every binding removal (by the proof argument
// passed) and every direct write to sess.abandonedAcks, by enclosing function. Frozen against the
// F-097/F-088 working tree of 2026-10-08 (base HEAD 660a82e plus the uncommitted WP2/WP3 edits).
var f088FrozenBindingSites = map[f088BindingSite]struct {
	n      int
	reason f088BindingReason
}{
	{"observeAbandonedAckEndLocked", "remove:mqttBindingReleaseEnded"}:           {1, f088BindingEndedObserved},
	{"finishAbandonedAckResolution", "remove:mqttBindingReleaseEnded"}:           {1, f088BindingIntentEndedOrOther},
	{"finishTerminalSource", "remove:proof"}:                                     {1, f088BindingTerminalSource},
	{"recoverInflightRegistersOnce", "remove:mqttBindingReleaseHandoff"}:         {1, f088BindingRecoveryHandoff},
	{"removeAbandonedAckBindingLocked", "exact"}:                                 {1, f088BindingBoundary},
	{"removeAbandonedAckLocked", "delete"}:                                       {1, f088BindingCompareDelete},
	{"installAbandonedAckLocked", "assign:index"}:                                {1, f088BindingInstallOrReplaced},
	{"installAbandonedAckLocked", "assign:make"}:                                 {1, f088BindingInstallOrReplaced},
	{"resetForReuseLocked", "assign:nil"}:                                        {1, f088BindingDiscard},
	{"acceptReclaimedLineageLocked", "remove:mqttBindingReleaseEndedAndDeleted"}: {1, f088BindingReclaimed},
}

// f088FrozenRoleReaders is every production function that reads a binding's ROLE (a `.role`
// selector, or a `.handBack()` call), with the count. Each dispatches the shared index on the role
// the plan names for it (WP2 "Binding roles").
var f088FrozenRoleReaders = map[string]struct {
	n   int
	why string
}{
	"handBack":                        {1, "the role predicate itself"},
	"installAbandonedAckLocked":       {3, "ended-lineage veto for AckRoute only; HandBack-over-AckRoute upgrade of the same allocation only"},
	"removeAbandonedAckBindingLocked": {2, "proof switch: a shared HandBack needs ended AND deleted; HANDOFF is AckRoute-only"},
	"endedBy":                         {1, "ending matcher: HandBack by the abandonment fact, AckRoute by lineage"},
	"sourceHandBackHeldLocked":        {1, "the source-ACK gate counts HandBack only"},
	"bindAbandonedAckOrigin":          {1, "a captured HandBack is re-validated by acquisition, never rebuilt"},
	"mqttAckSnapshot":                 {1, "the completion snapshot's observed state follows the role"},
	"resolvePubRelIntent":             {1, "an AckRoute at publish is still owed (StillOwed/Other by acquisition); a HandBack keeps Unresolved"},
	"recoverInflightRegistersOnce":    {1, "recovery's acting restore retires a same-allocation AckRoute (HANDOFF)"},
}

// f088ScanBindings scans the production sources.
func f088ScanBindings(t *testing.T) (sites map[f088BindingSite]int, readers map[string]int, files map[string]*ast.File) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	sites, readers, files = map[f088BindingSite]int{}, map[string]int{}, map[string]*ast.File{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", name, perr)
		}
		files[name] = f
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			fn := fd.Name.Name
			// The LHS selectors of assignments are writes, not reads.
			lhs := map[ast.Expr]bool{}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if as, ok := n.(*ast.AssignStmt); ok {
					for _, l := range as.Lhs {
						lhs[l] = true
						if ix, ok := l.(*ast.IndexExpr); ok {
							if sel, ok := ix.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "abandonedAcks" {
								sites[f088BindingSite{fn, "assign:index"}]++
							}
						}
						if sel, ok := l.(*ast.SelectorExpr); ok && sel.Sel.Name == "abandonedAcks" {
							form := "assign:expr"
							if len(as.Rhs) == len(as.Lhs) {
								switch r := as.Rhs[0].(type) {
								case *ast.Ident:
									if r.Name == "nil" {
										form = "assign:nil"
									}
								case *ast.CallExpr:
									if id, ok := r.Fun.(*ast.Ident); ok && id.Name == "make" {
										form = "assign:make"
									}
								}
							}
							sites[f088BindingSite{fn, form}]++
						}
					}
				}
				return true
			})
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.SelectorExpr:
					if x.Sel.Name == "role" && !lhs[x] {
						readers[fn]++
					}
				case *ast.CallExpr:
					switch fun := x.Fun.(type) {
					case *ast.Ident:
						if fun.Name == "delete" && len(x.Args) == 2 {
							if sel, ok := x.Args[0].(*ast.SelectorExpr); ok && sel.Sel.Name == "abandonedAcks" {
								sites[f088BindingSite{fn, "delete"}]++
							}
						}
					case *ast.SelectorExpr:
						switch fun.Sel.Name {
						case "handBack":
							readers[fn]++
						case "removeAbandonedAckBindingLocked":
							proof := "expr"
							if len(x.Args) == 2 {
								if id, ok := x.Args[1].(*ast.Ident); ok {
									proof = id.Name
								}
							}
							sites[f088BindingSite{fn, "remove:" + proof}]++
						case "removeAbandonedAckLocked":
							sites[f088BindingSite{fn, "exact"}]++
						}
					}
				}
				return true
			})
		}
	}
	return sites, readers, files
}

// TestF088EveryBindingRemovalHasAReason is T13c.
func TestF088EveryBindingRemovalHasAReason(t *testing.T) {
	t.Parallel()
	sites, readers, files := f088ScanBindings(t)
	var diffs []string
	for s, n := range sites {
		if want, ok := f088FrozenBindingSites[s]; !ok || want.n != n {
			diffs = append(diffs, s.fn+" / "+s.what+": source has "+itoa(n)+", frozen has "+itoa(want.n))
		}
	}
	for s, want := range f088FrozenBindingSites {
		if _, ok := sites[s]; !ok {
			diffs = append(diffs, s.fn+" / "+s.what+": frozen with "+itoa(want.n)+" but gone from the source (stale row)")
		}
	}
	for fn, n := range readers {
		if want, ok := f088FrozenRoleReaders[fn]; !ok || want.n != n {
			diffs = append(diffs, "role reader "+fn+": source has "+itoa(n)+", frozen has "+itoa(want.n))
		}
	}
	for fn, want := range f088FrozenRoleReaders {
		if _, ok := readers[fn]; !ok {
			diffs = append(diffs, "role reader "+fn+": frozen with "+itoa(want.n)+" but gone from the source (stale row)")
		}
	}
	sort.Strings(diffs)
	for _, d := range diffs {
		t.Errorf("binding census: %s", d)
	}

	// finishTerminalSource's proof is a VARIABLE: it may only ever hold the three proofs its
	// continuation can establish (an ending, an ending plus a proven delete, a discard).
	allowed := map[string]bool{"mqttBindingReleaseEnded": true, "mqttBindingReleaseEndedAndDeleted": true,
		"mqttBindingReleaseDiscard": true}
	var proofValues []string
	if fd := f088FindFunc(files, "finishTerminalSource"); fd == nil {
		t.Error("finishTerminalSource is gone from the source")
	} else {
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, l := range as.Lhs {
				if id, ok := l.(*ast.Ident); ok && id.Name == "proof" && i < len(as.Rhs) {
					v := "expr"
					if r, ok := as.Rhs[i].(*ast.Ident); ok {
						v = r.Name
					}
					proofValues = append(proofValues, v)
					if !allowed[v] {
						t.Errorf("finishTerminalSource assigns proof = %s, outside its three established proofs", v)
					}
				}
			}
			return true
		})
		if len(proofValues) == 0 {
			t.Error("finishTerminalSource no longer assigns its proof variable: re-classify its removal")
		}
	}

	// observeAbandonedAckEndLocked dispatches through the role's matcher ALONE: the pre-F-088
	// first-line `x.Abandoned == nil` guard made it HandBack-only and stranded every AckRoute.
	if fd := f088FindFunc(files, "observeAbandonedAckEndLocked"); fd == nil {
		t.Error("observeAbandonedAckEndLocked is gone from the source")
	} else {
		callsEndedBy, readsAbandoned := false, false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				if x.Sel.Name == "Abandoned" {
					readsAbandoned = true
				}
				if x.Sel.Name == "endedBy" {
					callsEndedBy = true
				}
			}
			return true
		})
		if !callsEndedBy || readsAbandoned {
			t.Errorf("observeAbandonedAckEndLocked must dispatch on endedBy alone (calls endedBy=%v, reads .Abandoned=%v)",
				callsEndedBy, readsAbandoned)
		}
	}

	exists := f088TestNames(t)
	for reason, tests := range f088BindingBacking {
		for _, name := range tests {
			if !exists[name] {
				t.Errorf("reason %s is backed by %s, which does not exist", reason, name)
			}
		}
	}
	for _, row := range f088FrozenBindingSites {
		if len(f088BindingBacking[row.reason]) == 0 {
			t.Errorf("reason %s has no behavioural backing", row.reason)
		}
	}
}

// f088FindFunc returns the production function (or method) declaration named name.
func f088FindFunc(files map[string]*ast.File, name string) *ast.FuncDecl {
	for _, f := range files {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil && fd.Name.Name == name {
				return fd
			}
		}
	}
	return nil
}
