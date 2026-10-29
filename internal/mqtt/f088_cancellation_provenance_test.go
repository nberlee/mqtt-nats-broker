// Copyright 2025 The mqtt-nats-broker Authors
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

// F-088 / F-097, consult C4: a cancellation's provenance is WRITTEN, never inferred, and the
// pending cancellation fence is the one record disposed of by provenance alone. This file holds
// the structural census of every site that constructs or persists a cancellation (T32).

// testUntrackNoBinding is the removal reason fixtures written before F-088 WP2 use when they remove
// a PUBLISH entry directly: it installs nothing, which is exactly the removal those fixtures were
// written against (an untrack had no binding consequence then). A fixture that tests the AckRoute
// itself names mqttUntrackUnanswered explicitly.
const testUntrackNoBinding = mqttUntrackAnswered

// f088ProvSite is one discovered site: the enclosing function and what it does.
type f088ProvSite struct {
	fn   string
	what string
}

// f088ScanProvenance walks the package's PRODUCTION sources for every construction of a
// cancellation: each assignment or composite-literal key setting `Cancelled` or `CancelOrigin`,
// and the value it sets where that value is a literal or a named constant. It also records the
// call sites of the fence's persistence paths, so construction and persistence are one census.
func f088ScanProvenance(t *testing.T) (sites map[f088ProvSite]int, funcs map[string]*ast.FuncDecl, fset *token.FileSet) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	sites = map[f088ProvSite]int{}
	funcs = map[string]*ast.FuncDecl{}
	fset = token.NewFileSet()
	value := func(e ast.Expr) string {
		switch v := e.(type) {
		case *ast.Ident:
			return v.Name
		case *ast.SelectorExpr:
			return v.Sel.Name
		case *ast.BasicLit:
			return v.Value
		}
		return "expr"
	}
	persist := map[string]bool{
		"storeCancellationTombstone": true, "opTombstoneRegister": true, "storePendingFenceCAS": true,
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", name, perr)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			fn := fd.Name.Name
			funcs[fn] = fd
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					for i, lhs := range x.Lhs {
						sel, ok := lhs.(*ast.SelectorExpr)
						if !ok || (sel.Sel.Name != "Cancelled" && sel.Sel.Name != "CancelOrigin") {
							continue
						}
						v := "expr"
						if len(x.Rhs) == len(x.Lhs) {
							v = value(x.Rhs[i])
						}
						sites[f088ProvSite{fn: fn, what: sel.Sel.Name + "=" + v}]++
					}
				case *ast.KeyValueExpr:
					if k, ok := x.Key.(*ast.Ident); ok && (k.Name == "Cancelled" || k.Name == "CancelOrigin") {
						sites[f088ProvSite{fn: fn, what: k.Name + ":" + value(x.Value)}]++
					}
				case *ast.CallExpr:
					var callee string
					switch c := x.Fun.(type) {
					case *ast.Ident:
						callee = c.Name
					case *ast.SelectorExpr:
						callee = c.Sel.Name
					}
					if persist[callee] {
						sites[f088ProvSite{fn: fn, what: "call " + callee}]++
					}
				}
				return true
			})
		}
	}
	return sites, funcs, fset
}

// f088FrozenProvenance is the FROZEN close set (plan rule 7, C4 section 2), by branch. A new
// site — a new writer of either member, a new value, or a new caller of a fence persistence
// path — fails the census until it is given a disposition here, and a removed one fails as stale.
var f088FrozenProvenance = map[f088ProvSite]int{
	// THE ONE pending-fence producer: the create-only sparse body (pending-create), and its
	// conflict branch's rebuild over the landed same-nonce occupant (occupant).
	{fn: "markInflightRegisterCancelledPending", what: "Cancelled:true"}:                             1,
	{fn: "markInflightRegisterCancelledPending", what: "CancelOrigin:mqttCancelOriginPendingCreate"}: 1,
	{fn: "markInflightRegisterCancelledPending", what: "Cancelled=true"}:                             1,
	{fn: "markInflightRegisterCancelledPending", what: "CancelOrigin=mqttCancelOriginOccupant"}:      1,
	{fn: "markInflightRegisterCancelledPending", what: "call storeCancellationTombstone"}:            1,
	// The copy-occupant CAS fallback: always an occupant cancellation.
	{fn: "markInflightRegisterCancelled", what: "Cancelled=true"}:                        1,
	{fn: "markInflightRegisterCancelled", what: "CancelOrigin=mqttCancelOriginOccupant"}: 1,
	{fn: "markInflightRegisterCancelled", what: "call storeCancellationTombstone"}:       1,
	// The withdraw-unsent ending: an occupant cancellation beside the exchange's ending.
	{fn: "mqttWithdrawUnsentTransition", what: "Cancelled=true"}:                        1,
	{fn: "mqttWithdrawUnsentTransition", what: "CancelOrigin=mqttCancelOriginOccupant"}: 1,
	// The fence's two persistence paths, one per class (rule 2).
	{fn: "storeCancellationTombstone", what: "call storePendingFenceCAS"}: 1,
	{fn: "storeCancellationTombstone", what: "call opTombstoneRegister"}:  1,
	// THE BOUNDARY'S expected bodies (not writers): the tombstone-register and withdraw-unsent
	// rules compare the member against the occupant with an occupant cancellation set.
	{fn: "checkRegisterReplacements", what: "Cancelled=true"}:                        1,
	{fn: "checkRegisterReplacements", what: "CancelOrigin=mqttCancelOriginOccupant"}: 1,
	{fn: "checkWithdrawalRegister", what: "Cancelled=true"}:                          1,
	{fn: "checkWithdrawalRegister", what: "CancelOrigin=mqttCancelOriginOccupant"}:   1,
	// THE THREE CLEARING MEMBERS (rule 2): the ack re-phase (recogniser + builder) and the token
	// acquisition (recogniser + builder) of the refuted-cancellation clearance clear BOTH members.
	// The register-only replacement over an occupant cancellation (edit 7) clears them by writing
	// this allocation's own record, which carries neither, so it has no site of its own here.
	{fn: "mqttIsAckPhaseAdvance", what: "Cancelled=false"}:                 1,
	{fn: "mqttIsAckPhaseAdvance", what: "CancelOrigin=_EMPTY_"}:            1,
	{fn: "mqttAdvanceExchangeAckTransition", what: "Cancelled=false"}:      1,
	{fn: "mqttAdvanceExchangeAckTransition", what: "CancelOrigin=_EMPTY_"}: 1,
	{fn: "mqttIsActionTokenAcquisition", what: "Cancelled=false"}:          1,
	{fn: "mqttIsActionTokenAcquisition", what: "CancelOrigin=_EMPTY_"}:     1,
	{fn: "composeAcquisition", what: "Cancelled=false"}:                    1,
	{fn: "composeAcquisition", what: "CancelOrigin=_EMPTY_"}:               1,
}

// TestF088CancellationRegisterProducersAreFrozen is T32 (plan rule 7): the census of every
// construction and every persistence path of a cancellation, frozen, plus the structural
// guarantees the dedicated disposal rule leans on.
//
// Not a scan of functions NAMED "tombstone": that would repeat the original inventory error
// (the producer that writes the fence is not named for it).
func TestF088CancellationRegisterProducersAreFrozen(t *testing.T) {
	t.Parallel()
	sites, funcs, fset := f088ScanProvenance(t)

	// CONSTRUCTION AND PERSISTENCE: discovered == frozen, both directions, with multiplicity.
	var diffs []string
	for s, got := range sites {
		if want := f088FrozenProvenance[s]; want != got {
			diffs = append(diffs, s.fn+" / "+s.what+": source has "+itoa(got)+", frozen set has "+itoa(want))
		}
	}
	for s, want := range f088FrozenProvenance {
		if _, ok := sites[s]; !ok {
			diffs = append(diffs, s.fn+" / "+s.what+": frozen with "+itoa(want)+" but no longer in the source (stale row)")
		}
	}
	sort.Strings(diffs)
	for _, d := range diffs {
		t.Errorf("cancellation provenance census: %s", d)
	}

	// HEAD's tombstoneBeforeDestroy copy-and-CAS is DELETED (WP3 edit 2): it could preserve the
	// door's missing-exchange population as a cancelled record.
	if _, ok := funcs["tombstoneBeforeDestroy"]; ok {
		t.Error("tombstoneBeforeDestroy is back: the door's MISSING row must refuse, not tombstone")
	}

	// THE GENERIC SINKS REFUSE THE FENCE PROVENANCE: each states the refusal itself, so a body
	// carrying `pending-create` reaches the store only through the two fence paths.
	refuses := func(fn string, needle string) bool {
		fd := funcs[fn]
		if fd == nil {
			return false
		}
		found := false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				if id, ok := x.Fun.(*ast.Ident); ok && id.Name == needle {
					found = true
				}
			case *ast.Ident:
				if x.Name == needle {
					found = true
				}
			}
			return !found
		})
		return found
	}
	for _, fn := range []string{"storeInflightCAS", "storeInflightPair", "casInflightSlot"} {
		if !refuses(fn, "mqttRefuseGenericFenceWrite") {
			t.Errorf("%s does not refuse a pending-fence body: a generic sink could mint the provenance", fn)
		}
	}
	if !refuses("clearCertifiedActionToken", "mqttCancelOriginPendingCreate") {
		t.Error("clearCertifiedActionToken does not state the generic fence refusal")
	}
	if !refuses("storePendingFenceCAS", "mqttValidPendingFence") {
		t.Error("storePendingFenceCAS writes without validating the fence form")
	}

	// THE RECOVERY PARTITION DIVERTS FENCES FIRST: before the shared-record exchange validation
	// and before any affine claim pass sees a register.
	fd := funcs["recoverInflightRegistersOnce"]
	if fd == nil {
		t.Fatal("recoverInflightRegistersOnce is gone")
	}
	first := map[string]token.Pos{}
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok {
			var name string
			switch f := c.Fun.(type) {
			case *ast.Ident:
				name = f.Name
			case *ast.SelectorExpr:
				name = f.Sel.Name
			}
			if _, seen := first[name]; !seen && name != _EMPTY_ {
				first[name] = c.Pos()
			}
		}
		return true
	})
	divert := first["mqttValidPendingFence"]
	if divert == token.NoPos {
		t.Fatal("recovery no longer identifies pending fences")
	}
	for _, later := range []string{"sharedRecordUnsupported", "claimRecoveredTransferableInflight",
		"claimRecoveredRetiredPubRelInflight"} {
		p := first[later]
		if p == token.NoPos {
			t.Errorf("recovery no longer calls %s: re-derive this ordering assertion", later)
			continue
		}
		if p < divert {
			t.Errorf("recovery calls %s (%s) before it diverts pending fences (%s)", later,
				fset.Position(p), fset.Position(divert))
		}
	}
}
