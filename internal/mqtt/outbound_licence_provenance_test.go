// Copyright 2025 The mqtt-nats-bridge Authors
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
	"strings"
	"testing"
)

// T18: THE ENTITLEMENT A DESTRUCTIVE DECISION CARRIES IS THE DECIDING CONNECTION'S (F-105 7b,
// plan-7b §3.3-§3.4). Every argument of type mqttDestroyLicence passed in production code is one
// of: mqttLicenceOf(x) where x is a *client parameter or receiver of the enclosing function, or a
// delivery callback's `cc := sub.client` (the connection whose decision the call executes); a licence threaded in as a parameter, or captured on a
// value (a parked action's lic); or the zero licence, which authorizes nothing. Never
// mqttLicenceOf(sess.c) — the session's CURRENT connection, which after a rebind is the
// successor — and never a licence minted from an entitlement read afresh
// (mqttEntitlementLicence has no production caller outside its definition).
//
// Mutation M-provenance: any call site's licence taken from sess.c -> this test names it.
func TestF105LicenceProvenanceIsTheDecidingConnection(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, de := range entries {
		n := de.Name()
		if !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, n, nil, 0)
		if perr != nil {
			t.Fatal(perr)
		}
		files = append(files, f)
	}
	// Pass 1: every function's parameter positions of type mqttDestroyLicence.
	licParams := map[string][]int{}
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Type.Params == nil {
				continue
			}
			i := 0
			for _, p := range fd.Type.Params.List {
				n := len(p.Names)
				if n == 0 {
					n = 1
				}
				if id, ok := p.Type.(*ast.Ident); ok && id.Name == "mqttDestroyLicence" {
					for k := 0; k < n; k++ {
						licParams[fd.Name.Name] = append(licParams[fd.Name.Name], i+k)
					}
				}
				i += n
			}
		}
	}
	var sites int
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			clientParams := map[string]bool{}
			licenceParams := map[string]bool{}
			// The receiver, when the method is the connection's own.
			if fd.Recv != nil {
				for _, r := range fd.Recv.List {
					if st, ok := r.Type.(*ast.StarExpr); ok {
						if id, ok := st.X.(*ast.Ident); ok && id.Name == "client" {
							for _, nm := range r.Names {
								clientParams[nm.Name] = true
							}
						}
					}
				}
			}
			// A delivery callback's connection is its subscription's: `cc := sub.client`.
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				as, ok := n.(*ast.AssignStmt)
				if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
					return true
				}
				if sel, ok := as.Rhs[0].(*ast.SelectorExpr); ok && sel.Sel.Name == "client" {
					if x, ok := sel.X.(*ast.Ident); ok && x.Name == "sub" {
						if id, ok := as.Lhs[0].(*ast.Ident); ok {
							clientParams[id.Name] = true
						}
					}
				}
				return true
			})
			for _, p := range fd.Type.Params.List {
				for _, nm := range p.Names {
					if st, ok := p.Type.(*ast.StarExpr); ok {
						if id, ok := st.X.(*ast.Ident); ok && id.Name == "client" {
							clientParams[nm.Name] = true
						}
					}
					if id, ok := p.Type.(*ast.Ident); ok && id.Name == "mqttDestroyLicence" {
						licenceParams[nm.Name] = true
					}
				}
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				var name string
				switch fn := call.Fun.(type) {
				case *ast.SelectorExpr:
					name = fn.Sel.Name
				case *ast.Ident:
					name = fn.Name
				}
				if name == "mqttEntitlementLicence" {
					t.Errorf("%s: %s mints a licence from an entitlement; a licence is the deciding connection's (mqttLicenceOf)",
						fset.Position(call.Pos()), fd.Name.Name)
				}
				for _, pos := range licParams[name] {
					if pos >= len(call.Args) {
						continue
					}
					sites++
					arg := call.Args[pos]
					switch a := arg.(type) {
					case *ast.CompositeLit:
						if id, ok := a.Type.(*ast.Ident); ok && id.Name == "mqttDestroyLicence" && len(a.Elts) == 0 {
							continue // the zero licence authorizes nothing
						}
					case *ast.Ident:
						if licenceParams[a.Name] {
							continue // threaded from the decision's caller
						}
					case *ast.SelectorExpr:
						if a.Sel.Name == "lic" {
							continue // captured on a value (a parked action, a re-materialisation)
						}
					case *ast.CallExpr:
						if id, ok := a.Fun.(*ast.Ident); ok && id.Name == "mqttLicenceOf" && len(a.Args) == 1 {
							if c, ok := a.Args[0].(*ast.Ident); ok && clientParams[c.Name] {
								continue // the enclosing function's own connection
							}
						}
					}
					t.Errorf("%s: %s passes a licence to %s whose provenance is not the deciding connection",
						fset.Position(arg.Pos()), fd.Name.Name, name)
				}
				return true
			})
		}
	}
	if sites < 20 {
		t.Fatalf("the scan found only %d licence-taking call sites: it is not reaching them", sites)
	}
}

// T18, the verdict half: a NEW cancellation decision enters cancelledRegNonces only through
// stageCancellationVerdictLocked, which refuses a connection the session is no longer bound to.
// The one other writer is a REPLAY of verdicts already durable (mergeForkedGrantLocked); the
// record hydrations assign the whole set from the record and add nothing.
//
// Mutation M13b-census: a decision site calls voidRegNonceLocked directly -> this test names it.
func TestF105VerdictWritersAreTheStagingFunctionAndReplays(t *testing.T) {
	t.Parallel()
	allowed := map[string]bool{"stageCancellationVerdictLocked": true, "mergeForkedGrantLocked": true}
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var found int
	for _, de := range entries {
		n := de.Name()
		if !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, n, nil, 0)
		if perr != nil {
			t.Fatal(perr)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "voidRegNonceLocked" {
						found++
						if !allowed[fd.Name.Name] {
							t.Errorf("%s: %s records a cancellation verdict directly; a decision goes through stageCancellationVerdictLocked",
								fset.Position(call.Pos()), fd.Name.Name)
						}
					}
				}
				return true
			})
		}
	}
	if found == 0 {
		t.Fatal("the scan found no verdict writer at all: it is not reaching them")
	}
}

// T15 (M33): every install of a recovery-built entry is GATED. In recoverInflightRegistersOnce,
// each assignment of a mqttRecoveredPending result into
// pendingPublish or pendingPubRel sits under a custody gate call on that same entry — in an
// enclosing condition, or in an earlier statement of an enclosing block. The structural half
// of the invariant; the release-before-apply and routes tests are its behaviour.
//
// Mutation M33: one site installs without its gate -> this test names the site.
func TestF105RecoveryInstallsAreGated(t *testing.T) {
	t.Parallel()
	gates := map[string]bool{"recoveryRestoreRefusedLocked": true, "recoveryInstallRefusedLocked": true}
	fns := map[string]bool{"recoverInflightRegistersOnce": true}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "inflight_register.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	gatedIdent := func(n ast.Node, ident string) bool {
		found := false
		ast.Inspect(n, func(m ast.Node) bool {
			call, ok := m.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !gates[sel.Sel.Name] {
				return true
			}
			for _, a := range call.Args {
				if id, ok := a.(*ast.Ident); ok && id.Name == ident {
					found = true
				}
			}
			return true
		})
		return found
	}
	var installs int
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil || !fns[fd.Name.Name] {
			continue
		}
		built := map[string]bool{}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, rhs := range as.Rhs {
				if call, ok := rhs.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "mqttRecoveredPending" && i < len(as.Lhs) {
						if lid, ok := as.Lhs[i].(*ast.Ident); ok {
							built[lid.Name] = true
						}
					}
				}
			}
			return true
		})
		var stack []ast.Node
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return false
			}
			stack = append(stack, n)
			// An install is the one map writer called with a recovery-built entry.
			as, ok := n.(*ast.CallExpr)
			if !ok || len(as.Args) != 4 {
				return true
			}
			sel, ok := as.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "setPendingLocked" {
				return true
			}
			rid, ok := as.Args[3].(*ast.Ident)
			if !ok || !built[rid.Name] {
				return true
			}
			installs++
			gated := false
			for k := len(stack) - 2; k >= 0 && !gated; k-- {
				switch anc := stack[k].(type) {
				case *ast.IfStmt:
					gated = gatedIdent(anc.Cond, rid.Name)
				case *ast.CaseClause:
					for _, e := range anc.List {
						gated = gated || gatedIdent(e, rid.Name)
					}
					child := stack[k+1]
					for _, st := range anc.Body {
						if st == child {
							break
						}
						if gatedIdent(st, rid.Name) {
							gated = true
							break
						}
					}
				case *ast.BlockStmt:
					child := stack[k+1]
					for _, st := range anc.List {
						if st == child {
							break
						}
						if gatedIdent(st, rid.Name) {
							gated = true
							break
						}
					}
				}
			}
			if !gated {
				t.Errorf("%s: %s installs %s into %s without a custody gate on it",
					fset.Position(as.Pos()), fd.Name.Name, rid.Name, sel.Sel.Name)
			}
			return true
		})
	}
	// EXACTLY the three restore sites (restored PUBLISH, restored PUBREL, opaque-decided with
	// identity): a fourth is a new install the gate's review has not seen, and fewer means the
	// scan stopped reaching them.
	if installs != 3 {
		t.Fatalf("the scan found %d recovery installs, want the 3 reviewed restore sites", installs)
	}
}
