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
// plan-7b §3.3-§3.4). Every licence that production code PASSES or STORES — an argument of type
// mqttDestroyLicence, a field of that type in a composite literal (keyed or positional), or an
// assignment to such a field — is one of: mqttLicenceOf(x) where x is a *client parameter or
// receiver of the enclosing function, or a delivery callback's `cc := sub.client` (the connection
// whose decision the call executes); a licence threaded in as a parameter; a licence captured on a
// value, i.e. read from a field of that type, whose own writes this same rule covers; any of these
// through withAdmission, which adds an admission and keeps its receiver's provenance; or the zero
// licence, explicit or omitted, which authorizes nothing. Never mqttLicenceOf(sess.c) or
// sess.boundEnt — the session's CURRENT connection, which after a rebind is the successor — and
// never a licence minted from an entitlement read afresh (mqttEntitlementLicence has no production
// caller outside its definition). A stored licence is checked where it is stored, because the
// record outlives the call that built it (F-097 batch 3, R1/R4: a continuity or withdrawOwed record
// holding the session's bound entitlement was invisible to an argument-only scan).
//
// Mutation M-provenance: any call site's licence taken from sess.c -> this test names it.
// Mutations R1/R4: a stored record's licence taken from sess.boundEnt -> this test names it.
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
	isLicenceType := func(e ast.Expr) bool {
		id, ok := e.(*ast.Ident)
		return ok && id.Name == "mqttDestroyLicence"
	}
	// Pass 1a: every struct type's fields, in declaration order, and which of them hold a
	// licence. Discovered by TYPE, so a new carrier is covered the day it is declared.
	type structShape struct {
		fields []string // one per field position, as a positional literal counts them
		lic    map[string]bool
	}
	carriers := map[string]structShape{}
	licFields := map[string]bool{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				return true
			}
			sh := structShape{lic: map[string]bool{}}
			for _, fl := range st.Fields.List {
				names := fl.Names
				if len(names) == 0 { // embedded: one position, no licence of its own here
					sh.fields = append(sh.fields, "")
					continue
				}
				for _, nm := range names {
					sh.fields = append(sh.fields, nm.Name)
					if isLicenceType(fl.Type) {
						sh.lic[nm.Name] = true
						licFields[nm.Name] = true
					}
				}
			}
			if len(sh.lic) > 0 {
				carriers[ts.Name.Name] = sh
			}
			return true
		})
	}
	// The five reviewed carriers at the time of writing (deferred disconnect, withdraw request,
	// re-materialisation, withdrawOwed, admission continuity); fewer means discovery broke.
	if len(carriers) < 5 {
		t.Fatalf("the scan found only %d struct types holding a licence: it is not reaching them", len(carriers))
	}
	// Pass 1b: every function's parameter positions of type mqttDestroyLicence.
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
				if isLicenceType(p.Type) {
					for k := 0; k < n; k++ {
						licParams[fd.Name.Name] = append(licParams[fd.Name.Name], i+k)
					}
				}
				i += n
			}
		}
	}
	var sites, stored int
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
					if isLicenceType(p.Type) {
						licenceParams[nm.Name] = true
					}
				}
			}
			// deciding reports whether e is a licence of the deciding connection's provenance.
			var deciding func(e ast.Expr) bool
			deciding = func(e ast.Expr) bool {
				switch a := e.(type) {
				case *ast.ParenExpr:
					return deciding(a.X)
				case *ast.CompositeLit:
					return isLicenceType(a.Type) && len(a.Elts) == 0 // the zero licence authorizes nothing
				case *ast.Ident:
					return licenceParams[a.Name] // threaded from the decision's caller
				case *ast.SelectorExpr:
					// Captured on a value: a field of licence type, whose writes this rule covers.
					return licFields[a.Sel.Name]
				case *ast.CallExpr:
					if sel, ok := a.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "withAdmission" {
						// withAdmission only adds the background admission (F-105 7c, consult
						// 105); the provenance is the licence it is called on.
						return deciding(sel.X)
					}
					if id, ok := a.Fun.(*ast.Ident); ok && id.Name == "mqttLicenceOf" && len(a.Args) == 1 {
						c, ok := a.Args[0].(*ast.Ident)
						return ok && clientParams[c.Name] // the enclosing function's own connection
					}
				}
				return false
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.CallExpr:
					var name string
					switch fn := n.Fun.(type) {
					case *ast.SelectorExpr:
						name = fn.Sel.Name
					case *ast.Ident:
						name = fn.Name
					}
					if name == "mqttEntitlementLicence" {
						t.Errorf("%s: %s mints a licence from an entitlement; a licence is the deciding connection's (mqttLicenceOf)",
							fset.Position(n.Pos()), fd.Name.Name)
					}
					for _, pos := range licParams[name] {
						if pos >= len(n.Args) {
							continue
						}
						sites++
						if arg := n.Args[pos]; !deciding(arg) {
							t.Errorf("%s: %s passes a licence to %s whose provenance is not the deciding connection",
								fset.Position(arg.Pos()), fd.Name.Name, name)
						}
					}
				case *ast.CompositeLit:
					id, ok := n.Type.(*ast.Ident)
					if !ok {
						return true
					}
					sh, ok := carriers[id.Name]
					if !ok {
						return true
					}
					for i, el := range n.Elts {
						field, val := "", el
						if kv, ok := el.(*ast.KeyValueExpr); ok {
							if k, ok := kv.Key.(*ast.Ident); ok {
								field = k.Name
							}
							val = kv.Value
						} else if i < len(sh.fields) {
							field = sh.fields[i] // positional: the field at that position
						}
						if !sh.lic[field] {
							continue
						}
						stored++
						if !deciding(val) {
							t.Errorf("%s: %s stores a licence in %s.%s whose provenance is not the deciding connection",
								fset.Position(val.Pos()), fd.Name.Name, id.Name, field)
						}
					}
					// An omitted licence field is the zero licence: no authority, nothing to check.
				case *ast.AssignStmt:
					if len(n.Lhs) != len(n.Rhs) {
						return true
					}
					for i, lhs := range n.Lhs {
						sel, ok := lhs.(*ast.SelectorExpr)
						if !ok || !licFields[sel.Sel.Name] {
							continue
						}
						stored++
						if !deciding(n.Rhs[i]) {
							t.Errorf("%s: %s assigns a licence to .%s whose provenance is not the deciding connection",
								fset.Position(n.Rhs[i].Pos()), fd.Name.Name, sel.Sel.Name)
						}
					}
				}
				return true
			})
		}
	}
	if sites < 20 {
		t.Fatalf("the scan found only %d licence-taking call sites: it is not reaching them", sites)
	}
	// The five reviewed stores (deferred disconnect, withdraw request, re-materialisation,
	// withdrawOwed, admission continuity); fewer means the literal scan is not reaching them.
	if stored < 5 {
		t.Fatalf("the scan found only %d stored licences: it is not reaching them", stored)
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
