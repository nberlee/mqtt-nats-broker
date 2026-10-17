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

//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The structural half of the pending-entry closure (F-105 cutover checkpoint; plan §5).
//
// The property: an entry in pendingPublish or pendingPubRel may ACT only if its kind says so, its
// kind and evidence are set only by a constructor, and both maps are written only by
// setPendingLocked, whose every call states its expectation. Behavioural tests exercise schedules;
// this test is what makes the property hold for the next writer too.
//
// # Why this check is TYPE-CHECKED when the package's other structural checks are syntactic
//
// Several mqttPending field names (qos, slot, seq) are also fields of other types, so a syntactic
// scan cannot tell a pending-entry write from an unrelated one, and a write through an alias — a
// local, a loop variable, a returned pointer — is only visible through the receiver's type. The
// engine therefore type-checks the package's production files against the compiler's export data
// (`go list -export`; the module carries no x/tools) and reads every assignment through go/types.
//
// # What it cannot see, stated so it is not mistaken for a proof
//
// Another package: impossible, the type is unexported. Semantics: a recorded mutation that
// changes meaning within its contract. A closure's writes are attributed to the enclosing
// function, which is where the census records them.

// pendingStructureConfig names the vocabulary the engine checks, so the self-test can run the
// same engine over a fixture package that declares the same shapes.
type pendingStructureConfig struct {
	pending, evidence, expect, session string
	writer                             string          // the one map writer (a method on session)
	constructors                       map[string]bool // functions that may assign kind / ev / identity
	producers                          map[string]bool // functions that may build an evidence literal
	expectCtors                        map[string]bool // the direct expectation constructors
	expectAny                          string
	identity                           map[string]bool // field paths only a constructor writes
	substate                           map[string]bool // Hold sub-state flags
}

// pendingStructureReport is what one scan found.
type pendingStructureReport struct {
	violations []string
	fieldWrite map[string]int // "func|field|class"
	escapes    map[string]int // "func|form"
	anyCalls   map[string]int // func
	acts       map[string]int // func -> references of acts() or a kind constant
	mapWrites  map[string]int // "func|map" for pending maps other than the writer's own
}

func (r *pendingStructureReport) violate(format string, args ...any) {
	r.violations = append(r.violations, fmt.Sprintf(format, args...))
}

// pendingStructureScan runs every rule over one type-checked package.
func pendingStructureScan(cfg pendingStructureConfig, fset *token.FileSet, files []*ast.File, info *types.Info, pkg *types.Package) *pendingStructureReport {
	rep := &pendingStructureReport{fieldWrite: map[string]int{}, escapes: map[string]int{},
		anyCalls: map[string]int{}, acts: map[string]int{}, mapWrites: map[string]int{}}
	lookup := func(name string) types.Type {
		if o := pkg.Scope().Lookup(name); o != nil {
			return o.Type()
		}
		return nil
	}
	pendT, evT, expT, sessT := lookup(cfg.pending), lookup(cfg.evidence), lookup(cfg.expect), lookup(cfg.session)
	if pendT == nil || evT == nil || expT == nil || sessT == nil {
		rep.violate("the vocabulary is incomplete: %s %s %s %s", cfg.pending, cfg.evidence, cfg.expect, cfg.session)
		return rep
	}
	deref := func(t types.Type) types.Type {
		if p, ok := t.(*types.Pointer); ok {
			return p.Elem()
		}
		return t
	}
	isPend := func(t types.Type) bool { return t != nil && types.Identical(deref(t), pendT) }
	isPendPtr := func(t types.Type) bool {
		p, ok := t.(*types.Pointer)
		return ok && types.Identical(p.Elem(), pendT)
	}
	isPendMap := func(t types.Type) bool {
		if t == nil {
			return false
		}
		m, ok := t.Underlying().(*types.Map)
		return ok && isPend(m.Elem())
	}
	isSessField := func(e ast.Expr) bool {
		sel, ok := e.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		s := info.Selections[sel]
		return s != nil && s.Kind() == types.FieldVal && types.Identical(deref(s.Recv()), sessT)
	}
	// pendingFieldPath is the pending-entry field an expression addresses ("slot.nonce" for a
	// nested write), or "" when it is not one.
	var pendingFieldPath func(e ast.Expr) (string, ast.Expr)
	pendingFieldPath = func(e ast.Expr) (string, ast.Expr) {
		for {
			switch x := e.(type) {
			case *ast.ParenExpr:
				e = x.X
				continue
			case *ast.IndexExpr:
				e = x.X
				continue
			case *ast.SelectorExpr:
				s := info.Selections[x]
				if s == nil || s.Kind() != types.FieldVal {
					return "", nil
				}
				if isPend(s.Recv()) {
					return x.Sel.Name, x.X
				}
				if outer, root := pendingFieldPath(x.X); outer != "" {
					return outer + "." + x.Sel.Name, root
				}
				return "", nil
			}
			return "", nil
		}
	}
	for _, f := range files {
		// The units scanned: every function body, AND every package-level initializer — a
		// variable initialized with a function literal is code too, and a closure written there
		// would otherwise construct or install an entry unseen.
		type scanUnit struct {
			name string
			body ast.Node
		}
		var units []scanUnit
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Body != nil {
					units = append(units, scanUnit{d.Name.Name, d.Body})
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					vs, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for i, v := range vs.Values {
						name := "package-level"
						if i < len(vs.Names) {
							name = vs.Names[i].Name
						}
						units = append(units, scanUnit{name, v})
					}
				}
			}
		}
		for _, unit := range units {
			fn := unit.name
			ctor := cfg.constructors[fn]
			// Locals of this function bound only to fresh values (a literal or a constructor's
			// result): a write through one is to a DETACHED value, not a resident entry.
			fresh := map[*types.Var]bool{}
			bound := map[*types.Var]bool{}
			isFreshRHS := func(e ast.Expr) bool {
				switch x := e.(type) {
				case *ast.UnaryExpr:
					if cl, ok := x.X.(*ast.CompositeLit); ok && x.Op == token.AND && isPend(info.TypeOf(cl)) {
						return true
					}
				case *ast.CompositeLit:
					return isPend(info.TypeOf(x))
				case *ast.CallExpr:
					if id, ok := x.Fun.(*ast.Ident); ok && cfg.constructors[id.Name] {
						return true
					}
				}
				return false
			}
			markBound := func(lhs ast.Expr, rhs ast.Expr) {
				id, ok := lhs.(*ast.Ident)
				if !ok {
					return
				}
				v, _ := info.ObjectOf(id).(*types.Var)
				if v == nil || !isPend(v.Type()) {
					return
				}
				if rhs != nil && isFreshRHS(rhs) {
					if !bound[v] {
						fresh[v] = true
					}
				} else {
					fresh[v] = false
				}
				bound[v] = true
			}
			ast.Inspect(unit.body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					for i, l := range x.Lhs {
						var r ast.Expr
						if len(x.Rhs) == len(x.Lhs) {
							r = x.Rhs[i]
						}
						markBound(l, r)
					}
				case *ast.ValueSpec:
					for i, name := range x.Names {
						var r ast.Expr
						if i < len(x.Values) {
							r = x.Values[i]
						}
						markBound(name, r)
					}
				case *ast.RangeStmt:
					if x.Value != nil {
						markBound(x.Value, nil)
					}
				}
				return true
			})
			classOf := func(root ast.Expr) string {
				if ctor {
					return "CONSTRUCTOR"
				}
				if id, ok := root.(*ast.Ident); ok {
					if v, _ := info.ObjectOf(id).(*types.Var); v != nil && fresh[v] {
						return "DETACHED"
					}
				}
				return "RESIDENT"
			}
			recordWrite := func(lhs ast.Expr, pos token.Pos) {
				path, root := pendingFieldPath(lhs)
				if path == "" {
					return
				}
				class := classOf(root)
				rep.fieldWrite[fn+"|"+path+"|"+class]++
				if cfg.identity[path] && class != "CONSTRUCTOR" {
					rep.violate("%s: identity field %s written outside a constructor (%s)", fn, path, fset.Position(pos))
				}
			}
			ast.Inspect(unit.body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					for i, l := range x.Lhs {
						if ix, ok := l.(*ast.IndexExpr); ok && isPendMap(info.TypeOf(ix.X)) {
							if fn != cfg.writer {
								if isSessField(ix.X) {
									rep.violate("%s: writes a session pending map outside %s (%s)", fn, cfg.writer, fset.Position(x.Pos()))
								} else {
									rep.mapWrites[fn+"|"+types.ExprString(ix.X)]++
								}
							}
							continue
						}
						if isSessField(l) && isPendMap(info.TypeOf(l)) && len(x.Rhs) == len(x.Lhs) {
							r := x.Rhs[i]
							okRHS := false
							if call, ok := r.(*ast.CallExpr); ok {
								if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "make" {
									okRHS = true
								}
							}
							if id, ok := r.(*ast.Ident); ok && id.Name == "nil" {
								okRHS = true
							}
							if !okRHS {
								rep.violate("%s: assigns a whole session pending map from %s (%s)", fn, types.ExprString(r), fset.Position(x.Pos()))
							}
							continue
						}
						if st, ok := l.(*ast.StarExpr); ok && isPend(info.TypeOf(st)) {
							rep.escapes[fn+"|WHOLEWRITE"]++
							continue
						}
						recordWrite(l, x.Pos())
						if len(x.Rhs) == len(x.Lhs) {
							if lt, rt := info.TypeOf(l), info.TypeOf(x.Rhs[i]); lt != nil && rt != nil &&
								types.IsInterface(lt) && (isPend(rt) || isPendMap(rt)) {
								rep.violate("%s: converts a pending value to an interface (%s)", fn, fset.Position(x.Pos()))
							}
						}
						if len(x.Rhs) == len(x.Lhs) {
							if t := info.TypeOf(x.Rhs[i]); t != nil && (isPendPtr(t) || isPendMap(t)) {
								if _, local := l.(*ast.Ident); !local {
									rep.escapes[fn+"|STORE "+types.ExprString(l)]++
								}
							}
						}
					}
				case *ast.IncDecStmt:
					recordWrite(x.X, x.Pos())
				case *ast.ValueSpec:
					for i, name := range x.Names {
						if i >= len(x.Values) {
							break
						}
						if lt, rt := info.TypeOf(name), info.TypeOf(x.Values[i]); lt != nil && rt != nil &&
							types.IsInterface(lt) && (isPend(rt) || isPendMap(rt)) {
							rep.violate("%s: converts a pending value to an interface (%s)", fn, fset.Position(x.Pos()))
						}
					}
				case *ast.UnaryExpr:
					if x.Op == token.AND {
						if path, _ := pendingFieldPath(x.X); path != "" {
							rep.violate("%s: takes the address of pending field %s (%s)", fn, path, fset.Position(x.Pos()))
						}
					}
				case *ast.StarExpr:
					if t := info.TypeOf(x); t != nil && isPend(t) && !isPendPtr(t) && info.Types[x].IsValue() {
						rep.escapes[fn+"|DEREFCOPY"]++
					}
				case *ast.CompositeLit:
					t := info.TypeOf(x)
					if t != nil && types.Identical(t, evT) && !cfg.producers[fn] {
						rep.violate("%s: builds an evidence literal outside a producer (%s)", fn, fset.Position(x.Pos()))
					}
					if t != nil && isPend(t) {
						for _, el := range x.Elts {
							kv, ok := el.(*ast.KeyValueExpr)
							if !ok {
								continue
							}
							key := types.ExprString(kv.Key)
							if (key == "kind" || key == "ev" || cfg.substate[key]) && !ctor {
								rep.violate("%s: a pending literal sets %s outside a constructor (%s)", fn, key, fset.Position(kv.Pos()))
							}
						}
					} else if t != nil {
						for _, el := range x.Elts {
							v := el
							if kv, ok := el.(*ast.KeyValueExpr); ok {
								v = kv.Value
							}
							if vt := info.TypeOf(v); vt != nil && (isPendPtr(vt) || isPendMap(vt)) {
								rep.escapes[fn+"|INLIT"]++
							}
						}
					}
				case *ast.ReturnStmt:
					for _, r := range x.Results {
						if t := info.TypeOf(r); t != nil && (isPendPtr(t) || isPendMap(t)) {
							rep.escapes[fn+"|RETURN"]++
						}
					}
				case *ast.SendStmt:
					if t := info.TypeOf(x.Value); t != nil && isPend(t) {
						rep.violate("%s: sends a pending entry on a channel (%s)", fn, fset.Position(x.Pos()))
					}
				case *ast.SelectorExpr:
					if s := info.Selections[x]; s != nil && s.Kind() == types.MethodVal {
						if x.Sel.Name == "acts" && isPend(s.Recv()) {
							rep.acts[fn]++
						}
						// A pointer-receiver method on an addressable pending FIELD mutates it
						// through an implicit address.
						if sig, ok := s.Obj().Type().(*types.Signature); ok && sig.Recv() != nil {
							_, fieldIsPtr := info.TypeOf(x.X).(*types.Pointer)
							if _, ptr := sig.Recv().Type().(*types.Pointer); ptr && !fieldIsPtr {
								if path, _ := pendingFieldPath(x.X); path != "" {
									rep.violate("%s: calls pointer method %s on pending field %s (%s)", fn, x.Sel.Name, path, fset.Position(x.Pos()))
								}
							}
						}
					}
				case *ast.Ident:
					if c, ok := info.Uses[x].(*types.Const); ok && strings.HasPrefix(c.Name(), "mqttPending") &&
						c.Pkg() == pkg && c.Name() != "mqttPendingHold" && c.Name() != "mqttPendingPublishMap" && c.Name() != "mqttPendingPubRelMap" {
						rep.acts[fn]++
					}
				case *ast.CallExpr:
					calleeName := ""
					var calleeObj types.Object
					switch f := x.Fun.(type) {
					case *ast.Ident:
						calleeName, calleeObj = f.Name, info.Uses[f]
					case *ast.SelectorExpr:
						calleeName, calleeObj = f.Sel.Name, info.Uses[f.Sel]
					}
					if calleeObj != nil && calleeObj.Pkg() != nil {
						if p := calleeObj.Pkg().Path(); p == "reflect" || p == "unsafe" {
							for _, a := range x.Args {
								if t := info.TypeOf(a); t != nil && (isPend(t) || isPendMap(t)) {
									rep.violate("%s: reaches a pending entry through %s (%s)", fn, p, fset.Position(x.Pos()))
								}
							}
						}
					}
					if calleeName == cfg.expectAny {
						rep.anyCalls[fn]++
					}
					if cfg.expectCtors[calleeName] {
						// Every expectation is built INSIDE a writer call's argument list.
						// (checked below from the writer's side; here only its placement)
					}
					if calleeName == cfg.writer {
						if cfg.constructors[fn] {
							rep.violate("%s: a constructor writes the map (%s)", fn, fset.Position(x.Pos()))
						}
						if len(x.Args) != 4 {
							rep.violate("%s: %s called with %d arguments", fn, cfg.writer, len(x.Args))
						} else {
							ec, ok := x.Args[2].(*ast.CallExpr)
							name := ""
							if ok {
								if id, ok := ec.Fun.(*ast.Ident); ok {
									name = id.Name
								}
							}
							if !cfg.expectCtors[name] {
								rep.violate("%s: %s's expectation is not a direct expectation call: %s (%s)",
									fn, cfg.writer, types.ExprString(x.Args[2]), fset.Position(x.Pos()))
							}
						}
					}
					for _, a := range x.Args {
						t := info.TypeOf(a)
						if t == nil || !(isPend(t) || isPendMap(t)) {
							continue
						}
						if calleeObj == nil {
							rep.violate("%s: passes a pending value to an unnamed function value (%s)", fn, fset.Position(x.Pos()))
							continue
						}
						if v, isVar := calleeObj.(*types.Var); isVar {
							// A function VALUE typed over the unexported pending type can only have
							// been built in this package, and its body is scanned where it is
							// written; it is recorded, by name, like any other escape.
							rep.escapes[fn+"|FUNCVALUE "+v.Name()]++
							continue
						}
						if calleeObj.Pkg() != nil && calleeObj.Pkg() != pkg && calleeObj.Pkg().Path() != "fmt" {
							rep.violate("%s: passes a pending value to another package's %s (%s)", fn, calleeName, fset.Position(x.Pos()))
						}
						if sig, ok := info.TypeOf(x.Fun).(*types.Signature); ok && sig.Params().Len() > 0 {
							last := sig.Params().At(sig.Params().Len() - 1).Type()
							if sl, ok := last.(*types.Slice); ok && sig.Variadic() {
								last = sl.Elem()
							}
							if _, iface := last.Underlying().(*types.Interface); iface && calleeObj.Pkg() != nil && calleeObj.Pkg().Path() != "fmt" {
								rep.violate("%s: converts a pending value to an interface for %s (%s)", fn, calleeName, fset.Position(x.Pos()))
							}
						}
						rep.escapes[fn+"|ARG "+calleeName]++
					}
				}
				return true
			})
			// Expectation constructors appear only as a writer argument (or inside another
			// expectation constructor): a variable or a wrapper would hide the mode.
			ast.Inspect(unit.body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := ""
				if id, ok := call.Fun.(*ast.Ident); ok {
					name = id.Name
				}
				if name == cfg.writer || isWriterSel(call, cfg.writer) {
					if len(call.Args) == 4 {
						// its expectation argument is sanctioned; skip visiting it as a stray.
						for i, a := range call.Args {
							if i != 2 {
								ast.Inspect(a, func(m ast.Node) bool { return checkStrayExpect(m, cfg, fn, fset, rep) })
							}
						}
						return false
					}
				}
				return checkStrayExpect(n, cfg, fn, fset, rep)
			})
		}
	}
	return rep
}

func isWriterSel(call *ast.CallExpr, writer string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == writer
}

func checkStrayExpect(n ast.Node, cfg pendingStructureConfig, fn string, fset *token.FileSet, rep *pendingStructureReport) bool {
	call, ok := n.(*ast.CallExpr)
	if !ok {
		return true
	}
	if id, ok := call.Fun.(*ast.Ident); ok && cfg.expectCtors[id.Name] && !cfg.expectCtors[fn] {
		rep.violate("%s: builds an expectation outside a %s argument (%s)", fn, cfg.writer, fset.Position(call.Pos()))
	}
	if isWriterSel(call, cfg.writer) {
		return false
	}
	return true
}

// pendingStructureLoad type-checks the package in dir against export data, failing closed on any
// load, parse or type error.
func pendingStructureLoad(t *testing.T, dir string) (*token.FileSet, []*ast.File, *types.Info, *types.Package) {
	t.Helper()
	cmd := exec.Command("go", "list", "-export", "-deps", "-json", ".")
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -export: %v", err)
	}
	type listPkg struct {
		ImportPath, Dir, Export string
		GoFiles                 []string
		Error                   *struct{ Err string }
	}
	dec := json.NewDecoder(strings.NewReader(string(out)))
	exports := map[string]string{}
	var target listPkg
	for {
		var p listPkg
		if err := dec.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decoding go list: %v", err)
		}
		if p.Error != nil {
			t.Fatalf("go list: %s: %s", p.ImportPath, p.Error.Err)
		}
		exports[p.ImportPath] = p.Export
		target = p // -deps lists the named package last
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, n := range target.GoFiles {
		f, err := parser.ParseFile(fset, filepath.Join(target.Dir, n), nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", n, err)
		}
		files = append(files, f)
	}
	imp := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		e, ok := exports[path]
		if !ok || e == "" {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(e)
	})
	pkg, info, err := pendingStructureCheck(fset, files, imp)
	if err != nil {
		t.Fatalf("type-checking the package failed, so no rule is evaluated: %v", err)
	}
	return fset, files, info, pkg
}

func pendingStructureCheck(fset *token.FileSet, files []*ast.File, imp types.Importer) (*types.Package, *types.Info, error) {
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	var errs []string
	conf := types.Config{Importer: imp, Error: func(err error) { errs = append(errs, err.Error()) }}
	pkg, _ := conf.Check("mqtt", fset, files, info)
	if len(errs) > 0 {
		return nil, nil, fmt.Errorf("%d type errors, first: %s", len(errs), errs[0])
	}
	return pkg, info, nil
}

func pendingStructureProductionConfig() pendingStructureConfig {
	set := func(names ...string) map[string]bool {
		m := map[string]bool{}
		for _, n := range names {
			m[n] = true
		}
		return m
	}
	return pendingStructureConfig{
		pending: "mqttPending", evidence: "mqttPendingEvidence", expect: "mqttPendingExpect", session: "mqttSession",
		writer: "setPendingLocked",
		constructors: set("mqttNewAllocation", "mqttNewUnmintedHold", "mqttNewCancelledHold",
			"mqttRecoveryOpaquePending", "mqttNewBoundOpaque", "mqttNewDemoted", "mqttNewPoisoned",
			"mqttNewPubRelPlaceholder", "mqttNewTerminalReservation", "mqttNewMarkerOpaque",
			"mqttPromoteLanded", "mqttRecoveredPending", "mqttReactivate", "mqttNewRetainedAllocation",
			"mqttPromoteRetainedLanded", "mqttRecoveredRetainedPending", "mqttWithRetainedReplay",
			"mqttPromoteRetainedPubRelAction", "mqttNewAbandonedRetainedPubRel",
			"mqttNewPubRelSuccessor", "mqttPromotePubRelAction", "mqttAckSnapshot", "mqttNewReleasedDelivery"),
		producers:   set("mqttEvidenceFromRecord", "mqttEvidenceFromLanding", "mqttEvidenceFromAction"),
		expectCtors: set("mqttExpectEmpty", "mqttExpectExactly", "mqttExpectCaptured", "mqttExpectAny"),
		expectAny:   "mqttExpectAny",
		identity:    set("kind", "ev", "regNonce", "slot", "slot.name", "slot.nonce", "allocFence", "demotedKind"),
		substate: set("regPending", "regCancelled", "regForeign", "markerOpaque", "regRecoveryOpaque",
			"regRecoveryDemoted", "regRecoveryPlaceholder", "terminalReconcile", "awaitingPubCompOwner"),
	}
}

// The recorded sites. Each table may neither grow nor shrink without this file changing: a new
// row is a new writer, escape or ANY that somebody must justify, and a stale row would hide the
// next one landing in the same function.

// pendingStructureAnySites: the only places the writer may overwrite whatever is resident.
var pendingStructureAnySites = map[string]int{
	"bindActionToPubRelLocked": 1, // validated the resident itself, under the same lock (§3.4)
	"retrackPublishLocked":     1, // puts back the exact entry its caller removed
}

// pendingStructureActs: the enforcement set (§3.7) and how many kind references each makes.
var pendingStructureActs = map[string]int{
	"beginSendLocked":            1,
	"trackPublish":               1,
	"mqttPendingRedeliverable":   1,
	"resendPendingRedelivery":    1,
	"mqttProcessPublishReceived": 6, // the duplicate-PUBREC NAK (and its retained re-emission), the resident-inferred ACK guard, and the retained exchange's ACK (F-105 7c)
	"drainClaimEligibleLocked":   1,
	"pubRelWriteKindLocked":      3, // the PUBREL callback's exact-kind check at write entry
}

func TestPendingEntryStructureIsClosed(t *testing.T) {
	cfg := pendingStructureProductionConfig()
	fset, files, info, pkg := pendingStructureLoad(t, ".")
	rep := pendingStructureScan(cfg, fset, files, info, pkg)
	for _, v := range rep.violations {
		t.Error(v)
	}
	compare := func(what string, got, want map[string]int) {
		t.Helper()
		keys := map[string]bool{}
		for k := range got {
			keys[k] = true
		}
		for k := range want {
			keys[k] = true
		}
		var ks []string
		for k := range keys {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			if got[k] != want[k] {
				t.Errorf("%s %q: found %d, recorded %d", what, k, got[k], want[k])
			}
		}
	}
	compare("ANY expectation in", rep.anyCalls, pendingStructureAnySites)
	actsGot := map[string]int{}
	for fn := range pendingStructureActs {
		actsGot[fn] = rep.acts[fn]
	}
	compare("kind references in", actsGot, pendingStructureActs)
	compare("field write", rep.fieldWrite, pendingStructureFieldWrites)
	compare("escape", rep.escapes, pendingStructureEscapes)
	compare("other pending-map write", rep.mapWrites, pendingStructureOtherMapWrites)
	if os.Getenv("MQTT_PENDING_STRUCTURE_DUMP") != "" {
		dump := func(name string, m map[string]int) {
			var ks []string
			for k := range m {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			fmt.Printf("var %s = map[string]int{\n", name)
			for _, k := range ks {
				fmt.Printf("\t%q: %d,\n", k, m[k])
			}
			fmt.Printf("}\n")
		}
		dump("pendingStructureFieldWrites", rep.fieldWrite)
		dump("pendingStructureEscapes", rep.escapes)
		dump("pendingStructureOtherMapWrites", rep.mapWrites)
	}
}

// The engine's own oracle (plan §5(f)): a fixture package declaring the same shapes, one function
// per bypass form. Every bypass must be reported; one that is not fails this test, which is what
// a mutation of the production code alone could never establish about the engine.
const pendingStructureFixture = `package mqtt

type mqttPendingKind uint8

const (
	mqttPendingHold mqttPendingKind = iota
	mqttPendingOrdinary
)

type mqttPendingEvidence struct{ lin string }
type mqttPendingExpect struct{ mode uint8 }
type slotRef struct{ nonce string; seq uint64 }

func (r *slotRef) bump() { r.seq++ }

type mqttPending struct {
	kind     mqttPendingKind
	ev       mqttPendingEvidence
	regNonce string
	slot     slotRef
	seq      uint64
	regForeign bool
}

func (p *mqttPending) acts() bool { return p.kind != mqttPendingHold }

type mqttSession struct {
	pendingPublish map[uint16]*mqttPending
	keep           *mqttPending
}

func mqttExpectEmpty() mqttPendingExpect { return mqttPendingExpect{} }

func (s *mqttSession) setPendingLocked(which int, pi uint16, e mqttPendingExpect, p *mqttPending) bool {
	s.pendingPublish[pi] = p
	return true
}

func mqttNewThing() *mqttPending { return &mqttPending{kind: mqttPendingOrdinary} }

func bypassIndexWrite(s *mqttSession)            { s.pendingPublish[1] = &mqttPending{} }
func bypassWholeMap(s *mqttSession, m map[uint16]*mqttPending) { s.pendingPublish = m }
func bypassKindInPlace(p *mqttPending)             { p.kind = mqttPendingOrdinary }
func bypassKindLiteral() *mqttPending              { return &mqttPending{kind: mqttPendingOrdinary} }
func bypassEvidenceLiteral() mqttPendingEvidence   { return mqttPendingEvidence{lin: "x"} }
func bypassIdentity(p *mqttPending)                { p.regNonce = "n" }
func bypassFieldPointer(p *mqttPending)            { q := &p.slot; q.nonce = "n" }
func bypassPointerMethod(p *mqttPending)           { p.slot.bump() }
func bypassFuncValue(p *mqttPending, f func(*mqttPending)) { f(p) }
func bypassInterface(p *mqttPending) any           { var x any = p; return x }
func bypassSubstateLiteral() *mqttPending          { return &mqttPending{regForeign: true} }
func bypassExpectVariable(s *mqttSession, p *mqttPending) {
	e := mqttExpectEmpty()
	s.setPendingLocked(0, 1, e, p)
}
func bypassChannel(p *mqttPending, c chan *mqttPending) { c <- p }

var bypassPackageClosure = func(s *mqttSession) { s.pendingPublish[1] = &mqttPending{kind: mqttPendingOrdinary} }
`

func TestPendingEntryStructureEngineReportsEveryBypass(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "fixture.go", pendingStructureFixture, 0)
	if err != nil {
		t.Fatal(err)
	}
	pkg, info, err := pendingStructureCheck(fset, []*ast.File{f}, importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		return nil, fmt.Errorf("the fixture imports nothing (%s)", path)
	}))
	if err != nil {
		t.Fatalf("the fixture must type-check: %v", err)
	}
	cfg := pendingStructureConfig{
		pending: "mqttPending", evidence: "mqttPendingEvidence", expect: "mqttPendingExpect", session: "mqttSession",
		writer: "setPendingLocked", constructors: map[string]bool{"mqttNewThing": true},
		producers: map[string]bool{}, expectCtors: map[string]bool{"mqttExpectEmpty": true},
		identity: map[string]bool{"kind": true, "ev": true, "regNonce": true, "slot.nonce": true},
		substate: map[string]bool{"regForeign": true},
	}
	rep := pendingStructureScan(cfg, fset, []*ast.File{f}, info, pkg)
	joined := strings.Join(rep.violations, "\n")
	for _, bypass := range []string{"bypassIndexWrite", "bypassWholeMap", "bypassKindInPlace", "bypassKindLiteral",
		"bypassEvidenceLiteral", "bypassIdentity", "bypassFieldPointer", "bypassPointerMethod",
		"bypassInterface", "bypassSubstateLiteral", "bypassExpectVariable", "bypassChannel", "bypassPackageClosure"} {
		if !strings.Contains(joined, bypass+":") {
			t.Errorf("the engine did not report %s; violations:\n%s", bypass, joined)
		}
	}
	for _, clean := range []string{"setPendingLocked:", "mqttNewThing:", "acts:"} {
		if strings.Contains(joined, clean) {
			t.Errorf("the engine reported a sanctioned site %s; violations:\n%s", clean, joined)
		}
	}
	if rep.escapes["bypassFuncValue|FUNCVALUE f"] != 1 {
		t.Errorf("a function value receiving a pending entry must be recorded: %v", rep.escapes)
	}
	// A type error aborts before any rule is evaluated.
	bad, err := parser.ParseFile(fset, "bad.go", "package mqtt\nvar x int = \"not an int\"\n", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := pendingStructureCheck(fset, []*ast.File{bad}, importer.Default()); err == nil {
		t.Error("an ill-typed package must fail the check, not be scanned")
	}
}

// The recorded census, generated by MQTT_PENDING_STRUCTURE_DUMP=1 and reviewed row by row.
//
// Field writes are keyed "function|field|class". A CONSTRUCTOR row is a constructor building its
// own fresh value; a DETACHED row writes a value that never was resident; a RESIDENT row is a
// checked update under one of the contracts of docs/ARCHITECTURE.md, "A pending entry's authority
// is its kind": a send section opening or closing (sendNonce, res, sendSettle), a send outcome
// (transmitted, markerSeq, intentUnresolved), a slot PHASE on the same slot (slot.seq, .state,
// .acq, .actionLive, .actionToken, .claim, .claimSeq, .abandonedAck, regSeq), a delivery refresh
// of a resident acting entry (trackPublish), affine and PUBREL record facts, the ledger charge
// (custody, set only), a settled store (regPending cleared) and the terminal reconcile marker.
// No RESIDENT row names kind, ev, regNonce, slot, slot.name or slot.nonce: the identity rule
// refuses those outright.
//
// Escapes are keyed "function|form": a call argument (ARG callee), a function value (FUNCVALUE),
// a store into a non-local (STORE), a composite-literal element (INLIT), a return (RETURN) and a
// whole-value copy (DEREFCOPY).
var pendingStructureFieldWrites = map[string]int{
	"abandonInflightRegister|regPending|RESIDENT":                   1,
	"acquirePublishAction|slot.acq|RESIDENT":                        1,
	"acquirePublishAction|slot.seq|RESIDENT":                        1,
	"advancePublishTerminal|slot.seq|RESIDENT":                      1,
	"advancePublishTerminal|slot.state|RESIDENT":                    1,
	"advanceResidentAfterClearanceLocked|regSeq|RESIDENT":           1,
	"advanceResidentAfterClearanceLocked|slot.actionLive|RESIDENT":  1,
	"advanceResidentAfterClearanceLocked|slot.actionToken|RESIDENT": 1,
	"advanceResidentAfterClearanceLocked|slot.seq|RESIDENT":         1,
	"advanceResidentPubRelPhaseLocked|regSeq|RESIDENT":              1,
	"advanceResidentPubRelPhaseLocked|slot.acq|RESIDENT":            1,
	"advanceResidentPubRelPhaseLocked|slot.actionLive|RESIDENT":     1,
	"advanceResidentPubRelPhaseLocked|slot.actionToken|RESIDENT":    1,
	"advanceResidentPubRelPhaseLocked|slot.seq|RESIDENT":            1,
	"advanceResidentPubRelPhaseLocked|slot.state|RESIDENT":          1,
	"beginSendLocked|res|RESIDENT":                                  1,
	"beginSendLocked|sendNonce|RESIDENT":                            1,
	"bindAbandonedAckOrigin|slot.abandonedAck|RESIDENT":             1,
	"bindAbandonedAckOrigin|slot.acq|RESIDENT":                      1,
	"bindActionToPubRelLocked|pubRelAffRef|DETACHED":                1,
	"condemnSendLocked|sendSettle|RESIDENT":                         1,
	"confirmInflightRegister|regPending|RESIDENT":                   1,
	"drainPublishAction|slot.acq|RESIDENT":                          1,
	"drainPublishAction|slot.actionLive|RESIDENT":                   1,
	"drainPublishAction|slot.actionToken|RESIDENT":                  1,
	"drainPublishAction|slot.seq|RESIDENT":                          1,
	"endPublishSend|intentUnresolved|RESIDENT":                      2,
	"endPublishSend|markerSeq|RESIDENT":                             1,
	"endPublishSend|transmitted|RESIDENT":                           2,
	"endSendCore|sendNonce|RESIDENT":                                1,
	"endSendCore|sendSettle|RESIDENT":                               1,
	"intendPubRelSlotOnly|slot.acq|RESIDENT":                        1,
	"intendPubRelSlotOnly|slot.seq|RESIDENT":                        1,
	"intendPubRelSlotOnly|slot.state|RESIDENT":                      1,
	"intendPubRelSlot|slot.acq|RESIDENT":                            1,
	"intendPubRelSlot|slot.seq|RESIDENT":                            1,
	"intendPubRelSlot|slot.state|RESIDENT":                          2,
	"markRecoveredCustodyLocked|custody|RESIDENT":                   2,
	"mqttDeliverQ2AffineCb|q2AffClaim|RESIDENT":                     1,
	"mqttDeliverQ2AffineCb|q2AffCopy|RESIDENT":                      1,
	"mqttNewBoundOpaque|ev|CONSTRUCTOR":                             1,
	"mqttNewBoundOpaque|jsDur|CONSTRUCTOR":                          1,
	"mqttNewBoundOpaque|kind|CONSTRUCTOR":                           1,
	"mqttNewBoundOpaque|qos|CONSTRUCTOR":                            1,
	"mqttNewBoundOpaque|regNonce|CONSTRUCTOR":                       1,
	"mqttNewBoundOpaque|regSeq|CONSTRUCTOR":                         1,
	"mqttNewBoundOpaque|sseq|CONSTRUCTOR":                           1,
	"mqttNewDemoted|demotedKind|CONSTRUCTOR":                        1,
	"mqttNewDemoted|kind|CONSTRUCTOR":                               1,
	"mqttNewDemoted|regForeign|CONSTRUCTOR":                         1,
	"mqttNewDemoted|regRecoveryDemoted|CONSTRUCTOR":                 1,
	"mqttNewMarkerOpaque|ev|CONSTRUCTOR":                            1,
	"mqttNewMarkerOpaque|kind|CONSTRUCTOR":                          1,
	"mqttNewMarkerOpaque|markerOpaque|CONSTRUCTOR":                  1,
	"mqttNewMarkerOpaque|markerSeq|CONSTRUCTOR":                     1,
	"mqttNewMarkerOpaque|qos|CONSTRUCTOR":                           1,
	"mqttNewMarkerOpaque|regNonce|CONSTRUCTOR":                      1,
	"mqttNewMarkerOpaque|regPending|CONSTRUCTOR":                    1,
	"mqttNewMarkerOpaque|regSeq|CONSTRUCTOR":                        1,
	"mqttNewMarkerOpaque|slot|CONSTRUCTOR":                          1,
	"mqttNewMarkerOpaque|transmitted|CONSTRUCTOR":                   1,
	"mqttNewPubRelSuccessor|custody|CONSTRUCTOR":                    1,
	"mqttNewPubRelSuccessor|ev|CONSTRUCTOR":                         1,
	"mqttNewPubRelSuccessor|kind|CONSTRUCTOR":                       1,
	"mqttNewPubRelSuccessor|regNonce|CONSTRUCTOR":                   1,
	"mqttNewPubRelSuccessor|regSeq|CONSTRUCTOR":                     1,
	"mqttNewPubRelSuccessor|slot|CONSTRUCTOR":                       1,
	"mqttNewReleasedDelivery|awaitingPubCompOwner|CONSTRUCTOR":      1,
	"mqttNewReleasedDelivery|kind|CONSTRUCTOR":                      1,
	"mqttNewTerminalReservation|custody|CONSTRUCTOR":                1,
	"mqttNewTerminalReservation|ev|CONSTRUCTOR":                     1,
	"mqttNewTerminalReservation|terminalReconcile|CONSTRUCTOR":      1,
	"mqttPromoteLanded|ev|CONSTRUCTOR":                              1,
	"mqttPromoteLanded|kind|CONSTRUCTOR":                            2,
	"mqttPromoteLanded|markerSeq|CONSTRUCTOR":                       1,
	"mqttPromoteLanded|qos|CONSTRUCTOR":                             1,
	"mqttPromoteLanded|regNonce|CONSTRUCTOR":                        1,
	"mqttPromoteLanded|regPending|CONSTRUCTOR":                      1,
	"mqttPromoteLanded|regSeq|CONSTRUCTOR":                          1,
	"mqttPromoteLanded|slot|CONSTRUCTOR":                            1,
	"mqttPromoteLanded|transmitted|CONSTRUCTOR":                     1,
	"mqttPromotePubRelAction|custody|CONSTRUCTOR":                   2,
	"mqttPromotePubRelAction|ev|CONSTRUCTOR":                        1,
	"mqttPromotePubRelAction|kind|CONSTRUCTOR":                      2,
	"mqttPromotePubRelAction|pubRelAffRef|CONSTRUCTOR":              1,
	"mqttPromoteRetainedLanded|ev|CONSTRUCTOR":                      1,
	"mqttPromoteRetainedLanded|kind|CONSTRUCTOR":                    1,
	"mqttPromoteRetainedLanded|markerSeq|CONSTRUCTOR":               1,
	"mqttPromoteRetainedLanded|qos|CONSTRUCTOR":                     1,
	"mqttPromoteRetainedLanded|regNonce|CONSTRUCTOR":                1,
	"mqttPromoteRetainedLanded|regPending|CONSTRUCTOR":              1,
	"mqttPromoteRetainedLanded|regSeq|CONSTRUCTOR":                  1,
	"mqttPromoteRetainedLanded|retainedInc|CONSTRUCTOR":             1,
	"mqttPromoteRetainedLanded|slot|CONSTRUCTOR":                    1,
	"mqttPromoteRetainedLanded|transmitted|CONSTRUCTOR":             1,
	"mqttPromoteRetainedPubRelAction|ev|CONSTRUCTOR":                1,
	"mqttPromoteRetainedPubRelAction|kind|CONSTRUCTOR":              1,
	"mqttPromoteRetainedPubRelAction|regNonce|CONSTRUCTOR":          1,
	"mqttPromoteRetainedPubRelAction|regSeq|CONSTRUCTOR":            1,
	"mqttPromoteRetainedPubRelAction|slot|CONSTRUCTOR":              1,
	"mqttReactivate|demotedKind|CONSTRUCTOR":                        1,
	"mqttReactivate|kind|CONSTRUCTOR":                               1,
	"mqttReactivate|regForeign|CONSTRUCTOR":                         1,
	"mqttReactivate|regRecoveryDemoted|CONSTRUCTOR":                 1,
	"mqttRecoveredPending|kind|CONSTRUCTOR":                         2,
	"mqttRecoveredPending|markerOpaque|CONSTRUCTOR":                 2,
	"mqttRecoveredPending|markerSeq|CONSTRUCTOR":                    2,
	"mqttRecoveredPending|regRecoveryPlaceholder|CONSTRUCTOR":       1,
	"mqttRecoveredPending|slot|CONSTRUCTOR":                         1,
	"mqttRecoveredPending|transmitted|CONSTRUCTOR":                  2,
	"mqttRecoveredRetainedPending|markerSeq|CONSTRUCTOR":            1,
	"mqttRecoveredRetainedPending|retainedInc|CONSTRUCTOR":          1,
	"mqttRecoveredRetainedPending|retainedReplay|CONSTRUCTOR":       1,
	"mqttRecoveredRetainedPending|transmitted|CONSTRUCTOR":          1,
	"mqttWithRetainedReplay|markerSeq|CONSTRUCTOR":                  1,
	"mqttWithRetainedReplay|retainedReplay|CONSTRUCTOR":             1,
	"mqttWithRetainedReplay|transmitted|CONSTRUCTOR":                1,
	"openAdmissionCustodyLocked|custody|RESIDENT":                   1,
	"phaseSlotToPubRelReady|regSeq|RESIDENT":                        1,
	"phaseSlotToPubRelReady|slot.seq|RESIDENT":                      1,
	"phaseSlotToPubRelReady|slot.state|RESIDENT":                    1,
	"recoverInflightRegistersOnce|regSeq|RESIDENT":                  1,
	"recoverInflightRegistersOnce|slot.claimSeq|RESIDENT":           1,
	"recoverInflightRegistersOnce|slot.claim|RESIDENT":              1,
	"recoverInflightRegistersOnce|slot.seq|RESIDENT":                1,
	"recoverInflightRegistersOnce|slot.state|RESIDENT":              1,
	"recoverInflightRegistersOnce|transmitted|RESIDENT":             1, // F-088 Site 4 (ii): a send outcome — same-allocation resident (recoveredResidentEligibleLocked), monotone false->true, under sess.mu; marker debt untouched
	"resolvePubRelIntent|slot.acq|RESIDENT":                         1,
	"resolvePubRelIntent|slot.seq|RESIDENT":                         1,
	"resolvePubRelIntent|slot.state|RESIDENT":                       1,
	"resolveUnacquiredPublishAction|slot.acq|RESIDENT":              1,
	"resolveUnacquiredPublishAction|slot.seq|RESIDENT":              1,
	"retainAndReconcileTerminal|terminalReconcile|RESIDENT":         3,
	"retrackPublishLocked|transmitted|RESIDENT":                     1,
	"trackPublish|jsAckSubject|RESIDENT":                            1,
	"trackPublish|jsDur|RESIDENT":                                   1,
	"trackPublish|sseq|RESIDENT":                                    1,
}
var pendingStructureEscapes = map[string]int{
	"abandonInflightRegister|ARG delete":                            1,
	"acquirePublishAction|ARG handOffAcknowledgedPublish":           1,
	"admitRetainedCopyOnce|ARG mqttExpectExactly":                   1,
	"admitRetainedCopyOnce|ARG mqttPromoteRetainedLanded":           1,
	"admitRetainedCopyOnce|ARG setPendingLocked":                    1,
	"advanceResidentAfterClearanceLocked|INLIT":                     2,
	"applyDisposalRecoveryLocked|STORE h.entry":                     1,
	"attemptOwedMaterialization|ARG phaseSlotToPubRelReady":         1,
	"attemptTerminalRegisterReleaseOnce|ARG advancePublishTerminal": 1,
	"attempt|ARG continueAbandonedRetainedPubRel":                   1,
	"attempt|ARG delete":                                            1,
	"attempt|ARG intendPubRelSlot":                                  1,
	"attempt|ARG resolvePubRelIntent":                               2,
	"attempt|ARG retrackPublishLocked":                              1,
	"bindActionToPubRelLocked|ARG mqttPromotePubRelAction":          1,
	"bindActionToPubRelLocked|ARG setPendingLocked":                 1,
	"bindRecoveryOpaqueLocked|ARG mqttExpectExactly":                1,
	"bindRecoveryOpaqueLocked|ARG mqttNewBoundOpaque":               1,
	"bindRecoveryOpaqueLocked|ARG mqttRecoveryPendingMatches":       1,
	"bindRecoveryOpaqueLocked|ARG setPendingLocked":                 1,
	"bindRetainedPubRelLocked|ARG mqttExpectExactly":                1,
	"bindRetainedPubRelLocked|ARG mqttPromoteRetainedPubRelAction":  1,
	"bindRetainedPubRelLocked|ARG setPendingLocked":                 1,
	"cancelPendingRegistrationsLocked|ARG cancelRegPendingLocked":   1,
	"cancelPendingRegistrationsLocked|ARG mqttExpectExactly":        1,
	"cancelPendingRegistrationsLocked|ARG setPendingLocked":         1,
	"capacityAvailableLocked|ARG len":                               1,
	"captureRecoveryReservationsLocked|ARG mqttRecoveryReservation": 2,
	"captureRecoveryReservationsLocked|INLIT":                       2,
	"commitRecoveryReleaseLocked|ARG delete":                        2,
	// TWO since F-088 WP3 edit 5: the cancelled arm's register-only landing removes only our
	// placeholder (identifier kept excluded), beside the arm's original removal.
	"confirmInflightRegister|ARG delete":                                 2,
	"confirmInflightRegister|ARG mqttExpectExactly":                      1,
	"confirmInflightRegister|ARG mqttPromoteLanded":                      1,
	"confirmInflightRegister|ARG setPendingLocked":                       1,
	"continueAbandonedRetainedPubRel|ARG installAbandonedRetainedPubRel": 1,
	"continueAbandonedRetainedPubRel|DEREFCOPY":                          1,
	"continueAbandonedRetainedPubRel|INLIT":                              1,
	"drainClaimDispositionLocked|ARG provenUnsentReleasableLocked":       1,
	"drainClaimEligibleLocked|RETURN":                                    1,
	"dropProvenUnsentPublishEntry|ARG deleteInflightRegisterFor":         1,
	"dropProvenUnsentPublishEntry|RETURN":                                1,
	"durableRestrictedLocked|ARG unresolvedReservationLocked":            1,
	"endSendCore|ARG handOffAcknowledgedPublish":                         2,
	"endSendCore|ARG deleteInflightRegisterFor":                          1,
	"endSendCore|ARG finalizeDrainSource":                                1,
	"endSendCore|ARG retireReleasedDeliveryLocked":                       1,
	"endSendCore|FUNCVALUE onEntry":                                      1,
	"enrolFloorClaimsLocked|ARG drainClaimDispositionLocked":             1,
	"forgetCancelledReg|ARG delete":                                      1,
	"handOffPublishEntryLocked|ARG delete":                               1,
	"handOffRecoveryPublishLocked|ARG delete":                            1,
	"handOffRecoveryPublishLocked|ARG mqttRecoveryReservation":           1,
	// F-088 WP2: an UNANSWERED removal reads the entry's send evidence and builds its AckRoute
	// from the entry (never from a record read), inside untrackPublishEntry.
	"installAckRouteForRemovalLocked|ARG mqttEntrySendEvidence":         1,
	"installAckRouteForRemovalLocked|ARG newMQTTAckRouteBinding":        1,
	"installAbandonedRetainedPubRel|ARG mqttNewAbandonedRetainedPubRel": 1,
	"installAbandonedRetainedPubRel|ARG setPendingLocked":               1,
	"installMarkerOpaqueReservation|ARG markerOpaqueEligibleLocked":     1,
	"installMarkerOpaqueReservation|ARG mqttExpectExactly":              1,
	"installMarkerOpaqueReservation|ARG mqttNewMarkerOpaque":            1,
	"installMarkerOpaqueReservation|ARG setPendingLocked":               1,
	"installRecoveredRetainedLocked|ARG mqttExpectCaptured":             1,
	"installRecoveredRetainedLocked|ARG mqttExpectExactly":              1,
	"installRecoveredRetainedLocked|ARG mqttWithRetainedReplay":         1,
	"installRecoveredRetainedLocked|ARG recoveryRestoreRefusedLocked":   1,
	"installRecoveredRetainedLocked|ARG setPendingLocked":               2,
	"installTerminalReservationLocked|ARG mqttExpectExactly":            1,
	"installTerminalReservationLocked|ARG mqttNewTerminalReservation":   1,
	"installTerminalReservationLocked|ARG setPendingLocked":             2,
	"intendPubRelSlotOnly|ARG resolvePubRelIntent":                      1,
	"intendPubRelSlot|ARG bindAbandonedAckOrigin":                       1,
	"intendPubRelSlot|ARG finishAbandonedAckResolution":                 1,
	"intendPubRelSlot|ARG intendPubRelSlotOnly":                         1,
	"intendPubRelSlot|ARG resolvePubRelIntent":                          4,
	"mqttAckSnapshot|RETURN":                                            1,
	"mqttDeliverMsgCbQoS12|ARG finalizeDrainSource":                     2,
	"mqttDeliverMsgCbQoS12|ARG provenUnsentReleasableLocked":            1,
	"mqttDeliverQ2AffineCb|ARG provenUnsentReleasableLocked":            1,
	"mqttExpectCaptured|ARG mqttExpectExactly":                          1,
	"mqttExpectExactly|INLIT":                                           1,
	"mqttNewAbandonedRetainedPubRel|RETURN":                             1,
	"mqttNewAllocation|RETURN":                                          1,
	"mqttNewBoundOpaque|DEREFCOPY":                                      1,
	"mqttNewBoundOpaque|RETURN":                                         1,
	"mqttNewCancelledHold|RETURN":                                       1,
	"mqttNewDemoted|DEREFCOPY":                                          1,
	"mqttNewDemoted|RETURN":                                             1,
	"mqttNewMarkerOpaque|DEREFCOPY":                                     1,
	"mqttNewMarkerOpaque|RETURN":                                        1,
	"mqttNewPoisoned|RETURN":                                            1,
	"mqttNewPubRelPlaceholder|RETURN":                                   1,
	"mqttNewPubRelSuccessor|RETURN":                                     1,
	"mqttNewReleasedDelivery|DEREFCOPY":                                 1,
	"mqttNewReleasedDelivery|RETURN":                                    1,
	"mqttNewRetainedAllocation|RETURN":                                  1,
	"mqttNewTerminalReservation|RETURN":                                 1,
	"mqttNewUnmintedHold|RETURN":                                        1,
	"mqttPendingRefs|ARG len":                                           2,
	"mqttProcessConnect|ARG mqttPendingRefs":                            2,
	"mqttProcessPubComp|ARG pubCompConvertLocked":                       1,
	"mqttProcessPublishReceived|ARG completeAcknowledgedPublish":        3,
	"mqttProcessPublishReceived|ARG continueAbandonedRetainedPubRel":    1,
	"mqttProcessPublishReceived|ARG delete":                             3,
	"mqttProcessPublishReceived|ARG drainOwnsSource":                    1,
	"mqttProcessPublishReceived|ARG finalizeDrainSource":                1,
	"mqttProcessPublishReceived|ARG intendPubRelSlot":                   2,
	"mqttProcessPublishReceived|ARG phaseSlotToPubRelReady":             1,
	"mqttProcessPublishReceived|ARG retrackPublishLocked":               3,
	"mqttProcessPublishReceived|ARG scheduleIntentResolution":           2,
	"mqttProcessPublishReceived|DEREFCOPY":                              1,
	"mqttProcessPublishReceived|FUNCVALUE releaseRegister":              6,
	"mqttPromoteLanded|DEREFCOPY":                                       1,
	"mqttPromoteLanded|RETURN":                                          1,
	"mqttPromotePubRelAction|RETURN":                                    1,
	"mqttPromoteRetainedLanded|DEREFCOPY":                               1,
	"mqttPromoteRetainedLanded|RETURN":                                  1,
	"mqttPromoteRetainedPubRelAction|DEREFCOPY":                         1,
	"mqttPromoteRetainedPubRelAction|RETURN":                            1,
	"mqttReactivate|DEREFCOPY":                                          1,
	"mqttReactivate|RETURN":                                             1,
	"mqttRecoveredPending|RETURN":                                       1,
	"mqttRecoveredRetainedPending|RETURN":                               1,
	"mqttRecoveryOpaquePending|RETURN":                                  1,
	"mqttRecoveryReplaceable|ARG mqttRecoveryPendingMatches":            1,
	"mqttRecoveryReplaceable|ARG mqttRecoveryReservation":               1,
	"mqttWithRetainedReplay|DEREFCOPY":                                  1,
	"mqttWithRetainedReplay|RETURN":                                     1,
	"pendingMapLocked|ARG make":                                         2,
	"pendingMapLocked|RETURN":                                           2,
	"pubCompConvertLocked|ARG mqttExpectExactly":                        1,
	"pubCompConvertLocked|ARG mqttNewTerminalReservation":               1,
	"pubCompConvertLocked|ARG setPendingLocked":                         1,
	"purgeConsumerPendingModeLocked|ARG cancelRegPendingLocked":         1,
	"purgeConsumerPendingModeLocked|ARG delete":                         1,
	"purgeConsumerPendingModeLocked|ARG mqttExpectExactly":              1,
	"purgeConsumerPendingModeLocked|ARG provenUnsentReleasableLocked":   1,
	"purgeConsumerPendingModeLocked|ARG setPendingLocked":               1,
	"reactivateDemotedLocked|ARG mqttExpectExactly":                     1,
	"reactivateDemotedLocked|ARG mqttReactivate":                        1,
	"reactivateDemotedLocked|ARG setPendingLocked":                      1,
	"receiveMaxOccupancyLocked|ARG ledgerOwnsEntryLocked":               2,
	"reclassifyReactivatedDrainsLocked|ARG cancelRegPendingLocked":      1,
	"reclassifyReactivatedDrainsLocked|ARG drainClaimDispositionLocked": 1,
	"recoverExchangeRouteLocked|ARG delete":                             1,
	"recoverExchangeRouteLocked|ARG mqttRecoveryPendingMatches":         2,
	"recoverExchangeRouteLocked|ARG reactivateDemotedLocked":            2,
	"recoverInflightRegistersOnce|ARG delete":                           2,
	"recoverInflightRegistersOnce|ARG make":                             2,
	"recoverInflightRegistersOnce|ARG mqttExpectCaptured":               3,
	"recoverInflightRegistersOnce|ARG mqttExpectExactly":                3,
	"recoverInflightRegistersOnce|ARG mqttNewDemoted":                   2,
	"recoverInflightRegistersOnce|ARG mqttNewPoisoned":                  1,
	"recoverInflightRegistersOnce|ARG mqttRecoveryPendingMatches":       1,
	"recoverInflightRegistersOnce|ARG mqttRecoveryReplaceable":          1,
	"recoverInflightRegistersOnce|ARG reactivateDemotedLocked":          1,
	"recoverInflightRegistersOnce|ARG recoveredResidentEligibleLocked":  1, // F-088 Site 4 (i): READ-ONLY eligibility (same allocation, ordinary acting, custody-held, lineage not ended) under sess.mu; writes nothing, no escapes of its own
	"recoverInflightRegistersOnce|ARG recoveryInstallRefusedLocked":     1,
	"recoverInflightRegistersOnce|ARG recoveryRestoreRefusedLocked":     3,
	"recoverInflightRegistersOnce|ARG setPendingLocked":                 11,
	"recoveryPIHeldLocked|INLIT":                                        2,
	"recoveryReleaseCurrentLocked|ARG mqttRecoveryReservation":          2,
	"recoveryRestoreRefusedLocked|ARG recoveryInstallRefusedLocked":     1,
	"releaseCancelledReservation|ARG delete":                            1,
	"releaseSettledVoidReservationsLocked|ARG delete":                   1,
	"releaseTerminalReservation|ARG delete":                             1,
	"releaseTransmittedIdentifier|ARG deleteInflightRegisterFor":        1,
	"removeLineageProjectionsLocked|ARG mqttExpectExactly":              1,
	"removeLineageProjectionsLocked|ARG mqttNewReleasedDelivery":        1,
	"removeLineageProjectionsLocked|ARG setPendingLocked":               1,
	"replayAllowanceLocked|ARG countsEntry":                             1,
	"replayAllowanceLocked|ARG ledgerOwnsEntryLocked":                   1,
	"replayAllowanceLocked|ARG len":                                     2,
	"resendPendingRedelivery|ARG len":                                   3,
	"resendPendingRedelivery|ARG make":                                  1,
	"resendPendingRedelivery|ARG mqttPendingRedeliverable":              1,
	"resendPendingRedelivery|ARG provenUnsentReleasableLocked":          1,
	"resendPendingRedelivery|ARG replayAllowanceLocked":                 1,
	"resolveAckAttempt|ARG handOffAcknowledgedPublish":                  1,
	"resolvePubRelIntent|ARG bindAbandonedAckOrigin":                    1,
	"resolvePubRelIntent|ARG finishAbandonedAckResolution":              1,
	"resolveUnacquiredPublishAction|ARG handOffAcknowledgedPublish":     3,
	"retainAndReconcileTerminal|ARG mqttExpectExactly":                  1,
	"retainAndReconcileTerminal|ARG mqttNewTerminalReservation":         1,
	"retainAndReconcileTerminal|ARG setPendingLocked":                   1,
	"retireReleasedDeliveryLocked|ARG mqttExpectExactly":                1,
	"retireReleasedDeliveryLocked|ARG mqttNewTerminalReservation":       1,
	"retireReleasedDeliveryLocked|ARG setPendingLocked":                 1,
	"retireReleasedDeliveriesLocked|ARG retireReleasedDeliveryLocked":   1,
	"retrackPublishLocked|ARG make":                                     1,
	"retrackPublishLocked|ARG setPendingLocked":                         1,
	"scheduleIntentResolution|DEREFCOPY":                                1,
	"scheduleIntentResolution|STORE r.placeholder":                      1,
	"settleAbandonedDeliveryLocked|ARG delete":                          1,
	"sourcePendingPubRelLocked|ARG mqttPendingExchangeSource":           1,
	"sourceRestrictedLocked|ARG unresolvedReservationLocked":            1,
	"stageDrain|ARG cancelRegPendingLocked":                             1,
	"stageDrain|ARG drainClaimDispositionLocked":                        1,
	"stepRetainedOwed|ARG installAbandonedRetainedPubRel":               1,
	"stepRetainedOwed|ARG withdrawRetainedOnce":                         1,
	"trackAsPubRel|ARG make":                                            1,
	"trackAsPubRel|ARG mqttExpectCaptured":                              1,
	"trackAsPubRel|ARG mqttNewPubRelSuccessor":                          2,
	"trackAsPubRel|ARG setPendingLocked":                                1,
	"trackPublishRetained|ARG make":                                     1,
	"trackPublishRetained|ARG setPendingLocked":                         1,
	"trackPublish|ARG make":                                             1,
	"trackPublish|ARG setPendingLocked":                                 1,
	"untrackPubRel|ARG delete":                                          1,
	"untrackPublishEntry|ARG delete":                                    1,
	"untrackPublishEntry|ARG installAckRouteForRemovalLocked":           1,
	"untrackPublishEntry|RETURN":                                        1,
	"voidRegistrationsLocked|ARG provenUnsentReleasableLocked":          1,
	"withdrawRetainedOnce|ARG reapWithdrawnRetainedMarker":              1,
	"withdrawRetainedUnsent|ARG withdrawRetainedOnce":                   1,
	"withdrawRetainedUnsent|DEREFCOPY":                                  1,
	"withdrawRetainedUnsent|INLIT":                                      1,
}
var pendingStructureOtherMapWrites = map[string]int{
	"resendPendingRedelivery|replay": 1,
}
