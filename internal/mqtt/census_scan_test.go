// The NEUTRAL scanner: parsing and site discovery, with no census policy in it.
//
// Two censuses read this package's own source and each asserts something different about
// it. F-034's persisted-identity census asks which durable identities the bridge carries
// (persisted_identity_ingress_test.go). F-109's point-read absence census asks which
// decisions rest on a store read that could not be completed
// (point_read_absence_census_test.go, docs/findings/point-read-absence-census.md).
//
// They must not each grow a walker. F-109's plan review ruled that reusing F-034's walker
// unchanged was insufficient for the second question — it filtered on a hard-coded method
// name set, saw only selector calls, dropped receiver identity, and returned neither
// arguments nor occurrence identity, so a bare classifier call was invisible to it — while
// two independent walkers over one package is the shape feedback_one_authoritative_definition
// names. So the walk lives here and carries no policy at all: it reports every call, every
// identifier reference and every field access it finds, and each census decides what those
// mean.
//
// Occurrence identity is why this file exists as much as sharing is. One function can call
// one callee with identical arguments twice — checkResourceOverwrites does exactly that on
// $MQTT_inflight — so a site key built from (function, callee, arguments) collides, and a
// manifest keyed on it silently merges two sites. The scan therefore numbers every call
// within its enclosing function in source order, INCLUDING calls inside nested function
// literals and deferred calls, which are part of the enclosing function's behaviour and
// would otherwise escape.

package mqtt

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// censusFuncKey identifies one function declaration. The receiver is part of the identity
// because this package has same-named methods on different types that do materially
// different things: (*mqttJSA).loadInflightRecord reads and classifies a register, while
// (*mqttAccountSessionManager).loadInflightRecord forwards that already-classified result
// through a test seam. A census that merged them would attribute one's disposition to the
// other.
type censusFuncKey struct {
	File string
	Recv string // the receiver TYPE name, without pointer or parameter name; empty for a plain function
	Name string
}

func (k censusFuncKey) String() string {
	if k.Recv == "" {
		return k.File + ":" + k.Name
	}
	return k.File + ":(" + k.Recv + ")." + k.Name
}

// censusCallSite is one call expression, located precisely enough to be a manifest key.
type censusCallSite struct {
	Fn     censusFuncKey
	Callee string // the selector's field name, or the identifier for a bare call
	On     string // the selector's receiver EXPRESSION, rendered; empty for a bare call
	Occ    int    // 0-based index of this call within Fn, source order, closures included
	Args   []string
	Line   int
}

// Key is the manifest key. Rendering comes from the AST through go/printer rather than from
// the source bytes, so gofmt-level reformatting does not move a site and a manifest does not
// have to be rewritten because a line wrapped.
func (s censusCallSite) Key() string {
	on := s.On
	if on != "" {
		on += "."
	}
	return s.Fn.String() + "#" + strconv.Itoa(s.Occ) + " " + on + s.Callee + "(" + strings.Join(s.Args, ", ") + ")"
}

// censusIdentRef is one reference to a package-level identifier, outside comments.
type censusIdentRef struct {
	Fn   censusFuncKey
	Name string
	Line int
}

// censusMutation is one MUTATION occurrence: a place where this package writes through a
// selector, an index or a dereference, or increments something. It is enumerated with no
// guard of any kind, because consult C2 refuted the idea that an absence-derived write can be
// recognised by its right-hand side. Its worked counterexample is in this package: the absent
// arm at inflight_register.go:8773 contributes NOTHING to the loaded set, the switch at :10489
// then sets or clears piSpaceIncomplete from what recovery found, and neither assignment
// mentions an absence-derived local. An effect caused by a write that never happened has no
// right-hand side to inspect.
//
// Base is the rendered base expression, so sess.piSpaceIncomplete and rec.piSpaceIncomplete are
// distinguishable where the source distinguishes them, and Occ makes two mutations of one field
// in one function separate obligations.
type censusMutation struct {
	Fn    censusFuncKey
	Base  string // rendered base expression; empty when the target is a bare identifier
	Field string // selector field name; empty for an index or dereference target
	Kind  string // "selector", "index", "deref", "incdec", "ident"
	Occ   int
	Line  int
	Pos   token.Pos // exact node identity, so a write and a same-line read are distinguishable
}

// censusFieldRef is one X.field REFERENCE that is not itself an assignment target. Assignment
// targets are censusMutation and are deliberately not reported here as well: C2 faulted the
// first scanner for reporting an assignment's own selector as a reference, which made a write
// look like a destination for itself.
type censusFieldRef struct {
	Fn    censusFuncKey
	Base  string
	Field string
	Occ   int
	Line  int
	Pos   token.Pos
}

// censusConst is one package-level constant, with the type its declaration block names.
// Collected so a census can tell a package enum member from `nil`, `true` or a local — Go
// spells all of them as identifiers, and a set membership test on the name alone would
// admit every one of them.
type censusConst struct {
	Name string
	Type string // the const block's declared type, empty for an untyped or inferred block
}

type censusScan struct {
	PkgVars   map[string]bool
	Calls     []censusCallSite
	Idents    []censusIdentRef
	Fields    []censusFieldRef
	Mutations []censusMutation
	Consts    map[string]censusConst
	Funcs     map[censusFuncKey]bool
}

// censusRenderExpr renders an expression through go/printer. Two calls that differ only in
// whitespace or line breaks render identically, which is what makes a rendered argument
// usable in a durable key.
func censusRenderExpr(fset *token.FileSet, e ast.Expr) string {
	var sb strings.Builder
	if err := printer.Fprint(&sb, fset, e); err != nil {
		return "<unrenderable>"
	}
	return strings.Join(strings.Fields(sb.String()), " ")
}

func censusRecvType(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return ""
	}
	t := fd.Recv.List[0].Type
	for {
		star, ok := t.(*ast.StarExpr)
		if !ok {
			break
		}
		t = star.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return id.Name
	}
	return "?"
}

// censusScanPackage parses every non-test .go file in the package directory and reports
// what it finds. It applies no filter: filtering is policy, and policy belongs to the
// census that owns the question.
func censusScanPackage(t *testing.T) *censusScan {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	fset := token.NewFileSet()
	sc := &censusScan{
		Consts:  map[string]censusConst{},
		Funcs:   map[censusFuncKey]bool{},
		PkgVars: map[string]bool{},
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || filepath.Ext(n) != ".go" || strings.HasSuffix(n, "_test.go") {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)
	if len(names) == 0 {
		t.Fatal("the package directory contains no non-test Go files, so the scan would " +
			"vacuously pass: the working directory is wrong, not the package")
	}
	for _, name := range names {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		collectConsts(sc, f)
		collectPkgVars(sc, f)
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			key := censusFuncKey{File: name, Recv: censusRecvType(fd), Name: fd.Name.Name}
			sc.Funcs[key] = true
			collectBody(sc, fset, key, fd.Body)
		}
	}
	return sc
}

// collectPkgVars records package-level variables. C2's mutation rule covers "assignments to
// package variables identified through lexical binding" — a write to a local is not a
// state-write-to-reader edge at all, because the local dies with the frame and whatever it
// carries leaves the node through a return or a call, which are their own candidate kinds.
func collectPkgVars(sc *censusScan, f *ast.File) {
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, s := range gd.Specs {
			vs, ok := s.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, n := range vs.Names {
				sc.PkgVars[n.Name] = true
			}
		}
	}
}

func collectConsts(sc *censusScan, f *ast.File) {
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		// A const block's type is stated on the first spec that carries one and then
		// carried down the block by iota, so remember it rather than reading each spec
		// in isolation.
		blockType := ""
		for _, s := range gd.Specs {
			vs, ok := s.(*ast.ValueSpec)
			if !ok {
				continue
			}
			if vs.Type != nil {
				if id, ok := vs.Type.(*ast.Ident); ok {
					blockType = id.Name
				} else {
					blockType = "?"
				}
			}
			for _, n := range vs.Names {
				sc.Consts[n.Name] = censusConst{Name: n.Name, Type: blockType}
			}
		}
	}
}

// collectBody walks one function body, numbering calls and mutations in source order.
// ast.Inspect descends into function literals, so a call inside a closure, a defer or a
// goroutine is numbered alongside its siblings rather than escaping — which is what the
// occurrence key needs, since a closure's call is the enclosing function's call as far as this
// package's behaviour is concerned, and consult C2 named all three explicitly.
func collectBody(sc *censusScan, fset *token.FileSet, key censusFuncKey, body *ast.BlockStmt) {
	occ, mocc, focc := 0, 0, 0

	// Assignment targets are recorded as mutations, never as references. Collected first so
	// the reference walk below can skip them: a write is not a destination for itself.
	targets := map[ast.Node]bool{}
	markTarget := func(e ast.Expr) {
		// An indexed or dereferenced target carries its field INSIDE it —
		// as.testMarkerGhosts[x] = y writes through the selector as.testMarkerGhosts — so the
		// nested selector is part of the write and must not also be counted as a reference.
		for {
			targets[e] = true
			switch t := e.(type) {
			case *ast.IndexExpr:
				e = t.X
			case *ast.StarExpr:
				e = t.X
			case *ast.ParenExpr:
				e = t.X
			default:
				return
			}
		}
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for _, lhs := range x.Lhs {
				markTarget(lhs)
			}
		case *ast.IncDecStmt:
			markTarget(x.X)
		}
		return true
	})

	recordMutation := func(target ast.Expr, kind string, line int) {
		// The FIELD identifier's position, not the expression's start: s.svcID.account and
		// its inner s.svcID both begin at `s`, so a start position cannot tell a nested write
		// from the reference it contains.
		m := censusMutation{Fn: key, Kind: kind, Occ: mocc, Line: line, Pos: target.Pos()}
		mocc++
		switch t := target.(type) {
		case *ast.SelectorExpr:
			m.Field = t.Sel.Name
			m.Base = censusRenderExpr(fset, t.X)
			m.Kind = "selector"
			m.Pos = t.Sel.Pos()
		case *ast.IndexExpr:
			// sess.pendingPublish[pi] — the field is the indexed BASE, which C2 named
			// specifically: a map element written through a field is a mutation of that
			// field's contents however the element is addressed.
			m.Base = censusRenderExpr(fset, t.X)
			m.Kind = "index"
			if se, ok := t.X.(*ast.SelectorExpr); ok {
				m.Field = se.Sel.Name
				m.Base = censusRenderExpr(fset, se.X)
				m.Pos = se.Sel.Pos()
			}
		case *ast.StarExpr:
			m.Base = censusRenderExpr(fset, t.X)
			m.Kind = "deref"
		case *ast.Ident:
			m.Base = t.Name
			m.Kind = "local"
			if sc.PkgVars[t.Name] {
				m.Kind = "pkgvar"
			}
		default:
			m.Base = censusRenderExpr(fset, target)
			m.Kind = "other"
		}
		if kind == "incdec" {
			m.Kind = "incdec"
		}
		sc.Mutations = append(sc.Mutations, m)
	}

	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			site := censusCallSite{Fn: key, Occ: occ, Line: fset.Position(x.Pos()).Line}
			occ++
			switch fn := x.Fun.(type) {
			case *ast.SelectorExpr:
				site.Callee = fn.Sel.Name
				site.On = censusRenderExpr(fset, fn.X)
			case *ast.Ident:
				site.Callee = fn.Name
			default:
				site.Callee = "<expr>"
				site.On = censusRenderExpr(fset, x.Fun)
			}
			for _, a := range x.Args {
				site.Args = append(site.Args, censusRenderExpr(fset, a))
			}
			sc.Calls = append(sc.Calls, site)
		case *ast.AssignStmt:
			// Compound assignment (+=, |= and the rest) is a mutation exactly as plain
			// assignment is; C2 named it.
			for _, lhs := range x.Lhs {
				recordMutation(lhs, "assign", fset.Position(lhs.Pos()).Line)
			}
		case *ast.IncDecStmt:
			recordMutation(x.X, "incdec", fset.Position(x.X.Pos()).Line)
		case *ast.Ident:
			sc.Idents = append(sc.Idents, censusIdentRef{Fn: key, Name: x.Name, Line: fset.Position(x.Pos()).Line})
		case *ast.SelectorExpr:
			if targets[ast.Node(x)] {
				return true
			}
			sc.Fields = append(sc.Fields, censusFieldRef{
				Fn: key, Field: x.Sel.Name, Base: censusRenderExpr(fset, x.X),
				Occ: focc, Line: fset.Position(x.Pos()).Line, Pos: x.Sel.Pos(),
			})
			focc++
		}
		return true
	})
}

// MutationsIn returns every mutation occurrence inside a set of functions.
func (sc *censusScan) MutationsIn(fns map[censusFuncKey]bool) []censusMutation {
	var out []censusMutation
	for _, m := range sc.Mutations {
		if fns[m.Fn] {
			out = append(out, m)
		}
	}
	return out
}

// FieldReferences returns every non-target reference to a field spelling, package-wide. C2
// requires the destination set for a field mutation to be exactly this, conservatively and
// including ambiguous bases: an unresolved identity must never collapse to an empty set, and
// these are CANDIDATES rather than proven reads.
func (sc *censusScan) FieldReferences(field string) []censusFieldRef {
	var out []censusFieldRef
	for _, f := range sc.Fields {
		if f.Field == field {
			out = append(out, f)
		}
	}
	return out
}

// CallsTo returns every call to a callee name, in scan order.
func (sc *censusScan) CallsTo(callee string) []censusCallSite {
	var out []censusCallSite
	for _, c := range sc.Calls {
		if c.Callee == callee {
			out = append(out, c)
		}
	}
	return out
}

// FunctionsReferencing returns every function that mentions an identifier, outside its own
// declaration. A reference is DISCOVERY and never a disposition: mentioning a verdict is
// sufficient to be admitted to a census and is not necessary to propagate one, which is why
// no census may use this as its definition of coverage.
func (sc *censusScan) FunctionsReferencing(name string) map[censusFuncKey]bool {
	out := map[censusFuncKey]bool{}
	for _, r := range sc.Idents {
		if r.Name == name {
			out[r.Fn] = true
		}
	}
	return out
}

// TestCensusScannerFindsThePackage is the guard on the scanner itself. Every census here
// asserts a bijection against what this walk reports, and a bijection against an empty scan
// passes vacuously — so the scan's own liveness is asserted before anything is built on it.
func TestCensusScannerFindsThePackage(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	if len(sc.Funcs) < 100 {
		t.Fatalf("the scan found only %d function declarations in this package, which cannot "+
			"be right: the parse or the directory is wrong, and every census built on this "+
			"walk would pass vacuously", len(sc.Funcs))
	}
	if len(sc.Calls) < 1000 {
		t.Fatalf("the scan found only %d call sites, which cannot be right for this package", len(sc.Calls))
	}
	if len(sc.Consts) < 100 {
		t.Fatalf("the scan found only %d package-level constants, so the const collection is "+
			"broken and every sentinel membership test would admit locals", len(sc.Consts))
	}
	if len(sc.Mutations) < 500 {
		t.Fatalf("the scan found only %d mutation occurrences, which cannot be right for this "+
			"package; the state-edge candidate set would be silently short", len(sc.Mutations))
	}
	// An assignment target must be a mutation and NOT also a reference, or a write becomes a
	// destination for itself and the state accounting is circular. C2 faulted the first
	// scanner for exactly this.
	var piWrites, piRefs int
	for _, m := range sc.Mutations {
		if m.Field == "piSpaceIncomplete" {
			piWrites++
		}
	}
	for _, f := range sc.Fields {
		if f.Field == "piSpaceIncomplete" {
			piRefs++
		}
	}
	if piWrites == 0 {
		t.Fatal("piSpaceIncomplete has no mutation occurrence, so the mutation walk missed the " +
			"one state carrier this census is known to depend on")
	}
	if piRefs == 0 {
		t.Fatal("piSpaceIncomplete has no non-target reference, so the destination walk found " +
			"no reader for a field bumpPI demonstrably consults")
	}
	// Exact node identity, not line: `h.slotOwed.binds = append(h.slotOwed.binds, x)` writes
	// and reads the same field on one line, and both occurrences are real.
	mutPos := map[token.Pos]censusMutation{}
	for _, m := range sc.Mutations {
		mutPos[m.Pos] = m
	}
	for _, f := range sc.Fields {
		if m, ok := mutPos[f.Pos]; ok {
			t.Fatalf("%s:%d writes %s.%s and the scan reports that same occurrence as a "+
				"REFERENCE too, so a write is a destination for itself",
				f.Fn.File, f.Line, m.Base, m.Field)
		}
	}
	// An indexed write through a field must be attributed to the field, not dropped: C2 named
	// sess.pendingPublish[pi] as the shape.
	var indexed bool
	for _, m := range sc.Mutations {
		if m.Kind == "index" && m.Field != "" {
			indexed = true
			break
		}
	}
	if !indexed {
		t.Fatal("no indexed mutation was attributed to a field, so a map element written " +
			"through a field escapes the state candidate set")
	}
	// Occurrence numbering must actually discriminate the known collision, or the key is
	// no better than the one F-109's plan review found merging two sites.
	var seen []censusCallSite
	for _, c := range sc.Calls {
		if c.Fn.Name == "checkResourceOverwrites" && c.Callee == "loadLastMsgFor" {
			seen = append(seen, c)
		}
	}
	if len(seen) < 2 {
		t.Fatalf("checkResourceOverwrites was expected to carry at least two loadLastMsgFor "+
			"calls, the collision this key exists to separate; the scan found %d", len(seen))
	}
	if seen[0].Key() == seen[1].Key() {
		t.Fatalf("two distinct call sites render the same key %q, so the occurrence index is "+
			"not discriminating and a manifest keyed on it would merge them", seen[0].Key())
	}
}
