// The NEUTRAL scanner: parsing and site discovery, with no census policy in it.
//
// Two checks read this package's own source and each asserts something different about it.
// F-034's persisted-identity census asks which functions interpret a JetStream observation
// (persisted_identity_ingress_test.go). The message-GET transport tests ask whether every
// point read still enters through the five mqttJSA load methods
// (jsa_msgget_transport_test.go). They must not each grow a walker, so the walk lives here
// and carries no policy at all: it reports every call and every identifier reference it
// finds, and each check decides what those mean.
//
// Calls inside nested function literals and deferred calls are attributed to the enclosing
// function, because they are part of its behaviour and would otherwise escape.

package mqtt

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// censusFuncKey identifies one function declaration. The receiver is part of the identity
// because this package has same-named methods on different types that do materially
// different things: (*mqttJSA).loadInflightRecord reads and classifies a register, while
// (*mqttAccountSessionManager).loadInflightRecord forwards that already-classified result
// through a test seam.
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

// censusCallSite is one call expression.
type censusCallSite struct {
	Fn     censusFuncKey
	Callee string // the selector's field name, or the identifier for a bare call
	On     string // the selector's receiver EXPRESSION, rendered; empty for a bare call
	Args   []string
}

// censusIdentRef is one identifier reference inside a function body, outside comments.
type censusIdentRef struct {
	Fn   censusFuncKey
	Name string
}

type censusScan struct {
	Calls  []censusCallSite
	Idents []censusIdentRef
	Funcs  map[censusFuncKey]bool
}

// censusRenderExpr renders an expression through go/printer. Two calls that differ only in
// whitespace or line breaks render identically, so a rendered argument is stable across a
// reformat.
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
// check that owns the question.
func censusScanPackage(t *testing.T) *censusScan {
	t.Helper()
	names := censusProductionFiles(t)
	fset := token.NewFileSet()
	sc := &censusScan{Funcs: map[censusFuncKey]bool{}}
	for _, name := range names {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
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

// censusProductionFiles lists the package's non-test .go files, sorted. A source guard that
// reads this list rather than naming files cannot be narrowed by moving a declaration from one
// file to another.
func censusProductionFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
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
	return names
}

// censusFuncSource finds the one production declaration of a function, by receiver type (empty
// for a plain function) and name, in whichever file declares it. It returns that file's lines
// and the half-open line range [start, end) of the declaration, from its func keyword (doc
// comment excluded) through its closing brace, so a function at the end of its file is covered.
func censusFuncSource(t *testing.T, recv, name string) (file string, lines []string, start, end int) {
	t.Helper()
	found := 0
	for _, n := range censusProductionFiles(t) {
		src, err := os.ReadFile(n)
		if err != nil {
			t.Fatalf("reading %s: %v", n, err)
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, n, src, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", n, err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Name.Name != name || censusRecvType(fd) != recv {
				continue
			}
			found++
			file, lines = n, strings.Split(string(src), "\n")
			start, end = fset.Position(fd.Pos()).Line-1, fset.Position(fd.End()).Line
		}
	}
	if found != 1 {
		t.Fatalf("found %d production declarations of %s (receiver %q), want exactly one; "+
			"this guard cannot check what it claims to", found, name, recv)
	}
	return file, lines, start, end
}

// collectBody walks one function body. ast.Inspect descends into function literals, so a
// call inside a closure, a defer or a goroutine is attributed to the enclosing function.
func collectBody(sc *censusScan, fset *token.FileSet, key censusFuncKey, body *ast.BlockStmt) {
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			site := censusCallSite{Fn: key}
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
		case *ast.Ident:
			sc.Idents = append(sc.Idents, censusIdentRef{Fn: key, Name: x.Name})
		}
		return true
	})
}

// FunctionsReferencing returns every function that mentions an identifier, outside its own
// declaration.
func (sc *censusScan) FunctionsReferencing(name string) map[censusFuncKey]bool {
	out := map[censusFuncKey]bool{}
	for _, r := range sc.Idents {
		if r.Name == name {
			out[r.Fn] = true
		}
	}
	return out
}

// TestCensusScannerFindsThePackage is the guard on the scanner itself. Every check built on
// this walk asserts a bijection or a confinement against what it reports, and either passes
// vacuously against an empty scan, so the scan's own liveness is asserted first.
func TestCensusScannerFindsThePackage(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	if len(sc.Funcs) < 100 {
		t.Fatalf("the scan found only %d function declarations in this package, which cannot "+
			"be right: the parse or the directory is wrong, and every check built on this "+
			"walk would pass vacuously", len(sc.Funcs))
	}
	if len(sc.Calls) < 1000 {
		t.Fatalf("the scan found only %d call sites, which cannot be right for this package", len(sc.Calls))
	}
	if len(sc.Idents) < 1000 {
		t.Fatalf("the scan found only %d identifier references, which cannot be right for this package", len(sc.Idents))
	}
	// A call inside a function literal must be attributed to the enclosing function, or a
	// closure-hosted read escapes every check built on this walk. checkResourceOverwrites
	// reads through loadLastMsgFor inside a closure.
	found := false
	for _, c := range sc.Calls {
		if c.Fn.Name == "checkResourceOverwrites" && c.Callee == "loadLastMsgFor" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("no loadLastMsgFor call was attributed to checkResourceOverwrites, so calls " +
			"inside function literals escape the walk")
	}
}
