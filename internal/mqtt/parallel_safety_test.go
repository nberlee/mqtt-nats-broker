package mqtt

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

// Which tests may run in parallel is a property of the process, not of the test.
//
// Most of this package's tests own everything they touch: testMQTTRunServer keys
// its embedded nats-server by a t.TempDir() store, so two tests never share a
// server, a stream or an account. What they do share is the process, and a
// handful of package-level variables live there: the mqttTest*HookV pointers, the
// suppression flags, and mqttOrphanSweepRuns, which testNewOrphanEnv reads as a
// barrier to wait out its own startup sweep. A test that installs a hook changes
// the behaviour of every test running beside it; a test that waits on a global
// counter is satisfied by somebody else's increment and proceeds before its own
// work finished.
//
// Go resolves that for us if the classification is right: top-level tests that do
// not call t.Parallel() all run to completion before the parallel ones resume, so
// leaving the global-state tests serial keeps them from ever overlapping. This
// test is what keeps the classification right, because both halves of it rot
// silently — a new global, or an existing helper that starts installing a hook,
// turns a correct t.Parallel() into a race nobody edited.
//
// It fails on the reachable direction only: a parallel test that can touch
// process-global state. The other direction — a serial test that could be
// parallel — costs time, not correctness, so it is reported and not enforced.
func TestParallelTestsTouchNoProcessGlobalState(t *testing.T) {
	fset := token.NewFileSet()

	atomics, declared, err := packageGlobalState(fset, ".")
	if err != nil {
		t.Fatalf("scanning package globals: %v", err)
	}
	// Both halves of the scan have found real state; an empty result means the
	// scan broke, not that the package became clean.
	if len(atomics) == 0 || len(declared) == 0 {
		t.Fatalf("found no process-global state at all (%d atomics, %d package vars); "+
			"the scan is broken, not the package", len(atomics), len(declared))
	}

	funcs, err := testFuncGraph(fset, ".", atomics, declared)
	if err != nil {
		t.Fatalf("building the test call graph: %v", err)
	}

	var violations []string
	for name, f := range funcs {
		if !strings.HasPrefix(name, "Test") || !f.parallel {
			continue
		}
		if via, ok := reachesGlobalState(funcs, name); ok {
			violations = append(violations, f.file+": "+strings.Join(via, " -> "))
		}
	}
	sort.Strings(violations)

	if len(violations) > 0 {
		t.Fatalf("%d test(s) call t.Parallel() but can reach process-global state, so they can\n"+
			"corrupt or be corrupted by whatever runs beside them. Either drop the t.Parallel()\n"+
			"call, or give the state an owner that is not the process. The chain shown is the\n"+
			"path from the test to the global:\n\n%s",
			len(violations), strings.Join(violations, "\n"))
	}
}

// TestNoPackageGlobalIsAssignedFromAGoroutine closes the one hole the guard
// above cannot see.
//
// Allowing parallel tests to READ a plain global rests on every writer being
// serial, and on Go finishing the serial tests — and their subtests — before the
// parallel ones resume. Go waits for tests and subtests; it does not wait for
// whatever goroutines they launched. A write that escapes into a `go` statement
// can therefore land after its serial test returned, while the parallel readers
// are running, and no amount of classification would notice: the assignment is
// attributed to a test that is already over.
//
// The check follows calls rather than only inspecting the literal go body — `go
// helper()` where helper assigns a global escapes identically — and it peels
// index, star and selector targets to their root, so `globalMap[k] = v` and
// `*alias = v` count too. Taking a global's address counts as a write, because
// the capability outlives the expression.
//
// There is no exemption mechanism, on purpose. An earlier version had one — a
// comment marker at the site saying why a particular goroutine's write was
// joined before the test returned — and it was true when written and unverified
// forever after: nothing checked that the restore still preceded the signal the
// test waited on. The one site that needed it was restructured instead, so the
// install and restore happen on the test's own goroutine. Moving the write is
// cheap; a claim nothing checks is not.
//
// What this does NOT catch, stated so nobody mistakes it for a proof. It is
// syntactic, so an alias that crosses a function boundary defeats it:
// `launch(&global)` where `func launch(p *T) { go func() { *p = v }() }` splits
// the address-taking from the `go` between two functions, and a reference-type
// capture such as `m := globalMap` needs no address at all. Closing those needs
// type-resolved or SSA analysis. Treat this as a lint that raises the cost of
// the mistakes people actually make — every instance that existed in this
// package was found by it and fixed — not as a guarantee that none is possible.
func TestNoPackageGlobalIsAssignedFromAGoroutine(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	atomics, declared, err := packageGlobalState(fset, ".")
	if err != nil {
		t.Fatalf("scanning package globals: %v", err)
	}
	funcs, err := testFuncGraph(fset, ".", atomics, declared)
	if err != nil {
		t.Fatalf("building the test call graph: %v", err)
	}

	paths, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatalf("globbing test files: %v", err)
	}

	var escaped []string
	for _, path := range paths {
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		// Every function that hands out a write capability to a package global by
		// taking its address. A local alias defeats any amount of peeling —
		// `p := &global` then `go func() { *p = v }()` roots at p, which is not
		// a global — so the address-taking itself is what disqualifies the
		// function's goroutines. There is deliberately no exemption comment:
		// an exemption is a claim that stops being true the moment somebody
		// edits nearby, and nothing checks it.
		aliased := map[*ast.FuncDecl]string{}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				u, ok := n.(*ast.UnaryExpr)
				if !ok || u.Op != token.AND {
					return true
				}
				if id, ok := u.X.(*ast.Ident); ok && declared[id.Name] {
					aliased[fd] = id.Name
				}
				return true
			})
		}

		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			g, isAliased := aliased[fd]
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				gs, ok := n.(*ast.GoStmt)
				if !ok {
					return true
				}
				line := fset.Position(gs.Pos()).Line
				where := fmt.Sprintf("%s:%d", path, line)
				if isAliased {
					escaped = append(escaped, where+
						" is launched by a function that took the address of "+g)
				}
				ast.Inspect(gs, func(inner ast.Node) bool {
					switch x := inner.(type) {
					case *ast.AssignStmt:
						for _, e := range x.Lhs {
							if g := assignedGlobalRoot(e, declared); g != "" {
								escaped = append(escaped, where+" assigns "+g)
							}
						}
					case *ast.IncDecStmt:
						if g := assignedGlobalRoot(x.X, declared); g != "" {
							escaped = append(escaped, where+" assigns "+g)
						}
					case *ast.UnaryExpr:
						if x.Op == token.AND {
							if id, ok := x.X.(*ast.Ident); ok && declared[id.Name] {
								escaped = append(escaped, where+" takes the address of "+id.Name)
							}
						}
					case *ast.CallExpr:
						// The write does not have to be written here. `go helper()`
						// where helper assigns a global escapes exactly the same way,
						// so every callee is followed through the graph.
						var name string
						switch fun := x.Fun.(type) {
						case *ast.Ident:
							name = fun.Name
						case *ast.SelectorExpr:
							name = fun.Sel.Name
						}
						if name == "" {
							return true
						}
						if via, ok := reachesGlobalState(funcs, name); ok {
							escaped = append(escaped, where+" calls "+strings.Join(via, " -> "))
						}
					}
					return true
				})
				return true
			})
		}
	}
	sort.Strings(escaped)
	escaped = slices.Compact(escaped)

	if len(escaped) > 0 {
		t.Fatalf("%d package-global assignment(s) escape into a goroutine, so they can land\n"+
			"after the test that launched them returned — while the parallel tests that read\n"+
			"those globals are running. Keep the write in the test body, or give the state an\n"+
			"owner that is not the process:\n\n%s",
			len(escaped), strings.Join(escaped, "\n"))
	}
}

// parallelSafetyExemptMarker exempts one `go` statement from the escaped-writer
// check. It must be followed by the reason the write cannot outlive the test.
type testFunc struct {
	file     string
	calls    map[string]bool
	parallel bool   // calls t.Parallel() directly
	global   string // the process-global it touches directly, if any
}

// packageGlobalState collects the package-level variables in the production
// files that make a test parallel-hostile. Discovering them beats listing them:
// a global added next month is covered without anyone remembering this rule.
//
// Two kinds qualify, and the second was learned the hard way — seeding only on
// the first left every TestMQTTWillOutbox* test marked parallel-safe, and nine
// of them failed the moment the suite ran concurrently:
//
//   - REFERENCING anything declared with atomic. — the hook pointers, the
//     suppression flags, and mqttOrphanSweepRuns, whose value testNewOrphanEnv
//     reads as a barrier. Reading is enough here: these vars exist to be
//     observed, and an observation satisfied by somebody else's increment is
//     the bug.
//   - ASSIGNING any package-level var, declared in production code or in the
//     fixtures. These are the plain tuning knobs — mqttWillClaimInterval,
//     mqttSessJailDur, the fixtures' own testMQTTTimeout — which a test lowers
//     or widens, exercises and restores. Two of those save/restore pairs
//     interleaved is one test running under a timeout it never chose.
//
// Reading a plain global is deliberately NOT disqualifying, and the difference
// is load-bearing: testMQTTTimeout is read by testMQTTRead and testMQTTWrite,
// so treating a read as unsafe marks essentially every test in the package
// unsafe and gives up the whole point. What makes reading safe is that every
// ASSIGNER is serial, and Go runs the serial tests to completion before the
// parallel ones resume — so a parallel reader can only ever see the restored
// value. That argument holds exactly as long as this guard keeps the assigners
// out of the parallel set.
func packageGlobalState(fset *token.FileSet, dir string) (atomics, declared map[string]bool, err error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, nil, err
	}

	atomics = map[string]bool{}
	declared = map[string]bool{}
	for _, path := range paths {
		// Test files are scanned too. A global is process-global wherever it is
		// declared, and the fixtures own some of the most contended ones —
		// testMQTTTimeout is saved, widened and restored by several tests, so
		// skipping _test.go declarations left five parallel tests racing on it
		// with the guard reporting clean.
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return nil, nil, err
		}
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				var b strings.Builder
				if vs.Type != nil {
					_ = printer.Fprint(&b, fset, vs.Type)
				}
				for _, v := range vs.Values {
					_ = printer.Fprint(&b, fset, v)
				}
				isAtomic := strings.Contains(b.String(), "atomic.")
				for _, n := range vs.Names {
					declared[n.Name] = true
					if isAtomic {
						atomics[n.Name] = true
					}
				}
			}
		}
	}
	return atomics, declared, nil
}

// testFuncGraph parses every test file in the package — all build tags, because a
// helper defined under one tag is still the same process-global state — and
// records, per function, what it calls and whether it touches a global directly.
func testFuncGraph(fset *token.FileSet, dir string, atomics, declared map[string]bool) (map[string]*testFunc, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		return nil, err
	}
	funcs := map[string]*testFunc{}
	for _, path := range paths {
		src, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			return nil, err
		}
		base := filepath.Base(path)
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			// Methods are in the graph too, keyed by their bare name, which is
			// what a selector call records. Skipping them left a hole wide
			// enough to drive a fixture through: testDeferredEnv's park helper
			// writes mqttTestSessStoreSuppressV, and a test calling it would
			// have been declared parallel-safe because the call was recorded
			// while the body was nowhere in the graph.
			cur := &testFunc{file: base, calls: map[string]bool{}}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					switch fun := x.Fun.(type) {
					case *ast.Ident:
						cur.calls[fun.Name] = true
						if isProcessGlobalSetter(fun.Name) {
							cur.global = fun.Name
						}
					case *ast.SelectorExpr:
						cur.calls[fun.Sel.Name] = true
						if id, ok := fun.X.(*ast.Ident); ok && id.Name == "t" && fun.Sel.Name == "Parallel" {
							cur.parallel = true
						}
					}
				case *ast.AssignStmt:
					// Writing any package-level var is disqualifying, however
					// innocuous the var looks.
					for _, e := range x.Lhs {
						if g := assignedGlobalRoot(e, declared); g != "" {
							cur.global = g
						}
					}
				case *ast.IncDecStmt:
					if g := assignedGlobalRoot(x.X, declared); g != "" {
						cur.global = g
					}
				case *ast.UnaryExpr:
					// Taking a global's address hands out a write capability the
					// scan cannot follow, so it counts as a write here.
					if x.Op == token.AND {
						if id, ok := x.X.(*ast.Ident); ok && declared[id.Name] {
							cur.global = id.Name
						}
					}
				case *ast.Ident:
					// Merely naming an atomic is disqualifying; they exist to be
					// observed across goroutines.
					if atomics[x.Name] {
						cur.global = x.Name
					}
				}
				return true
			})
			// The same helper can be declared under two build tags. Keep the
			// union, so a global reachable through either declaration is never
			// lost to whichever file was parsed last.
			if prev, ok := funcs[fd.Name.Name]; ok {
				for c := range cur.calls {
					prev.calls[c] = true
				}
				prev.parallel = prev.parallel || cur.parallel
				if prev.global == "" {
					prev.global = cur.global
				}
				continue
			}
			funcs[fd.Name.Name] = cur
		}
	}
	return funcs, nil
}

// assignedGlobalRoot returns the package-level variable an assignment target
// ultimately writes, or empty.
//
// The target is not always a bare identifier, and each of these forms mutates
// shared state exactly as much as `global = x` does:
//
//	global[key] = v    an IndexExpr rooted at the global
//	*alias = v         a StarExpr, where alias was taken from the global
//	global.field = v   a SelectorExpr rooted at the global
//
// Peeling to the root identifier is syntactic and therefore conservative in the
// right direction: it can only ever attribute a write to more state than a type
// checker would, never to less.
func assignedGlobalRoot(e ast.Expr, declared map[string]bool) string {
	for {
		switch x := e.(type) {
		case *ast.Ident:
			if declared[x.Name] {
				return x.Name
			}
			return ""
		case *ast.ParenExpr:
			e = x.X
		case *ast.IndexExpr:
			e = x.X
		case *ast.StarExpr:
			e = x.X
		case *ast.SelectorExpr:
			e = x.X
		default:
			return ""
		}
	}
}

// isProcessGlobalSetter names the installers that write a package-level hook on
// their caller's behalf. Seventeen helpers in this package call one of these, so
// the reachability below has to be transitive: a test that names no setter can
// still hold one for its whole run.
func isProcessGlobalSetter(name string) bool {
	return strings.HasPrefix(name, "setMQTTTest") || name == "setMQTTSessionExpirySweepInterval"
}

// reachesGlobalState returns the shortest call chain from name to a function that
// touches process-global state, so a failure names the path rather than only the
// verdict.
func reachesGlobalState(funcs map[string]*testFunc, start string) ([]string, bool) {
	type node struct {
		name string
		path []string
	}
	seen := map[string]bool{start: true}
	queue := []node{{name: start, path: []string{start}}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		f, ok := funcs[cur.name]
		if !ok {
			continue
		}
		if f.global != "" {
			return append(cur.path, f.global), true
		}
		callees := make([]string, 0, len(f.calls))
		for c := range f.calls {
			callees = append(callees, c)
		}
		sort.Strings(callees) // a deterministic chain in the failure message
		for _, c := range callees {
			if seen[c] {
				continue
			}
			seen[c] = true
			queue = append(queue, node{name: c, path: append(append([]string{}, cur.path...), c)})
		}
	}
	return nil, false
}
