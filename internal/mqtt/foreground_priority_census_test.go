// The background-writer census behind mqttBackgroundAuthorityWriters.
//
// A foreground authority operation that announces itself loses at most one attempt to each
// background writer that was admitted before the announcement, so its budget is sound only
// while two facts hold: every background admission belongs to one of the counted writers, and
// each counted writer runs single-flight per session, admitting one attempt at a time. Neither
// fact is visible at the budget's use site, so these tests pin both against the source.
//
// What they cannot do is prove the writers are single-flight. They pin the guard each launcher
// runs today, rendered, so that an edit to a guard — or a new launch site, or a new admission —
// fails here and has to be re-read against the bound before it is re-pinned.

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

// fgCensusAdmissions maps every production function that references admitBackgroundAuthority (or
// tryAdmitBackgroundAuthority) to the counted writers that reach it. One function may serve several: transmitRetained is the
// retained driver's and the continuations' send, and the foreground resume path calls it WITHOUT
// the admission.
var fgCensusAdmissions = map[string]string{
	"exclusion_reclaim.go:(mqttSession).runExclusionReclaim":                "exclusion reclaim",
	"outbound_floor_unit.go:(mqttSession).floorHorizonStep":                 "custody dispatcher (floor unit)",
	"outbound_floor_unit.go:(mqttSession).runFloorUnit":                     "custody dispatcher (floor unit)",
	"outbound_retained_abandon.go:(mqttSession).stepRetainedAbandonment":    "retained continuations",
	"outbound_retained_reclaim.go:(mqttSession).runRetainedReclaim":         "reclaimer (retained reclaim, source retirement)",
	"outbound_retained_replay.go:(mqttSession).stepRetainedOwed":            "retained continuations",
	"outbound_retained_replay_emit.go:(mqttSession).admitRetainedCopyOnce":  "retained driver",
	"outbound_retained_replay_emit.go:(mqttSession).closeAbandonedMaterial": "retained driver",
	"outbound_retained_replay_emit.go:(mqttSession).runRetainedStep":        "retained driver",
	"outbound_retained_replay_emit.go:(mqttSession).transmitRetained":       "retained driver, retained continuations",
	"outbound_retained_withdraw.go:(mqttSession).withdrawRetainedOnce":      "retained driver, retained continuations",
}

// fgCensusRoots are the counted writers' goroutine roots, with the field that is each one's
// single-flight flag. There are mqttBackgroundAuthorityWriters of them.
var fgCensusRoots = map[string]string{
	"runRetainedDriver":        "running",
	"runRetainedContinuations": "contRunning",
	"runRetainedReclaim":       "running",
	"runCustodyDispatcher":     "custodyObserving",
	"runExclusionReclaim":      "exclReclaimArmed",
}

// fgCensusLaunchers pins, for every function that calls a root, the guard it runs today:
// the rendered statements that read or set the root's flag, every lock and unlock, every `start`
// assignment and `if start`, and the launch itself. An `if` whose condition names the flag is
// rendered whole.
var fgCensusLaunchers = map[string][]string{
	"exclusion_reclaim.go:(mqttSession).armExclusionReclaimLocked": {
		"if sess.exclReclaimArmed { return }",
		"sess.exclReclaimArmed = true",
		"time.AfterFunc(mqttExclusionReclaimDelay, func() { sess.runExclusionReclaim(mqttExclusionReclaimDelay) })",
	},
	"exclusion_reclaim.go:(mqttSession).runExclusionReclaim": nil, // its tail; see fgCensusSelfLaunch
	"outbound_custody_dispatcher.go:(mqttSession).startCustodyDispatcher": {
		"sess.mu.Lock()",
		"if sess.custodyObserving || sess.custody == nil || sess.jsa == nil || sess.asm == nil { sess.mu.Unlock() return }",
		"sess.mu.Unlock()",
		"sess.mu.Lock()",
		"if sess.custodyObserving || sess.gen != gen { sess.mu.Unlock() if lease != nil { lease.release() } if admitted { srv.willWorkerWG.Done() } return }",
		"sess.custodyObserving = true",
		"sess.mu.Unlock()",
		"go sess.runCustodyDispatcher(srv, admitted, lease, gen, epoch)",
	},
	"outbound_retained_reclaim.go:(mqttRetainedReclaim).wake": {
		"rc.mu.Lock()",
		"start := !rc.running",
		"rc.running = true",
		"rc.mu.Unlock()",
		"if start { go sess.runRetainedReclaim() }",
	},
	"outbound_retained_replay.go:(mqttSession).oweRetained": {
		"d.mu.Lock()",
		"d.mu.Unlock()",
		"start := d.startContinuationLocked()",
		"d.mu.Unlock()",
		"if start { go sess.runRetainedContinuations() }",
	},
	"outbound_retained_replay.go:(mqttSession).ownRetainedAbandonment": {
		"d.mu.Lock()",
		"d.mu.Unlock()",
		"start := d.startContinuationLocked()",
		"d.mu.Unlock()",
		"if start { go sess.runRetainedContinuations() }",
	},
	"outbound_retained_replay.go:(mqttSession).wakeRetainedDriver": {
		"d.mu.Lock()",
		"start := !d.running",
		"d.running = true",
		"d.mu.Unlock()",
		"if start { go sess.runRetainedDriver() }",
	},
	"outbound_source_retire.go:(mqttSession).handSourceRetirement": {
		"rc.mu.Lock()",
		"start := !rc.running",
		"rc.running = true",
		"rc.mu.Unlock()",
		"if start { go sess.runRetainedReclaim() }",
	},
}

// fgCensusHelpers pins whole bodies of the small guard helpers the launchers delegate to.
var fgCensusHelpers = map[string]string{
	"exclusion_reclaim.go:(mqttSession).disarmExclusionReclaimLocked": "{ sess.exclReclaimArmed = false }",
	"outbound_retained_replay.go:(mqttRetainedDriver).startContinuationLocked": "{ if d.kick == nil { d.kick = make(chan struct{}, 1) } " +
		"if d.contRunning { select { case d.kick <- struct{}{}: default: } return false } d.contRunning = true return true }",
}

type fgCensusFunc struct {
	fd   *ast.FuncDecl
	fset *token.FileSet
}

func fgCensusParse(t *testing.T) map[string]fgCensusFunc {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	fset := token.NewFileSet()
	out := map[string]fgCensusFunc{}
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || filepath.Ext(n) != ".go" || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", n, err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			key := censusFuncKey{File: n, Recv: censusRecvType(fd), Name: fd.Name.Name}
			out[key.String()] = fgCensusFunc{fd: fd, fset: fset}
		}
	}
	if len(out) < 100 {
		t.Fatalf("parsed only %d functions: the working directory is wrong", len(out))
	}
	return out
}

func fgCensusRender(fset *token.FileSet, n ast.Node) string {
	var sb strings.Builder
	if err := printer.Fprint(&sb, fset, n); err != nil {
		return "<unrenderable>"
	}
	return strings.Join(strings.Fields(sb.String()), " ")
}

// fgCensusMentions reports whether n reads or writes a field named flag, or the identifier start.
func fgCensusMentions(n ast.Node, flag string) bool {
	found := false
	ast.Inspect(n, func(x ast.Node) bool {
		switch v := x.(type) {
		case *ast.SelectorExpr:
			if v.Sel.Name == flag {
				found = true
			}
		case *ast.Ident:
			if v.Name == "start" {
				found = true
			}
		}
		return !found
	})
	return found
}

// fgCensusLocks reports whether s is a lock or unlock of a mutex. The check-and-set is single-flight
// only under its lock, so where the lock is taken and released is part of the guard.
func fgCensusLocks(s ast.Stmt) bool {
	es, ok := s.(*ast.ExprStmt)
	if !ok {
		return false
	}
	c, ok := es.X.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	switch sel.Sel.Name {
	case "Lock", "Unlock", "RLock", "RUnlock":
		return true
	}
	return false
}

// fgCensusCallsRoot reports whether n calls one of the roots.
func fgCensusCallsRoot(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(x ast.Node) bool {
		if c, ok := x.(*ast.CallExpr); ok {
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
				if _, root := fgCensusRoots[sel.Sel.Name]; root {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

// fgCensusGuard renders a launcher's guard: in source order, every `if` whose condition names the
// flag or start (whole), every lock and unlock, and every other simple statement that names the
// flag or start or calls a root.
func fgCensusGuard(f fgCensusFunc, flag string) []string {
	var out []string
	var walk func(stmts []ast.Stmt)
	walk = func(stmts []ast.Stmt) {
		for _, s := range stmts {
			switch v := s.(type) {
			case *ast.IfStmt:
				if fgCensusMentions(v.Cond, flag) {
					out = append(out, fgCensusRender(f.fset, v))
					continue
				}
				walk(v.Body.List)
				if v.Else != nil {
					if b, ok := v.Else.(*ast.BlockStmt); ok {
						walk(b.List)
					} else {
						walk([]ast.Stmt{v.Else})
					}
				}
			case *ast.BlockStmt:
				walk(v.List)
			case *ast.ForStmt:
				walk(v.Body.List)
			case *ast.RangeStmt:
				walk(v.Body.List)
			case *ast.SwitchStmt:
				for _, c := range v.Body.List {
					walk(c.(*ast.CaseClause).Body)
				}
			case *ast.SelectStmt:
				for _, c := range v.Body.List {
					walk(c.(*ast.CommClause).Body)
				}
			default:
				if fgCensusMentions(s, flag) || fgCensusCallsRoot(s) || fgCensusLocks(s) {
					out = append(out, fgCensusRender(f.fset, s))
				}
			}
		}
	}
	walk(f.fd.Body.List)
	return out
}

// fgCensusTail is f cut to the top-level statements from its last Lock to its end.
func fgCensusTail(f fgCensusFunc) fgCensusFunc {
	list := f.fd.Body.List
	from := 0
	for i, s := range list {
		if fgCensusLocks(s) && strings.HasSuffix(fgCensusRender(f.fset, s), ".Lock()") {
			from = i
		}
	}
	fd := *f.fd
	fd.Body = &ast.BlockStmt{List: list[from:]}
	return fgCensusFunc{fd: &fd, fset: f.fset}
}

// fgCensusRootOf names the root a launcher starts.
func fgCensusRootOf(f fgCensusFunc) string {
	root := ""
	ast.Inspect(f.fd.Body, func(x ast.Node) bool {
		if c, ok := x.(*ast.CallExpr); ok {
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
				if _, isRoot := fgCensusRoots[sel.Sel.Name]; isRoot {
					root = sel.Sel.Name
				}
			}
		}
		return true
	})
	return root
}

// TestForegroundCensusCountsTheWriters pins the count the budget is built on.
func TestForegroundCensusCountsTheWriters(t *testing.T) {
	t.Parallel()
	if len(fgCensusRoots) != mqttBackgroundAuthorityWriters {
		t.Fatalf("the census names %d background writers but mqttBackgroundAuthorityWriters is %d",
			len(fgCensusRoots), mqttBackgroundAuthorityWriters)
	}
	if mqttForegroundAcquireAttempts <= mqttBackgroundAuthorityWriters {
		t.Fatalf("a foreground acquisition's budget (%d) must exceed the attempts the background "+
			"writers can take from it (%d)", mqttForegroundAcquireAttempts, mqttBackgroundAuthorityWriters)
	}
}

// TestForegroundCensusEveryAdmissionIsAWriters: every reference to admitBackgroundAuthority or
// its non-waiting form tryAdmitBackgroundAuthority (the dispatcher units' admission) is in a
// function the census attributes to a counted writer, and every attributed function still has one.
func TestForegroundCensusEveryAdmissionIsAWriters(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	got := sc.FunctionsReferencing("admitBackgroundAuthority")
	for k, v := range sc.FunctionsReferencing("tryAdmitBackgroundAuthority") {
		got[k] = v
	}
	var unclassified, stale []string
	for k := range got {
		if _, ok := fgCensusAdmissions[k.String()]; !ok {
			unclassified = append(unclassified, k.String())
		}
	}
	for k := range fgCensusAdmissions {
		found := false
		for g := range got {
			if g.String() == k {
				found = true
			}
		}
		if !found {
			stale = append(stale, k)
		}
	}
	sort.Strings(unclassified)
	sort.Strings(stale)
	if len(unclassified) > 0 {
		t.Errorf("background admissions no counted writer accounts for (a new writer raises "+
			"mqttBackgroundAuthorityWriters; a new site of a counted one is added to fgCensusAdmissions): %v", unclassified)
	}
	if len(stale) > 0 {
		t.Errorf("fgCensusAdmissions names functions that no longer admit: %v", stale)
	}
}

// TestForegroundCensusWritersAreLaunchedSingleFlight: every call to a writer's root is in a pinned
// launcher, and every launcher runs exactly its pinned guard.
func TestForegroundCensusWritersAreLaunchedSingleFlight(t *testing.T) {
	t.Parallel()
	funcs := fgCensusParse(t)
	sc := censusScanPackage(t)
	launchers := map[string]bool{}
	for _, c := range sc.Calls {
		if _, root := fgCensusRoots[c.Callee]; root {
			launchers[c.Fn.String()] = true
		}
	}
	for k := range launchers {
		if _, ok := fgCensusLaunchers[k]; !ok {
			t.Errorf("%s starts a background writer at an unpinned site: a second launch site can run "+
				"a second instance and take a second attempt from every foreground budget", k)
		}
	}
	for k, want := range fgCensusLaunchers {
		f, ok := funcs[k]
		if !ok {
			t.Errorf("pinned launcher %s no longer exists", k)
			continue
		}
		if !launchers[k] {
			t.Errorf("pinned launcher %s no longer starts a writer", k)
			continue
		}
		root := fgCensusRootOf(f)
		flag := fgCensusRoots[root]
		if k == "exclusion_reclaim.go:(mqttSession).runExclusionReclaim" {
			// The chain's own TAIL, from its last top-level lock: it never names the armed flag. It
			// re-arms only while an exclusion is held, and otherwise disarms
			// (disarmExclusionReclaimLocked, pinned below) and returns.
			flag, want = "anyExclusionHeldLocked", fgCensusSelfLaunch
			f = fgCensusTail(f)
		}
		got := fgCensusGuard(f, flag)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("the single-flight guard of %s (root %s) changed; re-read it against "+
				"mqttBackgroundAuthorityWriters before re-pinning.\n got:\n  %s\nwant:\n  %s",
				k, root, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
		}
	}
	for k, want := range fgCensusHelpers {
		f, ok := funcs[k]
		if !ok {
			t.Errorf("pinned guard helper %s no longer exists", k)
			continue
		}
		if got := fgCensusRender(f.fset, f.fd.Body); got != want {
			t.Errorf("the guard helper %s changed; re-read it against mqttBackgroundAuthorityWriters "+
				"before re-pinning.\n got: %s\nwant: %s", k, got, want)
		}
	}
}

// fgCensusSelfLaunch is exclusion reclaim's self-rescheduling tail: it re-arms only while the
// armed flag it shares with armExclusionReclaimLocked stands.
var fgCensusSelfLaunch = []string{
	"sess.mu.Lock()",
	"if !sess.anyExclusionHeldLocked() { sess.disarmExclusionReclaimLocked() sess.mu.Unlock() return }",
	"sess.mu.Unlock()",
	"time.AfterFunc(next, func() { sess.runExclusionReclaim(next) })",
}
