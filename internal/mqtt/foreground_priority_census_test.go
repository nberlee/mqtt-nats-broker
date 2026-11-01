// The background-actor census behind the foreground allowance.
//
// A foreground authority operation that announces itself loses at most one attempt to each
// background actor of its client registered at the announcement (its allowance), so its derived
// budget is sound only while these facts hold: every background admission belongs to a
// registered actor — one of the five session workers, or a recovery visit — each worker registers
// at its entry and runs single-flight per session, admitting one attempt at a time, and every
// authority transition recovery can submit is admitted per attempt or classified as one of the
// stated exclusions. None of that is visible at the budget's use site, so these tests pin it
// against the source.
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

// fgCensusRegistration is the first statement of every session worker: it registers with its
// client's coordinator for its whole run.
const fgCensusRegistration = "defer sess.registerForegroundActor()()"

// TestForegroundCensusEveryActorRegisters: each of the five session workers registers as its very
// first statement, and the recovery visit registers before its entry check, which precedes all of
// its root work.
func TestForegroundCensusEveryActorRegisters(t *testing.T) {
	t.Parallel()
	funcs := fgCensusParse(t)
	for root := range fgCensusRoots {
		var found []fgCensusFunc
		for k, f := range funcs {
			if strings.HasSuffix(k, "(mqttSession)."+root) {
				found = append(found, f)
			}
		}
		if len(found) != 1 {
			t.Errorf("found %d declarations of the worker %s, want one", len(found), root)
			continue
		}
		body := found[0].fd.Body.List
		if len(body) == 0 || fgCensusRender(found[0].fset, body[0]) != fgCensusRegistration {
			t.Errorf("the worker %s does not register with its client's coordinator as its first "+
				"statement: an unregistered worker's pre-announcement attempt is outside every allowance", root)
		}
	}
	visit, ok := funcs["outbound_recovery_pass.go:(mqttAccountSessionManager).visitRecoveryRoot"]
	if !ok {
		t.Fatal("visitRecoveryRoot no longer exists")
	}
	var got []string
	for _, st := range visit.fd.Body.List {
		got = append(got, fgCensusRender(visit.fset, st))
	}
	want := []string{
		"if h := mqttTestRecoveryRootVisitHookV.Load(); h != nil { (*h)() }",
		"defer as.registerForegroundActor(root.ID)()",
		"b.refused = false",
		"defer func() { refused, b.refused = b.refused, false }()",
		"if as.foregroundAnnounced(root.ID) { b.refused = true return }",
		"as.visitAdmittedRecoveryRoot(s, root, rootSeq, b, stop)",
		"return",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("the recovery visit's registration and entry check changed; re-read them against the "+
			"allowance before re-pinning.\n got:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
	// THE ENTRY CHECK PRECEDES EVERY ROOT WORK: the visit past it is reached from nowhere else.
	sc := censusScanPackage(t)
	for _, c := range sc.Calls {
		if c.Callee == "visitAdmittedRecoveryRoot" && c.Fn.String() != "outbound_recovery_pass.go:(mqttAccountSessionManager).visitRecoveryRoot" {
			t.Errorf("%s reaches a recovery visit past its entry check without making it", c.Fn)
		}
		if c.Callee == "visitRecoveryRoot" && c.Fn.String() != "outbound_recovery_pass.go:(mqttAccountSessionManager).walkRecoveryShard" &&
			c.Fn.String() != "outbound_recovery_kick.go:(mqttAccountSessionManager).visitKickedRoot" {
			t.Errorf("%s visits a recovery root from an unclassified caller, which must resume a refused visit", c.Fn)
		}
	}
}

// fgCensusRecoveryAdmissions are the recovery visit's admission sites, with the statement that
// admits, rendered: one per duty step, before the step's first read, and one per floor advancement.
var fgCensusRecoveryAdmissions = map[string]string{
	"outbound_duty_exec.go:(mqttAccountSessionManager).executeRecoveryDuty":  "if !as.admitRecoveryAttempt(root.ID, b) { return true }",
	"outbound_duty_exec.go:(mqttAccountSessionManager).concludeRecoveryRoot": "admit := func() bool { return as.admitRecoveryAttempt(root.ID, b) }",
}

// fgCensusRecoveryTransitions classifies every function a recovery visit can reach — by name, over
// the whole package, so a superset of what it does reach — that submits a transition
// (commitOp, commitAuthorityTransition): how its attempt is admitted, or why it needs none.
var fgCensusRecoveryTransitions = map[string]string{
	"outbound_ops.go:(mqttJSA).commitOp":                                     "the funnel itself: every submitter below reaches the store through it",
	"outbound_discard.go:(mqttJSA).opDiscardExchange":                        "DUTY STEP: admitted by executeRecoveryDuty, one attempt",
	"outbound_duty_ops.go:(mqttJSA).completeCleanupAndResolve":               "DUTY STEP: admitted by executeRecoveryDuty, one attempt (also the reclamation's step)",
	"outbound_duty_ops.go:(mqttJSA).opResolveDuty":                           "DUTY STEP: admitted by executeRecoveryDuty, one attempt; its exchange arm is the authority-free hold release",
	"outbound_duty_ops.go:(mqttJSA).opSealAffineProvisioning":                "DUTY STEP: admitted by executeRecoveryDuty, one attempt",
	"outbound_ops.go:(mqttJSA).opAuthorizeDeletion":                          "DUTY STEP: admitted by executeRecoveryDuty, one attempt (also the reclamation's step)",
	"outbound_ops.go:(mqttJSA).opSealProvisioning":                           "DUTY STEP: the reclamation's seal, admitted by executeRecoveryDuty, one attempt",
	"outbound_ops.go:(mqttJSA).commitResourceStep":                           "DUTY STEP: the disposal and dependency release, admitted by executeRecoveryDuty, one attempt",
	"outbound_retained_ops.go:(mqttJSA).opCloseRetainedManifest":             "DUTY STEP: the reclamation's manifest close, admitted by executeRecoveryDuty, one attempt",
	"outbound_material_delete.go:(mqttAccountSessionManager).stepPurgeCount": "DUTY STEP: the reclamation's purge count, admitted by executeRecoveryDuty, one attempt",
	"outbound_floor.go:(mqttJSA).opAdvanceFloor":                             "FLOOR: admitted immediately before it, after the position deletes (deleteRetiredPositionsUpTo's admit)",
	"outbound_release_holds.go:(mqttJSA).releaseHoldsOnce":                   "AUTHORITY-FREE bookkeeping (mqttTransitionReleaseHolds): writes no authority; its re-attempts in opReleaseHolds stay inside the admitted duty step",
	"shared_release_fence.go:(mqttJSA).opFenceSharedRelease":                 "SHARED-LEDGER: the shared closing's release fence, on the group's ledger, not the client's authority",
}

// fgCensusExclusions are the background authority writers that stand OUTSIDE the admission, each
// stated in outbound_foreground_priority.go; the census pins that each still exists, so a rename
// cannot silently drop one from the stated list.
var fgCensusExclusions = map[string]string{
	"sess_teardown.go:(mqttAccountSessionManager).finishAdoptedTeardown":        "the adopted teardown's completion: the one recovery exclusion",
	"pubrel_action.go:(mqttSession).startActionDrain":                           "the retained PUBREL action drain's completion",
	"outbound_disposal.go:(mqttDisposal).step":                                  "the denied delivery's disposal timer step",
	"outbound_connect_settle.go:(mqttAccountSessionManager).abandonOwnProposal": "abandonOwnProposal's asynchronous settlement",
	"sess_clear.go:(mqttSession).settleDiscardAuthority":                        "record-driven expiry's clear",
}

// TestForegroundCensusRecoveryAdmitsEveryAttempt: the recovery admission is made exactly at its
// pinned sites, and every transition a recovery visit can submit is classified.
func TestForegroundCensusRecoveryAdmitsEveryAttempt(t *testing.T) {
	t.Parallel()
	funcs := fgCensusParse(t)
	sc := censusScanPackage(t)
	got := sc.FunctionsReferencing("admitRecoveryAttempt")
	for k := range got {
		want, ok := fgCensusRecoveryAdmissions[k.String()]
		if !ok {
			t.Errorf("%s admits a recovery attempt at an unpinned site", k)
			continue
		}
		f := funcs[k.String()]
		found := false
		ast.Inspect(f.fd.Body, func(x ast.Node) bool {
			if st, ok := x.(ast.Stmt); ok && fgCensusRender(f.fset, st) == want {
				found = true
			}
			return !found
		})
		if !found {
			t.Errorf("%s no longer admits with the pinned statement %q", k, want)
		}
	}
	for k := range fgCensusRecoveryAdmissions {
		if !got[censusFuncKey{File: strings.SplitN(k, ":", 2)[0], Recv: "mqttAccountSessionManager",
			Name: k[strings.LastIndex(k, ".")+1:]}] {
			t.Errorf("pinned recovery admission site %s no longer admits", k)
		}
	}

	byName := map[string][]censusFuncKey{}
	for k := range sc.Funcs {
		byName[k.Name] = append(byName[k.Name], k)
	}
	callees := map[censusFuncKey][]string{}
	for _, c := range sc.Calls {
		callees[c.Fn] = append(callees[c.Fn], c.Callee)
	}
	seen := map[censusFuncKey]bool{}
	queue := append([]censusFuncKey(nil), byName["visitAdmittedRecoveryRoot"]...)
	if len(queue) != 1 {
		t.Fatalf("found %d declarations of visitAdmittedRecoveryRoot, want one", len(queue))
	}
	for len(queue) > 0 {
		k := queue[0]
		queue = queue[1:]
		if seen[k] {
			continue
		}
		seen[k] = true
		for _, n := range callees[k] {
			queue = append(queue, byName[n]...)
		}
	}
	submitters := map[string]bool{}
	for k := range seen {
		for _, n := range callees[k] {
			if n == "commitOp" || n == "commitAuthorityTransition" {
				submitters[k.String()] = true
			}
		}
	}
	var unclassified, stale []string
	for k := range submitters {
		if _, ok := fgCensusRecoveryTransitions[k]; !ok {
			unclassified = append(unclassified, k)
		}
	}
	for k := range fgCensusRecoveryTransitions {
		if !submitters[k] {
			stale = append(stale, k)
		}
	}
	sort.Strings(unclassified)
	sort.Strings(stale)
	if len(unclassified) > 0 {
		t.Errorf("transition submitters a recovery visit can reach that no admission or exclusion "+
			"accounts for (a submitter that retries is admitted per attempt): %v", unclassified)
	}
	if len(stale) > 0 {
		t.Errorf("fgCensusRecoveryTransitions names functions a recovery visit no longer reaches: %v", stale)
	}
	for k := range fgCensusExclusions {
		if _, ok := funcs[k]; !ok {
			t.Errorf("the stated exclusion %s no longer exists; re-read the not-covered list", k)
		}
	}
}

// fgCensusTransientRoots are the functions that build a TRANSIENT session object outside CONNECT:
// one no connection holds and no worker of its own registers for. A transient object that
// reached an admission would admit against its client's coordinator without being in any
// announcement's allowance.
var fgCensusTransientRoots = []string{
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).condemnRetiredSurvivors",
	"outbound_duty_exec.go:(mqttAccountSessionManager).performAffineReadiness",
	"sess_expiry.go:(mqttAccountSessionManager).expireSessionFromRecord",
	"sess_teardown.go:(mqttAccountSessionManager).finishAdoptedTeardown",
}

// TestForegroundCensusNoTransientSessionAdmits: every function that builds a session object
// outside CONNECT is listed, and none reaches an admission except through a registered actor's
// root — a session worker, which registers at its own entry, or a recovery visit. The walk is by
// name over the whole package, a superset of what each root reaches.
func TestForegroundCensusNoTransientSessionAdmits(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	builders := map[string]bool{}
	for _, c := range sc.Calls {
		if c.Callee == "mqttSessionCreate" {
			builders[c.Fn.String()] = true
		}
	}
	listed := map[string]bool{}
	for _, r := range fgCensusTransientRoots {
		listed[r] = true
		if !builders[r] {
			t.Errorf("the transient root %s no longer builds a session object", r)
		}
	}
	for b := range builders {
		// THE CONNECT PATHS build the resident object a connection serves, whose workers
		// register; everything else is a transient root and must be listed.
		if !listed[b] && !strings.HasPrefix(b, "sess_manager.go:") {
			t.Errorf("%s builds a session object outside CONNECT and is not a listed transient root", b)
		}
	}
	byName := map[string][]censusFuncKey{}
	for k := range sc.Funcs {
		byName[k.Name] = append(byName[k.Name], k)
	}
	callees := map[censusFuncKey][]string{}
	for _, c := range sc.Calls {
		callees[c.Fn] = append(callees[c.Fn], c.Callee)
	}
	registered := map[string]bool{"visitRecoveryRoot": true}
	for root := range fgCensusRoots {
		registered[root] = true
	}
	for _, r := range fgCensusTransientRoots {
		seen := map[censusFuncKey]bool{}
		var queue []censusFuncKey
		for k := range sc.Funcs {
			if k.String() == r {
				queue = append(queue, k)
			}
		}
		for len(queue) > 0 {
			k := queue[0]
			queue = queue[1:]
			if seen[k] {
				continue
			}
			seen[k] = true
			if _, admits := fgCensusAdmissions[k.String()]; admits {
				t.Errorf("the transient root %s reaches the admission in %s without a registered actor", r, k)
			}
			for _, n := range callees[k] {
				if registered[n] {
					continue
				}
				queue = append(queue, byName[n]...)
			}
		}
	}
}
