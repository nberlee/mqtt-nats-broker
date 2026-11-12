// The background-actor census behind the foreground allowance.
//
// A foreground authority operation that announces itself loses at most one attempt to each
// background actor of its client registered at the announcement (its allowance), so its derived
// budget is sound only while these facts hold: every background admission belongs to a
// registered actor — one of the five session workers, or a background root registered for the
// work it admits (a recovery visit, the lifecycle sweep, a Will-anchor transition) — each worker
// registers at its entry and runs single-flight per session, admitting one attempt at a time, and
// every authority write a background root can reach is admitted on every attempt, writes no
// client authority, or is one of the stated exclusions. None of that is visible at the budget's use site, so these tests pin it
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
	"outbound_retained_replay_emit.go:(mqttSession).settleRetainedHandle":   "retained driver",
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

// THE BACKGROUND AUTHORITY WRITERS. Every write of a client's authority (a transition, a shared
// ledger batch, which carries the authority as its fence, or a record created beside it) that a
// background path makes is admitted against the client's coordinator on EVERY attempt, by an
// actor registered for the work, or is a stated exclusion. The tests below pin that against the
// source in four parts: the background roots and what they reach; the inventory of every
// function that calls an authority-writing funnel; where each admission is built and that it is
// registered for; and, per attempt, that an admission dominates the write it licenses.

// fgCensusFunnels are the calls that write a client's authority. Every authority write in the
// package goes through one of them.
var fgCensusFunnels = map[string]bool{
	"commitOp":                   true,
	"commitAuthorityTransition":  true,
	"commitSharedLifecycleBatch": true,
	"createBesideAuthority":      true,
}

// fgCensusBackgroundRoots are the background entries that are not session workers (those admit
// through admitBackgroundAuthority and are pinned by fgCensusAdmissions): each runs on no
// connection's path and must admit every authority attempt it reaches.
var fgCensusBackgroundRoots = map[string]string{
	"visitAdmittedRecoveryRoot": "a recovery visit past its entry check, registered by visitRecoveryRoot",
	"sweepLifecycles":           "the lifecycle sweep, on the account ticker",
	"runAnchorRemovals":         "the account's owed Will-anchor removal loop",
	"scanWillOutbox":            "the delayed-Will outbox scan",
	"dischargeOwedDelayedWills": "the delayed-Will retry pass",
	"discardOwedDelayedWills":   "shutdown's last delayed-Will pass",
	"sweepAcqReleaseVerdicts":   "the shared-verdict sweep, on the account ticker",
	"runVerdictKick":            "a session's prompt verdict execution, off the connection's path",
	"runLifecycleKick":          "a client's prompt lifecycle drive after a landed release or a reopen",
}

// fgCensusWalkResolutions narrow one call edge of the by-name walk to the receiver type the call
// is actually made on. The walk follows a call to EVERY function of that name, which keeps it a
// superset only of what it can resolve; a name shared by many methods makes it reach the whole
// package and classify nothing. Each entry only narrows, names the receiver whose method is
// called, and is checked to still apply. Calls through function values are the walk's other
// blind spot: a closure built in a root is attributed to it (collectBody descends into function
// literals), and the func-typed parameters and fields the walk cannot follow are the admissions
// themselves (admit, cw.admit) and nil-in-production test seams.
var fgCensusWalkResolutions = map[[2]string]string{
	// mqttInitiateMsgDeliveryJSA issues the Will's publication and reports it: the complete it
	// calls is the issued publication's, not any of the package's other complete methods.
	{"mqttInitiateMsgDeliveryJSA", "complete"}: "mqttIssued",
}

// fgCensusAuthorityWriters classifies EVERY production function that calls an authority-writing
// funnel, by how its attempt is admitted when a background root reaches it. A function a root
// reaches must be FUNNEL, ADMITTED or NO CLIENT AUTHORITY; FOREGROUND and SESSION WORKER entries
// are writers no background root may reach, and EXCLUDED ones are stated in fgCensusExclusions.
var fgCensusAuthorityWriters = map[string]string{
	"outbound_ops.go:(mqttJSA).commitOp": "FUNNEL: every op submitter reaches the store through it",

	"generation_retirement.go:(mqttJSA).retireGeneration":                    "ADMITTED: by the admit it is handed, before every control (fgCensusAdmittedAttempts)",
	"lifecycle_retirement.go:(mqttJSA).retireLifecycle":                      "ADMITTED: by the admit it is handed, before its one fence (fgCensusAdmittedAttempts)",
	"teardown_claim.go:(mqttAccountSessionManager).releaseTeardownEntry":     "ADMITTED: by the admit it is handed, before every CAS attempt; the teardown's own release passes nil (fgCensusUnadmittedCalls)",
	"accept_abandon.go:(mqttAccountSessionManager).abandonPendingAcceptance": "ADMITTED: by the admit it is handed, before its one attempt",
	"will_anchor_ops.go:(mqttAccountSessionManager).runAnchorTransition":     "ADMITTED: by the admit it is handed, before every attempt; background callers go through runBackgroundAnchorTransition",
	"generation_boundary.go:(mqttJSA).createFencedGenRecordOnce":             "ADMITTED: one attempt of createFencedGenRecord, whose admit is asked before it in its loop",
	"shared_lifecycle_ops.go:(mqttJSA).sealSharedLedger":                     "ADMITTED: by the closing's cw.admits before every seal attempt",
	"shared_lifecycle_ops.go:(mqttJSA).executeSharedRelease":                 "ADMITTED: by the closing's cw.admits before every close and release attempt",
	"outbound_floor.go:(mqttJSA).opAdvanceFloor":                             "ADMITTED: one advancement of deleteRetiredPositionsUpTo, whose admit is asked before it",
	"outbound_discard.go:(mqttJSA).opDiscardExchange":                        "ADMITTED: a duty step, admitted by executeRecoveryDuty, one attempt",
	"outbound_duty_ops.go:(mqttJSA).completeCleanupAndResolve":               "ADMITTED: a duty step, admitted by executeRecoveryDuty, one attempt (also the reclamation's step)",
	"outbound_duty_ops.go:(mqttJSA).opResolveDuty":                           "ADMITTED: a duty step, admitted by executeRecoveryDuty, one attempt; its exchange arm is the authority-free hold release",
	"outbound_duty_ops.go:(mqttJSA).opSealAffineProvisioning":                "ADMITTED: a duty step, admitted by executeRecoveryDuty, one attempt",
	"outbound_ops.go:(mqttJSA).opAuthorizeDeletion":                          "ADMITTED: a duty step, admitted by executeRecoveryDuty, one attempt (also the reclamation's step)",
	"outbound_ops.go:(mqttJSA).opSealProvisioning":                           "ADMITTED: the reclamation's seal, a duty step admitted by executeRecoveryDuty, one attempt",
	"outbound_ops.go:(mqttJSA).commitResourceStep":                           "ADMITTED: the disposal and dependency release, a duty step admitted by executeRecoveryDuty, one attempt",
	"outbound_retained_ops.go:(mqttJSA).opCloseRetainedManifest":             "ADMITTED: the reclamation's manifest close, a duty step admitted by executeRecoveryDuty, one attempt",
	"outbound_material_delete.go:(mqttAccountSessionManager).stepPurgeCount": "ADMITTED: the reclamation's purge count, a duty step admitted by executeRecoveryDuty, one attempt",
	"shared_release_fence.go:(mqttJSA).opFenceSharedRelease":                 "ADMITTED: the release fence writes only while its generation SERVES, and a recovery visit reaches it only past a discarding closure, where executeSharedRelease's step 1b skips it",

	"outbound_release_holds.go:(mqttJSA).releaseHoldsOnce":      "NO CLIENT AUTHORITY: the authority-free hold release (mqttTransitionReleaseHolds); its re-attempts stay inside the admitted duty step",
	"shared_lifecycle_ops.go:(mqttJSA).retireSharedIncarnation": "NO CLIENT AUTHORITY: the retiring incarnation's group-only batch carries no ledger, so no authority member",

	"accept_drive.go:(mqttSession).bindAcceptance":                            "FOREGROUND: the CONNECT's bind, announced",
	"accept_drive.go:(mqttSession).resolveAcceptance":                         "FOREGROUND: the CONNECT's acceptance, announced by acceptOwnership",
	"sess_persist.go:(mqttSession).commitAcceptShape":                         "FOREGROUND: the CONNECT's acceptance batch, announced by acceptOwnership",
	"sess_teardown.go:(mqttSession).claimTeardown":                            "FOREGROUND: the connection's own teardown claim (destroyReleasing); the adopted teardown is the stated exclusion",
	"shared_lifecycle_ops.go:(mqttJSA).installSharedTenure":                   "FOREGROUND: the CONNECT's and the first acquisition's tenure installation",
	"shared_lifecycle_ops.go:(mqttSession).acquireShared":                     "FOREGROUND: a shared delivery's acquisition, on the delivery path",
	"shared_lifecycle_ops.go:(mqttSession).abortSharedAcquisition":            "FOREGROUND: a shared delivery's acquisition abort, on the delivery path",
	"outbound_ops.go:(mqttJSA).opAcquireActions":                              "FOREGROUND: the announced delivery acquisition",
	"outbound_ops.go:(mqttJSA).opAdmitPublish":                                "FOREGROUND: the announced ordinary admission ladder",
	"outbound_ops.go:(mqttJSA).opAdmitPublishes":                              "FOREGROUND: the announced admission batch",
	"outbound_ops.go:(mqttJSA).opReplacePublish":                              "FOREGROUND: the announced admission ladder's replacement",
	"outbound_ops.go:(mqttJSA).opProvisionResource":                           "FOREGROUND: the announced SUBSCRIBE and resume name claims",
	"outbound_ack_advance.go:(mqttJSA).opAdvanceExchangeAck":                  "FOREGROUND: the announced PUBACK, PUBREC and PUBCOMP advances",
	"outbound_ack_advance.go:(mqttJSA).opCompleteExchangeDrain":               "FOREGROUND: the PUBREL holder's drain completion; the retained action drain is the stated exclusion",
	"outbound_pubrel_rematerialize.go:(mqttJSA).opRebindCanonical":            "FOREGROUND: the PUBREL rematerialization on its connection's path",
	"outbound_abandon_delivery.go:(mqttJSA).opAbandonDeniedDelivery":          "EXCLUDED: the denied delivery's disposal timer step (fgCensusExclusions)",
	"outbound_transition.go:(mqttJSA).abortFencedOnce":                        "SESSION WORKER: one attempt of abortTransition, admitted by the holder's own admission before every attempt (consult 12); foreground holders pass nil",
	"outbound_withdraw_unsent.go:(mqttJSA).opTombstoneRegister":               "SESSION WORKER: admitted by the withdrawal licence's admit",
	"outbound_withdraw_unsent.go:(mqttJSA).opWithdrawUnsent":                  "SESSION WORKER: admitted by the withdrawal licence's admit",
	"pending_fence_disposal.go:(mqttSession).fenceBeforePendingFenceDisposal": "SESSION WORKER: the register reap's fence, on the session's own reap path",
	"outbound_duty_ops.go:(mqttJSA).opResolveDuties":                          "SESSION WORKER: the custody dispatcher's floor unit, admitted by tryAdmitBackgroundAuthority (fgCensusAdmissions)",
	"outbound_retained_ops.go:(mqttJSA).opAdmitRetained":                      "SESSION WORKER: the retained driver, admitted by admitBackgroundAuthority (fgCensusAdmissions)",
	"outbound_retained_ops.go:(mqttJSA).opReserveRetainedPayload":             "SESSION WORKER: the retained driver, admitted by admitBackgroundAuthority (fgCensusAdmissions)",
	"outbound_retained_ops.go:(mqttJSA).opSealRetainedPayload":                "SESSION WORKER: the retained driver, admitted by admitBackgroundAuthority (fgCensusAdmissions)",
	"outbound_retained_ops.go:(mqttJSA).opWriteRetainedChunks":                "SESSION WORKER: the retained driver, admitted by admitBackgroundAuthority (fgCensusAdmissions)",
}

// fgCensusBackgroundSafe are the classes a background root may reach.
var fgCensusBackgroundSafe = []string{"FUNNEL:", "ADMITTED:", "NO CLIENT AUTHORITY:"}

// fgCensusAdmissionSite is where a background admission is BUILT: the statement, rendered, and
// the registration that must precede it in the same block (a registered actor spans the work it
// admits), or the registered root that registers for it.
type fgCensusAdmissionSite struct {
	admission    string
	registration []string
	registeredBy string
}

// fgCensusBackgroundAdmissions are every site that builds a background admission against a
// client's coordinator. The recovery visit's are registered by visitRecoveryRoot (pinned in
// TestForegroundCensusEveryActorRegisters); the others register for exactly the work they admit.
var fgCensusBackgroundAdmissions = map[string]fgCensusAdmissionSite{
	"outbound_duty_exec.go:(mqttAccountSessionManager).executeRecoveryDuty": {
		admission: "if !as.admitRecoveryAttempt(root.ID, b) { return true }", registeredBy: "visitRecoveryRoot"},
	"outbound_duty_exec.go:(mqttAccountSessionManager).concludeRecoveryRoot": {
		admission: "admit := func() bool { return as.admitRecoveryAttempt(root.ID, b) }", registeredBy: "visitRecoveryRoot"},
	"outbound_duty_exec.go:(mqttAccountSessionManager).retireRecoveryRoot": {
		admission: "admit := func() bool { return as.admitRecoveryAttempt(root.ID, b) }", registeredBy: "visitRecoveryRoot"},
	"outbound_duty_exec.go:(mqttAccountSessionManager).performAffineReadiness": {
		admission: "admit := func() bool { return as.admitRecoveryAttempt(root.ID, b) }", registeredBy: "visitRecoveryRoot"},
	"outbound_recovery_shared_closing.go:(mqttAccountSessionManager).stepSharedClosing": {
		admission: "cw.admit = func() bool { return as.admitRecoveryAttempt(root.ID, b) }", registeredBy: "visitRecoveryRoot"},
	"lifecycle_retirement.go:(mqttAccountSessionManager).sweepLifecycles": {
		admission:    "admit := func() bool { _, _, idle := as.admitForegroundOrIdle(id) return idle == nil }",
		registration: []string{"deregister := as.registerForegroundActor(id)", "defer deregister()"}},
	"shared_lifecycle_sweep.go:(mqttAccountSessionManager).sweepAcqReleaseVerdicts": {
		admission:    "admit := func() bool { _, _, idle := as.admitForegroundOrIdle(clientID) return idle == nil }",
		registration: []string{"deregister := as.registerForegroundActor(clientID)", "defer deregister()"}},
	"shared_verdict_kick.go:(mqttSession).executeDurableVerdicts": {
		admission:    "admit := func() bool { return as.admitForegroundOrWait(id, quit) }",
		registration: []string{"deregister := as.registerForegroundActor(id)", "defer deregister()"}},
	"lifecycle_retirement.go:(mqttAccountSessionManager).runLifecycleKickOnce": {
		admission:    "admit := func() bool { return as.admitForegroundOrWait(id, quit) }",
		registration: []string{"deregister := as.registerForegroundActor(id)", "defer deregister()"}},
	"will_anchor_ops.go:(mqttAccountSessionManager).runBackgroundAnchorTransition": {
		admission:    "admit := func() bool { _, _, idle := as.admitForegroundOrIdle(clientID) return idle == nil }",
		registration: []string{"deregister := as.registerForegroundActor(clientID)", "defer deregister()"}},
}

// fgCensusAdmissionPrimitives reference the coordinator's admission as its definition, not as a
// site: the recovery form and the session workers' forms (pinned by fgCensusAdmissions).
var fgCensusAdmissionPrimitives = map[string]string{
	"outbound_foreground_priority.go:(mqttAccountSessionManager).admitRecoveryAttempt":  "the recovery visit's non-waiting admission",
	"outbound_foreground_priority.go:(mqttSession).admitBackgroundAuthority":            "the session workers' admission",
	"outbound_foreground_priority.go:(mqttSession).tryAdmitBackgroundAuthority":         "the session workers' non-waiting admission",
	"outbound_foreground_priority.go:(mqttAccountSessionManager).admitForegroundOrWait": "a registered background actor's waiting admission (the verdict kick)",
}

// fgCensusAdmittedAttempt names, for a function that admits per attempt, the admission call and
// the attempt it must dominate.
type fgCensusAdmittedAttempt struct {
	admit   string // the called identifier or selector: admit, admits, admitRecoveryAttempt
	attempt string // the call each admission licenses
}

// fgCensusAdmittedAttempts are the functions whose every attempt must be dominated by an
// admission: walking out from each attempt call, an earlier statement of an enclosing statement
// list holds the admission, no further out than the innermost loop body that contains the call.
// Asked once outside the retry loop does not count.
var fgCensusAdmittedAttempts = map[string]fgCensusAdmittedAttempt{
	"generation_retirement.go:(mqttJSA).retireGeneration":                    {"admit", "commitAuthorityTransition"},
	"lifecycle_retirement.go:(mqttJSA).retireLifecycle":                      {"admit", "commitAuthorityTransition"},
	"teardown_claim.go:(mqttAccountSessionManager).releaseTeardownEntry":     {"admit", "commitAuthorityTransition"},
	"accept_abandon.go:(mqttAccountSessionManager).abandonPendingAcceptance": {"admit", "commitAuthorityTransition"},
	"will_anchor_ops.go:(mqttAccountSessionManager).runAnchorTransition":     {"admit", "commitAuthorityTransition"},
	"generation_boundary.go:(mqttJSA).createFencedGenRecord":                 {"admit", "createFencedGenRecordOnce"},
	"outbound_floor.go:(mqttJSA).deleteRetiredPositionsUpTo":                 {"admit", "opAdvanceFloor"},
	"shared_lifecycle_ops.go:(mqttJSA).sealSharedLedger":                     {"admits", "commitSharedLifecycleBatch"},
	"shared_lifecycle_ops.go:(mqttJSA).executeSharedRelease":                 {"admits", "commitSharedLifecycleBatch"},
	"shared_release_fence.go:(mqttJSA).fenceSharedRelease":                   {"admit", "opFenceSharedRelease"},
}

// fgCensusAdmitParams are the dual-use helpers: they admit by what they are handed, at this
// argument position, so the caller decides the mode. A call passes the admission it built or was
// handed (admit, cw), or nil (or a contention without an admission) at a site
// fgCensusUnadmittedCalls lists, which no background root may reach.
var fgCensusAdmitParams = map[string]int{
	"runAnchorTransition":        4,
	"releaseTeardownEntry":       3,
	"abandonPendingAcceptance":   4,
	"retireGeneration":           3,
	"retireLifecycle":            2,
	"createFencedGenRecord":      6,
	"publishGenCompletionCert":   1,
	"publishAffineReadiness":     1,
	"deleteRetiredPositionsStep": 3,
	"deleteRetiredPositionsUpTo": 4,
	"sealSharedLedger":           3,
	"executeSharedRelease":       4,
	"releaseSealedAcquisitions":  4,
	"fenceSharedRelease":         3,
	"driveLifecycleRetirement":   3,
}

// fgCensusAdmitForwarders are the functions that hand on an admission they were handed.
var fgCensusAdmitForwarders = map[string]bool{
	"publishGenCompletionCert":   true,
	"publishAffineReadiness":     true,
	"deleteRetiredPositionsStep": true,
	"releaseSealedAcquisitions":  true,
	"driveLifecycleRetirement":   true,
}

// fgCensusUnadmittedCalls are the calls of a dual-use helper that pass no admission, "caller ->
// helper", each with the route that makes it not background: FOREGROUND (a connection's own path),
// SESSION WORKER (admitted before the call by the worker's admission) or EXCLUDED
// (fgCensusExclusions).
var fgCensusUnadmittedCalls = map[string]string{
	"will_disposition.go:(Server).mqttHandleWill -> runAnchorTransition":                                         "FOREGROUND: the closing connection owes its own Will's anchor",
	"sess_uncertain.go:(Server).dischargeDeferredWill -> runAnchorTransition":                                    "FOREGROUND: the deferred disconnect owes its own Will's anchor, on its session's resolution",
	"sess_teardown.go:(mqttSession).releaseTeardown -> releaseTeardownEntry":                                     "FOREGROUND: the teardown's own release; the adopted teardown is the stated exclusion",
	"sess_teardown_partition.go:(mqttAccountSessionManager).finishAffinePrerequisites -> publishAffineReadiness": "FOREGROUND: the teardown's own affine prerequisites; the adopted teardown is the stated exclusion",
	"outbound_floor_unit.go:(mqttSession).runFloorUnit -> deleteRetiredPositionsUpTo":                            "SESSION WORKER: the floor unit, admitted by tryAdmitBackgroundAuthority immediately before",
	"shared_lifecycle_ops.go:(mqttSession).executeOwnVerdicts -> executeSharedRelease":                           "FOREGROUND: a connection's own release verdicts, on its own path",
	"shared_lifecycle_ops.go:(mqttJSA).closeSharedGeneration -> sealSharedLedger":                                "FOREGROUND: the teardown's inline shared closing, with its own contention allowance",
	"shared_lifecycle_ops.go:(mqttJSA).closeSharedGeneration -> releaseSealedAcquisitions":                       "FOREGROUND: the teardown's inline shared closing, with its own contention allowance",
}

// fgCensusExclusions are the background authority writers that stand OUTSIDE the admission, each
// stated in outbound_foreground_priority.go; the census pins that each still exists, so a rename
// cannot silently drop one from the stated list.
var fgCensusExclusions = map[string]string{
	"sess_teardown.go:(mqttAccountSessionManager).finishAdoptedTeardown": "the adopted teardown's completion: the one recovery exclusion",
	"pubrel_action.go:(mqttSession).startActionDrain":                    "the retained PUBREL action drain's completion",
	"outbound_disposal.go:(mqttDisposal).step":                           "the denied delivery's disposal timer step",
	"sess_clear.go:(mqttSession).settleDiscardAuthority":                 "record-driven expiry's clear",
}

// fgCensusCallName is the called identifier: a selector's field, or a bare identifier.
func fgCensusCallName(c *ast.CallExpr) string {
	switch fn := c.Fun.(type) {
	case *ast.SelectorExpr:
		return fn.Sel.Name
	case *ast.Ident:
		return fn.Name
	}
	return ""
}

// fgCensusHasCall reports whether n calls name, not looking inside function literals: a closure
// runs when it is called, not where it is written.
func fgCensusHasCall(n ast.Node, name string) bool {
	found := false
	ast.Inspect(n, func(x ast.Node) bool {
		if _, lit := x.(*ast.FuncLit); lit {
			return false
		}
		if c, ok := x.(*ast.CallExpr); ok && fgCensusCallName(c) == name {
			found = true
		}
		return !found
	})
	return found
}

// fgCensusAdmitsUnconditionally reports whether s asks admit on every path through it: in an
// if's init or condition (the guard `if !admit() { return }`), or anywhere in a simple
// statement. An admission inside a branch body, a loop or a switch asks it on some paths only.
func fgCensusAdmitsUnconditionally(s ast.Stmt, admit string) bool {
	switch v := s.(type) {
	case *ast.IfStmt:
		return (v.Init != nil && fgCensusHasCall(v.Init, admit)) || fgCensusHasCall(v.Cond, admit)
	case *ast.ExprStmt, *ast.AssignStmt, *ast.DeclStmt, *ast.ReturnStmt:
		return fgCensusHasCall(v, admit)
	}
	return false
}

// fgCensusUndominated counts the calls to attempt in f and returns those no admission dominates.
// From the call outward, an earlier statement of an enclosing statement list must ask admit on
// every path through it (fgCensusAdmitsUnconditionally), and the walk ends at the innermost enclosing loop or function literal: an admission outside the
// retry loop is asked once, not per attempt.
func fgCensusUndominated(f fgCensusFunc, admit, attempt string) (int, []string) {
	n := 0
	var bad []string
	var stack []ast.Node
	dominated := func() bool {
		for i := len(stack) - 2; i >= 0; i-- {
			var list []ast.Stmt
			switch v := stack[i].(type) {
			case *ast.FuncLit, *ast.ForStmt, *ast.RangeStmt:
				return false
			case *ast.BlockStmt:
				list = v.List
			case *ast.CaseClause:
				list = v.Body
			case *ast.CommClause:
				list = v.Body
			}
			for _, s := range list {
				if ast.Node(s) == stack[i+1] {
					break
				}
				if fgCensusAdmitsUnconditionally(s, admit) {
					return true
				}
			}
		}
		return false
	}
	ast.Inspect(f.fd.Body, func(x ast.Node) bool {
		if x == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		stack = append(stack, x)
		if c, ok := x.(*ast.CallExpr); ok && fgCensusCallName(c) == attempt {
			n++
			if !dominated() {
				bad = append(bad, fgCensusRender(f.fset, c))
			}
		}
		return true
	})
	return n, bad
}

// fgCensusBlockWith finds the statement list in f (function literals included) holding a
// statement that renders as want, and that statement's index.
func fgCensusBlockWith(f fgCensusFunc, want string) ([]ast.Stmt, int) {
	var list []ast.Stmt
	at := -1
	ast.Inspect(f.fd.Body, func(x ast.Node) bool {
		if at >= 0 {
			return false
		}
		if b, ok := x.(*ast.BlockStmt); ok {
			for i, s := range b.List {
				if fgCensusRender(f.fset, s) == want {
					list, at = b.List, i
					return false
				}
			}
		}
		return true
	})
	return list, at
}

// fgCensusWalk is the by-name call graph with fgCensusWalkResolutions applied.
type fgCensusWalk struct {
	byName  map[string][]censusFuncKey
	callees map[censusFuncKey][]string
}

func fgCensusNewWalk(t *testing.T, sc *censusScan) *fgCensusWalk {
	t.Helper()
	w := &fgCensusWalk{byName: map[string][]censusFuncKey{}, callees: map[censusFuncKey][]string{}}
	for k := range sc.Funcs {
		w.byName[k.Name] = append(w.byName[k.Name], k)
	}
	for _, c := range sc.Calls {
		w.callees[c.Fn] = append(w.callees[c.Fn], c.Callee)
	}
	for edge, recv := range fgCensusWalkResolutions {
		calls, declared := false, false
		for _, c := range sc.Calls {
			if c.Fn.Name == edge[0] && c.Callee == edge[1] {
				calls = true
			}
		}
		for _, k := range w.byName[edge[1]] {
			if k.Recv == recv {
				declared = true
			}
		}
		if !calls || !declared {
			t.Errorf("the walk resolution %s -> (%s).%s no longer applies (calls %v, declared %v)",
				edge[0], recv, edge[1], calls, declared)
		}
	}
	return w
}

// reach is every function reachable from the functions named root.
func (w *fgCensusWalk) reach(root string) map[censusFuncKey]bool {
	seen := map[censusFuncKey]bool{}
	queue := append([]censusFuncKey(nil), w.byName[root]...)
	for len(queue) > 0 {
		k := queue[0]
		queue = queue[1:]
		if seen[k] {
			continue
		}
		seen[k] = true
		for _, n := range w.callees[k] {
			recv, narrowed := fgCensusWalkResolutions[[2]string{k.Name, n}]
			for _, c := range w.byName[n] {
				if !narrowed || c.Recv == recv {
					queue = append(queue, c)
				}
			}
		}
	}
	return seen
}

// fgCensusBackgroundReach is the union of what every background root reaches.
func fgCensusBackgroundReach(t *testing.T, sc *censusScan) map[censusFuncKey]bool {
	t.Helper()
	w := fgCensusNewWalk(t, sc)
	all := map[censusFuncKey]bool{}
	for root := range fgCensusBackgroundRoots {
		if n := len(w.byName[root]); n != 1 {
			t.Errorf("found %d declarations of the background root %s, want one", n, root)
		}
		for k := range w.reach(root) {
			all[k] = true
		}
	}
	return all
}

// TestForegroundCensusBackgroundWritersAreAdmitted: every function calling an authority-writing
// funnel is classified, and every one a background root reaches is admitted per attempt, writes
// no client authority, or is the funnel itself.
func TestForegroundCensusBackgroundWritersAreAdmitted(t *testing.T) {
	t.Parallel()
	funcs := fgCensusParse(t)
	sc := censusScanPackage(t)
	writers := map[string]bool{}
	for _, c := range sc.Calls {
		if fgCensusFunnels[c.Callee] {
			writers[c.Fn.String()] = true
		}
	}
	var unclassified, stale []string
	for k := range writers {
		if _, ok := fgCensusAuthorityWriters[k]; !ok {
			unclassified = append(unclassified, k)
		}
	}
	for k := range fgCensusAuthorityWriters {
		if !writers[k] {
			stale = append(stale, k)
		}
	}
	sort.Strings(unclassified)
	sort.Strings(stale)
	if len(unclassified) > 0 {
		t.Errorf("authority writers no classification accounts for (a new writer states how it is "+
			"admitted, or why no background root reaches it): %v", unclassified)
	}
	if len(stale) > 0 {
		t.Errorf("fgCensusAuthorityWriters names functions that no longer write an authority: %v", stale)
	}
	for k := range fgCensusBackgroundReach(t, sc) {
		class, ok := fgCensusAuthorityWriters[k.String()]
		if !ok {
			continue
		}
		safe := false
		for _, p := range fgCensusBackgroundSafe {
			if strings.HasPrefix(class, p) {
				safe = true
			}
		}
		if !safe {
			t.Errorf("a background root reaches %s, classified %q: it writes the client's authority "+
				"without a per-attempt admission", k, class)
		}
	}
	for k := range fgCensusExclusions {
		if _, ok := funcs[k]; !ok {
			t.Errorf("the stated exclusion %s no longer exists; re-read the not-covered list", k)
		}
	}
}

// TestForegroundCensusBackgroundAdmissionsAreRegistered: every reference to the coordinator's
// admission is a pinned site or a primitive; each site builds exactly its pinned admission, and
// the sites that admit against admitForegroundOrIdle register for the client just before, in the
// same block, so the actor spans the work it admits.
func TestForegroundCensusBackgroundAdmissionsAreRegistered(t *testing.T) {
	t.Parallel()
	funcs := fgCensusParse(t)
	sc := censusScanPackage(t)
	refs := sc.FunctionsReferencing("admitRecoveryAttempt")
	for k := range sc.FunctionsReferencing("admitForegroundOrIdle") {
		refs[k] = true
	}
	for k := range sc.FunctionsReferencing("admitForegroundOrWait") {
		refs[k] = true
	}
	for k := range refs {
		_, site := fgCensusBackgroundAdmissions[k.String()]
		_, prim := fgCensusAdmissionPrimitives[k.String()]
		if !site && !prim {
			t.Errorf("%s admits against the coordinator at an unpinned site", k)
		}
	}
	for k := range fgCensusAdmissionPrimitives {
		if !refs[censusFuncKeyOf(k)] {
			t.Errorf("the admission primitive %s no longer references the coordinator's admission", k)
		}
	}
	for k, want := range fgCensusBackgroundAdmissions {
		f, ok := funcs[k]
		if !ok || !refs[censusFuncKeyOf(k)] {
			t.Errorf("pinned background admission site %s no longer admits", k)
			continue
		}
		list, at := fgCensusBlockWith(f, want.admission)
		if at < 0 {
			t.Errorf("%s no longer admits with the pinned statement %q", k, want.admission)
			continue
		}
		if want.registeredBy != _EMPTY_ {
			continue
		}
		var before []string
		for _, s := range list[:at] {
			before = append(before, fgCensusRender(f.fset, s))
		}
		joined := "\n" + strings.Join(before, "\n") + "\n"
		pos := 0
		for _, r := range want.registration {
			i := strings.Index(joined[pos:], "\n"+r+"\n")
			if i < 0 {
				t.Errorf("%s admits without registering first in the same block (%q missing before %q): "+
					"an unregistered actor's attempt is outside every allowance", k, r, want.admission)
				break
			}
			pos += i + len(r) + 1
		}
	}
}

// TestForegroundCensusAdmissionDominatesEveryAttempt: in every function that admits per attempt,
// each attempt is dominated by its admission within its own loop iteration; and every recovery
// duty step is admitted before the step's first call.
func TestForegroundCensusAdmissionDominatesEveryAttempt(t *testing.T) {
	t.Parallel()
	funcs := fgCensusParse(t)
	for k, want := range fgCensusAdmittedAttempts {
		f, ok := funcs[k]
		if !ok {
			t.Errorf("the admitted function %s no longer exists", k)
			continue
		}
		n, bad := fgCensusUndominated(f, want.admit, want.attempt)
		if n == 0 {
			t.Errorf("%s no longer calls %s, so its admission pins nothing", k, want.attempt)
		}
		for _, b := range bad {
			t.Errorf("%s sends %s with no %s dominating it in the same attempt: an admission asked "+
				"once, or after the write, admits nothing per attempt", k, b, want.admit)
		}
	}
	// THE DUTY STEP: its one attempt follows the admission, which is the first statement that
	// calls anything.
	f, ok := funcs["outbound_duty_exec.go:(mqttAccountSessionManager).executeRecoveryDuty"]
	if !ok {
		t.Fatal("executeRecoveryDuty no longer exists")
	}
	want := fgCensusBackgroundAdmissions["outbound_duty_exec.go:(mqttAccountSessionManager).executeRecoveryDuty"].admission
	for _, s := range f.fd.Body.List {
		calls := false
		ast.Inspect(s, func(x ast.Node) bool {
			if _, ok := x.(*ast.CallExpr); ok {
				calls = true
			}
			return !calls
		})
		if !calls {
			continue
		}
		if got := fgCensusRender(f.fset, s); got != want {
			t.Errorf("executeRecoveryDuty's first statement that calls anything is %q, not its admission %q", got, want)
		}
		break
	}
}

// TestForegroundCensusDualUseHelpersAreCalledInTheirMode: every call of a helper that admits by
// what it is handed passes the admission its caller built or was handed, or is a listed
// unadmitted call that no background root reaches (an exclusion excepted).
func TestForegroundCensusDualUseHelpersAreCalledInTheirMode(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	reach := fgCensusBackgroundReach(t, sc)
	seen := map[string]bool{}
	for _, c := range sc.Calls {
		idx, ok := fgCensusAdmitParams[c.Callee]
		if !ok {
			continue
		}
		caller := c.Fn.String()
		if idx >= len(c.Args) {
			t.Errorf("%s calls %s with %d argument(s); its admission is argument %d", caller, c.Callee, len(c.Args), idx)
			continue
		}
		arg := c.Args[idx]
		_, builds := fgCensusBackgroundAdmissions[caller]
		_, admitted := fgCensusAdmittedAttempts[caller]
		forwards := fgCensusAdmitForwarders[c.Fn.Name] || admitted
		switch {
		case (arg == "admit" || arg == "cw" || arg == "cw.admits") && (builds || forwards):
			// cw.admits is the contention's own admission, handed on as a function.
			continue
		case arg == "admit":
			t.Errorf("%s hands %s an admit it neither built at a pinned site nor was handed", caller, c.Callee)
			continue
		}
		key := caller + " -> " + c.Callee
		seen[key] = true
		reason, listed := fgCensusUnadmittedCalls[key]
		if !listed {
			t.Errorf("%s passes %s no admission (%s) at an unlisted site: a background caller must hand "+
				"it the admission it built; a foreground one is listed with its route", caller, c.Callee, arg)
			continue
		}
		if reach[c.Fn] && !strings.HasPrefix(reason, "EXCLUDED:") {
			t.Errorf("%s passes %s no admission, and a background root reaches it", caller, c.Callee)
		}
	}
	for k := range fgCensusUnadmittedCalls {
		if !seen[k] {
			t.Errorf("fgCensusUnadmittedCalls names a call that no longer passes no admission: %s", k)
		}
	}
}

// censusFuncKeyOf parses a rendered function key back.
func censusFuncKeyOf(s string) censusFuncKey {
	file, rest, _ := strings.Cut(s, ":")
	if strings.HasPrefix(rest, "(") {
		recv, name, _ := strings.Cut(rest[1:], ").")
		return censusFuncKey{File: file, Recv: recv, Name: name}
	}
	return censusFuncKey{File: file, Name: rest}
}

// fgCensusTransientRoots are the functions that build a TRANSIENT session object outside CONNECT:
// one no connection holds and no worker of its own registers for. A transient object that
// reached an admission would admit against its client's coordinator without being in any
// announcement's allowance.
var fgCensusTransientRoots = []string{
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
