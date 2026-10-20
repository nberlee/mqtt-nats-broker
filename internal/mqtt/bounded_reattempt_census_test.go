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

package mqtt

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// THE BOUNDED RE-ATTEMPT FAMILY, classified (F-114).
//
// mqttInflightStoreAttempts is three attempts with no spacing; mqttForegroundAcquireAttempts is one
// more than the attempts this broker's admitted background writers can take from an announced
// foreground operation (outbound_foreground_priority.go), spaced by the authority-contention wait.
// They are not interchangeable retry policies: a register delete, a walk restart or a fence read is
// not helped by more attempts, a background loop gains no priority from a larger budget, and a
// foreground authority transition that announces itself is bounded only against the PRE-admitted
// background attempts. So every loop on either budget is classified here, and this table is the one
// classification: what each row's attempt writes, whether this session's own background writers can
// exhaust it, what exhaustion costs, and what was done about it.
//
// The table pins, per function, how many times the body names each budget, every `for` and range
// header in it, and every binding of a local or parameter named budget, so a second loop inside a listed
// function fails here over any bound, and a new function on either budget fails as unlisted. Before
// re-pinning a row, re-classify it.
//
// Loops on their own named budgets (mqttPubRecAdvanceAttempts, mqttSubscribeClaimAttempts, the
// shared-lifecycle and teardown budgets, ...) are separate policies with their own analysis and are
// not this family.

type reattemptRow struct {
	store, fg int      // body references to mqttInflightStoreAttempts and mqttForegroundAcquireAttempts
	fors      []string // every for and range header, rendered
	budgets   []string // every assignment to an identifier named budget, rendered
	class     string
	exhausted string // what exhaustion costs, and whether this session's background writers can cause it
	done      string // CONVERTED, LEFT (why), or OWED (the follow-up roadmap item)
}

var reattemptCensus = map[string]reattemptRow{
	"inflight_marker.go:(mqttAccountSessionManager).deleteInflightMarker": {store: 1, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "REGISTER-DELETE: an exact-sequence marker delete, re-attempted on a store or transport error only",
		exhausted: "a gone sequence is success, so no writer causes a re-attempt; exhaustion leaves the send intent ambiguous and the entry held, and the next section re-attempts (one AckWait)",
		done:      "LEFT: contention cannot cause it, so spacing or budget would buy nothing"},
	"inflight_register.go:(mqttAccountSessionManager).deleteInflightRegisterConfirmed": {store: 1, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "REGISTER-DELETE: an exact-sequence register delete, re-attempted on a store error only",
		exhausted: "a gone sequence is success, so no writer causes a re-attempt; exhaustion refuses the reap and the caller's door continuation retries",
		done:      "LEFT: contention cannot cause it"},
	"inflight_register.go:(mqttAccountSessionManager).reapIncarnationRegisters": {store: 2, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++", "subject := range subjects", "start := 0; start < len(names); start += mqttInflightLoadBatch", "i, resp := range resps"}, budgets: nil,
		class:     "READ: re-samples the stream's watermark for a teardown; writes nothing",
		exhausted: "a zero watermark is a store reporting artifact, not contention; exhaustion is a teardown error and the marked record waits for a later pass",
		done:      "LEFT: no write to space"},
	"inflight_register.go:(mqttAccountSessionManager).releaseBoundInflightRegister": {store: 1, fg: 0, fors: []string{"attempt := 1; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "REGISTER-DELETE (foreground drain finalize and its background timer): re-reads after a delete that found the same identity rewritten",
		exhausted: "only a same-identity rewrite of that register re-enters it; exhaustion is Undecided and the drain finalize retries every 2 s without limit",
		done:      "LEFT: not an authority CAS loop; its continuation is unbounded in attempts"},
	"inflight_register.go:(mqttSession).acquirePublishAction": {store: 1, fg: 1, fors: []string{"attempt := 0; ; attempt++"}, budgets: []string{"budget := mqttInflightStoreAttempts", "budget = mqttForegroundAcquireAttempts"},
		class:     "FG-AUTHORITY (a live delivery's send-section acquisition, announced) and BG-AUTHORITY (the retained driver's own, admitted per attempt)",
		exhausted: "foreground: the admitted background writers cannot exhaust it, other causes re-offer at AckWait; background: its outer loop backs off 50 ms<<n eight times, then closes the connection and the resume replays",
		done:      "CONVERTED (foreground arm, 2026-10-16); background arm LEFT: a larger budget buys a background attempt no priority"},
	"inflight_register.go:(mqttSession).attemptTerminalRegisterRelease": {store: 0, fg: 1, fors: []string{"attempt := 0; ; attempt++"}, budgets: nil,
		class:     "FG-AUTHORITY: the PUBACK or refused PUBREC terminal advance, announced",
		exhausted: "the admitted background writers cannot exhaust it; other causes fall back to the continuation at mqttActionReconcileDelay",
		done:      "CONVERTED (2026-10-16)"},
	"inflight_register.go:(mqttSession).casInflightSlot": {store: 1, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "REGISTER-CAS: the shared standalone register CAS primitive, foreground and background callers",
		exhausted: "background authority writers reach it only on a retained delivery's own identifier; exhaustion returns false like a refusal and every caller has its own continuation or recovery owner",
		done:      "LEFT: not an authority transition"},
	"inflight_register.go:(mqttSession).drainPublishAction": {store: 1, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "REGISTER-CAS: the send section's drain, through casInflightSlot",
		exhausted: "rarely contended, by writers rewriting a tokened register (mostly acknowledgement advances); exhaustion enrols a 2 s reconcile without limit, the identifier reserved meanwhile",
		done:      "LEFT: register-level, unbounded continuation"},
	"inflight_register.go:(mqttSession).markInflightRegisterCancelled": {store: 1, fg: 1, fors: []string{"attempt := 0; attempt < budget; attempt++"}, budgets: []string{"budget := mqttInflightStoreAttempts", "budget = mqttForegroundAcquireAttempts"},
		class:     "FG-AUTHORITY, announced unless its licence admits each attempt: withdrawUnsent or the cancellation tombstone from teardown and cancellation paths (CONNECT, SUBSCRIBE, UNSUBSCRIBE, close, the deferred-disconnect discharge, settled uncertain writes, the resident sweep) and inside the announced admission ladder's cleanup",
		exhausted: "the admitted background writers cannot exhaust it; other brokers, recovery, unadmitted writes, foreground contention and store failures can, and exhaustion leaves the exchange at Publish holding its Receive Maximum unit and its identifier until a later resume honours the nonce verdict (no owed withdrawal is recorded for a never-landed exhaustion)",
		done:      "CONVERTED (foreground: announced, mqttForegroundAcquireAttempts, spaced after a handled write); an admitting licence keeps the plain budget, unannounced"},
	"inflight_register.go:(mqttSession).markInflightRegisterCancelledPending": {store: 1, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "FG-AUTHORITY, UNANNOUNCED, cleanup tier: the create-only cancellation tombstone",
		exhausted: "yes; exhaustion leaves the verdict staged and saved on the session record, so the tombstone is promptness only",
		done:      "LEFT: the durable verdict is the guarantee"},
	"inflight_register.go:(mqttSession).recoverInflightRegisters": {store: 1, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "WALK-RESTART: the CONNECT resume walk, restarted on a stale snapshot",
		exhausted: "not a lost CAS; exhaustion marks the identifier space incomplete until the next good resume, or refuses the resume before CONNACK",
		done:      "LEFT: a contention wait is a category error for a walk restart"},
	"inflight_register.go:(mqttSession).runInflightAdmission": {store: 0, fg: 0, fors: []string{"attempt := 0; attempt < budget; attempt++"}, budgets: nil,
		class:     "the shared admission ladder; loops on its entrance's budget (reattemptEntranceBudgets) and spaces only an attempt its write reports contended, before the reread",
		exhausted: "by entrance: see admitThroughAuthority and storeInflightRegister",
		done:      "CONVERTED via its budget parameter"},
	"inflight_register.go:(mqttSession).storeInflightRegister": {store: 1, fg: 0, fors: nil, budgets: nil,
		class:     "CHAPTER-P: the experimental affine route's standalone pair write, unreachable from the production command",
		exhausted: "unspaced, three attempts; exhaustion re-offers at AckWait",
		done:      "LEFT: chapter P is not alpha work"},
	"outbound_admit_orchestrate.go:(mqttSession).admitThroughAuthority": {store: 0, fg: 1, fors: nil, budgets: nil,
		class:     "FG-AUTHORITY: the ordinary and shared delivery admission, announced for the whole ladder",
		exhausted: "the admitted background writers cannot exhaust an admission to an empty identifier; classification and cleanup attempts on an occupied one are not covered, nor are other brokers, recovery, unadmitted writes, foreground contention or store failures; exhaustion re-offers at AckWait",
		done:      "CONVERTED (F-114)"},
	"outbound_close_cert.go:(mqttSession).clearCertifiedRegister": {store: 1, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "REGISTER-CAS, with a rare authority arm (pubrel_terminal, mostly the resume pre-pass)",
		exhausted: "the authority arm can be moved by any same-session writer; exhaustion leaves the token standing, the acquisition refuses and the next redelivery re-attempts (one AckWait), or the pre-pass skips the subject",
		done:      "LEFT: rare post-takeover path, re-attempted per redelivery"},
	"outbound_disposal.go:(mqttDisposal).step": {store: 1, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "FG-AUTHORITY (an ACL-denied delivery's synchronous step) and BG-AUTHORITY UNADMITTED (its 2 s timer and the shutdown pass)",
		exhausted: "yes; exhaustion leaves the disposal pending for the 2 s timer, without limit; the timer step is a same-session background authority writer outside the handshake",
		done:      "LEFT: rare path, unbounded continuation; named in the handshake's not-covered list"},
	"pubrel_action.go:(mqttSession).acquirePubRelThroughAuthority": {store: 1, fg: 1, fors: []string{"attempt := 0; attempt < budget; attempt++"}, budgets: []string{"budget := mqttInflightStoreAttempts", "budget = mqttForegroundAcquireAttempts"},
		class:     "FG-AUTHORITY, announced (the PUBREL callback; the PUBREC handler's retained PUBREL) and BG-AUTHORITY (retained continuations, admitted per attempt)",
		exhausted: "foreground: the admitted background writers cannot exhaust it (an attempt spent clearing a certified token is not covered), and exhaustion waits out AckWait or hands the retained PUBREL to the continuation; background: the continuation's backoff and then a connection close",
		done:      "CONVERTED (foreground arms); background arm LEFT: a larger budget buys a background attempt no priority"},
	"pubrel_action.go:(mqttSession).admitStoredPubRel": {store: 1, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "FG redispatch over FG-AUTHORITY, announced for its whole span: the PUBREL callback re-dispatching into the acquisition, the recovered-PUBREC record and custody transfer",
		exhausted: "each authority write it reaches carries the foreground budget itself, and the span's announcement keeps background writers from admitting fresh attempts between them; iterations spent on the recovered PUBREC or custody transfer are not covered; exhaustion leaves the PUBREL unacknowledged until AckWait",
		done:      "CONVERTED (announced across the redispatch); its own budget LEFT at three, unspaced: each inner write is spaced"},
	"pubrel_action.go:(mqttSession).mqttTerminalizeOnPubComp": {store: 0, fg: 1, fors: []string{"attempt := 0; attempt < mqttForegroundAcquireAttempts; attempt++"}, budgets: nil,
		class:     "FG-AUTHORITY, announced: the PUBCOMP advance (also run by the completion worker)",
		exhausted: "the admitted background writers cannot exhaust it; the other causes enrol the completion worker (2 s), holding the Receive Maximum unit, the terminal reservation and the canonical PUBREL acknowledgement",
		done:      "CONVERTED (mqttForegroundAcquireAttempts, spaced after a handled advance)"},
	"pubrel_action.go:(mqttSession).proveAcceptedIncarnation": {store: 1, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "READ: the PUBREL fence's session-record read, re-read on a head/sequence mismatch",
		exhausted: "not an authority CAS and none of the counted writers saves a session record; exhaustion is Indeterminate and the PUBREL waits out AckWait",
		done:      "LEFT: outside the authority-contention question"},
	"pubrel_action.go:(mqttSession).recordRecoveredPubRec": {store: 0, fg: 1, fors: []string{"attempt := 0; attempt < mqttForegroundAcquireAttempts; attempt++"}, budgets: nil,
		class:     "FG-AUTHORITY, announced: the recovered PUBREC arrow, before CONNACK (route B), at the drain resume and from the PUBREL callback",
		exhausted: "the admitted background writers cannot exhaust it; the other causes, on route B, quarantine the slot and mark the identifier space incomplete, so nothing new is allocated session-wide until a later resume completes; at the drain resume the drain is skipped on this resume",
		done:      "CONVERTED (mqttForegroundAcquireAttempts, spaced after a handled advance)"},
	"pubrel_action.go:(mqttSession).startActionDrain": {store: 1, fg: 0, fors: []string{"attempt := 0; attempt < mqttInflightStoreAttempts; attempt++"}, budgets: nil,
		class:     "BG-AUTHORITY UNADMITTED (op-drain-complete, rare) or REGISTER-CAS: the PUBREL action drain's completion",
		exhausted: "yes for the authority arm; exhaustion schedules the 2 s reconcile without limit",
		done:      "LEFT: named in the handshake's not-covered list"},
}

// reattemptEntranceBudgets pins the budget argument each entrance passes to the shared admission
// ladder, which loops on that parameter and names neither budget itself.
var reattemptEntranceBudgets = map[string]string{
	"outbound_admit_orchestrate.go:(mqttSession).admitThroughAuthority": "mqttForegroundAcquireAttempts",
	"inflight_register.go:(mqttSession).storeInflightRegister":          "mqttInflightStoreAttempts",
}

type reattemptObserved struct {
	store, fg int
	fors      []string
	budgets   []string
}

func reattemptObserve(f fgCensusFunc) reattemptObserved {
	var o reattemptObserved
	ast.Inspect(f.fd.Body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.Ident:
			switch v.Name {
			case "mqttInflightStoreAttempts":
				o.store++
			case "mqttForegroundAcquireAttempts":
				o.fg++
			}
		case *ast.ForStmt:
			parts := make([]string, 3)
			for i, p := range []ast.Node{v.Init, v.Cond, v.Post} {
				if p != nil {
					parts[i] = fgCensusRender(f.fset, p)
				}
			}
			o.fors = append(o.fors, strings.Join(parts, "; "))
		case *ast.RangeStmt:
			// A range loop re-attempts as well as a three-clause one does (`for range budget`).
			head := "range " + fgCensusRender(f.fset, v.X)
			if v.Key != nil {
				vars := fgCensusRender(f.fset, v.Key)
				if v.Value != nil {
					vars += ", " + fgCensusRender(f.fset, v.Value)
				}
				head = vars + " " + v.Tok.String() + " " + head
			}
			o.fors = append(o.fors, head)
		case *ast.AssignStmt:
			for _, l := range v.Lhs {
				if id, ok := l.(*ast.Ident); ok && id.Name == "budget" {
					o.budgets = append(o.budgets, fgCensusRender(f.fset, v))
					break
				}
			}
		}
		return true
	})
	return o
}

func reattemptLiteral(key string, o reattemptObserved) string {
	q := func(ss []string) string {
		if len(ss) == 0 {
			return "nil"
		}
		out := make([]string, len(ss))
		for i, s := range ss {
			out[i] = fmt.Sprintf("%q", s)
		}
		return "[]string{" + strings.Join(out, ", ") + "}"
	}
	return fmt.Sprintf("%q: {store: %d, fg: %d, fors: %s, budgets: %s},", key, o.store, o.fg, q(o.fors), q(o.budgets))
}

func TestBoundedReattemptLoopsAreClassified(t *testing.T) {
	funcs := fgCensusParse(t)
	var unlisted []string
	for key, f := range funcs {
		o := reattemptObserve(f)
		if _, listed := reattemptCensus[key]; !listed && (o.store > 0 || o.fg > 0) {
			unlisted = append(unlisted, reattemptLiteral(key, o))
		}
	}
	sort.Strings(unlisted)
	for _, u := range unlisted {
		t.Errorf("unclassified bounded re-attempt site; classify it before listing it: %s", u)
	}
	keys := make([]string, 0, len(reattemptCensus))
	for k := range reattemptCensus {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		row := reattemptCensus[key]
		f, ok := funcs[key]
		if !ok {
			t.Errorf("%s is classified but no longer exists; re-read the family before removing its row", key)
			continue
		}
		if row.class == "" || row.exhausted == "" || row.done == "" {
			t.Errorf("%s: a row without its class, exhaustion cost and disposition classifies nothing", key)
		}
		o := reattemptObserve(f)
		if o.store != row.store || o.fg != row.fg || !reattemptSame(o.fors, row.fors) || !reattemptSame(o.budgets, row.budgets) {
			t.Errorf("%s changed shape; re-classify it before re-pinning. Observed:\n\t%s", key, reattemptLiteral(key, o))
		}
	}
	for _, c := range censusScanPackage(t).Calls {
		if c.Callee != "runInflightAdmission" {
			continue
		}
		key, budget := c.Fn.String(), "<missing>"
		if len(c.Args) > 8 {
			budget = c.Args[8]
		}
		want, ok := reattemptEntranceBudgets[key]
		if !ok {
			t.Errorf("unclassified admission ladder entrance %s passes budget %s", key, budget)
			continue
		}
		if budget != want {
			t.Errorf("admission ladder entrance %s passes budget %s, pinned %s; re-classify it", key, budget, want)
		}
	}
}

func reattemptSame(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestBoundedReattemptObserverSeesEverySecondLoop is the guard on the observer itself: a second loop
// added to a ladder that loops on a budget parameter names neither budget constant, so it is
// caught only if the observer records its header, whichever loop form it takes.
func TestBoundedReattemptObserverSeesEverySecondLoop(t *testing.T) {
	const base = `package p
func ladder(budget int) {
	for attempt := 0; attempt < budget; attempt++ {
	}
}`
	observe := func(src string) reattemptObserved {
		t.Helper()
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "p.go", src, 0)
		if err != nil {
			t.Fatalf("parsing the synthetic ladder: %v", err)
		}
		return reattemptObserve(fgCensusFunc{fd: f.Decls[0].(*ast.FuncDecl), fset: fset})
	}
	want := observe(base)
	if len(want.fors) != 1 {
		t.Fatalf("the base ladder's headers were observed as %q, want its one loop", want.fors)
	}
	for _, second := range []string{
		"for i := 0; i < budget; i++ {\n\t}",
		"for range budget {\n\t}",
		"for range 3 {\n\t}",
		"for i := range budget {\n\t\t_ = i\n\t}",
		"for {\n\t\tbreak\n\t}",
	} {
		src := strings.Replace(base, "\n}", "\n\t"+second+"\n}", 1)
		got := observe(src)
		if reattemptSame(got.fors, want.fors) && got.store == want.store && got.fg == want.fg {
			t.Errorf("a second loop %q left the observation unchanged (%q): the census would not fail", second, got.fors)
		}
	}
}
