package mqtt

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// An acknowledgement-authorized terminal release runs INLINE only on the read loop. Every
// other path to it — a send section's parked settlement or its action's resolution — hands
// it to the terminal-retry dispatcher (handOffAcknowledgedPublish), because a send section
// runs on the subscription's delivery goroutine, and a release run there holds back the
// source's own re-offers until its suppression has lifted, so they are admitted as new
// deliveries of an acknowledged message.
//
// WHAT THIS PROVES, and what it does not. It is a DIRECT-CALL guard over the package's
// production syntax: it binds every inline entry to the release — the high-level
// completeAcknowledgedPublish and the low-level attemptTerminalRegisterRelease with
// acknowledgement authority — to the functions allowed to run one, and the read-loop
// handler to the read loop's packet dispatch. A lexical enclosure does not prove which
// goroutine executes code; TestMQTTInflightParkedAckReleaseLeavesReOffersSuppressed is the
// behavioral proof. Every selector is counted, so one that quietly matched nothing fails.
func TestMQTTAcknowledgedReleaseRunsInlineOnlyOnTheReadLoop(t *testing.T) {
	type ref struct {
		fn   string // enclosing function declaration
		call *ast.CallExpr
	}
	refs := map[string][]ref{} // callee -> references
	var nonCalls []string

	callees := map[string]bool{
		"completeAcknowledgedPublish":    true,
		"attemptTerminalRegisterRelease": true,
		"mqttProcessPublishReceived":     true,
		"mqttProcessPubAck":              true,
		"mqttProcessPubRec":              true,
	}
	for _, file := range testMQTTProductionFiles(t) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			// Every selector naming a callee, and which of them are the Fun of a call.
			called := map[*ast.SelectorExpr]*ast.CallExpr{}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
						called[sel] = c
					}
				}
				return true
			})
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok || !callees[sel.Sel.Name] {
					return true
				}
				c := called[sel]
				if c == nil {
					nonCalls = append(nonCalls, fset.Position(sel.Pos()).String()+": "+sel.Sel.Name+" in "+fd.Name.Name)
					return true
				}
				refs[sel.Sel.Name] = append(refs[sel.Sel.Name], ref{fn: fd.Name.Name, call: c})
				return true
			})
		}
	}
	if len(nonCalls) > 0 {
		sort.Strings(nonCalls)
		t.Fatalf("a release entry is referenced other than by a direct call (a method value can run it "+
			"anywhere, past this guard):\n%s", strings.Join(nonCalls, "\n"))
	}

	expectCallers := func(callee string, allowed ...string) {
		t.Helper()
		if len(refs[callee]) == 0 {
			t.Fatalf("no call of %s found: the selector matched nothing, so this guard checks nothing", callee)
		}
		ok := map[string]bool{}
		for _, a := range allowed {
			ok[a] = true
		}
		seen := map[string]bool{}
		for _, r := range refs[callee] {
			seen[r.fn] = true
			if !ok[r.fn] {
				t.Errorf("%s is called from %s, which is not allowed to run it inline (allowed: %v)",
					callee, r.fn, allowed)
			}
		}
		for _, a := range allowed {
			if !seen[a] {
				t.Errorf("%s is allowlisted for %s but no longer calls it: drop the stale entry", a, callee)
			}
		}
	}

	// The high-level inline release: the read loop's acknowledgement handler only.
	expectCallers("completeAcknowledgedPublish", "mqttProcessPublishReceived")
	// That handler is reached only from the read loop's packet dispatch.
	expectCallers("mqttProcessPublishReceived", "mqttProcessPubAck", "mqttProcessPubRec")
	expectCallers("mqttProcessPubAck", "mqttParse")
	expectCallers("mqttProcessPubRec", "mqttParse")

	// The low-level attempt with acknowledgement authority. An argument that is not the literal
	// false counts as possibly true.
	acknowledgedSites := 0
	for _, r := range refs["attemptTerminalRegisterRelease"] {
		if len(r.call.Args) != 4 {
			t.Fatalf("attemptTerminalRegisterRelease in %s takes %d arguments; this guard reads the 4th",
				r.fn, len(r.call.Args))
		}
		arg := r.call.Args[3]
		if id, ok := arg.(*ast.Ident); ok && id.Name == "false" {
			continue
		}
		acknowledgedSites++
		switch r.fn {
		case "completeAcknowledgedPublish", "dispatchTerminalRetryLocked":
		case "finishOwedTerminals":
			// Phase 3b replays what was recorded: the authority must be the record's own.
			if sel, ok := arg.(*ast.SelectorExpr); !ok || sel.Sel.Name != "acknowledged" {
				t.Errorf("finishOwedTerminals must pass the recorded acknowledgement authority, not %T", arg)
			}
		default:
			t.Errorf("attemptTerminalRegisterRelease runs with acknowledgement authority in %s, outside "+
				"completeAcknowledgedPublish, the retry dispatcher and Phase 3b: a send section must hand "+
				"its release off (handOffAcknowledgedPublish)", r.fn)
		}
	}
	if acknowledgedSites == 0 {
		t.Fatal("no attemptTerminalRegisterRelease call with acknowledgement authority found: " +
			"the selector matched nothing, so this guard checks nothing")
	}
}
