//go:build mqtt_experimental_shared_qos2

// Copyright 2025 The mqtt-nats-broker Authors
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"strings"
	"testing"
)

// TestQ2AffineConsumerDeletesAreInventoried is F-036 course E's STRUCTURAL guard.
//
// WHY IT IS PACKAGE-WIDE AND NOT REACHABILITY-BASED. It began as a call-graph guard rooted
// at sweepOrphanedQoS2State, and four successive review rounds each found a narrower way to
// evade the REACHABILITY step rather than the inventory: a local alias of the stream name, a
// method value, a named function-value wrapper. Reachability over an AST cannot resolve
// receivers or function variables, so every version of it fails open somewhere. The
// terminating instrument is to delete the reachability step: this guard inventories EVERY
// $MQTT_out consumer delete in the package, so there is no "unreachable" classification left
// to attack.
//
// THE INVARIANT, stated precisely, because two earlier versions of this guard and its name
// stated a FALSE one. Course E's guarantee is:
//
//	the orphan candidate-reclamation route no longer infers affine-consumer deletion
//	from absence.
//
// It is NOT "nothing reachable from the sweep deletes an affine consumer": teardown-marker
// adoption reaches deleteTeardownTargets, which deletes t.q2AffDur. Course E removed an
// inference from absence; it did not and could not remove teardown.
//
// THREAT MODEL, stated so the guard's claim matches its reach. This detects DIRECT and
// COMMON-INDIRECT reintroduction: a new call site, a call inside an inventoried function, a
// duplicate of an inventoried call, a stream passed by alias or parameter, the primitive
// taken as a method value, a call routed through a generic helper, and a package-level
// function literal. It is deliberately NOT a soundness proof. AST analysis cannot resolve Go
// function values, interface dispatch or reflection, and making it sound would need
// closed-world type-checked SSA — disproportionate for an alarm that is not load-bearing.
//
// THE ACCEPTANCE CONDITION IS BEHAVIORAL, not this guard:
// TestOrphanSweepDoesNotReclaimAnAffineConsumerByAbsence drives the real sweep and asserts
// the consumer AND its exact retained copies survive every state that used to authorize the
// delete, so a reintroduction fails it whatever syntax carries it. This guard is a drift
// alarm with a precise error message, and an enforced inventory of which deletes exist and
// why — the inventory is what surfaced the teardown-adoption route in the first place.
//
// Why the removed inference cannot come back in any form: $MQTT_out is interest-retained, so
// deleting a consumer drops the acknowledged QoS 2 payloads it holds, and no positive fact
// available to a sweeper licenses that. The retirement watermark records the SESSION
// generation while the consumer is keyed by an AFFINE generation that outlives incarnations;
// consumer metadata has the same mismatch; NumPending == 0 is an instantaneous observation
// rather than a fence; a record re-read cannot tell "no session" from "a live session whose
// record is absent"; and ConsumerInfo.Created cannot see an ADOPTION, because provisioning is
// a create-or-update. See docs/findings/ha-arbitration.md and docs/CONFORMANCE.md.
func TestQ2AffineConsumerDeletesAreInventoried(t *testing.T) {
	t.Parallel()

	// Every $MQTT_out consumer delete in the package, keyed by (qualified function,
	// durable-argument expression) and expected EXACTLY this many times. Multiplicity is
	// part of the inventory: a second delete beside an allowed one is how the removed
	// route would return. Each entry carries WHY it exists, because course E's whole point
	// is that absence of a reference is not a licence.
	type site struct{ fn, arg string }
	expected := map[site]int{
		// Candidate/listing-based reclamation, PUBREL class only. Licensed by a complete
		// reference walk, a grace, a final record re-read and an instance re-lookup. It
		// never touches $MQTT_Q2AFF_*, which TestOrphanSweepLeavesForeignConsumersAlone
		// also checks at RUNTIME. The affine counterpart of this site is what course E
		// deleted.
		{"mqttAccountSessionManager.deleteOrphanPubRelConsumers", "cand.durable"}: 1,

		// Lifecycle teardown. Reached both by ordinary teardown and by the sweep, which
		// ADOPTS abandoned teardown markers and finishes them. Known pre-existing call
		// site, outside course E's behavioral change and owned by open F-036, retained
		// here WITHOUT any claim that it is race-free: a won claimTeardown authorizes
		// proceeding with lifecycle teardown relative to the claimed record, but does NOT
		// fence a stale same-affine-generation peer's beginQ2AffOp and create-only claim
		// CAS, so discardQ2AffClaimsFor's scan can be outrun. F-036's fourth
		// manifestation, with F-063's proof-to-ack residual behind its second half.
		{"mqttSession.deleteTeardownTargets", "t.pubRelDur"}: 1,
		{"mqttSession.deleteTeardownTargets", "t.q2AffDur"}:  1,

		// Provisioning rollback, minted generations only: no record ever referenced this
		// generation. Its failure has no retry anchor — see docs/CONFORMANCE.md.
		{"mqttSession.ensureQ2AffineConsumerSubscription", "durable"}: 1,

		// The unused-path unwind, under the session's own fence and after a record that no
		// longer names the path is confirmed. Failure likewise has no retry anchor.
		{"mqttSession.finishQ2AffineUnwind", "u.durable"}: 1,
	}

	// Stream-generic helpers, exempt with their reason stated at the skip site below.
	genericStreamHelpers := map[string]bool{
		"mqttJSA.deleteEphemeralConsumerDefinite":   true,
		"mqttJSA.deleteEphemeralConsumerBestEffort": true,
	}
	genericHelperNames := map[string]bool{
		"deleteEphemeralConsumerDefinite":   true,
		"deleteEphemeralConsumerBestEffort": true,
	}
	// Calls to those helpers whose stream is not statically another stream. Each needs a
	// licence, exactly as a direct delete does.
	expectedHelperCalls := map[site]int{
		// The ephemeral-provisioning rollback: it replays the consumers THIS failed attempt
		// created, from its own created-list, so the stream and name are that attempt's own
		// and never an affine durable. Per-run ephemeral identity class; see
		// docs/ARCHITECTURE.md.
		{"mqttAccountSessionManager.rollbackEphemeralConsumers",
			"deleteEphemeralConsumerBestEffort(c.stream, c.name)"}: 1,
	}

	// Streams other than $MQTT_out, named literally: a different class entirely.
	// Every broker-owned stream constant EXCEPT $MQTT_out. A delete named against one of
	// these is statically a different class. The list is exhaustive over the nine so an
	// omission cannot silently become a fail-closed false positive.
	otherStreams := map[string]bool{
		"mqttStreamName":                 true,
		"mqttRetainedMsgsStreamName":     true,
		"mqttSessStreamName":             true,
		"mqttWillQStreamName":            true,
		"mqttTeardownStreamName":         true,
		"mqttOriginStreamName":           true,
		"mqttQoS2IncomingMsgsStreamName": true,
		"mqttQoS2AssignStreamName":       true,
		"mqttInflightStreamName":         true,
	}

	fset := token.NewFileSet()
	found := map[site]int{}
	// indirect and methodValues are the FAIL-CLOSED classes: a stream this guard cannot
	// resolve to a named constant, and a reference to deleteConsumer that is not itself a
	// call (a method value, invoked later where no call site names it).
	indirect := map[site]int{}
	methodValues := map[string]int{}
	helperCalls := map[site]int{}

	for _, name := range testMQTTProductionFiles(t) {
		f, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		// Every function BODY in the file, including package-level variables initialised
		// with a function literal — a FuncDecl-only walk would miss those entirely.
		type namedBody struct {
			name string
			body *ast.BlockStmt
		}
		var bodies []namedBody
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Body != nil {
					bodies = append(bodies, namedBody{testQualifiedFuncName(d), d.Body})
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					vs, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for i, val := range vs.Values {
						lit, ok := val.(*ast.FuncLit)
						if !ok || lit.Body == nil {
							continue
						}
						nm := "package-level func value"
						if i < len(vs.Names) {
							nm = "package-level " + vs.Names[i].Name
						}
						bodies = append(bodies, namedBody{nm, lit.Body})
					}
				}
			}
		}
		for _, nb := range bodies {
			fn := &ast.FuncDecl{Body: nb.body}
			self := nb.name
			// mqttJSA.deleteConsumer is the primitive itself; its own body is not a site.
			if self == "mqttJSA.deleteConsumer" {
				continue
			}
			callFuns := map[ast.Node]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					callFuns[c.Fun] = true
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok &&
					sel.Sel.Name == "deleteConsumer" && !callFuns[ast.Node(sel)] {
					methodValues[self]++
				}
				return true
			})
			// Stream-GENERIC helper BODIES are skipped — their stream is a parameter by
			// design — but their CALL SITES are inventoried below under the same rules, so
			// the exemption is not a hole: routing a delete through one of them is caught
			// at the call.
			if genericStreamHelpers[self] {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !genericHelperNames[sel.Sel.Name] || len(call.Args) == 0 {
					return true
				}
				stream := exprText(fset, call.Args[0])
				arg := "?"
				if len(call.Args) > 1 {
					arg = exprText(fset, call.Args[1])
				}
				switch {
				case otherStreams[stream]:
					// Statically a different stream.
				default:
					helperCalls[site{self, sel.Sel.Name + "(" + stream + ", " + arg + ")"}]++
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				// Only mqttJSA.deleteConsumer(stream, name, noWait). The package used to have
				// mqttSession.deleteConsumer(cc *ConsumerConfig) too (deleted with F-105 7c-lc's
				// routes); the argument count still guards against any such method.
				if !ok || sel.Sel.Name != "deleteConsumer" || len(call.Args) < 3 {
					return true
				}
				stream := exprText(fset, call.Args[0])
				arg := "?"
				if len(call.Args) > 1 {
					arg = exprText(fset, call.Args[1])
				}
				switch {
				case stream == "mqttOutStreamName":
					found[site{self, arg}]++
				case otherStreams[stream]:
					// Statically a different stream. Not this guard's class.
				default:
					indirect[site{self, stream + "/" + arg}]++
				}
				return true
			})
		}
	}

	if len(found) == 0 {
		t.Fatal("no deleteConsumer(mqttOutStreamName, ...) call was found anywhere in the package, " +
			"so this guard checks nothing — the call was probably renamed and the guard must be " +
			"updated with it")
	}

	for fn, n := range methodValues {
		t.Errorf("%s takes the deleteConsumer method as a VALUE %d time(s). An indirectly "+
			"invoked delete cannot be inventoried by call site, so the guard fails closed: "+
			"call deleteConsumer directly, with a named stream constant.", fn, n)
	}
	for st, n := range helperCalls {
		want, ok := expectedHelperCalls[st]
		if !ok {
			t.Errorf("%s routes a consumer delete through the stream-generic helper %s, and that "+
				"call is not in this guard's inventory. Go enforces neither an ephemeral stream "+
				"nor an ephemeral durable on those helpers, so a call with $MQTT_out and an "+
				"affine durable would delete acknowledged QoS 2 payloads. It needs an inventory "+
				"entry NAMING ITS LICENCE.", st.fn, st.arg)
			continue
		}
		if n != want {
			t.Errorf("%s makes the inventoried helper call %s %d times, want exactly %d.",
				st.fn, st.arg, n, want)
		}
	}
	for st := range expectedHelperCalls {
		if helperCalls[st] == 0 {
			t.Errorf("inventoried helper call %s / %s no longer exists, so this guard is checking "+
				"a predicate nothing satisfies", st.fn, st.arg)
		}
	}

	for st, n := range indirect {
		t.Errorf("%s calls deleteConsumer %d time(s) with a stream this guard cannot resolve "+
			"statically (%s). The guard fails CLOSED: name the stream with a constant at the "+
			"call site, or the inventory cannot tell whether an affine consumer on $MQTT_out "+
			"is being deleted.", st.fn, n, st.arg)
	}
	for st, n := range found {
		want, ok := expected[st]
		if !ok {
			t.Errorf("%s deletes a consumer on $MQTT_out with durable %s, and that call site is "+
				"not in this guard's inventory. Course E's invariant is that the orphan "+
				"candidate-reclamation route never infers affine-consumer deletion from absence, "+
				"and $MQTT_out is interest-retained, so a wrong delete destroys acknowledged "+
				"QoS 2 payloads. A legitimate new site needs an inventory entry NAMING ITS "+
				"LICENCE, and absence of a reference is not one.", st.fn, st.arg)
			continue
		}
		if n != want {
			t.Errorf("%s deletes a consumer on $MQTT_out with durable %s %d times, want exactly "+
				"%d. A second delete beside an inventoried one is how this route would come "+
				"back, so multiplicity is part of the inventory.", st.fn, st.arg, n, want)
		}
	}

	// Anti-vacuity: every inventoried site must still exist, or the assertions above pass
	// against code that no longer contains what they describe.
	for st := range expected {
		if found[st] == 0 {
			t.Errorf("inventoried site %s / %s no longer deletes a consumer on $MQTT_out, so this "+
				"guard is checking a predicate nothing satisfies", st.fn, st.arg)
		}
	}
}

// exprText renders an expression back to source, so the inventory can key on the ARGUMENT
// rather than only on the enclosing function.
func exprText(fset *token.FileSet, e ast.Expr) string {
	var b strings.Builder
	if err := printer.Fprint(&b, fset, e); err != nil {
		return "?"
	}
	return b.String()
}
