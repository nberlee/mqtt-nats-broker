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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// T13 (issue #90 WP2, plan rev 20): EVERY removal of a PUBLISH entry states why, and each class of
// removal is backed by a behavioural test. Only one class — an UNANSWERED removal of an entry
// whose transmission was attempted — installs an AckRoute, and it does so through the one sink,
// untrackPublishEntry. A direct `delete(sess.pendingPublish, ...)` is a removal that installs
// nothing, so each one is classified here by what makes "nothing" correct; a new direct site, or
// a new untrack call, fails this census until it is classified.

// removalClass is why a removal installs no binding (or, for the sink's Unanswered callers,
// why the evidence decides).
type removalClass string

const (
	// The entry is a CANCELLED placeholder (regCancelled): never acting, Unknown evidence.
	removalCancelledPlaceholder removalClass = "cancelled-placeholder"
	// The entry is PROVEN UNSENT by a local classification (its marker stands): nothing to answer.
	removalProvenUnsent removalClass = "proven-unsent"
	// An ABANDONMENT installed its HandBack in the same section; the HandBack routes the answer.
	removalAbandonmentHandBack removalClass = "abandonment-handback"
	// Recovery removes the entry WITH PROOF (a drop of a provably settled or unsent register) or
	// hands it to a PUBREL-side owner it installed first.
	removalRecoveryOwner removalClass = "recovery-proof-or-owner"
	// A recovery RESERVATION (non-acting) goes after its owner or exclusion is installed.
	removalRecoveryReservation removalClass = "recovery-reservation"
	// The client ANSWERED (PUBREC/terminal successor owns the exchange now).
	removalAnsweredHandoff removalClass = "answered-handoff"
	// The sink itself.
	removalSink removalClass = "sink"
	// Through the sink, by reason.
	removalUntrackAnswered   removalClass = "untrack-answered"
	removalUntrackEnded      removalClass = "untrack-ended"
	removalUntrackUnanswered removalClass = "untrack-unanswered"
	removalUntrackBySettle   removalClass = "untrack-by-settle-kind"
)

// removalBacking names, per class, the behavioural tests that witness it. Each must exist.
var removalBacking = map[removalClass][]string{
	removalCancelledPlaceholder: {"TestMQTTInflightConsumerPurgeSecuresCancellations",
		"TestMQTTInflightConsumerPurgeSecuresCancellationsAuthority"},
	removalProvenUnsent:        {"TestMarkerStandingRemovalInstallsNoBinding"},
	removalAbandonmentHandBack: {"TestAbandonDeniedDeliveryReleasesTheChargeNotTheIdentifier", "TestUnitCAbandonedAckHandler"},
	removalRecoveryOwner:       {"TestPendingFenceFromTheProducerIsDroppedOnColdResume", "TestPreservedRegisterLatePubAckAfterColdPeerResume"},
	removalRecoveryReservation: {"TestOpaqueAndRereadFailureStayConservative"},
	removalAnsweredHandoff:     {"TestUnitCAbandonedAckDirectArrows"},
	removalSink:                {"TestUntrackedTransmittedDeliveryLatePubAckEndsTheExchange"},
	removalUntrackAnswered:     {"TestUntrackedTransmittedDeliveryLatePubAckEndsTheExchange"},
	removalUntrackEnded:        {"TestAckRouteEndsByLineage", "TestEndedLineageIsNotReboundOnIdentifierReuse"},
	removalUntrackUnanswered:   {"TestUntrackedTransmittedDeliveryLatePubAckEndsTheExchange", "TestMarkerStandingRemovalInstallsNoBinding"},
	removalUntrackBySettle:     {"TestEveryEntryRemovalHasAReason"},
}

// removalSite is one discovered removal: the enclosing function and what it does.
type removalSite struct {
	fn   string
	what string // "delete" or "untrack:<reason>"
}

// frozenRemovals is the FROZEN census, by enclosing function.
var frozenRemovals = map[removalSite]struct {
	n     int
	class removalClass
}{
	{"releaseCancelledReservation", "delete"}:          {1, removalCancelledPlaceholder},
	{"releaseSettledVoidReservationsLocked", "delete"}: {1, removalCancelledPlaceholder},
	{"forgetVoidedReg", "delete"}:                      {1, removalCancelledPlaceholder},
	{"confirmInflightRegister", "delete"}:              {2, removalCancelledPlaceholder},
	{"abandonInflightRegister", "delete"}:              {1, removalCancelledPlaceholder},
	{"purgeConsumerPendingModeLocked", "delete"}:       {1, removalProvenUnsent},
	{"settleAbandonedDeliveryLocked", "delete"}:        {1, removalAbandonmentHandBack},
	{"recoverInflightRegistersOnce", "delete"}:         {2, removalRecoveryOwner},
	{"recoverExchangeRouteLocked", "delete"}:           {1, removalAnsweredHandoff},
	{"handOffPublishEntryLocked", "delete"}:            {1, removalAnsweredHandoff},
	{"commitRecoveryReleaseLocked", "delete"}:          {1, removalRecoveryReservation},
	{"handOffRecoveryPublishLocked", "delete"}:         {1, removalRecoveryReservation},
	{"untrackPublishEntry", "delete"}:                  {1, removalSink},
}

// scanRemovals finds every production `delete(sess.pendingPublish, ...)` and every
// untrackPublish / untrackPublishEntry call with its reason argument.
func scanRemovals(t *testing.T) map[removalSite]int {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	out := map[removalSite]int{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", name, perr)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				ce, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := ce.Fun.(type) {
				case *ast.Ident:
					if fun.Name == "delete" && len(ce.Args) == 2 {
						if sel, ok := ce.Args[0].(*ast.SelectorExpr); ok && sel.Sel.Name == "pendingPublish" {
							out[removalSite{fd.Name.Name, "delete"}]++
						}
					}
				case *ast.SelectorExpr:
					if (fun.Sel.Name == "untrackPublish" || fun.Sel.Name == "untrackPublishEntry") && len(ce.Args) == 2 {
						reason := "expr"
						if id, ok := ce.Args[1].(*ast.Ident); ok {
							reason = id.Name
						}
						out[removalSite{fd.Name.Name, "untrack:" + reason}]++
					}
				}
				return true
			})
		}
	}
	return out
}

// TestEveryEntryRemovalHasAReason is T13.
func TestEveryEntryRemovalHasAReason(t *testing.T) {
	t.Parallel()
	got := scanRemovals(t)
	var diffs []string
	for s, n := range got {
		if strings.HasPrefix(s.what, "untrack:") {
			switch s.what {
			case "untrack:mqttUntrackAnswered", "untrack:mqttUntrackEnded", "untrack:mqttUntrackUnanswered":
			case "untrack:why":
				// endSendCore: the reason follows the settlement kind (ACK settlements Answered,
				// every parked teardown/deny/drop Unanswered); its one variable is that choice.
				if s.fn != "endSendCore" && s.fn != "untrackPublish" {
					diffs = append(diffs, s.fn+": an untrack whose reason is a variable outside endSendCore")
				}
			default:
				diffs = append(diffs, s.fn+": an untrack without a named reason ("+s.what+")")
			}
			continue
		}
		if want, ok := frozenRemovals[s]; !ok || want.n != n {
			diffs = append(diffs, s.fn+" / "+s.what+": source has "+itoa(n)+", frozen has "+itoa(want.n))
		}
	}
	for s, want := range frozenRemovals {
		if _, ok := got[s]; !ok {
			diffs = append(diffs, s.fn+" / "+s.what+": frozen with "+itoa(want.n)+" but gone from the source (stale row)")
		}
	}
	sort.Strings(diffs)
	for _, d := range diffs {
		t.Errorf("entry removal census: %s", d)
	}

	// BEHAVIOURAL BACKING: every class names tests that exist in this package.
	exists := testNames(t)
	for class, tests := range removalBacking {
		for _, name := range tests {
			if !exists[name] {
				t.Errorf("class %s is backed by %s, which does not exist", class, name)
			}
		}
	}
	for _, row := range frozenRemovals {
		if len(removalBacking[row.class]) == 0 {
			t.Errorf("class %s has no behavioural backing", row.class)
		}
	}
}

// testNames is every `func TestX(` declared in this package's test sources.
func testNames(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)\(`)
	out := map[string]bool{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		src, rerr := os.ReadFile(e.Name())
		if rerr != nil {
			t.Fatal(rerr)
		}
		for _, m := range re.FindAllSubmatch(src, -1) {
			out[string(m[1])] = true
		}
	}
	return out
}
