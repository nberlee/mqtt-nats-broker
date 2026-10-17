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

// T13 (F-088 WP2, plan rev 20): EVERY removal of a PUBLISH entry states why, and each class of
// removal is backed by a behavioural test. Only one class — an UNANSWERED removal of an entry
// whose transmission was attempted — installs an AckRoute, and it does so through the one sink,
// untrackPublishEntry. A direct `delete(sess.pendingPublish, ...)` is a removal that installs
// nothing, so each one is classified here by what makes "nothing" correct; a new direct site, or
// a new untrack call, fails this census until it is classified.

// f088RemovalClass is why a removal installs no binding (or, for the sink's Unanswered callers,
// why the evidence decides).
type f088RemovalClass string

const (
	// The entry is a CANCELLED placeholder (regCancelled): never acting, Unknown evidence.
	f088RemovalCancelledPlaceholder f088RemovalClass = "cancelled-placeholder"
	// The entry is PROVEN UNSENT by a local classification (its marker stands): nothing to answer.
	f088RemovalProvenUnsent f088RemovalClass = "proven-unsent"
	// An ABANDONMENT installed its HandBack in the same section; the HandBack routes the answer.
	f088RemovalAbandonmentHandBack f088RemovalClass = "abandonment-handback"
	// Recovery removes the entry WITH PROOF (a drop of a provably settled or unsent register) or
	// hands it to a PUBREL-side owner it installed first.
	f088RemovalRecoveryOwner f088RemovalClass = "recovery-proof-or-owner"
	// A recovery RESERVATION (non-acting) goes after its owner or exclusion is installed.
	f088RemovalRecoveryReservation f088RemovalClass = "recovery-reservation"
	// The client ANSWERED (PUBREC/terminal successor owns the exchange now).
	f088RemovalAnsweredHandoff f088RemovalClass = "answered-handoff"
	// The sink itself.
	f088RemovalSink f088RemovalClass = "sink"
	// Through the sink, by reason.
	f088RemovalUntrackAnswered   f088RemovalClass = "untrack-answered"
	f088RemovalUntrackEnded      f088RemovalClass = "untrack-ended"
	f088RemovalUntrackUnanswered f088RemovalClass = "untrack-unanswered"
	f088RemovalUntrackBySettle   f088RemovalClass = "untrack-by-settle-kind"
)

// f088RemovalBacking names, per class, the behavioural tests that witness it. Each must exist.
var f088RemovalBacking = map[f088RemovalClass][]string{
	f088RemovalCancelledPlaceholder: {"TestMQTTInflightConsumerPurgeSecuresCancellations",
		"TestMQTTInflightConsumerPurgeSecuresCancellationsAuthority"},
	f088RemovalProvenUnsent:        {"TestF088MarkerStandingRemovalInstallsNoBinding"},
	f088RemovalAbandonmentHandBack: {"TestF105AbandonDeniedDeliveryReleasesTheChargeNotTheIdentifier", "TestUnitCAbandonedAckHandler"},
	f088RemovalRecoveryOwner:       {"TestF088PendingFenceFromTheProducerIsDroppedOnColdResume", "TestF088PreservedRegisterLatePubAckAfterColdPeerResume"},
	f088RemovalRecoveryReservation: {"TestF097OpaqueAndRereadFailureStayConservative"},
	f088RemovalAnsweredHandoff:     {"TestUnitCAbandonedAckDirectArrows"},
	f088RemovalSink:                {"TestF088UntrackedTransmittedDeliveryLatePubAckEndsTheExchange"},
	f088RemovalUntrackAnswered:     {"TestF088UntrackedTransmittedDeliveryLatePubAckEndsTheExchange"},
	f088RemovalUntrackEnded:        {"TestF088AckRouteEndsByLineage", "TestF088EndedLineageIsNotReboundOnIdentifierReuse"},
	f088RemovalUntrackUnanswered:   {"TestF088UntrackedTransmittedDeliveryLatePubAckEndsTheExchange", "TestF088MarkerStandingRemovalInstallsNoBinding"},
	f088RemovalUntrackBySettle:     {"TestF088EveryEntryRemovalHasAReason"},
}

// f088RemovalSite is one discovered removal: the enclosing function and what it does.
type f088RemovalSite struct {
	fn   string
	what string // "delete" or "untrack:<reason>"
}

// f088FrozenRemovals is the FROZEN census, by enclosing function.
var f088FrozenRemovals = map[f088RemovalSite]struct {
	n     int
	class f088RemovalClass
}{
	{"releaseCancelledReservation", "delete"}:          {1, f088RemovalCancelledPlaceholder},
	{"releaseSettledVoidReservationsLocked", "delete"}: {1, f088RemovalCancelledPlaceholder},
	{"forgetVoidedReg", "delete"}:                      {1, f088RemovalCancelledPlaceholder},
	{"confirmInflightRegister", "delete"}:              {2, f088RemovalCancelledPlaceholder},
	{"abandonInflightRegister", "delete"}:              {1, f088RemovalCancelledPlaceholder},
	{"purgeConsumerPendingModeLocked", "delete"}:       {1, f088RemovalProvenUnsent},
	{"settleAbandonedDeliveryLocked", "delete"}:        {1, f088RemovalAbandonmentHandBack},
	{"recoverInflightRegistersOnce", "delete"}:         {2, f088RemovalRecoveryOwner},
	{"recoverExchangeRouteLocked", "delete"}:           {1, f088RemovalAnsweredHandoff},
	{"handOffPublishEntryLocked", "delete"}:            {1, f088RemovalAnsweredHandoff},
	{"commitRecoveryReleaseLocked", "delete"}:          {1, f088RemovalRecoveryReservation},
	{"handOffRecoveryPublishLocked", "delete"}:         {1, f088RemovalRecoveryReservation},
	{"untrackPublishEntry", "delete"}:                  {1, f088RemovalSink},
}

// f088ScanRemovals finds every production `delete(sess.pendingPublish, ...)` and every
// untrackPublish / untrackPublishEntry call with its reason argument.
func f088ScanRemovals(t *testing.T) map[f088RemovalSite]int {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	out := map[f088RemovalSite]int{}
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
							out[f088RemovalSite{fd.Name.Name, "delete"}]++
						}
					}
				case *ast.SelectorExpr:
					if (fun.Sel.Name == "untrackPublish" || fun.Sel.Name == "untrackPublishEntry") && len(ce.Args) == 2 {
						reason := "expr"
						if id, ok := ce.Args[1].(*ast.Ident); ok {
							reason = id.Name
						}
						out[f088RemovalSite{fd.Name.Name, "untrack:" + reason}]++
					}
				}
				return true
			})
		}
	}
	return out
}

// TestF088EveryEntryRemovalHasAReason is T13.
func TestF088EveryEntryRemovalHasAReason(t *testing.T) {
	t.Parallel()
	got := f088ScanRemovals(t)
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
		if want, ok := f088FrozenRemovals[s]; !ok || want.n != n {
			diffs = append(diffs, s.fn+" / "+s.what+": source has "+itoa(n)+", frozen has "+itoa(want.n))
		}
	}
	for s, want := range f088FrozenRemovals {
		if _, ok := got[s]; !ok {
			diffs = append(diffs, s.fn+" / "+s.what+": frozen with "+itoa(want.n)+" but gone from the source (stale row)")
		}
	}
	sort.Strings(diffs)
	for _, d := range diffs {
		t.Errorf("entry removal census: %s", d)
	}

	// BEHAVIOURAL BACKING: every class names tests that exist in this package.
	exists := f088TestNames(t)
	for class, tests := range f088RemovalBacking {
		for _, name := range tests {
			if !exists[name] {
				t.Errorf("class %s is backed by %s, which does not exist", class, name)
			}
		}
	}
	for _, row := range f088FrozenRemovals {
		if len(f088RemovalBacking[row.class]) == 0 {
			t.Errorf("class %s has no behavioural backing", row.class)
		}
	}
}

// f088TestNames is every `func TestX(` declared in this package's test sources.
func f088TestNames(t *testing.T) map[string]bool {
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
