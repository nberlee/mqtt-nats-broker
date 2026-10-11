// Copyright 2026 The mqtt-nats-bridge Authors
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
	"sort"
	"strings"
	"testing"
)

// T13d (F-088 WP3, plan rev 20, N3): EVERY production reader of a register's `Cancelled` bit has
// an EVIDENCE DISPOSITION. The bit is no longer evidence that a PUBLISH was never sent (arm 4 is
// marker-only, WP3 edit 1): a reader may treat it as a shape constraint, a provenance check, a
// trigger for a MARKER probe, the recogniser of a refuted (marker-absent) clearance, its own
// write's idempotence, the tombstone of an exchange a withdrawal positively ENDED, a conservative
// exclusion that destroys nothing, or chapter P's experimental affine class — and never as a
// licence to destroy or free an ordinary register on the bit alone. A new reader fails this census
// until it is classified here. Writers are T32's (TestF088CancellationRegisterProducersAreFrozen).

// f088CancelledDisposition is what a read of the bit decides.
type f088CancelledDisposition string

const (
	// A SHAPE rule (decode validation, a transition kind's body rule, a boundary refusal): the bit
	// constrains which bodies are writable; it licenses nothing.
	f088CancelledShape f088CancelledDisposition = "shape"
	// The written PROVENANCE decides (a pending fence), never the bit's presence.
	f088CancelledProvenance f088CancelledDisposition = "provenance"
	// The bit only TRIGGERS or routes a decision taken on the allocation's unsent MARKER
	// (standing: genuine cancellation; absent: possibly attempted; unread: nothing concluded).
	f088CancelledMarkerGated f088CancelledDisposition = "marker-gated"
	// The recogniser or builder of a REFUTED cancellation's clearance, admitted by the write
	// boundary only on the marker's authoritative absence (WP3 edit 3).
	f088CancelledRefutedClearance f088CancelledDisposition = "refuted-clearance"
	// The writer recognising an existing tombstone of its own allocation (idempotence): nothing is
	// destroyed or freed on it.
	f088CancelledIdempotence f088CancelledDisposition = "idempotence"
	// The tombstone a WITHDRAWAL wrote beside an exchange it positively ended: the decision rests
	// on the ended exchange (or is routed through the door, which re-checks marker and phase).
	f088CancelledEndedWithdrawal f088CancelledDisposition = "ended-withdrawal"
	// Conservative: the bit EXCLUDES the record from a claim, contest or admission, or retains
	// it; nothing is destroyed or freed on it.
	f088CancelledConservative f088CancelledDisposition = "conservative"
	// Chapter P's EXPERIMENTAL AFFINE claim pass: a matching-nonce slot-only tombstone is that
	// class's close decision and its exact generation is DELETED on the bit. Outside the ordinary
	// contract (P-f); recorded here so the uniform rule (Q3) can see it, not classified as evidence-safe.
	f088CancelledAffineClaimClose f088CancelledDisposition = "affine-claim-close"
)

// f088CancelledBacking names the behavioural tests that witness each disposition.
var f088CancelledBacking = map[f088CancelledDisposition][]string{
	f088CancelledShape:      {"TestF088UnmarkedCancelledRegisterIsRefused", "TestF088PendingFenceProvenanceIsEnforcedAtTheBoundary"},
	f088CancelledProvenance: {"TestF088CopiedFreshAdmissionTombstoneIsNotAFence"},
	f088CancelledMarkerGated: {"TestF088MarkerlessCancelledRegisterRestoresActingOnColdResume",
		"TestF088MarkerBackedCancellationStillWithdraws", "TestMQTTSlotAuthorityCancellationIsNotNonTransmission"},
	f088CancelledRefutedClearance: {"TestMQTTAckArrowClearsAKnownFalseCancellation",
		"TestMQTTAcquisitionOverAGenuineCancellationIsRefused"},
	f088CancelledIdempotence:     {"TestF105WithdrawCancellationTombstoneEndsTheObligation"},
	f088CancelledEndedWithdrawal: {"TestF105WithdrawLandedBeforeACrashIsFinishedByTheNextPass"},
	f088CancelledConservative:    {"TestF088PendingFenceFromTheProducerIsDroppedOnColdResume"},
	f088CancelledAffineClaimClose: {"TestF088PendingFenceFromTheProducerIsDroppedOnColdResume",
		"TestF088MarkerBackedCancellationStillWithdraws"},
}

// f088FrozenCancelledReaders is the FROZEN census: every production function reading `.Cancelled`
// (outside an assignment's left-hand side), the number of reads, its disposition, and why. Frozen
// against the F-097/F-088 working tree of 2026-10-08 (base HEAD 7dd2c3e plus the uncommitted
// WP2/WP3 edits).
var f088FrozenCancelledReaders = map[string]struct {
	n    int
	disp f088CancelledDisposition
	why  string
}{
	"mqttValidateInflightShape": {3, f088CancelledShape,
		"decode validation: a pending store body carries no cancellation; a cancelled record cannot sit past PUBLISH with a stored PUBREL"},
	"mqttValidateCancelOrigin": {2, f088CancelledShape, "Cancelled iff CancelOrigin: an unmarked cancellation is refused, never inferred"},
	"mqttCheckTransitionKindRules": {1, f088CancelledShape,
		"the tombstone-register kind's body rule: only an untokened ordinary publish cancellation"},
	"checkPairBindings": {1, f088CancelledShape, "a tombstone-register create allocates nothing"},
	"checkRegisterReplacements": {3, f088CancelledShape,
		"only the tombstone kinds may set the bit, and only over an uncancelled untokened publish occupant"},
	"mqttPendingFenceBodyOK": {1, f088CancelledProvenance, "a pending fence is identified by CancelOrigin = pending-create, never by sparsity"},
	"mqttClassifyInflightSlot": {1, f088CancelledMarkerGated,
		"a cancelled occupant is decided by its probed marker: standing reaps (through the door), absent falls through as possibly attempted, unread abandons"},
	"runInflightAdmission": {2, f088CancelledMarkerGated,
		"the pre-classification marker probe's trigger; our own ended (withdrawn) tombstone is reaped only through the gated door and the allocation abandoned"},
	"recoverInflightRegistersOnce": {6, f088CancelledMarkerGated,
		"the genuine-cancellation arm requires the walked marker (edit 4; and its observation seam); the custody-contest exclusions are conservative routings. (The exchange-reach exemption for a cancelled register was REMOVED: an occupant cancellation without its exchange is P-d2 and reserved like any register.)"},
	"mqttClearsCancellation": {2, f088CancelledRefutedClearance, "the SHAPE half of the clearance; the boundary's evidence half requires the marker absent"},
	"mqttAdvanceExchangeAckTransition": {1, f088CancelledRefutedClearance,
		"the acknowledgement's builder clears an occupant cancellation; checkRefutedCancellation admits it only on the marker's absence"},
	"opAcquireAction": {1, f088CancelledRefutedClearance,
		"a retransmission's acquisition clears an occupant cancellation; refused at the boundary while the marker stands"},
	"markInflightRegisterCancelled": {1, f088CancelledIdempotence, "an already-cancelled occupant of ours: AlreadySet, nothing destroyed"},
	"markInflightRegisterCancelledPending": {1, f088CancelledIdempotence,
		"the conflict branch meets our own same-nonce tombstone: nothing rewritten"},
	"landedRegisterSeq": {1, f088CancelledEndedWithdrawal,
		"our landed register already withdrawn: not confirmed (no live entry for an ended exchange)"},
	"endUnsentBeforeDestroy": {1, f088CancelledEndedWithdrawal,
		"after a LANDED withdrawal, only OUR tombstone (by identity) may be deleted"},
	"retainedPairVerdict": {1, f088CancelledEndedWithdrawal, "a tombstone over an ENDED retained exchange"},
	"claimRecoveredInflight": {2, f088CancelledAffineClaimClose,
		"chapter P's slot-only claim pass: a cancelled record is never a transferable claim (conservative), but a " +
			"matching-nonce affine tombstone is that class's close decision and is DELETED on the bit (open Q3 question)"},
	"disposeSettledAffineRegister": {1, f088CancelledConservative, "a cancelled register is RETAINED by the orphan sweep"},
	"pubRelAdmissionContext":       {1, f088CancelledConservative, "a cancelled slot contradicts a stored PUBREL: admission refused"},
}

// f088ScanCancelledReaders counts, per enclosing production function, the `.Cancelled` selectors
// that are not an assignment's target.
func f088ScanCancelledReaders(t *testing.T) map[string]int {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	out := map[string]int{}
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
			lhs := map[ast.Expr]bool{}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if as, ok := n.(*ast.AssignStmt); ok {
					for _, l := range as.Lhs {
						lhs[l] = true
					}
				}
				return true
			})
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "Cancelled" && !lhs[sel] {
					out[fd.Name.Name]++
				}
				return true
			})
		}
	}
	return out
}

// TestF088EveryCancelledReaderHasAnEvidenceDisposition is T13d.
func TestF088EveryCancelledReaderHasAnEvidenceDisposition(t *testing.T) {
	t.Parallel()
	got := f088ScanCancelledReaders(t)
	var diffs []string
	for fn, n := range got {
		if want, ok := f088FrozenCancelledReaders[fn]; !ok || want.n != n {
			diffs = append(diffs, fn+": source has "+itoa(n)+" read(s) of Cancelled, frozen has "+itoa(want.n))
		}
	}
	for fn, want := range f088FrozenCancelledReaders {
		if _, ok := got[fn]; !ok {
			diffs = append(diffs, fn+": frozen with "+itoa(want.n)+" read(s) but gone from the source (stale row)")
		}
	}
	sort.Strings(diffs)
	for _, d := range diffs {
		t.Errorf("Cancelled reader census: %s", d)
	}
	exists := f088TestNames(t)
	for disp, tests := range f088CancelledBacking {
		for _, name := range tests {
			if !exists[name] {
				t.Errorf("disposition %s is backed by %s, which does not exist", disp, name)
			}
		}
	}
	for fn, row := range f088FrozenCancelledReaders {
		if len(f088CancelledBacking[row.disp]) == 0 {
			t.Errorf("%s: disposition %s has no behavioural backing", fn, row.disp)
		}
		if row.why == _EMPTY_ {
			t.Errorf("%s: a disposition without its reason", fn)
		}
	}
}
