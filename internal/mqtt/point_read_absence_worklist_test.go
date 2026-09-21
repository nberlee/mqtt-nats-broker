// F-109's seed inventory and classification worklist: what the closure starts from, and which
// functions owe a reviewed classification.
//
// Two rules from the consults govern this file and neither is negotiable.
//
// SEED ADMISSION IS UNCONDITIONAL. A seed whose effect cannot be attributed to a precise output
// component is admitted with `unknown-effect` and stays outstanding in the deficit. If admission
// depended on attribution succeeding, "reviewed interpreter" would become a discovery filter —
// and the whole point of the frontier's replacement is that no author-controlled judgement gets
// to shrink the domain.
//
// THE WORKLIST BOUNDS CLASSIFICATION WORK, NEVER COVERAGE. Its mechanical half finds candidates;
// its reviewed half records candidates found by reading. A function OUTSIDE the worklist keeps
// its conservative obligations and its `unreviewed` status — absence from the worklist never
// manufactures a `forwarded`, harmless or terminal classification, which is the distinction
// consult 2 drew after C1 refuted function-level terminality.

package mqtt

import (
	"sort"
	"strconv"
	"strings"
)

// prPredicateCallees are the 404 tests this package spells as calls. mqtt.go:11611 uses
// IsNatsErr rather than isErrorOtherThan, which is why a census keyed on one predicate spelling
// misses it and why both are here.
var prPredicateCallees = map[string]bool{
	"IsNatsErr":        true,
	"isErrorOtherThan": true,
}

// prSentinelRoots are the absence-derived outputs read from the code: each is what some site
// returns when it has decided "absent". They are a REVIEWED seed list, not a search result, and
// the closure below is what turns them into the set.
var prSentinelRoots = []string{
	"mqttInflightResponseAbsent",
	"mqttRetirementAbsent",
	"mqttAcceptFactAbsent",
	"mqttOutboundAbsent",
	"mqttRecordAttemptGone",
	"mqttAttemptGone",
	"mqttQ2AffCopyAbsent",
	"mqttTeardownReleased",
	"mqttTeardownClaimSuperseded",
	"mqttMetadataUnresolved",
	"mqttDisposalOwnershipUnsettled",
}

// prSentinelClosure closes the roots under "a guarded arm on a sentinel returns a package
// constant". The predecessor lane prototyped this with a regex and it converged in three rounds
// to twenty members, rediscovering two witnesses that a plan-review round had found by hand
// WITHOUT either function being named — which is the property that matters here, and the reason
// the set is computed rather than listed. This implementation walks the AST arms instead, so it
// may legitimately find more.
//
// It is DISCOVERY ONLY. Membership never exempts an edge and never terminates a walk.
func prSentinelClosure(sc *censusScan) map[string]bool {
	return prSentinelClosureFrom(sc, prSentinelRoots)
}

// prSentinelClosureFrom takes its roots as a PARAMETER so a fixture can exercise the fixpoint
// without swapping a package global. Swapping one is what the parallel-safety guard in this
// package exists to catch, and it caught this: a fixture that reassigns prSentinelRoots can
// corrupt, or be corrupted by, whatever runs beside it.
func prSentinelClosureFrom(sc *censusScan, roots []string) map[string]bool {
	set := map[string]bool{}
	for _, r := range roots {
		set[r] = true
	}
	for {
		added := 0
		for _, arm := range sc.Arms {
			if !set[arm.Guard] {
				continue
			}
			for _, r := range arm.Returns {
				if _, isConst := sc.Consts[r]; !isConst || set[r] {
					continue
				}
				set[r] = true
				added++
			}
		}
		if added == 0 {
			return set
		}
	}
}

// prWorklistMember is one candidate and how it was found. Provenance is kept because a reviewed
// addition and a mechanical match are different evidence, and a stale reviewed addition must be
// detectable.
type prWorklistMember struct {
	Fn         string
	Mechanical bool
	Reasons    []string
}

// prReviewedAdditions are candidates found by READING rather than by a predicate or a sentinel:
// consult 2 required the worklist to be the union of the mechanical candidates and separately
// persisted reviewed additions, each carrying its provenance. An entry naming a function that no
// longer exists fails as stale.
var prReviewedAdditions = map[string]string{
	"mqtt.go:(mqttAccountSessionManager).loadRetainedMessages": "response review: it classifies " +
		"a batched result inline and spells its 404 test IsNatsErr, so repairing the shared " +
		"classifier does not repair it",
	"inflight_marker.go:(mqttAccountSessionManager).loadInflightMarker": "response review: its " +
		"err != nil || smsg == nil arm folds a failed read into the absent verdict a resume " +
		"consumes",
	"outbound_release_gate.go:(mqttAccountSessionManager).gatherAdoptedReleaseFacts": "consequence " +
		"review: it states in capitals that an unresolved read is an error, and its loadGenClosure " +
		"maps a 404 to absent on a production path",
	"outbound_transition.go:(mqttJSA).checkResourceOverwrites": "callback review: the sealable " +
		"closure's absent arm reports SUCCESS, which the helper consumes as the licence to seal",
	"outbound_completion.go:(mqttAccountSessionManager).enumerateGenerationObligations": "omitted-" +
		"update review: absence travels as additions that never happen to two fields of a " +
		"returned local, so no assignment occurrence names it",
	"mqtt.go:(mqttJSA).loadSessionMsg": "wrapper review: it forwards the primitive's error " +
		"untested, so its callers are the interpretation sites and an inventory keyed on the " +
		"loader hides all sixteen",
}

// prBuildWorklist is the union. Mechanical membership is recomputed from the scan every run;
// reviewed additions are persisted here and checked against the scan for staleness.
func prBuildWorklist(sc *censusScan) map[string]*prWorklistMember {
	out := map[string]*prWorklistMember{}
	add := func(fn string, mechanical bool, reason string) {
		m := out[fn]
		if m == nil {
			m = &prWorklistMember{Fn: fn}
			out[fn] = m
		}
		if mechanical {
			m.Mechanical = true
		}
		for _, r := range m.Reasons {
			if r == reason {
				return
			}
		}
		m.Reasons = append(m.Reasons, reason)
	}
	sentinels := prSentinelClosure(sc)
	for _, c := range sc.Calls {
		if prPredicateCallees[c.Callee] {
			add(c.Fn.String(), true, "404 predicate "+c.Callee)
		}
	}
	for _, r := range sc.Idents {
		switch {
		case r.Name == "JSNoMessageFoundErr":
			add(r.Fn.String(), true, "names JSNoMessageFoundErr")
		case sentinels[r.Name]:
			add(r.Fn.String(), true, "absence sentinel "+r.Name)
		}
	}
	for fn, why := range prReviewedAdditions {
		add(fn, false, "reviewed addition: "+why)
	}
	for _, m := range out {
		sort.Strings(m.Reasons)
	}
	return out
}

// ---------------------------------------------------------------------------
// The seed inventory.
// ---------------------------------------------------------------------------

// prSeedComponents is the REVIEWED attribution of a seed occurrence to the output component
// that carries its absence-derived fact. It is reviewed data, never inference: a component the
// engine guessed would be a claim about data flow this census does not make. An occurrence
// absent from this table is admitted with `unknown-effect`, which propagates through every
// outgoing edge — the conservative direction.
//
// The four worked mappings the plan-review rounds asked for are here, and each says why.
var prSeedComponents = map[string]string{
	// checkResourceOverwrites reads three times, and the three carry different things. The
	// reads at occurrences 7 and 19 are inside FUNCTION LITERALS — `terminal` and `sealable` —
	// so their results are not this function's return at all: lit#k/ret#i is the only identity
	// they have, and the sealable one is what mqttCheckResourceStep consumes as the licence to
	// seal. The read at occurrence 32 is the enclosing function's own.
	"outbound_transition.go:(mqttJSA).checkResourceOverwrites@7":  "lit#0/ret#0",
	"outbound_transition.go:(mqttJSA).checkResourceOverwrites@19": "lit#1/ret#0",
	"outbound_transition.go:(mqttJSA).checkResourceOverwrites@32": "ret#0",

	// Absence travels here as additions that never happen to two fields of a returned local.
	// Neither is a receiver field and neither has an assignment occurrence to attribute, so the
	// RETURN component conservatively encompasses both — and a field-scoped clause may
	// therefore never be offered as coverage of this origin, which the clause-scoping property
	// enforces.
	"outbound_completion.go:(mqttAccountSessionManager).enumerateGenerationObligations@10": "ret#0",
	"outbound_completion.go:(mqttAccountSessionManager).enumerateGenerationObligations@25": "ret#0",

	// Unnamed results: components are positional, because result names are optional and an
	// inventory keyed on them would lose every unnamed one. loadSessionMsg interprets nothing
	// and forwards the primitive's error untested, which is why its sixteen callers are the
	// real interpretation sites.
	"mqtt.go:(mqttJSA).loadSessionMsg@0": "ret#0",
}

// The SAME-OBLIGATION declaration: two seed OCCURRENCES that name ONE obligation. Plan §3.2 requires
// it for mqttClassifyMsgGetResponse, which qualifies twice — once by applying the 404
// predicate and once by producing the absence sentinel — and both are the same fact about the
// same response. Without the declaration the deficit counts one obligation as two.
//
// It is a DECLARATION, never an inference. Two occurrences in one function are not the same
// obligation merely for sharing a function: checkResourceOverwrites' three reads are the
// standing counterexample. The key is the canonical origin; the value lists the occurrences
// that resolve onto it, so both provenances stay recorded.
// prCanonicalOrigins maps each seed occurrence onto the occurrence that carries its obligation.
// Only the declared relation collapses anything; every other occurrence is its own origin.
func prCanonicalOrigins(sc *censusScan) map[string]string {
	out := map[string]string{}
	const classifier = "inflight_register.go:mqttClassifyMsgGetResponse"
	canonical := ""
	for _, c := range sc.Calls {
		if c.Fn.String() == classifier && prPredicateCallees[c.Callee] {
			canonical = classifier + "@" + strconv.Itoa(c.Occ)
			break
		}
	}
	if canonical == "" {
		return out
	}
	sentinels := prSentinelClosure(sc)
	for _, r := range sc.Idents {
		if r.Fn.String() == classifier && sentinels[r.Name] {
			out[classifier+"@"+strconv.Itoa(prIdentOccBase+r.Occ)] = canonical
		}
	}
	return out
}

// prSeedInventory builds the seeds. Categories 1-3 are mechanical; category 4, the legacy
// obligations the checkpoint already accounted, is supplied by the conversion in
// point_read_absence_conversion_test.go and mapped ONTO these identities rather than admitted as
// a disconnected new origin.
func prSeedInventory(sc *censusScan) []prSeed {
	var seeds []prSeed
	seen := map[string]int{}
	canonical := prCanonicalOrigins(sc)
	push := func(fn string, occ int, category, note string) {
		// A declared same-obligation occurrence resolves onto its canonical one, so the
		// obligation is counted once while both provenances reach the inventory.
		collapsedFrom := ""
		if to, ok := canonical[fn+"@"+strconv.Itoa(occ)]; ok {
			collapsedFrom = fn + "@" + strconv.Itoa(occ)
			at := strings.LastIndex(to, "@")
			fn = to[:at]
			occ, _ = strconv.Atoi(to[at+1:])
		}
		comp := prSeedComponents[fn+"@"+strconv.Itoa(occ)]
		if comp == "" {
			comp = prCompUnknown
		}
		o := prOrigin{Fn: fn, Occ: occ, Comp: comp}
		if collapsedFrom != "" {
			note = "collapsed from " + collapsedFrom + " by the declared same-obligation " +
				"relation: " + note
		}
		if i, dup := seen[o.String()]; dup {
			// BOTH provenances survive. An early return here would keep whichever occurrence
			// happened to be pushed first and silently drop the other, which is the opposite
			// of what a declared same-obligation relation is for: the point is that a reader
			// can audit WHICH occurrences were collapsed onto this origin.
			if !strings.Contains(seeds[i].Note, note) {
				seeds[i].Note += " + " + note
			}
			return
		}
		seen[o.String()] = len(seeds)
		seeds = append(seeds, prSeed{Origin: o, Category: category, Note: note})
	}
	for _, c := range sc.Calls {
		switch {
		case c.Callee == "loadLastMsgFor" || c.Callee == "loadLastMsgForMulti":
			push(c.Fn.String(), c.Occ, prSeedPrimitive, "per-subject point read "+c.Callee)
		case prPredicateCallees[c.Callee]:
			push(c.Fn.String(), c.Occ, prSeedPredicate, "404 predicate "+c.Callee)
		}
	}
	sentinels := prSentinelClosure(sc)
	for _, r := range sc.Idents {
		if !sentinels[r.Name] {
			continue
		}
		push(r.Fn.String(), prIdentOccBase+r.Occ, prSeedSentinel, "absence sentinel "+r.Name)
	}
	sort.Slice(seeds, func(i, j int) bool { return seeds[i].Origin.String() < seeds[j].Origin.String() })
	return seeds
}

// prIdentOccBase keeps an identifier-reference occurrence from colliding with a call occurrence
// in the same function. Occurrence numbers are per-function and per-kind, so the two spaces are
// separated by construction rather than by hoping they do not overlap.
const prIdentOccBase = 1000000
