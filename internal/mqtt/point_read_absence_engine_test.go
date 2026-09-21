// The properties the closure engine and its accounting must satisfy, and the deficit they
// measure rather than close.
//
// What these establish is closure under the DECLARED conservative relation, plus reviewed
// treatment of every discovered interpreter candidate. What they do NOT establish is that a
// discharge is true, that the census is complete, or that any conservatively reached function
// is a consequential caller. The dossier says the same thing in the same words, and
// TestPointReadCensusDossierAgrees checks that it still does.

package mqtt

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// prEngineOnPackage runs the closure over the real package with the real seeds and the
// converted clauses. It is the input every property below shares.
func prEngineOnPackage(t *testing.T) (*censusScan, *prClosureResult) {
	t.Helper()
	sc := censusScanPackage(t)
	res := prRunClosure(sc, prEngineInput{Seeds: prSeedInventory(sc), Clauses: prBuildClauses(sc)})
	return sc, res
}

// TestPointReadCensusSeedIntegrity is property 13, and it is the one that stops properties 1-3
// from holding vacuously over a domain the engine itself shrank. Every seed the inventory
// discovers must be admitted as an origin, whether or not its effect could be attributed and
// whether or not its classification has been reviewed.
func TestPointReadCensusSeedIntegrity(t *testing.T) {
	t.Parallel()
	sc, res := prEngineOnPackage(t)
	seeds := prSeedInventory(sc)
	if len(seeds) == 0 {
		t.Fatal("the seed inventory is empty, so every property here would pass vacuously")
	}
	for _, s := range seeds {
		id := s.Origin.String()
		if _, ok := res.OriginIndex[id]; !ok {
			t.Errorf("seed %s (%s) was not admitted as an origin. Seed admission is "+
				"unconditional: a seed whose effect cannot be attributed is admitted with "+
				"unknown-effect, never dropped", id, s.Category)
			continue
		}
		if !res.Has(s.Origin.Fn, id, prArrLocal) {
			t.Errorf("seed %s was admitted but never reached its own function as origin-local, "+
				"so the obligation it names starts nowhere", id)
		}
		if !prComponentLegal(s.Origin.Comp) {
			t.Errorf("seed %s carries component %q, which is outside the grammar", id, s.Origin.Comp)
		}
	}
	// Every mechanically discoverable seed category must be represented, or the discovery has
	// silently narrowed. Counts are a bound and drift; the CATEGORIES are the mechanism.
	byCat := map[string]int{}
	for _, s := range seeds {
		byCat[s.Category]++
	}
	for _, cat := range []string{prSeedPrimitive, prSeedPredicate, prSeedSentinel} {
		if byCat[cat] == 0 {
			t.Errorf("no seed of category %q was discovered. Each category finds obligations the "+
				"others cannot: the primitive callers, the 404 predicates, and the absence "+
				"sentinels a wrapper produces without calling a primitive at all", cat)
		}
	}
}

// TestPointReadCensusArrivalsAreLegal is the vocabulary half of properties 2 and 3: every
// reached state names an arrival inside the finite vocabulary, so no transfer can escape by
// inventing a carrier, and an unestablished transfer has its own value rather than vanishing.
func TestPointReadCensusArrivalsAreLegal(t *testing.T) {
	t.Parallel()
	_, res := prEngineOnPackage(t)
	seen := map[string]bool{}
	for st := range res.Reached {
		if !prArrivalLegal(st.Arrival) {
			t.Errorf("state %s/%s names an arrival outside the vocabulary", st.Fn, st.Arrival)
		}
		seen[st.Arrival] = true
	}
	// origin-local must be present, or nothing was seeded; ret-of-callee must be present, or
	// the caller direction is not running at all.
	for _, want := range []string{prArrLocal, prArrRet} {
		if !seen[want] {
			t.Errorf("no reached state carries arrival %q, so a whole edge direction is missing "+
				"from the closure", want)
		}
	}
}

// TestPointReadCensusUnresolvedObligationsRetained is property 11: resolution status is
// reported SEPARATELY from review status, and the three under-approximation routes each leave a
// positive record. A regression removes an expected obligation instead of shrinking a total,
// which is why each arm asserts a named witness rather than a count.
func TestPointReadCensusUnresolvedObligationsRetained(t *testing.T) {
	t.Parallel()
	sc, res := prEngineOnPackage(t)

	if len(res.UnresolvedCallee) == 0 {
		t.Error("no unresolved callee obligation was recorded, although this package calls " +
			"functions it does not declare. An external destination is not an empty successor set")
	}
	if len(res.UnresolvedDispatch) == 0 {
		t.Error("no possible-only dispatch obligation was recorded. A name match is not a proof " +
			"of dispatch, and the speculative destinations must be accompanied by one")
	}
	if len(res.UnresolvedState) == 0 {
		t.Error("no unresolved-state obligation was recorded, although this package writes " +
			"through indexes and dereferences whose targets have no field identity")
	}

	// The reverse-call inventory must name every function whose name is taken as a VALUE.
	// Recomputed here from the scan rather than read back from the engine.
	valued := map[string]bool{}
	for _, v := range sc.ValueRefs {
		valued[v.Name] = true
	}
	want := map[string]bool{}
	for k := range sc.Funcs {
		if valued[k.Name] {
			want[k.String()] = true
		}
	}
	if len(want) == 0 {
		t.Fatal("the scan found no function-value reference at all, so this property would pass " +
			"vacuously — the value-reference inventory is not being collected")
	}
	for fn := range want {
		if !res.UnresolvedReverse[fn] {
			t.Errorf("%s has its name taken as a value somewhere in the package, but carries no "+
				"unresolved reverse-call obligation. Name-based reverse lookup cannot find a "+
				"caller that invoked it through a variable", fn)
		}
	}
	for fn := range res.UnresolvedReverse {
		if !want[fn] {
			t.Errorf("%s carries an unresolved reverse-call obligation although its name is "+
				"never taken as a value. A spurious obligation inflates the deficit", fn)
		}
	}
}

// TestPointReadCensusDispositionsAreWellFormed is the schema half of property 6.
func TestPointReadCensusDispositionsAreWellFormed(t *testing.T) {
	t.Parallel()
	for fn, d := range prWorklistDispositions {
		if strings.TrimSpace(d.Why) == "" {
			t.Errorf("disposition for %s carries no reason. A disposition without one is the "+
				"\"I stopped looking\" shape this census rejects", fn)
		}
		switch d.Kind {
		case prDispoInterprets:
			if d.Mapping == "" || d.Auth == "" || d.ClassEv == "" || d.ConsEv == "" || d.Reach == "" {
				t.Errorf("%s interprets a read but leaves an axis unset. An interpreting site "+
					"owes the same axes a declared node owes", fn)
			}
			if d.Repair == "" {
				t.Errorf("%s interprets a read but names no repair candidate or premises. "+
					"Property 14 requires a repair record or a justified not-applicable", fn)
			}
		case prDispoConsumes:
			if d.Source == "" {
				t.Errorf("%s consumes a verdict but names no producing site, so the "+
					"interpretation it rests on is unaccounted", fn)
			}
			if d.Auth == "" {
				t.Errorf("%s consumes a verdict but records nothing about what it authorizes, "+
					"which is the consequence a repair has to satisfy", fn)
			}
		case prDispoNotAnInterp:
			if d.Auth != "" || d.Mapping != "" {
				t.Errorf("%s is recorded as not an interpretation yet carries a mapping or an "+
					"authorization; those are contradictory", fn)
			}
		default:
			t.Errorf("%s carries disposition kind %q, which is outside the vocabulary", fn, d.Kind)
		}
	}
}

// TestPointReadCensusWorklistCovered is property 6, and property 14's coverage half. Every
// member of the discovered worklist carries either a declared node record or a reviewed
// disposition; a disposition for a function that is not on the worklist is stale; and a
// `consumes` disposition must name a producer that is itself accounted.
func TestPointReadCensusWorklistCovered(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	wl := prBuildWorklist(sc)
	if len(wl) == 0 {
		t.Fatal("the worklist is empty, so its coverage would pass vacuously")
	}
	accounted := func(fn string) bool {
		if _, ok := prCensusNodes[fn]; ok {
			return true
		}
		_, ok := prWorklistDispositions[fn]
		return ok
	}
	for fn, m := range wl {
		if !accounted(fn) {
			t.Errorf("%s is on the classification worklist (%s) with neither a declared node "+
				"record nor a reviewed disposition. Absence from the worklist never manufactures "+
				"a classification, and neither does silence inside it",
				fn, strings.Join(m.Reasons, "; "))
		}
	}
	for fn, d := range prWorklistDispositions {
		if _, ok := wl[fn]; !ok {
			t.Errorf("a disposition is recorded for %s, which the worklist does not contain. A "+
				"stale disposition claims review of something no longer discovered", fn)
		}
		if _, dup := prCensusNodes[fn]; dup {
			t.Errorf("%s carries both a declared node record and a worklist disposition; one of "+
				"the two is the authority and two records state one fact", fn)
		}
		if d.Kind == prDispoConsumes && !accounted(d.Source) {
			t.Errorf("%s consumes a verdict from %s, which is neither a declared node nor a "+
				"dispositioned worklist member, so the interpretation is unaccounted", fn, d.Source)
		}
	}
	// Reviewed additions must still name functions the package declares.
	declared := map[string]bool{}
	for k := range sc.Funcs {
		declared[k.String()] = true
	}
	for fn := range prReviewedAdditions {
		if !declared[fn] {
			t.Errorf("reviewed worklist addition %q names a function this package no longer "+
				"declares", fn)
		}
	}
}

// TestPointReadCensusSentinelClosureFinds is the discovery property for the sentinel fixpoint.
// It asserts the two witnesses the predecessor lane's prototype rediscovered WITHOUT either
// function being named — that is the property that matters, because a hand-listed set would
// have found neither — and that the closure is strictly larger than its roots.
func TestPointReadCensusSentinelClosureFinds(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	set := prSentinelClosure(sc)
	for _, root := range prSentinelRoots {
		if !set[root] {
			t.Errorf("sentinel root %q is missing from the closure", root)
		}
	}
	for _, derived := range []string{"mqttRepairSaveForeign", "mqttUncertainForeign"} {
		if !set[derived] {
			t.Errorf("the sentinel closure did not derive %q. It is reached only through a "+
				"guarded arm on another sentinel, and it is one of the two witnesses a "+
				"hand-named list missed", derived)
		}
	}
	if len(set) <= len(prSentinelRoots) {
		t.Errorf("the sentinel closure derived nothing beyond its %d roots, so the fixpoint is "+
			"not closing over guarded arms at all", len(prSentinelRoots))
	}
}

// prDeficit is the measured semantic-review deficit. Every figure here is reported rather than
// defined away, and reviewed PROPAGATION is kept apart from reviewed DISCHARGE because some
// propagation is the right answer and is not a missing discharge.
type prDeficit struct {
	Origins            int
	States             int
	Triples            int
	ReachedFns         int
	Discharged         int
	Propagated         int
	DefaultPairs       int
	DefaultFns         int
	UnresolvedCallee   int
	UnresolvedDispatch int
	UnresolvedReverse  int
	UnresolvedState    int
	WorklistMembers    int
	// DeclaredInWorklist is the INTERSECTION of the declared nodes with the worklist, not the
	// declared-node population. Holistic round 6 found the dossier publishing the population
	// (42) in a table that reads as a partition of the 147 members, so the rows summed to 155.
	// Eight declared nodes are not mechanically discoverable — they call no 404 predicate and
	// reference no absence sentinel — so the worklist never proposes them.
	DeclaredInWorklist int
	Interprets         int
	Consumes           int
	NotInterpretations int
}

func prMeasureDeficit(t *testing.T) prDeficit {
	t.Helper()
	sc, res := prEngineOnPackage(t)
	pairs, fns := res.DefaultOnlyPairs()
	d := prDeficit{
		Origins:            len(res.OriginAt),
		States:             len(res.Reached),
		Triples:            res.TripleCount(),
		ReachedFns:         len(res.ReachedFunctions()),
		Discharged:         res.Disp[prDispDischarge],
		Propagated:         res.Disp[prDispPropagation],
		DefaultPairs:       pairs,
		DefaultFns:         fns,
		UnresolvedCallee:   len(res.UnresolvedCallee),
		UnresolvedDispatch: len(res.UnresolvedDispatch),
		UnresolvedReverse:  len(res.UnresolvedReverse),
		UnresolvedState:    len(res.UnresolvedState),
		WorklistMembers:    len(prBuildWorklist(sc)),
	}
	wl := prBuildWorklist(sc)
	for fn := range prCensusNodes {
		if wl[fn] != nil {
			d.DeclaredInWorklist++
		}
	}
	for _, disp := range prWorklistDispositions {
		switch disp.Kind {
		case prDispoInterprets:
			d.Interprets++
		case prDispoConsumes:
			d.Consumes++
		case prDispoNotAnInterp:
			d.NotInterpretations++
		}
	}
	return d
}

// TestPointReadCensusDeficitIsMeasured is the property that keeps this increment honest. The
// engine satisfies closure-completeness whatever the review coverage is, so the only thing
// standing between a green test and a vacuous claim is that the deficit is measured, nonzero
// where it is nonzero, and stated.
func TestPointReadCensusDeficitIsMeasured(t *testing.T) {
	t.Parallel()
	d := prMeasureDeficit(t)
	if d.DefaultPairs == 0 {
		t.Error("the measured deficit is zero. That would mean every reached (edge, origin, " +
			"arrival) triple carries a reviewed clause — which this increment does not claim and " +
			"has not done. A zero here is a broken measurement, not a complete census")
	}
	if d.Discharged == 0 || d.Propagated == 0 {
		t.Errorf("reviewed dispositions are not both represented (discharge=%d, propagation=%d). "+
			"They are counted apart because propagation is frequently the right answer and is "+
			"not a missing discharge", d.Discharged, d.Propagated)
	}
	if d.ReachedFns <= len(prCensusNodes) {
		t.Errorf("the closure reached %d functions against %d declared nodes, so it is not "+
			"expanding past the hand-authored tier at all", d.ReachedFns, len(prCensusNodes))
	}
}

// TestPointReadCensusDossierAgrees is property 7: the dossier agrees with the manifest and the
// engine BY IDENTITY — and holistic round 3 showed what that has to mean. Searching the
// document for each bold number anywhere in it is a COUNT check wearing an identity check's
// name: swapping the 361 reviewed discharges with the 223 reviewed propagations passed it,
// because both numbers were still present somewhere. So each figure is matched on the same
// LINE as its own label, and the reviewed work is matched by NAME.
func TestPointReadCensusDossierAgrees(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile(filepath.Join("..", "..", "docs", "findings", "point-read-absence-census.md"))
	if err != nil {
		t.Fatalf("reading the dossier: %v", err)
	}
	doc := string(body)
	d := prMeasureDeficit(t)

	// The dossier's NUMERIC sections are checked by exact rendering in
	// TestPointReadCensusDossierBlocksRender; lines outside those blocks are SCREENED, never
	// verified. The (row, column) registry that used to live here is deleted rather than kept
	// beside it: a byte comparison already covers every character of a block, and consult 4
	// ruled that formatting-based discovery cannot support exhaustive coverage anyway. What
	// remains here are the checks the rendering does NOT make — the prose baseline below, the
	// partition arithmetic, and the identities of reviewed work.

	// The attribution baseline is the one class-2 OWNED line: it sits outside every block, yet
	// the per-cause deltas cannot be read without it, so it needs a real owner rather than an
	// exemption. This is that owner, and it enforces the COMPLETE line — a `strings.Contains`
	// on the numeric fragment would leave the rest of the line unowned while the screen
	// skipped it, which is the round-12 defect one level down.
	if !prLineIsPresent(doc, prAttributionBaselineLine()) {
		t.Errorf("the dossier carries no line exactly equal to %q. The per-cause table is read "+
			"against that starting point, and the line is exempt from figure screening because "+
			"THIS check enforces it — so if it is absent or altered, nothing is checking it",
			prAttributionBaselineLine())
	}

	// The four rows below "discovered members" are a PARTITION of it, and the dossier presents
	// them as one. Round 6 found them summing to 155 against 147 members because one row
	// published a population rather than its intersection with the worklist — a discrepancy
	// every individual row check passed, because no row was wrong on its own terms.
	if sum := d.DeclaredInWorklist + d.Interprets + d.Consumes + d.NotInterpretations; sum != d.WorklistMembers {
		t.Errorf("the worklist breakdown sums to %d against %d discovered members (%d declared "+
			"nodes also discovered, %d interprets, %d consumes, %d not-an-interpretation). The "+
			"table is read as a partition, so a row that counts a different population "+
			"misattributes reviewed work", sum, d.WorklistMembers, d.DeclaredInWorklist,
			d.Interprets, d.Consumes, d.NotInterpretations)
	}

	// IDENTITIES, not just counts. Every interpreting site and every withdrawal is named in
	// the dossier, so a reader can check the reviewed work against the manifest rather than
	// against a total.
	// Matched as a TABLE ROW carrying the receiver-qualified identity, not as a substring of
	// the document. A bare name appears inside unrelated prose — deleting the
	// `(mqttInflightSourceCache).resolve` row still satisfied a strings.Contains check,
	// because `resolve` occurs inside the word `unresolved` — so an identity that is only
	// mentioned somewhere is not evidence that the site is listed.
	rows := map[string]bool{}
	for _, line := range strings.Split(doc, "\n") {
		if ident, ok := prDocRowIdentity(line); ok {
			rows[ident] = true
		}
	}
	for fn, disp := range prWorklistDispositions {
		if disp.Kind != prDispoInterprets {
			continue
		}
		if !rows[prDocIdentity(fn)] {
			t.Errorf("no dossier table row names %s (expected a row whose first cell is "+
				"`%s`), and it is reviewed as INTERPRETING a read. The interpreting population "+
				"is the part a repair has to act on, so it is named rather than counted",
				fn, prDocIdentity(fn))
		}
	}
	for node := range prConversionWithdrawn {
		if !rows[prDocIdentity(node)] {
			t.Errorf("no dossier table row names %s (expected a row whose first cell is "+
				"`%s`), and its reviewed coverage is WITHDRAWN. A withdrawal that is only "+
				"counted cannot be audited", node, prDocIdentity(node))
		}
	}

	for _, forbidden := range []string{
		"census complete",
		"all consequential callers identified",
		"repair-dependency map complete",
	} {
		if strings.Contains(strings.ToLower(doc), forbidden) {
			t.Errorf("the dossier contains %q. Consult 2 named that claim as one this increment "+
				"may not make: the census and the repair-dependency map remain incomplete", forbidden)
		}
	}
}

// TestPointReadCensusPredecessorsReconstruct is the integrity half of the provenance
// requirement. The relation is not materialised — storing one entry per admission cost
// gigabytes — so it has to be RECONSTRUCTIBLE, and a reconstruction that came back empty for a
// reached non-seed state would mean the relation had been lost rather than encoded.
func TestPointReadCensusPredecessorsReconstruct(t *testing.T) {
	t.Parallel()
	sc, res := prEngineOnPackage(t)
	in := prEngineInput{Seeds: prSeedInventory(sc), Clauses: prBuildClauses(sc)}
	seedStates := map[prState]bool{}
	for _, s := range in.Seeds {
		seedStates[prState{Fn: s.Origin.Fn, Arrival: prArrLocal}] = true
	}
	states := make([]prState, 0, len(res.Reached))
	for st := range res.Reached {
		if !seedStates[st] {
			states = append(states, st)
		}
	}
	sort.Slice(states, func(i, j int) bool {
		if states[i].Fn != states[j].Fn {
			return states[i].Fn < states[j].Fn
		}
		return states[i].Arrival < states[j].Arrival
	})
	if len(states) == 0 {
		t.Fatal("every reached state is a seed state, so the closure expanded nothing")
	}
	// A sample would not do for the CLAIM, but the claim is that reconstruction works; running
	// it over every state costs minutes, so the property checks a deterministic spread and the
	// dossier records that the relation is reconstructible rather than stored.
	step := len(states) / 64
	if step == 0 {
		step = 1
	}
	for i := 0; i < len(states); i += step {
		st := states[i]
		preds := prReconstructPredecessors(sc, in, res, st)
		if len(preds) == 0 {
			t.Errorf("state %s/%s is reached but reconstructs no predecessor, so the relation "+
				"that explains how the obligation got there has been lost", st.Fn, st.Arrival)
		}
	}
}

// TestPointReadCensusSameObligationCanonicalized is the declared relation's property, and it is
// the reason the published origin count is a count of OBLIGATIONS rather than of matches.
// mqttClassifyMsgGetResponse qualifies twice — by applying the 404 predicate, and by producing
// the absence sentinel — and those are one classification of one response. Both provenances
// must still reach the inventory; only the obligation is counted once.
func TestPointReadCensusSameObligationCanonicalized(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	const classifier = "inflight_register.go:mqttClassifyMsgGetResponse"
	canonical := prCanonicalOrigins(sc)
	if len(canonical) == 0 {
		t.Fatal("no same-obligation relation was derived, so the declaration is not in effect " +
			"and the deficit counts one obligation as two")
	}
	for from, to := range canonical {
		if !strings.HasPrefix(from, classifier+"@") || !strings.HasPrefix(to, classifier+"@") {
			t.Errorf("the relation maps %q -> %q, outside the declared site. Two occurrences in "+
				"one function are NOT the same obligation merely for sharing it — "+
				"checkResourceOverwrites' three reads are the standing counterexample", from, to)
		}
	}
	// The collapse must be real: the classifier contributes exactly one origin.
	seeds := prSeedInventory(sc)
	n := 0
	for _, s := range seeds {
		if s.Origin.Fn == classifier {
			n++
		}
	}
	if n != 1 {
		t.Errorf("the classifier contributes %d origins; the declared relation collapses its "+
			"predicate and its sentinel onto one", n)
	}
	// ...and both provenances must survive the collapse.
	joined := ""
	for _, s := range seeds {
		if s.Origin.Fn == classifier {
			joined = s.Note
		}
	}
	// BOTH occurrences must be auditable from the surviving inventory. Accepting a note that
	// merely says "predicate" was how holistic round 2 found the sentinel provenance being
	// dropped: the relation collapsed the obligation and threw away half of what produced it.
	if !strings.Contains(joined, "collapsed from") {
		t.Errorf("the surviving origin's provenance is %q, which does not name the occurrence "+
			"that was collapsed onto it", joined)
	}
	if !strings.Contains(joined, "predicate") || !strings.Contains(joined, "sentinel") {
		t.Errorf("the surviving origin's provenance is %q. Both occurrences produced it — the "+
			"404 predicate and the absence sentinel — and a reader must be able to audit which "+
			"were collapsed", joined)
	}
}

// TestPointReadCensusNonInterpretersCallNoProducer closes the class holistic rounds 1 and 3
// each found an instance of, rather than waiting for a third: a row dispositioned
// `not-an-interpretation` on the strength of its own predicate, whose body nonetheless CALLS an
// accounted per-subject producer and therefore consumes that producer's absence verdict.
//
// WHAT IT DOES NOT ESTABLISH, because a passing property is easy to over-read: it quantifies
// over the candidates a call NAMES, so a producer reached through an alias — `p := m.observe;
// p()` — escapes it. Indirect producer consumption is left explicitly unresolved, carried by
// the method's reverse-call obligation and counted in the deficit, not certified absent here.
// Passing means no row NAMES a producer it does not account for; it does not mean no row
// consumes one.
//
// It is mechanical on purpose. Round 1 found two such rows by reading, round 3 found two more,
// and a fifth would be found by neither — the enumeration is the defect, so the rule replaces
// the enumeration. A row that legitimately calls a producer without consuming its verdict is
// recorded as `consumes` with that producer as its Source, which is the honest place for it.
func TestPointReadCensusNonInterpretersCallNoProducer(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	// The accounted producers: every declared node, and every worklist member reviewed as
	// interpreting a read. Matched by NAME, because that is what a call site carries.
	producer := map[string]bool{}
	for node := range prCensusNodes {
		producer[node] = true
	}
	for fn, d := range prWorklistDispositions {
		if d.Kind == prDispoInterprets {
			producer[fn] = true
		}
	}
	byName := map[string][]censusFuncKey{}
	for k := range sc.Funcs {
		byName[k.Name] = append(byName[k.Name], k)
	}
	for fn, d := range prWorklistDispositions {
		if d.Kind != prDispoNotAnInterp {
			continue
		}
		for _, c := range sc.Calls {
			if c.Fn.String() != fn {
				continue
			}
			// CANDIDATE destinations, not a spelling. Matching names excluded a selector call
			// to a real producer whenever some local shared its name, and excluding every
			// bound name was broader still: a bare call with a same-named local in an
			// unrelated nested scope is AMBIGUOUS, and ambiguous must flag rather than pass.
			// Resolving instead removed the ensureBrokerStream false positive at its source.
			for _, d := range prResolveCall(byName, c) {
				if !producer[d.String()] {
					continue
				}
				t.Errorf("%s is dispositioned not-an-interpretation, but a call in its body NAMES "+
					"%s at CALL#%d — an accounted producer of a per-subject absence verdict. "+
					"Either it consumes that verdict, in which case it is `consumes` with %s as "+
					"its Source, or the reason must say why the verdict it receives carries "+
					"nothing. If the call cannot reach that candidate at all, the reason says so",
					fn, d.String(), c.Occ, d.String())
			}
		}
	}
}

// The per-cause attribution, MEASURED once by running the closure with and without the
// bare-call method filter, against the figures this increment started from. Withdrawing
// certainty ADDS propagation; only the filter removes edges. These are evidence, so they are
// written here rather than recomputed — and `TestPointReadCensusAttributionReconciles` ties
// them to the live figures, so they cannot drift apart unnoticed.
const (
	prAttrBaselineTriples  = 77687314
	prAttrBaselineCallee   = 4826
	prAttrBaselineDispatch = 4562

	prAttrWithdrawalTriples  = 1988
	prAttrWithdrawalCallee   = 0
	prAttrWithdrawalDispatch = 3917

	prAttrFilterTriples  = -272966
	prAttrFilterCallee   = 925
	prAttrFilterDispatch = -925
)

// TestPointReadCensusAttributionReconciles is the internal consistency the per-cause table
// claims: the increment's starting figures plus the two attributed causes ARE the figures
// published now. Without it the deltas are decoration — they would still sum correctly if the
// engine's real numbers moved, because nothing tied the arithmetic to a measurement.
func TestPointReadCensusAttributionReconciles(t *testing.T) {
	t.Parallel()
	d := prMeasureDeficit(t)
	for _, c := range []struct {
		what                     string
		base, withdrawal, filter int
		live                     int
	}{
		{"default-only triples", prAttrBaselineTriples, prAttrWithdrawalTriples, prAttrFilterTriples, d.DefaultPairs},
		{"unresolved callee", prAttrBaselineCallee, prAttrWithdrawalCallee, prAttrFilterCallee, d.UnresolvedCallee},
		{"possible-only dispatch", prAttrBaselineDispatch, prAttrWithdrawalDispatch, prAttrFilterDispatch, d.UnresolvedDispatch},
	} {
		if sum := c.base + c.withdrawal + c.filter; sum != c.live {
			t.Errorf("the attribution for %s reconciles to %d (%d baseline %+d withdrawal "+
				"%+d filter), and the engine measures %d. Either a cause is mis-attributed or "+
				"the engine moved and the published history of WHY it moved is now fiction",
				c.what, sum, c.base, c.withdrawal, c.filter, c.live)
		}
	}
}

func prIsBoldNum(cell string) bool {
	tok := cell
	if i := strings.Index(cell, "** "); i >= 0 {
		tok = cell[:i+2]
	}
	if !strings.HasPrefix(tok, "**") || !strings.HasSuffix(tok, "**") || len(tok) < 5 {
		return false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(tok, "**"), "**")
	body = strings.TrimPrefix(strings.TrimPrefix(body, "+"), "-")
	if body == "" {
		return false
	}
	for _, r := range body {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// prDossierCells splits a markdown table row into its cells, or reports that the line is not
// one. A separator row (|---|---|) is not a row of values.
func prDossierCells(line string) ([]string, bool) {
	l := strings.TrimSpace(line)
	if !strings.HasPrefix(l, "|") || !strings.HasSuffix(l, "|") {
		return nil, false
	}
	var cells []string
	for _, c := range strings.Split(strings.Trim(l, "|"), "|") {
		cells = append(cells, strings.TrimSpace(c))
	}
	// A delimiter row is one where EVERY cell is a delimiter. Judging it by the first cell
	// alone made `| --- | **999** |` invisible: holistic round 10 published a figure in a
	// qualitative table that way, and it passed both the guard and markdownlint.
	if len(cells) == 0 || prAllDelimiterCells(cells) {
		return nil, false
	}
	return cells, true
}

func prAllDelimiterCells(cells []string) bool {
	for _, c := range cells {
		t := strings.Trim(c, "-: ")
		if t != "" || !strings.Contains(c, "-") {
			return false
		}
	}
	return true
}

// prAttributionBaselineLine is the exact line the attribution baseline must occupy, built from
// the evidence constants so the declaration cannot drift away from the figures it states.
func prAttributionBaselineLine() string {
	return fmt.Sprintf(
		"this increment started from (**%d** triples, **%d** unresolved callee, **%d** possible-only",
		prAttrBaselineTriples, prAttrBaselineCallee, prAttrBaselineDispatch)
}

func prLineIsPresent(doc, want string) bool {
	for _, l := range strings.Split(doc, "\n") {
		if l == want {
			return true
		}
	}
	return false
}
