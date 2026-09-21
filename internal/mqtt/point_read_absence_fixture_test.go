// Discriminating fixtures for the closure engine.
//
// Consult 2's constraint: the engine must not certify itself by generating both sides of the
// same equality. So every fixture here is synthetic Go SOURCE parsed by the real scanner and
// resolved by the real resolver, and every expectation is a LITERAL written in the fixture —
// never a value the engine derived. A graph assembled by hand would not see a scanner that
// stopped descending into function literals, and an expectation read back from the engine
// would not see a resolver that resolved to the wrong destination.
//
// Each fixture states the mutation it exists to kill. A fixture whose assertion is only "the
// function was reached" is deliberately avoided: reachability can be satisfied by another
// route, so the assertions name edges, origins and arrivals.

package mqtt

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// prFixtureSeed builds one seed by hand, so the fixture's expectations do not depend on seed
// DISCOVERY. Discovery has its own fixtures below.
func prFixtureSeed(fn string, occ int, comp string) prSeed {
	return prSeed{Origin: prOrigin{Fn: fn, Occ: occ, Comp: comp}, Category: prSeedPrimitive}
}

func prFixtureRun(t *testing.T, src string, seeds ...prSeed) (*censusScan, *prClosureResult) {
	t.Helper()
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	return sc, prRunClosure(sc, prEngineInput{Seeds: seeds})
}

// TestPointReadClosureFixtureCallerDepth kills M3 (stop the engine at depth 1). The literal
// third-tier pair is written out; reaching only tier one leaves it absent.
func TestPointReadClosureFixtureCallerDepth(t *testing.T) {
	t.Parallel()
	src := `
func tierZero() error { return nil }
func tierOne() error  { return tierZero() }
func tierTwo() error  { return tierOne() }
func tierThree() error { return tierTwo() }
`
	_, res := prFixtureRun(t, src, prFixtureSeed("fixture.go:tierZero", 0, "ret#0"))
	origin := "fixture.go:tierZero@0/ret#0"
	for _, want := range []string{"fixture.go:tierOne", "fixture.go:tierTwo", "fixture.go:tierThree"} {
		if !res.Has(want, origin, prArrRet) {
			t.Errorf("%s did not receive %s at %s. The closure has a depth cutoff, which the "+
				"contract forbids: there is no tier at which an obligation stops being one",
				want, origin, prArrRet)
		}
	}
}

// TestPointReadClosureFixtureCallback kills M4a (skip calls inside a FuncLit, in the SCANNER)
// and M4b (drop the lit#k/ret#i -> callback-ret#i transfer). A graph fixture can see neither.
func TestPointReadClosureFixtureCallback(t *testing.T) {
	t.Parallel()
	src := `
func consumer(f func() error) error { return f() }

func insideTheLiteral() error { return nil }

func host() error {
	sealable := func() error { return insideTheLiteral() }
	return consumer(sealable)
}
`
	sc, res := prFixtureRun(t, src, prFixtureSeed("fixture.go:host", 2, "lit#0/ret#0"))
	// M4a: the scanner must descend INTO the function literal and number the call it contains
	// among the host's own. A scanner that returns false at a FuncLit reports neither the call
	// nor the literal, and nothing downstream can recover it.
	var inLiteral bool
	for _, c := range sc.Calls {
		if c.Fn.Name == "host" && c.Callee == "insideTheLiteral" {
			inLiteral = true
		}
	}
	if !inLiteral {
		t.Error("the scanner did not report the call made INSIDE the function literal as one of " +
			"the host's calls. A closure's call is the enclosing function's behaviour, and a " +
			"walk that stops at the literal loses the whole callback route")
	}
	if len(sc.Lits[censusFuncKey{File: "fixture.go", Name: "host"}]) != 1 {
		t.Errorf("the scanner recorded %d function literals in host, expected exactly 1; the "+
			"lit#k/ret#i component has no identity without them",
			len(sc.Lits[censusFuncKey{File: "fixture.go", Name: "host"}]))
	}
	origin := "fixture.go:host@2/lit#0/ret#0"
	if !res.Has("fixture.go:consumer", origin, "callback-ret#0") {
		t.Errorf("consumer did not receive %s as callback-ret#0. The carrier transfer for a "+
			"function-literal result is the only route by which a callback's absence-derived "+
			"success reaches the helper that consumes it", origin)
	}
}

// TestPointReadClosureFixtureStateEdges kills M5a (drop state edges) and M5b (drop mutations
// with no resolvable field). The fieldless case needs its own fixture: a field-destination
// fixture cannot see it, because an index target has no spelling to match destinations on.
func TestPointReadClosureFixtureStateEdges(t *testing.T) {
	t.Parallel()
	src := `
type holder struct {
	mark  bool
	table map[string]bool
}

func writer(h *holder, flag *bool, local map[string]bool) {
	h.mark = true
	*flag = true
	local["k"] = true
}

func reader(h *holder) bool { return h.mark }
`
	_, res := prFixtureRun(t, src, prFixtureSeed("fixture.go:writer", 0, "field:h.mark"))
	origin := "fixture.go:writer@0/field:h.mark"
	if !res.Has("fixture.go:reader", origin, prArrState+"mark") {
		t.Errorf("reader did not receive %s at state:mark. A field write reaches every "+
			"package-wide reference to that spelling; dropping state edges removes a whole "+
			"candidate kind", origin)
	}
	found := false
	for k := range res.UnresolvedState {
		if strings.Contains(k, "fixture.go:writer") {
			found = true
		}
	}
	if !found {
		t.Error("the dereference and local-map writes recorded no unresolved-state obligation. " +
			"A mutation with no resolvable field has no destination set to enumerate, so it " +
			"must be recorded as unresolved rather than dropped — and a field fixture cannot " +
			"see this, because a field write has a spelling to match destinations on")
	}
}

// TestPointReadClosureFixtureSecondArrival kills M6a (mark a function visited once regardless
// of origin) and M6b (ignore the arrival axis and dedupe on (function, origin)). ONE origin
// reaches ONE function through TWO carriers — an argument and a state field — which is the
// same-origin hazard plan-review round 2 named: a discharge reviewed for an incoming argument
// establishes nothing about the same origin arriving through state.
func TestPointReadClosureFixtureSecondArrival(t *testing.T) {
	t.Parallel()
	src := `
type box struct{ flag bool }

func producer(b *box, x error) error {
	b.flag = true
	return sink(x)
}

func sink(x error) error { return x }

func alsoSink(b *box) bool { return b.flag }
`
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	origin := "fixture.go:producer@0/ret#0"
	res := prRunClosure(sc, prEngineInput{Seeds: []prSeed{
		prFixtureSeed("fixture.go:producer", 0, "ret#0"),
	}})
	if !res.Has("fixture.go:sink", origin, "arg#0") {
		t.Errorf("%s did not reach sink at arg#0", origin)
	}
	if !res.Has("fixture.go:alsoSink", origin, prArrState+"flag") {
		t.Errorf("%s did not reach alsoSink at state:flag. The SAME origin arriving through a "+
			"second carrier is a second obligation to account for, and an engine keyed on "+
			"(function, origin) alone records it as a predecessor and never propagates it", origin)
	}
	// A discharge reviewed for the ARGUMENT carrier must leave the STATE carrier standing.
	res2 := prRunClosure(sc, prEngineInput{
		Seeds: []prSeed{prFixtureSeed("fixture.go:producer", 0, "ret#0")},
		Clauses: map[string][]prClause{
			"fixture.go:producer CALL#0": {{
				Pairs: map[string]bool{prPairKey(origin, prArrLocal): true},
				Disp:  edgeDischarge, Why: "fixture: reviewed for the call edge only",
			}},
		},
	})
	if res2.Has("fixture.go:sink", origin, "arg#0") {
		t.Error("the call edge was discharged and the argument arrival survived it")
	}
	if !res2.Has("fixture.go:alsoSink", origin, prArrState+"flag") {
		t.Error("discharging the CALL edge also terminated the STATE carrier. Applicability is " +
			"a relation over (origin, arrival); it is not a set of origins")
	}
}

// TestPointReadClosureFixtureUnresolved kills M7a (treat an unresolved callee as an empty
// successor set) and M7b (drop the retained unresolved obligation where a name COLLIDES with a
// declared function). The assertions name the exact obligation, because reachability alone
// cannot detect its disappearance.
func TestPointReadClosureFixtureUnresolved(t *testing.T) {
	t.Parallel()
	src := `
func hook() error { return nil }

type carrier struct{ hook func() error }

func viaValue(c *carrier) error { return c.hook() }

func viaExternal() string { return externalPackage.Thing() }

func target() error { return nil }

var alias = target

func viaBare() error { return target() }
`
	_, res := prFixtureRun(t, src, prFixtureSeed("fixture.go:viaValue", 0, "ret#0"))
	var collision, external bool
	for k := range res.UnresolvedDispatch {
		if strings.Contains(k, "fixture.go:viaValue") && strings.HasSuffix(k, "hook") {
			collision = true
		}
	}
	for k := range res.UnresolvedCallee {
		if strings.Contains(k, "Thing") {
			external = true
		}
	}
	if !collision {
		t.Error("a call through a function-valued FIELD named `hook`, colliding with a declared " +
			"`hook`, recorded no unresolved obligation. The engine would emit only the wrong " +
			"destination and the fallback would never fire")
	}
	// The BARE-IDENTIFIER route is a different branch of the resolver and needs its own
	// witness: `target` is called by name AND taken as a value, so the call site may be
	// invoking either, and the obligation is retained beside the candidate it names.
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	resBare := prRunClosure(sc, prEngineInput{Seeds: []prSeed{
		prFixtureSeed("fixture.go:viaBare", 0, "ret#0"),
	}})
	bare := false
	for k := range resBare.UnresolvedDispatch {
		if strings.Contains(k, "fixture.go:viaBare") && strings.HasSuffix(k, "target") {
			bare = true
		}
	}
	if !bare {
		t.Error("a bare call to `target`, whose name is also taken as a VALUE, recorded no " +
			"unresolved obligation. A unique declaration is not a proof of dispatch when the " +
			"same name denotes a function value somewhere in the package")
	}
	if !external {
		// viaExternal is only reached if seeded; assert the resolver's classification directly.
		res2 := prRunClosure(sc, prEngineInput{Seeds: []prSeed{
			prFixtureSeed("fixture.go:viaExternal", 0, "ret#0"),
		}})
		for k := range res2.UnresolvedCallee {
			if strings.Contains(k, "Thing") {
				external = true
			}
		}
	}
	if !external {
		t.Error("a call to a callee this package does not declare recorded no unresolved " +
			"obligation. An external destination is not an empty successor set")
	}
}

// TestPointReadClosureFixtureClauseScoping kills M8 (let a discharge apply to an origin it does
// not name) and M16's shape (widen a clause). The surviving successor is literal.
func TestPointReadClosureFixtureClauseScoping(t *testing.T) {
	t.Parallel()
	src := `
func source() error    { return nil }
func other() error     { return nil }
func target(e error) error { return e }

func middle(e error) error { return target(e) }
`
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	listed := "fixture.go:middle@0/ret#0"
	unlisted := "fixture.go:middle@0/unknown-effect"
	clauses := map[string][]prClause{
		"fixture.go:middle CALL#0": {{
			Pairs: map[string]bool{prPairKey(listed, prArrLocal): true},
			Disp:  edgeDischarge, Why: "fixture",
		}},
	}
	res := prRunClosure(sc, prEngineInput{
		Seeds: []prSeed{
			prFixtureSeed("fixture.go:middle", 0, "ret#0"),
			prFixtureSeed("fixture.go:middle", 0, prCompUnknown),
		},
		Clauses: clauses,
	})
	if res.Has("fixture.go:target", listed, "arg#0") {
		t.Errorf("the clause names %s and it still propagated, so the discharge did not apply", listed)
	}
	if !res.Has("fixture.go:target", unlisted, "arg#0") {
		t.Errorf("%s is NOT named by the clause and did not reach target at arg#0. A clause "+
			"terminates only the pairs its applicability relation names", unlisted)
	}
}

// TestPointReadClosureFixtureDestinationIdentity kills M14 (substitute one destination for
// another in resolution, count unchanged). The expectation is an edge-to-destination identity,
// because reached-function membership can be masked by another route to the same destination.
// Under plan revision 6 no call is proved at all, so the identity assertion is over the WHOLE
// candidate set: dropping a real destination and substituting one both fail here.
func TestPointReadClosureFixtureDestinationIdentity(t *testing.T) {
	t.Parallel()
	src := `
type alpha struct{}
type beta struct{}

func (a *alpha) act() error { return nil }
func (b *beta) act() error  { return nil }

func caller() error {
	a := alpha{}
	return a.act()
}
`
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	var site censusCallSite
	for _, c := range sc.Calls {
		if c.Fn.Name == "caller" && c.Callee == "act" {
			site = c
		}
	}
	byName := map[string][]censusFuncKey{}
	for k := range sc.Funcs {
		byName[k.Name] = append(byName[k.Name], k)
	}
	dests := prResolveCall(byName, site)
	got := map[string]bool{}
	for _, d := range dests {
		got[d.Recv+"."+d.Name] = true
	}
	if len(got) != 2 || !got["alpha.act"] || !got["beta.act"] {
		t.Errorf("the call resolved to %v; the literal expectation is exactly {(alpha).act, "+
			"(beta).act}. Substituting one for the other, or dropping either, must fail here",
			dests)
	}
}

// TestPointReadClosureFixtureReverseValueRef kills M17 (drop the unresolved reverse-call
// obligation for a function whose name is taken as a value), including the package-initializer
// route a body-oriented walk cannot see.
func TestPointReadClosureFixtureReverseValueRef(t *testing.T) {
	t.Parallel()
	src := `
func handler() error { return nil }

func initializerHost() error { return nil }

var table = map[string]func() error{"x": initializerHost}

func takesValue() func() error {
	f := handler
	return f
}
`
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	res := prRunClosure(sc, prEngineInput{Seeds: []prSeed{
		prFixtureSeed("fixture.go:handler", 0, "ret#0"),
	}})
	for _, want := range []string{"fixture.go:handler", "fixture.go:initializerHost"} {
		if !res.UnresolvedReverse[want] {
			t.Errorf("%s has its name taken as a VALUE and carries no unresolved reverse-call "+
				"obligation. No name-based reverse lookup can find a caller that invoked it "+
				"through a variable, and a package initializer is invisible to a body walk", want)
		}
	}
}

// TestPointReadClosureFixtureSentinelRounds kills M11 (stop the sentinel fixpoint after one
// round). The source needs MULTIPLE rounds, and the expected identities are literal.
func TestPointReadClosureFixtureSentinelRounds(t *testing.T) {
	t.Parallel()
	src := `
const (
	seedAbsent  = "seed-absent"
	roundOne    = "round-one"
	roundTwo    = "round-two"
)

// DECLARED IN REVERSE DEPENDENCY ORDER, deliberately. A single pass over the arms in source
// order would add roundOne and then, further down the same pass, see an arm guarded on it —
// which closes a two-round chain by luck of ordering. With the deriving arm declared FIRST, one
// pass can only add roundOne, and roundTwo needs the second round that a fixpoint provides.
func second(v string) string {
	switch v {
	case roundOne:
		return roundTwo
	}
	return ""
}

func first(v string) string {
	switch v {
	case seedAbsent:
		return roundOne
	}
	return ""
}
`
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	set := prSentinelClosureFrom(sc, []string{"seedAbsent"})
	for _, want := range []string{"seedAbsent", "roundOne", "roundTwo"} {
		if !set[want] {
			got := make([]string, 0, len(set))
			for k := range set {
				got = append(got, k)
			}
			sort.Strings(got)
			t.Errorf("the sentinel closure lacks %q; it derived %v. A fixpoint that stops after "+
				"one round finds roundOne and never roundTwo", want, got)
		}
	}
}

// TestPointReadClosureFixtureSeedAdmission kills M18 (omit a seed whose effect component cannot
// be attributed). Admission is unconditional; attribution is a separate question.
func TestPointReadClosureFixtureSeedAdmission(t *testing.T) {
	t.Parallel()
	src := `
func unattributable() error { return nil }
`
	_, res := prFixtureRun(t, src, prFixtureSeed("fixture.go:unattributable", 0, prCompUnknown))
	origin := "fixture.go:unattributable@0/unknown-effect"
	if _, ok := res.OriginIndex[origin]; !ok {
		t.Errorf("%s was not admitted. A seed whose effect cannot be attributed is admitted "+
			"with unknown-effect and stays outstanding in the deficit; dropping it would let "+
			"attribution act as a discovery filter", origin)
	}
	if !res.Has("fixture.go:unattributable", origin, prArrLocal) {
		t.Errorf("%s was admitted but is not origin-local at its own function", origin)
	}
}

// TestPointReadClosureFixtureDeficitCounts kills M9 (count reviewed propagation as reviewed
// discharge). The category membership is approved independently here: the fixture states which
// edge carries which disposition, so a mutated categorizer cannot supply both sides.
func TestPointReadClosureFixtureDeficitCounts(t *testing.T) {
	t.Parallel()
	src := `
func a() error { return b() }
func b() error { return c() }
func c() error { return nil }
`
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	origin := "fixture.go:a@0/ret#0"
	res := prRunClosure(sc, prEngineInput{
		Seeds: []prSeed{prFixtureSeed("fixture.go:a", 0, "ret#0")},
		Clauses: map[string][]prClause{
			// One reviewed PROPAGATION, explicitly not a discharge.
			"fixture.go:a CALL#0": {{
				Pairs: map[string]bool{prPairKey(origin, prArrLocal): true},
				Disp:  edgeExpand, Why: "fixture: reviewed propagation",
			}},
		},
	})
	if res.Disp[prDispPropagation] != 1 {
		t.Errorf("reviewed propagations = %d, expected exactly 1: the fixture declares one "+
			"propagating clause and nothing else", res.Disp[prDispPropagation])
	}
	if res.Disp[prDispDischarge] != 0 {
		t.Errorf("reviewed discharges = %d, expected 0. Counting propagation as discharge would "+
			"report reviewed coverage this fixture never granted", res.Disp[prDispDischarge])
	}
	if res.DefaultPairs == 0 {
		t.Error("no default-only pair was counted although only one edge carries a clause")
	}
	if !res.Has("fixture.go:b", origin, "arg#0") && !res.Has("fixture.go:b", origin, prArrRet) {
		t.Error("a reviewed PROPAGATION must still propagate; it is not a discharge")
	}
}

// TestPointReadClosureFixtureLateOrigin kills M6a (process a state once and never again, so a
// LATER origin arriving at an already-processed state is recorded and not propagated). The
// second origin reaches `mid` through a longer chain, so by construction it arrives after
// `mid` has been processed for the first — and the assertion is on what leaves `mid`, not on
// whether `mid` was reached.
func TestPointReadClosureFixtureLateOrigin(t *testing.T) {
	t.Parallel()
	src := `
func mid(e error) error  { return tail(e) }
func tail(e error) error { return e }

func near(e error) error { return mid(e) }

func farA(e error) error { return farB(e) }
func farB(e error) error { return farC(e) }
func farC(e error) error { return mid(e) }
`
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	early := "fixture.go:near@0/ret#0"
	late := "fixture.go:farA@0/ret#0"
	res := prRunClosure(sc, prEngineInput{Seeds: []prSeed{
		prFixtureSeed("fixture.go:near", 0, "ret#0"),
		prFixtureSeed("fixture.go:farA", 0, "ret#0"),
	}})
	if !res.Has("fixture.go:tail", early, "arg#0") {
		t.Errorf("%s did not reach tail through mid", early)
	}
	if !res.Has("fixture.go:tail", late, "arg#0") {
		t.Errorf("%s did not reach tail. It arrives at mid AFTER mid was processed for the "+
			"first origin, so an engine that processes a state once records it as a "+
			"predecessor and never propagates it onward", late)
	}
}

// TestPointReadClosureFixtureBatchDoesNotEraseAnArrival pins holistic round 1's finding 2:
// admitting one origin must never REMOVE another origin's triple. The carrier transfer used to
// be computed over the whole incoming batch, so a lit#k/ret#i origin joining the batch replaced
// a zero-argument call's `unknown-arrival` with `callback-ret#0` — and the ordinary origin,
// which has no callback arrival of its own, simply lost its only arrival. That is not monotone,
// and it makes the result depend on how origins happen to be batched.
func TestPointReadClosureFixtureBatchDoesNotEraseAnArrival(t *testing.T) {
	t.Parallel()
	src := `
func sink() error { return nil }

func host() error {
	cb := func() error { return nil }
	_ = cb
	return sink()
}
`
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	ordinary := "fixture.go:host@1/ret#0"
	callback := "fixture.go:host@1/lit#0/ret#0"

	alone := prRunClosure(sc, prEngineInput{Seeds: []prSeed{prFixtureSeed("fixture.go:host", 1, "ret#0")}})
	if !alone.Has("fixture.go:sink", ordinary, prArrUnknown) {
		t.Fatalf("%s did not reach sink at %s even on its own, so this fixture tests nothing",
			ordinary, prArrUnknown)
	}
	together := prRunClosure(sc, prEngineInput{Seeds: []prSeed{
		prFixtureSeed("fixture.go:host", 1, "ret#0"),
		prFixtureSeed("fixture.go:host", 1, "lit#0/ret#0"),
	}})
	if !together.Has("fixture.go:sink", ordinary, prArrUnknown) {
		t.Errorf("%s reached sink at %s alone but NOT when batched with %s. Adding an origin "+
			"removed an existing triple: the carrier transfer is being computed over the batch "+
			"rather than per origin", ordinary, prArrUnknown, callback)
	}
	if !together.Has("fixture.go:sink", callback, "callback-ret#0") {
		t.Errorf("%s did not transfer as callback-ret#0", callback)
	}
}

// TestPointReadClosureFixtureReconstructionAgrees pins holistic round 2's finding 2: the
// predecessor reconstruction must report exactly what the engine produced. After the carrier
// fix a callback origin goes only to callback-ret#0, so reconstructing the unknown-arrival
// state must not report it.
func TestPointReadClosureFixtureReconstructionAgrees(t *testing.T) {
	t.Parallel()
	src := `
func sink() error { return nil }

func host() error {
	cb := func() error { return nil }
	_ = cb
	return sink()
}
`
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	in := prEngineInput{Seeds: []prSeed{
		prFixtureSeed("fixture.go:host", 1, "ret#0"),
		prFixtureSeed("fixture.go:host", 1, "lit#0/ret#0"),
	}}
	res := prRunClosure(sc, in)
	callbackIdx, ok := res.OriginIndex["fixture.go:host@1/lit#0/ret#0"]
	if !ok {
		t.Fatal("the callback origin was not admitted, so this fixture tests nothing")
	}
	target := prState{Fn: "fixture.go:sink", Arrival: prArrUnknown}
	if !res.Has(target.Fn, "fixture.go:host@1/ret#0", prArrUnknown) {
		t.Fatalf("the ordinary origin did not reach %s/%s", target.Fn, target.Arrival)
	}
	for _, p := range prReconstructPredecessors(sc, in, res, target) {
		if prBitGet(p.Origins, callbackIdx) {
			t.Errorf("reconstructing %s/%s reports the CALLBACK origin, which the engine sent "+
				"only to callback-ret#0. A reconstruction that reads the whole reached set "+
				"instead of the carrier class invents provenance", target.Fn, target.Arrival)
		}
	}
}

// A BARE identifier call carries no receiver expression, so it cannot DIRECTLY INVOKE a method
// declaration however the names line up. This is the one kind claim the resolver still makes,
// and it is what removed round 4's second finding at its source: the property that no
// `not-an-interpretation` site calls a producer used to paper over the collision by skipping
// every call whose name was bound locally, which also skipped `marker.observe()` — a real
// producer call — whenever an unrelated local shared the name. The INDIRECT case
// (`x := m.observe; x()`) is accounted by the method's unresolved reverse-call obligation,
// which is why the filter is not an under-report.
func TestPointReadClosureFixtureBareCallNeverReachesAMethod(t *testing.T) {
	t.Parallel()
	src := `
type marker struct{}

func (m marker) observe() error { return nil }

func observe() error { return nil }

func callsBare() error { return observe() }

func callsMethod(m marker) error { return m.observe() }

type solo struct{}

func (s solo) only() error { return nil }

func callsMissing() error { return only() }
`
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	byName := map[string][]censusFuncKey{}
	for k := range sc.Funcs {
		byName[k.Name] = append(byName[k.Name], k)
	}
	find := func(fn string) censusCallSite {
		t.Helper()
		for _, c := range sc.Calls {
			if c.Fn.String() == fn {
				return c
			}
		}
		t.Fatalf("fixture reached no call site in %s — the fixture proves nothing", fn)
		return censusCallSite{}
	}

	bare := prResolveCall(byName, find("fixture.go:callsBare"))
	if len(bare) != 1 || bare[0].Recv != "" || bare[0].Name != "observe" {
		t.Errorf("the bare call `observe()` resolved to %v. A method needs a receiver "+
			"expression, so `(marker).observe` is not a candidate for it and including it "+
			"attributes the call to a body it can never enter", bare)
	}

	method := prResolveCall(byName, find("fixture.go:callsMethod"))
	var reachesMethod bool
	for _, d := range method {
		if d.Recv == "marker" && d.Name == "observe" {
			reachesMethod = true
		}
	}
	if !reachesMethod {
		t.Errorf("the selector call `m.observe()` resolved to %v, which does not include "+
			"(marker).observe. Filtering methods out of BARE calls must not filter them out "+
			"of the calls that do carry a receiver", method)
	}

	// No plain declaration at all: the bare call resolves to NOTHING rather than silently
	// borrowing the method's body, and it records an unresolved obligation.
	missing := prResolveCall(byName, find("fixture.go:callsMissing"))
	if len(missing) != 0 {
		t.Errorf("the bare call `only()`, with only a METHOD of that name in the package, "+
			"resolved to %v. A bare identifier cannot DIRECTLY INVOKE a method declaration, so "+
			"it reaches no destination here — and the call remains an obligation either way, "+
			"which is what keeps the filter from being an under-report", missing)
	}
}

// The dossier names its interpreting sites in a table, and a site is LISTED only when a row's
// first cell is its receiver-qualified identity. A substring check over the whole document
// accepted `resolve` because `unresolved` contains it, so deleting a real row still passed.
func TestPointReadDossierIdentityNeedsATableRow(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		line string
		want string
		ok   bool
	}{
		{"receiver-qualified row", "| `(mqttInflightSourceCache).resolve` | Absent becomes complete |", "(mqttInflightSourceCache).resolve", true},
		{"plain row", "| `claimTeardown` | abandons the claim |", "claimTeardown", true},
		{"prose mentioning the name", "The 1144 unresolved reverse obligations are listed below.", "", false},
		{"prose in a row cell", "| count | 1144 unresolved obligations remain |", "", false},
		{"unbackticked cell", "| resolve | a row without code formatting |", "", false},
	} {
		got, ok := prDocRowIdentity(tc.line)
		if ok != tc.ok || got != tc.want {
			t.Errorf("%s: prDocRowIdentity(%q) = (%q, %v), want (%q, %v)",
				tc.name, tc.line, got, ok, tc.want, tc.ok)
		}
	}
	if id := prDocIdentity("inflight_register.go:(mqttInflightSourceCache).resolve"); id != "(mqttInflightSourceCache).resolve" {
		t.Errorf("prDocIdentity dropped the receiver: %q. Matching on the bare method name is "+
			"what let an unrelated occurrence of `resolve` stand in for the site's row", id)
	}
}

// Holistic round 5's finding, as a fixture: the engine and the reconstruction must agree on
// which CALLER edges EXIST, not merely that some predecessor was found. A wrapper method whose
// name matches the method it delegates to — the real instance is
// `(mqttAccountSessionManager).loadInflightRecord` calling `(mqttJSA).loadInflightRecord` —
// makes the callers-by-name lookup return the wrapper ITSELF, so a reconstruction that does not
// apply the engine's self-edge exclusion reports a wrapper-to-itself predecessor the engine
// never traversed. The assertion is over exact edge IDENTITIES, because a nonempty-predecessor
// check passes while the set contains an invented one.
func TestPointReadClosureFixtureReconstructionExcludesSelfCallerEdges(t *testing.T) {
	t.Parallel()
	src := `
type jsa struct{}

type manager struct{ js jsa }

func (j jsa) loadRecord() error { return nil }

func (m manager) loadRecord() error { return m.js.loadRecord() }

func caller(m manager) error { return m.loadRecord() }
`
	sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + src})
	in := prEngineInput{Seeds: []prSeed{prFixtureSeed("fixture.go:(jsa).loadRecord", 0, "ret#0")}}
	res := prRunClosure(sc, in)

	// The obligation leaves (jsa).loadRecord through its results, so both name-matched callers
	// are candidate destinations. That the WRAPPER is reached is what makes the self edge
	// available to be invented.
	target := prState{Fn: "fixture.go:(manager).loadRecord", Arrival: prArrRet}
	if len(res.Reached[target]) == 0 {
		t.Fatalf("%s/%s was never reached, so the fixture cannot exercise the self edge",
			target.Fn, target.Arrival)
	}

	for _, p := range prReconstructPredecessors(sc, in, res, target) {
		if p.Edge.From == target.Fn {
			t.Errorf("reconstructing %s/%s reports the predecessor edge %+v, whose SOURCE is the "+
				"target itself. prRunClosure skips a CALLER edge from a function to itself, so "+
				"this edge was never traversed and naming it is invented provenance",
				target.Fn, target.Arrival, p.Edge)
		}
	}

	// The engine's own side of the agreement: a self edge must not have been traversed either,
	// or the exclusion would have moved rather than held.
	self := prState{Fn: "fixture.go:(manager).loadRecord", Arrival: prArrRet}
	if prCallerEdgeEligible(self.Fn, self.Fn) {
		t.Error("prCallerEdgeEligible admits a function as its own CALLER. Both the engine and " +
			"the reconstruction consult it, so admitting one here reintroduces the finding in " +
			"both places at once")
	}
}

// TestPointReadClosureTraversalAndReconstructionAgree closes the family holistic rounds 5 and 6
// each found an instance of, rather than waiting for a third. Both instances were the same
// shape: the traversal and prReconstructPredecessors state one rule twice and the two copies
// disagree — CALLER-edge self exclusion in round 5, clause PRECEDENCE in round 6. A reader
// cannot see such a disagreement by reading either function, and a nonempty-predecessor check
// cannot detect it, so the enumeration of rules is the defect and this property replaces it.
//
// The claim is EQUALITY at fixpoint, not containment. The source's final reached set is
// exactly what the traversal eventually pushed through every edge leaving it, so reconstructing
// from that set must name the same edges with the same origins. A superset means reconstruction
// invents provenance; a subset means it loses one. Either is a finding, and neither is a reason
// to weaken this to a subset check.
//
// Edges are compared on (From, Key) with To cleared, because the traversal covers an edge once
// for ALL its destinations while the reconstruction names the destination it is reconstructing.
func TestPointReadClosureTraversalAndReconstructionAgree(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		src   string
		seeds []prSeed
		claus func(sc *censusScan) map[string][]prClause
	}{
		{
			name: "caller depth and callbacks",
			src: `
func leaf() error { return nil }

func middle() error { return leaf() }

func top() error {
	cb := func() error { return middle() }
	return cb()
}
`,
			seeds: []prSeed{prFixtureSeed("fixture.go:leaf", 0, "ret#0")},
		},
		{
			name: "a same-named wrapper, whose self CALLER edge the traversal excludes",
			src: `
type jsa struct{}

type manager struct{ js jsa }

func (j jsa) loadRecord() error { return nil }

func (m manager) loadRecord() error { return m.js.loadRecord() }

func caller(m manager) error { return m.loadRecord() }
`,
			seeds: []prSeed{prFixtureSeed("fixture.go:(jsa).loadRecord", 0, "ret#0")},
		},
		{
			name: "state edges and a second arrival",
			src: `
type holder struct{ field error }

func writer(h *holder) { h.field = nil }

func reader(h *holder) error { return h.field }

func direct() error { return nil }

func both(h *holder) error {
	if err := direct(); err != nil {
		return err
	}
	return reader(h)
}
`,
			seeds: []prSeed{prFixtureSeed("fixture.go:direct", 0, "ret#0")},
		},
	} {
		for _, order := range []string{"expand then discharge", "discharge then expand"} {
			t.Run(tc.name+", "+order, func(t *testing.T) {
				t.Parallel()
				sc := censusScanSources(t, map[string]string{"fixture.go": "package mqtt\n" + tc.src})

				// OVERLAPPING clauses on every call edge, in both orders. Round 6's defect only
				// appears when two clauses cover the same pair and disagree, and it is
				// order-sensitive by construction: `prClauseFor` takes the first match, so the
				// two orders must both be exercised or half the defect stays invisible.
				clauses := map[string][]prClause{}
				pairs := map[string]bool{}
				for _, s := range tc.seeds {
					for _, arr := range []string{
						prArrLocal, prArrRet, prArrUnknown, prArrRecv,
						prArrArg + "0", prArrCallback + "0", prArrElem,
					} {
						pairs[prPairKey(s.Origin.String(), arr)] = true
					}
				}
				expand := prClause{Pairs: pairs, Disp: edgeExpand, Why: "fixture: reviewed propagation"}
				discharge := prClause{Pairs: pairs, Disp: edgeDischarge, Why: "fixture: reviewed discharge"}
				for _, c := range sc.Calls {
					key := prClauseKey(prEdgeRef{From: c.Fn.String(), Key: fmt.Sprintf("CALL#%d", c.Occ)})
					if order == "expand then discharge" {
						clauses[key] = []prClause{expand, discharge}
					} else {
						clauses[key] = []prClause{discharge, expand}
					}
				}

				in := prEngineInput{Seeds: tc.seeds, Clauses: clauses, Trace: true}
				res := prRunClosure(sc, in)
				if len(res.Reached) == 0 {
					t.Fatal("nothing was reached, so this case compares two empty relations")
				}

				var compared int
				for st := range res.Reached {
					want := res.Admitted[st] // what the traversal actually offered
					got := map[prEdgeRef][]uint64{}
					for _, p := range prReconstructPredecessors(sc, in, res, st) {
						id := prEdgeRef{From: p.Edge.From, Key: p.Edge.Key}
						cur := got[id]
						for w := range p.Origins {
							for len(cur) <= w {
								cur = append(cur, 0)
							}
							cur[w] |= p.Origins[w]
						}
						got[id] = cur
					}
					for id, bits := range got {
						compared++
						if !prBitsEqual(bits, want[id]) {
							t.Errorf("%s/%s: reconstruction reports edge %s with origins %s, the "+
								"traversal offered %s. The two state one rule twice and have "+
								"disagreed about it twice; they must agree exactly",
								st.Fn, st.Arrival, prClauseKey(id),
								prOriginNames(res, bits), prOriginNames(res, want[id]))
						}
					}
					for id, bits := range want {
						if prBitCount(bits) == 0 {
							continue
						}
						if _, ok := got[id]; !ok {
							compared++
							t.Errorf("%s/%s: the traversal admitted %s through edge %s, and "+
								"reconstruction names no such predecessor — provenance the "+
								"engine produced and the reconstruction LOSES",
								st.Fn, st.Arrival, prOriginNames(res, bits), prClauseKey(id))
						}
					}
				}
				if compared == 0 {
					t.Error("no edge was compared in either direction, so this case proves " +
						"nothing about agreement")
				}
			})
		}
	}
}

func prBitsEqual(a, b []uint64) bool {
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	for w := 0; w < n; w++ {
		var x, y uint64
		if w < len(a) {
			x = a[w]
		}
		if w < len(b) {
			y = b[w]
		}
		if x != y {
			return false
		}
	}
	return true
}

func prOriginNames(res *prClosureResult, bits []uint64) string {
	var out []string
	for i := 0; i < len(bits)*64; i++ {
		if prBitGet(bits, i) {
			out = append(out, res.OriginAt[i])
		}
	}
	if len(out) == 0 {
		return "{}"
	}
	sort.Strings(out)
	return "{" + strings.Join(out, ", ") + "}"
}
