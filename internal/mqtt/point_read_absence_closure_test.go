// F-109's closure engine: the conservative candidate relation, computed rather than staged.
//
// The checkpoint at c7becd7 accounted 42 direct primitive-caller nodes by hand and staged the
// rest on a 181-entry pending-expansion frontier. Measured, that frontier's own closure reaches
// 1749 of this package's 1844 functions, so the hand-authored form has no stopping rule: one
// more tier is ~2000 more dispositions and spawns another frontier. Codex's consult 1 ruled the
// SHAPE must change and consult 2 then retracted the requirement that the change complete the
// census, because a computed closure whose unreviewed edges propagate by default satisfies
// closure-completeness VACUOUSLY while reaching 95% of the package. So the engine here computes
// the closure and MEASURES what is unreviewed; it never defines the deficit away.
//
// WHAT THE ENGINE IS ALLOWED TO CONCLUDE.
//
// It computes reachability under a deliberately conservative relation and reports, per reached
// obligation, whether its accounting is reviewed or defaulted. It concludes nothing about
// whether a discharge is TRUE — that is review, as the dossier says — and nothing about which
// callers are consequential: a conservatively reached function is not a consequential caller,
// and this file never calls one that.
//
// THE THREE APPROXIMATIONS THAT COULD SHRINK IT, and what stops each:
//
//   - a call through a function VALUE whose name collides with a declared function. The engine
//     emits the speculative destinations AND retains an unresolved obligation, because `hook()`
//     is an identifier call and `x.Run()` a selector call however the target was bound.
//   - a caller that invokes through a function value. Name-based reverse lookup cannot see it,
//     so every function whose name appears in the scanner's value-reference inventory carries an
//     unresolved reverse-call obligation.
//   - a mutation with no resolvable field. It carries an occurrence-keyed unresolved-state
//     obligation rather than being dropped, which is what consult C2 required.
//
// Each of those is a POSITIVE record in the output, so a regression removes an expected
// obligation rather than silently shrinking a count.

package mqtt

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// Origins: what an obligation IS.
// ---------------------------------------------------------------------------

// prOrigin identifies one obligation by the OCCURRENCE that creates it. A function is not the
// unit: checkResourceOverwrites performs three reads with different consequences and
// enumerateGenerationObligations two, while loadSessionMsg interprets nothing and still carries
// an obligation onward.
type prOrigin struct {
	Fn   string // censusFuncKey.String() of the function containing the occurrence
	Occ  int    // the call occurrence that creates the obligation
	Comp string // the component of this function's output or effect that carries it
}

func (o prOrigin) String() string { return o.Fn + "@" + strconv.Itoa(o.Occ) + "/" + o.Comp }

// The component grammar. It is finite and closed: a component outside it fails the vocabulary
// property, and `prCompUnknown` is the conservative fallback that keeps a seed admissible when
// its effect cannot be attributed. Seed admission NEVER depends on attribution succeeding.
const (
	prCompRet     = "ret#"    // + position; positions, because result names are optional
	prCompLit     = "lit#"    // + k + "/ret#" + i, a function literal's result
	prCompField   = "field:"  // + Base.name
	prCompElem    = "elem#"   // + occurrence, an index or dereference write with no field
	prCompPkgVar  = "pkgvar:" // + name
	prCompUnknown = "unknown-effect"
)

func prComponentLegal(c string) bool {
	switch {
	case c == prCompUnknown:
		return true
	case strings.HasPrefix(c, prCompRet), strings.HasPrefix(c, prCompLit),
		strings.HasPrefix(c, prCompField), strings.HasPrefix(c, prCompElem),
		strings.HasPrefix(c, prCompPkgVar):
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Arrivals: HOW an obligation reaches a function. The same origin arriving through two
// different carriers is two obligations to account for, because a discharge reviewed for one
// incoming argument establishes nothing about the same origin arriving through state.
// ---------------------------------------------------------------------------

const (
	prArrLocal    = "origin-local"    // created here; it did not arrive at all
	prArrArg      = "arg#"            // + position
	prArrRecv     = "recv"            // the call's receiver expression
	prArrCallback = "callback-ret#"   // + position, a function literal's result consumed by the callee
	prArrRet      = "ret-of-callee"   // this function called one that returned the obligation
	prArrState    = "state:"          // + field spelling
	prArrElem     = "state:elem"      // a write with no field identity
	prArrPkgVar   = "pkgvar:"         // + name
	prArrUnknown  = "unknown-arrival" // the conservative fallback for a transfer that cannot be established
)

func prArrivalLegal(a string) bool {
	switch {
	case a == prArrLocal, a == prArrRecv, a == prArrRet, a == prArrElem, a == prArrUnknown:
		return true
	case strings.HasPrefix(a, prArrArg), strings.HasPrefix(a, prArrCallback),
		strings.HasPrefix(a, prArrState), strings.HasPrefix(a, prArrPkgVar):
		return true
	}
	return false
}

// prEdgeRef names one candidate boundary edge, occurrence-keyed relative to its source
// function, so it is the same identity the hand-authored manifest uses.
type prEdgeRef struct {
	From string // source function
	Key  string // "CALL#n", "CALLER <fn>#n", "MUT#n"
	To   string // destination function, or "" for an unresolved destination
}

// ---------------------------------------------------------------------------
// Resolution. NO call is ever proved. Five review rounds attacked a syntactic dispatch proof
// and each found a Go binding construct the previous fix had not modelled; plan revision 6
// withdraws the claim entirely rather than model a sixth. Every call carries its whole
// candidate set AND an unresolved obligation: the engine may over-retain a destination, never
// drop a real one by manufacturing certainty.
// ---------------------------------------------------------------------------

// prResolveCall returns the package functions a call site NAMES. An empty result is an
// unresolved CALLEE — an external package, a value with no name match — and a non-empty one is
// a candidate set, not a resolution. Both carry an obligation; the caller distinguishes them by
// length, because a separate certainty field would be a second mechanism stating that fact.
//
// The one thing syntax establishes here is a destination's KIND: a bare identifier call carries
// no receiver expression, so it cannot DIRECTLY INVOKE a method declaration, and methods are
// not candidates for it. Stated exactly, because the weaker claim is the true one: the filter
// DOES omit an alias like `observe := m.observe; observe()`, which executes the method. That
// omission is not a silent loss — the call keeps its obligation here, and the method keeps the
// unresolved REVERSE-call obligation every value-referenced function owes — but neither
// obligation PROPAGATES an origin into the omitted body. It is recorded, unreviewed work, of a
// piece with an incomplete census, and not a claim that no destination is ever missed.
// Widening the filter would not resolve arbitrary aliases anyway, and would reattribute a
// caller's own closure to a same-named package method.
func prResolveCall(byName map[string][]censusFuncKey, site censusCallSite) []censusFuncKey {
	if site.Callee == "<expr>" {
		return nil
	}
	dests := byName[site.Callee]
	if site.On != "" {
		return dests
	}
	plain := dests[:0:0]
	for _, d := range dests {
		if d.Recv == "" {
			plain = append(plain, d)
		}
	}
	return plain
}

// ---------------------------------------------------------------------------
// The engine.
//
// WHY BITSETS. The reached space is (function, arrival) x origins, and the package has 1844
// functions against a few hundred origins, so materialising one entry per triple costs
// hundreds of megabytes and, measured, kills the process. A per-(function, arrival) bitset over
// a fixed origin index costs 5 words per state and encodes exactly the same relation. Nothing
// about the accounting is approximated by it: the predecessor relation below stores, per edge
// traversal, the ORIGIN SET that crossed it, so the full relation is retained and only its
// ENCODING is compact. A count of omitted predecessors would not detect a substitution, which
// is why the relation is kept rather than sampled.
// ---------------------------------------------------------------------------

// prDisposition is what the accounting says about one reached triple at one outgoing edge.
const (
	prDispDischarge   = "reviewed-discharge"
	prDispPropagation = "reviewed-propagation"
	prDispDefault     = "default-only"
)

type prSeed struct {
	Origin   prOrigin
	Category string
	Note     string
}

const (
	prSeedPrimitive = "primitive-read"
	prSeedPredicate = "predicate"
	prSeedSentinel  = "sentinel"
	prSeedLegacy    = "legacy"
)

// prClause is one independently scoped clause of an edge record. Applicability is a RELATION
// over (origin, arrival), never a Cartesian product of two sets: evidence covering
// (origin A, argument 0) and (origin B, state field x) establishes neither (A, x) nor (B, 0).
type prClause struct {
	// Pairs are the exact (origin, arrival) pairs this clause was reviewed against, as
	// "origin|arrival" strings. A clause covers nothing outside this list.
	Pairs map[string]bool
	Disp  string
	Exp   string
	Why   string
	To    string
}

func prPairKey(origin, arrival string) string { return origin + "|" + arrival }

// prClauseFor returns the clause that GOVERNS an (origin, arrival) pair on an edge: the FIRST
// match in clause order. Both the traversal and the predecessor reconstruction must ask this
// one question, or they answer differently — holistic round 6 found them doing exactly that.
// With an expansion clause followed by a discharge covering the same pair, the traversal
// propagated the origin (it stops at the first match) while the reconstruction erased its
// predecessor (it looked for ANY matching discharge). That is the second instance of one
// family: a rule stated twice between those two functions. `prCallerEdgeEligible` was the
// first. Clause ORDER is therefore load-bearing and deliberately so: overlapping clauses are
// resolved by precedence rather than rejected, because a reviewed record that narrows a
// broader one is a legitimate thing to write.
func prClauseFor(clauses []prClause, key string) (prClause, bool) {
	for _, cl := range clauses {
		if cl.Pairs[key] {
			return cl, true
		}
	}
	return prClause{}, false
}

// prClauseKey is the manifest's own edge identity: source function and occurrence-keyed edge.
func prClauseKey(e prEdgeRef) string { return e.From + " " + e.Key }

// prEngineInput is everything the closure needs that is not the scan: the seeds, and the
// reviewed clauses keyed by edge. Fixtures supply their own, which is what keeps their verdicts
// independent of the real manifest.
type prEngineInput struct {
	Seeds []prSeed
	// Trace records, per destination state, the origins each edge OFFERED it. It exists so a
	// property can compare the traversal against prReconstructPredecessors — the two state one
	// rule twice, and two holistic rounds found them disagreeing — and it is opt-in because
	// materialising provenance for the whole package is what OOM-killed this engine twice.
	// Fixtures are small; the package run leaves it false.
	Trace bool
	// Clauses are keyed by "<source function> <edge key>" — the hand-authored manifest's own
	// identity. A record dispositions the EDGE, not one of its possible destinations.
	Clauses map[string][]prClause
}

// prState is one (function, arrival) state and the origins that have reached it.
type prState struct {
	Fn      string
	Arrival string
}

// prPredEntry is one traversal: which origins crossed which edge into a state. This IS the
// predecessor relation, encoded per edge rather than per triple.
type prPredEntry struct {
	Edge    prEdgeRef
	Origins []uint64
}

type prClosureResult struct {
	// OriginIndex assigns each admitted origin its bit, and Origins is the inventory.
	OriginIndex map[string]int
	OriginAt    []string
	Origins     map[string]prSeed
	// Reached maps a state to the origin set that reached it.
	Reached map[prState][]uint64
	// The predecessor relation is RECONSTRUCTED rather than materialised — see
	// prReconstructPredecessors. Storing one entry per admission cost gigabytes on this
	// package and killed the process twice; the relation is a pure function of the scan, the
	// clauses and the final reached sets, so recomputing it loses nothing. What a stored
	// SAMPLE would have lost is the point: a count of omitted predecessors cannot detect a
	// substitution, and a reconstruction is complete by construction.
	// Disp counts dispositions over (edge, origin, arrival).
	Disp map[string]int
	// The semantic-review deficit, counted as it is produced. Listing WHICH origins defaulted
	// at one edge is a reconstruction (prDefaultOnlyAt), for the same reason as the
	// predecessor relation.
	DefaultPairs  int
	DefaultByEdge map[string]int // "<from> <edge key>" -> defaulted (origin, arrival) pairs
	DefaultByFn   map[string]int // source function -> the same, aggregated
	// Unresolved obligations, by kind, each occurrence-keyed. Positive records: a regression
	// removes an expected one instead of shrinking a total.
	UnresolvedCallee   map[string]bool
	UnresolvedDispatch map[string]bool
	UnresolvedReverse  map[string]bool
	// Admitted is populated only under prEngineInput.Trace: destination state -> edge -> the
	// origins that edge offered it, unioned over the run. Keyed by the edge's (From, Key)
	// identity with To cleared, because the traversal covers an edge once for ALL its
	// destinations while the reconstruction names the one it is reconstructing.
	Admitted        map[prState]map[prEdgeRef][]uint64
	UnresolvedState map[string]bool
}

// Has reports whether one origin reached one state, which is the triple-level query the
// bitset encoding has to keep answerable.
func (r *prClosureResult) Has(fn, origin, arrival string) bool {
	i, ok := r.OriginIndex[origin]
	if !ok {
		return false
	}
	return prBitGet(r.Reached[prState{Fn: fn, Arrival: arrival}], i)
}

// TripleCount counts reached triples, for the deficit report.
func (r *prClosureResult) TripleCount() int {
	n := 0
	for _, bits := range r.Reached {
		n += prBitCount(bits)
	}
	return n
}

// ReachedFunctions is the conservatively reached set. It is NOT a set of consequential callers,
// and nothing in this package may report it as one.
func (r *prClosureResult) ReachedFunctions() map[string]bool {
	out := map[string]bool{}
	for st := range r.Reached {
		out[st.Fn] = true
	}
	return out
}

// DefaultOnlyPairs is the semantic-review deficit: (edge, origin, arrival) triples with no
// applicable reviewed clause, and the distinct functions carrying them.
func (r *prClosureResult) DefaultOnlyPairs() (pairs int, fns int) {
	return r.DefaultPairs, len(r.DefaultByFn)
}

func prBitSet(bits []uint64, i int) []uint64 {
	w := i / 64
	for len(bits) <= w {
		bits = append(bits, 0)
	}
	bits[w] |= 1 << uint(i%64)
	return bits
}

func prBitGet(bits []uint64, i int) bool {
	w := i / 64
	if w >= len(bits) {
		return false
	}
	return bits[w]&(1<<uint(i%64)) != 0
}

func prBitCount(bits []uint64) int {
	n := 0
	for _, w := range bits {
		for ; w != 0; w &= w - 1 {
			n++
		}
	}
	return n
}

// prRunClosure computes the fixpoint. Propagation is monotone over a finite space — at most
// functions x arrivals x origins admissions — so it terminates. That is FINITENESS, not a claim
// that the origin partition is correct, which is a reviewed reading recorded in the dossier.
func prRunClosure(sc *censusScan, in prEngineInput) *prClosureResult {
	byName := map[string][]censusFuncKey{}
	for k := range sc.Funcs {
		byName[k.Name] = append(byName[k.Name], k)
	}
	for n := range byName {
		sort.Slice(byName[n], func(i, j int) bool { return byName[n][i].String() < byName[n][j].String() })
	}
	callsFrom := map[string][]censusCallSite{}
	for _, c := range sc.Calls {
		callsFrom[c.Fn.String()] = append(callsFrom[c.Fn.String()], c)
	}
	callersByName := map[string][]censusCallSite{}
	for _, c := range sc.Calls {
		callersByName[c.Callee] = append(callersByName[c.Callee], c)
	}
	mutsFrom := map[string][]censusMutation{}
	for _, m := range sc.Mutations {
		mutsFrom[m.Fn.String()] = append(mutsFrom[m.Fn.String()], m)
	}
	fieldReaders := map[string][]string{}
	seenFieldReader := map[string]bool{}
	for _, f := range sc.Fields {
		k := f.Field + "\x00" + f.Fn.String()
		if seenFieldReader[k] {
			continue
		}
		seenFieldReader[k] = true
		fieldReaders[f.Field] = append(fieldReaders[f.Field], f.Fn.String())
	}
	pkgVarReaders := map[string][]string{}
	seenVarReader := map[string]bool{}
	for _, r := range sc.Idents {
		if !sc.PkgVars[r.Name] {
			continue
		}
		k := r.Name + "\x00" + r.Fn.String()
		if seenVarReader[k] {
			continue
		}
		seenVarReader[k] = true
		pkgVarReaders[r.Name] = append(pkgVarReaders[r.Name], r.Fn.String())
	}

	res := &prClosureResult{
		OriginIndex:        map[string]int{},
		Origins:            map[string]prSeed{},
		Reached:            map[prState][]uint64{},
		Disp:               map[string]int{},
		DefaultByEdge:      map[string]int{},
		DefaultByFn:        map[string]int{},
		UnresolvedCallee:   map[string]bool{},
		UnresolvedDispatch: map[string]bool{},
		UnresolvedReverse:  map[string]bool{},
		Admitted:           map[prState]map[prEdgeRef][]uint64{},
		UnresolvedState:    map[string]bool{},
	}

	// Seed admission is UNCONDITIONAL. A seed whose effect cannot be attributed is admitted
	// with `unknown-effect` and stays outstanding in the deficit; it is never dropped for being
	// unattributable, which is what stops "reviewed interpreter" becoming a discovery filter.
	for _, s := range in.Seeds {
		id := s.Origin.String()
		if _, dup := res.OriginIndex[id]; dup {
			continue
		}
		res.OriginIndex[id] = len(res.OriginAt)
		res.OriginAt = append(res.OriginAt, id)
		res.Origins[id] = s
	}

	// A COALESCING worklist. A per-admission FIFO re-processes a state once per arriving batch,
	// which on this package means millions of passes over the same outgoing edges; accumulating
	// pending bits per state and draining them once collapses that to a few rounds. The result
	// is identical — propagation is monotone, so the order bits arrive in cannot change the
	// fixpoint — and only the work is different.
	pendingBits := map[prState][]uint64{}
	var queue []prState
	queued := map[prState]bool{}
	admitInner := func(st prState, add []uint64) {
		cur := res.Reached[st]
		var fresh []uint64
		for w := range add {
			var have uint64
			if w < len(cur) {
				have = cur[w]
			}
			n := add[w] &^ have
			if n == 0 {
				continue
			}
			for len(fresh) <= w {
				fresh = append(fresh, 0)
			}
			fresh[w] = n
		}
		if len(fresh) == 0 {
			return
		}
		for w := range fresh {
			for len(cur) <= w {
				cur = append(cur, 0)
			}
			cur[w] |= fresh[w]
		}
		res.Reached[st] = cur
		pend := pendingBits[st]
		for w := range fresh {
			for len(pend) <= w {
				pend = append(pend, 0)
			}
			pend[w] |= fresh[w]
		}
		pendingBits[st] = pend
		if !queued[st] {
			queued[st] = true
			queue = append(queue, st)
		}
	}

	// admitVia is the traced form: it records what the edge OFFERED, which is what the
	// reconstruction recomputes from the source's final reached set — not the freshly added
	// bits, which depend on arrival order.
	admitVia := func(edge prEdgeRef, st prState, add []uint64) {
		if in.Trace && prBitCount(add) > 0 {
			id := prEdgeRef{From: edge.From, Key: edge.Key}
			per := res.Admitted[st]
			if per == nil {
				per = map[prEdgeRef][]uint64{}
				res.Admitted[st] = per
			}
			cur := per[id]
			for w := range add {
				for len(cur) <= w {
					cur = append(cur, 0)
				}
				cur[w] |= add[w]
			}
			per[id] = cur
		}
		admitInner(st, add)
	}

	for _, s := range in.Seeds {
		i := res.OriginIndex[s.Origin.String()]
		admitInner(prState{Fn: s.Origin.Fn, Arrival: prArrLocal}, prBitSet(nil, i))
	}

	// The unresolved reverse-call inventory is a property of the package, not of a traversal: a
	// function whose name is taken as a value can be called from a body no name match finds.
	for _, v := range sc.ValueRefs {
		for _, d := range byName[v.Name] {
			res.UnresolvedReverse[d.String()] = true
		}
	}

	// prCovered splits an incoming origin set into the part a reviewed clause discharges and the
	// part that goes on. It counts every disposition, so reviewed PROPAGATION is reported apart
	// from reviewed DISCHARGE and neither is inferred from the other.
	cover := func(edge prEdgeRef, arrival string, bits []uint64) []uint64 {
		clauses := in.Clauses[prClauseKey(edge)]
		if len(clauses) == 0 {
			// The overwhelmingly common case: no reviewed record touches this edge, so every
			// origin crossing it is default-only. Counting them in bulk rather than one bit at
			// a time is what keeps this test at seconds rather than minutes; the accounting is
			// identical, because a pair with no clause can only default.
			if n := prBitCount(bits); n > 0 {
				res.Disp[prDispDefault] += n
				res.DefaultPairs += n
				res.DefaultByEdge[prClauseKey(edge)] += n
				res.DefaultByFn[edge.From] += n
			}
			return bits
		}
		out := append([]uint64(nil), bits...)
		var defaulted []uint64
		for i := 0; i < len(bits)*64; i++ {
			if !prBitGet(bits, i) {
				continue
			}
			origin := res.OriginAt[i]
			key := prPairKey(origin, arrival)
			cl, matched := prClauseFor(clauses, key)
			if matched {
				if cl.Disp == edgeDischarge {
					res.Disp[prDispDischarge]++
					out[i/64] &^= 1 << uint(i%64)
				} else {
					res.Disp[prDispPropagation]++
				}
			}
			if !matched {
				res.Disp[prDispDefault]++
				defaulted = prBitSet(defaulted, i)
			}
		}
		if n := prBitCount(defaulted); n > 0 {
			res.DefaultPairs += n
			res.DefaultByEdge[prClauseKey(edge)] += n
			res.DefaultByFn[edge.From] += n
		}
		return out
	}

	for len(queue) > 0 {
		st := queue[0]
		queue = queue[1:]
		queued[st] = false
		bits := pendingBits[st]
		delete(pendingBits, st)
		if len(bits) == 0 {
			continue
		}
		fnKey := st.Fn

		// CALL edges. Every call expression is a candidate: closures, defers and goroutines
		// included, external and unresolved destinations included.
		for _, c := range callsFrom[fnKey] {
			edge := prEdgeRef{From: fnKey, Key: fmt.Sprintf("CALL#%d", c.Occ)}
			dests := prResolveCall(byName, c)
			// EVERY call is an obligation now, because no call is proved. An empty candidate
			// set is an unresolved callee; a non-empty one is unresolved dispatch over the
			// candidates it names.
			site := fmt.Sprintf("%s CALL#%d %s", fnKey, c.Occ, c.Callee)
			if len(dests) == 0 {
				res.UnresolvedCallee[site] = true
			} else {
				res.UnresolvedDispatch[site] = true
			}
			// Applicability is tested against the arrival AT THE SOURCE — the carrier this
			// obligation came in on, which is what a reviewed record was reviewed against. The
			// destination arrivals below are the carrier TRANSFER, and they are what the
			// destination's own records will later be scoped by.
			pass := cover(edge, st.Arrival, bits)
			if prBitCount(pass) == 0 {
				continue
			}
			// Arrivals are computed per ORIGIN CLASS, never per batch. Holistic round 1 found
			// the defect: unioning the callback arrivals over a whole batch means adding a
			// lit#k/ret#i origin to it REMOVES an ordinary origin's unknown-arrival at a
			// zero-argument call — so admitting one origin could delete another's triple, which
			// is not monotone and would make the result depend on batching.
			for _, grp := range prSplitByCarrier(pass, res) {
				for _, arr := range prCallArrivalsForClass(c, grp.callback) {
					for _, d := range dests {
						admitVia(edge, prState{Fn: d.String(), Arrival: arr}, grp.bits)
					}
				}
			}
		}

		// CALLER edges: the obligation leaves through this function's results.
		for _, c := range callersByName[prFnName(fnKey)] {
			if !prCallerEdgeEligible(fnKey, c.Fn.String()) {
				continue
			}
			e := prEdgeRef{From: fnKey, Key: fmt.Sprintf("CALLER %s#%d", c.Fn.String(), c.Occ), To: c.Fn.String()}
			pass := cover(e, st.Arrival, bits)
			if prBitCount(pass) == 0 {
				continue
			}
			admitVia(e, prState{Fn: c.Fn.String(), Arrival: prArrRet}, pass)
		}

		// STATE edges: every non-local mutation, INCLUDING package variables and targets with
		// no resolvable field. A fieldless target has no destination set to enumerate, so it
		// carries an occurrence-keyed unresolved-state obligation instead of vanishing.
		for _, m := range mutsFrom[fnKey] {
			if m.Kind == "local" {
				continue
			}
			e := prEdgeRef{From: fnKey, Key: fmt.Sprintf("MUT#%d", m.Occ)}
			var arr string
			var dests []string
			switch {
			case m.Kind == "pkgvar":
				arr, dests = prArrPkgVar+m.Base, pkgVarReaders[m.Base]
			case m.Field != "":
				arr, dests = prArrState+m.Field, fieldReaders[m.Field]
			default:
				res.UnresolvedState[fmt.Sprintf("%s MUT#%d %s", fnKey, m.Occ, m.Kind)] = true
				arr, dests = prArrElem, nil
			}
			pass := cover(e, st.Arrival, bits)
			if prBitCount(pass) == 0 {
				continue
			}
			for _, d := range dests {
				de := e
				de.To = d
				admitVia(de, prState{Fn: d, Arrival: arr}, pass)
			}
		}
	}
	return res
}

// prOriginClass is one subset of an incoming batch that shares a carrier transfer: the
// callback-carrying origins, and the rest. Splitting is what keeps the transfer per origin
// while the propagation stays batched.
type prOriginClass struct {
	bits     []uint64
	callback string // the callback-ret arrival these origins transfer as, empty for the rest
}

func prSplitByCarrier(bits []uint64, res *prClosureResult) []prOriginClass {
	byCB := map[string][]uint64{}
	for i := 0; i < len(bits)*64; i++ {
		if !prBitGet(bits, i) {
			continue
		}
		cb := ""
		comp := res.Origins[res.OriginAt[i]].Origin.Comp
		if strings.HasPrefix(comp, prCompLit) {
			if j := strings.Index(comp, "/"+prCompRet); j >= 0 {
				cb = prArrCallback + comp[j+len("/"+prCompRet):]
			}
		}
		byCB[cb] = prBitSet(byCB[cb], i)
	}
	out := make([]prOriginClass, 0, len(byCB))
	for cb, b := range byCB {
		out = append(out, prOriginClass{bits: b, callback: cb})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].callback < out[j].callback })
	return out
}

// prCallArrivalsForClass is the carrier transfer for one call edge and ONE origin class. EVERY
// argument position is emitted, because without data flow the engine cannot establish which
// parameter receives the obligation: emitting all of them is the conservative direction, and
// keeping positions exact is what stops a clause reviewed for argument 0 from covering an
// arrival at argument 1. A function-literal result additionally transfers as callback-ret#i,
// which is the only route by which checkResourceOverwrites' sealable result reaches
// mqttCheckResourceStep.
func prCallArrivalsForClass(c censusCallSite, callback string) []string {
	var out []string
	if callback != "" {
		out = append(out, callback)
	}
	for i := range c.Args {
		out = append(out, prArrArg+strconv.Itoa(i))
	}
	if c.On != "" {
		out = append(out, prArrRecv)
	}
	if len(out) == 0 {
		out = append(out, prArrUnknown)
	}
	return out
}

// prFnName recovers the function NAME from a rendered key, which is what a caller edge matches
// on. The key is "file:(Recv).Name" or "file:Name".
func prFnName(key string) string {
	if i := strings.LastIndex(key, "."); i >= 0 && strings.Contains(key, ")") {
		return key[i+1:]
	}
	if i := strings.LastIndex(key, ":"); i >= 0 {
		return key[i+1:]
	}
	return key
}

// prCallerEdgeEligible decides whether a CALLER edge from a callee to its caller EXISTS. The
// engine and the reconstruction must ask the same question here: an edge the engine never
// traverses but the reconstruction reports is invented provenance, and holistic round 5 found
// exactly that — `(mqttAccountSessionManager).loadInflightRecord` calls the same-named
// `(mqttJSA)` method, so a name-keyed reconstruction produced a manager-to-ITSELF predecessor
// the engine had skipped. A self edge would carry an obligation into the function that already
// holds it.
func prCallerEdgeEligible(callee, caller string) bool {
	return callee != caller
}

// prReconstructPredecessors recomputes, for one reached state, every (edge, origin set) that
// delivered an obligation into it. The relation is a pure function of the scan, the clauses and
// the final reached sets: for each function that reached any state, each of its outgoing edges
// that lands here carries exactly the origins present at the source state and not discharged by
// an applicable clause.
//
// It is RECOMPUTED rather than stored because storing one entry per admission cost gigabytes on
// this package — measured, twice. Recomputation loses nothing a stored relation had: it is
// complete by construction, where a stored SAMPLE would have been a count, and a count of
// omitted predecessors cannot detect a substitution.
func prReconstructPredecessors(sc *censusScan, in prEngineInput, res *prClosureResult, target prState) []prPredEntry {
	byName := map[string][]censusFuncKey{}
	for k := range sc.Funcs {
		byName[k.Name] = append(byName[k.Name], k)
	}
	sources := map[string][]prState{}
	for st := range res.Reached {
		sources[st.Fn] = append(sources[st.Fn], st)
	}
	var out []prPredEntry

	// The arrival is the SOURCE state's, never the destination's — clause applicability is
	// what a reviewed record was reviewed against, which is the carrier the obligation came in
	// on. An earlier signature took the destination arrival as a parameter and then ignored it,
	// which reads as if the destination were consulted; it is gone so it cannot mislead.
	keep := func(edge prEdgeRef, st prState, bits []uint64) []uint64 {
		if prBitCount(bits) == 0 {
			return nil
		}
		clauses := in.Clauses[prClauseKey(edge)]
		if len(clauses) == 0 {
			return bits
		}
		out := append([]uint64(nil), bits...)
		for i := 0; i < len(bits)*64; i++ {
			if !prBitGet(bits, i) {
				continue
			}
			key := prPairKey(res.OriginAt[i], st.Arrival)
			if cl, ok := prClauseFor(clauses, key); ok && cl.Disp == edgeDischarge {
				out[i/64] &^= 1 << uint(i%64)
			}
		}
		return out
	}

	for _, c := range sc.Calls {
		from := c.Fn.String()
		states := sources[from]
		if len(states) == 0 {
			continue
		}
		hit := false
		for _, d := range prResolveCall(byName, c) {
			if d.String() == target.Fn {
				hit = true
				break
			}
		}
		if hit {
			edge := prEdgeRef{From: from, Key: fmt.Sprintf("CALL#%d", c.Occ), To: target.Fn}
			for _, st := range states {
				// The class's OWN bits, never the whole reached set: after the carrier fix the
				// engine sends a callback origin only to callback-ret#i, so reconstructing an
				// unknown-arrival from every origin present at the source would invent
				// provenance the engine never produced.
				for _, grp := range prSplitByCarrier(res.Reached[st], res) {
					for _, arr := range prCallArrivalsForClass(c, grp.callback) {
						if arr != target.Arrival {
							continue
						}
						if bits := keep(edge, st, grp.bits); prBitCount(bits) > 0 {
							out = append(out, prPredEntry{Edge: edge, Origins: bits})
						}
					}
				}
			}
		}
		// The CALLER direction: this call site makes c.Fn a destination for the callee's
		// results, so the callee is the source.
		if target.Arrival == prArrRet && c.Fn.String() == target.Fn {
			for _, d := range byName[c.Callee] {
				from := d.String()
				if !prCallerEdgeEligible(from, c.Fn.String()) {
					continue
				}
				edge := prEdgeRef{From: from, Key: fmt.Sprintf("CALLER %s#%d", c.Fn.String(), c.Occ), To: target.Fn}
				for _, st := range sources[from] {
					if bits := keep(edge, st, res.Reached[st]); prBitCount(bits) > 0 {
						out = append(out, prPredEntry{Edge: edge, Origins: bits})
					}
				}
			}
		}
	}

	for _, m := range sc.Mutations {
		if m.Kind == "local" {
			continue
		}
		from := m.Fn.String()
		if len(sources[from]) == 0 {
			continue
		}
		var arr string
		switch {
		case m.Kind == "pkgvar":
			arr = prArrPkgVar + m.Base
		case m.Field != "":
			arr = prArrState + m.Field
		default:
			continue
		}
		if arr != target.Arrival {
			continue
		}
		reaches := false
		if m.Field != "" {
			for _, f := range sc.Fields {
				if f.Field == m.Field && f.Fn.String() == target.Fn {
					reaches = true
					break
				}
			}
		} else {
			for _, r := range sc.Idents {
				if r.Name == m.Base && r.Fn.String() == target.Fn {
					reaches = true
					break
				}
			}
		}
		if !reaches {
			continue
		}
		edge := prEdgeRef{From: from, Key: fmt.Sprintf("MUT#%d", m.Occ), To: target.Fn}
		for _, st := range sources[from] {
			if bits := keep(edge, st, res.Reached[st]); prBitCount(bits) > 0 {
				out = append(out, prPredEntry{Edge: edge, Origins: bits})
			}
		}
	}
	return out
}

// prDocIdentity is how a function is SPELLED in the dossier: receiver-qualified, without
// the file, because a reader matches a table row against the code, not against a path.
func prDocIdentity(key string) string {
	if i := strings.Index(key, ":"); i >= 0 {
		return key[i+1:]
	}
	return key
}

// prDocRowIdentity returns the identity in a markdown table row's FIRST cell. A row is the
// unit the dossier lists a site in, so a name occurring anywhere else in the document —
// including inside a longer word — is not a listing.
func prDocRowIdentity(line string) (string, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "|") {
		return "", false
	}
	cell := strings.TrimSpace(strings.SplitN(strings.TrimPrefix(line, "|"), "|", 2)[0])
	if len(cell) < 3 || !strings.HasPrefix(cell, "`") || !strings.HasSuffix(cell, "`") {
		return "", false
	}
	return strings.Trim(cell, "`"), true
}
