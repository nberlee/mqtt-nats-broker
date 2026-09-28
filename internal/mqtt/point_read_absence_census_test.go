// F-109's point-read absence census: which decisions in this broker rest on a store read
// that could not be completed, and what each would need before it could be repaired.
//
// The finding this enforces is docs/ROADMAP.md's F-109. In the pinned server every error from
// LoadMsg, LoadNextMsg and LoadLastMsg becomes NewJSNoMessageFoundError() in the message-GET
// handler, so this package's deliberate ABSENT-versus-INDETERMINATE distinction is INVERTED
// for exactly the case it was written to protect against: a failed store read arrives as a 404
// and is classified absent. The bullet's own caller matrix says it is "a sample, not the
// population". This file is the population, and docs/findings/point-read-absence-census.md is
// its dossier.
//
// WHAT THIS FILE ENFORCES, AND WHAT IT DOES NOT.
//
// It enforces complete accounting of a syntactic candidate set. For every reached node the
// scanner enumerates candidate boundary edges — return-to-caller, calls into callees, and
// state-write-to-reader — and every one must carry an entry that EXPANDS the destination,
// LINKS to an already-accounted obligation while retaining provenance, or DISCHARGES that
// particular edge with a cited reason. No role, kind or recorded consequence suppresses
// enumeration; that is the whole point, and it is the fix for the defect three plan-review
// rounds kept re-cutting, where terminality attached to a FUNCTION and a function that both
// decided and propagated could silently truncate the walk.
//
// It does NOT prove a classification or a discharge TRUE. A row could say `withheld` about a
// site that is not, and this test would pass. The discipline that covers that gap is review,
// and the concrete reason it matters is in this census's own history: the plan misread
// unsub_drain.go:3287 as fail-closed when its 404 arm returns ok=true, certifying a PUBREL
// scan complete from a read that may have failed. Field matching conservatively approximates
// identity; alias resolution, omitted-update consequences, and the truth of discharges are
// reviewed. This is not a mechanically proved state-flow analysis.

package mqtt

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// The vocabularies. Each axis partitions one question; a value outside its enum, or a value
// on an axis that does not apply to the row, fails.
// ---------------------------------------------------------------------------

// Axis A, local mapping: what this site does with the 404 HERE, claiming nothing about
// consequence. `withheld` is a mapping value only and licenses no termination — a fail-closed
// site that wraps the original error with %w keeps the distinction available downstream, which
// is why loadOutboundExchange cannot be terminal despite handling every read error in one arm.
const (
	mapClassifiedAbsent = "classified-absent"
	mapWithheld         = "withheld"
	mapDiscarded        = "discarded"
	mapForwarded        = "forwarded" // hands the result on untested; has no mapping of its own
)

// Axis B, no-response. Applies to any node interpreting a result ORIGINATING from a batched
// primitive, wrappers and classifiers included. A nil entry is a request that produced no
// response at all, which loadMsgsMulti's own comment says "must be treated as indeterminate
// rather than as absence"; a row that folds it into the 404 arm is a finding.
const (
	batchDistinguished = "distinguished"
	batchCollapsed     = "collapsed"
	batchNA            = "not-applicable"
)

// Axis D, classification evidence, and axis E, consequence evidence. Separate fields because
// both apply to one row: the defective mapping can be demonstrated by reading while the wire
// consequence is only suspected.
const (
	classDefective = "defective-mapping-demonstrated"
	classSound     = "mapping-sound"

	consDemonstrated = "consequence-demonstrated"
	consSuspected    = "consequence-suspected"
	consNoneFound    = "no-consequence-identified"
	consNA           = "not-applicable"
)

// Axis F, reachability, classified per caller chain with an aggregate for shared nodes.
// `liveReadUnavailableDuty` is its own value because a gate can perform LIVE reads on a
// production path while the duty those reads feed is unavailable at HEAD:
// gatherAdoptedReleaseFacts reads the closure, the ledger and the completion certificate
// before its vacuous return, and finishAdoptedTeardown calls it on production paths. Calling
// that "withheld" would hide a live read.
const (
	reachLive       = "live-supported"
	reachLiveNoDuty = "live-read-unavailable-duty"
	reachTestOnly   = "test-only"
	reachExperiment = "experimental"
)

// Edge dispositions.
const (
	edgeExpand    = "expand"
	edgeLink      = "link"
	edgeDischarge = "discharge"
)

// ---------------------------------------------------------------------------
// The named explanations. Repeated reasoning is factored here with EXPLICIT edge membership;
// a wildcard covering future calls is refused, because it would defeat the drift detection
// this census exists to provide. Each explanation is reviewed once and then cited by the edges
// that belong to it, and an edge may add a per-edge note where the explanation is not the
// whole reason.
// ---------------------------------------------------------------------------

var prCensusExplanations = map[string]string{
	"PRE-READ": "the call is evaluated BEFORE the point read in this node and its result is an " +
		"input to the read rather than a consumer of it, so no outcome of the read — absent, " +
		"unreadable or present — can reach it. Subject derivation and hashing are the shape. " +
		"The edge carries no obligation because the obligation does not exist yet at this " +
		"occurrence. Note that an ARGUMENT of the read call gets a HIGHER occurrence number " +
		"than the read itself, because ast.Inspect visits a CallExpr before its arguments — so " +
		"occurrence order is not evaluation order, and a subject builder numbered after the " +
		"read is still evaluated before it",

	"READ-SOURCE": "this call IS the point read that gives this node its incoming obligation. It " +
		"is not an outgoing boundary edge; it is enumerated because the contract enumerates " +
		"every call expression, and it links to the primitive inventory rather than being " +
		"discharged",

	"PRESENT-ONLY": "the call sits on the PRESENT path, downstream of a branch the absent arm " +
		"returns from, so the absence-derived outcome cannot reach it. Decoding and validating " +
		"a record body are the shape: a 404 — whether genuine or a failed read normalized into " +
		"one — has already returned before the body exists to decode",

	"ERR-WRAP": "the call wraps the read's error and the wrapped value leaves this node through " +
		"its return-to-caller edge, so the obligation is not discharged here — it is carried, " +
		"and the accounting for it is the return-to-caller entry this edge links to. Consult C1's " +
		"own witness is why wrapping is never a discharge: loadOutboundExchange handles every " +
		"read error in one arm and still keeps the distinction available downstream through %w",

	"ERR-MSG-ONLY": "the call renders an error message on a branch the absence-derived outcome " +
		"does not take — a sequence mismatch, a decode failure, an identity mismatch — so the " +
		"value it produces is about a different fact and carries no part of the read obligation",

	"SEAM": "the call reads or invokes an instance-scoped TEST SEAM. In production the hook is " +
		"nil and the call returns without reaching the store, so on every supported path the " +
		"edge carries nothing; where a test installs a hook the value it returns is the read's " +
		"result and is accounted by the same entries as the real read, because the seam stands " +
		"exactly where the read does",

	"LOCK": "the call is a mutex operation. It carries no value, and its effect on WHEN state " +
		"becomes observable is accounted by the state edges of the fields the critical section " +
		"touches rather than duplicated here. Recorded rather than excluded, because C2 refused " +
		"to let \"external\" stand as a reason on its own",

	"LOCAL-RECORD-FIELD": "the mutation writes a field of a LOCALLY CONSTRUCTED value that this " +
		"node returns, not a field of long-lived state. The package-wide references with the " +
		"same field spelling are therefore references to other values' fields, and the " +
		"apparent persistent-state connection is spurious. What this write actually carries " +
		"leaves the node through its return-to-caller edge, which is accounted separately — so " +
		"this is a cited discharge of the destination set, never a claim that the references " +
		"were examined and found irrelevant one by one",

	"LOCAL-CONTAINER": "the mutation writes into a container held in a LOCAL variable, so it has " +
		"no field identity to match destinations against. Its contents leave this node through " +
		"the return-to-caller edge, which is accounted separately. This is the occurrence-keyed " +
		"unresolved-state obligation C2 requires, RESOLVED — the carrier is named and its " +
		"consumers are the return edge's — rather than left recorded as unresolved, which C2 " +
		"says does not complete the obligation",

	"ABSENT-ARM-REFUSAL": "the call is ON the absent arm and the arm REFUSES: the transition or " +
		"the operation ends here, so nothing absence-derived propagates past it. A false absence " +
		"still costs availability — a legal operation is refused — and that cost is recorded on " +
		"this node's axis C rather than discharged here",

	"ABSENT-ARM-INTO-RETURN": "the call is ON the absent arm and records the absence-derived " +
		"result into the value this node RETURNS. The obligation is not discharged here: it " +
		"leaves through this node's return-to-caller edges, which are enumerated above and are " +
		"themselves either expanded or staged on the pending frontier",

	"CONTINUES-PAST-ABSENT": "the call is reached from the absent arm, because that arm does not " +
		"return, but it carries no part of the absence-derived value — it marshals or derives " +
		"something the read did not produce. It is recorded rather than folded into PRESENT-ONLY, " +
		"whose claim is that the absent arm already returned and which would be FALSE here",

	"COUNTER-LOCAL": "the increment counts occurrences into a local accumulator that this node " +
		"returns. The count is absence-derived — an unreadable or missing record is what is " +
		"being counted — so the obligation is real, and it leaves through the return-to-caller " +
		"edge that this entry links to rather than through any state destination",
}

// ---------------------------------------------------------------------------
// The manifest.
// ---------------------------------------------------------------------------

// prCensusEdge is one candidate boundary edge and its accounting. Key is the scanner's
// occurrence-keyed identity, relative to the node: "CALL#n", "CALLER <fn>#n", or "MUT#n".
type prCensusEdge struct {
	Key  string
	Disp string
	Exp  string // a named explanation, or empty when Why carries the whole reason
	Why  string // per-edge reason, or the specifics the explanation does not cover
	To   string // destination node for expand and link
	// Dests is the size of the CONSERVATIVE destination set for a state mutation: every
	// package-wide reference sharing the field's spelling, ambiguous bases included. Consult
	// C2 required that set to be enumerated rather than filtered, and warned that receiver-only
	// filtering would trade the false positives for unacknowledged false negatives — so the
	// count is recorded and checked, and the discharge explains the CLASS rather than claiming
	// the references were each examined. A mutation with no resolvable field carries zero and
	// must cite the unresolved-carrier explanation.
	Dests int
}

// prCensusNode is one node of the closure: its interpretation of a point read, if it makes one,
// and the accounting for every candidate edge leaving it.
type prCensusNode struct {
	// The interpretation this node performs, where it performs one. A node reached purely by
	// propagation carries Mapping mapForwarded and leaves the evidence axes at their
	// not-applicable values.
	Mapping string
	Batch   string
	Auth    string // axis C: what the absent outcome authorizes. Required where Mapping is not forwarded.
	ClassEv string
	ConsEv  string
	Reach   string
	// Source names the node supplying a typed status this node consumes, as provenance. It is
	// never a discharge: a consumer that links its classification still owes its own edges.
	Source string
	// Repair is the candidate and its OUTSTANDING PREMISES, written out. Naming a fact class
	// does not establish that a repair exists, so a row says what it would need.
	Repair string
	Edges  []prCensusEdge
}

// prCensusNodes is keyed by censusFuncKey.String().
var prCensusNodes = map[string]prCensusNode{}

func prCensusRegister(key string, n prCensusNode) {
	if _, dup := prCensusNodes[key]; dup {
		panic("duplicate census node " + key)
	}
	prCensusNodes[key] = n
}

// ---------------------------------------------------------------------------
// Discovery: the candidate edges of a node, from the scan alone.
// ---------------------------------------------------------------------------

// prCensusPrimitives are the per-subject message-GET constructors. loadMsg, loadMsgsMulti and
// loadNextMsgFromSeq are the FIXED-SEQUENCE and EXHAUSTION primitives and belong to F-108;
// they are excluded with a per-site ruling in censusExclusions rather than dropped.
var prCensusPrimitives = []string{"loadLastMsgFor", "loadLastMsgForMulti"}

func prCensusL0Nodes(sc *censusScan) map[censusFuncKey]bool {
	out := map[censusFuncKey]bool{}
	for _, p := range prCensusPrimitives {
		for _, c := range sc.CallsTo(p) {
			out[c.Fn] = true
		}
	}
	return out
}

// prCensusCandidateEdges enumerates one node's candidate boundary edges. It applies no filter
// beyond the node itself: C2 ruled tier 1, so every call expression in the body is a
// candidate, external and unresolved destinations included, and mutations are enumerated with
// no absence guard at all.
func prCensusCandidateEdges(sc *censusScan, k censusFuncKey) []string {
	var out []string
	for _, c := range sc.Calls {
		if c.Callee == k.Name && c.Fn != k {
			out = append(out, fmt.Sprintf("CALLER %s#%d", c.Fn.String(), c.Occ))
		}
	}
	for _, c := range sc.Calls {
		if c.Fn == k {
			out = append(out, fmt.Sprintf("CALL#%d", c.Occ))
		}
	}
	for _, m := range sc.Mutations {
		// A write to a plain local is not a state-write-to-reader edge: the local dies with
		// the frame, and whatever it carries leaves the node through a return or a call, both
		// of which are candidate kinds of their own. C2's rule covers bare identifiers only
		// where they bind a PACKAGE variable. This reading is stated in the dossier so a
		// review can block on it.
		if m.Fn == k && m.Kind != "local" {
			out = append(out, fmt.Sprintf("MUT#%d", m.Occ))
		}
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------------
// The properties.
// ---------------------------------------------------------------------------

// TestPointReadCensusEdgeAccounting is the property that replaced function-level terminality:
// every candidate boundary edge of every declared node carries exactly one accounting entry,
// and every entry names a candidate that exists. Nothing about a node's role can suppress an
// edge.
func TestPointReadCensusEdgeAccounting(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	if len(prCensusNodes) == 0 {
		t.Fatal("the census manifest is empty, so every property here would pass vacuously")
	}
	byName := map[string]censusFuncKey{}
	for k := range sc.Funcs {
		byName[k.String()] = k
	}
	for name, node := range prCensusNodes {
		k, ok := byName[name]
		if !ok {
			t.Errorf("the manifest declares node %q, which no longer exists in this package", name)
			continue
		}
		want := prCensusCandidateEdges(sc, k)
		got := map[string]int{}
		for _, e := range node.Edges {
			got[e.Key]++
		}
		for _, w := range want {
			switch got[w] {
			case 1:
			case 0:
				t.Errorf("node %s: candidate edge %q has no accounting entry. Every candidate "+
					"expands, links, or discharges with a cited reason; a missing entry is the "+
					"defect this census exists to make impossible", name, w)
			default:
				t.Errorf("node %s: candidate edge %q has %d entries; the accounting must be a "+
					"bijection so that flattening it reproduces the candidate set exactly",
					name, w, got[w])
			}
			delete(got, w)
		}
		for extra := range got {
			t.Errorf("node %s: the manifest accounts for edge %q, which the scan does not find. "+
				"A stale entry hides a real edge behind a passing count", name, extra)
		}
	}
}

// prCensusPrimitiveDest is the destination a READ-SOURCE link names. The primitives are the
// transport layer, not census nodes, so a link to one is accounted by the transport inventory
// rather than by a node record.
var prCensusPrimitiveDest = map[string]bool{
	"PRIMITIVE loadLastMsgFor":      true,
	"PRIMITIVE loadLastMsgForMulti": true,
}

// The former PENDING-EXPANSION FRONTIER, retained as DATA and no longer as a mechanism.
//
// The checkpoint staged 181 owed expansions here because expansion was hand-authored and had to
// stop somewhere. The closure engine performs those expansions, so keeping a frontier beside it
// would be two mechanisms stating one fact. What the frontier carried that a reached-function
// set does not is the MEANING of each owed obligation — "the sealable callback's
// absence-derived success, which the helper consumes as the licence to seal" — so the entries
// are kept verbatim, and TestPointReadCensusFormerFrontierConverted checks each one against the
// engine by (source, edge occurrence, destination) and requires its description to survive.
type prFormerFrontierRecord struct {
	To         string // the destination that was owed a node record
	Obligation string // what the incoming obligation IS, in the words it was reviewed in
}

var prFormerFrontier = map[string]prFormerFrontierRecord{}

func prFormerFrontierEntry(node, edge, to, obligation string) {
	prFormerFrontier[node+" "+edge] = prFormerFrontierRecord{To: to, Obligation: obligation}
}

// prFormerFrontierCallers records the caller edges of one node that all received the SAME
// outgoing obligation. Membership stays explicit and listed; the checkpoint refused a wildcard
// because it would cover a caller added later, and that reasoning is unchanged by the engine.
func prFormerFrontierCallers(node, obligation string, callers ...string) {
	for _, c := range callers {
		prFormerFrontierEntry(node, "CALLER "+c, callerDest(c), obligation)
	}
}

// callerDest strips the occurrence suffix from a caller edge key to get the destination node.
func callerDest(caller string) string {
	if i := strings.LastIndex(caller, "#"); i >= 0 {
		return caller[:i]
	}
	return caller
}

// TestPointReadCensusDestinationsResolve enforces destination integrity. Consult C3 identified
// the hole it closes: requiring a nonempty `To` establishes nothing about where the obligation
// went, and redirecting an expansion to an unrelated declared node left every count and every
// key identical.
//
// The frontier arm is gone with the frontier. An EXPAND no longer has to name a declared node,
// because the closure engine expands it — but it must still name a function this package
// actually declares, so a destination cannot drift into a name that resolves to nothing. A LINK
// still resolves ONLY to an already-accounted obligation: C3's "a link cannot substitute for an
// unfinished expansion" is unaffected by who performs the expansion.
func TestPointReadCensusDestinationsResolve(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	declaredKeys := map[string]bool{}
	for k := range sc.Funcs {
		declaredKeys[k.String()] = true
	}
	for name, node := range prCensusNodes {
		for _, e := range node.Edges {
			if e.Disp != edgeExpand && e.Disp != edgeLink {
				continue
			}
			if e.To == "" {
				t.Errorf("node %s edge %q is %s with no destination", name, e.Key, e.Disp)
				continue
			}
			_, accounted := prCensusNodes[e.To]
			primitive := prCensusPrimitiveDest[e.To]
			if e.Disp == edgeLink {
				if !accounted && !primitive {
					t.Errorf("node %s edge %q LINKS to %q, which is neither a declared census "+
						"node nor a transport primitive. A link names an obligation that is "+
						"already accounted; it may not stand in for an expansion that has not "+
						"happened", name, e.Key, e.To)
				}
				continue
			}
			if !accounted && !primitive && !declaredKeys[e.To] {
				t.Errorf("node %s edge %q EXPANDS to %q, which this package does not declare. "+
					"The engine expands a destination; it cannot expand a name that resolves to "+
					"nothing, and a destination that has drifted hides the real one", name, e.Key, e.To)
			}
		}
	}
}

// TestPointReadCensusFormerFrontierConverted is the conversion check for the deleted mechanism.
// The frontier is not merely dropped: every entry it carried must still name a real edge of a
// declared node, must still be an EXPAND, must still agree with that edge's destination, and
// must still carry the obligation's MEANING in the words it was reviewed in. A destination-only
// conversion would lose that meaning while a reached-function set went on passing.
func TestPointReadCensusFormerFrontierConverted(t *testing.T) {
	t.Parallel()
	if len(prFormerFrontier) == 0 {
		t.Fatal("the former frontier is empty, so this conversion check would pass vacuously")
	}
	// KEY-SET EQUALITY, both directions. Iterating only the live map let a registration be
	// DELETED: its digest was simply never consulted, and the dossier's count dropped to 180
	// — which pasting the renderer's expected block would then "fix", erasing the reviewed
	// obligation with every check green. A reference nothing requires to be present does not
	// preserve anything.
	for key := range prFrontierRefs {
		if _, live := prFormerFrontier[key]; !live {
			t.Errorf("prFrontierRefs holds a reference for former frontier entry %q, which is "+
				"no longer registered. The reviewed obligation it pinned has been deleted, and "+
				"re-rendering the dossier's count would hide that rather than restore it", key)
		}
	}

	for key, rec := range prFormerFrontier {
		i := strings.LastIndex(key, " CALL")
		if i < 0 {
			i = strings.LastIndex(key, " CALLER ")
		}
		if i < 0 {
			i = strings.LastIndex(key, " MUT")
		}
		if i < 0 {
			t.Errorf("former frontier entry %q has no recognisable edge key", key)
			continue
		}
		node, edgeKey := key[:i], key[i+1:]
		n, ok := prCensusNodes[node]
		if !ok {
			t.Errorf("former frontier entry %q names node %q, which the manifest no longer "+
				"declares", key, node)
			continue
		}
		if strings.TrimSpace(rec.Obligation) == "" {
			t.Errorf("former frontier entry %q records no obligation. The engine supplies the "+
				"expansion; what it cannot supply is what the obligation MEANS, which is the "+
				"only reason these entries are retained", key)
		}
		// Against the INDEPENDENT reference, not merely non-empty: the meaning is the whole
		// reason these entries exist, so replacing one description with different nonempty
		// text must fail rather than pass every count and fingerprint unchanged.
		if ref, known := prFrontierRefs[key]; !known {
			t.Errorf("former frontier entry %q has no persisted reference. An entry whose "+
				"reviewed content nothing pins can be rewritten silently", key)
		} else if got := prFrontierDigest(rec.To, rec.Obligation); got != ref {
			t.Errorf("former frontier entry %q has destination %q and obligation %q, digesting "+
				"to %s against the persisted reference %s. The retained provenance must match "+
				"what was reviewed, or it contradicts the record it exists to preserve",
				key, rec.To, rec.Obligation, got, ref)
		}
		found := false
		for _, e := range n.Edges {
			if e.Key != edgeKey {
				continue
			}
			found = true
			if e.Disp != edgeExpand {
				t.Errorf("former frontier entry %q names edge %q, which is %s rather than an "+
					"expand. The frontier staged expansions; an entry against a discharge or a "+
					"link is stale", key, edgeKey, e.Disp)
			}
			if e.To != rec.To {
				t.Errorf("former frontier entry %q records destination %q but the edge expands "+
					"to %q. A mismatch hides the real destination without changing any count",
					key, rec.To, e.To)
			}
		}
		if !found {
			t.Errorf("former frontier entry %q names edge %q, which node %q does not carry",
				key, edgeKey, node)
		}
	}
}

// TestPointReadCensusDischargesAreCited enforces the discharge contract. A discharge explains
// why THAT EDGE carries no further relevant consequence; three reasons were named insufficient
// by consult C1 and are rejected outright here, because each of them is a way of saying "I
// stopped looking" that reads like a reason.
func TestPointReadCensusDischargesAreCited(t *testing.T) {
	t.Parallel()
	refused := []string{
		"this function already decides",
		"one caller level was reviewed",
		"no sentinel spelling occurs",
		"external",
	}
	for name, node := range prCensusNodes {
		for _, e := range node.Edges {
			switch e.Disp {
			case edgeExpand, edgeLink:
				if e.To == "" {
					t.Errorf("node %s edge %q is %s but names no destination, so its provenance "+
						"is lost", name, e.Key, e.Disp)
				}
			case edgeDischarge:
				reason := e.Why
				if e.Exp != "" {
					x, ok := prCensusExplanations[e.Exp]
					if !ok {
						t.Errorf("node %s edge %q cites explanation %q, which is not defined",
							name, e.Key, e.Exp)
						continue
					}
					reason = x + " " + e.Why
				}
				if len(strings.TrimSpace(reason)) < 40 {
					t.Errorf("node %s edge %q is discharged with no cited reason. Recording a "+
						"disposition is not accounting for it", name, e.Key)
				}
				if strings.TrimSpace(strings.ToLower(reason)) == "external" {
					t.Errorf("node %s edge %q is discharged as %q; consult C2 ruled that "+
						"\"external\" alone is not a reason, because an external callee can "+
						"still transport a value or cause a relevant effect", name, e.Key, reason)
				}
				for _, bad := range refused {
					if strings.EqualFold(strings.TrimSpace(reason), bad) {
						t.Errorf("node %s edge %q is discharged with %q, which consult C1 named "+
							"insufficient", name, e.Key, bad)
					}
				}
			default:
				t.Errorf("node %s edge %q has disposition %q, which is not one of expand, link "+
					"or discharge", name, e.Key, e.Disp)
			}
		}
	}
}

// TestPointReadCensusStateDestinationsAreAccounted closes the second gap consult C3 found when
// it inspected this file: candidate enumeration emitted mutation keys without checking their
// conservative destination sets, and "scanner availability alone does not establish
// destination-set enforcement".
//
// For a field mutation the destination set is every package-wide reference with the same field
// spelling — noisy by construction, and deliberately so. The manifest records its size, this
// test recomputes it, and a mismatch fails: a field that grows a reader, or a write that moves
// to another field, stops the discharge from standing silently.
func TestPointReadCensusStateDestinationsAreAccounted(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	byNode := map[string]map[int]censusMutation{}
	for _, m := range sc.Mutations {
		if m.Kind == "local" {
			continue
		}
		k := m.Fn.String()
		if byNode[k] == nil {
			byNode[k] = map[int]censusMutation{}
		}
		byNode[k][m.Occ] = m
	}
	for name, node := range prCensusNodes {
		for _, e := range node.Edges {
			if !strings.HasPrefix(e.Key, "MUT#") {
				continue
			}
			occ, err := strconv.Atoi(strings.TrimPrefix(e.Key, "MUT#"))
			if err != nil {
				t.Errorf("node %s: mutation key %q is malformed", name, e.Key)
				continue
			}
			m, ok := byNode[name][occ]
			if !ok {
				t.Errorf("node %s: mutation %q is not in the scan", name, e.Key)
				continue
			}
			// The explanation must match the carrier the scan actually found: a write with a
			// resolvable field has a destination CLASS, and one without has an unresolved
			// carrier. Citing the wrong one would describe a destination set that does not
			// exist, which is the shape of an unsupported discharge.
			if m.Field == "" && e.Exp != "LOCAL-CONTAINER" {
				t.Errorf("node %s edge %q writes through a target with no resolvable field, so it "+
					"is an unresolved-carrier obligation, but it cites %q", name, e.Key, e.Exp)
			}
			if m.Field != "" && e.Exp == "LOCAL-CONTAINER" {
				t.Errorf("node %s edge %q writes the field %q, which HAS a destination class, but "+
					"cites the unresolved-carrier explanation", name, e.Key, m.Field)
			}
			if m.Field == "" {
				if e.Dests != 0 {
					t.Errorf("node %s edge %q writes through a target with NO resolvable field "+
						"identity, so it has no field-matched destination set; it records %d",
						name, e.Key, e.Dests)
				}
				continue
			}
			want := len(sc.FieldReferences(m.Field))
			if e.Dests != want {
				t.Errorf("node %s edge %q writes %s and the conservative destination set for that "+
					"field spelling is %d references package-wide; the manifest records %d. The "+
					"discharge covers a CLASS of destinations, so the class's size is part of what "+
					"it discharges", name, e.Key, m.Field, want, e.Dests)
			}
		}
	}
}

// TestPointReadCensusAxesAreWellFormed checks that every axis value is legal and applicable.
// It cannot check that a value is TRUE; that limit is stated at the top of this file and in
// the dossier, rather than implied by silence.
func TestPointReadCensusAxesAreWellFormed(t *testing.T) {
	t.Parallel()
	legal := func(v string, set ...string) bool {
		for _, s := range set {
			if v == s {
				return true
			}
		}
		return false
	}
	for name, n := range prCensusNodes {
		if !legal(n.Mapping, mapClassifiedAbsent, mapWithheld, mapDiscarded, mapForwarded) {
			t.Errorf("node %s: local mapping %q is not a legal value", name, n.Mapping)
		}
		if !legal(n.Batch, batchDistinguished, batchCollapsed, batchNA) {
			t.Errorf("node %s: no-response axis %q is not a legal value", name, n.Batch)
		}
		if !legal(n.Reach, reachLive, reachLiveNoDuty, reachTestOnly, reachExperiment) {
			t.Errorf("node %s: reachability %q is not a legal value", name, n.Reach)
		}
		if !legal(n.ClassEv, classDefective, classSound, consNA) {
			t.Errorf("node %s: classification evidence %q is not a legal value", name, n.ClassEv)
		}
		if !legal(n.ConsEv, consDemonstrated, consSuspected, consNoneFound, consNA) {
			t.Errorf("node %s: consequence evidence %q is not a legal value", name, n.ConsEv)
		}
		if n.Mapping == mapForwarded {
			if n.Auth != "" {
				t.Errorf("node %s forwards its result untested and therefore performs no local "+
					"mapping, but records what an absent outcome authorizes; that claim belongs "+
					"to the node that interprets it", name)
			}
			continue
		}
		if strings.TrimSpace(n.Auth) == "" {
			t.Errorf("node %s interprets a point read but does not say what the absent outcome "+
				"authorizes, which is the column a repair has to satisfy", name)
		}
		if n.ConsEv == consSuspected && !strings.Contains(strings.ToLower(n.Repair)+strings.ToLower(n.Auth), "suspect") &&
			!strings.Contains(strings.ToLower(n.Auth), "comment") {
			// A suspected consequence must name where the suspicion comes from; a comment is
			// not a witness, and the row has to say which it is relying on.
			t.Errorf("node %s records a SUSPECTED consequence without naming its source", name)
		}
	}
}

// TestPointReadCensusCoversEveryInterpretationSite is the bijection in the other direction:
// every function the scan finds calling a per-subject primitive is a declared node. A new
// call site in a function the manifest does not name fails here by name.
func TestPointReadCensusCoversEveryInterpretationSite(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	l0 := prCensusL0Nodes(sc)
	if len(l0) == 0 {
		t.Fatal("the scan found no per-subject point reads at all, so this census would pass " +
			"vacuously: the primitive set has drifted from the code")
	}
	for k := range l0 {
		if _, ok := prCensusNodes[k.String()]; !ok {
			t.Errorf("%s calls a per-subject message-GET primitive and is not a declared census "+
				"node. It interprets a point read, or it forwards one, and either way the "+
				"population is incomplete until it says which", k.String())
		}
	}
}

// ---------------------------------------------------------------------------
// The exclusions: the fixed-sequence and exhaustion primitives, which are F-108's.
// ---------------------------------------------------------------------------

// The boundary, from F-109's bullet: fixed-sequence absence is F-108's, per-subject absence is
// F-109's, and it is coherent only within a proven stream incarnation because a fixed sequence
// cannot acquire a replacement there while a subject can.
//
// Plan review round 1 accepted that boundary for the CURRENT population and refused it as an
// unconditional semantic rule about a method name — an exact-subject search starting at
// sequence 1 could decide subject occupancy, and would then be F-109's however it is spelled.
// So every excluded site carries its own ruling here rather than inheriting one from its
// callee, and a new site is ruled on its question.
var prCensusExclusions = map[string]string{
	// loadMsg and loadMsgsMulti address an EXACT SEQUENCE. A sequence cannot acquire a
	// replacement within a proven stream incarnation, so "the record at sequence N is gone"
	// is a different question from "this subject is empty", and it is F-108's.
	"inflight_register.go:(mqttAccountSessionManager).inflightSourceState#6": "fixed-sequence: probes whether the source delivery at an exact sequence still stands",
	"mqtt.go:(mqttSession).resendPendingRedelivery#17":                       "fixed-sequence: checks the source message at the sequence a pending redelivery names",
	"pubrel_action.go:(mqttSession).admitStoredPubRel#13":                    "fixed-sequence: reads the PUBREL record at the sequence the slot carries",
	"pubrel_action.go:(mqttSession).phaseSlotToPubRelReady#2":                "fixed-sequence: reads the PUBREL record at an exact sequence before phasing the slot",
	"qos2_shared_affine.go:(mqttAccountSessionManager).q2AffSourceGone#4":    "fixed-sequence, and EXPERIMENTAL: asks whether the affine claim's source sequence is gone",
	"unsub_drain.go:(mqttSession).recoverDrains#17":                          "fixed-sequence: probes a recorded drain's source sequence",
	"unsub_drain.go:(mqttSession).recreateDrainPubRel#5":                     "fixed-sequence: reads the PUBREL record at the sequence the drain recorded",
	"inflight_register.go:(mqttInflightSourceCache).resolve#24":              "fixed-sequence, batched: resolves many source deliveries by sequence in one pipelined batch",
	"outbound_disposal_recovery.go:(mqttSession).prepareDisposalRecovery#13": "fixed-sequence: reads the source delivery at the exact sequence a displaced disposal names, for the successor's fresh allow/deny subject; an error or absence prepares no handoff and the old disposal keeps its restriction",

	// loadNextMsgFromSeq is a FORWARD SEARCH over a filter. Its negative conclusion is
	// "nothing at or after seq matches", which is an exhaustion claim about an enumeration
	// rather than an occupancy claim about a subject — F-108's question, and F-108 is
	// implementation-blocked because firstMatching silently skips a record it cannot read.
	"mqtt.go:(mqttAccountSessionManager).sweepOrphanedConsumersBefore#22":              "exhaustion: a bounded forward walk of the session stream",
	"mqtt.go:(mqttAccountSessionManager).sweepSessionExpiries#3":                       "exhaustion: a bounded forward walk of the session stream",
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).adoptAbandonedTeardowns#4":       "exhaustion: a forward walk of the session stream",
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).buildSessionRefs#14":             "exhaustion: a forward walk of the session stream",
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).collectInboundQoS2Candidates#9":  "exhaustion: a forward walk of the inbound QoS 2 stream; the file states the candidate list is DISCOVERY and every decision is taken from a direct read afterwards, which is why a short walk here is not itself a licence",
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).collectInflightCandidates#6":     "exhaustion: a forward walk of the in-flight stream, discovery only",
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).condemnRetiredSurvivors#4":       "exhaustion: a forward walk of the teardown stream",
	"qos2_shared_affine.go:(mqttAccountSessionManager).scanQ2AffClaimRecordsCharged#3": "exhaustion, and EXPERIMENTAL: a charged forward scan of the assignment stream",
	"qos2_shared_affine.go:(mqttAccountSessionManager).walkQ2AffClaimSegment#2":        "exhaustion, and EXPERIMENTAL: a segment walk of the assignment stream",
	"sess_accept_fact.go:(mqttAccountSessionManager).loadNextAcceptFact#3":             "exhaustion: the step of the acceptance-fact scan, and a FORWARDER — it returns the primitive's error untested, so its single caller does the classifying",
	"will_outbox.go:(mqttAccountSessionManager).scanWillOutbox#3":                      "exhaustion: a forward walk of the Will outbox stream",
	"shared_lifecycle_sweep.go:(mqttAccountSessionManager).sweepSharedGroupRetirements#2": "exhaustion: a forward walk of the shared group records ($MQTT.shared.grp.*) finishing every retiring entry a crashed leaver left; the retirement it performs reads the group record positively",
	"shared_lifecycle_sweep.go:(mqttAccountSessionManager).sweepAcqReleaseVerdicts#2":     "exhaustion: a forward walk of the session-record stream executing committed release verdicts; the verdict, not the walk, is the release authority",

	// The one excluded site that carries a DEFEATED GUARD, recorded rather than filed as
	// ordinary. Plan review round 1 caught this plan itself misreading it: a non-404 returns
	// ok=false, so an unreadable scan is reported as incomplete — but a 404 returns ok=TRUE,
	// which certifies the PUBREL scan COMPLETE. A store-read failure normalized into a 404
	// therefore certifies completion from a read that failed. It is F-108's by the boundary,
	// and it is the clearest example of why the boundary is a ruling per site rather than a
	// rule about a method name.
	"unsub_drain.go:(mqttSession).scanPubRelRecordsCharged#5": "exhaustion, WITH A DEFEATED GUARD: the walk distinguishes an unreadable scan (ok=false) from an empty tail (ok=true) and a 404 takes the ok=true arm, so a failed read certifies the scan complete",
}

// TestPointReadCensusExclusionsAreAccounted requires every call site of the excluded
// primitives to carry its own ruling. Consult C2 was explicit that an inventory entry may not
// simply be absent: "Account for every inventory entry, including justified exclusions."
func TestPointReadCensusExclusionsAreAccounted(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	want := map[string]bool{}
	for _, p := range []string{"loadMsg", "loadMsgsMulti", "loadNextMsgFromSeq"} {
		for _, c := range sc.CallsTo(p) {
			// The primitives' own definitions live in mqtt.go's JSA block and are the
			// transport layer rather than call sites of it.
			if c.Fn.File == "mqtt.go" && c.Fn.Recv == "mqttJSA" {
				continue
			}
			key := fmt.Sprintf("%s#%d", c.Fn.String(), c.Occ)
			want[key] = true
			if strings.TrimSpace(prCensusExclusions[key]) == "" {
				t.Errorf("%s calls the fixed-sequence or exhaustion primitive %q and carries no "+
					"exclusion ruling. The F-108 boundary is applied per SITE, not per method "+
					"name, so a site without a ruling is unaccounted rather than out of scope",
					key, p)
			}
		}
	}
	for key := range prCensusExclusions {
		if !want[key] {
			t.Errorf("the exclusions manifest rules on %q, which no longer calls an excluded "+
				"primitive; a stale ruling hides a site that has moved", key)
		}
	}
}

// ---------------------------------------------------------------------------
// The transport invariant, and the honest split between what is automated and what is read.
// ---------------------------------------------------------------------------

// prCensusTransportSurface is the accounted reference inventory required by plan revision 4
// §3.1 property 1b. Every non-test reference to a message-GET identifier is listed with what it
// is; an UNACCOUNTED reference fails, rather than silently passing because it happens not to be
// an mqttJSAMsgLoad construction.
//
// Plan review round 2's correction is why this exists at all: an occurrence COUNT over one
// request kind cannot enforce the invariant, because a point read added through another request
// kind or decoder leaves the five constructions and the dispatch arm untouched. That the listed
// identifiers are the whole of the message-read routing surface is a VERIFIED HEAD INVARIANT,
// checked by reading — this test enforces that nothing new appears against the identifiers we
// know about, and does not and cannot prove there is no other route.
var prCensusTransportSurface = map[string]string{
	"loadLastMsgFor":      "per-subject read, IN SCOPE",
	"loadLastMsgForMulti": "batched per-subject read, IN SCOPE",
	"loadMsg":             "fixed-sequence read, F-108's",
	"loadMsgsMulti":       "batched fixed-sequence read, F-108's",
	"loadNextMsgFromSeq":  "forward search over a filter, F-108's",
	"processJSAPIReplies": "the single reply-dispatch arm that decodes a *JSApiMsgGetResponse",
}

// TestPointReadCensusTransportInvariant is property 1a: the five constructors and the one
// decode arm, by the functions that name the request kind. A sixth function naming it — a new
// constructor, or a second decoder — fails here by name.
func TestPointReadCensusTransportInvariant(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	got := map[string]bool{}
	for fn := range sc.FunctionsReferencing("mqttJSAMsgLoad") {
		got[fn.Name] = true
	}
	if len(got) == 0 {
		t.Fatal("no function references mqttJSAMsgLoad, so the transport invariant would pass " +
			"vacuously: the request kind has been renamed or the scan is broken")
	}
	for name := range got {
		if _, ok := prCensusTransportSurface[name]; !ok {
			t.Errorf("%s constructs or decodes an mqttJSAMsgLoad request and is not in the "+
				"accounted transport surface. A new point-read route is in scope for this census "+
				"whatever it is spelled; it is not out of scope for bypassing the invariant",
				name)
		}
	}
	for name, what := range prCensusTransportSurface {
		if !got[name] {
			t.Errorf("the transport surface accounts for %s (%s), which no longer references "+
				"mqttJSAMsgLoad; a stale entry hides the route that replaced it", name, what)
		}
	}
	// By OCCURRENCE, not by function: a second construction added INSIDE an existing mqttJSA
	// method leaves the function set unchanged and would otherwise pass. Plan review round 2
	// named exactly that case.
	const wantConstructions = 5
	constructions := 0
	for _, c := range sc.Calls {
		if c.Callee != "newRequest" && c.Callee != "newRequestExMulti" {
			continue
		}
		if len(c.Args) > 0 && c.Args[0] == "mqttJSAMsgLoad" {
			constructions++
		}
	}
	if constructions != wantConstructions {
		t.Errorf("the package constructs %d mqttJSAMsgLoad requests; the accounted transport "+
			"surface has %d. A new point read is in scope for this census wherever it sits, "+
			"including inside a method that already makes one", constructions, wantConstructions)
	}
}

// TestPointReadCensusNoAlternateDecoder is property 1b: every non-test reference to a
// message-GET type or subject template sits in an accounted function. This is the check that
// round 2 asked for — it catches an alternate decoder or a raw request route that leaves the
// five constructions alone.
func TestPointReadCensusNoAlternateDecoder(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	// mqttJSA methods carry the transport; the manifest's nodes and the census's own helpers
	// consume the decoded response. Anything else naming these identifiers is a new route.
	allowed := map[string]bool{
		"loadLastMsgFor": true, "loadLastMsgForMulti": true, "loadMsg": true,
		"loadMsgsMulti": true, "loadNextMsgFromSeq": true, "processJSAPIReplies": true,
		// The batched response type is handled by the two classifiers and by
		// loadRetainedMessages' inline classification, all of which are census nodes.
		"mqttClassifyMsgGetResponse": true, "mqttMarkerLoadFromResponse": true,
		"loadRetainedMessages": true, "loadInflightRecordsMulti": true,
		"recoverInflightRegistersOnce": true, "reapIncarnationRegisters": true,
		"reapIncarnationMarkers": true, "resolve": true,
	}
	for _, ident := range []string{"JSApiMsgGetRequest", "JSApiMsgGetResponse", "JSApiMsgGetT"} {
		fns := sc.FunctionsReferencing(ident)
		if len(fns) == 0 {
			t.Errorf("no function references %s, so this check would pass vacuously", ident)
		}
		for fn := range fns {
			if !allowed[fn.Name] {
				t.Errorf("%s references %s and is not an accounted part of the message-read "+
					"routing surface. Either it is a new point-read route, which this census "+
					"covers, or the surface has moved and the inventory is stale",
					fn.String(), ident)
			}
		}
	}
	// A direct-get route would be a SIXTH entry point with different server-side error
	// mapping, so the invariant asserts there is none at this revision. Round 1 and round 2
	// each searched for one independently and found none.
	for _, ident := range []string{"JSDirectMsgGetT", "JSApiDirectMsgGet", "getDirectRequest"} {
		if fns := sc.FunctionsReferencing(ident); len(fns) != 0 {
			for fn := range fns {
				t.Errorf("%s references %s: a DIRECT-GET route has appeared. Its server-side "+
					"error mapping differs from the message-GET handler's, so F-109's census and "+
					"its repair candidates both have to be re-derived against it", fn.String(), ident)
			}
		}
	}
}
