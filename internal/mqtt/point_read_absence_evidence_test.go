// Evidence currency, and the one explanation whose membership is GENERATED.
//
// TWO DEFECTS THIS FILE EXISTS TO CATCH, both named by the plan review.
//
// An occurrence key going on existing is not evidence currency. `CALL#7` still being a call
// does not detect changed arguments, a changed branch, or a different call substituted at the
// same ordinal — and for a clause, widening its applicability relation changes no source text
// at all. So every reviewed record carries a FINGERPRINT over the inputs its reason actually
// rests on, and the fingerprint's inputs are per record kind: the enclosing function's rendered
// body (which carries the calls, their arguments, the branch conditions and the definitions
// they depend on, including the one `absent` assignment that can change while every call stays
// identical), plus the record's own applicability relation.
//
// A predicate is not membership. Generating an explanation's members from a predicate and then
// checking the predicate against them proves nothing. So the approved membership is PERSISTED
// here, separately and by identity, and the test compares the freshly generated set against it
// by occurrence and destination — never by count, because a same-count substitution is exactly
// the failure a count cannot see. A newly added call that satisfies the predicate FAILS until
// its membership is reviewed.
//
// WHY CAPTURING THE FINGERPRINTS NOW IS A SNAPSHOT AND NOT A REGENERATED BASELINE. These
// records were reviewed against this revision, so recording what the scanner renders here
// states what was reviewed. Codex's plan-review round 2 confirmed the argument and named its
// three invalidating conditions, which hold as written: the capture must not use inputs that
// have changed, must not silently absorb a newly WIDENED interpretation — which is why the
// conversion's applicability relation is part of the fingerprint rather than only the source —
// and must never be rerun to accept later drift. From this commit on, a drift fails.

package mqtt

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// prEvidenceFor computes the fingerprint of one reviewed record's inputs.
func prEvidenceFor(sc *censusScan, fn string, extra ...string) string {
	var key censusFuncKey
	for k := range sc.Funcs {
		if k.String() == fn {
			key = k
			break
		}
	}
	h := sha256.New()
	fmt.Fprintf(h, "fn=%s\n", fn)
	fmt.Fprintf(h, "body=%s\n", sc.Bodies[key])
	sorted := append([]string(nil), extra...)
	sort.Strings(sorted)
	for _, e := range sorted {
		fmt.Fprintf(h, "extra=%s\n", e)
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// prNodeEvidenceInputs is a node's evidence: its rendered body, plus the applicability relation
// of every clause the conversion derives for it. Widening a clause changes no source text, so
// the relation has to be an input or the fingerprint cannot see the widening.
func prNodeEvidenceInputs(sc *censusScan, node string, clauses map[string][]prClause) []string {
	var extra []string
	// A state edge's reason depends on WHICH destinations its field spelling reaches, so the
	// destination IDENTITIES are an input — not their count. A same-count substitution outside
	// the enclosing body leaves a count, and a body fingerprint, completely unchanged.
	for _, m := range sc.Mutations {
		if m.Fn.String() != node || m.Kind == "local" || m.Field == "" {
			continue
		}
		dests := map[string]bool{}
		for _, f := range sc.Fields {
			if f.Field == m.Field {
				dests[f.Fn.String()] = true
			}
		}
		names := make([]string, 0, len(dests))
		for d := range dests {
			names = append(names, d)
		}
		sort.Strings(names)
		extra = append(extra, fmt.Sprintf("MUT#%d dests=%s", m.Occ, strings.Join(names, ",")))
	}
	// The node's own reviewed content. Holistic round 1: a fingerprint over the body and the
	// applicability pairs leaves Why, To, the axes, Auth and Repair free to be replaced with
	// anything, and replacing a reviewed repair premise is precisely the drift this is for.
	n := prCensusNodes[node]
	// The named explanation's own TEXT, not just its name. Holistic round 2: hashing `e.Exp`
	// leaves LOCK's reviewed justification free to be rewritten while every digest stands.
	for _, e := range n.Edges {
		if e.Exp != "" {
			extra = append(extra, "explanation "+e.Exp+"="+prCensusExplanations[e.Exp])
		}
	}
	extra = append(extra, strings.Join([]string{
		"axes", n.Mapping, n.Batch, n.Auth, n.ClassEv, n.ConsEv, n.Reach, n.Source, n.Repair,
	}, "|"))
	for _, e := range n.Edges {
		extra = append(extra, fmt.Sprintf("edge %s|%s|%s|%s|%s|%d",
			e.Key, e.Disp, e.Exp, e.Why, e.To, e.Dests))
		for _, cl := range clauses[node+" "+e.Key] {
			pairs := make([]string, 0, len(cl.Pairs))
			for p := range cl.Pairs {
				pairs = append(pairs, p)
			}
			sort.Strings(pairs)
			extra = append(extra, e.Key+"|"+cl.Disp+"|"+cl.Exp+"|"+strings.Join(pairs, ","))
		}
	}
	return extra
}

// prDispositionEvidenceInputs is a disposition's whole reviewed content. Every field is an
// input, because each of them IS the review: swapping one repair premise for another, or one
// producing site for another, changes what was reviewed while the source stands still.
func prDispositionEvidenceInputs(d prWorklistDisposition) []string {
	return []string{strings.Join([]string{
		d.Kind, d.Mapping, d.Auth, d.ClassEv, d.ConsEv, d.Reach, d.Source, d.Repair, d.Why,
	}, "|")}
}

// TestPointReadCensusEvidenceIsCurrent is property 4's currency half. A record whose inputs have
// moved is STALE and owes a re-review; it is not silently carried forward.
func TestPointReadCensusEvidenceIsCurrent(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	clauses := prBuildClauses(sc)
	if len(prNodeEvidence) == 0 || len(prDispositionEvidence) == 0 {
		t.Fatal("no evidence is persisted, so this property would pass vacuously")
	}
	for node := range prCensusNodes {
		want, ok := prNodeEvidence[node]
		if !ok {
			t.Errorf("node %s carries no persisted evidence fingerprint, so nothing detects a "+
				"change to the code its reason rests on", node)
			continue
		}
		if got := prEvidenceFor(sc, node, prNodeEvidenceInputs(sc, node, clauses)...); got != want {
			t.Errorf("node %s: evidence fingerprint is %s, persisted %s. Either the code this "+
				"record was reviewed against has changed, or its clause's applicability relation "+
				"has — both are a RE-REVIEW, and neither is a recapture", node, got, want)
		}
	}
	for node := range prNodeEvidence {
		if _, ok := prCensusNodes[node]; !ok {
			t.Errorf("evidence is persisted for %s, which is no longer a declared node", node)
		}
	}
	for fn, d := range prWorklistDispositions {
		want, ok := prDispositionEvidence[fn]
		if !ok {
			t.Errorf("disposition for %s carries no persisted evidence fingerprint", fn)
			continue
		}
		if got := prEvidenceFor(sc, fn, prDispositionEvidenceInputs(d)...); got != want {
			t.Errorf("disposition for %s: evidence fingerprint is %s, persisted %s. The site it "+
				"was reviewed against has changed", fn, got, want)
		}
	}
	for fn := range prDispositionEvidence {
		if _, ok := prWorklistDispositions[fn]; !ok {
			t.Errorf("evidence is persisted for %s, which carries no disposition", fn)
		}
	}
}

// ---------------------------------------------------------------------------
// The LOCK explanation, with GENERATED membership.
// ---------------------------------------------------------------------------

// prLockMethods is the predicate's method set. A selector-form call to one of these is a
// CANDIDATE for the LOCK explanation — and consult 2 was explicit that a predicate plus
// generated membership is insufficient on its own, which is why the approved set below is
// persisted separately and the applicability evidence is checked per member.
var prLockMethods = map[string]bool{
	"Lock": true, "Unlock": true, "RLock": true, "RUnlock": true,
}

// prReceiverField is the field spelling a mutex receiver expression names: the last selector
// of "as.mu" is "mu". A receiver that is not a selector has no field declaration to depend on.
func prReceiverField(on string) string {
	if i := strings.LastIndex(on, "."); i >= 0 {
		return on[i+1:]
	}
	return on
}

// prFieldDeclsFor is the declaration evidence for a mutex receiver. Where the receiver
// expression is the enclosing method's own receiver — `as.mu` inside a method on
// mqttAccountSessionManager — the declaration is that type's field and nothing else. Where it
// cannot be narrowed, EVERY declaration of the spelling is the evidence: over-sensitive costs
// a re-review, under-sensitive costs the guarantee.
func prFieldDeclsFor(sc *censusScan, c censusCallSite) []string {
	field := prReceiverField(c.On)
	all := sc.FieldDecls[field]
	base := c.On
	if i := strings.LastIndex(base, "."); i >= 0 {
		base = base[:i]
	}
	if base == sc.RecvOf[c.Fn] && c.Fn.Recv != "" {
		want := c.Fn.Recv + "." + field + " "
		for _, d := range all {
			if strings.HasPrefix(d, want) {
				return []string{d}
			}
		}
	}
	return all
}

// prLockMembership generates the membership from the scan: every edge of a declared node whose
// call is a selector-form call to a mutex method, keyed by "<node> <edge key>" and carrying its
// destination — which for a mutex operation is the rendered receiver, because that is what
// establishes WHOSE lock it is.
func prLockMembership(sc *censusScan) map[string]string {
	out := map[string]string{}
	for node := range prCensusNodes {
		for _, c := range sc.Calls {
			if c.Fn.String() != node || c.On == "" || !prLockMethods[c.Callee] {
				continue
			}
			// The DECLARATION of the field the receiver names is part of applicability: "as.mu"
			// establishes that this is a mutex only through what `mu` is declared as, and that
			// declaration can change with no call site changing.
			decl := strings.Join(prFieldDeclsFor(sc, c), ";")
			out[fmt.Sprintf("%s CALL#%d", node, c.Occ)] = c.On + "." + c.Callee + " [" + decl + "]"
		}
	}
	return out
}

// TestPointReadCensusLockMembership is property 5. The approved membership is the set of edges
// the manifest actually discharges UNDER the LOCK explanation, persisted separately; a mutex
// call discharged under a more specific reviewed explanation — `PRE-READ`, where the call is
// evaluated before the read and no outcome of the read can reach it — is recorded in the second
// map with that explanation named. Three things are established:
//
//   - every edge citing LOCK satisfies the predicate AND matches the approved destination by
//     occurrence and receiver identity, so a same-count substitution fails;
//   - every predicate match is in one of the two maps, so a NEWLY ADDED mutex call fails until
//     its membership is reviewed — the predicate never admits it on its own;
//   - the second map is not an escape hatch: each of its entries names the explanation that
//     covers it, and that explanation must be the one the manifest cites.
func TestPointReadCensusLockMembership(t *testing.T) {
	// Deliberately NOT parallel, and declared before the immutability harness below: a
	// parallel harness can run before the test whose regeneration it exists to catch, and
	// then it catches nothing.
	sc := censusScanPackage(t)
	generated := prLockMembership(sc)
	if len(prLockApproved) == 0 && len(prLockCoveredElsewhere) == 0 {
		t.Fatal("no LOCK membership is approved at all, so this property would pass vacuously")
	}
	cited := map[string]string{}
	for node, n := range prCensusNodes {
		for _, e := range n.Edges {
			cited[node+" "+e.Key] = e.Exp
		}
	}
	for key, dest := range generated {
		want, approved := prLockApproved[key]
		elsewhere, other := prLockCoveredElsewhere[key]
		switch {
		case approved && other:
			t.Errorf("%s is in both LOCK membership maps; one of the two is stale", key)
		case approved:
			if want != dest {
				t.Errorf("%s: generated destination %q, approved %q. Membership is compared by "+
					"occurrence AND receiver identity, because a same-count substitution leaves "+
					"every count unchanged", key, dest, want)
			}
			if cited[key] != "LOCK" {
				t.Errorf("%s is approved LOCK membership but the manifest discharges it under "+
					"%q", key, cited[key])
			}
		case other:
			if cited[key] != elsewhere {
				t.Errorf("%s is recorded as covered by %q, but the manifest cites %q. The second "+
					"map records which reviewed explanation covers a mutex call; it does not get "+
					"to name one the manifest does not", key, elsewhere, cited[key])
			}
		default:
			t.Errorf("%s satisfies the LOCK predicate and is in NEITHER membership map. A newly "+
				"added mutex call fails here until its membership is reviewed; the predicate "+
				"does not admit it on its own", key)
		}
	}
	for key := range prLockApproved {
		if _, ok := generated[key]; !ok {
			t.Errorf("approved LOCK member %s no longer satisfies the predicate, so the approval "+
				"is stale", key)
		}
	}
	for key := range prLockCoveredElsewhere {
		if _, ok := generated[key]; !ok {
			t.Errorf("%s is recorded as a mutex call covered elsewhere and is no longer one", key)
		}
	}
	for key, exp := range cited {
		if exp != "LOCK" {
			continue
		}
		if _, ok := prLockApproved[key]; !ok {
			t.Errorf("edge %s cites the LOCK explanation but is not in its approved membership, "+
				"so the explanation covers an edge nothing checked applicability for", key)
		}
	}
}

// TestPointReadCensusApprovedBaselineIsImmutable is the INDEPENDENT harness. The membership
// property above compares generated members against an approved map — and that comparison is
// worthless if anything regenerates the map before the comparison runs. This test holds a copy
// taken at package initialisation, before any test body executes, and fails if the approved
// maps have moved since.
//
// It is not hypothetical. Regenerating the baseline inside the membership test was tried as a
// mutation, and with it in place a same-count destination substitution PASSED: the corrupted
// value was simply rewritten before it was checked. Nothing in the membership test itself can
// detect that, which is exactly why the harness is separate.
func TestPointReadCensusApprovedBaselineIsImmutable(t *testing.T) {
	for _, c := range []struct {
		name         string
		live, atInit map[string]string
	}{
		{"prLockApproved", prLockApproved, prLockApprovedAtInit},
		{"prLockCoveredElsewhere", prLockCoveredElsewhere, prLockCoveredElsewhereAtInit},
		{"prNodeEvidence", prNodeEvidence, prNodeEvidenceAtInit},
		{"prDispositionEvidence", prDispositionEvidence, prDispositionEvidenceAtInit},
	} {
		if len(c.live) != len(c.atInit) {
			t.Errorf("%s has %d entries, %d at initialisation: something regenerated the "+
				"approved baseline, and a comparison against a regenerated baseline establishes "+
				"nothing", c.name, len(c.live), len(c.atInit))
			continue
		}
		for k, v := range c.atInit {
			if c.live[k] != v {
				t.Errorf("%s[%q] is %q, %q at initialisation: the approved baseline was "+
					"rewritten during the run", c.name, k, c.live[k], v)
			}
		}
	}
}
