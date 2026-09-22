// The conversion of the checkpoint's 663 hand-authored edge records into clauses the closure
// engine can apply — and the property that stops the conversion from quietly changing what was
// reviewed.
//
// WHY A CLAUSE IS NOT A RECORD. A checkpoint record says "this edge is discharged, for this
// reason". It was reviewed against the obligation the node's own read creates, arriving nowhere
// — the obligation is BORN in that function. It was never reviewed against an obligation
// arriving from somewhere else, and plan-review round 2 named the same-origin version of the
// hazard: one origin can reach a function through two different carriers, and evidence for an
// incoming argument establishes nothing about the same origin arriving through state. So the
// conversion maps each record onto exactly one clause whose applicability relation is
// {the node's own read origins} x {origin-local}, and everything else the closure delivers to
// that edge is default-only and shows up in the deficit.
//
// WHY THE INTEGRITY PROPERTY IS PRESERVATION, NOT IDENTITY. Round 2 refuted a total, injective
// map: widening a clause keeps the map total and injective, and so does narrowing reviewed
// coverage away into the deficit. The property here compares the derived clauses against an
// INDEPENDENTLY PERSISTED snapshot of each node's read occurrences, taken at the reviewed
// revision, and recomputes the expected relation in the test rather than calling the builder —
// so a builder that widens or narrows fails.
//
// THE SNAPSHOT'S WARRANT, and its limits. prNodeReadOccs records what the scanner rendered at
// c7becd7, the revision against which every one of these records was reviewed, so it states
// what was reviewed rather than regenerating a baseline. It is invalid if it is ever recaptured
// to accept drift, if it absorbs a newly widened interpretation, or if a record it covers was
// authored against a different revision. From this commit on, drift fails.

package mqtt

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// prNodeReadOccs is the per-node inventory of per-subject read occurrences at the reviewed
// revision. Captured once, from the scan, at c7becd7.
var prNodeReadOccs = map[string][]int{
	"handoff_metadata.go:(mqttJSA).checkHandoffPredecessorSession":                       {1},
	"handoff_metadata.go:(mqttJSA).resolveMetadataAttempt":                               {3},
	"inflight_marker.go:(mqttAccountSessionManager).loadInflightMarker":                  {7},
	"inflight_register.go:(mqttAccountSessionManager).loadInflightRecordsMulti":          {3},
	"inflight_register.go:(mqttJSA).loadInflightRecord":                                  {0},
	"shared_lifecycle.go:(mqttJSA).loadSharedGroup":                                      {2},
	"shared_lifecycle.go:(mqttJSA).loadSharedLedger":                                     {0},
	"mqtt.go:(mqttAccountSessionManager).loadRetainedMessages":                           {5},
	"mqtt.go:(mqttJSA).loadSessionMsg":                                                   {0},
	"outbound_affine_readiness.go:(mqttJSA).loadAffineReadiness":                         {2},
	"outbound_cleanup_authz.go:(mqttAccountSessionManager).enumerateGenerationResources": {10},
	"outbound_completion.go:(mqttAccountSessionManager).enumerateGenerationObligations":  {10, 25},
	"outbound_directory.go:(mqttJSA).loadDirShardAt":                                     {0},
	"outbound_directory.go:(mqttJSA).loadRecoveryRoot":                                   {0},
	"outbound_directory.go:(mqttJSA).sweepOneBatchCharged":                               {6},
	"outbound_discharge.go:(mqttAccountSessionManager).proveExchangeDischarged":          {14},
	"outbound_duty_ops.go:(mqttJSA).loadDutyEntryAt":                                     {2},
	"outbound_inventory_boundary.go:(mqttJSA).checkDutyContinuation":                     {0},
	"outbound_inventory_boundary.go:(mqttJSA).loadGenLedger":                             {2},
	"outbound_manifest.go:(mqttJSA).checkManifestAgainstStore":                           {0},
	"outbound_material_delete.go:(mqttAccountSessionManager).deleteChunkInterval":        {2},
	"outbound_material_delete.go:(mqttJSA).checkPurgeConfirmed":                          {2},
	"outbound_material_verify.go:(mqttJSA).verifyMaterial":                               {5},
	"outbound_ops.go:(mqttJSA).loadOutboundExchange":                                     {0},
	"outbound_ops.go:(mqttJSA).loadOutboundResource":                                     {0},
	"outbound_ops.go:(mqttJSA).opCommitMetadata":                                         {10},
	"outbound_release_gate.go:(mqttJSA).loadDirShard":                                    {0},
	"outbound_release_gate.go:(mqttJSA).loadGenCompletionCert":                           {2},
	"outbound_replace_register.go:(mqttJSA).loadOccupantMarkerLoad":                      {0},
	"outbound_transition.go:(mqttJSA).checkActionMembers":                                {15},
	"outbound_transition.go:(mqttJSA).checkExchangeOverwrites":                           {6},
	"outbound_transition.go:(mqttJSA).checkIdentifierExclusivity":                        {21},
	"outbound_transition.go:(mqttJSA).checkPredecessorAgainstStore":                      {0},
	"outbound_transition.go:(mqttJSA).checkResourceOverwrites":                           {7, 19, 32},
	"outbound_transition.go:(mqttJSA).loadGenClosure":                                    {2},
	"outbound_transition.go:(mqttJSA).loadOutboundAuthority":                             {2},
	"outbound_transition.go:(mqttJSA).loadTransitionOutcome":                             {2},
	"qos2_move.go:(mqttJSA).loadQoS2Record":                                              {0},
	"qos2_release_claim.go:(mqttJSA).loadQoS2RelClaim":                                   {0},
	"qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffCopy":                    {3},
	"qos2_shared_affine.go:(mqttJSA).loadQ2AffClaim":                                     {0},
	"sess_accept_fact.go:(mqttJSA).readAcceptFact":                                       {0},
	"sess_teardown.go:(mqttJSA).loadGenRetirementRecord":                                 {0},
	"will_outbox.go:(mqttJSA).loadWillOutbox":                                            {0},
}

// prConversionWithdrawn records coverage the conversion does NOT carry forward, with the reason.
// A withdrawal is an explicit correction, never a disappearance into the deficit: the plan
// review was specific that silently narrowing reviewed coverage passes any identity-shaped
// check, so it has to be stated where a reader can see it and a test can count it.
//
// Both entries are the two MULTI-READ nodes, and the reason is the same one that refuted the
// mechanical conversion in the first place. Their records were reviewed against the node as a
// whole — a `PRESENT-ONLY` discharge, for instance, is relative to a PARTICULAR read's absent
// arm — and there is no sound way to attribute one to one of three reads without re-reading
// every edge. Their coverage is therefore withdrawn and their edges are default-only, which
// makes the deficit larger and honest rather than smaller and unwarranted.
var prConversionWithdrawn = map[string]string{
	"outbound_transition.go:(mqttJSA).checkResourceOverwrites": "three reads at occurrences 7, " +
		"19 and 32, two of them inside function literals with different consequences. A record " +
		"discharging an edge relative to one read's absent arm says nothing about the other two, " +
		"and the checkpoint's records do not say which read they were reviewed against",
	"outbound_completion.go:(mqttAccountSessionManager).enumerateGenerationObligations": "two " +
		"reads at occurrences 10 and 25, whose absence travels as additions that never happen to " +
		"two fields of a returned local. No record names which read it was reviewed against",
}

// prNodeOwnOrigins is the relation the conversion is allowed to cover for one node: the origins
// its own reads create, arriving nowhere because they are born here.
func prNodeOwnOrigins(node string, occs []int) []string {
	out := make([]string, 0, len(occs))
	for _, occ := range occs {
		comp := prSeedComponents[node+"@"+strconv.Itoa(occ)]
		if comp == "" {
			comp = prCompUnknown
		}
		out = append(out, prOrigin{Fn: node, Occ: occ, Comp: comp}.String())
	}
	sort.Strings(out)
	return out
}

// prBuildClauses converts the checkpoint's records. One record becomes one clause, carrying the
// record's own disposition, explanation and reason unchanged; the applicability relation is the
// node's own read origins arriving origin-local, and nothing else.
func prBuildClauses(sc *censusScan) map[string][]prClause {
	out := map[string][]prClause{}
	for node, n := range prCensusNodes {
		if _, withdrawn := prConversionWithdrawn[node]; withdrawn {
			continue
		}

		origins := prNodeOwnOrigins(node, prNodeReadOccs[node])
		if len(origins) == 0 {
			continue
		}
		for _, e := range n.Edges {
			pairs := map[string]bool{}
			for _, o := range origins {
				pairs[prPairKey(o, prArrLocal)] = true
			}
			key := node + " " + e.Key
			out[key] = append(out[key], prClause{
				Pairs: pairs, Disp: e.Disp, Exp: e.Exp, Why: e.Why, To: e.To,
			})
		}
	}
	return out
}

// prRecordDigest is the checkpoint reference's unit: one legacy record's reviewed content.
func prRecordDigest(disp, exp, why, to string) string {
	h := sha256.Sum256([]byte(disp + "\x00" + exp + "\x00" + why + "\x00" + to))
	return hex.EncodeToString(h[:8])
}

// TestPointReadCensusConversionPreservesReviewedEvidence is property 9. It recomputes the
// expected applicability relation from the persisted snapshot INSIDE the test — never by asking
// the builder what it thinks — so a builder that covers one pair more, or one pair fewer, fails
// here rather than passing a count.
func TestPointReadCensusConversionPreservesReviewedEvidence(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	clauses := prBuildClauses(sc)
	if len(clauses) == 0 {
		t.Fatal("the conversion produced no clauses, so every check here would pass vacuously")
	}
	for node, n := range prCensusNodes {
		_, withdrawn := prConversionWithdrawn[node]
		occs, snapped := prNodeReadOccs[node]
		if !snapped {
			t.Errorf("node %s has no entry in the reviewed-revision read snapshot, so the "+
				"conversion has nothing to preserve its records against", node)
			continue
		}
		// Independently recomputed here, from the snapshot, without the builder.
		want := map[string]bool{}
		if !withdrawn {
			for _, occ := range occs {
				comp := prSeedComponents[node+"@"+strconv.Itoa(occ)]
				if comp == "" {
					comp = prCompUnknown
				}
				want[prPairKey(prOrigin{Fn: node, Occ: occ, Comp: comp}.String(), prArrLocal)] = true
			}
		}
		for _, e := range n.Edges {
			got := clauses[node+" "+e.Key]
			if withdrawn {
				if len(got) != 0 {
					t.Errorf("node %s edge %q has a clause, but this node's coverage is recorded "+
						"as WITHDRAWN: %s. A withdrawal that the builder quietly reinstates is "+
						"exactly the widening this property exists to catch",
						node, e.Key, prConversionWithdrawn[node])
				}
				continue
			}
			if len(got) != 1 {
				t.Errorf("node %s edge %q converted to %d clauses; each checkpoint record "+
					"converts to exactly one, and a record that produced none has lost its "+
					"reviewed disposition", node, e.Key, len(got))
				continue
			}
			cl := got[0]
			// Compared against the IMMUTABLE CHECKPOINT DIGEST, not against the live record
			// the clause was derived from. Holistic round 1: comparing a derived clause with
			// its own source establishes only that the derivation copied something.
			ref, known := prCheckpointRecords[node+" "+e.Key]
			if !known {
				t.Errorf("node %s edge %q has no checkpoint reference digest, so its conversion "+
					"has nothing immutable to be preserved against", node, e.Key)
			} else if got := prRecordDigest(cl.Disp, cl.Exp, cl.Why, cl.To); got != ref {
				t.Errorf("node %s edge %q: the converted clause digests to %s, the checkpoint "+
					"record to %s. Either the clause no longer carries the record's reviewed "+
					"disposition, explanation and reason, or the record itself was edited — and "+
					"both are a re-review", node, e.Key, got, ref)
			}
			if cl.Disp != e.Disp || cl.Exp != e.Exp || cl.Why != e.Why || cl.To != e.To {
				t.Errorf("node %s edge %q: the converted clause does not carry the record's own "+
					"disposition, explanation and reason", node, e.Key)
			}
			for p := range want {
				if !cl.Pairs[p] {
					t.Errorf("node %s edge %q: the conversion dropped reviewed coverage of %q. "+
						"Coverage may only be withdrawn through prConversionWithdrawn, with a "+
						"stated reason", node, e.Key, p)
				}
			}
			for p := range cl.Pairs {
				if !want[p] {
					t.Errorf("node %s edge %q: the conversion covers %q, which this record was "+
						"never reviewed against. New coverage needs separately approved "+
						"evidence, not a wider derivation", node, e.Key, p)
				}
			}
		}
	}
	for node := range prConversionWithdrawn {
		if _, ok := prCensusNodes[node]; !ok {
			t.Errorf("a withdrawal is recorded for %q, which the manifest no longer declares. A "+
				"stale withdrawal overstates the deficit and hides what is actually covered", node)
		}
	}
	// The snapshot must still describe the tree. A read added to or removed from a node changes
	// what its records were reviewed against, and that is a re-review, not a recapture.
	live := map[string][]int{}
	for _, c := range sc.Calls {
		if c.Callee == "loadLastMsgFor" || c.Callee == "loadLastMsgForMulti" {
			live[c.Fn.String()] = append(live[c.Fn.String()], c.Occ)
		}
	}
	for node, occs := range prNodeReadOccs {
		got := live[node]
		sort.Ints(got)
		want := append([]int(nil), occs...)
		sort.Ints(want)
		if len(got) != len(want) {
			t.Errorf("node %s reads at %v, but the reviewed-revision snapshot records %v. The "+
				"snapshot states what the records were reviewed against; a changed read set is a "+
				"re-review, never a recapture", node, got, want)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("node %s reads at %v against a snapshot of %v", node, got, want)
				break
			}
		}
	}
	for node := range live {
		if _, ok := prNodeReadOccs[node]; !ok {
			t.Errorf("%s performs a per-subject read and is absent from the reviewed-revision "+
				"snapshot, so a new read has appeared with no reviewed record behind it", node)
		}
	}
}

// TestPointReadCensusClauseScoping is property 3, and it is the one that stops a clause reviewed
// for one carrier from terminating another. It checks the two shapes the plan review named: a
// clause may not cover a pair outside its own relation, and a FIELD-scoped component may not be
// offered as coverage of a RETURN origin — enumerateGenerationObligations is why, since its
// absence travels as two omitted field updates that only the return conservatively encompasses.
func TestPointReadCensusClauseScoping(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	for key, cls := range prBuildClauses(sc) {
		for _, cl := range cls {
			if len(cl.Pairs) == 0 {
				t.Errorf("clause on %s covers no (origin, arrival) pair at all. An empty relation "+
					"is not a discharge; it is a record with nothing behind it", key)
			}
			for p := range cl.Pairs {
				origin, arrival, ok := strings.Cut(p, "|")
				if !ok {
					t.Errorf("clause on %s carries malformed pair %q", key, p)
					continue
				}
				if !prArrivalLegal(arrival) {
					t.Errorf("clause on %s names arrival %q, which is outside the vocabulary. An "+
						"arrival outside it cannot be reasoned about, and an unknown transfer has "+
						"its own value", key, arrival)
				}
				i := strings.LastIndex(origin, "/")
				if i < 0 || !prComponentLegal(origin[i+1:]) {
					t.Errorf("clause on %s names origin %q, whose component is outside the "+
						"grammar", key, origin)
				}
			}
		}
	}
}

// prFrontierDigest is the checkpoint reference's unit for a RETAINED FORMER-FRONTIER entry:
// its destination and the words its obligation was reviewed in. The engine supplies the
// expansion, so what a reference has to protect is the MEANING — holistic round 13 found the
// obligation text checked only for non-emptiness and absent from every fingerprint, so
// replacing `loadSessionMsg`'s description with "no downstream interpretation owed" would have
// passed every check while contradicting the reviewed record.
func prFrontierDigest(to, obligation string) string {
	h := sha256.Sum256([]byte(to + "\x00" + obligation))
	return hex.EncodeToString(h[:8])
}
