// The orphan sweep's retirement proof: what licenses destroying a generation's state.
//
// The sweep decides absence from a complete $MQTT_sess walk plus this broker's residents.
// A session live on ANOTHER broker whose record is absent still uses these names — a
// resume PRESERVES the generation — so every destructive arm keyed on that absence can
// destroy an acknowledged QoS 2 payload belonging to a generation that is still serving.
// That is issue #40's site 3, and it is a LOSS rather than a leak, which is why it blocks the
// alpha.
//
// THE PROPERTY, and it is one rule rather than three site fixes:
//
//	Any orphan-sweep arm that can still destroy live session-generation state requires a
//	valid retirement watermark whose Gen EQUALS the candidate generation, or a valid
//	permanent closure of that generation which carried an authorized discard. Without
//	either proof, RETAIN.
//
// Everything the arms already required is KEPT — the complete reference walk, the
// quarantine grace, the exact identity revalidation, issue #66's cutoff, the claim-before-hold
// ordering, the consumer-instance relookup. The proof is an ADDITIONAL licence, never
// a replacement for any of them.
//
// MarkerSeq SUPPLIES NO SECOND ARM HERE, and this is the one thing about the sweep that
// most invites a wrong fix. The PUBREL classifier is two-dimensional because a releasing
// session has its own sess.seq to compare a marker against. This pass does not: the
// candidate generation is a REMOTE party's, and MarkerSeq is the sequence of that party's
// session-record marker, so comparing it against a $MQTT_qos2in or consumer sequence
// crosses incomparable sequence spaces, and against an in-flight record's sequence (the
// same stream now) compares two writes of unrelated records. Only exact generation
// equality proves anything here.
//
// WHAT THAT COSTS, stated plainly because it is the trade this design accepts: a
// PERMANENT, UNBOUNDED leak for a generation with no discarding closure. The watermark is a
// single slot per client, so once a later retirement replaces it the earlier generation can be
// proven retired only through its own closure. A generation whose closure carried an
// authorized discard — every receipted teardown, and every clean-start handoff's predecessor
// — keeps that proof permanently: the closure is one create-only subject per generation,
// never deleted while the recovery epoch stands, and a closed generation is never admitted
// again (checkProposedGenerationIsFresh). A generation retired without one — a receiptless
// teardown — is retained forever once the slot moves on: retire G1 that way, then create and
// retire G2, and G1's holds, registers and PUBREL consumers stay. There is no bounded variant
// for that population from current facts: a dead generation with an absent record and a LIVE
// REMOTE generation with an absent record are observationally identical to this pass, and
// waiting longer does not distinguish them. Stream limits and MaxAge are not a solution,
// because their bound is implemented by deleting the acknowledged payload. A leak is preferred
// to a loss, which is the same trade course E already took. See docs/CONFORMANCE.md.
//
// WHAT STILL RUNS WITHOUT PROOF is the non-destructive tail, and it is what keeps this
// from being a blanket retain: once the payload is already ABSENT, removing the fence that
// protected it destroys nothing. Those arms are in reconcileOneInboundQoS2.
//
// THE CURRENT CENSUS of arms that mutate from this false-absence premise — recorded as
// today's enumeration, NOT as a claim that it stays complete:
//
//   - reconcileOneInboundQoS2: the `discarded` claim creation and the nonterminal hold
//     purge. Keyed on refs.hasInbound.
//   - reconcileOrphanInflight: the absent-generation register delete. Also
//     refs.hasInbound.
//   - (deleteOrphanPubRelConsumers is no longer an arm of this premise: since issue #107 7c-lc its
//     licence is the name's RETIRED ownership alone, consult 14 finding 2.)
//   - discardAbsentWinnerClaim: commits Discarded for an affine winner. DELIBERATELY
//     UNGATED and out of scope: it is affine assignment mutation, and issue #40's
//     supported-scope ruling of 2026-09-02 says a closure need not preserve the
//     experimental session-affine shared-QoS-2 branch, with affine-only regressions
//     accumulating under ROADMAP chapter P.
//
// A structural inventory guard over these was designed, reviewed three times and DELETED.
// A primitive-only AST inventory cannot catch a new unsafe CALLER of an
// already-inventoried destructive helper, and completing it needs the bounded SSA
// reachability this repository has already ruled disproportionate — so the guard could not
// honestly claim that a fifth arm cannot appear silently. What protects the property
// instead is this census, the behavioral negative tests with exact payload assertions, and
// their positive controls.
//
// See docs/ARCHITECTURE.md, "Orphan reconciliation", and
// https://github.com/nberlee/mqtt-nats-broker/issues/131 issue #40.

package mqtt

// mqttRetireProof is the outcome of asking whether a candidate generation is provably
// retired.
type mqttRetireProof int

const (
	// mqttRetireUnproven: no valid watermark names this generation and no valid discarding
	// closure of it stands. Every destructive arm RETAINS. For a generation whose watermark
	// slot has been replaced and which was closed without a discard, or never closed, that is
	// the leak.
	mqttRetireUnproven mqttRetireProof = iota

	// mqttRetireProven: a valid watermark names exactly this generation, or its permanent
	// closure carried an authorized discard, so the session that owned these names was
	// durably discarded.
	mqttRetireProven
)

// retirementProvenFor reports whether the candidate generation of one client is provably
// retired, and is the ONE definition every gated arm uses.
//
// A valid MATCHING observation is monotone — a generation never becomes rightful again,
// and the single slot can lose the fact's discoverability but not the fact — so it is
// remembered in a bounded positive-only cache. Losing that memory to eviction or a restart
// costs a cleanup OPPORTUNITY and never a licence, which is what makes caching sound here
// and unsound for the negative outcomes. Absent, different-generation, corrupt and
// indeterminate are never cached: each can be invalidated by a retirement landing
// afterwards, and a finite window never arbitrates ownership.
//
// No lock held on entry; performs at most two JetStream reads: the watermark, then the
// generation's closure when the watermark does not name it.
func (as *mqttAccountSessionManager) retirementProvenFor(idHash, gen string) mqttRetireProof {
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		// Nothing to key a proof by. Refusing is the same disposition every
		// unattributable candidate already gets.
		return mqttRetireUnproven
	}
	if as.retirementMemoHas(idHash, gen) {
		return mqttRetireProven
	}
	r, _, status := as.jsa.loadGenRetirement(as.domainTk, idHash)
	if status == mqttRetirementValid && r.Gen == gen {
		as.retirementMemoAdd(idHash, gen)
		return mqttRetireProven
	}
	// THE GENERATION'S OWN PERMANENT CLOSURE, when the single watermark slot does not name it.
	// A closure that carried an authorized DISCARD is the per-generation retirement history the
	// watermark cannot be: one subject per generation, create-only, its Discard member immutable,
	// never deleted while the recovery epoch stands — and a closed generation is never admitted
	// again (checkProposedGenerationIsFresh), so no broker serves these names. Only a VALID
	// discarding closure proves; a preserving one kept its obligations, and absent, corrupt and
	// indeterminate prove nothing.
	if c, _, cst := as.jsa.loadGenClosureByHash(idHash, as.sessStreamEpoch, gen); cst == mqttOutboundValid && c.Discard {
		as.retirementMemoAdd(idHash, gen)
		return mqttRetireProven
	}
	return mqttRetireUnproven
}

// mqttRetirementMemoMax bounds the positive-only cache. When it is full the memo simply
// stops admitting: an eviction policy would be wrong here for the reason the sweep's
// observation table states — a deterministic scan revisits candidates in the same order,
// so evicting the entry due next would make every sighting look like a first sighting. A
// full memo costs one watermark read per candidate per pass, which is the pre-cache cost,
// never a licence.
const mqttRetirementMemoMax = 4096

// retirementMemoHas reports whether a valid watermark naming exactly this generation has
// already been observed.
func (as *mqttAccountSessionManager) retirementMemoHas(idHash, gen string) bool {
	as.mu.RLock()
	defer as.mu.RUnlock()
	return as.retirementMemo[idHash] == gen
}

// retirementMemoAdd remembers a proven retirement.
//
// The map is keyed by client-ID hash and holds ONE generation. That bounds it; it does not
// mirror what is provable, which a replaced entry still is, through its closure when it had a
// discarding one. A replaced entry costs a read on its next sighting, never a licence.
func (as *mqttAccountSessionManager) retirementMemoAdd(idHash, gen string) {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.retirementMemo == nil {
		as.retirementMemo = make(map[string]string)
	}
	if _, known := as.retirementMemo[idHash]; !known && len(as.retirementMemo) >= mqttRetirementMemoMax {
		return
	}
	as.retirementMemo[idHash] = gen
}
