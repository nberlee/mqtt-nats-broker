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
//	Any orphan-sweep arm that can still destroy live session-generation state requires the
//	candidate generation to read NONCURRENT in its client's authority: outstanding, or
//	retired (gen_standing.go). Without that proof, RETAIN.
//
// Everything the arms already required is KEPT — the complete reference walk, the
// quarantine grace, the exact identity revalidation, issue #66's cutoff, the claim-before-hold
// ordering, the consumer-instance relookup. The proof is an ADDITIONAL licence, never
// a replacement for any of them.
//
// The proof names the candidate generation exactly, by its lifecycle and ordinal, and no
// stream sequence takes part: the candidate generation is a REMOTE party's, and a sequence of
// its records is not comparable with a $MQTT_qos2in or consumer sequence.
//
// It holds for every generation the authority's lifecycle ever allocated, and for every
// generation of an earlier lifecycle once the registry records that lifecycle retired (issue
// #133). The retirement watermark it replaces was a single slot per client: once a later
// teardown replaced it, an earlier generation retired without a discarding closure was
// retained forever, a leak this classification does not have.
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
	// mqttRetireUnproven: the generation is current, or where it stands cannot be read.
	// Every destructive arm RETAINS.
	mqttRetireUnproven mqttRetireProof = iota

	// mqttRetireProven: the generation is outstanding or retired in its authority, so the
	// session that owned these names was durably discarded.
	mqttRetireProven
)

// retirementProvenFor reports whether the candidate generation of one client is provably
// retired, and is the ONE definition every gated arm uses.
//
// A noncurrent observation is monotone — a generation never becomes rightful again — so it is
// remembered in a bounded positive-only cache. Losing that memory to eviction or a restart
// costs a cleanup OPPORTUNITY and never a licence, which is what makes caching sound here
// and unsound for the negative outcomes. Absent, different-generation, corrupt and
// indeterminate are never cached: each can be invalidated by a retirement landing
// afterwards, and a finite window never arbitrates ownership.
//
// No lock held on entry; performs the authority read, and the registry read when the authority
// no longer carries the generation's lifecycle.
func (as *mqttAccountSessionManager) retirementProvenFor(idHash, gen string) mqttRetireProof {
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		// Nothing to key a proof by. Refusing is the same disposition every
		// unattributable candidate already gets.
		return mqttRetireUnproven
	}
	if as.retirementMemoHas(idHash, gen) {
		return mqttRetireProven
	}
	// WHERE THE GENERATION STANDS (gen_standing.go): an outstanding generation was made
	// noncurrent by a teardown claim or a Clean Start, both discarding, and a retired one is
	// past its retirement. No transition makes either current again, so no broker serves these
	// names. A current generation, and every undecided read, prove nothing.
	if standing, _ := as.genStandingByHash(idHash, gen); standing == mqttGenOutstanding || standing == mqttGenRetired {
		as.retirementMemoAdd(idHash, gen)
		return mqttRetireProven
	}
	return mqttRetireUnproven
}

// mqttRetirementMemoMax bounds the positive-only cache. When it is full the memo simply
// stops admitting: an eviction policy would be wrong here for the reason the sweep's
// observation table states — a deterministic scan revisits candidates in the same order,
// so evicting the entry due next would make every sighting look like a first sighting. A
// full memo costs one authority read per candidate per pass, which is the pre-cache cost,
// never a licence.
const mqttRetirementMemoMax = 4096

// retirementMemoHas reports whether this generation has already been observed noncurrent.
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
