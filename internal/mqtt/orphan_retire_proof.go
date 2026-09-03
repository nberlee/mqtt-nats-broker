// The orphan sweep's retirement proof: what licenses destroying a generation's state.
//
// The sweep decides absence from a complete $MQTT_sess walk plus this broker's residents.
// A session live on ANOTHER broker whose record is absent still uses these names — a
// resume PRESERVES the generation — so every destructive arm keyed on that absence can
// destroy an acknowledged QoS 2 payload belonging to a generation that is still serving.
// That is F-036's site 3, and it is a LOSS rather than a leak, which is why it blocks the
// alpha.
//
// THE PROPERTY, and it is one rule rather than three site fixes:
//
//	Any orphan-sweep arm that can still destroy live session-generation state requires a
//	valid retirement watermark whose Gen EQUALS the candidate generation. Without that
//	proof, RETAIN.
//
// Everything the arms already required is KEPT — the complete reference walk, the
// quarantine grace, the exact identity revalidation, F-062's cutoff, the claim-before-hold
// ordering, the consumer-instance relookup. The watermark is an ADDITIONAL licence, never
// a replacement for any of them.
//
// MarkerSeq SUPPLIES NO SECOND ARM HERE, and this is the one thing about the sweep that
// most invites a wrong fix. The PUBREL classifier is two-dimensional because a releasing
// session has its own sess.seq to compare a marker against. This pass does not: the
// candidate generation is a REMOTE party's, and MarkerSeq is a $MQTT_sess sequence, so
// comparing it against a $MQTT_qos2in, $MQTT_inflight or consumer sequence crosses
// incomparable sequence spaces. Only exact generation equality proves anything here.
//
// WHAT THAT COSTS, stated plainly because it is the trade this design accepts: a
// PERMANENT, UNBOUNDED leak. The watermark is a single slot per client, so once a later
// retirement replaces it the earlier generation's state can never again be proven
// retired — retire G1, then create and retire G2, and G1's holds, registers and PUBREL
// consumers are retained forever. There is no bounded variant available from current
// facts: a dead generation with an absent record and a LIVE REMOTE generation with an
// absent record are observationally identical to this pass, and waiting longer does not
// distinguish them. An exact bound would need permanent per-generation retirement history
// (which trades unbounded payload retention for unbounded metadata) or a distributed
// liveness lease. Stream limits and MaxAge are not a solution, because their bound is
// implemented by deleting the acknowledged payload. A leak is preferred to a loss, which
// is the same trade course E already took. See docs/CONFORMANCE.md.
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
//   - deleteOrphanPubRelConsumers: the consumer delete. Keyed on refs.pubRelDurables and
//     its own record re-read, NOT on refs.hasInbound — so it takes the gate on its own
//     terms, after the durable-name parse has produced the generation to prove.
//   - discardAbsentWinnerClaim: commits Discarded for an affine winner. DELIBERATELY
//     UNGATED and out of scope: it is affine assignment mutation, and F-036's
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
// docs/findings/ha-arbitration.md F-036.

package mqtt

// mqttRetireProof is the outcome of asking whether a candidate generation is provably
// retired.
type mqttRetireProof int

const (
	// mqttRetireUnproven: no valid watermark names this generation. Every destructive
	// arm RETAINS. This is the ordinary outcome for a generation whose watermark slot
	// has been replaced, and it is the leak.
	mqttRetireUnproven mqttRetireProof = iota

	// mqttRetireProven: a valid watermark names exactly this generation, so the session
	// that owned these names was durably discarded.
	mqttRetireProven
)

// mqttRetireProofPass is a PASS-SCOPED memo of negative observations.
//
// The positive memo below is durable for the process, because a proven retirement is
// monotone. A negative one must never be, or a generation whose retirement lands later
// would be retained forever. But within ONE pass a negative may be reused safely, and
// holistic review round 2 is why that matters: the accepted retention leak is unbounded
// in cardinality, so without this every retained candidate pays a fresh watermark read on
// every pass and the pass's JetStream traffic grows without bound — monopolising the
// account manager's sweep goroutine long after the storage cost was accepted.
//
// The scope is exactly one pass, so a retirement recorded between passes is still
// recognised on the next one. That is the whole difference between this and a cache.
type mqttRetireProofPass struct {
	negative map[string]struct{}
}

// newRetireProofPass starts a pass-scoped negative memo. A nil receiver is valid and
// simply memoizes nothing, so a caller that has no pass context still gets the right
// answer at the pre-change cost.
func newRetireProofPass() *mqttRetireProofPass {
	return &mqttRetireProofPass{negative: make(map[string]struct{})}
}

func (p *mqttRetireProofPass) key(idHash, gen string) string { return idHash + "\x00" + gen }

// seenNegative reports whether THIS pass has already read an unproven answer for the pair.
func (p *mqttRetireProofPass) seenNegative(idHash, gen string) bool {
	if p == nil {
		return false
	}
	_, ok := p.negative[p.key(idHash, gen)]
	return ok
}

func (p *mqttRetireProofPass) noteNegative(idHash, gen string) {
	if p == nil {
		return
	}
	p.negative[p.key(idHash, gen)] = struct{}{}
}

// retirementProvenForInPass is retirementProvenFor with a pass-scoped negative memo, for
// an arm that revisits many candidates in one pass.
func (as *mqttAccountSessionManager) retirementProvenForInPass(pass *mqttRetireProofPass,
	idHash, gen string) mqttRetireProof {

	if pass.seenNegative(idHash, gen) {
		return mqttRetireUnproven
	}
	got := as.retirementProvenFor(idHash, gen)
	if got != mqttRetireProven {
		pass.noteNegative(idHash, gen)
	}
	return got
}

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
// No lock held on entry; performs at most one JetStream read.
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
	if status != mqttRetirementValid || r.Gen != gen {
		return mqttRetireUnproven
	}
	as.retirementMemoAdd(idHash, gen)
	return mqttRetireProven
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
// The map is keyed by client-ID hash and holds ONE generation, which is exactly what the
// durable watermark holds: remembering more would claim a history the single slot never
// had. A later retirement for the same client simply replaces the entry, and the
// generation it replaces goes back to being unprovable — which is the leak, faithfully
// mirrored rather than papered over in memory.
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
