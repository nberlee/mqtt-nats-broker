// Repairing the grant a recognised forked record may be missing.
//
// PRECISION about what is computed WHERE, because the mint site knows less than the
// merge site: the capsule is a CANDIDATE delta, built without ever reading the survivor.
// Only mergeForkedGrantLocked compares it against the surviving body and decides, filter
// by filter, whether a candidate is applied or skipped.
//
// sess_fork_lineage.go recognises that a surviving record is a SIBLING of the one this
// broker was acknowledged for, and stops there. The client is told 0x89 "server busy"
// rather than a takeover claim — which invites the reconnect — but the subscriptions it was SUBACKed
// for, and the UNSUBSCRIBEs it was UNSUBACKed for, are gone. MQTT requires the Session
// State to carry the client's subscriptions ([MQTT-4.1.0-1], Spec5 [4.1.1]), so an
// acknowledged SUBSCRIBE or UNSUBSCRIBE silently ceasing to exist is a conformance break.
//
// WHAT IS RECOVERABLE IS THE RECORD DELTA, and it is a WELL-DEFINED RECONCILIATION
// AGAINST THE RETAINED BODY — deliberately not described as a three-way merge over a
// proven common ancestor, which is more than anything here establishes.
//
// The anchor condition classifyRecordLineage tests (found.PWID == ackPWID) means the
// survivor CLAIMS the parent our erased record claimed. That is a claim, not a proof:
// restore adopts WID and PWID without validating provenance. Three things are true, and
// keeping them apart is what keeps this honest:
//
//   - The comparison baseline is concrete, and it is the retained PARENT body
//     (sess.grantBodyParent) — the record our acknowledged record was DERIVED FROM. The
//     delta re-asserted is our acknowledged body (sess.grantBody) relative to it; see
//     mqttGrantFromBody's two arguments. Its standing comes from this broker having held
//     BOTH bodies, never from the survivor's pointer.
//   - The computation is therefore deterministic WITHOUT trusting the survivor's PWID —
//     the delta is ours-versus-baseline, and the survivor's body is the thing it is
//     re-asserted onto.
//   - It does NOT establish that the survivor descended from that body, nor that the
//     baseline is a genuine common ancestor of both. It never needs to: what is
//     re-asserted is state this broker's own client was acknowledged for.
//
// AUTHORIZATION IS WEAKER THAN RECOGNITION, and the two must not be conflated. The
// classifier's conclusion is NEGATIVE — no takeover is established — and needs no
// provenance. This file ACTS, and it acts on an inference: mqttSurvivorDecidedGrant reads
// "the survivor's grant equals the retained parent's" as "the survivor decided nothing
// about this filter". Since the reachable route is an UNCONDITIONAL foreign write (see the
// same-base boundary in docs/CONFORMANCE.md), that body is whatever its author wrote, so
// the inference is about what the survivor RECORDED, never about what it intended. The
// conflict predicate is conservative in the direction that matters — anything stated
// differently is left alone and the non-repair is counted — and an author able to publish
// that body could set the state directly anyway. Stated as a residual in
// docs/CONFORMANCE.md rather than claimed as an invariant.
//
// A UNION IS WRONG, and that is the trap this file exists to avoid. The verdict proves
// that the survivor CLAIMS our parent — explicitly NOT that it erased us (sess_fork_lineage.go).
// With base P = {a}, our child X = {a,b} and a survivor Y = {} carrying an ACKNOWLEDGED
// UNSUBSCRIBE of a, a union yields {a,b} and resurrects a subscription the client was
// UNSUBACKed for. "Absent from the survivor" is ambiguous between "our addition was
// erased" and "the survivor removed a base field"; only the parent's content
// disambiguates it, which is why two snapshots are retained rather than one.
//
// NOTHING HERE WRITES A REPAIR. A repair write has four recorded rejections — it
// resurrects an incarnation a teardown deleted, it needs a byte snapshot save() discards,
// its worker races a same-broker reconnect with no ownership guard, and its CAS is exposed
// to the very window it repairs. The applier is instead the client's OWN CONNECT: the
// capsule below is re-asserted as ordinary session state immediately after the accept
// record transaction opens, and the accept save's existing CAS is what persists it. So
// the repair inherits that write's conditional arbitration, its rejection classification
// and its re-drive, and adds no window of its own.
//
// Everything here is MEMORY-ONLY: no persisted field, no encoding change, no new stream or
// subject. A restart loses a capsule, which forfeits a repair and breaks nothing.
//
// See docs/ARCHITECTURE.md and the bounds recorded in docs/CONFORMANCE.md.

package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// mqttGrantBodyMax bounds the record bytes ONE session retains for the repair.
//
// Two bodies per resident session is the whole memory cost of this feature, and it is bounded
// rather than trusted: a session whose record exceeds this forgoes the repair instead of
// doubling its footprint. The degradation is honest — a nil body means nothing is repairable,
// the same state a restored-but-never-resaved session is in — and it is stated as a residual
// in docs/CONFORMANCE.md. 64 KiB is far above any ordinary session record and far below a
// size at which retaining two copies per session matters.
const mqttGrantBodyMax = 64 * 1024

// mqttForkedGrantMax bounds the capsules one account manager holds.
//
// The ANCHOR bounds harm — a capsule whose survivor is no longer current is refused
// permanently, because record identities are fresh NUIDs and never recur — and this bounds
// MEMORY. Two ordinary outcomes leave a capsule behind: a client that never reconnects
// here, and a client whose reconnect MERGED, because a merge deliberately does not consume
// the capsule (a re-driven CONNECT must be able to merge again). There is deliberately no
// TTL: an old but still-anchored capsule is a correct repair, so a TTL would only add a
// second reason to refuse one.
const mqttForkedGrantMax = 256

// mqttSidState is one sid's EXCLUSIVE durable state. At most one field is set: that
// exclusivity is the invariant stageDrain and adoptDrainLocked maintain (unsub_drain.go),
// and modelling it as one value is what stops the merge producing a sid that is both live
// and draining.
type mqttSidState struct {
	con   *ConsumerConfig     // a live delivery consumer
	drain *mqttPersistedDrain // draining after an acknowledged membership removal
}

// mqttSidShape is the sid's grant CONSEQUENCE, and it has two values on purpose.
//
// It deliberately does not distinguish "live consumer" from "no consumer": an absent
// consumer identity beside present membership is the resume reconciler's absent-identity
// case (sess_resume_reconcile.go), a server-side condition with no MQTT grant decision
// behind it, and reading it as a conflict would let a reaped-and-recreated durable suppress
// an otherwise correct repair. Only a DRAIN's presence is a grant consequence, because only
// an acknowledged UNSUBSCRIBE creates one.
type mqttSidShape byte

const (
	mqttSidNotDraining mqttSidShape = iota
	mqttSidDraining
)

func (s mqttSidState) shape() mqttSidShape {
	if s.drain != nil {
		return mqttSidDraining
	}
	return mqttSidNotDraining
}

// mqttSidKind is the three-value form the APPLY needs, and it is a different question from
// shape(): the transition table has to tell "install a consumer" from "do nothing", which
// the grant predicate must not.
type mqttSidKind byte

const (
	mqttSidAbsent mqttSidKind = iota
	mqttSidLive
	mqttSidDrainingKind
)

func (s mqttSidState) kind() mqttSidKind {
	switch {
	case s.drain != nil:
		return mqttSidDrainingKind
	case s.con != nil:
		return mqttSidLive
	default:
		return mqttSidAbsent
	}
}

// mqttGrantTerms is the ACKNOWLEDGED half of one filter's grant: membership and the v5
// options. All scalars, so it is comparable by == and a snapshot taken from a record this
// broker marshalled equals one taken from a record it decoded — no JSON round-trip
// canonicalization question arises. QoS, No Local, Retain As Published and the Subscription
// Identifier are ONE grant and are compared together, because merging them independently
// resurrects stale options.
type mqttGrantTerms struct {
	present  bool
	qos      byte
	noLocal  bool
	rap      bool
	subID    int
	hasSubID bool
}

// mqttGrantEntry is one plain filter's whole persisted grant: the acknowledged terms plus
// the per-sid durable state. A multi-level filter has TWO independent identities — the main
// sid and the level-up companion — which the resume reconciler collects and verifies
// separately, so both live here and a removal must delete both.
type mqttGrantEntry struct {
	mqttGrantTerms
	sids map[string]mqttSidState
}

// terms reads the acknowledged half through a possibly-absent entry: a filter that appears
// nowhere in the record has the zero terms, which is what makes an addition and a removal
// the same comparison.
func (e *mqttGrantEntry) terms() mqttGrantTerms {
	if e == nil {
		return mqttGrantTerms{}
	}
	return e.mqttGrantTerms
}

func (e *mqttGrantEntry) sidState(sid string) mqttSidState {
	if e == nil {
		return mqttSidState{}
	}
	return e.sids[sid]
}

// mqttGrantSnap is the grant of ONE session record: the per-filter map plus the two
// session-scoped members, so the capsule's whole payload has a single provenance.
//
// Shared-subscription membership is deliberately absent. A per-filter shared restore can
// fail while the membership stays durable, so a re-asserted SharedSubs membership would be
// while a per-filter restore failure leaves no group join and no consumer — the
// accepted-but-dead state F-050 already records. Repairing into that ordering would extend
// an open finding rather than respect it; shared membership becomes repairable when F-050
// closes. See docs/CONFORMANCE.md.
type mqttGrantSnap struct {
	filters   map[string]mqttGrantEntry
	regNonces map[string]struct{}
	pubRel    *ConsumerConfig
}

// mqttFilterForSid maps a persisted sid back to the membership key it belongs to.
//
// mqttUnsubSids derives sids FROM a filter and is forward-only, which is not enough here: a
// record can persist a level-up companion's drain after the filter itself is gone, and such
// a drain must attach to its filter's entry rather than become an orphan the merge cannot
// see. This is buildPersistedSessionLocked's own reversal, reused rather than restated so
// the two cannot drift.
func mqttFilterForSid(sid string) string {
	if strings.HasSuffix(sid, mqttMultiLevelSidSuffix) {
		return strings.TrimSuffix(sid, mqttMultiLevelSidSuffix) + ".>"
	}
	return sid
}

// mqttGrantFromRecord builds the grant snapshot of a persisted record.
//
// THE RECORD IS THE ONLY SOURCE. buildPersistedSessionLocked returns sess.subs, sess.subIDs
// and sess.sharedSubs BY REFERENCE and shares *ConsumerConfig pointers, so a snapshot built
// from memory would alias state that keeps changing and could capture another writer's
// staged, not-yet-committed mutation — the hazard the record fence exists for. Everything
// kept here is copied and every consumer config cloned.
//
// Sourcing from the record also gets one rule for free: buildPersistedSessionLocked omits a
// drain that claims NOTHING, deliberately, so finishDrainIfEmpty can delete its durable
// with no further write. A record-sourced snapshot therefore never carries a zero-claim
// drain, and no reclaim can re-assert a reference to a durable whose deletion was already
// licensed. A memory-sourced one would.
func mqttGrantFromRecord(ps *mqttPersistedSession) *mqttGrantSnap {
	if ps == nil {
		return nil
	}
	snap := &mqttGrantSnap{pubRel: mqttCloneConsumerConfig(ps.PubRel)}
	// update reads a filter's entry, hands it to fn, and writes it back. mqttGrantEntry is
	// a VALUE in the map, so every mutation goes through here — a caller holding a copy and
	// forgetting to store it would silently drop half a grant.
	update := func(filter string, fn func(e *mqttGrantEntry)) {
		if snap.filters == nil {
			snap.filters = make(map[string]mqttGrantEntry, len(ps.Subs)+len(ps.Cons))
		}
		e := snap.filters[filter]
		fn(&e)
		snap.filters[filter] = e
	}
	updateSid := func(sid string, fn func(st *mqttSidState)) {
		update(mqttFilterForSid(sid), func(e *mqttGrantEntry) {
			if e.sids == nil {
				e.sids = make(map[string]mqttSidState, 1)
			}
			st := e.sids[sid]
			fn(&st)
			e.sids[sid] = st
		})
	}

	for filter, qos := range ps.Subs {
		update(filter, func(e *mqttGrantEntry) { e.present, e.qos = true, qos })
	}
	for _, filter := range ps.NoLocal {
		update(filter, func(e *mqttGrantEntry) { e.noLocal = true })
	}
	for _, filter := range ps.RetainAsPublished {
		update(filter, func(e *mqttGrantEntry) { e.rap = true })
	}
	for filter, id := range ps.SubIDs {
		update(filter, func(e *mqttGrantEntry) { e.subID, e.hasSubID = id, true })
	}
	for sid, cc := range ps.Cons {
		if cc == nil {
			continue
		}
		updateSid(sid, func(st *mqttSidState) { st.con = mqttCloneConsumerConfig(cc) })
	}
	for sid, pd := range ps.Drains {
		if pd == nil || pd.Cons == nil {
			continue
		}
		// EXCLUSIVE: a sid the record names in both maps is a shape the writer cannot
		// produce (stageDrain removes cons[sid] in the same transaction that adds the
		// drain), and the drain is the grant consequence, so it wins and the stray live
		// config is dropped rather than carried into a state that is both.
		updateSid(sid, func(st *mqttSidState) { st.con, st.drain = nil, mqttClonePersistedDrain(pd) })
	}
	if len(ps.VoidedRegs) > 0 {
		snap.regNonces = make(map[string]struct{}, len(ps.VoidedRegs))
		for _, n := range ps.VoidedRegs {
			if n != _EMPTY_ {
				snap.regNonces[n] = struct{}{}
			}
		}
	}
	return snap
}

// mqttCloneConsumerConfig deep-copies a consumer config, reference-typed fields included: a
// shallow copy would leave the snapshot aliasing slices and maps the live config shares.
func mqttCloneConsumerConfig(cc *ConsumerConfig) *ConsumerConfig {
	if cc == nil {
		return nil
	}
	out := *cc
	if cc.BackOff != nil {
		out.BackOff = append([]time.Duration(nil), cc.BackOff...)
	}
	if cc.FilterSubjects != nil {
		out.FilterSubjects = append([]string(nil), cc.FilterSubjects...)
	}
	if cc.PriorityGroups != nil {
		out.PriorityGroups = append([]string(nil), cc.PriorityGroups...)
	}
	if cc.Metadata != nil {
		out.Metadata = make(map[string]string, len(cc.Metadata))
		for k, v := range cc.Metadata {
			out.Metadata[k] = v
		}
	}
	if cc.OptStartTime != nil {
		t := *cc.OptStartTime
		out.OptStartTime = &t
	}
	if cc.PauseUntil != nil {
		t := *cc.PauseUntil
		out.PauseUntil = &t
	}
	return &out
}

// mqttClonePersistedDrain deep-copies a persisted drain, its consumer config and its
// started set included.
func mqttClonePersistedDrain(pd *mqttPersistedDrain) *mqttPersistedDrain {
	if pd == nil {
		return nil
	}
	out := *pd
	out.Cons = mqttCloneConsumerConfig(pd.Cons)
	if pd.Started != nil {
		out.Started = append([]mqttPersistedDrainSeq(nil), pd.Started...)
	}
	return &out
}

// mqttPersistedDrainsEqual compares two persisted drains.
//
// The started set is compared ORDER-INDEPENDENTLY, keyed by Seq, over every
// mqttPersistedDrainSeq field: startedSeqs builds its slice by ranging a map
// (unsub_drain.go), so slice order carries no meaning and a positional comparison would
// report differences that do not exist. Every field of an entry is persisted protocol state
// — PubRel/PI are the QoS 2 second phase's intent, Attempt its store generation, RegSeq the
// in-flight register the drain owes a release to — so "same sequences" is not "same drain".
//
// The consumer config is compared by DURABLE only, which is the same line the grant
// predicate draws: the durable is the drain's identity, inseparable from its started
// sequences, while the rest of the config is implementation state a reconciliation may have
// rewritten with no MQTT decision behind it.
func mqttPersistedDrainsEqual(a, b *mqttPersistedDrain) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Subject != b.Subject || a.QoS != b.QoS || a.NoLocal != b.NoLocal ||
		a.RAP != b.RAP || a.SubID != b.SubID {
		return false
	}
	if mqttDrainDurable(a) != mqttDrainDurable(b) {
		return false
	}
	if len(a.Started) != len(b.Started) {
		return false
	}
	if len(a.Started) == 0 {
		return true
	}
	bySeq := make(map[uint64]mqttPersistedDrainSeq, len(b.Started))
	for _, s := range b.Started {
		bySeq[s.Seq] = s
	}
	for _, s := range a.Started {
		o, ok := bySeq[s.Seq]
		if !ok || o != s {
			return false
		}
	}
	return true
}

// mqttForkConDurable is the durable of a sid's live consumer, or empty.
func mqttForkConDurable(s mqttSidState) string {
	if s.con == nil {
		return _EMPTY_
	}
	return s.con.Durable
}

func mqttDrainDurable(pd *mqttPersistedDrain) string {
	if pd == nil || pd.Cons == nil {
		return _EMPTY_
	}
	return pd.Cons.Durable
}

// mqttSidsOf collects the union of two entries' sids.
func mqttSidsOf(a, b *mqttGrantEntry) map[string]struct{} {
	var out map[string]struct{}
	for _, e := range []*mqttGrantEntry{a, b} {
		if e == nil {
			continue
		}
		for sid := range e.sids {
			if out == nil {
				out = make(map[string]struct{}, len(e.sids))
			}
			out[sid] = struct{}{}
		}
	}
	return out
}

// mqttGrantEntriesDiffer is the DELTA test: does our record's grant for this filter differ
// from the base's?
//
// Membership, any compared option, and a sid's DRAIN CONTENT are differences. A consumer
// identity's presence or contents alone is NOT: that is implementation state the resume
// reconciliation owns, and treating it as a delta would mint a capsule every time a reaped
// durable was recreated.
func mqttGrantEntriesDiffer(base, ours *mqttGrantEntry) bool {
	if base.terms() != ours.terms() {
		return true
	}
	for sid := range mqttSidsOf(base, ours) {
		if !mqttPersistedDrainsEqual(base.sidState(sid).drain, ours.sidState(sid).drain) {
			return true
		}
	}
	return false
}

// mqttSurvivorDecidedGrant reports whether the survivor made its OWN grant decision about
// this filter, which is the one thing that forbids re-asserting ours.
//
// It is the whole conflict predicate: membership, the options, and each sid's drain
// presence. A change applies only when this is false — only when the survivor's grant for
// the filter equals the parent's, so nothing the survivor acknowledged is being overwritten.
// Where it is true the survivor wins, because it is the record that is actually durable and
// nothing orders the two acknowledgements against each other; the non-repair is counted and
// logged rather than guessed at.
func mqttSurvivorDecidedGrant(survivor, base *mqttGrantEntry) bool {
	if survivor.terms() != base.terms() {
		return true
	}
	for sid := range mqttSidsOf(survivor, base) {
		if survivor.sidState(sid).shape() != base.sidState(sid).shape() {
			return true
		}
	}
	return false
}

// mqttGrantChange is one filter's three-way merge input. A nil pointer is "absent", which
// is what makes an addition, a removal and a modification the same operation over different
// rows of one table.
type mqttGrantChange struct {
	filter string
	base   *mqttGrantEntry
	ours   *mqttGrantEntry
}

// mqttForkedGrant is the CANDIDATE change — our acknowledged body against our retained
// baseline, computed without reading the survivor — held in memory for this client's own
// reconnect to re-assert through the ordinary accept save.
type mqttForkedGrant struct {
	// gen is the incarnation the change belongs to. clear() re-mints it, so a capsule
	// from a retired incarnation can never match — which is what stops this resurrecting
	// an incarnation a teardown already deleted.
	gen string
	// ackWID / ackPWID is the pair the forked verdict was reached against: our own
	// record's identity and the parent the survivor also claims.
	ackWID, ackPWID string
	changes         []mqttGrantChange
	// regNonces are the void-register verdicts our record held and the survivor's
	// does not. Merged by UNION, because the set is a monotonically accumulating record
	// of fact rather than a linear state.
	regNonces []string
	// pubRel is the PUBREL consumer. A QoS 2 SUBSCRIBE creates it, so a repaired QoS 2
	// addition without it would produce a record carrying QoS 2 membership and no PUBREL
	// infrastructure. Installed only where the survivor has NONE, and never compared:
	// the durable name is deterministic per generation but the config around it is not —
	// AckWait, MaxAckPending, storage and inactivity come independently from server
	// options — so two branches of one generation can hold different configs for one
	// durable. Survivor-wins-if-present is a rule about presence only.
	pubRel   *ConsumerConfig
	mintedAt time.Time
}

// isEmpty reports a delta that repairs nothing. All THREE members are tested: they are
// independent persisted fields, so testing `changes` alone would throw away a
// session-scoped-only delta — a lost void-register verdict, or a lost PUBREL consumer
// with no subscription change beside it.
func (g *mqttForkedGrant) isEmpty() bool {
	return len(g.changes) == 0 && len(g.regNonces) == 0 && g.pubRel == nil
}

// buildForkedGrantLocked computes the delta between the record this broker was acknowledged
// for and its retained parent baseline. That is a CANDIDATE set, not the state the
// survivor lacks: this function never reads the survivor, which may carry the same change
// or a conflicting one. mergeForkedGrantLocked is where the survivor is compared and a
// candidate becomes an applied change or a counted skip.
//
// A nil parent snapshot means NOTHING IS REPAIRABLE and nothing is minted: the record was
// restored from the stream, which has no known parent content here. The first record after
// a teardown re-mint no longer reaches this question at all — the write-lineage audit made
// it PARENTLESS, so lineage concedes before a repair is considered — and its protection
// remains the teardown claim protocol rather than this repair. The invariant is "a nil
// parent means unrepairable", never "grantSnapParent always names sess.pwid".
//
// sess.mu must NOT be held.
func (sess *mqttSession) buildForkedGrantLocked(ackWID, ackPWID string) *mqttForkedGrant {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	// Materialised HERE, on the fork path, from the retained bytes — never on the save that
	// produced them.
	ours, base := mqttGrantFromBody(sess.grantBody), mqttGrantFromBody(sess.grantBodyParent)
	if ours == nil || base == nil {
		return nil
	}
	g := &mqttForkedGrant{
		gen: sess.gen, ackWID: ackWID, ackPWID: ackPWID, mintedAt: time.Now(),
	}
	seen := make(map[string]struct{}, len(ours.filters)+len(base.filters))
	for _, m := range []map[string]mqttGrantEntry{base.filters, ours.filters} {
		for filter := range m {
			if _, dup := seen[filter]; dup {
				continue
			}
			seen[filter] = struct{}{}
			b, hasB := base.filters[filter]
			o, hasO := ours.filters[filter]
			var bp, op *mqttGrantEntry
			if hasB {
				bp = &b
			}
			if hasO {
				op = &o
			}
			if mqttGrantEntriesDiffer(bp, op) {
				g.changes = append(g.changes, mqttGrantChange{filter: filter, base: bp, ours: op})
			}
		}
	}
	for n := range ours.regNonces {
		if _, had := base.regNonces[n]; !had {
			g.regNonces = append(g.regNonces, n)
		}
	}
	if ours.pubRel != nil && base.pubRel == nil {
		g.pubRel = mqttCloneConsumerConfig(ours.pubRel)
	}
	if g.isEmpty() {
		return nil
	}
	return g
}

// mintForkedGrant records the CANDIDATE delta our acknowledged record carried relative to
// its retained baseline. Whether the survivor lacks any of it is decided later, at merge.
//
// Called at the point the verdict is MADE rather than where it is reported, which puts it
// inside the record transaction that classified: every disposition arm returns after its
// transaction is released, so a mint there could not sample the record and the pair under one
// fence. Six of the seven sites use this form, and it takes as.mu itself.
//
// An empty delta mints nothing, which is what replaces the site-specific refusal an earlier
// design had: a rollback of a CONNECT that reclaimed nothing produces no capsule because
// grant terms are not carried fields, so no site has to know whether it might be undoing a
// repair.
//
// Neither sess.mu nor as.mu may be held.
func (sess *mqttSession) mintForkedGrant(ackWID, ackPWID string) {
	as := sess.asm
	if as == nil {
		return
	}
	g := sess.buildForkedGrantLocked(ackWID, ackPWID)
	if g == nil {
		return
	}
	as.mu.Lock()
	as.storeForkedGrantLocked(sess, g)
	as.mu.Unlock()
}

// mintForkedGrantLocked is mintForkedGrant for the one site that already holds as.mu for
// its whole decision (evictDecideOnce). Two named functions rather than one that guesses,
// because taking as.mu there would deadlock.
//
// as.mu must be held; sess.mu must NOT be held.
func (as *mqttAccountSessionManager) mintForkedGrantLocked(sess *mqttSession, ackWID, ackPWID string) {
	if g := sess.buildForkedGrantLocked(ackWID, ackPWID); g != nil {
		as.storeForkedGrantLocked(sess, g)
	}
}

// as.mu must be held.
func (as *mqttAccountSessionManager) storeForkedGrantLocked(sess *mqttSession, g *mqttForkedGrant) {
	// Counted because the final state cannot reveal a SECOND store: every one replaces
	// as.forkedGrants[id] with a fresh pointer and mintedAt, so "a usable capsule exists"
	// is satisfied by one mint and by two alike, and a double mint means the record and
	// the acknowledged pair were sampled twice under one fence. Per ACCOUNT MANAGER rather
	// than package-global, so parallel tests cannot read each other's mints.
	//
	// It counts STORES, not calls: mintForkedGrant stores nothing when the candidate delta
	// is empty, so a fixture asserting on it must construct a real delta first or it is
	// comparing zero to zero.
	as.forkedGrantStores.Add(1)
	if as.forkedGrants == nil {
		as.forkedGrants = make(map[string]*mqttForkedGrant, 1)
	}
	// A later verdict for one client SUPERSEDES the earlier capsule rather than
	// accumulating beside it: the newer one is anchored to the record that is now current,
	// and the older one's anchor can never match again.
	as.forkedGrants[sess.id] = g
	for len(as.forkedGrants) > mqttForkedGrantMax {
		var oldest string
		var at time.Time
		for id, e := range as.forkedGrants {
			if oldest == _EMPTY_ || e.mintedAt.Before(at) {
				oldest, at = id, e.mintedAt
			}
		}
		delete(as.forkedGrants, oldest)
		as.jsa.c.Debugf("MQTT: discarding the forked-record repair of session %q: %d capsules held",
			oldest, mqttForkedGrantMax)
	}
	as.jsa.c.Debugf("MQTT: recorded a forked-record repair for session %q (%d filters, %d register verdicts)",
		sess.id, len(g.changes), len(g.regNonces))
}

// adoptGrantBodyLocked rotates the pair: the record bytes just proven durable become the
// base, and the bytes they replaced become that base's parent.
//
// It retains BYTES rather than a built grant, and that is what keeps this feature off the hot
// path: the body is already marshalled by the save that is landing, so a rotation is two slice
// assignments and no allocation, where building the grant here cost O(grant size) under
// sess.mu on EVERY save attempt including rejected ones and retries. The grant is materialised
// only on the rare fork path, from bytes that are immutable by construction — json.Marshal
// hands out a fresh buffer per call and mqttFrameRecordWrite copies into its own, so nothing
// ever writes through these slices again.
//
// Every site that adopts a landed record's SEQUENCE rotates here too, because the three are
// one fact — the record now at that sequence is the one carrying this identity and these
// bytes. A site that moved the sequence while leaving the bytes stale would compute the next
// fork's delta against a record two generations back and re-assert state the client never
// held.
//
// sess.mu must be held.
func (sess *mqttSession) adoptGrantBodyLocked(body []byte) {
	if len(body) > mqttGrantBodyMax {
		// Over the bound: retain nothing and make the session unrepairable rather than
		// doubling its record footprint. Both are cleared, because a parent whose child is
		// unknown computes no delta either.
		sess.grantBody, sess.grantBodyParent = nil, nil
		return
	}
	sess.grantBody, sess.grantBodyParent = body, sess.grantBody
}

// setRestoredGrantBodyLocked adopts the bytes of a record this broker did NOT author.
//
// The parent is nil, and that is the honest answer rather than an omission: this broker has
// no copy of what the restored record's own parent contained, so no delta is computable and
// a fork on the first authored write mints nothing.
//
// sess.mu must be held, or the session must not yet be reachable.
func (sess *mqttSession) setRestoredGrantBodyLocked(body []byte) {
	if len(body) > mqttGrantBodyMax {
		sess.grantBody, sess.grantBodyParent = nil, nil
		return
	}
	sess.grantBody, sess.grantBodyParent = body, nil
}

// mqttGrantFromBody materialises the grant of a retained record body. nil for absent or
// undecodable bytes, which concedes: an unrepairable session is the safe outcome.
func mqttGrantFromBody(body []byte) *mqttGrantSnap {
	if len(body) == 0 {
		return nil
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(body, ps); err != nil {
		return nil
	}
	return mqttGrantFromRecord(ps)
}

// mqttForkAnchor is what the capsule's anchor establishes about the record now current.
type mqttForkAnchor uint8

const (
	// mqttForkAnchorDrop: the capsule is stale. Any write since the verdict makes the
	// current record a CHILD of the survivor rather than its sibling, and a re-minted
	// generation makes it another incarnation's entirely.
	mqttForkAnchorDrop mqttForkAnchor = iota
	// mqttForkAnchorKeep: this object has not re-read the record, so it still carries the
	// capsule's own base. Merging would be wrong (there is nothing to merge against) and
	// dropping would discard a capsule that is still valid — its accept save CASes at a
	// sequence the record has moved past, is definitely rejected, and T2 re-drives to the
	// authoritative restore, whose pass carries the survivor's pair and merges.
	mqttForkAnchorKeep
	// mqttForkAnchorMerge: the record now current is still the body claiming our parent, in
	// the same incarnation.
	mqttForkAnchorMerge
)

// anchorForkedGrantLocked RE-EVALUATES the repair's licence instead of remembering it.
//
// The lineage is read from the session's own adopted pair, which after a restore IS the
// survivor's, so no record has to be threaded out of the restore into the CONNECT — and
// none of the classifiers returns the survivor record anyway, they all return an enum.
//
// The generation conjunct is separately load-bearing because classifyRecordLineage
// deliberately IGNORES the generation (a recorded reversal). It no longer has the
// clean-start sibling to catch — the write-lineage audit made the post-re-mint write
// parentless, so that pair concedes before it reaches here — but the conjunct is what
// keeps the repair from re-asserting state into an incarnation a teardown destroyed,
// and nothing else states that. Without it, any future lineage match across a re-mint
// is a resurrection.
//
// What the anchor does NOT do is arbitrate ownership of the re-asserted state. Only the
// accept save's expected-sequence CAS does that, and a CAS cannot exclude an
// UNCONDITIONAL foreign write by construction — see the same-base boundary in
// docs/CONFORMANCE.md, which is also the route this repair exists for. This is a
// staleness and eligibility guard over an ordinary write.
//
// Note this consult has NO sequence gate, unlike the other classifier call sites: it
// compares the in-memory pair after its own WID and generation gates. A capsule retained
// across a live stream recreation or an external restore can therefore be anchored
// against a restored survivor whose sequences restarted.
//
// sess.mu must be held.
func (sess *mqttSession) anchorForkedGrantLocked(g *mqttForkedGrant) mqttForkAnchor {
	switch {
	case sess.wid == g.ackWID:
		// g.ackWID is never empty — classifyRecordLineage requires it to be non-empty to
		// report forked at all — so this cannot match a session that holds no record.
		return mqttForkAnchorKeep
	case sess.gen == g.gen &&
		classifyRecordLineage(&mqttPersistedSession{WID: sess.wid, PWID: sess.pwid},
			g.ackWID, g.ackPWID) == mqttLineageForked:
		return mqttForkAnchorMerge
	default:
		return mqttForkAnchorDrop
	}
}

// mqttTestForkRepairMergeHookV fires immediately before the reclaim takes sess.mu to
// merge, which is what lets a test fix the acquisition order against the record fence
// instead of racing it. nil in production.
var mqttTestForkRepairMergeHookV atomic.Pointer[func(sess *mqttSession)]

func setMQTTTestForkRepairMergeHook(h func(sess *mqttSession)) func() {
	prev := mqttTestForkRepairMergeHookV.Load()
	if h == nil {
		mqttTestForkRepairMergeHookV.Store(nil)
	} else {
		mqttTestForkRepairMergeHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestForkRepairMergeHookV.Store(prev) })
}

// reassertForkedGrantInto re-asserts the candidates the merge ACCEPTED — those the
// survivor was found not to have decided differently — into a session that has
// just restored the surviving record.
//
// It runs from mqttProcessConnect IMMEDIATELY AFTER the accept record transaction opens and
// before that block's own mutations, because a transaction that starts after the mutation it
// protects cannot make that mutation invisible to another writer. The sessLocked ownership
// gate narrows concurrency but is not a recordTx and does not substitute for one. That
// placement also makes one call serve both entry states — a session restored this pass and a
// resident one — and puts the merge immediately before the save that persists it.
//
// The capsule is NOT consumed on merge. Within this function the only thing that removes it
// is the anchor refusing it, and the accept save landing is what makes the anchor refuse
// (the record's identity becomes ours). So a CONNECT that merges and then fails forfeits
// nothing, and a re-drive that re-enters the restore simply re-merges — idempotently,
// because the merge is a function of the survivor's content.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) reassertForkedGrantInto(sess *mqttSession, tok recordTx) error {
	// Every pass starts unfenced: a fence is this pass's statement about its own reads.
	sess.mu.Lock()
	sess.forkFence, sess.forkNotServing = 0, nil
	sess.mu.Unlock()

	// Look up WITHOUT removing: the T2 re-drive, a failed CONNECT's retry and the keep arm
	// below all depend on the entry outliving one pass.
	as.mu.Lock()
	g := as.forkedGrants[sess.id]
	as.mu.Unlock()
	if g == nil {
		return nil
	}

	if h := mqttTestForkRepairMergeHookV.Load(); h != nil {
		(*h)(sess)
	}

	// NO DECIDED CONSUMER IS REINSTATED (F-105 7f). The capsule is this broker's own erased
	// branch, and a consumer it names may since have been decided by another broker's tenure —
	// the survivor's settlement disposes of a consumer the survivor's record does not name. A
	// decided consumer reinstated into the accepted record would then need a lifecycle of its
	// own; instead it never enters it. The authority sequence is sampled FIRST, every consumer
	// the merge could reinstate is read SECOND, and the accept's own handoff preparation CASes
	// that sample: a disposal is an authority write, so one landing after these reads refuses the
	// accept and the re-driven pass reads again. Every read completes before anything is merged.
	notServing, fence, perr := as.forkRepairPreflight(sess, g)
	if perr != nil {
		return perr
	}

	sess.mu.Lock()
	anchor := sess.anchorForkedGrantLocked(g)
	var tally mqttForkRepairTally
	var survivorUnreadable bool
	if anchor == mqttForkAnchorMerge {
		// The fence must already be held by the caller, and by THIS token: the merge
		// mutates subs, the options, cons, the drains and the nonce set.
		if err := sess.validateRecordTxLocked(tok); err != nil {
			sess.mu.Unlock()
			sess.jsa.c.Errorf("MQTT: refusing to repair the forked record of session %q: %v", sess.id, err)
			return nil
		}
		// The SURVIVOR's grant must be materialisable, or the merge is refused for THIS
		// PASS. The anchor consults only lineage, so it still licenses a merge when the
		// restore retained no body — an oversize record, or bytes that stopped decoding —
		// and a nil survivor grant would make every conflict invisible: filters the survivor
		// decided about would read as absent-therefore-unconflicted, and the losing branch
		// would overwrite grant decisions the survivor actually made. That is survivor-wins
		// inverted, so this fails CLOSED: no per-filter fallback, no partial merge.
		//
		// The capsule is RETAINED, not discarded, and that is a correction the review's
		// third round made to its own second: "retrying cannot succeed" assumed this pass's
		// accept save lands, but a rejected accept save T2-re-drives to a NEW record — and
		// a second, smaller sibling of the same parent still satisfies the anchor while
		// being materialisable, so the repair can still apply on that pass. Once an accept
		// save does land the record becomes ours and the anchor drops the capsule on the
		// next pass; until then the cap bounds it, like every other retained capsule.
		if survivor := mqttGrantFromBody(sess.grantBody); survivor == nil {
			survivorUnreadable = true
		} else {
			sess.forkNotServing = notServing
			tally = sess.mergeForkedGrantLocked(g, survivor)
			sess.forkNotServing = nil
			sess.forkFence = fence
		}
	}
	sess.mu.Unlock()

	if survivorUnreadable {
		sess.jsa.c.Warnf("MQTT: forgoing the forked-record repair of session %q this pass: the "+
			"surviving record's grant is not materialisable (oversize or undecodable), so "+
			"conflicts cannot be seen and nothing may be re-asserted over it; the capsule is "+
			"retained for a re-driven pass against a readable sibling", sess.id)
		return nil
	}

	switch anchor {
	case mqttForkAnchorDrop:
		// Only on drop, and only under POINTER IDENTITY, so a capsule minted by a newer
		// verdict between the lookup and here is not deleted by this pass's stale decision.
		as.mu.Lock()
		if as.forkedGrants[sess.id] == g {
			delete(as.forkedGrants, sess.id)
		}
		as.mu.Unlock()
	case mqttForkAnchorKeep:
		sess.jsa.c.Debugf("MQTT: the forked-record repair of session %q is still on its own base; "+
			"deferring to the authoritative restore", sess.id)
	case mqttForkAnchorMerge:
		sess.jsa.c.Warnf("MQTT: repaired the forked session record of %q: re-asserted %v, "+
			"left to the surviving record %v, %d register verdicts, pubrel-consumer=%v",
			sess.id, tally.applied, tally.conflicts, tally.nonces, tally.pubRel)
	}
	return nil
}

// forkRepairPreflight reads, for every consumer the coming merge would REINSTATE from our branch,
// whether it still serves, and returns the ones that do not with the authority sequence sampled
// before those reads (zero when no authority stands: a creation-arm accept, under which no
// disposal can exist, prepares no handoff). An unanswerable read fails the pass before anything is
// merged — the CONNECT is refused before its accept, and the capsule is kept for the retry.
//
// The reinstating rows are exactly the ones applyGrantSidLocked installs OUR consumer on: the
// survivor has no consumer for the sid and ours is live or draining, or the survivor's is live and
// ours is draining. The survivor's own consumers are its record's, named, and not read here.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) forkRepairPreflight(sess *mqttSession, g *mqttForkedGrant) (map[string]struct{}, uint64, error) {
	sess.mu.Lock()
	var names []string
	if sess.anchorForkedGrantLocked(g) == mqttForkAnchorMerge {
		if survivor := mqttGrantFromBody(sess.grantBody); survivor != nil {
			for _, ch := range g.changes {
				var sv *mqttGrantEntry
				if e, ok := survivor.filters[ch.filter]; ok {
					sv = &e
				}
				for sid := range mqttSidsOf(sv, ch.ours) {
					names = append(names, mqttForkReinstatedDurable(sv.sidState(sid), ch.ours.sidState(sid))...)
				}
			}
		}
	}
	gen := sess.gen
	sess.mu.Unlock()
	if len(names) == 0 {
		return nil, 0, nil
	}
	jsa := sess.jsa
	epoch := as.sessStreamEpoch
	// THE SAMPLE PRECEDES THE READS. A sequence sampled after them could already include a
	// disposal a read before it missed.
	var fence uint64
	switch load := jsa.loadOutboundAuthority(sess.id, epoch); load.status {
	case mqttOutboundValid:
		fence = load.seq
	case mqttOutboundAbsent:
	default:
		return nil, 0, fmt.Errorf("repairing the forked record of %q: its outbound authority reads %v, so "+
			"whether the consumers it would reinstate still serve cannot be fenced", sess.id, load.status)
	}
	if h := mqttTestForkPreflightReadHookV.Load(); h != nil {
		(*h)(sess.id)
	}
	var notServing map[string]struct{}
	for _, name := range names {
		serving, err := jsa.sourceConsumerServing(sess.id, epoch, gen, name)
		if h := mqttTestForkPreflightClassifiedV.Load(); h != nil {
			(*h)(name, serving, err)
		}
		if err != nil {
			return nil, 0, fmt.Errorf("repairing the forked record of %q: %w", sess.id, err)
		}
		if !serving {
			if notServing == nil {
				notServing = make(map[string]struct{})
			}
			notServing[name] = struct{}{}
		}
	}
	return notServing, fence, nil
}

// mqttForkReinstatedDurable names the consumer of OURS the per-sid transition would install, if
// any. It mirrors applyGrantSidLocked's table, which is what keeps the preflight's reads and the
// merge's installs one set.
func mqttForkReinstatedDurable(survivor, ours mqttSidState) []string {
	switch survivor.kind() {
	case mqttSidAbsent:
		switch ours.kind() {
		case mqttSidLive:
			if ours.con != nil && ours.con.Durable != _EMPTY_ {
				return []string{ours.con.Durable}
			}
		case mqttSidDrainingKind:
			if d := mqttDrainDurable(ours.drain); d != _EMPTY_ {
				return []string{d}
			}
		}
	case mqttSidLive:
		if ours.kind() == mqttSidDrainingKind {
			if d := mqttDrainDurable(ours.drain); d != _EMPTY_ {
				return []string{d}
			}
		}
	}
	return nil
}

// sourceConsumerServing reports whether a source consumer name still SERVES generation gen: its
// ownership is LIVE, held by this client at gen, and the resource it names is a source consumer,
// LIVE, with no disposal decision. An absent ownership is not serving — every source consumer is
// claimed before it is created, and nothing may be admitted against an unclaimed one. A read that
// cannot be answered is an error, never an answer (F-105 7f).
func (jsa *mqttJSA) sourceConsumerServing(id, epoch, gen, name string) (bool, error) {
	own, _, oerr := jsa.loadNameOwnershipPhysical(epoch, mqttOwnedStreamMsgs, name)
	if oerr != nil {
		return false, oerr
	}
	if own == nil || own.ID != id || own.OwnerGen != gen || own.State != mqttNameOwnLive {
		return false, nil
	}
	r, _, rerr := jsa.loadOutboundResource(id, epoch, gen, own.OwnerInc)
	if rerr != nil {
		return false, fmt.Errorf("the resource of consumer %q could not be read: %w", name, rerr)
	}
	if r == nil || r.Kind != mqttResourceSourceConsumer || r.State != mqttResourceLive || r.Disposal != nil {
		return false, nil
	}
	return true, nil
}

// mqttTestForkPreflightReadHookV runs between the preflight's authority sample and its consumer
// reads. nil in production.
var mqttTestForkPreflightReadHookV atomic.Pointer[func(id string)]

// mqttTestForkPreflightClassifiedV observes each preflight read. nil in production.
var mqttTestForkPreflightClassifiedV atomic.Pointer[func(name string, serving bool, err error)]

// mqttForkRepairTally is what one merge actually did, for the log line: a non-repair is
// COUNTED rather than silently guessed at.
type mqttForkRepairTally struct {
	applied   []string
	conflicts []string
	nonces    int
	pubRel    bool
}

// mergeForkedGrantLocked applies the capsule's delta against the survivor's grant.
//
// The survivor grant is MATERIALISED BY THE CALLER and passed in, never defaulted: a nil
// grant here would make every conflict invisible and invert survivor-wins, which is why the
// caller refuses the whole merge when the retained body cannot produce one. It comes from
// the restored record's bytes, and on this arm that is necessarily the SURVIVOR's: the merge
// anchor requires the session's adopted pair to name a sibling of the capsule's parent, and
// no local save can produce such a pair (a later local save is {C, ackWID}, whose parent is
// not ackPWID). The only adoption that reads a foreign record is the restore, so the grant
// describes the survivor and the object was built from it this pass — which is also why no
// consumer or drain here is CHARGED, and why the merge never has to transfer consCharged
// ownership.
//
// sess.mu must be held, under an open record transaction.
func (sess *mqttSession) mergeForkedGrantLocked(g *mqttForkedGrant, survivor *mqttGrantSnap) mqttForkRepairTally {
	var tally mqttForkRepairTally
	survivorFilters := survivor.filters
	for _, ch := range g.changes {
		var survivor *mqttGrantEntry
		if e, ok := survivorFilters[ch.filter]; ok {
			survivor = &e
		}
		if mqttSurvivorDecidedGrant(survivor, ch.base) {
			tally.conflicts = append(tally.conflicts, ch.filter)
			continue
		}
		if !sess.applyGrantChangeLocked(ch, survivor) {
			tally.conflicts = append(tally.conflicts, ch.filter)
			continue
		}
		tally.applied = append(tally.applied, ch.filter)
	}
	// Union, and through voidRegNonceLocked rather than by assigning the map: the restore
	// asserts the restored set is already durable (voidedRegsRev = 0), so a merged nonce
	// must be marked NOT durable. That helper bumps voidedRegsRev and stamps the nonce with
	// its own insertion revision, which is what makes "this verdict is durable" provable
	// against voidedRegsSavedRev — so credit-every-landed-save persists it with the accept
	// save. Assigning the map instead would leave it unsaved forever.
	for _, n := range g.regNonces {
		if _, have := sess.voidedRegNonces[n]; have {
			continue
		}
		sess.voidRegNonceLocked(n)
		tally.nonces++
	}
	if g.pubRel != nil && sess.pubRelConsumer == nil {
		sess.pubRelConsumer = mqttCloneConsumerConfig(g.pubRel)
		tally.pubRel = true
	}
	return tally
}

// applyGrantChangeLocked makes one filter's grant equal to ours, and reports whether every
// sid was applied.
//
// The refusal is PER SID, not per filter, and that is not a half-measure. What it rests on
// is that the two things happen at different granularities: MEMBERSHIP AND THE OPTIONS are
// applied for the whole filter, while DRAIN CONTENT is reconciled per sid and can refuse.
// A refusal therefore leaves the applied membership standing and the survivor's drain
// contents preserved — it does not, and need not, mean no membership change accompanied it.
//
// Two earlier statements here were WRONG and are named so they do not come back. "A
// drained sid has no membership on either side" is false: a drained sid can carry a QoS 0
// membership, which is what a downgrade that converts its consumer leaves. Scoping that to
// the refused row is also false, and a review round supplied the counterexample — a base
// with a QoS 0 membership and a drain holding two claims, our branch unsubscribing (dropping
// membership, keeping the drain) while the survivor completes one claim and keeps
// membership. mqttSurvivorDecidedGrant permits the repair, because membership, options and
// drain PRESENCE all still equal the base; this function then applies the membership change
// before the per-sid step refuses the differing started sets. So that row really can
// accompany a membership change, and the reason refusing one sid is safe is the granularity
// above rather than an absence of membership.
//
// A multi-level filter's two sids drain and complete independently, so refusing one must not
// withdraw the other.
//
// The rule has two halves that come from different sides. This runs at MERGE, so the
// survivor has already been compared and this candidate survived that comparison. The
// GRANT becomes ours — membership, the options, and each sid's drain presence — because
// that is the acknowledged decision the survivor was found not to have decided
// differently. The CONSUMER IDENTITY stays the survivor's wherever the
// resulting state is LIVE, because identity is never a grant decision (that is the conflict
// predicate's whole premise) and the reconciliation resolves the durable and deliver subject
// from whichever config the session holds.
//
// A resulting DRAIN always keeps its OWN persisted Cons. A drain's durable is not
// interchangeable with another's: drainByDur is keyed by it, in-flight registers correlate
// through the durable they were created under, drain recovery queries that durable's
// consumer state, and restoration subscribes to — and completion deletes — that same
// durable. Re-pointing our drain's Cons at a survivor's live durable would evaluate our
// started sequences against another consumer's ack state, orphan our registers and
// eventually delete the wrong durable.
//
// Nothing here deletes a DURABLE, on any row. An unreferenced consumer is the orphan
// sweep's business, and proving a delete here is what a late delete must never do.
//
// sess.mu must be held.
func (sess *mqttSession) applyGrantChangeLocked(ch mqttGrantChange, survivor *mqttGrantEntry) bool {
	sids := mqttSidsOf(survivor, ch.ours)

	// A HELD drain refuses the WHOLE FILTER, checked BEFORE any membership, option,
	// consumer or drain is touched — the same property processSubs applies, at the second
	// caller of it.
	//
	// This was a fail-open path a review round found, and the shape is worth naming: the
	// quarantine was enforced where the FIRST caller of adoptDrainLocked lives, and this is
	// the second. Repair mutated sess.subs and the option maps first, then adopted per sid
	// and DISCARDED adoptDrainLocked's refusal — so for a wildcard with a usable main drain
	// and a quarantined companion it reported the filter applied, having adopted the main
	// drain while the companion refused. The accept save then persists the live membership
	// and consumer but no longer the adopted drain's Started obligations. That is exactly
	// the partial adoption the processSubs preflight exists to prevent, reached through the
	// other door.
	//
	// THE TWO HOLDS REFUSE DIFFERENT ROWS, and F-094's review rounds cost two revisions to
	// get this right. An unusable identity refuses EVERY row, unchanged: nothing may act on
	// it and nothing may take its place, so even a drop is refused. A past-PUBREC intent
	// refuses only the rows that DESTROY the drain, which against the transition table below
	// is exactly `live` (adoption erases it) and `absent` (the drop erases it); the two
	// draining->draining rows preserve the survivor's drain either way and are left alone.
	//
	// The `absent` row is NOT exempt, and an earlier revision's reasoning that it is — "our
	// record's omission IS the acknowledged completion" — is refuted by this file's own
	// terms. mqttSurvivorDecidedGrant compares drain PRESENCE via shape(), never the
	// sequences or their intents; the header above disclaims that the survivor descended
	// from our baseline at all; and our omission can equally come from recoverDrains pruning
	// a source it proved absent rather than from any completion. So the omission proves
	// nothing about the survivor's obligations, and dropping its maps destroys evidence.
	//
	// Refusing the whole change is the conservative direction, and its cost is stated in
	// docs/CONFORMANCE.md rather than claimed away: the merge counts this filter as a
	// conflict and completes, so the refusal is NOT retried once the accept save lands.
	for sid := range sids {
		switch sess.drainSidHoldLocked(sid) {
		case mqttDrainHeldUnusableIdentity:
			return false
		case mqttDrainHeldPastPubRecIntent:
			switch ch.ours.sidState(sid).kind() {
			case mqttSidLive, mqttSidAbsent:
				return false
			}
		}
	}

	terms := ch.ours.terms()
	if terms.present {
		if sess.subs == nil {
			sess.subs = make(map[string]byte, 1)
		}
		sess.subs[ch.filter] = terms.qos
	} else {
		delete(sess.subs, ch.filter)
	}
	sess.setFilterFlagLocked(&sess.noLocalSubs, ch.filter, terms.present && terms.noLocal)
	sess.setFilterFlagLocked(&sess.rapSubs, ch.filter, terms.present && terms.rap)
	if terms.present && terms.hasSubID {
		if sess.subIDs == nil {
			sess.subIDs = make(map[string]int, 1)
		}
		sess.subIDs[ch.filter] = terms.subID
	} else {
		delete(sess.subIDs, ch.filter)
	}

	// EVERY sid, not just the main one. A removal that dropped only the main sid would
	// leave a stale cons entry: the next record omits it (buildPersistedSessionLocked drops
	// a consumer whose filter is absent), so it is invisible durably — but it stays in
	// memory, and processJSConsumer's existing-consumer arm then returns on a later
	// re-SUBSCRIBE without rebuilding delivery interest, so that filter silently stops
	// delivering.
	applied := true
	for sid := range sids {
		if !sess.applyGrantSidLocked(sid, survivor.sidState(sid), ch.ours.sidState(sid)) {
			applied = false
		}
	}
	return applied
}

// applyGrantSidLocked is the per-sid transition table: ten actions over the nine state pairs,
// draining -> draining splitting on drain equality. It returns false only for the one row that
// is refused.
//
// sess.mu must be held.
func (sess *mqttSession) applyGrantSidLocked(sid string, survivor, ours mqttSidState) bool {
	// A CONSUMER OF OURS THAT NO LONGER SERVES IS NEVER REINSTATED (F-105 7f; read by
	// forkRepairPreflight before this merge, fenced by the accept's preparation). A membership
	// keeps its grant WITHOUT the consumer — the KEY is omitted, never a nil value — and the resume
	// reconcile provisions a fresh one, exactly as for a membership with no consumer identity. A
	// drain is omitted, identity and contents: the stated residual in docs/CONFORMANCE.md.
	_, oursConDecided := sess.forkNotServing[mqttForkConDurable(ours)]
	_, oursDrainDecided := sess.forkNotServing[mqttDrainDurable(ours.drain)]
	switch survivor.kind() {
	case mqttSidAbsent:
		switch ours.kind() {
		case mqttSidLive:
			if oursConDecided {
				break
			}
			if sess.cons == nil {
				sess.cons = make(map[string]*ConsumerConfig, 1)
			}
			sess.cons[sid] = mqttCloneConsumerConfig(ours.con)
		case mqttSidDrainingKind:
			if oursDrainDecided {
				break
			}
			sess.restoreDrainLocked(sid, mqttDrainFromPersisted(mqttClonePersistedDrain(ours.drain)))
		}
	case mqttSidLive:
		switch ours.kind() {
		case mqttSidAbsent:
			delete(sess.cons, sid)
		case mqttSidDrainingKind:
			// Our drain arrives INTACT and the survivor's displaced live consumer is left
			// to the orphan sweep. Its durable is not ours to delete and not ours to
			// borrow.
			delete(sess.cons, sid)
			if oursDrainDecided {
				break
			}
			sess.restoreDrainLocked(sid, mqttDrainFromPersisted(mqttClonePersistedDrain(ours.drain)))
		case mqttSidLive:
			// Nothing: the survivor's consumer config is PRESERVED, never replaced with
			// ours. Stated explicitly rather than by omission — installing ours would
			// repoint delivery at a durable the survivor may have replaced.
		}
	case mqttSidDrainingKind:
		switch ours.kind() {
		case mqttSidLive:
			// The re-SUBSCRIBE-over-a-drain case. adoptDrainLocked is a pure sess.mu
			// transition that moves drains[sid] to cons[sid] keeping the durable; written
			// by hand, cons and drains end up both populated. It does not transfer the
			// started obligations, which the later durable scans re-establish.
			//
			// Its return is deliberately NOT treated as a refusal, and getting that wrong
			// is instructive. adoptDrainLocked returns false for TWO different facts:
			// "quarantined" and "there is no drain here to adopt". The second is benign and
			// reachable on this row — the SURVIVOR's record says draining, which says
			// nothing about whether our own resume restored a drain for that sid — so
			// refusing the whole change on it would abandon a repair for a state that is
			// merely absent. The quarantine is handled by the filter-wide preflight above,
			// before anything is mutated, which is where a refusal can still be honoured
			// atomically. A mutation confirmed the check here was unreachable.
			sess.adoptDrainLocked(sid)
		case mqttSidAbsent:
			// Our record's omission is the acknowledged completion of the last claim on OUR
			// branch, and this drops only the two map entries, never a durable.
			//
			// It is NOT evidence about the SURVIVOR's obligations, which is why a HELD sid
			// refuses this row in the preflight above: mqttSurvivorDecidedGrant compares
			// drain PRESENCE only, and our own omission can come from recoverDrains pruning
			// a source it proved absent rather than from any completion. Nor does reaching
			// here establish that our branch's durable was disposed of: retirement now
			// CONFIRMS its delete, but this row is not retirement — it drops the two map
			// entries and issues nothing — so the durable is left for the orphan sweep.
			if d := sess.drains[sid]; d != nil {
				sess.dropDrainLocked(d)
			}
		case mqttSidDrainingKind:
			if !mqttPersistedDrainsEqual(survivor.drain, ours.drain) {
				// REFUSED, and the residual is stated in docs/CONFORMANCE.md: the CLAIMS are
				// safe either way — the base was already draining, a committed drain adds no
				// fresh claims and completion only removes them, so the survivor still names
				// everything it says is owed — but a per-sequence PROGRESSION our branch
				// acknowledged and the survivor's did not is not repaired.
				return false
			}
		}
	}
	return true
}

// setFilterFlagLocked adds or removes one filter from a v5 option set, allocating and
// releasing the map the way mqttFilterSetFromList does.
//
// sess.mu must be held.
func (sess *mqttSession) setFilterFlagLocked(set *map[string]struct{}, filter string, on bool) {
	if !on {
		delete(*set, filter)
		if len(*set) == 0 {
			*set = nil
		}
		return
	}
	if *set == nil {
		*set = make(map[string]struct{}, 1)
	}
	(*set)[filter] = struct{}{}
}
