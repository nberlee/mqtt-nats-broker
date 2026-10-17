// Copyright 2020-2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// In-flight delivery types: pending entries, send holds, slot exclusions, send actions and
// their settle kinds, retained frames, and pending references.

package mqtt

type mqttPending struct {
	// retainedInc is a RETAINED exchange's payload owner (F-105 7c): the incarnation whose copy
	// the exchange depends on, carried so its terminal observations wake that copy's
	// reclamation. Set by mqttPromoteRetainedLanded and nothing else.
	retainedInc string
	// retainedReplay is a RESTORED retained PUBLISH's verified material (F-105 7c): read and
	// verified by the resume before any acquisition, so the replay never reads payload material.
	// Set by mqttRecoveredRetainedPending and mqttWithRetainedReplay, and nothing else.
	retainedReplay *mqttRetainedReplayMaterial
	sseq           uint64 // stream sequence
	jsAckSubject   string // the ACK subject to send the ack to
	jsDur          string // JS durable name
	// Persisted lifecycle metadata; it does not imply a current delivery offer.
	sourceAckReply string
	// q2AffClaim is the subject of the assignment claim that authorized this
	// delivery, set only for a session-affine shared QoS 2 copy. It travels with
	// the pending entry because the claim's affine-side completion can only be
	// recorded from the terminal paths (PUBREC, expiry, oversize discard), which
	// see the packet identifier and nothing else. See qos2_shared_affine.go.
	q2AffClaim string
	// q2AffCopy is the SUBJECT of the copy this delivery carried, OBSERVED by the
	// delivery gate rather than derived from anything. It travels with the claim
	// subject because the terminal transition must be bound to the copy it belongs to:
	// re-reading whatever claim occupies the subject at PUBREC time would credit a
	// replacement election with a delivery it never made, and a bare incarnation
	// cannot say WHICH copy a claim reference is for — so a reference naming another
	// claim's valid subject would let that claim's own copy be acknowledged away
	// unpublished (F-065).
	q2AffCopy string
	// pubRelAffRef is the affine assignment reference carried by the STORED PUBREL
	// this entry tracks, read back off the record by mqttDeliverPubRelCb rather than
	// remembered from the PUBREC. That is what makes it available on a connection that
	// did not store it — the crash window this whole mechanism exists for — so PUBCOMP
	// can refuse to acknowledge a PUBREL away while the proof it carries is unrecorded.
	// Meaningful on pendingPubRel entries only.
	pubRelAffRef mqttPubRelAffineRef

	// qos is this exchange's RECORDED QoS, and the wire QoS of every PUBLISH admitted
	// for it. Ordinarily that is the value its in-flight register holds; a RETAINED
	// send has no register at all (R6), so for one it is the QoS selected when the
	// identifier was allocated. It is NOT a claim that a transmission happened: an
	// admitted replacement donates the occupant's QoS to an allocation that may never
	// have been transmitted (storeInflightRegister's same-source arm). This broker has no
	// durable "was transmitted" fact: its one durable transmission fact is the opposite one,
	// the allocation's unsent marker, which proves the PUBLISH never went out (F-072, F-088).
	//
	// It is NOT recomputed from the subscription. A re-SUBSCRIBE may replace the
	// filter's maximum QoS at any time ([MQTT-3.8.4-3]) and must not interrupt a
	// publication already in flight: the client holds this identifier in a state fixed
	// by what it already received, so changing the QoS under it abandons a QoS 2
	// handshake (2 -> 1) or drives a PUBREC whose slot transition the CAS then refuses
	// (1 -> 2).
	//
	// Sourced ONLY from per-exchange evidence, and these three are the whole list: the
	// QoS the register actually landed holding, the register's own QoS on recovery, or
	// -- for a retained send, which has no register -- the QoS selected at identifier
	// allocation. Subscription-level and drain-level QoS are NOT evidence about an
	// individual exchange: one drain routinely holds both, since a QoS 1 publication
	// through a QoS 2 subscription is transmitted at QoS 1.
	//
	// Zero means "no per-exchange evidence", which for a transmissible entry is a
	// fail-closed withhold, not a default. It is NOT a phase or history discriminator:
	// some pendingPubRel entries are built through mqttRecoveredPending and so carry a
	// non-zero value.
	qos byte

	// regSeq is the stream sequence of this delivery's durable in-flight register on
	// $MQTT_inflight, and regNonce the register's immutable identity. Every delete
	// targets regSeq exactly, because the register's subject is reused by later
	// exchanges on the same identifier (R4). Zero/empty for a delivery with no
	// register: a retained send (no JetStream backing at all). A slot-backed exchange
	// recovered from a PUBREL record carries the whole-life owner slot's sequence here.
	//
	// regNonce is minted with the ALLOCATION, one step ahead of the store that writes
	// it, so an allocation cancelled while its store is in flight can be named durably
	// before any sequence for it exists. regSeq appears only on confirmation.
	regSeq   uint64
	regNonce string
	// kind is what this entry may DO, and ev which obligation it stands for; ev.lin is the
	// LINEAGE (see mqttInflightRecord.Lin), distinct from regNonce, which names the
	// ALLOCATION — a same-delivery replacement re-mints the nonce and may inherit the
	// lineage. Both are set only by the constructors in outbound_pending_kind.go: a promotion
	// REPLACES the entry, and an Ordinary entry exists only while custody holds ev.lin.
	kind mqttPendingKind
	ev   mqttPendingEvidence
	// allocFence is what an ALLOCATION knew about custody when its identifier was taken, carried
	// to the admission's conversions (markerOpaqueEligibleLocked). Zero on every other entry.
	allocFence mqttAllocationFence
	// demotedKind is the kind a DEMOTED entry had, which a reactivation restores (and no more).
	demotedKind mqttPendingKind
	// awaitingPubCompOwner marks a RELEASED DELIVERY: a PUBREL this connection transmitted whose
	// exchange ended (its lineage released) while the client's PUBCOMP is still owed here. It is a
	// Hold kept only so that PUBCOMP can settle the slot; the connection it names owns the wire
	// debt, and once that owner is no longer live the entry is retired
	// (retireReleasedDeliveriesLocked). Zero on every other entry.
	awaitingPubCompOwner uint64
	// custody marks an ORDINARY entry whose Receive Maximum charge the custody ledger owns:
	// set in the critical section that converts the allocation to an admission reservation, or
	// by recovery for a lineage the ledger charges, and never cleared. The capacity projection
	// excludes a custody entry by this flag alone, so a charge the ledger released cannot
	// resurrect through a scheduling entry that outlived it.
	custody bool
	// regForeign marks a reservation NO TERMINAL PATH MAY SETTLE. The identifier stays
	// reserved (the client may hold it), but nothing acts
	// on the record behind it. Only recovery installs it, over a record this object may not
	// install — unreadable, an affine pair its own source does not prove, a shared shape
	// this broker does not write, or a member of a source group recovery refused as a whole
	// (poisoned) — where settling it would destroy the evidence on the strength of the
	// state that was just refused. See inflight_register.go.
	regForeign bool
	// Recovery provenance is separate from foreign ownership. Opaque entries
	// own only occupancy; demoted entries retain their resident identity.
	regRecoveryOpaque  bool
	regRecoveryDemoted bool
	// A known recovered PUBREL has an owner nonce but no delivered slot reference.
	regRecoveryPlaceholder bool

	// transmitted is THE source of the outgoing DUP flag: a send of this entry carries DUP
	// exactly when it is set ([MQTT-3.3.1-1]). It is set under sess.mu by the section whose
	// write possibly reached the socket (endPublishSend with wrote == true), by a PUBREC
	// rollback (the client's PUBREC proves receipt), and by recovery when the durable pair
	// says so (the register present and its unsent marker absent). Nothing derives it from
	// a JetStream delivery count, and nothing else writes it. See inflight_marker.go.
	transmitted bool
	// markerSeq is the unsent marker's exact stream sequence, captured when the pair's
	// commit was followed by the authoritative load (step 3 of the store) or by recovery's
	// marker walk. Memory-only. Zero on a transmitted entry ("none held"): the marker was
	// deleted, or was never observed by this process. A zero here on an entry that is
	// neither transmitted nor marker-opaque has no send authority (beginSendLocked).
	markerSeq uint64
	// markerOpaque marks the MARKER-OPAQUE reservation: the register landed but its marker
	// could not be read as present-and-ours or as authoritatively absent, so whether the
	// PUBLISH was ever transmitted is unknown. The identifier stays reserved, nothing may be
	// sent or settled under it, no source mapping is kept, and the session's identifier
	// space is marked incomplete until a resume reads the pair whole. Declined everywhere
	// regForeign is declined.
	markerOpaque bool
	// intentUnresolved marks an entry whose CONFIRMED intent delete of the marker at
	// markerSeq returned AMBIGUOUSLY: the marker may or may not be gone. Nothing may be
	// written under the identifier until a re-attempt settles it (the next send section
	// re-issues the exact same delete — a re-attempt, never a readback), and nothing may
	// release the identifier: the durable state may already read possibly-attempted, so
	// expiry and withdrawal treat the entry as such. Memory-only: a crash inside the
	// ambiguity resolves it at recovery from whatever the marker walk finds, and both
	// answers are correct (present: proven-unsent, DUP=0; absent: possibly-attempted, DUP=1).
	intentUnresolved bool

	// terminalReconcile marks the in-memory reservation installed after PUBCOMP when
	// terminal slot cleanup could not be confirmed. The durable terminal slot remains
	// the authority; this bit only prevents duplicate retry workers for its local
	// reservation. See retainAndReconcileTerminal.
	terminalReconcile bool

	// pubCompAnswered marks a TERMINAL RESERVATION: the Hold this broker installs once it has
	// decided the exchange complete and owns its advance to pubrel_terminal. Set only by
	// mqttNewTerminalReservation, whose every installer follows the exchange's PUBCOMP — the
	// handler's conversion, an owed or recorded completion, a drain or recovery that observed
	// the terminal slot or the exchange's recorded Terminal phase — or, for the slot-only class,
	// recovery's own completion decision. No PUBREL is owed for such an exchange, so a stored
	// PUBREL of it is withheld before admission (answeredByPubComp). See docs/ARCHITECTURE.md,
	// "A PUBREL exchange decided complete is not admitted again".
	pubCompAnswered bool

	// (see the session's piSpaceIncomplete for the other half of R3)
	//
	// regPending marks an identifier that has been ALLOCATED but whose register is
	// not yet confirmed, so the PUBLISH has not been transmitted and must not be
	// (R1). Nothing else may act on the entry while it is set: a concurrent
	// redelivery declines it, resendPendingRedelivery does not NAK it, and
	// stageDrain does not enrol it. Cleared by the register store's owner on a
	// confirmed write; on a definite failure the entry is removed outright.
	regPending bool

	// regCancelled marks an entry a consumer teardown has WITHDRAWN. It stays in
	// pendingPublish, inert, until BOTH release conditions hold: the register store has
	// SETTLED (freeing the identifier earlier would let a new delivery take the subject
	// while the old CAS was still in flight, and the cancellation tombstone would then be
	// indistinguishable from that new delivery's record), and some durable rung of the
	// cleanup ladder is PROVEN (released with none, a crash could resurrect the register
	// as an ordinary delivery nothing anywhere calls void). While regPending is still set
	// the store is unsettled; the settle points drop it, and
	// releaseSettledVoidReservationsLocked frees the identifier once the verdict's save
	// lands. Holding it costs one in-flight store's round trip in the common case.
	regCancelled bool

	// sendNonce names the send section currently transmitting this entry, empty when
	// none is. It is the ENTRY-side half of the send reservation: the identifier itself
	// is held by the session's sendsInFlight registry, because a successful PUBREC
	// REPLACES this entry with a PUBREL one while the write may still be blocked, and
	// per-entry state alone would strand the identifier. What this half is for is
	// fencing the condemnation and the settle execution to one section (F-087 WP5).
	sendNonce string
	// res is this exchange's acknowledgement-resolution cell, shared with whatever send
	// section is transmitting it. It is what an acknowledgement registers against, and it
	// outlives both the section and this entry's own untracking (F-098, round 7).
	res *mqttAckResolution
	// slot is the CAPTURED durable identity of this delivery's owner slot: its subject and
	// name, the sequence it was observed at, the exchange nonce, the source and the custody.
	//
	// Captured rather than re-derived, and that is the whole point. A destructive path that
	// rebuilds the subject from the session's CURRENT idHash and generation plus a passed
	// identifier loses the snapshot boundary — it would address whatever the session is now
	// rather than the exchange this entry was about. The reference also carries the observed
	// sequence, which is the freshness fence a stale terminal continuation is checked
	// against (see reapRegisterWithVerdictGated).
	//
	// Zero for an entry with no durable slot: a retained send has none (R6).
	slot mqttInflightSlotRef

	// sendSettle is the continuation a removal path parked because a send section was
	// active. The FIRST parked settle wins: a later remover still performs its own
	// durable work, but neither replaces this disposition nor executes it early, which
	// is sound because every continuation is idempotent against another remover's
	// durable work (an exact-sequence delete's "already gone" is the existing success
	// reading). endSend executes it.
	sendSettle mqttSendSettle
}

// mqttSendHold is one packet identifier's reservation. The identifier may not be
// re-minted while the hold is LIVE, and it is live while ANY condition below holds.
// The three have different drains and behave differently across a connection change,
// which is why one hold carries all of them rather than three registries carrying one
// each.
//
// Adding a term here is NOT additive: it changes what every reader and every mutator of
// sendsInFlight means. The ones that ask sendHoldLiveLocked adapt for free
// (beginSendLocked, sendReservedLocked, sweepDeadSendHoldsLocked, endSend); the ones
// that re-spell the condition themselves do not. When the retained term arrived,
// consumePubRelAckLocked was the one that had to change — it switched to the shared
// predicate, so a PUBCOMP can no longer delete a hold carrying a live retained reservation.
// beginPubRelSendLocked was ALSO given a retained-term refusal and it was DELETED again;
// that opener's comment records why, and it must not be reinstated (F-091, D4).
type mqttSendHold struct {
	// token names the transmission whose write has not returned, empty when none is
	// out. It drains at endSend, on the transmitting goroutine, so it SURVIVES every
	// connection transition including a discard (F-087 WP5, class F2).
	token string
	// pubCompOwed counts PUBRELs POSSIBLY transmitted under this identifier whose PUBCOMP
	// has not arrived, plus the one attempt whose section is open. [MQTT-2.3.1-4] frees a
	// PUBREL's identifier when its acknowledgement is processed, and for a PUBREL that
	// acknowledgement is the PUBCOMP — not the write's return, which is only the proxy the
	// PUBLISH side must use because no acknowledgement has been asked for yet (F-089).
	//
	// The open attempt's unit is taken before the write (beginPubRelSendLocked) and given
	// back at the section's end only when its frame was PROVABLY not emitted (F-116): the
	// client answers exactly the PUBRELs it received, so a unit for one it never received
	// could never be answered and would hold the identifier for the connection's life.
	pubCompOwed int
	// relEntered says the OPEN PUBREL attempt (the one token names) has entered its write,
	// so its unit may now stand for a frame on the wire. Reset by the opener, set where
	// write entry succeeds while the token is still that attempt's, cleared with the token.
	// Read only by pubCompEmittableOwnerLocked: an attempt not entered when a lineage
	// release converts its entry can never emit, because write entry refuses the converted
	// entry (F-116).
	relEntered bool
	// owner is the CONNECTION IDENTITY those PUBRELs went out on — c.cid, minted by
	// srv.gcid.Add(1), so it is monotonic, never reused and never zero for an accepted
	// connection. A PUBCOMP is only ever processed on the session's CURRENT connection
	// (mqttProcessPubComp refuses on sess.c != c), so a debt owed to any other
	// connection can never be answered and must stop holding the identifier the moment
	// ownership moves.
	//
	// A SCALAR, deliberately, not a *client. An inert hold outlives its connection —
	// that is the whole point of retiring it by predicate rather than at a transition
	// site — and a pointer here would keep the departed client's entire object graph
	// (its connection, parse state, account and server references) reachable from a
	// resident session for as long as that session lives, which for a never-expiring
	// one is forever. The identity answers the only question asked of it.
	//
	// Read through sess.c rather than dropped at named transition sites, because the
	// sites cannot be enumerated safely: clear() never assigns sess.c at all, a
	// persistent-session disconnect never calls clear(), and the clean-start window
	// runs clear() while the displaced client is still bound. A debt dropped at clear()
	// would leak on the first and free an identifier the client still holds on the
	// third.
	owner uint64
	// retainedGen names the session INCARNATION under which a still-unwritten buffered
	// RETAINED PUBLISH allocated this identifier; empty when there is none. A retained
	// send has no JetStream backing, so it has no in-flight register (R6) and the send
	// sections the other two fields serve never covered it: it allocates in
	// serializeRetainedMsgsForSub, is buffered on sub.mqtt.prm, and is written by a
	// LATER queueOutbound. Across that span its only other reservation is the
	// pendingPublish entry, which clear() destroys — so this is what carries the
	// identifier through the discard (F-091).
	//
	// The INCARNATION rather than a bool, and that is the whole subtlety. Written
	// completion ends the reservation only while the allocating generation is still
	// current: then pendingPublish is still there to carry custody on, or a PUBACK for
	// this very frame removed it and the identifier is genuinely free. Past a clear()
	// neither is true — the map was nilled and last_pi reset — so this hold is the
	// identifier's ONLY reservation and the write returning does not free it. clear()
	// re-mints the generation and an acknowledgement does not, which is exactly the
	// discriminator, and it never errs toward releasing: no path nils pendingPublish
	// without re-minting.
	//
	// Owner-scoped like pubCompOwed, for a different reason. Once the connection is no
	// longer the session's, no ADDITIONAL identifier-bearing PUBLISH under this
	// identifier can reach that socket — every route that could build one opens a
	// reservation this session refuses on sess.c != cc — so the orphaned frame is one
	// PUBLISH under one identifier on one socket and needs no reservation at all.
	retainedGen string
	// action is the LOCAL half of the PUBREL action claim: the durable half lives on the
	// slot as ActionToken. Nil when no action is held.
	//
	// NOT owner-scoped, unlike pubCompOwed and retainedGen, and that is the whole point.
	// A durable claim on the slot outlives whoever owns the connection now: a holder that
	// acquired an action must be able to drain it after a takeover, because refusing that
	// would abandon the action and manufacture an unbounded identifier reservation out of
	// an ordinary, fully successful takeover. What a departed holder may NOT do is acquire
	// again or perform a new handoff — the predicate is POSSESSION OF THIS CONTINUATION,
	// not current custody.
	action *mqttSendAction
	// pubIdent names the PUBLISH transmission whose write has not returned, and it
	// survives the PUBREC map handoff.
	//
	// It exists because the admission check needs an EXACT live-PUBLISH-send-token
	// refusal, and "the entry's sendNonce paired with the hold's token" cannot supply one:
	// a successful PUBREC removes pendingPublish[pi] while its PUBLISH send token is still
	// live here, so the entry-side half of that pair is gone at exactly the moment the
	// check must refuse. This registry is already the thing designed to outlive its entry
	// (F-087 class F2, F-089), so the fact belongs here rather than in a map the PUBREC
	// path is entitled to mutate.
	pubIdent string
	// pubOut is the OUTPUT of the PUBLISH section pubIdent names, set and cleared with it. It is
	// how the client's acknowledgement reaches a transmission that has not begun writing: the
	// acknowledgement is processed on the read loop while the section is still spending its
	// round trips, and a PUBLISH the client has acknowledged must not then be written (F-115).
	pubOut *mqttPublishOutput
	// terminalOwed names the ACKNOWLEDGED PUBLISH exchange (its nonce) whose durable
	// terminal cleanup has not been confirmed (F-098, holistic review round 1, finding 5).
	//
	// The client's PUBACK — or a failed PUBREC — ends the exchange for the CLIENT, so its
	// Receive Maximum capacity is legitimately free and is freed at once. The IDENTIFIER is
	// not: until the slot is recorded terminal and deleted it still names this exchange, and
	// a re-minted identifier would collide with it. This term reserves exactly that, through
	// sendReservedLocked, without counting toward capacity, and the terminal continuation
	// releases it when the delete is confirmed.
	terminalOwed string
	// slotOwed is the IDENTIFIER EXCLUSION a removal site hands off when it cannot
	// confirm the durable slot released (F-098). It is the sixth liveness term.
	//
	// It exists because preserving identifier EXCLUSION is a different operation from
	// preserving the original actionable, capacity-charging ENTRY. Every removal site
	// removes exactly what it removed before — its actionability, source identity,
	// admission and capacity contracts are untouched — and additionally hands the
	// identifier's exclusion here, where bumpPI already looks through
	// sendReservedLocked. Retaining the entry instead breaks three separate contracts:
	// the drain's cancellation handoff, the real release at commitStagedReleases, and
	// registerless retained frames, which can never obtain a durable confirmation and
	// would leak their Receive Maximum charge forever.
	//
	// Like terminalOwed it reserves the IDENTIFIER ONLY and charges no capacity, so no
	// capacity semantics change anywhere. Like action it is NOT owner-scoped: an
	// obligation a peer may still be transmitting outlives whoever owns this connection,
	// and beginPubRelSendLocked carries it across a hold replacement for that reason.
	slotOwed *mqttSlotExclusion
}

// mqttSlotExclusion is the identifier exclusion a hold carries, as a CONJUNCTION of every
// restriction ever bound to it.
//
// A merge APPENDS and never rewrites or drops a bind, in BOTH directions. A named
// restriction arriving over an empty one must not make it adoptable, and an empty one
// arriving over a named one must not leave the name adoptable — either would let a later
// nonce match discharge a reservation whose occupant was never identified. So:
//
//   - ADOPTION requires a unanimous, non-empty, matching identity: any empty bind, or two
//     differing nonces, makes the exclusion permanently un-adoptable;
//   - an ABSENCE LIFT requires a confirmed absence for EVERY bound subject. One
//     constituent's absence never discharges the conjunction.
//
// Binds are a SET: an equivalent restriction is not duplicated. That bounds duplicates
// only — distinct restrictions per identifier remain unbounded by identifier count, since
// differing nonces and generation-bearing subjects retained across a discard are separate
// constituents. The bound is 65,535 map entries with storage proportional to the
// accumulated distinct restrictions, and that residual is disclosed rather than defended.
type mqttSlotExclusion struct {
	binds []mqttSlotExclusionBind
	// inst is taken from the session's monotone counter on every install or strengthen.
	// It is never reset, so it cannot repeat across a hold's deletion and recreation —
	// a per-hold counter would permit ABA, where two readers observe instance 1, one
	// lifts it, a new hold starts at 1, and the second reader lifts the NEW exclusion.
	inst uint64
	// fenceCursor is the subject of the last PENDING-FENCE step the exclusion-reclaim pass
	// attempted on THIS exclusion (F-088 WP3 rule 6 (b)). Scheduling state only: never absence
	// evidence and never read by the lift. It lives as long as this object — install and
	// reassertion mutate the object in place, so it survives bind additions and instance
	// advances — and goes with it when the exclusion is lifted or adopted.
	fenceCursor string
}

// mqttSlotExclusionBind is one restriction: the exact subject protected, and the exchange
// named, if this site could name one.
type mqttSlotExclusionBind struct {
	// subject carries the idHash, the GENERATION and the packet identifier, so an absence
	// of the current generation's subject cannot discharge an exclusion retained for an
	// older generation's — which matters precisely because exclusions survive a discard.
	subject string
	// nonce is the exchange this restriction names, or EMPTY where the site has none to
	// give. A staged recovery release (commitRecoveryReleaseLocked) creates exactly that
	// shape: a real restriction whose occupant it cannot name. Empty is representable, never
	// a refusal, and never
	// silently promoted to a name.
	nonce string
}

// mqttSendAction is one acquired PUBREL action, from before its durable CAS is issued
// until its drain is durably confirmed.
//
// ARMED BEFORE THE CAS CAN BE ISSUED, which is what closes the window between a CAS
// returning and a deferred cleanup being installed — including an ordinary panic that
// unwinds the stack. A tentative holder is installed first and its abort armed; only then
// may the request go out.
type mqttSendAction struct {
	// token is the action's identity, minted locally and written to the slot by the CAS.
	// A drain matches on this and nothing else.
	token string
	// state is the local lifecycle. Write and abort are mutually exclusive through it:
	// the write entry point atomically moves acquired -> writing, so the abort path and
	// the write can never both win.
	state mqttSendActionState
	// slot is the exchange this action is about: subject, generation, PI, nonce, source,
	// custody and canonical record identity. Captured, never a pointer into session state
	// — a pointer would silently follow the state it exists to be compared against.
	slot mqttInflightSlotRef
	// released is the LOCAL released bit, set under sess.mu before any drain begins.
	// Once set, every saved capability attempting a write is rejected. A locally released
	// but durably UNRESOLVED action still reserves its identifier: the reservation ends
	// when the durable drain is confirmed, not when the local half gives up.
	released bool
	// drained records that a reread proved this exact token is gone — resolved to
	// pubrel_ready, resolved to pubrel_terminal, replaced by a DIFFERENT valid token, or
	// the slot absent. The last two matter: without them a finite ambiguous drain retries
	// forever, which would contradict the convergence this lifecycle claims.
	drained bool
	// pubCompAnswered records that the client's PUBCOMP completed this action's exchange while
	// the action was live and the PUBCOMP handed it the terminal cleanup
	// (answerLocalActionLocked). Set under sess.mu BEFORE the terminal reservation is released,
	// so the reservation's refusal and this one overlap: the action may still drain and finish
	// the cleanup, but a write it has not entered is refused at the bind and at write entry
	// [MQTT-4.3.3]. A write already entered is the stated retransmission.
	pubCompAnswered bool

	// THE ACQUISITION'S F-105 IDENTITY (row 2), in two tiers. exec (the acquisition's execution
	// cell, bound to the ORIGINAL connection) and lin (the obligation lineage, from the fresh read) are fixed when the holder
	// is created, before any store I/O, so a drain that later proves the token ours — with or
	// without a client, as the shutdown pass is — can still record its abandonment on the
	// connection it was acquired for. outAction is the exact action record an ORDINARY acquisition
	// landed through the authority, published under sess.mu in the section that moves the holder
	// tentative -> acquired, and never on an adoption. Nil for a slot-only holder, which has no
	// authority record (the experimental affine class, chapter P).
	exec      *mqttExecCell
	lin       string
	outAction *mqttOutboundAction
}

// abandonProven records the holder's acquisition at counter acq — proven this holder's by a read
// that found its exact token — as never to execute on its connection. A consumed one is left alone.
func (act *mqttSendAction) abandonProven(acq uint64) {
	if act == nil || act.exec == nil || act.lin == _EMPTY_ || acq == 0 {
		return
	}
	act.exec.abandon(mqttActionKey{gen: act.slot.name.gen, nonce: act.lin, acq: acq})
}

// mqttSendActionState is the local half of the action lifecycle.
type mqttSendActionState int

const (
	// mqttSendActionTentative: installed and armed, CAS not yet resolved. The holder is
	// responsible for cleanup until it is.
	mqttSendActionTentative mqttSendActionState = iota
	// mqttSendActionAcquired: the CAS landed. The opener accepts THIS holder and binds
	// its send capability to the durable token already minted, rather than minting a
	// second identity.
	mqttSendActionAcquired
	// mqttSendActionWriting: the socket write is in progress. Entering this state is what
	// charges the PUBCOMP debt, so a panic between the opener and the write cannot create
	// debt no client can answer.
	mqttSendActionWriting
	// mqttSendActionReleased: no write will follow. The durable drain may still be
	// unresolved, and the identifier stays reserved until it is.
	mqttSendActionReleased
)

// mqttRetainedFrame is one buffered retained PUBLISH and the reservation that holds its
// packet identifier.
//
// The frame is FRAMED at SUBSCRIBE time, under the session lock, and written by a later
// queueOutbound — either the SUBACK tail's flush or the first live delivery to this
// subscription. The identifier is carried alongside because that later writer is what
// discharges the reservation, and because a departing subscription has to release the
// reservations of frames nobody will ever write.
//
// A QoS 0 retained frame allocates no identifier and carries pi == 0. That is the same
// exclusion R6 makes for every QoS 0 send: nothing is reserved, so nothing is released.
type mqttRetainedFrame struct {
	header []byte
	msg    []byte
	pi     uint16
	// nonce is the ALLOCATION this frame belongs to, copied from the entry
	// trackPublishRetained just minted. It travels with the frame because the frame's
	// disposition is what releases the entry, and by then a packet identifier is not
	// enough to name the exchange: clear() removes the entry and re-mints last_pi, so a
	// disposition arriving afterwards would otherwise untrack whatever exchange now holds
	// the identifier. That is the race F-091's reservation exists for, and the nonce is
	// what makes the release identity-keyed rather than identifier-keyed (F-101).
	nonce string
	// qos is the effective QoS; for QoS 1/2, mat is the immutable SNAPSHOT the driver
	// materializes and transmits (F-105 7c), and ent the entitlement the SUBSCRIBE was
	// accepted under, compared with — never adopted from — the standing authority.
	qos byte
	mat *mqttRetainedMaterial
	ent mqttOutboundEntitlement
}

// mqttSendSettleKind names the continuation a removal path owes an entry whose send
// section is still active. It is a KIND, not a boolean, because the paths that condemn
// an entry mid-send owe materially different work: an acknowledgement still owes the
// register's delete, a teardown's ladder already issued it, and a recovery pass has
// already made every durable decision it is going to make.
type mqttSendSettleKind uint8

const (
	// mqttSendSettleNone: nothing parked; the section ends by simply clearing.
	mqttSendSettleNone mqttSendSettleKind = iota
	// mqttSendSettleAckComplete: a PUBACK (or PUBCOMP-side completion) arrived while
	// the write was in flight. The protocol answer already went out; what is deferred
	// is the identifier-freeing untrack and the register's delete.
	mqttSendSettleAckComplete
	// mqttSendSettlePubRecFailed: a PUBREC whose PUBREL store failed, which frees the
	// identifier exactly as a terminal acknowledgement does.
	mqttSendSettlePubRecFailed
	// mqttSendSettlePurgeTeardown: a consumer teardown condemned the entry. Its ladder
	// owns the register's durable disposition, so only the memory untrack is deferred.
	mqttSendSettlePurgeTeardown
	// mqttSendSettleDenyRelease: a subscribe denial released the identifier; its own
	// register delete is deferred with the untrack.
	mqttSendSettleDenyRelease
	// mqttSendSettleRecoveryDrop: a recovery apply dropped the entry. The pass already
	// issued the durable deletes it owed, so only the memory untrack is deferred.
	mqttSendSettleRecoveryDrop
)

// mqttSendSettleOwesExclusion says whether a deferred settlement must hand the
// IDENTIFIER's exclusion to the hold before its entry is untracked (F-098, C-IV).
//
// The split is between settlements whose durable half is CONFIRMED by the time the entry
// goes and settlements whose durable half may outlive it. The two acknowledgement kinds
// are not here: each holds the identifier through its own terminal cleanup, which is a
// stronger claim than an exclusion and carries the cleanup obligation with it, so adding
// one would install a second owner for the same identifier.
//
// The other three are here because each one's register delete can be refused by a
// freshness fence, fail outright, or never have been issued at all — "issued or
// deliberately declined" is what those paths promise, and a declined delete leaves the
// durable pair standing. Without the exclusion the memory untrack is the moment the
// identifier becomes allocatable again, over a slot nobody has confirmed gone.
//
// mqttSendSettlePurgeTeardown is included because the enum arm is retained and its
// handler is live, NOT because any production caller currently parks it — no caller does,
// and this is deliberately not evidence of a purge schedule.
func mqttSendSettleOwesExclusion(k mqttSendSettleKind) bool {
	switch k {
	case mqttSendSettleDenyRelease, mqttSendSettleRecoveryDrop, mqttSendSettlePurgeTeardown:
		return true
	}
	return false
}

// mqttSendSettle is one parked continuation.
type mqttSendSettle struct {
	kind   mqttSendSettleKind
	regSeq uint64 // the register this continuation deletes, when it owns that delete
	nonce  string // the condemned allocation's identity, so the delete can name its verdict
}

// mqttPendingRedeliverable reports whether forcing a redelivery of this entry can
// achieve anything. Three states say it cannot, and a NAK for any of them would be
// answered by a trackPublish that declines the offer:
//
//   - regPending: allocated but not yet durable, so never transmitted (R1), and the
//     identifier may still be released if the store fails;
//   - regForeign: a recovery reservation whose record this object may not interpret,
//     which nothing here may transmit or settle;
//   - an active send section: the payload is already on its way out on a write that has
//     not returned (F-087 WP5, class E).
func mqttPendingRedeliverable(ack *mqttPending) bool {
	return ack.acts() && !ack.regPending && !ack.regForeign && !ack.markerOpaque && ack.sendNonce == _EMPTY_
}

// mqttPendingRef identifies one in-flight QoS 1/2 delivery. A packet identifier
// alone does not: identifiers are released and reused, so the same number can
// name a different delivery a moment later. The (durable, stream sequence) it
// was delivered from is what makes it the same delivery.
type mqttPendingRef struct {
	pi    uint16
	sseq  uint64
	jsDur string
}

// mqttPendingRefs snapshots the identities currently tracked in a pending map.
// Returns nil for an empty map, which reads the same as "nothing was pending".
//
// Session lock held on entry.
func mqttPendingRefs(pending map[uint16]*mqttPending) map[mqttPendingRef]struct{} {
	if len(pending) == 0 {
		return nil
	}
	refs := make(map[mqttPendingRef]struct{}, len(pending))
	for pi, ack := range pending {
		refs[mqttPendingRef{pi, ack.sseq, ack.jsDur}] = struct{}{}
	}
	return refs
}

// mqttDeferredKey identifies a JS message delivery withheld by outbound flow
// control: the consumer durable it came from and its stream sequence.
type mqttDeferredKey struct {
	jsDur string
	sseq  uint64
}
