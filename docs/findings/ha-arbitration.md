# HA arbitration and write-lineage findings

These findings share one architectural dependency: JetStream per-subject CAS is a
linearization point only within a leader term. Treat them as one design epic before
scheduling stream-specific work.

Multi-frontend HA is supported alpha scope (decided 2026-08-10, `docs/ROADMAP.md`
section 0.2), so every finding here is an unconditional alpha blocker.

The one arbitration design these findings share was made on 2026-08-11 and is sliced
in dependency order: Slice A (the observation and convergence backbone) landed first
and NARROWED F-036 without closing it, F-029 is Slice B and closed on 2026-08-11
leaving F-029a and F-029b behind, and F-027 is Slice C, closed on 2026-08-12. Each
remaining slice gets its own
implementation-depth plan against that design; the roadmap bullets carry the
constraints each must answer.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10.

## F-027

**Title:** Repair a recognised session-record fork

- Status: **Closed 2026-08-12** (Slice C), for the recoverable subset; every bound is
  recorded in `docs/CONFORMANCE.md`, and shared-subscription membership stays excluded
  until F-050 closes
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 27

### Original finding record

- [ ] A proven fork is recognised but never REPAIRED: the surviving sibling stays and the
  rightful owner's granted state is lost. A naive repair was designed and rejected — it
  would resurrect an incarnation a teardown had already deleted (`mqtt.go`, "A CLAIMED
  teardown must be FINISHED, never resumed and never overwritten"), it needs a
  last-durable byte snapshot that does not exist (`save()` discards
  `mqttSessSaveDetail`), its worker races a same-broker reconnect with no ownership guard,
  and the repair CAS is itself exposed to the window it repairs. Any future attempt has to
  answer all four.

### What closed it

A fork's durable loss is exactly the record DELTA. The anchor condition
`classifyRecordLineage` tests means the survivor and the erased record are siblings of ONE
NAMED parent, so that parent is a single-hop common ancestor and a three-way merge against
it is well defined — which is what a union cannot be: with base `{a}`, ours `{a,b}` and a
survivor `{}` carrying an acknowledged UNSUBSCRIBE of `a`, a union resurrects `a`, because
"absent from the survivor" is ambiguous between a lost addition and a survivor-side
removal.

Two grant snapshots, both built from the persisted RECORD rather than from memory, rotate
at every adoption that can later mint. A forked verdict at any of seven classification
points mints an in-memory capsule of the delta, inside the transaction that classified.
The client's own reconnect re-asserts it through the ordinary accept-save CAS, behind the
record fence and strictly after the CONNECT's uncertainty resolution.

That answers the four rejections rather than waiving them: generation-anchored, so a
retired incarnation can never be resurrected; a semantic delta rather than a byte replay,
so no snapshot `save()` discards is needed; no worker and no repair write, so nothing
races a same-broker reconnect; and the write IS the accept save, so it inherits F-029's
conditional arbitration, rejection classification and re-drive instead of opening a window
of its own.

The repair claim is: an acknowledged SUBSCRIBE or UNSUBSCRIBE a proven fork erased is
re-asserted **where the survivor made no grant decision about the same filter**. It is NOT
that every recognised loss is recovered — a conflict is left to the survivor and counted,
a restart forfeits the capsule, and the cardinality bound forfeits the oldest client's.
See "Repairing a recognised fork" in `docs/ARCHITECTURE.md`.

## F-029

**Title:** Arbitrate simultaneous takeovers from one session-record base

- Status: **Closed 2026-08-11** (Slice B), with two shapes explicitly left open and
  filed as their own `docs/ROADMAP.md` §0.2 blockers — see F-029a and F-029b below.
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 29

### What closed it

Every session-record write is conditional, base 0 included, so a first-ever create is
CASed on "the subject must currently be empty" instead of landing on top of whatever is
there. A superseded teardown claim is a typed outcome rather than success, so the path
that produced the unconditional create no longer runs; four callers are terminal on it
and two re-drive. `finishAdoptedTeardown`'s superseded release is surfaced rather than
swallowed — the one production path that wrote on a base it had authoritatively been
told was stale. The lineage chain crosses the teardown re-mint (`reMintedFrom`), and the
incarnation generation no longer short-circuits the fork test, because a clean-start
sibling always carries a different generation and that short-circuit hid exactly the
fork the rule exists to catch. See "Two frontends taking over one session from the same
base" in `docs/ARCHITECTURE.md`.

The convergence claim is: every interleaving converges on one owner, and each client is
told something true. It is NOT that neither side destroys what the arbitration awarded
the other — that is F-029a.

## F-029a

**Title:** The claim-won deletion window

- Status: **Closed** on 2026-08-12 by the generation-retirement watermark
  (`internal/mqtt/sess_teardown.go`, `$MQTT_teardown`; `docs/ARCHITECTURE.md`, "The
  generation-retirement watermark").
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Filed: 2026-08-11, out of F-029's implementation

A teardown that WINS its claim deletes the retiring generation's consumers and durables
before its release tells it whether it still owns the claim, so a cross-term resume off
the same base can survive with a record naming state that is already gone. Pre-existing:
a property of the teardown protocol, not something F-029 introduced. Reordering release
before delete was designed and refuted — `releaseTeardown` proves absence rather than
authorship, the unsafe interval merely moves, shared group durables are not
generation-scoped, and after the release the durable target list is gone while PUBREL
durables have no orphan reaper. The roadmap bullet carries the full refutation.

**How it was closed.** The refutation above says what the fix had to be: a durable
teardown intent that does not live on the replaceable record subject. Every teardown that
wins its claim now records a permanent per-client retirement watermark on a dedicated
`$MQTT_teardown` stream — naming the generation and the `$MQTT_sess` sequence its claim
marker landed at — before its first deletion, always confirmed, and fails the teardown
closed with nothing deleted if it cannot. Three readers make the window recognisable and
repair it: the winner's own release-superseded arm condemns a same-generation survivor
with a fresh teardown marker (mark only, so the survivor's broker can still observe the
move); the CONNECT restore refuses to resume a retired generation and routes the record
through the existing overdue-discard shape instead; and the orphan sweep condemns one
with no CONNECT at all, deferring the finish to a later pass. Replacement is ordered by
the marker sequence alone, nothing ever reaps a watermark, and the stream is epoch-coupled
to `$MQTT_sess` so a partial recreation of the unit is refused at startup. The repair is
truthful TERMINATION, never restoration: the deleted state stays deleted, and what is
guaranteed is that no client is served as though it existed. The stacked apply-time
escapes that remain — the fence names only the latest retirement, and the slot itself can
be regressed by a cross-term apply — are residuals in `docs/CONFORMANCE.md`, strictly
narrower than the parent CAS hole they come from.

## F-029b

**Title:** Cross-term create × create recognition

- Status: Open
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Filed: 2026-08-11, out of F-029's implementation

Two brokers creating from an empty subject across a leader term both apply; the later
survives and the erased side concedes 0x8E, correct about ownership and wrong about
cause. Both records are parentless, and the rule requires a non-empty acknowledged
parent precisely so two unrelated first-creates do not read as siblings. A `bound`-gated
parentless arm was designed and refuted: its premise is false, and a FALSE fork verdict
suppresses delayed-Will cancellation, so this is not a vocabulary-only gap. The roadmap
bullet carries the full refutation.

### Original finding record

- [ ] Takeover versus takeover at one base is unordered. With the persist observation now
  classifying, both sides WITHHOLD the claim on a recognisable (resume-shaped) fork — but
  nothing orders or arbitrates the two evictions themselves, a clean-start side's
  re-minted generation still concedes 0x8E, and a lost observation decides nothing.
  Converging the two brokers on one owner needs the arbitration this item's parent bullet
  could not provide.

## F-030

**Title:** Add write lineage to the Will outbox

- Status: Open
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 30

### Original finding record

- [ ] `$MQTT_willq` carries no write lineage, so a create-only enqueue erased by a
  same-base sibling across a leader transition is unrecognisable. The Will it named is
  then either lost outright — nothing re-enqueues it, and the enqueuing incarnation has
  moved on — or, if the surviving sibling holds an earlier state, re-fired after `done`
  was supposed to have retired it. The record shape and the classifier from
  `sess_fork_lineage.go` are reusable; this stream's CONSEQUENCE analysis is not.

## F-031

**Title:** Add write lineage to Will-cancellation watermarks

- Status: Open
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 31

### Original finding record

- [ ] `$MQTT_willcancel` carries no write lineage. Its per-client watermark is monotonic
  by design, so a surviving sibling that holds a LOWER `SessionSeq` silently un-suppresses
  a Will an accepted CONNECT had already cancelled — the one failure this stream exists to
  prevent. Worse than the others because the marker is permanent: nothing deletes it, so
  the lowered value stands until the next accepted CONNECT advances it again.

## F-032

**Title:** Add write lineage to inbound QoS 2 holds and release claims

- Status: Open
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 32

### Original finding record

- [ ] `$MQTT_qos2in` carries no write lineage, on both its hold subjects and their `.rel`
  siblings. A hold claim erased mid-exchange lets a second broker claim the same inbound
  QoS 2 message, which is the duplicate delivery [MQTT-4.3.3] forbids; a `.rel` claim
  erased between PUBREL and fan-out loses the release, stalling the exchange until the
  orphan sweep reaps it. The two subject families have different consequences and need
  naming separately even though one bullet covers the stream.

## F-033

**Title:** Add write lineage to shared QoS 2 assignment claims

- Status: Open
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 33

### Original finding record

- [ ] `$MQTT_qos2assign` carries no write lineage. An affine claim erased by a sibling
  leaves contenders waiting on a `claimed` record whose owner never materialises, which
  stalls the shared-group delivery of that message indefinitely — the same stall the
  generation-scoped naming was introduced to make impossible.

## F-034

**Title:** Add write lineage to outbound in-flight registers

- Status: Open
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 34

### Original finding record

- [ ] `$MQTT_inflight` carries no write lineage. A register erased by a sibling frees a
  packet identifier that is still in flight (so a later delivery can collide with it) or,
  in the other direction, leaves a register whose deletion was proven, reserving an
  identifier and a Receive Maximum slot for the life of the session.

## F-036

**Title:** Revalidate ownership for otherwise quiet resident sessions

- Status: Open — NARROWED, not resolved. Slice A landed on 2026-08-11 (the
  session-record watch plus its committed-read backstop,
  `internal/mqtt/sess_record_watch.go`; `docs/ARCHITECTURE.md`, "The session-record
  watch"): ownership changes only by a record write, so the watch observes every one
  of them, a quiet session still costs no write per interval, and every MOVED record
  now converges — bound sessions included, through the eviction worker itself. What
  remains is the DELETED record on a BOUND session: the backstop arms the ownership
  marker, and the record-rejection settlement that answers it is packet-driven for a
  bound connection, so a Keep Alive 0 client that goes silent still holds a session
  another broker tore down. That is this finding's own failure shape, so the finding
  stays open until a bound-session disposal exists that does not wait for client
  traffic. Residuals in `docs/CONFORMANCE.md`.

  **A second route into the same shape arrived with F-029a's fix (2026-08-12), and any
  fix here must revalidate against it.** A condemnation marker written onto a bound
  hollow record converges as MOVED, but whoever FINISHES that marker deletes the record —
  and if the survivor's broker dropped the marker observation, it is then facing an
  absent record under a bound, silent Keep Alive 0 client: this finding's own shape. Two
  variants, deliberately different: a CONNECT that adopts the marker finishes it
  IMMEDIATELY and cannot be deferred, and the orphan sweep defers its finish by a pass
  (which narrows the case it owns to a survivor whose serial committed-read backstop does
  not reach the record within the widened window — the unbounded-walk residual). Neither
  is claimed to be closed by the deferral; both are listed here so this finding's
  eventual fix tests them alongside the pre-existing route.
- Classification: HA arbitration architecture
- Release treatment: Alpha blocker
- Original roadmap order: 36

### Original finding record

- [ ] Investigate: a resident session NEVER re-validates its ownership absent a local
  rejection, so a remote takeover whose persist notification is lost still strands the
  object when this broker made no failing write. The rejection settlement above bounds
  convergence only for rejection-caused staleness: its marker is armed by the store
  boundary, so a session that simply never writes again is never looked at. The expiry
  sweep cannot cover it either (it skips never-expire outright, and defers to a resident
  object for the rest). Any fix has to arbitrate rather than read — the same 10164
  in-process argument applies — and a periodic unconditional probe of every resident
  session is a write per session per interval, which is why this is filed rather than
  folded in. Found while planning the rejection settlement.
