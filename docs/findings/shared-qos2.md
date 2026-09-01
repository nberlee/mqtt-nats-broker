# Shared QoS 2 and affine restoration findings

These findings cover transactional affine cleanup, malformed-claim recovery, and
truthful restoration of shared-subscription sessions.

The text under each “Original finding record” heading is preserved from the roadmap as
it existed on 2026-08-10.

## F-047

**Title:** Keep affine assignments when a SUBSCRIBE packet fails but the session
survives

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 47

### Original finding record

- [ ] The packet's two affine unwinds treat a FAILED PACKET as an ENDED SESSION. They mark
  the generation's assignments `Discarded`, which this design defines as "the owning
  session ended" (`docs/ARCHITECTURE.md`, claim state) and which [MQTT-4.8.2-5] permits on
  that basis — but the session survives a failed SUBSCRIBE, and [MQTT-4.8.2-5] requires a
  live session to complete a delivery already started to it. Reachable when a packet
  carries a SECOND shared QoS 2 filter: it re-enters `ensureQ2AffineConsumerSubscription`
  with the consumer present, skips the save and opens the readiness gate while the session
  is still unresolved, so a move can claim and copy under the generation before the
  membership rollback. The settlement reclaim was guarded when this was found — it fences,
  drains, and then RETAINS the path whenever `q2AffineGenerationOwnsClaims` says the
  generation still owes one, leaving teardown as the event that ends it — and the same
  guard is what the two packet sites need. Pinned for the reclaim by
  `TestQ2AffineReclaimKeepsAPathThatStillOwesADelivery`, and its predicate is `terminal()`
  rather than `Discarded` alone so a permanently-retained `Lost` claim cannot pin the path
  for the life of the session (`TestQ2AffineReclaimIsNotBlockedByATerminalClaim`). A
  second, smaller part of the same cluster is untouched: `discardQ2AffClaimsFor` skips a
  claim that is already `Discarded` but will still stamp `Discarded` onto an `AffineDone`
  or `Lost` one, which is imprecise rather than harmful — neither becomes reapable as a
  result — and belongs with the per-site fix because the helper is shared with `clear()`
  and teardown. The gate opening on an unresolved session is itself worth closing
  separately.

## F-048

**Title:** Arbitrate affine cleanup before discarding assignments

- Status: Open
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 48

### Original finding record

- [ ] The packet's two affine unwinds discard this generation's assignments BEFORE the CAS
  that arbitrates whether they still own them, so a cleanup CAS lost to a remote winner
  that PRESERVED the generation leaves that winner's started QoS 2 delivery discarded —
  terminated rather than redistributed, which is the [MQTT-4.8.2-5] outcome for the wrong
  session, and unrecoverable because the discard is monotonic. The two sites are
  `prepareQ2AffineUnwind(c, true)` from the packet's unused-path unwind (`mqtt.go:6755`)
  and from `rollbackSubscribePacket` (`mqtt.go:7262`). The settlement reclaim
  `unwindProvisionedQ2AffineOnLanded` was converted when this was found and passes
  `disposeNow` false, disposing only once its cleanup record is confirmed; the remaining
  two were left behaviorally unchanged deliberately, because each sits inside a packet
  rollback with its own failure semantics — converting them is a transactional-rollback
  change needing its own plan and race fixtures, not a parameter flip. Session TEARDOWN is
  the model to follow: `claimTeardown` (`sess_teardown.go:151`) durably CASes an ownership
  marker and resolves every rejection by re-read, and only a WON claim reaches
  `deleteTeardownTargets`. Disposing LATE is recoverable rather than stalling: a claim
  whose record no longer names its generation is detected by `materializeQ2AffOnBehalf`
  (`qos2_shared_affine.go:1800-1816`), which discards it, acks the source and reaps it on
  the next source redelivery. Both of those were verified in the code after a first
  attempt to refute the finding asserted the opposite of each. Reaching the race also
  requires the remote to hold a shared QoS 2 membership and to start a delivery under the
  same generation inside the claim scan. Stated in `docs/CONFORMANCE.md`; the converted
  site's race is pinned by
  `TestQ2AffineReclaimRejectedByAGenerationPreservingWinnerKeepsItsClaim`.

## F-049

**Title:** Provide an operator recovery path for unreadable affine claims

- Status: Open
- Classification: Operational corruption hardening
- Release treatment: Post-alpha candidate if normal broker operation cannot create the
  malformed state
- Original roadmap order: 49

### Scope correction, F-036 course E

**Appended rather than rewritten**, because the record below is preserved as written.

The original text says an incomplete inventory "is what protects every affine consumer
from deletion", and that "every affine consumer delete" stays blocked. **The orphan sweep's
CANDIDATE-RECLAMATION route no longer deletes an affine consumer by absence** — F-036 course
E removed `deleteOrphanQ2AffConsumers` outright, because no positive fact could authorize
a by-name delete of an interest-retained consumer. (Not "no affine consumer delete is
reachable from the sweep": teardown-marker adoption still reaches `deleteTeardownTargets`,
a pre-existing route that is not claimed race-free and is F-036's fourth manifestation.) So
what an unreadable claim now wedges is
`reconcileOrphanQ2AffClaims` and the sweep-driven reaps, not consumer deletion. The
fail-closed reasoning and the account-wide scope are unchanged; only the resource class
the refusal protects is smaller.

### Original finding record

- [ ] One unreadable or unattributable assignment claim wedges affine reclamation
  ACCOUNT-WIDE, with no way to clear it. The fail-closed behavior is correct and
  deliberate — `scanQ2AffClaims` aborts rather than conclude anything about a record it
  cannot decode, and the orphan sweep skips affine reconciliation entirely on an
  incomplete inventory (`orphan_qos2_sweep.go:191-195`), which is what protects every
  affine consumer from deletion — but nothing repairs or quarantines the offending record,
  so every affine consumer delete and every sweep-driven reap stay blocked for as long as
  it exists. Note the discard itself is STREAMING and therefore partial, not atomic:
  `discardQ2AffClaimsFor` mutates each matching claim inside the scan callback, so the
  claims reached before the malformed record are discarded (and a both-sides-proven one
  may be reaped) while the rest are not — a validate-first two-pass discard is the
  alternative if that partial progress is ever judged unacceptable. Reached by `clear()`,
  by session teardown, and by the settlement reclaim (which releases its local
  subscription and MaxAckPending charge anyway and leaves the rest pending, see
  `docs/CONFORMANCE.md`). What is missing is an
  operator-visible path: surface the offending subject, and either quarantine it out of
  the inventory once it is provably undecodable or provide a supported way to remove it.
  Pinned as behavior by `TestQ2AffineUnreadableClaimFailsScanClosed` and
  `TestQ2AffineReclaimDiscardFailureStillReleasesTheLocalHalf`; raised by the holistic
  review of the ambiguous affine provisioning reclaim.

## F-050

**Title:** Refuse or retry a CONNECT with incomplete shared-subscription restoration

- Status: **Closed 2026-08-23 by refusing, not retrying.**
- Classification: Confirmed correctness or liveness defect
- Release treatment: Alpha blocker
- Original roadmap order: 50
- **Filed twice.** `docs/ROADMAP.md` carried this defect as both the §0.3 F-050 entry and
  an unnamed §0.2 bullet added by F-064's holistic review ("a saved shared filter that
  fails restoration at CONNECT is silently dropped"). Same mechanism, same consequence,
  one dossier. Both boxes are ticked by the same change; the §0.2 filing is kept because
  it is the one that recorded the F-064 review's constraint on any fix.

### What closed it

**The success CONNACK follows a complete restoration, or the CONNECT is refused.**
Subscription restoration moved to before the CONNACK, so a filter it cannot install
refuses the CONNECT through the pre-CONNACK abort discipline that already discards this
connection's Will and leaves the requested lifetime ungranted. That is what answers the
F-064 review's objection: the problem was never "refuse" — it was refusing *after*
acknowledging, which publishes the Will and lets a requested expiry of 0 destroy a
persistent session.

**Five reachable exits, each with its own fixture**, because a remedy demonstrated on one
says nothing about the others: `ensurePubRelConsumerSubscription`,
`ensureQ2AffineConsumerSubscription`, the raw subject `processSub`, the plain-or-group
JetStream consumer, and the level-up companion of the last two. The verdict is
`mqttFirstUnrestoredFilter`, which reads a value `processSubs` was always required to set
(on a SUBSCRIBE those bytes become the SUBACK) rather than adding bookkeeping. It is named
**local failure aggregation**: it does not arbitrate ownership, does not prove this broker
still owns the record, does not prove a group durable exists, and does not prove an
installed subscription yields end-to-end delivery.

**The reorder needed exactly one new fact, and it is connection-scoped.** Three plan
revisions were spent establishing that it cannot be per-subscription, for two structural
reasons: a per-subscription flag cannot be armed before the subscription exists — a bare
`c.processSub` makes the NATS handler live *before* it returns, so a stored PUBREL can
enter its callback with no metadata attached at all — and a released gate is
indistinguishable from one that never existed, so a refusal cannot express "woken and
still not allowed out". So transmission suppression is one atomically-published verdict on
the session, consulted by all four delivery callbacks before they touch `sub.mqtt`, armed
unconditionally on every accepted CONNECT and deliberately left installed after a refusal.
`mqttSub` gains no field, `processSub` is unchanged, and the reconciliation phase's
adoption and sweep are untouched. Both terminal transitions are CAS-guarded: they are
exclusive today, and a double channel close panics the process.

Two orderings are load-bearing. The grant point moves after restoration, per its own rule
that the requested lifetime applies only once everything that can refuse has passed. The
3.1.1 forced leave stays *after* the CONNACK, so the pre-CONNACK restore excludes shared
filters for a non-v5 connection **explicitly**, where the old code relied on ordering
alone.

**A restore-path failure also stops being destructive.** It no longer deletes a plain
durable the record still names (reachable when a multi-level filter's level-up companion
fails after the main half succeeded), and it refunds `tmaxack` per filter from the amount
each group join **reported** — not from the durable, because the config comes back whether
a call charged it or reused a resident entry, and refunding by durable erases an earlier
connection's charge and leaves the accounting permanently short.

### Designs refuted in review, so none returns

- **Failing the CONNECT after the CONNACK.** The F-064 objection: the connection is
  `connectReceived`, so a bare error return sends a v5 DISCONNECT as the first packet
  ([MQTT-3.2.0-1]) and close bookkeeping publishes the Will; the lifetime is already
  granted, so a requested expiry of 0 destroys a persistent session.
- **Retrying in place.** A retry that also fails lands in the same state, so the
  disposition question is only deferred; the two JetStream exits already retry internally.
- **Removing the failed filter from the record.** Silently deleting durable client state on
  a transient failure is the shape this finding condemns.
- **Duplicating the provisioning into the reconciliation phase.** Two mechanisms stating
  one fact, and it leaves the raw-subscription exits after the CONNACK.
- **Installing the raw subscription ungated in a final pre-CONNACK sub-pass.** Tried to
  avoid the shared QoS 0 swallow; `mqttDeliverMsgCbQoS0` honours neither `closed` nor any
  gate and writes the packet, so it makes a QoS 0 PUBLISH the client's **first packet**.
  There is no safe version of "ungated raw".
- **Proving the group durable by re-issuing its recorded config.** Its premise is false —
  shared consumer configurations are deliberately not persisted — and
  `ActionCreateOrUpdate` with the stale in-memory one rewrites a live group durable's
  configuration for every other member. Filed as its own roadmap item.
- **Moving the 3.1.1 forced leave before the CONNACK.** Converts an unacknowledged CONNECT
  attempt into a permanent destructive mutation of session state. Filed separately.
- **Carrying the restore state on each subscription.** Killed twice: `phaseOwned` is
  load-bearing both for `sweepUnadoptedPhaseSubs` and for the withheld-first-transmission
  bookkeeping, so no split of it works; and a third per-subscription fact still cannot be
  armed before the subscription exists.

### Residuals

Stated in `docs/CONFORMANCE.md`: a permanently unrestorable filter refuses every resume
(clean start is the recovery, and a v5 client is told the reason); a shared QoS 0 message
can be swallowed during the restore window; **a withheld QoS 1/2 delivery can lose its
never-transmitted marker to a rebind racing the refusal, after which it waits out AckWait
instead of being NAKed forward and its first client-visible PUBLISH can carry DUP — the
lost prompt NAK is this finding's own residual, the DUP derivation is F-072's**; the same
marker is lost for a shared durable even without that race, because the ordinary close
runs `purgeSharedPending`; a timed-out affine gate wait falls into F-072's population; and
the displaced-broker window has no upper bound — the record-state backstop interval is its
first opportunity, not a limit.

Two remedies for the rebind race were built and withdrawn across holistic rounds 1-3, and
are recorded so neither returns: **draining** the woken callbacks before the refusal
returns (a counter cannot be an admission barrier — a callback can be descheduled between
loading the verdict and registering, or arrive after the drain observed zero, and delivery
interest stays live until teardown), and deciding the recording on a **durable-scoped**
predicate rather than the connection (removes the race but not the lateness, and the set of
places a durable can live is neither complete — a record-sourced shared membership's
durable exists only through `SharedSubs` and an in-flight config — nor sequence-precise for
a drain). Closing it needs a real callback-admission boundary, which this finding does not
justify; HEAD already treats the same boundary as best-effort.

Holistic round 4 added one defect to the inventory, and it is F-050's own shape in
miniature: a **malformed persisted shared key** was silently skipped when the filter list
was built, so it produced no failed filter, the aggregation saw nothing wrong, and the
client was told Session Present for a membership nothing restored. It now refuses. The
mechanism is worth recording: a pre-decided failure must be carried in the filter's
`reason`, not only its `qos`, because `processSubs`' first loop clamps any `qos` above 2
down to 2 before it tests the failure sentinel.

### Original finding record

- [ ] A CONNECT restore ACCEPTS a session whose shared filter failed per-filter, leaving a
  persisted membership with no live queue subscription and no group consumer. The
  restoration loop marks the filter failed in `mqttProcessSubs`' first loop and its second
  loop skips it, but `update()` skips failed filters too — so `sess.sharedSubs` keeps the
  restored entry, the CONNACK reports Session Present, and a client that is not required
  to re-subscribe waits forever for shared messages that are never delivered. Reachable
  from ANY per-filter restore failure (a failed group-consumer create, a failed
  subscription, an ambiguous affine provisioning save), not from one path. The fix is a
  choice the CONNECT path has to make explicitly — retry the failed restoration for the
  current binding, or refuse the CONNECT so a retry performs a complete restore — and it
  changes CONNECT acceptance semantics, so it is its own item. Raised by the holistic
  review of the ambiguous affine provisioning reclaim, whose retention test now proves
  delivery after a REAL re-subscribe (`TestQ2AffineAmbiguousProvisioningKeepsAPathAMembershipOwns`)
  and deliberately does not claim delivery without one.

### Downstream

F-027's forked-record repair (closed 2026-08-12) excludes shared-subscription membership
for exactly this failure asymmetry: a re-asserted group membership would be durable while
a
per-filter failure leaves no join and no consumer — the accepted-but-dead state above. (The
reason was once stated as an ordering, "the success CONNACK precedes shared restoration".
F-050 moved restoration BEFORE the CONNACK, so that phrasing is no longer true; the exclusion
rests on the asymmetry.) **F-027's repair set widens to shared
membership when this closes**, and its snapshot constructor is where that exclusion lives
(`sess_fork_repair.go`).

## F-076

**Title:** Prove a shared group durable exists before restoring its membership

- Status: **Closed 2026-08-23.**
- Classification: Confirmed correctness defect
- Release treatment: Alpha blocker
- Roadmap section: §0.2 (multi-frontend HA blockers)
- **ID note.** Filed as F-076, not F-073: F-073, F-074 and F-075 were allocated on
  2026-08-22 by F-034's scope ruling and their filing commit has not landed, so `docs/`
  reports them free while they are not.

### The finding

An MQTT 5.0 shared subscriber reconnects to a broker where its `mqttSession` object is
still resident. `processSharedJSConsumer` finds the consumer config left by the previous
connection, finds no live delivery subscription on this connection, and takes its
fall-through: it subscribes to the deterministic deliver subject and returns success.

Every proof that the group durable still exists sits behind that function's `!exists`
branch, and its post-subscribe repair sits behind `charged != 0`, which is false on this
path by construction. **So if the durable is gone, nothing notices.** The client is told
Session Present = 1 and holds a live NATS subscription on a subject nothing publishes to —
no error, no reason code — and it recurs on every reconnect to the same broker while the
condition persists. The only recovery available to the client is a clean start, which
discards the session it was trying to resume.

It is **not** F-050. That refuses a CONNECT whose restoration *reports a failure*; here
every step returns success, so `mqttFirstUnrestoredFilter` cannot see it. This is why
F-050's dossier does not claim to prove an end-to-end delivery path.

### Two populations reach the fall-through

**A resident session, which is the ordinary case.** Shared configs are never persisted —
`buildPersistedSessionLocked` skips every entry whose `DeliverGroup` is non-empty, because
`SharedSubs` is the sole authority — so a session rebuilt from its record has no shared
`cons` entry and takes the create path. A resident one does not. **The defective path is
therefore the ordinary same-broker reconnect, not an exotic one.**

The narrow claim the design rests on, pinned by test rather than left as prose: no writer
of `sess.cons` can introduce a *shared* entry that was not already resident. The SUBSCRIBE
rollback restores this session's own snapshot; fork repair's config comes from a record
body, which carries no shared config; resume reconciliation is plain-only; drains skip
shared filters.

**An unsupported persisted record**, which the restore used to assign verbatim. See
"The record shape" below.

### How the durable disappears

- **A concurrent leaver's last-holder race.** The scan walks in-memory membership plus
  every other session's record; a leaver whose scan completed before this session's
  membership was durable issues a queued, fire-and-forget delete that can land later. The
  join path already treats this as real — it has dedicated repair code for it — which is
  the internal evidence that the resume path's omission is a defect and not a theory.
- **The orphan consumer sweep's own grace window.**
- **Administrative deletion, or an account reset.**

**One route was proposed and falsified, and is recorded so it does not return.** A
stream-level `ConsumerLimits.InactiveThreshold` imposed after startup does **not** reap an
existing group durable: nats-server applies a stream's consumer limits to a consumer's
config only when the consumer requests zero and only at create or update time, and a
stream update is rejected only when an existing consumer *exceeds* the new limit, which a
consumer at zero does not. The converse does hold and is load-bearing — a consumer
*created* while such a limit is in force inherits a delete timer — which is why the join
path refuses under a live stream limit.

### What landed

**A batched classification, twice, around the restore.** A pre-pass before `processSubs`
collects exactly the shared sids that have a resident `cons` entry and resolves them through
`lookupConsumersMulti` in **bounded chunks** of `mqttInflightLoadBatch`, stopping at the
first chunk-level failure and marking every unresolved target indeterminate — the filter
count is client-controlled, each lookup has its own timeout, and an indeterminate verdict
already refuses the CONNECT, so continuing would only stall it while installed queue
interest drops QoS 0 deliveries. The resume arm acts on the verdict *before* subscribing,
keeping
the join path's own create-before-subscribe rule. A post-pass after `processSubs` repeats
the **full** classification — not a presence check, because a consumer can become
non-canonical between the two reads — adopts the second reading, and feeds any regression
into F-050's existing pre-CONNACK aggregation rather than adding a second refusal path.

**Four outcomes, and no fifth.** Present-and-canonical adopts a clone of the live config,
replacing the remembered one. Present-and-non-canonical refuses, and never updates or
deletes: the durable may be serving other members, and a configuration this broker would
not have built is not one it may silently normalize on their behalf. Authoritatively
absent refused too **as F-076 left it** — F-077 later made that one verdict REPAIRABLE in
the CONNECT pre-pass, and terminal still in the post-pass; see the F-077 entry. Anything
else — a failed read, a response carrying no consumer, a consumer carrying no
configuration — is **indeterminate** and refuses, because recreating on an unproven absence
is how one broker's transient error becomes a group-wide change. That last sentence is
exactly why absence is the only verdict that became repairable.

**One canonical derivation.** `buildSharedGroupConsumerConfig` is the single constructor:
the join path builds with it and the resume path validates against it, so "what we would
have created" and "what we accept" cannot drift.

**Whole-struct validation, closed by construction.** Every `ConsumerConfig` field carries
exactly one of four dispositions, enforced by a reflection bijection, because a list of
fields worth checking cannot close a struct a dependency bump can extend. Three fields are
legitimately variable across members and are *bounded* rather than ignored. Two are
server-normalized and compared against an **expected value**, never copied from the live
consumer — copying `MaxDeliver` would accept a group durable that discards a delivery after
one attempt. One is server-*owned*: `Metadata`, whose content moves with the dependency, so
it is constrained instead by requiring it to hold nothing but the three **named** server
keys. A test measures the normalized set against a real create rather than trusting the
table — and caught a real error on its first run, where `Metadata` had been pinned as unset
and a whole-struct comparison would have refused every healthy group durable.

**The batched lookup can now express what it needs to.** `lookupConsumersMulti` encoded an
authoritative not-found and a successful response carrying a nil `ConsumerInfo` identically,
so absence and indeterminacy were indistinguishable. The per-response decode is extracted
and a successful nil now leaves the outcome unsettled. **That fixed a latent defect beyond
this path**: the in-flight source cache reads a settled absence as
`mqttInflightSourceLost`, a terminal verdict, so it was concluding "known to be gone and
unrecoverable" from a reply that proved nothing. It now resolves to `Unknown`, the
conservative arm. Downstream treats both as non-`Complete`, so this corrects a
classification invariant rather than a demonstrated delivery bug.

### The record shape

A `Cons` entry whose `DeliverGroup` is non-empty is a format this broker never writes. Under
the compatibility boundary it is **refused**, not accommodated, at every ingress — the
CONNECT restore prologue (which also covers the overdue/retired branch built from the same
record), record-driven expiry, the session-expiry sweep, the orphan-consumer sweep, the
common `mqttValidateSessionRecord`, and the shared last-holder scan
(`sharedDurablesHeldByOthers`). The two expiry paths matter most: they would claim,
rewrite and delete the record, normalizing a shape nothing here understands. The two walks
that decide by absence of a reference refuse in the way that withholds their destructive
act, because a partial view must never be reported as a complete one: the orphan sweep
**aborts** rather than skipping, and the last-holder scan **reports every requested durable
held** rather than skipping, which withholds the group-consumer deletion.

### What this establishes, and what it does not

**This section describes what F-076 established. It is not the current contract** — F-077
took up both of the residuals below, and its own entry is authoritative for what the broker
does today.

F-076 establishes **configuration evidence**: the durable exists and carries a configuration
this broker recognises. It establishes **nothing about ownership**. A stale resident broker
can validate a present canonical durable and serve from it, and a co-restored filter with
no resident entry still creates through the untouched join path, unarbitrated. Both were
**F-077**, and both were defects that existed independently of F-076.

The verification arm itself performs no group-durable write. That claim is about the arm,
not about the restore operation: a filter with no resident entry still takes the create
path, which is every filter after a broker restart.

### Residuals

- **The delivery gap is not repaired.** Messages published while the durable was absent are
  gone: with no consumer, an interest-retained stream keeps nothing. The refusal makes the
  state visible; it does not recover the messages.
- **The proof is fresh but not atomic.** `CONSUMER.DELETE` and `CONSUMER.CREATE` are both
  unconditioned, so a durable deleted *or made non-canonical* after the post-pass gets no
  further check on this CONNECT.
- **No self-repair — CLOSED by F-077 for one verdict.** As F-076 left it, this broker
  would not recreate the durable at all. F-077 made an **authoritative absence** repairable:
  the CONNECT pre-pass hands it to the resume arm, which recreates the durable, and a
  classified record CAS then proves the membership it was recreated into is still this
  broker's. A **non-canonical** or **indeterminate** verdict still refuses, for the reasons
  that made the verdicts distinct in the first place, and recovers only when the condition
  clears by any means. The post-pass keeps absence terminal.
- **Ownership was unarbitrated — CLOSED by F-077 on the CONNECT path and by F-078 on the
  live SUBSCRIBE path.** Every
  restoration of shared interest now proves ownership before the CONNACK. A **live
  SUBSCRIBE** installed and revived that interest with no ownership write, which was
  **F-078**.

### Refuted designs, kept so they are not re-proposed

- **Re-issuing the remembered config.** `ActionCreateOrUpdate` against a live group durable
  rewrites its `AckWait`, `MaxAckPending` and storage for every other member on every other
  broker. The join path is acceptable only because it derives fresh; a remembered config is
  not a derived one.
- **Blind canonical `CreateOrUpdate` on every resume.** Correct config, but it writes to a
  live durable on the common path where nothing is wrong. A proof is a read.
- **Treating any lookup error as absent.**
- **Validating a short allowlist of identity fields.** A list cannot close a struct.
- **Copying the server-normalized fields out of the live consumer.**
- **Dropping the local `cons` entry on an absent verdict** so the next CONNECT recreates
  through the join path. Mechanically safe, and rejected on scope: retiring the charge is
  the accounting half of F-077, and steering the next CONNECT into the unarbitrated create
  means this change caused a recreation by another route.

## F-077

**Title:** Arbitrate ownership after any shared group durable create

- Status: **Closed — the CONNECT half landed first, and F-078 closed the live SUBSCRIBE
  half.**
- Classification: HA arbitration / delivery safety
- Release treatment: Alpha blocker
- Roadmap section: §0.2 (multi-frontend HA blockers)
- Split out of F-076 by a scope consult. **This is a defect on `main`, not one F-076
  introduced.** Split again — CONNECT half / live half — by a non-convergence escalation
  after three plan rounds each re-cut the live interest gate and every finding of the third
  faulted the mechanism the second had just introduced.

### The finding

**No route that restores shared group delivery interest revalidated that this broker still
owns the membership it was keyed on.** A commits its accept save; B commits a newer accept
for the same client and removes the membership; B's last-holder path deletes the group
durable; A restores the membership anyway and serves the group.

"Serves" includes **acknowledging**, and that is what makes it a delivery-safety defect
rather than a tidiness one: `mqttDeliverMsgCbQoS12` acks and drops on a reserved-subject
violation, on a subscribe deny, on No Local suppression, and delivers-and-acks a shared
QoS 0 member. Each destroys a group message for the members that legitimately hold the
membership.

### The class is wider than "a create", which was review round 1's finding

- **R1 — a resume whose durables all verify present-and-canonical.** Adopts, creates
  nothing, installs interest under a membership it never revalidated.
  `docs/ARCHITECTURE.md` assigned exactly this to F-077 by name.
- **S1 — the resume repair F-076 declined to perform.** An authoritatively absent durable.
- **S2 — the CONNECT `!exists` create.** Every shared filter of a record-sourced session
  takes this arm, because shared configs are never persisted.
- **R2, S3 — the live SUBSCRIBE fall-through and its `!exists` create.** **F-078.**

`createDurableConsumer` has nine production call sites; exactly two carry a non-empty
`DeliverGroup`, both inside `processSharedJSConsumer`. `TestSharedGroupCreateCensus` holds
that down as a drift guard — a config reaching a create through a variable escapes it, and
the test says so.

### What landed for R1, S1 and S2

**One classified record write**, `proveSharedMembershipOwnership`, CAS'd at `sess.seq` inside
the CONNECT's still-open accept transaction, run once per restore that installed, repaired
or adopted shared interest, after the last group-durable write and before the CONNACK.

It is a **write** because a read cannot arbitrate: JetStream returns 10164 from the
clustered pre-proposal check while a write for the subject is in process, so "the record did
not move" is unprovable from a load. It is a **semantic** no-op rather than a byte-level one:
every write mints a fresh `WID` and parents it to the current lineage.

Five outcomes. **10071 keeps its own lineage classification** through
`disposalForDirectRejection` — the only place a sibling record is recognised on that arm,
and the place the fork grant is minted, so collapsing it to one undifferentiated verdict
would lose the grant. (It also asserted `0x8E` where the record supported only `0x89`, until
F-084 withdrew that claim from every record-derived route.) **10164** goes through
`resolveContendedRecordSave`. **Ambiguous** keeps the mutation and permits no further write.
**Definite local** refuses.

**The trigger is restored interest, and it is stack-local.** Committed per filter only once
the whole filter succeeds, its level-up companion included; deliberately not a session field,
because a fact parked there outlives its operation and would govern the next one — which is
F-078's D2 in miniature. It is kept apart from `created` (which drives the post-subscribe
verify create) and `charged` (which drives the refund), because each had previously been
inferred from the other wrongly: the repair creates while charging nothing, so a verify
create gated on the charge skips exactly the population that just recreated a durable.

**Absence became repairable, and nothing else did.** The verification's four outcomes are
re-cut into one predicate, *terminal unless repairable*, stated as a class rather than an
`if absent` at each site because F-076's batching seam closed only when the special cases
were deleted. Repairability moves **three** decisions together — the batch termination in
`classifySharedGroups`, the pre-pass filter failure in `failSharedGroupFilters`, and the
resume arm's own `v.err` return — and moving fewer leaves the repair arm unreachable, which
is the defect plan round 4 found. The **post-pass keeps absence terminal**: a durable absent
again after the repair is one something is deleting underneath this CONNECT.

**No transmission gate is needed here.** The restore gate is armed before the first
subscription the CONNECT installs and opens only after a confirmed success CONNACK, and all
four delivery callbacks consult it — including the PUBREL and affine subscriptions a shared
QoS 2 filter provisions before the group consumer exists. The caveat is that it stops
transmission and acknowledgement but cannot undo queue selection, so a shared QoS 0 message
selected while it is closed is dropped.

### Designs refuted, so none returns

- **The accept transaction fences it.** An in-process fence on one `mqttSession`.
- **`sweepOrphanedConsumers` bounds the damage.** It skips candidates a resident session's
  `cons` or `sharedSubs` names, so the stale broker's own object protects the durable it
  just recreated. Eventual cleanup is not arbitration.
- **A bare forced record write.** Plain `save` formats the store error and destroys the
  10071/10164 distinction the verdicts depend on.
- **Read the record and compare the sequence.** 10164 is returned while a write is in
  process, so a matching read proves nothing.
- **Delete the created durable on a foreign verdict.** The holder scan skips same-client-ID
  records, which is exactly false after a takeover, so it deletes the NEW owner's durable.
- **Reuse `releaseSharedConsumer` for the foreign cleanup.** It refunds the whole historic
  charge, purges through an already-rejected transaction, and inherits the same-ID hole.
- **Blind canonical `CreateOrUpdate` on an unverified resident config.** F-076's refutation,
  unchanged: the repair acts only on an authoritatively absent verdict.
- **A per-subscription transmission gate.** Refuted for the live path; see F-078.

### Residuals

In `docs/CONFORMANCE.md`. Ownership is proven at the write's linearization point, not
continuously. The proof does not extend to the consumer, which may vanish at any point after
the last verification. A durable created and then lost to a foreign verdict is left in place
and is **not inert**. A refused resume sends no DISCONNECT, though it is no longer silent
—
the pre-CONNACK refusal signal now carries a v5 failure CONNACK. And the repair **broadens
the
population** that reaches the live SUBSCRIBE path — which F-078 has since arbitrated.

### Two F-076 fixtures were rewritten rather than relaxed

`TestMQTTSharedGroupAbsentDurableRefusesTheResume` became `...RepairsTheResume`, and
`...LevelUpCompanionIsVerifiedToo` became `...IsRepairedToo`. Both pinned the refusal F-076's
own conformance entry called a residual and assigned to F-077 ("Making recovery the broker's
own job is F-077"). What they assert now is strictly more: the resume succeeds, the specific
deleted durable is back, and it carries the canonical shape.

## F-078

**Title:** Arbitrate a live SUBSCRIBE's shared interest before it serves

- Status: **Closed 2026-08-25.**

### What closed it

**A live SUBSCRIBE proves it still owns the membership before anything it touched may
serve, and withholds the deliveries it could authorise until that proof lands.** Which
those are is stated exactly, because one class is deliberately exempt: QoS 1/2, PUBREL and
affine deliveries **wait**, **shared** QoS 0 ones are **dropped**, and **plain** QoS 0 ones
keep flowing, because a plain QoS 0 delivery acknowledges nothing on the group's
behalf and gating it would lose messages while protecting nobody. A mixed packet therefore
authorises plain QoS 0 traffic before the proof lands, and
`TestLiveSubPlainQoS0IsNotGated` requires that exemption positively.

The instrument is F-077's classified record write, reused unchanged, placed as the packet's
**last** record write — after the final membership save and the optional affine unwind — so
its CAS linearizes where the packet left the record.

**The trigger is wider than F-077's, and that is the half that is not obvious.** The gate
opens without a proof only for a packet that committed no trigger **and** none of whose
record writes was rejected by its **per-subject CAS**. Four record writes in a SUBSCRIBE lose
that evidence before the caller sees it: the early per-filter membership save converts a
definite rejection into a per-filter code and *continues*, so a record that demonstrably
moved leaves both the trigger and the returned error empty; the affine provisioning save was
reduced the same way, behind a bare error covering four operations that touch no record at
all; the affine unwind logs its error and never assigns it outward; and the void-register
store is swallowed by a bool-returning helper reached through **two** chains — and, as review
found after the first fix, through the **failed-filter cleanup** as well, which returns
normally. A packet-local fact carries them, never a session field.

**Only a CAS rejection counts, and the wide rule was wrong.** Implementing "any failed record
write" broke 1352 tests in the ported suite against a clean baseline: it converted an ordinary
per-filter failure into a connection-level refusal, a contract the affine items own. The
evidence differs — a CAS rejection means the write was *refused*, a local failure moved
nothing, and an ambiguity is our own bytes maybe-landing under our own CAS — so the fact is
read from the store-boundary detail via the CAS-rejection predicate, never from the outward
error (which is reformatted and loses its NATS identity) and never from the definite-failure
classification (which merges a CAS rejection with a local failure and a refused
transaction).

**The transmission gate** is armed before the packet installs anything — before the PUBREL
and affine subscriptions a shared QoS 2 filter provisions — and resolved by exactly one CAS
through a defer, so an error path or a `runtime.Goexit` unwind cannot leave it pending.
It is scoped to the **connection** and to a per-session **generation**: the first stops an
abandoned verdict crossing into the next connection, the second stops a waiter that watched
one packet's gate from escaping the next packet's — **up to that waiter's final read**. A
gate published before the read is observed; a waiter past it is the final-check residual,
which the generation does not reach and no longer claims to. All four delivery callbacks
consult it
and the restore gate, and the three that can wait — QoS 1/2 delivery, PUBREL and the
affine copy — do it through **one** wait under **one** absolute deadline; that wait
replaced `awaitRestoreGate`, which was deleted rather than left beside it. The fourth,
shared QoS 0, consults both verdicts without waiting on either and drops.

**The disposition follows the two existing precedents.** Foreign disposes, closes
synchronously, and returns *nothing* to the parser — speaking again would race the displacer
for the reason the client is left with. Forked, Local and an unresolvable ambiguity dispose
nothing: none demonstrates another owner.

**REBASED BY F-084.** As this finding landed, Foreign additionally discarded the Will
"as a local takeover does" and notified `0x8E`, while the others said `0x89`. Foreign demonstrates
no takeover either — a rejected ownership CAS establishes that the subject moved and nothing
about who moved it — so it now says `0x89` too and consumes the Will like the rest. What
still separates it is the DISPOSAL alone.

**Every close here consumes the Will under the held gate.** A record-observation
eviction can detach the session first, and the detached path publishes only an already-owed
*immediate* Will, so a delayed one would never reach the durable outbox.

**An ambiguous proof is resolved by the packet itself, under the gate it already holds.**
Releasing first lets the periodic pass — which resolves *bound* uncertain sessions — apply
its generic policy, which **disposes** on forked. A forked resolution therefore retires the
uncertainty *without* disposing: it clears the pending write, **discards** the continuations
owed only if those bytes had landed, leaves the void-register revision uncredited, and leaves
the sequence, lineage and record body at the acknowledged base.

### Designs refuted in review, so none returns

- **A per-subscription flag.** F-050's first refutation, unchanged: a shared QoS 2 SUBSCRIBE
  installs delivery interest before any subscription exists to hang a flag on.
- **Copying the restore gate's `opened`-then-CAS ordering.** Safe there because its two
  terminal transitions are exclusive; here a losing opener could flip a withheld verdict to
  open.
- **Releasing the ownership gate and re-acquiring it around resolution.** Killed as a
  *class*: the periodic pass resolves bound uncertain sessions, so it can win that gap, and
  both re-acquisition outcomes are then wrong — `sess.c != c` does not prove a takeover, so
  the rebound arm would discard a Will this very verdict must preserve.
- **Treating a fence-busy resolution as terminal.** It reproduces the exact Will-loss chain
  the forked settlement exists to prevent, and its producer is F-078's own proof, so it
  cannot be deferred to another finding.
- **Widening the CONNECT path's trigger to match.** F-077's contract is committed and a
  failed write there already refuses the CONNECT; the wider predicate is live-path only.

### Follow-up coverage, accepted as non-blocking

The verification that landed with this finding is the replicated production-order
fixture — a live SUBSCRIBE driven through the real packet path with a second member
holding the group, whose oracles are that the ownership write ran, that the refused
broker's FIRST packet is a DISCONNECT (**0x8E** as this finding landed; rebased to `0x89`
plus a disposal assertion by F-084 — see below), that the group durable survives, and
that a publish reaches the legitimate member — plus the unit-level invariants of the gate,
the fact accumulator and the forked settlement. **Eleven** mutations kill their paired tests:
M1, M5b, M6, M11, M13b, M18, M21, M32, M33, M34 and M35. They include the one reproducing
the pre-F-078 behaviour; the one reverting `mqttProcessSubs` to unnamed results, whose only
killer is the store-count oracle; M33, collapsing the Foreign arm into a non-disposing
refusal; M34, hoisting the unresolved-write check above the quiet branch; and M35, capturing
the gate once instead of re-reading it each pass.

**One of those tests was flaky, and the fix was a seam rather than a retry.** The
later-generation test resolved generation N and then armed N+1, which races the waiter: a
CORRECT waiter may wake, reload while N is still current and open, and return Open. Under
`-count=200 -race` it failed 26 times. Arming N+1 first has the opposite defect — a
single-load waiter reads N+1 straight away and passes. The property was therefore
**unmeasurable in both directions**, and the assertion as written claimed more than the
code guarantees: what the generation closes is a gate published before the waiter's **final
read**, not one published before its send. `mqttTestGateAboutToWaitV` pauses the waiter after
it reads the gates and picks one to block on, so the test publishes both transitions inside
that window: 200/200 clean afterwards, and M35 kills it immediately.

**A third end-to-end oracle pins the quiet branch's ORDERING, as correct.** A review
round read that ordering as a defect: a packet whose ancillary save went ambiguous opens
the gate while the session carries an unresolved write, which is an outcome nobody read
and therefore includes "CAS-rejected, response lost". Withholding there was implemented,
measured and **reverted** — it takes the ported suite from 0 failures to 1355, because it
turns an ambiguous affine provisioning failure into a connection-level refusal, the
contract the affine items own and the same class the wider trigger predicate broke once
already. The consult that settled it REFUTED the finding rather than deferring it: a quiet
packet installed nothing, so its open releases only traffic backed by interest predating
the packet, and that population is the convergence window `docs/CONFORMANCE.md` already
documents, now with this case stated explicitly.

`TestLiveSubQuietPacketOpensDespiteAnAmbiguousAncillaryWrite` drives the route — a shared
QoS 2 filter's affine provisioning save suppressed, so it fails the FILTER and continues,
no group consumer and therefore no trigger, on a session now uncertain — and asserts all
four ways the ordering could break unnoticed: the SUBACK carries exactly 0x80, the ownership
CAS never runs, the connection stays live, and the resolver's re-attempt happens BEFORE the
SUBACK is enqueued (by store count) while the session stays uncertain — which is why the
residual says "may continue serving until settlement converges" rather than claiming a
brief window.
**M34 is invisible to all four earlier fixtures** — the replicated one, the store-count
oracle, the plain-QoS-0 control and the withholding test all pass with it applied — and is
killed only by this one.

**Why the 0x8E assertion replaced "not a SUBACK", measured rather than argued.** The
earlier form rejected only the positive acknowledgement, which accepts every other verdict
in the tail — Forked, Local and an unresolvable ambiguity all refuse the packet too, and
all three say 0x89 and dispose nothing — while the durable-survival and delivery-to-B
oracles cannot separate them either, because B holds the group throughout whatever A is
told. **M33 passed the earlier fixture and fails this one**, which is the measurement:
`0x8E` was reachable only from the Foreign arm, after `disposeForeignStateLocked`, so it
pinned the verdict.

**F-084 REBASED that oracle**, because `0x8E` no longer reaches the wire from this arm and
Foreign now shares both the code and the Reason String with Forked — no byte separates them.
The fixture asserts the **disposal** instead, which is the fact the code stood proxy for and
the one thing Forked and Local do not do. So the state effect this paragraph called unpinned
is now pinned there, and the Will pair below carries the rest.

These fixtures are **owed and deliberately not claimed**, and review accepted them as
follow-up rather than commit-blocking once the replicated case existed:

- the four delivery-callback ack arms driven at the ownership-CAS boundary with a
  second-member positive oracle;
- the resume fall-through chain end to end (the route whose reachability took two review
  rounds to establish);
- **a SHARED QoS 0 drop.** Only the plain-QoS-0 *control* is asserted today. The paired half
  needs an MQTT 5.0 fixture: shared subscriptions are a v5 feature, so a 3.1.1 attempt failed
  its own positive control and was reduced rather than shipped passing for the wrong reason;
- PUBREL and affine deliveries inside the withholding window;
- a mixed packet gating plain QoS 1/2 traffic on a shared filter's arbitration;
- the disposal ordering assertion with an exactly-one-DISCONNECT oracle;
- forked-keeps-the-Will with the Will **armed**, in both the direct and the eventual forms;
- **the eventual-Foreign arm.** It needs a SECOND BROKER sharing the session stream: a local
  contender cannot interleave a takeover while the SUBSCRIBE holds this session's ownership
  gate, so a single-broker fixture can only reach the unclassified arm. The direct Foreign
  **verdict** is covered by the replicated production-order fixture — through its disposal
  assertion since F-084 rebased it off `0x8E`; the Will pair below carries the rest.

### Residuals F-078 states

Stated in `docs/CONFORMANCE.md`: a delivery admitted **before** the arm can still escape the
gate — bounded, with its direction stated, because arming precedes every install so the
escape can only carry interest that predates the packet; the gate's collateral cost to the
connection's other QoS 1/2, PUBREL, affine and shared QoS 0 traffic, with plain QoS 0
ungated; the window between a foreign verdict and the detach; and the active-durable
residual, widened from resume-scoped to the whole operation class.

### The original finding record

- Status was: **Open.**
- Classification: HA arbitration / delivery safety
- Release treatment: Alpha blocker
- Roadmap section: §0.2 (multi-frontend HA blockers)
- The live half of F-077, split out by that item's non-convergence escalation.

### The finding

A live SUBSCRIBE that installs or revives shared group delivery interest serves under a
membership it never revalidated. Same consequence as F-077's: group messages acknowledged
and destroyed for the members that legitimately hold the membership. F-077 closed the
CONNECT path; this is the defect `main` carries today.

**S3 — the `!exists` create**, plus the post-subscribe verify `CreateOrUpdate`, which
genuinely recreates a durable a concurrent leaver deleted. On `main` that second create was
gated on the charge; F-077 replaced that with the explicit `created` fact, so F-078 inherits
`created`.

**R2 — the resume fall-through**, which creates nothing, verifies nothing and arbitrates
nothing. Its reachability took two review rounds to establish and must not be re-derived:

1. a SUBSCRIBE packet adds a shared filter;
2. its rollback save fails **definitely**, so `readd` restores `sess.cons[sid]` while
   `sess.sharedSubs` has already been reset to the pre-packet snapshot;
3. the client disconnects and reconnects to the same broker;
4. the CONNECT builds its filters from `sharedSubs`, which lacks the key, so
   `collectResidentSharedGroups` never sees the sid and `processSubs` never touches it — the
   resident `cons[sid]` survives while the new connection has no deliver subscription;
5. the client SUBSCRIBEs to that filter and the fall-through subscribes on a remembered
   config.

**`readd` alone is not sufficient**, and a plan revision that cited it alone was wrong:
`reopenTeardownDelivery` clears `closed` and the subscription was never unsubscribed, so on
the *same* connection the refresh arm still runs. Step 3 is what makes `sub == nil` real.

F-076 justified passing nil verdicts on this path with "a live SUBSCRIBE cannot reach that
arm". R2 falsifies that.

### A per-subscription gate cannot work, and this is settled

A shared **QoS 2** SUBSCRIBE runs `ensurePubRelConsumerSubscription`, and
`ensureQ2AffineConsumerSubscription` after it, **before** `processSharedJSConsumer` is
reached. So the live path contains the bare `c.processSub` whose NATS handler goes live
before `processSub` returns — `mqttDeliverPubRelCb`'s own comment says so — and both that
callback and the affine one consult only the connection-scoped restore gate, which is
already open on a live connection. This is F-050's first refutation and it applies here. Do
not re-propose an `mqttSub` field.

**The callback census is complete at four**: the QoS 0 raw subscription, the QoS 1/2 deliver
subject, PUBREL, and the affine path. There is no fifth production delivery callback. Plain
QoS 0 is excludable because it cannot acknowledge a stored group message.

A **session-scoped** gate covers all of them by construction — reused subscriptions, the
level-up companion, shadow subscriptions, PUBREL, affine, and the plain subscriptions of a
mixed packet — which is why it is preferred to an enumeration a later round extends.

### Four open defects in that gate

- **D1, the arming point.** Raising the fact only in `processSharedJSConsumer`'s arms is too
  late for PUBREL and affine. Arming before them creates a different reachable case: every
  shared filter can then fail per-filter, no ownership write is owed, and the connection
  still gets a SUBACK and stays live with the gate never opened. Needs an explicit
  **no-effect terminal outcome**: an early-armed gate must open when the packet finishes
  with no surviving shared interest.
- **D2, an abandoned verdict crosses packets and connections.** Copying
  `abandonRestoreGate`'s leave-installed rule is unsound: restore can leave one installed
  because *every* accepted CONNECT unconditionally replaces it, while a packet gate is
  re-armed only by a later effectful shared SUBSCRIBE. So a local, unresolved or
  deliberately non-disposing forked outcome leaves a resident session carrying a gate that
  permanently blocks its QoS 1/2, PUBREL, affine and shared QoS 0 traffic — and a later
  CONNECT re-arms only the restore gate, so deliveries pass the new verdict and are then
  blocked by the stale one. The gate must be connection/generation scoped.
- **D3, the wait needs the restore gate's THREE-way verdict.** `mqttPhaseGateWaitFor` is a
  cap, not AckWait; above it a timeout leaves PUBREL or the affine copy unsent for the
  remainder of AckWait. The restore gate distinguishes resolved-but-withheld (return, no
  ack) from timeout (**NAK** for PUBREL and affine), and that distinction is load-bearing.
  QoS 1/2 additionally needs the withheld-delivery bookkeeping so a timed-out attempt is not
  later presented as a first-sight DUP.
- **D4, a waiter escapes a later packet's gate.** Packets are serialized but delivery
  callbacks are not: a callback loads gate G1 and waits, the packet opens G1, the readLoop
  installs armed G2, and the callback wakes, sees *its* G1 opened, and transmits or
  acknowledges without ever consulting G2. `awaitRestoreGate` loads one pointer and never
  re-validates it — safe for CONNECT, because another CONNECT cannot begin on a live
  connection, and unsafe for recurring packet generations. Needs a generation-aware recheck
  under one absolute deadline.

Once D2 is fixed, a delivery meeting both gates should require **both** open, under one wait
budget.

### The disposition, and its two precedents

Only **foreign** DISPOSES on the live path. **Forked** says a record claiming our parent
erased our write and that nobody was demonstrated to own the session, so there it does not
dispose — `mqttRejectionForked`'s precedent.

**REBASED BY F-084.** As this finding landed, foreign additionally took the *takeover* arm:
it discarded the Will while forked did not. Foreign demonstrates no takeover either, so it
now consumes the Will exactly as forked does, and suppressing one on either arm would have
been the silent loss. The two are separated by the DISPOSAL alone.

The CONNECT path disposes on both, matching
`mqttReconcileRefuseForeign`, because pre-CONNACK there is no acknowledged session to
preserve. Two paths, two existing precedents, followed rather than re-derived.

Order: **dispose state → end the record transaction → CONSUME THE WILL → release the
ownership gate → notify and close**, the Will step added by F-084 and placed there for the
reason that section gives: the consume does JetStream work so the fence cannot be held
across it, and the gate must still be held so nothing rebinds between the disposal and the
Will it owes. The gate and the fence are held across the disposal, and the notify/close
tail synchronous on the readLoop. `disposeForeignStateLocked` applies only if this session
object is still current. `mqttProcessSubs` must keep idempotent defers for abnormal exits,
because a test hook calling `t.Fatal` unwinds with `runtime.Goexit`.

Inside `processSubs`, before returning the foreign error: mark this packet's shared filters
failed, close the subscriptions it installed, and resolve or abandon the gate.

### Residuals F-078 must state

The **operation-wide** active-durable residual: F-077 states a narrower version covering only
a durable created by a *resume*, which does not cover S3's live create. A durable created
and then lost to a foreign verdict holds stream interest, can accumulate pending across the
sweep grace, may feed a later joiner, and its verify `CreateOrUpdate` can move group-wide
settings before arbitration loses. Also: the gate's collateral cost (other QoS 1/2, PUBREL
and affine deliveries wait; shared QoS 0 drops; plain QoS 0 ungated), and the interval
between a foreign verdict and `sess.c = nil` in which the session's other shared
subscriptions are briefly still live.

### Tests it inherits, and five it still needs

Carried from the pre-split plan: R2's full chain; the four ack arms driven in production
ordering with a second-member positive oracle; shared-QoS-0-drop with an unrelated plain
QoS 0 control; reused and companion subscriptions; PUBREL and affine inside the window;
a mixed packet; resolve-exactly-once; the gate-and-fence disposal assertion with an
exactly-one-DISCONNECT oracle; the forked-keeps-the-Will case with the Will **armed**; and
the replicated case whose ordering must land A's early membership save first, pause after
the verify create, and only then let B take over — with three terminal oracles (A acknowledged
nothing, A is disposed, a publish reaches the legitimate member).

Still missing, and these pin D1 to D4: an early-armed gate followed by every shared filter
failing per-filter; an abandoned gate followed by a later CONNECT; a waiter captured on G1
while G2 is armed; a timeout with AckWait above the cap; and a stale trigger fact across a
later non-shared packet.

Note the mutation whose delivery must be driven at the `"after-ownership-cas"` boundary
rather than after the verify create — driving it earlier does not discriminate a gate opened
before the write. F-077 already extends `make cluster-failover-tests`' regex, so the
replicated case is visible when it lands.

## F-082

**Title:** Release a failed shared filter's own state independently of the last-holder answer

- Status: **Closed.** Alpha blocker, filed by `docs/ROADMAP.md` §0.2 and split out of the
  last-holder fail-closed change (`b659236`) by its plan review.

### The finding, in four parts

`releaseFailedSharedFilter` is the cleanup a live shared SUBSCRIBE owes when one of its
filters failed and the compensating membership REMOVAL was only proven durable later — it
is
registered as a landed continuation by `rollbackSharedSub`'s `saveMaybeLanded` arm and run
by
`settleLandedAttempt` under the resolver's fence. It began by computing
`sharedDurablesHeldByOthers` and skipping the whole loop body on a "held" answer.

**D1 — a refusal withheld this session's own cleanup, not just the group delete.**
`held[durable]` authorizes exactly one thing: destroying a group consumer whose backlog is
interest-retained, which is other members' business. The resident `cons` entry, the
deliver-subject subscription, the pending deliveries with their reserved packet identifiers
and the `MaxAckPending` charge are this session's own, and are owed whatever the answer.
Since `b659236` the scan also reports held for an indeterminate walk and for a record it
cannot interpret, so one unreadable record anywhere in the account suppressed this session's
cleanup indefinitely. The other four callers of the scan withhold only the delete.

**D2 — the release was keyed on `sess.cons`, which the failing attempt has not written to.**
The half the roadmap bullet did not state, and the larger one. `addJSConsToSess` runs only
once the WHOLE filter has succeeded, so a `cons` walk cannot see the join a failing filter
just made, which makes the no-config branch the COMMON shape of this route rather than a
corner. **That is not the same as "there is never an entry", and the difference is
load-bearing:** a PRE-EXISTING entry for the same durable can be resident — F-078's `readd`
shape leaves one with no membership behind it — which is why the release sweeps every matching
entry instead of treating absence as given, and why one test arm seeds two of them. What that
branch left behind was created by the failing packet itself: the `sharedCharged`/`tmaxack`
charge (taken at create time, before the failing exit), the pending deliveries under that
durable, and the deliver-subject subscription. `refundSharedCharges` deliberately skips the
SUBSCRIBE path because "it disposes of the consumer through `releaseSharedConsumer`, which
refunds as part of the release" — true of the INLINE cleanup, which passes the join's
returned config, and false of this continuation.

**The consequence is stated from the code, not from that comment.** There is **no double
count**: `processSharedJSConsumer` samples `alreadyCharged` and does not re-charge a durable
already in `sharedCharged`. The defect is that the orphan charge permanently consumes the
resident session's ack budget — `tmaxack` stays raised for a membership that no longer
exists, and the ceiling check then refuses later joins the session is entitled to. A
never-expiring resident session is never reconstructed, so this is indefinite.

**D3 — the scan's answer merges two facts, and only one licenses a release.**
`sharedDurablesHeldByOthers` marks a durable held from THIS session's own remaining
`sharedSubs` before it walks any record, and `mqttProcessUnsubs` relies on that. For this
caller the own-hold half is load-bearing twice over. *Overlapping filters:* a multi-level
key
joins two group consumers, and the level-up one of `$share/g/foo.>` IS the consumer of the
plain filter `$share/g/foo`, so an unconditional local cleanup would purge a surviving
filter's pending state, unsubscribe its delivery and refund its charge. *The `priorExisted`
branch:* `undo()` is shared, so the re-SUBSCRIBE branch — whose write is an ADD that KEEPS
the membership — registered the same continuation, and only the own-hold arm stood between
that and a live membership's consumers being released.

**D4 — the pre-save delivery barrier was `cons`-keyed too.** `rollbackSharedSub`'s removal
arm
lowered the barrier by walking `sess.cons` while taking the register snapshot BY DURABLE.
Mixed, that is the worst combination: on the common shape the barrier was never lowered, and
`closeTeardownDeliveryLocked` states why that matters — the snapshot is then "only a
moment in
time", and a delivery arriving after it allocates an identifier whose cancellation misses
the
membership-removal write.

### What closed it

**Three dispositions per group consumer, not two.** Own-held → discharge nothing and delete
nothing. A foreign holder found, or the scan refused → discharge this session's state,
withhold the delete. The scan reported unheld → both. Own-hold is decided in the caller from
`sess.sharedSubs` via `mqttSharedKeyOwnsDurable`, so `sharedDurablesHeldByOthers` is
unchanged and is consulted only for targets that survive that test.

**The release is keyed on the group consumer's NAMES.** `releaseSharedGroupTarget` /
`...Classified` is the former `releaseSharedConsumerClassified` body re-keyed on `(durable,
deliverSubject)`, with the config form kept as a thin wrapper; both names come from
`mqttSharedGroupNamesForKey`, the ONE derivation of what a shared key joins, over which
`mqttSharedDurablesForKey` and `mqttSharedKeyOwnsDurable` are now projections rather than
independent copies of the same main/level-up rule. The `held`-gated delete lives in exactly
one place, so a refusal can no longer reach a `CONSUMER.DELETE` by another route, and the
duplicate scan the old config branch performed is gone.

**The `cons` sweep deletes EVERY match**, not the first: a durable can be named by more than
one stale entry, and one left behind has no membership to reach it by.

**The staging half is keyed the same way**, through `closeSharedDeliveryLocked`, and its
ownership test EXCLUDES the doomed key — it runs before the removal, so counting that key
would make every closure suppress itself. A durable a REMAINING key still reaches keeps both
its delivery and its registrations.

**BOTH halves of this release apply the own-hold fence.** The landed continuation is one;
the
other is the CONFIRMED inline cleanup, `cleanupFailedSubClassified`, which hands the join's
returned config straight to the whole-durable release. Without the fence there, the
overlapping level-up case closes and unsubscribes the SURVIVING filter's delivery, purges
its
pending identifiers and refunds its charge — and the last-holder scan stops none of that,
because it withholds the consumer DELETE and nothing more. The extracted helper does not
assume the invariant: it names the two callers that enforce it and points the others at F-083.

**The `priorExisted` branch registers nothing**, which is the same rule the early per-filter
save already states for its own ambiguous arm: nothing was deferred for want of proof, so
there is no landed continuation to register. The own-hold fence stays as an independent second
fence, and the two are pinned by separate mutations — one fails only the overlap test, the
other only the registration test.

### Five designs refuted while planning this, recorded so none returns

A maintainer simplifying any of these back is the failure mode the plan's own review round
raised, so each carries the code that kills it.

1. **"Just move the `held[dur]` check down to the delete."** D1 alone. It leaves D2
   entirely —
   the common shape still discharges nothing, because the work is still derived from `sess.cons`
   — and it walks straight into D3, releasing an overlapping live membership's state and
   running a release the `priorExisted` branch never owed.
2. **"Call the existing config-keyed release unconditionally, passing the precomputed
   answer."** Same D3 exposure, and it still only fires when a resident config happens to
   exist, so the common shape is untouched.
3. **"Refund by `cc.Durable` from the join's returned config."** `processSharedJSConsumer`'s
   own comment refutes this for the CONNECT path: the config comes back whether the call
   charged or reused a resident entry, so refunding by durable can erase an EARLIER
   connection's charge and leave `tmaxack` permanently short. Here the release is
   whole-durable — the own-held fence establishes that this session ends up holding no
   membership for it — so the whole `sharedCharged` entry is exactly what goes. Same numbers,
   different justification, and the justification is what a later reader needs.
4. **"Capture `jscons`/`jssub` in the continuation closure."** `rollbackSharedSub` does not
   have them; its callers do. It means plumbing two more parameters through four call
   sites and
   rooting a `*subscription` in a closure that may run much later — the shape F-078 had to
   undo
   for its gate's `owner` field. The names are deterministic, so deriving them from the key
   costs nothing and roots nothing.
5. **"Sample ownership once, at registration time."** Wrong for both halves. In the
   continuation the membership may legitimately have been re-added by a later SUBSCRIBE, and
   own-held must then refuse the discharge; in the staging half the doomed key is still present
   and must be EXCLUDED, or every closure suppresses itself.

### The precondition the design rests on

Every production path that runs a landed continuation holds the session's OWNERSHIP GATE as
well as the record fence: the periodic pass and CONNECT take `lockSession` before
`resolveUncertainSession`, `resolveUncertainNow` takes it for a resident session (its ungated
arm is `sess.asm == nil`, which a live SUBSCRIBE's session never is), and the live SUBSCRIBE
tail keeps the packet's. That is what makes the own-hold sample still true after `sess.mu`
is
released — `sess.mu` alone would only make each map access race-free. A TEST can reach
`resolveUncertainWrite` without the gate and must take it explicitly.

### Two claims that were REFUTED during review, so neither returns

- **"A forked verdict leaves the strand unreleased forever."** False for this route. A
  failed-filter rollback makes `processSubs` return an error, so `mqttProcessSubs` returns
  with `resolveEventually` false, `mqttSettleLiveArbitration` never runs and
  `settledOwnAmbiguity` stays false — `mqttResolveAfterPacket` therefore runs the generic
  resolver, whose forked arm EVICTS the session. There is no resident object left to strand,
  and no conformance residual was added.
- **"The next resume charges again."** False, as above: `alreadyCharged` prevents it. The
  defect is budget permanently spent, not a double count.

### What this does NOT change

`sharedDurablesHeldByOthers` and its three fail-closed arms; the non-snapshot-read
residual (a
leaver completing its scan before a joiner's record lands); the same-client-ID own-record
skip;
and the fact that a durable spared by a refusal is reclaimed by nothing, since the
orphan-consumer sweep aborts on the same record. The helper's existing behaviour on a
CAS-rejected or unsecured register write is inherited verbatim: it logs, unsubscribes and
still issues the delete, and a re-armed ambiguity is settled by the NEXT resolution rather
than
by re-running this continuation, which `takeUncertainActions` has already consumed.

### Coverage

Nine unit cases over the continuation and the staging arm, one route case, and one row
added to
the scan's own table. The route case matters because a fixture that calls the release directly
cannot show that anything still REGISTERS it: it drives a real multi-level shared SUBSCRIBE
whose level-up join is refused by a real production check (the messages stream is given a
consumer inactivity limit between the two joins, which `processSharedJSConsumer` refuses to
create a reapable consumer under), with the rollback's own save suppressed so it goes ambiguous
and the readLoop's resolver then settles it. Both of its arming confirmations are load-bearing:
without them it passes for the wrong reason when the filter fails BEFORE its first join, or
when the rollback save fails definitely and registers nothing.

The scan-table row is **C7, the indeterminate walk**, which predates `b659236` and was pinned
by nothing at either layer — the three existing fixtures using that seam drive the PUBREL
barrier scan and the in-flight walks. Its paired mutation is the first thing that has ever
made
that arm's deletion fail a test.

## F-083

**Title:** Release a shared UNSUBSCRIBE's and a SUBSCRIBE rollback's own state independently
of the last-holder answer

- Status: **Closed.** Alpha blocker. Filed by F-082's second consult as a separate item —
  another caller with its own commit path — and WIDENED during planning to a fourth site
  by an
  explicit decision (see "The scope decision" below).

### The finding

F-082 established that `sharedDurablesHeldByOthers`'s single `held` bit merges two facts and
that only one licenses a caller to release its OWN state. Everything
`releaseSharedGroupTargetClassified` does is whole-durable, so a durable this session still
reaches through another of its own shared keys must never reach it. The overlap is
deterministic: `$share/g/foo.>` joins a level-up group consumer whose durable AND deliver
subject are the plain filter `$share/g/foo`'s.

**D-a. The UNSUBSCRIBE commit half killed a surviving filter's delivery.** Leaving
`$share/g/foo/#` while still holding `$share/g/foo` handed the level-up config to the
whole-durable release: the survivor's pending state was purged, its identifiers freed, its
`sharedCharged` amount subtracted from `tmaxack`, and its delivery closed and unsubscribed.
The scan withheld only the `CONSUMER.DELETE`. A resident never-expiring session is never
reconstructed, so nothing repaired it short of a reconnect.

**D-b. It discharged nothing without a `cons` entry**, the population `purgeSharedPending`'s
comment already named. **And a second leak in that branch, found by enumeration:**
guarded by
`!HasSuffix(sid, " fwc")`, so in the mixed shape — a config for the main shared sid but none
for its companion — *neither* level-up durable was released: the main sid hit `cons` and
released only its own durable, and the companion was excluded by the suffix guard.

**D-c. The staging half mixed a `cons`-keyed barrier with a durable-keyed void
snapshot**, so
for the no-`cons` population the barrier was never lowered and the snapshot was "only a moment
in time"; and neither half was fenced, so it also barriered and voided the survivor.

**D-d. `finishSubscribeRollback` was the same defect at a fourth site**, with no roadmap
bullet. A SUBSCRIBE adding `$share/g/foo/#` to a session already holding `$share/g/foo` gives
the level-up companion a distinct `cons` key but the SAME durable, so a failed save put
it in
`added`; both rollback halves then stripped the pre-existing filter. `sess.sharedSubs =
snap.shared` is restored before `saveClassified`, so the rescan reported held and withheld
only the delete.

### The scope decision

Consult 2 (`task-mt8zt7du-i3r8a8`) ruled D-d into F-083 rather than a fifth finding: it is
F-082's TWIN — the packet-level variant of the two sites F-082 fenced — and the class
had by
then produced instances three times, which makes the enumeration the defect. The
precedent was
put against the recommendation (F-082's consults had excluded "the other four callers" without
enumerating them) and Codex ruled it did not require a separately filed item for every later
instance of the same invariant.

### What closed it

**One property, four authorities.** The fence asks *does a membership that SURVIVES this
operation still reach this durable?*, of an explicit immutable `mqttSharedSurvivors` key set
the operation supplies: the live map minus the packet's whole doomed SET at UNSUBSCRIBE
staging; the live map at its commit half; the live map for a failed filter's release;
and the
PRE-PACKET SNAPSHOT at both rollback halves. An exclusion-list API was refused because it
encodes the UNSUBSCRIBE's ordering rather than the invariant and misdescribes a rollback.
`sharedKeyStillOwnsLocked` is deleted and its two F-082 callers converted.

**The doomed SET, not one `exceptKey`.** One packet can leave both overlapping filters,
and a
single exclusion lets each doomed key protect the other, suppressing a release nothing will
then owe.

**Three operations at the commit half, not one.** "This config is doomed" and "this
durable is
owed" are different questions: the doomed keys' configs go unconditionally, then the owed
targets are computed, then every REMAINING config naming an owed durable is swept and the
target released. Without the first, a doomed multi-level key's `" fwc"` config outlives its
membership whenever the plain key keeps the durable alive.

**`removeUnsubConsumer` is DELETED, not guarded.** Two review rounds each cut the same
seam —
a plain-path fallthrough, then the lexical guard that fix invited — and the third named the
cause. `execDeferredConsumerDeleteClassified` already was the correct plain teardown; the
deleted function duplicated it, mutated `sess.cons` without `sess.mu`, and never unsubscribed
`cc.DeliverSubject`. It now serves the QoS downgrade and the plain UNSUBSCRIBE alike.

**No lexical shared/plain discrimination, anywhere.** `mqttToNATSSubjectConversion`
encodes a
literal `.` as `//`, so plain `$share.foo` has the sid `$share//foo`. `sessionKey`'s own
comment claimed a plain sid "never contains `/`" and is corrected in place. The
authorities are
`f.shared` and `cc.DeliverGroup`.

### Designs refuted in review, so none returns

1. **"Move the `heldShared` check down to the delete."** A NO-OP for D-a: the release was
   already unconditional below the delete and the answer is already "held".
2. **"Skip the whole release when held."** The pre-F-082 shape; one unreadable record anywhere
   in the account then suppresses this session's cleanup indefinitely.
3. **"Reuse `sharedKeyStillOwnsLocked(durable, sid)` at both halves."** Wrong in opposite
   directions — meaningless at commit, and a single-key exclusion at staging.
4. **"Test against the live `sess.sharedSubs` everywhere."** Wrong at D-d, whose live
   map is
   the attempted membership.
5. **A `$share/` prefix guard on the plain path.** Strands ordinary plain filters.
6. **A `cc.DeliverGroup` guard as a second authority with its own test.** Refused as a
   test of
   a state no production constructor can build; `f.shared` is the dispatch authority and the
   config check is a backstop, not a second rule.

### Residuals, unchanged by this item

The scan remains a non-snapshot read that authorizes a deletion, not an arbitration: the
non-snapshot join/delete race and the same-client-ID skip stand. The surviving-key
sample is
LOCAL-SESSION truth held by the ownership gate, not cluster-global truth. A crash after the
save but before the release completes leaves any prefix of a fixed per-target order, and
reclamation is not guaranteed — the orphan sweep aborts on the same unreadable records the
scan fails closed on. All pre-existing; `docs/CONFORMANCE.md` carries them.

### Out of scope, and why

The 3.1.1 forced leave and teardown are **vacuous** for this property — neither can have a
surviving membership. The forced leave nonetheless carries its own separate defect, a
`cons`-versus-membership target mismatch, deferred to its own roadmap item. Teardown carries
none and is deferred nowhere.

### Coverage, stated as delivered rather than as planned

**12 tests — ten unit and two fulltests — and 18 mutations, 18 kills.** The separations
that matter are proven rather than
asserted: the commit and staging fences fail DIFFERENT tests (M1/M2), the rollback's two halves
fail different ASSERTIONS of one test (M8/M9), the rollback control catches over-fencing
(M10/T10), and the two rollback obligations are pinned separately (M15a/M15b).

**Production routes actually exercised.** Two end-to-end tests drive real packets through
`mqttProcessUnsubs`: one for the plain `$share.foo` teardown, one for a shared UNSUBSCRIBE
against a planted foreign holder — the latter is the only test that reaches `commitUnsub`'s
`f.shared` dispatch. The rollback tests drive `rollbackSubscribePacket` itself, so both
of its
halves run.

**What is NOT covered, and is therefore not claimed.** The plan called for two tests running
the UNSUBSCRIBE and rollback LANDED CONTINUATIONS through the resolver while contesting the
ownership gate. Neither was written. The continuation mechanism itself is untouched by this
item — `retainOnLanded` and the resolver fence are unchanged, and the existing F-082 resolver
fulltest exercises that route for the shared release — but the premise that a
`mqttSharedSurvivors` sample stays true across the gate is **argued from the call sites, not
tested**. A future change to the gate or to either continuation should add those tests before
relying on this item's evidence.

**A three-part redundant defence, and what the mutations do and do not establish.** An
ungated `CONSUMER.DELETE` on a shared durable is prevented by THREE independent things: the
commit half removes the doomed shared configs before the plain loop runs, `commitUnsub`
dispatches on `f.shared`, and `plainConsumerForUnsub` refuses a config with a `DeliverGroup`.
Removing any ONE changes no observable behaviour (M16), so **none of the three is individually
pinned**. M16b removes all three together and the end-to-end test then fails, which
proves the
CONJUNCTION protects the delete — **it does not establish that any one of them, ordering
included, is individually load-bearing**, and no test here distinguishes them. A maintainer
removing one should not read that as safe: it means the failure would be invisible to this
suite, not that it is harmless.

The lane's `mutation-ledger.md` records three defects the mutation run and the holistic review
found in the tests themselves: a helper that reimplemented the code under test so every
mutation passed, an assertion that demanded the opposite of a documented contract, and a suite
that bypassed the dispatch it claimed to pin.
