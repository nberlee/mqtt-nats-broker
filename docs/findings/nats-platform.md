# NATS platform-adoption audit

This dossier records the 2026-08-14 audit of released and announced JetStream
features against the bridge's MQTT, authorization, consistency, and HA contracts.
The roadmap links the actionable results. Rejected candidates stay here so the
JetStream request audit does not repeatedly rediscover an attractive but unsound
substitution.

The audited baseline is nats-server 2.14.5 and `nats.go` 1.52.0. The server
dependency is both the embedded test server and the source of the exported types
used by the MQTT shim; production still talks to an external server, so a
load-bearing server behavior also belongs in the documented runtime floor.

## Term-fenced Raft proposals

nats-server 2.14.4 shipped
[`nats-io/nats-server#8370`](https://github.com/nats-io/nats-server/pull/8370),
which requires the JetStream layer to pass the leadership term under which an
asynchronous operation began into `Propose`/`ProposeMulti`. Raft rejects the
proposal when that term is no longer current, and leadership changes tear down
the old stream-leader state. The
[2.14.4 release note](https://github.com/nats-io/nats-server/releases/tag/v2.14.4)
describes the consequence as preventing stale proposals from a previous term
from changing state after a fast election. The pinned upstream evidence is
[`raft.proposeLocked`](https://github.com/nats-io/nats-server/blob/v2.14.5/server/raft.go#L914-L919)
and
[`TestNRGTermFencedProposals`](https://github.com/nats-io/nats-server/blob/v2.14.5/server/raft_test.go#L6393-L6455).

This is **not an apply-time recheck of**
`Nats-Expected-Last-Subject-Sequence`. It targets the stale stream-leader goroutine
that could perform the pre-proposal CAS check under one leadership epoch and
propose after a fast election; it does not make arbitrary unconditional stores
safe, add cross-stream transactions, explain administrative deletion, or provide
lineage across stream recreation.

**What the bridge actually gets from it was measured and upstream-confirmed** (F-030,
closed 2026-08-16). `test/depproof/` enumerates every route by which a write conditioned
on a superseded base could reach a proposal across a leader transition, fixtures the two
live ones inside isolated checkouts, refutes seven other shapes, scopes post-start
mirror/source drift outside F-030, and pairs mutations with each live mechanism. The term
fence covers ONE of those two routes; the other
is refused because a new leader does not accept messages before pending entries are
applied and then reconstructs the subject's expectation from its own applied state. A
NATS maintainer explicitly confirmed that these leader-only mechanisms guarantee header
enforcement across elections
([`nats-server#8469`](https://github.com/nats-io/nats-server/issues/8469#issuecomment-5304075553)).
The four repaired oracle defects keep the harness from being the sole proof, but do not
defeat the independent source and upstream contract. The v2.14.3 control run corrected
this audit's expectation: an old-term write is ADMITTED there, which is the difference
the floor buys, but it never becomes the surviving record, because an apply-time
log-order check that both generations have discards it.

That closure belongs to `$MQTT_willq`. The per-stream findings inherit the server contract
and the canary, and each needs its own consequence audit before it closes. **All three
have now run.** F-032's ran on 2026-08-17 and closed without lineage, producing F-062, which
closed on 2026-08-18; F-033's ran on 2026-08-18, also closed without lineage, producing
F-063, F-064, F-065 and F-066; and F-034's ran on 2026-08-22, closing without lineage too
but as the one of the three whose consequence turned out to be REACHABLE, producing F-067
and F-068. The lineage-deletion audit is its own roadmap bullet and is now unblocked.
Fields used for other facts do not fall merely because the term window does — though
`RWID` is no longer one of them at all, and the field has been **REMOVED**. F-036's
2026-08-27 attempt disproved it as that finding's predicate, because the watermark holds one
slot per client, so an `ackWID == rwid` conjunct recreates the superseded-watermark false
negative the fix exists to close. The vanished-record vocabulary item was the only remaining
candidate reader and decided against it too: a write identity names a record's bytes rather
than one occurrence of them, so it cannot say which occurrence a retirement removed.

### Rejected follower-local apply-time CAS

Issue #8469 proposed marking conditional entries so every replica would recheck its
physical store during apply. That design was withdrawn. Interest retention can make
storage depend on replica-local consumer state, and MaxAge or per-message TTL expiry is
timer-driven on each replica. Physical per-subject store state is therefore not a
deterministic consensus input. If one peer rejected while another stored, their failed
sequence accounting could diverge. Restricting emission by the stream's current
configuration does not repair historical state or order a concurrent configuration
change with the stream log. The maintainer confirmed the architectural rule: the leader
alone decides admission; followers persist the sequence it selected.

## Subject delete markers for retained convergence

`$MQTT_rmsgs` already enables per-message TTL and keeps one record per retained
topic. JetStream's `SubjectDeleteMarkerTTL` can make expiry of that last value
observable: the server writes a marker with `Nats-Marker-Reason: MaxAge`, including
when per-message `Nats-TTL` caused the expiry. See
[ADR-43](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-43.md).

This is a candidate mechanism for F-059, not an independent shortcut. The retained
observer must recognize a marker before decoding an MQTT retained record, and both
values and markers must be applied by stream sequence so a delayed marker cannot
erase a newer generation. A replayable consumer or authoritative reconciliation is
still required because a marker delivered through the current `AckNone` consumer
can itself be lost. Explicit empty-body MQTT tombstones remain the immediate durable
delete authority.

Enabling delete markers also enables rollup and requires purge permission. The
current broker-owned stream contract rejects rollup, so the implementation needs a
stream-prerequisite and ACL review. In server 2.14 the useful marker is age/TTL;
ADR-43 records delete-API and purge-API markers as not yet implemented. The sourced
`$MQTT_rmsgs` topology must be in the test matrix because source handling of TTL and
markers has its own rules.

**Decision (F-059, 2026-10-12): declined for `$MQTT_rmsgs`.** The ACL review's answer is
no: markers require `AllowRollup`, and the shared broker-owned stream shape refuses rollup
because any NATS writer with publish permission on the stream's subjects could purge up to
the whole stream with one `Nats-Rollup` message (the MQTT encoder does not forward that
header, so this concerns NATS-level writers, not MQTT clients). Correctness does not need
them: expiry is enforced at serve time from the record's own expiry, a TTL-reaped record
leaves the index through the load's no-message answer, and a lost replacement that expired
is covered by the observer's cache flush on resume. What markers would add is earlier
reclamation of index entries, and they would not replace the resume. `validateRetainedStream`
keeps refusing a nonzero `SubjectDeleteMarkerTTL`. A marker can still arrive through a
SOURCE (v2.15.1 preserves marker headers on ingestion, `MaxAge`, `Purge` and `Remove`
alike), so the observer and the load recognise any `Nats-Marker-Reason` record before
decoding and apply it as a deletion at its destination sequence.

## Stable ephemeral-consumer identities and metadata — DONE

**Landed.** The retained observer and session-record watch derive a stable name **and
delivery subject** from a per-`Server`-run identity, so an initialization retry asks
for the same consumer instead of increasing cardinality. A restarted process receives a
new identity; `InactiveThreshold` remains the crash cleanup. See "Per-run ephemeral
consumer identity" in `docs/ARCHITECTURE.md` and the five residual statements in
`docs/CONFORMANCE.md`.

**Two corrections this audit item needed, both found by implementing it:**

- **A stable name alone does not suffice**, and would have been a silent regression. A
  repeated named create ADOPTS the existing consumer at its current position, so a retry
  would inherit an observer already drained by a discarded manager and leave the retained
  index empty — invisible to every wildcard subscription. The observer therefore proves
  its name ABSENT before creating it, and `ActionCreate` keeps an unexpected occupant a
  refusal rather than an adoption.
- **Metadata is not an ownership predicate for the rollback.** The rollback reads no
  metadata: its licence is a confirmed create, because no error distinguishes "our create
  landed" from "a foreign consumer is at that name". The metadata's only machine reader
  is the server's own `ActionCreate` equality check, where the independent per-run nonce
  is what makes a colliding run's config unequal; an operator and any future sweep are
  the human readers.

## Staged use of the high-level JetStream client

**DECIDED 2026-10-17: not adopted. `mqttJSA` stays the MQTT core's one JetStream
operation layer.** The recommendation this section used to make - a narrow transport
boundary migrating "ordinary" stream/consumer CRUD, information calls, publish
acknowledgements and double acknowledgements to `nats.go/jetstream`, plus a spike of
library ordered consumers - was written on 2026-08-14 against `nats.go` 1.52.0. Re-read
against the pinned v1.54.0 and the broker as it now stands, no operation class is
ordinary, so the premise is superseded rather than deferred. Nothing is owed under it:
no migration, no interface with `mqttJSA` as its sole implementation, and no
ordered-consumer spike. Request-count and latency questions belong to the section 2
query-audit and benchmark bullets in `docs/ROADMAP.md`, which may change a query path
only on measurements and do not inherit a migration obligation from this one.

**The contract every operation shares.** A request is a method on a `mqttJSA` view.
Views share one send queue, one reply registry and the fault seams, and differ only in
the absolute deadline (F-105 8a) or per-request admission gate (F-119) a protocol run
hands them, which every nested request inherits by construction. Around each request
the layer distinguishes a refusal before submission from an ambiguous timeout after
it, answers shutdown with `ErrServerNotRunning`, returns partial results from a
pipelined batch, and classifies errors on the nats-server `ApiError` types (`ErrCode`,
`IsNatsErr`, `isJSErrCausedBy` on `Description`) that hundreds of production call sites
and about a thousand test call sites are written against. A library `context` deadline
implements a timeout; it does not reproduce those semantics. The existing distinctions
are themselves part of the contract and stay as they are: a no-wait send and the
confirmed acknowledgement's wait are not deadline- or gate-checked, and close-certificate
persistence deliberately clears the deadline (`withDeadline(time.Time{})`).

**Stream and consumer CRUD and unfiltered information calls.** The library has them.
They stay on `mqttJSA` on architectural grounds: moving them adds a second submission
and error path for one class of operation, needs server-type conversion or caller
changes, and either duplicates or loses the seams. It would also break ordering. No-wait
consumer deletes, message deletes, purges and acknowledgements enter the same FIFO
`sendq` as requests, so a request submitted after them is SUBMITTED after them; a direct
library request can overtake an entry still waiting in the queue. The guarantee is
submission order, not server-side completion order. The read-only startup and
administrative calls show no benefit that would justify splitting the layer for them.

**Subject walks.** `Stream.Info(WithSubjectFilter(...))` accumulates subjects and
advances its offset by `len(subjectMap)`. It rejects a nil `StreamInfo`, but checks
neither the page envelope, nor progress, nor that each subject matches the filter - the
walk F-105 replaced with `mqttSubjectPageAdvance`, under `streamSubjectsComplete`. The
deliberately single-page `streamSubjects` stays too: its `LastSeq` bounds a destructive
purge and is not evidence that every returned subject lies at or below it.

**Message gets, every variant.** Correctness-sensitive loads keep the leader-routed
management GET, including the pipelined multi-reads and their partial-result semantics
(see "Deliberate non-adoptions"). Library `GetMsg` switches to Direct Get whenever the
cached stream info says `AllowDirect`. No broker-created stream sets it, but the stream
prerequisites accept an operator-set `AllowDirect` as harmless precisely because the
current reads ignore it (`stream_prereqs_test.go`), so a migration would change behavior
for a configuration already accepted, not merely for a hypothetical one.

**Publish acknowledgements and atomic batches.** Ordinary stores depend on the queue's
ordering, the existing ambiguity handling, and the pre-store and post-store seams that
model "never landed" and "landed, response lost"; library publish adds retry and error
translation of its own that would need its own review. The acceptance batch and the
authority transitions additionally need the commit-only reply correlation and the
batch-result validation in `newAtomicBatchRequest` and `storeAcceptBatch`. Library
support for the `AllowAtomicPublish` configuration field is not an atomic publishing
API.

**Confirmed (double) acknowledgements.** The source-ACK gate prepares and registers the
reply before taking `sess.mu`, runs the ownership and HandBack checks and the enqueue
under it, and waits only after releasing it (`outbound_source_ack_gate.go`).
`jetstream.Msg.DoubleAck` has no split form, and the broker's push deliveries arrive on
core subscriptions rather than as `jetstream.Msg`. The linearization and the no-wait
acknowledgement path stay as they are.

**Ordered consumers: retracted.** A library ordered consumer is a pull consumer whose
reset increments its name suffix, deletes the previous consumer asynchronously, and
creates a replacement. Setting `NamePrefix` and metadata does not change that
lifecycle, and the lifecycle contradicts the committed mechanism in
`docs/ARCHITECTURE.md` "Per-run ephemeral consumer identity": stable per-run names, an
independent metadata nonce compared under `ActionCreate`, the observer's
delete-before-create, the watch's create-without-delete, a rollback that deletes only a
confirmed create, and push delivery on reserved `$MQTT.sub.<id>.*` subjects. The
observer already carries its own ordered resume ("The retained index is ordered by
stream sequence"), and the session watch keeps its committed-read backstop. The gate the
spike waited on, the session-lineage deletion audit, ran on 2026-08-16, so the spike is
retracted on its merits rather than postponed again.

**Reply namespace, for any future library use.** A library request replies on the
connection's inbox prefix, `_INBOX.` by default. An MQTT `#` subscription maps to NATS
`>`, and the `$`-topic rule ([MQTT-4.7.2-1]) does not cover `_INBOX`, so a same-account
MQTT subscriber whose permissions allow it receives such replies as QoS 0 PUBLISHes on
`_INBOX/...` - the reason identity replies use `$MQTT.uinfo.` (`authz_identity.go`) and
JS API replies use `$MQTT.JSA.`. Any library request on the service connection at
runtime therefore needs a reserved `CustomInboxPrefix` under `$MQTT.`, or equivalent
reserved reply routing, together with ACLs denying that namespace to native NATS
principals - a prefix alone is not authorization - and per-client connections stay the
authority boundary under auth passthrough. One library request exists today: the backend
probes `AccountInfo` at startup, before the listener opens. Since 2026-10-20 the service
connection carries `CustomInboxPrefix("$MQTT.inbox")`, so that reply is reserved too;
Residual 13 in `docs/CONFORMANCE.md` is closed by it.

## Message schedules: post-alpha Will-outbox spike

JetStream schedules can durably own a delayed timer and can atomically cancel a
schedule while publishing another fact **in the same stream**. See
[ADR-51](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md)
and the [nats-server 2.14.0 release](https://github.com/nats-io/nats-server/releases/tag/v2.14.0).
A possible future design schedules an internal "Will due" fact; the bridge still
performs MQTT authorization, fan-out, retained handling, and terminalization.

This is not an F-030 or F-031 replacement. The accepted session record and its
acceptance fact commit together in `$MQTT_sess`, while the natural schedule lives in
`$MQTT_willq`; schedule cancellation supplies no cross-stream atomicity. A sound
adoption therefore requires a schema redesign that co-locates the required facts or
retains the present acceptance proof. Schedules also imply rollup/purge capabilities.

## Inherited JetStream request patterns: audit (2026-10-20)

The §2 roadmap item "Audit every JetStream query and request pattern" is recorded here. It
changes no query path: the Benchmark item gates every such change on measurement, and
several repeated reads below are distinct validation moments, so removing one needs a
consistency argument as well as a number. What landed with it is the closure of
`docs/CONFORMANCE.md` Residual 13 and the stream-count correction (ten broker-owned
streams, not nine). Request counts below are STATIC estimates from reading the code at
`eb8e8a9`; the benchmark's first need is a request-counting seam that confirms them.

### The primitives

Every typed loader wraps one of these, all in `internal/mqtt/jsa.go`. Production call
sites are textual counts at that commit.

| Primitive | JS API | Kind | Sites | Main paths |
|---|---|---|---|---|
| `newAtomicBatchRequest` | publish batch, reply on the commit only | 1 request, N messages | 5 | CONNECT metadata, every F-105 authority transition, shared join, teardown receipt |
| `createEphemeralConsumer` / `createDurableConsumer` | CONSUMER.CREATE | request | 1 / 7 | startup; SUBSCRIBE, resume repair, PUBREL and affine consumers |
| `deleteConsumer` | CONSUMER.DELETE | request or no-wait | 14 | teardown, sweeps, unsubscribe, startup |
| `createStream` / `lookupStream` | STREAM.CREATE / STREAM.INFO | request | 1 / 16 | startup; admission readiness, resume, shared join, Will cutoff |
| `streamSubjects`, `streamSubjectsComplete`, `streamSubjectsAllPages` | STREAM.INFO with `subjects_filter` | single page / validated page walk / page walk | 3 / 3 / 1 | disconnect hold discard; close certificates, cleanup authorization, obligations; tenure settlement |
| `purgeStream` | STREAM.PURGE with subject and sequence bound | request | 1 | disconnect hold discard |
| `lookupConsumer` / `lookupConsumersMulti` | CONSUMER.INFO | request / pipelined | 9 / 3 | per-delivery admission, PUBCOMP terminal path, drains; resume barrier, shared verify |
| `consumerList` | CONSUMER.LIST paged | page walk | 1 | `$MQTT_msgs` orphan-durable sweep |
| `loadLastMsgFor` / `loadLastMsgForMulti` | MSG.GET `last_by_subj`, leader-routed | request / pipelined | 57 / 3 | every F-105 record, session records, QoS 2 holds and claims; retained replay, duty inventory |
| `loadMsg` / `loadMsgsMulti` | MSG.GET by sequence | request / pipelined | 10 / 1 | resume NAK reconcile, drains, PUBREL paths; resume source cache |
| `loadNextMsgFromSeq` | MSG.GET `seq` + `next_by_subj` | request per record | 14 | every sequence walk: sweeps, Will scan, PUBREL scan, acceptance facts |
| `storeMsg` / `storeSessionMsg` | publish with PubAck, CAS by `Nats-Expected-Last-Subject-Sequence` | request | 22 / 3 | inbound QoS 1/2 store, retained store, QoS 2 and Will CAS, F-105 single records; session persist |
| `deleteMsg` | MSG.DELETE | request or no-wait | 24 | PUBREL, claim reap, floor and material deletes, teardown |
| `sendAck` / `sendMsg` / `sendMsgReply` | core publish on ack subjects | no-wait (double ack awaits) | 1 / 19 / 2 | ack and NAK on every QoS 1/2 path |

### Kept, with the reason

- **The leader-routed management GET**, for every correctness-sensitive load (see
  "Deliberate non-adoptions").
- **The PUBREL fences**: `verifyQoS2HoldForRelease` "is a re-read whose job is to catch
  what changed"; it cannot be cached away.
- **Startup's second STREAM.INFO per stream** in `ensureBrokerStream`, unconditional even
  for an existing stream: the validated config is the one read after the create-or-not,
  so a create-race loser and a concurrent changer are both judged on what stands.
- **Session-record walks and the account-wide affine claim scan**: no complete by-subject
  alternative exists. STREAM.INFO subject listings are paged or truncated over a set that
  deletes, and "a truncated map read as complete is exactly the partial-view-as-absence
  mistake" (`collectInboundQoS2Candidates`). The claim subject carries no owner token, so
  a narrower filter is a subject-scheme change, not a query change.
- **The per-PUBCOMP PUBREL scan**: a session's PUBRELs share one subject, and "a peer may
  have stored more than one same-exchange PUBREL before the phase gate closed record
  creation"; each must be discharged.
- **The sweep's ordering**: abandoned teardowns are adopted "on their own session
  snapshot, which is then discarded", and candidates are listed before references
  (invariant 3), so the sweep's walks cannot simply share one read.
- **`streamSubjectsAllPages`** (`outbound_tenure_settle.go`) advances by the entries a page
  returned; that is sound under its stated premise that resource records are never
  deleted (F-112), so a page can only re-read. Consolidating it with
  `streamSubjectsComplete` is optional, not a repair.
- **The disconnect hold discard** is already concurrent (`mqttWalkBounded`, width 32).

### Candidates for the Benchmark item

The estimates below are the audit's, kept as it wrote them; the measured costs and each
candidate's disposition are in "JetStream path benchmark (2026-10-22)" below.

1. **Outbound QoS 1/2 admission, the hottest path.** Per delivery,
   `readyDependencyInc` reads the consumer name's ownership, then
   `mqttSourcePubRelReadyForUse` reads the resource and the SAME ownership subject again,
   then `streamInactivitySafe` takes a full STREAM.INFO (a snapshot only), then a
   CONSUMER.INFO; `opAdmitPublish` re-reads the authority, the resources and the ledger;
   the transition's checks re-read the predecessor and the ledger and load each live
   exchange of the generation serially. About 8-10 serial requests for QoS 1, about 5 more
   for QoS 2, plus O(live exchanges), before the batch. Collapsing a re-read needs a
   consistency argument: the predecessor check compares the standing BODY, not only its
   sequence.
2. **Resume.** The session's PUBREL subject on `$MQTT_out` can be walked three times
   (`recoverInflightRegistersOnce`, `recoverDrainPubRels`, `reconcileQ2AffPubRelProofs`);
   durables are verified by `lookupConsumersMulti` and again serially in `recoverDrains`;
   `$MQTT_msgs` is looked up twice.
3. **Serial loops beside an existing multi primitive**: the resume NAK reconcile
   (`qos_track.go`, `loadMsg` per pending publish), `recoverDrains` (`lookupConsumer` and
   `loadMsg` per item), generation resource and action enumeration (one `loadLastMsgFor`
   per listed subject), the inventory traversal's per-exchange loads, and
   `inflightSourceState` on the PUBCOMP terminal path.
4. **The 15-minute sweep** walks `$MQTT_sess` about five times (expiries, orphan durables,
   acquisition verdicts, teardown adoption, references).
5. **`willClaimLoop`** walks the whole `$MQTT_willq` every 5 s on every replica; the
   schedule-based replacement is the post-alpha spike below.
6. **Startup**: 31 serial STREAM.INFO for a provisioned ten-stream account (one
   retired-origin lookup, ten preflight lookups, two per stream in `ensureBrokerStream`);
   only the preflight's ten are independent.
7. **The one-fetch PUBREL load cache** already recorded under "Deliberate non-adoptions".

### Follow-ups filed

- **Dead code and test support. DONE 2026-10-20.** The `mqttJSAStreamUpdate` decode arm
  and the `JSApiStreamUpdateT` alias serve only the tagged `updateStream` test helper, so
  removing them needs that helper moved to a raw request first; `JSApiStreamDeleteT`
  serves the untagged `jsRawDeleteStream`, already a raw request, and
  `JSApiStreamDeleteResponse` has no use beyond its alias. The client-ID hash that
  `storeSessionMsg` and the teardown receipt still put in their reply subjects is read by
  nothing, and `mqttJSAClientIDPos`/`mqttJSAIdTokenPos` have no users. Resolution: the
  hash is gone from every reply subject by deleting the parameter from the whole request
  family (`newRequestEx` and `newRequestExMultiSubjectsCID` folded away), which also
  covers a third caller the audit had not named, the accepted-CONNECT batch in
  `commitMetadataAttempt`. Every JS API reply is now `$MQTT.JSA.<node id>.<kind>.<nuid>`.
  The `SU` kind and its decode arm are deleted; `updateStream` is a raw request on the
  service connection through `prefixDomain`, witnessed by
  `TestUpdateStreamHelperTakesDomainRoute`. A census of the alias file found the whole
  JetStream API family without a production user, and six aliases are deleted:
  `JSApiStreamDeleteResponse`, `JSApiStreamNamesResponse`, `JSApiStreamUpdateResponse`,
  `JSApiStreamDeleteT`, `JSApiStreams` and `JSApiStreamUpdateT`. The two test helpers name
  `server.*` directly. Observed and left: `LEN_CR_LF` and `ServerShutdown` have no users
  but are not JetStream request surface.
- **The JetStream API error contract on no responders.** `processSub` frames a status
  message's headers and `processJSAPIReplies` JSON-decodes the framed message, so a
  header-only 503 would surface as an invalid-JSON error rather than "JetStream not
  available". Established by source; the trigger (a JetStream-disabled account behind a
  startup probe that passed) and its production reachability are UNVERIFIED. Resolution
  (2026-10-20): reproduced on a real server, with a double-ack confirmation that a
  503 satisfied; both fixed by one status classification before the per-kind decode. See
  the roadmap item and `docs/ARCHITECTURE.md`, "A publish nobody receives is answered with
  a header-only status".

## Deliberate non-adoptions

- **Direct multi/batch get:** useful for request count, but Direct Get may answer
  from a lagging replica and explicitly lacks read-after-write coherence. The
  bridge's correctness-sensitive loaders retain the leader-routed management GET;
  see [ADR-31](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-31.md).
  A future leader-routed batch API could change this decision.
- **Pinned/prioritized consumers:** pin the pull consumer as a whole, are not an
  exclusivity guarantee, and would concentrate an MQTT shared group on one member
  rather than preserve per-delivery load sharing. See
  [ADR-42](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-42.md).
- **KV or counters as record stores:** do not preserve the raw stream-sequence
  identities, arbitrary record bodies/headers, exact-sequence delete classification,
  source behavior, or cross-subject atomic batch used by the current protocols.
- **Consumer pause/reset:** current delete/recreate sites change identity,
  configuration, or ownership; they are not cursor rewinds. Pause also stops all
  delivery and therefore does not express "stop new work while draining pending".
- **Fast-ingest batch publishing:** adds a second framing protocol and is justified
  only if the roadmap benchmark proves publish ingress is the bottleneck. Atomic
  batches remain appropriate for the F-031 session-record/acceptance-fact commit.
- **Upstream MQTT PR 8416's in-memory `qos2Exchanges` map:** a per-connection,
  PI-keyed cache holding the staged PUBLISH (so PUBREL skips the load) and a nil
  entry marking "released, discard in flight" (so a retransmitted PUBREL is
  screened). Three reasons it does not transfer. It is **connection-scoped**, and
  here a PUBREL can be served by a peer, so an in-memory fact is not evidence about
  the exchange. Its duplicate detection **swallows a `max_msgs_per_subject`
  rejection**, which learns only "the subject was occupied" — ported here it would
  collapse `held`, the `delivered` tombstone and `corrupt` into one verdict, and
  misreading a tombstone as a duplicate is the case that silently loses a payload;
  `mqttStoreQoS2MsgOnce`'s create-and-reconcile loop classifies the occupant
  instead. And the **fences cannot be cached away**: `verifyQoS2HoldForRelease` is a
  re-read whose job is to catch what changed between the load and the claim.
  *Precisely one fetch could go* — a subordinate cache carrying the generation, the
  hold sequence, an owned copy of the payload and the record's **expiry origin**
  (PUBREL ages Message Expiry from `stored.Time`, so restarting it at PUBREL would
  extend the message's lifetime), with explicit eviction on PUBCOMP, teardown and PI
  reuse, could skip the load on a same-connection hit and still run the claim and the
  proof. That is a performance option, not a reason to adopt the protocol.
- **PR 8416's fire-and-forget subject purge:** an unbounded subject purge can apply
  after PI reuse and destroy a later occupant. Every removal here carries a proven
  cutoff — see F-062 in [ha-arbitration.md](ha-arbitration.md).
- **PR 8416's `mqttIsMaxMsgsPerSubjectErr` / `exceptDuplicateError`:** interpret a
  store error to infer a duplicate. `qos2_move.go` deliberately replaced that with
  structural expected-sequence CAS classification.
- **Upstream MQTT PR 8415's `LoadAndDelete` in the JS API reply dispatcher:** the
  non-atomic `Load`+`Delete` in `processJSAPIReplies` is safe here because the handler
  is registered on exactly one wildcard subscription per `mqttJSA` and nats-go
  dispatches a subscription's callbacks on a single goroutine. Splitting that
  subscription — floated in `createSubscriptions` — does **not** by itself make the
  race reachable: disjoint parts give concurrent callbacks for *different* reply
  subjects. Only an overlapping or duplicate subscription would. The inherited comment
  claiming multi-goroutine dispatch was corrected instead, and it now records the four
  preconditions a split must satisfy.

## Already better than upstream

- **Invalid staged QoS 2 records.** PR 8416 changes upstream to fail the connection
  immediately on an invalid staged message rather than deferring PUBCOMP, so client
  retries converge. `mqttProcessPubRel` is stronger and already shipped: it answers
  0x92 only after **confirmed exact-sequence cleanup**, and where cleanup cannot be
  confirmed it suppresses PUBCOMP and closes — because PUBCOMP frees the packet
  identifier, and a surviving record would then swallow the client's next PUBLISH on
  it (invariant 3, `qos2_move.go`).

## Deferred prior art, not rejected

Distinct from the non-adoptions above: these are upstream mechanisms this broker has
**not assessed against measurements of its own**, not ones it has refused.

- **PR 8415, pipelining inbound QoS 1 stores.** A per-connection ack goroutine drains
  a bounded FIFO so the readLoop no longer blocks on the JetStream round trip. Our
  inbound QoS 1 path is the pre-8415 synchronous shape. Upstream reports 2.7K → 68K
  msg/s single-connection on an R3 cluster (p99 102ms → 5ms), with 1.1–1.2x
  aggregate gain across many connections.
- **PR 8416's pipelining half**, separable from the dedup protocol rejected above:
  moving QoS 2 PUBLISH and PUBREL work behind the same FIFO and carrying PUBREC and
  PUBCOMP through it. Evaluable against **this broker's existing durable protocol**.
  Upstream reports 690/s → 18.1K/s single-connection (p99 384ms → 19ms) and
  **1.4–1.5x aggregate** across many connections — a materially better aggregate
  result than 8415's, so the two must not be quoted with one shared figure.

  **Those numbers measure the whole of PR 8416, not the half proposed here.** 8416
  attributes its single-connection gain to dropping the load and the waited delete,
  and its aggregate gain to removing per-message JetStream API round trips — i.e. to
  exactly the mechanisms rejected above. **No published measurement isolates a
  pipeline-only QoS 2 design**, so the QoS 2 case rests on the QoS 1 result plus the
  structural argument, and producing a pipeline-only number is part of the
  evaluation rather than an input to it.

Both are recorded against the roadmap's *Performance and resource audits* item,
together with the invariants any pipeline here must preserve. Treat upstream's numbers
as **comparison data, not this broker's cap or target**: this port uses real `nats.go`
connections and a standalone JSA dispatcher, and a retained PUBLISH incurs an extra
store. The roadmap's own rule applies — benchmark before changing a query path.

For the inbound QoS 2 mechanism itself, see `docs/ARCHITECTURE.md`'s inbound-move
section rather than a second description here.

**Measured 2026-10-20**: both halves were measured against this broker, with a
pipeline-only QoS 2 prototype. The depth gain reproduced; the one-exchange cost did not
resolve against the bound fixed before measuring. The user then decided for an
implementation that is adaptive per connection. See the next section.

## Inbound ack pipelining: evaluation (2026-10-20)

The §2 roadmap item "Evaluate pipelining the inbound QoS 1 (and QoS 2) ack paths" is
recorded here. Scope (lane consult 1): a committed benchmark, an experimental pipeline for
both QoS levels built only to be measured, and this record. No production code changed.

### What was measured

- **The benchmark** is `TestMQTTInboundAckThroughput`
  (`internal/mqtt/inbound_ack_throughput_test.go`, `make bench-inbound`, opt-in by
  `MQTTNB_INBOUND_ACK_BENCH=1`). Raw MQTT v5 clients, 128 B payloads, one topic per
  connection, at most W exchanges outstanding; QoS 2 answers each PUBREC with PUBREL at
  once. A fresh broker per case. Two rigs: **standalone** (one embedded nats-server, R1)
  and **r3** (an in-process three-node cluster on loopback, R3 streams: replication cost,
  not network latency).
- **Validity.** A row is reported only when (1) every response matched the client's
  ledger of what its own writes were owed, in write order, closed by a PINGREQ fence and a
  quiet check after the read-back, so no response beyond the owed ones passes (run 1 predates
  the fence and the quiet check); (2) the eventual read-back
  through an interest consumer created before the first PUBLISH found exactly the
  acknowledged messages, each once, with their exact bytes, in per-connection publish
  order (the consumer is also what keeps `$MQTT_msgs`, an interest stream, from
  discarding the stores); and (3) the request census drained cleanly. The census is a
  plain subscriber on `$JS.API.>` and `$MQTT.>` on the broker's own node: it counts every
  broker request and broker-owned publish by class (API verb and stream; holds, claims and
  tombstones by the `Nmqtt-Q2Done` header and the `.rel` token; batch members and commits),
  attributes by reply prefix, and drains by reaped holds, a flush of the broker's service
  connection, a flush of the tap and zero pending and dropped messages.
- **Temporal and ordering properties** that a throughput row cannot show are committed
  probes that run in `make full-suite` on any tree: `TestMQTTInboundAckFollowsDurability`
  (no PUBACK, PUBREC or successful PUBCOMP enqueued before the store, hold CAS or terminal
  CAS it reports had returned, checked at enqueue) and `TestMQTTInboundAckOrderUnderPressure`
  (store issuance and response order with an earlier packet's work held). They observe; they
  do not force a wrong path to misbehave.
- **Matrix and protocol, fixed in advance**: QoS 1 at (1 connection, W=1), (1, W=256),
  (8, W=256); QoS 2 likewise. Three repetitions per arm, baseline and prototype interleaved
  (B, P, B, P, B, P), one `go test` at a time. Medians of throughput and of per-repetition
  p99 (PUBACK for QoS 1, PUBCOMP for QoS 2).
- **Environment**: baseline `d22e245` plus the benchmark; prototype = the same plus
  `inbound-ack-pipeline-prototype.patch` (this directory). Go 1.26.8, nats-server v2.15.1,
  nats.go v1.54.0, linux/arm64, 8 cores (Cortex-A55 + Cortex-A76, heterogeneous), 8 GB.
  Resource figures are whole-process (client, broker and nats-servers share the test
  process).

### The prototype

`inbound-ack-pipeline-prototype.patch` (applies to this change's tree; `git apply`). One
connection's inbound QoS 0/1/2 PUBLISHes and PUBRELs become jobs in one FIFO in readLoop
receipt order, each response position reserved in the same act. An **issuer** goroutine
takes jobs strictly in FIFO order, waits for a release's fences, fans out (with the
publication passed in, not read from the readLoop's `c.pa`), runs the retained update,
waits the subs barrier, re-checks the service epoch and submits the `$MQTT_msgs` store
without awaiting it (the JSA's `storeMsg` split into submit and await, `sendq`'s single
drainer keeping submission order). A **completion** goroutine per job awaits its own
durable effects: the PubAck, the hold CAS and its reconcile, a release's terminal CAS. An
**emitter** writes ready responses from the head only. A release's load, claim, proof and
retirement classification run concurrently with other jobs; the classifications of one
connection's releases are serialized, because two overlapping ones would refuse each other
the session record transaction (`classifyQoS2ReleaseAgainstRetirement` returns Unresolved
when it is held). Each queued job takes its own incarnation lease while the packet's is
held. **Every request of the serial path is kept**: every request class on the exchange
streams (`$MQTT_msgs`, `$MQTT_qos2in`, `$MQTT_teardown`, `$MQTT_sess`, `$MQTT_inflight`,
`$MQTT_out`) matched per exchange in every case (QoS 2: 4 stores, 2 hold-stream reads, 2
hold-stream deletes and 1 retirement read per exchange in both arms; zero mismatches over
all 12 cases). Replies were reported, not compared, because a reply kind spans streams;
the only difference, in both requests and their `ML` replies, is the Will claim loop's
`$MQTT_willq` walk, which runs on wall time.

Its kill tests (in the patch, on its own decision boundaries; evidence about that tree only)
each fail on their named assertion under one mutation: P1 the issuer takes a ready job that
is not the head; P2 the emitter writes a response that is not the head's; P3, P4, P5 a
PUBACK, PUBREC or PUBCOMP marked ready before its store, hold CAS or terminal CAS returned;
P7 the classification serialization removed. P6 (every 1000th store dropped with its PUBACK
sent) fails the committed read-back. The unmutated prototype passes the kill tests and both
committed probes three times under `-race`. Sanity: 568 of 569 full-suite tests matching
`(?i)Publish|QoS2|PubRel|Retain|ReceiveMax|FlowControl|NoLocal|Expiry` pass on the
prototype; the one failure, `TestQoS2InEveryWriteCarriesItsExpectation`, is the prototype's
asynchronous hold create bypassing the session manager's instance seam the test counts on.

Excluded from the prototype, and required of any implementation: no proof that nothing is
acknowledged after a DISCONNECT or takeover; reply registrations released on close only by
their timeout; no proof the workers cannot outlive the JSA at shutdown; no QoS 1 Receive
Maximum accounting (queued QoS 1 is newly observable work keyed on packet identifiers);
outbound paths untouched; concurrent releases of one session on one broker audited only as
far as the classifier.

### The invariants, one by one

The roadmap bullet's invariant list, with what the serial path does, what the prototype did,
and what an implementation must prove. "Committed" is a test in this change that runs on any
tree; "prototype kill" is evidence from the prototype tree's own seams only, which an
implementation must re-establish against its own decision boundaries.

| # | Invariant | Serial path | Prototype | An implementation must prove |
|---|---|---|---|---|
| 1 | `subsBarrier.wait` before every `$MQTT_msgs` store | on the readLoop, per store | on the issuer, per job | committed: none direct; review of every submit site |
| 2 | Store submission in packet order | trivially serial | one issuer in FIFO order, `sendq`'s one drainer | committed: order probe; prototype kill P1 |
| 3 | One FIFO, positions reserved at receipt | trivially serial | every response, no-store answers included, is a job | committed: ledger, durability and order probes; prototype kills P2-P5 |
| 4 | Owned queued data | readLoop scratch, used at once | deep-copied publication; the fan-out takes it as an argument; `pubOrigin` is the issuer's | review: nothing after receipt reads `c.pa` or `c.mqtt.pp` |
| 5 | No Local stamp and retained flag captured before handoff | taken on the readLoop | carried by the job | review |
| 6 | Message Expiry ages while queued | no queue | aged once, at issuance, from receipt (PUBLISH) or the hold's store time (release) | a test with a message that expires in the queue (not written) |
| 7 | Wills synchronous | yes | unchanged path | review |
| 8 | Receive Maximum keyed on packet identifiers | QoS 2 only, because QoS 1 is synchronous | QoS 2 counts held plus queued PUBLISHes; QoS 1 **not implemented** | unique outstanding QoS 1 identifiers, DUP and non-DUP reuse rules, with tests |
| 9 | A worker failure closes through the readLoop | the readLoop's error exit | socket close after a v5 DISCONNECT with the mapped reason; no worker calls `closeConnection` | the existing retained-failure tests pass on the prototype; a failure-at-depth test |
| 10 | No response after DISCONNECT or takeover | serial | **excluded** | a suppression proof and tests |
| 11 | Bounded window, every reply registration released | n/a | bound 1024; release on close by timeout only (**excluded**) | release on close and shutdown, with tests |
| 12 | The worker has an owner in the shutdown graph | n/a | per-job incarnation lease, drained by retirement; outliving the JSA **excluded** | a wait group and the proof |
| 13 | Store failure closes the connection, with F-060 | retained failure joined with the store's, connection closed | same | the existing F-060 tests |
| 14 | Trace at emission | at enqueue | at emission | review |
| 15 | Concurrent releases of one session (found here) | one at a time | classifications serialized per connection; each job leases | prototype kill P7; an audit of the claim, the proof and `beginQoS2ReleaseOp` |

### Results

Three measurements, all valid (every repetition's ledger, read-back and census passed; the
census matched between arms, zero mismatches, in all three). Runs 1 and 2 are the full matrix
under the original protocol, unpinned; they differ only in that run 2 used the final
benchmark, whose added end-of-ledger fence and quiet check run after the measurement
endpoint. Medians of three; ranges are throughput min-max; p99 is PUBACK for QoS 1, PUBCOMP
for QoS 2.

**Depth and aggregate reproduced** (run 1 / run 2):

| Rig | QoS | Conns | W | Baseline /s | Prototype /s | Ratio | p99 baseline | p99 prototype |
|---|---|---|---|---|---|---|---|---|
| standalone | 1 | 1 | 256 | 2509 / 2494 | 22623 / 23198 | 9.0 / 9.3 | 134 / 128 ms | 24.5 / 24.6 ms |
| standalone | 1 | 8 | 256 | 16670 / 17191 | 36074 / 34655 | 2.2 / 2.0 | 150 / 150 ms | 118 / 155 ms |
| standalone | 2 | 1 | 256 | 300 / 302 | 1949 / 1965 | 6.5 / 6.5 | 975 / 957 ms | 198 / 191 ms |
| standalone | 2 | 8 | 256 | 2072 / 2091 | 3317 / 3255 | 1.6 / 1.6 | 1022 / 1008 ms | 882 / 941 ms |
| r3 | 1 | 1 | 256 | 1028 / 1000 | 22775 / 23491 | 22 / 23 | 292 / 299 ms | 22.5 / 25.7 ms |
| r3 | 1 | 8 | 256 | 11027 / 8886 | 33140 / 33889 | 3.0 / 3.8 | 238 / 270 ms | 114 / 123 ms |
| r3 | 2 | 1 | 256 | 160 / 217 | 1357 / 1411 | 8.5 / 6.5 | 1715 / 1271 ms | 239 / 231 ms |
| r3 | 2 | 8 | 256 | 1163 / 1206 | 2436 / 2415 | 2.1 / 2.0 | 1792 / 1736 ms | 1221 / 1379 ms |

**One exchange at a time did not reproduce** (W=1, one connection):

| Rig | QoS | Run 1 baseline / prototype | Run 1 change | Run 2 baseline / prototype | Run 2 change |
|---|---|---|---|---|---|
| standalone | 1 | 1794 (1731-1847) / 1557 | -13%, p99 +9% | 1767 (1755-1818) / 1607 | -9.1%, p99 +7.9% |
| standalone | 2 | 271 (263-272) / 256 | -5.5%, p99 +2.7% | 271 (267-273) / 257 | -5.2%, p99 +3.4% |
| r3 | 1 | 1168 (860-1185) / 819 | -30%, p99 +33% | 868 (856-868) / 826 | -4.8%, p99 +5.3% |
| r3 | 2 | 161 (153-200) / 143 | -11.2%, p99 +10.3% | 196 (195-199) / 179 | -8.7%, p99 +6.1% |

**The pinned W=1 extension** (amendment frozen 2026-10-20 23:40, before its data; lane
consult 3): ten paired repetitions of the four W=1 cases, pair order balanced five and
five, both arms under `taskset -c 4-7` (the Cortex-A76 cores) with `GOMAXPROCS=4`. Point
estimate = ratio of medians, bounds unchanged (throughput >= 0.90, p99 <= 1.10); a guard
on the ten paired ratios classes a metric as clearly passing, clearly failing, or within
observed noise (a conservative guard, not a confidence interval):

| Rig | QoS | Throughput ratio | Paired range | Guard | p99 ratio | Paired range | Guard |
|---|---|---|---|---|---|---|---|
| standalone | 1 | 0.925 (3240 / 2998) | 0.878-0.956 | noise | 1.086 | 1.060-1.128 | noise |
| standalone | 2 | 0.973 (480 / 468) | 0.947-0.992 | pass | 1.042 | 0.935-1.115 | noise |
| r3 | 1 | 1.114 (1620 / 1805) | 0.947-1.287 | pass | 0.912 | 0.781-1.033 | pass |
| r3 | 2 | 0.956 (318 / 304) | 0.779-1.187 | noise | 1.030 | 0.817-1.270 | noise |

### Outcome

The criteria, fixed before any prototype number: **go** for a rig when, at one connection,
W=256 throughput is at least 3x the baseline's and p99 is no worse, at W=1 neither throughput
nor p99 is worse by more than 10%, and the 8-connection aggregate is not worse by more than
10%; a QoS level is go only if both rigs are.

- **Run 1: valid no-go** for both QoS levels (QoS 1 fails W=1 on both rigs; QoS 2 on r3, by
  1.2 points of throughput and 0.3 of p99).
- **Run 2: valid go** for both QoS levels (every criterion passes on both rigs).
- **The extension: unresolved** for both QoS levels. Every point estimate passes the
  bound, but for each QoS level at least one W=1 metric has paired ratios on both sides of
  it, which the frozen rule classes as unresolved.

**Decision (2026-10-21, the user).** The measurement above stayed unresolved; the user
decided GO for an implementation that is adaptive per connection: a client with nothing
outstanding keeps today's direct path, and the pipeline engages only once a second packet
arrives before the first is answered, so the one-exchange cost lands only on connections
that also get the depth gain. A per-installation switch was declined (one setting would be
wrong for a fleet mixing one-at-a-time devices and streaming gateways). The implementation
item in the roadmap carries the one-exchange bound as its acceptance criterion.

**So the measured evaluation is UNRESOLVED.** Contradictory valid runs are inconclusive, a
disposition the original criteria did not have and the amendment added; calling runs 1 and
2 pilots is the extension's retrospective designation, not a reason to discard either
verdict. No systematic difference between the runs is known: the final benchmark's
additions run after the measurement endpoint. Scheduling across the heterogeneous cores is
the hypothesis that motivated pinning, not a diagnosis; pinned, the baseline itself ran
faster (QoS 1 standalone 3240/s against 1767-1794/s unpinned), so the pinned and unpinned
numbers are not one series. A go would in any case have combined unpinned depth evidence
with pinned W=1 evidence, and would not have shown that unrestricted scheduling meets the
W=1 bound.

**What the numbers do show.** A pipelined path removes the per-connection round-trip cap:
6.5-23x at a window of 256 on one connection and 1.6-3.8x across eight, reproduced in both
runs. On one connection at that window p99 fell 4.9-13x with the queueing the pipeline removes;
across eight connections p99 fell in every case but one (standalone QoS 1, run 2: 150 to
155 ms, +3%). Its cost to a client that keeps one
exchange outstanding (every exchange crosses three goroutines instead of one) lies, across
the three measurements, between an 11% gain and a 13% loss in throughput (30% in one
unpinned run), straddling the 10% bound; this hardware does not resolve it at that bound.
What would resolve it: an explicit decision that a W=1 cost of that size is acceptable for
the depth gain (a product choice, not a measurement), a design whose W=1 cost is clearly
inside the bound (for example one that answers on the readLoop while nothing is queued and
enters the pipeline only when a second exchange arrives: an untested hypothesis), or a
measurement with enough power on quieter hardware. Any implementation carries the whole
invariant list in the roadmap bullet and the prototype's exclusions, and re-establishes every
kill against its own decision boundaries; the prototype's kill evidence does not transfer.

**Implemented (2026-10-21).** The adaptive design the user decided on landed as
`internal/mqtt/inbound_pipeline.go` (docs/ARCHITECTURE.md, "The inbound acknowledgement
pipeline"): a connection with nothing outstanding and nothing more buffered runs each
PUBLISH or PUBREL inline, on the same steps the pipeline runs. Measured under the frozen
W=1 rule against `87d1682` plus this benchmark's two new 64-connection W=1 rows: candidate
1 failed one of eight cases (r3, QoS 1, 64 connections, pooled throughput 0.894; an A/A
control read 0.998); with the direct path's per-exchange allocations removed, candidate 2
passed all eight (throughput ratios 0.980-1.003, p99 0.984-1.022). At W=256, descriptive,
one pair each: QoS 1 one connection 8.6x standalone and 15.5x r3, eight connections 2.6x
and 3.4x; QoS 2 one connection 4.4x and 4.9x, eight connections 1.4x and 2.1x. Those
ratios are not comparable with the prototype's 6.5-8.5x for QoS 2, because the baseline
they divide by moved: these runs used the pinned protocol (`taskset -c 4-7`,
`GOMAXPROCS=4`), and pinned the serial path itself ran faster (QoS 2 one connection 502/s
standalone and 270/s r3, against 300/s and 160-217/s unpinned in the evaluation's depth
runs). In absolute throughput the implementation is above the prototype on standalone (QoS
2 2203/s against 1949-1965/s; QoS 1 39456/s against 22623-23198/s) and level with it on r3
(QoS 2 1334/s against 1357-1411/s). Whether the FIFO-ordered leases cost QoS 2 depth was
not measured (the same-identifier chain adds nothing to this bench, which never reuses an
identifier, and the prototype serialized a connection's retirement classifications too).
The prototype patch stays as the evaluation's artifact; it does not describe the
implementation.

## JetStream path benchmark (2026-10-22)

The §2 roadmap item "Benchmark the current and proposed JetStream paths" is recorded here.
Scope (lane consult 1, option B): measure every named load, change each candidate that shows
an improvement AND has a consistency argument, record one disposition per candidate, file
no new bullets. The gate for an overlap change (lane consult 2): proceeding paths unchanged
in requests; a refusal, error or restart path may issue only read-only requests the
proceeding round would issue, its cost stated per class; recurring cost under AckWait
re-offer accepted.

**What finally changed** (lane consults 4 to 6): the instruments, Q1 (admission
readiness reads the name ownership once), Q6 (the no-wait consumer delete takes the
domain route) and Q5's nil-ConsumerInfo correction. Q2, Q3 and Q4 (overlapping
readiness, traversal and composition reads) were measured and not adopted because their
benefits did not justify their refusal-path costs and hot-path complexity; Q5's
resume-reconcile rounds and Q7's preflight round were measured and not adopted on
risk/benefit while F-126 (below) was open. Neither removal names a cause of F-126, and
neither is a fix for it. The first measurement below is of the item's FIRST, larger
change (all seven candidates, tree `bb432dba`); the reduced tree's own rows follow it
("The final change, measured"), and nothing measured on the larger tree is a claim about
the reduced one.

### Method

- **Instruments** (docs/ARCHITECTURE.md, "Measuring the request paths"): the request census
  (an ordered plain subscriber: counts and bytes by class, request/reply pairing), the
  per-JSA request observer (send and delivery order: depth and latency) with the reply hold
  that makes overlap deterministic in the committed tests, and the server's API counter.
- **Loads**: `make bench-jspaths`, a fresh broker per load, 20 operations per row unless
  stated (window: 32 deliveries; reconnect: 20 one-operation windows; session recovery:
  5 rounds of a subscriber offline with 8 QoS 1 and 2 QoS 2 messages published to it and
  2 PUBRELs owed, warm and after a broker restart; sweep tick: 50 offline sessions;
  startup: 3 restarts of a provisioned account). Two rigs: standalone (one embedded
  server, R1) and r3 (three in-process nodes, R3 streams).
- **Arms**: baseline = `97ffa57` plus only the measurement seams, census and bench;
  change = the full tree. Pinned (`taskset -c 4-7`, `GOMAXPROCS=4`), arms interleaved run
  by run (B C C B for the full matrix, then B C C B B C for the publish, window and
  reconnect loads). Go 1.26.8, nats-server v2.15.1, nats.go v1.54.0, linux/arm64.
- **Validity**: a row counts only if its window closed cleanly (see ARCHITECTURE). Every row
  reported below was valid. r3 session recovery under the pinned protocol completed in
  one of four runs of the first measurement (a baseline run; the others, on both arms, never
  reached the idle point) and in both baseline runs but neither change run of the second (one
  met a PUBREL re-offered after the counted completions, which the harness did not yet
  answer; one an exclusion reclaim that stayed armed past the bound). Its r3 figures are the
  baseline's, descriptive only.

### What each operation costs (the first change's arm, per operation)

Requests are JS API requests the broker made (stores, batch members and acknowledgements are
not requests and are counted separately in the census rows); bytes are request, publication
and reply bytes together; time is wall time per operation under the pinned protocol.

| Load | Requests standalone / r3 | Server API counter | Bytes standalone / r3 | ms standalone / r3 |
|---|---|---|---|---|
| idle window (the accounting's self-check) | 0 / 0 | 0 / 0 | 0 / 0 | |
| connect (new persistent session) | 19 / 19 | 9.0 / 10.1 | 16.8 k / 17.1 k | 25.7 / 14.4 |
| disconnect | 0 / 0 (one session store) | 0 / 0 | 0.4 k / 0.5 k | <0.1 |
| subscribe QoS 1 / QoS 2 | 9.0 / 9.5 (both rigs) | 5.1 / 5.25 | 12.8 k / 20.1 k | 11.8 / 11.6 (r3 75 / 79) |
| publish QoS 1 (inbound store + delivery + PUBACK) | 105.0 / 104.3 | 15.75 | 104 k / 103 k | 65.1 / 56.0 |
| publish QoS 2 | 138.7 / 136.7 | 18.6 | 147 k / 149 k | 144 / 139 |
| publish, window of 32 | 83.0-84.1 / 83.0 per delivery | 11.0-11.4 | 92 k / 94 k | 48.5 / 38.0 |
| reconnect (4 subscriptions, nothing pending) | 47 / 47 | 9.0 | 55 k / 58 k | 34.6 / 26.7 |
| retained store | 0 / 0 (two stores) | 0 | 0.5 k / 0.6 k | 0.6 / 1.5 |
| retained replay (new subscriber) | 190.8 / 190.8 | 32.7 | 201 k / 204 k | 104 / 119 |
| session recovery, warm (per round, descriptive) | 1 450-1 560 | 205-227 | about 1.5 M | 590-720 |
| session recovery, cold (descriptive) | 1 420-1 530 | 194-216 | about 1.5 M | 600-700 |
| sweep tick (per offline session) | 14.4 / 14.4 | 0.36 | 15.8 k / 16.5 k | 4.6 / 6.7 |
| broker start (provisioned account) | 45 / 45 | 45 / 16 | 43 k / 54 k | 25 / 31 |

These are the second of two complete measurements. The first, taken before the window
close waited for a broker start's own sweep pass and before the API counter's accounting
was corrected, agreed on every request count within the first window's startup tail
(connect 19.3 for 19, startup 45.7-46.7 for 45) and is superseded for the API column.

**The audit's static estimates were low by an order of magnitude on the delivery path.** It
estimated 8-10 serial requests for a QoS 1 admission; a QoS 1 publish costs about 105
requests end to end, about 98 of them message GETs on `$MQTT_inflight` (the F-105 records:
readiness, composition, the write-boundary checks, the inventory traversal, the send
section's acquire and drain, the PUBACK's advance and terminal path, the exclusion reclaim).
Startup measured 45 requests (31 STREAM.INFO, the ephemeral consumers and the startup
sweep pass), not 31.

**The server's API counter is not the request count.** It reported 5-33 per operation where
the census counted 9-191 (and the same 45 at startup, where no message GET is made): the
server answers a successful message GET outside its API layer and never counts it (see
ARCHITECTURE, "JetStream behaviors this design depends on"). The r3 startup reading (16)
is the counter of the account as the cluster reports it, which this measurement does not
reconcile further.

**Found by the measurement, and not changed here.** Nearly every QoS 1 delivery's first
admission attempt loses its CAS on the source consumer's resource record and is retried
("mutates resource ... at sequence N but it stands at N+12"): the previous delivery's ending
is still releasing its hold on that same record. Every admission and every ending of a
consumer's deliveries writes that one record, so it serializes them. Unpinned, 19 of 20
deliveries lost the first attempt on both arms; pinned, about 0.2 per delivery on the
baseline and 0.75 with the change on standalone (r3: 0 and 0.1), because the change's
admission composes sooner. Net, the change's requests and wall time are still lower (table
below). Reducing it would be a change to how dependency edges are recorded, not a query
change, and is outside this item.

### Candidates and dispositions

Measured change per operation, base -> change, medians (requests; time ratio
change/base, descriptive), as measured on the first change. Each overlap was witnessed
there by a hold test (the reply hold parks a round's head, and a pipelined round shows
every read sent); those tests went with the candidates not adopted. The bench's depth
column is not that evidence, because the per-load maximum is set by rounds that were
already pipelined (the duty batch, the resume barrier).

| # | Candidate | Disposition | Measured |
|---|---|---|---|
| 1 | Admission readiness's duplicate ownership read | **Changed** (Q1): read once, with the resource-name, stream and client equalities explicit | -1 read per dependency per attempt (committed test; refusal table below) |
| 1 | Admission readiness's five serial reads | **Measured, not adopted** (Q2: two rounds) | readiness round trips per delivery QoS 1 5 -> 2, QoS 2 10 -> 2; readiness 1.8-2.6 ms -> 1.0-1.3 ms (QoS 1), 2.4-4.5 -> 1.8-1.9 ms (QoS 2); refusals read up to the admitting round (ownership absent 1 -> 3, QoS 2 1 -> 7) on every AckWait re-offer |
| 1 | Per-delivery STREAM.INFO | **Kept** | the only fresh evidence of a limit a later re-add substitutes |
| 1 | The three per-delivery operations' composition reads | **Measured, not adopted** (Q4: `opAdmitPublish` one round, `opAcquireAction` one round, `opAdvanceExchangeAck` two) | overlap only; a refused admission read its whole round (4 against 1); the source-resource CAS retry rate rose (0.2 -> 0.75 per delivery, standalone) |
| 1 | Write-boundary re-reads (predecessor body, ledger, resource, ownership, registers) | **Kept** | the write-boundary rule; consult 1 named the predecessor read |
| 1 | The two `terminalSourcePair` exchange reads | **Kept** | a drain finalization with its own I/O runs between them |
| 2 | Resume's PUBREL walks A/B/C; srcCache, `inflightSourceState`, post-interest consumer re-reads | **Kept** | intervening writers; different moments with stated purposes. The affine walk is chapter P, not exercised |
| 3 | The inventory traversal's per-exchange reads | **Measured, not adopted** (Q3: one round per duty chunk) | overlap only; a restart read the chunk's exchanges after the crossing (5 exchange reads against 2 serially) |
| 3 | Resume's redelivery reconcile and `recoverDrains` loops | **Measured, not adopted** (Q5: rounds of 1024; FirstSeq read only when needed); its nil-ConsumerInfo correction kept | reconnect 48 -> 47 requests (both rigs); connect 20.1 -> 19.3 and retained replay 191.7 -> 190.7 (a new session's CONNECT runs the same reconcile) |
| 3 | `inflightSourceState`'s conditional message GET | **Kept** | unconditional issue would add requests to the proceeding path |
| 4 | The sweep's `$MQTT_sess` walks | **Kept** | 14.4 requests per offline session per tick, measured; merging the walks no invariant forbids moves verdict execution ahead of two arms, for at most one read per record per tick |
| 5 | `willClaimLoop` | **Excluded** | owned by its post-alpha ADR bullet |
| 6 | Startup's preflight lookups | **Measured, not adopted** (Q7: one round, judged in table order) | 46 requests either way; start time 19.0 -> 15.1 ms standalone, 23.5 -> 21.1 ms r3 (descriptive) |
| 7 | The one-fetch PUBREL load cache | **Carried forward** from "Deliberate non-adoptions", not re-measured | |

Also changed: the no-wait consumer delete now takes the `-js-domain` route (found by the
census research; whether a current-format production path issues it was not established, so
no leak is claimed).

**Per load, base -> the first change** (requests per operation, medians; time per operation,
descriptive). Standalone: connect 20 -> 19, 26.1 -> 25.7 ms; publish QoS 1 105.5 ->
105.05, 66.5 -> 65.1 ms; publish QoS 2 140.45 -> 138.65, 150.8 -> 143.6 ms; window 83.97
-> 82.97 or 84.09 per delivery (the admission retry above), 53.8 -> 48.5 ms; reconnect
48 -> 47, 34.4 -> 34.6 ms; retained replay 191.7 -> 190.75. r3: connect 20 -> 19;
publish QoS 1 105.25 -> 104.3, 68.8 -> 56.0 ms; publish QoS 2 140.15 -> 136.65, 155.4 ->
138.6 ms; window 83.97 -> 82.97, 49.0 -> 38.0 ms; reconnect 48 -> 47, 24.9 -> 26.7 ms.
Subscribe, retained store, sweep tick and startup: identical request counts on both
rigs; startup time 29.0 -> 24.9 ms standalone, 37.2 -> 30.9 ms r3. The first measurement
put the r3 publish time ratios at 1.01-1.07 and this one at 0.78-0.89: run-to-run spread
on this hardware, recorded rather than claimed either way. Session recovery is
descriptive: requests vary by round with the exclusion reclaim's backoff phase (warm 1
451-1 559 on the baseline against 1 526 on the change, cold 1 442-1 524 against 1 421),
and some runs ended early (see the validity note above and the paragraph below).

**F-126: a recovering session's PUBREC advance can exhaust its budget, on the baseline
too.** In the session-recovery load the PUBREC advance (`intendPubRelSlot`) can lose all
eight attempts, and the broker closes the recovering connection (DISCONNECT 0x80). Every
failed attempt was a CAS conflict won principally by the same session's publish
admissions and acquisitions and some PUBREL acquisitions. Two batches, kept apart
because their trees and instrumentation differ:

- On the first, larger change, pinned and interleaved: 5 of 32 change runs against 0 of
  28 baseline runs, 1 of 6 with Q4 reverted. Traced PUBREC advances (QoS 2 to
  PubRecObserved): baseline 160 resolved with consecutive losses {0: 83, 1: 33, 2: 27,
  3: 6, 4: 6, 5: 1, 6: 2, 7: 2}; change 136 resolved with {0: 90, 1: 24, 2: 9, 3: 6, 4:
  3, 5: 2, 6: 2} plus 2 unresolved runs of 8. The 0 of 28 is a true observation; read as
  an exhaustion-free baseline, it was wrong.
- Screen `screen1` (30 randomized balanced blocks, seed 20261022, one test binary per arm,
  a durable ledger, every trial classified): the baseline (`97ffa57` plus inert seams)
  exhausted in 4 of 30 trials, an intermediate version of the change (Q2 to Q4 already
  removed) in 6 of 30, nothing else failed. No difference was established at this sample
  size, a substantial increase remains compatible with the data (two-sided 90% interval
  about -9.5 to +22.6 points), and non-inferiority was not demonstrated.

The defect is therefore the broker's, not this item's: filed as **F-126**
(`docs/findings/runtime-correctness.md`), accepted as a nonblocking residual
(`docs/CONFORMANCE.md`, "F-126"). The 300-run gate planned when the baseline looked
clean was superseded, not passed: with a baseline failing about one trial in eight it
could not bound an increase to one point at any feasible size. The item then dropped
Q5's optimization and Q7 as well, so its final change alters recovery's request path
only by Q1's one fewer read per dependency; whether that alters F-126's frequency was
not measured, and no further comparative study is owed.

### The final change, measured

The reduced tree (`85285f39`) against the same baseline arm, both carrying identical
instruments, pinned, B C C B over the full matrix on both rigs; medians of two runs per arm,
requests per operation (time descriptive, ms):

| Load | Standalone requests | Standalone ms | r3 requests | r3 ms |
|---|---|---|---|---|
| idle | 0 / 0 | | 0 / 0 | |
| connect | 20 / 20 | 25.9 / 25.3 | 20 / 20 | 12.8 / 14.8 |
| subscribe QoS 1 / QoS 2 | 9.0 / 9.0, 9.5 / 9.5 | 11.8 / 12.1, 13.1 / 12.4 | same | 73.8 / 74.3, 78.4 / 78.9 |
| publish QoS 1 | 105.5 / 104.5 | 67.1 / 69.6 | 105.25 / 104.25 | 62.0 / 68.1 |
| publish QoS 2 | 139.7 / 137.6 | 150.3 / 149.3 | 137.1 / 137.3 | 139.7 / 154.7 |
| publish, window of 32 | 84.0 / 83.0 | 53.2 / 53.1 | 84.0 / 83.0 | 50.4 / 43.8 |
| reconnect | 48 / 48 | 34.8 / 34.5 | 48 / 48 | 25.8 / 27.7 |
| retained replay | 191.8 / 191.7 | 106.1 / 107.8 | 191.8 / 191.75 | 115.3 / 123.5 |
| sweep tick (per offline session) | 14.37 / 14.37 | 5.2 / 5.2 | 14.36 / 14.37 | 6.8 / 6.2 |
| broker start | 45 / 45 | 29.3 / 27.7 | 45 / 45 | 36.3 / 36.6 |

Q1's read is the only request change: about one fewer per QoS 1 delivery, up to two per
QoS 2 (r3's QoS 2 median moved by the admission-retry noise above). The r3 publish times
read about 10% higher on the change in this pair of runs; the earlier measurements on
this hardware put the same ratio anywhere from 0.78 to 1.07, so it is recorded, not
claimed. Session recovery exhausted F-126's budget in some runs on both arms (the r3
recovery runs on the change did not complete); its counts are descriptive.

**Refusal costs, readiness reads per admission attempt** (the bench's `refusals` case,
final change against the baseline, QoS 1 unless stated): admits 5 -> 4; ownership absent
1 -> 1; ownership names another client 3 -> 2; ownership retired 3 -> 2; resource absent
2 -> 2; resource names another consumer 3 -> 2 (the second, client-checked ownership
read follows the resource's name, so on the baseline it reads the other name's subject);
stream inactivity limit 4 -> 3; consumer absent 5 -> 4; QoS 2 admits 10 -> 8; QoS 2 with
the source's ownership absent 1 -> 1. No refusal reads more than it did, and each still
recurs on AckWait re-offer, as before.

## Version and forward-compatibility actions

nats-server 2.14.5 fixes potential data loss when an idempotent stream create is
handled while an offline node catches up from a metadata snapshot. The bridge's
create-or-validate provisioning makes that patch directly relevant; see the
[2.14.5 release](https://github.com/nats-io/nats-server/releases/tag/v2.14.5). That
made 2.14.5 the floor rather than 2.14.4, where the term fence first appeared.

**The external-server floor is now 2.15.1**, raised from 2.15.0 on 2026-10-11 together
with the pin (2.15.0 itself was raised from 2.14.5 on 2026-09-09; see the compatibility
boundary in `docs/ARCHITECTURE.md`). The 2.14.5 reasoning above is retained as the
provenance of an earlier floor, not as the current one; every fix it names is carried
forward by the higher floor.

Upstream MQTT PR **8415 is merged** (it landed between v2.15.0-preview.1 and
v2.15.0-RC.1; status corrected 2026-09-09). PR **8416** was merged on 2026-09-17 and is
**released in v2.15.1** (it is absent from v2.15.0; status corrected 2026-10-11). Neither
fact reaches this repository: a `go.mod` bump does **not** update
`internal/mqtt/mqtt.go`, which is a *copy*
of upstream's `server/mqtt.go` taken before either PR existed, and this broker never runs
the dependency's own MQTT server. Tracking these is a **manual source-port audit**, not
something a version bump does for us. Their dispositions are above: two protocol
mechanisms rejected, one dispatcher change declined, and both pipelining halves deferred
pending our own benchmark.

**The 2.15.1 source-port triage (2026-10-11).** Every `server/mqtt.go` change between
v2.15.0 and v2.15.1 was checked against our copy, reading the corresponding function from
its entry:

- **#8692, packet length bounded before buffering: applicable**, filed as **F-120** in
  `docs/ROADMAP.md`. A connected MQTT 3.1.1 client's packet length is unbounded until the
  packet is complete, because `maxInPacketSize` is set only for MQTT 5. **Ported
  2026-10-14** (see the F-120 bullet), with the size check ordered before the
  remaining-length check to keep v5's Packet Too Large disposition.
- **#8692, a PUBREC for a packet identifier not delivered as QoS 2: not applicable.**
  Upstream stored a PUBREL for it. Our copy stores none: an unknown identifier is
  ignored, and a QoS 1 entry fails `mqttPhaseForAck` and rolls the PUBREC back, closing
  the connection, which the protocol permits for a client that breaks it. A drained QoS 1
  source records its PUBREL intent before that refusal, but the intent is the "may owe"
  form that recovery settles by a presence check (`mqtt.go`, the drained-source arm of
  `mqttProcessPublishReceived`), and with no PUBREL stored the QoS 1 payload is
  redelivered.
- **#8671, a reload zeroing every session's `max_ack_pending`: not applicable.** This
  broker has no option reload, and every session and consumer creation site falls back to
  the default when the option is unset.
- **#8692's partial-packet copy, #8624 and #8625:** performance only; #8624 and #8625
  build on the 8415/8416 pipeline this copy predates. They stay with the pipelining item
  in `docs/ROADMAP.md` ("Performance and resource audits").

## F-095

**A server-side default caps consumers per stream, and no stream or account setting this
broker can read opts out of it.** Filed 2026-09-09 by the nats-server v2.15.0-RC.1
dependency audit. Filed as an alpha blocker; the roadmap entry is in §0.3. **Current
release treatment (2026-10-12): not an alpha blocker.** The decision below was taken as
option (a), and the limitation remains a stated residual in `docs/CONFORMANCE.md`.

**Decided 2026-10-12 (convergence triage): option (a), a deployment prerequisite.** That
closes the policy choice, not the limitation: the broker still cannot read the server
default through its own API access, and the `docs/CONFORMANCE.md` residual stays. The
conformance environments set `default_max_consumers: -1` and `test/conformance/run.sh`
checks the effective `/varz` value; the N+1 behavioral experiment was not performed by the
decision.

**The mechanism.** From the v2.15.0 line the server carries a `default_max_consumers`
option under `jetstream { limits { … } }` (`server/opts.go`), and when it is left at zero
the server substitutes `JSDefaultMaxConsumersPerStream` = **1000**
(`server/jetstream_api.go`). Both consumer-create paths apply it — standalone in
`addConsumerWithAssignment` and clustered in `jsClusteredConsumerRequest` — under the same
condition: the stream's `MaxConsumers` and the account tier's `MaxConsumers` must **both**
be `<= 0`. Upstream's own comment on both sites is explicit that this is deliberate:

> Only apply default limits if the user specified neither stream nor account limit.
> An unlimited (-1) stream or account limit counts as not set, since that is also
> the default for accounts, so the default can't be opted out of per stream or account.

So asking for unlimited is not an opt-out; it is the trigger.

**Why it lands on this broker specifically — the canonical inventory.** Four of the
broker-owned stream validators *require* `MaxConsumers <= 0`, each with the reasoning that
a cap eventually rejects a legitimate operation. Other documents refer to this list rather
than repeating it, so it does not drift:

| Stream | Validator | Consumer population that accumulates |
| --- | --- | --- |
| `$MQTT_msgs` | `validateMsgsStream`, `internal/mqtt/stream_prereqs.go:275` | one durable per subscription, shared group and drain, across every session on the account |
| `$MQTT_out` | `validateOutStream`, `internal/mqtt/qos2_shared_affine.go:1698` | per-session PUBREL and affine durables |
| `$MQTT_rmsgs` | `validateRetainedStream`, `internal/mqtt/stream_prereqs.go:354` | one ephemeral per broker |
| `$MQTT_sess` | `validateSessStream`, `internal/mqtt/sess_recordtx.go:579` | one ephemeral session-record watch per broker |

`$MQTT_qos2assign` is **not** in this list: it has no consumers at all and waives
`MaxConsumers`, as the stream prerequisites table in `docs/ARCHITECTURE.md` records. The
shape the broker refuses to run without is exactly the shape that arms the new default.

**What actually trips, and what does not.** The cap is charged on **new consumer
admission only**: both create paths skip the count entirely when the request names a
consumer that already has an assignment (`oname == _EMPTY_ ||
js.consumerAssignmentOrInflight(…) == nil`). So a full stream does **not** break steady
state, and it does not categorically refuse a resume whose durables are all intact — those
creates are adopts, and adopts are exempt. What fails is any operation that must create a
consumer that is not there yet, on a stream already at 1000:

- On `$MQTT_msgs`, whose durables accumulate one per subscription, shared group and drain
  across every session on the account: a SUBSCRIBE is refused with
  `JSMaximumConsumersLimitErr` (10026). This is the primary and most observable symptom.
- On `$MQTT_out`, whose per-session PUBREL and affine durables scale with concurrent
  sessions rather than subscriptions: a new QoS 2 SUBSCRIBE fails directly, and a
  restoration that has to create a **missing** durable for a saved filter fails its
  resume. A recovery with no such filter can instead fail *after* a successful CONNACK —
  PUBREL setup failure is logged and drains are withheld, and `resendPendingRedelivery`
  logs affine and PUBREL failures — which is the accepted-but-stalled outcome, less
  observable than a refused packet and worth naming separately.

The two streams are independent axes. In every case every stream setting conforms,
startup validation passed, and nothing in the stream shape explains the refusal.

**Why it is not detectable from a client connection.** The option is server-side and
appears in neither `STREAM.INFO` nor `$JS.API.INFO`. `JetStreamAccountLimits.MaxConsumers`
is reachable — the bridge already calls `AccountInfo` at startup
(`internal/backend/backend.go:62`) — but it is the *other* input to the `<= 0` test, not
the server default: a tier value of `-1` or `0` is precisely the case in which the
invisible 1000 applies, and the bridge cannot tell that from a peer configured `-1`.
Reading the option itself needs `$SYS` VARZ, which the bridge's client connection has no
claim to. The one client-visible discriminator is an account tier `max_consumers > 0`,
which disables the default — but that is itself a cap, so "detected" and "safe" are not
the same answer.

**Where the bump left it.** A documented deployment prerequisite beside the cluster-wide
version floor in `docs/ARCHITECTURE.md`, with its own bullet in "JetStream behaviors this
design depends on", and a stated residual under the stream-prerequisites section of
`docs/CONFORMANCE.md`. Every JetStream peer must be configured
`jetstream { limits { default_max_consumers: -1 } }`, or the account tier must carry an
explicit positive `max_consumers` above the deployment's working set.

**That instruction is unconditional, and making it so is why the deployment floor was
raised to 2.15.0** (owner's decision, 2026-09-09). Written against the old 2.14.5 floor it
needed a version boundary, and two review rounds were spent getting that boundary wrong in
opposite directions: first stated for every peer, which hands the key to a 2.14.5 server
that never had it; then scoped to "the v2.15.0 line", which still includes
v2.15.0-**preview.1**, where `default_max_consumers` and `JSDefaultMaxConsumersPerStream`
are both absent and `parseJetStreamLimits` raises an `unknownConfigFieldErr` for that
literal key — so following the instruction would break the very deployment it was written
to protect. Deleting the boundary rather than restating it a third time is the fix. There
is no released artifact and no deployed population, so the floor cost nothing to raise.
The prerequisite is still a statement of the gap rather than a fix, and it is still
unenforced.

**What this needed was a decision (historical; taken 2026-10-12 as option (a)).** (a)
Keep it a deployment prerequisite only.
(b) Require an explicit positive account tier `max_consumers` at startup and validate it
against a declared working set, converting an invisible cap into a visible one the broker
refuses early — at the cost of making an operator commit to a number. (c) Ask upstream to
expose the effective per-stream consumer limit on a path a JetStream client can reach,
which is the only option that makes startup validation honest.

**The acceptance test is behavioral, not an options assertion.** This applies to option
(b) or (c) if either is ever taken up; option (a) changes no broker behavior and has no
such test. With the remedy in place,
N+1 durables on one broker-owned stream succeed; with it reverted, the N+1st fails with
10026. An assertion that the broker read some option value proves nothing about whether a
peer applies it.

The `js_ack_fc_v2` feature flag changes ACK and flow-control subjects to include the
JetStream domain and account hash. It was introduced in
[2.14.0](https://github.com/nats-io/nats-server/releases/tag/v2.14.0), is still off at the
v2.15.1 pin (`server/feature_flags.go`), and upstream's `main` states it becomes the
default in 2.16.0 (checked 2026-10-23; the earlier "v2.15 default" here was stale). The
broker parser accepts both token layouts. Enabling the flag in clustered CI was a
compatibility check for parsing **and ACLs**, not a production behavior change; it is
done, with `TestMQTTAckReplyLayoutV2Replicated` as the witness (see
`docs/ARCHITECTURE.md` "JetStream behaviors this design depends on").

## Outbound delivery throughput (2026-10-26)

The section 2 roadmap item "Delivery to a QoS 1 subscriber caps at about 5 messages per
second" is recorded here. Scope (two consults, recorded in the item's lane): remove the
avoidable history-dependent reads, suppress re-offers of sources the session already
acknowledged, keep contention from reordering a subscription, fix the PUBREL name
claim's single attempt, and state the remaining ceiling as a residual. The delivery
protocol itself (serial admission per subscription, its fixed reads) is the next item.

### What was measured before the change

A reproduction on one broker with embedded JetStream on local disk (R1): a burst
published to a persistent QoS 1 subscriber before it reads, then read and acknowledged
as fast as possible.

| Burst | Rate | Median gap | Message GETs per delivery |
|---|---|---|---|
| 20 | 11.1 msg/s | 91 ms | 98 |
| 200 | 3.7 msg/s | 274 ms | 464 |
| 500 | 1.7 msg/s (8.6 falling to 1.0 per 50) | 588 ms | - |

Wall-clock stack sampling put about 78% of the delivery path in waits for JetStream
replies, nearly half of it in `checkIdentifierExclusivity`'s inventory walk. Split by
record class, the duty and exchange reads grew with the burst (23 and 27 per delivery at
20, 203 and 207 at 200) while the rest stayed near 50. Four mechanisms:

1. **The identifier walk was O(span).** Every admission read every exchange in its generation's
   `[Lo, Hi]`, ended ones included, serially.
2. **The span was not bounded during a burst.** The floor only works above a slack of
   256 and is a background writer that yields to deliveries: after 120 messages all 120
   duties stood Pending at `Lo = 1` indefinitely; after 300, `Lo` moved only about 80 s
   later.
3. **Every first admission attempt lost its CAS** (199 contention waits for 200
   deliveries): the previous delivery's acknowledgement rewrites the client's authority
   record between the next admission's reads and its batch. The walk was therefore paid
   twice per delivery.
4. **Backlog redelivery.** The source consumer pushes up to MaxAckPending offers at
   once, and an offer queued longer than AckWait is re-offered while its first offer is
   still queued: a burst of 150 re-delivered 15 acknowledged messages with fresh
   identifiers and DUP=0, a burst of 300 re-delivered 115 (the consumer reported 164
   deliveries for 150 messages). Each duplicate cost a full delivery.

Found along the way, both pre-existing: an admission that lost all 6 of its attempts was
abandoned and its re-offer queued behind the backlog, so the subscription received one
message after the next (4 of 15 bursts of 500 on the tree before the change); and a
clean-session client reusing its identifier within about 200 ms had its QoS 2 SUBSCRIBE
refused, because the PUBREL name claim made one attempt and lost it to the previous
connection's closing.

### What changed

Each in docs/ARCHITECTURE.md:

- **The exchange duty names its identifier**, bound to its exchange by the boundary and
  carried unchanged by continuation and relocation; identifier exclusivity reads only
  the exchanges whose duties name a claimed identifier (the duty reads stay O(span),
  overlapped).
- **The floor slack is 32**, chosen by measurement (the idle row below at 256, 64 and
  32: 6.1, 9.7 and 11.3 msg/s; 613, 241 and 178 requests per delivery, 499, 125 and 62
  of them duty reads; 15.9, 18.4 and 18.9 batch members per delivery).
- **A re-offer of a source this session acknowledged is declined** before allocation and
  re-acknowledged on its own reply. The proof is the session's own submission through
  the one acknowledgement gate; two other proofs were refuted in plan review (an exact
  read finding the message absent also covers expiry; the consumer's acknowledgement
  floor also advances when a pending message disappears, nats-server v2.15.1
  `consumer.go` 6150).
- **An admission defeated only by stale expectations is re-attempted** for its
  connection's life instead of abandoned, at the delivery entrance only.
- **The PUBREL name claim is re-attempted** under the connection's pinned entitlement,
  adopting a landed attempt and settling every attempt on exhaustion.

### Measured after the change

`make bench-outbound`, one run per rig, the burst published and acknowledged before the
subscribers read; every row held its oracle (all delivered, first deliveries in publish
order, every QoS 2 exchange completed, every consumer settled). No row delivered a message
under a new identifier twice; one of four standalone QoS 2 runs retransmitted one PUBLISH
under its own identifier with DUP set, which QoS 2 permits. (The first run of this
benchmark let the subscribers read while the burst was being published and timed the
publishing too; holistic review round 1 caught it, and every row here is from the
corrected workload.)

| Case | Standalone | R3 | Requests per delivery (standalone) |
|---|---|---|---|
| QoS 1, one subscriber, 500 | 6.2 msg/s (13.0 to 4.7 per 50) | 8.0 msg/s | 665 |
| QoS 2, one subscriber, 200 | 5.5 msg/s | 7.4 msg/s | 606 |
| QoS 1, four subscribers, 200 each | 30.6 msg/s aggregate | 45.4 msg/s aggregate | 360 |
| QoS 1, 20 at rest after 300 | 11.0 msg/s | 12.8 msg/s | 182 |

The burst rows' request count is dominated by duty reads (about 530 per delivery at
500), which are overlapped; the exchange reads fell to about 20 per delivery. Mechanism
4's duplicates are gone in these rows (the 500 burst was re-offered 339 times in a
reproduction run and delivered nothing twice), and so are mechanism 3's reorders (0 of
15 capped bursts, no admission exhausted). What remains — about 50 fixed reads and three
batches per delivery, one delivery at a time per subscription, the CAS loss on every
first attempt, and duty reads that grow through a burst — is the design's and is stated
in docs/CONFORMANCE.md with these numbers.

The Paho interoperability test `test_flow_control2`, which failed once in CI on
2026-10-26 and was baselined pending this item, passed 5 of 5 runs locally against the
change, and 5 of 5 against the tree before it: the CI failure was not reproduced here,
so its cause is not confirmed. Its baseline entry is removed, so a recurrence fails CI.

Mutations, each run and each failing its paired test: 26 (the identifier's shape,
binding, continuation, marker and representative; the walk's filter both ways; the
slack; the decline, its re-acknowledgement, both recording points, the
failed-confirmation retention, the set's keying and both bounds, the decline's
dependence on the record, the hand-back; the claim's attempts, its ownership re-read,
its abort, its entitlement rule; the extension, its qualification, its connection check
and the classifier). The decline's explicit drain exclusion has no discriminating test.

## Outbound delivery protocol (2026-10-27)

The section 2 roadmap item "Raise the per-subscriber delivery ceiling" is recorded here,
measured against the rows above (21eca60). Scope came from two consults on measured
spikes in a throwaway worktree: on the burst of 500, dropping the acknowledgement's
authority member and moving its hold release out of its batch raised 6.4 to 7.8 msg/s
(19.7 against 11.1 on a burst of 60, every admission loss gone), and adding a cache of the
identifier walk's validated duty identities raised it to 15.0 msg/s, with duty reads per
delivery falling from about 530 to 32. Concurrent or batched admission per subscription
changes the ordering carrier and is the next roadmap bullet.

### Where the losses came from

At a small span, per delivery: three authority transitions (admission, acquisition,
acknowledgement), and each admission lost its first attempt. By record, those losses were
mostly on the SOURCE RESOURCE (32 of 59; the acknowledgement released its hold edge on the
record the next admission adds its edge to), then a store CAS (26), and the authority
record itself only once: the authority was not the main contended record, the resource was.

### What changed

Each in docs/ARCHITECTURE.md:

- **A completion writes no authority record.** The acknowledgement advance and the drain
  completion are authority-free: no authority member, revision 0, a receipt that resolves
  LANDED and final. Implementation finding: the retained abandoned-origin PUBREC arrow
  creates a canonical PUBREL with no material to CAS, and its refusal in a Discarded
  generation is a closure read made atomic only by the authority CAS (consult 103); it
  keeps the authority, reinstalled exactly, and the boundary refuses it without. An audit
  of every other read the boundary makes for an advance found each fenced by a member's own
  CAS.
- **An acknowledgement ending leaves its holds release-owed.** The release is an
  authority-free `release-holds` batch over the stored ending; the debt is durable because
  the delivery duty may not be written terminal while a hold of its ended exchange is
  listed. The session's releaser runs at 16 owed or 200 ms; the floor, the recovery and
  discard executors and the source retirement release through the same composer.
- **The identifier walk caches validated duty identities, never state**, scoped by client,
  epoch, generation and ordinal, bounded, pruned below the floor and version-guarded. Stated
  contract change: a corrupted cached duty is not re-detected by the walk.

### Measured, stage by stage

`make bench-outbound`, the burst cases (standalone / R3, msg/s; contention waits per run
standalone in brackets):

| Stage | QoS 1, one subscriber, 500 | QoS 2, one subscriber, 200 |
|---|---|---|
| 21eca60 | 6.2 / 8.0 [498] | 5.5 / 7.4 [-] |
| completion authority-free | 6.4 / 8.9 [498] | 7.6 / 10.8 [659] |
| and release-owed holds | 10.4 / 13.9 [6] | 9.3 / 9.2 [558] |
| and the identity cache | 17.6 / 18.6 [13] | 10.3 / 13.6 [509] |

The completion alone moved the QoS 1 case little: its admissions then lost to the resource
record instead. The release-owed holds removed that, and the cache removed the walk's
growth (requests per delivery on the 500 burst: 744, 376, 105). QoS 2 still loses about two
attempts per delivery to its PUBREL acquisition, which CASes the authority the next
admission CASes.

Full run with all three (every row held its oracle; no duplicate):

| Case | Standalone | R3 | Requests per delivery (standalone) |
|---|---|---|---|
| QoS 1, one subscriber, 500 | 17.6 msg/s (21-26 to 13 per 50) | 18.6 msg/s | 105 |
| QoS 2, one subscriber, 200 | 10.3 msg/s | 13.6 msg/s | 185 |
| QoS 1, four subscribers, 200 each | 43.0 msg/s aggregate | 101.3 msg/s aggregate | 140 |
| QoS 1, 20 at rest after 300 | 20.9 msg/s | 29.3 msg/s | 114 |

Mutations, each run and each failing its paired test: 17 (the completion written with the
authority again; its receipt classified against the authority's revision; the abandoned
arrow's fence dropped by the composer and its boundary guard removed; the completion
releasing in its batch again; the releaser idle; a release accepted without a stored
ending; the inventory's resolution kept on the authority path; the duty judged on stored
resources only, on the batch's resources only, and without its holds; the retirement
waiting instead of releasing; the boundary accepting a completion's in-batch release; the
walk's highest-seen shortcut, a cached exchange, no prune, and an unguarded fill). The
highest-seen shortcut is caught as an uncertain position (the walk refuses), not as a
missed holder.

Plan tests not written, stated rather than implied: the retained QoS 2 PUBREC against
cleanup and takeover with a lost commit response (the retained arrows' existing suites ran
green instead), and the audit that every caller of an advance's returned sequence re-reads
the register's (done by reading every caller, not by a test). Written beyond the plan: the
abandoned arrow's fence (both halves), racing releasers, a lost release response, a pending
drain's release refused, and a release owed in a generation that is then discarded.

Found by the full suite on the change: the recovery kick set's requeue of a root its
worker held bypassed the set's cap, so kicks landing during the worker's turn could grow
it to the cap plus one (`TestF105RecoveryKickIsBoundedAndFair`, 3 of 8 runs under `-race`
on the change, 0 of 8 on 21eca60, where only the timing differed). The requeue now drops
the root to the periodic pass when the set is full, as a kick does (12 of 12).

Found by the final suite on the reviewed tree:
`TestF112AHealthySessionAdvancesItsFloorWithoutAReconnect` failed intermittently under
`-race` (the floor short of its deadline after 600 deliveries). A diagnostic run showed
the hold releaser starved for the whole burst (546 completions owed at its end) and the
floor then clearing the backlog too slowly. Two causes: the release composed its resource
CAS after reading every exchange it released for, so an admission adding its edge to the
same source resource defeated it nearly every time; and the floor unit, which waits for
foreground admission, blocked the session's shared custody dispatcher for the whole burst,
the releaser behind it (pre-existing in the floor, made consequential by the releaser).
Fixed: the release recomposes only its resource half on a stale expectation; the floor
yields without waiting; the floor hands its exchange reads to the release; the first owed
completion starts the dispatcher. After the fix, four `-race` runs of the scenario passed
with at most 4 completions owed, and 600 deliveries took about 92 s against 3m42s before.
Mutations, each killed: the floor waiting again, no recomposition, and the first owed
completion not starting the dispatcher.

Two pre-existing intermittent test failures the final suite also hit, fixed at the user's
direction rather than baselined, both fixture races with no broker change:
`TestWillClaimantAbortsThePassOnATransientFactOutage` (5 of 20 runs on 21eca60) counted the
fact reads of the account's own claim loop, which the package runs every 50 ms beside the
test's direct pass; it and its pending-arm sibling now pin the loop's interval long and wait
for its initial pass (30 of 30 each). `TestMQTTSubscribeRollbackAmbiguousKeepsTheRolledBackMembership`
sampled the first record-transaction release of the session, which under shard load could
be an earlier transaction's; it now samples the rollback's own, by the save ordinal, as its
sibling T6 does (20 of 20). Both still fail against the mutations they exist for (the
recovery arm's pass abort removed; F-039's M3).

## nats-server 2.15.2 and Go 1.27 (2026-10-27)

The pin and the floor moved from nats-server 2.15.1 to 2.15.2, and the toolchain from Go
1.26 to 1.27.3, at the user's direction.

**The upstream delta** (`docs/ARCHITECTURE.md`, "The v2.15.1 → v2.15.2 audit"): fourteen
server files changed and no JetStream file did. `make depproof` matched every row of its
evidence table on v2.15.2. The one store change, `readStreamState`'s bounds checks, only
refuses a malformed index state file, so the store paths behind F-108, F-109, F-110 and
MAXMSGSPER-REVIVAL are unchanged and none of them closes. CALLOUT-CONN-TYPES does: the
callout's issued user is now checked with `connectionTypeAllowed`, and its witness fails on
v2.15.1 and passes on v2.15.2.

**Images.** `synadia/nats-server:2.15.2-scratch` by digest for the conformance jobs, Compose
and the image smoke test (the Docker Official `nats` repository had no 2.15.2 tag). Neither
a 2.15.2 NATS chart nor `nats:2.15.2-alpine` exists, so the chart's optional NATS and the
shared-pod example override chart 2.15.1's `nats:2.15.1-alpine` with
`synadia/nats-server:2.15.2-alpine3.22`. The first version of that override was silently
discarded: both values files already had a `container:` key, and YAML keeps the last; the
render check caught it, and the image now merges into the existing block.

**Go 1.27.** No gofmt or vet change in the tree. golangci-lint's release binaries (v2.14.0
and v2.15.0 alike) are built with Go 1.27.0 and cannot read the export data Go 1.27.3
writes ("export data version 5 is greater than maximum supported version 4"), so every
import fails to type-check; CI now builds the pinned v2.14.0 with the job's own Go
(`install-mode: goinstall`), and `CONTRIBUTING.md` says how locally. The claim that the
runtime does not derive `GOMEMLIMIT` from the cgroup still holds on 1.27 (golang/go#75164
is open).

**One intermittent fixture race surfaced on the new toolchain**, fixed rather than
baselined: `TestF105RetainedCloseLocatesTheRegisterByItsAdmission/ended-control-closes` (2
of 10 runs under `-race` on the bump, 0 of 10 on `main`) called the reclaim step directly
while the session's own reclaimer, released from the same held read, stepped the same
copy, and one of the two lost the manifest CAS. The test now parks the reclaimer with a
foreground announcement before it releases the hold and fails if a reclaimer step lands
beside its own (20 of 20). No broker change.

**And one broker defect it exposed**, fixed: `TestMQTTReconcileChargeAccounting`'s reconnect
after a refused CONNECT was itself refused, intermittently, because the resume repair
claimed its replacement consumer's name in ONE attempt and that attempt lost the authority
CAS ("wrong last sequence") to concurrent authority writes. The claim now re-attempts on a
positively stale expectation like the PUBREL name claim; the witness
`TestMQTTReconcileRepairClaimSurvivesContention` fails when the claim gets one attempt.

## Batched admission (2026-10-28)

The section 2 roadmap item "Admit several deliveries per subscription in one batch" is recorded
here. It stays OPEN: its acceptance gate is missed on both rigs (below). Scope from three
consults (the item's lane): admission batched, the action acquisition kept per delivery, phases
serialized per subscription, no intentional coalescing delay; acceptance twice the QoS 1 burst
rate of a same-day baseline by the ratio of three-run medians, standalone and R3 separately.

### Where the time went before the change

Timing probes in the QoS 1/2 delivery callback on 6548ed9 (one run each rig, means per
delivery):

| Case | Rate standalone / R3 | callback | admission | acquisition | intent | write |
|---|---|---|---|---|---|---|
| QoS 1, 500 | 25.9 / 22.9 msg/s | 41.8 ms | 28.4 ms | 10.2 ms | 0.8 ms | 0.1 ms |
| QoS 2, 200 | 14.1 / 22.1 msg/s | 62.1 ms | 46.9 ms | 11.7 ms | 1.0 ms | 0.1 ms |

### What changed

docs/ARCHITECTURE.md, "A subscription's fresh deliveries are admitted in batches": an
offer with another already queued behind it is held (up to 16), the callback that finds
nothing more queued processes them all in order, and their fresh ordinary deliveries are
admitted in one atomic batch with one authority revision, one member per resource and one
ledger member, settled once for every participant.

### Measured

`make bench-outbound`, the base tree (6548ed9) and the change interleaved, three runs each,
msg/s:

| Case | Rig | Base | Change | Median ratio |
|---|---|---|---|---|
| QoS 1, one subscriber, 500 | standalone | 25.9, 25.4, 25.1 | 38.8, 41.8, 38.4 | 1.53 |
| QoS 1, one subscriber, 500 | R3 | 34.8, 35.8, 46.4 | 103.5, 63.8, 66.5 | 1.86 |
| QoS 2, one subscriber, 200 | standalone | 14.4, 14.8, 14.1 | 19.3, 23.8, 20.6 | 1.43 |
| QoS 2, one subscriber, 200 | R3 | 20.8, 21.3, 28.0 | 37.7, 36.9, 37.2 | 1.75 |
| QoS 1, four subscribers, 200 each | standalone | 42.4, 41.9, 42.0 | 62.3, 61.4, 62.7 | 1.48 |
| QoS 1, four subscribers, 200 each | R3 | 124.3, 142.3, 148.4 | 187.9, 209.8, 222.9 | 1.47 |
| QoS 1, 20 at rest after 300 | standalone | 26.0, 25.4, 28.2 | 36.6, 32.0, 37.8 | 1.41 |
| QoS 1, 20 at rest after 300 | R3 | 35.6, 36.9, 26.2 | 55.4, 49.7, 70.9 | 1.56 |

Every row held its oracle (all delivered, first deliveries in publish order, every QoS 2
exchange completed, every consumer settled, nothing delivered twice). Requests per
delivery on the QoS 1 burst: 115-118 to 80-83 standalone, 101-108 to 69-73 on R3. Batch
bound, single runs of the QoS 1 burst (not acceptance figures): 8 gave 36.9 / 59.6, 32
gave 40.0 / 99.2; 16 is kept, the gain of 32 being within the run-to-run spread.

### Why the gate is missed

With batches of about 15, admission costs 5.8 ms per delivery (92.8 ms per batch) against
28.4. The send section rose from 11 to 19.9 ms, nearly all of it the acquisition (16.5 ms,
from 10.2). A commit-phase probe (boundary checks against the atomic batch write, mean per
commit, standalone):

| Kind | Before: checks / write | After: checks / write |
|---|---|---|
| acquire-action | 2.73 / 6.42 ms | 3.02 / 9.68 ms |
| advance-exchange (completion) | 3.30 / 8.26 ms | 3.62 / 9.41 ms |
| admit-exchange | 7.28 / 7.84 ms per delivery | 34.07 / 8.60 ms per batch |
| release-holds | 12.84 / 9.28 ms | 20.30 / 10.35 ms |

And a store probe on the same rig (200 writes each on `$MQTT_inflight`): a plain store
write 0.36 ms mean (p50 0.33), an atomic batch of two 6.40 ms (p50 5.55, p90 9.41), of
eight 6.16 ms (p50 5.27, p90 9.71). On the pinned server (v2.15.2, file storage, R1) an
atomic batch costs about 6 ms however few members it carries. Each delivery still makes
two of them, its acquisition and its completion, and acquisitions sent back to back now
overlap the client's completions on the one stream, which raised the acquisition write by
about 3 ms. Disabling the floor in a probe cut requests per delivery from 82 to 51 and
changed neither the rate nor the acquisition time. A model consistent with these figures
puts the standalone rate near 50 msg/s while those two commits remain; it is a model, not
a proven ceiling.

The remaining work is batching the acquisition (several send sections open at once), which
consult 1 kept separate as a transmission-lifecycle audit. The atomic-batch latency is a
candidate upstream performance report: plain 0.36 ms against 6.2-6.4 ms for an atomic batch
of any size, standalone, file storage, reproducible with a 200-iteration loop of
`newAtomicBatchRequest` beside `storeMsg` on one stream.

### Tests and mutations

`outbound_admission_batch_test.go` holds the item's tests: the resource step's
several-hold rule and its refusals, a backlog admitted in committed multi-delivery batches
in order, no hold at rest, the hold bound, held offers dying with their connection, a lost
response settled once (two custody checkpoints), a superseded receipt landing, a
never-landed batch settled before its retry, a contended batch extended whole past the
budget, a takeover before the batch's commit, an occupied slot splitting the batch in
order, a failed read-back taking the single ladder, an unresolved outcome abandoning every
participant, held offers keeping their own headers, a shared subscription never holding, a
retransmission following the collected deliveries, Message Expiry judged once at the flush
(expired and still live), the flush before a gate wait, an immediate Withheld verdict and
a declined re-offer's acknowledgement, and the census that the delivery body has one
unflushed exit.

Mutations, each run and each failing its paired test: 26 (no flush before a non-batch
send; the hold bound ignored; held header lengths not restored; a batch settled without
resolving its handle; only the first added edge judged; the suffix admitted before the
split delivery; a contended batch abandoned at its budget; holding at a pending count of
one; the settlement skipping a participant; one resource member per edge; Superseded not
charged; the mixing rule on one added edge; expiry not judged at the flush; expiry reduced
twice; an expired delivery disposed of before the live ones ahead of it; the batch
operation without its entitlement comparison; the identifier walk given only the batch's
first identifier; a non-stale refusal retried whole; a plain unlock on a non-collect exit;
a plain unlock on the drain's exit; no flush before a gate wait; a declined re-offer
acknowledged before the flush; no flush on an immediate Withheld; a never-landed verdict
not applied to the reservations; the batch's reservations never finished; every
participant given the first participant's dependencies). Two survived their
first pairing and were re-paired: the declined re-offer test reached the terminal-source
arm, not the decline, and was reworked so the re-offer really is declined; the identifier
walk's mutation survived the two-broker test, whose displaced batch is refused at the
entitlement before the walk runs, and is killed by a store-level batch claiming, as its
second participant, an identifier a standing obligation holds.

Written after holistic review round 1: the two-broker races (a displaced broker's batch
paused before its authority read while the client resumes on the peer, which is then
offered the same sources and allocates its own identifiers, and a displaced broker's batch
paused after its commit and before its sends), the non-stale refusal handing its head to
the single ladder, a drain standing when a later offer is processed, and an expired
delivery behind live ones in one flush (whose disposal had run before the live ones were
sent; fixed). After round 2: two subscriptions of one session whose first batches are held
at the store until both arrive, so they race on the authority and ledger under one
entitlement (the loser settled before it is attempted again, every message once and in
order, no reservation or charge left); Receive Maximum 2 over a backlog of five; and a
committed batch mixing QoS 1 and QoS 2 deliveries, each holding the source consumer and
only the QoS 2 ones the PUBREL consumer, every exchange completed. Two batches claiming
overlapping identifiers while both pass the entitlement comparison cannot be staged:
identifiers come from one session's allocator, and a tenure belongs to exactly one
accepted connection, which `opAdmitPublishes` compares before anything is composed; the
identifier walk's own rule is tested against a standing obligation instead. Not written,
covered structurally by the census and the flushing-exit rule: a deny release, a retired
source, an uncertain terminal source and a phase-gate wait behind a collected delivery.

Found along the way, pre-existing and fixed:
`TestMQTTInflightStrandedReservationOverADeletedConsumerIsReleased` failed 2 of 8 runs on
6548ed9 and 10 of 10 under `-v`. Its strand needs the source live at recovery and gone at
the reap with no offer in between, but the resume installs interest before its CONNACK and
JetStream redelivered the long-unacknowledged source at once, so the redelivery opened a
send section and the reap rightly reserved the entry. The fixture now refuses that
redelivery before tracking for the staged resume (6 of 6 under `-v`); no broker change.
`TestMQTTInflightRetiredSourceSurvivesRestart` failed twice in `-race` shards and never in
isolation: it stages a retired source between PUBREC and a register read, and with a 1 s
AckWait a suite stall let JetStream redeliver the source in that window, which the
retired-source settlement acknowledges with confirmation and so releases the register
(correct behavior, the end of the staged state). Delaying the read past AckWait
reproduces it every time. Its AckWait is 10 s; 3 s still lost the race once under a
memory-starved shard. No broker change.

Interleaved runs of shard 3's test list on 6548ed9 and on the change (three each, -race,
the pod's memory) showed four more load-sensitive tests failing on both trees, each fixed
in its fixture and none needing a broker change: `TestV5SharedSubscription`'s 10 s window
was a throughput bound (a probe showed every offer arriving once, about a second apart,
the last ones after the window; now 60 s); `TestMQTTDrainLevelUpCoversBothConsumers` read
two PUBLISHes expecting one per consumer while its 250 ms AckWait could bring one
consumer's retransmission first (it now reads until each consumer's first delivery,
accepting only a same-identifier DUP retransmission in between);
`TestMQTTReconcileStreamForcedThresholdRefusesAdmission` drained its captured warnings
without waiting for the admission to log (it now waits for the first refusal); and
`TestF105RetainedAbandonedArrowRefusedAfterDiscard` asserted the first attempt of an
advance whose window the retained copy's background reclamation can move (it now
re-attempts from fresh reads on a stale refusal, as production does). One more failed only
on the change: `TestF105RetainedAbandonPassesToATakeover` (3 of 10 against 0-1 of 10),
because its rig resumed while the server still held its first connection, the resume took
that connection over and marked the client ID a flapper, and the test's own takeover was
then refused; the batched path's slower close made that race likelier. The rig now waits
for the session to release the first connection (20 of 20). Reading these also showed a
lone collected delivery paying readiness twice (the batch's, then the single ladder's); a
flush of one now takes the single route.

The final chain on the reviewed tree then failed two more, once each.
`TestWillUnconditionalWriteRefiresAPublishedWill` counted every fact read on the account,
so the claim loop's own 50 ms pass could land in its window (1 of 10 on the change, 0 of
10 on 6548ed9); it now pins the loop as its siblings do (20 of 20).
`TestMQTTTeardownReservesAnOpenSendSectionInEveryOutcome` found one Receive Maximum charge
left after a recovery release, in one of three runs of its shard and in none of 20
isolated runs on either tree; it is a recovery path this change does not touch. Not
reproduced, so not fixed: its assertion now reports the custody ledger's exchanges,
admissions and uncertain subjects, so a recurrence names the holder (a read that timed out
under the pod's memory pressure would be held as an uncertain subject, by design).

## Batched acquisition (2026-10-28)

The second part of the section 2 roadmap item "Admit several deliveries per subscription in
one batch": the PUBLISH action acquisition of a flush, batched. The item stays OPEN: the
acceptance gate (twice the QoS 1 burst rate of a same-day 6548ed9 baseline, by the ratio of
three-run medians, standalone and R3 separately) is met standalone and missed on R3.

### What changed

docs/ARCHITECTURE.md, "A subscription's fresh deliveries are admitted in batches": after a
flush's admission every admitted delivery's send section is opened and their actions are
acquired in one acquire-action transition (one authority revision, one token member and
one action record per delivery); every submitted attempt is settled by its receipt, an
unresolved one carried by the custody dispatcher as an owed settlement; then each section
is written and ended in offer order, and none records intent once the connection's output
is sealed.

### Measured

`make bench-outbound`, the base tree (6548ed9) and the change interleaved, three runs each,
msg/s:

| Case | Rig | Base | Change | Median ratio |
|---|---|---|---|---|
| QoS 1, one subscriber, 500 | standalone | 25.7, 25.3, 25.1 | 60.6, 62.6, 58.0 | 2.40 |
| QoS 1, one subscriber, 500 | R3 | 46.1, 34.9, 46.7 | 90.5, 67.3, 89.5 | 1.94 |
| QoS 2, one subscriber, 200 | standalone | 10.9, 14.1, 14.6 | 27.1, 22.5, 25.6 | 1.82 |
| QoS 2, one subscriber, 200 | R3 | 20.8, 29.2, 20.9 | 33.1, 38.6, 21.2 | 1.58 |
| QoS 1, four subscribers, 200 each | standalone | 42.0, 41.9, 42.1 | 101.7, 105.6, 104.2 | 2.48 |
| QoS 1, four subscribers, 200 each | R3 | 121.0, 123.3, 127.0 | 215.1, 203.9, 269.4 | 1.74 |
| QoS 1, 20 at rest after 300 | standalone | 21.5, 24.1, 24.7 | 37.9, 47.0, 41.1 | 1.71 |
| QoS 1, 20 at rest after 300 | R3 | 31.1, 34.3, 30.1 | 61.6, 52.1, 49.6 | 1.68 |

Every row held its oracle; no change run retransmitted a message or delivered one twice.
Requests per delivery on the QoS 1 burst: 115-118 to 71-73 standalone, 103-107 to 66-71 on
R3; contention waits 32-36 to 5-11 standalone. The slowest change R3 burst run (67.3) is
uniformly slow across its ten windows (52-72), the shape of the slow base R3 run (34.9,
windows 32-34 against 41-47 in the other two); the batched-admission record's base R3 runs
on the same commit were 34.8, 35.8, 46.4. A single run before the measurement gave 57.1 /
96.9. Consult C1 kept the gate as written: the spread is context, not an exception.

### Mutations

Fifteen, each killed by its paired test in `outbound_acquisition_batch_test.go`: the
boundary's action-without-token and two-actions-for-one-exchange refusals removed; the
adoption reading the pre-commit sequence; a failed read-back withdrawing ownership;
settlement by a register read instead of the receipt; sections ended after the loop instead
of each after its write; the aggregate end removed; the sealed-output check removed;
composition under the serving authority's tenure; abandoning a contended batch; keeping a
participant whose premise failed; dropping fall-outs; writing in reverse order; composing
one token member only; not recording an unresolved attempt; the owed settlement not
draining. One more (the planned skip of a section end's drain while an attempt is owed)
survived because the owed record drains each exact token after a landed receipt anyway, and
that flag was deleted.

### Remaining

Each QoS 1 delivery still makes one atomic commit of its own, its completion (one
advance-exchange per PUBACK on the client's read loop); whether back-to-back advances can
share one, keeping each exchange's checks and acknowledgement order, is the item's open
part.
