# Dependency proof: can the Will outbox's terminal state be regressed?

This directory holds the evidence for `docs/findings/ha-arbitration.md` F-030, which is
**open and narrowed**, not closed. Of the two routes by which the hazard could occur,
R-old is closed on evidence that stands alone; R-new was refused in every run measured
but is not yet accepted as harness-enforced closure, because four review rounds each
found a distinct false-positive path in its oracle. The dossier states what would settle
that. Nothing here should be cited as though F-030 were ticked.

The question it answers is one about the SERVER, not about this broker's code, so the
tests live in `testdata/` as `package server` files and run inside an isolated
nats-server checkout:

```bash
./run.sh                       # the version pinned in go.mod, unmutated
./run.sh --version v2.14.3     # the control generation
./run.sh --mutation m-term-guard
./run.sh --matrix              # every row of the table below, direction asserted
./run.sh --bridge              # the bridge sentinel and its own mutation
```

`run.sh` clones the pinned tags into `$DEPPROOF_DIR` (default
`${TMPDIR:-/tmp}/mqtt-depproof`), copies the fixtures in, applies the named
mutations, runs the tests and reverts. Nothing is written to this repository, and
concurrent invocations serialize on a lock in the work directory - they would
otherwise install fixtures under each other and collide on the cluster helper's
fixed ports.

## The claim being proved

A delayed Will's outbox record reaches `done` when its publication is complete. A
recovering claimant EXECUTES a persisted publish decision rather than re-deciding, so
a write that took a `done` record back to `publishing` would republish the Will
deterministically. Every `$MQTT_willq` write this broker makes is conditional on the
sequence the writer read (`storeWillOutboxCAS`, `internal/mqtt/will_outbox.go`), so
the claim reduces to a property of the server: **a write whose per-subject
expectation read base `S` must not reach the stream after a committed `done` has
replaced `S`** - including across a stream-leader transition, which is where the
expectation is checked pre-proposal and the in-process per-subject state is cleared.

## Route inventory

The inventory is what any closure would have to rest on, more than the fixtures: a
route left unclassified keeps F-030 open on its own. Each row states how a write
conditioned on `S` could reach a proposal after `done` is committed, and what stops it.

| Route | The shape | What refuses it | Evidence |
|---|---|---|---|
| **R-old** | An operation captures the proposing term with the expectation check, then proposes after that term has ended. | `raft.proposeLocked` refuses a proposal whose captured term is not current (`term != n.term`), shipped in v2.14.4 ([#8370](https://github.com/nats-io/nats-server/pull/8370)). | `TestJetStreamWillqStaleTermReclaimIsRefused`, mutations `m-term-guard`, `m-captured-term`, control run on v2.14.3 |
| **R-new** | A newly elected leader admits a CURRENT-term write from `S`. The term comparison cannot refuse this, so this route needs its own mechanisms. | Two, and each has its own mutation. The READINESS envelope: a node behind on applies does not take leadership, and a new leader does not serve until the log it was elected with has been processed (`switchToCandidate`; `switchToLeader` sets `aflr = pindex` and `leaderState` flips only in `Processed`). Then, once a leader IS serving, the per-subject expectation it compares against is reconstructed from its OWN APPLIED state, which contains `done`, so the write is rejected before proposal. | `TestJetStreamWillqNewLeaderCannotServeBeforeApplying` (readiness, higher-order mutant `m-behind-campaign` + `m-premature-ready` + `m-freeze-store`) and `TestJetStreamWillqNewLeaderRefusesTheSupersededBase` (the comparison, mutation `m-expected-subject`) |
| **R-elect** | A candidate that never received the committed `done` wins an election and serves from state that predates it. | Raft election safety: a candidate is granted a vote only when its log is at least as up to date as the voter's (`processVoteRequest`), and `done` is committed only once a quorum holds it, so no quorum can elect a leader missing it. Refuted, no fixture: this is the property Raft is, and a fixture asserting it would be testing the consensus library rather than this claim. | written refutation |
| **R-inflight** | Two writes on one subject inside one term, the second reading `S` before the first applies. | `checkMsgHeadersPreClusteredProposal` tracks in-process per-subject expectations and refuses the second with 10164 while the first is in flight; this broker resolves 10164 by re-reading rather than classifying (`docs/ARCHITECTURE.md`). Pre-existing, not a cross-term route. | covered by the bridge's own CAS tests |
| **R-apply** | A write that reaches the log anyway - a proposal admitted by a generation without the term comparison. | TWO version-common backstops, stacked: apply-time log order (`processJetStreamMsg` returns `errLastSeqMismatch` when the entry's position is not `mset.lseq + clfs`), and beneath it the store, which refuses an explicit sequence that is not exactly `LastSeq+1` (`ErrSequenceMismatch`). Together they are why the control run does NOT produce the surviving stale record the plan predicted. | measured on v2.14.3; reported as an OBSERVED backstop, see below |
| **R-unclustered** | `node == nil` (an R1 stream, or a scale-up with queued inbound), where the write goes straight to `processJetStreamMsg`. | The per-subject expectation is then enforced at apply (`canConsistencyCheck := !isClustered`), against the same store the write would land in. There is no leader transition and no window: this broker also refuses to run with a `$MQTT_willq` that is not R3 when replicas are configured (`internal/mqtt/stream_prereqs.go`). | written refutation |
| **R-batch** | `ProposeMulti`, the atomic-batch path. | It carries and compares the captured term identically (`raft.go` `ProposeMulti`). `$MQTT_willq` is not an atomic-batch stream in any case - only `$MQTT_sess` sets `AllowAtomicPublish`, for the record-plus-acceptance-fact commit. | written refutation |
| **R-forward** | `ForwardProposal`, the one proposal entry point that does not take a term. | Its only stream-data call site is a message DELETE, proposed directly when this node is the stream leader and forwarded to it otherwise (`stream.go`), and a forwarded proposal always uses the receiving node's CURRENT term (asserted by upstream's own `TestNRGTermFencedProposals`). It carries no per-subject expectation, so it cannot be the write this claim is about; a delete is separately harmless here, since reaping a terminal record is what the protocol does with it. | written refutation |
| **R-mirror** | The stream ingests writes from somewhere other than a client publish - a mirror, or a source. | Provisioning refuses both before the broker will run: a `$MQTT_willq` that is a mirror is rejected ("a mirror rejects the owner's direct writes") and one carrying `Sources` is rejected ("a foreign record could occupy a Will's CAS slot"), in `validateBrokerOwnedStreamShape` and `validateWillQStream`. Neither shape can exist on a stream this broker serves. | written refutation |
| **R-delete** | This broker's own terminal cleanup, which deletes by sequence. | It targets the sequence the finalize returned, and a delete cannot revive `publishing`: the record is gone, and an absent record is the scan's "nothing owed" (`finalizeWillPublish`, `willPublishWorker`). | written refutation |

Two properties bound every route above, and both belong to this broker rather than
to the server: every `$MQTT_willq` write goes through `storeWillOutboxCAS`, which
always sets the expectation header (audited at `will_outbox.go` lines 719, 794, 1375
and 1507 - enqueue, cancel, claim, finalize), and the stream is provisioned
`MaxMsgsPer: 1` so each Will's subject is one CAS slot.

## The evidence table

`--matrix` runs exactly these rows and asserts the direction of each. A `FAIL` row is
a mutation that MUST break the assertion it targets, and must do so for the named
reason: a run that does not build, or in which no test ran, is `BROKEN` and never a
kill; a failure in a test the row did not select, one missing the marker the row
requires, or one carrying a marker the row forbids, is `OFFTARGET`.

| Expect | Version | Tests | Mutation | What it establishes |
|---|---|---|---|---|
| PASS | pinned | all | none | both routes are closed on the supported floor |
| FAIL | pinned | R-old | `m-term-guard`, requiring `R-OLD-TERM-ADMITTED` | the refusal is Raft's captured-term comparison |
| FAIL | pinned | R-old | `m-captured-term`, requiring `R-OLD-TERM-ADMITTED` | JetStream propagates the term the operation captured, not the node's current one |
| FAIL | pinned | R-new | `m-expected-subject`, requiring `R-NEW-ACKED` and `R-NEW-STALE-PROPOSAL` | the new leader's per-subject expectation, read from applied state, is what refuses the write |
| FAIL | pinned | R-new readiness, half 1 | `m-behind-campaign` + `m-freeze-store`, requiring `R-NEW-LED-BEHIND` and FORBIDDING `R-NEW-SERVED-STALE` and `R-NEW-STALE-PROPOSAL` | with the campaign refusal removed, a node LEADS from a stale applied view -- and the stream still refuses to serve, which is the other half holding |
| PASS | pinned | R-new readiness, half 2 alone | `m-premature-ready` + `m-freeze-store` | removing the announcement alone changes nothing: the campaign refusal keeps the node from leading at all |
| FAIL | pinned | R-new readiness, both halves | `m-behind-campaign` + `m-premature-ready` + `m-freeze-store`, requiring all three markers | with both halves removed, a leader serves from the superseded base and PROPOSES a write carrying it |
| PASS | v2.14.3 | all | none | the control generation: the older-term write is ADMITTED, and still does not survive |
| PASS | working tree | bridge sentinel | none | the broker's own writes go through the contract, over a real 3-node cluster |
| FAIL | working tree | bridge sentinel | unconditional store | the sentinel reaches the EXTERNAL consequence: a Will published once, `done` committed, and then published a SECOND time (the harness requires the count, not just the phrase) |

Rows run in that order, and the two bridge rows run last because they copy the working
tree rather than a dependency checkout.

One mutation exists but is NOT a matrix row, because it does not discriminate what its
name suggests, and the measurement is worth keeping:

- `m-log-order` bypasses the apply-time ordering comparison. An admitted entry still
  does not survive, because the store refuses an explicit sequence that is not exactly
  `LastSeq+1`. Making it survive would need a second mutation to the store itself, so
  R-apply is reported as an OBSERVED backstop rather than a mutation-proved mechanism.

The R-new readiness mutant is deliberately HIGHER-ORDER, and each part does a distinct
job. `m-behind-campaign` removes the refusal that keeps a node behind on applies from
taking leadership. `m-premature-ready` removes the second half of the envelope: it
announces complete leadership when raft enters the leader state rather than when the log
the node was elected with has been processed. `m-freeze-store` is not a mechanism removal
at all -- it is the staging seam, a barrier that HOLDS one node's store application so
its applied view stays at the superseded base.

Two devices stage the state, because they cover different moments. `PauseApply` is a
real server API and is what holds it on an UNMUTATED run: the node stops taking
committed entries from its apply queue, so its store stays at the base and nothing is
processed. It does not survive an election -- `tryCommit` applies on the leader path
with no pause check, so a node that becomes leader processes its whole log regardless --
and `m-freeze-store` is what holds the store THROUGH the election. The clean row
therefore needs no mutation at all, which is the property that makes it an assertion
rather than a construction.

The barrier BLOCKS; it must never skip the write and report success. An earlier version
did skip, and it was wrong in a way worth recording: the apply loop then called
`Applied()`, `processed` advanced past `aflr` on its own, and the state staged was
"applied, with the data silently lost" rather than "committed, not yet applied" -- so it
never exercised the envelope. It also made `m-premature-ready` look like dead weight,
for the wrong reason: the skip had already opened the gate, so removing the announcement
could not matter. Under the blocking barrier that mutation is inert only ON ITS OWN --
the campaign refusal keeps the node from leading at all -- and becomes load-bearing the
moment the campaign refusal is removed with it, which is exactly what a redundant pair
looks like. Blocking keeps raft's own view honest: the entry is committed, `processed`
does not move, and the readiness envelope is the thing being removed.

Every mutation row states the EXACT outcome it claims, as markers the fixtures print:
`@NAME` must appear in the log, `!NAME` must not. Requiring is not enough on its own.
The campaign-only row claims that the OTHER half of the envelope still held, so it
requires `R-NEW-LED-BEHIND` and forbids both `R-NEW-SERVED-STALE` and
`R-NEW-STALE-PROPOSAL`: if removing the campaign refusal alone were enough to serve and
propose from stale state, that row would go OFFTARGET instead of quietly scoring its
expected FAIL, and the redundancy claim would fail loudly rather than silently.

The same applies to the rows that are not about readiness. `R-OLD-TERM-ADMITTED` is the
oracle for both R-old mutations, and the expected-subject row requires both
`R-NEW-ACKED` and `R-NEW-STALE-PROPOSAL`. Without per-row oracles, any failure inside a
selected clustered test -- a flake, a staging error, an unrelated assertion -- would
count as a kill.

The three rows together are what makes the envelope's redundancy measured rather than
asserted: each half alone still refuses, and only removing both produces a proposal
carrying the superseded base. That is also why the middle row is a PASS row -- a
mutation that changes nothing is evidence too, and hiding it would overstate what the
announcement gate does on its own.

Reporting the backstop as observed rather than proved is deliberate. Any claim made from
this harness rests only on the mechanisms that have a discriminating mutation; the
backstops are stated because they explain why the control run's outcome differs from the
one the plan predicted, not as part of the argument.

That same standard is why F-030 is not ticked. R-new's mechanisms refused the write in
every run, but this harness's ability to SEE a future regression on that route is what
four review rounds kept faulting — a mutation that staged the wrong state and reported
success, a scorer that counted a kill without its boundary firing, a scorer that required
a marker but did not forbid the others, and a fixture whose final observation was
unmarked. Each is fixed. The judgement that the class has stopped recurring is not one
this file can make about itself.

## What the control run actually showed

The plan predicted that on v2.14.3 the stale write would apply and be the surviving
record. **That prediction was falsified.** What v2.14.3 shows is that the older-term
write IS ADMITTED at the proposal boundary - the mechanism #8370 changed, and the
reason the fixture is evidence rather than an artefact - but the entry is then
discarded by the apply-time log-order comparison, which both generations have. So the
proven version difference is proposal ADMISSION, not final stored state, and the
terminal record stands on both.

That is why the assertions are layered the way they are: each fixture's primary
assertion is at the boundary its mutation targets (the proposal being refused, or no
proposal being made at all), and the record-state assertions are documented defence in
depth. A fixture asserting only the final record could not discriminate any of these
mechanisms, because the record survives with all of them removed but one.

## Running these after a dependency bump

`--matrix` is the CI form. A version bump must run it, not just the broker's own
suites: the generic upstream `TestNRGTermFencedProposals` and this repository's
bridge sentinel (`TestMQTTWillOutboxReplicatedTerminalDoneStands`) can both pass while
captured-term propagation, new-leader readiness, or the pre-proposal expectation check
is broken.
