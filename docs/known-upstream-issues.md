# Known upstream issues

Defects that live in `nats-server` (this broker's data layer), not in this
repository. They are recorded here, left blocked, and ticked only when upstream
ships a correction and the pin moves onto it under the ordinary reviewed-bump
procedure.

Filing these upstream in prose the maintainers can act on takes real time, and
that is deferred: it may happen after alpha. This file is the standing record in
the meantime.

## Boundary ruling (2026-09-21)

JetStream is the data layer. A store that misreports its own contents - a record
silently skipped because its checksum failed, a failed read answered as "no
message", a conditional creation admitted over a record the server could not
read, a limit-evicted record revived on recovery - is the data layer breaking
its own contract. It is never this broker's defect. The broker does not
compensate for it, does not fork or `replace` the dependency, and does not carry
a store-integrity layer of its own. The obligation, per `CLAUDE.md` "An upstream
bug belongs upstream": verify against the pinned version, reproduce with a
real-server witness, file it upstream, record it, and move the pin when the
correction lands.

None of these is selected by roadmap automation (the selection rules skip
chapters P and U by name) and none is an alpha blocker. The alpha gate owes only
the dependency floor: when a correction ships, the pin and the documented floor
move onto it and the item is ticked.

Pin: `nats-server v2.15.1` (latest release), moved from v2.15.0 on 2026-10-11. Every
issue below was re-reproduced on v2.15.1 that day with its real-server witness, and the
store functions each one cites are byte-identical between v2.15.0 and v2.15.1
(`docs/ARCHITECTURE.md`, "The v2.15.0 → v2.15.1 audit"). The earlier check against
`main` (`v2.16.0-dev`) dates from each issue's own section and was not repeated.

## Index

| ID | One line | Upstream | Blocks | Status |
|---|---|---|---|---|
| MAXMSGSPER-REVIVAL | A `MaxMsgsPer:1` limit-evicted record revives after raw-block recovery when its successor was explicitly deleted | PR [#8506](https://github.com/nats-io/nats-server/pull/8506) (closed unmerged) | every admitted QoS 1/2 register on `$MQTT_inflight` at crash or raw-block recovery; since F-105 the stream is non-empty for every account from its first accepted CONNECT | Known upstream, not fixed; not (re)filed; reproduces on v2.15.1 |
| F-108 | A failed store read and an empty suffix are the same response, so every enumeration reads a read failure as its own end | Issue [#8632](https://github.com/nats-io/nats-server/issues/8632) | supported suffix enumerations (`scanPubRelRecords`, the account sweeps) and F-105 affine cleanup | Filed; reproduces on v2.15.1 |
| F-109 | Point-read store failures are classified as absence, invalidating release and ownership decisions | Same root as #8632 (broker-side face) | supported release/ownership point reads | Tracked, not separately filed; reproduces on v2.15.1 |
| F-110 | A conditional creation is admitted over a record the server could not read, so a create-only CAS does not prove the subject was empty | To be filed | F-105 substrate premise (create-only CAS as an absence proof) | Witness ready, not yet filed; reproduces on v2.15.1 |
| INPROCESS-REFUSAL | A connect the server refuses over an in-process connection waits out the server's write deadline and reports a closed pipe instead of the refusal | To be reported to nats.go and nats-server after the alpha (ROADMAP section 8) | embedded mode's refusal latency for a wrong MQTT credential or service credential (a stated residual, not a blocker) | Witness ready; reproduces on v2.15.1 with nats.go v1.54.0; `InProcessConn` unchanged on `main` |
| CALLOUT-CONN-TYPES | A user an auth callout issues is admitted on any connection type, whatever its `allowed_connection_types` | Not filed; no existing report found (searched 2026-10-25; [#5786](https://github.com/nats-io/nats-server/issues/5786) is the scoped-signing-key case, without a callout) | nothing the broker relies on; an operator following the callout pattern in `docs/AUTHENTICATION.md` who restricts connection types | Witness ready, not yet filed; reproduces on v2.15.1; the callout path is unchanged on `main` at `f8f6db89` (2026-10-20) |

## MAXMSGSPER-REVIVAL: a limit-evicted record revives on raw-block recovery

**What.** On a `MaxMsgsPerSubject:1` file-backed stream, a message removed by
per-subject limit enforcement (which writes no tombstone) reappears after a
recovery that rebuilds from raw blocks (missing or stale `index.db`), when the
message that superseded it was later removed by an explicit delete (which does
write a tombstone). During the rebuild the per-subject index counts every message
present in the blocks including the tombstoned one, so limit enforcement leaves
the older evicted copies in place; the tombstone loop then removes the survivor
and an intermediate evicted copy is left revived.

**Why it matters here.** `$MQTT_inflight` uses a `MaxMsgsPer:1` subject as a
single-slot in-flight register. A QoS 1 register is rewritten via
`Nats-Expected-Last-Subject-Sequence` CAS (each landing limit-evicts the prior
copy) and deleted at its exact sequence on ack. On a crash or stale-index
recovery an evicted intermediate copy revives and recovery restores it as a
phantom pending message, which exhausts the `MaxAckPending` window.
`ARCHITECTURE.md` states the broker depends on "a new record evicts the old one";
the revival violates that across a raw-block recovery.

**Precondition.** Recovery from raw blocks - a crash, an unclean shutdown, or a
stale or missing `index.db`. The earlier "a clean shutdown recovers correctly" is
withdrawn: the second witness below reproduces the revival after a clean shutdown,
and no index or rebuild cause is inferred from it.

**Exposure.** Present independently of F-105: a clean tree fails
`TestMQTTMaxAckPending` when a single inert permanent record is added to
`$MQTT_inflight`, so any account whose inflight stream is non-empty at a crash
restart is exposed. Since F-105 that stream is non-empty for every account from its
first accepted CONNECT, so the population is any account with an admitted register,
no longer only multi-session accounts. Not claimed for every restart, and not claimed
universal (below).

**F-105 widens the potential exposure.** An earlier form of the shared-group
transfer wrote its per-generation ledger to `$MQTT_inflight` on every resume; those
records moved to the dedicated `$MQTT_shared` stream (`DenyDelete`, so no tombstone
and no revival shape), and that history stands. The F-105 cutover itself keeps
permanent records ON `$MQTT_inflight`: every accepted CONNECT CASes the per-client
authority and leaves at least two permanent transition outcomes, and every admitted
QoS 1/2 register is written in one batch with its permanent exchange, outcome and
dependency records, then CAS-rewritten by its acknowledgement advance and
exact-deleted at terminal cleanup. Name-ownership records and closure certificates
are permanent too. So the stream stops being collapsible from an account's first
accepted CONNECT, single-session accounts included; a register is revival-eligible
only once an obligation was admitted and the defect's own conditions hold. The
neighbours remove whole-stream emptiness as an escape but establish no shared-block
placement, so the widening is of the POTENTIAL exposure, not a claim that every
restart revives. The disposition and the per-defect exposure are in
`docs/CONFORMANCE.md`, "The storage this design keeps on `$MQTT_inflight`".

**Upstream.** Known as PR
[#8506](https://github.com/nats-io/nats-server/pull/8506) ("Filestore MaxMsgsPer
removed message revived on recovery", MauriceVanVeen), closed unmerged on
2026-09-18 and deferred to the 2.16 line, with the maintainer flagging a deeper
compaction dimension. No open issue exists. The PR's reorder does not fully
address the `MaxMsgsPer:1` shape here, because limit-based removals write no
tombstone, so a raw-block rebuild has no record that the intermediate copies were
evicted.

**Not a client-side fix.** Re-deleting every occupied sequence does not make the
absence durable: `removeMsgFromBlock` returns early for a sequence below the
block's first sequence or already in the deleted map, before the explicit-delete
tombstone is written (`server/filestore.go` near L6097 on v2.15.0). Only the
shared-group lifecycle records moved off `$MQTT_inflight` (to `$MQTT_shared`). The
F-105 authority classes cannot move: an atomic batch is per stream, and the authority
commits with the register it authorizes. So the single-session exposure returns by
design and is accepted as a disposition, and the register path on `$MQTT_inflight`
remains subject to this defect until upstream corrects it.

**Witness.** Minimal broker-free reproduction kept with the lane scratch
(`minimal-witness-maxmsgsper-revival.go`): a `MaxMsgsPer:1` stream, one
keep-subject, a three-write CAS chain on a second subject, delete the survivor,
remove `index.db`, restart; `GetLastMsg` returns a limit-evicted intermediate.
That witness's clean-shutdown control is clean. Confirmed on `v2.15.0` and
`v2.16.0-dev`. A draft report is prepared but not posted.

**A second witness reproduces it after a CLEAN shutdown (2026-09-24).** Through the
broker's own harness on the pinned `v2.15.0` GA (the F-105 PUBCOMP lane,
`revival-witness_test.go.txt`, kept outside the tree): a `MaxMsgsPer:1` register on
`$MQTT_inflight` written, a neighbour record written, the register rewritten once — by a
plain CAS, and separately by an atomic batch — the survivor exact-deleted and the subject
reading "no message found", then an ordinary server `Shutdown()` and a restart on the same
store directory: the subject reads the limit-evicted FIRST version again, in both modes. It
contradicts the first witness's clean-shutdown control; no index or rebuild cause is
inferred from it. The draft report is to be amended with it before filing (not posted;
filing needs the user's approval). What the broker does with the revived register is in
`docs/CONFORMANCE.md`.

**Re-run on v2.15.1 (2026-10-11): both witnesses reproduce.** The minimal witness prints
`RESURRECTED seq=3` after dropping `index.db`, and its clean-shutdown control stays clean.
The harness witness no longer runs at the current tree, because the broker's own write
boundary now refuses its fixture shape before anything is stored, so it was run at the
commit it was recorded on (`1b2a49a`), first on v2.15.0 and then with only the pin moved to
v2.15.1: on both, in both the plain-CAS and atomic-batch modes, the limit-evicted first
version revives after a clean restart. None of the 2.15.1 compaction or recovery changes
(#8623, #8629, #8710) touches the per-subject limit, the tombstones or the index rebuild.

**Unblock.** An upstream correction plus a reviewed pin bump closes this defect and
the register path is no longer exposed. Neither F-105 unit waits on it. The
shared-group transfer's own records are on `$MQTT_shared` and add no revival shape;
the F-105 cutover WIDENS the potential exposure as stated above, which is a disclosed
disposition, not a repair this repository owes.

## F-108: a failed read reads as an empty suffix

Every error from `LoadMsg`, `LoadNextMsg` and `LoadLastMsg` in the message-GET
handler becomes `NewJSNoMessageFoundError()`, so an enumeration reads a read
failure as its own end. The supported suffix enumerations it reaches (with
`scanPubRelRecordsCharged` among them) are listed by file and line in
`docs/CONFORMANCE.md`, "The storage this design keeps on `$MQTT_inflight`", beside
the F-109 and F-110 exposure. Receiver is `msgBlock`: `firstMatching` returns early only
on `errPartialCache`/`errNoCache`, and `firstMatchingMulti`'s three `cacheLookup`
sites do not propagate the error (one discards it and reports success). Filed as
[nats-server#8632](https://github.com/nats-io/nats-server/issues/8632). Reproduced
against `v2.15.0`; byte-identical on `main` as of the filing date. Witness at
`~/.claude/lanes/f-109-remainder/` (`UPSTREAM-WITNESS.md`, `issue1/`). Unblock: an
upstream correction plus a reviewed pin bump. The affected-caller analysis was carried in
the roadmap's former chapter U, now removed; this section plus the lane witness are the
standing record until the issue is filed after alpha.

**Re-run on v2.15.1 (2026-10-11): reproduces.** Both enumeration witnesses (`issue1/` and
`jsrepro/`) deliver only the later record and never the damaged one, with no
error and no gap; the undamaged control delivers both. Upstream status on that date: #8632
is open, and the latest maintainer comment there (2026-09-22) treats a checksum failure as
lost data.

## F-109: a point-read failure reads as absence

The broker-side face of the same store conflation: an individual subject's read
failure is classified as "the subject is empty", which is what a release or
ownership decision then acts on. Same root cause as F-108, different question (is
an individual subject empty, versus did an enumeration reach its end) and
different repair. Tracked under #8632 rather than filed separately.

**Re-run on v2.15.1 (2026-10-11): reproduces.** A point read of the damaged sequence still
answers `message not found`.

**The point-read absence census was RETIRED on 2026-10-15** (`docs/ROADMAP.md` chapter U).
It mapped which broker decisions rest on this answer, which is bookkeeping for a broker-side
repair this broker does not make. Its two checks that state a transport fact rather than
census bookkeeping survive as `internal/mqtt/jsa_msgget_transport_test.go`: every point read
enters through the five `mqttJSA` message-GET methods and one decode arm, no second decoder
of their replies exists, and no direct-get route exists. The record below is why it was
already not a completeness gate.

**The census was historical, not a current completeness gate** (decided with F-105 7b,
2026-09-30). Its tests were already failing before 7b (122 diagnostics at `3e937fd`,
including unaccounted edges and stale evidence fingerprints in functions 7b does not
touch), and 7b's reads are not entered in it (186 diagnostics on the 7b tree; three more
of its tests fail: `TestPointReadCensusDestinationsResolve`,
`TestPointReadCensusExclusionsAreAccounted`, `TestPointReadCensusNoAlternateDecoder`). The
reads 7b adds or moves are `traverseGenerationInventory` (replacing
`enumerateCustodyExchanges`: batched duty reads through `loadInflightRecordsMulti`,
decoded by `decodeDutyResponse`, and the `loadGenLedger` read), `readCustodySubject` /
`loadDutyEntryAt`, `readCustodyExchange`, `rematerializeOnce` and `canonicalPubRelProven`
(`loadMsg`), `pubRelResourceHoldsExchange`, `checkCanonicalRebindRegister`,
`disposeRematerializeCandidate`, `opWithdrawUnsent` / `opTombstoneRegister`, and
`resumeEntitlementServes`. Accepting these failures neither fixes nor closes F-109, which
stays tracked under #8632.

## F-110: a create-only CAS does not prove the subject was empty

A `Nats-Expected-Last-Subject-Sequence: 0` publish is admitted at a new sequence
even when the subject holds a record the server could not read (a damaged occupant
that reads back `message not found`), so a create-only CAS does not establish that
the subject was empty. This refuted F-105's original substrate premise. Reproduced
against `v2.15.0` on 2026-09-21. Fact sheet at
`~/.claude/lanes/f-109-remainder/ISSUE-2-facts-for-defect-template.md`, witness in
`issue2/`. Upstream issue to be filed after alpha; the fact sheet and witness in the
lane are the standing record.

**Re-run on v2.15.1 (2026-10-11): reproduces.** The create-only publish is admitted at a new
sequence over the unreadable occupant, which is gone afterwards and stays gone across a
restart; the readable-occupant, empty-subject and nonzero-expectation controls behave as
before. The two `issue2/` variants print output identical to a v2.15.0 run of the same
module. On #8632 the maintainer's 2026-09-22 comment says an expectation of zero "still
legitimately passes" over a record that failed its checksum.

## INPROCESS-REFUSAL: a refused in-process connect waits out the write deadline

**What.** A client connecting through `Server.InProcessConn` (nats.go's `InProcessServer`
option) whose connect the server refuses (a wrong password; an account
`max_connections` limit) does not get the refusal. Over TCP the same connect fails in
milliseconds with `nats: Authorization Violation` or `nats: maximum account active
connections exceeded`; in-process it fails after 10 s with `io: read/write on closed
pipe`. A correct connect is unaffected (under a millisecond).

**Where (pinned nats-server v2.15.1, nats.go v1.54.0), from goroutine stacks taken
during the hang.** nats.go `processConnectInit` sets the connect deadline
(`nats.go:2900`, `Timeout`, 2 s by default), then `sendConnect` writes the PING through
`timeoutWriter` (`nats.go:3205` → `2354` → `6921`), which replaces the write deadline with
`FlusherTimeout` (1 minute by default, installed at `nats.go:2326`). nats-server, having
read CONNECT and refused it, is in `flushAndClose` writing `-ERR` (`client.go:6134` →
`1890`) on the unbuffered `net.Pipe` that `InProcessConn` returns (`server.go:2913`,
unchanged on `main`). Neither side reads; nats-server's write deadline (10 s default)
closes the pipe. Two contracts are involved: nats.go's connect `Timeout` does not bound
the handshake's writes, and the in-process transport lets the server's refusal block on
a client that is itself blocked writing.

**Witness.** `~/.claude/lanes/embedded-nats-image/upstream/inprocess_refusal_test.go`:
one server from a configuration file, a wrong-password connect and an over-limit connect,
each over TCP and in-process; it requires the in-process refusal to match the TCP error
within the 2 s connect timeout, and fails on both cases at the pin.

**Here.** Embedded mode refuses a wrong MQTT credential about 10 s late, and a wrong
service credential fails startup after the same delay with the pipe error.
`docs/CONFORMANCE.md` states it; the broker does not work around it (no buffered pipe,
no shortened `FlusherTimeout`, no extra reader). Reporting it to both projects is a
post-alpha roadmap item (section 8); the user writes the reports personally.

## CALLOUT-CONN-TYPES: an auth callout issued user's connection types are not applied

**What.** In operator mode, a user JWT that an auth callout issues with
`allowed_connection_types: [MQTT]` is admitted on a standard client connection. The
same restriction on a configuration-file user, or on a user JWT the client presents
itself, is enforced.

**Where (pinned nats-server v2.15.1).** The configuration-user and direct-JWT paths
call `connectionTypeAllowed` (`auth.go:929`, `943`, `958`, `1008`, `1095`). The
callout's reply handler in `processClientOrLeafCallout` converts the claim
(`getExpirationAndAllowedConnections`, `auth_callout.go:197`, called at `286`) and
stores it on the internal user (`buildInternalNkeyUser`, `auth_callout.go:318`;
`RegisterNkeyUser`, `client.go:1055`), and nothing on that path compares it with the
connection's type. `auth_callout.go` on `main` at `f8f6db89` (2026-10-20) has the same
lines.

**Witness.** `~/.claude/lanes/migration-security-guide/upstream-callout-conn-types/repro_test.go`,
nats.go only: an operator, an `AUTH` account with a callout, a `default_sentinel`, and
a responder that issues every user into `APP`, restricting `mqtt-only` to `MQTT`. The
control connects; `mqtt-only` connecting as a standard client must be refused, and is
admitted at the pin.

**Here.** Nothing in the broker depends on it: auth passthrough always dials a
standard connection, and the broker reads the issued user's account and permissions
itself. `docs/AUTHENTICATION.md` tells operators not to rely on the restriction in a
callout. The broker does not work around it, and no test in this repository asserts
the defect. The user writes and posts the report.
