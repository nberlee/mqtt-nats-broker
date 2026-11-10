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

Pin: `nats-server v2.15.2` (latest release), moved from v2.15.1 on 2026-10-27. Every issue
below was re-reproduced on v2.15.1 on 2026-10-11 with its real-server witness, and the
store functions each one cites are byte-identical between v2.15.0 and v2.15.1
(`docs/ARCHITECTURE.md`, "The v2.15.0 → v2.15.1 audit") and again between v2.15.1 and
v2.15.2 ("The v2.15.1 → v2.15.2 audit": the one store change, `readStreamState`'s bounds
checks, touches only a malformed index state file). Their witnesses were not re-run on
v2.15.2. CALLOUT-CONN-TYPES is corrected in v2.15.2, and its witness was re-run on both.
The earlier check against `main` (`v2.16.0-dev`) dates from each issue's own section and
was not repeated.

## Index

| ID | One line | Upstream | Blocks | Status |
|---|---|---|---|---|
| MAXMSGSPER-REVIVAL | A `MaxMsgsPer:1` limit-evicted record revives after raw-block recovery when its successor was explicitly deleted | PR [#8506](https://github.com/nats-io/nats-server/pull/8506) (closed unmerged) | every admitted QoS 1/2 register in the `$MQTT.inflight.` space of `$MQTT_sess` at crash or raw-block recovery; since [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) the stream is non-empty for every account from its first accepted CONNECT | Known upstream, not fixed; not (re)filed; reproduces on v2.15.1; store paths unchanged in v2.15.2 |
| [issue #110](https://github.com/nberlee/mqtt-nats-broker/issues/110) | A failed store read and an empty suffix are the same response, so every enumeration reads a read failure as its own end | Issue [#8632](https://github.com/nats-io/nats-server/issues/8632) | supported suffix enumerations (`scanPubRelRecords`, the account sweeps) and [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) affine cleanup | Filed; reproduces on v2.15.1; store paths unchanged in v2.15.2 |
| [issue #111](https://github.com/nberlee/mqtt-nats-broker/issues/111) | Point-read store failures are classified as absence, invalidating release and ownership decisions | Same root as #8632 (broker-side face) | supported release/ownership point reads | Tracked, not separately filed; reproduces on v2.15.1; store paths unchanged in v2.15.2 |
| [issue #112](https://github.com/nberlee/mqtt-nats-broker/issues/112) | A conditional creation is admitted over a record the server could not read, so a create-only CAS does not prove the subject was empty | To be filed | [issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) substrate premise (create-only CAS as an absence proof) | Witness ready, not yet filed; reproduces on v2.15.1; store paths unchanged in v2.15.2 |
| INPROCESS-REFUSAL | A connect the server refuses over an in-process connection waits out the server's write deadline and reports a closed pipe instead of the refusal | To be reported to nats.go and nats-server after the alpha (ROADMAP section 8) | embedded mode's refusal latency for a wrong MQTT credential or service credential (a stated residual, not a blocker) | Witness ready; reproduces on v2.15.1 with nats.go v1.54.0; `InProcessConn` unchanged on `main` |
| PUSH-DIDNOTDELIVER | A push consumer that fails to route a delivery does not requeue it (the pull branch does), so it waits for AckWait | To be reported by the owner | a QoS 1/2 offer racing a subscriber's detach ([issue #170](https://github.com/nberlee/mqtt-nats-broker/issues/170)), a stated residual, not a blocker | Reproduced on v2.15.2 with an embedded-server witness (6 of 200 rounds wait for AckWait); same source on `main` |
| DRAIN-ACROSS-RECONNECT | nats.go re-sends SUB for a draining subscription on reconnect and later removes it locally without an UNSUB, leaving server interest | To be reported by the owner | delivery subscriptions drained at detach ([issue #170](https://github.com/nberlee/mqtt-nats-broker/issues/170)), a stated residual, not a blocker | Reproduced with nats.go v1.55.0 (3 of 3, `ForceReconnect`); same source on `main` |

## MAXMSGSPER-REVIVAL: a limit-evicted record revives on raw-block recovery

**What.** On a `MaxMsgsPerSubject:1` file-backed stream, a message removed by
per-subject limit enforcement (which writes no tombstone) reappears after a
recovery that rebuilds from raw blocks (missing or stale `index.db`), when the
message that superseded it was later removed by an explicit delete (which does
write a tombstone). During the rebuild the per-subject index counts every message
present in the blocks including the tombstoned one, so limit enforcement leaves
the older evicted copies in place; the tombstone loop then removes the survivor
and an intermediate evicted copy is left revived.

**Why it matters here.** The `$MQTT.inflight.` space of `$MQTT_sess` uses a
`MaxMsgsPer:1` subject as a single-slot in-flight register. A QoS 1 register is rewritten
via `Nats-Expected-Last-Subject-Sequence` CAS (each landing limit-evicts the prior
copy) and deleted at its exact sequence on ack. On a crash or stale-index
recovery an evicted intermediate copy revives and recovery restores it as a
phantom pending message, which exhausts the `MaxAckPending` window.
`ARCHITECTURE.md` states the broker depends on "a new record evicts the old one";
the revival violates that across a raw-block recovery.

**Precondition.** Recovery from raw blocks - a crash, an unclean shutdown, or a
stale or missing `index.db`. The second witness below also reproduces the revival after
a clean shutdown, and no index or rebuild cause is inferred from it.

**Exposure.** Present independently of
[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107): a clean tree fails
`TestMQTTMaxAckPending` when a single inert permanent record is added to
`$MQTT_inflight` (then a stream of its own), so any account whose in-flight records share
a non-empty stream at a crash
restart is exposed. Since
[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) that stream
is non-empty for every account from its
first accepted CONNECT, so the population is any account with an admitted register,
no longer only multi-session accounts. Not claimed for every restart, and not claimed
universal (below).

**[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) and the stream
merge widen the potential exposure.** The shared-group transfer's records had a
dedicated `$MQTT_shared` stream whose `DenyDelete` left no tombstone and so no revival
shape; since 2026-11-09 they are in the `$MQTT.shared.` space of `$MQTT_sess`, a stream
that deletes, and that immunity is given up
([issue #133](https://github.com/nberlee/mqtt-nats-broker/issues/133)). A group record
or ledger is CAS-rewritten under `MaxMsgsPer:1` but never deleted, so the witnessed
rewritten-then-explicitly-deleted shape does not apply to it; what changes is that it is
now a permanent neighbour of records that are, and its presence can keep a block holding
an eligible deleted register from collapsing. No group-record revival is claimed: none
has been witnessed. The
[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) cutover keeps
permanent records in the `$MQTT.inflight.` space too: every accepted CONNECT CASes the per-client
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
`docs/CONFORMANCE.md`, "The storage this design keeps in `$MQTT.inflight.`".

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
tombstone is written (`server/filestore.go` near L6097 on v2.15.0). The
[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) authority
classes cannot be separated from the registers: an atomic batch is per stream, and the
authority commits with the register it authorizes. The shared-group records once had a
stream of their own for this reason, and share `$MQTT_sess` since the merge, so that the
reclamation of issue #133 can fence them in the same batch. So the single-session
exposure returns by design and is accepted as a disposition, and the register path in the
`$MQTT.inflight.` space remains subject to this defect until upstream corrects it.

**Witness.** Minimal broker-free reproduction kept with the lane scratch
(`minimal-witness-maxmsgsper-revival.go`): a `MaxMsgsPer:1` stream, one
keep-subject, a three-write CAS chain on a second subject, delete the survivor,
remove `index.db`, restart; `GetLastMsg` returns a limit-evicted intermediate.
That witness's clean-shutdown control is clean. Confirmed on `v2.15.0` and
`v2.16.0-dev`. A draft report is prepared but not posted.

**A second witness reproduces it after a CLEAN shutdown (2026-09-24).** Through the
broker's own harness on `v2.15.0` (`revival-witness_test.go.txt`, kept outside the
tree): a `MaxMsgsPer:1` register on `$MQTT_inflight` (then its own stream) beside a
neighbour record,
rewritten once by a plain CAS and separately by an atomic batch, its survivor
exact-deleted, then an ordinary server `Shutdown()` and a restart on the same store
directory: the subject reads the limit-evicted FIRST version again, in both modes. It
contradicts the first witness's clean-shutdown control; no index or rebuild cause is
inferred from it. The draft report is to be amended with it before filing (not posted;
filing needs the user's approval). What the broker does with the revived register is in
`docs/CONFORMANCE.md`.

**Re-run on v2.15.1 (2026-10-11): both witnesses reproduce.** The minimal witness prints
`RESURRECTED seq=3` after dropping `index.db`, and its clean-shutdown control stays clean.
The harness witness runs only at the commit it was recorded on (`1b2a49a`), because the
current write boundary refuses its fixture shape before anything is stored; there, with
only the pin moved to v2.15.1, the limit-evicted first version revives after a clean
restart in both the plain-CAS and atomic-batch modes.

**Unblock.** An upstream correction plus a reviewed pin bump closes this defect and
the register path is no longer exposed. Neither
[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107) unit waits on it:
the cutover's and the merge's widened potential exposure (above) is a disclosed
disposition, not a repair this repository owes.

## issue #110: a failed read reads as an empty suffix

Tracked in [issue #110](https://github.com/nberlee/mqtt-nats-broker/issues/110).

Every error from `LoadMsg`, `LoadNextMsg` and `LoadLastMsg` in the message-GET
handler becomes `NewJSNoMessageFoundError()`, so an enumeration reads a read
failure as its own end. The supported suffix enumerations it reaches (with
`scanPubRelRecordsCharged` among them) are listed by file and line in
`docs/CONFORMANCE.md`, "The storage this design keeps in `$MQTT.inflight.`", beside
the [issue #111](https://github.com/nberlee/mqtt-nats-broker/issues/111) and
[issue #112](https://github.com/nberlee/mqtt-nats-broker/issues/112) exposure.
Receiver is `msgBlock`: `firstMatching` returns early only
on `errPartialCache`/`errNoCache`, and `firstMatchingMulti`'s three `cacheLookup`
sites do not propagate the error (one discards it and reports success). Filed as
[nats-server#8632](https://github.com/nats-io/nats-server/issues/8632). Reproduced
against `v2.15.0`; byte-identical on `main` as of the filing date. Unblock: an
upstream correction plus a reviewed pin bump.

**Re-run on v2.15.1 (2026-10-11): reproduces.** Both enumeration witnesses deliver only
the later record and never the damaged one, with no error and no gap; the undamaged
control delivers both. Upstream status on that date: #8632 is open, and the latest
maintainer comment there (2026-09-22) treats a checksum failure as lost data.

## issue #111: a point-read failure reads as absence

The broker-side face of the same store conflation: an individual subject's read
failure is classified as "the subject is empty", which is what a release or
ownership decision then acts on. Same root cause as
[issue #110](https://github.com/nberlee/mqtt-nats-broker/issues/110), different
question (is
an individual subject empty, versus did an enumeration reach its end) and
different repair. Tracked under #8632 rather than filed separately.

**Re-run on v2.15.1 (2026-10-11): reproduces.** A point read of the damaged sequence still
answers `message not found`.

**The point-read absence census was RETIRED on 2026-10-15.** Its two checks that state a
transport fact survive as `internal/mqtt/jsa_msgget_transport_test.go`: every point read
enters through the five `mqttJSA` message-GET methods and one decode arm, no second
decoder of their replies exists, and no direct-get route exists.

## issue #112: a create-only CAS does not prove the subject was empty

Tracked in [issue #112](https://github.com/nberlee/mqtt-nats-broker/issues/112).

A `Nats-Expected-Last-Subject-Sequence: 0` publish is admitted at a new sequence
even when the subject holds a record the server could not read (a damaged occupant
that reads back `message not found`), so a create-only CAS does not establish that
the subject was empty. This refuted
[issue #107](https://github.com/nberlee/mqtt-nats-broker/issues/107)'s original
substrate premise. Reproduced
against `v2.15.0` on 2026-09-21. Upstream issue to be filed after alpha.

**Re-run on v2.15.1 (2026-10-11): reproduces.** The create-only publish is admitted at a new
sequence over the unreadable occupant, which is gone afterwards and stays gone across a
restart; the readable-occupant, empty-subject and nonzero-expectation controls behave as
before. The two witness variants print output identical to a v2.15.0 run of the same
module. On #8632 the maintainer's 2026-09-22 comment says an expectation of zero "still
legitimately passes" over a record that failed its checksum.

## INPROCESS-REFUSAL: a refused in-process connect waits out the write deadline

**What.** A client connecting through `Server.InProcessConn` (nats.go's `InProcessServer`
option) whose connect the server refuses (a wrong password; an account
`max_connections` limit) does not get the refusal. Over TCP the same connect fails in
milliseconds with `nats: Authorization Violation` or `nats: maximum account active
connections exceeded`; in-process it fails after 10 s with `io: read/write on closed
pipe`. A correct connect is unaffected (under a millisecond).

**Where (nats-server v2.15.1, nats.go v1.54.0), from goroutine stacks taken
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

**Witness.** A test outside this repository:
one server from a configuration file, a wrong-password connect and an over-limit connect,
each over TCP and in-process; it requires the in-process refusal to match the TCP error
within the 2 s connect timeout, and fails on both cases on v2.15.1.

**Here.** Embedded mode refuses a wrong MQTT credential about 10 s late, and a wrong
service credential fails startup after the same delay with the pipe error.
`docs/CONFORMANCE.md` states it; the broker does not work around it (no buffered pipe,
no shortened `FlusherTimeout`, no extra reader). Reporting it to both projects is a
post-alpha roadmap item (section 8); the user writes the reports personally.

## CALLOUT-CONN-TYPES: an auth callout issued user's connection types are not applied

**What.** In operator mode, a user JWT that an auth callout issued with
`allowed_connection_types: [MQTT]` was admitted on a standard client connection on
v2.15.1. **Fixed in v2.15.2**: the callout's reply handler now checks the issued user
with `connectionTypeAllowed`. The witness fails on v2.15.1 and passes on v2.15.2
(2026-10-27), and the pin and the floor moved to v2.15.2 that day. Nothing to file.

**Here.** Nothing in the broker depended on the defect: auth passthrough always dials a
standard connection, and the broker reads the issued user's account and permissions
itself. At the 2.15.2 floor a callout that restricts its issued users to `MQTT` alone
refuses them on that standard connection, which `docs/AUTHENTICATION.md` now says.

## PUSH-DIDNOTDELIVER: a push consumer does not requeue a delivery it knows failed

**What.** nats-server `consumer.didNotDeliver` runs when a delivery could not be routed.
In pull mode it puts a still-pending sequence on the redelivery queue ("we know it was not
delivered"); in push mode it only clears `o.active` and rechecks interest, leaving the
sequence pending, so it is redelivered only at AckWait. A push consumer learns of lost
interest asynchronously (`loopAndGatherMsgs` gates on `o.active`, updated from the
interest channel), so a message selected just after the last subscriber's UNSUB is routed
to nobody.

**Where.** `server/consumer.go` `didNotDeliver` and `loopAndGatherMsgs`, v2.15.2 and
`main` (checked 2026-11-04). No existing upstream issue found. Witness (outside this repository):
a racing publisher, the subscription drained before resubscribing; 6 of 200 rounds are
redelivered only at AckWait.

**Here.** The remaining part of a QoS 1/2 offer racing its subscriber's detach
(`docs/CONFORMANCE.md`). The broker never sees a reply subject for such a push and does
not work around it.

## DRAIN-ACROSS-RECONNECT: a drained subscription keeps its server interest across a reconnect

**What.** nats.go `Subscription.Drain` keeps the subscription in the connection's map
until its queue is empty, suppresses the UNSUB while reconnecting, and
`resendSubscriptions` re-sends SUB for every entry, draining ones included; `checkDrained`
then removes it locally with no UNSUB. The server keeps the interest until the next
reconnect and routes to a sid the client discards.

**Where.** nats.go v1.55.0 and `main` (checked 2026-11-04): `unsubscribe` (drain mode),
`checkDrained`, `resendSubscriptions`. No existing upstream issue found. Witness (outside
this repository): server interest returns with the reconnect and survives the drain's local
removal, 3 of 3 runs.

**Here.** A delivery subscription drained at detach across a service-connection reconnect
keeps a JetStream push consumer active while its session is offline; its messages are
pushed and discarded on every AckWait cycle until the next reconnect
(`docs/CONFORMANCE.md`). The broker does not work around it.
