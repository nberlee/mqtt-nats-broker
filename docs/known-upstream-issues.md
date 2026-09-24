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

Pin at time of writing: `nats-server v2.15.0` (latest release). Every issue below
was checked against `main` (`v2.16.0-dev`) and is still present unless noted.

## Index

| ID | One line | Upstream | Blocks | Status |
|---|---|---|---|---|
| MAXMSGSPER-REVIVAL | A `MaxMsgsPer:1` limit-evicted record revives after raw-block recovery when its successor was explicitly deleted | PR [#8506](https://github.com/nats-io/nats-server/pull/8506) (closed unmerged) | the in-flight register path on `$MQTT_inflight` at crash recovery | Known upstream, not fixed; not (re)filed |
| F-108 | A failed store read and an empty suffix are the same response, so every enumeration reads a read failure as its own end | Issue [#8632](https://github.com/nats-io/nats-server/issues/8632) | F-105 affine cleanup, supported `scanPubRelRecords` | Filed |
| F-109 | Point-read store failures are classified as absence, invalidating release and ownership decisions | Same root as #8632 (broker-side face) | supported release/ownership point reads | Tracked, not separately filed |
| F-110 | A conditional creation is admitted over a record the server could not read, so a create-only CAS does not prove the subject was empty | To be filed | F-105 substrate premise (create-only CAS as an absence proof) | Witness ready, not yet filed |

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
stale or missing `index.db`. A clean shutdown writes a fresh index and recovers
correctly.

**Exposure.** Present on `HEAD` independently of F-105: clean `HEAD` fails
`TestMQTTMaxAckPending` when a single inert permanent record is added to
`$MQTT_inflight`, so any account whose inflight stream is non-empty at a crash
restart is exposed. Not claimed for every restart - only crash or stale-index
recovery.

**F-105 does not widen it.** An earlier form of the shared-group transfer wrote its
per-generation ledger to `$MQTT_inflight` on every resume, including plain sessions,
which turned single-session accounts (previously safe, empty inflight at restart)
into neighbours of their own registers - a regression. Those records now live on the
dedicated `$MQTT_shared` stream (`DenyDelete`, so no tombstone and no revival shape),
so a plain session leaves `$MQTT_inflight` collapsible again. That removes the
regression; it does not fix the multi-session exposure, where other sessions' own
registers are the neighbours. That residual is HEAD's and is documented in
`docs/CONFORMANCE.md`.

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
tombstone is written (`server/filestore.go` near L6097 on v2.15.0). Moving the
broker's own permanent records off `$MQTT_inflight` (done: they are on
`$MQTT_shared`) removes the single-session REGRESSION F-105 would otherwise cause,
but it does not fix the multi-session exposure and is not claimed to: the register
path on `$MQTT_inflight` remains subject to this defect until upstream corrects it.

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

**Unblock.** An upstream correction plus a reviewed pin bump closes this defect and
the register path is no longer exposed. F-105's shared-group transfer does not wait
on it: with its records relocated to `$MQTT_shared` it neither triggers nor widens
the defect, and it ships with the same residual `HEAD` already carries.

## F-108: a failed read reads as an empty suffix

Every error from `LoadMsg`, `LoadNextMsg` and `LoadLastMsg` in the message-GET
handler becomes `NewJSNoMessageFoundError()`, so an enumeration reads a read
failure as its own end. Receiver is `msgBlock`: `firstMatching` returns early only
on `errPartialCache`/`errNoCache`, and `firstMatchingMulti`'s three `cacheLookup`
sites do not propagate the error (one discards it and reports success). Filed as
[nats-server#8632](https://github.com/nats-io/nats-server/issues/8632). Reproduced
against `v2.15.0`; byte-identical on `main` as of the filing date. Witness at
`~/.claude/lanes/f-109-remainder/` (`UPSTREAM-WITNESS.md`, `issue1/`). Unblock: an
upstream correction plus a reviewed pin bump. The affected-caller analysis was carried in
the roadmap's former chapter U, now removed; this section plus the lane witness are the
standing record until the issue is filed after alpha.

## F-109: a point-read failure reads as absence

The broker-side face of the same store conflation: an individual subject's read
failure is classified as "the subject is empty", which is what a release or
ownership decision then acts on. Same root cause as F-108, different question (is
an individual subject empty, versus did an enumeration reach its end) and
different repair. Tracked under #8632 rather than filed separately. The point-read
absence census that maps which broker decisions rest on this answer is
`docs/findings/point-read-absence-census.md`.

## F-110: a create-only CAS does not prove the subject was empty

A `Nats-Expected-Last-Subject-Sequence: 0` publish is admitted at a new sequence
even when the subject holds a record the server could not read (a damaged occupant
that reads back `message not found`), so a create-only CAS does not establish that
the subject was empty. This refuted F-105's original substrate premise. Reproduced
against `v2.15.0` on 2026-09-21. Fact sheet at
`~/.claude/lanes/f-109-remainder/ISSUE-2-facts-for-defect-template.md`, witness in
`issue2/`. Upstream issue to be filed after alpha; the fact sheet and witness in the
lane are the standing record.
