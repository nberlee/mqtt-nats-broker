# The point-read absence census

Canonical for **which decisions in this broker rest on a store read that could not be
completed**. Owned by [F-109](../ROADMAP.md), which it does not close.

**This is a CHECKPOINT, not the census.** At the reviewed revision, the direct
per-subject primitive-caller inventory is complete, and its candidate edges are
accounted with outstanding expansions explicitly recorded. The complete
interpretation-site population, recursive consequence accounting and repair-dependency
map remain open. **Expansion completion is owed and unenforced; frontier integrity is
enforced** — every expansion resolves either to an accounted node or to an explicit
pending-frontier entry carrying its source edge, its destination and the obligation that
destination would receive, and a missing, stale or mismatched entry fails a test.

## The finding this enumerates

In `github.com/nats-io/nats-server/v2 v2.15.0` every error returned by `LoadMsg`,
`LoadNextMsg` and `LoadLastMsg` becomes `NewJSNoMessageFoundError()` in the message-GET
handler (`server/jetstream_api.go:4017-4021`). This package implements a deliberate
ABSENT versus INDETERMINATE distinction. That distinction is therefore **inverted for
exactly the case it was written to protect against**: a failed store read is classified
ABSENT.

F-109's bullet carries a caller matrix and says in terms that it is "a sample, not the
population". This document is the population question's answer for the direct
primitive-caller tier, and it exists because the sample is not merely incomplete but
**systematically** so:

- **A forwarding wrapper hides a family.** `mqtt.go:6716` `loadSessionMsg` returns the
  primitive's error untested, and its **16 non-test callers** are the real
  interpretation sites. Three are named in F-109's own bullet. An inventory keyed on the
  primitive call would carry one row here and hide all sixteen.
- **A classifier can be reached without calling a primitive at all.**
  `classifyRecordAgainstAttempt` (`sess_resume_reconcile.go:246`) maps
  `JSNoMessageFoundErr` to `mqttRecordAttemptGone` from an error handed to it as an
  **argument**.
- **A guard that states the rule can still be defeated.** `gatherAdoptedReleaseFacts`
  (`outbound_release_gate.go:302`) says in capitals that "AN UNRESOLVED READ IS AN
  ERROR, never an absence", and that treating an unreadable closure as missing "would
  take the gate's vacuous path — releasing exactly the generation it exists to protect".
  Its `loadGenClosure` maps a 404 to `mqttOutboundAbsent`, and `finishAdoptedTeardown`
  calls the gatherer **on production paths**.
- **A site can look fail-closed and not be.** A plan-review round caught this census's
  own plan misreading `unsub_drain.go:3287`: a non-404 returns `ok=false`, but a **404
  returns `ok=true`**, certifying a PUBREL scan complete from a read that may have
  failed.

## What is enumerated, and what each number is

Measured at the reviewed revision by `internal/mqtt/point_read_absence_census_test.go`,
which derives every figure from the AST rather than from this document:

| | |
|---|---|
| direct per-subject primitive callers (nodes) | **42** |
| accounted candidate boundary edges | **663** — **185** expand, **45** link, **433** discharge |
| pending-expansion frontier entries | **181** |
| excluded sites with a per-site ruling | **22** |
| named explanations | **13** |
| local mapping | 32 `classified-absent`, 8 `withheld`, 2 `forwarded` |
| reachability | 12 `live-supported`, 28 `live-read-unavailable-duty`, 2 `experimental` |

## The transport invariant

> Every point read in this package is issued by an `mqttJSA` method that calls
> `newRequest` or
> `newRequestExMulti` with request kind `mqttJSAMsgLoad`, and every such response is
> decoded as
> `*JSApiMsgGetResponse` at the single reply-dispatch arm in `processJSAPIReplies`.

Five constructors and one decode arm. `loadLastMsgFor` and `loadLastMsgForMulti` are
per-subject and in scope; `loadMsg`, `loadMsgsMulti` and `loadNextMsgFromSeq` are
fixed-sequence or exhaustion primitives and are F-108's.

**Enforcement is split, and the split is the honest part.** An occurrence count over one
request kind cannot enforce the invariant, because a point read added through another
request kind or decoder leaves the five constructions untouched. So: a test enforces
that only accounted functions name `mqttJSAMsgLoad`; a second enforces that only
accounted functions reference `JSApiMsgGetRequest`, `JSApiMsgGetResponse` or
`JSApiMsgGetT`, and that no direct-get route has appeared. **That these identifiers are
the whole of the message-read routing surface is a verified invariant checked by
READING, not by a test.** A point read reaching this broker by another route is in scope
for this census; it is not out of scope for bypassing the invariant.

`STREAM.INFO` subject counts, consumer lookups and lists, store acknowledgements and
consumer delivery are different response mechanisms and are not point-read absences.
Where one is a guard or premise of an accounted node, it is recorded on that node.

## The unit, and why it is not the loader call

A function **defers** when it hands the primitive's error or response on untested — by
returning it, or by passing it as an argument. Deferral is structural, not a
disposition: the closure expands to the callers until every leaf tests the error. At
this revision the direct result-forwarding layer is `loadSessionMsg` (16 callers),
`loadInflightRecordsMulti` (5) and `loadNextAcceptFact` (1), the response classifiers
taking a `*JSApiMsgGetResponse` are `mqttClassifyMsgGetResponse` and
`mqttMarkerLoadFromResponse` with `loadRetainedMessages` classifying inline, and the
error-carrying parameter classifiers are `classifyRecordAgainstAttempt`,
`classifyRejectedProbe` and `classifyAttemptReadback`.

**None of those lists is a definition.** They are what the closure found; the definition
is the accounting rule below, and the lists are recorded so a reader can check the rule
against them.

## The accounting rule

> For every reached node, mechanically enumerate candidate boundary edges:
> return-to-caller,
> calls into callees, and state-write-to-reader. Every candidate has an occurrence-keyed
> accounting entry. Each entry either **expands** the destination, **links** to an
> already-accounted obligation while retaining provenance, or **discharges** that
> particular
> edge with a cited reason. Function roles never suppress enumeration.

Discovery is conservative: callers and call sites are enumerated without first proving
they carry absence, and state references are enumerated conservatively where precise
identity is unavailable. **Calls into callees means every call expression** — nested
literals, deferred calls, goroutines, conversions, and external or unresolved
destinations included. "External" alone is not a discharge reason: an external callee
can transport a value or cause a relevant effect, and `loadOutboundExchange` is the
worked example — it references `JSNoMessageFoundErr` nowhere and handles every read
error in one arm, and still carries the distinction downstream by wrapping the original
with `%w`.

A discharge explains why **that edge** carries no further relevant consequence, covering
returned values, control effects and state effects. Three reasons are rejected outright
by a test, because each is a way of saying "I stopped looking" that reads like a reason:
*"this function already decides"*, *"one caller level was reviewed"*, *"no sentinel
spelling occurs"*.

Repeated reasoning is factored into **named explanations with explicit edge
membership**. There is no wildcard: a wildcard would cover a call added later and defeat
the drift detection this census exists to provide.

## The axes

| Axis | Values | Applies to |
|---|---|---|
| local mapping | `classified-absent` / `withheld` / `discarded` / `forwarded` | every node |
| no-response | `distinguished` / `collapsed` / `not-applicable` | results originating from a batched primitive |
| what absence authorizes | free text, required | every node that interprets |
| classification evidence | `defective-mapping-demonstrated` / `mapping-sound` | every node |
| consequence evidence | `demonstrated` / `suspected` / `none-identified` / `not-applicable` | every node |
| reachability | `live-supported` / `live-read-unavailable-duty` / `test-only` / `experimental` | per caller chain, aggregated |

Two of these need their reasons stated rather than assumed.

**`withheld` is a mapping value and licenses no termination.** A site that refuses on
every read error still owes every one of its edges, because sharing an arm establishes
neither equal returned values nor equal state effects.

**`live-read-unavailable-duty` is its own value because a gate can perform live reads
whose duty is unavailable.** 28 of the 42 nodes are F-105's outbound substrate, which no
production path admits an obligation through — but `gatherAdoptedReleaseFacts` reads the
closure, the ledger and the completion certificate before its vacuous return, and
`finishAdoptedTeardown` calls it on production paths. Calling that "withheld" would hide
a live read. The statement that production does not create the inventory records the
duty needs is an **implementation-reachability fact** about this revision; it is not a
rollout or cutover condition, and it does not erase the reads.

**`absent-is-success` is deliberately not a value on any axis.** A site that reads a 404
as confirmation of a removal it was trying to achieve — `checkPurgeConfirmed` accepting
a chunk as purged — is `classified-absent`, with what it authorizes recorded. The intent
to delete does not make the evidence sound.

## The F-108 boundary

Fixed-sequence absence is F-108's; per-subject absence is F-109's, and that is coherent
only within a proven stream incarnation, because a fixed sequence cannot acquire a
replacement there while a subject can.

The boundary is applied as a **per-site ruling, not as a rule about a method name**: an
exact-subject search starting at sequence 1 could decide subject occupancy and would
then be F-109's however it is spelled. All 22 excluded sites carry their own ruling, and
one of them is recorded as carrying a **defeated guard** — `scanPubRelRecordsCharged`,
whose 404 arm returns `ok=true` and so certifies its scan complete from a read that may
have failed.

## Repair routing

Every interpreting node carries a repair candidate **and its outstanding premises**.
Naming a fact class does not establish that a repair exists.

- An **exact-sequence re-attempt** is a candidate only where the row identifies the
  available sequence, the bytes to attempt, the incarnation it is coherent within, and
  the settlement semantics. It does **not** inherit F-110.
- A **create-only conditional creation** inherits [F-110](../ROADMAP.md): a create-only
  CAS does not establish that the subject was empty.
- An **enumeration** inherits [F-108](../ROADMAP.md), which is implementation-blocked.
- **Direct multi-last** retains its six unresolved limits, one of which — `UpToSeq`
  pushing the request into HISTORICAL backward selection, which ignores lookup errors —
  is structurally the defect that blocked F-108.
- **Delayed writes** and **concurrent replacement** survive all of the above.

The recurring shape across the interpreting nodes: the absent arm is precisely the one
with no sequence to name, so the site that most needs an exact-sequence re-attempt is
the site that cannot make one.

## What is enforced, and what is owed

| Property | Status |
|---|---|
| transport invariant, request-kind surface (1a) | **implemented** |
| transport invariant, reference inventory and no direct-get route (1b) | **implemented** |
| exact edge-accounting bijection, both directions | **implemented** |
| pending-expansion frontier integrity | **implemented** |
| discharge citation, with the three rejected reasons | **implemented** |
| axis well-formedness with applicability | **implemented** |
| state destination-set accounting for field mutations | **implemented** |
| exclusions accounted per site | **implemented** |
| dossier/manifest agreement on the stated totals | **implemented** (totals only) |
| dossier/manifest agreement by keyed table content | **owed** |
| sentinel fixpoint cross-check | **owed** — an accelerator and a cross-check, never an exemption |
| recursive expansion of the 181 frontier entries | **owed** — this is the bulk of the remaining census |
| the complete interpretation-site population beyond direct primitive callers | **owed** |

## What this does not claim

> The test enforces complete accounting of the stated syntactic candidate set. Field
> matching
> conservatively approximates identity; alias resolution, omitted-update consequences,
> and
> the
> truth of discharges are reviewed. This is not a mechanically proved state-flow
> analysis.

It also does not prove a **classification** true. A row could say `withheld` about a
site that is not, and the test would pass. The discipline covering that gap is review,
and the concrete reason it matters is this census's own history: the plan misread
`unsub_drain.go:3287` as fail-closed, and a review round caught it. Every classification
here is a reviewed reading of the code, cited to a line.

Two further limits, stated rather than implied:

- **Plain local assignments are not enumerated as state edges.** A write to a local dies
  with the frame, and whatever it carries leaves the node through a return or a call,
  both of which are candidate kinds of their own. The state rule covers package
  variables bound lexically, selector, indexed and dereference targets, compound
  assignment and increment/decrement. This is a reading of the accounting rule, recorded
  so a review can block on it.
- **A field mutation's destination set is matched on the field's SPELLING**,
  package-wide, ambiguous bases included. It is noisy by construction — `Gen` has 381
  such references — and a locally constructed record supports a cited discharge of that
  whole class while its real propagation stays accounted through the return-to-caller
  edge. Receiver-only filtering would trade those false positives for unacknowledged
  false negatives, which is the worse error for a census.

## Where it lives

`internal/mqtt/point_read_absence_census_test.go` carries the contract and the
properties; `internal/mqtt/point_read_absence_manifest_test.go` carries the population;
`internal/mqtt/census_scan_test.go` is the neutral scanner.

The sharing with the [persisted-identity census](persisted-identity-census.md) is real and
not merely intended: `censusScanIngress` now calls `censusScanPackage` rather than parsing
the package a second time. What stays with F-034 is its POLICY — which callees count as
ingress, and that a site's identity for its purposes is the unqualified function name,
since this census needs the receiver and the occurrence index and taking them there would
change what F-034's manifest is keyed on. F-034's own bijection and its mutation-pinned
claims are unchanged by the move, and its tests were run to confirm it.
