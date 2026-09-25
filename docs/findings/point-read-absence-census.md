# The point-read absence census

Canonical for **which decisions in this broker rest on a store read that could not be
completed**. Owned by [F-109](../ROADMAP.md), which it does not close.

**This is an INCOMPLETE CENSUS INCREMENT, and it is not the census.** What the increment
adds is a computed conservative closure, reviewed treatment of every discovered
interpreter candidate, and a MEASURED semantic-review deficit. **The census and the
repair-dependency map remain incomplete.** F-109 stays open.

The predecessor of this revision staged its unfinished expansions on a 181-entry
pending-expansion frontier. That frontier is gone as a MECHANISM — the closure engine
performs those expansions — and retained as DATA, because what a reached-function set
cannot carry is the MEANING of each owed obligation.

**Why the deficit is the deliverable rather than an embarrassment.** An edge with no
reviewed clause PROPAGATES. So an engine that computes the fixpoint, resolves
destinations and classifies the interpreters satisfies every clause of a
closure-completeness property while its reached set is 95% of the package — at which
point every function is a conservatively reached caller and a repair-dependency map
built from that population identifies nothing. Closure completeness is therefore
satisfiable VACUOUSLY, and the only thing standing between a green suite and an
unwarranted claim is that the unreviewed remainder is counted and stated. It is, below,
and a test fails if it is ever zero.

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

Measured at the reviewed revision by `internal/mqtt/point_read_absence_engine_test.go` and
compared against this document by `point_read_absence_dossier_test.go`. **Only the six
marked blocks are verified**, and they combine live measurements, an immutable reviewed
SNAPSHOT of the reads, and reviewed evidence constants — not one uniform derivation. Figures
outside those blocks are screened, not verified; the status table at the end states which
class each line of this document is in. Counts drift; the mechanisms behind them are what
hold.

### The hand-reviewed tier, unchanged from the checkpoint

<!-- checked-block: hand-reviewed-tier -->
| | |
|---|---|
| direct per-subject primitive callers (declared nodes) | **44** |
| accounted candidate boundary edges | **697** — 208 expand, 47 link, 442 discharge |
| excluded sites with a per-site F-108 ruling | **22** |
| named explanations | **13** |
| retained former-frontier entries | **180** |
| recorded conversion withdrawals | **2** |
<!-- end checked-block: hand-reviewed-tier -->

### The computed closure

<!-- checked-block: computed-closure -->
| | |
|---|---|
| admitted origins (seed inventory) | **318** |
| reached (function, arrival) states | **12051** |
| reached triples | **3231134** |
| conservatively reached functions | **1811** of 1905 |
| reviewed discharges | **370** |
| reviewed propagations | **237** |
<!-- end checked-block: computed-closure -->

### The measured semantic-review deficit

<!-- checked-block: measured-deficit -->
| | |
|---|---|
| default-only (edge, origin, arrival) triples | **90513037** |
| functions carrying at least one | **1808** |
| unresolved callee obligations | **5850** |
| possible-only dispatch obligations | **7887** |
| unresolved reverse-call obligations | **1173** |
| unresolved state obligations | **314** |
<!-- end checked-block: measured-deficit -->

### The classification worklist

<!-- checked-block: classification-worklist -->
The four rows below are a PARTITION of the discovered members. Note the first: of the **44**
declared nodes, **36** are also discovered mechanically, and the other 8 are not — they call
no 404 predicate and reference no absence sentinel, so nothing proposes them and they are
accounted only as declared nodes. Publishing 42 in this table made the rows sum to 155
against 147 members, which holistic round 6 caught.

| | |
|---|---|
| discovered members | **159** |
| already declared nodes, also discovered here | **36** |
| dispositioned `interprets` | **17** |
| dispositioned `consumes` | **57** |
| dispositioned `not-an-interpretation` | **49** |
<!-- end checked-block: classification-worklist -->

**Reviewed propagation is counted apart from reviewed discharge** because propagation is
frequently the right answer and is not a missing discharge. **Resolution status is a
separate axis from review status**: an edge can be reviewed and still carry an unresolved
obligation, and folding the two would let a three-way partition conceal one.

### The interpreting sites, by name

Counted figures can be swapped; names cannot. These are the worklist members reviewed as
INTERPRETING a per-subject read themselves — the population a repair has to act on — and
each carries its axes, what its absent arm authorizes, and its outstanding repair premises
in `point_read_absence_dispositions_test.go`.

| Site | |
|---|---|
| `mqttClassifyMsgGetResponse` | the Absent/Indeterminate split for the register, marker and source batches |
| `mqttMarkerLoadFromResponse` | the unsent-marker verdict a resume consumes |
| `(mqttMarkerLoad).observe` | turns a marker load into an OBSERVATION |
| `(mqttInflightSourceCache).resolve` | Absent becomes `mqttInflightSourceComplete` |
| `(mqttAccountSessionManager).createOrRestoreSessionWithin` | treats the session as absent and creates a fresh one |
| `(mqttAccountSessionManager).evictDisplacedOnRemotePersist` | proceeds with no record; a non-404 is only warned |
| `(mqttAccountSessionManager).loadExpiringSessionRecord` | concludes the session was cleared, ending the expiry work |
| `(mqttAccountSessionManager).reconcileRemoteSessionExpiry` | reconciles against a record it may have failed to read |
| `(mqttAccountSessionManager).proveGoneRetirement` | the LICENCE for gone-retirement; its non-404 refusal cannot fire |
| `(mqttSession).claimTeardown` | abandons the claim as already released |
| `(mqttSession).releaseTeardown` | "absent: done", which licenses the deletions that follow |
| `(mqttAccountSessionManager).reconcileResidentOwnership` | withheld on the error it can see, defeated on the one it cannot |
| `(mqttAccountSessionManager).loadFenceSession` | the Will fence acting on an absent session record |
| `(mqttSession).disposalForDirectRejection` | Unsettled on ANY failure — one of the few sound mappings |
| `classifyRecordAgainstAttempt` | `mqttRecordAttemptGone`, from an error passed as an ARGUMENT |
| `classifyAttemptReadback` | `mqttAttemptGone`, read as released by a teardown or expiry |
| `classifyRejectedProbe` | `mqttRejectionForeign` on a gone record, which disposes the connection |

## The obligation algebra

An **origin** is `(function, occurrence, component)`. A function is not the unit:
`checkResourceOverwrites` performs three reads with different consequences — two inside
function literals — `enumerateGenerationObligations` performs two, and `loadSessionMsg`
interprets nothing yet carries an obligation onward. The component grammar is finite:
`ret#i` (positional, because result names are optional), `lit#k/ret#i` for a callback
result, `field:Base.name`, `elem#k`, `pkgvar:name`, and `unknown-effect` as the
conservative fallback.

**Seed admission never depends on attribution succeeding.** A seed whose effect cannot be
attributed is admitted with `unknown-effect` and stays outstanding. If admission depended
on attribution, "reviewed interpreter" would become a discovery filter, which is exactly
what the frontier's replacement exists to prevent.

An **arrival** is how an obligation reaches a function: `origin-local`, `arg#i`, `recv`,
`callback-ret#i`, `ret-of-callee`, `state:field`, `state:elem`, `pkgvar:name`, and
`unknown-arrival`. Propagation is over `(function, origin, arrival)` TRIPLES, and a new
arrival distinction re-admits a triple even where the function was already processed —
without that, an origin already handled through argument 0 would record a later state
arrival as a predecessor and never propagate it.

Argument positions are emitted for EVERY position, because without data flow the engine
cannot establish which parameter receives the obligation. Emitting all of them is the
conservative direction and keeps positions exact, so that a clause reviewed for argument 0
can never cover an arrival at argument 1.

**Two occurrences can name ONE obligation, and that is DECLARED rather than inferred.**
`mqttClassifyMsgGetResponse` qualifies twice — by applying the 404 predicate and by producing
the absence sentinel — and both are one classification of one response, so its occurrences
canonicalize onto a single origin with both provenances retained. The relation is never
inferred from sharing a function: `checkResourceOverwrites`' three reads are the standing
counterexample.

**Carrier transfer is computed per ORIGIN, never per batch.** Unioning a batch's callback
arrivals meant that adding a `lit#k/ret#i` origin to a batch REMOVED an ordinary origin's
`unknown-arrival` at a zero-argument call — admitting one origin deleted another's triple,
which is not monotone and made the result depend on batching. A fixture pins it.

The predecessor relation is **reconstructed rather than stored**: materialising one entry
per admission cost gigabytes on this package, twice. Reconstruction is complete by
construction, where a stored sample would have been a count — and a count of omitted
predecessors cannot detect a substitution.

Reconstruction and traversal must agree on which edges EXIST, not merely that some
predecessor was found. Both therefore consult one definition of CALLER-edge eligibility.
Holistic round 5 found the asymmetry: a wrapper method sharing its delegate's name —
`(mqttAccountSessionManager).loadInflightRecord` calling `(mqttJSA).loadInflightRecord` —
makes the callers-by-name lookup return the wrapper itself, and the traversal skipped that
self edge while the reconstruction reported it. A nonempty-predecessor check passes while
the set contains an invented edge, so the fixture compares exact edge identities.

## Resolution, and the four ways this census could shrink

**No call is proved.** The engine claims no dispatch certainty at all: every call site
carries the destinations it NAMES and an unresolved obligation beside them, so no
destination is ever dropped in favour of a manufactured certainty.

That is the claim, and it is deliberately narrower than "no destination is ever missed" —
which would be false. A call through an alias (`observe := m.observe; observe()`) reaches
a method the name-based filter below omits. The call still carries its obligation and the
method still carries its reverse-call obligation, but **an obligation records missing
analysis; it does not propagate an origin into the omitted body**. That is unreviewed work,
counted in the deficit, which is what an explicitly incomplete census is for.

That is a withdrawal, arrived at the hard way. This package declares interfaces
(`brokerStreamAPI` is one), so a package-declared receiver type proves nothing, and a
concrete struct can embed an interface or hold a function-valued field. Without a type
checker, every proof on offer was a hand-written enumeration of Go's scoping rules, and
five review rounds each defeated the current one with a construct the last fix had not
modelled:

| round | the construct that defeated the proof |
|---|---|
| holistic 1 | a receiver spelling rebound elsewhere in the function |
| holistic 2 | a function literal's parameters rebinding it |
| holistic 3 | a later assignment restoring a concrete type |
| holistic 4 | the enclosing function's own parameters |
| plan review of revision 5 | generic type parameters — `func host[target ~int]()` makes `target(1)` a CONVERSION — and local type declarations |

The fourth round withdrew the method case. The fifth withdrew the rest, because the
surviving bare-identifier rule rested on the same shape: it refused when the name appeared
in a recorded set of bindings, and **negating an incomplete inventory asserts absence**. An
enumeration used to refuse is no safer than one used to assert unless it is known complete,
and five rounds are evidence that this one is not. Precision was the only thing `definite`
bought; correctness was what it cost.

**The one claim syntax still supports is about a destination's KIND, not its binding.** A
bare identifier call carries no receiver expression, so it cannot DIRECTLY INVOKE a method
declaration, and methods are not candidates for it. The indirect case is real —
`x := m.observe; x()` does execute a method — and it is RECORDED rather than resolved: the
call keeps its obligation and the method keeps the unresolved reverse-call obligation every
value-referenced function owes. Neither propagates an origin into that body, so this is
declared unreviewed work rather than covered work; the two obligations must not be separated
from the filter that relies on them.

**What the withdrawal cost, measured per cause** rather than asserted. Against the figures
this increment started from (**90784015** triples, **4925** unresolved callee, **4895** possible-only
dispatch):

<!-- checked-block: per-cause-attribution -->
| change | default-only triples | unresolved callee | possible-only dispatch |
|---|---|---|---|
| withdrawing all dispatch certainty | **+1988** | **+0** | **+3917** |
| the bare-call method filter | **-272966** | **+925** | **-925** |
| net, as published above | **90513037** | **5850** | **7887** |

Withdrawing certainty ADDED triples here, because a formerly proved call now propagates to
every name match. That +1988 is an EMPIRICAL result on this package, not a necessary
consequence of the change: how much extra propagation a withdrawn certainty buys depends on
how many name matches the formerly proved calls had. The decrease comes entirely from the
filter, which removes candidate edges that a bare call could never have taken; the 925 calls
it empties move from dispatch obligations to callee obligations rather than disappearing.
<!-- end checked-block: per-cause-attribution -->

The four under-approximation routes, each with a positive record rather than a silent
omission:

| Route | Treatment |
|---|---|
| a call through a function value whose name collides with a declared function | speculative destinations PLUS a retained unresolved obligation |
| interface dispatch to a non-package type | the same |
| a caller invoking through a function value | an unresolved reverse-call obligation for every function whose name appears in the value-reference inventory, including package initializers and method values |
| a mutation with no resolvable field | an occurrence-keyed unresolved-state obligation |

## The conversion of the checkpoint's records

A checkpoint record says "this edge is discharged, for this reason", and it was reviewed
against the obligation the node's own read creates — an obligation BORN in that function,
arriving nowhere. It was never reviewed against an obligation arriving from elsewhere. So
each record converts to one clause whose applicability relation is
{the node's own read origins} x {`origin-local`}, and everything else the closure delivers
to that edge is default-only.

**The comparison is against an IMMUTABLE CHECKPOINT DIGEST**, one per hand-authored
record, over the reviewed content itself — disposition, explanation, reason,
destination. Comparing a derived clause against the live record it was derived from
establishes only that the derivation copied something.

**The integrity property is PRESERVATION, not identity.** A total, injective map is
satisfied by widening a clause and equally by narrowing reviewed coverage away into the
deficit. The test recomputes the expected relation from an independently persisted
snapshot of each node's read occurrences, taken at the reviewed revision, so a builder
that covers one pair more or one pair fewer fails.

**Two withdrawals are recorded rather than hidden.** `checkResourceOverwrites` and
`enumerateGenerationObligations` are the package's only multi-read nodes, and their
records do not say which read they were reviewed against — a `PRESENT-ONLY` discharge is
relative to a PARTICULAR read's absent arm. Their coverage is withdrawn and their edges
are default-only, which makes the deficit larger and honest rather than smaller and
unwarranted.

<!-- checked-block: conversion-withdrawals -->
| withdrawn node | reads | why the record cannot be attributed |
|---|---|---|
| `(mqttAccountSessionManager).enumerateGenerationObligations` | 2 | the record names no read, so neither absent arm is the one discharged |
| `(mqttJSA).checkResourceOverwrites` | 3 (two inside function literals) | the record names no read, and its reads have different consequences |
<!-- end checked-block: conversion-withdrawals -->

## The classification worklist, and the three dispositions

The worklist is the union of mechanical candidates — a 404 predicate, or a reference to an
absence sentinel whose set is an AST fixpoint over guarded arms — and separately persisted
reviewed additions carrying their provenance. It **bounds classification work, never
coverage**: a function outside it keeps its conservative obligations and its `unreviewed`
status, and absence from it never manufactures a classification.

Mechanical discovery is deliberately noisy, so every member carries one of three reviewed
dispositions: `interprets` (owing the axes and repair premises a node owes), `consumes`
(naming the producing site and what the verdict authorizes here), and
`not-an-interpretation` (naming the error identifier or primitive it actually tests). The
two large non-interpretation families are a different error identifier — consumer, stream,
sequence or CAS — and a fixed-sequence or exhaustion read, which is F-108's side of the
boundary.

The sentinel fixpoint is DISCOVERY ONLY and never an exemption. It derives
`mqttRepairSaveForeign` and `mqttUncertainForeign` without either function being named,
which is the property a hand-listed set cannot have.

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
- An **enumeration** inherits [F-108](../ROADMAP.md), an upstream defect filed as
  nats-server#8632.
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
| discharge citation, with the three rejected reasons | **implemented** |
| axis well-formedness with applicability | **implemented** |
| state destination-set accounting for field mutations | **implemented** |
| exclusions accounted per site | **implemented** |
| seed and arrival integrity, including unconditional admission | **implemented** |
| clause scoping over (origin, arrival) pairs | **implemented** |
| conversion preservation against the checkpoint's records | **implemented** |
| former-frontier conversion by source edge, destination and meaning | **implemented** |
| unresolved obligations retained and reported per kind | **implemented** |
| worklist coverage, with a reviewed disposition per member | **implemented** |
| disposition well-formedness, including a repair record per interpreting site | **implemented** |
| predecessor relation reconstructible for a reached state | **implemented** |
| traversal and reconstruction agree edge-for-edge at fixpoint | **implemented** |
| numeric sections RENDERED from measurement and compared byte-for-byte inside declared blocks | **implemented** |
| every line of this document is in exactly one ownership class, and **no line is implicitly treated as verified** | **implemented** |
| class 1, inside a checked block: authoritative, byte-for-byte | **implemented** |
| class 2, an exact OWNED line: excluded from screening because a named check enforces it verbatim, and that enforcement is itself tested | **implemented** — one line, the attribution baseline |
| class 3, the UNCHECKED REMAINDER: outside authoritative figure verification, SCREENED only — a declared screening EXEMPTION stays class 3, it is not promoted | **screening only, by design** |
| "every published figure is owned" | **WITHDRAWN.** A clean run means only that no violation of the screening rules was detected. Screening catches an accidental or misplaced figure; it cannot establish a census of published figures. An undetected class-3 figure is a limitation of this contract, not a defect |
| figures in class 3 today, as EXAMPLES and not an inventory | **`95%` of the package, the `16/5/1` caller counts, `28 of the 42` — and any other outside-block count — are NOT checked** |
| evidence currency: a fingerprint over each record's own inputs, including its applicability relation | **implemented** |
| generated explanation membership, compared by occurrence and receiver identity | **implemented** for `LOCK`; the other twelve explanations keep hand-authored membership |
| dossier agreement on every figure INSIDE a checked block, byte-for-byte | **implemented** |
| the semantic-review deficit measured and nonzero | **implemented** |
| **reviewed accounting of the default-only remainder** | **FROZEN** by the boundary ruling of 2026-09-21 (`docs/known-upstream-issues.md`): the census is neither deleted nor extended. This was the census's remaining bulk and is no longer owed |
| **the complete interpretation-site population** | **FROZEN**, same ruling. The population question stays open as a record, not as work |
| **the repair-dependency map** | **FROZEN**, same ruling; reviewed repair premises exist only for the declared nodes and the interpreting worklist members, and no broker-side repair is planned |
| runtime repair of any classified site, and its behavioural witness | **NOT PLANNED**: the conflation of a failed read with absence is the data layer's defect (nats-server#8632 for the enumeration half; the point-read half to be filed), and its correction upstream is what closes F-109. See F-109 in `docs/known-upstream-issues.md` |

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

`internal/mqtt/point_read_absence_census_test.go` carries the hand-reviewed contract;
`internal/mqtt/point_read_absence_manifest_test.go` carries the 42 declared nodes;
`internal/mqtt/point_read_absence_closure_test.go` is the engine;
`internal/mqtt/point_read_absence_conversion_test.go` converts the checkpoint's records
and enforces preservation; `internal/mqtt/point_read_absence_worklist_test.go` builds the
seed inventory and the worklist; `internal/mqtt/point_read_absence_dispositions_test.go`
carries the reviewed disposition of every worklist member;
`internal/mqtt/point_read_absence_engine_test.go` carries the properties and the deficit;
`internal/mqtt/point_read_absence_evidence_test.go` carries evidence currency and the
generated `LOCK` membership, with its snapshot in
`internal/mqtt/point_read_absence_evidence_data_test.go`;
`internal/mqtt/point_read_absence_fixture_test.go` carries the discriminating fixtures;
`internal/mqtt/census_scan_test.go` is the neutral scanner.

The sharing with the [persisted-identity census](persisted-identity-census.md) is real and
not merely intended: `censusScanIngress` now calls `censusScanPackage` rather than parsing
the package a second time. What stays with F-034 is its POLICY — which callees count as
ingress, and that a site's identity for its purposes is the unqualified function name,
since this census needs the receiver and the occurrence index and taking them there would
change what F-034's manifest is keyed on. F-034's own bijection and its mutation-pinned
claims are unchanged by the move, and its tests were run to confirm it.
