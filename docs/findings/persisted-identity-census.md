# The persisted-identity census

Canonical for **what durable state this bridge holds, what identities it carries, and
which of those identities another stream issues**. Owned by
[F-034](ha-arbitration.md#f-034), which inherits it; cited by
[F-066](ha-arbitration.md#f-066), whose completeness deferral it does **not** yet
discharge — see the retraction below.

## Why this document exists, and why its shape is unusual

F-033's audit needed this enumeration and deliberately did not attempt it. **Four
attempts were made and each was falsified by review**, every one in the same way: it
was a *filter* presented as a sweep — the streams already known, the `json:` tags, a
keyword grep, a hand-listed carrier set — and two were refuted by an entry in the table
they themselves introduced. F-066 then recorded a **verified sample** of cross-stream
identities and said in terms that it was not a census, deferring completeness here.

**This census does NOT claim to discharge F-066's completeness deferral, and the reason
is stated at the top rather than buried in a residual.** Review of the first
implementation found that the interpretation-surface guard is bijective with a *manually
chosen* set of ingress roots and keys sites by unqualified function name, so it is a
strong drift guard and not a proof of semantic closure: a new branch inside an
already-named function, a new wrapper around the transport primitives, or a same-named
method on another receiver can still pass. The plan's own no-fallback rule says an
unproven closure class means claiming neither completeness nor F-066's discharge, so
neither is claimed.

What follows is therefore an enumeration with two mechanically-defended spans and a
stated frontier — which is worth more than a total claim a fifth review round falsifies,
given that four earlier attempts were each falsified exactly that way.

Its claims are mechanically enforced:

| Claim | Enforced by |
|---|---|
| every field of every persisted artifact carries exactly one disposition | `...CensusFieldCoverage` |
| a container or blob never passes as a leaf | `...CensusContainersDeclareContents` |
| a composite binding cannot be dissolved | `...CensusCompositesArePaired` |
| an embedding site names a type that is itself audited | `...CensusEmbeddingsResolve` |
| F-066's sample is rediscovered, not copied | `...CensusRediscoversF066Sample` |
| every interpretation site is named | `...IngressBijection` |
| the audited ingress surface still matches the code | `...IngressSurfaceIsEnumerated` |
| a declared ENCODING is the carrier the real encoder uses | `...CensusEncodingsAreConsistent` |

All eight live in `internal/mqtt/persisted_identity_census_test.go` and
`internal/mqtt/persisted_identity_ingress_test.go`, and every one of them is pinned by at
least one mutation that was applied, observed to produce a named failing test, and
reverted. The ingress bijection is pinned in BOTH directions (M5a removes a manifest entry,
M5b adds an ingress call in an unnamed function) but its ROOT SET is hand-chosen, which is
the limitation above rather than a missing mutation.

The mutation runs themselves are working notes rather than a durable claim about the
system, so they are not carried here — what belongs in a dossier is the invariant and why
it holds, and for that the discriminating cases are named where they matter: the encoding
axis was falsified twice before it was defended (see below), M5b's first run failed to
kill, and the ingress surface is a guard rather than a proof.

## Closure is anchored to the interpretation surface

F-034's plan review reopened producer closure **three times**, each time with a
transition class deeper inside JetStream: cross-account sourcing and TTL eviction; then
stream lifecycle and configuration mutation; then server-internal delivery and
timer-driven redelivery, which mutate consumer sequence, pending state and timestamps
with no acknowledgement call at all. Every addition was correct, and that is the point —
*"enumerate every durable-state transition"* enumerates JetStream's implementation and
has no dry-out criterion. A fourth round can always add snapshotting, or pause.

**So a transition is in scope if and only if it can change the outcome of an enumerated
bridge INTERPRETATION SITE.** Delivery and redelivery enter through the reads and
callbacks that observe them, rather than as classes of their own — which is also how
deduplication already enters, and exactly why `pubrel_correlation.go` carries an attempt
counter instead of trusting a dedup window.

That yields the completion test the transition model lacked. **A new row is required
only for a new interpretation site, a new observable, a new outcome partition, or a new
authority** — never for a newly discovered nats-server function. If delivery, timer
redelivery, pause, expiry or snapshot restoration merely move an existing observation
between partitions already enumerated, they map to that row.

### The surface, measured — and what the measurement does not prove

290 interpretation sites at 2026-10-15, derived by parsing this package's own source with
`go/ast`. The enumeration reads the code and the manifest is checked against it in both
directions, so the manifest cannot be complete merely by agreeing with itself.

**What it does not prove, stated plainly.** The scan recognises a call only when the
method name appears in a hand-maintained root set, and it keys a site by its *unqualified
enclosing function name*, collapsing every call in that function to one entry. So three
kinds of drift still pass: a new branch or a new ingress call inside an already-named
function; a new wrapper around the transport primitives (`newRequest`, `sendq.push`,
`PublishMsg`) that the root set does not yet name; and a same-named method on a different
receiver. Closing those needs type-aware analysis and site-level keys — receiver-qualified
name plus source position — which is why this document claims a *guard* rather than
closure.

The surface is the **semantic** layer, not the transport. That distinction was not free:
an early version anchored it at the `mqttJSA` methods, and a mutation adding a new caller
of `loadInflightRecord` — a package wrapper, not a transport method — **passed**.
Widening the surface to the wrappers that establish an outcome partition of their own
added **39 sites, a third again of the 117 it had then** — the manifest has grown with the
code since — which is the measure of what the under-anchoring hid.

**It drifted, and was brought current on 2026-10-15.** From F-105 onwards new interpreting
functions were added without rows, and the bijection test failed: 52 functions reached an
ingress method unnamed, and 5 rows named functions whose read had moved into a helper. Each
missing row was written from the function's current body, the 5 stale rows were removed (four
of them now delegate to the new rows' functions), and the guard is green again. The bijection
checks that a row exists, not that its prose is true, so prose accuracy is a review question.

## The inclusion boundary

**In scope: broker-owned durable state.** The nine `$MQTT_*` streams, their messages,
configuration and state; the consumers this bridge creates against them, and those
consumers' configuration and state; the deduplication entries its own `Nats-Msg-Id`
writes create; and records sourced into `$MQTT_rmsgs`, a supported cross-account
topology.

**Out of scope: operator-owned streams that capture MQTT client publishes.** The reason
is **ownership and recovery reach** — this bridge does not own, validate, provision,
recreate or read those streams during any recovery procedure. It is explicitly *not*
"no identity reaches them": MQTT-originated publications do carry bridge-authored
`Nmqtt-*` metadata, including the authenticated No Local origin marker. An earlier draft
gave the weaker reason and review rejected it.

## Artifact families

Six, each inventoried independently rather than derived from another's list — the same
rule the stream validators already follow.

1. **Messages** — record bodies, their subjects, and their headers.
2. **Stream configuration** — including `Metadata`, which is how `$MQTT_teardown`'s
   epoch coupling is carried and which no body walk can see; `StreamSource`; and source
   subject transforms.
3. **Consumer configuration** — the complete `ConsumerConfig`. Some durable configs are
   nested in session records; the ephemeral watch and retained consumers' configs are
   **not**, so a record-body walk misses them entirely.
4. **Consumer state** — `ConsumerInfo.Created` as a JetStream-issued *instance
   fingerprint*, ack floors, delivered counts, pending sets. Ack reply subjects belong
   here too: they sit in no record of ours, but they carry the stream, consumer and
   delivery identities that mutate persisted consumer state.
5. **Stream state** — `Msgs`, `FirstSeq`, `LastSeq` (the orphan sweep's cursor), the
   per-subject map and its keys, deleted-sequence state, and source progress.
6. **Deduplication state and server-generated metadata** — dedup entries and the
   `Duplicates` window, whose entry outlives its message; server-issued sequence and
   timestamp.

## Carriers

Stream metadata; subject components, token by token; headers, including `Nats-Msg-Id`
and the `Nmqtt-*` family; body fields; durable and filter names, including
deterministically derived ones; delivery-subject prefixes and dynamically minted inbox
components; sequence references, with the issuing stream named; hashes and derived
projections; composite identities, with their kind; and **map keys and slice
membership**, which Go reflection does not recurse into.

### Three rules the encoding of real records forced

**A `json:` tag is not the test of persistence.** `mqttRetainedMsg` carries **no**
`json:` tags at all: since F-056 removed the legacy reader it is persisted as headers.
Its `expires` field is persisted and **unexported**; `expiresFromCache` is unexported
and *not* persisted; `Subject` is reconstructed on read from the message's own subject;
and `Topic` is written by the encoder but **ignored** by the decoder, which derives it
from the subject too — both deliberately, so a sourced stream with subject transforms
still decodes. A tag-driven walk sees one of the nine streams as having no fields at
all. So the manifest walks unexported fields and records each field's **encoding** as a
separate axis from its kind.

**An encoding axis nobody verifies is a comment.** Review found `StreamSource.iname`
recorded as a JSON body field while being unexported, and then — after the fix — found
`mqttRetainedMsg.Msg` recorded as one while being the raw message body, appended verbatim
after the header block. Both passed because the reflection tests checked that a field *has*
a disposition, never that the disposition is *true*. Three rules now hold the axis down: a
JSON field must be exported; a type whose wire form a NAMED encoder writes may declare no
JSON field at all; and for `mqttRetainedMsg` a sentinel is round-tripped through the real
encoder and decoder, so a declared carrier is checked against where the bytes actually
land — header block, message body, or nowhere.

**A container is never a leaf.** `Cons` and `Drains` keys are subscription filters;
The **unsent marker** (`mqttInflightMarker`, F-072) is a second artifact on
`$MQTT_inflight`, on `$MQTT.inflight.<hash>.<gen>.<pi>.unsent`: its one field `Nonce` is
an identity-local reference to the register it is the sibling of, and its presence — not
any field — is the durable fact the outgoing DUP flag reads. It is created with the
register in one atomic batch and deleted at the transmission, never rewritten.

`CancelledRegs` elements are `$MQTT_inflight` register nonces; `Props` is a structured
blob with a named decoder; `Msg` is opaque application payload — carried as the message
BODY rather than as a field — with no decoder that could enumerate anything. A map,
slice or `[]byte` field must declare which it is and what its keys and elements carry —
and a byte slice must be *either* a structured blob naming its decoder *or* an opaque
payload, because demanding a decoder for application bytes would invite a fictional one.

**The manifest is keyed by type, not by path.** `ConsumerConfig` is 35 fields and
`mqttPersistedSession` reaches it at four sites, with `mqttPersistedDrain` reaching a
fifth. Path-keying would therefore carry 140 `ConsumerConfig` rows inside that one record
where type-keying carries 35 — 105 of them duplicates, and five chances for the copies to
disagree. A named type is dispositioned once and every embedding site inherits it, with
the site itself a row asserting which type it embeds.

## The two questions F-033's route inventories never asked

**Which recovery procedures name this stream alone?** None, since F-066's enforcement
landed 2026-09-01. They USED TO: seven of the nine validators called the shared `stopAll`
helper naming their own stream, and `$MQTT_sess` and `$MQTT_teardown` each named the
other and called the pair one logical unit — so the provisioning table itself asserted
seven single-stream recovery procedures, F-066's falsified premise stated in code at
seven citable sites. Every one of those now names the nine-stream unit, and startup
refuses an account holding a proper subset. That is an assertion in operator guidance,
**not proof that the procedures are safe**; F-066 is precisely the evidence that some
were not.

**What does this stream's persisted state reference that another stream issues?** The
per-field answer is the manifest.

### A pattern across the population, which a sample cannot produce

The JS domain token appears in the subject space of exactly **three** streams —
`$MQTT_teardown`, `$MQTT_sess` (both its subject spaces) and `$MQTT_willq` — and in none
of the other six. Those three are **exactly** the three F-066 records as *coupled*;
every stream that omits it is one F-066 records as *uncoupled*. Whether that is causal —
the streams whose identities are session-scoped being the ones scoped by domain and
epoch — or coincidence is not asserted here. It is recorded because a pattern across the
whole population is the one thing a verified sample can never yield, and it is the
clearest evidence that this census differs from F-066's table in kind rather than
length.

## What the F-066 oracle found

F-066's fourteen-row sample is applied as an **oracle**, after the independent
enumeration, never as its seed — seeding would inherit the sample's blind spots and make
the completeness claim circular. Every sample row must already be carried, either by a
named manifest entry whose issuer agrees or by an explicitly named artifact family with
a reason.

It found four real defects on its first pass:

1. **`mqttQ2AffClaim` was missing from the manifest entirely**, though two sample rows
   name that type.
2. **`mqttPersistedDrainSeq.RegSeq` was dispositioned as not an identity**, on the
   reasoning that F-034 demotes it to a hint. Wrong: demoting what a mechanism *relies
   on* does not stop the field being a cross-stream reference, and the sample records it
   as one. Both facts now live in the row.
3. **The model could not express `CancelledRegs`**, whose *elements* are
   `$MQTT_inflight` identities — a container disposition alone would have hidden a
   cross-stream reference.
4. **`ConsumerConfig` was deferred**, though three sample rows resolve to
   `ConsumerConfig.Durable`.

Three sample rows are **deferred to a named artifact family** rather than to a body
field, because their carriers are not body fields: the `$MQTT_teardown` epoch (stream
configuration), the affine copy's subject and `Nmqtt-Q2Claim` header, and the stored
PUBREL's correlation headers. A row may be deferred to a family; it may not be silently
absent, and the test enforces that distinction.

## Residuals

- **Three rows claimed `identityCrossStream` and none of them was**, and it takes **two
  different unverified properties** to account for them. All three are corrected to
  `identityLocal`, and every census test passed throughout — before and after.
- **Property one: the HOLDER stream is never compared against the declared issuer.** Two rows
  fail only this. `mqttAcceptFact.SessEpoch` (holder `$MQTT_sess`, issuer `$MQTT_sess`) and
  `mqttQoS2RelClaim.Fence` (holder `$MQTT_qos2in`, issuer `$MQTT_qos2in`) each named their
  issuer **correctly** and were wrong only in calling the reference foreign. `Fence`
  contradicted itself outright: its kind said cross-stream while its own reason said *"a
  sequence in the hold stream's own space"*.
- **Property two: a declared ISSUER is never verified.** Exactly one row fails this, and it
  is the one a holder-versus-issuer comparison would have **accepted**: `mqttQoS2RelClaim.Gen`
  declared issuer `$MQTT_sess` on a holder of `$MQTT_qos2in`, so those differ and the
  comparison passes it as cross-stream. It is wrong anyway, because **no stream issues that
  value at all** — it is the exchange generation, minted in process with `nuid.Next()` at
  PUBLISH time and merely stamped onto the held record.
- **Keeping the two apart is the point.** A guard built for issuer provenance would not have
  caught `SessEpoch` or `Fence`; a guard built for holder-versus-issuer foreignness would
  not have caught `Gen`. Both properties are unverified today. This is the encoding-axis failure
  recorded above (*"an encoding axis nobody verifies is a comment"*) reappearing on two more
  axes — found by F-066's 2026-08-26 attempt rather than by the guard.
- **The F-066 oracle cannot catch that class.** It iterates the fourteen sample rows and compares
  issuers only where a manifest row is **already** declared `identityCrossStream` and the
  sample names the same field. So a false cross-stream declaration on a field **outside**
  the sample is invisible to it, which is exactly what happened to all three rows above.
  The oracle detects a manifest that omits or under-classifies a sampled row; it does not
  detect one that over-classifies an unsampled one.
- **The interpretation-surface guard is not a closure proof**, for the three reasons in
  "The surface, measured". It is the load-bearing limitation, and it is why F-066's
  deferral is not discharged.
- **One artifact family is named and not enumerated field by field**: message headers and
  subjects. Stream configuration and consumer and stream state ARE now enumerated —
  `StreamConfig`, `ConsumerConfig`, `ConsumerInfo`, `SequenceInfo`, `StreamState`,
  `StreamSource`, `SubjectTransformConfig`, `RePublish`, `Placement`,
  `StreamConsumerLimits` and `ExternalStream` all carry field-by-field dispositions, and
  the type universe is DERIVED by a recursive walk from the persisted roots rather than
  hand-listed, so a nested type cannot be missed by forgetting it. Types the walk stops
  at are declared with a reason in `censusOutOfScope`, so an exclusion is visible rather
  than silent.
- **`updateStream` has no production caller**, the code-level form of
  `docs/ARCHITECTURE.md`'s "nothing is repaired or updated in place". Recorded as an
  audited absence rather than removed, so a caller appearing later is noticed.
- **`ConsumerConfig.OptStartSeq` and `OptStartTime` are set by no non-test site.**
  Recorded for the same reason: "nobody sets it" is a claim this census makes, and the
  guard re-checks it.
