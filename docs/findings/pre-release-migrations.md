# Pre-release persistence compatibility findings

This dossier records internal persistence paths that preserve development-era
formats even though the bridge has no released persisted-state contract. The
governing compatibility boundary is in `CONTRIBUTING.md` and
`docs/ARCHITECTURE.md`: before the first release, prefer one current schema and
actionable recreate guidance over migrations, dual readers, or mixed-version
protocols.

This audit does **not** apply to the roadmap's operator migration guide from NATS
Server's embedded MQTT listener. That guide describes cutover from a different
product and remains first-release documentation.

[F-056](resolved.md#f-056) (the retained-message format and delete protocol) and
[F-057](resolved.md#f-057) (the reserved-subscription migration) are resolved and
their records now live in the resolved-findings archive.

## F-058

**Title:** Reject generationless delayed-Will outbox records

- Status: Open
- Classification: Pre-release outbox-format compatibility cleanup
- Release treatment: Immediate cleanup before further MQTT-core implementation

### Evidence

Every current session has a validated NUID incarnation generation, and session
records without one already fail restoration with recreate guidance. Every current
delayed-Will enqueue therefore writes `mqttWillOutbox.Gen`.

The outbox still documents and accepts an empty generation as a legacy bucket.
Scoped displaced-connection cancellation may match that empty bucket, while scan
and load paths do not validate the field before a record can be cancelled,
claimed, or published. This is inconsistent with the same record's
`OriginSessionSeq` and `OriginSessEpoch`, which explicitly reject legacy or
migrated forms.

The empty-generation allowance is small, but it silently preserves a
development-era outbox format and weakens the invariant used to scope delayed-Will
cancellation to one session incarnation.

### Commit-sized resolution

Land one delayed-Will format-hardening commit:

1. Centralize validation of every decoded outbox record, including a valid
   session generation, origin sequence, epoch, identity fields, and state.
2. Make scan, point-load, and cancellation paths treat a generationless or
   malformed record as unsupported/corrupt and never publish or mutate it as if it
   were current state.
3. Remove empty-generation legacy wording and tests; add fail-closed coverage for
   pending and publishing records plus actionable diagnostics.

The resolution must not change unscoped reconnect cancellation of valid records or
the generation-and-origin-sequence scope used by displaced-connection cleanup.
