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

F-057 (the reserved-subscription migration) is resolved and its record now lives in
[the resolved-findings archive](resolved.md#f-057).

## F-056

**Title:** Remove inherited retained-message format and delete-protocol compatibility

- Status: Open
- Classification: Pre-release persistence compatibility cleanup
- Release treatment: Immediate cleanup before further MQTT-core implementation

### Evidence

The retained-message path still carries three coordinated compatibility mechanisms:

- `mqttDecodeRetainedMessage` falls back to JSON when the current
  `Nmqtt-RFlags` header is absent. The production encoder always emits the
  header-based format; direct JSON fixtures exist only to exercise the inherited
  legacy reader.
- Empty retained-message tombstones prepend `mqttRetainedFlagDelMarker` so older
  servers fail to decode and ignore them. No older bridge release or supported
  mixed-version fleet exists.
- Every account manager subscribes to the internal `RD` retained-delete
  notification protocol. `processRetainedMsg` publishes that notification and
  `processRetainedMsgDel` consumes it, even though current brokers already observe
  the authoritative retained-stream tombstone. The source comments explicitly
  identify this as backward compatibility intended for later removal.

This is inherited compatibility with older NATS Server MQTT persistence, not MQTT
wire-protocol compatibility. Keeping it expands the accepted storage format and
adds an account-wide subscription, sender, receiver, message type, and tests to
every current broker.

### Commit-sized resolution

Land one retained-message compatibility cleanup commit:

1. Remove the `RD` constant, subscription, message type, sender, receiver, and
   notification call while preserving current tombstone processing and cleanup.
2. Require the current header-based retained record, remove the JSON fallback and
   old-server delete-marker encoding, and keep validation of all current headers.
3. Replace legacy-format fixtures with current-format corruption and restart
   coverage where the underlying validation remains relevant.

Acceptance requires retained publish, replacement, deletion, restart recovery,
expiry, malformed-header, and multi-broker cache-convergence tests. A retained
record without the current required headers must be rejected with a clear log; it
must never be interpreted as another format.

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
