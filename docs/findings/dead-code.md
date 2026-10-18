# Dead-code and reachability findings

These findings came from a production and tagged-test reachability audit of
`internal/mqtt`, with emphasis on `mqtt.go`. The audit used the `unused` analyzer
twice—once without tests and once with `mqttport_fulltests`—then traced every
reported declaration and reviewed constant-state branches manually. Staticcheck
found no compiler-provable unreachable statement in the recent session state
machines.

The distinction between dead and production-inert matters here:

- **Dead** means no production or supported tagged-test path references the
  declaration.
- **Production-inert** means production cannot select the path, but a tagged test
  or explicitly parked future feature still can.
- A defensive error branch guarded by a documented invariant is not dead merely
  because tests cannot schedule it.

## Audit boundary

The reproducible analyzer passes were:

```sh
golangci-lint run --no-config --enable-only=unused --tests=false \
  ./internal/mqtt/...
golangci-lint run --no-config --enable-only=unused --tests \
  --build-tags mqttport_fulltests ./internal/mqtt/...
golangci-lint run --no-config --enable-only=staticcheck --tests=false \
  ./internal/mqtt/...
```

The production-only pass exposes both dead declarations and APIs kept solely for
tests. The tagged-test pass removes the latter and leaves the declarations that
neither build reaches. The staticcheck pass found no unreachable recent session,
settlement, in-flight-register, or classified-eviction branch. Every non-test
function added since the stream-prerequisite audit has a production caller; the
confirmed dead Will wrapper predates that run.

## F-054

**Title:** Test and future-feature scaffolding remains in production source

- Status: Open
- Classification: Production-inert compatibility scaffolding
- Release treatment: Maintainability cleanup; WebSocket behavior remains governed
  by the transport roadmap

### Evidence

Production-only analysis reports test-hook setters, `updateStream`, and several
validation helpers that become reachable only when tagged tests are compiled.
Some of that is intentional: the backing hook checks must exist in the code under
test. The setter APIs and test-only convenience methods do not necessarily need to
ship in production source.

Three constant-state families also deserve explicit treatment:

- `srvMQTT.v5Enabled` is assigned `true` at startup and is never assigned false,
  yet live publish paths retain false branches and comments describing a
  3.1.1-only server.
- The only production caller of `createMQTTClient` passes a nil WebSocket marker;
  non-nil behavior is parked for the later WebSocket milestone.
- `rejectQoS2Pub` and `downgradeQoS2Sub` are unexported options selected only by
  in-package upstream-reference tests.

These paths are not safe to delete as one batch: some are valuable fault-injection
seams, and WebSocket is a planned feature rather than abandoned behavior.

**Option fields production never reads** (inventoried by F-052's resolution, which
deleted the validator checks about them but left the fields to this item). In
`MQTTOpts`: `Username`, `Password`, `Token`, `NoAuthUser`, `ConsumerReplicas`,
`AllowNonTLS`. In `Options`: `Host`, `Port`, `ServerName`, `PingInterval`,
`Username`, `Password`, `Token`, `Authorization`, `NoAuthUser`, `Users`, `Nkeys`,
`TLSConfig`, `TLSPinnedCerts`, `JetStream`, `Cluster`, `Gateway` and `LeafNode`,
besides the harness-only `StoreDir`, `NoLog`, `Debug` and `Trace`. `MaxSubs` is
copied into `client.msubs`, which nothing reads. Not on the list, though they look
like embedded-server relics: `Options.Routes` and `Options.StreamReplicas`, live
through `JetStreamIsClustered` and the replica derivation in `sess_manager.go`;
`Options.AllowNonTLS`, `MaxConn`, `MaxPayload`, `AuthTimeout` and `MaxTracedMsgLen`
are read too. `MQTTOpts.TLSPinnedCerts` is read only to be refused at `Start`.

**Bullet 1 resolved 2026-10-18** (test-only helper APIs). An uncapped `unused` pass
over every buildable configuration (production only; default with tests;
`mqttport_fulltests`; with `mqttport_experimental_shared_qos2`) found 75 hook setters
and 70 other declarations in production files that only tests reached, and 15 that
nothing reached. Moved verbatim into `testhooks_setters_test.go`,
`testonly_helpers_test.go` and their `mqttport_fulltests` twins (placement follows
the build that uses them, so `make lint` stays clean): the setters, test vocabulary
and oracles, and test-only members of live types - 115 declarations, proven by a
byte-identity check with six enumerated doc-comment corrections. The hook variables
and their production reads stay. Deleted: the 15 dead declarations; the obsolete
per-connection acknowledgement-binding model (`outbound_ack_binding.go`, replaced by
`mqttAllocationAckBinding`) with the custody release predicates that read it and the
tests whose only subject was that model; `batchIdOf`/`batchSizeOf` (replaced by the
explicit missing-`PubAck` refusals); and `recordTx.nested` with the `owner` field,
because production never minted a non-owner token (id and epoch refuse a stale one).
Kept in production: the four named zero values of live enums
(`mqttMarkerDispositionUnset`, `mqttDutyWorkNone`, `mqttTeardownOwnerUnknown`,
`mqttOriginClient`) and the ten superseded mechanisms below, which are F-055's.

**Superseded mechanisms only tests still use** (owned by F-055; each with the live
path that replaced it): `recomputeDrainRegisterStateLocked`
(`computeDrainRegisterStateLocked` + `applyDrainRegisterStateLocked` in
recovery), `closeGenerationEnrolled` (`closeTeardownGeneration` and the accept
path's ownership reconciliation), `mqttAdmitExchangeTransition`
(`mqttAdmitPublishTransition`), `opAdvanceExchange` (`opAdvanceExchangeAck`,
`opCompleteExchangeDrain`; a fixture for 16 tagged test files), `opCompleteCleanup`
(`completeCleanupAndResolve`), `mqttSourceNeedsManifest` (the manifest
arbitration and admission checks), `mqttResourceEdgeDelta` (`mqttResourceEdgeChanges`
with `mqttCheckResourceStep`), `saveAcceptWithDetail` (`saveAcceptWithOwnership`),
`willDue` (the delayed-Will outbox), `noteOwedTerminal` (`recordOwedTerminal(...,
false)` from the terminal worker). `unused` cannot see two more test-only arms
inside live functions: `persistRecord`'s accept-without-ownership arm and
`recordOwedTerminal(..., replace=true)`. Deleting `opAdvanceExchange` also needs a
decision on the exchange-only advance the write boundary still admits
(`checkAckPhaseAdvanceMembers` checks slot members only when present).

### Commit-sized resolution

Resolve this as four independent commits:

1. Move setter functions and test-only convenience methods into appropriately
   tagged `_test.go` files where the production hook points do not need them.
2. Derive the v5 capability from real configuration or collapse its impossible
   false branch.
3. Isolate the parked WebSocket path without implementing the future transport.
4. Isolate or explicitly retain the upstream-only QoS compatibility switches
   without exposing them as supported runtime options.

## F-055

**Title:** Lint configuration suppresses reachability diagnostics for all of
`mqtt.go` and the files split from it

- Status: Open
- Classification: Tooling blind spot
- Release treatment: Maintainability gate after F-052 through F-054

### Evidence

`.golangci.yml` excludes `unused` and every staticcheck diagnostic for the entire
ported `mqtt.go` file and, since the §2 split, for each file split from it (the rule
names them exactly). That policy hides both inherited leftovers and any newly
orphaned function added by current development. Running the excluded analyzers
manually found the declarations recorded above. Staticcheck itself found no
unreachable recent state-machine branch; its remaining reports were narrow style
or simplification diagnostics that can receive equally narrow exclusions.

### Commit-sized resolution

After the code and test scaffolding are separated, remove the file-wide `unused`
and staticcheck exclusions. Add a lint invocation that analyzes the default build
and the `mqttport_fulltests` build, using targeted rule or symbol exclusions only
where compatibility code is deliberately retained.

**Acceptance condition (added by F-054 bullet 1).** Those lint runs compile the tests,
so they cannot see a production mechanism that only its own tests still use. F-055
closes only when each of the ten superseded mechanisms listed under F-054 is deleted
with its fixtures rewritten to reach the live path, and the two test-only arms named
there are removed, checked by a production-only `unused` audit (`--tests=false`)
that reports nothing but deliberately excepted declarations.
