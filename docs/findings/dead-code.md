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

- Status: Open (bullets 1-4 resolved; bullet 5, the unread option fields, remains)
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

**Bullet 2 resolved 2026-10-18** (the constant-state v5 capability). Collapsed, not
derived: `srvMQTT.v5Enabled` and its startup assignment are deleted, and the three
live functions that read it (`mqttNoLocalStamp`, `mqttStampedOrigin`,
`mqttOriginRetained`) lose their false arm and keep their nil and session guards.
The field was `true` under every configuration, including the `MaxProtocolVersion=4`
cap, so production publication, persistence, stamps, QoS 2 holds and replay are
unchanged. Deriving it from the cap would have added an arm reachable only from tests
and would have changed what a capped server records; the CONNECT gate stays the one
definition of the accepted versions, with `requireMsgTTL` its storage prerequisite.
The comments and docs that described a 3.1.1-only or opt-in-v5 server are corrected,
and the CONNECT refusal no longer tells the operator to set a configuration key this
broker does not have.

**Also found: the `MaxProtocolVersion=4` cap is selected only by tests.** No flag or
environment variable sets `MaxProtocolVersion` (`internal/config/config.go`,
`cmd/mqtt-nats-broker/serve.go`), so production always accepts 5.0. Bullet 4 owns its
disposition: retain it explicitly as test compatibility, expose a supported production
mode, or remove it together with its TTL waiver and the tests and documentation
(`docs/ARCHITECTURE.md` stream table, `docs/CONFORMANCE.md`) that describe a
`max_protocol_version: 4` broker.

**Bullet 3 resolved 2026-10-18** (the parked WebSocket path). Deleted, not kept as a
hook: the placeholder `websocket` type, the `client.ws` field, the `ws` parameter of
`createMQTTClient` (its one caller passed nil), the constant-false `isWebsocket()`,
and the `ws == nil` term of the accept path's TLS decision, which was the only place a
non-nil value would have changed behavior (it skipped the TLS handshake). A future
transport defines its own listener, TLS and client marking. The pre-CONNECT HTTP `GET`
check is not WebSocket code and stays: it names an HTTP client on the MQTT listener
instead of reporting a bad packet type. Its error, renamed
`errMQTTHTTPRequestOnMQTTListener`, no longer directs the operator to a WebSocket port
that does not exist, and `TestMQTTHTTPRequestLineOnMQTTListenerIsNamed` (default build)
asserts it against a non-HTTP control. Test-only references stay for the transport
roadmap: `mqtt_ws_ref_test.go` (`mqttport_wstests`, which already did not compile),
`mqttConnInfo.ws`, and the WebSocket subtest of `mqtt_cluster_ref_test.go`.

**Bullet 4 resolved 2026-10-18** (the QoS and protocol-version compatibility switches).
Removed, not retained or isolated: the bullet's "keep reference-test behavior" was
superseded by decision, because a reference test does not justify carrying a mode no
deployment can select, and moving the selection into test hooks would keep the same arms.
`rejectQoS2Pub` and `downgradeQoS2Sub` are deleted from `MQTTOpts` and the per-client
state with their four arms (the QoS 2 PUBLISH refusal, the QoS 2 Will refusal with its
only CONNACK code `mqttConnAckRCQoS2WillRejected` and that code's v5 mapping
`mqttReasonQoSNotSupported`, the QoS 2 retained skip, the QoS 2 SUBSCRIBE downgrade) and
`TestMQTTQoS2SubDowngrade`, `TestMQTTQoS2PubReject`, `TestMQTTQoS2WillReject` and
`TestMQTTQoS2RetainedReject`; retained persistence across a restart, the last one's
incidental half, is `TestMQTTPersistRetainedMsg`'s. The shared-subscription QoS 1 cap is a
separate policy and stays. `MaxProtocolVersion` is deleted with the CONNECT cap, its
`ValidateOptions` check, `TestMQTTv5MaxProtocolVersionCap` and the `AllowMsgTTL` waiver:
`requireMsgTTL` and its parameter on `validateMsgsStream`, `validateRetainedStream` and
`brokerStreamSpecs` are gone, so both streams require `AllowMsgTTL` on every broker, as
every production broker already did. `TestV4OnlyBrokerAcceptsTTLlessStreams` became
`TestBrokerRefusesTTLlessMessageStreams`, one TTL-less member per subtest so neither
validator's refusal can mask the other's. The protocol-level switch in
`mqttParseConnect` is now the one definition of the accepted versions, and
`TestMQTTParseConnect` gained the adjacent level 3. The cluster reference tests that
set these fields (`mqttport_clustertests`) already did not compile and are unchanged.

**Not covered by any of the four bullets: the unread option fields above.** F-052 left
them to this finding, and a production-only `unused` pass does not report a write-only
field, so F-055 does not catch them. They are bullet 5.

### Commit-sized resolution

Resolve this as five independent commits:

1. Move setter functions and test-only convenience methods into appropriately
   tagged `_test.go` files where the production hook points do not need them.
2. Derive the v5 capability from real configuration or collapse its impossible
   false branch.
3. Isolate the parked WebSocket path without implementing the future transport.
4. Isolate or explicitly retain the upstream-only QoS compatibility switches
   without exposing them as supported runtime options, and decide the disposition of
   the test-only `MaxProtocolVersion=4` cap.
5. Delete the option fields production never reads, including `MaxSubs` and the
   `client.msubs` it is copied into, and adapt `internal/config`, the command and the
   fulltests harness that still write them.

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
