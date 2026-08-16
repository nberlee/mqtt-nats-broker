# Porting the MQTT core off NATS-server internals

The MQTT protocol implementation in `internal/mqtt/mqtt.go` is a port of NATS
server's `server/mqtt.go` (Apache-2.0). Upstream, the MQTT connection *is* a
nats-server internal `*client`, and persistence/routing ride nats-server
internals. This project turns that into a **standalone broker** that talks to an
**external** NATS/JetStream cluster via the public `nats.go` client.

The core now builds and serves in the **default** tree — no build tag. The
upstream regression suite runs against the standalone broker via an embedded
nats-server test harness under the `mqttport_fulltests` tag:

```sh
go build ./...                                  # builds the serving broker
go test  ./...                                  # transport/session/e2e/auth tests
go test  -tags mqttport_fulltests ./internal/mqtt/   # upstream suite
```

> **Design boundary:** this port history does not create an upstream source-
> compatibility requirement. Keeping ported files easy to compare with NATS is
> useful when it does not compromise the bridge design, but divergence is
> allowed. The bridge is also pre-release: internal MQTT persistence and
> `$MQTT_*` stream formats need no legacy readers, migrations, or mixed-version
> support yet. Backend work targets only the latest JetStream behavior exercised
> by the `nats-server/v2` version currently pinned in `go.mod` and CI.

## What's already done

- **Alias layer** (`shim_natsalias.go`): 76 exported NATS-server symbols (wire
  types, enums, JS-API request/response structs, error identifiers) are aliased
  to `github.com/nats-io/nats-server/v2/server`. mqtt.go only touches their
  exported fields, so aliasing is safe and zero-cost.
- **Vendored helpers** (`compat_helpers.go`, `compat_proto.go`): self-contained
  utilities copied verbatim from nats-server (hashing, header parsing, subject
  tokenisation, protobuf varints, byte/string helpers, ack-reply parsing,
  constants). No runtime coupling.
- **Small shims** (`shim_small.go`): `subscription`, `Account`, `ipQueue`,
  `msgHandler`.
- **Engine shims** (`shim_engine.go`): the `client` and `Server` structs,
  `Options`/`MQTTOpts`, client flags, permissions (permissive — NATS enforces via
  auth passthrough), and TLS/listener helpers.

**Milestone: the core compiles, serves, and passes tests in the default build**
(no build tag). No `TODO(port)` markers remain.

The wiring points that turned the compiling shell into a working broker (all
done):

1. ✅ **Done** — `client.processSub`/`processUnsub` attach a `nats.go`
   `Subscribe`/`QueueSubscribe` on the client's routing connection; the handler
   reframes the NATS message (`frameNATSMsg`) and invokes the subscription's
   `icb`. `client.processInboundClientMsg` publishes the caller-framed message
   (headers parsed via `parseFramedHeader`, `ClientInfoHdr` stripped) on the
   routing connection. Covered by `shim_transport_test.go`.
   This is the single seam the `mqttJSA` request/reply cycle and MQTT
   publish/deliver paths both ride, so the JSA transport is effectively wired —
   `sendJSAPIrequests`' send loop already funnels through `processInboundClientMsg`
   and the reply subscription through `processSub`.
2. ✅ **Done** — the account session manager comes up against a live backend:
   `getOrCreateMQTTAccountSessionManager` / `mqttCreateAccountSessionManager`
   create the `$MQTT_sess` / `$MQTT_msgs` / `$MQTT_rmsgs` streams in real
   JetStream through the wired JSA transport. Covered by
   `shim_sessmgr_test.go` (in-process JetStream). The `Server` needs a
   non-empty unique node id (`NodeName()`, used for JSA reply routing);
   the shim lazily generates one if unset, and the bridge bootstrap should set
   it explicitly (e.g. from hostname/config).
3. ✅ **Done** — the connection I/O engine: `client.readLoop` reads the socket
   and feeds `c.mqttParse` (via the buffering `mqttReader`); `queueOutbound`
   writes back under a write mutex; `Server.acceptConnections` runs the accept
   loop; `initClient` assigns the client id and routing connection. `mqtt.NewServer`
   / `Start` / `Addr` / `Shutdown` (`shim_public.go`) construct and run the broker
   from a `*nats.Conn`. Verified end-to-end with a real paho MQTT client
   (CONNECT → SUBSCRIBE → PUBLISH → receive) over TCP + JetStream
   (`shim_e2e_test.go`), and by running the `cmd` binary against NATS.
4. ✅ **Done** — MQTT 5.0 is always enabled (`v5Enabled = true`, experimental
   warning removed, CONNECT version gate defaults to accept 5.0). The `cmd`
   binary serves MQTT in the default build (`serve.go`).

5. ✅ **Done** — per-client auth passthrough: `Server.isClientAuthorized` (the
   existing CONNECT auth gate) dials a per-client `*nats.Conn` from the CONNECT
   username/password via the configured dialer (`Server.SetClientDialer` →
   `backend.DialClient`). NATS accepting the credentials authorizes the MQTT
   client and binds its routing connection so NATS enforces per-client authz;
   rejection returns a "not authorized" CONNACK. The per-client connection is
   drained on disconnect. Verified by `shim_auth_test.go` (valid creds accepted,
   invalid rejected). With no dialer configured, clients fall back to the shared
   service connection (tests).

6. ✅ **Done** — graduated the core out of the `mqttport_wip` build tag: it
   builds, vets, and serves in the default tree. Two `go vet` printf-wrapper
   warnings in `mqtt.go` were fixed; the ported files are excluded from lint to
   stay diff-able upstream.

## Testing strategy

The upstream single-server suite (`mqtt_test.go`, `mqtt_v5_test.go`) **runs**
against the standalone broker under the `mqttport_fulltests` tag, via
`fulltests_harness_test.go`: an embedded nats-server supplies NATS+JetStream
(the upstream suite assumes the MQTT server embeds one — see
`testMQTTRunServer`), and upstream's assertion helpers / `*Server` accessors
are mapped onto the shim.

Three subsets of the upstream suite genuinely cannot run this way and are
parked, unbuilt, as reference material under their own tags:

- `mqttport_clustertests` (`mqtt_cluster_ref_test.go`, `mqtt_v5_cluster_ref_test.go`,
  `mqtt_ex_test_test.go`, `mqtt_ex_bench_test.go`): cluster tests
  (`createJetStreamCluster*`, config-reload, leafnodes) and any test that reaches
  upstream's **unexported** internal JetStream/client APIs (`Account.lookupStream`,
  `Account.addStream`, `stream.lookupConsumer`, the internal leafnode parser) —
  inaccessible from this package regardless of architecture, and any test whose
  defense boundary is `Options.Users[].Permissions`, which the shim does not
  enforce (authorization is delegated to NATS via auth passthrough,
  `Server.isClientAuthorized`).
- `mqttport_wstests` (`mqtt_ws_ref_test.go`): MQTT-over-WebSocket (the
  `websocket` shim is a placeholder).
- `mqttport_tlstests` (`mqtt_tls_ref_test.go`): TLS config tests needing cert
  fixtures (`../test/configs/certs/...`) not present in this repo.

Coverage for those areas instead comes from a **native suite** that drives
real MQTT clients against the standalone broker over TCP + in-process
JetStream, mining the same upstream scenarios:

- `protocol_test.go` (MQTT 3.1.1 via paho.mqtt.golang): QoS1, QoS2, wildcards,
  unsubscribe, fan-out, **retained replay**, **offline persistent-session queue**.
- `will_test.go`: **ungraceful-disconnect Will** (raw socket).
- `ha_test.go`: **cross-instance session takeover** — two broker instances on one
  NATS cluster; a client resumes its session and receives offline-queued messages
  after reconnecting to a *different* instance (the core HA promise).
- `v5_test.go` / `shared_test.go` / `expiry_test.go` (genuine MQTT 5.0 via
  paho.golang): CONNECT, SUBSCRIBE/PUBLISH, user-property round-trip, **shared
  subscriptions** (load-balanced $share groups), **message-expiry TTL**.
- `shim_*_test.go`: transport, session-manager bring-up, auth passthrough
  (including retained delivery over per-client connections), e2e.

One canary layer sits outside all of these, because its subject is the DEPENDENCY
rather than the broker: `test/depproof/` (`make depproof`) runs `package server`
fixtures inside isolated nats-server checkouts. It exercises the dependency
boundary for a bridge-authored conditional write whose base is superseded across a
leader transition. It cannot live in this package — it needs upstream's unexported
cluster helpers and Raft internals, which is the same boundary that parks
`mqttport_clustertests` — and the broker's own suites cannot substitute for this
package-internal canary. F-030 closure does not rest on the canary alone: the pinned
v2.14.5 source and upstream maintainer confirmation complete the contract evidence.
Its counterpart inside this package is the bridge sentinel
`TestMQTTWillOutboxReplicatedTerminalDoneStands`.

The `mqttport_fulltests` suite runs with no skips. Its inherited upstream cases
were adapted as follows:

- **Config-based auth (7 removed)** —
  `Options.Username/Password/Token/Authorization/NoAuthUser` and
  `Users[].Permissions` are inert in the shim; authorization is delegated to
  NATS via auth passthrough (`Server.isClientAuthorized`), and the harness
  wires no per-client dialer. The seven tests of that inapplicable surface were
  removed rather than permanently skipped. Exercising equivalent behavior
  here would mean translating those options into embedded-nats-server accounts
  plus a per-client dialer; the architecturally correct auth path is covered by
  `shim_auth_test.go`.
- **Cross-topic / cross-QoS delivery order (3 rewritten)** — MQTT orders
  delivery only per (Topic, QoS): an "Ordered Topic" server sends PUBLISHes
  "for the same Topic and QoS in the order that they were received"
  [MQTT-4.6.0-9]; it does
  **not** order across different topics or across QoS levels. That
  per-(Topic,QoS) guarantee **is** preserved here — each topic-filter+QoS
  stream rides a single ordered carrier (one core-NATS subscription for QoS0,
  one JetStream push consumer for QoS1/2) on a single dispatch goroutine.
  Two Subscription-Identifier cases now validate both topic/property pairs in
  either arrival order, and `TestMQTTv5OutboundTopicAliasMixedQoS` validates
  that whichever QoS path arrives first binds the alias and the other reuses
  it.
- **Deliberate product decision (1 removed)** — the upstream
  `TestMQTTParseConnect/client ID with invalid character` asserts the removed
  upstream restriction that rejected
  client IDs with non-subject-token characters (e.g. `.`); this fork accepts
  arbitrary-UTF-8 client IDs, which is **required for Eclipse Paho interop
  conformance** and matches mainstream brokers (the ID is hashed, never used
  as a raw subject token). The accept-path is covered by
  `TestClientIDArbitraryChars` (`clientid_test.go`), so the obsolete rejection
  row was removed. (Upstream's
  `TestMQTTv5DisabledByDefault` is not skipped but omitted entirely — v5 is
  always enabled here, so it tests behavior this fork does not have.)
- **Inherited flaky skip (1 re-enabled)** —
  `TestMQTTQoS2InflightMsgsDeliveredAfterUnsubscribe` now runs normally; its
  old 10 ms AckWait made the test race its own initial redelivery timer, so the
  test now uses a 250 ms window and passes repeated live runs.

Notably NOT skipped (they surfaced real bugs, now fixed — see below):
`SetLogger`-based log capture now works (the shim routes slog to the test
logger); the retained-then-subscribe timing races are fixed by seeding the
retained index synchronously at publish; retained deletion keeps the (TTL-less)
tombstone as the subject's last message, so peer consumers observe the deletion
(item 8 below explains why the cross-restart resurrection this was once credited
with cannot have happened by the mechanism it claimed); and the `$MQTT.*` publish
spoof is rejected broker-side.

## Resolved: the disconnect / stored-delivery gap

The two previously-skipped tests now pass. Three distinct bugs in the shim were
fixed (`internal/mqtt/shim_engine.go`):

1. **`closeConnection` leaked NATS subscriptions.** It now unsubscribes each
   `sub.nsub` (plus the per-client `Drain()`), so a disconnected client's
   deliver-subject subscriptions no longer linger on the shared service
   connection and steal the durable consumer's messages from a reconnecting
   client. (Fixed offline persistent-session queueing.)
2. **`readLoop` bypassed `mqttHandleClosedClient` on socket EOF.** It now runs the
   closed-client cleanup before `closeConnection`, so an ungraceful drop publishes
   the Will and detaches the session. (Fixed ungraceful-disconnect Will.)
3. **`parseFramedHeader` canonicalized header keys.** It used stdlib
   `textproto.ReadMIMEHeader`, which rewrote case-sensitive MQTT headers
   (`Nmqtt-RFlags` → `Nmqtt-Rflags`); the case-exact `mqttSliceHeaders` decoder
   then missed them and the retained message failed to decode. It now parses
   header lines manually preserving exact case (as nats.go itself does). Also
   restored the `c.mqttHandlePubRetain()` hook inside `processInboundClientMsg`
   that upstream calls for MQTT clients (client.go:4324), which the shim had
   dropped. (Fixed retained storage + replay.)

## Resolved: bugs found by making the upstream suite run

Getting `mqttport_fulltests` to actually run (rather than sit unbuilt), and
then driving the skip count down, found nine genuine bugs, all in
`internal/mqtt/mqtt.go` unless noted:

1. **No Local was a no-op for QoS0 direct delivery.** `mqttDeliverMsgCbQoS0`
   only read the publisher's identity from `pc.mqtt.sess` when `pc.isMqtt()` —
   true in upstream, where a same-server delivery's `pc` is the live in-process
   publishing `*client`, but never true here: every QoS0 delivery arrives over
   a real NATS subscription through a synthetic per-subscription client (see
   `(*client).processSub`, `shim_engine.go`), so `pc.isMqtt()` never reports
   true for a live message. Fixed by using the same authenticated-marker check
   (`mqttOriginMarker`) QoS1/2 delivery already uses, and by always stamping
   that marker at publish time (previously skipped for QoS0 as an
   optimization that assumed the now-nonexistent in-process fast path).
2. **Re-SUBSCRIBE to an already-subscribed topic leaked the old NATS
   subscription.** `(*client).processSub` (`shim_engine.go`) always created a
   new `nats.Subscribe`, overwriting the map entry without unsubscribing the
   old one — the stale subscription (with stale MQTT metadata baked into its
   `*subscription`) kept delivering alongside the new one. Fixed by
   unsubscribing any existing subscription for the same `sid` first.
3. **A malformed SUBSCRIBE filter reached NATS instead of failing cleanly.**
   The multi-level wildcard (`#`) position was never validated at parse time;
   a filter like `foo/#/bar` converted to the NATS-invalid subject `foo.>.bar`
   and was handed to `nats.Subscribe` — which accepts it locally (nats.go's
   client-side check is looser than upstream's), disrupting the shared
   connection's other in-flight requests rather than failing that one filter.
   Fixed by validating `#` is last per Spec [MQTT-4.7.1-3] before conversion.
4. **Retained-message payloads were truncated by 2 bytes.**
   `processRetainedMsg` stripped a trailing `\r\n` that upstream's raw
   wire-protocol messages carry but this shim's `frameNATSMsg`-built messages
   never do (nats.go's `m.Data` is already the clean payload) — the last 2
   bytes of every retained message were silently dropped.
5. **`s.clients` only ever grew.** Nothing removed a client from the server's
   client map on disconnect (`(*client).closeConnection`, `shim_engine.go`),
   so the connected-client count was meaningless for any test or monitoring
   that reads it.
6. **A cold restart could see a stale (pre-delete) retained message.**
   `loadRetainedMessages`'s direct-from-stream fallback didn't recognize a
   zero-length body (the delete tombstone convention, see
   `mqttHandlePubRetain`) as "no retained message," delivering an empty
   PUBLISH instead of nothing.
7. **A retained message published shortly before a matching SUBSCRIBE could be
   missed entirely** (an exact-topic, non-wildcard filter only). Which
   subjects have a retained message is tracked in an in-memory index
   (`mqttAccountSessionManager.retmsgs`) populated asynchronously by a
   subscription reacting to the JetStream store (`processRetainedMsg`); a
   SUBSCRIBE could race ahead of that index, and `loadRetainedMessages`'
   authoritative stream fallback only ran for subjects the index already
   knew about, so a just-published message the index hadn't caught up on was
   never queried for at all. A literal (non-wildcard) filter names one exact
   subject, so it's now always queried against the stream regardless of the
   index. Additionally, the publishing node now seeds its own index/cache
   **synchronously** at store time (`mqttHandlePubRetain` uses the `storeMsg`
   sequence), so retained-then-subscribe is deterministic on that node for
   wildcard filters too. (`processSubs` + `mqttHandlePubRetain`, mqtt.go.)
8. **The retained-deletion item is preserved as a correction, not as an
   explanation.** This entry used to describe a deleted retained message
   resurfacing after a cold restart, and attributed it to a mechanism that is
   false against the pinned backend: it claimed that storing an empty body does
   NOT evict the prior message the way a non-empty replacement does.
   `MaxMsgsPer=1` enforcement runs on every store with no empty-body exception
   (nats-server v2.14.4 `filestore.go storeRawMsg`), so the predecessor is
   already gone once the tombstone is stored. The stated resurrection path —
   "deleting the tombstone left the prior message as the subject's last record"
   — therefore cannot have happened as described: with the predecessor already
   evicted, deleting the tombstone leaves the subject with NO record, which is
   read as deleted just the same. What the original diagnosis really was cannot
   be reconstructed from what it wrote down, so no replacement story is invented
   here.
   What is true of the code now: an empty-body publish stores a tombstone that
   evicts its predecessor at store time; the tombstone carries no TTL and stays
   the subject's last record; last-per-subject reads interpret that empty body
   as "deleted"; and no explicit delete is issued at all — the compaction delete
   written on the strength of the false premise was removed by
   [F-056](findings/resolved.md#f-056), which also demonstrated that keeping it
   destroys a newer record when a tombstone callback is delayed past a newer
   publish (`TestRetainedDelayedTombstoneKeepsNewerRecord`).
   Retaining the tombstone rather than deleting it is still worth doing, for a
   reason about DELIVERY rather than about storage: the tombstone is the record
   whose delivery makes every other broker's consumer drop its index entry, and
   a deleted message is never delivered. Removing it can therefore strand a
   peer's index entry until that peer restarts.
   `TestRetainedCrossBrokerConvergence` asserts a deleted subject holds exactly
   one stream record, which is what pins the eviction premise.
9. **An MQTT client could publish into the broker's internal `$MQTT.*`
   namespace.** A client mapping a `$MQTT/JSA/...` topic onto `$MQTT.JSA.*`
   could forge session-persist / JSA traffic and evict or corrupt other
   sessions. `mqttProcessPub` now rejects any client PUBLISH whose mapped
   subject falls under the internal `$MQTT.` prefix (the broker's own internal
   publishes never take this path), independent of NATS permissions — closing
   a real gap that the shipped example `test/conformance/nats.conf` (which
   gave the `mqtt` user `publish: ">"`) did not. Defense in depth: that config
   now also denies `$MQTT.>` publish for the client user.
10. **`mqttSliceHeaders` silently dropped every header from a given point.**
    The delimiter search returns an offset into the REMAINING slice, but the
    zero-length-key guard compared it against the absolute scan position
    (`del == i`). Whenever a key's length happened to equal that position the
    scan stopped, discarding that header and all after it. The first header
    starts at `len(hdrLine) == 10`, so any ten-character key triggered it —
    `Nmqtt-RExp` is exactly ten, and NATS emits headers in map order, so this
    hit roughly one retained message in seven that carried an MQTT 5.0 Message
    Expiry, dropping `Nmqtt-RFlags` with the rest and making the record
    undecodable. Inherited from the upstream parser. The guard is now
    `del == 0`, which is what a zero-length key actually is (and which the
    empty-key check below it already covered). Found because
    [F-056](findings/resolved.md#f-056) made an undecodable record evict its
    index entry: before that, the publishing broker's local seed masked it, so
    only brokers that had no seed — every REMOTE broker — silently failed to
    index the affected retained messages. Pinned by the "key length equals the
    scan position" row in `TestMQTTSliceHeadersAndDecodeRetainedMessage`.

The log-capture tests (`TestMQTTStart`, `TestMQTTConnectNotFirstPacket`,
`TestMQTTClientIDInLogStatements`) now pass rather than skip: the harness
`SetLogger` installs an `slog.Handler` that routes the shim's slog output to
the upstream-style capture logger, and three small production log
improvements make the observed lines match nats-server — client log methods
now carry the client label (client id), readLoop logs a parse failure at
Error (a protocol violation should be visible), and `closeConnection` logs
"Client connection closed".

Also fixed as test-harness bugs (not production code) while chasing the
above: the ported suite's `s.Shutdown(); o.StoreDir = <same dir>; s =
testMQTTRunServer(t, o)` restart idiom started a second embedded nats-server
against the same on-disk store while the first was still running (`Shutdown`
only stops the shim's MQTT listener, not the paired embedded nats-server) —
`testMQTTRunServer` now reuses the still-live instance for a StoreDir it
already has one for, matching production restart semantics where the bridge
restarts but JetStream doesn't. And `Server.log` is now an
`atomic.Pointer[slog.Logger]` so `SetLogger` on an already-running server is
race-free.

## Remaining

- **MQTT-over-WebSocket** transport (the `websocket` shim is a placeholder;
  `TestMQTTWebsocket*` are parked under `mqttport_wstests`).
- **TLS config tests**: `TestMQTTTLS*` are parked under `mqttport_tlstests`,
  needing cert fixtures (`../test/configs/certs/...`) not present in this repo.
- **Config-based auth in the fulltests harness**: equivalent coverage for the
  removed upstream config-surface tests would require translating
  `Options.Users`/`Permissions` into embedded-nats-server accounts plus a
  per-client dialer. The production NATS auth-passthrough path is covered
  separately.

Conformance against `vibesrc/mqttconformance` (124/124 v3.1.1, 106/106 v5) is
wired into CI (`test/conformance/run.sh`, ratcheting baseline). See
docs/CONFORMANCE.md.

## The behavioral shim surface (reference)

mqtt.go references a small set of **behavioral** NATS-server types by their
*unexported* methods/fields, so they can't be aliased — they must be
re-implemented against `nats.go`. The current porting-significant surface is
below; the compiler remains authoritative for the exhaustive dependency set.

### `client` — `shim_engine.go`

Fields: `mu`, `srv`, `acc`, `nc`, `rawConn`, `cid`, `kind`, `ncs`, `opts`,
`flags`, `in`, `out`, `pa`, `perms`, `mperms`, `subs`, `mpay`, `msubs`,
`trace`, `headers`, `start`, `last`, `ws`, `mqtt`, `nrc`, `ownNRC`, `wmu`,
`connectTimer`, `connectAdmission`, `testConnectDeadlinePause`, `closed`,
`finishOnce`, `connAckWritten`.
Porting-facing methods: `msgParts`, `processSub`, `processUnsub`,
`processInboundClientMsg`, `queueOutbound`, `enqueueProto`, `enqueueProtoErr`,
`flushClients`, `closeConnection`, `registerWithAccount`, `readLoop`,
`writeLoop`, `addToPCD`, `selectMappedSubject`, `pubAllowedFullCheck`,
`checkDenySub`, `maxPayloadViolation`, `doTLSServerHandshake`, `writeSocket`,
`writeDirect`, `shutdownDisconnect`, and
`Errorf`/`Warnf`/`Debugf`/`Tracef`/`traceInOp`/`traceOutOp`.

This is the broker engine: replace the NATS-server socket read/write loop with a
per-connection handler (`internal/conn`), route `processSub`/`processInboundClientMsg`
through a per-client `*nats.Conn` (see `backend.DialClient`), and back
`queueOutbound`/`flushClients` with a buffered writer to the MQTT socket.

### `Server` — `shim_engine.go` and `shim_public.go`

Fields: `mu`, `opts`, `info`, `log`, `nrc`, `dialClient`, `mqtt`, `clients`,
`ipQueues`, `totalClients`, `quitCh`, `grWG`, `clientWG`, `connSetupWG`,
`connGateClosed`, `shutdownOnce`, `shutdownDone`, `willDispatchMu`,
`willDispatchClosed`, `willTimers`, `willClaimStopCh`, `willTimerWG`,
`willClaimWG`, `willWorkerWG`, `ldm`, `nodeName`, `running`, `gacc`, `gcid`.
Porting-facing methods/accessors: `getOpts`, `globalAccount`,
`getOrCreateMQTTAccountSessionManager`, `mqttDetermineReplicas`,
`createMQTTClient`, `createInternalAccountClient`, `startGoRoutine`,
`isRunning`/`isShuttingDown`, `isClientAuthorized`, `JetStreamIsClustered`,
`NodeName`, and `Noticef`/`Warnf`/`Debugf`/`Errorf`/`Fatalf`.

Back these with the `backend.Backend` (service `*nats.Conn` + JetStream) and the
config; `JetStreamIsClustered` etc. come from `js.AccountInfo`/server info.

### `Account` (2 members) — `GetName`, `hasMappings`

Trivial: a name plus a subject-mapping predicate (v1: no mappings).

### `subscription` (5 members) — `shim_sub.go`

Fields: `client`, `mqtt`, `shadow`, `sid`, `subject`. A thin adapter that wraps a
`*nats.Subscription` and carries the MQTT delivery metadata the callbacks expect
(`mqttDeliverMsgCbQoS0/12`, `mqttDeliverPubRelCb`).

### Small pieces

- `ipQueue`/`newIPQueue[T]` — a generic push/pop queue with a drain channel
  (used by `mqttJSA.sendq` and delivery). Straightforward re-implementation.
- `msgHandler`, `ackReplyInfo` — callback signature + JS ack-reply parse struct.
- Flag consts `connectReceived`, `expectConnect`, `hasMappings` + a `clientFlag`
  bitset type.
- `Options`/`MQTTOpts` — replace with `internal/config.Config` field access
  (a thin adapter, or edit the ~5 read sites).
- `natsListen`, `tlsMixConn`, `tlsHandshakeMQTT`, `tlsVersion` — listener/TLS
  helpers (use `crypto/tls` + `internal/conn`).
- `validateNoAuthUser`, `validatePinnedCerts` — option validators (map to
  `config.Validate`).

## Persistence & routing seams

- **`mqttJSA`** (mqtt.go) already speaks JetStream purely over NATS request/reply
  subjects. Re-point its transport (`sendMsg`/`newRequest`/`newRequestEx`) from
  the internal `sendq`+client to `backend.Conn().Request`/`Publish`. The JS-API
  structs are already aliased.
- **QoS0** delivery uses `client.processSub` → replace with `nc.QueueSubscribe`.
- **QoS1/2** use JetStream durable consumers → drive via `backend.JetStream()`.

## Simplifications baked into this port

- **MQTT 5 always-on**: delete `v5Enabled` and the "experimental" warning; the
  CONNECT version gate accepts 3, 4 and 5 unconditionally.
- **Single account (v1)**: session/retained/QoS streams are account-scoped, so
  all MQTT users share the service connection's NATS account. Auth passthrough
  still enforces per-client publish/subscribe permissions.
- **WebSocket**: deferred (see README roadmap).
