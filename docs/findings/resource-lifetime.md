# Memory and resource lifetime: the census and its acceptance soak

- Status: Done 2026-10-23 (`docs/ROADMAP.md` §2, "Performance and resource audits", the scan
  bullet and the churn/soak bullet, taken as one item by Codex consult).
- No finding ID. Defects found by the census are named D1 to D8 below; D1 is a stated
  residual, D2 to D8 are fixed.
- Scope: `internal/mqtt`, `internal/backend`, `cmd/mqtt-nats-broker`, and, since §3 "TLS
  and certificate rollover" landed, `internal/tlsreload`. Its rows are in "TLS material
  reload" below, and its soak is `TestResourceTLSReloadChurn`. Static TLS configuration,
  failed and stalled handshakes are in scope.

This dossier is the evidence index the census left behind. A row says what owns a resource,
what creates it, what ends it on every exit (normal, failure, refusal, takeover, shutdown),
whether shutdown joins it, and the verdict. Rows were produced by six read-only slices
(goroutines, timers, channels and NATS subscriptions, server and account containers, session
and connection state, the process boundary) and read against the code at `9d440ae`. A row
marked **verified** was re-read by the planner or confirmed by a plan-review round; a row
marked **carried** is the slice's reading, checked by the plan-review rounds only where they
cited it. The soak in "Acceptance" is the dynamic half: a census row claims an exit, the soak
fails if the exit does not happen.

## Defects found, and what became of each

| | Defect | Disposition |
|---|---|---|
| D1 | A session object removed from the account maps WITHOUT being cleared (record-watch eviction, foreign disposition, the expiry "superseded" hand-off, re-drive detach, six CONNECT-path refusal removals) keeps its background work (custody dispatcher and tick, floor unit, disposals, the reconcile retries) while that work is unresolved, and stays reachable through it. | **Residual**, decided by Codex consult: some of that work is the displaced holder's alone (an exact-token action drain the successor cannot clear without a close certificate), the floor's reclamation needs no entitlement, and a removal does not prove a successor exists. Suppression machinery would trade a retention bounded by progress for a possible stranded token. `docs/CONFORMANCE.md` states the conditional end; the failover soak fails if an object stays reachable after its work completed and the ledger is quiescent. One case exceeded the residual and is fixed: a displaced disposal whose exchange the successor abandoned under its OWN tenure waited forever for a pair naming the displaced tenure; it now ends once a serving authority names another generation or tenure AND its source is positively settled: the ack floor past the delivery, or the consumer absent and its group incarnation or source resource positively retired (`endIfDisplacedAndSourceSettled`, decided by Codex consult; the settlement keeps a queued terminal retry from acknowledging a shared group's handed-back message, and the exit does not need the exchange record, which the floor may already have reclaimed). |
| D2 | A PUBLISH-action drain retry that finds its generation gone returned without clearing `Server.owedDrains`, pinning the session until shutdown. | Fixed: both stale exits clear the exact token (`schedulePublishDrainReconcile`). |
| D3 | Persistent sessions are never removed at shutdown, so `resident()` stayed true and four untracked retained loops (terminal cleanup, manifest re-read, `runRetainedReclaim`, the abandonment arm of `runRetainedContinuations`) retried every 5 s forever in a process that outlives the server. | Fixed: `resident()` is false once `quitCh` is closed; the abandonment arm exits when the server has stopped (not on residency, which a clean disconnect or an expiry interrupts temporarily); `admitBackgroundAuthority` refuses on a closed `quitCh` even when idle. |
| D4 | `processSub` could install a subscription after `closeConnection` had swept `c.subs`; a `$MQTT.` deliver subject then rode the service connection with no owner for the life of the process. | Fixed: the install checks `closed` under `c.mu` and unsubscribes its own interest. |
| D5 | Emptied `cpending` buckets were never dropped (eight inner-delete sites, several through local aliases, and `trackPublish` creating a bucket before its early returns), so a persistent session that unsubscribed with deliveries in flight kept one empty map per durable for its life. | Fixed: `mqttConsumerPending` (`qos_consumer_pending.go`) owns every mutation and drops a bucket with its last entry; no method hands out a bucket. An AST guard pins that production code reaches `.cpending` only through its methods. The inflight census now sees the type's removals, which exposed two previously invisible edges (rows C37d, C37e). |
| D6 | The accept loop returned for good on any `Accept` error that was not a timeout (EMFILE/ENFILE are temporary, not timeouts), unlogged, leaving the listener open with nobody accepting and the readiness probe green. | Fixed: every error while running is logged and retried with upstream's backoff (10 ms doubling to 1 s); only shutdown ends the loop. |
| D7 | Shutdown left the account manager's subscriptions on the caller's service connection and the identity handlers armed on it; a failed `Start` left the handlers too. | Fixed: shutdown Phase 5 unsubscribes the internal clients' subscriptions and restores the handlers; every `Start` failure exit after arming restores them. |
| D8 | `backend.Close` started an asynchronous drain and returned, so the process exited before buffered publishes were flushed. | Fixed: `Close` waits for the closed callback, bounded at DrainTimeout (10 s) plus flush and callback slack, then forces the close. Asynchronous connection errors are now logged. |

## Classes and rows

### Goroutines (52 spawn sites)

| Resource | Owner | Ends on | Joined by shutdown | Verdict |
|---|---|---|---|---|
| `runShutdown` | server | all phases complete | is the lifecycle | verified |
| accept loop (`acceptConnections`), one per listener: MQTT, the plaintext listener beside TLS (`-plain-port`), WebSocket | server | listener closed during shutdown; errors while running back off | untracked; each exits on its listener's synchronous close in `BeginShutdown` (`TestMQTTPlainListenerClosedByBeginShutdown`) | D6 fixed; verified |
| connection setup (`createMQTTClient`) | connection | setup done; TLS and peek deadlines | `connSetupWG` | carried |
| `readLoop` | connection | read or parse error, then `disconnect` | `clientWG` | carried |
| `writeLoop` | connection | returns at once | `grWG` | carried |
| inbound pipeline (`runHold`, `runRelease`, `issuer`, QoS 1 step) | connection | abort or completion; at most `mqttInboundWindow` | `p.wg` joined by `p.close` from `disconnect` | carried |
| account manager loops (JSA send, retained-cache cleanup, sweeps, retained observer) | account | `closeCh` or `quitCh` | `grWG` | carried; the sweep goroutine's possible hang is the stated residual |
| `willClaimLoop` | account | `closeCh`, `willClaimStopCh`, `quitCh` | `willClaimWG` | carried |
| `runRecoveryKick` | account | queue empty or `quitCh` | `grWG` | carried |
| record-watch workers (expiry reconcile, eviction) | account | one load; coalesced triggers | `willWorkerWG` | carried |
| `go x.closeConnection(...)` (five sites) | connection | bounded, idempotent | untracked child of a joined spawner | carried |
| closure-certificate write | connection | one store attempt | untracked | carried; loss at graceful shutdown stated |
| Will dispatch workers and owed-Will discharge | server | publish and finalize | `willWorkerWG` | carried |
| custody dispatcher | session | dispatch gate, incarnation closing, idle | `willWorkerWG` + lease | carried; on a removed object, D1 |
| pending-discard worker | session | drained, then gate claimed or obligation gone | `willWorkerWG` | carried; see note on `reopen` |
| retained terminal cleanup, manifest re-read, `runRetainedReclaim`, `runRetainedContinuations` | session | work done, or the object stops serving (now including a stopped server) | untracked | D3 fixed; verified |
| `runRetainedDriver`, `discoverRetainedPayloads`, connect-settle abandonment, verdict kick, tenure settlement | session | bounded work; departure; tenure or quit checks | untracked | carried |
| bounded fan-out walks (`qos2_release_claim`, register reap, claim workers) | request | local `WaitGroup` | joined by their caller | carried |
| health HTTP server | process | `httpSrv.Shutdown` after MQTT shutdown | explicit | carried |

### Timers and tickers (38 sites)

| Resource | Ends on | Verdict |
|---|---|---|
| delayed-Will fire timers (`willTimers`) | fire, cancel, scan sweep, `closeWillDispatch` | verified |
| custody tick (`willTimers` key `$MQTT_custody_tick.<id>.<gen>.<arm>`) | generation change, tracker close, shutdown | carried; on a removed object, D1 |
| CONNECT deadline, identity expiry | admission or `closeConnection` | verified |
| flapper sweep (`flapTimer`) | periodic until account teardown | carried |
| session-expiry timers (`pendingExpiries`) | fire, cancel, shutdown; pin the account, not the session | carried |
| reconcile retries (PUBLISH drain, PUBREL action, owed materialization, terminal reservation, rematerialization, PUBCOMP owed, intent resolution, terminal release, drain finalize, disposal, retained terminal cleanup) | resolution, generation change, map membership, shutdown gate | verified for the PUBLISH drain (D2) and the disposal; carried for the rest; on a removed object, D1 |
| exclusion reclaim | non-residency, empty set, shutdown | carried |
| request-scoped `NewTimer`s (JSA waits, ack gate, phase and operation gates, incarnation drain ceilings) | `defer Stop` | carried |
| `time.After` in loops | GC-eligible since Go 1.23 (the module is Go 1.26) | carried |

### Channels (37 sites) and the JSA reply table

All 37 channels are owned and either closed by exactly one party or not needing a close
(buffered for their maximum sends). The JSA reply table (`replies`, four registration
sites) deletes on reply and on every timeout; two quit arms skip the delete, which is bounded
because `quitCh` closes once, at shutdown. Carried.

### NATS subscriptions and connections

| Resource | Created at | Ended at | Verdict |
|---|---|---|---|
| client subscriptions (`c.processSub`) | SUBSCRIBE, restore, PUBREL and affine deliver subjects | `processUnsub`, rollback, `closeConnection` | D4 fixed; verified |
| identity request subscription | `mqttFetchIdentity` | deferred `Unsubscribe` | carried |
| account manager subscriptions (JSA replies, retained observer, session-record watch) | manager init | init failure; shutdown Phase 5 | D7 fixed; verified |
| per-client NATS connection (`DialClient`, `MaxReconnects(-1)`) | `isClientAuthorized` | refusal: `Close`; adopted: `Drain` in `closeConnection` (a reconnecting connection is closed instead) | carried |
| service connection | `backend.Connect` | `backend.Close`, now waiting for the drain | D8 fixed; verified |
| JetStream consumers (per-filter, PUBREL, shared group, per-run ephemerals) | SUBSCRIBE, resume, init | their retirement owners, the orphan sweep, inactivity thresholds | stated residuals: orphan reconciliation retention, shared group release, ephemeral cleanup per run |

### Server- and account-lifetime containers (35)

Bounded by pairing: `Server.clients`, `ipQueues` (one per account), `willTimers`,
`sessLocked`, `pendingExpiries`, `reconcileWM`, the subscribe barrier's `live`, the JSA
`replies`, `sharedVerdictSeen` (rebuilt per pass), the retained index and its placeholders,
`rmsCache` (TTL sweep). Bounded by caps: `forkedGrants` (256), `retirementMemo` (4096),
`sweepQuarantine` (8192), the recovery kick queue, the owed-Will queues (4 MiB each, stated).
Shutdown-only: `owedTerminals`, `owedActions`, `owedRetained`. Bounded by operator
remediation, not stated: `refusedWillRecords`, `refusedAcceptFacts` (one per refused durable
record). `owedDrains`: D2. `owedDisposals`: on a removed object, D1. Package-level maps are
constant schema tables. Carried, except `owedDrains` and `owedDisposals` (verified).

### Session- and connection-lifetime state (60 fields)

Every per-session map is bounded within a connection by the identifier space, Receive
Maximum, the live filters or the consumers' MaxAckPending, and is reset by the session's
reset (`resetForReuseLocked`), its generation change, or its replacement on restore. The
exceptions: `cpending` (D5), and the lifetime of an object removed without a reset (D1).
Per-connection state (`topicAliases`, `outTopicAliases`, the inbound pipeline's window and
ledger, `client.subs`, timers) ends with the connection. Carried, except `cpending`
(verified).

### Objects removed on failed CONNECT and acceptance paths

A category the first census pass missed (plan review round 1): the six `connect_process.go`
removals on fork-repair failure and on ownership or tenure reconciliation failure take a
resumed object out of the maps without a reset, as eviction does. They belong to D1 and
share its residual. Verified by plan review round 2.

### The process boundary

Startup failures end the process, after releasing what was acquired. For an embedded
`Server` (tests, embedders) a failed `Start` now restores the caller's handlers (D7). The
per-connection read buffer and the packet length bound are stated residuals; outbound
writes are synchronous under the write deadline (stated). nats.go buffers each
subscription's pending messages up to its defaults (500k messages, 64 MiB) for a client that
reads just fast enough to beat the write deadline; that bound is now stated in
`docs/CONFORMANCE.md`, and a slow-consumer drop on the service connection is now logged.

### TLS material reload (`internal/tlsreload`, added with §3)

| Resource | Owner | Ends on | Joined by shutdown | Verdict |
|---|---|---|---|---|
| reload watcher goroutine (`Source.watch`) | the command (`run`) | `Stop`, deferred after `BeginShutdown` completes; never started on a failed startup | `Stop` waits on its done channel, including a `Check` in progress | verified; `TestWatcherAppliesAndStopJoins`, soak |
| reload ticker | the watcher | the watcher's exit (deferred `Stop`) | with the watcher | verified |
| file descriptors of a check | `Check` | `os.ReadFile` closes before returning; none held between checks | not applicable | verified; soak descriptor count |
| superseded per-handshake `*tls.Config` and its client-CA pool | the connections admitted under it (their `tls.Conn` keeps it) | the last such connection's teardown; the source keeps only the active material | not applicable | verified; soak weak pointers per configuration and per pool |
| refused content | none | not retained: re-read and re-validated each check; only the last refusal's cause string is kept | not applicable | verified |

## Notes with a disposition each

- `consCharged[pubRelDurable] +=` on every PUBREL consumer re-creation without a refund
  before reset: a budget drift bounded by the refusal at `sub_consumer.go:173`. Recorded;
  it is a capacity budget, and changing it changes admission.
- `refusedCarriers`: its comment said "lives for the connection"; it lives until the
  session's reset. Comment corrected. Unreachable under the compatibility boundary.
- `pubRelRedrive`: not reset; bounded by the identifier space; a stale waiter is woken by
  the next send on its identifier.
- `pendingDiscardFinisher`: its comment claimed the `quitCh` exit keeps it inside its
  `WaitGroup`; during shutdown `quitCh` closes only after Phase 3 waits for that group.
  Comment corrected.
- `startGoRoutine` and the claim loop's `willClaimWG.Add` take no shutdown gate. No path
  reaches them after Phase 4; recorded.
- `reopen()` replaces the tracker's `drained` channel without closing it. No reopen of a
  retirement-pending cycle was found reachable; a worker waiting on it would hang shutdown
  if one were. Recorded with that condition.
- The identity-expiry timer is created before it is stored; it is armed on the read loop
  before teardown can run. Recorded.
- A disposal on a RESIDENT offline session polls every 2 s with point reads until resume.
  Bounded work, not growth. Recorded.
- The `ldm` branch and the `totalClients` counter were dead and are deleted.

## Acceptance

The soak (`make resource-soak`, gated by `MQTTNB_RESOURCE_SOAK=1`, run alone) and the
focused tests, each paired with a mutation that was run and fails it:

| Test | Proves | Mutation |
|---|---|---|
| `TestResourceChurnSingleBroker` | after each block of 100 cycles (clean QoS 0/1/2, persistent resume with the drain route, retained overwrite, v5 expiry, a drop mid QoS 2, a silent connection, failed and stalled TLS handshakes) every container is at its expected value, broker goroutines equal the idle set by root, every removed session object is collected, broker-allocated heap and descriptors show no sustained growth | the D2 to D5 mutations below fail their focused tests; S1 is the integration check |
| `TestResourceChurnFailover` | 20 takeovers between two brokers on a three-node cluster, each displacing an object that holds a pending PUBLISH, a pending PUBREL and its custody tick, a `$MQTT_sess` leader restart, the floor driven past the slack while held: once every exchange completed and the ledger is quiescent, every displaced object is collected and nothing is owed | M9 (a completed displaced session kept reachable from a server map) |
| `TestResourceDisplacedDisposalCollected` | a denied delivery's disposal held on the object a takeover displaces lets go once the successor ended the exchange; `DelayedDisplacedDisposal` holds the displaced step before its first pair read until the successor abandoned the allocation under its own tenure, the client ended the identifier and the floor reclaimed the exchange (a classified no-message read), and `SourceRetiredBeforeRelease` until the source consumer was unsubscribed and positively retired: before the fix the object stayed reachable (the witness), after it both are collected | M9, M14 (the exit disabled) |
| `TestResourceTLSReloadChurn` | 75 measured renewals of the listener pair AND the client CA (after a 25-renewal warm-up), each followed by an mTLS client connecting, publishing at QoS 1 and disconnecting, and by a refused update (mismatched key): every published generation is observed (consecutive, distinct pools), every superseded configuration and client-CA pool is collected, the source runs exactly one goroutine (a positive count through the soak's classifier), broker goroutines return to the idle set, broker-allocated heap (now including `internal/tlsreload` frames) and descriptors show no sustained growth, and `Stop` leaves no watcher | M9 (superseded materials retained), M9b (superseded pools retained), M14 (the classifier blind to `internal/tlsreload`) |
| `TestResourceShutdownComplete` | after churn, a completed shutdown leaves no broker goroutine and no broker subscription on the caller's connection | M7a |
| `TestOwedDrainClearedOnStaleGeneration` | D2, both exits | M2a, M2b |
| `TestRetainedLoopsEndAtShutdown` | D3, each loop observed retrying, then gone after the stop | M3a, M3b, M3a+M3c |
| `TestRetainedAbandonmentSurvivesTemporaryRemoval` | an abandonment met while its object is only temporarily out of the maps is kept | M12 (the arm exits on residency) |
| `TestDisplacedDisposalEndsOnlyOnBothFacts` | the displaced-disposal exit refuses when the tenure still serves, when the source is unsettled, for a recovery-staged owner, without a serving authority, and for an absent but unretired ordinary or shared consumer; it ends once both facts are read, leaving the identifier's entry reserved | M11b (ack floor ignored), M15 (ordinary absence taken as retirement), M16 (shared absence taken as retirement) |
| `TestSubscribeAfterCloseLeavesNoSubscription`, `TestSubscribeRacingCloseLeavesNoSubscription` | D4, sequential and inside the install window | M4 |
| `TestPendingBucketsDoNotAccumulate`, `TestAbandonedDeliveryLeavesNoPendingBucket`, `TestConsumerPendingIsTouchedOnlyThroughItsMethods` | D5 through the drain route, the compare-delete route, and the access guard | M5a, M5a', M5b |
| `TestAcceptLoopSurvivesErrors` | D6 | M6 |
| `TestShutdownReleasesServiceConnection` | D7 after a successful start and after identity, listener and eager-init failures | M7a, M7b, M7c |
| `TestBackendCloseWaitsForDrain` | D8, including the force-close bound | M8 |

The process heap is logged but not asserted: in the calibration run it grew about 23 MB
per block with every count exact, and the heap-profile difference put all of it in the
embedded nats-server's file-store block buffers and subject index, which grow with stored
records and belong to a separate process in production. The asserted heap is the
broker-allocated share, sampled at a 4 KiB profile rate: about 0.3 to 0.5 MB in use after
each block, growing by 68 KB from block 2 to block 4 in the profiled run. A broker-focused
profile difference attributes that to the oracle's own bookkeeping (the weak pointers and
generation strings the removal hook captures) and to the capacity of the custody ledger's
maps, which Go keeps after their entries are deleted and which is bounded by the session's
peak in-flight population (the entry counts were zero at every measurement).
