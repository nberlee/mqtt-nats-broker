# Contributing

Thanks for helping build `mqtt-nats-broker`.

## Dev setup

- Go **1.26+**.
- A local NATS with JetStream for integration work:

  ```sh
  docker compose -f test/conformance/docker-compose.yml up nats
  ```

  Unit tests spin up an in-process JetStream server, so Docker is not required
  for `go test`.

## Build, test, lint

```sh
make test        # unit tests (default tree)
make vet lint    # static analysis: go vet, golangci-lint, markdownlint
make build       # host binary
```

`make lint` covers Markdown as well as Go (`make lint-go` / `make lint-md` run
them separately). `make lint-go` runs golangci-lint four times with one
configuration: production only (`--tests=false`, the only pass that reports a
declaration nothing but tests still reaches), the default tests, and both tagged
builds (`mqtt_fulltests`, and with `mqtt_experimental_shared_qos2`); each
must be clean. Markdown rules live in `.markdownlint.jsonc`; vendored
third-party docs are excluded via `.markdownlintignore`, since their style is
upstream's decision. CI runs the same linter and configuration: it installs the
pinned golangci-lint and runs `make lint-go`.

The documents `docs/WIKI.md` lists are also published as the GitHub Wiki, and
`make test` renders them: a broken link or anchor fails it, and so does Markdown outside
the subset the wiki publishes (no raw HTML, reference-style links or footnotes;
headings of plain text and code spans). `make wiki` renders a local preview. The rules
are in `docs/ARCHITECTURE.md`, "The wiki is generated from docs/".

## Code layout

```text
cmd/mqtt-nats-broker   entrypoint: config, backend connect, health, shutdown
internal/config        flags/env configuration
internal/backend       NATS/JetStream service conn + per-client auth-passthrough dial
internal/conn          (planned) TCP/TLS accept + per-connection read/write loop
internal/mqtt          MQTT protocol core (derived from NATS-server; see below)
test/conformance       compose env + conformance runner + baselines
```

## Planning and compatibility assumptions

This project is working toward its **first public release**. Use these defaults
in design documents and implementation plans unless a task explicitly changes
them:

- There is no released broker version, persisted MQTT-state format, or
  mixed-version broker deployment to support yet.
- Do not add migration code, dual readers/writers, legacy fallbacks, or rolling-
  upgrade protocols for pre-release contents of the internal `$MQTT_*`
  JetStream streams. Prefer the clean current schema and fail fast with an
  actionable instruction to recreate development data when necessary.
- Support only the current JetStream behavior and APIs exercised by the latest
  `nats-server/v2` version pinned in `go.mod` and CI. Do not add compatibility
  branches for older JetStream releases.
- These rules concern broker-internal persistence and backend compatibility.
  They do **not** relax MQTT 3.1.1/5.0 wire-protocol conformance.

Call out any proposed exception explicitly, including who needs compatibility,
which released artifact or persisted format establishes the contract, and how
the compatibility path will be tested.

## The MQTT core and its upstream origin

`internal/mqtt` derives from NATS-server's MQTT implementation (`server/mqtt.go`, as
extended for MQTT 5.0); [docs/PROVENANCE.md](docs/PROVENANCE.md) records the source
revision, the license and the intentional divergence. It builds and serves in the
default tree; the behavioral shims that replace NATS-server's runtime live in
`shim_*.go` and the vendored helpers in `compat_*.go` ("The shim surface" in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)).

- The core is `mqtt.go` plus the files split from it along this broker's
  responsibilities (protocol codec and properties, connection lifecycle,
  sessions, QoS tracking, publish, subscribe, retained messages, the JetStream
  request layer); `internal/mqtt/doc.go` lists them. The layout follows this
  broker, not upstream: maintainability takes precedence over a line-by-line
  upstream diff. Each file keeps the NATS Authors' Apache-2.0 header.
- No file is excluded from lint. A deliberately retained declaration that only
  tests reach gets a `//nolint:unused` directive stating its invariant, on its own
  line, never a path rule.

### Test suites and build tags

| Build tag | What it selects | How it runs |
|---|---|---|
| none | the default tree: transport, session, end-to-end, auth and unit tests | `make test`, CI `build-test` |
| `mqtt_fulltests` | the upstream regression suite (`mqtt_test.go`, `mqtt_v5_test.go`) and the broker's own tests that need its harness | `make full-suite`, CI `fulltests` |
| `mqtt_fulltests mqtt_experimental_shared_qos2` | the parked shared-subscription QoS 2 suite, in place of its disabled twins | `make experimental-shared-qos2-tests`, CI non-gating |
| `mqtt_cluster_reference` | upstream cluster, leafnode and internal-API tests, kept unbuilt as reference | never built |
| `mqtt_tls_reference` | upstream TLS configuration tests, kept unbuilt as reference | never built |

The upstream suite assumes the MQTT server embeds a nats-server (see
`testMQTTRunServer`). `fulltests_harness_test.go` supplies that: an embedded
nats-server provides NATS and JetStream, and upstream's assertion helpers and
`*Server` accessors are mapped onto the shim. The suite runs with no skips. Its
inherited cases were adapted where this broker deliberately differs; the
adaptations and their reasons are in [docs/PROVENANCE.md](docs/PROVENANCE.md).

```sh
make full-suite   # go test -tags mqtt_fulltests ./internal/mqtt/
```

The two reference tags park what cannot run against a standalone broker:

- `mqtt_cluster_reference` (`mqtt_cluster_ref_test.go`, `mqtt_v5_cluster_ref_test.go`,
  `mqtt_ex_test_test.go`, `mqtt_ex_bench_test.go`): tests that need upstream's
  clustered test harness (`createJetStreamCluster*`), config reload, leafnodes, or
  upstream's **unexported** JetStream and client APIs (`Account.lookupStream`,
  `Account.addStream`, `stream.lookupConsumer`, the internal leafnode parser), which
  are inaccessible from this package whatever the architecture; and tests whose
  defense boundary is `Options.Users[].Permissions`, which the shim does not enforce
  because authorization is delegated to NATS through auth passthrough
  (`docs/AUTHENTICATION.md`).
- `mqtt_tls_reference` (`mqtt_tls_ref_test.go`): TLS configuration tests needing
  upstream's cert fixtures (`../test/configs/certs/...`), which are not in this
  repository. TLS is covered by the native suite instead.

Coverage for the parked areas comes from tests that drive real MQTT clients against
the standalone broker over TCP and in-process JetStream, mining the same upstream
scenarios: `protocol_test.go` (MQTT 3.1.1 via paho.mqtt.golang: QoS 1 and 2,
wildcards, unsubscribe, fan-out, retained replay, the offline persistent-session
queue), `will_test.go` (ungraceful-disconnect Will), `ha_test.go` (cross-instance
session takeover), `v5_test.go`, `shared_test.go` and `expiry_test.go` (MQTT 5.0 via
paho.golang), and `shim_*_test.go` (transport, session-manager bring-up, auth
passthrough, end to end). Config-based authentication has no harness equivalent:
it would need `Options.Users` translated into embedded-server accounts plus a
per-client dialer, and the production auth-passthrough path is covered by
`shim_auth_test.go` instead.

The delayed-Will outbox, takeover arbitration and shared groups also have **real
replicated-JetStream** tests behind `mqtt_fulltests`. They launch **three embedded
nats-server processes** joined into one cluster so the streams are genuinely `R3`.
The cheap R3 smoke test (`TestMQTTWillOutboxReplicatedStream`) runs in the normal
`make full-suite`; the slow failure-injection tests self-skip unless
`MQTTNB_CLUSTER_FAILOVER_TESTS=1`:

```sh
make cluster-failover-tests   # MQTTNB_CLUSTER_FAILOVER_TESTS=1 go test -tags mqtt_fulltests -race -run '^TestMQTTWillOutboxReplicated' ./internal/mqtt/
```

One canary sits outside all of these, because its subject is the dependency rather
than the broker: `test/depproof/` (`make depproof`) runs `package server` fixtures
inside isolated nats-server checkouts, which needs upstream's unexported cluster
helpers and Raft internals, the same boundary that parks `mqtt_cluster_reference`.
Its broker-side counterparts are the broker sentinels it runs with
`./test/depproof/run.sh --broker`; see `test/depproof/README.md`.

### Syncing with upstream NATS

The core derives from nats-server but no longer mirrors its layout. When NATS
patches its MQTT behavior, find the changed functions in
`github.com/nats-io/nats-server/v2/server/mqtt.go` and forward-port them by symbol
into the file that now declares each one (`grep -n 'func.*<name>' internal/mqtt/*.go`).
Record the upstream commit you synced to in the PR description.

## Conformance

CI gates on three suites via `test/conformance/run.sh`
(`vibesrc/mqttconformance`, `nats-io/mqtt-test`, Eclipse Paho interop). Each suite
has a **known-failures baseline** under `test/conformance/baseline/`.

- CI fails only on **new** failures (regressions).
- A PR may **only shrink** a baseline, never grow it. If you fix a conformance
  test, remove its ID from the baseline in the same PR.

## Investigating an intermittent test failure

`make soak` runs the same gated selector against the working tree and a clean
baseline worktree, alternating the two arms, and compares failure rates:

```sh
make soak                                    # 20 runs per arm of TestQoS2Move
SOAK_RUNS=40 SOAK_RUN='TestQoS2Move' make soak
SOAK_BASE=HEAD~1 make soak                   # compare against the previous commit
SOAK_BASE_TAGS=<tag> make soak               # baseline built with other tags (before a tag rename)
```

It exists because a single red run proves nothing on its own, and the tempting
conclusion — "that one is a known flake" — is exactly the one that hides a real
regression. A failure rate is only meaningful next to the rate the *same* command
produces on a tree without the change. Two things it enforces that are easy to get
wrong by hand: the arms alternate, so a slow patch of machine time cannot land on
one of them; and every run must execute at least one test, because a gated suite
whose environment variable is missing skips everything and exits 0 — "0 failures"
would then mean "nothing ran".

Note that `git stash` does **not** isolate a baseline: untracked files stay
behind and leak into it. `make soak` uses a detached worktree instead.

## Commit / PR conventions

- Keep the default tree green: `make vet test` must pass.
- Conventional-commit style messages are appreciated (`feat:`, `fix:`, `docs:`…).
- One logical change per PR.
