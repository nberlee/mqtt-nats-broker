# Contributing

Thanks for helping build `mqtt-nats-bridge`.

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
them separately). Markdown rules live in `.markdownlint.jsonc`; vendored
third-party docs are excluded via `.markdownlintignore`, since their style is
upstream's decision. CI runs the same linter and configuration.

## Code layout

```text
cmd/mqtt-nats-bridge   entrypoint: config, backend connect, health, shutdown
internal/config        flags/env configuration
internal/backend       NATS/JetStream service conn + per-client auth-passthrough dial
internal/conn          (planned) TCP/TLS accept + per-connection read/write loop
internal/mqtt          MQTT protocol core (ported from NATS-server; see below)
test/conformance       compose env + conformance runner + baselines
```

## Planning and compatibility assumptions

This project is working toward its **first public release**. Use these defaults
in design documents and implementation plans unless a task explicitly changes
them:

- There is no released bridge version, persisted MQTT-state format, or
  mixed-version bridge deployment to support yet.
- Do not add migration code, dual readers/writers, legacy fallbacks, or rolling-
  upgrade protocols for pre-release contents of the internal `$MQTT_*`
  JetStream streams. Prefer the clean current schema and fail fast with an
  actionable instruction to recreate development data when necessary.
- Support only the current JetStream behavior and APIs exercised by the latest
  `nats-server/v2` version pinned in `go.mod` and CI. Do not add compatibility
  branches for older JetStream releases.
- These rules concern bridge-internal persistence and backend compatibility.
  They do **not** relax MQTT 3.1.1/5.0 wire-protocol conformance.

Call out any proposed exception explicitly, including who needs compatibility,
which released artifact or persisted format establishes the contract, and how
the compatibility path will be tested.

## The ported MQTT core

`internal/mqtt` is a port of NATS-server's `server/mqtt.go`. It builds and serves
in the default tree; the behavioral shims that replace NATS-server's runtime live
in `shim_*.go` and the vendored helpers in `compat_*.go`.

- **Do not hand-edit** `mqtt.go` (the upstream port), `compat_*.go` (vendored
  helpers), or the aliased wire types beyond what is needed — keep them diff-able
  against upstream where practical. This is a maintenance preference, not a
  source-compatibility or product-design requirement; bridge correctness and a
  clean architecture take precedence. These files are excluded from lint for
  that reason.
- The upstream regression/conformance suite (`mqtt_test.go`, `mqtt_v5_test.go`,
  `mqtt_ex_*`) is retained behind the `mqttport_fulltests` tag until its NATS
  test-server harness is ported:

  ```sh
  make full-suite   # go test -tags mqttport_fulltests ./internal/mqtt/
  ```

- The delayed-Will outbox also has **real replicated-JetStream** tests behind the
  same tag. They launch **three embedded nats-server processes** joined into one
  cluster so `$MQTT_willq` is genuinely `R3`. The cheap R3 smoke test
  (`TestMQTTWillOutboxReplicatedStream`) runs in the normal `make full-suite`; the
  slow failure-injection tests self-skip unless `MQTTNB_CLUSTER_FAILOVER_TESTS=1` and
  cover both **stream-leader node loss** (`…ReplicatedLeaderFailover`) and
  **stale-replica restart/catch-up** (`…ReplicatedNodeCatchup`):

  ```sh
  make cluster-failover-tests   # MQTTNB_CLUSTER_FAILOVER_TESTS=1 go test -tags mqttport_fulltests -race -run '^TestMQTTWillOutboxReplicated' ./internal/mqtt/
  ```

- The port mapping and remaining work are documented in
  [docs/PORTING.md](docs/PORTING.md).

### Syncing with upstream NATS

The core mirrors nats-server. When NATS patches its MQTT 3.1.1 behavior, diff the
relevant hunks from `github.com/nats-io/nats-server/v2/server/mqtt.go` against
`internal/mqtt/mqtt.go` and forward-port. Record the upstream commit you synced
to in the PR description.

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
