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

## F-052

**Title:** MQTT option validation is disconnected from the production startup path

- Status: Open
- Classification: Validation-boundary and misleading-test finding
- Release treatment: Maintainability item; promote if startup testing finds an
  accepted invalid configuration

### Evidence

`validateMQTTOptions` is called only by `mqttport_fulltests` files. Neither
`NewServer`, `Start`, nor the command bootstrap calls it. Several tests therefore
prove the behavior of a validator that production never executes.

The command's `internal/config.Config.Validate` independently protects the subset
of settings exposed by flags and environment variables. That limits the current
CLI consequence, but does not make the ported validator live. Calling it unchanged
is not a fix: it still contains embedded-NATS assumptions, including an internal
JetStream-enabled check that does not describe this external-backend process.

### Commit-sized resolution

Define one validation boundary for the standalone broker. Either adapt the useful
checks and invoke them from `Start`, or delete the obsolete validator and retarget
its valuable cases to the command configuration and actual startup boundary. Tests
must call the production entry point they claim to validate.

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
