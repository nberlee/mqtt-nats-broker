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

Every finding from this audit is resolved; the dossiers are in `resolved.md`.
