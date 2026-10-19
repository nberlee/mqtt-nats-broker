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
