#!/usr/bin/env bash
# Run the MQTT conformance suites against a running broker and compare the
# results against a ratcheting known-failures baseline.
#
# Suites:
#   1. vibesrc/mqttconformance  — OASIS spec conformance (MQTT 3.1.1 + 5.0), Rust CLI
#   2. nats-io/mqtt-test        — interop CLI (also driven by the Go external tests)
#   3. Eclipse Paho interop     — canonical Python interoperability scripts
#
# Env:
#   BROKER_HOST        (default localhost)
#   BROKER_PORT        (default 1883)
#   SUITES             (default "conformance") space-separated subset of: conformance natstest paho
#   NATS_MONITOR_URL   (default http://127.0.0.1:8222) NATS monitoring endpoint
#   NATS_VERSION_FLOOR (default 2.15.0) minimum backing nats-server version
#
# Every run first asserts the backing NATS server against the deployment floor and
# the consumer-cap prerequisite; see check_nats_floor. There is deliberately no
# opt-out, because a skipped check reports green from a capped server.
#
# Requires curl and python3. python3 is a hard requirement of that check even though
# the Paho suite only warns and skips without it: the check decodes /varz rather than
# pattern-matching it, and a check that cannot run must not be silently passed over.
#
# The baseline files in ./baseline/ list tests that are known to fail. CI fails
# only on NEW failures (regressions); the baseline may only shrink over time.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BROKER_HOST="${BROKER_HOST:-localhost}"
BROKER_PORT="${BROKER_PORT:-1883}"
SUITES="${SUITES:-conformance}"
RESULTS="${HERE}/results"
mkdir -p "$RESULTS"

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mWARN:\033[0m %s\n' "$*"; }

# Assert the backing NATS server meets the deployment floor AND has the per-stream
# consumer cap disarmed. BOTH, and the second is the one that is easy to lose: from
# the 2.15.0 line the server substitutes a 1000-consumer default for a zero value,
# so an absent `default_max_consumers` is indistinguishable from an armed cap.
# Asserting the version alone would leave every launch capped while reporting green,
# which is the failure this check exists to prevent (F-095).
#
# Measured against the 2.15.0 GA binary: `-js` with no config file and a config with
# no `limits` block both report 1000; only `default_max_consumers: -1` reports -1.
# That is what makes this assertion discriminating rather than decorative.
check_nats_floor() {
  local url="${NATS_MONITOR_URL:-http://127.0.0.1:8222}" floor="${NATS_VERSION_FLOOR:-2.15.0}"
  command -v curl >/dev/null 2>&1 || { echo "curl is required to verify the NATS floor" >&2; exit 1; }

  # Bound the individual request AND the whole retry window. An endpoint that accepts
  # the connection and then never answers must fail the prerequisite, not hang the
  # suite behind it -- an unbounded read is indistinguishable from a passing check
  # that simply has not returned.
  local varz="" deadline=$((SECONDS + 60))
  while ((SECONDS < deadline)); do
    varz="$(curl -sf --connect-timeout 3 --max-time 10 "${url}/varz" 2>/dev/null)" && [[ -n "$varz" ]] && break
    varz=""; sleep 2
  done
  if [[ -z "$varz" ]]; then
    echo "could not read ${url}/varz within 60s — the NATS server must expose a monitoring port (-m 8222)" >&2
    exit 1
  fi

  # DECODE the document and decide inside the decoder. Four review rounds each defeated
  # a different way of doing this by hand -- a prefix match reading "-1.5" as -1, a
  # duplicate whose second value was malformed, a duplicate key written with a \u escape
  # so it did not match literally, and a version string containing a newline that shifted
  # a line-based handoff so an injected "-1" was read as the limit. The pattern under all
  # four is the same: re-deriving a value after carrying it across a boundary. So there
  # is no boundary. python3 parses, validates and compares, and this function consumes
  # only its exit status -- there is no value left for the shell to misread.
  command -v python3 >/dev/null 2>&1 || {
    echo "python3 is required to verify the NATS floor (it parses ${url}/varz)" >&2; exit 1; }

  local report
  report="$(printf '%s' "$varz" | NATS_FLOOR="$floor" python3 -c '
import json, os, re, sys

RELEASE = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")

def reject_duplicates(pairs):
    seen = set()
    for key, _ in pairs:
        if key in seen:
            raise ValueError("duplicate key in /varz object: %s" % key)
        seen.add(key)
    return dict(pairs)

floor = os.environ["NATS_FLOOR"]
if not RELEASE.fullmatch(floor):
    sys.exit("NATS_VERSION_FLOOR %r is not an X.Y.Z release version" % floor)

try:
    doc = json.loads(sys.stdin.read(), object_pairs_hook=reject_duplicates)
except ValueError as exc:
    sys.exit("/varz is not a single unambiguous JSON document: %s" % exc)

try:
    version = doc["version"]
    limit = doc["jetstream"]["limits"]["default_max_consumers"]
except (KeyError, TypeError):
    sys.exit("/varz has no jetstream.limits.default_max_consumers: the per-stream "
             "consumer cap is armed at its 1000 default unless it reads exactly -1")

# fullmatch, so an embedded newline or any suffix is rejected rather than truncated
# away. Rejecting prereleases is the point and not a side effect: sort -V orders
# 2.15.0-RC.1 ABOVE 2.15.0, so a range test alone admits precisely the population a
# GA floor exists to exclude.
if not isinstance(version, str) or not RELEASE.fullmatch(version):
    sys.exit("NATS server version %r is not a release version: the %s floor names the "
             "GA, so prereleases do not meet it" % (version, floor))
# bool is an int subclass in Python, and true must not read as 1.
if isinstance(limit, bool) or not isinstance(limit, int):
    sys.exit("/varz default_max_consumers is not an integer: %r" % (limit,))

as_parts = lambda v: tuple(int(p) for p in v.split("."))
if as_parts(version) < as_parts(floor):
    sys.exit("NATS server is %s, below the %s deployment floor" % (version, floor))
if limit != -1:
    sys.exit("NATS default_max_consumers is %d, not -1: the per-stream consumer cap is "
             "ARMED.\nSet  jetstream { limits { default_max_consumers: -1 } }  in the "
             "config for this launch." % limit)

sys.stdout.write("NATS %s (>= %s), default_max_consumers=-1" % (version, floor))
' 2>&1)" || { echo "$report" >&2; exit 1; }
  log "$report"
}

wait_for_broker() {
  log "waiting for broker at ${BROKER_HOST}:${BROKER_PORT}"
  for _ in $(seq 1 30); do
    if (exec 3<>"/dev/tcp/${BROKER_HOST}/${BROKER_PORT}") 2>/dev/null; then
      exec 3>&- ; log "broker is up"; return 0
    fi
    sleep 1
  done
  echo "broker never came up" >&2; exit 1
}

# Compare a newline-separated list of failing test IDs ($1) against a baseline
# file ($2). Exit non-zero if any failure is NOT in the baseline.
check_regressions() {
  local failures="$1" baseline="$2" name="$3"
  touch "$baseline"
  # Strip comments/blank lines from the baseline before comparing.
  local clean; clean="$(mktemp)"
  grep -vE '^\s*(#|$)' "$baseline" > "$clean" || true
  local new
  new="$(comm -23 <(sort -u "$failures") <(sort -u "$clean") || true)"
  if [[ -n "$new" ]]; then
    warn "[$name] NEW failures (regressions) not in baseline:"
    echo "$new" | sed 's/^/    /'
    return 1
  fi
  local fixed
  fixed="$(comm -13 <(sort -u "$failures") <(sort -u "$clean") || true)"
  if [[ -n "$fixed" ]]; then
    log "[$name] baseline entries now passing (please remove from baseline):"
    echo "$fixed" | sed 's/^/    /'
  fi
  rm -f "$clean"
  log "[$name] no regressions"
}

run_conformance() {
  if ! command -v mqtt-conformance >/dev/null 2>&1; then
    warn "mqtt-conformance not found; build from https://github.com/vibesrc/mqttconformance (cargo build --release)"
    return 0
  fi
  # vibesrc/mqttconformance CLI: `run -H host -p port -v 3|5` (verbose -V lists
  # per-test lines). It has no JSON output, so parse the verbose text. As of the
  # last run the broker passed 124/124 (v3.1.1) and 106/106 (v5).
  local rc=0
  for v in 3 5; do
    log "running vibesrc/mqttconformance v${v}"
    mqtt-conformance run -H "$BROKER_HOST" -p "$BROKER_PORT" -v "$v" -V \
      > "$RESULTS/conformance-v${v}.log" 2>&1 || true
    # Verbose failing lines look like "  ✗ [FAIL] MQTT-3.1.0-1 (..)"; collect the
    # normative IDs. The "[FAIL]" tag distinguishes them from the "✗ N failed"
    # summary line.
    grep -F '[FAIL]' "$RESULTS/conformance-v${v}.log" | sed -E 's/.*\[FAIL\] //; s/ .*//' \
      > "$RESULTS/conformance-v${v}.failures" || : > "$RESULTS/conformance-v${v}.failures"
    check_regressions "$RESULTS/conformance-v${v}.failures" "$HERE/baseline/conformance-v${v}.txt" "conformance-v${v}" || rc=1
  done
  return $rc
}

run_natstest() {
  if ! command -v mqtt-test >/dev/null 2>&1; then
    warn "mqtt-test not found; install from https://github.com/nats-io/mqtt-test"
    return 0
  fi
  log "running nats-io/mqtt-test"
  mqtt-test -server "tcp://${BROKER_HOST}:${BROKER_PORT}" > "$RESULTS/natstest.log" 2>&1 || true
  grep -iE 'FAIL' "$RESULTS/natstest.log" | awk '{print $2}' > "$RESULTS/natstest.failures" || : > "$RESULTS/natstest.failures"
  check_regressions "$RESULTS/natstest.failures" "$HERE/baseline/natstest.txt" natstest
}

# Pinned Eclipse Paho paho.mqtt.testing revision. Bump deliberately (and re-run
# the baseline) rather than tracking a moving branch.
PAHO_REPO="${PAHO_REPO:-https://github.com/eclipse-paho/paho.mqtt.testing}"
PAHO_REF="${PAHO_REF:-9d7bb80bb8b9d9cfc0b52f8cb4c1916401281103}"
PAHO_TEST_TIMEOUT="${PAHO_TEST_TIMEOUT:-60}"

run_paho() {
  if ! command -v python3 >/dev/null 2>&1; then
    warn "python3 not found; skipping Eclipse Paho interoperability suite"
    return 0
  fi
  local dir="$HERE/paho"
  if [[ ! -d "$dir/interoperability" ]]; then
    log "cloning Eclipse Paho interoperability suite ($PAHO_REF)"
    rm -rf "$dir"
    if ! git clone --quiet "$PAHO_REPO" "$dir"; then
      warn "unable to clone $PAHO_REPO; skipping Paho suite"
      return 0
    fi
    (cd "$dir" && git checkout --quiet "$PAHO_REF") || {
      warn "unable to checkout $PAHO_REF; skipping Paho suite"
      return 0
    }
  fi
  local interop="$dir/interoperability"
  local driver="$HERE/paho_driver.py"

  log "running Eclipse Paho interoperability suite (MQTT 5.0)"
  : > "$RESULTS/paho.log"
  : > "$RESULTS/paho.failures"

  # One test per process, each under a timeout: several suite tests busy-wait
  # for their expected messages with no internal timeout and would hang the run
  # on any dropped message. A timeout is recorded as a failure.
  local tests
  tests="$(python3 "$driver" "$interop" "$BROKER_HOST" "$BROKER_PORT" --list 2>>"$RESULTS/paho.log")" || true
  if [[ -z "$tests" ]]; then
    warn "could not enumerate Paho tests (see $RESULTS/paho.log); skipping"
    return 0
  fi
  for t in $tests; do
    echo "=== $t ===" >> "$RESULTS/paho.log"
    if timeout "$PAHO_TEST_TIMEOUT" python3 "$driver" "$interop" "$BROKER_HOST" "$BROKER_PORT" "$t" \
        >> "$RESULTS/paho.log" 2>&1; then
      :
    else
      echo "$t" >> "$RESULTS/paho.failures"
    fi
  done
  check_regressions "$RESULTS/paho.failures" "$HERE/baseline/paho.txt" paho
}

check_nats_floor
wait_for_broker
rc=0
for s in $SUITES; do
  case "$s" in
    conformance) run_conformance || rc=1 ;;
    natstest)    run_natstest || rc=1 ;;
    paho)        run_paho || rc=1 ;;
    *) warn "unknown suite: $s" ;;
  esac
done
exit $rc
