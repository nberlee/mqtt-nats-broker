#!/usr/bin/env bash
#
# Paired flakiness soak: run the same gated test selector against the working
# tree ("cand") and a clean baseline worktree ("base"), and compare failure
# rates.
#
# Why paired rather than "run it a few times and see": an intermittent failure
# proves nothing on its own, and the tempting conclusion — "that one is a known
# flake" — is exactly the one that hides a real regression. A rate is only
# meaningful against the rate the same command produces on a tree that does not
# contain the change.
#
# Two properties this script exists to guarantee, both of which are easy to get
# wrong by hand:
#
#   * The arms ALTERNATE. Running one arm to completion and then the other lets
#     a slow patch of machine time land entirely on one of them, which reads as
#     a difference between the arms.
#   * Every run is checked for VACUITY. A gated suite whose environment variable
#     is missing skips everything and exits 0, so "0 failures" can mean "nothing
#     ran". Each run must execute at least one test or the soak aborts.
#
# Usage:
#   make soak                                   # defaults below
#   SOAK_RUNS=40 SOAK_RUN='TestQoS2Move' make soak
#   SOAK_BASE=origin/main make soak
#
# The baseline worktree is created from SOAK_BASE, which must be a committed
# ref: uncommitted work in the working tree is precisely what is under test, and
# `git stash` does NOT isolate it (untracked files stay behind and leak into the
# "baseline"), which is why this uses a detached worktree instead.

set -uo pipefail

RUNS="${SOAK_RUNS:-20}"
SELECTOR="${SOAK_RUN:-TestQoS2Move}"
BASE_REF="${SOAK_BASE:-HEAD}"
DIR="${SOAK_DIR:-/tmp/mqtt-nats-bridge-soak}"

REPO="$(git rev-parse --show-toplevel)"
CAND="$REPO"
BASE="$DIR/baseline"
LOGS="$DIR/logs"
SUMMARY="$DIR/summary.txt"

# The gated cluster tests self-skip without this; see CONTRIBUTING.md.
export MQTTNB_CLUSTER_FAILOVER_TESTS=1
TEST_CMD=(go test -count=1 -v -tags mqttport_fulltests -race -run "$SELECTOR" ./internal/mqtt/)

echo "soak: $RUNS runs per arm, selector '$SELECTOR', baseline '$BASE_REF'"
echo "soak: artifacts in $DIR"

rm -rf "$DIR"
mkdir -p "$LOGS"
git -C "$REPO" worktree prune
if ! git -C "$REPO" worktree add --detach "$BASE" "$BASE_REF" >/dev/null 2>&1; then
  echo "soak: could not create a baseline worktree at $BASE_REF" >&2
  exit 1
fi
trap 'git -C "$REPO" worktree remove --force "$BASE" >/dev/null 2>&1; git -C "$REPO" worktree prune' EXIT

# A selector that names tests which exist in only one arm is not a comparison.
for arm_dir in "$BASE" "$CAND"; do
  if ! (cd "$arm_dir" && go vet -tags mqttport_fulltests ./internal/mqtt/ >/dev/null 2>&1); then
    echo "soak: $arm_dir does not build with the fulltests tag" >&2
    exit 1
  fi
done

: >"$SUMMARY"
for i in $(seq 1 "$RUNS"); do
  for arm in base cand; do
    [ "$arm" = base ] && d="$BASE" || d="$CAND"
    log="$LOGS/$arm.$i.log"
    start=$(date +%s)
    (cd "$d" && "${TEST_CMD[@]}") >"$log" 2>&1
    rc=$?
    secs=$(( $(date +%s) - start ))
    ran=$(grep -c '^=== RUN' "$log")
    failed=$(grep -c '^--- FAIL' "$log")

    if [ "$ran" -eq 0 ]; then
      echo "soak: ABORT — $arm run $i executed no tests at all; '0 failures' would be meaningless." >&2
      echo "soak: see $log" >&2
      exit 1
    fi
    echo "$arm $i rc=$rc secs=$secs ran=$ran failed=$failed" | tee -a "$SUMMARY"
    if [ "$rc" -ne 0 ]; then
      echo "--- $arm run $i failures ---"
      grep -E '^--- FAIL|^    [a-z_]+\.go:[0-9]+:' "$log" | head -20
    fi
  done
done

base_fail=$(grep -c '^base .* rc=[1-9]' "$SUMMARY")
cand_fail=$(grep -c '^cand .* rc=[1-9]' "$SUMMARY")
echo
echo "soak: baseline  $base_fail failed / $RUNS runs"
echo "soak: candidate $cand_fail failed / $RUNS runs"
echo "soak: full output per run in $LOGS"

if [ "$cand_fail" -gt "$base_fail" ]; then
  echo "soak: VERDICT — the working tree failed more often than the baseline."
  echo "soak: treat this as a regression until shown otherwise; the logs above name the tests."
  exit 1
fi
if [ "$cand_fail" -gt 0 ]; then
  echo "soak: VERDICT — both arms failed at a comparable rate, so the failure predates this tree."
  exit 0
fi
echo "soak: VERDICT — no failures in either arm."
echo "soak: this bounds the rate below roughly 1/$RUNS; it does not prove a rarer failure absent."
