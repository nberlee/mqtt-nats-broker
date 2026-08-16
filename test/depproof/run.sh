#!/usr/bin/env bash
#
# Dependency canary for F-030: run the repository-owned JetStream route regressions
# inside an isolated nats-server checkout.
#
# The fixtures live in testdata/ because they are package `server` files: they cannot
# compile inside this module, and the Go tool never builds a directory called
# testdata, so `go build ./...` here stays unaffected.
#
#   ./run.sh                    # the pinned version from go.mod, unmutated
#   ./run.sh --version v2.14.3  # one other version, unmutated
#   ./run.sh --mutation m-term-guard
#   ./run.sh --run <regexp> --require <marker> --forbid <marker> --mutation <name> ...
#   ./run.sh --matrix           # every row of the evidence table in README.md
#   ./run.sh --bridge           # the bridge sentinel and its own mutation
#
# A mutation run is EXPECTED TO FAIL: that is what makes the paired assertion
# evidence rather than decoration. --matrix asserts the direction of every row and
# exits nonzero if any row deviates, so it is the form CI runs.
#
# Outcomes are PASS, FAIL, SKIPPED (nothing actually ran), BROKEN (the mutated
# package did not build, or no test ran -- never counted as a kill) and OFFTARGET
# (something failed, but not a test this row selected).
set -u -o pipefail

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
fixtures="$repo_root/test/depproof/testdata"
work=${DEPPROOF_DIR:-${TMPDIR:-/tmp}/mqtt-depproof}
upstream=${DEPPROOF_UPSTREAM:-https://github.com/nats-io/nats-server.git}

# The control version predates the term-carrying proposal (nats-io/nats-server#8370,
# v2.14.4). It is NOT supported by this broker; it exists to prove the fixture reaches
# the mechanism that changed.
control_version=${DEPPROOF_CONTROL:-v2.14.3}

# Two invocations cannot share a checkout: they would install fixtures and revert
# mutations under each other, and the upstream cluster helper binds fixed ports, so
# the second run's servers fail to listen. Serialize on the work directory.
take_lock() {
  mkdir -p "$work" || die "cannot create $work"
  command -v flock >/dev/null || die "flock is required to serialize runs"
  exec 9>"$work/.lock"
  flock -w "${DEPPROOF_LOCK_WAIT:-1800}" 9 \
    || die "another depproof run is holding $work/.lock"
}

pinned_version() {
  awk '/nats-io\/nats-server\/v2 v/ {print $2; exit}' "$repo_root/go.mod"
}

die() { echo "depproof: $*" >&2; exit 2; }

# checkout prints the path of a worktree at $1, creating the clone and the worktree
# on first use. Checkouts are cached across runs; nothing here writes to the repo.
checkout() {
  local version=$1 src="$work/src" tree="$work/$version"
  mkdir -p "$work" || die "cannot create $work"
  if [ ! -d "$src" ]; then
    git clone --quiet --no-checkout --filter=blob:none "$upstream" "$src" \
      || die "clone failed"
  fi
  if [ ! -d "$tree" ]; then
    git -C "$src" fetch --quiet --tags origin || die "fetch failed"
    git -C "$src" worktree add -q --detach "$tree" "$version" \
      || die "no such nats-server version: $version"
  fi
  echo "$tree"
}

# install_fixtures copies the shared scenario plus the adapter this checkout's
# proposal signature calls for. The choice is made from the source, not from a
# version comparison, so a backport or a rename is picked up by what the code says.
install_fixtures() {
  local tree=$1
  cp "$fixtures/willq_terminal_routes_test.go" "$tree/server/" || die "copy failed"
  rm -f "$tree/server/willq_adapter_term_test.go" "$tree/server/willq_adapter_noterm_test.go"
  if grep -q 'Propose(term uint64' "$tree/server/raft.go"; then
    cp "$fixtures/willq_adapter_term_test.go" "$tree/server/" || die "copy failed"
    echo "term-carrying"
  else
    cp "$fixtures/willq_adapter_noterm_test.go" "$tree/server/" || die "copy failed"
    echo "pre-fence"
  fi
}

# edit applies one exact substitution and fails unless it matched the expected
# number of times. A mutation that silently matched nothing would score a passing
# test as a surviving mutant.
edit() {
  local file=$1 want=$2 from=$3 to=$4 got
  # A multi-line pattern would be split into several patterns by grep -F, and the
  # count would then be a line tally rather than an occurrence count -- which reads as
  # a mismatch and aborts a run that was actually fine. Anchor on ONE line.
  case $from in
    *"
"*) die "mutation patterns must be single-line, got: $from" ;;
  esac
  got=$(grep -F -c -- "$from" "$file") || got=0
  [ "$got" = "$want" ] || die "mutation pattern matched $got times, want $want, in $file: $from"
  python3 - "$file" "$from" "$to" <<'PY'
import sys
path, old, new = sys.argv[1], sys.argv[2], sys.argv[3]
with open(path) as f:
    body = f.read()
with open(path, "w") as f:
    f.write(body.replace(old, new))
PY
}

apply_mutation() {
  local tree=$1 name=$2
  case $name in
    m-term-guard)
      # Raft's refusal of a proposal whose captured term is no longer current.
      edit "$tree/server/raft.go" 2 \
        'if state := n.State(); state != Leader || term != n.term {' \
        'if state := n.State(); state != Leader {'
      ;;
    m-captured-term)
      # JetStream's propagation of the term the operation captured: the fixture
      # replays with the node's current term instead.
      edit "$tree/server/willq_terminal_routes_test.go" 1 \
        'useCurrentTerm := false' 'useCurrentTerm := true'
      ;;
    m-log-order)
      # DIAGNOSTIC, not a matrix row. Bypasses the apply-time log-order comparison
      # only. It does NOT make an admitted stale entry survive, because the store
      # refuses an explicit sequence that is not exactly LastSeq+1 (filestore's
      # ErrSequenceMismatch) -- which is why README.md reports the downstream discard
      # as an observed backstop rather than a mutation-proved mechanism.
      edit "$tree/server/stream.go" 1 \
        'if lseq != (mset.lseq + clfs) {' 'if false {'
      ;;
    m-expected-subject)
      # The pre-proposal comparison of a write's per-subject expectation against the
      # leader's APPLIED state: the mechanism that refuses a write conditioned on a
      # base a committed record has replaced.
      edit "$tree/server/jetstream_batching.go" 1 \
        'if err != nil || fseq != seq {' 'if false {'
      ;;
    m-premature-ready)
      # The readiness envelope's ANNOUNCEMENT half: signal complete leadership, and
      # notify the upper layer, the moment raft enters the leader state rather than when
      # the log this node was elected with has been processed.
      edit "$tree/server/raft.go" 1 \
        'n.aflr = n.pindex' \
        'n.aflr = n.pindex
	n.leaderState.Store(true)
	n.updateLeadChange(true)'
      ;;
    m-freeze-store)
      # The staging seam, not a mechanism removal: hold ONE node's store application in
      # both writers -- the apply path and the catch-up path -- so its applied view stays
      # at the superseded base. It BLOCKS; it must never skip the write and report
      # success, because the apply loop would then call Applied(), `processed` would
      # advance past `aflr` on its own, and the state staged would be "applied, with the
      # data lost" rather than "committed, not yet applied". The fixture arms it for one
      # server and releases it before the cluster shuts down.
      #
      # A mutated checkout no longer satisfies `go build ./server/`, because a non-test
      # file then references a symbol the fixture declares in a _test.go file. `go test`
      # compiles an internal test together with its package, so the runs here are
      # unaffected; and a build failure is scored BROKEN rather than a kill in any case.
      # The checkout is reverted immediately afterwards.
      edit "$tree/server/jetstream_cluster.go" 1 \
        '	// Process the actual message here.' \
        '	willqWaitStore(mset.srv.Name())
	// Process the actual message here.'
      edit "$tree/server/jetstream_cluster.go" 1 \
        '	} else if err := mset.store.StoreRawMsg(subj, hdr, msg, seq, ts, ttl, false); err != nil {' \
        '	} else if willqWaitStore(mset.srv.Name()); false {
	} else if err := mset.store.StoreRawMsg(subj, hdr, msg, seq, ts, ttl, false); err != nil {'
      ;;
    m-behind-campaign)
      # The two refusals that keep a node behind on applies from taking leadership.
      edit "$tree/server/raft.go" 1 \
        'if !n.observer && !n.paused {' 'if !n.observer {'
      edit "$tree/server/raft.go" 1 \
        'if n.observer || n.paused || n.processed < n.commit {' 'if n.observer {'
      ;;
    *) die "unknown mutation: $name" ;;
  esac
}

# revert restores the checkout and PROVES it is clean. A silent failure here would
# carry one row's mutation into the next, which is the one way this harness could
# report a result that is not about the row it names.
revert() {
  local tree=$1 dirty
  git -C "$tree" checkout -- server/ 2>/dev/null
  rm -f "$tree/server/willq_terminal_routes_test.go" \
    "$tree/server/willq_adapter_term_test.go" \
    "$tree/server/willq_adapter_noterm_test.go"
  dirty=$(git -C "$tree" status --porcelain 2>&1) \
    || die "cannot read the checkout at $tree: $dirty"
  [ -z "$dirty" ] || die "the checkout at $tree is not clean after revert, so the next
run would inherit this one's mutations:
$dirty"
}

# one_run prints PASS or FAIL for a single configuration and leaves the checkout
# clean. $3 onwards are mutation names.
one_run() {
  local version=$1 tests=$2; shift 2
  local tree adapter log outcome args=()
  local -a want_markers=() deny_markers=()
  # A row states the EXACT outcome it claims, as markers the fixture prints:
  #   @NAME  the log must carry it for a FAIL to count
  #   !NAME  the log must NOT carry it
  # Requiring alone is not enough. A row that removes one guard and claims the other
  # still holds has to show that the other guard's failure did NOT also occur --
  # otherwise the row scores its expected FAIL either way, and the table would assert a
  # redundancy the run never demonstrated.
  for a in "$@"; do
    case $a in
      @*) want_markers+=("${a#@}") ;;
      !*) deny_markers+=("${a#!}") ;;
      *) args+=("$a") ;;
    esac
  done
  set -- ${args[@]+"${args[@]}"}
  tree=$(checkout "$version") || exit 2
  revert "$tree"
  adapter=$(install_fixtures "$tree") || exit 2
  for m in "$@"; do apply_mutation "$tree" "$m"; done
  # The log name carries the test filter as well as the mutations: two rows can
  # apply the SAME mutation to different tests, and a shared name would make the two
  # runs indistinguishable afterwards -- the second silently overwrites the first.
  log="$work/log-$version-$(printf '%s' "${tests#^}" | tr -c 'A-Za-z0-9' '-')${*:+-}${*// /-}.txt"
  ( cd "$tree" && go test ./server/ -run "$tests" -count=1 -v -timeout "${DEPPROOF_TIMEOUT:-600s}" ) \
    > "$log" 2>&1 && outcome=PASS || outcome=FAIL
  revert "$tree"

  # `go test` reports FAIL for a build failure too, so a mutation that stopped the
  # package compiling would otherwise be scored as a kill. A FAIL is only accepted
  # when a test actually ran and failed, and when the test that failed is one this
  # row selected -- a mutation killing some unrelated assertion is not evidence
  # about the route under test.
  if [ "$outcome" = FAIL ]; then
    if grep -qE '\[build failed\]|^# ' "$log"; then
      outcome=BROKEN
    elif ! grep -qE '^\s*--- FAIL: ' "$log"; then
      outcome=BROKEN
    elif ! grep -E '^\s*--- FAIL: ' "$log" | grep -qE "FAIL: ${tests#^}"; then
      outcome=OFFTARGET
    else
      for m in ${want_markers[@]+"${want_markers[@]}"}; do
        grep -qF "$m" "$log" || { outcome=OFFTARGET; break; }
      done
      if [ "$outcome" = FAIL ]; then
        for m in ${deny_markers[@]+"${deny_markers[@]}"}; do
          if grep -qF "$m" "$log"; then outcome=OFFTARGET; break; fi
        done
      fi
    fi
  fi

  # A skipped test is not a passing one, including when a broad row also contains a
  # passing neighbour. `go test` exits 0 in both cases, so accepting any SKIP would let
  # the clean all-routes row pass without exercising every selected fixture.
  if [ "$outcome" = PASS ]; then
    if grep -qE '^\s*--- SKIP: ' "$log"; then
      outcome=SKIPPED
    elif ! grep -qE '^\s*--- PASS: ' "$log"; then
      outcome=BROKEN
    fi
  fi
  echo "$outcome $version [$adapter] ${*:-no-mutation} -> $log"
}

# ---------------------------------------------------------------------------
# The bridge sentinel's own mutation.
#
# It lives here rather than in the broker's suites because it is the same kind of
# evidence and needs the same scoring: a mutation is only evidence if it fails, and a
# failure is only a kill if a test actually ran and the failure is the one claimed.
#
# The copy is of the working tree (tracked plus untracked, ignored files excluded)
# rather than a worktree of HEAD, so a dependency-canary change exercises the exact
# bridge code and documentation tree being reviewed rather than the previous commit.
# ---------------------------------------------------------------------------

bridge_test='^TestMQTTWillOutboxReplicatedTerminalDoneStands$'
bridge_file=internal/mqtt/will_terminal_cluster_test.go

# bridge_run copies the working tree, optionally applies the unconditional-store
# mutation, runs the sentinel and prints one scored line.
bridge_run() {
  local mutate=$1 dest="$work/bridge" log outcome
  rm -rf "$dest"
  mkdir -p "$dest" || die "cannot create $dest"
  ( cd "$repo_root" && git ls-files --cached --others --exclude-standard -z \
      | tar --null -T - -cf - ) | tar -C "$dest" -xf - \
    || die "could not copy the working tree"

  if [ "$mutate" = mutate ]; then
    # Exactly the substitution the sentinel's own comment names: the conditional
    # reclaim becomes the unconditional store the negative tests use.
    edit "$dest/$bridge_file" 1 \
      'reclaim := licensed' \
      'reclaim := licensed
	{
		body, _ := json.Marshal(&reclaim)
		_, _ = asm.jsa.storeMsg(subject, -1, body)
	}'
    edit "$dest/$bridge_file" 1 '	"fmt"' '	"encoding/json"
	"fmt"'
  fi

  log="$work/log-bridge-${mutate}.txt"
  ( cd "$dest" && go test -tags mqttport_fulltests -count=1 -run "$bridge_test" \
      ./internal/mqtt/ -v ) > "$log" 2>&1 && outcome=PASS || outcome=FAIL

  if [ "$outcome" = FAIL ]; then
    if grep -qE '\[build failed\]|^# ' "$log"; then
      outcome=BROKEN
    elif ! grep -qE '^\s*--- FAIL: TestMQTTWillOutboxReplicatedTerminalDoneStands' "$log"; then
      outcome=OFFTARGET
    elif [ "$mutate" = mutate ] && ! grep -q 'published again: got 2 publications' "$log"; then
      # The kill has to be the EXTERNAL act, and specifically a SECOND publication of a
      # Will already published once. Requiring the count makes that explicit: a failure
      # that only noticed the stored bytes, or one that counted a first publication as a
      # repeat, leaves the sentinel proving nothing about republication.
      outcome=OFFTARGET
    fi
  elif ! grep -qE '^\s*--- PASS: TestMQTTWillOutboxReplicatedTerminalDoneStands' "$log"; then
    outcome=BROKEN
  fi
  rm -rf "$dest"
  echo "$outcome bridge-sentinel ${mutate} -> $log"
}

# bridge_matrix asserts both directions when the bridge sentinel is run on its own.
# Printing two outcomes and exiting zero would let callers mistake a broken mutation
# for evidence, so this is shared by --bridge and the full matrix.
bridge_matrix() {
  local failures=0 line want mode row
  for row in "PASS clean" "FAIL mutate"; do
    # shellcheck disable=SC2086
    set -- $row
    want=$1
    mode=$2
    line=$(bridge_run "$mode") || exit 2
    echo "$line"
    case $line in
      "$want "*) ;;
      *) echo "  ^ expected $want"; failures=$((failures + 1)) ;;
    esac
  done
  [ "$failures" -eq 0 ] || return 1
  echo "depproof: both bridge-sentinel rows matched the evidence table"
}

matrix() {
  local pinned failures=0 line
  pinned=$(pinned_version)
  # Each row is: expectation, version, test filter, mutations.
  # A FAIL row is a mutation that must break the assertion it targets.
  local rows=(
    "PASS $pinned ^TestJetStreamWillq"
    "FAIL $pinned ^TestJetStreamWillqStaleTerm @R-OLD-TERM-ADMITTED m-term-guard"
    "FAIL $pinned ^TestJetStreamWillqStaleTerm @R-OLD-TERM-ADMITTED m-captured-term"
    "FAIL $pinned ^TestJetStreamWillqNewLeaderRefusesTheSupersededBase @R-NEW-STALE-PROPOSAL @R-NEW-ACKED m-expected-subject"
    "FAIL $pinned ^TestJetStreamWillqNewLeaderCannotServeBeforeApplying @R-NEW-LED-BEHIND !R-NEW-SERVED-STALE !R-NEW-STALE-PROPOSAL !R-NEW-ACKED m-behind-campaign m-freeze-store"
    "PASS $pinned ^TestJetStreamWillqNewLeaderCannotServeBeforeApplying m-premature-ready m-freeze-store"
    "FAIL $pinned ^TestJetStreamWillqNewLeaderCannotServeBeforeApplying @R-NEW-LED-BEHIND @R-NEW-SERVED-STALE @R-NEW-STALE-PROPOSAL m-behind-campaign m-premature-ready m-freeze-store"
    "PASS $control_version ^TestJetStreamWillq"
  )
  for row in "${rows[@]}"; do
    # shellcheck disable=SC2086
    set -- $row
    local want=$1; shift
    line=$(one_run "$@") || exit 2
    echo "$line"
    case $line in
      "$want "*) ;;
      *) echo "  ^ expected $want" ; failures=$((failures + 1)) ;;
    esac
  done

  # The bridge sentinel and its own mutation, scored the same way.
  bridge_matrix || failures=$((failures + 1))
  if [ "$failures" -gt 0 ]; then
    echo "depproof: $failures row(s) did not match the evidence table" >&2
    return 1
  fi
  echo "depproof: every row matched the evidence table"
}

take_lock

version=$(pinned_version)
tests='^TestJetStreamWillq'
mutations=()
while [ $# -gt 0 ]; do
  case $1 in
    --matrix) matrix; exit $? ;;
    --bridge) bridge_matrix; exit $? ;;
    --version) version=${2:?}; shift 2 ;;
    --mutation) mutations+=("${2:?}"); shift 2 ;;
    --require) mutations+=("@${2:?}"); shift 2 ;;
    --forbid) mutations+=("!${2:?}"); shift 2 ;;
    --run) tests=${2:?}; shift 2 ;;
    -h | --help) sed -n '2,20p' "$0"; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done
one_run "$version" "$tests" "${mutations[@]+"${mutations[@]}"}"
