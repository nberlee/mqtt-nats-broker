#!/usr/bin/env bash
#
# Dependency canary: run the repository-owned JetStream write-contract regressions
# inside an isolated nats-server checkout. Two questions -- the Will outbox's terminal
# state (F-030) and the accepted-CONNECT atomic batch -- answered the same way.
#
# The fixtures live in testdata/ because they are package `server` files: they cannot
# compile inside this module, and the Go tool never builds a directory called
# testdata, so `go build ./...` here stays unaffected.
#
#   ./run.sh                    # the pinned version from go.mod, unmutated
#   ./run.sh --version v2.14.3  # one other version, unmutated
#   ./run.sh --mutation m-term-guard
#   ./run.sh --run <regexp> --require <marker> --forbid <marker> --mutation <name> ...
#   ./run.sh --run <regexp> --fixture acceptbatch_partial_test.go
#   ./run.sh --matrix           # every row of the evidence table in README.md
#   ./run.sh --bridge           # the bridge sentinels and their own mutations
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
#
# It prints the adapter name and NOTHING else: one_run captures that as the label it
# reports. A fixture that only some rows want is installed by one_run instead (see
# --fixture), because selecting it here would mean asking which version is running --
# exactly the rollout-shaped branching this repository's compatibility boundary
# forbids, and it would also have to be kept out of this function's stdout.
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
    m-batch-apply-eager)
      # The apply-layer mechanism the accepted-CONNECT batch's indivisibility rests on:
      # a committed member of a batch is BUFFERED until its commit entry arrives, and
      # only a gap-free commit replays the members into the store. This applies each
      # member the moment it is committed instead.
      #
      # Written as an ADDED eager side effect rather than as a removal of the retention
      # at the end of applyStreamEntries: dropping the retention would leave the members
      # unstored, and the fixture's negative assertion would hold for the wrong reason
      # and score the mutant a survivor.
      #
      # NO batch lock is taken. Through v2.14.5 batchApply hung off the stream with its
      # own mutex, and this mutation had to release it across the apply because
      # applyStreamMsgOp takes mset.mu and the stream lock is acquired BEFORE the batch
      # lock everywhere else in that function. From v2.15.0-preview.1 batchApply is a
      # monitor-goroutine LOCAL with no mutex at all, so there is no lock to juggle and
      # no order to invert. mset.mu is already released at the insertion point.
      #
      # This patch therefore targets the PINNED version's shape. Re-pinning below
      # v2.15.0 needs the batch.mu form back; the fixture that pairs with it carries the
      # same note.
      edit "$tree/server/jetstream_cluster.go" 1 \
        '				// If the sequence is not monotonically increasing/we identify gaps, the batch can'"'"'t be accepted.' \
        '				// depproof m-batch-apply-eager: apply the member as it is committed.
				if batchSeq == batch.count {
					_, _, eop, ebuf, eerr := decodeBatchMsg(buf[1:])
					if eerr != nil {
						panic(eerr.Error())
					}
					if eerr = js.applyStreamMsgOp(mset, eop, ebuf, isRecovering, true); eerr != nil {
						return 0, eerr
					}
				}
				// If the sequence is not monotonically increasing/we identify gaps, the batch can'"'"'t be accepted.'
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
  # Every fixture this harness can install, unconditionally -- including the ones only
  # some rows ask for. A row-selected fixture left behind would compile into the NEXT
  # row's run, which is the same class of cross-row contamination the mutation check
  # below exists to catch.
  rm -f "$tree/server/willq_terminal_routes_test.go" \
    "$tree/server/willq_adapter_term_test.go" \
    "$tree/server/willq_adapter_noterm_test.go" \
    "$tree/server/acceptbatch_partial_test.go"
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
  local -a want_markers=() deny_markers=() extra_fixtures=()
  # A row states the EXACT outcome it claims, as markers the fixture prints:
  #   @NAME  the log must carry it for a FAIL to count
  #   !NAME  the log must NOT carry it
  # Requiring alone is not enough. A row that removes one guard and claims the other
  # still holds has to show that the other guard's failure did NOT also occur --
  # otherwise the row scores its expected FAIL either way, and the table would assert a
  # redundancy the run never demonstrated.
  #   +FILE  a fixture this row needs on top of the shared ones
  for a in "$@"; do
    case $a in
      @*) want_markers+=("${a#@}") ;;
      !*) deny_markers+=("${a#!}") ;;
      +*) extra_fixtures+=("${a#+}") ;;
      *) args+=("$a") ;;
    esac
  done
  set -- ${args[@]+"${args[@]}"}
  tree=$(checkout "$version") || exit 2
  revert "$tree"
  adapter=$(install_fixtures "$tree") || exit 2
  for f in ${extra_fixtures[@]+"${extra_fixtures[@]}"}; do
    [ -f "$fixtures/$f" ] || die "no such fixture: $f"
    cp "$fixtures/$f" "$tree/server/" || die "copy failed"
  done
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

  # Markers are scored on BOTH directions, because they state what the run OBSERVED and
  # that is a claim about a passing row as much as a failing one. A clean row whose
  # whole contract is "every replica committed and processed the prefix AND nothing was
  # stored" is unenforced if
  # a PASS is accepted without reading them: the required marker would go unchecked if
  # the observation never happened, and the forbidden one would go unchecked if it did.
  # BROKEN and OFFTARGET are left alone -- there is no run to make a claim about.
  if [ "$outcome" = PASS ] || [ "$outcome" = FAIL ]; then
    for m in ${want_markers[@]+"${want_markers[@]}"}; do
      grep -qF "$m" "$log" || { outcome=OFFTARGET; break; }
    done
  fi
  if [ "$outcome" = PASS ] || [ "$outcome" = FAIL ]; then
    for m in ${deny_markers[@]+"${deny_markers[@]}"}; do
      if grep -qF "$m" "$log"; then outcome=OFFTARGET; break; fi
    done
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

accept_test='^TestMQTTAcceptBatchReplicatedIndivisible$'
accept_file=internal/mqtt/accept_batch_cluster_test.go

# copy_working_tree materializes the working tree (tracked plus untracked, ignored
# files excluded) at $1, replacing whatever is there.
copy_working_tree() {
  local dest=$1
  rm -rf "$dest"
  mkdir -p "$dest" || die "cannot create $dest"
  ( cd "$repo_root" && git ls-files --cached --others --exclude-standard -z \
      | tar --null -T - -cf - ) | tar -C "$dest" -xf - \
    || die "could not copy the working tree"
}

# bridge_run copies the working tree, optionally applies the unconditional-store
# mutation, runs the sentinel and prints one scored line.
bridge_run() {
  local mutate=$1 dest="$work/bridge" log outcome
  copy_working_tree "$dest"

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

# ---------------------------------------------------------------------------
# The accepted-CONNECT batch sentinel and its own mutation.
#
# m-accept-split-write replaces the one atomic batch with the two independent
# conditional writes the bridge claims it does NOT make: the acceptance fact is
# acknowledged on its own, the session record write is then suppressed through the
# sentinel's counterfactual phase, and the caller is handed an acknowledgement it
# cannot tell from a real commit. Both messages already carry their own per-subject
# CAS, so removing the batch headers is the whole difference.
#
# The mutation must produce a TORN PAIR specifically. A CONNECT that merely failed, or
# a cluster that never formed, is not evidence about indivisibility, so the row is
# scored on the sentinel marker rather than on any failure.
# ---------------------------------------------------------------------------

accept_run() {
  local mutate=$1 dest="$work/accept" log outcome
  copy_working_tree "$dest"

  if [ "$mutate" = mutate ]; then
    # The two messages stop being members of a batch...
    edit "$dest/internal/mqtt/sess_accept_fact.go" 1 \
      '	fb, fh := mqttFrameBatchWrite(0, factBody, batchID, 1, false)' \
      '	fb, fh := mqttFrameBatchWrite(0, factBody, _EMPTY_, 1, false)'
    edit "$dest/internal/mqtt/sess_accept_fact.go" 1 \
      '	rb, rh := mqttFrameBatchWrite(baseSeq, recBody, batchID, 2, true)' \
      '	rb, rh := mqttFrameBatchWrite(baseSeq, recBody, _EMPTY_, 2, true)'
    # ... and the store issues them one after the other instead of committing them
    # together. The helper is inserted into the sentinel file, where the
    # counterfactual sits next to the assertion it must break.
    edit "$dest/internal/mqtt/mqtt.go" 1 \
      '	bi, err := jsa.newAtomicBatchRequest(mqttJSASessPersist, cidHash, []mqttBatchMsg{fact, record})' \
      '	bi, err := jsa.storeAcceptSplit(cidHash, batchID, fact, record)'
    edit "$dest/$accept_file" 1 \
      '// MUTATION ANCHOR: test/depproof/run.sh inserts the m-accept-split-write helper here' \
      'func (jsa *mqttJSA) storeAcceptSplit(cidHash, batchID string, fact, record mqttBatchMsg) (any, error) {
	fi, err := jsa.newRequestEx(mqttJSASessPersist, fact.subj, cidHash, fact.hdr, fact.msg)
	if err != nil {
		return nil, err
	}
	resp, _ := fi.(*JSPubAckResponse)
	if resp == nil || resp.PubAck == nil {
		return resp, fmt.Errorf("m-accept-split-write: the fact write was not acknowledged")
	}
	if aerr := resp.ToError(); aerr != nil {
		return resp, aerr
	}
	if oerr := jsa.observeAcceptStore(mqttAcceptStoreSplitRecordPending, batchID, fact, record, resp.Sequence); oerr == nil {
		ri, rerr := jsa.newRequestEx(mqttJSASessPersist, record.subj, cidHash, record.hdr, record.msg)
		if rerr != nil {
			return nil, rerr
		}
		rr, _ := ri.(*JSPubAckResponse)
		if rr == nil || rr.PubAck == nil {
			return rr, fmt.Errorf("m-accept-split-write: the record write was not acknowledged")
		}
		if aerr := rr.ToError(); aerr != nil {
			return rr, aerr
		}
		resp = rr
	}
	// Presented as an acknowledgement of the batch, so the acceptance-batch validation
	// cannot tell two writes from one commit.
	resp.BatchId, resp.BatchSize, resp.Stream = batchID, mqttAcceptBatchSize, mqttSessStreamName
	return resp, nil
}

// MUTATION ANCHOR: test/depproof/run.sh inserts the m-accept-split-write helper here'
  fi

  log="$work/log-accept-${mutate}.txt"
  ( cd "$dest" && go test -tags mqttport_fulltests -count=1 -run "$accept_test" \
      ./internal/mqtt/ -v ) > "$log" 2>&1 && outcome=PASS || outcome=FAIL

  if [ "$outcome" = FAIL ]; then
    if grep -qE '\[build failed\]|^# ' "$log"; then
      outcome=BROKEN
    elif ! grep -qE '^\s*--- FAIL: TestMQTTAcceptBatchReplicatedIndivisible' "$log"; then
      outcome=OFFTARGET
    elif [ "$mutate" = mutate ] && ! grep -qF 'ACCEPT-BATCH-TORN' "$log"; then
      # The kill has to be the TORN PAIR, not a refused CONNECT and not a broken
      # cluster. Both of those fail the sentinel while saying nothing about whether
      # the acceptance fact can outlive its session record.
      outcome=OFFTARGET
    fi
  elif ! grep -qE '^\s*--- PASS: TestMQTTAcceptBatchReplicatedIndivisible' "$log"; then
    outcome=BROKEN
  elif grep -qF 'ACCEPT-BATCH-TORN' "$log"; then
    # A clean row that printed the marker at all is not clean, whatever go test made
    # of it: the marker is only ever written by an assertion that observed the tear.
    outcome=OFFTARGET
  fi
  rm -rf "$dest"
  echo "$outcome accept-sentinel ${mutate} -> $log"
}

accept_matrix() {
  local failures=0 line want mode row
  for row in "PASS clean" "FAIL mutate"; do
    # shellcheck disable=SC2086
    set -- $row
    want=$1
    mode=$2
    line=$(accept_run "$mode") || exit 2
    echo "$line"
    case $line in
      "$want "*) ;;
      *) echo "  ^ expected $want"; failures=$((failures + 1)) ;;
    esac
  done
  [ "$failures" -eq 0 ] || return 1
  echo "depproof: both accept-sentinel rows matched the evidence table"
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

    # The accepted-CONNECT batch: a committed prefix with no commit entry applies
    # nothing. The clean row forbids the stored marker as well as requiring the
    # committed-and-processed one, so it cannot be satisfied by a run in which the prefix never
    # arrived. The fixture is named by the ROW rather than chosen from the version,
    # which keeps it off the control run without any version branching.
    "PASS $pinned ^TestJetStreamAcceptBatchPrefixStoresNothing +acceptbatch_partial_test.go @ACCEPT-BATCH-PREFIX-BUFFERED !ACCEPT-BATCH-PREFIX-STORED"
    "FAIL $pinned ^TestJetStreamAcceptBatchPrefixStoresNothing +acceptbatch_partial_test.go @ACCEPT-BATCH-PREFIX-STORED m-batch-apply-eager"

    # The dependency's OWN batch contract, unmutated. Nothing here asserts it, and a
    # bump that broke batch atomicity outright would otherwise leave the rows above
    # measuring a mechanism that no longer exists -- which is the canary's stated job.
    "PASS $pinned ^TestJetStreamAtomicBatchPublish"
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

  # The bridge sentinels and their own mutations, scored the same way.
  bridge_matrix || failures=$((failures + 1))
  accept_matrix || failures=$((failures + 1))
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
    --bridge) rc=0; bridge_matrix || rc=1; accept_matrix || rc=1; exit $rc ;;
    --version) version=${2:?}; shift 2 ;;
    --mutation) mutations+=("${2:?}"); shift 2 ;;
    --require) mutations+=("@${2:?}"); shift 2 ;;
    --forbid) mutations+=("!${2:?}"); shift 2 ;;
    --fixture) mutations+=("+${2:?}"); shift 2 ;;
    --run) tests=${2:?}; shift 2 ;;
    -h | --help) sed -n '2,20p' "$0"; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done
one_run "$version" "$tests" "${mutations[@]+"${mutations[@]}"}"
