# Working in this repository

An MQTT 3.1.1 / 5.0 broker over NATS JetStream. The protocols that carry
session, QoS 2, Will and in-flight state across brokers live in per-feature
files under `internal/mqtt/` (`qos2_move.go`, `qos2_shared_affine.go`,
`sess_incarnation.go`, `sess_teardown.go`, `unsub_drain.go`,
`inflight_register.go`, `orphan_qos2_sweep.go`).

## The docs are load-bearing

- `docs/ROADMAP.md` is the authoritative todo. Its bullets carry the finding
  and the prior analysis, not just a title. Section 0 is release blockers.
- `docs/ARCHITECTURE.md` documents the mechanism. "JetStream behaviors this
  design depends on" is the section to read before anything touching streams
  or consumers.
- `docs/CONFORMANCE.md` records stated residuals. A residual documented there
  is a constraint on a fix, not a free target.
- `docs/ARCHITECTURE.md`'s "Compatibility boundary" is **normative for every
  plan**: no migration, legacy-format or rolling-upgrade machinery, no residual
  conditioned on a rollout, and an unsupported persisted shape is **refused**
  rather than migrated (F-056, F-057, F-058). There are no releases and no
  pushes, so a mixed fleet is a population that does not exist; reasoning about
  one costs review rounds and invites preserving behavior for a broker that will
  never run.

Any change to a mechanism updates `docs/ARCHITECTURE.md`; any behavior gap it
leaves updates `docs/CONFORMANCE.md`. Both in the same change, not later.

## An upstream bug belongs upstream

Several §0 findings are defects in `nats-server`, not in this broker. **An
upstream bug is an issue upstream, not an item we carry.** JetStream is the
data layer: a store that misreports its own contents — a corrupt record
silently skipped, a failed read answered as "no message" — is never this
broker's defect, and the broker does not compensate for it. Once the root cause
is in the dependency, this repository's deliverable is not a fix and not a
workaround:

1. **Verify it against the pinned version**, reading each function from its
   entry rather than taking a line number on report, and reproduce it with a
   real-server witness. A source-reading claim is the start of the work, not
   the end of it — F-108 and F-110 got theirs on 2026-09-21, after two runs had
   rested on source reading alone. **Where the witness lives is the plan's
   decision, not this rule's**: a defect in the dependency's
   store is a dependency-level test, and the form its maintainers can act on
   fastest may be a test written against their tree and attached to the issue,
   not one carried here.
2. **Check it is still a bug** before honoring it as a blocker: is the pin
   behind, is it corrected on the dependency's `main`, is it already filed. A
   finding that upstream has fixed is not a blocker, it is a pin bump.
3. **File it upstream**, with the minimal reproduction and the source locations.
   Filing is outward-facing, so the report text gets the user's explicit
   approval before it is posted — the direction does not need re-asking, the
   wording does. Prefer a comment on an existing issue over a duplicate.
4. **Record the issue in the roadmap bullet** and leave the item blocked on the
   correction. `docs/findings/` gets the reference.

**Never fork, never `replace`, and never patch around it.** The module pin is
evidence about the tested contract, and changing a client build's dependency
does not correct an independently deployed server. Where the broker must fail
closed until the correction lands, that is a stated residual in
`docs/CONFORMANCE.md` describing a gap — never a fix, and never a reason to
close the finding.

**Do not route around the blocker.** Selecting an easier item because this one
is hard leaves the defect exactly where it was and burns a run rediscovering
that it is blocked. Verifying and reporting is the available work inside an
item recorded as blocked; moving elsewhere is the user's explicit call.

## Code and tests

Name things after the protocol fact they encode, and comment the invariant
rather than the mechanics. That is the existing style; match it.

Never loosen a test assertion to make a change pass. If a test fails, the
default hypothesis is that the broker is wrong. Trace the root cause in
the protocol core (`mqtt.go` and the files split from it, listed in
`internal/mqtt/doc.go`) / `shim_engine.go` rather than adjusting what the test demands.

A test that passes against deliberately broken code proves nothing. For
anything cluster- or race-shaped, name the mutations up front, apply each one,
confirm the paired test fails, and revert it. Report which mutations were run.
`mqttport_clustertests` passing is not evidence for a clustered claim; a test
that fails under mutation is.

## Verification

```bash
make test
make vet
make lint          # Go and Markdown; markdownlint covers **/*.md
make full-suite    # ported upstream MQTT suite against the standalone broker
```

For anything touching sessions, QoS 2, Wills, or cluster handoff, also:

```bash
make cluster-failover-tests
```

If the area has flaked before, `make soak` runs the paired baseline soak
against a clean tree.

## Codex reviews

Codex setup and the stop-time review gate are one-time and already done here;
do not re-run `/codex:setup` unless a call reports the CLI missing or
unauthenticated, in which case stop and ask the user to run it rather than
carrying on with reviews skipped.

The companion's install path is version-pinned, so resolve it at call time.
From the repository root:

```bash
node "$(node -e 'const p=require(process.env.HOME+"/.claude/plugins/installed_plugins.json");console.log(p.plugins["codex@openai-codex"][0].installPath)')/scripts/codex-companion.mjs" <subcommand> ...
```

`<plugin-root>` below means the path that inner `node -e` prints.

`/codex:rescue`, `/codex:review`, `/codex:adversarial-review`, `/codex:status`
and `/codex:result` are slash commands or `disable-model-invocation`; only the
user can type them. The companion script is the same runtime, and for a plan
review inside plan mode the `codex:codex-rescue` subagent is the route, because
a subagent's `Bash` call is not subject to plan mode while this thread's is.

### Retrieving a Codex result without fooling yourself

These rules are the residue of runs that reported a clean review that had never
happened. They apply to every Codex call, not just one workflow.

- **Never wait in the foreground.** `status --wait` blocks its own process for
  up to `--timeout-ms` (default 240000) while the `Bash` tool's ceiling is
  600000 ms and its default is 120 s, so a foreground wait is killed by the
  harness first. Background the launch with `Bash(run_in_background: true)` and
  read its output file when the harness re-invokes you.
- **One output file per round, never reused**, e.g.
  `<scratchpad>/codex-review-round-<N>.log`. Reading a round-1 log after
  launching round 2 is the stale-result failure, and an overwritten name makes
  the two indistinguishable.
- **`status --wait` exits 0 whether or not the job finished.** A timeout is
  reported in the payload as `waitTimedOut: true` with `job.status` still
  `queued` or `running`. Sequence follow-up commands with `;`, never `&&`, and
  never read an exit code as a verdict.
- **Terminal is not the same as reviewed.** Jobs end `completed`, `failed` or
  `cancelled` (`lib/job-control.mjs:172,262`), and any nonzero Codex exit sets
  `failed` (`lib/tracked-jobs.mjs:156`). Only `completed` produces a review; a
  cancelled or failed job's output is an error tail or a partial transcript.
- **The rendered payload is never empty, so its length proves nothing.** The
  renderer substitutes prose for a missing result: `Codex did not return a
  final message.` and `No captured result payload was stored for this job.`
  (`lib/render.mjs:314-322,390-444`), and for reviews `Codex did not return
  valid structured JSON.` / `Codex returned JSON with an unexpected review
  shape.` (`lib/render.mjs:211-240`). Any of those means no review happened.
  A properly rendered review that lists no findings is the opposite: a
  legitimate clean round.
- **Check the machine-readable field.** For a `task` job, `result <job-id>
  --json` and require `storedJob.result.rawOutput` (or
  `storedJob.result.codex.stdout`) to be a non-empty string. For a review job,
  add `--json` to the launch itself; there is no trustworthy way to name a
  review job after it finishes, so do not plan on a second `result` fetch.
- **Identify a job only from `running[]`** in `status --json`, and only when
  you need mid-flight progress. `recent[]` and `latestFinished` accumulate jobs
  of every class — the stop-time gate spawns one at each turn boundary — so a
  recency-based pick returns a previous round's job. A finished job is never in
  `running[]`, which is what makes that field the safe one.
- **`--background` is not honored for review jobs.** `handleReviewCommand`
  parses the flag and always runs `runForegroundCommand`
  (`codex-companion.mjs:729-751`), which is why the payload lands in the launch's
  own log and why its exit status is meaningful: nonzero means the review job
  failed (`codex-companion.mjs:658-668`). For `task` jobs `--background` is real
  and returns a receipt carrying the job id.
- A review whose output you never read has not reviewed anything, and neither
  has one that never ran. Silence is never agreement.

### A holistic review before any commit

Before suggesting a commit, a Codex **holistic review of the whole change**
must have been run and read — not per-file or incremental rounds, and not the
turn-boundary stop gate:

```bash
node "<plugin-root>/scripts/codex-companion.mjs" adversarial-review --scope working-tree <focus>
# or, once the work is committed:
node "<plugin-root>/scripts/codex-companion.mjs" adversarial-review --scope branch --base main <focus>
```

Focus text is positional and goes after the flags; an unrecognized `--flag` is
swallowed into the focus text rather than rejected, so keep to `--base`,
`--scope`, `--model`, `--background`, `--json`.

The review is holistic in the sense of `feedback_delete_redundant_mechanisms`:
one pass over the assembled change against a pre-enumerated scenario matrix,
with a stopping rule naming the release-blocking invariants — not another lap
of the incremental cadence that keeps finding one seam per round.

Assemble the whole change first, documentation included. The doc updates above
are part of the change, so they belong in the tree before the review, not after
it — a review that never saw the `docs/ARCHITECTURE.md` section or the roadmap
bullet cannot catch one that claims more than the code delivers.

**Any working-tree edit after the review invalidates it**, including a doc
touch-up, a checkbox, or a re-format, and the review is re-run before the
commit is proposed. Files outside the repository — the memory directory — do
not invalidate it.

Recollection is not evidence for that, so fingerprint the tree instead of
remembering it. Take the fingerprint **immediately before launching the
review**, and again immediately before proposing the commit:

```bash
GIT_INDEX_FILE=<scratchpad>/review-tree.idx sh -c \
  'git read-tree HEAD && git add -A && git write-tree'
```

That builds a throwaway index — the real one is untouched — and prints a tree
OID covering tracked edits, staged content, and untracked non-ignored files,
which is the same span `--scope working-tree` reviews. Write it next to the
round's log, e.g. `<scratchpad>/codex-review-round-<N>.tree`.

Identical OIDs mean the reviewed tree is the tree being committed. Different
OIDs mean it is not: re-run the verification and the review under the next
round number. Do not eyeball a `git status` and decide the delta looked
harmless — the two failure modes this catches, a forgotten doc edit and a
reformat, both look harmless. The user can waive a re-review; you cannot waive
it on their behalf, and if they do, say in the commit proposal which OID was
actually reviewed.

Every finding gets one of two outcomes before the commit is proposed: fixed, or
refuted with the code, the spec, or `docs/ARCHITECTURE.md` behind the
refutation. Nothing is dropped silently.

## Commits

Ask for approval before every commit, every time — including when the work is
finished, the suite is green and every review round is clean. Approval for one
commit does not carry to the next. Show the intended message and the file list,
and wait for an explicit yes. The same applies to `git push`, tags, and
anything else that leaves the working tree.

Commit a checkpoint before a large deletion or refactor. Untracked files have
no HEAD copy and a stray `git checkout` destroys them outright.

## Memory

Durable findings live in
`/home/nicob/.claude/projects/-home-nicob-repo-mqtt-nats-bridge/memory/`, with
`MEMORY.md` as the index. Several files encode invariants that reviews have
already caught being broken — generation identity over sequences, durable
arbitration over dedup windows, fence before mutation, naming over re-checking
before a late delete. Read the index before planning work in an area, and add
the item's own file when the work lands.
