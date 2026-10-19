# Deterministic scheduling research for Go

Research on observable scheduling and bounded interleaving exploration in Go
`testing/synctest` bubbles. A bubble shares its process/runtime/heap; it is not a
machine or process-isolation boundary. Characterization and bounded exploration
are not claims of exhaustive correctness or production readiness.

CPSC 448 Directed Studies — Shubhaankar Sharma; supervisors Arpan Gujarati and
Ivan Beschastnikh. Original research documents, papers and reports are retained.

## Layout and test boundaries

- `go/`: pinned modified Go runtime (nested repository).
- `explorer/`, `orchestrator/`, `rafttest/`: maintained exploration, coordination
  and public HashiCorp transport adapter in the root Go module.
- `campaign/`: checked trial artifacts and bounded, process-isolated campaign
  workers. Parallelism is across independent trials, not within a trial.
- `experiments/runtime/`: byte-preserved runtime characterization (including
  existing scheduling/race demonstrations), not discarded experiments.
- `experiments/raft/`: fresh real three-voter memory service in one bubble/clock,
  with Apply/futures/FSM journal and independent tiny-history oracle. Distributed
  per-node full-service integration remains pending. Separate two-node adapter
  RPC and schedule-only replay tests pass; these are not a three-node Raft service.
- `experiments/etcd/`: separate dependency module; full embedded 3.6.15 client API
  baseline with real loopback/files and clean follower stop/restart. **Not a
  deterministic Bubbles simulation.** Controlled integration is explicitly
  unsupported until its separate capability gates are met.
- `archive/legacy/`: whole historical workload+data+figure units and factual hash
  inventory. `bugs` and `old_charts` are compatibility symlinks. The bugs module
  is opt-in; quorum store/client/transport/trace/seed regressions remain maintained
  through an explicit runner. Historical `rafttest` tests, BUGS.md and original
  transport are also preserved as a unit; its separate module has a known pinned-
  Raft compile incompatibility, not a passing historical reproduction. Root
  `rafttest/transport.go` remains the maintained adapter. See
  [archive boundaries](archive/legacy/README.md).
- `raft/`, `etcd/`: pinned public dependency forks, not workload directories.
- `docs/`, `papers/`, `charts/`, reports: preserved research and evidence.

## Setup (pinned sources)

```sh
git clone --recurse-submodules <repo-url>
cd synctest
# For an existing clone, initialize gitlink pins; NEVER use update --remote:
git submodule update --init --recursive
python3 scripts/verify-dependencies.py
# Build only with an explicit bootstrap, at most four compiler jobs, and an
# external process-group watchdog (at most 600 seconds). Save command/status.
# Before testing: copy a complete GOROOT, seal it, and smoke-test that copy.
make go-version GO_ROOT=/immutable/go GO_BIN=/immutable/go/bin/go
```

Runtime read access currently uses SSH (`durably-blocked-labs/go`); configure Git
credentials first. Public Raft/etcd forks use HTTPS. Raft remains at
`9e3b7155dd6f45b128d0679f22df32229319e81f`; selected maintained etcd v3.6.15 is
`a3346427fbe6e456e9b6d6e429346481c6be7c71`, not a claim of latest release.
`experiments/dependencies.json` records fork/upstream/module identities. Local
replacements use those source checkouts, not a mutable branch. Runtime source
and executable identities must be recorded before interpreting new results;
never run a concurrently rebuilding GOROOT. The legacy `build-go` helper alone
provides neither the required external watchdog nor an immutable checkpoint.
The Go gitlink identifies the runtime source, not a prebuilt qualified toolchain.
Record the complete source and executable hashes for the sealed build used by
each experiment.
Root Go tests require this custom toolchain, not ordinary Go. The separate etcd
external baseline also supports stock Go >=1.26 with `ETCD_PROFILE=stock` and
explicit `GO_ROOT`/`GO_BIN`. Custom older1.27-dev net/http lacks the wrapping
interface expected by pinned x/net: untagged execution currently panics. The
named `custom-http2legacy` profile uses upstream x/net's `http2legacy` build tag
(the full HTTP2/gRPC implementation remains enabled); no dependency is downgraded
and no global GOFLAGS is silently added. This is still external I/O, not control.

```sh
make test-all                  # Explicit maintained root package graph
make test-det                  # Runtime characterization, async preemption off
make test-raft                 # Real memory service + independent oracle
make test-etcd ETCD_PROFILE=custom-http2legacy # Explicit compatibility profile, external I/O
make test-legacy-regressions   # Protected quorum characterization, no bug search
# Explicit historical fixture; known Config.SkipTermCheck compile blocker:
make test-legacy-raft TEST_PATTERN=TestStaleTermBug
make test-entrypoints          # TEMP-only safe-runner sentinel fixtures
make verify-archive            # Byte-level preserved inventory
# Override both toolchain locations for an immutable checkpoint:
make test-raft GO_ROOT=/immutable/go GO_BIN=/immutable/go/bin/go
```

`go test ./...` does not cross nested module boundaries. `scripts/run-tests.sh`
is the explicit module runner; it does not silently execute historical bug
searches. Named legacy tests require opt-in package and pattern. Runtime
characterization includes old race demonstrations; success does not prove all
select/preemption behavior is controlled. CHESS bounds non-FIFO choices, not
arbitrary instruction preemptions. The legacy etcd7492 reduction's reproduction
validity remains unresolved/invalid; preserved data is not re-certified.

## Current qualification limits

The maintained explorer, orchestrator, adapter, campaign and fresh Raft packages
pass bounded tests. **The full maintained graph is not green:** the preserved
`TestPhase0_CrossGOMAXPROCS_Determinism/4to4_100x` characterization fails.
Concurrent GC can cooperatively suspend a bubble goroutine and later requeue it,
changing runnable sets even with `asyncpreemptoff=1`. Actual strict replay can
also reject a recorded trace under default GC. It fails closed; this is not a
successful replay or evidence of a different completed application state.
Disabling GC was a diagnostic comparison, not a fix or supported default.

Distributed orchestration needs at least one P per node plus a spare host P
(minimum two). Insufficient `GOMAXPROCS` is rejected before target startup; it is
not silently increased. Bubble-owned select replay is tested separately from
schedule-only adapter replay. Host-owned channel selects remain unsupported.
Peer closure after RPC publication stops the trial as infrastructure failure;
it is not modeled as an ordinary RPC response. Resource limits can make teardown
unsafe: a worker marked `WorkerUnsafe` must be disposed of, never reset or reused.
The local unified replay helper preserves exact failure causes but currently
labels non-divergence causes as panic; resource/infrastructure outcome labels
are not yet aligned with exploration. Inspect the cause, not only the label.

Run custom-toolchain commands with an external process-group watchdog and saved
command/status JSON. Go's internal test timeout alone is not that watchdog.
Use a complete, sealed, smoke-tested GOROOT with explicit `GOROOT` and
`GOTOOLCHAIN=local`. Passing a narrow test does not establish universal runtime
safety, successful replay under GC, or sound partial-order reduction.

## Safe bounded runs and charts

```sh
make benchmark-charts pkg=bugs/ra-gate RUN_DIR="$PWD/runs/ra-gate-001"
make charts-collect PKG=experiments/raft TEST=TestThreeVoterMemoryServiceSingleBubble \
  RUN_DIR="$PWD/runs/election-001" WATCHDOG=60
# Plotting is separate and never installs dependencies automatically:
make charts-plot OUTDIR=/path/to/selected/data FIGURE_DIR=/new/exclusive/figures
```

Every collection requires a **new, exclusive run directory**. Existing paths are
refused, including interrupted attempts. No Make target implicitly deletes
benchmark evidence, replaces prior outputs, or installs Python packages.
Per-attempt test/data directories, raw output and `status.json` remain readable
after failure. Discovery failure, zero tests, nonzero unclassified test failure,
infrastructure timeout and interruption are distinct outcomes; a failing Go test
is never automatically labeled an expected target bug. A host watchdog is an
infrastructure limit, not target logical-time semantics. Plot only selected runs;
never combine historical and new campaign identities implicitly.

The safe runner does not establish strict replay, complete resource containment,
or sound long-running campaigns. Those are separate maintained lifecycle/replay
qualification gates. No 12-hour run is claimed here.
