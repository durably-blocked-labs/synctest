# Bubbles

Bubbles extends Go `testing/synctest` with local scheduling choices, coordinated
message delivery and logical time, bounded exploration, and trace replay.
Bubbles share a process, runtime and heap. The represented choices and configured
search bounds limit which executions a run explores.

## Source layout

- `explorer/`: local exploration and replay helpers.
- `orchestrator/`: multi-bubble coordination, global/local scheduling policies,
  delivery and replay.
- `rafttest/`: public HashiCorp Raft transport adapter.
- `campaign/`: checked trial artifacts and bounded workers for independent tasks.
- `experiments/runtime/`: runtime characterization and scheduling tests.
- `experiments/resourcegc/`: injected resource-lifecycle bug, one-shot workers,
  exact trace/application comparisons, and focused GC qualifications.
- `experiments/ragate/`: synthetic Ricart–Agrawala gate mechanism and protocol
  unit tests. A distributed Bubbles experiment adapter is not implemented here.
- `experiments/raft/`: real three-voter memory service in one bubble, with an
  independent oracle for small histories. Per-node-bubble service integration
  remains pending.
- `experiments/etcd/`: separate module for a full embedded etcd baseline using
  real loopback sockets and files. Controlled full-etcd integration is unsupported.
- `experiments/etcdkernels/`: preserved, reduced historical etcd bug kernels;
  the #6873 schedule search uses process-isolated workers and a stack oracle.
- `evaluations/`: partner BubbleGun campaign workers, synthetic RA scenario,
  and multi-bubble Raft integration scenarios.
- `go/`, `raft/`, `etcd/`: pinned dependency repositories; see
  `experiments/dependencies.json`.
- `scripts/`: dependency checks and bounded run helpers. New outputs belong in
  ignored, exclusive directories under `runs/`.

Papers and proposals live in the separate
[writing repository](https://github.com/durably-blocked-labs/bubbles-writing).

## Setup and commands

Initialize the committed dependency pins, without advancing their remote branches:

```sh
git submodule update --init --recursive
python3 scripts/verify-dependencies.py
```

The runtime repository currently requires SSH access. Root Go packages require
its modified toolchain; ordinary Go cannot run their scheduling APIs. The Go
gitlink identifies source, not a qualified executable. Build from an explicit
source and bootstrap into a new directory:

```sh
make build-go GO_SOURCE="$PWD/go" BOOTSTRAP=/bootstrap/go \
  TOOLCHAIN_DIR="$PWD/runs/toolchain-001" BUILD_JOBS=4 BUILD_WATCHDOG=600
```

The builder copies the source, builds that disposable copy, copies the complete
generated GOROOT, removes write permissions, and runs a basic synctest smoke test
using the sealed copy. A host process-group watchdog covers the whole workflow,
including copies and smoke, with a maximum of 600 seconds. Bootstrap concurrency
is serial or four jobs. Logs, statuses, source/file inventories and executable
hashes remain in the exclusive output directory on success or failure. Sealing
prevents accidental writes; the owner can change permissions. This smoke test
does not qualify every scheduler path or workload. Record the resulting hashes
with experiments and never test a changing GOROOT.

Set `GO_ROOT` and `GO_BIN` to that sealed toolchain. The generic `test` entrypoint
owns a new run directory, saves raw output and status, and applies a host
process-group watchdog:

```sh
make test PKG=experiments/raft TEST_PATTERN=TestThreeVoterMemoryServiceSingleBubble \
  RUN_DIR="$PWD/runs/raft-001" WATCHDOG=60 \
  GO_ROOT=/sealed/go GO_BIN=/sealed/go/bin/go
```

Existing run paths are refused. A nonzero Go test is an unclassified failure;
text announcing a bug does not classify it. Timeout, interruption, discovery
failure and zero executed tests remain distinct outcomes. A host watchdog is
an infrastructure bound, separate from target logical time.

For package groups, use the explicit module runner below **under an external
process-group watchdog with saved command/status evidence**. Their Go test
timeouts do not provide that external watchdog:

```sh
make test-all GO_ROOT=/sealed/go GO_BIN=/sealed/go/bin/go
make test-ragate GO_ROOT=/sealed/go GO_BIN=/sealed/go/bin/go
make test-raft GO_ROOT=/sealed/go GO_BIN=/sealed/go/bin/go
make test-etcd ETCD_PROFILE=custom-http2legacy \
  GO_ROOT=/sealed/go GO_BIN=/sealed/go/bin/go
make test-entrypoints  # Python fixtures only; no Go toolchain required
make bubblegun-build GO_BIN=/sealed/go/bin/go \
  BUBBLEGUN_BUILD_DIR="$PWD/runs/bubblegun-build-001" WATCHDOG=60
```

Resource-GC smoke uses the saved, externally bounded runner:

```sh
make test-resourcegc-smoke RUN_DIR="$PWD/runs/resourcegc-smoke-001" \
  GO_ROOT=/sealed/go GO_BIN=/sealed/go/bin/go WATCHDOG=60
make test-campaign-entrypoints  # campaign supervisor Python fixtures only
```

The smoke target checks application paths and observation overflow. It does not
launch the one-shot GC driver or continuation qualifications. Those require
separate fresh, bounded processes; see [resourcegc](experiments/resourcegc/README.md).
`test-entrypoints` includes builder, script and campaign supervisor fixtures.

`test-all` selects the maintained root packages and runtime, RA protocol and Raft
experiments. It does not include every new `evaluations/` scenario. The direct
multi-bubble Raft API checks pass, but its broader exploration currently has
intermittent incomplete runs from a replay queue-order mismatch and a separate
512-decision limit. Do not treat that search as qualified merely because an
individual run passes. `go test ./...` does not cross nested module boundaries. The etcd
baseline is opt-in: `stock` uses explicit stock Go >=1.26; `custom-http2legacy`
selects upstream x/net's full legacy HTTP2 implementation for the older modified
standard library. `custom-untagged` is a compatibility probe that may fail.
Profiles do not change dependency versions or silently set global `GOFLAGS`.
See [the etcd baseline](experiments/etcd/README.md) for its I/O and oracle limits.
For the reduced historical #6873 bug, see
[the kernel search](experiments/etcdkernels/README.md). Its bounded DFS result
concerns only the choices exposed by that small kernel.

## Qualification limits

The full maintained graph is not qualified as deterministic. Earlier GC
characterization exposed runtime pauses that changed application execution under
the same recorded schedule. The resource-GC fixture and runtime qualifications
exercise continuation, ownership and host-progress repairs with explicit scope.
Their bounded results do not establish determinism for every workload, collector
timing or processor count. Use the committed runtime pin that supplies the
experimental observation API; older pins cannot build these tests. Disabling GC
remains a diagnostic comparison, not a supported repair.

Scheduling is cooperative: an actor runs until it blocks, explicitly yields,
or finishes. Internal runtime preemption preserves that actor's turn. A CPU
spinner that never blocks or yields can starve peers. This differs from ordinary
Go scheduling and excludes some preemptive interleavings.

Use fixed topology: one P per bubble plus at least one ordinary host P. GC
requires that spare capacity; missing capacity fails closed. Dynamic bubble
capacity changes and GOMAXPROCS resizing during a run are unqualified.
Distributed orchestration requires one P per node plus a spare host P (minimum
two). Supported select control requires bubble-owned channels; host-owned or
mixed select cases remain unsupported. Message delivery precedes logical-time
advancement. Yield uses Gosched and may select the same actor again. A worker
marked `WorkerUnsafe` must be disposed of rather than reset and reused.

A bounded pass does not establish universal safety. Replay does not establish
state equivalence; non-FIFO choice bounds do not count instruction preemptions;
a heuristic reduction does not establish sound independence. These tools do not
claim exhaustive coverage or long-run stability.
