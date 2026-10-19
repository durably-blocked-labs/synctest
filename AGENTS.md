# Working on Bubbles

Research on observable scheduling and bounded exploration in a modified Go runtime.
Read [README.md](README.md) for current setup, module boundaries and qualification limits.
Historical design/evaluation documents are not implementation proofs.

## Source and module boundaries

- `go/` is the existing `durably-blocked-labs/go` submodule. Use its gitlink pin,
  not a remote branch tip. Commit runtime changes there before updating the root
  gitlink; a root commit cannot capture dirty nested source.
- `explorer/`, `orchestrator/`, `rafttest/` and `campaign/` are maintained root
  packages. `rafttest/transport.go` is the current adapter.
- `experiments/runtime/` preserves runtime characterization tests.
- `experiments/raft/` exercises a real three-voter memory service in one bubble;
  it is not a per-node-bubble full-service integration.
- `experiments/etcd/` is a separate module running real loopback/file I/O.
  Controlled full-etcd integration is not implemented.
- `archive/legacy/` preserves historical source, data and figures. `bugs` and
  `old_charts` are compatibility symlinks. Do not rewrite archived bytes to make
  historical tests pass. Use the explicit legacy runners.

## Execution safety

Custom-runtime builds/tests require an external process-group watchdog and saved
command/status evidence. Go's internal test timeout is not the external watchdog.

- Set `GOROOT` explicitly and use `GOTOOLCHAIN=local`.
- Use at most four compiler jobs and a build watchdog of at most 600 seconds.
- Build first, copy a complete GOROOT including generated sources/tools, seal it,
  then smoke-test the copy. Never test a concurrently changing GOROOT.
- `scripts/build-go.sh` is a legacy helper, not the complete qualified procedure.
- Keep new run directories exclusive. Never overwrite historical evidence or
  automatically launch long campaigns.
- Treat `WorkerUnsafe` as process-terminal; reset does not establish cleanup.

Useful entrypoints, run under the required external watchdog:

```sh
make test-all GO_ROOT=/sealed/go GO_BIN=/sealed/go/bin/go
make test-raft GO_ROOT=/sealed/go GO_BIN=/sealed/go/bin/go
make test-etcd ETCD_PROFILE=custom-http2legacy GO_BIN=/sealed/go/bin/go
make test-legacy-regressions GO_BIN=/sealed/go/bin/go
make verify-archive
```

The named etcd compatibility profile retains full HTTP2. Do not silently disable
protocols, change dependency versions or add global GOFLAGS to hide failures.

## Model and evidence constraints

Bubbles share a process, runtime and heap; they are not machine/address-space
isolation. Delivery precedes logical-time advancement. Yield is Gosched and may
reselect the same actor; it does not establish fairness.

Distributed trials require one P per node plus a spare host P (minimum two).
Supported select control requires bubble-owned channels; host-owned/mixed cases
are unsupported. Schedule-only adapter replay does not establish select replay.

The full maintained graph is currently red: GC stack scanning can cooperatively
suspend/requeue an actor and change scheduling even with `asyncpreemptoff=1`.
Strict replay may reject such a trace. Do not disable GC, filter events or weaken
assertions to advertise determinism. The repair remains open.

Distinguish a bounded pass from universal safety, replay from state equivalence,
non-FIFO deviations from instruction preemptions, and heuristic reduction from
sound independence. A timeout/limit/tool failure is not automatically a target
bug. Do not claim formal verification, exhaustive coverage or long-run stability
without corresponding evidence.

Preserve unrelated staged/unstaged user work, backups and nested histories.
Stage explicit owned paths; do not use blanket staging or destructive cleanup.
