# Preserved legacy workloads and evidence

The `bugs/`, `old_charts/` and historical `rafttest/` units were preserved intact from root commit
`2de6f11c05c624386048f3d64f44623cd6ab0544`. `inventory.json` records each
original path, current path, size and SHA-256. It also records the byte-identical
runtime characterization relocation to `experiments/runtime/` (not deprecated).
Run `python3 scripts/verify-archive.py` from the repository root to check bytes.

- `bugs/gobench`: 14 reduced historical kernels, their shared benchmark and data.
- `bugs/ra-gate`: source, tests, data and figures as one unit.
- `bugs/quorum-read-repair`: source, tests, data and figures as one unit.
  Its store/client/transport/trace/seed/budget regressions remain maintained via
  `make test-legacy-regressions`; archive placement does **not** retire those tests.
- `old_charts`: historical plotting code and requirements, preserved together.
- `rafttest`: original `transport.go`, `BUGS.md` and both historical test files.
  Source bytes are identical to the recorded root commit. Historical tests and
  BUGS.md moved out of root discovery; root `rafttest/transport.go` remains in
  place, maintained separately (the archive copy came from original HEAD).

Root `bugs` and `old_charts` are compatibility symlinks so existing chart/data
consumers still find the same bytes. `bugs/go.mod` explicitly retains module
identity `github.com/shubhaankar/synctest/bugs`; unchanged historical imports
resolve inside that module. Its root-library and Raft replacements refer back
to the maintained repository and pinned local fork. This added module metadata
is not part of the original byte inventory. Root `go test ./...` intentionally
excludes this nested module; use the explicit runners below, not silent coverage.

```sh
make test-legacy-regressions
GO_BIN=/immutable/toolchain/bin/go sh scripts/run-tests.sh legacy ./gobench/etcd7492 '^TestName$'
# Explicit benchmark opt-in, NEW output directory, no deletion or overwrite:
make benchmark-charts pkg=bugs/ra-gate RUN_DIR="$PWD/runs/ra-gate-001"
```

No historical data is re-certified by relocating it. In particular **etcd7492
reproduction validity remains unresolved/invalid**: the preserved reduction has
channel capacity one, two assigners and a ticker select; existing names/metrics
are not evidence that it reproduces the original upstream defect. It is neither
the fresh full-etcd integration nor a reason to strengthen a detection claim.
Other historical comments and reports likewise retain their original claims,
not new correctness/reproduction endorsements.

Consumers retained by compatibility paths include root charts Python programs,
historical generated documentation and package-local source comments. Reports,
dirty root charts, papers and maintained root `rafttest/` adapter were not moved.

## Historical Raft fixture: preserved, currently incompatible

Original context was module `github.com/shubhaankar/synctest`, package directory
`rafttest/`. External tests import `github.com/shubhaankar/synctest/rafttest` and
`github.com/shubhaankar/synctest/orchestrator`. The archive's explicit module is
`github.com/shubhaankar/synctest/archive/legacy/rafttest`; this distinct identity
avoids colliding with the maintained root adapter module path. **Those external
historical tests still import the CURRENT root adapter/orchestrator.** The local
original transport is source preservation, NOT proof it is the implementation
under those external tests. Module metadata is added, outside the byte inventory.

Raft remains pinned to `9e3b7155dd6f45b128d0679f22df32229319e81f`. Historical
`bug_demo_test.go` uses `Config.SkipTermCheck`, absent from that real dependency;
even selecting another test cannot bypass package-wide compilation. This is a
known compile blocker, not an expected target failure, successful bug reproduction
or reason to patch the source/dependency. A faithful historical restoration needs
a matching historical checkout and its dependency context, not silent import edits.
The inventory records this module/import boundary and pin.

```sh
# Explicit opt-in only; currently reports the compile incompatibility:
make test-legacy-raft TEST_PATTERN=TestStaleTermBug
# The safe runner can also capture it in a NEW exclusive evidence directory:
make test pkg=archive/legacy/rafttest TEST_PATTERN=TestStaleTermBug RUN_DIR="$PWD/runs/legacy-raft-001"
```

Protected quorum regressions remain independently runnable via
`make test-legacy-regressions`; they do not require compiling this fixture.
