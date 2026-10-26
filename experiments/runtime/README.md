# Runtime scheduling fixtures

The bubble fixtures check decision recording, prefix following, FIFO and
non-default choices, hook state, logical time and repeated trace comparisons.
Each active bubble has one owned P and at least one spare host P. Comparisons
at GOMAXPROCS 2, 4 and 8 resize only between completed runs; they do not test
dynamic resizing of an active bubble. No fixture disables GC to obtain a pass.

`TestReplayDivergenceFails` deliberately creates an impossible prefix in a fresh
subprocess. The child requires a replay-divergence cause and `WorkerUnsafe`, then
exits immediately with a specific nonzero sentinel status. It performs no reset
or further workload after unsafe recovery. The parent checks both that status
and the sentinel text.

The older `manual_race_test.go` and `race_test.go` demonstrations use ordinary
host goroutines outside bubbles. They retain their one-P workloads and restore
the previous processor count afterward. Their logging and hand-selected
schedules are separate from bubble replay and GC qualification.

Run with the sealed toolchain and a new saved directory from the repository root:

```sh
make test PKG=experiments/runtime TEST_PATTERN=. RUN_DIR="$PWD/runs/runtime-001" \
  GO_ROOT=/sealed/go GO_BIN=/sealed/go/bin/go WATCHDOG=60
```

The external process-group watchdog bounds the whole package and its child.
These finite checks preserve their trace/outcome assertions. A passing package
does not establish determinism for every Go program or collector timing, and
does not show that all operation-level interleavings are represented.
