# Quorum Read-Repair Lost Sibling Bug

This package models a small Dynamo-style replicated key/value system and injects
a read-repair merge bug that needs both network and local goroutine scheduling to
surface.

The point of the bug is to exercise mixed interleavings:

- global decisions choose which network message is delivered next;
- local decisions choose which goroutine inside a replica runs next;
- the failing run needs a particular global message order and a particular local
  goroutine order on `R1`;
- the scenario is deterministic and does not rely on sleeps or wall-clock timing.

## System Model

The scenario has three replicas and three client nodes:

- `R1`, `R2`, `R3`: replicas storing sibling sets of vector-clocked values.
- `C1`: writes `x=A`.
- `C2`: writes `x=B`.
- `Reader`: performs quorum reads and sends read-repair messages.

Each replica has multiple local goroutines:

- `router`: receives network messages and dispatches work.
- `applyLoop`: applies client writes from `applyCh`.
- `repairLoop`: applies read-repair writes from `repairCh`.
- the transport bridge goroutines used by the orchestrator.

The local goroutines matter because `router` marks a write as pending before the
write is actually applied by `applyLoop`. A repair can therefore race with a
locally queued write inside the same replica bubble.

## Correct Behavior

The store uses vector-clock siblings. Concurrent writes must both survive.

In this scenario:

1. `C1` writes `A`.
2. `C2` writes concurrent value `B`.
3. `Reader` performs quorum reads and repairs stale replicas.

Since `A` and `B` are concurrent, the correct final value for every observed
replica and the reader is:

```text
[A B]
```

The invariant checked by the tests and benchmarks is:

```text
every recorded observer must see exactly the sibling set [A B]
```

Any observed singleton value such as `[A]` or `[B]`, or any divergent replica
state, is a semantic bug.

## Injected Bug

The bug is in `Replica.repairLoop` in `store.go`.

When a repair arrives while `pendingApply > 0`, the replica calls
`buggyRepairMerge` instead of the normal `mergeSiblings`:

```go
if r.pendingApply > 0 {
    r.store[req.msg.Key] = buggyRepairMerge(r.store[req.msg.Key], req.msg.Values)
} else {
    r.store[req.msg.Key] = mergeSiblings(r.store[req.msg.Key], req.msg.Values)
}
```

`mergeSiblings` correctly retains all non-dominated vector-clock siblings.
`buggyRepairMerge` first computes the correct sibling set, but if more than one
sibling remains it chooses one winner by clock score and discards the rest.

That is intentionally wrong: whether a local write is pending should not change
the conflict-resolution semantics of read repair. A pending local write is only
an implementation detail of the replica pipeline.

## Required Interleaving

The full quorum scenario is designed so the bug needs more than a single simple
branch choice.

At the global level, the run must create a stale/late `R1` path:

1. `C1` writes `A` to the cluster.
2. `C2` writes `B` to `R2` and `R3`.
3. `Reader` reads from a quorum that includes stale `R1` and up-to-date `R2`.
4. `Reader` sends a repair to `R1`.
5. `C2`'s late `B` write to `R1` is delivered so it is locally pending when the
   repair is processed.

At the local level, `R1` must then run its local goroutines in the exposing
order. The router has to mark the late put as pending, and the repair path has
to observe that pending write before the normal apply path removes it.

This is why `TestQuorumReadRepair_ExploreGlobalOnly` is expected not to find
the full bug: global message reordering alone is insufficient. The failing trace
needs both non-default global choices and non-default local choices inside `R1`.

`TestQuorumReadRepair_FindBug` is the targeted witness. It steers the global
messages into the stale-read/late-write shape, then chooses a non-default local
alternative on `R1`. The test asserts that the observed failing trace contains
both non-default global and local decisions.

## Tests

Useful tests in this package:

- `TestQuorumReadRepair_FIFOPasses`: the default FIFO run preserves both
  siblings.
- `TestQuorumReadRepair_ExploreGlobalOnly`: global-only exploration should not
  expose the mixed local/global bug.
- `TestQuorumReadRepair_FindBug`: targeted full-scenario witness for the bug.
- `TestBenchmarkOutcomeIsolatesStaleRecorders`: guards against stale benchmark
  recorders polluting later runs.
- `TestCompleteQuorumBugTraceRejectsEmptyTrace`: ensures an empty trace cannot
  be counted as a benchmark bug.
- `TestCompleteQuorumBugTraceAcceptsQuorumTrace`: ensures the trace gate accepts
  a complete quorum trace involving `R1` put/get/repair traffic.

Run the package tests with the custom Go toolchain:

```bash
GODEBUG=asyncpreemptoff=1 ./go/bin/go test -count=1 -v ./bugs/quorum-read-repair
```

## Benchmarking

The benchmark harness lives in `bench_test.go` and is run through the repository
chart target:

```bash
make benchmark-charts pkg=bugs/quorum-read-repair ATTEMPTS=3 BENCH_TIMEOUT=60s
```

The target runs every `TestBench_*` test and writes package-local benchmark data
under:

```text
bugs/quorum-read-repair/benchmarking/data/
```

It then renders charts under:

```text
bugs/quorum-read-repair/benchmarking/figures/
```

The benchmark policies are:

- `chess-global`: CHESS over global decisions only.
- `chess-gl`: CHESS over global and local decisions.
- `pct-d2`: PCT with depth 2.
- `pct-d3`: PCT with depth 3.
- `random`: random exploration.
- `targeted`: one directed witness run, used as a sanity check that the
  scenario and instrumentation can still produce the bug.

The benchmark uses the full quorum scenario, not the reduced focused race. Each
exploration run gets its own recorder through `benchmarkOutcome.beginRun`.
That isolation is important because clients and observers from one run must not
be able to mark a later run as failed.

A benchmark run is counted as a found bug only when both conditions hold:

1. the per-run recorder observes a semantic invariant violation; and
2. `completeQuorumBugTrace` sees the required `R1` traffic in the trace:
   a `Put`, a `Get`, and a `Repair`.

This gate prevents chart artifacts such as an empty trace with
`user_failed=true`, or a stale side-channel failure that did not come from the
full quorum read-repair scenario.

## Chart Interpretation

The main figures are:

- `summary_table.png`: one-row-per-policy summary of found bugs and first bug
  runs.
- `runs_to_bug.png`: how quickly each policy found the first complete semantic
  bug.
- `seed_runs_to_bug.png`: a strip plot for repeated benchmark attempts where
  each dot is one PCT/Random seed, with medians, worst observed attempts, and a
  CHESS reference line when CHESS finds the bug.
- `bug_trace_decision_mix.png`: global, local, and non-FIFO decision counts in
  the first bug trace for each policy that found one.
- `nonfifo_comparison.png` and `nonfifo_over_runs.png`: how much non-default
  scheduling each policy used.
- `cumulative_unique_traces.png`: trace diversity over the run budget.
- `swimlane_*.png`, `sequence_*.png`, `detailed_*.png`, and `narrative_*.txt`:
  per-policy views of the first complete bug trace.

If CHESS global-only reports no bug, that is expected for this scenario. It does
not explore the local `R1` goroutine interleaving required by the defect. If a
policy reports bug finds in the summary table, then `bug_trace_decision_mix`
should show non-zero decisions for that policy because the benchmark only marks
complete traced semantic bugs as `user_failed`.

## Audit Notes

The benchmark used to be vulnerable to inconsistent charts because bug detection
could be driven by package-global outcome state and a reduced focused scenario.
That allowed stale or incomplete observations to appear as benchmark failures,
including failures with empty or misleading decision traces.

The current benchmark avoids that by:

- using the full quorum scenario in `runBenchmark`;
- allocating a fresh per-run recorder for each exploration run;
- converting a run to `user_failed=true` only inside the benchmark observer;
- requiring the trace to include the quorum-relevant `R1` put/get/repair
  sequence before charts can treat it as a found bug.

Those checks are what make the generated charts meaningful: the rows, traces,
and decision-mix plots are all derived from the same complete run that actually
violated the sibling-preservation invariant.
