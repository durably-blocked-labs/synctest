# Evaluation Framework: orchestrator

This document describes how to evaluate the systematic concurrency testing framework we built – what to measure, why it matters, and how it compares to prior work.

---

## A. Bug Reproduction Effectiveness

The most direct measure of a testing tool's value.

### Metrics

- **Paths to first bug** — how many interleavings does the explorer examine before hitting a failing trace? Lower is more targeted.
- **Guaranteed coverage** — with DFS + context bound k, any bug of depth ≤ k is *always* found within a bounded number of runs. This might be harder to measure beause we don't know where bugs are until we find them.
- **Bug depth** — for each known bug, what is the minimum k needed to expose it? This characterizes where bugs sit in the interleaving space.

### Baselines to compare against

- **k=0 (single FIFO run)**: does the bug appear under pure sequential delivery? If not, at least k = 1 is required.
- **k=1, 2, 3 (FIFO with multiple branching points)**: What's the minimum k to expose the first bug?
- **Random baseline**: at each step, pick uniformly at random from the schedulable queue — run N times. Reports mean + stddev over multiple seeds. Unlike systematic DFS, this offers no guarantee of finding a bug within a fixed budget.
- **Context bounding literature & Systematically Picking Goroutines**: CHESS and PCT show most concurrency bugs manifest at k ≤ 2. How does non-FIFO compared to FIFO? 

### Expected result

Systematic DFS finds the bug in exactly `paths_to_bug` runs (deterministic, no variance). Random baseline requires far more runs on average and may miss the bug entirely within a fixed budget. The paper should show this gap clearly.

---

## B. Speed

### Metrics

- **Time to first bug** — wall-clock from `Explore()` start to the first failing trace. Captures both search efficiency (fewer paths explored) and per-run execution cost.
- **Time per run** — mean/median/p95 wall-clock time for a single trace execution. Reflects the cost of running a distributed protocol under synctest scheduling control.
- **Exploration throughput** — interleavings per second. Useful for estimating how far into the search space we can get in a fixed time budget.
- **Instrumentation overhead** — how much slower is an orchestrated run (with scheduling hooks active) vs. a plain `synctest.Test` run? Characterizes the cost of the infrastructure itself.

### Expected result

Systematic exploration should reach the bug faster in wall-clock time than random exploration, even if the random approach has lower per-run overhead — because it explores fewer paths. The overhead measurement justifies the approach's practicality.

---

## C. Coverage

### State Coverage

What fraction of reachable program states does the exploration visit? For distributed protocols, a "state" is a snapshot of all node states plus in-flight messages.

- Hard to enumerate exhaustively; instead report: number of *distinct global traces* (unique delivery sequences) observed, and how this count grows as k increases from 0 to 3.
- Does a sharp jump of distinct global traces from k=1 to k=2 signal anything?

### Line / Branch Coverage

Standard code coverage aggregated across all runs:
- Run all explored traces with `-coverprofile`, merge profiles.
- Report: line coverage % for the protocol-under-test.
- Shows whether systematic exploration reaches more code paths than a single run or random testing within the same run budget.

### Systematic Coverage (Decision Space)

The novel metric specific to this tool — what fraction of the *observable event ordering space* is exercised.

- **Decision coverage**: with DFS + bound k, all (step, alternative-index) pairs reachable within k injection points are exhaustively covered. This is a formal guarantee, not a statistical estimate.
- **Queue size distribution**: at each delivery decision, how many alternatives exist? This gives the branching factor and informs how fast the search space grows with k.
- **Bug discovery vs. search cost**: the key empirical question — how do *new bugs found* and *interleavings explored* each grow as k increases from 0 to 3? The interesting result is if bug discovery saturates at small k (all bugs found by k=2) while interleavings keep growing exponentially. That validates context bounding as practical: you get most of the value at k=2 with a fraction of the cost of exhaustive search.

---

## D. New Bug Discovery

### Approach

Apply the tool to:
1. **Injected faults** (controlled): deliberately broken implementations with known bugs at known k-depths. Validates that the tool finds what it should.
2. **Known protocol bugs** (existing): `TestRemoveLeaderBug` and `TestStaleTermBug` — show these are found deterministically rather than relying on lucky scheduling.
3. **Real systems**: apply to other Go distributed system and report real bugs.

### What makes a bug "new" for the paper

- Not exposed by a single test run (requires non-default scheduling)
- Not reliably found by random testing within a reasonable budget
- Found deterministically by systematic exploration at some bound k

---

## E. Usability / Integration Effort

A testing tool is only useful if people can actually wire it up to their system. This dimension evaluates how much work it takes to apply orchestrator to a new distributed system.

### Metrics

- **Lines of adapter code** — how many LOC does a user need to write to connect their system to the orchestrator? The core interface is just two methods (`Addr()` and `Outbox()`), but realistic adapters also need `StartBridge()`, `ExternalWait` calls, and response routing. Measure this for each case study and report it as a concrete number.
- **Invasiveness** — does the system under test need to be modified? Ideally zero modifications: the user only writes an adapter that wraps the existing transport layer. Report whether the original code is touched at all.
- **Conceptual surface area** — how many new concepts must the user understand to get started? (synctest bubbles, ExternalWait, NodeTransport, the outbox/Execute pattern). A simpler API lowers the barrier for adoption.
- **Time to first working test** — anecdotally, how long did it take to wire up each case study from scratch? This is hard to measure rigorously but can be reported qualitatively in the paper.

### Case Studies

The paper should apply the tool to at least 2–3 distinct systems to demonstrate generality, not just Raft:

- **Raft** (existing) — already done, serves as the primary case study
- **Two-Phase Commit** — simpler, different communication pattern (coordinator/participant vs. peer-to-peer); shows the tool is not Raft-specific
- **A Go open-source system** (aspirational) — e.g., a Go implementation of Paxos, etcd client logic, or similar; demonstrates real-world applicability with minimal adapter code

For each case study, report:
- LOC of adapter code
- LOC of system under test (to contextualize the adapter cost)
- Whether the system needed modification
- How many bugs were found and at what k

## Paper Table Structure

**Table 1 — Bug detection**: scenario × strategy → paths-to-bug, time-to-bug, found?

**Table 2 — Bound scaling**: scenario × k → interleavings explored, time, bugs found

**Table 3 — Systematic vs. random**: for each scenario, DFS vs. 10 random seeds → detection rate, mean first failure run, stddev

**Figure 1 — Bug discovery vs. search cost**: dual-axis plot of bugs found and interleavings explored as k increases, showing the saturation point where additional exploration yields no new bugs.

---

## Performance Testing Charts

The following charts are intended for performance testing the orchestrator itself — measuring search efficiency, execution cost, and how bugs relate to trace complexity.

### Metrics export

Most charts below require per-run data from `Explore`: the run number, elapsed wall-clock time, the recorded global/local traces, and whether that run passed. This is exposed through an optional observation callback:

```go
type RunObserver func(runNum int, nonFIFO int, elapsed time.Duration, rec RecordedRun, passed bool)

func WithObserver(obs RunObserver) ExploreOption
```

Called once per `runOnce` invocation inside `Explore`, this unblocks the chart generator without changing existing call sites. The JSONL metrics exporter writes these fields for offline plotting:

- `elapsed_ns`: physical wall-clock time for the run
- `logical_time_ns`: fake time advanced by the orchestrated system
- `global_decision_count`: number of global message-delivery decisions
- `local_decision_total`: total intra-bubble scheduling decisions across all nodes
- `trace_fingerprint` and `deliver_seq`: the global delivery sequence explored by the run

---

### Figure 1 — Bug discovery vs. search cost

**Data source**: run `Explore` once per k value per bug scenario; count of runs explored (from `t.Log` output or observer) and pass/fail.

**Producibility note**: works as-is assuming one injected bug per scenario. `Explore` stops at first failure, so "bugs found" is binary (0 or 1) per run of `Explore`. This must be made explicit in the chart — it is not tracking cumulative bugs across multiple distinct bugs within a single `Explore` call.

**Status: producible today.**


---

### Figure 2 — Physical time vs. logical time

**X-axis**: cumulative physical wall-clock time.
**Y-axis**: cumulative logical time advanced by the system under test.

**What it shows**: how much simulated protocol time the explorer covers per unit of real execution time. This is useful for checking whether deeper or more constrained schedules require significantly more physical time to advance the same amount of logical time. A flattening curve means later explored runs are more expensive per unit of logical progress; a stable slope means the orchestrator's replay and scheduling overhead is roughly constant.

**Data source**: `elapsed_ns` and `logical_time_ns` from the JSONL metrics records, accumulated across runs.

**Status: implemented in `charts/charts.py` as `fig2_physical_vs_logical_time.png`. The chart is skipped when the data has no meaningful fake-time advancement; the 1ns startup barrier used by some tests does not count.**

---

### Figure 3 — Decisions at bug discovery

**X-axis**: bug scenario and context bound for the run where the bug is found.
**Y-axis**: scheduling decisions made in that specific failing run, plotted separately for global and local decisions.

**What it shows**: how many scheduling choices were needed in the exact run that exposed the bug. Global decisions measure cross-node delivery choices. Local decisions count only meaningful intra-bubble choices: scheduler decision points where more than one non-root bubble goroutine was runnable. Bgid 0 is the synctest root/control-plane goroutine and is excluded from the local-choice definition. Plotting global and local decisions separately is more informative than combining them: a bug may be primarily about message ordering, local goroutine ordering, or both.

**Data source**: `global_decision_count`, `local_decision_total`, `run_num`, and `passed` from the JSONL metrics records. For each scenario/k/mode, select the first row where `passed == false`. For local decisions, `orchestrator/metrics.go` scans the synctest trace and counts a decision only when the recorded run queue contains at least two non-root goroutines.

**Status: implemented in `charts/charts.py` as `fig3_decisions_at_bug.png`.**

---

### Figure 4 — Global delivery search space

**X-axis**: context bound k (0, 1, 2, 3, ...) when k-sweep data is available; otherwise scenario × current k for a single-bound run.
**Y-axis** (log scale): actual DFS runs explored at that k vs. theoretical exhaustive global delivery orderings, computed as the product of `QueueSize` at each delivery decision point.

**What it shows**: the pruning power of context bounding for global message-delivery choices. The theoretical global delivery space grows quickly; actual DFS runs grow much more slowly because the bound caps non-FIFO choices. This chart intentionally excludes local goroutine scheduling interleavings inside each bubble. Local scheduling can interact with global delivery availability, so the total distributed schedule space is larger and not represented by this product.

**Data source**: run count per k from the observer; theoretical total = `∏ QueueSize_i` computed from `GlobalDecisions` of the k=0 FIFO baseline run (from `Run()`), which serves as the fixed reference trace. Using the FIFO baseline is important: `QueueSize` values change across orderings, so a fixed reference is required for the theoretical bound to be well-defined.

**Status: implemented in `charts/charts.py` as `fig4_theoretical_vs_actual.png`. With one k value it produces a single-bound bar comparison; with a k-sweep it produces the scaling plot. The theoretical baseline is producible from `Run()`.**

---

### Figure 5 — Unique interleavings until bug found

**X-axis**: bug scenario and context bound.
**Y-axis**: number of unique delivery sequences explored up to and including the first failing run.

**What it shows**: how many distinct global interleavings the explorer had to try before finding the bug. In systematic exploration, each call to `runOnce` attempts one global delivery ordering, so this often equals the number of runs to first bug. The chart still measures unique `trace_fingerprint` values rather than blindly using `run_num`, because replay divergence, duplicate traces, or random baselines can make run count and unique interleaving count differ.

**Data source**: `trace_fingerprint`, `run_num`, and `passed` from the JSONL metrics records. For each scenario/k/mode, keep rows through the first `passed == false` run and count distinct `trace_fingerprint` values.

**Status: implemented in `charts/charts.py` as `fig5_unique_interleavings_until_bug.png`.**
