# Evaluation Framework: orchestratorv2

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

A testing tool is only useful if people can actually wire it up to their system. This dimension evaluates how much work it takes to apply orchestratorv2 to a new distributed system.

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

### Required API change

Most charts below require per-run data that `Explore` does not currently expose. Its signature returns only `bool`, discarding intermediate `RecordedRun`s and run counts. The minimal fix is an optional observation callback passed via a new `ExploreOption`:

```go
type RunObserver func(runNum int, nonFIFO int, elapsed time.Duration, rec RecordedRun, passed bool)

func WithObserver(obs RunObserver) ExploreOption
```

Called once per `runOnce` invocation inside `Explore`, this unblocks Figures 2, 3, 4, 5, and 6 without changing the existing call sites. Figures that depend on this are marked **[needs observer]** below. Figure 1 and the paper tables are producible today without it.

---

### Figure 1 — Bug discovery vs. search cost

**Data source**: run `Explore` once per k value per bug scenario; count of runs explored (from `t.Log` output or observer) and pass/fail.

**Producibility note**: works as-is assuming one injected bug per scenario. `Explore` stops at first failure, so "bugs found" is binary (0 or 1) per run of `Explore`. This must be made explicit in the chart — it is not tracking cumulative bugs across multiple distinct bugs within a single `Explore` call.

**Status: producible today.**

---


### Figure 2 — Unique interleavings until bug found

**X-axis**: bug scenario (one bar per injected bug, grouped by k value).
**Y-axis**: number of unique delivery sequences explored before the first failing trace (unique = distinct `(From, To, OpType)` fingerprint from `GlobalTrace`).

**What it shows**: how many structurally distinct executions the tool examines before hitting a bug. For systematic DFS this is exact and deterministic — always the same count for a given k. For the random baseline it is a distribution (mean ± stddev across seeds); because random exploration can revisit the same delivery sequence, its unique interleaving count may be lower than its run count, but it still offers no guarantee of finding the bug within a fixed budget. The gap between the two strategies is the core efficiency argument for systematic exploration.

**Data source**: `rec.GlobalTrace` and `passed` from the observer callback; accumulate unique fingerprints per run, record the count at the first `runNum` where `passed == false`.

**Status: needs observer.**

---

### Figure 3 — Physical time over logical time

**X-axis**: logical time — cumulative runs explored (or cumulative total scheduling decisions across all runs).
**Y-axis**: cumulative wall-clock time.

**What it shows**: execution efficiency. A straight line means constant per-run cost; increasing slope means later runs (with longer DFS prefixes) are more expensive. The slope (time-per-run) is the key metric — compare systematic vs. random to check whether the per-run overhead of prefix replay outweighs the reduction in runs needed to find a bug. A flat per-run cost validates that prefix replay does not add meaningful overhead.

**Data source**: `elapsed` and `runNum` from the observer callback, accumulated across runs.

**Status: needs observer** (the `elapsed time.Duration` field in the observer carries per-run wall-clock cost).

---

### Figure 4 — Decision count distribution per context bound

**X-axis**: context bound k (0, 1, 2, 3, ...).
**Y-axis**: total scheduling decisions per run (global + local combined) — shown as a box plot across all runs explored at that k.

**What it shows**: whether higher k produces genuinely longer, more complex executions or just forces a different prefix onto traces of similar total depth. A non-FIFO delivery early in a trace can cause the protocol to terminate faster or slower than the default ordering, so the tail after the forced prefix is not fixed. Wide variance at a given k means delivery ordering significantly affects execution length; narrow variance means the protocol runs for roughly the same number of decisions regardless of ordering. This directly informs how expensive exploration becomes as k grows — if median decisions scale linearly with k the cost is manageable, but superlinear growth signals that non-FIFO choices open up substantially longer executions.

**Data source**: `len(rec.GlobalDecisions)` and `sum(len(rec.LocalTraces[addr]))` from the observer, grouped by `nonFIFO` (which equals k for the run that first reaches that depth).

**Status: needs observer.**

---

### Figure 5 — Scheduling decisions at bug discovery

**X-axis**: total scheduling decisions in the run where a bug was found (global + local combined, or plotted separately).
**Y-axis**: cumulative bugs found.

**What it shows**: whether bugs are shallow or deep in the decision space. If all bugs appear at low decision counts, it empirically validates context bounding — the tool finds bugs without needing long traces. Comparing global-only vs. global+local decision counts at bug discovery shows whether the bug required a specific cross-node delivery ordering, a specific intra-node goroutine schedule, or both.

**Data source**: `rec.GlobalDecisions` and `rec.LocalTraces` from the observer when `passed == false`.

**Status: needs observer.**

---

### Figure 6 — Explored vs. theoretical search space

**X-axis**: context bound k (0, 1, 2, 3, ...).
**Y-axis** (log scale): two lines — (a) actual DFS runs explored at that k, (b) theoretical exhaustive interleavings, computed as the product of `QueueSize` at each delivery decision point.

**What it shows**: the pruning power of context bounding. Theoretical interleavings grow exponentially with k; actual DFS runs grow much more slowly because the bound caps non-FIFO choices. The ratio between the two lines at each k quantifies how much of the search space context bounding discards while still finding all bugs of depth ≤ k. This is the core quantitative argument that context bounding is practical — without this chart the claim is asserted but not demonstrated.

**Data source**: run count per k from the observer; theoretical total = `∏ QueueSize_i` computed from `GlobalDecisions` of the k=0 FIFO baseline run (from `Run()`), which serves as the fixed reference trace. Using the FIFO baseline is important: `QueueSize` values change across orderings, so a fixed reference is required for the theoretical bound to be well-defined.

**Status: run count needs observer; theoretical baseline producible today from `Run()`.**
