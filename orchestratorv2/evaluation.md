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

**Figure 2 — Queue size distribution**: histogram of branching factor at delivery decision points, showing where non-determinism concentrates.
