# Algorithm Implementation

Three scheduling algorithms -- CHESS, PCT, and Random -- drive
interleaving exploration. All three implement a shared `Algorithm`
interface, making them interchangeable across single-bubble (local
concurrency) and multi-bubble (distributed) execution modes.

---

## 1. Algorithm Interface

The orchestrator calls the algorithm in a loop: `BeforeRun` to
initialize, `Decide` at every scheduling choice during execution,
`AfterRun` to record the outcome and compute next work.

```go
// Algorithm controls multi-run exploration.
//
// The orchestrator calls BeforeRun before each execution. If it returns
// false, exploration is done. During the run, Decide is called at every
// decision point -- global and local. After the run, AfterRun receives
// the result.
//
// Decide may panic with replayDivergenceError to signal that the
// execution has diverged from the expected prefix. The orchestrator
// catches this panic, marks the run as diverged, and calls AfterRun
// with Diverged=true.
type Algorithm interface {
    BeforeRun() bool
    Decide(dp DecisionPoint) int
    AfterRun(result RunResult)
}
```

`DecisionPoint` is the algorithm's view of a scheduling choice:

```go
type DecisionPoint struct {
    Kind DecisionKind   // Local or Global
    Step int            // 0-indexed position in the unified trace
    Node string         // bubble name for Local, "" for Global
    Alts []Alt          // choosable alternatives (len >= 1)
}
```

| Field | Type | Meaning |
|---|---|---|
| `Kind` | `Local` or `Global` | Local = goroutine scheduling within a bubble. Global = message delivery across bubbles. |
| `Step` | `int` | Position in the unified (G+L interleaved) trace. |
| `Node` | `string` | For Local: the bubble name (e.g., `"A"`). For Global: empty string. |
| `Alts` | `[]Alt` | The set of choosable alternatives. Index 0 is the default (FIFO for local, queue-order for global). |

Each `Alt` carries an `ID` string used as a stable map key for priority
assignment and dependence tracking:

| Decision kind | `Alt.ID` format | Example |
|---|---|---|
| Local | `"B{bgid}"` | `"B5"` (goroutine with BGID 5) |
| Global | `"msg:{From}->{To}({MsgType})"` | `"msg:A->B(Request)"` |

The algorithm returns an index into `Alts`. The caller (orchestrator or
single-bubble explorer) executes that choice and records it in the trace.

---

## 2. CHESS (Context-Bounded DFS)

CHESS systematically enumerates scheduling interleavings using
depth-first search with a context bound $K$.

### Mechanism

1. **Seed run.** The first run uses an empty prefix -- every decision
   defaults to FIFO (index 0). This produces the baseline trace.
2. **Prefix replay.** On subsequent runs, CHESS replays a stored prefix
   (a modified trace) by returning the recorded index at each decision
   point. Past the prefix, it returns 0 (FIFO).
3. **Alternative generation.** After each run, CHESS scans the trace
   for decision points with alternatives (branching factor > 1). For
   each such point and each non-taken alternative, it constructs a new
   prefix: the original trace up to that point with the last entry
   replaced by the alternative index.
4. **Stack.** New prefixes are pushed onto a LIFO stack. `BeforeRun`
   pops the next item; when the stack is empty, exploration terminates.

### Context Bound

Each prefix tracks how many non-FIFO decisions it contains. A new
prefix is only pushed if `nonFIFO + 1 <= Bound`. This limits
exploration to traces with at most $K$ non-default choices.

The bound $K$ controls the tradeoff between coverage and cost:

| $K$ | Traces explored | Guarantee |
|---|---|---|
| 0 | 1 | Only the FIFO baseline |
| 1 | $O(D \cdot f)$ | All single-deviation traces |
| 2 | $O(D^2 \cdot f^2)$ | All double-deviation traces |
| $K$ | $O(D^K \cdot f^K)$ | Polynomial in $D$ for fixed $K$ |

where $D$ is the number of decision points with branching factor > 1
and $f$ is the maximum branching factor.

### GlobalOnly Mode

When `GlobalOnly = true`, CHESS restricts branching to Global decisions
only. Local decisions always return index 0 (FIFO) and are excluded
from the prefix. This is useful for distributed bugs that depend on
message delivery order but not on intra-node goroutine scheduling.

The implementation uses two separate alternative-generation routines:

- **`pushGlobalOnlyAlts`**: extracts only Global steps from the trace,
  builds prefixes from those. Replay is deterministic because local
  scheduling is always FIFO.
- **`pushAllAlts`**: generates alternatives from all steps. Two passes
  (local first, global second) ensure global alternatives are explored
  first via LIFO ordering on the stack.

### Divergence Handling

When replaying a prefix, the actual execution may produce a different
number of local decisions than the prefix expects (because a changed
global decision altered intra-node behavior). This causes a
kind/node mismatch or an out-of-range index at the replay point.

CHESS handles this by panicking with `replayDivergenceError`. The
orchestrator catches the panic via `recover()`, marks the run as
diverged (`RunResult.Diverged = true`), and calls `AfterRun`. CHESS
discards diverged runs -- they generate no new alternatives:

```go
if result.Diverged {
    return
}
```

Divergence is inherent to the full G+L mode (not GlobalOnly). The
divergence rate depends on how tightly global delivery order couples
to local scheduling. In practice, most prefixes converge because
local scheduling within a bubble is largely independent of which
message was delivered in a different round.

### Tree Export

CHESS records a `RunNode` per run for visualization and analysis:

```go
type RunNode struct {
    Run        int   // 1-indexed run number
    Parent     int   // index of parent RunNode, -1 for root
    BranchStep int   // which trace step diverged from parent
    NonFIFO    int   // non-FIFO decisions in this trace
    Passed     bool
    TraceLen   int
}
```

The tree is exported as JSON via `WriteTreeJSON` for offline
visualization of the exploration structure.

---

## 3. PCT (Probabilistic Concurrency Testing)

PCT is a randomized algorithm with a provable lower bound on finding
bugs of a given depth. It assigns priorities to schedulable entities and
samples a small number of priority-change points.

### Priority Assignment

Each schedulable entity -- goroutine (identified by BGID) or message
class (identified by sender/receiver/type) -- receives a random
priority drawn uniformly from $[0, 2^{30})$ on first encounter. The
priority persists for the duration of the run.

At each decision point, PCT picks the alternative with the **lowest**
priority number:

```go
func (p *PCT) Decide(dp DecisionPoint) int {
    if dp.N() <= 1 {
        p.step++
        return 0
    }
    chosen := 0
    chosenPri := p.priorityOf(dp.Alts[0].ID)
    for i := 1; i < dp.N(); i++ {
        pri := p.priorityOf(dp.Alts[i].ID)
        if pri < chosenPri {
            chosen = i
            chosenPri = pri
        }
    }
    // At change points, demote the winner.
    if _, ok := p.changePoints[p.step]; ok {
        p.priority[dp.Alts[chosen].ID] = p.nextLow
        p.nextLow++
    }
    p.step++
    return chosen
}
```

### Change Points

PCT samples $d-1$ **change points** uniformly from $[0, \text{MaxSteps})$
before each run. At a change point, the winning entity is **demoted**:
its priority is replaced with a value above $2^{30}$, pushing it to
the back of the priority order. This forces a different entity to win
at that step, creating the scheduling perturbation needed to expose
depth-$d$ bugs.

### Theoretical Guarantee

For a bug requiring $d$ specific scheduling decisions among $N$
entities over $k$ steps:

$$P(\text{finding bug}) \geq \frac{1}{N \cdot k^{d-1}}$$

This is the Burckhardt et al. (ASPLOS 2010) result. The key insight:
the initial random priority order has probability $\geq 1/N$ of placing
the "right" entity first, and each of the $d-1$ change points has
probability $\geq 1/k$ of landing at the right step.

### Seed Control

Each run uses seed `baseSeed + runCount`. Different base seeds explore
different priority assignments and change-point placements. The seed
fully determines the run's scheduling decisions, making PCT
reproducible.

### Unified G+L Scheduling

PCT does not distinguish between Local and Global decisions. The same
priority map and change-point logic apply to both. This means a
goroutine BGID and a message class compete on the same priority scale
-- the algorithm naturally interleaves local and global perturbations.

---

## 4. Random

Random picks uniformly at each decision point. It serves as the
baseline for comparison.

```go
func (r *Random) Decide(dp DecisionPoint) int {
    if dp.N() <= 1 {
        return 0
    }
    return r.rng.Intn(dp.N())
}
```

Each run uses seed `baseSeed + runCount`, producing a deterministic but
distinct schedule per run. No state carries between runs -- priorities,
change points, and prefix replay do not exist.

Random provides no coverage guarantees. Its expected runs to find a bug
requiring $d$ non-default decisions among alternatives of sizes
$b_1, b_2, \ldots, b_d$ is:

$$E[\text{runs}] = \prod_{i=1}^{d} b_i$$

This is the reciprocal of the probability that all $d$ decisions
independently land on the right alternative.

---

## 5. Two Execution Modes

The same `Algorithm` interface drives both execution modes.

### Single-Bubble (`explorer.Test`)

The workload runs in one bubble. Only Local decisions exist -- the
algorithm sees `DecisionPoint{Kind: Local}` at every call. The
`explorer` package wraps the algorithm and translates between the
bubble's `BubbleState` (goroutine IDs, runnable set) and the
`DecisionPoint` struct.

```go
explorer.Test(t, func(t *testing.T) {
    // concurrent workload
}, &explorer.CHESS{Bound: 2})
```

The `explorer` package re-exports algorithm types from `orchestrator`,
so single-bubble users only import `explorer`:

```go
type CHESS  = orchestrator.CHESS
type PCT    = orchestrator.PCT
type Random = orchestrator.Random
```

### Multi-Bubble (`orchestrator.ExploreWith`)

$N$ nodes run in $N$ bubbles. The orchestrator calls `algo.Decide` for
both Global decisions (which message to deliver) and Local decisions
(which goroutine to schedule within the active bubble). The algorithm
receives `DecisionPoint{Kind: Global}` or `DecisionPoint{Kind: Local}`
and returns an index -- it does not need to know which mode it is
operating in.

```go
orch := orchestrator.New()
algo := &orchestrator.CHESS{Bound: 2, GlobalOnly: true}
orch.ExploreWith(t, setup, algo, orchestrator.GlobalMaxRuns(500))
```

### Comparison

| Property | Single-bubble | Multi-bubble |
|---|---|---|
| Entry point | `explorer.Test` / `explorer.Explore` | `orchestrator.ExploreWith` |
| Decision kinds | Local only | Local + Global |
| Bubbles | 1 | $N$ |
| Trace structure | Flat sequence of L-transitions | Interleaved G+L rounds |
| Algorithm interface | Same | Same |
| Divergence source | CHESS prefix mismatch (rare) | Global delivery changes local behavior |

Algorithms are unaware of the distinction. CHESS pushes alternatives
for any decision point with branching factor > 1, whether Local or
Global. PCT assigns priorities to any `Alt.ID`, whether it represents a
goroutine or a message class. Random picks uniformly regardless of kind.

---

## 6. Seed Sweep Results (etcd#7443)

The `bugs/ra-gate` test case reproduces etcd issue #7443 -- a request
amplification bug in hashicorp/raft where a follower re-gates a
request it should have already applied. The system has 3 nodes (A, B, C)
in 3 bubbles with both Global and Local scheduling decisions.

### Protocol

- **CHESS $K$=3**: single deterministic exploration (GlobalOnly mode).
- **Random**: 100 seeds (base seed 1..100), max 500 runs per seed.
- **PCT $d$=2**: 100 seeds (base seed 1..100), max 500 runs per seed.
- **PCT $d$=3**: 100 seeds (base seed 1..100), max 500 runs per seed.

Each run stops at first failure.

### Results: Runs to First Bug

| Algorithm | Deterministic? | Runs (avg) | Runs (min) | Runs (max) |
|---|---|---|---|---|
| CHESS $K$=3 | Yes | 74 | 74 | 74 |
| Random (100 seeds) | No | 41 | 1 | 111 |
| PCT $d$=2 (100 seeds) | No | 38 | 2 | 83 |
| PCT $d$=3 (100 seeds) | No | 38 | 2 | 83 |

### Analysis

**CHESS is deterministic.** The same code always explores 74 runs
before finding the bug. There is no variance and no dependence on seed
choice. The 74-run cost reflects the DFS enumeration order -- the
bug-triggering delivery sequence is 74th in the systematic traversal.

**Random and PCT are faster on average but have tail risk.** Both find
the bug in ~38-41 runs on average, roughly half the CHESS cost. But
their worst case (83-111 runs) exceeds CHESS. A user who picks an
unlucky seed pays more than the systematic approach.

**PCT $d$=2 and $d$=3 perform identically on this bug.** The
ra-gate bug requires a specific message delivery order (depth 2 in
global decisions). PCT's extra change point at $d$=3 adds no value
because the bug does not involve a third non-default decision. The
identical distributions confirm this.

**Key insight.** CHESS provides a **worst-case guarantee**: 74 runs,
always. Random and PCT provide better expected performance but no upper
bound. For CI pipelines where predictable cost matters, CHESS is
preferable. For exploratory testing where average cost dominates,
PCT or Random are reasonable.

### Theoretical Context

The ra-gate bug has depth $d$=2 in the global delivery dimension. For
a system with $N$=6 message classes and $k \approx 60$ total steps:

| Algorithm | Bound on $P(\text{hit})$ per run | Expected runs |
|---|---|---|
| Random | $\prod 1/b_i$ (product of branching factors) | Depends on trace structure |
| PCT $d$=2 | $\geq 1/(6 \cdot 60) \approx 1/360$ | $\leq 360$ |
| CHESS $K$=2 | Exhaustive within bound | $\leq O(D^2 \cdot f^2)$ traces total |

The empirical results are much better than the theoretical PCT bound
because the ra-gate bug's triggering condition has high probability
under random priority assignments -- most priority orderings that
deliver the right two messages "out of order" will find it.
