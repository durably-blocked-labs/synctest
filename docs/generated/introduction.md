# Introduction

## 1. Problem Statement

Go's concurrency model --- goroutines, channels, and mutexes --- makes
it easy to write concurrent code but hard to test it. A concurrent
program may work correctly under one goroutine scheduling order and
fail under another. The Go runtime's scheduler uses a per-M PRNG
(`cheaprand()`) to break ties between runnable goroutines, so the
interleaving that executes in any given test run is effectively
random. A bug that requires a specific ordering of goroutine
executions may never manifest under normal testing, yet appear
reliably under production load.

Go's existing testing tools do not address this systematically:

- **`go test -race`** instruments memory accesses to detect data races
  (unsynchronized reads and writes to the same location). It does not
  detect deadlocks, livelocks, or protocol violations that arise from
  legal but unfortunate scheduling orders.

- **Go's built-in deadlock detector** fires when every goroutine in
  the program is blocked simultaneously. It misses partial deadlocks
  --- a subset of goroutines stuck in a circular wait while others
  continue running --- which are the common case in server software.

- **Stress testing** (`go test -count=1000`) reruns the test many
  times, hoping that OS thread scheduling jitter triggers the bug.
  This is probabilistic. Bugs with low trigger probability survive
  thousands of runs. Bugs that require two or more non-default
  scheduling decisions compound: if each decision has probability $p$,
  the trigger probability is $p^k$, and the expected number of runs to
  first detection is $p^{-k}$.

- **Static analysis tools** (GCatch) model individual synchronization
  primitives in isolation. GCatch tracks channel operations
  path-sensitively but does not model mutexes. Bugs that require
  reasoning about the interaction between channels and locks --- the
  "Channel & Lock" category in GoBench --- are invisible to it.

- **Dynamic tools** (GFuzz, GoAT, Goleak) perturb execution or
  analyze traces from individual runs. GFuzz injects delays at channel
  operations but does not control goroutine scheduling. GoAT analyzes
  happens-before relationships from a single trace but cannot force
  the scheduler into orderings that never appear under default
  scheduling. Goleak detects leaked goroutines post-mortem but not the
  deadlock mechanism that caused the leak.

The result: real concurrency bugs in production Go software go
undetected. The GoBench suite (Yuan et al., CGO 2021) catalogs 103
concurrency bug kernels extracted from Kubernetes, etcd, Docker, gRPC,
and Istio. On the hardest category --- 13 bugs involving both channels
and locks --- the best existing tool (Goleak) detects 7 out of 13
(54%). The remaining tools detect between 31% and 38%.

For distributed systems, the problem is worse. A distributed
concurrency bug may require a specific combination of message delivery
order *and* goroutine scheduling within a node. Neither dimension of
non-determinism is controlled by any Go testing tool. The standard
approach --- injecting random delays or using fault injection
frameworks --- is the distributed analog of stress testing:
probabilistic, with no coverage guarantees.

---

## 2. Our Approach

We build on Go 1.27's `testing/synctest` package, which provides
**bubbles** --- isolated execution environments with a fake clock,
their own goroutine set, and idle detection. Synctest was designed for
testing time-dependent code deterministically: inside a bubble, time
advances only when all goroutines are durably blocked, so tests do not
depend on wall-clock timing. But synctest does not control scheduling.
Two goroutines that become runnable simultaneously are ordered by
`cheaprand()`, which varies across runs.

Our contribution: we modify the Go runtime to make scheduling
decisions within bubbles **observable**, **recordable**, and
**replayable**. At each decision point --- where `findRunnable` on the
bubble's P must choose among multiple runnable goroutines --- we
record the choice (which goroutine, from what runnable set) and allow
it to be controlled by an external scheduling function. This gives us
three capabilities:

1. **Record.** Run a test and capture every scheduling decision as a
   trace: a sequence of (index, chosen goroutine, runnable set)
   tuples.

2. **Replay.** Feed a recorded trace back as a prefix. The runtime
   follows the prefix exactly, reproducing the same interleaving
   deterministically.

3. **Explore.** At each decision point with alternatives (runnable set
   size $> 1$), branch: try a different choice and observe the
   outcome. Systematic exploration covers the space of interleavings.

We implement three exploration algorithms:

- **CHESS (context-bounded DFS).** Systematically explores all
  interleavings within a bound $K$ on non-FIFO scheduling decisions.
  A decision is non-FIFO if it picks a goroutine other than the
  default (head of the run queue). CHESS enumerates all traces with at
  most $K$ non-FIFO decisions. The state space is polynomial for fixed
  $K$: $O(D^K \cdot f^K)$ traces, where $D$ is the number of decision
  points with alternatives and $f$ is the maximum branching factor.
  CHESS is complete within the bound --- it finds every bug that
  manifests with $\leq K$ non-default scheduling choices.

- **PCT (Probabilistic Concurrency Testing).** Assigns random
  priorities to goroutines and flips priorities at $d{-}1$ randomly
  chosen decision points (where $d$ is a depth parameter). PCT has a
  theoretical lower bound on bug-finding probability:
  $1 / (n \cdot D^{d-1})$ for a bug of depth $d$ in a program with
  $n$ goroutines and $D$ decision points. It is fast (one scheduling
  decision per decision point, no backtracking) but probabilistic.

- **Random.** Uniform random choice at every decision point. No
  guarantees, but fast and effective as a baseline.

For distributed systems, we extend the model to **multi-bubble
orchestration**. Each node runs in its own bubble. An orchestrator
controls two dimensions of non-determinism:

- **Global decisions**: which pending message to deliver next (from a
  queue of in-flight messages across all nodes).
- **Local decisions**: which goroutine to schedule within the resumed
  node's bubble.

One bubble is active at a time. The orchestrator resumes a bubble by
delivering a message, the bubble runs until all its goroutines are
idle, and the orchestrator collects any outbound messages before making
the next global decision. This gives a clean round structure where
each round is one global decision followed by a sequence of local
decisions. The exploration algorithms (CHESS, PCT, Random) operate
over both dimensions with a shared context budget.

---

## 3. Key Results

### Single-process bugs: GoBench evaluation

We ported all 13 "Channel & Lock" bugs from the GoBench/GoKer suite
--- the hardest category, requiring reasoning about the interaction
between Go channels and mutexes. Results:

| Tool | Detected | Recall |
|------|----------|--------|
| Goleak | 7/13 | 54% |
| GCatch | 5/13 | 38% |
| GFuzz | 4/13 | 31% |
| GoAT | 5/13 | 38% |
| **Ours** | **10/13** | **77%** |

The three undetected bugs are fundamental limitations of
goroutine-scheduling granularity, not search depth:

- **etcd#7492** requires a timer to fire while a goroutine holds a
  mutex. Bubble time advances only when all goroutines are blocked, so
  the timer cannot fire mid-execution.
- **k8s#1321** and **serving#2137** require preemption between two
  non-blocking operations within a single goroutine quantum (no yield
  point exists between them).

These limitations are inherent to any tool that operates at
goroutine-scheduling granularity rather than instruction-level
interleaving.

A single runtime change was decisive: adding `waitReasonSemacquire`
(the wait reason for mutex acquisition) to the bubble's
durable-blocking list. Without this change, the bubble never
recognizes a decision point at lock contention, and 0 out of 13 bugs
are detectable. With it, 10 out of 13.

**Headline result.** etcd#7443 --- a real etcd bug involving an
RWMutex and a buffered channel in a 5-goroutine system --- is found
deterministically in 74 CHESS runs at context bound 3. No existing
tool in the GoBench evaluation detects it.

### Distributed bugs: Ricart-Agrawala evaluation

We implemented five bugs in Ricart-Agrawala distributed lock variants,
each requiring a different combination of global and local
non-determinism:

| Bug | Mechanism | CHESS bound | Non-FIFO decisions |
|-----|-----------|-------------|---------------------|
| ra-gate | G+L | $K=2$ | 1 global + 1 local |
| ra-stale-reply | G only | $K=4$ | 4 global |
| ra-duplicate-request | G+L | $K=3$ | 2 global + 1 local |
| ra-premature-defer | G only | $K=4$ | 4 global |
| ra-deferred-storm | G+L | $K=4$ | 2 global + 2 local |

The key finding: bugs marked G+L require **composition** of global
(message reorder) and local (goroutine order) non-determinism.
Controlling only message delivery order or only goroutine scheduling
is insufficient --- neither dimension alone triggers the bug. Our
system controls both dimensions within a single exploration framework.

Detection is black-box: each test asserts a safety invariant (e.g.,
mutual exclusion via a shared counter) and reports a violation through
Go's standard `t.Errorf`. No internal state inspection of the
protocol is required. A networked KV store running in its own bubble
provides the shared counter for cross-node assertions.

---

## 4. Report Structure

The remainder of this report is organized as follows:

- **Formal Model** (`model.md`): defines bubbles, decision points,
  traces, and the state space formally. Establishes determinism and
  BGID stability theorems. Defines the three transition types (local,
  global, select) and proves the context-bounding complexity result.

- **Go Scheduler Deep Dive** (`go-scheduler-deep-dive.md`): explains
  the Go runtime's scheduling mechanisms --- the GMP model, run
  queues, `findRunnable`, and `cheaprand()` --- and describes the
  specific runtime modifications that make scheduling controllable.

- **Synctest Bubble Internals** (`synctest-bubble-deep-dive.md`):
  details the bubble abstraction --- creation, idle detection, time
  advancement, external operations, and the decision hook protocol
  between the root goroutine and the scheduler.

- **Algorithm Implementation** (`algorithm-implementation.md`):
  describes the CHESS, PCT, and Random algorithm implementations,
  including the DFS work-item stack, context budget tracking, and the
  orchestrator's round-based exploration loop.

- **GoBench Evaluation** (`gobench-evaluation.md`): per-bug results
  for all 13 Channel & Lock kernels, comparison with four existing
  tools, and analysis of the three undetected bugs.

- **Distributed Evaluation** (`ra-gate-evaluation.md`): the
  Ricart-Agrawala bug suite, strategy comparison (CHESS G-only vs G+L
  vs PCT vs Random), and benchmark data.

- **Distributed Model** (`distributed-model.md`): the multi-bubble
  orchestration design --- transport abstraction, hook protocol, idle
  resolution, and the drain-resume cycle.
