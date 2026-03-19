# Synctest Bubble: Formal Model

## Terminology

| Term | Meaning |
|---|---|
| **Bubble** | Isolated execution domain. Has goroutines, fake time, scheduling control, trace recording, idle detection. One per node. |
| **Hook** | Callback mechanism — how the orchestrator controls the bubble |
| **Node** | A bubble running application code in a distributed test — has a name and transport |
| **Orchestrator** | Controls message delivery and time across nodes. Records global trace. |
| **Explorer** | Searches across interleavings. Uses the orchestrator repeatedly. |

---

## 1. Bubble (Local Model)

### Definition 1.1: Goroutine

A **goroutine** is a sequential thread of execution within a Go program.
Each goroutine in a bubble is assigned a **bubble-local goroutine ID** (BGID),
a monotonically increasing integer assigned at creation time. The root
goroutine of a bubble has BGID 0.

### Definition 1.2: Bubble State

A **bubble** is an isolated scheduling domain. Each bubble is assigned a
deterministic **bubble ID** at creation time. A bubble has a one-to-one
relationship with a P (processor) — the bubble owns exactly one P for
its lifetime, and that P only runs goroutines belonging to the bubble.

A bubble at time $t$ is described by:

- $G_t = \{b_0, b_1, \ldots, b_m\}$: the set of live goroutines, identified by their BGIDs
- $R_t \subseteq G_t$: the **runnable set** — goroutines eligible to execute, ordered by position in the runq
- $S_t$: the **shared state** — memory, channels, and synchronization state visible to goroutines in the bubble
- $\sigma$: the **scheduling function** that picks the next goroutine at each decision point

### Definition 1.3: Decision Point

A **decision point** occurs when `findRunnable` on the bubble's P must
choose the next goroutine to run. This happens when:

1. The currently running goroutine **blocks** (channel op, mutex, sleep, park)
2. The currently running goroutine **exits**
3. The bubble is first entered (initial pick from runq)

At a decision point, $\sigma$ observes $R_t$ and picks one $b_i \in R_t$.
The chosen goroutine runs uninterrupted until the next yield point —
with `asyncpreemptoff=1` and a single P, there is no mid-execution
preemption.

### Definition 1.4: Scheduling Function

The **scheduling function** maps each decision point to a selection from
the runnable set. It can be expressed as an **index** into the ordered runq:
index 0 is the FIFO choice (runnext, then head of runq), index 1 is the
next, and so on.

The **default scheduling function** always returns index 0 (FIFO).
Alternatively, $\sigma$ can follow a recorded prefix (for replay) or
delegate to a decision hook controlled by the orchestrator (see Section 5).

### Definition 1.5: Trace

A **trace** is the complete sequence of scheduling decisions in a bubble execution:

$$\tau = (d_0, d_1, \ldots, d_n)$$

Each decision $d_t$ records:
- **Step**: position in the sequence
- **Index**: the scheduling choice (which position in the runq)
- **ChosenBgid**: the BGID of the selected goroutine
- **RunqSize**: number of runnable goroutines
- **RunqBgids**: the ordered BGIDs of all runnable goroutines

This maps directly to the `synctest.Decision` struct in the implementation.

### Definition 1.6: Interleaving

An **interleaving** is a complete trace from bubble creation to idle
resolution (all goroutines blocked or exited). Two interleavings are
**distinct** if they differ in at least one scheduling choice.

---

## 2. Guarantees

### Theorem 2.1: Determinism (Record-Replay)

Given a program and a recorded trace, replaying with the same scheduling
choices produces:

1. **Identical trace**: same BGIDs, runnable sets, and decisions at every step
2. **Identical shared state**: same final memory state
3. **Identical observable behavior**: same output, same side effects

*Preconditions*:
- `GODEBUG=asyncpreemptoff=1` (no async preemption)
- Single P per bubble (cooperative scheduling)
- No external nondeterminism (time, I/O) unless wrapped in `External()` or `ExternalWait()`
- Identical program across runs

### Theorem 2.2: BGID Stability

The BGID assigned to each goroutine is deterministic and depends only on
creation order, which is determined by the trace prefix up to the
goroutine's creation point.

If two traces agree on the first $k$ decisions, any goroutine created
during those $k$ quanta receives the same BGID in both executions.

---

## 3. Yield Points

A **yield point** is a program location where execution can take a
different path based on a non-deterministic choice.

| Yield point | Mechanism |
|---|---|
| Channel send (blocking) | Goroutine parks on channel wait queue |
| Channel receive (blocking) | Goroutine parks on channel wait queue |
| Select (multiple ready cases) | Pollorder determined by seed; goroutine does not park |
| Mutex.Lock (contended) | Goroutine parks on semaphore |
| WaitGroup.Wait | Goroutine parks on semaphore |
| Goroutine exit | Goroutine removed from live set |
| `runtime.Gosched()` | Voluntary yield |

Most yield points park the goroutine, creating a scheduling decision
(which goroutine runs next). Select is different: the goroutine does
not park - it picks a case and continues. The choice of which case is
a separate kind of decision (see Section 5).

**Current limitation**: Select with multiple ready cases is controlled
by a seed-determined counter (`selectCounter`) but the goroutine does
not park — it picks a case and continues. This means select ordering
is deterministic for a given seed but not independently controllable
per-decision. Future work: make select a true yield point by parking
the goroutine, firing the decision hook, and resuming with the chosen
case index (same mechanism as goroutine scheduling decisions).

---

## 4. External Operations

When a goroutine needs to interact with something outside the bubble
(network, disk, orchestrator channels), it must **detach** from the
bubble temporarily. The mechanism is the same in both cases:

1. A counter is incremented and the goroutine is detached (`gp.bubble = nil`)
2. The goroutine executes `fn` outside the bubble — invisible to $\sigma$
3. When `fn` returns, the goroutine reattaches and the counter is decremented

Two variants exist, distinguished by who controls when `fn` completes:

| Variant | Counter | Who unblocks | Use case |
|---|---|---|---|
| `External(fn)` | `external` | The outside world (Redis, disk, OS) | Uncontrolled I/O |
| `ExternalWait(fn)` | `externalWait` | The orchestrator (message delivery) | Any inter-node interaction |

`External` is for operations the bubble has no control over. The bubble
waits until the goroutine comes back.

`ExternalWait` is for operations on channels that cross the bubble
boundary, controlled by the orchestrator. The orchestrator decides when
to complete the operation by writing to the channel.

### Idle Resolution

When all goroutines in the bubble are blocked (`running = 0`), the
bubble must decide what to do next. The resolution depends on the
bubble's state:

| Condition | What happens |
|---|---|
| `external > 0` | Wait — the bubble can't help, external IO must complete on its own |
| Hook set, runq empty | Idle hook fires — orchestrator decides (deliver message or advance time) |
| No hook, timers exist | Auto-advance time to next timer |
| No hook, no timers | Deadlock |

The **hook** (`SetDecisionHook`) is what distinguishes a standalone
bubble from an orchestrator-controlled one. When a hook is set, the
orchestrator owns all progress decisions — including time advancement.
When no hook is set, the bubble manages itself (the original
`testing/synctest` behavior).

---

## 5. Decision Hook

The **decision hook** is a function registered via `SetDecisionHook`
that gives the orchestrator control over the bubble's scheduling and
idle behavior.

```go
synctest.SetDecisionHook(func(state BubbleState) int32)
```

The hook is called in two contexts:

| Context | `state.Idle` | Return value | When |
|---|---|---|---|
| Scheduling decision | `false` | Index into runq (0 = FIFO) | `findRunnable` has multiple runnable goroutines |
| Idle | `true` | Ignored | All goroutines blocked, no external pending |

### Scheduling Decisions

When `findRunnable` observes $|R_t| > 1$ at the frontier (past any
pre-loaded prefix), it signals root. Root calls the hook with
`BubbleState` containing the runnable set. The hook returns an index.
Root stores the answer and parks. `findRunnable` picks the goroutine
at that index.

The `distributed.Bubble` package wraps this — it follows a recorded
prefix or defaults to FIFO, without contacting the orchestrator.

### Idle

When the bubble reaches idle resolution and a hook is set (see
Section 4), the hook fires with `Idle: true`. The `BubbleState`
reports the bubble's timers, counters, and clock. The hook blocks —
typically sending the state to the orchestrator and waiting for
a `Resume`.

While the hook blocks, the bubble is frozen. `rootInHook = true`
prevents `findRunnable` from scheduling other goroutines. The
orchestrator delivers a message or advances time, then replies. The
hook returns, the bubble resumes.

### Hook Communication

The hook communicates with the orchestrator via **non-bubble channels**
(created outside the bubble). When root blocks on a non-bubble channel,
the bubble sees root as "running" — `running` is not decremented.
This prevents a bounce loop where `maybeWakeLocked` repeatedly wakes
root.

---

## 6. Distributed Model

A **distributed test** runs $k$ nodes, each in its own bubble. The
nodes communicate through an orchestrator that controls message
delivery and time.

### Definition 6.1: Node

A **node** $n_i$ is a bubble running application code (e.g., a raft
instance). The ordered sequence $N = (n_1, n_2, \ldots, n_k)$ is
fixed at test setup. The ordering is called the **registration order**.

Each node has:
- A **bubble** $B_i$ with its own goroutines, runq, fake clock $c_i$
- A **transport** that routes messages through the orchestrator
- A **hook** wired to the orchestrator for scheduling and idle control

### Definition 6.2: The Orchestrator Cycle

The orchestrator drives execution in a loop:

1. **Start** each bubble sequentially in registration order. Wait for
   each to go idle before starting the next.
2. **Collect** idle states from all active bubbles.
3. **Drain** pending outbound messages from each node's transport.
4. **Decide**: pick one message to deliver (index into pending queue),
   or advance time if no messages pending.
5. **Resume** the target bubble. It runs until idle.
6. Back to step 2.

One bubble is active at a time. All others are frozen in their hooks.

### Definition 6.3: Complete Trace

The local and global decisions are not independent — they form a
single interleaved sequence. A global decision (deliver message to
node $n_i$) changes what $n_i$ processes, which changes its local
scheduling decisions, which changes what messages it produces.

The **complete trace** is a single sequence:

$$T = (d_0, d_1, d_2, \ldots)$$

Each entry $d_j$ is tagged with:

| Type | Scope | Index | Meaning |
|---|---|---|---|
| **Schedule** | Bubble $B_i$ | Index into $R_t$ | Which goroutine runs next |
| **Deliver** | Global | Index into pending queue | Which message to deliver |
| **TimeAdvance** | Global | — | Advanced time to $t$ |
| **Done** | Global | — | Node completed |

Schedule and Deliver entries carry an index (the decision). TimeAdvance
and Done are deterministic given prior decisions, but recorded for
replay.

Replaying $T$ from the beginning reproduces the exact execution.
Each decision is valid because the trace up to that point determines
the state — the same prefix produces the same runnable set (for
scheduling) or the same pending queue (for delivery).

---

## 7. Time Model

### Fake Clock

Each bubble $B_i$ has a fake clock $c_i$ (nanoseconds since epoch).
The initial value is midnight UTC 2000-01-01. Real wall-clock time
does not advance inside a bubble — only the fake clock does.

### Standalone Mode (no hook)

When no hook is set, the bubble manages time itself. At idle
resolution (Section 4), if timers exist, the bubble advances $c_i$ to
the next timer deadline. This is the standard `testing/synctest`
behavior.

### Orchestrator Mode (hook set)

When a hook is set, the orchestrator owns time. The bubble
**never** auto-advances its clock. Instead:

1. The idle hook fires, reporting `NextTimer` in `BubbleState`
2. The orchestrator decides whether to advance time
3. The orchestrator sends `Resume{AdvanceTimeTo: t}`
4. `SetTime(t)` is called inside the hook: $c_i \leftarrow \max(c_i, t)$
5. The hook returns, the bubble's event loop fires timers at the new clock

Clocks never go backward.

### Time Advancement Across Nodes

When no pending messages exist, the orchestrator computes:

$$t' = \min_{n_i \in \text{active}} \text{nextTimer}(n_i)$$

and advances all bubbles to $t'$, **sequentially in registration
order** — resume $B_1$, wait for idle, resume $B_2$, wait for idle,
and so on.

Sequential advancement ensures deterministic ordering of any
application-level randomness (e.g., `rand.Int63()` calls) across
bubbles that share a global PRNG. This is a practical constraint for
programs like hashicorp/raft that use the global `math/rand` source;
programs with per-node PRNGs could be advanced in parallel without
affecting determinism.
