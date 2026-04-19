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
| `sync.Mutex.Lock` / `sync.RWMutex.Lock` / `sync.RWMutex.RLock` (contended) | Goroutine parks on semaphore; treated as durably blocked |
| WaitGroup.Wait | Goroutine parks on semaphore |
| Goroutine exit | Goroutine removed from live set |
| `runtime.Gosched()` | Voluntary yield |

All of the above park waits count toward bubble idleness — the runtime's
`isIdleInSynctest` table (`runtime/runtime2.go`) includes
`waitReasonSyncMutexLock`, `waitReasonSyncRWMutexRLock`, and
`waitReasonSyncRWMutexLock` alongside the channel/select wait reasons, so
a bubble whose only running goroutines are parked on `sync`-package
locks will decrement `running` to zero and trigger idle resolution. This
is a change from upstream Go's `testing/synctest`, which treats mutex
waits as non-durable. The consequence: programs that use `sync.Mutex` to
coordinate between goroutines in the same bubble can rely on idle
resolution (message delivery via the orchestrator, or time advancement)
to break lock contention deadlocks, rather than hanging.

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

The **complete trace** is a flat sequence of **rounds**:

$$T = (r_0, r_1, r_2, \ldots)$$

Each round $r_k$ records:

- **Node**: which bubble $B_i$ was active
- **Local segment**: the scheduling decisions $B_i$ made during this
  run — a sequence of $(index, bgid, runqSize, runqBgids)$ entries,
  one per decision point
- **Global decision**: what the orchestrator did after $B_i$ went idle

The global decision is one of:

| Type | Fields | Meaning |
|---|---|---|
| **Deliver** | index into pending queue | Which message to deliver next |
| **TimeAdvance** | target time $t$ | Advanced all clocks to $t$ |
| **Done** | node name | A node completed |

Only Deliver carries a decision (the index). TimeAdvance and Done are
deterministic given prior decisions, but recorded for replay.

### Replay

Replaying $T$ from the beginning: for each round $r_k$, run $B_i$
with the recorded local segment as a scheduling prefix, wait for idle,
then execute the recorded global decision. Each round is valid because
the trace up to that round determines the bubble's state and the
pending message queue.

### Branching

To explore an alternative: take $T[0..k-1]$ as prefix, change either
a local decision (different goroutine index within the local segment)
or the global decision (different delivery index) at round $k$, and
run fresh from there. Everything after $k$ is invalidated — the new
execution may produce different local traces and different pending
messages.

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

---

## 8. State Space

This section defines the state space of the system precisely: what
constitutes a state, what constitutes a transition, how the three
dimensions of nondeterminism compose, and how the space relates to
prior work (CHESS, DPOR, FlyMC).

### Definition 8.1: Local State

The **local state** of bubble $B_i$ at decision point $j$ is the tuple:

$$L_i^j = (G_i^j,\; R_i^j,\; S_i^j,\; c_i^j)$$

where:

- $G_i^j = \{b_0, b_1, \ldots, b_m\}$ is the set of live goroutines
  (identified by BGID)
- $R_i^j \subseteq G_i^j$ is the ordered runnable set (the runq
  contents at this decision point)
- $S_i^j$ is the shared state — memory, channel queues,
  synchronization state, and goroutine stacks visible within the bubble
- $c_i^j \in \mathbb{N}$ is the fake clock (nanoseconds)

The local state is fully determined by the program text and the
sequence of decisions made so far within this bubble (Theorem 2.1).

### Definition 8.2: Global State

The **global state** of a distributed test with $k$ nodes at
orchestrator step $p$ is:

$$\Sigma^p = (L_1^{j_1},\; L_2^{j_2},\; \ldots,\; L_k^{j_k},\; M^p,\; t_g^p)$$

where:

- $L_i^{j_i}$ is the local state of node $n_i$ at its current local
  decision index $j_i$
- $M^p = (m_1, m_2, \ldots, m_q)$ is the ordered queue of pending
  messages (messages that have been sent but not yet delivered). Each
  $m_l$ has fields: source node, target node, payload, and position in
  the queue.
- $t_g^p \in \mathbb{N}$ is the global virtual time

The global state is fully determined by the complete trace prefix
$T[0..p-1]$ (Definition 6.3).

**Idle snapshot.** The orchestrator only observes $\Sigma^p$ at idle
points — when all bubbles are frozen in their hooks. Between idle
points, individual bubbles execute internally and their local states
evolve through local decision points. The orchestrator never sees
intermediate local states.

### Definition 8.3: Composite State

The **composite state** at an arbitrary point in execution is:

$$\mathcal{S} = (\Sigma^p,\; a,\; j_a)$$

where:

- $\Sigma^p$ is the global state at the most recent orchestrator step
- $a \in \{1, \ldots, k\} \cup \{\bot\}$ identifies the currently
  active bubble ($\bot$ when the orchestrator is deciding)
- $j_a$ is the current local decision index within the active bubble

A composite state where $a = \bot$ (all bubbles idle) is an
**orchestrator state**. A composite state where $a \neq \bot$ is a
**local execution state** — the active bubble is between consecutive
local decision points.

### Definition 8.4: Transitions

A **transition** is a single nondeterministic choice that moves the
system from one composite state to another. There are three kinds,
corresponding to the three dimensions of nondeterminism:

**Local transition (L-transition).** At local decision point $j$ in
the active bubble $B_a$, the scheduling function picks goroutine
$b_x \in R_a^j$. The choice is an index $\ell \in \{0, 1, \ldots,
|R_a^j| - 1\}$ into the ordered runq. The chosen goroutine runs until
the next yield point, producing local state $L_a^{j+1}$.

$$\mathcal{S} \xrightarrow{\text{L}(\ell)} \mathcal{S}'
\quad\text{where}\quad
\mathcal{S}.a \neq \bot,\;
\ell \in \{0,\ldots,|R_a^j|-1\}$$

**Global transition (G-transition).** At an orchestrator state (all
bubbles idle, $a = \bot$), the orchestrator picks a message
$m_x \in M^p$ to deliver. The choice is an index
$g \in \{0, 1, \ldots, |M^p| - 1\}$ into the pending queue. The
message is delivered to its target bubble, which then becomes active.

$$\mathcal{S} \xrightarrow{\text{G}(g)} \mathcal{S}'
\quad\text{where}\quad
\mathcal{S}.a = \bot,\;
g \in \{0,\ldots,|M^p|-1\}$$

**Select transition (S-transition).** When a goroutine executes a
`select` statement with $s > 1$ ready cases, the runtime picks which
case fires. The choice is an index $e \in \{0, 1, \ldots, s-1\}$ into
the ready cases. The goroutine does not park — it continues execution
with the selected case.

$$\mathcal{S} \xrightarrow{\text{S}(e)} \mathcal{S}'
\quad\text{where}\quad
\mathcal{S}.a \neq \bot,\;
e \in \{0,\ldots,s-1\}$$

**Deterministic transitions.** When $|R_a^j| = 1$ (only one runnable
goroutine), $|M^p| = 0$ (no pending messages — time advance), or
$s = 1$ (only one ready select case), no choice exists. These
transitions are deterministic and do not contribute to state space
branching.

### Definition 8.5: State Space Graph

The **state space graph** is a directed acyclic graph:

$$\mathcal{G} = (\mathcal{V}, \mathcal{E})$$

where:

- $\mathcal{V}$ is the set of all reachable composite states from the
  initial state $\mathcal{S}_0$
- $\mathcal{E} \subseteq \mathcal{V} \times \mathcal{V}$ is the set
  of transitions. Each edge
  $(\mathcal{S}, \mathcal{S}') \in \mathcal{E}$ is labeled with the
  transition type and index: $\text{L}(\ell)$, $\text{G}(g)$, or
  $\text{S}(e)$.

The **initial state** $\mathcal{S}_0$ has all bubbles started but not
yet past their first scheduling decision, $M^0 = \emptyset$, and
$t_g^0 = 0$.

A **terminal state** $\mathcal{S}_f$ is one where all bubbles have
completed (all goroutines exited or in deadlock) and $M^f = \emptyset$.

The graph is a DAG because local time advances monotonically (decision
indices and fake clocks never decrease), so no state is visited twice
on any path.

### Definition 8.6: Composition of Dimensions

The three dimensions compose by **interleaving**, not by product.

In a Cartesian product, every combination of local-schedule $\times$
global-delivery $\times$ select-choice would be a valid execution. In
our system, the choices are **causally dependent**: a global delivery
changes which goroutines are runnable (affecting subsequent local
choices), and a local scheduling decision may cause a goroutine to
send a message (affecting subsequent global choices). A select choice
may cause a goroutine to take a branch that creates new goroutines or
sends messages.

Formally, the state space graph is the **unfolding** of the
nondeterministic transition system:

$$\mathcal{S}_0
  \xrightarrow{t_1} \mathcal{S}_1
  \xrightarrow{t_2} \mathcal{S}_2
  \xrightarrow{t_3} \cdots
  \xrightarrow{t_n} \mathcal{S}_f$$

where each $t_i$ is an L-, G-, or S-transition. The type and
branching factor of $t_i$ depend on $\mathcal{S}_{i-1}$, which
depends on all prior transitions. The state space is a **tree** when
explored statelessly (no state caching) and a **DAG** when equivalent
states are merged.

**Alternation structure.** Due to the one-bubble-active-at-a-time
invariant, transitions strictly alternate between phases:

1. **Global phase**: one G-transition (orchestrator picks a message),
   or a deterministic time advance.
2. **Local phase**: a sequence of L-transitions and S-transitions
   within the resumed bubble, until it goes idle.

This gives the trace its round structure (Definition 6.3): each round
is one G-transition followed by zero or more L/S-transitions.

### Definition 8.7: Trace

A **trace** (equivalently, an **execution** or **interleaving**) is a
path from $\mathcal{S}_0$ to a terminal state $\mathcal{S}_f$ in the
state space graph:

$$T = (t_1, t_2, \ldots, t_n)$$

where each $t_i$ is a transition. The trace encodes every
nondeterministic choice made during the execution. Two traces are
**distinct** if they differ in at least one transition.

The trace decomposes into the round structure of Definition 6.3:

$$T = (r_0, r_1, r_2, \ldots, r_P)$$

Each round $r_p$ consists of:

- A global decision $g_p$ (the G-transition, or a deterministic time
  advance / done marker)
- A local segment $\lambda_p = (\ell_1, \ell_2, \ldots, \ell_{n_p})$
  of L-transitions and S-transitions within the resumed bubble

The **complete trace** from Definition 6.3 is exactly this: the flat
sequence of rounds, each carrying its node identifier, local segment,
and global decision.

### Definition 8.8: Branching

**Branching** is the act of generating a new trace that differs from
a known trace at exactly one decision point.

Given a trace $T = (t_1, \ldots, t_n)$ and a position $i$ where
$t_i$ has alternatives (the branching factor at $\mathcal{S}_{i-1}$
is $> 1$):

1. **Prefix**: Keep $(t_1, \ldots, t_{i-1})$ unchanged.
2. **Branch**: Replace $t_i$ with an alternative choice $t_i'$
   ($t_i' \neq t_i$, same type, valid at $\mathcal{S}_{i-1}$).
3. **Suffix**: Replay the prefix, execute the new choice, then
   continue with fresh (default FIFO) decisions from
   $\mathcal{S}_i'$ onward.

The suffix is **invalidated** — it cannot be reused from the original
trace because the new choice at position $i$ may produce a different
state $\mathcal{S}_i' \neq \mathcal{S}_i$, leading to different
runnable sets, different messages, different select cases.

In the implementation, branching at a local decision within a round
requires replaying both the global prefix (all rounds before $r_p$)
and the local prefix (all local decisions before position $i$ within
$r_p$). Branching at a global decision requires replaying only the
global prefix.

### Definition 8.9: Trace Equivalence

Two traces $T_1$ and $T_2$ are **observationally equivalent**,
written $T_1 \sim T_2$, if they produce the same observable outcome:

$$T_1 \sim T_2 \iff O(T_1) = O(T_2)$$

where $O(T)$ is the **observable outcome** of trace $T$. The
definition of $O$ depends on the property being checked:

| Property | Observable $O(T)$ |
|---|---|
| Safety (assertion) | Whether any assertion fails during $T$ |
| Final state | The shared state $S_i^{\text{final}}$ of each bubble at termination |
| Output | The sequence of externally visible side effects (log messages, RPC responses) |
| Linearizability | Whether the history of operations admits a linearization |

**Mazurkiewicz equivalence.** Two traces $T_1$ and $T_2$ are
**Mazurkiewicz equivalent** if one can be obtained from the other by
swapping adjacent independent transitions. Transitions $t_a$ and
$t_b$ are **independent** if:

1. They are both enabled in the same state, and
2. Executing them in either order produces the same resulting state:
   $\mathcal{S} \xrightarrow{t_a} \xrightarrow{t_b} \mathcal{S}''$
   and
   $\mathcal{S} \xrightarrow{t_b} \xrightarrow{t_a} \mathcal{S}''$.

Mazurkiewicz equivalence is strictly finer than observational
equivalence: $T_1 \equiv_M T_2 \implies T_1 \sim T_2$, but not
conversely. Two traces may produce the same observable outcome via
entirely different mechanisms.

**Independence in our model:**

- **L-transitions in different bubbles**: Always independent (bubbles
  share no memory). But they never execute concurrently due to the
  one-active-bubble invariant, so this independence is not directly
  exploitable.
- **G-transitions delivering to different nodes**: Independent if
  the messages do not causally depend on each other. Delivering
  $m_1 \to n_1$ then $m_2 \to n_2$ produces the same state as
  $m_2 \to n_2$ then $m_1 \to n_1$, provided neither delivery
  generates a message that the other delivery consumes.
- **L-transitions within the same bubble**: Two goroutines touching
  disjoint state (different channels, different memory) are
  independent. But our granularity is yield-to-yield quanta, not
  individual instructions, so independence analysis requires tracking
  the aggregate memory footprint of each quantum — which we do not
  currently do.
- **S-transitions**: Independent of L-transitions at other decision
  points (a select choice is local to the goroutine making it).
  Dependent on subsequent L-transitions within the same bubble (the
  chosen case determines what the goroutine does next).

### Definition 8.10: State Space Size

The **state space size** $|\mathcal{V}|$ — the number of distinct
composite states reachable from $\mathcal{S}_0$ — depends on the
program. We bound the number of **distinct traces** (paths through
the graph), which is the quantity that matters for stateless
exploration.

#### Unbounded

For a single bubble with $n$ goroutines making $D$ total decision
points where each has at most $n$ choices:

$$|\text{Traces}| \leq n^D$$

For $k$ bubbles and $P$ orchestrator steps with at most $Q$ pending
messages at each:

$$|\text{Traces}| \leq \prod_{p=1}^{P} Q_p \cdot \prod_{i=1}^{k} n_i^{D_i}$$

where $Q_p$ is the number of pending messages at global step $p$,
$n_i$ is the max goroutine count in bubble $i$, and $D_i$ is the
number of local decision points in bubble $i$.

If select transitions are included, with $s_j$ ready cases at each
select point and $E_i$ total select points in bubble $i$:

$$|\text{Traces}| \leq \prod_{p=1}^{P} Q_p \cdot \prod_{i=1}^{k} \left( n_i^{D_i} \cdot \prod_{j=1}^{E_i} s_j \right)$$

These are loose upper bounds. The actual count is far smaller because:

1. Most decision points have $|R| = 1$ (no choice).
2. Many traces converge to the same state (DAG, not tree).
3. Independent transitions produce equivalent traces.

#### With Context Bound $K$

**Context bounding** restricts exploration to traces with at most $K$
non-default (non-FIFO) decisions. This is the key reduction.

At each decision point with branching factor $f$ (number of runnable
goroutines, pending messages, or ready select cases), there is 1
default choice (FIFO / index 0) and $f - 1$ non-default choices.
A non-default choice consumes one unit of the context budget.

The number of traces with at most $K$ non-default decisions, given
$D_{\text{total}}$ total decision points with branching factor $> 1$
and maximum branching factor $f_{\max}$:

$$|\text{Traces}_{K}| \leq \sum_{j=0}^{K} \binom{D_{\text{total}}}{j} \cdot (f_{\max} - 1)^j$$

This is polynomial in $D_{\text{total}}$ for fixed $K$:

$$|\text{Traces}_{K}| = O(D_{\text{total}}^K \cdot f_{\max}^K)$$

For $K = 2$ (our default):

$$|\text{Traces}_{2}| = O(D_{\text{total}}^2 \cdot f_{\max}^2)$$

This is the CHESS result (Musuvathi & Qadeer, OSDI 2008): preemption
bounding with bound $c$ gives $O(n^c \cdot k^c)$ where $n$ is the
number of steps and $k$ is the number of threads. Our formulation
generalizes it across all three dimensions.

**Shared budget.** In our system, the context bound $K$ is shared
across all three dimensions. A trace that uses one non-FIFO local
decision and one non-FIFO global delivery has used $K = 2$. This
means:

$$K = K_L + K_G + K_S$$

where $K_L$, $K_G$, $K_S$ are the non-default decisions consumed by
local, global, and select transitions respectively.

The implementation currently maintains separate budgets: the local
explorer (explorer.go) tracks $K_L$ per bubble, and the global
explorer (orchestrator) tracks $K_G$. A unified explorer (the
ExploreAll TODO in orchestrator.go) would share a single budget
across all dimensions.

### Definition 8.11: Comparison with Prior Work

#### CHESS (OSDI 2008)

**State:** CHESS is stateless — it does not maintain an explicit state
representation. The "state" is implicitly defined by the sequence of
scheduling decisions (the trace prefix). Replay reconstructs state
from scratch.

**Transition:** A preemption — forcing a context switch at a program
point that would not naturally yield. Between preemptions, threads run
to their next synchronization point. CHESS's scheduling unit is the
**voluntary yield-to-yield** execution of a thread, identical to our
L-transition quantum.

**Space:** $O(n^c \cdot k^c)$ traces for $c$ preemptions, $n$ steps,
$k$ threads. This directly corresponds to our context-bounded space
(Definition 8.10) with $K = c$ and only L-transitions (single-process
CHESS does not have G-transitions).

**Mapping to our model:**

| CHESS concept | Our concept |
|---|---|
| Thread | Goroutine (BGID) |
| Preemption point | L-transition with non-FIFO choice |
| Fair scheduling | FIFO default ($\ell = 0$) |
| Preemption bound $c$ | Context bound $K$ (L-transitions only) |
| Happens-before trace | Trace $T$ (Definition 8.7) |
| Three-phase iteration | Prefix replay + fresh suffix (Definition 8.8) |

CHESS uses happens-before caching to skip traces that produce
equivalent partial orders. We do not currently implement this but
could: at each branching point, hash the happens-before graph of the
prefix and skip branches that produce a cached hash.

#### DPOR (POPL 2005)

**State:** DPOR is also stateless but maintains a **backtrack set**
$B(s)$ at each state $s$ on the current DFS path. $B(s)$ contains
transitions that must still be explored from $s$.

**Transition:** An atomic action by one process (read, write, lock,
unlock, send, receive). DPOR's granularity is finer than ours: each
memory access is a separate transition. In our model, one L-transition
(a full yield-to-yield quantum) may contain many memory accesses.

**Space:** DPOR explores exactly one trace per **Mazurkiewicz
equivalence class** (in the optimal variant). The number of
equivalence classes is at most $|\text{Traces}|$ but often
exponentially smaller.

**Mapping to our model:**

| DPOR concept | Our concept |
|---|---|
| Process | Goroutine or node |
| Atomic action | One quantum (yield-to-yield) for L-transitions; one delivery for G-transitions |
| Independence | G-transitions to disjoint nodes; L-transitions touching disjoint state |
| Backtrack set $B(s)$ | Branching alternatives at decision point (Definition 8.8) |
| Sleep set | Not implemented (future: skip known-equivalent branches) |
| Persistent set | Not implemented (future: subset of runq that covers all conflicts) |

**Key difference:** DPOR requires knowing which transitions conflict
(touch the same memory location). For G-transitions, we can determine
this structurally: messages to different nodes are independent. For
L-transitions, we would need to instrument memory accesses within each
quantum — which we do not do. Our current approach (context bounding)
is complementary: it limits depth rather than pruning by independence.

**Combining DPOR and context bounding.** Abdulla et al. (TACAS 2023)
show how to reconcile preemption bounding with DPOR. The idea: DPOR
prunes independent reorderings, bounding prunes deep reorderings.
Together they explore fewer traces than either alone. This is directly
applicable to our system — DPOR for G-transitions (message
independence) combined with context bounding for L-transitions
(goroutine scheduling depth).

#### FlyMC (EuroSys 2019)

**State:** FlyMC uses an **abstract state** — the application state
with node identities removed. Two states that differ only in which
follower holds a value are considered equivalent (symmetry reduction).
FlyMC also uses a **state-event cache**: if state $s$ with pending
event set $E$ has been explored, skip it.

**Transition:** Delivery of one network event (message, timer, crash,
reboot). FlyMC operates exclusively at the G-transition level — it
does not control intra-node scheduling. Each node is a black box that
runs to quiescence after receiving an event.

**Space:** FlyMC reduces the space through three orthogonal
techniques:

1. **Symmetry**: Collapse states differing only by node permutation.
   Reduction factor up to $k!$ for $k$ symmetric nodes.
2. **Independence**: Messages to disjoint nodes with disjoint
   read/update/send/disk sets commute. Reduction factor depends on
   system structure.
3. **Parallel flips**: Reorder a pair of events at all nodes
   simultaneously. Explores more states per path.

**Mapping to our model:**

| FlyMC concept | Our concept |
|---|---|
| Node (black box) | Bubble (with internal scheduling hidden) |
| Event | G-transition (message delivery) |
| Abstract state | $\Sigma^p$ with node identities permuted |
| Event independence | G-transitions to disjoint nodes (Definition 8.9) |
| Parallel flip | Branching at multiple G-transitions simultaneously (not yet implemented) |
| Quiescence detection | Bubble idle detection (all goroutines blocked) |

**Key difference:** FlyMC ignores local scheduling — each node runs
deterministically (or nondeterminism is hidden). We control both
levels. FlyMC's symmetry and independence reductions apply directly to
our G-transitions. The combination — FlyMC-style reduction for
G-transitions, CHESS-style bounding for L-transitions — is our
system's unique contribution.

### Definition 8.12: Summary

The state space of our system is a DAG whose nodes are composite
states $\mathcal{S} = (\Sigma^p, a, j_a)$ and whose edges are
transitions of three kinds: local (L), global (G), and select (S).
The three dimensions compose by causal interleaving, not Cartesian
product. Context bounding with budget $K$ reduces the explorable space
from exponential to $O(D^K \cdot f^K)$, polynomial for fixed $K$.

| Property | Value |
|---|---|
| **State** | $(\Sigma^p, a, j_a)$ — global snapshot + active bubble + local position |
| **Transition** | L$(\ell)$, G$(g)$, or S$(e)$ — index-based choice |
| **Graph** | DAG, tree when explored statelessly |
| **Composition** | Causal interleaving (not product) |
| **Unbounded size** | $\prod Q_p \cdot \prod n_i^{D_i} \cdot \prod s_j$ |
| **Bounded size ($K$)** | $O(D_{\text{total}}^K \cdot f_{\max}^K)$ |
| **CHESS analog** | L-transitions = preemption points, $K$ = preemption bound |
| **DPOR analog** | G-transitions to disjoint nodes = independent transitions |
| **FlyMC analog** | Bubbles = black-box nodes, G-transitions = events |
