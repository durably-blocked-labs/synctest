# Formal Model of the Global Orchestrator

This document gives mathematical definitions for the global orchestrator in `orchestrator/`. The goal is to make the invariants and correctness conditions precise.

---

## 1. Primitives

### 1.1 Nodes

Let $N = (n_1, n_2, \ldots, n_k)$ be the ordered sequence of nodes in registration order. The ordering is fixed and used throughout to eliminate non-determinism.

### 1.2 Bubbles

Each node $n_i$ executes inside a **bubble** $B_i$ — an isolated synctest environment with its own fake clock, goroutine set, and idle detector.

Let $G(B_i)$ denote the set of goroutines in bubble $B_i$.

A goroutine $g \in G(B_i)$ is **durable-blocked** if it is waiting on a bubble-internal channel, a select statement, or `synctest.ExternalWait`.

The bubble is **idle** when every goroutine is durable-blocked:

$$\text{Idle}(B_i) \iff \forall\, g \in G(B_i) : \text{durableBlocked}(g)$$

When $\text{Idle}(B_i)$ becomes true, the synctest runtime calls the bubble's decision hook, which transmits an `IdleState` to the orchestrator and blocks until a `Resume` message is received.

### 1.3 Fake clock

Each bubble $B_i$ maintains a fake clock $c_i \in \mathbb{Z}_{\geq 0}$ (nanoseconds). The orchestrator advances it by sending `Resume{AdvanceTimeTo: τ'}`, setting $c_i \leftarrow \tau'$.

Let $\text{nextTimer}(B_i)$ denote the earliest pending timer deadline (nanoseconds) inside bubble $B_i$ at the moment it becomes idle. $\text{nextTimer}(B_i) = 0$ when no timers are pending.

---

## 2. Global State

The global state is the tuple

$$S = (\tau,\; \Sigma,\; Q,\; P,\; \mathcal{U},\; \mathcal{G})$$

| Symbol | Type | Meaning |
|--------|------|---------|
| $\tau$ | $\mathbb{Z}_{\geq 0}$ | Current global fake time (nanoseconds) |
| $\Sigma$ | $N \to \\\{\texttt{running},\, \texttt{idle},\, \texttt{done}\\\}$ | Per-node execution status |
| $Q$ | $\text{PendingOp}^*$ | Ordered list of schedulable operations |
| $P$ | $N \rightharpoonup \text{IdleState}$ | Partial map: idle nodes to their reported idle state |
| $\mathcal{U}$ | $\text{NewStep}^*$ | Unified trace: all recorded decisions (local + global) |
| $\mathcal{G}$ | $\text{GlobalStep}^*$ | Event log: delivery, advance, and done events (for metrics) |

**Active nodes**: $A = \\\{n_i \in N \mid \Sigma(n_i) \neq \texttt{done}\\\}$

**Fully idle**: all active nodes have reported idle, i.e., $\text{dom}(P) = A$.

---

## 3. Decision Points and Algorithms

### 3.1 Decision kinds

The orchestrator makes two kinds of scheduling decisions:

- **Local** (`Kind = Local`): which goroutine runs next inside bubble $B_i$, when multiple goroutines are simultaneously runnable.
- **Global** (`Kind = Global`): which pending operation to deliver next, when all active nodes are idle and $Q \neq \emptyset$.

A **decision point** is a tuple

$$d = (\text{kind},\; \text{step},\; \text{node},\; \text{alts})$$

where $\text{kind} \in \\\{\text{Local}, \text{Global}\\\}$, $\text{step} \in \mathbb{N}$ is the 0-indexed position in $\mathcal{U}$, $\text{node} \in N$ is the active bubble (empty string for Global), and $\text{alts}$ is the non-empty list of choosable alternatives.

Each alternative $a \in \text{alts}$ carries a stable string ID, and for Global decisions the sender/receiver node names and RPC type.

### 3.2 Algorithm interface

An **algorithm** $\mathcal{A}$ is a stateful object satisfying:

- $\mathcal{A}.\text{BeforeRun}() \to \text{bool}$ — called before each run; returns false to stop exploration.
- $\mathcal{A}.\text{Decide}(d) \to \mathbb{N}$ — called at each decision point; returns the 0-indexed choice from $\text{alts}$.
- $\mathcal{A}.\text{AfterRun}(\text{result})$ — called after each run with the run outcome.

`Decide` may panic with `replayDivergenceError` to signal that the execution has diverged from an expected prefix. The orchestrator catches this panic, marks the run as `Diverged`, and calls `AfterRun`.

Concrete algorithms:

| Algorithm | Strategy |
|-----------|----------|
| **FIFO** | Always returns 0 (no exploration). Used by `Run`. |
| **CHESS** | Context-bounded DFS over Local and/or Global decisions. |
| **PCT** | Randomized priority assignment (Priority-based Concurrency Testing). |
| **DPOR** | Dynamic partial-order reduction, pruning equivalent interleavings. |

### 3.3 Pending operation

A **pending operation** is a tuple

$$p = (\text{from},\; \text{to},\; \text{type},\; \text{exec})$$

where $\text{from}, \text{to} \in N$ identify sender and receiver, $\text{type}$ is the RPC name, and $\text{exec} : () \to ()$ is the delivery closure (writes to the receiver's mailbox).

Each node $n_i$ exposes an outbox channel. The drain step non-blockingly reads **all** available operations from each outbox in $N$-order and appends them to $Q$:

$$\text{Drain}(S) : Q \leftarrow Q \cdot \bigl(\text{all}(\text{out}_1) \cdot \ldots \cdot \text{all}(\text{out}_k)\bigr)$$

where $\text{all}(\text{out}_i)$ is the sequence of all ops currently buffered in $n_i$'s outbox channel.

---

## 4. Unified Trace and Event Log

### 4.1 Unified trace

The **unified trace** $\mathcal{U} = (u_1,\, u_2,\, \ldots,\, u_m)$ records every scheduling decision from one run as `NewStep` records:

$$u_j = (\text{kind},\; \text{node},\; \text{index},\; \text{alternatives},\; \text{chosenID},\; \text{resource},\; \ldots)$$

where $\text{index}$ is the chosen alternative (0 = FIFO/default) and $\text{alternatives}$ is how many choices existed. This trace is used by `ExploreWith` for systematic prefix replay.

### 4.2 Global event log

The **global event log** $\mathcal{G}$ is a separate sequence used only for metrics and compatibility:

$$e_j \in \\\{\;\text{Deliver}(\text{from},\, \text{to},\, \text{type},\, \tau),\;\;\text{Advance}(\tau),\;\;\text{Done}(n_i)\;\\\}$$

$\mathcal{G}$ is not used for replay.

---

## 5. Orchestrator Transition System

The orchestrator is a labelled transition system $(\mathcal{S}, S_0, \Delta)$ where $\mathcal{S}$ is the set of global states, $S_0$ is the initial state, and $\Delta$ is the transition relation defined below.

### 5.1 Startup (sequential)

For $i = 1, \ldots, k$ in registration order:

- **Precondition**: $\Sigma(n_i) = \texttt{init}$
- **Effect**: attach local scheduler hook to $B_i$; start $B_i$; await `IdleState` or `done(n_i)`.
  - If `IdleState` received: set $P \leftarrow P[n_i \mapsto \text{idleState}(B_i)]$.
  - If `done(n_i)`: set $A \leftarrow A \setminus \\\{n_i\\\}$, append $\text{Done}(n_i)$ to $\mathcal{G}$.

After this phase every active node satisfies $n_i \in \text{dom}(P)$.

### 5.2 Collect idle states

Wait until $\text{dom}(P) = A$. For any $n_i \in A \setminus \text{dom}(P)$, fire the matching rule:

- If `IdleState` received: drain local steps from $B_i$'s hook buffer; set $P \leftarrow P[n_i \mapsto \text{idleState}(B_i)]$.
- If `done(n_i)`: drain local steps and outbox; set $A \leftarrow A \setminus \\\{n_i\\\}$; append $\text{Done}(n_i)$ to $\mathcal{G}$.

### 5.3 Local decisions (synchronous)

Local decisions are made **synchronously** inside each bubble's hook callback. When bubble $B_i$ reaches a goroutine scheduling point with $r \geq 1$ runnable goroutines, the callback fires while the orchestrator goroutine is blocked in `waitNodeEvent`:

1. Construct decision point $d = (\text{Local},\; \text{traceStep},\; n_i,\; \text{alts})$ where $\text{alts}$ lists all runnable goroutines by bubble goroutine ID.
2. Call $\text{idx} \leftarrow \mathcal{A}.\text{Decide}(d)$.
3. Append corresponding `NewStep` with $\text{kind} = \text{Local}$ to $\mathcal{U}$; increment $\text{traceStep}$.
4. Return $\text{idx}$ to the runtime — the goroutine at position $\text{idx}$ in the run queue runs next.

Because local callbacks fire synchronously inside the hook (no channel send), they do not interleave with the orchestrator's main loop.

### 5.4 Drain and deliver

**Precondition**: $\text{dom}(P) = A$ and $Q \neq \emptyset$ (after $\text{Drain}(S)$).

1. Construct decision point $d = (\text{Global},\; \text{traceStep},\; \emptyset,\; \text{alts}(Q))$.
2. Call $\text{idx} \leftarrow \mathcal{A}.\text{Decide}(d)$ — or panic with `replayDivergenceError` if diverged.
3. Let $p = Q[\text{idx}]$; remove $p$ from $Q$.
4. Append `NewStep` with $\text{kind} = \text{Global}$, $\text{index} = \text{idx}$, to $\mathcal{U}$; increment $\text{traceStep}$.
5. $p.\text{exec}()$ — write message to target mailbox (safe because $B_{p.\text{to}}$ is frozen idle).
6. Append $\text{Deliver}(p.\text{from}, p.\text{to}, p.\text{type}, \tau)$ to $\mathcal{G}$.
7. If $n_{p.\text{to}} \in \text{dom}(P)$:
   - Delete $n_{p.\text{to}}$ from $P$.
   - Send `Resume{}` to $B_{p.\text{to}}$.
   - Await `IdleState` or `done(n_{p.\text{to}})`.
   - Retry loop: if the bridge goroutine (in `ExternalWait`) has not yet reattached, a spurious idle arrives. Resume again until `HasNewLocal()` confirms the message was processed, with a 1 ms wall-clock deadline.

### 5.5 Time advance (sequential)

**Precondition**: $Q = \emptyset$ and $\exists\, n_i \in A : \text{nextTimer}(P[n_i]) > 0$.

Compute the target time:

$$\tau' = \min_{\substack{n_i \in A \\ \text{nextTimer}(P[n_i]) > 0}} \text{nextTimer}(P[n_i])$$

Set $\tau \leftarrow \tau'$ and append $\text{Advance}(\tau')$ to $\mathcal{G}$.

Then, for $i = 1, \ldots, k$ in registration order, if $n_i \in \text{dom}(P)$:

1. Delete $n_i$ from $P$.
2. Send `Resume{AdvanceTimeTo: τ'}` to $B_i$.
3. Await `IdleState` or `done(n_i)`.
4. Update $P$ accordingly (same rules as §5.2).

### 5.6 Drain (no timers, no pending ops)

**Precondition**: $Q = \emptyset$ and $\forall\, n_i \in A : \text{nextTimer}(P[n_i]) = 0$.

For $i = 1, \ldots, k$ in registration order, if $n_i \in \text{dom}(P)$:

1. If $P[n_i].\text{ExternalWait} > 0$: call `Shutdown()` on $n_i$'s transport to unblock goroutines waiting in `ExternalWait`.
2. Delete $n_i$ from $P$.
3. Send `Resume{DelegateIdle: true}` to $B_i$ — delegates idle handling to the synctest runtime so it can advance its own clock or detect deadlock.
4. Await `IdleState` or `done(n_i)`.
5. Update $P$ accordingly (same rules as §5.2).

### 5.7 Termination

The orchestrator terminates when $A = \emptyset$.

---

## 6. Determinism

### 6.1 Conditions for trace equality

Let $R_1$ and $R_2$ be two runs of the same test with the same algorithm and initial configuration. Their unified traces $\mathcal{U}_1$ and $\mathcal{U}_2$ are equal if and only if:

1. **Local schedule identity**: for every node $n_i$, `Algorithm.Decide` returns the same index at every Local decision point.

2. **Global delivery identity**: `Algorithm.Decide` returns the same index at every Global decision point, selecting the same message $(\text{from}, \text{to}, \text{type})$ at the same queue position.

3. **Application randomness identity**: any `math/rand` calls made by application code use the same seed and occur in the same order.

If any condition fails, $\mathcal{U}_1 \neq \mathcal{U}_2$ in general. Condition (3) is not enforced by the orchestrator; callers must seed `math/rand` deterministically in the `setup` callback.

### 6.2 Replay

`Replay(rec)` re-runs the system following only the Global decisions from `rec.GlobalDecisions`. For each Global decision point the replay function:

- Checks that the recorded `QueueSize` matches the current $|Q|$; panics with `replayDivergenceError` on mismatch.
- Returns the recorded index.

Local decisions always return 0 (FIFO) during replay. Replay is **sound** when conditions (1)–(3) hold. When a `replayDivergenceError` is caught, `RunResult.Diverged = true` and the run is discarded.

### 6.3 Context-bounded exploration (CHESS)

CHESS enumerates scheduling interleavings by DFS over prefixes. A prefix is a `Trace` — a sequence of `NewStep` records. At each step in the replay phase CHESS checks that `Kind` and `Node` match; on mismatch it panics with `replayDivergenceError`, pruning that branch.

In **GlobalOnly** mode the prefix contains only Global steps; Local decisions always return 0. This is cheaper but misses intra-bubble concurrency bugs. In full mode the prefix contains all steps (Local and Global); divergences due to non-deterministic local step counts are pruned automatically.

The **context bound** $b$ limits the number of non-zero indices in any prefix. Most concurrency bugs manifest with $b \leq 2$.

---

## 7. Key Invariants

**I1 — At most one active bubble**: At any point in the delivery or time-advance phase, at most one bubble has $\Sigma(n_i) = \texttt{running}$. Local decisions fire synchronously inside the hook while the orchestrator goroutine is blocked, preserving this property.

**I2 — Sequential rand access**: No two goroutines from distinct bubbles call `rand` functions concurrently. This follows from I1 and the sequential resumption rule.

**I3 — Message causality**: A message $p$ is only delivered after the sender's bubble has gone idle with $p$ in its outbox, ensuring no message is observed before it is sent.

**I4 — Monotone clock**: $\tau$ is non-decreasing. Each `Resume{AdvanceTimeTo: τ'}` satisfies $\tau' \geq \tau$.

**I5 — No concurrent cross-bubble writes**: `p.exec()` is called by the orchestrator goroutine while the target bubble is frozen idle, so the write to the target's mailbox is safe without additional synchronisation.

**I6 — Algorithm controls all decisions**: Every scheduling choice — both Local (goroutine order within a bubble) and Global (message delivery order across bubbles) — flows through `Algorithm.Decide`. This is the single point of control for systematic exploration.
