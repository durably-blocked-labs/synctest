# Formal Model of the Global Orchestrator

This document gives mathematical definitions for the global orchestrator in `orchestratorv2`. The goal is to make the invariants and correctness conditions precise.

---

## 1. Primitives

### 1.1 Nodes

Let $N = (n_1, n_2, \ldots, n_k)$ be the ordered sequence of nodes in registration order. The ordering is fixed and used throughout to eliminate non-determinism.

### 1.2 Bubbles

Each node $n_i$ executes inside a **bubble** $B_i$ — an isolated synctest environment with its own fake clock, goroutine set, and idle detector.

Let $G(B_i)$ denote the set of goroutines in bubble $B_i$.

A goroutine $g \in G(B_i)$ is **durable-blocked** if it is waiting on:
- A bubble-internal channel or select statement, or
- `synctest.ExternalWait`.

The bubble is **idle** when every goroutine is durable-blocked:

$$\text{Idle}(B_i) \iff \forall\, g \in G(B_i) : \text{durableBlocked}(g)$$

When $\text{Idle}(B_i)$ becomes true, the synctest runtime calls the bubble's decision hook, which transmits an $\text{IdleState}$ to the orchestrator and blocks until a $\text{Resume}$ is received.

### 1.3 Fake clock

Each bubble $B_i$ maintains a fake clock value $c_i \in \mathbb{R}_{\geq 0}$. The orchestrator advances it by sending $\text{Resume}\{t'\}$, setting $c_i \leftarrow t'$.

Let $\text{nextTimer}(B_i)$ denote the earliest pending timer deadline inside bubble $B_i$ at the moment it becomes idle ($\infty$ if no timers are pending).

---

## 2. Global State

The global state is the tuple

$$S = \bigl(\tau,\; \Sigma,\; Q,\; P\bigr)$$

| Symbol | Type | Meaning |
|---|---|---|
| $\tau$ | $\mathbb{R}_{\geq 0}$ | Current global fake time |
| $\Sigma$ | $N \to \{\texttt{running},\, \texttt{idle},\, \texttt{done}\}$ | Per-node execution status |
| $Q$ | $\text{PendingOp}^*$ | Ordered queue of schedulable operations |
| $P$ | $N \rightharpoonup \text{IdleState}$ | Partial map: idle nodes $\to$ their reported idle state |

**Active nodes**: $A = \{n_i \in N \mid \Sigma(n_i) \neq \texttt{done}\}$

**Fully idle**: all active nodes have reported idle, $\text{dom}(P) = A$.

---

## 3. Operations

### 3.1 Pending operation

A **pending operation** is a tuple

$$p = (\text{from},\; \text{to},\; \text{type},\; \text{exec})$$

where $\text{from}, \text{to} \in N$ identify sender and receiver, $\text{type} \in \Sigma^*$ is the RPC name, and $\text{exec} : () \to ()$ is the delivery closure (writes to the receiver's mailbox).

Each node $n_i$ exposes an outbox $\text{out}_i$ from which the orchestrator drains pending operations. Let $\text{head}(\text{out}_i)$ take at most one element non-blockingly (returning $\varepsilon$ if empty). The drain step appends each non-empty head in $N$-order:

$$\text{Drain}(S) : Q \leftarrow Q \cdot \bigl(\text{head}(\text{out}_1),\; \ldots,\; \text{head}(\text{out}_k)\bigr)$$

where $\cdot$ denotes sequence concatenation (empty results are dropped).

### 3.2 Selection function

The orchestrator picks the next operation via a **selection function** $\text{sel} : \text{PendingOp}^* \to \text{PendingOp}$:

$$\text{sel}_{\text{FIFO}}(Q) = Q[0]$$

$$\text{sel}_{\text{replay}}(Q,\, j) = \arg\min_{p \in Q}\;\text{rank}(p,\, \mathcal{T}_{\text{rec}},\, j)$$

where $j$ is the index of the next unmatched $\text{Deliver}$ step in the recorded trace $\mathcal{T}_{\text{rec}}$, and $\text{rank}$ returns the position of the first step in $\mathcal{T}_{\text{rec}}[j:]$ whose $(\text{from}, \text{to}, \text{type})$ matches $p$, or $\infty$ if no match exists (triggering FIFO fallback).

---

## 4. Global Trace

A **global trace** is a finite sequence of labelled events:

$$\mathcal{T} = (s_1,\, s_2,\, \ldots,\, s_m)$$

Each event $s_j$ has one of three forms:

$$s_j \in \bigl\{\;\text{Deliver}(\text{from},\, \text{to},\, \text{type},\, \tau),\;\;\text{Advance}(\tau),\;\;\text{Done}(n_i)\;\bigr\}$$

Two traces are **equivalent**, $\mathcal{T}_1 \sim \mathcal{T}_2$, if they are equal as sequences. Equivalence is the correctness condition for replay.

---

## 5. Orchestrator Transition System

The orchestrator is a labelled transition system $(\mathcal{S}, S_0, \Delta)$ where $\mathcal{S}$ is the set of global states, $S_0$ is the initial state, and $\Delta$ is the transition relation defined below.

### 5.1 Startup (sequential)

For $i = 1, \ldots, k$ in registration order:

- **Precondition**: $\Sigma(n_i) = \texttt{init}$
- **Effect**: start bubble $B_i$; await $\text{Idle}(B_i)$ or $\text{done}(n_i)$.
  - If $\text{Idle}(B_i)$: set $P \leftarrow P[n_i \mapsto \text{idleState}(B_i)]$.
  - If $\text{done}(n_i)$: set $A \leftarrow A \setminus \{n_i\}$, append $\text{Done}(n_i)$ to $\mathcal{T}$.

After this phase every active node satisfies $n_i \in \text{dom}(P)$, i.e., all are idle.

### 5.2 Collect idle states

Wait until $\text{dom}(P) = A$. For any $n_i \in A \setminus \text{dom}(P)$, fire the matching rule:

- If $\text{Idle}(B_i)$: set $P \leftarrow P[n_i \mapsto \text{idleState}(B_i)]$.
- If $\text{done}(n_i)$: set $A \leftarrow A \setminus \{n_i\}$, append $\text{Done}(n_i)$ to $\mathcal{T}$.

### 5.3 Drain and deliver

**Precondition**: $\text{dom}(P) = A$ and $Q \neq \emptyset$ (after $\text{Drain}(S)$).

Let $p = \text{sel}(Q)$. Then:

1. $Q \leftarrow Q \setminus \{p\}$
2. $p.\text{exec}()$ — write message to target mailbox
3. $\mathcal{T} \leftarrow \mathcal{T} \cdot \text{Deliver}(p)$
4. $\text{Resume}(B_{p.\text{to}})$
5. $P \leftarrow P \setminus \{p.\text{to}\}$

### 5.4 Time advance (sequential)

**Precondition**: $Q = \emptyset$ and $\exists\, n_i \in A : \text{nextTimer}(P[n_i]) < \infty$.

Compute the target time:

$$\tau' = \min_{n_i \in A}\, \text{nextTimer}(P[n_i])$$

Append $\text{Advance}(\tau')$ to $\mathcal{T}$ and set $\tau \leftarrow \tau'$.

Then, for $i = 1, \ldots, k$ in registration order, if $n_i \in \text{dom}(P)$:

1. $\text{Resume}(B_i,\, \tau')$
2. Await $\text{Idle}(B_i)$ or $\text{done}(n_i)$.
3. Update $P$ accordingly (same rules as §5.2).

### 5.5 Drain (no timers)

**Precondition**: $Q = \emptyset$ and $\forall\, n_i \in A : \text{nextTimer}(P[n_i]) = \infty$.

For $i = 1, \ldots, k$ in registration order, if $n_i \in \text{dom}(P)$:

1. $\text{Resume}(B_i)$
2. Await $\text{Idle}(B_i)$ or $\text{done}(n_i)$.
3. Update $P$ accordingly (same rules as §5.2).

### 5.6 Termination

The orchestrator terminates when $A = \emptyset$.

---

## 6. Determinism

### 6.1 Rand sequence

Let $\rho : \mathbb{N} \to \mathbb{Z}$ be the sequence produced by `math/rand` seeded with value $s$:

$$\rho_s = \bigl(\rho_s(1),\; \rho_s(2),\; \ldots\bigr)$$

Let $\phi(n_i, r)$ denote the $r$-th call to `rand.Int63()` made by goroutines in bubble $B_i$ across the entire run. The orchestrator's sequential resumption rule enforces a strict **inter-bubble ordering** on these calls.

**Proposition**: Under sequential startup and sequential time advance in registration order, the global call sequence is:

$$\phi(n_1, 1),\; \ldots,\; \phi(n_1, r_1),\; \phi(n_2, 1),\; \ldots,\; \phi(n_k, r_k),\; \phi(n_1, r_1 + 1),\; \ldots$$

i.e., each node's calls are fully serialized relative to every other node's calls at each time-advance boundary. The exact value assigned to call $\phi(n_i, r)$ is therefore a deterministic function of the seed $s$ and the position of that call in the global sequence.

### 6.2 Conditions for $\mathcal{T}_1 = \mathcal{T}_2$

Let $R_1$ and $R_2$ be two runs of the same test with the same initial configuration. Their global traces are equal if and only if the following three conditions hold simultaneously:

1. **Seed identity**: both runs use `rand.Seed(s)` for the same $s$, with `randseednop=0`.

2. **Local schedule identity**: for every node $n_i$, the intra-bubble goroutine scheduling decisions are identical. In `Run` this is guaranteed by FIFO. In `Replay` it is enforced by replaying the recorded `Decision.Index` sequence via `WithPrefix`.

3. **Global delivery identity**: for every $\text{Deliver}$ step, the same operation $({\text{from}}, {\text{to}}, {\text{type}})$ is chosen at the same position in $Q$.

If any condition fails, $\mathcal{T}_1 \neq \mathcal{T}_2$ in general:

- Violating (1) changes timer jitter, potentially changing which node wins an election.
- Violating (2) changes the intra-bubble code path, altering when and how many times `rand.Int63()` is called per node.
- Violating (3) changes the order messages are processed, changing application state and subsequent decisions.

### 6.3 Replay soundness

Let $\mathcal{T}_{\text{rec}}$ be the trace produced by `Run` and $\mathcal{T}_{\text{rep}}$ be the trace produced by `Replay(rec)`. Replay is **sound** if conditions (1)–(3) hold, giving $\mathcal{T}_{\text{rep}} = \mathcal{T}_{\text{rec}}$.

Replay is **best-effort** when condition (3) fails to match a recorded op (i.e., $\text{rank}(p, \mathcal{T}_{\text{rec}}, j) = \infty$ for all $p \in Q$): the selection falls back to FIFO and the replay is considered diverged, logging a warning. The run still completes but $\mathcal{T}_{\text{rep}} \neq \mathcal{T}_{\text{rec}}$ is possible.

---

## 7. Key Invariants

**I1 — At most one active bubble**: At any point in the delivery or time-advance phase, at most one bubble has $\Sigma(n_i) = \texttt{running}$.

**I2 — Sequential rand access**: No two goroutines from distinct bubbles call `rand.Int63()` concurrently. This follows from I1 and the sequential resumption rule.

**I3 — Message causality**: A message $p$ is only delivered after the sender's bubble has gone idle with $p$ in its outbox, ensuring no message is observed before it is sent.

**I4 — Monotone clock**: $\tau$ is non-decreasing. Each `Resume{t'}` sends $t' \geq \tau$.

**I5 — No concurrent cross-bubble writes**: `Execute()` is called by the orchestrator goroutine while the target bubble is frozen (idle), so the write to the target's mailbox is safe without additional synchronisation.
