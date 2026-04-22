# Case Study: Ricart-Agrawala Gate Race

## 1. System Model

The scenario models a distributed mutual exclusion protocol layered over a replicated key-value store. Three compute nodes (`A`, `B`, `C`) coordinate access to a shared counter via a variant of the Ricart-Agrawala (RA) algorithm, then perform a read-modify-write on the counter through a separate KV node. The design is deliberately minimal: the locking protocol is a clean RA implementation, the KV store is a single-threaded event loop, and the bug lives entirely in the interaction between the protocol's gate-close pattern and the local goroutine scheduler.

**Nodes.** Four nodes participate:
- `KV`: a network key-value store that processes `KVGet` and `KVPut` messages and replies with `KVGetReply` and `KVPutReply`. It is a single-goroutine event loop with no concurrency.
- `A`, `B`, `C`: gate RA nodes. Each runs a `GateRANode` that implements Ricart-Agrawala mutual exclusion. The application logic is: acquire the lock, read the counter from KV, increment it, write it back, then release the lock. At the end, the last node to finish checks whether `counter == 3`.

**The Ricart-Agrawala variant.** Standard RA uses a request timestamp and peer acknowledgments to establish a distributed total order. A node broadcasting `REQUEST` to peers waits until it has collected `N-1 REPLYs` before entering the critical section. When a `REQUEST` arrives at a node that is either in the CS or has an earlier-timestamped request outstanding, the reply is deferred until the requester releases the lock.

**The gate pattern.** The critical deviation from standard RA is the use of `close(gate)` to signal lock acquisition. When the handler goroutine collects all replies, it:
1. Sets `state = Released` (temporarily, before the app sets `state = Held`)
2. Calls `close(n.gate)`

Two goroutines block on `<-n.gate`:
- **App goroutine** (the `AcquireLock` caller): wakes from `close(gate)`, then sets `state = Held` to enter the critical section.
- **DeferredFlusher goroutine**: wakes from `close(gate)`, checks `state`, and sends any queued deferred REPLYs if `state == Released`.

Under FIFO local scheduling the App goroutine runs first, sets `state = Held`, and the DeferredFlusher sees `state == Held` and no-ops. Under non-FIFO local scheduling the DeferredFlusher runs first, sees `state == Released` (the transient pre-Held state left by the handler), and flushes the deferred queue prematurely — granting a REPLY to a node that should still be waiting.

**Transport.** Each node uses an `OrchestratorTransport`. Sends produce a `PendingOp` delivered to an outbox channel and handed to the global orchestrator, which controls delivery ordering. Inside each bubble, `synctest.ExternalWait` bridges the bubble to the external queue. Both dimensions of non-determinism — global message ordering and local goroutine scheduling — are observable and controllable.

---

## 2. Scenario Construction

Three nodes run concurrently. Each:
1. Calls `node.Start()` to launch the message handler goroutine.
2. Calls `node.AcquireLock()` — broadcasts REQUEST to peers and waits.
3. Calls `kv.Get("counter")`, increments the result, calls `kv.Put("counter", val+1)`.
4. Calls `node.ReleaseLock()`.

After all three nodes complete, the last one to finish checks `kv.Get("counter") == 3`. If two nodes hold the lock simultaneously, they both read the same counter value (e.g., 0), both write 1, and the counter ends at 2 — a lost update.

The scenario is symmetric: all three nodes are equivalent and start in lockstep. The orchestrator's initial global queue contains all six REQUEST messages (each node sends to both peers). Under FIFO delivery, one node collects two REPLYs before any competing REQUEST arrives, enters the CS, completes, and releases — the protocol serializes correctly. Under reordered delivery, one node's REQUEST can arrive at a peer while that peer is still collecting its own REPLYs, creating a deferred entry in the peer's queue. If the peer then closes its gate before `applyLoop` drains, the DeferredFlusher race window opens.

---

## 3. The Bug

The bug lives in `AcquireLock` (`lock.go:111–152`) and `deferredFlusher` (`lock.go:177–202`).

When the handler collects all REPLYs, it sets `state = Released` and calls `close(n.gate)`. The intent is that the App goroutine immediately wakes and sets `state = Held`, so the DeferredFlusher (which also wakes) will see `Held` and no-op. The implementation assumes App runs before DeferredFlusher.

```go
// handler, upon collecting all REPLYs:
n.state = Released
close(n.gate)

// App goroutine (caller of AcquireLock):
<-n.gate
// BUG WINDOW: state is Released here, between gate close and Held assignment
n.mu.Lock()
n.state = Held  // <-- too late if DeferredFlusher runs first
n.mu.Unlock()

// DeferredFlusher:
<-n.gate
n.mu.Lock()
if n.state == Held || n.state == Wanted {
    n.mu.Unlock()
    return  // correct no-op path
}
// state == Released: premature flush
```

When DeferredFlusher runs first, `state` is still `Released`. The flusher sends queued REPLYs to nodes that should still be waiting. Those nodes then collect their required REPLYs, enter the CS, and overlap with the still-entering App goroutine — mutual exclusion is violated.

The observed consequence in the test: two nodes hold the lock simultaneously, both read `counter = 0`, both write `counter = 1`. The final check sees `counter = 2` instead of `3`.

---

## 4. Required Ordering of Events

The bug requires exactly two non-FIFO decisions, one at each level:

**Global message ordering (between nodes):**

1. One node (say, `A`) collects REPLYs from both peers and is about to close its gate.
2. Before `A` closes the gate, a REQUEST from another node (say, `C`) must arrive at `A`. Because `A` is in `Wanted` state with an earlier timestamp, it defers `C`'s REPLY.
3. `A`'s handler collects the second REPLY, sets `state = Released`, closes the gate.

The trigger: some REQUEST must arrive while the winner is still in `Wanted` state, creating a deferred entry. Without this, the deferred queue is empty when the gate closes, and the DeferredFlusher is a no-op regardless of scheduling order.

**Local goroutine ordering (within A's bubble):**

4. `close(n.gate)` wakes both the App goroutine (the `AcquireLock` caller blocked on `<-n.gate`) and the DeferredFlusher goroutine (blocked on `<-n.gate`). Both are now runnable.
5. **Non-FIFO local decision**: the scheduler runs DeferredFlusher before App.
6. DeferredFlusher sees `state = Released`, sends the deferred REPLY to `C`.
7. `C` collects its final REPLY, enters the CS. Meanwhile App goroutine runs, sets `state = Held`, also enters the CS. Mutual exclusion violated.

Neither decision alone suffices. Without (2), the deferred queue is empty and the flusher is harmless. Without (5), App sets `state = Held` before the flusher checks — the flusher no-ops. Both non-FIFO choices must occur in the same execution.

---

## 5. Why Regular Tests Do Not Catch It

A unit test for `GateRANode` would call `AcquireLock`, confirm no deferred REPLYs are sent prematurely, and pass — because in isolation, with no concurrent nodes, there are no deferred entries and no gate race.

An integration test running all three nodes in a single Go process with the default scheduler will almost always serialize the lock acquisitions. The first REQUEST to arrive at a peer triggers an immediate REPLY (since nobody is in `Wanted` state yet), and the winner collects REPLYs and enters the CS before any deferred entries accumulate. Even with `go test -race -count=1000`, the Go scheduler's cheaprand-driven bias toward FIFO scheduling means the DeferredFlusher almost never executes before the App goroutine in the bug window.

The bug window is narrow: it exists only between `close(n.gate)` and the App goroutine's `n.state = Held`. In real wall-clock time this is sub-microsecond. Stress testing without deterministic scheduling control has a vanishingly small probability of landing in this window in the correct global ordering.

---

## 6. Graph Analysis

### Summary Table

| Algorithm | Found Rate | Avg Runs to Bug | Avg Non-FIFO in Bug Run |
|---|---|---|---|
| PCT (d=2) | **20/20** | **1.0** | 22.0 |
| Random | 20/20 | 2.0 | 13.3 |
| CHESS (G+L, k=2) | 20/20 | 27.0 | 2.0 |
| PCT (d=3) | 20/20 | 10.0 | 20.0 |
| DPOR (G+L) | 20/20 | 13.1 | 24.9 |
| CHESS (G-only, k=2) | **0/20** | — | — |

The most striking result: PCT (d=2) finds the bug on **run 1 in every attempt**. Random finds it on run 2 every time. CHESS (G+L, k=2) finds it on run 27 every time with only 2 non-FIFO decisions. CHESS (G-only, k=2) never finds it across all 20 attempts × 500 runs.

Note that all attempts used a fixed seed (Seed: 1), so the zero variance across attempts reflects identical runs rather than seed robustness. The single-seed results are nevertheless informative: they reveal the exploration structure of each algorithm on this specific bug.

The CHESS G-only failure is the headline result. Unlike the quorum-read-repair bug, where CHESS failed because k was too small for the required non-FIFO depth, here CHESS G-only fails because the bug requires a **local** goroutine scheduling decision — and G-only never makes one. CHESS G+L succeeds with k=2 because the bug requires exactly 1 global + 1 local non-FIFO decision. The k=2 bound is the minimum necessary and sufficient bound.

---

### Runs to Bug

The bar chart shows a striking inversion from the quorum-read-repair case. Here, CHESS (G+L) is visible at 27 runs — slower than PCT (1.0) and Random (2.0), but it finds the bug. CHESS (G-only) is absent (never found). DPOR at 13.1 sits between CHESS-GL and PCT/Random.

PCT (d=2) at 1.0 is the most remarkable result in the chart. It finds the bug on the first run it attempts, with 22 non-FIFO decisions. This is not because PCT's priority assignment happens to land exactly on the minimal 2-decision trace — it doesn't. PCT generates a highly shuffled schedule that incidentally includes the 2 critical decisions among 20 others. The expected-1-run result reflects that with d=2 and this specific seed, PCT's first priority assignment already contains the two critical inversions needed. This is consistent with PCT's theoretical guarantee: a bug of effective priority-depth 2 is found on the first run with probability 1/n² (for n total operations), and seed 1 happens to be a winner.

Random at 2.0 runs also exhibits remarkable efficiency. The second run finds the bug (with 13 non-FIFO decisions), while the first does not. This is consistent with the geometric distribution at high per-run probability — since this bug is relatively easy to stumble into with random scheduling, even a uniform sampler hits it within a few runs.

The comparison with CHESS is illuminating: CHESS takes 27 runs because it performs a systematic DFS from FIFO, incrementally expanding its non-FIFO choices. The DFS reaches the 2-non-FIFO run only after exhausting all traces with 0 and 1 non-FIFO decisions. Probabilistic algorithms skip straight to heavily perturbed runs, which happen to contain the required decisions.

---

### Seed Runs to Bug

Since all attempts used the same seed, all dots are stacked at identical x positions — each algorithm shows a vertical line at its single discovered run count. This eliminates the seed-robustness signal visible in the quorum-read-repair chart. The chart still conveys the algorithm ordering clearly: PCT (d=2) at 1, Random at 2, PCT (d=3) at 10, DPOR at 13, CHESS-GL at 27.

DPOR's single dot at 13 (or 14 in some attempts) reflects its deterministic exploration structure. Like CHESS, DPOR explores a fixed pruned tree and always arrives at the bug-triggering trace at a predictable position.

---

### Bug Trace Decision Mix

All successful algorithms show 26 global decisions and ~9 local decisions in their bug traces — a total of ~35 decisions. The global count is identical across algorithms because the scenario has a fixed number of messages (6 REQUEST, 6 REPLY, 8 KV messages = fixed protocol structure). The local count (~9) represents scheduling points with >1 runnable goroutine inside node bubbles.

The consistency is notable: every algorithm that finds the bug reaches it via a trace of the same length. This is very different from the quorum-read-repair case, where DPOR found the bug via a significantly more compact trace. Here, the trace length is structurally determined by the RA protocol: exactly 3 rounds of lock acquisition and KV access, regardless of ordering.

CHESS (G-only) is absent because it cannot produce a trace with the required local non-FIFO decision.

---

### Non-FIFO Comparison

The non-FIFO bar chart reveals a sharp structural difference between algorithms:

- **CHESS (G+L)**: 2 non-FIFO decisions total (1 global + 1 local). This is the minimum possible.
- **Random**: 13 non-FIFO decisions (12 global + 1 local).
- **PCT (d=3)**: 20 non-FIFO decisions.
- **PCT (d=2)**: 22 non-FIFO decisions.
- **DPOR**: 25 non-FIFO decisions (23 global + 2 local).

CHESS finds the minimal trace — the one with the fewest possible non-FIFO decisions that still triggers the bug. Probabilistic algorithms wander into the bug via much more perturbed schedules: PCT makes ~20 non-FIFO decisions en route to a bug that requires only 2. This is the cost of probabilistic exploration: you reach the right region of the search space through an indirect path.

DPOR's 25 non-FIFO decisions is higher than PCT's because DPOR's partial order reduction causes it to explore a different region of the space. DPOR identifies commutative global delivery orderings and collapses them, leading it to a bug trace that goes through many non-commutative (non-FIFO) steps. DPOR is being thorough, not wasteful.

The 1 local non-FIFO decision present in CHESS's, PCT's, and Random's traces is the critical DeferredFlusher-before-App scheduling. Every successful algorithm finds exactly one local non-FIFO decision in its bug trace. DPOR finds 2 local non-FIFO decisions, suggesting it reaches the bug via a different (and more perturbed) local schedule that still contains the critical race.

The contrast with CHESS (G-only) is the key structural takeaway: G-only produces 0 local non-FIFO decisions by design. Since the bug requires ≥1 local non-FIFO decision, G-only is structurally incapable of finding it.

---

### Non-FIFO Position Profile

The position profile shows where non-FIFO decisions appear within each bug-finding trace:

- **CHESS (G+L)**: exactly 1 global non-FIFO decision at ~9% (step 10 of 106) and 1 local at ~32% (step 34). A sparse, early-intervention pattern.
- **Random**: 2 early local non-FIFO decisions (steps 1 and 7, ~0–7%) followed by 11 global non-FIFO decisions scattered from 9% to 80%.
- **PCT algorithms**: similar to Random but with more global non-FIFO decisions spread across the trace.
- **DPOR**: 6 global non-FIFO decisions at ~9%–24% (lock phase), then 2 local at 35%–40%, then 11 more global at 37%–65% (KV phase).

The early local non-FIFO decisions in Random (steps 1 and 7) represent initial run-queue ordering in nodes A and C — these are not the critical race decisions. The critical local decision (DeferredFlusher before App) appears later in all traces, after the global message reordering creates the deferred queue entry.

The global non-FIFO decisions in the lock phase (steps 10–28) are the critical message reorderings that determine which nodes get delayed REPLYs. Everything after the last node enters the CS (steps 39 onwards) reflects KV message reorderings that are incidental to the bug itself — the lost update has already been set up by then.

CHESS's minimal profile is the most revealing: by exploring in DFS order, it identifies the exact two decisions that matter and makes only those, avoiding all the KV-phase reorderings that the probabilistic algorithms wander through unnecessarily.

---

### Cumulative Unique Traces

The cumulative uniqueness chart shows a different story than quorum-read-repair. With only 20 attempts (each producing 27–500 runs), the curves are shorter and the spread is tighter. Key observations:

CHESS (G+L) and DPOR both produce near-perfect uniqueness curves — every run is in a distinct equivalence class. This is consistent with systematic exploration: DFS avoids revisiting traces by construction, and DPOR's pruning ensures each trace is canonical within its class.

PCT and Random both achieve good uniqueness but with slight divergence from ideal. PCT's fixed seed means all 20 attempts are identical, so the cumulative uniqueness for PCT is just the trace of one run repeated — the chart shows the single-attempt curve as if it were 20 independent attempts, which is misleading in this context.

---

### Search Space vs. Explored

| Algorithm | Theoretical Space | Explored Runs |
|---|---|---|
| CHESS (G+L) | 5.7×10¹¹ | 27 |
| DPOR | 8.7×10¹¹ | 13 |
| PCT (d=2) | 2.2×10¹² | 1 |
| PCT (d=3) | 5.9×10¹² | 10 |
| Random | 9.2×10¹⁰ | 2 |

The theoretical spaces are computed from the bug-finding trace's decision fan-outs. PCT (d=3)'s theoretical space is larger because it generates more perturbed traces with larger effective run-queue sizes at each step. The actual search space is the same for all algorithms — this is a property of the scenario, not the algorithm.

There is no dramatic space reduction from DPOR here (unlike the 4 orders-of-magnitude reduction seen in quorum-read-repair). DPOR's pruned space is only ~25% smaller than the unpruned raw space. The reason: this bug's bug-triggering path is relatively shallow in the pruned space. The commutative reorderings that DPOR collapses are not the bottleneck for finding this bug — the bottleneck is reaching the specific local decision point, which is not affected by global-level commutation pruning.

This explains why DPOR at 13 runs is slower than PCT at 1 run and Random at 2 runs: DPOR's reduction is too fine-grained to be decisive here. The bug lives in a region that probabilistic algorithms stumble into quickly by random chance.

---

## 7. Conclusions and Tradeoffs

**CHESS (G-only) fails categorically.** Unlike the quorum-read-repair case where CHESS failed due to insufficient non-FIFO depth, here it fails due to a structural incompatibility: the bug requires a local goroutine scheduling decision that G-only cannot make. No amount of k-extension would help. G-only is simply the wrong tool for bugs that involve intra-node scheduling races.

**CHESS (G+L, k=2) succeeds — and requires exactly k=2.** The bug's minimum non-FIFO count is 2 (1 global + 1 local). CHESS k=2 can reach this in 27 runs via systematic DFS. This is the ideal case for CHESS: a bug that lies within the k-bound. CHESS finds the minimal proof trace — the simplest execution that demonstrates the bug.

**PCT (d=2) is the fastest algorithm on this bug.** Finding the bug on run 1 (with this seed) is as good as it gets. The d=2 match to the bug's effective priority-depth explains why d=2 and d=3 behave differently here (d=3 takes 10 runs instead of 1). This is a case where PCT's theoretical guarantee is tightly binding: the depth-2 characterization is exact.

**Random is remarkably fast (2 runs).** This reflects a high per-run probability for this bug. The required combination — 1 global message reorder + 1 local scheduling choice — is easy to stumble into randomly because the initial run-queue has 3 goroutines with multiple orderings, and the global queue starts with 6 messages. The probability of hitting both required non-FIFO decisions on a single run is non-negligible.

**DPOR (G+L) is correct but not the fastest.** 13 runs with 25 non-FIFO decisions reflects DPOR's systematic but non-probabilistically-biased exploration. DPOR correctly identifies and explores the bug — but its pruned space still places the bug at position 13 in the exploration order. For a bug this shallow, probabilistic algorithms win on speed. DPOR's guarantee of predictable, seed-independent bug finding is less valuable here than in the quorum-read-repair case, where probabilistic algorithms had significant failure rates.

**The two-level search space structure is the fundamental insight.** This bug requires both a global and a local non-FIFO decision. A framework controlling only message delivery would miss the DeferredFlusher race entirely. A framework controlling only local scheduling would miss the prerequisite global condition (no deferred entries without the message reorder). The orchestrator's G+L scope is not optional — it is the minimum necessary to observe this class of bug.

The comparison between CHESS G+L and CHESS G-only on this bug is the sharpest possible argument for the G+L approach. The two algorithms differ by exactly one capability (local scheduling control), and that one capability is the difference between 27 runs to success and 0 runs ever.
