# Case Study: Quorum Read-Repair Lost Sibling Bug

## 1. System Model

The scenario models a minimal Dynamo-style replicated key-value store — a simplified but faithful instance of the conflict-resolution semantics used in systems like Amazon Dynamo, Riak, and Cassandra. The design choices are deliberate: the store is small enough to reason about precisely, yet complex enough that bugs emerge only under specific multi-level orderings.

**Replicas.** Three replicas (`R1`, `R2`, `R3`) each maintain a map from keys to *sibling sets* — slices of `VersionedValue{Value string, Clock map[string]int}`. The data model uses vector clocks for causality. Rather than a last-write-wins policy, the store keeps all causally concurrent values as siblings. This is the correct semantics for eventual consistency under network partitions: when two clients write concurrently, both values must survive until a reconciler explicitly resolves them.

**Clients.** Three logical clients participate:
- `C1` writes `x=A` to all replicas (simulating a successful first write).
- `C2` writes `x=B` to only `R2` and `R3` — then *separately* sends a late Put of `B` to `R1` with a fresh vector clock `{C2: 1}`. This simulates C2's write reaching the third replica after a network delay.
- `Reader` performs quorum reads then issues read-repair to stale replicas.

**Internal replica pipeline.** Each replica runs three concurrent goroutines inside its synctest bubble:

- `router`: receives messages from the network mailbox and dispatches them. For `MsgPut`, it increments `pendingApply` and enqueues to `applyCh` *before* the apply has happened. This gap — between counting a write as pending and it being committed to the store — is the load-bearing invariant that the bug exploits.
- `applyLoop`: drains `applyCh`, decrements `pendingApply`, and merges the incoming value into the store using the correct `mergeSiblings`.
- `repairLoop`: drains `repairCh` and merges repair values — but uses `buggyRepairMerge` when `pendingApply > 0`.

**Transport.** Each node uses an `OrchestratorTransport`. Sends are not delivered immediately; they produce a `PendingOp` that is placed in an outbox channel and handed to the global orchestrator. The orchestrator holds all in-flight messages across all bubbles and chooses, at each step, which message to deliver next. Inside each bubble, `synctest.ExternalWait` bridges the synchronous bubble world to the external message queue. This is what makes both dimensions of non-determinism — message delivery order (global) and goroutine scheduling order (local) — observable and controllable.

---

## 2. Scenario Construction

The scenario is a coordinated choreography of six nodes across two phases, designed to produce the exact preconditions the bug requires.

**Phase 1 — Establishing concurrent writes.**

`C1` goes first, writing `A` to all three replicas and collecting quorum acknowledgments. After quorum is achieved, it signals `C2` ("c1-all-done") and `Reader` ("c1-done"). This establishes `A` with clock `{C1: 1}` on all replicas.

`C2` then writes `B` to only `R2` and `R3` with clock `{C2: 1}`. These two writes are *concurrent* with `A` from the vector clock perspective — neither dominates the other, because `C2`'s clock does not include `C1`. After its quorum write completes, `C2` signals `Reader` and sends a *separate*, late Put of `B` directly to `R1` with the same clock. This late message is injected into the network but intentionally not waited upon — it becomes a free-floating pending message that the orchestrator may deliver at any time.

The key asymmetry: after Phase 1, `R2` and `R3` hold `[A, B]` (correct sibling set), while `R1` still holds only `[A]`. `R1` is stale.

**Phase 2 — Reader performs quorum read and repair.**

After both `C1` and `C2` signal done, `Reader` sends `Get` to all three replicas and waits for quorum responses. In the bug-triggering path, `Reader` receives responses from `R1` (stale: `[A]`) and `R2` (current: `[A, B]`). The merged result is `[A, B]`. `Reader` detects that `R1`'s response is missing `B`, so it sends a `Repair` to `R1` carrying the full `[A, B]` sibling set. A second `GetAndRepair` follows to confirm convergence.

The trace gate in `completeQuorumBugTrace` requires that the bug trace include a `Put` to `R1`, a `Get` to `R1`, and a `Repair` to `R1` — all three must be present to count as a complete quorum scenario. This gate exists because the orchestrator can explore interleavings where `Reader` completes quorum without ever querying `R1` (e.g., using `R2`+`R3`), in which case `R1` is never repaired and the semantic invariant may still hold from the reader's perspective. Only traces that actually exercise the R1 stale-path are meaningful for this bug.

---

## 3. The Bug

The bug lives in `repairLoop` (`store.go:229-233`):

```go
if r.pendingApply > 0 {
    r.store[req.msg.Key] = buggyRepairMerge(r.store[req.msg.Key], req.msg.Values)
} else {
    r.store[req.msg.Key] = mergeSiblings(r.store[req.msg.Key], req.msg.Values)
}
```

The intention behind this branch was presumably defensive — a developer believed that if a write was pending, some special handling was needed during repair. The implementation of `buggyRepairMerge` is subtly wrong:

```go
func buggyRepairMerge(existing, incoming []VersionedValue) []VersionedValue {
    all := mergeSiblings(existing, incoming)   // correctly compute all siblings
    if len(all) <= 1 { return all }
    // pick a single "winner" by summing clock components
    winner := all[0]
    for _, v := range all[1:] {
        if clockScore(v.Clock) >= clockScore(winner.Clock) {
            winner = v
        }
    }
    return []VersionedValue{{Value: winner.Value, Clock: cloneClock(winner.Clock)}}
}
```

The function first correctly computes the sibling set, then discards all but the highest-clock-score value. This collapses concurrent siblings into a single winner — exactly the wrong thing to do in an eventually-consistent store. The `pendingApply` count is a pipeline implementation detail; it has no bearing on whether two writes are causally concurrent. The conflation of "a write is in flight" with "conflict resolution should be different" is the semantic error.

The specific failure: when `R1` holds `[A]`, has `C2`'s late `Put(B)` pending in `applyCh` (`pendingApply = 1`), and simultaneously receives the repair with `[A, B]`, `buggyRepairMerge` computes the correct `[A, B]` but then reduces it to `[B]` (since `B`'s clock `{C2: 1}` ties with `A`'s clock `{C1: 1}` by score, and the last-wins tiebreak selects `B`). The replica ends up with only `[B]`, losing the `A` sibling permanently.

---

## 4. Required Ordering of Events

The bug requires a specific sequence at two levels of granularity:

**Global message ordering (between nodes):**

1. `C1` puts `A` to `R1`, `R2`, `R3` (establishes baseline).
2. `C2` puts `B` to `R2` and `R3` only (makes them current, leaves `R1` stale).
3. `Reader` sends `Get` to the cluster; the first two quorum responses come from `R1` (stale) and `R2` (current). `R1` must be in the quorum — if `Reader` reads from `R2`+`R3`, `R1` is never repaired and no bug.
4. `Reader` computes the merged value `[A, B]` and sends `Repair` to `R1`.
5. **Before `R1` processes the repair**: `C2`'s late `Put(B)` to `R1` must be delivered and dispatched to `applyCh`. The router must have incremented `pendingApply` but `applyLoop` must not yet have decremented it.

**Local goroutine ordering (within R1's bubble):**

6. `router` runs, receives the late `Put(B)`, increments `pendingApply++`, and enqueues to `applyCh`.
7. `router` runs again, receives the `Repair`, enqueues to `repairCh`.
8. `repairLoop` runs, sees `pendingApply > 0`, calls `buggyRepairMerge`, overwrites store with `[B]`.
9. `applyLoop` runs, decrements `pendingApply--`, merges `B` into store (already `[B]`, no change).

The critical constraint is step 8 preceding step 9. If `applyLoop` runs between steps 6 and 7 (i.e., between the router dispatching the Put and the router dispatching the Repair), `pendingApply` drops back to 0 before `repairLoop` ever sees the repair. The correct `mergeSiblings` path is taken and the bug is not exposed.

This is why neither dimension of non-determinism alone is sufficient. The global ordering must deliver `C2`'s late Put and the Repair to `R1` before `R1`'s `applyLoop` drains the queue. And the local goroutine scheduling must sequence `router → router → repairLoop → applyLoop` rather than `router → applyLoop → router → repairLoop`. Both non-FIFO choices must happen, and they must be coordinated.

---

## 5. Why Regular Tests Do Not Catch It

A conventional unit test for read-repair would create a replica, pre-populate it with `[A]`, send a repair carrying `[A, B]`, and assert that the result is `[A, B]`. That test passes — because `pendingApply` is 0, `buggyRepairMerge` is never called.

An integration test that runs all three replicas and all three clients in a single goroutine, or with Go's default scheduler, will almost always execute in FIFO network order. Messages are delivered in the order they are sent. In that order, `C2`'s late Put to `R1` arrives and is fully applied before `Reader` ever reads. By the time `Reader` queries `R1`, it already holds `[A, B]` — it is not stale, so `Reader` sends no repair. The bug path is never exercised.

Even stress-testing with `go test -race -count=1000` is unlikely to help, because:
- The `testing/synctest` bubble serializes goroutines; there is no real parallelism between `router`, `applyLoop`, and `repairLoop` within a single bubble's tick.
- The Go scheduler's `cheaprand()` biases toward FIFO (the recently-unblocked goroutine typically continues). The specific sequence needed — router running twice consecutively *without* applyLoop intervening — requires a non-default scheduling choice.
- Network message ordering across bubbles is mediated by the orchestrator. Without the orchestrator, messages are delivered as soon as they are sent (via buffered channels), which again defaults to FIFO ordering.

The bug is invisible to any test that does not control both (a) which message the network delivers next across all nodes, and (b) which goroutine runs next within a node's internal pipeline.

---

## 6. Graph Analysis

### Summary Table

The most striking result is CHESS's complete failure at k=4 in both configurations. This is not a sampling issue — CHESS ran all 2000 runs in both cases and found nothing, 20 times over. The non-FIFO column shows why: CHESS made only ~7 non-FIFO decisions per run on average, while the bug-finding runs for every successful policy required 21-31 non-FIFO decisions. CHESS with k=4 is systematically under-perturbing. The context bound is too tight to reach the combination of global message reordering and local goroutine reordering the bug requires.

This reveals a fundamental tension in CHESS: the bound k is meant to guarantee coverage of "shallow" bugs — bugs that manifest with few departures from FIFO. This bug is not shallow by that measure. The required non-FIFO decision count of ~21 means the failing trace requires the 21st context switch to be non-default, which is far outside k=4.

PCT and Random both find the bug reliably and efficiently — PCT at ~6.6 runs median, Random at ~78 — but for different structural reasons discussed below.

---

### Runs to Bug

The bar chart confirms the summary table's ordering and puts the gap in visual perspective. Random at 78.5 vs. PCT at 6.6 is roughly a 12× efficiency advantage for PCT. This is meaningful: with a 2000-run budget, Random wastes ~96% of its budget after the first bug, while PCT finds it in the first 0.3%. CHESS (both variants) occupies a flat "not found" region — their bars are not even visible, shown only as text annotations.

One important nuance: Random's 78.5 average understates its variance. The seed_runs_to_bug chart shows the full distribution.

---

### Seed Runs to Bug

This strip plot shows each of the 20 seeds as an individual dot, with a vertical median line and worst-case marker. It is the most information-dense chart in the suite.

**PCT (d=2 and d=3):** Near-identical distributions. Median is 5 for both, worst case is 26 for both. The tight clustering around 5 means PCT is both fast and consistent — most seeds find the bug in 3–7 runs. The worst-case outlier at 26 is still very fast (1.3% of the 2000-run budget). The d=2 vs. d=3 indistinguishability is telling: the bug requires only 2 priority levels' worth of reordering. Going from d=2 to d=3 adds overhead without benefit for this specific bug. This is consistent with the theoretical motivation for PCT — a bug that manifests with d non-FIFO decisions should require at most O(n^d) runs to find — but the practical benefit of increasing d beyond the bug's actual depth is zero.

**Random:** Median is 78, worst is 88, tight band spanning roughly 60–90. The distribution is concentrated (small standard deviation relative to mean), which is exactly what you expect from a Poisson process — if the bug appears with probability p per run, the distribution of first-hit times is geometric with mean 1/p. The consistency of the band suggests p ≈ 1/78 regardless of seed. The worst case at 88 is 4.4× better than the 2000-run budget, but 17× worse than PCT's median. For bug-finding workflows where you want a deterministic guarantee of finding a known bug, Random's tail risk is a practical concern.

The comparison between PCT and Random directly shows the value of structured exploration. PCT does not just sample randomly — it biases toward priority inversions at specific depths, which matches the structure of real concurrency bugs more efficiently than uniform random sampling.

---

### Bug Trace Decision Mix

This stacked bar chart shows how many decisions (global vs. local) were made in the bug-finding run itself, not the exploration budget. Every policy that found the bug required approximately the same trace structure: ~15 local decisions (orange) and ~30-32 global decisions (blue), totaling ~45-47.

The near-identical trace sizes across Targeted, Random, PCT (d=2), and PCT (d=3) are important: they confirm that all four policies found **the same bug via structurally equivalent traces**. The scenario has a fixed number of messages and goroutine scheduling points — any run that exercises the full quorum scenario will traverse roughly the same decision graph. The algorithms differ only in how quickly they navigate to a bug-triggering leaf of that graph, not in the shape of the leaf itself.

The local decision count (~15) being roughly half the global count (~32) is characteristic of this scenario. Each replica runs 3 internal goroutines; the reader and writer clients run 1. Local decisions are mostly binary (run goroutine A or goroutine B at the next scheduler yield). Global decisions are larger-fan-out (choose from the set of all in-flight messages across all nodes). The mix validates that this is a genuinely mixed-level bug — approximately 32% of the decisions in the failing trace are local goroutine choices.

The absence of CHESS from this chart is the strongest evidence of its failure mode: CHESS never reached a bug-triggering trace, so there is nothing to show.

---

### Non-FIFO Comparison

This chart decomposes the non-FIFO decisions in the bug-finding run into global (blue) and local (orange) components.

The Targeted trace requires 21 total non-FIFO decisions (16 global, 5 local). This is the ground truth — a human-authored trace encoding exactly the decisions needed. Random and PCT both arrive at traces with 27-31 non-FIFO decisions. The excess over 21 is noise — random algorithms make non-FIFO choices that turn out not to affect the bug path, but happen to be present in the trace anyway.

The local non-FIFO count is lower than the global for all policies (~5-6 local vs. ~16-25 global). This makes structural sense: the local race inside R1 requires only one key non-FIFO scheduling choice (repairLoop before applyLoop). The additional non-FIFO global decisions are the message reorderings needed to set up the stale-read condition. The 5:16 ratio in the targeted trace is the minimum necessary; PCT and Random add global noise around this.

A concerning implication: CHESS's k=4 limit allows at most 4 non-FIFO decisions total. The bug requires at least 21. This is a 5× gap. Even k=8 (the bench test's configuration for CHESS) would fall short — the bug requires more non-FIFO choices than any fixed small bound is likely to cover in this scenario geometry.

---

### Non-FIFO Position Profile

This dot plot maps where non-FIFO decisions occur within the failing trace (x-axis is 0%–100% of trace length), distinguishing global (diamond) and local (circle) by shape.

**Local non-FIFO decisions cluster at the trace beginning (0%–10%).** For all policies, the orange circles are concentrated in the early trace. This is the signature of the local race: the goroutine scheduling choice inside R1 that causes `repairLoop` to run before `applyLoop` must happen early — specifically, right when the late Put arrives and before the Repair is delivered. Local non-FIFO decisions that appear near 90% (visible in Random and Targeted) are late-trace noise, likely from the Reader's internal goroutine scheduling during the second `GetAndRepair` call.

**Global non-FIFO decisions are spread throughout the trace.** The blue diamonds appear from ~15% through ~90% of the trace. This reflects the message reordering needed to route `R1` into the stale-read path — a multi-step global choice spanning the entire quorum read phase. The mid-trace concentration from ~40% to ~85% corresponds to the Reader's get/repair phase.

**PCT and Random produce structurally similar profiles.** Both show early local clustering and distributed global spread, consistent with the decision mix chart. The targeted trace is sparser (fewer total non-FIFO decisions) but structurally identical in shape.

**The profile directly explains CHESS's failure.** Even if CHESS were allowed k=8 instead of k=4, the non-FIFO decisions in the bug trace are not front-loaded — they are spread across the full trace. CHESS's DFS would need to backtrack to points far into the tree to explore the late-trace branches, and the search tree size grows exponentially with trace depth. The bug sits deep in a part of the tree that CHESS cannot reach without a bound far exceeding what is tractable.

---

### Cumulative Unique Traces

This chart plots the number of distinct delivery traces seen over the 2000-run budget. The dashed line is the ideal "no repetition" reference (slope = 1.0).

All algorithms stay close to the ideal up to ~500 runs, then begin to diverge — meaning significant repetition sets in by run 500. By run 2000:
- **Random and Targeted**: ~1900 unique traces (95% unique). These are the closest to the ideal.
- **CHESS (G-only)**: ~1700 unique traces (85% unique).
- **PCT (d=2, d=3) and CHESS (G+L)**: ~1700 unique traces (85% unique).

The chart is counterintuitive at first: CHESS and PCT, which are more structured, show *more* repetition than Random. This is because structured algorithms revisit similar global delivery orderings by design — CHESS's DFS backtracks to the same branching points repeatedly to explore siblings, and PCT's priority-based scheduling produces correlated traces when priorities happen to be similar across runs.

However, uniqueness of traces is not the right metric for bug-finding efficiency. Random has the highest uniqueness but takes 78 runs to find the bug. PCT has more repetition but finds the bug in 6 runs. The reason: PCT biases toward traces that include priority inversions — the specific class of non-FIFO decisions the bug requires. It sacrifices diversity to focus search on a structurally relevant subspace. High uniqueness without bias is wasted coverage.

The CHESS data reveals a subtler problem: CHESS's 85% uniqueness comes from repeatedly exploring the shallow part of the tree (within k=4 context switches) with high coverage. It is diverse *within its reachable subspace* but that subspace does not contain the bug. CHESS explores broadly but in the wrong neighborhood.

---

### Search Space vs. Explored

The log-scale chart shows the theoretical interleaving space (upper bound from per-step runq sizes and global queue sizes in the bug-finding trace) vs. the actual number of runs explored.

The gap is staggering:
- **Theoretical space**: 6.6×10²⁵ (Targeted), 3.3×10²¹ (Random/PCT).
- **Runs explored**: 1, 79, 6, 6.

No algorithm is actually "exploring" in any meaningful sense — all of them are finding the bug by hitting a small fraction of an astronomically large space. The theoretical space calculation is an upper bound (the product of queue sizes at each step), so the true reachable space is smaller, but the orders-of-magnitude gap is structurally real.

The difference between the Targeted trace's space (10²⁵) and the others (10²¹) is likely because the targeted trace, which makes very specific global choices, ends up visiting decision points with larger runq/queue sizes (more options to permute). This is an artifact of the path-dependent nature of these calculations — different traces witness different sets of branching points.

The key takeaway is that this chart should generate skepticism about any claim that any algorithm is "systematically exploring" the space. PCT at 6 runs is not finding the bug by comprehensively covering the space — it is finding it by structural bias. Random at 79 runs is not even close to sampling the space uniformly. All the effective algorithms succeed because the bug-triggering trace is structurally accessible given their particular biases, not because they are anywhere near exhaustive.

---

## 7. Conclusions and Tradeoffs

**CHESS (both variants) fails categorically on this bug class.** The context bound k is the wrong axis to optimize for bugs that require many accumulated non-FIFO decisions spread across a long trace. CHESS excels when bugs manifest with 1–3 context switches from FIFO — a reasonable assumption for data-race-style bugs in shared-memory programs. But distributed system bugs involving coordinated message reordering and pipeline races accumulate non-FIFO choices structurally, not incidentally. The k bound is not a weakness in CHESS's search strategy; it is a fundamental mismatch between CHESS's bug model and the geometry of this bug class.

The G+L vs. G-only CHESS comparison is instructive: both fail identically. If CHESS cannot reach the bug with global-only decisions (G-only), adding local decisions (G+L) does not help — it only expands the search tree CHESS must traverse with the same per-trace bound. Local decisions multiply the search space without any benefit if the required global ordering cannot be reached.

**PCT is the standout performer.** A median of 5 runs to find a bug requiring 21+ non-FIFO decisions, with a worst case of 26 and 100% success rate across 20 seeds, is remarkable. The theoretical justification (PCT finds d-non-FIFO-depth bugs with probability 1/n^d per run) is borne out here, though the effective d for this bug appears to be low in practice — d=2 and d=3 are near-identical. This suggests the bug's structure is simpler in priority-space than in non-FIFO-count space: the number of distinct priority levels needed to separate the key goroutines is 2, even though 21 individual non-FIFO decisions occur.

**Random is reliable but slow.** A 78-run median means Random will always find the bug given enough budget, but the 12× efficiency gap vs. PCT is significant in practice. Random's consistency (tight variance in the seed plot) is reassuring — it is not seed-sensitive in the way PCT can be for bugs near depth boundaries. But for a bug that requires this much non-FIFO perturbation, Random is paying the full price of an unbiased search.

**The orchestrator's cost: two-level search doubles the problem.** This is the most important architectural observation. The orchestrator coordinates global delivery decisions (which message to deliver next across all nodes) and per-bubble local decisions (which goroutine runs next within each node). Both dimensions are necessary to expose this bug. But most existing model checking frameworks handle only one dimension. A framework that only controls message ordering (like a traditional network-level permuter) will miss the local goroutine race. A framework that only controls intra-process scheduling (like a classic CHESS implementation for a single-process program) will miss the global delivery ordering. The orchestrator's value is precisely this two-level scope — but the cost is a search space that grows multiplicatively: N_global × N_local interleavings instead of max(N_global, N_local).

This is why CHESS's k bound fails so badly: the bound was designed for single-level search spaces. In a two-level space, k=4 covers a vanishing fraction of reachable states. Any fixed-k bound is doubly punished — it must simultaneously cover global and local branching within the same budget.

**The trace gate is essential for meaningful benchmarking.** The `completeQuorumBugTrace` function that requires a Put, Get, and Repair to R1 in the recorded trace prevents two important artifacts: (1) false positives from stale state in package-level globals carrying over between exploration runs, and (2) counting any invariant violation — even one from an irrelevant trace that happened to observe a partial state — as a bug find. Without this gate, a policy that ran a trace where Reader read from R2+R3 only (never touching R1) could still appear to "find" the bug if R1's state happened to be inspected by the recorder from a previous run. The gate ensures that bugs counted in charts are genuine, complete, causally coherent instances of the failure mode under study.

**The per-run recorder isolation is equally critical.** The `benchmarkOutcome.beginRun()` pattern — allocating a fresh recorder function per exploration run — ensures that observations from one run cannot be attributed to another. A naive implementation using a package-level `outcome` variable would allow a failing run's `bug = true` state to persist into subsequent runs, making every later run appear to also find the bug. The isolation means the "runs to bug" metric is causally clean: a bug is counted only for the run that actually caused the invariant violation.

**Implication for tooling.** The CHESS failure suggests that context-bounding, while theoretically appealing, needs to be accompanied by dynamic bound estimation — or replaced by algorithms like PCT that do not have a fixed bound. For two-level distributed system exploration, a promising direction is budget-adaptive bounds: start with k=2, and if the bug is not found, double k. But this converges poorly if the required k is large (as here). PCT's probability-theoretic guarantee — that it finds bugs with bounded probability regardless of depth — is more robust in the face of unknown bug geometry. The practical recommendation: use PCT as the default exploration algorithm for distributed simulation, and reserve CHESS for targeted "shallow bug" verification passes where the bound k can be justified by domain knowledge.
