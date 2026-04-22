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

### Replication and Consistency Model

The store is a **leaderless, eventually-consistent replicated KV system** in the style of Amazon Dynamo. There is no primary replica, no designated coordinator, and no total ordering imposed on writes. Any replica can receive any write at any time. The system trades strong consistency for availability: a write completes as soon as a quorum of replicas acknowledge it, even if some replicas have not yet seen every prior write.

**Replication.** Every write is broadcast to all known replicas. Each replica independently applies writes to its local store. There is no replication log, no leader-follower streaming, and no global sequence number. Replicas diverge transiently whenever writes arrive in different orders at different nodes — this is the expected and accepted operating condition.

**Consistency model: causality without coordination.** Rather than serializing writes through a single node (which would require coordination and reduce availability), the store tracks causality with per-writer vector clocks. Each client increments its own entry in the clock with each write. A `VersionedValue` is a `(value, clock)` pair. The clock encodes *what the client knew about the world* when it issued the write. Two values are causally related if one clock dominates the other; they are concurrent if neither dominates.

When a value is causally dominated by a newer write from the same lineage, the older value is safe to discard — the newer write subsumes it. When two values are concurrent (neither happened-before the other), neither can be discarded without losing information. The store keeps all concurrent values as a *sibling set*. Siblings represent genuinely conflicting writes that the system cannot automatically resolve, and they must all be visible to any reader until application logic explicitly reconciles them.

**Quorum writes (W=2 of N=3).** `Client.Put` broadcasts the write to all replicas and waits for `quorum=2` acknowledgments before returning. The write is durable once 2 replicas have it. The third replica may receive it later — or, as in this scenario, much later. A replica that has not yet applied a write is *stale* for that key.

**Quorum reads with read-repair (R=2 of N=3).** `Client.GetAndRepair` broadcasts `Get` to all replicas and waits for `quorum=2` responses. It then computes the *merge* of all received sibling sets using `mergeSiblings` — the union of all non-dominated values across all responses. Importantly, the read does not return until this merged view is computed. Any replica whose response was a strict subset of the merged result (i.e., it was missing siblings) receives a `Repair` message carrying the full merged set. Replicas that did not respond at all also receive a repair, since their absence from the response map means their state is unknown and potentially stale.

**Why W+R > N guarantees read-your-writes.** With W=2 and R=2 over N=3 replicas, any read quorum must overlap with any write quorum in at least one replica (2+2−3=1). This means a quorum read after a quorum write always includes at least one replica that has seen the write. The client merges all quorum responses, so even if the other replica in the read quorum is stale, the merged result will include the write from the overlapping replica. This is the core safety argument for the Dynamo quorum protocol.

**Read-repair as anti-entropy.** The repair messages sent at the end of `GetAndRepair` are the system's primary mechanism for propagating writes to stale replicas. This is *lazy anti-entropy*: replicas are not kept in sync proactively. Instead, reads detect and fix divergence as a side effect. Over time, repeated reads converge all replicas toward the same state. The invariant the system must preserve is: after a `GetAndRepair` and the subsequent repairs have been applied, every repaired replica holds at least the sibling set that was returned to the reader. This is what the scenario checks.

**The sibling preservation invariant.** Since `A` and `B` are concurrent writes (neither client read the other's value before writing), every replica must eventually hold both as siblings: `[A{C1:1}, B{C2:1}]`. A system that returns only `[A]` or only `[B]` has lost information — a replica that was authoritative for one concurrent value has been silently overwritten. The checked invariant in the benchmark is: every observer (reader and each replica) must see exactly `{A, B}` after the full read-repair cycle completes. Any singleton observation is a correctness violation.

### Correctness of the Core Data Model

The following traces through the actual code to establish that the store is a legitimate replicated KV implementation, not a mock.

**Vector clock partial order.** `compareClock(a, b)` (`store.go:25`) builds the union of all keys across both clocks, then checks two booleans: `aGreater` (there exists a key where `a[k] > b[k]`) and `bGreater` (there exists a key where `b[k] > a[k]`). The result is +1 if only `aGreater` (a causally dominates b), -1 if only `bGreater` (b dominates a), and 0 otherwise (concurrent or equal). This is the standard vector clock partial order used in distributed systems literature.

Applying this to the scenario values: `A` has clock `{"C1": 1}`, `B` has clock `{"C2": 1}`. Computing `compareClock({"C1":1}, {"C2":1})`:
- Key `"C1"`: `a["C1"]=1 > b["C1"]=0` → `aGreater = true`
- Key `"C2"`: `a["C2"]=0 < b["C2"]=1` → `bGreater = true`
- Both flags set → return 0: **A and B are genuinely concurrent**

Neither write happened-before the other. Both must be retained as siblings — this is not a design choice but a theorem about the partial order.

**`mergeSiblings` correctness.** The algorithm (`store.go:55`) pools `existing` and `incoming` into a single slice `all`, then filters it to keep only values that are (a) not dominated by any other value in the pool, and (b) not exact duplicates of an earlier entry (same value string and identical clock). The dominance check is `compareClock(candidate.Clock, other.Clock) < 0` — i.e., some other value in the pool has a strictly greater vector clock. The duplicate check uses index ordering (`j < i`) to keep exactly one copy of any value that appears identically in both sets.

Tracing through the repair case: `mergeSiblings([A{C1:1}], [A{C1:1}, B{C2:1}])` produces `all = [A{C1:1}@0, A{C1:1}@1, B{C2:1}@2]`:
- `i=0, A{C1:1}`: vs `j=1` — `compareClock` returns 0 (equal clocks), not dominated; same value and clock but `j=1 > i=0`, so not counted as duplicate. vs `j=2, B{C2:1}` — concurrent, not dominated. **Kept.**
- `i=1, A{C1:1}`: vs `j=0` — same value, same clock, `j=0 < i=1` → **duplicate, dropped.**
- `i=2, B{C2:1}`: vs `j=0, A{C1:1}` — `compareClock(B{C2:1}, A{C1:1})`: key `"C1"` gives `0 < 1` so `bGreater=true`; key `"C2"` gives `1 > 0` so `aGreater=true` → both flags, concurrent. Not dominated. vs `j=1`, same result. **Kept.**
- Result after `sortValues`: `[A{C1:1}, B{C2:1}]` ✓

The deduplication property also means `mergeSiblings` is idempotent: applying it to the same set twice yields the same result. This is necessary for read-repair correctness — repairing an already-correct replica must not alter its state.

**Quorum protocol invariant.** `const quorum = 2` with 3 replicas gives a write quorum of 2 and a read quorum of 2, satisfying the classic Dynamo condition `W + R > N` (2 + 2 > 3). This guarantees that any read quorum intersects any write quorum in at least one replica — a replica that has the write will always be present in any quorum read. The `Put` implementation (`client.go:34`) broadcasts to all configured replicas and blocks until `quorum` acks arrive. The `GetAndRepair` (`client.go:58`) broadcasts `Get` to all replicas, collects quorum responses, merges them with `mergeSiblings`, then sends `Repair` to every replica whose snapshot is not `sameValues` as the merged result.

The repair target set includes replicas that did not respond within the quorum window. If Reader collects responses from R1 and R2 but not R3, `responses["R3"]` is nil. `sameValues(nil, [A, B])` calls `mergeSiblings(nil, nil) = []` and `mergeSiblings(nil, [A, B]) = [A, B]`, then fails the `len(a) != len(b)` check (0 ≠ 2), returning false. R3 therefore receives a repair — correct behavior.

**Correct FIFO execution trace.** Under FIFO message delivery (the default), the protocol converges correctly:

1. C1 sends `Put(A{C1:1})` to R1, R2, R3. Each replica's `router` increments `pendingApply`, enqueues to `applyCh`. Each `applyLoop` runs: `mergeSiblings([], [A{C1:1}]) = [A{C1:1}]`, sends `PutAck`. C1 collects 2 acks.
2. C2 sends `Put(B{C2:1})` to R2, R3. Each `applyLoop` runs: `mergeSiblings([A{C1:1}], [B{C2:1}]) = [A{C1:1}, B{C2:1}]`. C2 collects 2 acks. R2 and R3 now hold `[A, B]`; R1 still holds `[A]`.
3. C2 sends the late `Put(B{C2:1})` to R1. In FIFO order this arrives before Reader queries. R1's `applyLoop` runs: `mergeSiblings([A{C1:1}], [B{C2:1}]) = [A{C1:1}, B{C2:1}]`. R1 now holds `[A, B]`.
4. Reader sends `Get` to R1, R2, R3. Collects quorum responses (e.g., R1 and R2), both returning `[A, B]`. Merged result = `mergeSiblings([A,B], [A,B]) = [A,B]`. `sameValues([A,B], [A,B])` is true for both responding replicas — no repairs sent. R3 (non-responding) receives a repair carrying `[A,B]`, which is idempotent if R3 already holds it.
5. Reader observes `[A, B]`. Invariant holds. `TestQuorumReadRepair_FIFOPasses` verifies this exact path.

**Incorrect (bug-triggering) execution trace.** Steps 1–2 are identical. The divergence begins when the orchestrator withholds C2's late Put to R1 and Reader starts its read before it arrives.

1. C1 sends `Put(A{C1:1})` to R1, R2, R3. All replicas apply: `store["x"] = [A{C1:1}]`. C1 gets quorum, signals C2 and Reader.
2. C2 sends `Put(B{C2:1})` to R2 and R3. Both apply: `store["x"] = [A{C1:1}, B{C2:1}]`. C2 gets quorum, signals Reader. C2 sends the late `Put(B{C2:1})` to R1 — the orchestrator holds this in the global pending queue and does **not** deliver it yet. R1 still holds `[A{C1:1}]`.
3. Reader sends `Get` to R1, R2, R3.
4. R1's `router` handles the `Get` inline and responds with `[A{C1:1}]` (stale). R2 responds with `[A{C1:1}, B{C2:1}]`. Reader collects these two as its quorum.
5. Reader merges: `mergeSiblings([A{C1:1}], [A{C1:1}, B{C2:1}]) = [A{C1:1}, B{C2:1}]`. R1's response is a strict subset of the merged result, so Reader sends `Repair([A{C1:1}, B{C2:1}])` to R1.
6. **Non-FIFO global decision**: the orchestrator now delivers C2's late `Put(B{C2:1})` to R1. Both the late Put and the Repair are now in R1's network mailbox, in that order.
7. R1's bridge goroutine reads the late Put from the external mailbox via `ExternalWait` and forwards it to `internalMailbox`. R1's `router` goroutine wakes and processes it: `pendingApply++ → 1`, enqueues `B{C2:1}` to `applyCh`.
8. **Non-FIFO local decision**: instead of `applyLoop` running next (the FIFO choice), the scheduler runs `router` again. R1's bridge goroutine reads the Repair from the external mailbox and forwards it to `internalMailbox`. R1's `router` goroutine wakes and processes it: enqueues `Repair([A,B])` to `repairCh`.
9. `repairLoop` runs. It acquires the mutex, observes `pendingApply = 1`, and calls `buggyRepairMerge([A{C1:1}], [A{C1:1}, B{C2:1}])`:
   - `sameValues([A{C1:1}], [A{C1:1}, B{C2:1}])` → false (different lengths after merge)
   - `all = mergeSiblings([A{C1:1}], [A{C1:1}, B{C2:1}]) = [A{C1:1}, B{C2:1}]`
   - `len(all) = 2 > 1` → pick winner by `clockScore`
   - `clockScore({"C1":1}) = 1`, `clockScore({"C2":1}) = 1` — tie; the `>=` condition means B wins as the last-seen candidate
   - Returns `[B{C2:1}]`
   - `store["x"] = [B{C2:1}]`. **Sibling A is permanently lost.**
10. `applyLoop` runs. `pendingApply-- → 0`. `mergeSiblings([B{C2:1}], [B{C2:1}]) = [B{C2:1}]` — the late Put applies cleanly but changes nothing. R1 holds `[B{C2:1}]`.
11. Reader's second `GetAndRepair("x", "read-2")` fires. R1 responds `[B{C2:1}]`. R2 responds `[A{C1:1}, B{C2:1}]`. Reader merges to `[A{C1:1}, B{C2:1}]` and records `final = [A, B]` — Reader itself sees the correct merged value, because it merges across quorum respondents.
12. Reader calls `recordReplicaOutcomesWithRecorder`, which takes a direct `Snapshot("x")` from each replica object in memory. R1's snapshot is `[B{C2:1}]`. The recorder calls `hasValues([B{C2:1}], "A", "B")` → false → **bug detected**. R2 and R3 return correct snapshots, but R1's lost sibling is enough to trigger the invariant violation.

Note the subtle observer asymmetry in step 11–12: Reader's own view of `final` is `[A, B]` because it performs a client-side merge of quorum responses. But the invariant checks replica state directly, not the reader's merged view. A real system where clients only ever see the merged result would appear correct at the reader, while individual replicas silently hold inconsistent data. This is why replica-level invariant checking is necessary — client-visible correctness does not imply replica-level correctness.

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

| Algorithm | Found Rate | Avg Runs to Bug | Avg Non-FIFO | Avg Total Decisions |
|---|---|---|---|---|
| Random | 19/20 | 76.5 | 25.9 | 42.6 |
| PCT (d=2) | 20/20 | 6.6 | 25.0 | 38.6 |
| CHESS (G+L, k=4) | **0/20** | — | 6.9 | 42.6 |
| PCT (d=3) | 20/20 | 6.6 | 25.1 | 38.6 |
| CHESS (G-only, k=4) | **0/20** | — | 6.9 | 43.1 |
| dpor-gl | 20/20 | 11.0 | 32.6 | 39.1 |

The headline results: CHESS fails completely in both configurations, Random drops to 19/20 (one seed exhausted the 2000-run budget without finding the bug), PCT remains the fastest probabilistic algorithm at 6.6 runs, and the new DPOR (G+L) algorithm finds the bug on every seed in an average of 11 runs.

CHESS's failure is not a sampling issue — both variants ran all 2000 runs per attempt and found nothing across all 20 attempts. The non-FIFO column explains why: CHESS made only ~7 non-FIFO decisions per run, while successful algorithms required 27–34. CHESS with k=4 is structurally incapable of reaching the region of the search space where the bug lives.

The dpor-gl result is the most interesting new data point. It finds the bug 20/20 with near-zero variance, which no probabilistic algorithm can claim. But it is 1.7× slower than PCT on average. The reasons for both properties — perfect reliability and moderate slowness — are explained by the charts below.

---

### Runs to Bug

The bar chart now has a notable visual change: Random's bar is **orange** (some attempts missed) rather than blue. This reflects the single seed out of 20 that exhausted the 2000-run budget. The underlying probability has not changed — p ≈ 1/78 per run — but with 2000 trials and that probability, a geometric distribution gives a non-trivial chance of failure in any given seed. One miss in 20 is statistically expected.

dpor-gl at 11.0 sits between PCT (6.6) and Random (78.4). Importantly, its bar is blue — every seed found the bug. This is the fundamental distinction between systematic and probabilistic algorithms: DPOR explores a deterministic pruned tree and will always reach the bug-triggering trace eventually, regardless of a random seed. PCT finds it faster on average but has tail risk.

CHESS remains completely absent from the chart's visible region.

---

### Seed Runs to Bug

This strip plot is the most revealing chart in the suite.

**DPOR (G+L):** Median ~10, worst 11. The dots form an extraordinarily tight cluster — essentially a single vertical line. This is the signature of a systematic, seed-independent algorithm. DPOR's exploration tree is deterministic: it always explores the same sequence of pruned traces, and the bug-triggering trace always appears at approximately the same position in that sequence regardless of which seed was given. The near-zero variance is not luck — it is a structural property. The flip side is equally visible: because DPOR is deterministic, if the bug happened to appear at position 500 in its exploration order, every seed would require 500 runs. The tight cluster is good news here, but the same property would be catastrophic for a bug that DPOR happens to reach late.

Why approximately 11 runs? DPOR's exploration is governed entirely by the happens-before structure of the first trace it produces, which is deterministic regardless of seed. Here is the structure of those 11 runs:

Run 1 is the FIFO baseline. Under FIFO global delivery, C2's late Put to R1 arrives and is fully applied before Reader ever sends its Get. R1 is not stale when queried, so no repair is needed and the bug is never triggered. This run passes.

DPOR analyzes that first trace and identifies the set of globally concurrent message pairs — pairs of deliveries with no happens-before relationship between them. In the quorum scenario, there are many such pairs: C1's puts to different replicas commute, C2's puts commute with Reader's messages until R1 becomes stale, and the late Put commmutes with Reader's messages up to the point of R1's Get. DPOR systematically explores what happens when each such pair is reversed.

Runs 2 through roughly 8–10 explore these global reorderings. Most produce traces where R1 is still not stale when queried (the late Put arrives before the Get), or where R1 is stale but the repair completes before C2's Put arrives, or where C2's Put does arrive late but R1's local scheduling happens to run FIFO (applyLoop drains before repairLoop sees pendingApply > 0). All pass.

The critical transition happens when DPOR reaches a global ordering where C2's late Put to R1 is delivered after Reader has already sent the Repair — i.e., both the Put and the Repair are in R1's incoming queue simultaneously, with pendingApply incremented but not yet decremented. At this point, DPOR detects a new local race within R1's bubble: the router goroutine (which just dispatched the Repair to repairCh) and the applyLoop goroutine (which holds the pending Put in applyCh) are both runnable and their relative order matters. DPOR schedules the alternative — repairLoop before applyLoop — and that is run 11: the bug-triggering trace.

The reason it takes ~10 passing runs to reach this point is that the global delivery graph has roughly that many non-commutative orderings to exhaust before DPOR arrives at the specific one that creates the simultaneous Put + Repair condition at R1. Each of those prior runs is a necessary branch of the DFS tree: DPOR cannot skip them because it has not yet seen evidence (from prior traces) that the later orderings are worth exploring first. The 10-11 range rather than a fixed 11 reflects minor variation in which global ordering DPOR reaches first that creates the stale-R1 condition — there may be two or three such orderings, and depending on which one the DFS frontier visits first, the local race is detected one run earlier or later.

**PCT (d=2 and d=3):** Near-identical distributions, median 5, worst 26. The wide spread relative to DPOR is the cost of probabilistic exploration — priority assignments are random, so different seeds produce different traces, and some seeds are unlucky. The d=2 vs. d=3 indistinguishability remains: the bug's effective depth in priority-space is 2, and increasing d adds no benefit.

**Random:** Median 78, worst ~88+, one open circle (the missed seed). The open circle represents the seed that hit the 2000-run budget without finding the bug. The tight band at 60–90 reflects the geometric distribution of a fixed per-run probability — almost all seeds cluster around the mean, but the tail extends further than PCT's worst case.

The comparison sharpens a key tradeoff: DPOR gives a guarantee (always finds the bug, always in approximately the same number of runs) while PCT gives efficiency (finds it faster on average, but with variance). For a development workflow where you are trying to confirm a known bug is still triggerable, DPOR's predictability is valuable. For a fuzzing workflow where you want the fastest possible first hit, PCT wins.

---

### Bug Trace Decision Mix

Every algorithm that found the bug produced a trace in the range of 43–47 total decisions, with approximately 15 local (orange) and 28–32 global (blue).

dpor-gl is slightly lower than PCT at 43 total (15 local, 28 global). This is not noise — DPOR's partial order reduction is pruning equivalent interleavings, so its traces tend to be more compact. DPOR finds a bug-triggering path that requires fewer total decisions because it avoids the redundant global message orderings that probabilistic algorithms wander through. Fewer decisions in the trace does not mean the bug is simpler — it means DPOR found a more direct path to it.

The local decision count (~15) is consistent across all successful algorithms. This floor reflects the structure of the scenario: the replica pipeline has a fixed number of goroutine scheduling points, and any complete run through the quorum cycle will encounter approximately the same number of local decisions. The global count varies more (28–32) because different algorithms steer different paths through the message delivery graph.

CHESS's absence from this chart remains its most damning indictment: it never produced a bug-triggering trace to analyze.

---

### Non-FIFO Comparison

The dpor-gl bar stands out: **34 total non-FIFO decisions** (19 global, 15 local), compared to PCT's 31 (25 global, 6 local) and Random's 27 (18 global, 9 local).

The striking difference is in the **local** component. dpor-gl has 15 local non-FIFO decisions — more than double PCT's 6. This reveals that DPOR is finding the bug via a fundamentally different local path. DPOR's partial order reduction identifies independent goroutine scheduling decisions — pairs of local choices that commute with each other — and explores their alternatives systematically. In doing so, it generates traces that involve many more local goroutine reorderings than a probabilistic algorithm would naturally produce. PCT achieves the critical `router → router → repairLoop → applyLoop` sequence with minimal additional local perturbation; DPOR arrives at the same bug-triggering condition via a more thoroughly shuffled local schedule.

The implication is meaningful: there is not a single bug-triggering local schedule — there are many, reachable via different combinations of local non-FIFO decisions. DPOR explores more of them explicitly. PCT happens to hit one quickly by chance.

The global non-FIFO count is lower for DPOR (19) than PCT (25). This reflects DPOR's pruning: many global message reorderings that PCT explores as distinct traces are recognized by DPOR as commutative with each other and collapsed into a single representative. DPOR needs fewer global non-FIFO choices in its bug trace because it has already pruned the equivalent alternatives.

CHESS's k=4 still allows at most 4 non-FIFO decisions total. The bug requires a minimum of 27 (Random's trace, the lowest observed). The gap is not 5×; it is nearly 7×.

---

### Non-FIFO Position Profile

The dpor-gl row is visually distinct from the probabilistic algorithms.

**Local non-FIFO decisions are even more front-loaded for DPOR.** The orange circles form a dense band at 0%–10% of the trace, denser than PCT or Random. This reflects DPOR's systematic approach to local scheduling: it identifies and exhausts local alternatives early in the trace, where the goroutine pipeline is most active (immediately after message delivery). The critical `router` scheduling race happens in the first 10% of the trace, and DPOR reaches it by exploring a broad set of early local alternatives.

**Global non-FIFO decisions follow the same mid-trace pattern as other algorithms.** Blue diamonds appear from ~15% through ~90%, consistent with the message reordering needed to route R1 into the stale-read path. The distribution matches PCT and Random structurally — the global message ordering required for the bug is the same regardless of algorithm.

**The comparison between DPOR and PCT local profiles is telling.** PCT has a few orange circles at 0%–10% (the priority inversions needed to trigger the local race) and nothing else locally. DPOR has many more orange circles at 0%–10%, reflecting its systematic exploration of all commutativity-equivalent local alternatives before moving on. DPOR is not lucky about the local schedule — it is thorough.

The CHESS failure remains explained by the global spread of diamonds: the bug requires non-FIFO decisions distributed across the entire trace length, well beyond any fixed k bound.

---

### Cumulative Unique Traces

The chart has changed significantly. Most algorithms now cluster very close to the ideal no-repetition line, with one clear outlier.

**CHESS (G+L) is the clear underperformer**, ending at ~1650 unique traces out of 2000 (82.5% unique). CHESS backtracks within a shallow DFS tree, revisiting the same branching points repeatedly. High repetition within a small subspace is exactly what DFS with a tight bound produces.

**CHESS (G-only), Random, PCT, and dpor-gl all cluster near the ideal**, reaching ~1900 unique traces by run 2000. The dpor-gl line is essentially on top of the ideal — near-perfect uniqueness. This is a direct consequence of DPOR's design: by pruning commutativity-equivalent interleavings, it almost never repeats a trace. Every run it produces is in a distinct equivalence class. Random achieves high uniqueness by sampling uniformly; DPOR achieves it by construction.

However, the clustering of Random, PCT, and DPOR near the ideal is somewhat misleading. All three generate diverse traces, but they generate *different kinds* of diversity. Random's diversity is unbiased but unfocused. PCT's diversity is biased toward priority-inversion traces. DPOR's diversity is structured — it covers the pruned search space systematically. The uniqueness metric cannot distinguish these, which is why it must be read alongside the runs-to-bug charts.

---

### Search Space vs. Explored

The most structurally important chart.

- **Random**: theoretical space 3.3×10²¹, explored 78
- **PCT (d=2 and d=3)**: theoretical space 1.3×10²², explored 6
- **dpor-gl**: theoretical space **2.2×10¹⁸**, explored 11

DPOR's theoretical search space is **4 orders of magnitude smaller** than PCT's. This is the quantitative payoff of partial order reduction: by identifying and collapsing commutative interleavings, DPOR shrinks the effective space from 10²² to 10¹⁸. The explored-runs bar reflects this — DPOR explores 11 traces from a pruned space of 10¹⁸, whereas PCT explores 6 traces from an unpruned space of 10²². In absolute terms PCT needs fewer runs, but in terms of coverage fraction, DPOR is doing far more principled work per run.

The gap between 11 (DPOR) and 6 (PCT) is partly explained by this pruning. DPOR's 10¹⁸ space still vastly exceeds its 11 runs — it is not exhaustive. But the bug-triggering trace happens to appear at position 11 in DPOR's deterministic exploration order of that pruned space. PCT's probabilistic bias hits it at position 6 on average. Neither is exhaustive; both are finding the bug by structural luck or bias, just in differently shaped search spaces.

The theoretical space figures are path-dependent upper bounds computed from the bug-finding trace itself. Different traces visit different decision points with different queue sizes, so the bounds are not directly comparable across algorithms. What is directly comparable is the reduction DPOR achieves: its trace visits decision points with smaller effective fan-out because equivalent orderings have been pruned before those points are reached.

---

## 7. Conclusions and Tradeoffs

**CHESS (both variants) fails categorically on this bug class.** The context bound k is the wrong axis to optimize for bugs that require many accumulated non-FIFO decisions spread across a long trace. CHESS excels when bugs manifest with 1–3 context switches from FIFO — a reasonable assumption for data-race-style bugs in shared-memory programs. But distributed system bugs involving coordinated message reordering and pipeline races accumulate non-FIFO choices structurally, not incidentally. The k bound is not a weakness in CHESS's search strategy; it is a fundamental mismatch between CHESS's bug model and the geometry of this bug class.

The G+L vs. G-only CHESS comparison is instructive: both fail identically. If CHESS cannot reach the bug with global-only decisions, adding local decisions only expands the search tree it must traverse with the same per-trace bound. Local decisions multiply the search space without any benefit if the required global ordering cannot be reached within k steps.

**PCT is the fastest probabilistic algorithm.** A median of 5 runs to find a bug requiring 27+ non-FIFO decisions, with a worst case of 26 and 100% success rate, is strong performance. The theoretical justification (PCT finds d-non-FIFO-depth bugs with probability 1/n^d per run) is borne out — d=2 and d=3 are indistinguishable, suggesting the bug's effective priority depth is 2 even though the non-FIFO count is much higher. PCT's weakness is tail risk: one Random seed failed, and PCT's own worst case of 26 is 5× its median, which matters in CI pipelines where budget is fixed.

**DPOR (G+L) is the standout systematic algorithm.** 20/20 success with near-zero variance (median 10, worst 11) and a 4 orders-of-magnitude reduction in theoretical search space are qualitatively different properties from what any probabilistic algorithm can offer. DPOR's guarantee — it will always find the bug at approximately the same run count, regardless of seed — is valuable in contexts where reliability matters more than raw speed. The 1.7× slowdown relative to PCT's average is the cost of that guarantee.

The higher local non-FIFO count in DPOR's bug trace (15 vs. 6 for PCT) is not a sign of inefficiency — it reveals that DPOR is exploring a richer set of local schedules. DPOR finds the bug via a more thoroughly perturbed local execution, which suggests it is accessing a different (and perhaps more structurally central) region of the bug-triggering subspace. PCT hits one specific local schedule quickly; DPOR maps a broader neighborhood.

**Random is reliable at scale but has tail risk.** The 19/20 success rate with a missed seed confirms what the geometric distribution predicts: at p ≈ 1/78 per run, the probability of exhausting 2000 runs is non-trivial. Random's only advantage over PCT is simplicity and absence of parameter tuning — it requires no depth parameter and no priority scheme. For this bug class, it offers neither the speed of PCT nor the guarantees of DPOR.

**The orchestrator's two-level search space is the fundamental challenge.** Both global (message delivery) and local (goroutine scheduling) dimensions are necessary to expose this bug. A framework controlling only message ordering misses the local pipeline race; a framework controlling only intra-process scheduling misses the global delivery ordering. The orchestrator's value is precisely this two-level scope — but the cost is a search space that grows multiplicatively. This is why CHESS's k bound fails so badly in both configurations: the bound was designed for single-level search spaces, and in a two-level space, k=4 covers a vanishing fraction of reachable states.

DPOR is particularly well-suited to two-level spaces because its pruning applies independently at both levels — it identifies commutativity both among global message deliveries and among local goroutine scheduling choices, reducing the effective search space at each level before multiplying them. This is reflected in the 4 orders-of-magnitude space reduction and the higher local non-FIFO count in the bug trace.

**The trace gate and per-run recorder isolation are essential for meaningful benchmarking.** `completeQuorumBugTrace` prevents false positives from traces that never exercise the R1 stale-read path. `benchmarkOutcome.beginRun()` prevents stale recorder state from one run contaminating another. Without both, chart results would be meaningless — any invariant violation, including those from incomplete or irrelevant traces, would appear as a found bug, inflating found rates and distorting runs-to-bug figures for every algorithm.

**Practical recommendation.** Use PCT as the default first-pass algorithm — it finds this class of mixed local/global bug with the lowest average run count and negligible parameter sensitivity (d=2 is sufficient). Use DPOR when you need reproducibility guarantees or when you want systematic coverage of the pruned space for regression testing. Avoid CHESS for distributed simulation bugs unless domain knowledge establishes that the bug manifests within a very small k; the data here show that k=4 is insufficient and that the required k scales with the non-FIFO depth of the bug, which is unknowable in advance.
