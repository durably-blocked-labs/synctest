# Scheduling Algorithms

The orchestrator package implements several algorithms that help the orchestrator determine how to pick which global and local operations to run from a queue. We’ve implemented 4 algorithms: **CHESS** (DFS + k bound), **DPOR** (Dynamic Partial-Order Reduction), **PCT**, and a **random** pick algorithm. 

All three take decisions at a **DecisionPoint**:
* **Global:** Which “rpc” message to deliver next or fire timer.
* **Local:** Which goroutine to run next inside a bubble.

---

## CHESS - Context Bounded DFS
CHESS explores scheduling interleavings by exhaustive depth-first search, bounded by a parameter $k$ that limits how many decisions can differ from the default FIFO order in any single trace. With $k=2$, at most 2 decisions are non-default; the rest follow FIFO. This keeps the search space small while still catching most bugs, since most concurrency bugs need only 1-2 non-default scheduling choices to trigger.

In our case, we defined these preemptive context switches as the points where we have more than 1 decision to make at the global or local level.

### Algorithm
1.  **First run:** Run the test with default scheduling (FIFO: always pick index 0). Record every decision point and what alternatives existed.
2.  **Find alternatives:** Walk the trace. At each step where there were multiple goroutines (or messages) to choose from, create a variant: *"replay everything up to this step identically, but pick a different alternative here."* Add each variant to a stack. Skip any variant that would exceed $k$ non-default choices.
3.  **Replay:** Pop the next work item. Replay its prefix step-by-step (each step forces a specific decision). Once the prefix is exhausted, default to FIFO for all remaining decisions. Record the new trace.
4.  **Repeat:** Branch on the new trace (step 2), then pop the next item (step 3). Stop when the stack is empty—all interleavings within bound $k$ have been explored.

### Modes
* **Global Mode (Explore):** This only branches on global messages. Local goroutine scheduling always returns 0. This mode explores global interleavings.
* **Global + Local Mode (ExploreAll):** Branches on both global and local decisions. This mode explores the cross product of global and local interleavings. 
* **Local Mode:** Only branches on local decisions.

### Properties
| Property | Value |
| :--- | :--- |
| **Complete** | Yes - All interleavings within bound are explored |
| **Termination** | Yes - Finite search space for fixed bound |
| **Deterministic** | Yes - Identical results every run |

---

## DPOR - Dynamic Partial-Order Reduction

DPOR improves on CHESS by using the *dependency relation* between scheduling decisions to prune redundant interleavings. Two steps are **dependent** if reordering them can produce a different observable outcome. DPOR only branches at steps where the chosen alternative is actually dependent on something that happened later in the trace — skipping branches that CHESS would explore but that are guaranteed to produce equivalent executions.

The dependency relation is scoped to what the orchestrator can observe without runtime memory instrumentation:
- **Global message deliveries** conflict when they share a sender or a target node (endpoint overlap).
- **Local scheduling decisions** conflict when they occur within the same node.
- **Messages with disjoint endpoints** are treated as independent and do not generate branches against each other.

An alternative at step $i$ is considered dependent if: (a) the chosen index and the alternative index share a resource (i.e., are dependent with each other), or (b) some later step in the trace is dependent with the alternative. If a later step already "consumes" the alternative (its `ChosenID` matches the alternative's ID), the alternative is no longer relevant and the branch is skipped.

### Algorithm
1. **First run:** Run with FIFO (always pick index 0). Record every decision point — the chosen index, all alternatives, their IDs, and their resources.
2. **Find dependent alternatives:** Walk the completed trace. For each step $i$ (starting after any replay prefix), for each alternative `alt ≠ chosen`:
   - Check if `alt` is dependent with the chosen step, or dependent with any later step in the trace (and not overtaken by it).
   - If dependent, and the non-FIFO count for the new prefix does not exceed the bound $k$, push a new work item: *"replay the trace identically up to step $i$, but pick `alt` at step $i$."*
   - Deduplicate using a trace key (kind + node + index + chosenID per step) to avoid re-exploring the same prefix.
3. **Replay:** Pop the next work item from the stack. Follow its prefix exactly, then use the frontier scheduler for all remaining decisions.
4. **Repeat:** Branch on each new trace (step 2), then pop the next item (step 3). Stop when the stack is empty.

### Dependency relation

The resource for a step is derived from its `Resource` field (if set), otherwise from the node name (local) or the message target (global). Two resources `a` and `b` are dependent if they share any endpoint:

| a \ b | node | `X\|Y` |
| :--- | :--- | :--- |
| **node** | `a == b` | `a == X` or `a == Y` |
| **`A\|B`** | `A == b` or `B == b` | `A == X` or `A == Y` or `B == X` or `B == Y` |

`ConservativeGlobal` mode treats *all* pairs of global deliveries as dependent, appropriate for protocols where disjoint-endpoint messages can still race through quorum logic.

### Modes
* **GlobalOnly:** Only branches on global message deliveries. Local decisions always return 0 (FIFO). Reduces the search space by ignoring intra-node scheduling.
* **Global + Local (default):** Branches on both global and local decisions separately. Two passes over the trace — one for local steps, one for global steps — keep their alternative sets independent.

### Search-order hints (do not affect soundness)
* **`PrioritizeEndpoints`:** Branches whose alternative touches a named endpoint are pushed with a higher score, so they are explored first.
* **`PrioritizeRequests`:** At the frontier, prefer request-like messages (those not ending in `Ack`/`Resp`) over responses.
* **`Frontier`:** Custom callback to override the frontier choice before the built-in heuristic runs.

### Properties
| Property | Value |
| :--- | :--- |
| **Complete** | Yes - All non-equivalent interleavings within bound are explored |
| **Termination** | Yes - Finite search space for fixed bound |
| **Deterministic** | Yes - Identical results every run |
| **Pruning** | Skips interleavings that are provably equivalent under the dependency relation |

---

## PCT - Probabilistic Concurrency Testing
The core idea of PCT is to assign random priorities to schedulable entities and introduce $d-1$ randomly sampled priority change points. At each change point, the currently winning entity is demoted (its priority is raised above all others). This controlled randomness gives PCT a lower bound on bug-finding probability.

Entities in our algorithm are anything that can be scheduled. When choosing global entities, we group messages by type—for example `msg:A→B(AppendEntriesRequest)`. Local entities are individual goroutine IDs such as `B5`.

### Algorithm
1.  **Setup:** Seed the RNG. Setup with parameters:
    * **Depth:** Branching factor.
    * **Max Steps:** Step range from which change points are sampled.
    * **Seed:** Base RNG seed.
2.  **Decide:** Scan all alternatives, calling `priorityOf(alt.ID)` for each. The first call for a given ID draws a random value in $[0, pctBasePriority)$ and caches it. Pick the alternative with the lowest priority. Then, if step is a sampled change point, overwrite the winner’s priority with `nextLow` and increment `nextLow`. This demotes the winner: it will lose every future comparison against any non-demoted entity. Increment step and return the chosen index.
3.  **Next run:** Increment the seed, clear all state, repeat.

The parameter $d$ controls how many priority flips happen per run. $d=2$ means 1 flip, $d=3$ means 2 flips. Higher $d$ finds deeper bugs but each run is less likely to find any specific bug.

### Properties
| Property | Value |
| :--- | :--- |
| **Complete** | No - Probabilistic, not exhaustive |
| **Termination** | No - Requires `MaxRuns` cap |
| **Deterministic** | Yes per run if seeded |
| **Bug finding Guarantee** | $\ge 1 / (n \cdot k^{d-1})$ per run |

---

## Random
At every decision point (global and local), pick uniformly at random among all alternatives.

### Properties
| Property | Value |
| :--- | :--- |
| **Complete** | No |
| **Termination** | No - Requires `MaxRuns` cap |
| **Deterministic** | Yes per run if seeded |
| **Bug finding Guarantee** | None - Depends on luck and run budget |