# Paper Summaries — Extended Key Analysis

## Quick Reference

| Paper | Year | Venue | Key Idea | Our Takeaway |
|-------|------|-------|----------|-------------|
| CHESS | 2008 | OSDI | Preemption bounding | Validates our context-bound DFS |
| DPOR | 2005 | POPL | Dynamic independence | We should add for message-level DPOR |
| PCT | 2010 | ASPLOS | Priority-based randomized scheduling | Add as alternative strategy |
| Fray | 2025 | OOPSLA | Shadow locking on JVM | Closest single-node analog, validates runtime-level approach |
| FlyMC | 2019 | EuroSys | Symmetry + independence + parallel flips | Key ideas for our global orchestrator |
| Morpheus | 2020 | ASPLOS | POS + conflict analysis | Conflict analysis is our biggest quick win |
| SandTable | 2024 | EuroSys | Spec-level exploration | Two-level exploration idea |
| MACE | 2007 | PLDI | Atomic transitions as scheduling unit | Validates our bubble model |

---

## 1. CHESS (OSDI 2008)

**Problem:** Heisenbugs in concurrent programs are hard to reproduce. Stress testing can't reliably trigger specific interleavings.

**Key insight:** Most bugs need only 1-2 preemptions. Bounding preemptions reduces space from exponential to polynomial (k^c for c preemptions).

**Technique:** Thin wrappers around concurrency API. Three-phase iteration: replay prefix → record new decisions → search for alternatives. Single-threaded execution. Happens-before graph as state representation.

**Problems faced:**
- Imperfect replay (environmental nondeterminism causes divergence — retry and prune)
- Fair scheduling needed (spin-loops hang without it)
- Data races require expensive detection
- State capture for large programs unsolved (stayed stateless)

**Results:** 27 bugs in 8 Microsoft systems (up to 174K LOC). 25 previously unknown. Most needed only 2 preemptions. One PLINQ bug was root cause of 30+ other failures. CCR Heisenbug reproduced in 20 seconds.

**Ideas for us:** Preemption bounding = our context bounding. Happens-before caching. Scoping preemptions to interesting code regions. Fair scheduling for liveness.

**Open problems:** Data race exploration, state capture, weak memory models, parallelizing exploration.

---

## 2. DPOR (POPL 2005)

**Problem:** Exploring all thread interleavings is exponential. Static partial-order reduction over-approximates conflicts.

**Key insight:** Compute independence DYNAMICALLY during execution. Only explore reorderings of transitions that ACTUALLY conflict (touch same memory, at least one write).

**Technique:** DFS with lazily-computed backtrack sets. At each state, track memory accesses. When conflict found, add backtrack point at the earliest state where both transitions were enabled. Vector clocks for efficient happens-before tracking.

**Problems faced:**
- Soundness requires careful backtrack point placement
- Implementation complexity (tracking memory accesses is expensive)
- Stateless — may revisit equivalent states
- Granularity: instruction-level for shared memory, needs adaptation for message-passing

**Results:** 100x reduction on dining philosophers. 600x on Bluetooth driver vs static POR.

**Ideas for us:** Dynamic independence for message delivery — messages to different nodes are independent. For local scheduling, goroutines touching different channels are independent. But we can't track within-quantum memory accesses (our granularity limitation).

**Open problems:** Combining with stateful exploration, weak memory, optimal DPOR.

---

## 3. PCT (ASPLOS 2010)

**Problem:** Systematic testing is complete but slow. Random testing is fast but no guarantees. Can we get provable probabilistic bounds?

**Key insight:** A bug of depth d needs d ordering constraints. Assign random priorities to threads, make d-1 random priority changes → probability ≥ 1/(n·k^(d-1)) of hitting any depth-d bug.

**Technique:** Random initial priorities. Pick d-1 random "priority change points." Always run highest-priority enabled thread. Priority changes force context switches at the right places.

**Problems faced:**
- User must guess depth d (typically 2-3)
- Probability degrades with thread count n and step count k
- No completeness guarantee — may run forever without finding a bug
- Assumes sequential consistency

**Results:** Theoretical framework. PCT(d=2) consistently outperforms random walk. Formal probability bound matches observed rates.

**Ideas for us:** Add PCT as alternative to DFS. Our context bound 2 ≈ PCT depth 2. PCT is trivially parallelizable. Could run alongside DFS for coverage.

**Open problems:** Auto-inferring depth d, extending to distributed systems, combining with DPOR.

---

## 4. Fray (OOPSLA 2025)

**Problem:** No practical general-purpose concurrency testing tool for the JVM. Existing tools: inapplicable (CalFuzzer), too slow (JPF, 31.5x overhead), incomplete (Lincheck), or limited (rr).

**Key insight:** Don't replace concurrency primitives — add extra synchronization around them. Shadow locking: for each thread and resource, maintain a shadow lock that only the scheduler can release.

**Technique:** Bytecode instrumentation via Java agent. Shadow locks around monitors, atomics, volatiles, wait/notify. notify→notifyAll trick for deterministic wake selection. Pluggable strategies: Random, PCT, POS, SURW.

**Problems faced:**
- wait/notify semantics (converted notify→notifyAll + shadow lock selection)
- Thread.sleep (rewritten to no-op wait)
- Object.hashCode (recorded for replay)
- Data races (assumes race-free, optional --memory flag for full instrumentation)
- Kotlin coroutines (can't interleave single-threaded coroutines)

**Results:** 46/53 benchmark bugs found (87%). 70% more than JPF, 77% more than rr. 10x faster than JPF, 457x faster than rr. 18 real bugs in Kafka/Lucene/Guava. 16 confirmed, 12 fixed. Runs all 2,664 real-world test cases (JPF: 0).

**Ideas for us:** Pluggable strategies architecture (we already have this via hooks). SURW implementation (~200 LOC). Push-button applicability matters for adoption. Recording environmental nondeterminism for perfect replay.

**Open problems:** Data race support without overhead, coroutines, automatic "interesting event" identification for SURW.

---

## 5. FlyMC (EuroSys 2019)

**Problem:** Distributed model checkers suffer path explosion. Paxos with 3 concurrent updates = 54 events, existing checkers can't find the bug within 10,000 paths.

**Key insight:** Distributed systems have exploitable structure: state symmetry (followers are interchangeable), event independence (messages to disjoint state commute), parallel flips (reorder at all nodes simultaneously).

**Technique:** Stateless model checker intercepting messages. Three algorithms: (1) abstract state by removing node IDs, skip symmetric paths; (2) static analysis of readSet/updateSet for independence; (3) flip pairs across all nodes simultaneously. Requires ~19 LOC annotations per system.

**Problems faced:**
- Static analysis precision (must also track diskSet, sendSet)
- Parallel flips must verify causal independence via vector clocks
- Annotation burden for C++/Scala (manual set construction)
- 300ms+ wait between events for quiescence detection

**Results:** 16x faster than alternatives (up to 78x). 12 old bugs reproduced, 10 new bugs found. Applied to Cassandra, ZooKeeper, Hadoop, Spark, Raft, Ethereum. Two orders of magnitude path reduction vs DPOR+SAMC.

**Ideas for us:** State symmetry for raft followers. Event independence for cross-bubble messages. Parallel flips as prioritization heuristic. State-event caching to skip known transitions.

**Open problems:** Controlling local thread schedules within nodes. Multi-variable correlation analysis. Scaling beyond 3 concurrent updates.

---

## 6. Morpheus (ASPLOS 2020)

**Problem:** POS applied to distributed systems wastes randomization on non-conflicting operations (>90% of operations). The massive operation count dilutes bug-finding probability.

**Key insight:** After each trial, identify which operations ACTUALLY conflicted (via vector clocks). In future trials, skip non-conflicting operations (schedule them immediately). Focus randomization budget on the ~5% of operations that matter.

**Technique:** Erlang module transformation + message interception. POS scheduling (random priorities per operation). Conflict analysis: vector clocks detect concurrent operations on same process. History table: {ProcessID, ProgramCounter} → ever-conflicted? Non-conflicting operations get immediate scheduling.

**Problems faced:**
- Advanced POS (POS*) performs WORSE than basic POS for distributed systems (false dependencies)
- Conflict signature precision tradeoff ({P,PC} is good balance)
- Non-Erlang code handled with 50ms delay heuristic
- No active fault injection
- Module transformation is the dominant overhead

**Results:** 11 new bugs across locks, gproc, mnesia, rabbitmq/ra. POS+conflict analysis: 280% advantage over baselines. Conflict analysis alone: 64.82% average improvement, up to 241.94%. Only 325/6593 operations conflict in gproc-1.

**Ideas for us:** Conflict analysis is our highest-value optimization. Tag decisions with {goroutineID, programCounter}. After each trial, build vector clocks, identify conflicts. Skip non-conflicting in future trials. POS as scheduling strategy.

**Open problems:** Fault injection, false dependency reduction, non-Erlang code, scaling.

---

## 7. SandTable (EuroSys 2024)

**Problem:** Implementation-level distributed model checking is too slow. One 54-event path takes 36 seconds. 10,000 paths = 100+ hours.

**Key insight:** Explore the state space at the SPECIFICATION level (TLA+), not the implementation level. Specs explore 114x-2989x faster. Confirm bugs by replaying spec traces on the real implementation.

**Technique:** TLA+ specs that model the buggy implementation (not the ideal protocol). Conformance checking validates spec matches impl. BFS exploration at spec level. Constraint ranking prioritizes interesting configurations. Deterministic replay via POSIX interception (LD_PRELOAD) for bug confirmation.

**Problems faced:**
- Spec must model implementation bugs, not ideal behavior (requires conformance checking)
- Manual spec writing: 3-20 person-days
- Thread-level bugs within nodes not modeled
- Sleep-based synchronization in some impls slows replay to 24-28s/trace
- Bounded exploration (infinite state space requires user bounds)

**Results:** 23 bugs across 8 Raft/Zab implementations. 18 new, 17 confirmed, 13 fixed. Depths 8-41 events. Spec explores up to 10^9 states/day. All bugs found within 1 machine-hour. 114x-2989x speedup vs implementation-level.

**Ideas for us:** Two-level approach (fast model-level exploration + implementation replay). Constraint ranking for prioritizing exploration branches. Conformance checking concept.

**Open problems:** Spec maintenance, thread-level bugs, liveness checking, bounded exploration completeness.

---

## 8. MACE (PLDI 2007)

**Problem:** Distributed systems mix layering, concurrency, failures, and analysis concerns. Ad-hoc C++/Java implementations are unreadable and un-analyzable.

**Key insight:** Structure distributed systems as atomic event handlers (transitions). Each handler runs to completion without blocking. Interleave only at event boundaries. This makes model checking tractable.

**Technique:** Mace language (C++ extension) with service objects, events, and aspects. Source-to-source compiler generates dispatch loops, serialization, logging, causal-path tracing. MaceMC model checker: bounded DFS + deep random walks from boundary states.

**Problems faced:**
- Expressiveness restriction (no blocking within transitions)
- MaceMC limited to Mace programs
- Model checking scales to small configurations only
- Distributed failure detection is heuristic (timeouts)

**Results:** 9+ distributed systems. 5x code reduction vs original. MaceMC found 50 bugs in 5 systems. 92%+ consistency in DHT benchmarks.

**Ideas for us:** Atomic transitions = our goroutine yield-to-yield execution. Aspects for invariant checking between decisions. Causal-path tracing (tag decisions with path IDs). Bounded DFS + random walks from boundaries.

**Open problems:** Applicability beyond Mace, scaling model checking, data races.
