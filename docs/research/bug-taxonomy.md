# Bug Taxonomy — Where Bugs Hide

## Bug Types Found by Tools

| Type | CHESS | FlyMC | Morpheus | Fray | SandTable | Dimension |
|------|-------|-------|----------|------|-----------|-----------|
| Safety violations | 27 | 12+10 | 11 | 18 | 23 | All |
| Liveness | Singularity | ZOOK-3 | locks-1, mnesia-1 | — | WRaft#3,#8 | Thread/message |
| Data races | PLINQ | — | — | out of scope | — | Thread |
| Atomicity violations | CCR | — | locks-2 | 6/18 | RaftOS#4 | Thread |
| Order violations | — | — | gproc-1,3 | 5/18 | — | Thread/message |
| Message reordering | — | ALL | ALL | — | ALL | Message delivery |
| Timeout/timer | — | ZOOK-1 | virtual clock | — | WRaft#8 | Timing |

## Bug Depth

- CHESS: max 2 preemptions for most bugs (Table 2)
- PCT: depth 1 (order), depth 2 (atomicity/deadlock)
- FlyMC: 2-3 critical reorderings within 12-54 events
- SandTable: 5-41 events, but few critical choices
- **Small-scope hypothesis holds for distributed systems**

## Where Bugs Hide in Raft

1. **Leader election**: stale votes (Xraft#1, depth 8), split brain, pre-vote bugs
2. **Log replication**: conflicting entries committed (WRaft#1, depth 22), wrong match index (PySyncObj#4, depth 25)
3. **Commitment**: stale commit (PySyncObj#2, depth 13), Figure 8 violation (PySyncObj#5, depth 14)
4. **Config changes**: RemoveLeader select race (our bug #6), ra-1,2,3 (Morpheus)
5. **Snapshot**: compaction + replication interaction (WRaft#1+#2)
6. **Linearizability**: stale reads (Xraft-KV#1, depth 15)

## Figure 8 Problem

- Needs 3 crashes, 3 elections, specific partial replication
- ~8-10 non-default delivery choices (beyond K=2)
- SandTable found it in PySyncObj at depth 14
- **Requires crash injection** — must add to orchestrator
- Practical: write targeted test, not discover from scratch

## Do We Need Heap Analysis?

**No.** All raft bugs detectable through protocol-level state:
- r.Leader(), r.State(), r.Stats()["term"], r.Stats()["commit_index"]
- Log contents, FSM state, configuration

We need good **invariant checks** (oracles):
- At most one leader per term
- Committed entries never overwritten
- Log monotonicity
- Configuration changes atomic

## Finding Real Bugs (Not Synthetic)

1. **Run existing raft tests through orchestrator** — Fray found 371 flaky tests this way
2. **Write scenario tests** — leader crash during replication, partition during election, config change + leader failure
3. **Add crash injection** — 50% of DC bugs need crashes (FlyMC)
4. **Per-link FIFO** — ensures bugs are production-possible
