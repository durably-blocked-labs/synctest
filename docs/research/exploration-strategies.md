# Exploration Strategies — Research Notes

Source: Deep analysis of CHESS, PCT, DPOR, Fray, FlyMC, Morpheus papers.

## Priority Order for Implementation

1. **Per-link FIFO** — only reorder across links, not within. TCP guarantees.
2. **POS** — replace/complement DFS for large scenarios. Better than PCT.
3. **Conflict analysis** — record conflicts via vector clocks, skip non-conflicting ops. 64% improvement (Morpheus).
4. **DPOR for messages** — messages to different nodes are independent. Don't explore both orders.
5. **Symmetry** — collapse equivalent follower orderings in raft.
6. **Fair scheduling** — FIFO default already helps. Formally enforce eventually.
7. **Happens-before caching** — lightweight state caching via vector clock hashing.

## Key Findings

- Context bound K=2 finds most bugs (depth 1-3 holds for distributed systems)
- PCT: probability 1/(n*k^(d-1)). POS is strictly better for distributed.
- DPOR: messages to different nodes are independent. FlyMC gets 6 orders of magnitude reduction.
- Morpheus conflict analysis: 64.82% average improvement. Only 325/6593 operations conflict.
- Per-link FIFO: essential for production relevance. Only reorder across links.
- State caching: use happens-before graph hashing, not full heap state.
- Bug depth: 1-3 critical reorderings, but embedded in traces of 12-54 events.
