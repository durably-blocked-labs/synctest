# Algorithm Comparison: Detection Probabilities

## Algorithms

1. **Random Walk** — uniform random at each decision. P(bug) = product of 1/b_i for each non-default decision.
2. **PCT(d)** — priority-based. P ≥ 1/(N·k^(d-1)). N=entities, k=steps, d=depth parameter.
3. **POS** — per-operation priorities. Independent at each step. P = product of 1/b_i (same as Random Walk when decisions are causally independent).
4. **DFS(K)** — exhaustive within bound. Guaranteed at depth ≤ K. Hard cliff at K.
5. **POS+conflict** — POS but only randomize conflicting operations. First trial wasted on detection.

## Results

| Algorithm | Quorum (G+S, d=2) | RA Mutex (G+L, d=2) | Dual-Guard (G+L+S, d=3) |
|---|---|---|---|
| Random Walk | P=1/4, E=4 | P=1/6, E=6 | P=1/8, E=8 |
| PCT(d=2) | P≥1/18, E≤18 | P≥1/96, E≤96 | **No guarantee** |
| PCT(d=3) | P≥1/108, E≤108 | P≥1/1536, E≤1536 | P≥1/256, E≤256 |
| POS | P=1/4, E=4 | P=1/6, E=6 | P=1/8, E=8 |
| DFS(K=2) | 4 traces ✓ | 118 traces ✓ | 7 traces ✗ |
| DFS(K=3) | 4 traces ✓ | ~800 traces ✓ | 8 traces ✓ |
| POS+conflict | P=1/4, E=5 | P=1/6, E=7 | P=1/8, E=9 |

## Key Findings

- PCT is weakest on small examples (1/(N·k^(d-1)) >> 1/2^d)
- POS = Random Walk when decisions are causally independent
- DFS(K=2) misses depth-3 bugs entirely
- POS+conflict negligible benefit on small examples (all decisions are conflicts)
- Depth-3 example discriminates bounded vs unbounded algorithms

## DFS State Space Sizes

### Quorum Read (2 binary decisions)
| K | Traces | Found? |
|---|--------|--------|
| 0 | 1 | No |
| 1 | 4 | No |
| 2 | 4 | Yes |

### RA Mutex (~12 decisions, avg branching ~2.25)
| K | Traces | Found? |
|---|--------|--------|
| 0 | 1 | No |
| 1 | 16 | No |
| 2 | 118 | Yes |

### Dual-Guard (3 binary decisions)
| K | Traces | Found? |
|---|--------|--------|
| 0 | 1 | No |
| 1 | 4 | No |
| 2 | 7 | No |
| 3 | 8 | Yes |
