# Quorum Read Stale Replica Bug (G+S, Depth 2)

## The Bug

Client reads from two replicas using `select` (races responses).
Should collect both and pick highest version. Instead takes first/random.
Returns stale value when fresh value exists.

## Pseudocode

```go
// 4 nodes: ReplicaA, ReplicaB, Writer, Client
// A.x=0, B.x=0. Writer sets A.x=1. B stays stale.

func writer() {
    send(ReplicaA, Write{Key: "x", Value: 1})
}

func client() {
    chA := make(chan int, 1)
    chB := make(chan int, 1)
    go func() { chA <- read(ReplicaA, "x") }()
    go func() { chB <- read(ReplicaB, "x") }()

    select {                  // BUG: should collect both
    case v := <-chA: return v //      and pick max version
    case v := <-chB: return v
    }
}
```

## Complete State Space (4 traces)

| Trace | G (Write timing) | S (Select) | A reads | B reads | Result | Safe? |
|-------|-------------------|------------|---------|---------|--------|-------|
| τ₀ | FIFO (after reads) | default (chA) | 0 | 0 | 0 | Yes |
| τ₁ | FIFO (after reads) | non-default (chB) | 0 | 0 | 0 | Yes |
| τ₂ | non-FIFO (before reads) | default (chA) | 1 | 0 | 1 | Yes |
| τ₃ | non-FIFO (before reads) | non-default (chB) | 1 | 0 | **0** | **BUG** |

## Why Depth 1 Misses It

- **G only (τ₂):** Write arrives first. A=1, B=0. Select default picks A. Returns 1. Safe.
- **S only (τ₁):** Write arrives after reads. Both=0. Select picks B. Returns 0. Same as A. Safe.
- **G+S (τ₃):** Write first AND select picks stale replica. Returns 0 when 1 exists. **BUG.**

The G decision creates the OPPORTUNITY (different values). The S decision exploits it (picks wrong one).

## Why Existing Tools Miss It

- **CHESS:** Only explores goroutine scheduling (L). Can't reorder Write delivery (G) or control select (S).
- **FlyMC:** Explores delivery order (G). Finds τ₂ but can't control select. If runtime happens to pick chA, reports safe.
- **Our system:** Explores G+S together. Systematically finds τ₃ at depth 2.

## Real-World Analog

Dynamo/Cassandra stale reads. Jepsen findings. Kleppmann DDIA Chapter 5.
Client reads from fastest replica instead of comparing versions across quorum.
