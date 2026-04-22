# Crown Jewel: Depth-3 Cross-Dimensional Bug

## The Dual-Guard Bypass

A depth-3 bug that requires non-default decisions in ALL THREE dimensions (L, G, S). Any subset of 2 non-defaults is safe. Only the full combination triggers the bug.

**Real-world pattern:** Distributed database with lease-based locking and dual validation (HBase region servers, CockroachDB leaseholders, Kleppmann's fence token pattern).

## The System

```
STORE (orchestrator):
  counter = 0, version = 1

NODE B (competitor):
  rpc("WRITE", 42)  // counter=42, version=2, triggers notifications to A

NODE A (victim — 3 goroutines):
  lockValid := true
  val, ver := rpc("READ")                // val=0, ver=1

  // GUARD 1: Push listener (goroutine W1)
  go func() {
      <-invalidationCh                   // "lock stolen" notification
      lockValid = false                  //    ← L: runs before main reads it?
  }()

  // GUARD 2: Dual-source listener (goroutine W2)
  go func() {
      select {                           //    ← S: which channel?
      case <-invalidationCh2:            // "lock stolen" (second channel)
          lockValid = false
      case <-renewalCh:                  // "lock renewed" (stale)
          // lockValid stays true — missed the invalidation!
      }
  }()

  // Version check (main goroutine)
  verOk := rpc("CHECK_VER", ver)         //    ← G: B wrote yet?

  // Write only if BOTH guards pass
  if lockValid && verOk {
      rpc("WRITE", val+1)               // BUG: overwrites B's 42!
  }
```

## Three Dimensions

| Decision | Dimension | Default (safe) | Non-default (unsafe) |
|----------|-----------|----------------|----------------------|
| **G** | Global | B writes before CHECK_VER → verOk=false | B writes after → verOk=true (FP) |
| **L** | Local | W1 runs before main → lockValid=false | W1 delayed → lockValid=true (stale) |
| **S** | Select | W2 picks invalidation → lockValid=false | W2 picks renewal → lockValid stays true |

**Write gate:** `lockValid && verOk`
- `verOk = true` requires **G_nd**
- `lockValid = true` requires **L_nd AND S_nd** (both watchers fail)
- **Bug = G_nd AND L_nd AND S_nd**

## All 8 Traces

| # | G | L | S | verOk | W1 | W2 | lockValid | Bug? |
|---|---|---|---|-------|----|----|-----------|------|
| 1 | 0 | 0 | 0 | F | set F | set F | F | Safe |
| 2 | 0 | 0 | 1 | F | set F | missed | F (W1) | Safe |
| 3 | 0 | 1 | 0 | F | missed | set F | F (W2) | Safe |
| 4 | 0 | 1 | 1 | F | missed | missed | T | Safe (verOk=F) |
| 5 | 1 | 0 | 0 | T | set F | set F | F | Safe |
| 6 | 1 | 0 | 1 | T | set F | missed | F (W1) | Safe |
| 7 | 1 | 1 | 0 | T | missed | set F | F (W2) | Safe |
| 8 | 1 | 1 | 1 | T | missed | missed | T | **BUG** |

**7 safe, 1 buggy. QED.**

## Why Each Depth-2 Subset is Safe

- **G+L (trace 7):** W2 picks invalidation (S default) → lockValid=false. Safe.
- **G+S (trace 6):** W1 runs (L default) → lockValid=false. Safe.
- **L+S (trace 4):** Version check catches it (G default) → verOk=false. Safe.

Each pair is saved by the THIRD guard. Only when all three fail simultaneously does the bug occur.

## State Space Size

| Bound K | Traces | Bugs Found |
|---------|--------|------------|
| 0 | 1 | 0 |
| 1 | 4 | 0 |
| 2 | 7 | 0 |
| **3** | **8** | **1** |

**Context bound K=2 (the default in CHESS, our explorer) MISSES this bug.** Only K=3 finds it.

## Why This Matters

Redundant safety mechanisms create a false sense of security. Engineers add a second guard because the first can fail. But each guard has its own failure mode in a different dimension. The bug hides at the intersection of three independent failures — each pair is covered by the remaining guard.

This is the argument for exploring L × G × S together, not in isolation.
