# Microbenchmarks: Depth-1 Bugs

Four minimal examples, one per dimension. Each has exactly one bug at depth 1.

## Summary

| Example | Dimension | Bug Pattern | Traces at d=0 | Traces at d=1 |
|---------|-----------|-------------|----------------|----------------|
| L1 | Local (L) | Use-before-init | 1 (safe) | 2 (1 buggy) |
| G1 | Global (G) | Stale term | 1 (safe) | 2 (1 buggy) |
| S1 | Select (S) | Timeout beats result | 1 (safe) | 2 (1 buggy) |
| LG1 | L+G | Read-your-writes | 1 (safe) | 3 (1 buggy) |

---

## L1: Local Scheduling — "Init Before Use"

Two goroutines, shared state. Writer initializes config. Reader uses it.
FIFO runs writer first (runnext). Non-FIFO runs reader first → panic.

```go
func InitBeforeUse() {
    var config string
    var wg sync.WaitGroup
    wg.Add(2)

    // G1 (BGID=1): reader
    go func() {
        defer wg.Done()
        if config == "" {
            panic("not initialized")  // BUG
        }
    }()

    // G2 (BGID=2, runnext): writer
    go func() {
        defer wg.Done()
        config = "safe-mode"
    }()

    wg.Wait()
}
```

```
D0: runq = [G1, G2(runnext)]

  FIFO (idx=0) → G2:           Non-FIFO (idx=1) → G1:
    config = "safe-mode"          config == "" → PANIC
    G1: reads "safe-mode" ✓       G2: config = "safe-mode"
    SAFE                          BUG (depth 1)
```

---

## G1: Global Delivery — "Stale Term"

One follower node, two messages from different leaders in the pending queue.
FIFO delivers the higher-term first (safe). Reversed delivery applies stale entries.

```go
func Follower(inbox <-chan Msg) {
    term := 0
    for msg := range inbox {
        if msg.Term >= term {
            term = msg.Term
            apply(msg.Entries)
        }
    }
}

// Pending: [M1{Term:2, From:"A"}, M2{Term:1, From:"B"}]
```

```
D0: pending = [M1(term=2), M2(term=1)]

  FIFO (idx=0) → M1 first:     Non-FIFO (idx=1) → M2 first:
    accept M1, term=2             accept M2, term=1
    reject M2 (1 < 2) ✓          apply B's stale entries ✗
    SAFE                          then accept M1, term=2
                                  BUG: stale entries applied
```

---

## S1: Select Ordering — "Timeout Beats Result"

One goroutine, select with result channel AND timeout both ready.
Default case order picks result (safe). Non-default picks timeout (spurious failure).

```go
func FetchWithTimeout(resultCh <-chan string, timeout <-chan time.Time) (string, error) {
    select {
    case val := <-resultCh:   // case 0
        return val, nil
    case <-timeout:           // case 1
        return "", errors.New("timeout")  // BUG
    }
}
// Both channels ready simultaneously
```

```
S0: ready = [resultCh(0), timeout(1)]

  Default (case 0):            Non-default (case 1):
    return "data", nil           return "", "timeout"
    SAFE                         BUG (depth 1)
```

---

## LG1: Combined — "Read Your Writes"

One node, two goroutines: writer sends SET, reader sends GET. Under FIFO
local, writer runs first → messages serialized → safe. Under non-FIFO local,
reader runs first → both messages pending simultaneously → FIFO global
delivers GET first → reads empty → bug.

```go
func ReadYourWrite(io NodeIO) string {
    var result string
    var wg sync.WaitGroup
    wg.Add(2)

    // G1 (BGID=1): reader
    go func() {
        defer wg.Done()
        result = nodeGet(io, "key")
    }()

    // G2 (BGID=2, runnext): writer
    go func() {
        defer wg.Done()
        nodeSet(io, "key", "written")
    }()

    wg.Wait()
    if result == "" { panic("read-your-write violated") }
}
```

```
D0 (LOCAL): runq = [G1(reader), G2(writer, runnext)]

  FIFO → G2 first:                Non-FIFO → G1 first:
    SET → deliver → store="written"   GET → ExternalWait
    GET → deliver → result="written"  SET → ExternalWait
    SAFE (serialized)                 pending = [GET, SET]
                                      D1 (GLOBAL): FIFO delivers GET first
                                        → result="" → BUG

Bug at depth 1 (one local non-default). Dimensions interact:
local reorder creates concurrent messages → global FIFO hits the bug.
```
