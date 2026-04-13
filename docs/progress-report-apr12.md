# Progress Report — April 12, 2026

## Session Summary

Two-part session: (1) deep investigation of two orchestrator bugs, (2) codebase cleanup to establish the golden example for future work.

---

## Part 1: Bug Investigation

### Bug 1 — Drain-path Deadlocks in `ra-depth2` (~3/200 ExploreGlobalOnly runs)

**Investigation method**: 7 parallel agents (3 investigation + 4 council review with correctness, failure modes, adversarial, and protocol analysis perspectives).

**Key correction**: The investigation doc incorrectly stated the stale clock mutation was applied in `ra-depth2/lock.go`. It is NOT — `ra-depth2` uses correct RA (`n.reqClock` comparison, Lamport clock updated for all message types). The stale clock mutation only exists in `ra-distributed-lock/lock.go`.

**Root cause identified**: Node early-exit under non-FIFO delivery causes silent message drops.

Concrete scenario (2 non-FIFO global choices):
1. Non-FIFO delivery reorders REPLYs ahead of REQUESTs for node A
2. A collects all REPLYs, enters CS, releases, and **exits** (Stop + Close)
3. A's `deferred` list is empty — A never received B's or C's REQUESTs
4. When B→A:REQ is finally delivered, `op.Execute()` hits `case <-peer.closeCh:` → **message silently dropped**
5. B and C wait forever for A's REPLY
6. Orchestrator detects: all idle, no sends, no timers → drain path → 500ms timeout → `Shutdown()`

This is a **test structure issue**, not a protocol bug. Nodes exit after one lock cycle. Real RA nodes stay alive. The orchestrator's drain path + `Shutdown()` handles it correctly.

**Council consensus** (5/5 agents + codex):
- Circular deferral impossible with correct RA (total order on priority)
- The stale clock mutation cannot cause deadlock — only mutual exclusion violations
- The 500ms wall-clock timeout is a non-deterministic band-aid but functionally correct for this case

### Bug 2 — Run() Hangs in `ra-distributed-lock` KV-server Tests

**Root cause identified**: `ra-distributed-lock/OrchestratorTransport` is missing the `Shutdown()` method that `ra-depth2/OrchestratorTransport` has.

When the drain path fires (no sends, no timers), it tries:
```go
type shutdowner interface{ Shutdown() }
if s, ok := ctrl.transport.(shutdowner); ok {
    s.Shutdown()
}
```
This no-ops for `ra-distributed-lock` transports. After the 500ms timeout, `waitNodeEvent()` blocks forever because the bridge goroutine is stuck in `ExternalWait` and the transport can't be force-closed.

**Resolution**: Package removed entirely (see Part 2).

---

## Part 2: Codebase Cleanup

### Removed (Tier 2 — old API examples)
- `examples/local_replayer/` — used `synctest.Test()`, not orchestrator
- `examples/replayer_scheduler/` — used `synctest.Explore()`, not orchestrator
- `examples/distributed_replayer/` — used old hook-based orchestration

### Removed (Tier 3 — broken architecture)
- `ra-distributed-lock/` — 4-bubble architecture with KV server bubble, hangs in `Run()`, missing `Shutdown()`
- `ra-depth2.test` — stale test binary

### Created — `bugs/ra-gate/` (Golden Example)

Moved and cleaned `ra-depth2/` → `bugs/ra-gate/`, package `ragate`.

**Cleanup**:
- Stripped `MsgKVGet`, `MsgKVPut`, `MsgKVDone`, `MsgKVReply` constants (unused)
- Stripped `Key`, `Value` fields from `Message` struct
- Stripped dead `msgKindName` cases
- Message struct is now minimal: `Kind`, `From`, `Timestamp`

**Files**:
| File | Purpose |
|------|---------|
| `lock.go` | `GateRANode` — RA lock with close(gate) bug pattern |
| `transport.go` | `OrchestratorTransport` with `Shutdown()` support |
| `kv.go` | In-memory KV store (CS body) |
| `bug_demo_test.go` | 4 tests: FIFOPasses, ExploreGlobalOnly, ExploreAll, FindBug |
| `simple_test.go` | Basic sanity test |

**All tests pass**. `ExploreAll` with `AllBound(2)` finds the gate bug.

---

## Part 3: Gate Bug Deep Dive

### Bug Classification: Depth-2 (1G + 1L)

**Verified empirically**:
- `AllBound(1)`: 60 runs, all pass — bug NOT found
- `AllBound(2)`: ~114-227 runs, bug found — mutual exclusion violation
- `AllBound(3)`: ~156 runs, bug found (more permissive, still works)

The bug requires exactly 2 non-default decisions in the DFS tree:

1. **Global (G)**: Reorder message delivery so a REQUEST arrives at a node while it's still collecting REPLYs, creating a deferred entry. This also changes the global interleaving such that a race window exists between nodes.

2. **Local (L)**: After `close(gate)` wakes both App and DeferredFlusher goroutines, schedule DeferredFlusher first. It sees `state=Released` (not `Held` — App hasn't run yet) and prematurely grants deferred REPLYs.

Neither alone suffices:
- G-only (`Explore`): local scheduling is FIFO → App always runs before Flusher → `state=Held` before Flusher checks
- L-only (`AllBound(1)`): FIFO delivery means A completes its entire CS before the orchestrator delivers premature REPLYs to B → no simultaneous CS occupancy

### Concrete Failing Trace (run 114)

Global delivery sequence:
```
B→C(Request) A→B(Request) A→C(Request) B→A(Request) C→A(Request) C→B(Request) C→B(Reply) B→A(Reply) C→A(Reply)
```

| Step | Deliver | Effect |
|------|---------|--------|
| 1 | B→C:REQ **(non-FIFO)** | C replies to B ("C" > "B", lower priority) |
| 2 | A→B:REQ | B replies to A ("B" > "A") |
| 3 | A→C:REQ | C replies to A ("C" > "A") |
| 4 | B→A:REQ | A **defers** B ("A" < "B", A has priority) |
| 5 | C→A:REQ | A **defers** C ("A" < "C") |
| 6 | C→B:REQ | B defers C |
| 7 | C→B:Reply | B gets reply 1/2 |
| 8 | B→A:Reply | A gets reply 1/2 |
| 9 | C→A:Reply | A gets reply 2/2 — **gate closes** |

At gate close: `A.deferred = ["B", "C"]`, `A.state = Released`.

Local decision **(non-FIFO)**: DeferredFlusher runs before App.
- Flusher sees `Released` → sends premature REPLY to B and C
- App runs → sets `Held` (too late)
- B now has 2/2 replies → enters CS simultaneously with A
- **Mutual exclusion violated**

### Comparison: Naive `go test` vs Explorer

Attempted 100k iterations of the same protocol with standard Go scheduler (no orchestrator):
- **Result**: Protocol deadlocks from uncontrolled delivery ordering. Cannot even complete a single run reliably, let alone find the subtle gate bug.
- The Go scheduler's `cheaprand()` provides some scheduling variation, but not systematic coverage. Getting both the right G and L decisions simultaneously is astronomically unlikely.
- Even if hit, the failure is not reproducible (no trace, no replay).

The explorer finds it in **~1 second across ~227 runs**, with a reproducible trace.

---

## Current State

### What's in the repo

| Directory | Status | Purpose |
|-----------|--------|---------|
| `explorer/` | Stable | Core DFS library with context bounding |
| `orchestrator/` | Stable | Distributed orchestrator (delivery loop, bubbles) |
| `bugs/ra-gate/` | **Golden example** | RA gate bug — depth-2 G+L, template for future bugs |
| `experiments/` | Stable | Infrastructure validation (determinism, hooks, P-pinning) |
| `rafttest/` | WIP/Parked | Raft with injected + real bugs |

### Next Steps

- **New bug examples** in `bugs/`: stale clock (`ra-staleclock/`), missing held guard (`ra-noheld/`), correct RA baseline (`ra-correct/`) — all using the same architecture as `ra-gate/`
- **Fix the drain-path 500ms timeout**: replace with instant deadlock detection (all idle + no sends + no timers = deadlock)
- **Document the depth-2 exploration algorithm** for the paper: baseline FIFO → depth-1 branches → depth-2 branches, with the DFS tree structure
