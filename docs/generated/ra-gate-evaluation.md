# ra-gate Evaluation — Concurrency Bug Detection in Ricart-Agrawala Lock Variants

## Overview

ra-gate is a mutual exclusion protocol bug in a Ricart-Agrawala distributed lock implementation. The protocol uses a `close(gate)` pattern: when a node collects all REPLY messages, the handler closes a gate channel, simultaneously waking both an App goroutine (which enters the critical section by setting `state=Held`) and a DeferredFlusher goroutine (which processes deferred REQUESTs). Under FIFO local scheduling the App goroutine always runs first and sets `state=Held` before the flusher checks state. Under non-FIFO local scheduling the DeferredFlusher can run first, observe `state=Released`, and grant deferred REPLYs to nodes that should still be waiting --- violating mutual exclusion.

The bug is found by controlling scheduling in a 3-node cluster where each raft node runs in its own synctest bubble. An orchestrator controls both message delivery order (global decisions) and goroutine scheduling within each node (local decisions). Neither dimension alone triggers the bug:

- **Global only**: Without reordering message delivery, no REQUEST arrives while a node is collecting REPLYs, so the deferred queue is empty and the flusher is a no-op.
- **Local only**: Without a deferred REQUEST in the queue, the flusher has nothing to send regardless of scheduling order.

Detection requires a specific RequestVote delivery order AND a specific goroutine ordering after `close(gate)`.

---

## The 5 Raft Bugs

All bugs are implemented as standalone packages under `bugs/ra-*`. Each has a `lock.go` (protocol implementation), `transport.go` (orchestrator transport), `kv.go` (networked KV store for black-box detection), `bench_test.go` (benchmarks), and `bug_demo_test.go` (demonstration tests).

| Bug | Description | Nodes | Mechanism | CHESS bound | First bug run (CHESS G+L) |
|---|---|---|---|---|---|
| ra-gate | Premature lock release: `close(gate)` lets DeferredFlusher race with App goroutine | 3 | G+L | k=2 | 47 |
| ra-stale-reply | Stale AppendEntries reply from old term accepted toward new quorum | 3 | G | k=4 | -- |
| ra-duplicate-request | Duplicate request processing: same deferred requester counted twice | 3 | G+L | k=3 | -- |
| ra-premature-defer | Deferred flush fires as soon as quorum REPLY arrives, before CS exit | 3 | G | k=4 | -- |
| ra-deferred-storm | 4-node variant of gate bug with cascading deferred operations across two rounds | 4 | G+L | k=4 | -- |

**Mechanism key**: G = bug requires only message reordering (global non-FIFO decisions). G+L = bug requires both message reordering AND goroutine scheduling reordering.

---

## Global vs Local Decisions

The key architectural insight: distributed concurrency bugs span two orthogonal dimensions of non-determinism.

**Global decisions** control message delivery order between nodes. When multiple messages are pending in the orchestrator's queue, the choice of which message to deliver next is a global decision. Non-FIFO delivery = reordering messages relative to the default queue order.

**Local decisions** control goroutine scheduling within a single node (bubble). When multiple goroutines are runnable after a channel operation (e.g., `close(gate)` waking two receivers), the choice of which goroutine runs first is a local decision. Non-FIFO scheduling = running a goroutine other than the default cheaprand() pick.

| Bug | Global non-FIFO needed | Local non-FIFO needed | Total non-FIFO | Explanation |
|---|---|---|---|---|
| ra-gate | 1 | 1 | 2 | One reorder to populate deferred queue, one to schedule flusher first |
| ra-stale-reply | 4 | 0 | 4 | Multiple reorders to delay a stale reply across rounds |
| ra-duplicate-request | 2 | 1 | 3 | Reorders to duplicate-deliver a request, plus local scheduling |
| ra-premature-defer | 4 | 0 | 4 | Reorders to populate deferred queue before quorum fires |
| ra-deferred-storm | 2 | 2 | 4 | Two rounds, each needing one global + one local non-FIFO |

The ra-stale-reply and ra-premature-defer bugs are pure global bugs --- no goroutine scheduling reordering is needed. The ra-gate, ra-duplicate-request, and ra-deferred-storm bugs require both dimensions.

---

## Strategy Comparison on ra-gate

Results from `bugs/ra-gate/bench_test.go` with `benchMaxRuns=500`. Data collected in `bugs/ra-gate/charts/data/`.

| Strategy | Parameters | First bug (run #) | Total runs | Notes |
|---|---|---|---|---|
| CHESS Global-only | k=2, G-only | 142 | 142 | Only branches on message delivery order; local scheduling is FIFO |
| CHESS G+L | k=2 | 47 | 47 | Branches on both message delivery AND goroutine scheduling |
| PCT d=2 | depth=2, seed=1 | 2 | 2 | Prioritized randomization; seed-dependent |
| PCT d=3 | depth=3, seed=1 | 2 | 2 | Similar to d=2 for this bug depth |
| Random | seed=1 | 1 | 1 | Uniform random at every decision point |

Key observations:

1. **G+L is 3x faster than G-only.** CHESS G+L finds the bug in 47 runs vs 142 for G-only. G-only must exhaustively enumerate global orderings until it stumbles into one that, combined with the default FIFO local schedule, triggers the bug. G+L targets the right dimension directly.

2. **PCT and Random find it faster than CHESS.** For ra-gate, the state space is small enough that randomized strategies hit the bug quickly. PCT at depth 2 matches the bug's actual depth (2 non-FIFO decisions). Random succeeds on the first run because the bug probability under uniform random scheduling is high in a 3-node cluster.

3. **CHESS provides guarantees, Random does not.** CHESS systematically explores up to bound k, guaranteeing it finds any bug of that depth. Random may miss bugs with lower probability. The tradeoff matters for deeper bugs like ra-deferred-storm (k=4).

4. **Seed matters for PCT/Random.** The results above are for seed=1. Different seeds will produce different first-bug run numbers. CHESS is deterministic regardless of seed.

---

## How Detection Works

Detection is black-box: the test function asserts a safety invariant and calls `t.Errorf` on violation. No internal state inspection of the lock protocol is needed.

The pipeline:

1. **Setup.** The test creates a 3-node cluster. Each node runs in its own synctest bubble via `orchestrator.AddNode()`. Nodes are connected through `OrchestratorTransport`, an in-memory transport where messages flow through the orchestrator's message queue. A separate KV node provides a shared counter (also in its own bubble).

2. **Scenario.** All three nodes concurrently acquire the lock, read-modify-write a shared counter in the KV store, and release the lock. Under correct mutual exclusion, the final counter equals 3.

3. **Exploration.** The orchestrator runs the scenario repeatedly via `ExploreWith()`. Each run uses a scheduling algorithm (CHESS, PCT, or Random) to choose at every decision point --- both global (which message to deliver) and local (which goroutine to run). The algorithm varies choices across runs to cover the interleaving space.

4. **Assertion.** The last node to finish checks `counter == 3`. If the lock was violated and two nodes entered the critical section simultaneously, their read-modify-write operations overlap: both read the same counter value, both increment, and one write is lost. The counter ends at 2 instead of 3.

5. **Verdict.** A `t.Errorf("lost update: counter=%d, want %d", final, expected)` fires. This is a user-asserted failure, not a deadlock or crash. The orchestrator records `passed=false` for that run.

The `OnViolation` callback on `GateRANode` provides additional white-box diagnostics (logging which node's flusher saw `state=Released`), but detection relies solely on the black-box counter check.

---

## Benchmark Data

All benchmark data lives under `bugs/ra-gate/charts/data/`. Files are JSONL (one JSON object per line) or JSON.

| File | Format | Contents |
|---|---|---|
| `chess-global.jsonl` | JSONL | Per-run summary for CHESS Global-only (run number, passed, elapsed) |
| `chess-global-trace.jsonl` | JSONL | Per-run detailed trace for CHESS Global-only (142 runs, all decision steps) |
| `chess-global-tree.json` | JSON | CHESS search tree structure for Global-only exploration |
| `chess-gl.jsonl` | JSONL | Per-run summary for CHESS G+L |
| `chess-gl-trace.jsonl` | JSONL | Per-run detailed trace for CHESS G+L (47 runs, all decision steps) |
| `chess-gl-tree.json` | JSON | CHESS search tree structure for G+L exploration |
| `pct-d2.jsonl` | JSONL | Per-run summary for PCT depth=2 |
| `pct-d2-trace.jsonl` | JSONL | Per-run detailed trace for PCT depth=2 (2 runs) |
| `pct-d3.jsonl` | JSONL | Per-run summary for PCT depth=3 |
| `pct-d3-trace.jsonl` | JSONL | Per-run detailed trace for PCT depth=3 (2 runs) |
| `random.jsonl` | JSONL | Per-run summary for Random |
| `random-trace.jsonl` | JSONL | Per-run detailed trace for Random (1 run) |

### Trace format

Each trace line is a JSON object with fields:

```json
{
  "policy": "chess-gl",
  "run_num": 47,
  "passed": false,
  "elapsed_ns": 788125,
  "steps": [
    {
      "kind": "local",
      "node": "A",
      "index": 0,
      "alternatives": 3,
      "chosen_id": "B5",
      "chosen_bgid": 5,
      "runq_bgids": [5, 3, 4]
    },
    {
      "kind": "global",
      "index": 5,
      "alternatives": 6,
      "chosen_id": "msg:C->B(Request)",
      "from": "C",
      "to": "B",
      "msg_type": "Request"
    }
  ]
}
```

- `kind`: `"local"` (goroutine scheduling) or `"global"` (message delivery)
- `index`: which alternative was chosen (0 = FIFO default, >0 = non-FIFO)
- `alternatives`: total number of choices at this decision point
- `chosen_bgid` / `runq_bgids`: bubble goroutine IDs for local decisions
- `from` / `to` / `msg_type`: message metadata for global decisions

The summary `.jsonl` files contain the same data without the `steps` array.
