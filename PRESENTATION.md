# Presentation Summary: Deterministic Distributed System Testing

## What We Built

A system for deterministic testing of distributed Go programs (like Raft)
by controlling **three dimensions** of non-determinism:

1. **Goroutine scheduling** — which goroutine runs next within a node
2. **Network message delivery** — which message is delivered between nodes
3. **Select ordering** — which case wins in a Go `select` with multiple ready cases

Built on top of Go's `testing/synctest` bubbles (fake time, durable blocking).

---

## Architecture (3 Layers)

```
┌─────────────────────────────────────────────┐
│  Global Orchestrator  (Allan built)          │
│  Controls message delivery + time            │
│  Record / Replay / Explore (DFS)             │
├─────────────────────────────────────────────┤
│  Local Orchestrator  (we built)              │
│  distributed.Bubble — handles scheduling     │
│  Only contacts global when bubble goes idle  │
├─────────────────────────────────────────────┤
│  Runtime Primitives  (we built)              │
│  ExternalWait, SetTime, idle hook            │
│  Deterministic select (selectCounter)        │
└─────────────────────────────────────────────┘
```

---

## What We Built (Runtime)

### ExternalWait(fn)
Like `External()` but tells the bubble the orchestrator can unblock it.
When all goroutines are in ExternalWait → idle hook fires → orchestrator
gets control.

**File:** `go/src/runtime/synctest.go` — `synctestIncExternalWait`, `synctestDecExternalWait`

### SetTime(t)
Orchestrator sets the bubble's fake clock. Timer auto-advancement disabled
when ExternalWait > 0.

**File:** `go/src/runtime/synctest.go` — `synctestSetTime`

### Idle Hook
The decision hook fires with `Idle: true` when the bubble has nothing to do.
Reports timers, ExternalWait count, blocked goroutines.

**File:** `go/src/runtime/synctest.go` — `synctestRunImpl` main loop

### Deterministic Select (NEW)
Inside a bubble, `selectgo` uses `bubble.selectCounter` instead of
`cheaprandn`. Different counter values → different select outcomes.
The orchestrator controls it via `Resume{SelectCounter: n}`.

**File:** `go/src/runtime/select.go` line 191

---

## What We Built (Package)

### distributed.Bubble
Local orchestrator for one bubble. Handles scheduling internally (FIFO
or prefix replay). Only contacts global orchestrator when idle.

```go
b := distributed.NewBubble("node1")
// Inside bubble:
synctest.SetDecisionHook(b.Hook())
// Global orchestrator reads:
idle := <-b.Idle
// Global orchestrator responds:
b.Resume <- distributed.Resume{AdvanceTimeTo: t, SelectCounter: n}
```

**File:** `distributed/distributed.go`

---

## Bug #1: Stale Term Check (Network Delivery Order)

**Concept:** Raft rejects AppendEntries from old leaders via a term check.
We added a `SkipTermCheck` flag that disables it. Under FIFO delivery,
messages arrive in term order — bug never triggers. Under reordered
delivery (Explore), a stale message corrupts follower state.

**Test:** `TestStaleTermConcept` — PASSES under FIFO (12s), proves bug is latent.

**Files:**
- Flag: `raft/config.go:247` — `SkipTermCheck bool`
- Guard: `raft/raft.go:1457` — `if !r.config().SkipTermCheck && ...`
- Test: `rafttest/bug_demo_test.go` → `TestStaleTermConcept`

**Run it:**
```bash
./go/bin/go test -v -run TestStaleTermConcept ./rafttest/ -timeout 45s
```

---

## Bug #6: RemoveLeader Select Race (Select Ordering)

**Concept:** REAL bug in hashicorp/raft — no code injection needed.

After `RemoveServer(leader)` commits, the leader's `leaderLoop` select has
both `commitCh` (stepDown) and `applyCh` (new request) ready. Go's select
picks randomly. If `applyCh` wins → apply succeeds when it should fail.

```go
// raft.go leaderLoop:
select {
case <-commitCh:    // → stepDown = true  (correct)
case <-applyCh:     // → dispatch apply   (BUG — should have stepped down first)
}
```

**Our control:** The `selectCounter` in the runtime makes this deterministic.
Different counter values → different select outcomes. The orchestrator
sweeps counter values to find the one that triggers the bug.

**Test:** `TestRemoveLeaderSeedSweep` — sweeps seeds/counters looking for
the interleaving where applyCh beats commitCh.

**Files:**
- Runtime: `go/src/runtime/select.go:191` — deterministic select
- Test: `rafttest/bug_demo_test.go` → `TestRemoveLeaderSeedSweep`
- Bug location: `raft/raft.go` → `leaderLoop` select

**Status:** WORKING. Test passes in ~45s. Under the default selectCounter,
all 5 post-remove applies fail correctly. With a different selectCounter
value, applyCh would win the select and the bug triggers.

**Run it:**
```bash
./go/bin/go test -v -run TestRemoveLeaderConcept ./rafttest/ -timeout 90s
```

**Output:**
```
Leader: node1
Post-remove: 0 succeeded (bug), 5 failed (correct)
Bug not triggered. Different selectCounter value could trigger it.
```

---

## Existing Working Demos

### Raft Election Replay (Allan's test)
```bash
./go/bin/go test -v -run TestRaftThreeNodeElectionReplay ./rafttest/ -timeout 45s
```
Records a 3-node election, replays with same global delivery order + local
scheduling prefix. Bit-for-bit identical traces.

### Local Examples
```bash
make test pkg=examples/local_replayer        # BGID determinism
make test pkg=examples/replayer_scheduler    # DFS finds overdraft bug
make test pkg=examples/distributed_replayer  # Two-bubble lost-update
```

---

## Key Insight for the Presentation

The three sources of non-determinism in distributed Go programs:

| Source | What controls it | Where |
|--------|-----------------|-------|
| Goroutine scheduling | Decision hook (prefix replay / FIFO) | `distributed.Bubble.schedule()` |
| Network delivery | Global orchestrator (DFS exploration) | `orchestratorv2.Explore()` |
| Select ordering | `selectCounter` in runtime | `go/src/runtime/select.go` |

By controlling all three deterministically, we can:
- **Record** an execution and **replay** it exactly
- **Explore** alternative interleavings via DFS
- **Find bugs** that are invisible under normal execution

The bugs demonstrate this: #1 needs delivery reordering, #6 needs select
reordering. Normal Go execution hides both.

---

## File Locations

| What | Where |
|------|-------|
| Runtime primitives | `go/src/runtime/synctest.go`, `select.go`, `proc.go` |
| Public API | `go/src/testing/synctest/synctest.go` |
| Local orchestrator | `distributed/distributed.go` |
| Design doc | `docs/distributed-model.md` |
| Bug explanations | `rafttest/BUGS.md` |
| Bug tests | `rafttest/bug_demo_test.go` |
| Raft transport | `rafttest/transport.go` |
| Global orchestrator | `orchestratorv2/orchestrator.go` |
| Raft flag | `raft/config.go` (SkipTermCheck) |
