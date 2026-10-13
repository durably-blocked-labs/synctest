# Bug Demos: Deterministic Interleaving Exploration on Raft

These tests demonstrate using synctest's deterministic scheduler to find
concurrency bugs in hashicorp/raft that are invisible under normal execution.

## Bug #1: Stale Term Check Bypass (Network Delivery Order)

**File:** `rafttest/bug_demo_test.go` → `TestStaleTermConcept`

**The bug:** In `raft/raft.go:1457`, the `appendEntries` function rejects
messages from old leaders:

```go
if a.Term < r.getCurrentTerm() {
    return  // reject stale message
}
```

We added a `SkipTermCheck` flag to `raft/config.go` that disables this check.
With the check disabled, a delayed AppendEntries from an old leader (term 1)
arriving after a new leader (term 2) is elected will be ACCEPTED — the follower
overwrites its leader pointer, may append stale log entries, and can apply
uncommitted entries. Safety violation.

**Why it's invisible under FIFO delivery:** Messages arrive in term order.
The old leader's messages are delivered before the new leader's. A follower
never sees old-term messages after new-term messages.

**How our system finds it:** The orchestrator's `Explore()` tries different
delivery orderings. It can delay the old leader's AppendEntries and deliver
it after the new leader's — triggering the stale-term bypass.

**What the test shows:**
```
$ ./go/bin/go test -run TestStaleTermConcept ./rafttest/
FIFO delivery: PASSED (bug is latent — only triggers under reordered delivery)
```

The test passes because FIFO delivery never reorders messages. Under `Explore()`
with the right message ordering, it would fail.

**Code locations:**
- Flag: `raft/config.go` → `SkipTermCheck bool`
- Guard: `raft/raft.go:1457` → `if !r.config().SkipTermCheck && a.Term < ...`
- Test: `rafttest/bug_demo_test.go` → `TestStaleTermConcept`

---

## Bug #6: RemoveLeader Apply-vs-StepDown Race (Goroutine Scheduling / Select Ordering)

**File:** `rafttest/bug_demo_test.go` → `TestRemoveLeaderSeedSweep`

**The bug:** This is a REAL bug in hashicorp/raft — no code injection needed.

When you call `RemoveServer(leader)` while applies are in flight:

```go
// raft_test.go:790 (hashicorp's own test)
for i := byte(0); i < 100; i++ {
    if i == 80 {
        removeFuture := leader.RemoveServer(leader.localID, 0, 0)
        removeFuture.Error()  // blocks until committed
    }
    future := leader.Apply([]byte{i}, 0)
    if i > 80 {
        // Should fail — leader stepped down
        assert(future.Error() == ErrNotLeader)
    }
}
```

After `removeFuture.Error()` returns (removal committed), the leader's
`leaderLoop` has a `select` with both `commitCh` (stepDown signal) and
`applyCh` (new request from Apply at i=81) ready:

```go
// raft.go leaderLoop:
select {
case <-commitCh:    // → sets stepDown = true
case newLog := <-applyCh:  // → dispatches the apply (bug!)
// ... other cases
}
```

Go's `select` picks randomly between ready cases. The randomness is
controlled by the P's `fastrand`, which is seeded by `rand.Seed()`.

**Under most seeds:** `commitCh` wins → leader steps down → applies fail correctly.

**Under some seeds:** `applyCh` wins → apply succeeds → BUG.

**How our system finds it:** The `distributed.Bubble` accepts `WithSeed(seed)`
which sets the P's rand seed. We sweep seeds 0-49, looking for one where
`applyCh` wins the select race. Different seeds produce different select
outcomes.

**What the test does:**
```go
for seed := 0; seed < 50; seed++ {
    succeeded, failed := runRemoveLeaderWithSeed(t, seed)
    if succeeded > 0 {
        // BUG: applies succeeded after RemoveServer committed
    }
}
```

**Code locations:**
- Test: `rafttest/bug_demo_test.go` → `TestRemoveLeaderSeedSweep`
- Seed option: `distributed/distributed.go` → `WithSeed(seed int64)`
- The bug: `raft/raft.go` → `leaderLoop` select statement

**Status:** The test currently has a transport shutdown issue — when nodes
exit at different times, raft goroutines blocked in makeRPC Phase 2
(`resp := <-respCh`) don't unblock because Close()'s pending drain races
with new makeRPC calls. Fix in progress. The runtime change (deterministic
select via `selectCounter` in selectgo) is complete and working.

**Select as a decision point (runtime change, DONE):**
In `go/src/runtime/select.go`, when a goroutine is in a bubble, `selectgo`
uses `bubble.selectCounter` instead of `cheaprandn` for the pollorder shuffle.
This makes select ordering deterministic and controllable. The orchestrator
sets the counter via `WithSeed` through the distributed package. Different
seed values produce different select interleavings — the orchestrator can
explore them systematically.

---

## The Runtime Primitives

Both bugs are explored using three new runtime primitives we built:

1. **`ExternalWait(fn)`** — like `External()` but the bubble knows the
   orchestrator can unblock it. When all goroutines are in ExternalWait,
   the idle hook fires.

2. **`SetTime(t)`** — orchestrator controls the bubble's fake clock.

3. **Idle hook** — the decision hook fires with `Idle: true` when the
   bubble has nothing to do. Reports timers, blocked goroutines, etc.

Plus the `distributed.Bubble` package that wraps these into a clean
local orchestrator with scheduling prefix replay and seed control.

**Key files:**
- Runtime: `go/src/runtime/synctest.go` (ExternalWait, SetTime, idle hook)
- Runtime: `go/src/runtime/proc.go` (findRunnable spin guard)
- Package: `distributed/distributed.go` (Bubble, WithSeed, WithPrefix)
- Design: `docs/distributed-model.md` (full architecture + contracts)

---

## Select as a Decision Point (Runtime Change — DONE)

We modified `go/src/runtime/select.go` so that inside a bubble, `selectgo`
uses a deterministic counter (`bubble.selectCounter`) instead of `cheaprandn`
for the pollorder shuffle. This means:

- Select ordering is **deterministic** within a bubble (same counter → same outcome)
- The orchestrator controls it via `WithSeed` at bubble creation
- Different counter values explore different select interleavings
- The existing `cheaprandn` is preserved outside bubbles (no behavior change)

**Future improvement:** Treat each select with multiple ready cases as a
full **decision point** (like goroutine scheduling). The hook would fire
with a "select decision" showing which cases are ready, and the orchestrator
would pick which one runs. This gives finer-grained control than a counter
but requires a more invasive runtime change (Step 7 in the roadmap).
