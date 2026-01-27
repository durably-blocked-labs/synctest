# Plan: Week of Jan 27

## Goal

Figure out what's needed to make scheduling deterministic inside synctest bubbles.

---

## Two Paths to Evaluate

### Path 1: GOMAXPROCS > 1

If we allow multiple processors, we have to deal with:

| Randomization Point | Always Random? | Problem |
|---------------------|----------------|---------|
| `stealWork()` | YES | Steal order uses `cheaprand()` unconditionally |
| `selectgo()` | YES | Case order always randomized |
| `runqput()` | Only with `-race` | runnext decision |
| `runqputslow()` | Only with `-race` | Shuffle to global queue |
| `runqputbatch()` | Only with `-race` | Shuffle from global queue |

**Plus**: True parallelism means OS thread scheduling comes into play. Even if we control `cheaprand()`, multiple Ms running on multiple cores can interleave unpredictably.

**Verdict**: Hard mode. Would need to:
- Control `cheaprand()` for stealWork
- Control `cheaprand()` for selectgo
- Somehow serialize execution across Ps (defeats the purpose?)

---

### Path 2: GOMAXPROCS = 1

With only one P:

| Randomization Point | Relevant? | Notes |
|---------------------|-----------|-------|
| `stealWork()` | NO | Nothing to steal from (only 1 P) |
| `selectgo()` | **YES** | Still randomizes case order |
| `runqput()` | Only with `-race` | Skip if no `-race` |
| `runqputslow()` | Only with `-race` | Skip if no `-race` |
| `runqputbatch()` | Only with `-race` | Skip if no `-race` |

**With GOMAXPROCS=1 and no `-race`**:
- Only `selectgo()` is non-deterministic
- Everything else is deterministic

**To achieve full determinism**:
1. Set `GOMAXPROCS=1`
2. Don't use `-race`
3. Control `cheaprand()` in `selectgo()` (or seed it deterministically)

---

## Plan

### Step 1: Validate GOMAXPROCS=1 determinism

Write a test that:
- Creates a synctest bubble
- Spawns N goroutines that do predictable work (no select)
- Logs execution order
- Run 100 times, verify same order every time

Expected: Should be deterministic (no select, no race flag).

### Step 2: Show select non-determinism

Write a test that:
- Has a `select` with multiple ready channels
- Logs which case wins
- Run 100 times

Expected: Different cases win different times (random).

### Step 3: Seed cheaprand for selectgo

Modify `go/src/runtime/select.go`:
- Before the `cheaprandn()` calls in `selectgo()`
- If in a bubble, use a deterministic seed based on bubble ID + some counter

```go
// Hypothetical change in selectgo()
if gp := getg(); gp != nil && gp.bubble != nil {
    // Use deterministic random for this bubble
    seed := gp.bubble.id ^ uint64(gp.goid)
    // ... use seed instead of cheaprandn()
}
```

### Step 4: Verify determinism with seeded select

Re-run Step 2 test with modified runtime.

Expected: Same case wins every time (deterministic).

### Step 5: Explore schedules

Once deterministic, exploration is about **manipulating the run queue**, not changing seeds.

The seed just makes select deterministic. The actual interleaving is determined by:
- Order of goroutines in `runq`
- Which goroutine is in `runnext`
- When goroutines yield/block

**Exploration strategies from papers:**

| Paper | Approach |
|-------|----------|
| **CHESS** | Preemption bounding - limit context switches, enumerate all schedules within bound |
| **PCT** | Assign random priorities to threads, change priority at random points |
| **DPOR** | Track dependencies, prune equivalent interleavings |

Need to review papers and see how they manipulate scheduling:
- Do they reorder queues directly?
- Do they insert preemption points?
- How do they enumerate schedules?

**Key insight**: The run queue IS the schedule. To explore interleavings:
1. Make everything deterministic (seed select)
2. Control the queue order
3. Replay with different queue orders

---

## Questions to Answer

1. Does `cheaprand()` state persist across goroutine switches? (It's per-M, not per-G)
2. With GOMAXPROCS=1, is there only one M? Or can there be multiple Ms but only one with a P?
3. How do we inject a deterministic seed into `selectgo()` cleanly?

---

## Files to Modify

| File | Change |
|------|--------|
| `go/src/runtime/select.go` | Seed `cheaprandn()` deterministically in bubbles |
| `go/src/runtime/proc.go` | Maybe seed `cheaprand()` for new goroutines in bubbles |
| `go/src/runtime/synctest.go` | Add seed/counter to bubble struct? |

---

## Next Steps

1. Build the Go runtime from source
2. Write validation tests (Step 1 & 2)
3. Make the modification (Step 3)
4. Verify (Step 4)
5. Review papers for queue manipulation strategies:
   - How does CHESS enumerate schedules?
   - How does PCT assign/change priorities?
   - How does DPOR track dependencies?
6. Design queue manipulation API for bubbles
