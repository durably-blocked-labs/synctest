# Runtime Modifications for Synctest-Based Concurrency Bug Detection

This document describes every modification made to the Go 1.27 runtime (the `go/` submodule on the `synctest-explorer` branch) that enables deterministic scheduling, decision recording/replay, and hook-driven exploration inside synctest bubbles.

**Source files**: `go/src/runtime/synctest.go`, `go/src/runtime/runtime2.go`, `go/src/runtime/proc.go`, `go/src/runtime/chan.go`, `go/src/runtime/select.go`, `go/src/runtime/rand.go`, `go/src/testing/synctest/synctest.go`, `go/src/internal/synctest/synctest.go`

---

## 1. Bubble Model

### What a Bubble Is

A synctest bubble is an isolated execution environment with three properties:

1. **Fake clock.** Time inside the bubble is virtual. The initial value is midnight UTC 2000-01-01 (`946684800000000000` nanoseconds since epoch). Real wall-clock time never advances inside a bubble; the fake clock advances only when all goroutines are durably blocked and timers exist.

2. **Goroutine set.** Every goroutine created inside the bubble belongs to it. The set is tracked by two counters: `total` (live goroutines) and `running` (non-durably-blocked goroutines). When `running == 0 && active == 0`, the bubble is idle.

3. **Durable-blocking tracker.** The bubble distinguishes between transient blocking (syscall, stack growth) and durable blocking (channel wait, mutex, sleep). Only durable blocking decrements `running`. This is what enables idle detection, time advancement, and deadlock detection.

### One Bubble Per P, P Pinning

Each bubble owns exactly one P (processor) for its lifetime. The P is pinned in `synctestRunImpl` (`runtime/synctest.go:359-381`):

```go
pp := gp.m.p.ptr()
pp.bubble = bubble
bubble.pp = pp
```

The reverse pointer `bubble.pp` is used by `ready()` to route woken goroutines back to the correct P (Section 6). The P's `bubble` field (`runtime/runtime2.go:832`) tells `findRunnable` that this P is dedicated to a bubble and must follow bubble scheduling rules.

If the current P already belongs to another bubble (multiple bubbles spawned sequentially from the same goroutine), `synctestRunImpl` releases the old P via `handoffp` and acquires a fresh idle P (`runtime/synctest.go:362-379`).

### The `synctestBubble` Struct

Defined at `runtime/synctest.go:67-154`. Key field groups:

**Identity and state:**
- `id uint64` -- unique bubble ID, assigned from `bubbleGen` atomic counter
- `now int64` -- fake clock (nanoseconds since epoch)
- `root *g` -- the goroutine that called `synctest.Run`
- `main *g` -- the goroutine running the user's function `f`
- `waiter *g` -- goroutine blocked in `synctest.Wait`

**Activity tracking:**
- `total int` -- live goroutines in the bubble
- `running int` -- goroutines that are not durably blocked
- `active int` -- external activity sources that prevent idle detection (incremented by the event loop, timer checks, etc.)
- `external int` -- goroutines inside `External`/`CallExternal`
- `externalWait int` -- goroutines inside `ExternalWait` (orchestrator-controlled)

**Decision recording (our addition):**
- `decisions *[512]bubbleDecision` -- trace buffer, allocated via `persistentalloc` (non-GC)
- `decisionLen int32` -- decisions recorded so far (also the step counter)
- `decisionFence int32` -- boundary of pre-loaded decisions for replay

**Hook signaling (our addition):**
- `signal uint32` -- signal type from `findRunnable` to root (0=none, 1=needDecision, 2=replayDiverged, 3=idleHook)
- `decisionReady bool` -- set by root after the hook returns, read by `findRunnable`
- `decidedIndex int32` -- the hook's chosen goroutine index
- `pendingRunq [16]uint32` -- runq snapshot (BGIDs) for the hook
- `pendingSize int32` -- size of the snapshot
- `rootInHook bool` -- true while root is executing the `onDecision` hook
- `onDecision func(bubbleState) int32` -- the decision hook itself

**Goroutine ID tracking (our addition):**
- `nextGid uint32` -- counter for assigning deterministic bubble-local goroutine IDs (BGIDs)

### The `running` Counter

The `running` counter is the central mechanism for idle detection. It increments when a goroutine transitions from a non-running state to a running state (e.g., `_Gwaiting` to `_Grunnable`), and decrements when a goroutine durably blocks (e.g., `_Grunning` to `_Gwaiting` with a durable wait reason).

The logic lives in `changegstatus` (`runtime/synctest.go:158-222`). At every goroutine status change, the method checks whether the old and new states are "running" from the bubble's perspective:

```go
// runtime/synctest.go:170-189
switch oldval {
case _Gwaiting:
    if gp.waitreason.isIdleInSynctest() {
        wasRunning = false
    }
}
switch newval {
case _Gwaiting:
    if gp.waitreason.isIdleInSynctest() {
        isRunning = false
    }
}
```

When `running` drops to zero (and `active == 0`), `maybeWakeLocked` (`runtime/synctest.go:247-293`) decides what to do: wake the root goroutine for timer processing, wake a `Wait` caller, or (when a hook is set) wake root for hook-controlled idle handling.

---

## 2. Decision Points

### Where Decisions Happen

A decision point occurs when `findRunnable` (`runtime/proc.go:3535`) on the bubble's P must choose the next goroutine to run. This happens when the currently running goroutine blocks, exits, or yields. With `GODEBUG=asyncpreemptoff=1` and a single P per bubble, there is no mid-execution preemption -- the goroutine runs uninterrupted from one yield point to the next.

### `cheaprand()` and Scheduling Randomness

In the standard Go runtime, `cheaprand()` (`runtime/rand.go:228`) is a per-M PRNG (wyrand algorithm) that drives random scheduling choices in `runqput` and `stealWork`. This randomness makes scheduling non-deterministic across runs, even when the program logic is identical.

Our approach does not modify `cheaprand()` itself. Instead, we intercept scheduling decisions at a higher level: inside `findRunnable`, after the runnable set is known but before a goroutine is picked. The decision is either replayed from a pre-loaded trace, delegated to a hook, or made by the default FIFO path. Either way, the decision is recorded.

### The `bubbleDecision` Struct

Defined at `runtime/synctest.go:24-39`:

```go
type bubbleDecision struct {
    index         int32              // which goroutine was picked (0 = FIFO)
    chosenBgid    uint32             // bubble-local goroutine ID of the chosen goroutine
    chosenSpawnPC uintptr            // PC of the "go" statement that created it
    runqSize      int32              // total runnable goroutines at this decision point
    runqBgids     [16]uint32         // BGIDs of all runnable goroutines
    step          int32              // sequence number within the bubble
    waitReason    uint8              // why the previous goroutine yielded
}
```

This struct captures both the decision (index) and the context (runnable set, identities). The `runqBgids` array enables an external explorer to verify that replayed decisions land in the same state, and to enumerate alternatives for branching.

### Record Mode vs. Follow Mode

The `decisionFence` field partitions the decisions array into two regions:

- **Follow mode** (`decisionLen < decisionFence`): Pre-loaded decisions exist at `decisions[0..decisionFence-1]`. The scheduler reads `decisions[decisionLen].index` and uses `runqpick` to select that goroutine (`runtime/proc.go:3632-3641`). If the goroutine at that index is not runnable (the trace has diverged), `bubbleReplayDiverged` fires a panic via the `bubbleSignalReplayDiverged` signal.

- **Frontier mode** (`decisionLen >= decisionFence`): No more pre-loaded decisions. If a hook (`onDecision`) is registered, `findRunnable` signals the root goroutine to call the hook and waits for the response. If no hook is set, the default FIFO path runs (`runqget`, index 0).

- **Record mode** (always active): Regardless of follow/frontier, every decision is recorded at `decisions[decisionLen]` via `bubblePreSnapshot` and `bubbleFinishRecord` (`runtime/proc.go:758-797`).

`bubblePreSnapshot` captures the runnable set (BGIDs of all goroutines in the runq) before a goroutine is removed. `bubbleFinishRecord` writes the chosen goroutine's identity and increments `decisionLen`.

### The `runqpick` Function

Standard `runqget` always returns position 0 (runnext, then head of circular buffer). We added `runqpick` (`runtime/proc.go:825-865`) which removes a goroutine at an arbitrary index:

```go
func runqpick(pp *p, n int32) (*g, bool) {
    // Position 0 = runnext if available
    if next := pp.runnext; next != 0 {
        if n == 0 {
            if !pp.runnext.cas(next, 0) { return nil, false }
            return next.ptr(), true
        }
        n-- // runnext occupies position 0; adjust
    }
    // Now n indexes into the circular buffer
    // ... remove from middle by shifting tail entries left
}
```

This is the mechanism that makes non-FIFO scheduling possible. When the hook returns index 2, `runqpick` removes the third goroutine from the runq, shifting the rest left.

---

## 3. Durable Blocking

### The `isIdleInSynctest` Table

Defined at `runtime/runtime2.go:1398-1415`. This boolean table maps `waitReason` values to whether a goroutine with that wait reason is considered "durably blocked" by the bubble:

```go
var isIdleInSynctest = [len(waitReasonStrings)]bool{
    waitReasonChanReceiveNilChan:    true,
    waitReasonChanSendNilChan:       true,
    waitReasonSelectNoCases:         true,
    waitReasonSleep:                 true,
    waitReasonSyncCondWait:          true,
    waitReasonSynctestWaitGroupWait: true,
    waitReasonCoroutine:             true,
    waitReasonSynctestRun:           true,
    waitReasonSynctestWait:          true,
    waitReasonSynctestChanReceive:   true,
    waitReasonSynctestChanSend:      true,
    waitReasonSynctestSelect:        true,
    waitReasonSyncMutexLock:         true,   // OUR ADDITION
    waitReasonSyncRWMutexRLock:      true,   // OUR ADDITION
    waitReasonSyncRWMutexLock:       true,   // OUR ADDITION
}
```

### What We Added and Why

The upstream `testing/synctest` package does **not** consider mutex waits as durable blocking. The official documentation (`testing/synctest` package doc, lines 86-94) explicitly lists mutex locking as non-durable:

> In particular, the following operations may block a goroutine, but are not durably blocking because the goroutine can be unblocked by an event occurring outside its bubble:
>
>   - locking a sync.Mutex or sync.RWMutex

The rationale is sound for general-purpose testing: a mutex could in principle be held by a goroutine outside the bubble, so the bubble cannot assume the lock will be released.

However, for concurrency bug detection inside a self-contained bubble, this is a severe limitation. When all goroutines in a bubble are blocked on mutexes (or a combination of mutexes and channels), the bubble's `running` counter never reaches zero. The bubble thinks goroutines are "still running." Consequences:

1. **No idle detection.** `maybeWakeLocked` never fires.
2. **No time advancement.** Timers never fire.
3. **No deadlock detection.** The bubble hangs forever instead of panicking.
4. **No hook invocation.** The decision hook is never called at idle points.

We added `waitReasonSyncMutexLock`, `waitReasonSyncRWMutexRLock`, and `waitReasonSyncRWMutexLock` to the `isIdleInSynctest` table. This is safe within our usage model because:

- All goroutines contending on the mutex are inside the same bubble.
- The bubble is self-contained (no external code holds the lock).
- If a goroutine outside the bubble held the lock, it would not be in the bubble's goroutine set, which is a precondition violation.

**Impact**: In our benchmark suite of 13 channel-and-lock concurrency bugs (the `bugs/ra-gate` test suite), this change moved detection from 0/13 to 10/13. Without it, every bug involving `sync.Mutex` or `sync.RWMutex` caused the test to hang rather than triggering the explorer's assertion checks.

---

## 4. Hook Signaling (g0 -> root -> g0 Round-Trip)

### The Three Layers

The scheduling decision protocol involves three execution contexts:

1. **Orchestrator** (external to the bubble). Creates the bubble, sets the decision hook via `SetDecisionHook`, and communicates with the hook via non-bubble channels.

2. **Root goroutine** (BGID 0, inside the bubble). Created by the caller of `synctest.Run`. Parks in the `synctestRunImpl` event loop (`runtime/synctest.go:428-592`). Handles timer processing, hook invocation, and idle detection.

3. **`findRunnable`** (runs on g0, the scheduler goroutine on the bubble's P). This is where scheduling decisions are made. It reads the runq, determines which goroutine to run, and records the decision.

### The Decision Protocol

When `findRunnable` reaches the frontier (`decisionLen >= decisionFence`) with a non-empty runq and a registered hook (`runtime/proc.go:3643-3682`):

**Step 1: g0 snapshots the runq and signals root.**

```go
// runtime/proc.go:3652-3670
// Snapshot runq for root to inspect.
pos := int32(0)
if next := pp.runnext; next != 0 {
    gp := next.ptr()
    b.pendingRunq[pos] = gp.bubbleGid
    b.pendingGlob[pos] = gp.bubbleGlobal
    pos++
}
// ... iterate circular buffer ...
b.pendingSize = pos
b.signal = bubbleSignalNeedDecision
```

**Step 2: g0 wakes root via `casgstatus`.**

```go
// runtime/proc.go:3678
casgstatus(b.root, _Gwaiting, _Grunnable)
return b.root, false, false
```

This makes root runnable and returns it from `findRunnable`. The scheduler's `execute` function runs root, which unparks from `gopark` and re-enters the event loop.

**Step 3: Root sees the signal and calls the hook.**

```go
// runtime/synctest.go:454-476
if bubble.signal == bubbleSignalNeedDecision {
    bubble.signal = bubbleSignalNone
    idx := int32(0) // default FIFO
    if bubble.onDecision != nil {
        // Build BubbleState from pendingRunq, counters, timers...
        bubble.rootInHook = true
        idx = bubble.onDecision(state)
        bubble.rootInHook = false
    }
    bubble.decidedIndex = idx
    bubble.decisionReady = true
    // ... continue event loop (unlock -> timer check -> gopark)
}
```

The hook receives a `BubbleState` struct with the runnable set (BGIDs, global flags), blocked count, fake time, timer info, and the BGID of the last scheduled goroutine. The hook returns an index into the runnable set.

**Step 4: Root parks. g0 picks up the decision.**

Root's `gopark` call in the event loop returns it to `_Gwaiting`. On the next iteration of `findRunnable`, the `decisionReady` check fires (`runtime/proc.go:3620-3629`):

```go
if b := pp.bubble; b != nil && b.decisionReady {
    b.decisionReady = false
    bubblePreSnapshot(pp, b)
    gp, inheritTime := runqpick(pp, b.decidedIndex)
    if gp != nil {
        bubbleFinishRecord(b, gp, b.decidedIndex)
        return gp, inheritTime, false
    }
    return bubbleReplayDiverged(b, b.decidedIndex, 1)
}
```

`findRunnable` picks the goroutine at the hook's chosen index, records the decision, and returns it.

### The `rootInHook` Flag

While `rootInHook` is true (`runtime/synctest.go:128-131`), `findRunnable` must not try to wake root again (it is already awake, executing the hook). The hook may block -- for example, waiting for an orchestrator response on a non-bubble channel.

During `rootInHook`, `findRunnable` enters a tight spin loop (`runtime/proc.go:3711-3720`) that only picks root from `runnext`:

```go
if b := pp.bubble; b != nil && b.rootInHook {
    for {
        if next := pp.runnext; next != 0 && next.ptr() == b.root {
            if pp.runnext.cas(next, 0) {
                return b.root, true, false
            }
        }
        osyield()
    }
}
```

This spin is brief -- the orchestrator responds within microseconds via a channel send, which triggers `ready()` to place root in `runnext` (see Section 6). The spin uses `osyield()` instead of `goto top` to avoid `gcstopm` releasing the bubble P during GC.

### Idle Hook Signaling

When all bubble goroutines are durably blocked, the runq is empty, and `externalWait > 0` (goroutines are waiting on orchestrator-controlled channels), `findRunnable` signals root via `bubbleSignalIdleHook` (`runtime/proc.go:3684-3700`):

```go
if b := pp.bubble; b != nil && b.externalWait > 0 && b.onDecision != nil &&
    !b.delegateIdle && runqempty(pp) && !b.rootInHook {
    if readgstatus(b.root)&^_Gscan == _Gwaiting {
        b.signal = bubbleSignalIdleHook
        // ... increment active, casgstatus root to _Grunnable ...
    }
}
```

Root handles this signal by calling the hook with `Idle: true` (`runtime/synctest.go:487-509`). This tells the orchestrator that the bubble is idle and needs external input (a message delivery, a time advancement). If the hook returns a negative value, `delegateIdle` is set, which delegates back to the default synctest idle/time logic for that iteration.

---

## 5. Channel and Select Modifications

### Channel Durable Wait Reasons

When a goroutine blocks on a channel send or receive inside a bubble, the runtime uses bubble-specific wait reasons instead of the standard ones. This is what makes channel waits "durable" -- they appear in the `isIdleInSynctest` table.

**Channel send** (`runtime/chan.go:278-281`):

```go
reason := waitReasonChanSend
if c.bubble != nil {
    reason = waitReasonSynctestChanSend
}
```

**Channel receive** (`runtime/chan.go:662-665`):

```go
reason := waitReasonChanReceive
if c.bubble != nil {
    reason = waitReasonSynctestChanReceive
}
```

The distinction between `waitReasonChanSend` (non-durable) and `waitReasonSynctestChanSend` (durable) exists because a standard channel could be shared with goroutines outside the bubble. A channel created inside the bubble (`c.bubble != nil`) is guaranteed to be operated on only by bubble goroutines (the runtime enforces this with `fatal("send on synctest channel from outside bubble")`), so the wait is durable.

### Channel-Bubble Association

Channels inherit the bubble of the goroutine that creates them (`runtime/chan.go:116-118`):

```go
if b := getg().bubble; b != nil {
    c.bubble = b
}
```

The `hchan` struct (`runtime/chan.go:46`) has a `bubble *synctestBubble` field. Every channel operation (send, receive, close) checks this field against the current goroutine's bubble and panics on mismatch (`runtime/chan.go:193-195`, `319-321`, `418-419`, `540-541`).

### Select Durable Wait Reason

When all channels in a `select` statement belong to the bubble, the wait is durable (`runtime/select.go:199-203`):

```go
waitReason := waitReasonSelect
if gp.bubble != nil && allSynctest {
    waitReason = waitReasonSynctestSelect
}
```

The `allSynctest` flag is computed by iterating over all select cases and checking whether every channel has `cas.c.bubble != nil` and matches the goroutine's bubble (`runtime/select.go:179-182`).

---

## 6. Scheduler Integration

### Bubble-Aware `findRunnable`

`findRunnable` (`runtime/proc.go:3535`) is the Go scheduler's main loop for finding the next goroutine to run on a P. Our modifications add a series of bubble-specific paths that execute before the standard scheduling logic. In order of priority:

1. **Decision-ready path** (line 3620): Root has returned from the hook. Pick the chosen goroutine via `runqpick`.

2. **Follow path** (line 3632): Pre-loaded decisions exist. Follow `decisions[decisionLen].index` via `runqpick`.

3. **Frontier path** (line 3643): Past pre-loaded decisions, hook is registered. Snapshot the runq, signal root, return root as the next goroutine to run.

4. **ExternalWait idle path** (line 3684): All goroutines blocked, `externalWait > 0`. Signal root for idle hook.

5. **rootInHook spin** (line 3711): Hook is blocking. Spin until root lands in `runnext`.

6. **Default FIFO path** (line 3722): No special bubble state. Snapshot the runq (`bubblePreSnapshot`), call `runqget` (which returns position 0), record the decision (`bubbleFinishRecord`).

After the default FIFO path, several standard scheduler paths are **skipped for bubble Ps**:

- **Trace reader** (line 3564): `if pp.bubble == nil && (traceEnabled() || traceShuttingDown())`
- **GC workers** (line 3580): `if gcBlackenEnabled != 0 && pp.bubble == nil`
- **Global runq fairness check** (line 3592): `if pp.bubble == nil && pp.schedtick%61 == 0`
- **Finalizer wake** (line 3603): `if pp.bubble == nil`
- **Cleanup wake** (line 3611): `if pp.bubble == nil`
- **Global runq get** (line 3747): `if pp.bubble == nil && !sched.runq.empty()`

These are skipped because trace readers, GC workers, finalizers, and cleanup goroutines are not bubble goroutines. Scheduling them on the bubble's P would contaminate the bubble's runq, corrupt the decision trace, and break isolation.

### Bubble P Never Gets Released

Two guards prevent the bubble P from entering the idle pool:

**Before `pidleput`** (`runtime/proc.go:3852-3858`):
```go
if pp.bubble != nil {
    osyield()
    goto top
}
```

**Before `releasep`** (`runtime/proc.go:3897-3906`):
```go
if pp.bubble != nil {
    unlock(&sched.lock)
    osyield()
    goto top
}
```

If the bubble P were released, goroutines arriving via cross-P `goready` would be placed on the P's runq with no M to process them. The goroutine would be orphaned. Instead, the bubble P spins with `osyield()`, waiting for work to arrive.

### External Wait Backoff

When `external > 0` or `externalWait > 0` and the runq is empty, `findRunnable` yields briefly instead of entering the full work-stealing path (`runtime/proc.go:3740-3743`):

```go
if b := pp.bubble; b != nil && (b.external > 0 || b.externalWait > 0) && runqempty(pp) {
    osyield()
    goto top
}
```

This avoids wasting CPU on work-stealing while the bubble is waiting for an external event (cross-P `goready` will deposit the woken goroutine directly on the bubble P's runq).

### Cross-P `goready` Fix

The `ready()` function (`runtime/proc.go:1229`) is modified to redirect bubble goroutines to the correct P (`runtime/proc.go:1246-1282`):

```go
gpBubble := gp.bubble
if gpBubble == nil {
    gpBubble = gp.bubbleHome  // during External, bubble is temporarily nil
}
if gpBubble != nil && gpBubble.pp != nil && gpBubble.pp != pp {
    pp = gpBubble.pp
    if gp == gpBubble.root && gpBubble.rootInHook {
        next = true  // root goes to runnext for immediate pickup
    } else {
        next = false  // regular goroutines go to the circular buffer
    }
} else if gpBubble == nil && pp.bubble != nil {
    // Non-bubble goroutine woken from bubble P context.
    // Route to global runq to avoid stranding on bubble P.
    lock(&sched.lock)
    globrunqput(gp)
    unlock(&sched.lock)
    wakep()
    releasem(mp)
    return
}
```

Three cases are handled:

1. **Bubble goroutine woken on wrong P.** Redirect to `bubble.pp`. This happens when an orchestrator goroutine (running on an arbitrary P) sends on a channel to unblock a bubble goroutine.

2. **Root woken during `rootInHook`.** Force `next = true` so root lands in `runnext`, where the `rootInHook` spin loop can pick it up immediately.

3. **Non-bubble goroutine woken from bubble P.** Route to global runq. This happens when bubble root sends on a non-bubble channel to the orchestrator. Without this, the orchestrator goroutine would be stranded on the bubble P, unable to run during `rootInHook`.

### P Migration at Bubble Creation

Before any bubble goroutines run, `synctestRunImpl` migrates non-bubble goroutines off the bubble P's local runq (`runtime/synctest.go:393-416`):

```go
systemstack(func() {
    // Migrate runnext
    if next := pp.runnext; next != 0 {
        if pp.runnext.cas(next, 0) {
            lock(&sched.lock)
            globrunqput(next.ptr())
            unlock(&sched.lock)
        }
    }
    // Migrate circular buffer
    for {
        gp, _ := runqget(pp)
        if gp == nil { break }
        lock(&sched.lock)
        globrunqput(gp)
        unlock(&sched.lock)
    }
})
```

These goroutines were queued before the bubble was created. If left on the bubble P, they would be stranded by the `rootInHook` spin loop (which only picks root from `runnext`) or scheduled as if they were bubble goroutines (corrupting the decision trace).

---

## 7. Goroutine Identity

### Bubble-Local Goroutine IDs (BGIDs)

Each goroutine in a bubble is assigned a deterministic ID at creation time. The root goroutine is BGID 0. Subsequent goroutines receive sequential IDs from `bubble.nextGid`.

The assignment happens in `newproc1` (`runtime/proc.go:5666-5670`):

```go
newg.bubble = callergp.bubble
if newg.bubble != nil {
    newg.bubbleGid = atomic.Xadd(&newg.bubble.nextGid, 1)
    newg.bubbleSpawnPC = callerpc
    newg.bubbleGlobal = callergp.bubbleGlobal  // children inherit global flag
}
```

The `g` struct fields added for bubble tracking (`runtime/runtime2.go:575-583`):

- `bubble *synctestBubble` -- the bubble this goroutine belongs to
- `bubbleGid uint32` -- bubble-local goroutine ID
- `bubbleSpawnPC uintptr` -- PC of the `go` statement that spawned this goroutine
- `bubbleGlobal bool` -- true if marked as global (scheduling decisions forwarded to orchestrator)
- `bubbleHome *synctestBubble` -- saved bubble pointer during `External`/`ExternalWait` (when `bubble` is temporarily nil)

BGIDs are deterministic: given the same scheduling trace prefix up to a goroutine's creation point, the goroutine receives the same BGID. This is critical for trace replay -- the explorer identifies goroutines by BGID, and replay correctness depends on BGIDs being stable across runs with the same prefix.

### The Global Flag

`MarkGlobal()` (`runtime/synctest.go:760-766`) sets `gp.bubbleGlobal = true`. Children inherit the flag (`newproc1` line 5670). When a global goroutine appears in the runq, the decision hook receives `RunnableGlob[i] = true` in the `BubbleState`, allowing the orchestrator to distinguish local goroutines (whose scheduling is a local decision) from cross-node goroutines (whose scheduling corresponds to a network event).

---

## 8. External Operations

### Three Variants

The runtime supports three ways to interact with the outside world from inside a bubble:

**`External(fn)`** (`internal/synctest/synctest.go:116-122`):
```go
func External(fn func()) {
    incExternal()
    detachBubble()     // gp.bubble = nil; gp.bubbleHome = b; b.running--
    fn()
    reattachBubble()   // gp.bubble = gp.bubbleHome; b.running++
    decExternal()
}
```
The goroutine is detached from the bubble. Channels created inside `fn` are untagged (`c.bubble = nil`). The bubble parks silently -- `external > 0` suppresses idle detection in `maybeWakeLocked` (`runtime/synctest.go:250-256`).

**`CallExternal(fn)`** (`internal/synctest/synctest.go:131-136`):
```go
func CallExternal(fn func()) {
    MarkGlobal()
    incExternal()
    fn()
    decExternal()
}
```
The goroutine stays attached to the bubble (`gp.bubble` is not cleared). The decision hook sees this goroutine as global. Used for RPC calls where the orchestrator needs to intercept the scheduling decision.

**`ExternalWait(fn)`** (`internal/synctest/synctest.go:150-156`):
```go
func ExternalWait(fn func()) {
    incExternalWait()
    detachBubble()
    fn()
    reattachBubble()
    decExternalWait()
}
```
Like `External`, but increments `externalWait` instead of `external`. When all goroutines are blocked with `externalWait > 0`, the idle hook fires with `Idle: true` -- telling the orchestrator to deliver a message or advance time. This is the mechanism for orchestrator-controlled inter-node communication.

### The `maybeWakeLocked` Guard

`maybeWakeLocked` (`runtime/synctest.go:247-293`) has two guards that prevent wake bouncing:

```go
if bubble.external > 0 {
    return nil  // Don't wake root -- External goroutine will return eventually
}
if bubble.externalWait > 0 {
    return nil  // Don't wake root -- orchestrator owns the next step
}
```

Without these guards, root would wake, find nothing to do, park in `synctestidle_c`, wake again -- an infinite CPU-wasting bounce loop. The `findRunnable` idle hook path (`bubbleSignalIdleHook`) handles the `externalWait` case directly.

---

## 9. Public API

### `testing/synctest/synctest.go`

The public API layer (`go/src/testing/synctest/synctest.go`) wraps the internal runtime mechanisms:

**`Test(t, f, prefix ...[]Decision) []Decision`** (line 299): Runs `f` in a new bubble. If a prefix is provided, the scheduler follows those decisions before making its own. Returns the full scheduling trace. Calls `t.FailNow()` if the test fails.

**`Explore(t, f, prefix) (trace []Decision, ok bool)`** (line 324): Like `Test`, but does not call `t.FailNow()` on failure. Returns `(nil, false)` on panic (e.g., deadlock). Used by the explorer to test many interleavings without stopping at the first failure.

**`SetDecisionHook(hook func(BubbleState) int32)`** (line 347): Registers a callback called at each frontier decision point (past the prefix). The hook receives:

- `Step int32` -- current decision step
- `RunnableN int32` -- number of runnable goroutines
- `RunnableBgid [16]uint32` -- BGIDs of runnable goroutines
- `RunnableGlob [16]bool` -- global flags for each runnable goroutine
- `Blocked int32` -- number of durably blocked goroutines
- `Idle bool` -- true when the bubble is idle (no runnable goroutines)
- `Now int64` -- current fake time
- `NextTimer int64` -- next timer deadline (0 if none)
- `LastBgid uint32` -- BGID of the goroutine that just yielded
- `External int32` -- goroutines in `External`/`CallExternal`
- `ExternalWait int32` -- goroutines in `ExternalWait`

The hook returns an index (0 = FIFO). When `Idle` is true, returning a negative value delegates to the default synctest idle/time logic.

**`Wait()`** (line 426): Blocks until all other goroutines in the bubble are durably blocked. Unchanged from upstream.

**`External(fn)`, `CallExternal(fn)`, `ExternalWait(fn)`**: See Section 8.

**`SetTime(t int64)`** (line 416): Sets the bubble's fake clock. Intended to be called from inside the decision hook by the orchestrator to synchronize time across multiple bubbles.

**`MarkGlobal()`** (line 358): Marks the current goroutine as global. Children inherit the flag.

### `internal/synctest/synctest.go`

The internal package (`go/src/internal/synctest/synctest.go`) provides the `go:linkname` bridge between the public API and the runtime. Key types:

**`Decision`** (line 17): Layout must match `runtime.bubbleDecision` exactly. The explorer works with this type.

**`BubbleState`** (line 29): Layout must match `runtime.bubbleState` exactly. The hook receives this type.

Both structs are passed across the `go:linkname` boundary by value, so field types and order must match exactly between the runtime and internal packages. A mismatch causes silent corruption.

---

## 10. Summary of Modified Files

| File | What Changed |
|---|---|
| `runtime/synctest.go` | `synctestBubble` struct extended with decision recording, decision fence, hook signaling, external counters, goroutine ID tracking. Added `bubbleDecision`, `bubbleState`, `bubblePreSnapshot`, `bubbleFinishRecord`, `runqpick`. Added `synctestRunExplore`, `synctestSetDecisionHook`, `synctestMarkGlobal`, `synctestIncExternal`/`synctestDecExternal`, `synctestDetachBubble`/`synctestReattachBubble`, `synctestIncExternalWait`/`synctestDecExternalWait`, `synctestSetTime`. Modified `synctestRunImpl` to handle prefix replay, hook signals, external waits, and P migration. |
| `runtime/runtime2.go` | Added `bubbleGid`, `bubbleSpawnPC`, `bubbleGlobal`, `bubbleHome` fields to `g` struct. Added `bubble` field to `p` struct. Added `waitReasonSyncMutexLock`, `waitReasonSyncRWMutexRLock`, `waitReasonSyncRWMutexLock` to `isIdleInSynctest` table. |
| `runtime/proc.go` | Added `bubblePreSnapshot`, `bubbleFinishRecord`, `bubbleReplayDiverged`, `runqpick` functions. Modified `findRunnable` with 6 bubble-specific paths (decision-ready, follow, frontier, ExternalWait idle, rootInHook spin, default FIFO with recording). Added bubble P guards (never release, never idle). Modified `ready()` for cross-P goroutine routing. Modified `newproc1` for BGID assignment and global flag inheritance. Skipped non-bubble work (GC, trace, finalizer, global runq) on bubble Ps. |
| `runtime/chan.go` | Channel operations use `waitReasonSynctestChanSend`/`waitReasonSynctestChanReceive` when the channel belongs to a bubble. |
| `runtime/select.go` | Select uses `waitReasonSynctestSelect` when all cases are bubble channels. |
| `runtime/rand.go` | Unchanged. `cheaprand()` still runs, but its effects are superseded by decision recording/replay at the `findRunnable` level. |
| `testing/synctest/synctest.go` | Added `Test` (with prefix support), `Explore`, `SetDecisionHook`, `MarkGlobal`, `External`, `CallExternal`, `ExternalWait`, `SetTime`. |
| `internal/synctest/synctest.go` | Added `Decision`, `BubbleState` types. Added `RunExplore`, `SetDecisionHook`, `MarkGlobal`, `External`, `CallExternal`, `ExternalWait`, `SetTime`, `incExternal`/`decExternal`, `detachBubble`/`reattachBubble`, `incExternalWait`/`decExternalWait` linkname stubs. |
