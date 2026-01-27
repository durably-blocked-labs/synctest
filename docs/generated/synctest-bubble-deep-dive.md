# Synctest Bubbles: A Deep Dive from First Principles

This document explains how synctest bubbles work at the code level, tracing through the actual runtime implementation.

**Source**: `go/src/runtime/synctest.go`, `go/src/runtime/runtime2.go`, `go/src/runtime/chan.go`

## 1. What Problem Does Synctest Solve?

### The Problem

Testing concurrent code is difficult because:

1. **Time-dependent tests are slow**: `time.Sleep(2 * time.Second)` actually waits 2 seconds
2. **Race conditions are hard to reproduce**: Depend on precise timing
3. **Deadlock detection is manual**: No automatic detection
4. **Tests are non-deterministic**: Same test can pass or fail randomly

### Synctest's Solution

Synctest provides an **isolated execution context** called a "bubble" where:

- **Time is virtualized**: Fake clock that only advances when all goroutines are blocked
- **Blocking is detectable**: Runtime knows when goroutines are "durably blocked"
- **Deadlocks are caught**: Automatic panic when all goroutines stuck

**Example**: This completes instantly, not after 2 seconds:

```go
func TestSleep(t *testing.T) {
    synctest.Test(t, func(t *testing.T) {
        start := time.Now()
        time.Sleep(2 * time.Second)
        // Elapsed time is 2s, but test ran instantly!
        assert.Equal(t, 2*time.Second, time.Since(start))
    })
}
```

---

## 2. The Bubble Concept

### What is a Bubble?

A bubble is a **logically isolated execution environment**:

- All goroutines created within the bubble belong to it
- All channels created within the bubble are associated with it
- The bubble has its own fake clock
- Bubbles are strictly isolated from outside code

### Isolation Enforcement

```go
// go/src/runtime/chan.go:193
if c.bubble != nil && getg().bubble != c.bubble {
    fatal("send on synctest channel from outside bubble")
}
```

You CANNOT send on a bubble's channel from outside the bubble.

---

## 3. Key Data Structures

### A. `synctestBubble` Struct

Location: `go/src/runtime/synctest.go:14-39`

```go
type synctestBubble struct {
    mu      mutex
    timers  timers           // Bubble's own timer heap
    id      uint64           // Unique identifier
    now     int64            // Current fake time (nanoseconds)
    root    *g               // Goroutine that called synctest.Run()
    waiter  *g               // Goroutine blocked in synctest.Wait()
    main    *g               // User's function goroutine
    waiting bool             // Is Wait() in progress?
    done    bool             // Has main exited?

    // Idleness tracking - THE KEY TO EVERYTHING
    total   int              // Total goroutines in bubble
    running int              // Non-durably-blocked goroutines
    active  int              // External activity sources
}
```

**Critical Invariant**:
```
Bubble is active when: running > 0 || active > 0
Bubble is idle when:   running == 0 && active == 0
```

### B. Goroutine-Bubble Association

Location: `go/src/runtime/runtime2.go:575`

```go
type g struct {
    // ... other fields ...
    bubble  *synctestBubble    // Which bubble this G belongs to
}
```

### C. Channel-Bubble Association

Location: `go/src/runtime/chan.go:46`

```go
type hchan struct {
    // ... other fields ...
    bubble   *synctestBubble    // Which bubble owns this channel
}
```

When a channel is created inside a bubble:

```go
// go/src/runtime/chan.go:116-118
if b := getg().bubble; b != nil {
    c.bubble = b  // Capture bubble at creation
}
```

---

## 4. Bubble Lifecycle

### Phase 1: Bubble Creation

Location: `go/src/runtime/synctest.go:171-204`

```go
func synctestRun(f func()) {
    gp := getg()
    if gp.bubble != nil {
        panic("synctest.Run called from within a synctest bubble")
    }

    // Create the bubble
    bubble := &synctestBubble{
        id:      bubbleGen.Add(1),
        total:   1,      // root goroutine
        running: 1,      // root is running
        root:    gp,
    }

    // Fake time starts at midnight UTC 2000-01-01
    const synctestBaseTime = 946684800000000000
    bubble.now = synctestBaseTime

    // Associate root with bubble
    gp.bubble = bubble
    defer func() { gp.bubble = nil }()

    // Create main goroutine
    systemstack(func() {
        bubble.main = newproc1(fv, gp, pc, false, waitReasonZero)
        pp := getg().m.p.ptr()
        runqput(pp, bubble.main, true)
        wakep()
    })

    // Enter event loop...
}
```

### Phase 2: The Event Loop

Location: `go/src/runtime/synctest.go:206-235`

```go
lock(&bubble.mu)
bubble.active++  // Track loop activity
for {
    unlock(&bubble.mu)

    // Fire any ready timers
    systemstack(func() {
        gp.bubble.timers.check(bubble.now, bubble)
    })

    // Park until something happens
    gopark(synctestidle_c, nil, waitReasonSynctestRun, ...)

    lock(&bubble.mu)

    // Check for more timers
    next := bubble.timers.wakeTime()
    if next == 0 {
        break  // No more timers
    }
    if bubble.done {
        break  // Main exited
    }

    // ADVANCE FAKE TIME
    bubble.now = next
}
```

**The magic**: When all goroutines are durably blocked, `bubble.now` jumps to the next timer deadline instantly.

### Phase 3: Idleness Detection

Location: `go/src/runtime/synctest.go:269-280`

```go
func synctestidle_c(gp *g, _ unsafe.Pointer) bool {
    lock(&gp.bubble.mu)
    canIdle := true
    if gp.bubble.running == 0 && gp.bubble.active == 1 {
        // All goroutines blocked, only loop active
        canIdle = false  // Don't actually park, wake immediately
    } else {
        gp.bubble.active--
    }
    unlock(&gp.bubble.mu)
    return canIdle
}
```

### Phase 4: Teardown and Deadlock Detection

Location: `go/src/runtime/synctest.go:237-257`

```go
total := bubble.total
unlock(&bubble.mu)

if total != 1 {
    var reason string
    if bubble.done {
        reason = "deadlock: main exited but blocked goroutines remain"
    } else {
        reason = "deadlock: all goroutines in bubble are blocked"
    }
    panic(synctestDeadlockError{reason: reason, bubble: bubble})
}
```

---

## 5. Durable Blocking - THE KEY CONCEPT

### What Makes a Block "Durable"?

A goroutine is **durably blocked** if it can ONLY be unblocked by:
1. Another goroutine in the same bubble, OR
2. Time advancing

**Durable blocks** (from `runtime2.go:1381-1399`):

```go
var isIdleInSynctest = [...]bool{
    waitReasonSleep:                 true,   // time.Sleep
    waitReasonSynctestChanReceive:   true,   // Channel receive (bubble channel)
    waitReasonSynctestChanSend:      true,   // Channel send (bubble channel)
    waitReasonSynctestSelect:        true,   // Select (all bubble channels)
    waitReasonSyncCondWait:          true,   // sync.Cond.Wait
    waitReasonSynctestWaitGroupWait: true,   // sync.WaitGroup.Wait
    // ...
}
```

**NOT durable** (can be unblocked from outside):

```go
waitReasonSyncMutexLock           // Mutex - NOT durable!
waitReasonIOWait                  // Network I/O - NOT durable!
```

### Why Mutex is NOT Durable

A mutex can be unlocked by ANY goroutine, not just those in the bubble. So blocking on a mutex doesn't count as "durably blocked" - the bubble stays active.

### The Tracking Mechanism

Every goroutine status change goes through `casgstatus()`:

```go
// go/src/runtime/proc.go:1316-1319
if gp.bubble != nil {
    systemstack(func() {
        gp.bubble.changegstatus(gp, oldval, newval)
    })
}
```

Which calls:

```go
// go/src/runtime/synctest.go:43-107
func (bubble *synctestBubble) changegstatus(gp *g, oldval, newval uint32) {
    wasRunning := !isIdleInSynctest[oldval.waitreason]
    isRunning := !isIdleInSynctest[newval.waitreason]

    if wasRunning != isRunning {
        lock(&bubble.mu)
        if isRunning {
            bubble.running++
        } else {
            bubble.running--
        }
        wake := bubble.maybeWakeLocked()
        unlock(&bubble.mu)

        if wake != nil {
            goready(wake, 0)  // Wake root or waiter
        }
    }
}
```

---

## 6. Call Graph: How It All Fits Together

### `synctest.Run()` Flow

```
synctest.Run(f)
│
├─ Create synctestBubble
│  ├─ id = nextID++
│  ├─ now = 946684800000000000 (year 2000)
│  ├─ total = 1, running = 1
│  └─ root = getg()
│
├─ Create main goroutine
│  ├─ newproc1(f)
│  ├─ runqput(pp, main, true)
│  └─ wakep()
│
└─ EVENT LOOP:
   │
   └─ WHILE true:
      │
      ├─ bubble.timers.check(bubble.now)
      │  └─ Fire timers where when <= now
      │
      ├─ gopark(synctestidle_c)
      │  └─ IF running==0 && active==1:
      │     └─ Don't park, wake immediately
      │
      └─ IF woken:
         ├─ next = bubble.timers.wakeTime()
         ├─ IF next == 0: BREAK
         ├─ IF bubble.done: BREAK
         └─ bubble.now = next  ← TIME JUMP!
```

### `time.Sleep()` in a Bubble

```
time.Sleep(duration)
│
├─ Create timer: when = bubble.now + duration
├─ Add to bubble.timers (not P.timers!)
│
├─ gopark(reason=waitReasonSleep)
│  └─ changegstatus(_Grunning → _Gwaiting)
│     └─ bubble.running--
│     └─ maybeWakeLocked():
│        └─ IF running==0 && active==0:
│           ├─ next = bubble.timers.wakeTime()
│           ├─ bubble.now = next  ← TIME JUMP!
│           └─ Wake root
│              └─ Root fires timer
│                 └─ goready(sleeper)
│
└─ Returns immediately (no real time passed!)
```

### Channel Operations

```
ch <- value  (inside bubble)
│
├─ Check: ch.bubble == getg().bubble? YES
│
├─ IF receiver waiting:
│  └─ Copy value, goready(receiver)
│     └─ changegstatus(receiver, _Gwaiting → _Grunnable)
│        └─ bubble.running++
│
└─ ELSE (no receiver):
   └─ gopark(reason=waitReasonSynctestChanSend)
      └─ changegstatus(_Grunning → _Gwaiting)
         └─ bubble.running--
         └─ maybeWakeLocked()
```

---

## 7. Integration Points with Scheduler

### A. Status Change Hook

Location: `go/src/runtime/proc.go:1277-1320`

```go
func casgstatus(gp *g, oldval, newval uint32) {
    // ... CAS to change status ...

    if gp.bubble != nil {
        systemstack(func() {
            gp.bubble.changegstatus(gp, oldval, newval)
        })
    }
}
```

**Every** goroutine status change in a bubble triggers tracking.

### B. Channel Association

Location: `go/src/runtime/chan.go:116-118`

```go
func makechan(t *chantype, size int) *hchan {
    // ...
    if b := getg().bubble; b != nil {
        c.bubble = b
    }
    return c
}
```

### C. Timer Management

Bubble timers are separate from P timers:

- Normal timer: Added to `P.timers`
- Bubble timer: Added to `bubble.timers`

When `time.Sleep()` is called:
1. Check if goroutine is in bubble
2. If yes: Add timer to `bubble.timers` with `when = bubble.now + duration`
3. Timer fires when `bubble.now >= when`

---

## 8. Example Trace

```go
func TestExample(t *testing.T) {
    synctest.Test(t, func(t *testing.T) {
        ch := make(chan int)
        go func() {
            ch <- 42
        }()
        x := <-ch
    })
}
```

**Execution**:

```
1. synctestRun() creates bubble
   total=1, running=1, root=G0

2. Create main goroutine G1
   total=2, running=2

3. G1 runs: ch := make(chan int)
   ch.bubble = bubble

4. G1 runs: go func() { ch <- 42 }
   Create G2, G2.bubble = bubble
   total=3, running=3

5. Scheduler runs G2: ch <- 42
   No receiver, G2 parks (waitReasonSynctestChanSend)
   running-- → running=2 (durable block!)

6. Scheduler runs G1: x := <-ch
   G2 is in send queue
   Copy value, goready(G2)
   running++ → running=3

7. G1 exits
   total=2, done=true

8. G2 wakes, exits
   total=1

9. Root checks: total==1? YES
   SUCCESS - no deadlock

Time elapsed: ~0ms (no real waiting!)
```

---

## 9. Key Takeaways

### What Synctest Controls

1. **Time**: Virtual clock that jumps forward
2. **Durable blocking detection**: Knows when goroutines are truly stuck
3. **Channel isolation**: Prevents outside interference
4. **Deadlock detection**: Automatic panic when stuck

### What Synctest Does NOT Control

1. **Execution order**: Scheduler still decides which goroutine runs
2. **`cheaprand()` randomization**: Still random if `-race` enabled
3. **Mutex blocking**: Not considered durable
4. **External I/O**: Can't virtualize network/disk

### The Gap for Our Research

Synctest solves **time determinism** but not **schedule determinism**.

To achieve full determinism, we need to also control:
- `cheaprand()` in scheduler decisions
- Work stealing order
- Select case ordering

That's what Phase 1 of our research is about!

---

## 10. References

- Source: `go/src/runtime/synctest.go`
- Source: `go/src/runtime/runtime2.go`
- Source: `go/src/runtime/chan.go`
- Source: `go/src/runtime/proc.go`
