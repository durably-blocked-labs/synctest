# Notes on Synctest

Synctest just added the concept of **durable blocking** so we know when all goroutines are "durably"/truly sleeping - stuck waiting for time to move forward.

If everyone is truly stuck, we can jump time forward instantly. No actual waiting.

---

## How time.Sleep Normally Works

In regular Go, `time.Sleep(5 * time.Second)`:

1. Adds a timer to the **P's timer heap** (`P.timers`)
2. Parks the goroutine via `gopark()`
3. Something wakes it later - either the P's timer checker (during scheduling) or `sysmon` (background thread that monitors things)
4. `goready()` puts the goroutine back on a run queue

```
P (Processor)
+------------------+
| timers (heap)    |  <-- Timers live here
|   [wake G1 at T+5s]
|   [wake G2 at T+10s]
+------------------+
```

The runtime checks `timers.wakeTime()` and fires ready timers via `timers.check()`.

---

## How time.Sleep Works in a Bubble

In a bubble, `time.Sleep(5 * time.Second)` works differently:

1. Timer goes to **`bubble.timers`** (NOT P's timers!)
2. Goroutine parks
3. `bubble.running--`
4. If `running == 0` → wake the event loop
5. Event loop sets `bubble.now = next timer deadline` (TIME JUMP!)
6. Timer fires, goroutine wakes, `running++`

Key difference: timers live in the bubble, time is fake.

```
Bubble
+------------------+
| timers (heap)    |  <-- Bubble's own timer heap!
| now = T0         |  <-- Fake time, not real time
| running = 0      |  <-- When this hits 0, jump time!
+------------------+
```

From [time.go](https://github.com/golang/go/blob/master/src/runtime/time.go):

```go
func timeSleep(ns int64) {
    gp := getg()
    // ...
    var now int64
    if bubble := gp.bubble; bubble != nil {
        now = bubble.now  // Use bubble's fake time!
    } else {
        now = nanotime()  // Use real time
    }
    when := now + ns
    // Add timer to bubble.timers or P.timers depending on bubble
}
```

---

## The synctestBubble Struct

[runtime/synctest.go:14-39](https://github.com/golang/go/blob/master/src/runtime/synctest.go#L14)

```go
type synctestBubble struct {
    mu      mutex
    timers  timers           // Bubble's timer heap (NOT P's!)
    id      uint64           // Unique identifier
    now     int64            // Fake time in nanoseconds
    root    *g               // Event loop goroutine
    waiter  *g               // synctest.Wait() goroutine
    main    *g               // User's test function goroutine
    waiting bool             // Is Wait() in progress?
    done    bool             // Has main exited?

    // THE KEY COUNTERS
    total   int              // Total goroutines in bubble
    running int              // Non-durably-blocked goroutines
    active  int              // External activity sources (event loop)
}
```

**Fields:**

- **`timers`**: Bubble's timer heap. `time.Sleep` adds timers here, not P.
- **`now`**: Fake clock. Starts at year 2000. Only moves when `running == 0`.
- **`root`**: Goroutine that called `synctest.Run()`. Runs the event loop.
- **`main`**: Your test function. Different from root!
- **`waiter`**: Goroutine parked in `synctest.Wait()`.
- **`total`**: Total goroutines in bubble. For deadlock detection.
- **`running`**: THE critical counter. Goroutines not durably blocked. When 0, time advances.
- **`active`**: Event loop activity. Prevents false idleness while processing.

**Critical Invariant**:
```
Bubble is active when: running > 0 || active > 0
Bubble is idle when:   running == 0 && active == 0
```

<details>
<summary>What's the difference between root and main?</summary>

They're different goroutines!

- **`root`**: The goroutine that called `synctest.Run()`. This goroutine runs the event loop (checking timers, advancing time).
- **`main`**: The goroutine that runs YOUR code inside `synctest.Run(f)`.

```
synctest.Run(func() {
    // This code runs in the `main` goroutine
    // The `root` goroutine is elsewhere, running the event loop
})
```

The root creates main, then enters the event loop. They run concurrently (well, cooperatively - Go's M:P:G scheduler).

</details>

---

## The Event Loop

The root goroutine runs an event loop. This is the heart of synctest.

[runtime/synctest.go:206-235](https://github.com/golang/go/blob/master/src/runtime/synctest.go#L206)

```go
lock(&bubble.mu)
bubble.active++  // "I'm active"
for {
    unlock(&bubble.mu)

    // Fire ready timers
    systemstack(func() {
        bubble.timers.check(bubble.now)
    })

    // Try to park
    gopark(synctestidle_c, nil, waitReasonSynctestRun, ...)

    lock(&bubble.mu)
    bubble.active++  // Back in play after wake

    // Check exit conditions
    next := bubble.timers.wakeTime()
    if next == 0 {
        break  // No more timers
    }
    if bubble.done {
        break  // Main goroutine exited
    }

    // ADVANCE TIME!
    bubble.now = next  // TIME JUMPS INSTANTLY!
}
bubble.active--
unlock(&bubble.mu)
```

The loop:
1. Fires any timers whose deadline <= `bubble.now`
2. Tries to park (but might not actually park - see `synctestidle_c`)
3. When woken, advances `bubble.now` to the next timer's deadline
4. Repeat

<details>
<summary>Deep dive: synctestidle_c logic</summary>

The `synctestidle_c` callback decides whether to actually park or continue immediately.

[runtime/synctest.go:269-280](https://github.com/golang/go/blob/master/src/runtime/synctest.go#L269)

```go
func synctestidle_c(gp *g, _ unsafe.Pointer) bool {
    lock(&gp.bubble.mu)

    if gp.bubble.running == 0 && gp.bubble.active == 1 {
        // All goroutines blocked, only the event loop is active
        // Don't park - advance time immediately!
        unlock(&gp.bubble.mu)
        return false  // Don't park, continue the loop
    }

    // There's still activity, actually park and wait
    gp.bubble.active--
    unlock(&gp.bubble.mu)
    return true  // Park and wait for goready()
}
```

The check is `running == 0 && active == 1`:
- `running == 0`: All goroutines are durably blocked
- `active == 1`: Only the event loop itself is active

If both conditions are met, returning `false` means "don't actually park, continue immediately." This is how time advances without any real delay - the event loop just keeps spinning, advancing `bubble.now` each iteration.

If `running > 0`, actual work is happening somewhere, so the event loop parks and waits to be woken by `maybeWakeLocked()`.

</details>

---

## The running Counter

This is the key to everything. Every durable block decrements `running`:

```
G1: time.Sleep(5s)     -> running--
G2: <-ch               -> running-- (if bubble channel)
G3: select { ... }     -> running-- (if all bubble channels)
```

Every wake increments `running`:

```
Timer fires            -> running++
Channel send/recv      -> running++
```

When `running == 0`: everyone is truly stuck, advance time!

The tracking happens in `changegstatus()`:

[runtime/synctest.go:43-107](https://github.com/golang/go/blob/master/src/runtime/synctest.go#L43)

```go
func (bubble *synctestBubble) changegstatus(gp *g, oldval, newval uint32) {
    wasRunning := !isIdleInSynctest[gp.waitreason]
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
            goready(wake, 0)  // Wake event loop or waiter
        }
    }
}
```

This is called by `casgstatus()` ([proc.go:1277-1320](https://github.com/golang/go/blob/master/src/runtime/proc.go#L1277)) every time a goroutine's status changes. If the goroutine is in a bubble, `changegstatus()` updates the counters.

---

## Durable vs Non-Durable Blocking

**Durable** = can ONLY be woken by bubble-internal activity or time:

| Operation | Wait Reason | Durable? |
|-----------|-------------|----------|
| `time.Sleep` | `waitReasonSleep` | **Yes** |
| `<-ch` (bubble channel) | `waitReasonSynctestChanReceive` | **Yes** |
| `ch <-` (bubble channel) | `waitReasonSynctestChanSend` | **Yes** |
| `select` (bubble channels) | `waitReasonSynctestSelect` | **Yes** |
| `sync.Cond.Wait` | `waitReasonSyncCondWait` | **Yes** |
| `sync.WaitGroup.Wait` | `waitReasonSynctestWaitGroupWait` | **Yes** |

**NOT Durable** = could be woken by external code:

| Operation | Wait Reason | Durable? |
|-----------|-------------|----------|
| `sync.Mutex.Lock` | `waitReasonSyncMutexLock` | **No** |
| `sync.RWMutex` | `waitReasonSyncRWMutex*` | **No** |
| Network I/O | `waitReasonIOWait` | **No** |
| `<-ch` (non-bubble channel) | `waitReasonChanReceive` | **No** |

The `isIdleInSynctest` array defines this:

[runtime/runtime2.go:1381-1399](https://github.com/golang/go/blob/master/src/runtime/runtime2.go#L1381)

```go
var isIdleInSynctest = [...]bool{
    // NOT durable
    waitReasonIOWait:                false,
    waitReasonChanReceive:           false,  // Regular chan (outside bubble)
    waitReasonChanSend:              false,  // Regular chan
    waitReasonSelect:                false,  // Regular select
    waitReasonSyncMutexLock:         false,  // Mutex!
    waitReasonSyncRWMutexRLock:      false,
    waitReasonSyncRWMutexLock:       false,
    // ... many more false entries ...

    // DURABLE (synctest-specific)
    waitReasonSleep:                 true,
    waitReasonSynctestChanReceive:   true,   // Bubble channel
    waitReasonSynctestChanSend:      true,   // Bubble channel
    waitReasonSynctestSelect:        true,   // Select on bubble channels
    waitReasonSyncCondWait:          true,
    waitReasonSynctestWaitGroupWait: true,
    waitReasonSemacquire:            true,
    waitReasonSynctestRun:           true,   // Event loop itself
}
```

<details>
<summary>Why is mutex NOT durable?</summary>

A mutex can be unlocked by ANY goroutine, even ones outside the bubble:

```go
var mu sync.Mutex

// In bubble:
go func() {
    mu.Lock()  // This blocks...
}()

// Outside bubble (hypothetically):
mu.Unlock()  // Could unlock it!
```

Because external code could release the lock, waiting on a mutex is NOT considered "durably blocked." The bubble stays active, `running` doesn't decrement, and time won't advance.

**Practical implication**:
```go
synctest.Test(t, func(t *testing.T) {
    var mu sync.Mutex
    mu.Lock()

    go func() {
        mu.Lock()  // Blocks, but NOT durable
    }()

    synctest.Wait()  // Will NOT return!
    // Deadlock: goroutine blocked on mutex,
    // but bubble thinks it's still running
})
```

</details>

<details>
<summary>Why is network I/O NOT durable?</summary>

Network I/O depends on external servers - completely outside the bubble's control.

```go
conn.Read(buf)  // Waiting for remote server
```

The remote server is outside the bubble. If synctest advanced time while waiting for network data, your test would break - the data would never arrive.

**Recommendation**: Don't do real I/O in synctest bubbles. Use mocks or interfaces.

</details>

<details>
<summary>Why does channel receive have TWO wait reasons?</summary>

There are two different wait reasons:
- `waitReasonChanReceive` - for channels created **outside** the bubble
- `waitReasonSynctestChanReceive` - for channels created **inside** the bubble

Only the second one is durable. This is how synctest distinguishes "safe" blocking from potentially external-dependent blocking:

```go
// go/src/runtime/chan.go
if c.bubble != nil {
    waitreason = waitReasonSynctestChanReceive  // Durable!
} else {
    waitreason = waitReasonChanReceive  // Not durable
}
```

If you use a global channel inside a bubble, blocking on it is NOT durable - external goroutines could send to it.

</details>

---

## Channel Operations in Bubbles

When a channel is created inside a bubble:

[runtime/chan.go:116-118](https://github.com/golang/go/blob/master/src/runtime/chan.go#L116)

```go
if b := getg().bubble; b != nil {
    c.bubble = b  // Channel belongs to this bubble
}
```

Channel isolation is enforced - you can't use bubble channels from outside:

[runtime/chan.go:193](https://github.com/golang/go/blob/master/src/runtime/chan.go#L193)

```go
if c.bubble != nil && getg().bubble != c.bubble {
    fatal("send on synctest channel from outside bubble")
}
```

So bubble channels can only be used by bubble goroutines. This is why channel ops on bubble channels are durable - no external interference is possible.

---

## The Wake Chain

When a goroutine durably blocks:

```
gopark(reason = waitReasonSleep / waitReasonSynctestChanRecv / etc.)
    |
    v
casgstatus(_Grunning -> _Gwaiting)
    |
    v
bubble.changegstatus()
    |
    v
if isIdleInSynctest[waitreason]:
    bubble.running--
    |
    v
maybeWakeLocked()
    |
    v
if running == 0 && active == 0:
    goready(bubble.root)  // Wake the event loop!
```

The `maybeWakeLocked()` function decides what to wake:

[runtime/synctest.go:131-158](https://github.com/golang/go/blob/master/src/runtime/synctest.go#L131)

```go
func (bubble *synctestBubble) maybeWakeLocked() *g {
    if bubble.running > 0 || bubble.active > 0 {
        return nil  // Bubble isn't idle yet
    }

    bubble.active++  // Preemptively increment
    next := bubble.timers.wakeTime()

    if next > 0 && next <= bubble.now {
        // Timer ready to fire - wake root to handle it
        return bubble.root
    }
    if gp := bubble.waiter; gp != nil {
        // Someone called synctest.Wait() - wake them
        return gp
    }
    // All blocked, nothing waiting - wake root (probably deadlock)
    return bubble.root
}
```

---

## Complete Example Trace

```go
func TestComplete(t *testing.T) {
    synctest.Test(t, func(t *testing.T) {
        ch := make(chan int)

        go func() {
            time.Sleep(1 * time.Second)
            ch <- 42
        }()

        x := <-ch
        assert.Equal(t, 42, x)
    })
}
```

**Step by step**:

```
T=0ms (real), T=0 (fake): synctest.Run() creates bubble
       bubble = {running: 1, active: 1, now: T0, total: 1}
       Event loop starts

T=0ms: Main goroutine (G1) starts
       bubble.running = 1 (already counted)

T=0ms: G1 creates channel
       ch.bubble = bubble

T=0ms: G1 spawns G2
       bubble.total++ -> 2
       bubble.running++ -> 2

T=0ms: Event loop: gopark(synctestidle_c)
       running==0 && active==1? NO (running=2)
       active-- -> 0, actually park

T=0ms: G2 runs: time.Sleep(1s)
       Add timer {when: T0+1s, wake: G2} to bubble.timers
       gopark() -> running-- -> running=1
       maybeWakeLocked() -> running>0, don't wake

T=0ms: G1 runs: x := <-ch
       No sender, enqueue in ch.recvq
       gopark(waitReasonSynctestChanReceive)
       running-- -> running=0
       maybeWakeLocked() -> running==0 && active==0, goready(root)!

T=0ms (real): Event loop wakes
       active++ -> 1
       next = T0+1s
       bubble.now = T0+1s  <-- TIME JUMPS 1 SECOND INSTANTLY!
       timers.check():
           G2's timer ready -> goready(G2)
           running++ -> 1
       gopark(synctestidle_c)
           running==0? NO (running=1)
           active-- -> 0, park

T=0ms (real), T=1s (fake): G2 runs
       ch <- 42
       Found G1 in recvq
       Copy 42 to G1
       goready(G1) -> running++ -> 2
       G2 continues, exits
       running-- -> 1

T=0ms (real): G1 wakes with x=42
       Continues, assertion passes, returns
       bubble.done = true
       running-- -> 0

T=0ms (real): Cleanup
       Event loop detects bubble.done == true
       Exits successfully

Total real time: ~0ms
Total fake time: 1s
```

The magic: 1 second of sleep, 0 milliseconds of real time.

---

## Another Example: Multiple Goroutines with Staggered Sleeps

```go
func TestStaggered(t *testing.T) {
    synctest.Test(t, func(t *testing.T) {
        results := make([]int, 0, 3)
        var mu sync.Mutex

        for i := 1; i <= 3; i++ {
            i := i
            go func() {
                time.Sleep(time.Duration(i) * time.Second)
                mu.Lock()
                results = append(results, i)
                mu.Unlock()
            }()
        }

        // Wait for all via channel
        done := make(chan bool)
        go func() {
            time.Sleep(4 * time.Second)
            done <- true
        }()
        <-done

        // Results should be [1, 2, 3] due to timing
    })
}
```

Time jumps from 0 -> 1s -> 2s -> 3s -> 4s (four instant jumps). The mutex locks between results are NOT durable, but they complete quickly so it doesn't matter.

Real time elapsed: ~0ms.

---

## Deadlock Detection

When does synctest detect a deadlock?

[runtime/synctest.go:237-257](https://github.com/golang/go/blob/master/src/runtime/synctest.go#L237)

```go
total := bubble.total
unlock(&bubble.mu)

if total != 1 {
    var reason string
    if bubble.done {
        reason = "deadlock: main bubble goroutine has exited but blocked goroutines remain"
    } else {
        reason = "deadlock: all goroutines in bubble are blocked"
    }
    panic(synctestDeadlockError{reason: reason, bubble: bubble})
}
```

Two types:

1. **All blocked**: `running == 0`, no timers, but `done == false` (main hasn't finished)
2. **Main exited with stragglers**: `done == true` but `total > 1` (goroutines still alive)

Example:
```go
synctest.Test(t, func(t *testing.T) {
    ch := make(chan int)
    <-ch  // Blocks forever, no sender
    // PANIC: deadlock: all goroutines in bubble are blocked
})
```

---

## References

- [synctest.go](https://github.com/golang/go/blob/master/src/runtime/synctest.go) - Bubble implementation, event loop
- [proc.go](https://github.com/golang/go/blob/master/src/runtime/proc.go) - gopark, goready, casgstatus
- [chan.go](https://github.com/golang/go/blob/master/src/runtime/chan.go) - Channel operations, isolation checks
- [runtime2.go](https://github.com/golang/go/blob/master/src/runtime/runtime2.go) - synctestBubble, g, hchan structs, isIdleInSynctest
- [time.go](https://github.com/golang/go/blob/master/src/runtime/time.go) - Timer implementation, fake time
- [select.go](https://github.com/golang/go/blob/master/src/runtime/select.go) - Select implementation
- [sema.go](https://github.com/golang/go/blob/master/src/runtime/sema.go) - Semaphore (WaitGroup, Cond)
