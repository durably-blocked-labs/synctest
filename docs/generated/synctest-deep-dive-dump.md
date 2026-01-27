# Synctest Deep Dive: A Rabbithole Journey

This document is a comprehensive exploration of Go's `synctest` package - how it works at the runtime level, the key abstractions that make it tick, and where it might go in the future. It is designed to be read as a learning journey, with collapsible sections for deeper questions along the way.

**Source Files Referenced**:
- `go/src/runtime/synctest.go` - Core bubble implementation
- `go/src/runtime/runtime2.go` - Struct definitions (g, hchan, waitReason)
- `go/src/runtime/proc.go` - Scheduler integration
- `go/src/runtime/chan.go` - Channel operations
- `go/src/runtime/select.go` - Select statement handling
- `go/src/runtime/time.go` - Timer and sleep operations
- `go/src/runtime/sema.go` - Semaphore (WaitGroup, Cond)
- `go/src/runtime/rand.go` - Random number generation

---

## 1. The Problem Synctest Solves

### Why Testing Concurrent Code is Hard

Testing concurrent code is notoriously difficult because:

1. **Time-dependent tests are slow**: `time.Sleep(2 * time.Second)` actually waits 2 seconds
2. **Race conditions are hard to reproduce**: They depend on precise timing
3. **Deadlock detection is manual**: No automatic detection without extra tooling
4. **Tests are non-deterministic**: The same test can pass or fail randomly

Consider this real-world scenario:

```go
// WITHOUT SYNCTEST: Takes 100ms of real time
func TestConcurrency(t *testing.T) {
    done := make(chan bool)
    go func() {
        time.Sleep(100 * time.Millisecond)
        done <- true
    }()
    <-done
}
```

If you have 1000 tests like this, your test suite takes **100 seconds** just waiting. That's almost 2 minutes of pure waiting.

### Synctest's Solution

Synctest creates an **isolated execution context** called a "bubble" where:

- **Time is virtualized**: A fake clock that only advances when all goroutines are blocked
- **Blocking is detectable**: The runtime knows when goroutines are "durably blocked"
- **Deadlocks are caught**: Automatic panic when all goroutines are stuck

```go
// WITH SYNCTEST: Takes microseconds!
func TestConcurrency(t *testing.T) {
    synctest.Run(func() {
        done := make(chan bool)
        go func() {
            time.Sleep(100 * time.Millisecond)
            done <- true
        }()
        <-done
    })
}
```

The same 1000 tests now complete **instantly** because fake time jumps forward whenever nothing can happen.

<details>
<summary>Q: What does "time jumps forward" actually mean?</summary>

When all goroutines in the bubble are "durably blocked" (more on this later), the bubble's fake clock (`bubble.now`) advances instantly to the next scheduled timer. The goroutines don't know this happened - they wake up thinking they slept for the full duration.

Example:
- T=0: Goroutine calls `time.Sleep(10 * time.Second)`
- T=0: Goroutine parks, bubble detects all goroutines blocked
- T=0 (real): `bubble.now` jumps to T=10s (fake)
- T=0 (real): Timer fires, goroutine wakes up
- Goroutine thinks 10 seconds passed, but only microseconds of real time elapsed

</details>

<details>
<summary>Q: What does synctest NOT solve?</summary>

**Execution order determinism.** Goroutines can still interleave in non-deterministic ways. The scheduler still makes random choices about which goroutine runs next.

If you have two goroutines writing to the same variable:
```go
synctest.Run(func() {
    var x int
    go func() { x = 1 }()
    go func() { x = 2 }()
    // Which one wins? Still non-deterministic!
})
```

Synctest solves TIME non-determinism, not SCHEDULE non-determinism.

</details>

---

## 2. The Bubble Concept

### What is a Bubble?

A bubble is a **logically isolated execution environment**:

- All goroutines created within the bubble belong to it
- All channels created within the bubble are associated with it
- The bubble has its own fake clock
- Bubbles are strictly isolated from outside code

### The `synctestBubble` Struct

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

<details>
<summary>Q: What does the `active` counter track exactly?</summary>

The `active` counter tracks "external activity sources" - things that keep the bubble from being considered idle even if all goroutines are blocked.

The primary use is for the **event loop itself**. When the event loop is running (checking timers, deciding what to do), it increments `active` so the bubble doesn't think it's idle.

From the event loop code:
```go
lock(&bubble.mu)
bubble.active++  // "I'm doing work, don't think we're idle"
for {
    // ... do timer processing ...
    gopark(synctestidle_c, ...)  // This decrements active if we actually park
    // ...
    bubble.active++  // We woke up, back to being active
}
bubble.active--  // Done with loop
```

Without this, the bubble would incorrectly detect idleness while the event loop was in the middle of firing timers.

</details>

<details>
<summary>Q: What's the difference between `root` and `main`?</summary>

- **`root`**: The goroutine that called `synctest.Run()`. This goroutine runs the event loop.
- **`main`**: The goroutine that runs the user's function `f` inside `synctest.Run(f)`.

They are different goroutines! The root goroutine creates `main`, then enters the event loop. The `main` goroutine executes your test code.

```
synctest.Run(func() {
    // This code runs in `main` goroutine
    // The `root` goroutine is elsewhere, running the event loop
})
```

</details>

<details>
<summary>Q: Why is `now` an `int64` in nanoseconds?</summary>

For precision and compatibility with Go's time package. The `time.Time` type internally stores nanoseconds since a fixed epoch. Using nanoseconds for `bubble.now` allows direct comparison with timer deadlines without conversion.

The base time is:
```go
const synctestBaseTime = 946684800000000000 // midnight UTC 2000-01-01
```

This specific date is arbitrary but memorable - the start of the year 2000.

</details>

### Goroutine-Bubble Association

Every goroutine has a `bubble` field:

Location: `go/src/runtime/runtime2.go:575`

```go
type g struct {
    // ... other fields ...
    bubble  *synctestBubble    // Which bubble this G belongs to
}
```

When a goroutine spawns another goroutine inside a bubble, the child **inherits** the bubble membership.

### Channel-Bubble Association

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

### Isolation Enforcement

Bubbles enforce strict isolation. You CANNOT interact with a bubble's channel from outside:

```go
// go/src/runtime/chan.go:193
if c.bubble != nil && getg().bubble != c.bubble {
    fatal("send on synctest channel from outside bubble")
}
```

This prevents external code from accidentally (or intentionally) interfering with bubble operations.

<details>
<summary>Q: What happens if I try to use a global channel inside a bubble?</summary>

If the channel was created **outside** the bubble (so `ch.bubble == nil`), then:
- You CAN use it inside the bubble
- But blocking on it is **NOT** considered durable
- The bubble won't advance time while you're waiting on it

This is intentional - external channels might receive data from goroutines outside the bubble, so it's not safe to assume you're "durably blocked."

</details>

---

## 3. The Event Loop

The event loop is the heart of synctest. It runs in the `root` goroutine that called `synctest.Run()`.

### Event Loop Structure

Location: `go/src/runtime/synctest.go:206-235`

```go
lock(&bubble.mu)
bubble.active++  // Mark loop as active
for {
    unlock(&bubble.mu)

    // Fire any ready timers
    systemstack(func() {
        bubble.timers.check(bubble.now)
    })

    // Try to park (may not actually park)
    gopark(synctestidle_c, nil, waitReasonSynctestRun, ...)

    lock(&bubble.mu)
    bubble.active++  // Back in play after wake

    // Check for exit conditions
    next := bubble.timers.wakeTime()
    if next == 0 {
        break  // No more timers
    }
    if bubble.done {
        break  // Main goroutine exited
    }

    // ADVANCE FAKE TIME
    bubble.now = next
}
bubble.active--
unlock(&bubble.mu)
```

### The `synctestidle_c` Callback

This callback decides whether the event loop should actually park or continue immediately:

Location: `go/src/runtime/synctest.go:269-280`

```go
func synctestidle_c(gp *g, _ unsafe.Pointer) bool {
    lock(&gp.bubble.mu)

    if gp.bubble.running == 0 && gp.bubble.active == 1 {
        // All goroutines blocked, only loop is active
        // Don't park - advance time immediately!
        unlock(&gp.bubble.mu)
        return false  // Don't park
    }

    // There's still activity, actually park
    gp.bubble.active--
    unlock(&gp.bubble.mu)
    return true  // Park and wait
}
```

**Key insight**: When `running == 0` (all goroutines durably blocked) and `active == 1` (only the event loop is active), returning `false` means "don't actually park, continue immediately." This is how time advances without delay.

<details>
<summary>Q: Who wakes the event loop when it's parked?</summary>

The `maybeWakeLocked()` function determines what to wake when the bubble becomes idle:

```go
// go/src/runtime/synctest.go:131-158
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

This is called every time a goroutine's status changes to check if the bubble is now idle.

</details>

### Event Loop State Machine

```
                    +----------------------------------+
                    |         LOOP RUNNING             |
                    |         active = 1               |
                    +----------------+-----------------+
                                     |
                                     v
                    +----------------------------------+
                    |      timers.check(now)           |
                    |      Fire ready timers           |
                    +----------------+-----------------+
                                     |
                                     v
                    +----------------------------------+
                    |   gopark(synctestidle_c)         |
                    +----------------+-----------------+
                                     |
                    +----------------+----------------+
                    |                                 |
            running > 0                        running == 0
                    |                                 |
                    v                                 v
        +---------------------+       +---------------------------+
        | ACTUALLY PARK       |       | DON'T PARK                |
        | active--  -> 0      |       | active stays 1            |
        | Wait for goready()  |       | Continue immediately      |
        +----------+----------+       +-------------+-------------+
                   |                                |
                   | (woken by                      |
                   |  maybeWakeLocked)              |
                   |                                |
                   v                                |
        +---------------------+                     |
        | active++  -> 1      |                     |
        +----------+----------+                     |
                   |                                |
                   +-----------------+--------------+
                                     |
                                     v
                    +----------------------------------+
                    |   next = timers.wakeTime()       |
                    |   bubble.now = next              |
                    |   (TIME ADVANCES!)               |
                    +----------------+-----------------+
                                     |
                                     v
                               Loop continues
```

### Wake Chain

When a goroutine becomes durably blocked, a chain of events leads to the event loop waking:

```
User goroutine parks (durably)
        |
        v
casgstatus(_Grunning -> _Gwaiting)
        |
        v
bubble.changegstatus()
        |
        v
bubble.running--
        |
        v
maybeWakeLocked()
        |
        +---> running > 0?  -> return nil (don't wake)
        |
        +---> running == 0? -> goready(bubble.root)
                                    |
                                    v
                           Event loop wakes!
                           Advances bubble.now
                           Fires ready timers
```

---

## 4. Durable Blocking - THE KEY CONCEPT

This is the most important concept in synctest. Without understanding durable blocking, the rest doesn't make sense.

### What Makes a Block "Durable"?

A goroutine is **durably blocked** if it can ONLY be unblocked by:
1. Another goroutine **in the same bubble**, OR
2. Time advancing

The key question synctest must answer: **"When is it safe to jump time forward?"**

**Bad approach**: "When all goroutines are sleeping"
```go
mu := sync.Mutex{}
mu.Lock()
go func() {
    mu.Lock()  // Sleeps waiting for mutex
    // ...
}()
// If synctest advanced time here, it would be WRONG!
// The goroutine will wake when the mutex is released, not when time advances
```

**Good approach**: "When all goroutines are **durably blocked**"

### The `isIdleInSynctest` Array

Location: `go/src/runtime/runtime2.go:1381-1399`

This array defines which wait reasons count as "durably blocked":

```go
var isIdleInSynctest = [...]bool{
    // === NOT DURABLE ===
    waitReasonZero:                  false,
    waitReasonGCAssistMarking:       false,
    waitReasonIOWait:                false,  // Network I/O - NOT durable!
    waitReasonChanReceiveNilChan:    false,
    waitReasonChanSendNilChan:       false,
    waitReasonSelect:                false,  // Regular select
    waitReasonChanReceive:           false,  // Regular chan
    waitReasonChanSend:              false,  // Regular chan
    waitReasonSyncMutexLock:         false,  // Mutex - NOT durable!
    waitReasonSyncRWMutexRLock:      false,  // NOT durable!
    waitReasonSyncRWMutexLock:       false,  // NOT durable!
    // ... many more false entries ...

    // === DURABLE (synctest-specific) ===
    waitReasonSleep:                 true,   // time.Sleep
    waitReasonSynctestChanReceive:   true,   // chan recv in bubble
    waitReasonSynctestChanSend:      true,   // chan send in bubble
    waitReasonSynctestSelect:        true,   // select in bubble
    waitReasonSyncCondWait:          true,   // sync.Cond.Wait
    waitReasonSynctestWaitGroupWait: true,   // sync.WaitGroup.Wait
    waitReasonSemacquire:            true,   // semaphore
    waitReasonSynctestRun:           true,   // event loop itself
}
```

### Why Certain Operations Are Durable

| Operation | Wait Reason | Durable? | Why |
|-----------|-------------|----------|-----|
| `time.Sleep` | `waitReasonSleep` | **Yes** | Only time can wake it |
| `<-ch` (bubble chan) | `waitReasonSynctestChanReceive` | **Yes** | Only bubble goroutines can send |
| `ch <-` (bubble chan) | `waitReasonSynctestChanSend` | **Yes** | Only bubble goroutines can recv |
| `select` (bubble chans) | `waitReasonSynctestSelect` | **Yes** | Only bubble activity |
| `sync.Cond.Wait` | `waitReasonSyncCondWait` | **Yes** | Signal comes from bubble |
| `sync.WaitGroup.Wait` | `waitReasonSynctestWaitGroupWait` | **Yes** | Done comes from bubble |
| `sync.Mutex.Lock` | `waitReasonSyncMutexLock` | **No** | Any goroutine can unlock |
| `sync.RWMutex` | `waitReasonSyncRWMutex*` | **No** | Any goroutine can unlock |
| Network I/O | `waitReasonIOWait` | **No** | External dependency |
| `<-ch` (non-bubble chan) | `waitReasonChanReceive` | **No** | External send possible |

<details>
<summary>Q: Why is mutex NOT durable?</summary>

A mutex can be unlocked by **ANY** goroutine, not just those in the bubble:

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

**Implication**:
```go
synctest.Test(t, func(t *testing.T) {
    var mu sync.Mutex
    mu.Lock()

    go func() {
        mu.Lock()  // Blocks, but NOT durable
        // ...
    }()

    synctest.Wait()  // Will NOT return!
    // Deadlock: goroutine blocked on mutex,
    // but bubble thinks it's still running
})
```

</details>

<details>
<summary>Q: So on I/O calls we don't decrement running?</summary>

Correct! When a goroutine blocks on network I/O (reading from a socket, etc.), it uses `waitReasonIOWait`, which is **NOT** in the durable list.

This means:
- `bubble.running` does **NOT** decrement
- The bubble stays "active"
- Time does **NOT** advance
- The bubble might eventually deadlock if you're waiting on I/O that never arrives

**Recommendation**: Don't do real I/O in synctest bubbles. Use mocks or interfaces instead.

</details>

<details>
<summary>Q: Why does channel receive have TWO wait reasons?</summary>

There are two different wait reasons:
- `waitReasonChanReceive` - for channels created **outside** the bubble (or nil bubble)
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

</details>

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

Which calls the bubble's tracking function:

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

This is how the bubble knows exactly when to advance time - by tracking every goroutine state transition.

---

## 5. Blocking Scenarios

This section provides complete call graphs for every type of blocking operation in synctest.

### 5.1 time.Sleep

#### Call Graph

```
time.Sleep(duration)
        |
        v
timeSleep(ns int64)                           [time.go]
        |
        +---> gp := getg()
        |
        +---> If gp.bubble != nil:
        |       Add timer to bubble.timers    [NOT P.timers!]
        |   Else:
        |       Add timer to P.timers
        |
        +---> gopark(resetForSleep, nil, waitReasonSleep, ...)
                |
                v
        park_m(gp)                            [proc.go]
                |
                +---> casgstatus(gp, _Grunning, _Gwaiting)
                |       |
                |       +---> bubble.changegstatus(gp, old, new)
                |               |
                |               +---> bubble.running--
                |                   maybeWakeLocked() -> maybe wake root
                |
                +---> schedule()  // Run something else
```

#### Timer Firing

```
Event loop: timers.check(bubble.now)
        |
        v
For each timer where timer.when <= bubble.now:
        |
        +---> Remove from heap
        |
        +---> timer.f(timer.arg)
                |
                +---> goready(sleeping_gp)
                        |
                        v
                casgstatus(gp, _Gwaiting, _Grunnable)
                        |
                        +---> bubble.changegstatus()
                                |
                                +---> bubble.running++

                runqput(pp, gp, next=true)
```

#### Complete State Trace

```
Initial:
    running=2 (G1 main, G2 worker), active=0 (loop parked)
    bubble.now = T0
    bubble.timers = []

G1: time.Sleep(5s)
    |
    +---> bubble.timers = [{when: T0+5s, wake: G1}]
    +---> gopark() -> running-- -> running=1
    +---> maybeWakeLocked() -> running>0, don't wake

G2: time.Sleep(3s)
    |
    +---> bubble.timers = [{when: T0+3s, wake: G2},
    |                      {when: T0+5s, wake: G1}]  (heap ordered)
    +---> gopark() -> running-- -> running=0
    +---> maybeWakeLocked() -> running==0, goready(root)!

Event loop wakes:
    |
    +---> active++ -> active=1
    +---> next = T0+3s
    +---> bubble.now = T0+3s  <-- TIME JUMP!
    +---> timers.check():
    |       G2's timer ready -> goready(G2) -> running++ -> running=1
    |
    +---> gopark(synctestidle_c):
            running==0 && active==1? NO (running=1)
            active-- -> active=0
            Actually park

G2 runs, finishes, exits:
    |
    +---> running-- -> running=0
        maybeWakeLocked() -> goready(root)

Event loop wakes:
    |
    +---> active++ -> active=1
    +---> next = T0+5s
    +---> bubble.now = T0+5s  <-- TIME JUMP!
    +---> timers.check():
    |       G1's timer ready -> goready(G1) -> running=1
    |
    +---> gopark() -> parks (running=1)

G1 runs, finishes...
```

### 5.2 Channel Receive

#### Call Graph: `<-ch` (No Sender Ready)

```
x := <-ch
        |
        v
chanrecv(c *hchan, ep unsafe.Pointer, block bool)     [chan.go:458]
        |
        +---> lock(&c.lock)
        |
        +---> Check bubble isolation:
        |       if c.bubble != nil && getg().bubble != c.bubble {
        |           fatal("recv on synctest channel from outside bubble")
        |       }
        |
        +---> Check for sender in sendq:
        |       sg := c.sendq.dequeue()
        |       if sg != nil -> direct receive (Case 1)
        |
        +---> Check buffer:
        |       if c.qcount > 0 -> receive from buffer (Case 2)
        |
        +---> Must block (Case 3):
                |
                +---> gp := getg()
                +---> mysg := acquireSudog()
                +---> mysg.g = gp
                +---> mysg.elem = ep  // Where to put received value
                +---> c.recvq.enqueue(mysg)
                |
                +---> gopark(chanparkcommit, &c.lock,
                           waitReasonSynctestChanReceive, ...)
                        |
                        v
                park_m(gp):
                        |
                        +---> casgstatus(_Grunning -> _Gwaiting)
                        |       +---> bubble.running--
                        |           maybeWakeLocked()
                        |
                        +---> chanparkcommit():
                        |       unlock(&c.lock)
                        |       return true
                        |
                        +---> schedule()
```

#### Waking: Sender Arrives

```
ch <- value
        |
        v
chansend(c *hchan, ep unsafe.Pointer, block bool)     [chan.go:160]
        |
        +---> lock(&c.lock)
        |
        +---> Check for receiver in recvq:
        |       sg := c.recvq.dequeue()
        |       if sg != nil:
        |           |
        |           +---> recv(c, sg, ep)  // Copy value to receiver
        |           |       +---> memmove(sg.elem, ep, c.elemsize)
        |           |
        |           +---> gp := sg.g
        |           |
        |           +---> goready(gp, 4)
        |                   |
        |                   v
        |               casgstatus(gp, _Gwaiting, _Grunnable)
        |                   +---> bubble.running++
        |               runqput(pp, gp, next=true)
        |
        +---> unlock(&c.lock)
```

### 5.3 Channel Send

#### Call Graph: `ch <- value` (No Receiver Ready, Buffer Full)

```
ch <- value
        |
        v
chansend(c *hchan, ep unsafe.Pointer, block bool)     [chan.go:160]
        |
        +---> lock(&c.lock)
        |
        +---> Check bubble isolation
        |
        +---> Check for receiver in recvq:
        |       if sg != nil -> direct send (Case 1)
        |
        +---> Check buffer space:
        |       if c.qcount < c.dataqsiz -> buffer send (Case 2)
        |
        +---> Must block (Case 3):
                |
                +---> gp := getg()
                +---> mysg := acquireSudog()
                +---> mysg.g = gp
                +---> mysg.elem = ep  // Value to send
                +---> c.sendq.enqueue(mysg)
                |
                +---> gopark(chanparkcommit, &c.lock,
                           waitReasonSynctestChanSend, ...)
                        |
                        +---> bubble.running--
                            maybeWakeLocked()
```

#### Waking: Receiver Arrives

```
x := <-ch
        |
        v
chanrecv():
        |
        +---> sg := c.sendq.dequeue()
        |
        +---> send(c, sg, ep)  // Copy value from sender
        |
        +---> goready(sg.g)
                +---> bubble.running++
```

### 5.4 Select Statement

#### Call Graph: `select` with Multiple Cases

```
select {
case x := <-ch1:
case ch2 <- y:
case <-ch3:
}
        |
        v
selectgo(cas0 *scase, order0 *uint16, ncases int)     [select.go:121]
        |
        +---> Build scases array from cases
        |
        +---> Generate random poll order (ALWAYS random!):
        |       for i := range scases {
        |           j := cheaprandn(uint32(i + 1))  // <-- RANDOMIZATION
        |           pollorder[i], pollorder[j] = pollorder[j], pollorder[i]
        |       }
        |
        +---> Check if all channels are in bubble:
        |       allSynctest := true
        |       for cas := range scases {
        |           if cas.c.bubble != gp.bubble {
        |               allSynctest = false
        |           }
        |       }
        |
        +---> Generate lock order (sorted by channel address):
        |       // Prevents deadlock when locking multiple channels
        |
        +---> Lock all channels in lock order
        |
        +---> Pass 1: Check if any case is ready
        |       for _, casei := range pollorder {
        |           cas := &scases[casei]
        |           if cas.isReady() {
        |               // Found ready case!
        |               unlock all
        |               return casei
        |           }
        |       }
        |
        +---> Pass 2: No case ready, must block
                |
                +---> Create sudog for each case
                +---> Enqueue on each channel's sendq/recvq
                |
                +---> waitReason := waitReasonSelect
                |     if allSynctest {
                |         waitReason = waitReasonSynctestSelect  // Durable!
                |     }
                |
                +---> gopark(selparkcommit, nil, waitReason, ...)
                        |
                        +---> bubble.running--
                            maybeWakeLocked()
```

#### Waking: One Case Becomes Ready

```
Some channel operation (send or recv):
        |
        v
Found our sudog in channel's queue:
        |
        +---> Remove sudog from channel
        |
        +---> sg.success = true
        |
        +---> goready(sg.g)
                |
                v
        bubble.running++

Goroutine wakes in selectgo():
        |
        +---> Determine which case won
        |
        +---> Remove sudogs from all OTHER channels
        |   (we were waiting on multiple)
        |
        +---> Return winning case index
```

<details>
<summary>Q: Why is select randomization important?</summary>

Go's spec requires that when multiple select cases are ready, one is chosen **pseudo-randomly**. This prevents:

1. **Priority inversion**: Without randomization, the first case would always win
2. **Starvation**: Later cases might never be selected
3. **Predictable bugs**: Code that "works" because of deterministic case selection

The randomization uses `cheaprandn()`:
```go
j := cheaprandn(uint32(norder + 1))
pollorder[norder] = pollorder[j]
pollorder[j] = uint16(i)
```

Even inside synctest bubbles, this randomization still applies - synctest does NOT make select deterministic.

</details>

### 5.5 sync.Cond.Wait

#### Call Graph

```
cond.Wait()
        |
        v
func (c *Cond) Wait()                                 [sync/cond.go]
        |
        +---> c.checker.check()  // Verify lock held
        |
        +---> t := runtime_notifyListAdd(&c.notify)
        |
        +---> c.L.Unlock()
        |
        +---> runtime_notifyListWait(&c.notify, t)
                |
                v
        notifyListWait(l *notifyList, t uint32)       [runtime/sema.go]
                |
                +---> gopark(nil, nil,
                           waitReasonSyncCondWait, ...)
                        |
                        +---> bubble.running--
                            maybeWakeLocked()
```

#### Waking: `Signal()` or `Broadcast()`

```
cond.Signal()
        |
        v
runtime_notifyListNotifyOne(&c.notify)
        |
        +---> goready(waiting_gp)
                +---> bubble.running++

cond.Broadcast()
        |
        v
runtime_notifyListNotifyAll(&c.notify)
        |
        +---> for each waiter:
                goready(gp)
                bubble.running++
```

### 5.6 sync.WaitGroup.Wait

#### Call Graph

```
wg.Wait()
        |
        v
func (wg *WaitGroup) Wait()                           [sync/waitgroup.go]
        |
        +---> Check if counter already 0 -> return immediately
        |
        +---> runtime_Semacquire(&wg.sema)
                |
                v
        semacquire1(addr *uint32, ...)                [runtime/sema.go]
                |
                +---> gopark(nil, nil,
                           waitReasonSynctestWaitGroupWait, ...)
                        |
                        +---> bubble.running--
                            maybeWakeLocked()
```

#### Waking: Counter Reaches Zero

```
wg.Done()  // or wg.Add(-1)
        |
        v
func (wg *WaitGroup) Add(delta int)
        |
        +---> Decrement counter
        |
        +---> if counter == 0:
                runtime_Semrelease(&wg.sema)
                        |
                        v
                goready(waiting_gp)
                        +---> bubble.running++
```

### 5.7 NOT Durable: sync.Mutex.Lock

#### Why Mutex Is NOT Durable

A mutex can be unlocked by ANY goroutine, including ones outside the bubble. The wait reason `waitReasonSyncMutexLock` is NOT in `isIdleInSynctest[]`.

If a goroutine blocks on mutex:
- `bubble.running` does NOT decrement
- Bubble stays "active"
- Time does NOT advance

#### Practical Impact

```go
synctest.Test(t, func(t *testing.T) {
    var mu sync.Mutex
    mu.Lock()

    go func() {
        mu.Lock()  // Blocks, but NOT durable
        // ...
    }()

    synctest.Wait()  // Will NOT return!
    // Deadlock: goroutine blocked on mutex,
    // but bubble thinks it's still running
})
```

### 5.8 NOT Durable: Network I/O

#### Why Network Is NOT Durable

Network operations depend on external systems:

```go
conn.Read(buf)  // Waiting for remote server
```

The remote server is outside the bubble, so this isn't durable. The wait reason `waitReasonIOWait` is NOT in `isIdleInSynctest[]`.

#### Recommendation

Don't do real I/O in synctest bubbles. Use mocks or interfaces:

```go
type Reader interface {
    Read([]byte) (int, error)
}

// In tests:
type MockReader struct {
    data []byte
}
func (m *MockReader) Read(b []byte) (int, error) {
    copy(b, m.data)
    return len(m.data), io.EOF
}
```

---

## 6. Timer Management

### Bubble vs P Timers

Normal timers and bubble timers live in different places:

- **Normal timer**: Added to `P.timers` (processor's timer heap)
- **Bubble timer**: Added to `bubble.timers` (bubble's own timer heap)

### Timer Creation in Bubbles

From `go/src/runtime/time.go`:

```go
func timeSleep(ns int64) {
    gp := getg()
    t := gp.timer
    if t == nil {
        t = new(timer)
        t.init(goroutineReady, gp)
        if gp.bubble != nil {
            t.isFake = true  // Mark as fake for bubble goroutines
        }
        gp.timer = t
    }

    var now int64
    if bubble := gp.bubble; bubble != nil {
        now = bubble.now  // Use bubble's fake time
    } else {
        now = nanotime()  // Use real time
    }
    when := now + ns
    // ...
}
```

### The `timers.check()` Function

The event loop calls `bubble.timers.check(bubble.now)` to fire all timers whose deadline has passed:

```go
// Pseudocode
func (ts *timers) check(now int64) {
    for {
        t := ts.heap.peek()
        if t == nil || t.when > now {
            return  // No more timers ready
        }
        ts.heap.pop()
        t.f(t.arg)  // Usually goready(sleeping_gp)
    }
}
```

### The `wakeTime()` Function

Returns the time of the next timer to fire (or 0 if none):

```go
func (ts *timers) wakeTime() int64 {
    if len(ts.heap) == 0 {
        return 0
    }
    return ts.heap[0].when
}
```

This is how the event loop knows how far to advance `bubble.now`.

---

## 7. Integration Points with the Scheduler

### Status Change Hook

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

### gopark and goready

These are the fundamental primitives for putting goroutines to sleep and waking them up:

**gopark** (sleep):
```go
func gopark(unlockf func(*g, unsafe.Pointer) bool, lock unsafe.Pointer, reason waitReason, ...) {
    // ... setup ...
    mcall(park_m)  // Switch to m stack and call park_m
}

func park_m(gp *g) {
    casgstatus(gp, _Grunning, _Gwaiting)  // This triggers bubble tracking!
    // ... more cleanup ...
    schedule()  // Run something else
}
```

**goready** (wake):
```go
func goready(gp *g, traceskip int) {
    systemstack(func() {
        ready(gp, traceskip, true)
    })
}

func ready(gp *g, traceskip int, next bool) {
    casgstatus(gp, _Gwaiting, _Grunnable)  // This triggers bubble tracking!
    runqput(pp, gp, next)
    // ... maybe wake a P ...
}
```

<details>
<summary>Q: What happens if goready is called on a goroutine that's not waiting?</summary>

The `casgstatus` function performs a CAS (compare-and-swap) operation. If the goroutine isn't in `_Gwaiting` state, the CAS fails and the runtime panics with an error message.

This is a safety mechanism - you can't "double wake" a goroutine.

</details>

---

## 8. Complete Trace Examples

### Example 1: Simple Sleep and Channel

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

**Detailed Trace**:

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
       Add timer {when: T0+1s, wake: G2}
       gopark() -> running-- -> running=1
       maybeWakeLocked() -> running>0, don't wake

T=0ms: G1 runs: x := <-ch
       No sender, enqueue in ch.recvq
       gopark(waitReasonSynctestChanReceive)
       running-- -> running=0
       maybeWakeLocked() -> running==0, goready(root)!

T=0ms (real): Event loop wakes
       active++ -> 1
       next = T0+1s
       bubble.now = T0+1s  <-- TIME JUMPS 1 SECOND!
       timers.check():
           G2's timer ready -> goready(G2)
           running++ -> 1
       gopark(synctestidle_c)
           running==0? NO
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

T=0ms (real): G2 finishes
       total-- -> 1 (only root remains)

T=0ms (real): Event loop checks
       total == 1? YES
       No deadlock, success!

Total real time: ~0ms
Total fake time: 1s
```

### Example 2: Multiple Goroutines with Staggered Sleeps

```go
func TestStaggered(t *testing.T) {
    synctest.Test(t, func(t *testing.T) {
        results := make([]int, 0, 3)
        var mu sync.Mutex  // Note: mutex blocks are NOT durable!

        for i := 1; i <= 3; i++ {
            i := i
            go func() {
                time.Sleep(time.Duration(i) * time.Second)
                mu.Lock()
                results = append(results, i)
                mu.Unlock()
            }()
        }

        // Wait for all goroutines via channel
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

**Key observations**:
- Time jumps from 0 -> 1s -> 2s -> 3s -> 4s (four instant jumps)
- The mutex locks between results are NOT durable, but they complete quickly
- Real time elapsed: ~0ms

---

## 9. Deadlock Detection

### When Deadlock Is Detected

Location: `go/src/runtime/synctest.go:237-257`

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

Two types of deadlock:

1. **All blocked**: `bubble.done == false` but `running == 0` and no timers
2. **Main exited with stragglers**: `bubble.done == true` but `total > 1`

### Example: Intentional Deadlock

```go
func TestDeadlock(t *testing.T) {
    defer func() {
        if r := recover(); r != nil {
            t.Log("Caught expected deadlock:", r)
        }
    }()

    synctest.Test(t, func(t *testing.T) {
        ch := make(chan int)
        <-ch  // Blocks forever, no sender
        // PANIC: deadlock: all goroutines in bubble are blocked
    })
}
```

---

## 10. The `cheaprand()` Factor - Remaining Non-Determinism

While synctest solves **time** non-determinism, **schedule** non-determinism remains. The scheduler makes random choices at several points, all using `cheaprand()`.

### Sources of Randomness

1. **runqput**: Random decision on using `runnext` vs regular queue
2. **runqputslow**: Random shuffle when moving goroutines to global queue
3. **runqputbatch**: Random shuffle when inserting batch
4. **stealWork**: Random order for work stealing attempts
5. **select**: Random case ordering (Go spec requirement)

### The `cheaprand()` Function

Location: `go/src/runtime/rand.go:208-251`

```go
func cheaprand() uint32 {
    mp := getg().m
    // wyrand implementation
    mp.cheaprand += 0xa0761d6478bd642f
    hi, lo := math.Mul64(mp.cheaprand, mp.cheaprand^0xe7037ed1a0b428db)
    return uint32(hi ^ lo)
}
```

**Key insight**: This is a per-M (machine/OS thread) PRNG. If we could control its seeding, we could make scheduling deterministic.

<details>
<summary>Q: Could we make synctest fully deterministic?</summary>

Yes, but it would require:

1. **Control cheaprand() seeding**: Modify `mrandinit()` to use deterministic seeds for bubble goroutines
2. **Bubble-aware randomness**: Make `cheaprand()` return deterministic values when the current goroutine is in a bubble
3. **Handle select specially**: The Go spec requires random select case ordering, but for testing we might want deterministic ordering

This is future work - synctest currently only solves time determinism.

</details>

---

## 11. Distributed Synctest - Future Vision

### The Problem with Distributed Systems

Testing distributed systems is even harder than testing concurrent code:

- Multiple processes with independent clocks
- Network delays are unpredictable
- Partial failures are hard to simulate
- Debugging requires correlating events across processes

### Extending Bubbles to Distributed Systems

Conceptually, we could connect multiple bubbles:

```
+------------------+         +------------------+
|   Bubble A       |  RPC    |   Bubble B       |
|   (Service 1)    |<------->|   (Service 2)    |
|   now = T1       |         |   now = T2       |
+------------------+         +------------------+
        |                            |
        +------------+---------------+
                     |
              Global Time Controller
```

### Key Questions

1. **What's the synchronization point?**
   - When does global time advance?
   - How do we handle in-flight RPCs?

2. **Is RPC a durable block?**
   - If the RPC is to another bubble, it could be durable
   - If it's to external service, not durable

3. **Global vs Local Time**
   - Each bubble has `now`
   - Do we need a global coordinator?

<details>
<summary>Q: How would RPC blocking work?</summary>

If both endpoints are in connected bubbles:

```go
// Bubble A
response := client.Call(request)  // This would be durable!

// Bubble B handles the request
// Bubble A's call is unblocked when Bubble B responds
```

The call could be durable because:
- Bubble B is the only thing that can respond
- No external dependency

This is similar to how FoundationDB's simulation testing works.

</details>

<details>
<summary>Q: How would time advance across bubbles?</summary>

Option 1: **Global coordinator**
- All bubbles report "I'm idle at time T"
- Coordinator advances all to `min(next_timer)` across all bubbles

Option 2: **Causal ordering**
- Each RPC carries a vector clock
- Time advances locally, but respects causality

Option 3: **Single logical time**
- All bubbles share one `now` variable
- Requires careful synchronization

</details>

### Prior Art: FoundationDB Simulation Testing

FoundationDB uses a similar approach:
- Entire cluster runs in one process
- All network I/O is mocked
- Time is virtualized
- Faults are injected deterministically

Key differences:
- FDB wrote their own actor framework
- Go has goroutines built-in
- Synctest leverages runtime integration

### Future Work

To enable distributed synctest:

1. **Bubble linking**: Protocol for connecting bubbles
2. **RPC interception**: Mark cross-bubble calls as durable
3. **Global time coordination**: Advance time across all bubbles
4. **Fault injection**: Simulate network partitions, delays
5. **Deterministic network**: Mock all network operations

---

## 12. Summary and Key Takeaways

### What Synctest Controls

1. **Time**: Virtual clock that jumps forward when all goroutines are durably blocked
2. **Durable blocking detection**: Knows when goroutines are truly stuck
3. **Channel isolation**: Prevents outside interference
4. **Deadlock detection**: Automatic panic when stuck

### What Synctest Does NOT Control

1. **Execution order**: Scheduler still decides which goroutine runs
2. **`cheaprand()` randomization**: Still random if `-race` enabled
3. **Mutex blocking**: Not considered durable
4. **External I/O**: Can't virtualize network/disk

### The Core Loop

```
1. Goroutine blocks (gopark)
2. casgstatus changes state
3. bubble.changegstatus() called
4. If durable: bubble.running--
5. If running == 0 && active == 0:
   a. maybeWakeLocked() returns root
   b. Event loop wakes
   c. bubble.now = next timer time
   d. Timers fire
   e. Goroutines wake (goready)
   f. bubble.running++
6. Repeat
```

### Mental Model

Think of a synctest bubble as a **snow globe**:
- Everything inside is isolated
- Time only flows when you shake it (all goroutines blocked)
- Nothing from outside can reach in
- You can see deadlocks (all the snow settled, nothing moving)

---

## 13. References

### Source Files

| File | Description |
|------|-------------|
| `go/src/runtime/synctest.go` | Bubble implementation, event loop |
| `go/src/runtime/runtime2.go` | `synctestBubble`, `g`, `hchan` structs |
| `go/src/runtime/proc.go` | `gopark`, `goready`, `casgstatus` |
| `go/src/runtime/chan.go` | Channel operations, isolation checks |
| `go/src/runtime/select.go` | Select implementation, case randomization |
| `go/src/runtime/time.go` | Timer implementation, fake time |
| `go/src/runtime/sema.go` | Semaphore (WaitGroup, Cond) |
| `go/src/runtime/rand.go` | `cheaprand()`, scheduler randomization |
| `go/src/testing/synctest/synctest.go` | Public API |

### Key Functions

| Function | Location | Purpose |
|----------|----------|---------|
| `synctestRun` | synctest.go | Creates bubble, runs event loop |
| `changegstatus` | synctest.go | Tracks goroutine state changes |
| `maybeWakeLocked` | synctest.go | Determines what to wake when idle |
| `synctestidle_c` | synctest.go | Event loop park callback |
| `isIdleInSynctest` | runtime2.go | Defines durable wait reasons |
| `casgstatus` | proc.go | Status change with bubble hook |
| `gopark` | proc.go | Put goroutine to sleep |
| `goready` | proc.go | Wake goroutine |
| `cheaprand` | rand.go | Scheduler randomization |

### Related Concepts

- **Virtual time**: The technique of advancing a clock only when safe
- **Simulation testing**: Running entire systems with mocked dependencies
- **Deterministic testing**: Reproducing exact execution sequences
- **Actor model**: Message-passing concurrency (similar to goroutines+channels)

---

*This document was created as a comprehensive "rabbithole" exploration of Go's synctest package. It combines source code analysis, conceptual explanations, and future speculation to provide a complete picture of how synctest works and where it might go.*
