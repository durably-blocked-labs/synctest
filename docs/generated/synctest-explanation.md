# Synctest Implementation in Go Runtime

This document explains how synctest works in the Go runtime, covering its implementation, design decisions, and implications for deterministic scheduling.

## What Synctest Solves

Synctest solves **time non-determinism** in concurrent Go programs, not execution order determinism.

When you run concurrent code, time-based operations like `time.Sleep()` or `time.After()` introduce unpredictability in testing. Synctest creates an isolated "bubble" where:

- Time is fake and starts at midnight UTC 2000-01-01
- Time only advances when all goroutines in the bubble are "durably blocked"
- Tests run instantly rather than waiting for real time to pass

**What synctest does NOT solve**: Execution order determinism. Goroutines can still interleave in non-deterministic ways. The scheduler still makes random choices about which goroutine runs next.

Example from `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/testing/synctest/synctest.go` (lines 31-40):

```go
func TestTime(t *testing.T) {
    synctest.Test(t, func(t *testing.T) {
        start := time.Now() // always midnight UTC 2000-01-01
        go func() {
            time.Sleep(1 * time.Second)
            t.Log(time.Since(start)) // always logs "1s"
        }()
        time.Sleep(2 * time.Second) // the goroutine above will run before this Sleep returns
        t.Log(time.Since(start))    // always logs "2s"
    })
}
```

This test runs instantly because fake time advances immediately when all goroutines block.

## Why This Mechanism Exists

### Why Goroutines Sleep At All

When you write code like:
```go
ch := make(chan int)
x := <-ch  // Nothing in the channel yet
```

The goroutine can't proceed because there's no data. It has two choices:
1. **Busy-wait**: Keep checking "is there data yet?" repeatedly (wastes CPU)
2. **Sleep**: Tell the scheduler "wake me when there's data" (efficient)

Go chooses option 2. The goroutine calls `gopark()` to sleep. When another goroutine sends on the channel, it calls `goready()` to wake the sleeping goroutine.

### Why Synctest Needs to Wake Goroutines

Consider this test without synctest:
```go
func TestTimeout(t *testing.T) {
    go func() {
        time.Sleep(10 * time.Second)
        // do something
    }()
    // test waits 10 real seconds
}
```

The goroutine sleeps for 10 real seconds. Your test takes forever. With 1000 such tests, your test suite takes hours.

With synctest:
1. The goroutine calls `time.Sleep(10 * time.Second)`
2. This puts it to sleep with a fake timer
3. Synctest detects "all goroutines are asleep"
4. Synctest advances fake time to 10 seconds
5. Synctest manually wakes the goroutine
6. Test completes instantly

The goroutine doesn't know it's in a bubble. It thinks it slept 10 seconds, but only microseconds of real time passed.

### Why "Durable Blocking" Exists

Synctest needs to answer: **"When is it safe to jump time forward?"**

Bad approach: "When all goroutines are asleep"
```go
mu := sync.Mutex{}
mu.Lock()
go func() {
    mu.Lock()  // Sleeps waiting for mutex
    // ...
}()
// If synctest advanced time here, it would be wrong!
// The goroutine will wake when the mutex is released, not when time advances
```

Good approach: "When all goroutines are **durably blocked**"

Durable blocking means: "This goroutine can ONLY wake up if time advances OR another bubble goroutine wakes it"

Examples:
- ✅ `time.Sleep(1 * time.Second)` - durably blocked (needs time to advance)
- ✅ `<-ch` where ch is in the bubble - durably blocked (needs another bubble goroutine to send)
- ❌ `mu.Lock()` - NOT durably blocked (a currently running goroutine might release it)
- ❌ Network read - NOT durably blocked (OS could respond anytime)

### The Problem Synctest Solves

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

// WITH SYNCTEST: Takes microseconds
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

If you have 1000 tests like this, without synctest they take 100 seconds. With synctest they complete instantly because fake time jumps forward whenever nothing can happen.

## How Synctest Bubbles Work

### Bubble Creation and Structure

A synctest bubble is an isolated execution environment containing goroutines started within `synctest.Run()`. The bubble tracks these goroutines and controls time flow.

**Core struct** (`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/synctest.go`, lines 13-39):

```go
type synctestBubble struct {
    mu      mutex
    timers  timers
    id      uint64 // unique id
    now     int64  // current fake time
    root    *g     // caller of synctest.Run
    waiter  *g     // caller of synctest.Wait
    main    *g     // goroutine started by synctest.Run
    waiting bool   // true if a goroutine is calling synctest.Wait
    done    bool   // true if main has exited

    // The bubble is active (not blocked) so long as running > 0 || active > 0.
    //
    // running is the number of goroutines which are not "durably blocked":
    // Goroutines which are either running, runnable, or non-durably blocked
    // (for example, blocked in a syscall).
    //
    // active is used to keep the bubble from becoming blocked,
    // even if all goroutines in the bubble are blocked.
    total   int // total goroutines
    running int // non-blocked goroutines
    active  int // other sources of activity
}
```

### Goroutine Membership

Every goroutine has a `bubble` field that associates it with a synctest bubble.

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/runtime2.go` (line 575):

```go
type g struct {
    // ... many other fields ...
    bubble  *synctestBubble
    // ... more fields ...
}
```

When a goroutine spawns another goroutine inside a bubble, the child inherits the bubble membership. All goroutines in the bubble share the same fake time and isolation.

### Durable Blocking

The key concept in synctest is "durable blocking" - a goroutine is durably blocked when it can ONLY be unblocked by another goroutine in the same bubble.

**Operations that durably block** (`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/runtime2.go`, lines 1385-1398):

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
}
```

These wait reasons mark a goroutine as "idle in synctest" - durably blocked. When all goroutines have `running == 0` and `active == 0`, the bubble advances time or wakes the waiter.

**Operations that do NOT durably block**:
- Mutex locks (could be unlocked from outside the bubble)
- Network I/O (data could arrive from outside)
- Syscalls (OS can respond at any time)

### Channel Association

Channels created inside a bubble are associated with that bubble. This prevents goroutines outside the bubble from interfering.

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/chan.go` (lines 34-55):

```go
type hchan struct {
    qcount   uint           // total data in the queue
    dataqsiz uint           // size of the circular queue
    buf      unsafe.Pointer // points to an array of dataqsiz elements
    elemsize uint16
    closed   uint32
    timer    *timer // timer feeding this chan
    elemtype *_type // element type
    sendx    uint   // send index
    recvx    uint   // receive index
    recvq    waitq  // list of recv waiters
    sendq    waitq  // list of send waiters
    bubble   *synctestBubble  // <-- Associates channel with bubble

    lock mutex
}
```

Channels inherit the bubble when created (lines 116-118):

```go
if b := getg().bubble; b != nil {
    c.bubble = b
}
```

When a goroutine performs a select operation, synctest checks if all channels are in the bubble (`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/select.go`, lines 179-204):

```go
allSynctest := true
for i := range scases {
    cas := &scases[i]

    // Omit cases without channels from the poll and lock orders.
    if cas.c == nil {
        cas.elem = nil // allow GC
        continue
    }

    if cas.c.bubble != nil {
        if getg().bubble != cas.c.bubble {
            fatal("select on synctest channel from outside bubble")
        }
    } else {
        allSynctest = false
    }
    // ...
}

waitReason := waitReasonSelect
if gp.bubble != nil && allSynctest {
    // Every channel selected on is in a synctest bubble,
    // so this goroutine will count as idle while selecting.
    waitReason = waitReasonSynctestSelect
}
```

### Fake Time

Timers in a bubble use fake time instead of real time.

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/time.go` (lines 55-62):

```go
type timer struct {
    mu mutex

    astate atomic.Uint8 // atomic copy of state bits at last unlock
    state  uint8        // state bits
    isChan bool         // timer has a channel; immutable; can be read without lock
    isFake bool         // timer is using fake time; immutable; can be read without lock
    // ...
}
```

When `time.Sleep()` is called in a bubble (lines 330-360):

```go
func timeSleep(ns int64) {
    if ns <= 0 {
        return
    }

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
    if when < 0 { // check for overflow.
        when = maxWhen
    }
    gp.sleepWhen = when
    if t.isFake {
        // Call timer.reset in this goroutine, since it's the one in a bubble.
        // We don't need to worry about the timer function running before the goroutine
        // is parked, because time won't advance until we park.
        resetForSleep(gp, nil)
    }
    // ... park the goroutine ...
}
```

The bubble maintains its own timer heap and advances `bubble.now` when all goroutines are durably blocked.

## Runtime Implementation Details

### Key Files

1. **`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/synctest.go`** - Core bubble implementation
2. **`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/runtime2.go`** - Struct definitions (g, hchan, waitReason)
3. **`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/proc.go`** - Scheduler integration
4. **`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/chan.go`** - Channel operations
5. **`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/select.go`** - Select statement handling
6. **`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/time.go`** - Timer and sleep operations
7. **`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/internal/synctest/synctest.go`** - Public API
8. **`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/testing/synctest/synctest.go`** - Testing integration

### Status Tracking

The bubble tracks goroutine status changes to determine when all goroutines are durably blocked. This happens in `changegstatus()`.

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/synctest.go` (lines 41-107):

```go
func (bubble *synctestBubble) changegstatus(gp *g, oldval, newval uint32) {
    // Determine whether this change in status affects the idleness of the bubble.
    totalDelta := 0
    wasRunning := true
    switch oldval {
    case _Gdead, _Gdeadextra:
        wasRunning = false
        totalDelta++
    case _Gwaiting:
        if gp.waitreason.isIdleInSynctest() {
            wasRunning = false
        }
    }
    isRunning := true
    switch newval {
    case _Gdead, _Gdeadextra:
        isRunning = false
        totalDelta--
        if gp == bubble.main {
            bubble.done = true
        }
    case _Gwaiting:
        if gp.waitreason.isIdleInSynctest() {
            isRunning = false
        }
    }

    if wasRunning == isRunning && totalDelta == 0 {
        return
    }

    lock(&bubble.mu)
    bubble.total += totalDelta
    if wasRunning != isRunning {
        if isRunning {
            bubble.running++
        } else {
            bubble.running--
            if raceenabled && newval != _Gdead && newval != _Gdeadextra {
                racereleasemergeg(gp, bubble.raceaddr())
            }
        }
    }
    if bubble.total < 0 {
        fatal("total < 0")
    }
    if bubble.running < 0 {
        fatal("running < 0")
    }
    wake := bubble.maybeWakeLocked()
    unlock(&bubble.mu)
    if wake != nil {
        goready(wake, 0)
    }
}
```

This function is called from `casgstatus()` whenever a goroutine's status changes (`/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/proc.go`, lines 1316-1320):

```go
if gp.bubble != nil {
    systemstack(func() {
        gp.bubble.changegstatus(gp, oldval, newval)
    })
}
```

### Wake Logic

When `running == 0` and `active == 0`, the bubble decides what to wake:

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/synctest.go` (lines 131-158):

```go
func (bubble *synctestBubble) maybeWakeLocked() *g {
    if bubble.running > 0 || bubble.active > 0 {
        return nil
    }
    // Increment the bubble active count, since we've determined to wake something.
    // The woken goroutine will decrement the count.
    bubble.active++
    next := bubble.timers.wakeTime()
    if next > 0 && next <= bubble.now {
        // A timer is scheduled to fire. Wake the root goroutine to handle it.
        return bubble.root
    }
    if gp := bubble.waiter; gp != nil {
        // A goroutine is blocked in Wait. Wake it.
        return gp
    }
    // All goroutines in the bubble are durably blocked, and nothing has called Wait.
    // Wake the root goroutine.
    return bubble.root
}
```

The priority is:
1. Fire timers that should have fired (advance time)
2. Wake the waiter if `synctest.Wait()` was called
3. Wake the root goroutine (likely a deadlock)

### Bubble Run Loop

The main execution loop for a bubble is in `synctestRun()`:

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/synctest.go` (lines 170-258):

```go
func synctestRun(f func()) {
    gp := getg()
    if gp.bubble != nil {
        panic("synctest.Run called from within a synctest bubble")
    }
    bubble := &synctestBubble{
        id:      bubbleGen.Add(1),
        total:   1,
        running: 1,
        root:    gp,
    }
    const synctestBaseTime = 946684800000000000 // midnight UTC 2000-01-01
    bubble.now = synctestBaseTime
    lockInit(&bubble.mu, lockRankSynctest)
    lockInit(&bubble.timers.mu, lockRankTimers)

    gp.bubble = bubble
    defer func() {
        gp.bubble = nil
    }()

    // Create the main goroutine for the bubble
    pc := sys.GetCallerPC()
    systemstack(func() {
        fv := *(**funcval)(unsafe.Pointer(&f))
        bubble.main = newproc1(fv, gp, pc, false, waitReasonZero)
        pp := getg().m.p.ptr()
        runqput(pp, bubble.main, true)
        wakep()
    })

    lock(&bubble.mu)
    bubble.active++
    for {
        unlock(&bubble.mu)
        systemstack(func() {
            // Clear gp.m.curg while running timers
            curg := gp.m.curg
            gp.m.curg = nil
            gp.bubble.timers.check(bubble.now, bubble)
            gp.m.curg = curg
        })
        gopark(synctestidle_c, nil, waitReasonSynctestRun, traceBlockSynctest, 0)
        lock(&bubble.mu)
        if bubble.active < 0 {
            throw("active < 0")
        }
        next := bubble.timers.wakeTime()
        if next == 0 {
            break
        }
        if next < bubble.now {
            throw("time went backwards")
        }
        if bubble.done {
            // Time stops once the bubble's main goroutine has exited.
            break
        }
        bubble.now = next  // Advance fake time
    }

    total := bubble.total
    unlock(&bubble.mu)
    if raceenabled {
        raceacquireg(gp, gp.bubble.raceaddr())
    }
    if total != 1 {
        var reason string
        if bubble.done {
            reason = "deadlock: main bubble goroutine has exited but blocked goroutines remain"
        } else {
            reason = "deadlock: all goroutines in bubble are blocked"
        }
        panic(synctestDeadlockError{reason: reason, bubble: bubble})
    }
}
```

## Scheduler Decision Points and Non-Determinism

While synctest solves time non-determinism, execution order remains non-deterministic. The Go scheduler makes random choices at several key points.

### 1. runqput - Adding Goroutines to Run Queue

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/proc.go` (lines 7478-7519):

```go
func runqput(pp *p, gp *g, next bool) {
    if !haveSysmon && next {
        next = false
    }
    if randomizeScheduler && next && randn(2) == 0 {
        next = false  // Randomly decide not to use runnext
    }

    if next {
    retryNext:
        oldnext := pp.runnext
        if !pp.runnext.cas(oldnext, guintptr(unsafe.Pointer(gp))) {
            goto retryNext
        }
        if oldnext == 0 {
            return
        }
        // Kick the old runnext out to the regular run queue.
        gp = oldnext.ptr()
    }

retry:
    h := atomic.LoadAcq(&pp.runqhead)
    t := pp.runqtail
    if t-h < uint32(len(pp.runq)) {
        pp.runq[t%uint32(len(pp.runq))].set(gp)
        atomic.StoreRel(&pp.runqtail, t+1)
        return
    }
    if runqputslow(pp, gp, h, t) {
        return
    }
    goto retry
}
```

**Non-determinism**: When `randomizeScheduler` is true (which happens when race detector is enabled), the scheduler randomly decides whether to put a goroutine in the `runnext` slot (high priority, runs immediately) or the regular queue.

### 2. runqputslow - Shuffling When Queue is Full

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/proc.go` (lines 7541-7546):

```go
if randomizeScheduler {
    for i := uint32(1); i <= n; i++ {
        j := cheaprandn(i + 1)
        batch[i], batch[j] = batch[j], batch[i]
    }
}
```

**Non-determinism**: When moving goroutines from the local queue to the global queue, the order is randomized using `cheaprandn()`.

### 3. runqputbatch - Batch Queue Insertion

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/proc.go` (lines 7579-7587):

```go
if randomizeScheduler {
    off := func(o uint32) uint32 {
        return (pp.runqtail + o) % uint32(len(pp.runq))
    }
    for i := uint32(1); i < n; i++ {
        j := cheaprandn(i + 1)
        pp.runq[off(i)], pp.runq[off(j)] = pp.runq[off(j)], pp.runq[off(i)]
    }
}
```

**Non-determinism**: When inserting a batch of goroutines, the order is shuffled.

### 4. runqget - Retrieving Goroutines

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/proc.go` (lines 7598-7619):

```go
func runqget(pp *p) (gp *g, inheritTime bool) {
    // If there's a runnext, it's the next G to run.
    next := pp.runnext
    if next != 0 && pp.runnext.cas(next, 0) {
        return next.ptr(), true
    }

    for {
        h := atomic.LoadAcq(&pp.runqhead)
        t := pp.runqtail
        if t == h {
            return nil, false
        }
        gp := pp.runq[h%uint32(len(pp.runq))].ptr()
        if atomic.CasRel(&pp.runqhead, h, h+1) {
            return gp, false
        }
    }
}
```

**Non-determinism**: The scheduler always checks `runnext` first, but which goroutine ends up in `runnext` depends on the random decisions in `runqput()`.

### 5. stealWork - Work Stealing

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/proc.go` (lines 3837):

```go
for enum := stealOrder.start(cheaprand()); !enum.done(); enum.next() {
```

**Non-determinism**: When a processor has no work, it steals from other processors. The order in which it attempts to steal is randomized using `cheaprand()`.

### 6. select Statement - Case Ordering

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/select.go` (lines 191-194):

```go
j := cheaprandn(uint32(norder + 1))
pollorder[norder] = pollorder[j]
pollorder[j] = uint16(i)
norder++
```

**Non-determinism**: Select cases are evaluated in random order (as required by Go spec). The randomization uses `cheaprandn()`.

### randomizeScheduler Flag

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/proc.go` (line 7471):

```go
const randomizeScheduler = raceenabled
```

This constant is true when the race detector is enabled. The comment explains (lines 7458-7470):

```go
// Randomness in scheduling:
//
// The race detector and some other tools benefit from non-deterministic
// scheduling to help uncover bugs. The Go scheduler is already non-deterministic,
// but a common bug pattern is two goroutines racing on a variable
// without synchronization that accidentally almost always work correctly
// when tested. To help flush out such bugs, when the race detector is enabled,
// the runtime adds randomness in several places in the scheduler.
//
// With the randomness here, as long as the tests pass
// consistently with -race, they shouldn't have latent scheduling
// assumptions.
```

## The cheaprand() Function - Key to Determinism

All the scheduler randomization uses `cheaprand()`, making it the single point of non-determinism.

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/rand.go` (lines 208-251):

```go
// cheaprand is a non-cryptographic-quality 32-bit random generator
// suitable for calling at very high frequency (such as during scheduling decisions)
// and at sensitive moments in the runtime (such as during stack unwinding).
// it is "cheap" in the sense of both expense and quality.
//
//go:nosplit
func cheaprand() uint32 {
    mp := getg().m
    // Implement wyrand: https://github.com/wangyi-fudan/wyhash
    // Only the platform that math.Mul64 can be lowered
    // by the compiler should be in this list.
    if goarch.IsAmd64|goarch.IsArm64|goarch.IsPpc64|
        goarch.IsPpc64le|goarch.IsMips64|goarch.IsMips64le|
        goarch.IsS390x|goarch.IsRiscv64|goarch.IsLoong64 == 1 {
        mp.cheaprand += 0xa0761d6478bd642f
        hi, lo := math.Mul64(mp.cheaprand, mp.cheaprand^0xe7037ed1a0b428db)
        return uint32(hi ^ lo)
    }

    // Implement xorshift64+: 2 32-bit xorshift sequences added together.
    t := (*[2]uint32)(unsafe.Pointer(&mp.cheaprand))
    s1, s0 := t[0], t[1]
    s1 ^= s1 << 17
    s1 = s1 ^ s0 ^ s1>>7 ^ s0>>16
    t[0], t[1] = s0, s1
    return s0 + s1
}
```

**Key insight**: `cheaprand()` is a per-M (machine/OS thread) PRNG that uses `mp.cheaprand` as state. This state is initialized in `mrandinit()`:

From `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/rand.go` (lines 187-196):

```go
func mrandinit(mp *m) {
    var seed [4]uint64
    for i := range seed {
        seed[i] = bootstrapRand()
    }
    bootstrapRandReseed() // erase key we just extracted
    mp.chacha8.Init64(seed)
    mp.cheaprand = rand()  // Initialize cheaprand state from cryptographic PRNG
}
```

**Path to determinism**:

1. **Single point of randomness**: All scheduler decisions use `cheaprand()` or `cheaprandn()`
2. **Per-M state**: Each OS thread has its own PRNG state in `mp.cheaprand`
3. **Controlled seeding**: If we could control the seed, we could make scheduling deterministic

**Making it deterministic would require**:

1. Seeding `mp.cheaprand` with a deterministic value (e.g., based on bubble ID)
2. Ensuring `mrandinit()` uses deterministic seeds for bubbled Ms
3. Potentially disabling or controlling other sources of non-determinism (timer resolution, syscalls, etc.)

The function is marked `//go:nosplit` because it's called in sensitive runtime contexts where stack splits aren't safe.

## Non-Determinism Summary

Current sources of execution order non-determinism in synctest:

1. **runqput**: Random decision on using `runnext` vs regular queue
2. **runqputslow**: Random shuffle when moving goroutines to global queue
3. **runqputbatch**: Random shuffle when inserting batch
4. **stealWork**: Random order for work stealing attempts
5. **select**: Random case ordering (required by Go spec)
6. **cheaprand**: Per-M PRNG with non-deterministic seed

All of these use `cheaprand()` or `cheaprandn()`, making `cheaprand()` the single control point for achieving deterministic scheduling.

## Next Steps for Deterministic Scheduling

To make synctest provide execution order determinism:

1. **Control cheaprand() seeding**: Modify `mrandinit()` to use deterministic seeds when in a bubble
2. **Bubble-aware randomness**: Make `cheaprand()` return deterministic values for goroutines in bubbles
3. **Disable randomizeScheduler**: Or make it no-op within bubbles
4. **Handle select randomization**: May need special handling since randomness is part of Go spec
5. **Test extensively**: Ensure all race conditions surface with fewer random schedules

The synctest infrastructure provides most of what's needed - isolation, time control, and tracking. Adding execution determinism is primarily about controlling `cheaprand()`.

## References

- Synctest public API: `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/testing/synctest/synctest.go`
- Runtime implementation: `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/synctest.go`
- Scheduler: `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/proc.go`
- Random number generation: `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/rand.go`
- Goroutine struct: `/Users/shubhaankar/github.com/Shubhaankar-Sharma/synctest/go/src/runtime/runtime2.go`
