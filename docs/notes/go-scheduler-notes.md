# Notes on Go Scheduler

Go has a concept of M:P:G - three abstractions that coordinate concurrent execution.

```
                 Global Scheduler State (schedt)
                 - Global runnable queue
                 - Idle M and P lists
                         |
                         v
      P (Processor) - one per GOMAXPROCS
      - Local runnable queue [256]G
      - runnext (VIP slot)
      - Timer heap
                         |
                         v
      M (Machine/OS Thread)
      - g0 (scheduler goroutine)
      - curg (current user G)
      - cheaprand (per-M random state)
                         |
                         v
      G (Goroutine)
      - User code stack
      - Execution state
```

---

## M -> Machine

Every machine is mapped to an OS thread.

```go
// https://github.com/golang/go/blob/master/src/runtime/runtime2.go#L618
type m struct {
    g0      *g        // scheduler/system goroutine (large fixed stack)
    curg    *g        // current user goroutine
    p       puintptr  // associated P (nil if not executing Go code)
    nextp   puintptr  // P to attach on wakeup
    oldp    puintptr  // P that was attached before syscall
    spinning bool     // actively looking for work

    // Random number generator - KEY FOR DETERMINISM
    cheaprand   uint32    // fast pseudo-random state (wyrand algorithm)
    cheaprand64 uint64    // 64-bit version

    // Thread identity
    thread uintptr        // OS thread handle

    // Preemption
    preemptoff string     // if != "", keep curg running on this M
}
```

The global scheduler tracks all Ms in a linked list:

```go
// https://github.com/golang/go/blob/master/src/runtime/proc.go
var (
    allm       *m        // Linked list of all M's
    gomaxprocs int32     // Number of P's
    ncpu       int32     // Number of CPUs
    sched      schedt    // Global scheduler state
    allp       []*p      // All P's, len == gomaxprocs
)
```

Ms are created on demand:
- When there's work but no idle M
- When an M enters a blocking syscall (new M created to keep P busy)

---

## P -> Processor

A virtual processor. Holds the execution state and resources needed to execute Go code.

**Key constraints:**
- A machine with no P can't execute Go code
- A P with no machine can't run its queue
- `GOMAXPROCS` controls max P count (defaults to CPU count)

```go
// https://github.com/golang/go/blob/master/src/runtime/runtime2.go#L773
type p struct {
    id          int32
    status      uint32    // _Pidle, _Prunning, _Pgcstop, _Pdead
    m           muintptr  // back-link to M (nil if idle)

    // Run queue management
    runqhead    uint32
    runqtail    uint32
    runq        [256]guintptr  // circular buffer of runnable Gs
    runnext     guintptr       // VIP slot - bypasses queue, not scanned by GC

    // Timers
    timers      timers         // heap of timers for this P

    // Memory allocation cache
    mcache      *mcache        // per-P cache for small allocations

    // Scheduling metadata
    schedtick   uint32         // incremented on every scheduler call

    // GC state
    gcBgMarkWorker guintptr
    gcw            gcWork
}
```

Ps are created at startup by `procresize()` - exactly `GOMAXPROCS` of them. They're stored in `allp` slice and reused, never freed during normal execution.

<details>
<summary>Why is p stored as puintptr instead of *p?</summary>

```go
type puintptr uintptr

func (pp puintptr) ptr() *p {
    return (*p)(unsafe.Pointer(pp))
}
```

It's stored as `uintptr` (raw integer) instead of `*p` (pointer) to **hide it from the garbage collector**. The GC doesn't scan `uintptr` fields, which is important for runtime internals.

This is why you see patterns like `getg().m.p.ptr()` - the `.ptr()` converts back to a usable pointer.

</details>

---

## G -> Goroutine

A lightweight user-space thread. Multiple goroutines can run on a single OS thread.

```go
// https://github.com/golang/go/blob/master/src/runtime/runtime2.go#L473
type g struct {
    // Stack management
    stack       stack   // [lo, hi) memory range - LIVES ON HEAP
    stackguard0 uintptr // checked on function prologue for preemption/growth
    stackguard1 uintptr // checked during stack growth

    // Scheduling
    m           *m      // current M (nil if not running)
    sched       gobuf   // saved CPU registers for context switch

    // Execution state
    atomicstatus atomic.Uint32  // _Gidle, _Grunnable, _Grunning, _Gwaiting, _Gdead

    // Goroutine identity
    goid        uint64    // unique goroutine ID
    gopc        uintptr   // PC of 'go' statement that created this G

    // Queue linkage
    schedlink   guintptr  // next G in runnable queue

    // Waiting state
    waitreason  waitReason

    // Synctest bubble association
    bubble      *synctestBubble
}
```

**G Status Constants** ([runtime2.go:17-120](https://github.com/golang/go/blob/master/src/runtime/runtime2.go#L17)):
- `_Gidle` (0): Just allocated, not initialized
- `_Grunnable` (1): On a run queue, ready to execute
- `_Grunning` (2): Currently executing user code
- `_Gsyscall` (3): In a syscall
- `_Gwaiting` (4): Blocked (channel, lock, timer, etc.)
- `_Gdead` (6): Unused, on free list for reuse

---

## Goroutine Lifecycle

When we call `go myfunc()`:

### newproc(&fn)

[proc.go:5295](https://github.com/golang/go/blob/master/src/runtime/proc.go#L5295)

```go
func newproc(fn *funcval) {
    gp := getg()
    pc := sys.GetCallerPC()
    systemstack(func() {
        newg := newproc1(fn, gp, pc, false, waitReasonZero)

        pp := getg().m.p.ptr()
        runqput(pp, newg, true)  // Add to run queue

        if mainStarted {
            wakep()  // Wake idle P to potentially steal this work
        }
    })
}
```

**Step by step:**

1. **getg()**: Returns pointer to current goroutine (stored in CPU register/TLS)

   ```
     CPU
     Registers:
     +-- RAX, RBX, RCX... (general purpose)
     +-- TLS / special register --> current *g
                                       ^
                                       |
                                 getg() reads this
   ```

2. **pc (program counter)**: Captures "where was `go` called from?" for stack traces. The symbol table maps this address to source file + line number.

3. **systemstack(func() { ... })**:
   - Runs function on g0 of current M
   - M has two goroutines:
     - **g0**: system G, large fixed 8KB+ stack for scheduling/GC/runtime ops
     - **curg**: user G, small 2KB growable stack
   - This solves the chicken-and-egg problem for stack growth

<details>
<summary>Deep dive: Stack growth and the chicken-egg problem</summary>

### Every M has two stacks

```
  M (OS Thread)

  +------------------+    +------------------+
  | g0 (system G)    |    | curg (user G)    |
  |                  |    |                  |
  | Stack: 8KB+      |    | Stack: 2KB       |
  | (large, fixed)   |    | (small, grows)   |
  |                  |    |                  |
  | Used for:        |    | Used for:        |
  | - Scheduling     |    | - Your code      |
  | - GC             |    | - Your funcs     |
  | - Stack growth   |    |                  |
  | - Runtime ops    |    |                  |
  +------------------+    +------------------+
```

### Why two stacks?

User goroutine stacks start small (2KB) and can grow dynamically:

```go
func recursive(n int) {
    var buf [1024]byte  // Need more stack!
    recursive(n - 1)
}
```

But **stack growth itself needs stack space** to run! The grow operation must:
1. Allocate new memory
2. Copy everything over
3. Fix all pointers
4. Free old stack

### The chicken-and-egg problem

```
Your stack is FULL
       |
       v
Need to call growStack()
       |
       v
But growStack() needs stack space for:
  - Local variables
  - Function call frames
  - malloc() internals
       |
       v
But your stack is FULL!
       |
       v
    CRASH
```

### The solution: g0's stack

```
User G stack (FULL):          g0 stack (BIG, SAFE):
+-------------+               +-------------+
| FULL        |               |             |
| FULL        |               |  Plenty of  |
| FULL        |               |  room here! |
+-------------+               |             |
       |                      |             |
       | switch to ---------> |             |
       |                      +-------------+
                                    |
                                    v
                             growStack() runs safely
                                    |
                                    v
                             User G now has bigger stack
                                    |
                             switch back
                                    |
                                    v
                             User G continues
```

### Where do goroutine stacks actually live?

**User goroutine stacks live on the HEAP.** This is key to understanding Go.

```
Traditional threads (C/pthreads):    Go goroutines:

  +----------+  Fixed, OS-managed    +----------+
  | Thread 1 |  1-8MB each           | g0 stack |  Only this is "real"
  |  Stack   |  Can't grow!          | (per M)  |  OS stack
  +----------+                       +----------+

  +----------+                       +----------------------------+
  | Thread 2 |                       |          HEAP              |
  |  Stack   |                       |   +------+  +------+       |
  +----------+                       |   | G1   |  | G2   |       |
                                     |   |stack |  |stack |  ...  |
  +----------------------------+     |   | 2KB  |  | 8KB  |       |
  |          HEAP              |     |   +------+  +------+       |
  +----------------------------+     +----------------------------+
```

This is why Go can have millions of goroutines - they're just small heap allocations, not OS threads with huge fixed stacks.

**Stack limits:**
- g0: ~8KB (Unix) or ~64KB (Windows), fixed, never grows
- User G: Starts at 2KB, can grow up to 1GB (64-bit systems)

</details>

Inside systemstack, the work happens:

a. **`newg := newproc1(fn, gp, pc, false, ...)`** - reuse dead G or allocate new one with stack
b. **`pp := getg().m.p.ptr()`** - get current P (we're on g0 now)
c. **`runqput(pp, newg, true)`** - add to P's runq, try runnext first
d. **`if mainStarted { wakep() }`** - wake idle P to potentially steal this work

---

## runqput

[proc.go:7478](https://github.com/golang/go/blob/master/src/runtime/proc.go#L7478)

Adds goroutine to P's run queue:

```go
func runqput(pp *p, gp *g, next bool) {
    // RANDOMIZATION POINT #1
    if randomizeScheduler && next && randn(2) == 0 {
        next = false  // 50% chance: skip runnext
    }

    if next {
        // Try to place in runnext (VIP slot)
        oldnext := pp.runnext
        if !pp.runnext.cas(oldnext, guintptr(unsafe.Pointer(gp))) {
            goto retryNext
        }
        if oldnext == 0 {
            return  // runnext was empty, we're done
        }
        gp = oldnext.ptr()  // Evict old runnext to regular queue
    }

    // Add to regular run queue (circular buffer)
    h := atomic.LoadAcq(&pp.runqhead)
    t := pp.runqtail
    if t-h < uint32(len(pp.runq)) {
        pp.runq[t%uint32(len(pp.runq))].set(gp)
        atomic.StoreRel(&pp.runqtail, t+1)
        return
    }

    // Queue full (256), spill to global
    runqputslow(pp, gp, h, t)
}
```

**Logic:**
- If `next=true` and runnext empty -> use runnext slot
- Else -> shift old runnext to queue tail, put new G in runnext
- If queue full (256) -> `runqputslow()`

<!-- VERIFIED: runnext shifting always succeeds. The CAS in runqput has a retry loop
     (retryNext at line 7497) that guarantees eventual success. Once the CAS succeeds
     and oldnext is non-zero, the evicted goroutine falls through to be added to the
     regular runq, which also has a retry loop if momentarily full. -->

<details>
<summary>What is runnext and why does it exist?</summary>

`runnext` is a single slot that bypasses the FIFO queue:

```
Normal queue (FIFO):
+------------------------------------+
| runq: [G1] [G2] [G3] [G4]          |
|        ^                     ^     |
|       head                  tail   |
|       (next out)       (new go in) |
+------------------------------------+

New G goes to back -> waits behind everyone

With runnext:
+------------------------------------+
| runnext: [G5]  <-- CUTS THE LINE   |
| runq: [G1] [G2] [G3] [G4]          |
+------------------------------------+

G5 runs BEFORE G1, G2, G3, G4
```

**Why this optimization?**

1. **Cache locality**: If you do `data := prepare(); go process(data)`, the new goroutine runs while data is still hot in cache.

2. **Producer-consumer is instant**: `go func() { ch <- 42 }()` puts producer in runnext, it runs immediately.

3. **Time slice inheritance**: G in runnext inherits remaining time slice - no scheduler overhead.

**The tradeoff**: runnext = UNFAIR but FAST. That's why `-race` mode randomizes it with 50% probability.

</details>

### runqputslow

[proc.go:7524](https://github.com/golang/go/blob/master/src/runtime/proc.go#L7524)

When local queue is full:
- Takes 128 from P's queue + 1 new = 129 goroutines
- Shuffles if `-race` flag (Fisher-Yates shuffle)
- Moves batch to global queue

```go
func runqputslow(pp *p, gp *g, h, t uint32) bool {
    var batch [len(pp.runq)/2 + 1]*g
    n := t - h
    n = n / 2  // Take half (128)

    for i := uint32(0); i < n; i++ {
        batch[i] = pp.runq[(h+i)%uint32(len(pp.runq))].ptr()
    }
    batch[n] = gp  // Plus the new one

    // RANDOMIZATION POINT #2 - Fisher-Yates shuffle
    if randomizeScheduler {
        for i := uint32(1); i <= n; i++ {
            j := cheaprandn(i + 1)
            batch[i], batch[j] = batch[j], batch[i]
        }
    }

    // Move to global queue
    lock(&sched.lock)
    globrunqputbatch(&batch, n+1)
    unlock(&sched.lock)
    return true
}
```

---

## schedule()

[proc.go:4135](https://github.com/golang/go/blob/master/src/runtime/proc.go#L4135)

The main scheduling loop. Runs on g0.

```go
func schedule() {
    mp := getg().m

top:
    pp := mp.p.ptr()

    // Find next runnable goroutine
    gp, inheritTime, tryWakeP := findRunnable()

    // Execute the goroutine
    execute(gp, inheritTime)
}
```

### findRunnable()

[proc.go:3389](https://github.com/golang/go/blob/master/src/runtime/proc.go#L3389)

Priority order for finding work:

1. **Every 61 ticks**: `globrunqget()` - check global queue for fairness
   <!-- VERIFIED: 61 is a hardcoded prime constant, entirely deterministic (not random).
        schedtick increments once per goroutine scheduling decision. The prime number
        prevents harmonic resonance patterns where two interacting goroutines could
        perpetually avoid triggering global queue fairness checks. -->

2. **runqget()**: Get from local queue (check runnext first, then runq)

3. **globrunqget()**: Check global again if local was empty

4. **netpoll()**: Check network poller for I/O-ready goroutines
   - Go parks goroutines on I/O calls (file read, network recv)
   - netpoll + sysmon marks G as ready when data available
   - Non-blocking check here

5. **stealWork()**: Steal from other Ps (random order!)
   - With GOMAXPROCS=1: no stealing possible (nothing to steal from)
   - With GOMAXPROCS>1: must control this randomness for determinism

6. **Idle-time GC work**: Help with garbage collection if nothing else to do

7. **Park and wait**: No work anywhere, go to sleep

### execute(gp, inheritTime)

[proc.go:3331](https://github.com/golang/go/blob/master/src/runtime/proc.go#L3331)

Actually runs a goroutine:

```go
func execute(gp *g, inheritTime bool) {
    mp := getg().m

    // Link M and G together
    mp.curg = gp
    gp.m = mp

    // Transition G state: _Grunnable -> _Grunning
    casgstatus(gp, _Grunnable, _Grunning)

    // Reset tracking
    gp.waitsince = 0
    gp.preempt = false

    if !inheritTime {
        mp.p.ptr().schedtick++  // Fresh time slice
    }

    // Jump to goroutine code (assembly, never returns)
    gogo(&gp.sched)
}
```

`gogo()` is assembly that restores the saved registers from `gp.sched` and jumps to the goroutine's code. It never returns - when the goroutine yields/blocks, it calls back into the scheduler via `mcall()`.

---

## Randomization Points

The Go scheduler uses `cheaprand()` at 5 key points. **NOT all are controlled by `randomizeScheduler`!**

| # | Location | Function | What it randomizes | Guarded? |
|---|----------|----------|-------------------|----------|
| 1 | [proc.go:7490](https://github.com/golang/go/blob/master/src/runtime/proc.go#L7490) | `runqput()` | Whether to use runnext or regular queue | **YES** - only with `-race` |
| 2 | [proc.go:7541](https://github.com/golang/go/blob/master/src/runtime/proc.go#L7541) | `runqputslow()` | Shuffle batch going to global queue | **YES** - only with `-race` |
| 3 | [proc.go:3837](https://github.com/golang/go/blob/master/src/runtime/proc.go#L3837) | `stealWork()` | Order of Ps to steal from | **NO** - ALWAYS random |
| 4 | [proc.go:7579](https://github.com/golang/go/blob/master/src/runtime/proc.go#L7579) | `runqputbatch()` | Shuffle batch from global queue | **YES** - only with `-race` |
| 5 | [select.go:191](https://github.com/golang/go/blob/master/src/runtime/select.go#L191) | `selectgo()` | Order of select cases checked | **NO** - ALWAYS random |

**Critical insight:**
- `stealWork()` and `selectgo()` are ALWAYS random, regardless of `-race` flag
- This is intentional: steal order for load balancing, select order per Go spec for fairness

### The randomizeScheduler constant

[proc.go:7471](https://github.com/golang/go/blob/master/src/runtime/proc.go#L7471)

```go
const randomizeScheduler = raceenabled
```

**Key insight**: Without `-race`, all randomization is skipped. Production code is essentially deterministic (given same inputs). The randomization exists specifically to help find race conditions during testing.

### cheaprand()

[rand.go:228](https://github.com/golang/go/blob/master/src/runtime/rand.go#L228)

```go
func cheaprand() uint32 {
    mp := getg().m
    // wyrand algorithm
    mp.cheaprand += 0x53c5ca59
    hi, lo := bits.Mul32(mp.cheaprand, mp.cheaprand^0x74743c1b)
    return hi ^ lo
}
```

State is in `mp.cheaprand` (per-M). Seeded in `mrandinit()` from crypto source.

---

## select and Channels

### selectgo() - ALWAYS Random (Not Guarded!)

[select.go:121](https://github.com/golang/go/blob/master/src/runtime/select.go#L121)

```go
select {
case <-ch1:
case <-ch2:
case <-ch3:
}
```

If all channels have values ready, one is picked randomly. **This is a language guarantee for fairness** - you can't rely on case order.

**IMPORTANT**: Unlike runqput/runqputslow, this is NOT guarded by `randomizeScheduler`. Select case ordering is ALWAYS random, even without `-race` flag. This is per the Go spec.

```go
// Inside selectgo():
// Generate random poll order - NO randomizeScheduler CHECK!
norder := 0
for i := range pollorder {
    j := cheaprandn(uint32(norder + 1))  // ALWAYS called, not guarded
    pollorder[norder] = pollorder[j]
    pollorder[j] = uint16(i)
    norder++
}
```

### Channel Internals

[chan.go](https://github.com/golang/go/blob/master/src/runtime/chan.go)

```go
type hchan struct {
    qcount   uint           // number of elements in buffer
    dataqsiz uint           // buffer capacity
    buf      unsafe.Pointer // circular buffer
    elemsize uint16
    closed   uint32
    sendx    uint           // send index
    recvx    uint           // receive index
    recvq    waitq          // waiting receivers (doubly-linked list of sudogs)
    sendq    waitq          // waiting senders
    lock     mutex
}
```

**Buffered channel:**
```
buf: [7] [3] [ ] [ ] [ ]
      ^       ^
    recvx   sendx
```

**Unbuffered:** Single slot, direct handoff between sender and receiver.

### Send: `ch <- val`

1. **Receiver waiting?** -> `goready(receiver)`, direct handoff (copy value directly)
2. **Buffer has space?** -> copy to buffer at sendx, increment sendx
3. **Block:** enqueue self in sendq, `gopark()`

### Receive: `val := <-ch`

1. **Sender waiting?** -> recv from sender, `goready(sender)`
2. **Buffer has data?** -> copy from buffer at recvx, increment recvx
3. **Block:** enqueue self in recvq, `gopark()`

`sendq` and `recvq` are doubly-linked lists of `sudog` structures (goroutine waiting on a channel).

---

## gopark and goready

### gopark - Parking a Goroutine

[proc.go](https://github.com/golang/go/blob/master/src/runtime/proc.go)

When a goroutine needs to wait (channel, lock, timer, I/O):

```go
func gopark(unlockf func(*g, unsafe.Pointer) bool, lock unsafe.Pointer, reason waitReason, ...) {
    mp := getg().m
    gp := mp.curg

    // Set wait reason (for debugging/stack traces)
    gp.waitreason = reason

    // Release any lock before parking
    if unlockf != nil {
        unlockf(gp, lock)
    }

    // Transition: _Grunning -> _Gwaiting
    casgstatus(gp, _Grunning, _Gwaiting)

    // Back to scheduler (switch to g0, call schedule())
    mcall(park_m)
}
```

Common wait reasons: `waitReasonChanReceive`, `waitReasonChanSend`, `waitReasonSelect`, `waitReasonSleep`, `waitReasonSyncCondWait`

### goready - Waking a Goroutine

When a goroutine should wake up (channel send/recv, timer fires, lock released):

```go
func goready(gp *g, traceskip int) {
    systemstack(func() {
        ready(gp, traceskip, true)
    })
}

func ready(gp *g, traceskip int, next bool) {
    // Transition: _Gwaiting -> _Grunnable
    casgstatus(gp, _Gwaiting, _Grunnable)

    // Put on run queue (runnext if next=true)
    runqput(pp, gp, next)

    // Wake up a P if needed
    wakep()
}
```

**The flow:**
```
G1 running:  ch <- val
                |
                v
           G2 in recvq?  ----yes----> goready(G2)
                |                         |
               no                    G2: _Gwaiting -> _Grunnable
                |                         |
                v                    runqput(G2, next=true)
          buffer full?                    |
                |                    G2 in runnext or runq
               yes
                |
                v
          gopark(G1, waitReasonChanSend)
          G1: _Grunning -> _Gwaiting
          G1 added to sendq
          schedule() picks next G
```

---

## Complete Call Graph

```
User Code: go myFunc()
|
+-- newproc(&fn)                          [proc.go:5295]
    |
    +-- systemstack(func() {
       |
       +-- newproc1(fn, ...)               [proc.go:5313]
       |  +-- Allocate or reuse G from gfree list
       |  +-- Setup stack and PC
       |  +-- Assign goid
       |  +-- casgstatus(_Gdead -> _Grunnable)
       |
       +-- runqput(pp, newg, true)         [proc.go:7478]
       |  +-- if randomizeScheduler: 50% skip runnext
       |  +-- Add to pp.runnext or pp.runq[]
       |  +-- if full: runqputslow() -> global queue
       |
       +-- wakep()                          [wake idle P]
          +-- startm(pp, spinning=true)
    })

Later, on some M (in schedule loop):
|
+-- schedule()                             [proc.go:4135]
|  |
|  +-- findRunnable()                      [proc.go:3389]
|     +-- every 61 ticks: globrunqget() (fairness)
|     +-- Try local: runqget(pp)
|     +-- Try global: globrunqget()
|     +-- Try netpoll: netpoll()
|     +-- Try stealing: stealWork()
|     |  +-- for enum := stealOrder.start(cheaprand())
|     |  +-- runqsteal(pp, victim, ...)
|     +-- Return gp
|
+-- execute(gp, inheritTime)               [proc.go:3331]
   +-- mp.curg = gp; gp.m = mp
   +-- casgstatus(_Grunnable -> _Grunning)
   +-- gogo(&gp.sched)  ----------------> USER CODE RUNS
```

---

## What GOMAXPROCS=1 Eliminates

With only one P:

```
    P0
   [G1, G2, G3, G4, ...]
     |
    M0
```

**Eliminated:**
- Work stealing (`stealWork()` has nothing to steal from - only 1 P)
- Cross-P migration
- Multiple Ms with different `cheaprand` states
- True parallelism

**Still present (only with -race):**
- `runqput` randomization (runnext vs queue)
- `runqputslow` shuffle (if queue overflows)
- `runqputbatch` shuffle

**ALWAYS present (even without -race):**
- **`select` case ordering** - This is ALWAYS random per Go spec!

**Summary**: With GOMAXPROCS=1 and WITHOUT `-race`, the scheduler is mostly deterministic EXCEPT for `select` statements with multiple ready cases. To achieve full determinism, you must also control `cheaprand()` in `selectgo()`.

---

## References

- [proc.go](https://github.com/golang/go/blob/master/src/runtime/proc.go) - Main scheduler implementation
- [runtime2.go](https://github.com/golang/go/blob/master/src/runtime/runtime2.go) - Core data structures (g, m, p)
- [chan.go](https://github.com/golang/go/blob/master/src/runtime/chan.go) - Channel implementation
- [select.go](https://github.com/golang/go/blob/master/src/runtime/select.go) - Select statement implementation
- [rand.go](https://github.com/golang/go/blob/master/src/runtime/rand.go) - cheaprand and random number generation
- [Go 1.1 Scheduler Design Doc](https://golang.org/s/go11sched)
