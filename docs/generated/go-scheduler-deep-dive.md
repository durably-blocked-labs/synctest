# Go Scheduler: A Deep Dive from First Principles

This document provides a comprehensive explanation of the Go scheduler by analyzing the runtime source code. The scheduler is responsible for distributing goroutines across OS threads (M), coordinating with processors (P), and managing the execution of user code.

**Source**: `go/src/runtime/proc.go`, `go/src/runtime/runtime2.go`

## 1. The M:P:G Model

The Go scheduler uses three key abstractions to coordinate concurrent execution:

### **G - Goroutine**
A goroutine is a lightweight user-space thread. Multiple goroutines can run on a single OS thread. Each goroutine has its own stack, registers, and execution state.

### **M - Machine (OS Thread)**
An M represents an OS thread. The runtime creates M's as needed, and each M can run multiple goroutines sequentially. Each M has an associated `g0` which is a special goroutine used for scheduler work.

### **P - Processor**
A P is a logical processor that represents the resource needed to execute Go code. It holds per-processor state including:
- Local runnable queue (up to 256 goroutines)
- Memory allocator cache
- Timer heap
- GC work buffer

**Critical Invariant**: Only an M with an associated P can execute user Go code. At any time:
- A P is either assigned to exactly one M or is idle
- An M can execute with a P, in a syscall, or blocked
- GOMAXPROCS controls the number of Ps

### **Key Relationships**

```
┌─────────────────────────────────────────────────┐
│ Global Scheduler State (schedt struct)          │
│ - Global runnable queue                         │
│ - Idle M and P lists                            │
│ - GC state and synchronization                  │
└─────────────────────────────────────────────────┘
         │
         │ manages
         ▼
   ┌──────────────────────────────────────────────────┐
   │ P (Processor) - one per GOMAXPROCS              │
   │ - Local runnable queue [256]G                    │
   │ - runnext (optimized next G)                     │
   │ - Timer heap                                     │
   │ - Memory cache                                   │
   └──────────────────────────────────────────────────┘
         │
         │ owns (when running)
         ▼
   ┌──────────────────────────────────────────────┐
   │ M (Machine/OS Thread)                        │
   │ - g0 (scheduler goroutine)                   │
   │ - curg (current user G)                      │
   │ - p (associated P)                           │
   │ - cheaprand (per-M random state)             │
   └──────────────────────────────────────────────┘
         │
         │ executes
         ▼
   ┌──────────────────────────────────────────────┐
   │ G (Goroutine)                                │
   │ - User code stack                            │
   │ - Execution state                            │
   │ - Wait reason (if blocked)                   │
   │ - bubble (synctest association)              │
   └──────────────────────────────────────────────┘
```

---

## 2. Key Data Structures

### **2.1 The `g` struct (Goroutine)**

Location: `go/src/runtime/runtime2.go:473`

```go
type g struct {
    // Stack management
    stack       stack   // [lo, hi) memory range
    stackguard0 uintptr // checked on prologue for preemption/growth

    // Scheduling
    m         *m      // current M (nil if not running)
    sched     gobuf   // saved processor registers for context switch

    // Execution state
    atomicstatus atomic.Uint32  // _Gidle, _Grunnable, _Grunning, _Gwaiting, _Gdead

    // Goroutine identity
    goid         uint64    // unique goroutine ID

    // Queue linkage
    schedlink    guintptr  // next G in runnable queue

    // Waiting state
    waitreason   waitReason

    // Synctest
    bubble  *synctestBubble    // Which bubble this goroutine belongs to
}
```

**G Status Constants** (runtime2.go:17-120):
- `_Gidle` (0): Just allocated, not initialized
- `_Grunnable` (1): On a run queue, ready to execute
- `_Grunning` (2): Currently executing user code
- `_Gsyscall` (3): In a syscall
- `_Gwaiting` (4): Blocked (channel, lock, etc.)
- `_Gdead` (6): Unused, on free list

### **2.2 The `p` struct (Processor)**

Location: `go/src/runtime/runtime2.go:773`

```go
type p struct {
    id     int32
    status uint32  // _Pidle, _Prunning, _Pgcstop, _Pdead
    m      muintptr // back-link to M

    // Runnable queue management
    runqhead uint32
    runqtail uint32
    runq     [256]guintptr  // circular buffer of runnable Gs
    runnext  guintptr       // optimized next G to run

    // Timers
    timers timers  // heap of timers for this P

    // Scheduling metadata
    schedtick   uint32  // incremented on every scheduler call
}
```

### **2.3 The `m` struct (Machine/OS Thread)**

Location: `go/src/runtime/runtime2.go:618`

```go
type m struct {
    // Goroutines
    g0     *g        // scheduler goroutine
    curg   *g        // current user goroutine

    // Processor
    p      puintptr  // current P (nil if not executing Go code)

    // Thread state
    spinning  bool    // actively looking for work

    // Random number generator - KEY FOR DETERMINISM
    cheaprand   uint32    // fast pseudo-random state
    cheaprand64 uint64    // 64-bit version
}
```

### **2.4 The `schedt` struct (Global Scheduler State)**

Location: `go/src/runtime/runtime2.go:931`

```go
type schedt struct {
    // Goroutine ID generation
    goidgen    atomic.Uint64

    // M management
    midle        listHeadManual // idle M's waiting for work
    nmidle       int32

    // P management
    pidle        puintptr       // idle P's
    npidle       atomic.Int32

    // Spinning workers
    nmspinning   atomic.Int32   // count of spinning M's

    // Global runnable queue
    runq gQueue
}
```

---

## 3. Goroutine Lifecycle

### **3.1 Goroutine Creation: `newproc()` → `newproc1()`**

Location: `go/src/runtime/proc.go:5295`

```go
// go/src/runtime/proc.go:5295
func newproc(fn *funcval) {
    gp := getg()
    pc := sys.GetCallerPC()
    systemstack(func() {
        newg := newproc1(fn, gp, pc, false, waitReasonZero)

        pp := getg().m.p.ptr()
        runqput(pp, newg, true)  // Add to run queue

        if mainStarted {
            wakep()  // Wake up a P if needed
        }
    })
}
```

**Flow**:
1. `go func()` → compiler calls `newproc()`
2. `newproc1()` allocates/reuses G with stack
3. Sets up execution state (PC points to user function)
4. Transitions G from `_Gdead` → `_Grunnable`
5. Adds to local runqueue via `runqput()`

### **3.2 Adding to Run Queue: `runqput()`**

Location: `go/src/runtime/proc.go:7478`

```go
// go/src/runtime/proc.go:7478
func runqput(pp *p, gp *g, next bool) {
    // RANDOMIZATION POINT #1
    if randomizeScheduler && next && randn(2) == 0 {
        next = false  // Randomly decide not to use runnext
    }

    if next {
        // Place in runnext (will run after current G's time slice)
        oldnext := pp.runnext
        if !pp.runnext.cas(oldnext, guintptr(unsafe.Pointer(gp))) {
            goto retryNext
        }
        if oldnext == 0 {
            return
        }
        gp = oldnext.ptr()  // Evict old runnext to queue
    }

    // Add to regular run queue (circular buffer)
    h := atomic.LoadAcq(&pp.runqhead)
    t := pp.runqtail
    if t-h < uint32(len(pp.runq)) {
        pp.runq[t%uint32(len(pp.runq))].set(gp)
        atomic.StoreRel(&pp.runqtail, t+1)
        return
    }

    // Queue full, spill to global
    runqputslow(pp, gp, h, t)
}
```

### **3.3 Queue Overflow: `runqputslow()`**

Location: `go/src/runtime/proc.go:7524`

```go
// go/src/runtime/proc.go:7524
func runqputslow(pp *p, gp *g, h, t uint32) bool {
    var batch [len(pp.runq)/2 + 1]*g
    n := t - h
    n = n / 2  // Take half

    for i := uint32(0); i < n; i++ {
        batch[i] = pp.runq[(h+i)%uint32(len(pp.runq))].ptr()
    }
    batch[n] = gp

    // RANDOMIZATION POINT #2 - Fisher-Yates shuffle
    if randomizeScheduler {
        for i := uint32(1); i <= n; i++ {
            j := cheaprandn(i + 1)
            batch[i], batch[j] = batch[j], batch[i]
        }
    }

    // Move to global queue
    lock(&sched.lock)
    globrunqputbatch(&q)
    unlock(&sched.lock)
    return true
}
```

### **3.4 The Main Scheduling Loop: `schedule()`**

Location: `go/src/runtime/proc.go:4135`

```go
// go/src/runtime/proc.go:4135
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

### **3.5 Finding Work: `findRunnable()`**

Location: `go/src/runtime/proc.go:3389`

Priority order:
1. Stop-the-world GC check
2. Trace reader (special system G)
3. GC mark worker
4. **Global queue (every 61 ticks)** - fairness
5. **Local queue** - fast path
6. **Global queue** - batch grab
7. **Network poll** - I/O ready goroutines
8. **Work stealing** - steal from other Ps
9. Idle-time GC work
10. Park and wait

### **3.6 Work Stealing: `stealWork()`**

Location: `go/src/runtime/proc.go:3828`

```go
// go/src/runtime/proc.go:3828
func stealWork(now int64) (gp *g, inheritTime bool, ...) {
    pp := getg().m.p.ptr()

    const stealTries = 4
    for i := 0; i < stealTries; i++ {
        // RANDOMIZATION POINT #3 - Random steal order
        for enum := stealOrder.start(cheaprand()); !enum.done(); enum.next() {
            p2 := allp[enum.position()]
            if pp == p2 {
                continue
            }

            // Try to steal goroutines
            if gp := runqsteal(pp, p2, stealTimersOrRunNextG); gp != nil {
                return gp, false, now, pollUntil, ranTimer
            }
        }
    }
    return nil, false, now, pollUntil, ranTimer
}
```

### **3.7 Executing a Goroutine: `execute()`**

Location: `go/src/runtime/proc.go:3331`

```go
// go/src/runtime/proc.go:3331
func execute(gp *g, inheritTime bool) {
    mp := getg().m

    // Link M and G
    mp.curg = gp
    gp.m = mp

    // Transition G state
    casgstatus(gp, _Grunnable, _Grunning)

    // Jump to goroutine code (never returns)
    gogo(&gp.sched)
}
```

---

## 4. Call Graph: `go func()` to Execution

```
User Code: go myFunc()
│
└─ newproc(&fn)                          [proc.go:5295]
   │
   └─ systemstack(func() {
      │
      ├─ newproc1(fn, ...)               [proc.go:5313]
      │  ├─ Allocate or reuse G
      │  ├─ Setup stack and PC
      │  ├─ Assign goid
      │  └─ casgstatus(_Gdead → _Grunnable)
      │
      ├─ runqput(pp, newg, true)         [proc.go:7478]
      │  ├─ if randomizeScheduler: maybe skip runnext
      │  └─ Add to pp.runnext or pp.runq[]
      │
      └─ wakep()                          [wake idle P]
         └─ startm(pp, spinning=true)
   })

Later, on some M:
│
├─ schedule()                             [proc.go:4135]
│  │
│  └─ findRunnable()                      [proc.go:3389]
│     ├─ Try local queue: runqget(pp)
│     ├─ Try global queue: globrunqget()
│     ├─ Try stealing: stealWork()
│     │  └─ for enum := stealOrder.start(cheaprand())
│     └─ Return gp
│
└─ execute(gp, inheritTime)               [proc.go:3331]
   ├─ mp.curg = gp
   ├─ casgstatus(_Grunnable → _Grunning)
   └─ gogo(&gp.sched)  ──────────────────► USER CODE RUNS
```

---

## 5. Randomization Points (ALL of them)

The Go scheduler uses `cheaprand()` at these points. **All controlled by `randomizeScheduler`** (which equals `raceenabled`).

| # | Location | Function | What it randomizes |
|---|----------|----------|-------------------|
| 1 | proc.go:7490 | `runqput()` | Whether to use runnext or regular queue |
| 2 | proc.go:7543 | `runqputslow()` | Shuffle batch going to global queue |
| 3 | proc.go:3837 | `stealWork()` | Order of Ps to steal from |
| 4 | proc.go:7584 | `runqputbatch()` | Shuffle batch from global queue |
| 5 | select.go:191 | `selectgo()` | Order of select cases |

### The `randomizeScheduler` Constant

Location: `go/src/runtime/proc.go:7471`

```go
const randomizeScheduler = raceenabled
```

**This means**: Scheduler randomization is ONLY enabled when you use `-race` flag!

Without `-race`: All the randomization code paths are skipped.
With `-race`: `cheaprand()` is called at all 5 points above.

### The `cheaprand()` Function

Location: `go/src/runtime/rand.go:228`

```go
// go/src/runtime/rand.go:228
func cheaprand() uint32 {
    mp := getg().m
    // wyrand algorithm
    mp.cheaprand += 0x53c5ca59
    hi, lo := bits.Mul32(mp.cheaprand, mp.cheaprand^0x74743c1b)
    return hi ^ lo
}
```

**Key insight**: State is in `mp.cheaprand` (per-M). Seeded in `mrandinit()` from crypto source.

---

## 6. What Happens with GOMAXPROCS=1?

With only one P:

```
    P0
   [G1, G2, G3, G4, ...]
     ↓
    M0
```

**Eliminated**:
- Work stealing (`stealWork()` has nothing to steal)
- Cross-P migration
- Multiple Ms with different `cheaprand` states

**Still present**:
- `runqput` randomization (runnext vs queue)
- `runqputslow` shuffle (if queue overflows to global)
- `select` case ordering

**Answer to your question**: With GOMAXPROCS=1 and WITHOUT `-race`, the scheduler is essentially deterministic because `randomizeScheduler = false`.

With GOMAXPROCS=1 and WITH `-race`, there's still randomization in:
1. `runqput` - runnext decision
2. `select` - case ordering

---

## 7. Key Invariants

1. **P Ownership**: Only one M can own a P at a time
2. **G Ownership**: Only one M can execute a given G
3. **Fairness**: Global queue checked every 61 ticks
4. **Progress**: Blocking syscall doesn't block scheduler (P reassigned)

---

## 8. References

- Source: `go/src/runtime/proc.go`
- Source: `go/src/runtime/runtime2.go`
- Source: `go/src/runtime/rand.go`
- Design Doc: https://golang.org/s/go11sched
