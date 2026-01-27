# Go Scheduler Deep Dive: A Complete Rabbithole

This document combines structured knowledge with Q&A exploration to provide a comprehensive understanding of the Go scheduler. Start with the basics, then follow the questions that naturally arise as you dig deeper.

**Sources**:
- `go/src/runtime/proc.go` - Main scheduler implementation
- `go/src/runtime/runtime2.go` - Core data structures
- `go/src/runtime/rand.go` - Random number generation

---

## 1. The M:P:G Model

The Go scheduler uses three key abstractions to coordinate concurrent execution:

### G - Goroutine
A goroutine is a lightweight user-space thread. Multiple goroutines can run on a single OS thread. Each goroutine has its own stack, registers, and execution state.

### M - Machine (OS Thread)
An M represents an OS thread. The runtime creates M's as needed, and each M can run multiple goroutines sequentially. Each M has an associated `g0` which is a special goroutine used for scheduler work.

### P - Processor
A P is a logical processor that represents the resource needed to execute Go code. It holds per-processor state including:
- Local runnable queue (up to 256 goroutines)
- Memory allocator cache
- Timer heap
- GC work buffer

**Critical Invariant**: Only an M with an associated P can execute user Go code. At any time:
- A P is either assigned to exactly one M or is idle
- An M can execute with a P, in a syscall, or blocked
- GOMAXPROCS controls the number of Ps

### Key Relationships

```
                 Global Scheduler State (schedt struct)
                 - Global runnable queue
                 - Idle M and P lists
                 - GC state and synchronization
                         |
                         | manages
                         v
      P (Processor) - one per GOMAXPROCS
      - Local runnable queue [256]G
      - runnext (optimized next G)
      - Timer heap
      - Memory cache
                         |
                         | owns (when running)
                         v
      M (Machine/OS Thread)
      - g0 (scheduler goroutine)
      - curg (current user G)
      - p (associated P)
      - cheaprand (per-M random state)
                         |
                         | executes
                         v
      G (Goroutine)
      - User code stack
      - Execution state
      - Wait reason (if blocked)
      - bubble (synctest association)
```

<details>
<summary>Q: What does GOMAXPROCS=1 mean exactly?</summary>

`GOMAXPROCS` controls how many **P**s (processors) the Go runtime uses.

**Go's scheduler model is M:N:P:**

```
G (goroutines)     - your code, millions possible
      |
P (processors)     - logical processors, GOMAXPROCS of them
      |
M (machines)       - OS threads, created as needed
```

**P is the key abstraction.** Each P:
- Has its own local run queue of goroutines
- Can only run one goroutine at a time
- Is bound to one M (OS thread) at a time

**With GOMAXPROCS=4 (default on 4-core machine):**

```
    P0          P1          P2          P3
   [G1,G2]     [G3,G4]     [G5]        [G6,G7,G8]
     |           |           |           |
    M0          M1          M2          M3
```

- 4 goroutines can run truly in parallel
- Work stealing: if P2 finishes, it can steal from P3's queue
- `stealWork()` uses `cheaprand()` to pick which P to steal from

**With GOMAXPROCS=1:**

```
    P0
   [G1,G2,G3,G4,G5,G6,G7,G8]
     |
    M0
```

- Only ONE goroutine runs at a time
- No work stealing (nothing to steal from)
- No true parallelism, just concurrency
- `stealWork()` becomes irrelevant

**Why this matters for deterministic testing:**

With `GOMAXPROCS=1`, we eliminate:
- Work stealing randomization (`stealWork` with `cheaprand()`)
- Cross-P migration
- Multiple Ms with different `cheaprand` states

We're left with just:
- `runqput` randomization (runnext vs queue)
- `runqputslow` shuffle (when queue overflows)
- `select` case ordering

</details>

<details>
<summary>Q: Where do these M and P structs actually live in memory?</summary>

The M and P structs are allocated in Go's heap memory and tracked by the global scheduler state (`sched`):

```go
// In runtime/proc.go
var (
    allm       *m        // Linked list of all M's
    gomaxprocs int32     // Number of P's
    ncpu       int32     // Number of CPUs
    sched      schedt    // Global scheduler state
    allp       []*p      // All P's, len == gomaxprocs
)
```

**M's are created on demand:**
- When there's work but no idle M
- When an M enters a blocking syscall (new M created to keep P busy)
- Linked together in a list (`allm`)

**P's are created at startup:**
- `procresize()` allocates exactly `GOMAXPROCS` P's
- Stored in `allp` slice
- Reused, never freed during normal execution

</details>

---

## 2. Key Data Structures

### 2.1 The `g` struct (Goroutine)

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

### 2.2 The `p` struct (Processor)

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

### 2.3 The `m` struct (Machine/OS Thread)

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

<details>
<summary>Q: Why is p stored as puintptr instead of *p?</summary>

```go
// In runtime2.go
type m struct {
    p  puintptr  // Not *p, but a uintptr disguised as pointer
}

type puintptr uintptr

func (pp puintptr) ptr() *p {
    return (*p)(unsafe.Pointer(pp))
}
```

It's stored as `uintptr` (raw integer) instead of `*p` (pointer) to **hide it from the garbage collector**. The GC doesn't scan `uintptr` fields, which is important for runtime internals.

This is why you see patterns like `getg().m.p.ptr()` - the `.ptr()` converts back to a usable pointer.

</details>

---

## 3. Goroutine Lifecycle

### 3.1 Goroutine Creation: `newproc()` -> `newproc1()`

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
1. `go func()` -> compiler calls `newproc()`
2. `newproc1()` allocates/reuses G with stack
3. Sets up execution state (PC points to user function)
4. Transitions G from `_Gdead` -> `_Grunnable`
5. Adds to local runqueue via `runqput()`

<details>
<summary>Q: What is getg() and how does Go know the current goroutine?</summary>

`getg()` = **"get current goroutine"**

It returns a pointer to the `g` struct of the goroutine that's currently running.

**How it works:**

```go
func getg() *g  // Returns pointer to current G
```

It's implemented in **assembly** because the current G pointer is stored in a special place:

```
  CPU

  Registers:
  +-- RAX, RBX, RCX... (general purpose)
  +-- TLS / special register --> current *g
                                    ^
                                    |
                              getg() reads this
```

On AMD64 (most common):
```asm
// go/src/runtime/asm_amd64.s
TEXT runtime.getg(SB),NOSPLIT,$0-8
    MOVQ    TLS, AX          // Read thread-local storage
    MOVQ    (AX)(TLS*1), AX  // Get G pointer from it
    MOVQ    AX, ret+0(FP)    // Return it
    RET
```

**Why it exists:**

Every piece of runtime code needs to know "which goroutine am I?"

```go
func someRuntimeFunc() {
    gp := getg()           // Who am I?

    gp.m                   // What M (thread) am I on?
    gp.m.p.ptr()          // What P (processor) am I using?
    gp.bubble             // Am I in a synctest bubble?
    gp.stack              // Where's my stack?
    gp.status             // Am I running, waiting, dead?
}
```

**The chain:**

```
getg()          ->  *g     "I am goroutine G7"
getg().m        ->  *m     "I'm running on OS thread M2"
getg().m.p      ->  puintptr  "M2 is attached to processor P0"
getg().m.p.ptr() -> *p     "Here's P0's actual struct"
```

**It's FAST:**

Because it's just reading a register/TLS, `getg()` is essentially free - a single instruction. That's why it's used everywhere in the runtime.

</details>

<details>
<summary>Q: What is systemstack and why do we need it?</summary>

`systemstack` = **"run this code on the system stack (g0), not my goroutine's stack"**

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

**User goroutine stacks are small and can grow:**

```go
func recursive(n int) {
    var buf [1024]byte  // Need more stack!
    // ... Go automatically grows stack if needed
    recursive(n - 1)
}
```

But **stack growth itself needs stack space** to run. Chicken and egg problem!

**Solution:** Do sensitive runtime operations on g0's stack, which is large and never grows.

### What `systemstack` does

```go
func someFunc() {
    // Running on user G's stack (small, 2KB)

    systemstack(func() {
        // NOW running on g0's stack (large, 8KB+)
        // Safe to do runtime operations here
    })

    // Back on user G's stack
}
```

### Operations that need systemstack

- Creating goroutines (`newproc1`)
- Garbage collection
- Stack growing/shrinking
- Scheduler operations
- Channel operations (sometimes)
- Memory allocation internals

**TL;DR:**
```
systemstack(fn) = "switch to big safe stack, run fn, switch back"
```

</details>

<details>
<summary>Q: Can you explain the chicken-and-egg problem with stack growth?</summary>

### Goroutine stacks start small and grow

```go
func myFunc() {
    var buf [4096]byte  // Needs 4KB on stack
    // ...
}
```

```
Goroutine stack (2KB):
+----------------+ <-- stack top (high address)
|                |
|   used: 1KB    |
|                |
+----------------+ <-- SP (stack pointer)
|                |
|   free: 1KB    | <-- Only 1KB left!
|                |
+----------------+ <-- stack bottom (low address)

buf needs 4KB... but only 1KB free!
```

### Stack growth requires work

To grow the stack, Go must:

```go
// Pseudocode of what stack growth does:
func growStack() {
    newStack := malloc(biggerSize)    // 1. Allocate new memory
    copy(newStack, oldStack)          // 2. Copy everything over
    adjustPointers(newStack)          // 3. Fix all pointers
    free(oldStack)                    // 4. Free old stack
}
```

**Each of these steps needs stack space to run!**

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
  - copy() internals
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

### Code flow

```go
func someUserCode() {
    var bigArray [10000]byte  // Needs lots of stack

    // Before using bigArray, Go checks: do I have enough stack?
    // This check is inserted by the compiler at function entry
}

// Compiler actually generates:
func someUserCode() {
    if SP < stackguard {      // Not enough stack?
        morestack()           // <-- This switches to g0 and grows
    }
    var bigArray [10000]byte
}

func morestack() {
    systemstack(func() {      // Run on g0's stack
        newstack()            // Safe to allocate/copy here
    })
}
```

**TL;DR:**
```
Problem:  Stack full -> need to grow -> growing needs stack -> crash

Solution: Keep a separate big stack (g0) that never grows
          Switch there to do the growth
          Switch back when done
```

</details>

<details>
<summary>Q: What is the max for g0? What happens when the stack request exceeds g0?</summary>

### g0's stack size

```
g0 stack:  ~8KB (Unix) or ~64KB (Windows)
           Fixed size, allocated by OS at thread creation
           Does NOT grow
```

### What if g0 overflows?

```
g0 stack overflows
       |
       v
   FATAL

"runtime: stack overflow"
Program crashes. No recovery.
```

**There's no backup for g0.** It's the last line of defense.

### Why this almost never happens

**1. Runtime code is carefully written:**

```go
// BAD - runtime developers don't do this:
func runtimeFunc() {
    var buf [100000]byte  // Huge stack allocation
}

// GOOD - what they actually do:
func runtimeFunc() {
    buf := make([]byte, 100000)  // Allocate on HEAP, not stack
}
```

**2. No recursion in critical paths:**

```go
// BAD:
func walkAllGoroutines(g *g) {
    process(g)
    walkAllGoroutines(g.next)  // Recursive = stack grows
}

// GOOD:
func walkAllGoroutines(g *g) {
    for g != nil {             // Iterative = constant stack
        process(g)
        g = g.next
    }
}
```

**3. Functions are marked `nosplit`:**

```go
//go:nosplit  <-- "This function promises to use minimal stack"
func tinyRuntimeFunc() {
    // Compiler enforces: no stack growth check
    // Developer must ensure: uses very little stack
}
```

### The safety hierarchy

```
User G stack:     Small (2KB start), can grow, can overflow -> grows
                           |
                           v overflow

g0 stack:         Large (8KB+), fixed, cannot grow -> FATAL crash
                           |
                           v overflow (rare)

                       Dead
```

### Real world

In practice, g0 overflow is **extremely rare** because:

1. Runtime devs are paranoid about stack usage
2. Complex operations use heap, not stack
3. 8KB is plenty for typical runtime ops
4. Code is tested extensively

You'd basically need a runtime bug to hit this. Normal user code can't cause it because user code never runs on g0.

</details>

<details>
<summary>Q: Where do goroutine stacks live - heap or stack?</summary>

**Yes! User goroutine stacks live on the HEAP.** This is a key insight about Go.

### Traditional threads (C/pthreads)

```
OS allocates fixed stack per thread:

  Process Memory

  +----------+  Fixed, OS-managed
  | Thread 1 |  stack (usually 1-8MB)
  |  Stack   |  Can't grow!
  +----------+

  +----------+
  | Thread 2 |
  |  Stack   |
  +----------+

  +----------------------------+
  |          HEAP              |
  +----------------------------+
```

### Go goroutines

```
Goroutine "stacks" are heap-allocated:

  Process Memory

  +----------+
  | g0 stack |  Only this is "real" OS
  | (M's)    |  stack per thread
  +----------+

  +----------------------------+
  |          HEAP              |
  |                            |
  |   +------+  +------+       |
  |   | G1   |  | G2   |       |
  |   |stack |  |stack |  ...  |  Goroutine stacks
  |   | 2KB  |  | 8KB  |       |  allocated here!
  |   +------+  +------+       |
  |                            |
  +----------------------------+
```

### Why this is powerful

```
Traditional thread:
- 1000 threads x 1MB stack = 1GB memory (mostly wasted)
- Can't grow if you need more
- Stack overflow = crash

Go goroutines:
- 1000 goroutines x 2KB stack = 2MB memory
- Grows on demand (2KB -> 4KB -> 8KB -> ...)
- Can have millions of goroutines
```

### The "stack" is just semantics

```go
// What Go calls a "stack" is really:
type stack struct {
    lo uintptr  // Pointer to heap memory (low address)
    hi uintptr  // Pointer to heap memory (high address)
}

// It's heap memory that Go USES like a stack:
// - Push = decrement SP, write value
// - Pop = read value, increment SP
// - But the memory itself is from malloc()
```

### So the full picture

```
g0:     Real OS stack (small, fixed, per M)
        Used for: runtime operations

User G: Heap-allocated "stack" (starts small, can grow)
        Used for: your code

        It's called "stack" because of how it's USED,
        not where it LIVES.
```

This is why Go can have millions of goroutines - they're just small heap allocations, not OS threads with huge fixed stacks.

</details>

<details>
<summary>Q: Can we request more stack growth than g0's stack size at once?</summary>

No constraint! Even a single growth can be huge.

### Why?

`malloc(size)` uses the **same amount of stack** regardless of `size`:

```go
// These use roughly the same g0 stack space:
newStack := malloc(4KB)      // ~200 bytes of g0 stack
newStack := malloc(1GB)      // ~200 bytes of g0 stack
                    ^
                    |
        Just passing a number and getting a pointer back
        The 1GB comes from HEAP, not from g0 stack
```

### Copying is a loop with pointers, not a stack operation

```go
// How memcpy works (simplified):
func copyStack(dst, src uintptr, size int) {
    // These are just TWO pointers + a counter
    // Uses ~24 bytes of g0 stack, regardless of size

    for i := 0; i < size; i++ {
        *(*byte)(dst + i) = *(*byte)(src + i)
    }
}
```

```
g0 stack during copy:        Memory being copied:

+-------------+              +------------+    +------------+
|             |              | Old stack  |    | New stack  |
|  dst: 0x..  | -------------|------------|---> |            |
|  src: 0x..  | -----------> |  100MB     |    |   100MB    |
|  i:   0     |              |            |    |            |
|             |              +------------+    +------------+
+-------------+                  HEAP              HEAP
     ~24 bytes
```

### The key insight

```
g0 is the ORCHESTRATOR, not the CONTAINER

g0 doesn't hold the data
g0 just holds the instructions (pointers, counters)

Like a forklift operator:
- Operator needs a small seat        (g0 = small)
- Can move a 10-ton container        (user stack = huge)
- Container doesn't go through seat  (data flows heap-to-heap)
```

### Stack limits

```
g0:       ~8KB, fixed, never grows

User G:   Starts at 2KB
          Can grow up to 1GB (64-bit systems)
          Allocated from heap
```

**g0's size doesn't limit how big user stacks can grow!**

</details>

### 3.2 What is `pc` (Program Counter)?

`pc` = **Program Counter** - the memory address of the instruction that's currently executing.

```go
func newproc(fn *funcval) {
    gp := getg()
    pc := sys.GetCallerPC()  // Capture: "where was 'go' called from?"

    systemstack(func() {
        newg := newproc1(fn, gp, pc, ...)
        //                      ^^
        //              Passed to newproc1
    })
}

func newproc1(fn *funcval, callergp *g, callerpc uintptr, ...) *g {
    newg.gopc = callerpc  // Store it in the G struct
    //    ^^^^
    //    "go program counter" - where this G was created
}
```

<details>
<summary>Q: How does pc link to source code?</summary>

When Go compiles your code, it creates a **symbol table** that maps memory addresses to source locations.

### Compilation produces two things

```
  Source: main.go

  1: package main
  2:
  3: func main() {
  4:     go worker()   <-- We want to track this
  5: }
  6:
  7: func worker() {
  8:     // ...
  9: }

                         | compile
                         v

  Binary

  Machine code:              Symbol table:
  +-----------------+        +--------------------------+
  | 0x4a2f00: ...   |        | 0x4a2f00 -> main.go:3    |
  | 0x4a2f15: ...   |        | 0x4a2f15 -> main.go:4    |
  | 0x4a2f28: CALL  |<--pc   | 0x4a2f28 -> main.go:4    |
  | 0x4a2f30: ...   |        | 0x4a2f30 -> main.go:5    |
  | ...             |        | ...                      |
  +-----------------+        +--------------------------+
```

### When you need a stack trace

```go
pc := 0x4a2f28  // Just a number

// Runtime looks up in symbol table:
file, line, fn := runtime.FuncForPC(pc).FileLine(pc)
// file = "main.go"
// line = 4
// fn   = "main.main"
```

### You can do this yourself

```go
package main

import (
    "fmt"
    "runtime"
)

func main() {
    pc, file, line, ok := runtime.Caller(0)  // Get current PC
    if ok {
        fmt.Printf("PC:   0x%x\n", pc)
        fmt.Printf("File: %s\n", file)
        fmt.Printf("Line: %d\n", line)

        fn := runtime.FuncForPC(pc)
        fmt.Printf("Func: %s\n", fn.Name())
    }
}
```

Output:
```
PC:   0x4a2f28
File: /app/main.go
Line: 9
Func: main.main
```

### The symbol table

```
  Binary layout

  +-------------+
  | .text       |  <-- Machine code (executable)
  | (your code) |
  +-------------+
  | .rodata     |  <-- Read-only data (strings, etc)
  +-------------+
  | .gopclntab  |  <-- Go's PC-to-line table
  | (symbol tbl)|     Maps addresses -> source locations
  +-------------+
  | .gosymtab   |  <-- Go's symbol table
  |             |     Function names, types, etc
  +-------------+
```

### So the chain is:

```
pc (0x4a2f28)
      |
      v lookup in .gopclntab
      |
file="main.go", line=4, func="main.main"
      |
      v print stack trace

"created by main.main
    /app/main.go:4 +0x28"
```

The `+0x28` is the offset from the function start - helps pinpoint exact instruction within the function.

</details>

---

## 4. Adding to Run Queue: `runqput()`

Location: `go/src/runtime/proc.go:7478`

`runqput` = "**put a goroutine on the run queue**"

It's called whenever a goroutine becomes runnable:
- `go func()` creates new G -> `runqput`
- G wakes from channel receive -> `runqput`
- G wakes from `time.Sleep` -> `runqput`

### The P's Run Queue Structure

```
P (processor)
+-- runnext    ->  [G5]         <-- "VIP slot" - runs next, inherits time slice
+-- runq[256]  ->  [G1][G2][G3][G4]...  <-- Regular FIFO queue
```

### Implementation

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

<details>
<summary>Q: What is runnext and why does it exist?</summary>

`runnext` is a **single slot** that bypasses the queue. It's "optimized" because:

### Normal queue vs runnext

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

### Why is this "optimized"?

**1. Cache locality**

```go
data := prepareData()  // Data in CPU cache
go process(data)       // New G uses same data
```

If `process` runs immediately (via runnext), the data is still hot in cache. If it waits in queue, cache is cold by the time it runs.

**2. Producer-consumer is instant**

```go
ch := make(chan int)
go func() { ch <- 42 }()  // Producer goes to runnext
x := <-ch                  // Consumer blocks, producer runs NEXT
```

Without runnext: producer waits behind other Gs, consumer waits longer.

**3. Time slice inheritance**

```
Normal scheduling:
G1 runs ---------> [time slice ends] --> scheduler overhead --> G2 runs

With runnext:
G1 runs --> spawns G2 --> G1 blocks --> G2 runs (inherits remaining time)
                                         ^
                                    No scheduler overhead!
```

### The tradeoff

```
runnext = UNFAIR but FAST

Without runnext:  Fair FIFO, but slower for common patterns
With runnext:     Unfair (queue-jumper), but faster for spawner->spawnee
```

That's why `-race` mode randomizes it - to catch bugs where code accidentally depends on this optimization:

```go
if randomizeScheduler && next && randn(2) == 0 {
    next = false  // 50% chance: "no cutting the line today"
}
```

</details>

<details>
<summary>Q: How does runqget work (getting next G to run)?</summary>

```go
// go/src/runtime/proc.go:7598
func runqget(pp *p) (gp *g, inheritTime bool) {
    // Step 1: Check runnext FIRST
    next := pp.runnext
    if next != 0 {
        pp.runnext = 0        // Clear it
        return next, true     // inheritTime=true (keeps time slice)
    }

    // Step 2: Only if runnext empty, check regular queue
    gp := pp.runq[head]
    head++
    return gp, false          // inheritTime=false (fresh time slice)
}
```

**Visual:**

```
Scheduler needs next G to run:

+------------------------------------+
| runnext: [G5]  <-- Check this FIRST |
| runq: [G1] [G2] [G3] [G4]          |
+------------------------------------+
            |
            v
         Return G5 (runnext)


Next time scheduler needs a G:

+------------------------------------+
| runnext: [empty]                   |
| runq: [G1] [G2] [G3] [G4]          |
+------------------------------------+
            |
            v
         Return G1 (head of queue)
```

**Priority:** `runnext > runq[0] > runq[1] > ... > runq[255]`

**Why "inheritTime"?**

```go
return next, true   // inheritTime=true for runnext
return gp, false    // inheritTime=false for regular queue
```

- `inheritTime=true`: G keeps running on **same time slice** (no scheduler overhead)
- `inheritTime=false`: G gets a **fresh time slice** (normal scheduling)

This makes spawner->spawnee handoff nearly free.

</details>

---

## 5. Queue Overflow: `runqputslow()`

Location: `go/src/runtime/proc.go:7524`

When the local queue is full (256 goroutines), half are moved to the global queue:

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

---

## 6. The Main Scheduling Loop

### 6.1 `schedule()`

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

### 6.2 Finding Work: `findRunnable()`

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

### 6.3 Work Stealing: `stealWork()`

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

### 6.4 Executing a Goroutine: `execute()`

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

## 7. Complete Call Graph: `go func()` to Execution

```
User Code: go myFunc()
|
+-- newproc(&fn)                          [proc.go:5295]
   |
   +-- systemstack(func() {
      |
      +-- newproc1(fn, ...)               [proc.go:5313]
      |  +-- Allocate or reuse G
      |  +-- Setup stack and PC
      |  +-- Assign goid
      |  +-- casgstatus(_Gdead -> _Grunnable)
      |
      +-- runqput(pp, newg, true)         [proc.go:7478]
      |  +-- if randomizeScheduler: maybe skip runnext
      |  +-- Add to pp.runnext or pp.runq[]
      |
      +-- wakep()                          [wake idle P]
         +-- startm(pp, spinning=true)
   })

Later, on some M:
|
+-- schedule()                             [proc.go:4135]
|  |
|  +-- findRunnable()                      [proc.go:3389]
|     +-- Try local queue: runqget(pp)
|     +-- Try global queue: globrunqget()
|     +-- Try stealing: stealWork()
|     |  +-- for enum := stealOrder.start(cheaprand())
|     +-- Return gp
|
+-- execute(gp, inheritTime)               [proc.go:3331]
   +-- mp.curg = gp
   +-- casgstatus(_Grunnable -> _Grunning)
   +-- gogo(&gp.sched)  ----------------> USER CODE RUNS
```

---

## 8. The 5 Randomization Points

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

- Without `-race`: All the randomization code paths are skipped.
- With `-race`: `cheaprand()` is called at all 5 points above.

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

<details>
<summary>Q: Does production code have -race enabled?</summary>

**No, production code does NOT use `-race`.**

The race detector is for **testing only** because:

| | With `-race` | Without `-race` (production) |
|---|---|---|
| **Performance** | 2-20x slower | Normal speed |
| **Memory** | 5-10x more | Normal |
| **Binary size** | Larger | Normal |
| **`randomizeScheduler`** | `true` | `false` |

### This is actually a key insight

In production:
- `randomizeScheduler = false`
- All 5 randomization points are **skipped**
- Scheduler is basically deterministic (given same inputs)

So why do concurrency bugs still happen in production? Because:

1. **Multi-core non-determinism** - With GOMAXPROCS > 1, multiple goroutines run *truly in parallel* on different cores. OS thread scheduling is non-deterministic.

2. **External timing** - Network latency, disk I/O, user input all vary

3. **GC timing** - Garbage collection can pause goroutines at unpredictable times

### What this means for research

The randomization in `-race` mode is **intentional** - it helps expose bugs by varying the schedule. The Go team added it specifically to help find race conditions during testing.

Our goal is similar but more systematic:
- Instead of random exploration, do **controlled** exploration
- Instead of hoping to hit a bug, **guarantee** we cover interesting interleavings

So we actually WANT randomization, but we want to **control** it:

```
Current:  -race uses random cheaprand() -> unpredictable schedules
Our goal: -race uses seeded cheaprand() -> reproducible + explorable schedules
```

</details>

---

## 9. What Happens with GOMAXPROCS=1?

With only one P:

```
    P0
   [G1, G2, G3, G4, ...]
     |
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

**Summary**: With GOMAXPROCS=1 and WITHOUT `-race`, the scheduler is essentially deterministic because `randomizeScheduler = false`.

With GOMAXPROCS=1 and WITH `-race`, there's still randomization in:
1. `runqput` - runnext decision
2. `select` - case ordering

<details>
<summary>Q: So with GOMAXPROCS=1 + seeded cheaprand, we can explore all schedules?</summary>

**Exactly right.**

```
GOMAXPROCS=1  +  Custom seed flag  =  Explorable schedules
```

### Why GOMAXPROCS=1 is key

With GOMAXPROCS=1:
- **One goroutine runs at a time** (no true parallelism)
- Every "who runs next?" is a **decision point**
- Total ordering of all operations
- Finite, explorable state space

```
GOMAXPROCS=4 (parallel):          GOMAXPROCS=1 (sequential):

G1 -------->                      G1 ---+
G2 -------->  (chaos)                 G2 --+
G3 -------->                              G3 --+
G4 -------->                                   G1 ---> ...

Hard to reason about              Clear decision tree
```

### The exploration model

```
         +-> G1 runs -+-> G2 runs -> ...
         |            +-> G3 runs -> ...
Start ---+
         |            +-> G1 runs -> ...
         +-> G2 runs -+-> G3 runs -> ...

Each branch = different seed/schedule
```

### What we need to build

```go
// Usage vision:
GOMAXPROCS=1 SYNCTEST_SEED=12345 go test -race ./...

// Or with exploration:
GOMAXPROCS=1 SYNCTEST_EXPLORE=true go test -race ./...
// ^ Tries multiple seeds automatically, finds bugs
```

### The implementation path

1. **Add flag**: `SYNCTEST_SEED` environment variable or `-synctest.seed` flag
2. **Hook into `mrandinit()`**: When in bubble + seed set, use deterministic seed instead of crypto
3. **Validate**: Same seed -> same schedule (Phase 1)
4. **Explore**: Try different seeds systematically (Phase 2)

</details>

---

## 10. gopark and goready

### gopark - Parking a Goroutine

When a goroutine needs to wait (channel, lock, timer):

```go
// go/src/runtime/proc.go
func gopark(unlockf func(*g, unsafe.Pointer) bool, lock unsafe.Pointer, reason waitReason, ...) {
    mp := getg().m
    gp := mp.curg

    // Set wait reason
    gp.waitreason = reason

    // Release any lock before parking
    if unlockf != nil {
        unlockf(gp, lock)
    }

    // Transition: _Grunning -> _Gwaiting
    casgstatus(gp, _Grunning, _Gwaiting)

    // Back to scheduler
    mcall(park_m)
}
```

### goready - Waking a Goroutine

When a goroutine should wake up:

```go
// go/src/runtime/proc.go
func goready(gp *g, traceskip int) {
    systemstack(func() {
        ready(gp, traceskip, true)
    })
}

func ready(gp *g, traceskip int, next bool) {
    // Transition: _Gwaiting -> _Grunnable
    casgstatus(gp, _Gwaiting, _Grunnable)

    // Put on run queue
    runqput(pp, gp, next)

    // Wake up a P if needed
    wakep()
}
```

---

## 11. Virtual Memory and 64-bit Addressing

<details>
<summary>Q: How does virtual memory work with 64-bit addressing?</summary>

### Virtual vs Physical

```
Your computer:
+------------------------------------+
| Physical RAM: 16 GB                |  <-- Actual hardware
+------------------------------------+

What your program SEES (virtual):
+------------------------------------+
| Address space: 128 TB              |  <-- Just numbers!
+------------------------------------+
```

### How 64-bit works

```
64 bits can address: 2^64 = 16 exabytes (EB)

But CPUs actually use 48 bits: 2^48 = 256 TB

Split in half:
  - Upper 128 TB: Kernel space
  - Lower 128 TB: User space (your program)
```

### Most of it is EMPTY

```
128 TB virtual address space:

0x7FFF_FFFF_FFFF +--------------+
                 |    Stack     |  ~8 MB used
                 |    (8MB)     |
                 +--------------+
                 |              |
                 |              |
                 |   NOTHING    |  <-- No physical RAM mapped here
                 |  (unmapped)  |     Just empty addresses
                 |              |     ~127.99 TB of "nothing"
                 |              |
                 |              |
                 +--------------+
                 |    Heap      |  ~1 GB used (example)
                 |              |
0x0000_0000_0000 +--------------+

Actually used: ~1-2 GB
Address space: 128 TB
Ratio: 0.001% used
```

### Think of it like a hotel

```
Hotel "64-bit" has room numbers:
  Room 0000_0000_0001
  Room 0000_0000_0002
  ...
  Room 7FFF_FFFF_FFFF   (trillions of room numbers!)

But the hotel only has 16 ACTUAL rooms (your RAM).

Most room numbers are just... numbers on paper.
No physical room exists behind them.
```

### What happens when you access unmapped address?

```
Program: "Read from address 0x1234_5678_9ABC"

CPU: "Let me check page table..."
     "That address is not mapped to any RAM"

OS:  "SEGFAULT!"

Program crashes.
```

</details>

<details>
<summary>Q: What happens if the thread stacks and heap stacks meet in the middle?</summary>

This is the classic **stack-heap collision** problem. Modern systems prevent it:

### The old days (bad)

```
Stack growing down:      Heap growing up:
       |                      ^
       v                      |
+--------------+        +--------------+
|    Stack     |        |    Heap      |
|      v       |        |      ^       |
|              |        |              |
|    CRASH     |<------>|    CRASH     |
|              |        |              |
+--------------+        +--------------+

Collision = Memory corruption, crashes, security exploits
```

### Modern protection: Guard pages

```
+------------------------------------+
|            Stack                   |
|              |                     |
|              v                     |
|     [used stack space]             |
+------------------------------------+
| GUARD PAGE (unmapped)              | <-- No physical RAM here!
+------------------------------------+    Touching = SIGSEGV (crash)
|                                    |
|     (big gap in 64-bit)            |
|                                    |
+------------------------------------+
| GUARD PAGE (unmapped)              |
+------------------------------------+
|              ^                     |
|              |                     |
|            Heap                    |
+------------------------------------+
```

### 64-bit makes collision nearly impossible

```
32-bit address space:  4 GB total
                       Stack and heap might be close

64-bit address space:  16 EB (exabytes) total

+----------------------------------------------------------+
|                                                          |
|  Stack at: 0x7FFF_FFFF_FFFF                             |
|                                                          |
|                                                          |
|            ~140 TB of empty space                        |
|                                                          |
|                                                          |
|  Heap at:  0x0000_5555_5555                             |
|                                                          |
+----------------------------------------------------------+

They'll never meet. The sun will burn out first.
```

### Summary

| Protection | How it works |
|------------|--------------|
| **Guard pages** | Unmapped memory, instant crash if touched |
| **64-bit space** | Stack and heap are ~140TB apart |
| **Stack limit** | OS enforces max size (typically 8MB) |
| **ASLR** | Randomizes locations, harder to exploit |

So in practice: **they can't meet**. You hit a guard page or stack limit first, and the program crashes safely instead of corrupting memory.

</details>

---

## 12. Comparison with Other Languages

<details>
<summary>Q: How do other languages (Erlang, Java) handle lightweight threads?</summary>

### Comparison Table

| Language | Lightweight unit | Stack | Can spawn millions? |
|----------|-----------------|-------|---------------------|
| **Go** | Goroutine | Heap, 2KB start | Yes |
| **Erlang/Elixir** | Process | Heap, ~300 bytes start | Yes |
| **Java 21+** | Virtual Thread | Heap | Yes (Project Loom) |
| **Kotlin** | Coroutine | Heap | Yes |
| **Rust** | async Task | Heap (with tokio) | Yes |

### Traditional threads (expensive)

```
C/C++/Java (old):     OS Thread = 1-8 MB stack
                      10,000 threads = 10-80 GB

Max threads = RAM / stack_size
```

### Lightweight concurrency (cheap)

```
Go:                   Goroutine = 2 KB start
Erlang:               Process = ~300 bytes start
Java 21:              Virtual Thread = ~1 KB

1,000,000 goroutines = ~2 GB
```

### What Go did well

```
Not the first, but made it:

1. Easy:     Just write "go func()"
2. Built-in: No external library needed
3. Fast:     Very optimized scheduler
4. Safe:     Channels for communication
```

### Erlang was actually first (1986!)

```erlang
% Erlang - spawn millions of processes since the 80s
spawn(fun() -> do_work() end).
```

WhatsApp ran 2 million connections per server using Erlang.

### TL;DR

```
Go:     Made it mainstream and easy
Erlang: Did it first (1986)
Java:   Finally caught up (2023, Project Loom)
Rust:   async/await + tokio

The idea: Don't use OS threads, use heap-allocated lightweight units
```

Go isn't unique, but it made this pattern very accessible and popular.

</details>

---

## 13. Key Invariants

1. **P Ownership**: Only one M can own a P at a time
2. **G Ownership**: Only one M can execute a given G
3. **Fairness**: Global queue checked every 61 ticks
4. **Progress**: Blocking syscall doesn't block scheduler (P reassigned)

---

## 14. The Gap Synctest Leaves

```
+---------------------------------------------------------+
|                    SYNCTEST CONTROLS                     |
|  [x] Time (fake clock, instant time.Sleep)              |
|  [x] Durable blocking detection                         |
|  [x] Channel isolation                                  |
|  [x] Deadlock detection                                 |
+---------------------------------------------------------+

+---------------------------------------------------------+
|               SYNCTEST DOES NOT CONTROL                  |
|  [ ] Goroutine execution order                          |
|  [ ] cheaprand() randomization                          |
|  [ ] Select case ordering                               |
|  [ ] Work stealing order                                |
+---------------------------------------------------------+
```

**This is the research gap**: Synctest solves time non-determinism, but not execution order non-determinism.

---

## 15. References

- Source: `go/src/runtime/proc.go`
- Source: `go/src/runtime/runtime2.go`
- Source: `go/src/runtime/rand.go`
- Source: `go/src/runtime/select.go`
- Design Doc: https://golang.org/s/go11sched
