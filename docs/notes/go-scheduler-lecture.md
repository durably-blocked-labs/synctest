# Go Runtime Scheduler - Lecture Notes

## Overview

This document covers Go's M:P:G scheduler model, focusing on determinism and concurrency testing.

**Key source files:**
| File | Purpose |
|------|---------|
| `runtime/runtime2.go` | Core data structures (g, m, p, schedt) |
| `runtime/proc.go` | Scheduler logic (schedule, findrunnable, gopark, goready) |
| `runtime/select.go` | Select statement implementation |
| `runtime/chan.go` | Channel implementation |
| `runtime/time.go` | Timers |
| `runtime/netpoll.go` | Network poller |

---

## 1. M:P:G Model

| Component | What it is | Key fields |
|-----------|------------|------------|
| **G** (Goroutine) | User code execution context | `stack`, `sched` (sp/pc), `atomicstatus`, `m`, `waitsince` |
| **M** (Machine) | OS thread | `g0`, `curg`, `p`, `spinning`, `park` |
| **P** (Processor) | Scheduling context / resources | `runq`, `runnext`, `timers`, `m`, `status` |

**Relationship:**
```
M executes G, but needs P to do so.
Without P, M cannot run user goroutines.

     ┌─────────────────┐
     │   M (OS thread) │
     │  ┌───────────┐  │
     │  │ g0        │  │  ← scheduler stack
     │  │ curg ─────┼──┼──→ G (user goroutine)
     │  │ p ────────┼──┼──→ P (processor)
     │  └───────────┘  │
     └─────────────────┘
```

### Code locations

| Concept | File | Line/Function |
|---------|------|---------------|
| g struct | `runtime2.go` | `type g struct` |
| m struct | `runtime2.go` | `type m struct` |
| p struct | `runtime2.go` | `type p struct` |
| schedt (global) | `runtime2.go` | `type schedt struct` |

---

## 2. g0 and curg

| Term | Meaning |
|------|---------|
| **g0** | Special goroutine per M for scheduler/runtime code |
| **curg** | Current user goroutine running on M |

**Why g0 exists:**
- Scheduler code needs its own stack (can't use user G's stack)
- System calls, GC, stack growth all run on g0
- `mcall()` switches from curg → g0 to run scheduler

```
User code running:     m.curg = G1, executing user code on G1's stack
Scheduler running:     m.curg = nil, executing on g0's stack
```

### Code locations

| Concept | File | Function |
|---------|------|----------|
| mcall (switch to g0) | `proc.go` | `mcall(fn)` |
| systemstack | `proc.go` | `systemstack(fn)` |

---

## 3. Goroutine Status (atomicstatus)

| Status | Value | Meaning |
|--------|-------|---------|
| `_Gidle` | 0 | Just allocated, not used |
| `_Grunnable` | 1 | On runq, ready to run |
| `_Grunning` | 2 | Currently executing on M |
| `_Gsyscall` | 3 | In syscall, P may be stolen |
| `_Gwaiting` | 4 | Blocked (channel, mutex, etc.) |
| `_Gdead` | 6 | Finished, on free list |
| `_Gpreempted` | 9 | Stopped by async preemption |

**Transitions:**
```
                    ┌──────────────┐
     newproc()      │   _Gidle     │
         │          └──────────────┘
         ▼
  ┌──────────────┐    schedule()    ┌──────────────┐
  │  _Grunnable  │ ───────────────► │  _Grunning   │
  └──────────────┘                  └──────────────┘
         ▲                               │
         │ goready()                     │ gopark()
         │                               ▼
  ┌──────────────┐                  ┌──────────────┐
  │              │ ◄─────────────── │  _Gwaiting   │
  └──────────────┘   (wake event)   └──────────────┘
         ▲
         │ exitsyscall()                 │ entersyscall()
         │                               ▼
  ┌──────────────┐                  ┌──────────────┐
  │              │ ◄─────────────── │  _Gsyscall   │
  └──────────────┘                  └──────────────┘
```

### Code locations

| Concept | File | Function |
|---------|------|----------|
| Status constants | `runtime2.go` | `const _Gidle, _Grunnable...` |
| casgstatus | `proc.go` | `casgstatus(gp, old, new)` |
| readgstatus | `proc.go` | `readgstatus(gp)` |

---

## 4. P's runq, runnext, and timers

### Local Run Queue

| Field | Type | Purpose |
|-------|------|---------|
| `runnext` | `guintptr` | Next G to run (hot/fast path) |
| `runq` | `[256]guintptr` | Circular buffer of runnable Gs |
| `runqhead` | `uint32` | Head index (consume from here) |
| `runqtail` | `uint32` | Tail index (add here) |

**Priority:** `runnext` > `runq[head]` > global runq > steal

### Timers

| Field | Purpose |
|-------|---------|
| `timers` | Min-heap of timers for this P |
| `numTimers` | Count of timers |
| `timerModifiedEarliest` | Earliest modified timer |

**Who checks timers?**
- `checkTimers()` called in `schedule()` loop
- `sysmon` wakes Ps if timers are due and P is idle
- `findrunnable()` checks timers before going idle

### synctest difference

In synctest bubbles:
- Fake time (not real wall clock)
- Timers controlled by `synctest.Wait()`
- All Gs in bubble must be durably blocked for time to advance

### Code locations

| Concept | File | Function |
|---------|------|----------|
| runqget | `proc.go` | `runqget(pp *p) (gp *g, inheritTime bool)` |
| runqput | `proc.go` | `runqput(pp *p, gp *g, next bool)` |
| checkTimers | `proc.go` | `checkTimers(pp *p, now int64)` |

---

## 5. gopark and goready

### gopark - Block a goroutine

```go
func gopark(unlockf func(*g, unsafe.Pointer) bool, lock unsafe.Pointer, reason waitReason, ...)
```

| Step | What happens |
|------|--------------|
| 1 | Save state, set `waitreason` |
| 2 | `mcall(park_m)` - switch to g0 |
| 3 | `casgstatus(gp, _Grunning, _Gwaiting)` |
| 4 | Call `unlockf` (e.g., unlock channel) |
| 5 | `schedule()` - pick next G |

**Used by:** channels, mutexes, select, timers, I/O

### goready - Wake a goroutine

```go
func goready(gp *g, traceskip int)
```

| Step | What happens |
|------|--------------|
| 1 | `casgstatus(gp, _Gwaiting, _Grunnable)` |
| 2 | `runqput(pp, gp, next=true)` - put in runnext! |
| 3 | `wakep()` - wake idle P if needed |

**Key insight:** goready puts G in **runnext**, giving it priority.

### Code locations

| Concept | File | Function |
|---------|------|----------|
| gopark | `proc.go` | `gopark(...)` |
| park_m | `proc.go` | `park_m(gp *g)` |
| goready | `proc.go` | `goready(gp *g, traceskip int)` |
| ready | `proc.go` | `ready(gp *g, traceskip int, next bool)` |

---

## 6. newproc - Spawning Goroutines

```go
go func() { ... }  // compiles to newproc()
```

| Step | Function | What happens |
|------|----------|--------------|
| 1 | `newproc(fn)` | Entry point |
| 2 | `newproc1(fn, gp, pc)` | Allocate/reuse g struct |
| 3 | `gfget(pp)` | Try to get g from free list |
| 4 | `malg(stacksize)` | Or allocate new g with stack |
| 5 | `runqput(pp, newg, true)` | Add to runnext! |
| 6 | `wakep()` | Wake idle P if exists |

**Key insight:** New goroutines go to **runnext** (next=true), so last spawned runs first (LIFO behavior).

### Code locations

| Concept | File | Function |
|---------|------|----------|
| newproc | `proc.go` | `newproc(fn *funcval)` |
| newproc1 | `proc.go` | `newproc1(fn *funcval, callergp *g, callerpc uintptr, ...)` |
| gfget | `proc.go` | `gfget(pp *p) *g` |
| malg | `proc.go` | `malg(stacksize int32) *g` |

---

## 7. runqput Family

### runqput - Add G to local runq

```go
func runqput(pp *p, gp *g, next bool)
```

| `next` | Behavior |
|--------|----------|
| `true` | Put in `runnext`, kick old runnext to runq |
| `false` | Put directly in runq tail |

### runqputslow - Overflow to global

Called when local runq is full (256 entries):
- Moves half of local runq to global runq
- Prevents one P from hogging all Gs

### runqputbatch - Batch add

For adding multiple Gs at once (e.g., from cgo callbacks).

### Code locations

| Concept | File | Function |
|---------|------|----------|
| runqput | `proc.go` | `runqput(pp *p, gp *g, next bool)` |
| runqputslow | `proc.go` | `runqputslow(pp *p, gp *g, h, t uint32) bool` |
| runqputbatch | `proc.go` | `runqputbatch(pp *p, q *gQueue, qsize int)` |

---

## 8. schedule and findrunnable

### schedule - Main scheduler loop

```go
func schedule() {
    // Runs on g0

    // 1. Check if GC needs to run
    // 2. Find next goroutine
    gp, inheritTime, tryWakeP := findrunnable()  // blocks until work found
    // 3. Execute it
    execute(gp, inheritTime)
}
```

### findrunnable - Find work to do

**Search order (with GOMAXPROCS=1):**

| Priority | Source | Code |
|----------|--------|------|
| 1 | GC workers | `gcBlackenEnabled` check |
| 2 | Trace reader | trace buffer |
| 3 | Global runq (1/61 chance) | Fairness: `schedtick%61 == 0` |
| 4 | **Local runq** | `runqget(pp)` |
| 5 | Global runq | `globrunqget(pp, 0)` |
| 6 | Netpoll | `netpoll(0)` |
| 7 | **Work stealing** | `stealWork(...)` |
| 8 | Go idle | `stopm()` |

### Code locations

| Concept | File | Function |
|---------|------|----------|
| schedule | `proc.go` | `schedule()` |
| findrunnable | `proc.go` | `findrunnable() (gp *g, inheritTime, tryWakeP bool)` |
| execute | `proc.go` | `execute(gp *g, inheritTime bool)` |

---

## 9. stealWork

```go
func stealWork(now int64) (gp *g, inheritTime bool, rnow, pollUntil int64, newWork bool)
```

**Why it doesn't happen with GOMAXPROCS=1:**

```go
// In stealWork:
pp := getg().m.p.ptr()
if gomaxprocs == 1 {
    // No other P to steal from!
    return nil, false, now, pollUntil, false
}
```

With only 1 P, there's no other P's runq to steal from.

**Stealing algorithm (when GOMAXPROCS > 1):**
1. Randomize order of Ps to check
2. For each P, try to steal half its runq
3. Also check P's timers
4. Use `runqsteal()` to atomically grab Gs

### Code locations

| Concept | File | Function |
|---------|------|----------|
| stealWork | `proc.go` | `stealWork(...)` |
| runqsteal | `proc.go` | `runqsteal(pp, p2 *p, stealRunNextG bool)` |
| runqgrab | `proc.go` | `runqgrab(pp *p, batch *[256]guintptr, ...)` |

---

## 10. Async Preemption

### How it works

| Component | Role |
|-----------|------|
| `sysmon` | Detects G running >10ms |
| `preemptone(pp)` | Requests preemption |
| `signalM(mp, sigPreempt)` | Sends SIGURG to M |
| Signal handler | Sets `gp.preempt = true`, `gp.stackguard0 = stackPreempt` |
| Function prologue | Checks stackguard0, calls `morestack` → `newstack` → `gopreempt_m` |

### Disabling preemption

```bash
GODEBUG=asyncpreemptoff=1
```

This disables signal-based preemption. Only cooperative yields remain.

### G status during preemption

```
_Grunning → (SIGURG) → _Gpreempted → (schedule) → _Grunnable
```

### Code locations

| Concept | File | Function |
|---------|------|----------|
| preemptone | `proc.go` | `preemptone(pp *p) bool` |
| preemptM | `signal_unix.go` | `preemptM(mp *m)` |
| doSigPreempt | `signal_unix.go` | `doSigPreempt(gp *g, ctxt *sigctxt)` |
| asyncPreempt | `preempt.go` | `asyncPreempt()` |

---

## 11. sysmon

**What is sysmon?**
- Background goroutine running on its own M (no P needed!)
- Wakes up periodically (20μs to 10ms)
- Does housekeeping the scheduler can't do itself

### sysmon responsibilities

| Task | What it does |
|------|--------------|
| **Preemption** | Detects Gs running >10ms, sends SIGURG |
| **Netpoll** | Polls network if no one else is |
| **Timers** | Wakes Ps with due timers |
| **P stealing** | Takes P from M in syscall >10ms |
| **GC** | Helps with GC pacing |

### Timers and sysmon

**Q: Who checks timers if M is busy with curg?**

| Checker | When |
|---------|------|
| `schedule()` | Every scheduling round, calls `checkTimers()` |
| `findrunnable()` | Before going idle |
| `sysmon` | Periodically, wakes Ps with due timers |

sysmon iterates over **all Ps** and checks `p.timers`:

```go
// In sysmon loop:
for _, pp := range allp {
    if pp.numTimers.Load() > 0 {
        // Check if timer is due
        // Wake P if needed
    }
}
```

### Code locations

| Concept | File | Function |
|---------|------|----------|
| sysmon | `proc.go` | `sysmon()` |
| retake | `proc.go` | `retake(now int64) uint32` |
| checkTimers | `proc.go` | `checkTimers(pp *p, now int64) (rnow, pollUntil int64, ran bool)` |

---

## 12. I/O and Syscalls

### Syscall flow

```
User code: file.Read()
     │
     ▼
entersyscall()
  - Save G state (sp, pc)
  - casgstatus(_Grunning, _Gsyscall)
  - Detach P (P can be stolen!)
     │
     ▼
[actual syscall - M blocks in kernel]
     │
     ▼
exitsyscall()
  - Try to reacquire P
  - If no P available: put G on global runq
  - casgstatus(_Gsyscall, _Grunnable)
```

### sysmon's role in syscalls

If M is in syscall for >10ms:
1. sysmon calls `retake()`
2. Steals P from M
3. Gives P to another M (or wakes one)
4. When syscall returns, G goes to global runq

### Network I/O (netpoll)

Network I/O is special - uses non-blocking I/O + epoll/kqueue:
- G calls `netpoll` → adds fd to poller → gopark
- When data ready, poller returns G → goready
- No M blocked in kernel!

### Code locations

| Concept | File | Function |
|---------|------|----------|
| entersyscall | `proc.go` | `entersyscall()` |
| exitsyscall | `proc.go` | `exitsyscall()` |
| retake | `proc.go` | `retake(now int64)` |
| netpoll | `netpoll_epoll.go` | `netpoll(delay int64)` |

---

## 13. Global State (schedt)

```go
var sched schedt  // Global scheduler state
```

| Field | Purpose |
|-------|---------|
| `runq` | Global run queue |
| `runqsize` | Size of global runq |
| `nmidle` | Number of idle Ms |
| `nmidlelocked` | Idle Ms with locked P |
| `npidle` | Number of idle Ps |
| `nmspinning` | Ms looking for work |
| `gFree` | Free G list (for reuse) |
| `freem` | Free M list |
| `gcwaiting` | GC waiting to run |

### Code locations

| Concept | File | Location |
|---------|------|----------|
| schedt struct | `runtime2.go` | `type schedt struct` |
| sched var | `proc.go` | `var sched schedt` |
| globrunqget | `proc.go` | `globrunqget(pp *p, max int32)` |
| globrunqput | `proc.go` | `globrunqput(gp *g)` |

---

## 14. Select Randomization

### The non-determinism source

```go
select {
case <-ch1:  // ready
case <-ch2:  // ready
case <-ch3:  // ready
}
// Which one? RANDOM!
```

### Implementation

```go
// runtime/select.go
func selectgo(cas0 *scase, order0 *uint16, ...) {
    // Fisher-Yates shuffle for poll order
    for i := 1; i < ncases; i++ {
        j := cheaprandn(uint32(i + 1))  // RANDOM!
        pollorder[i] = pollorder[j]
        pollorder[j] = uint16(i)
    }

    // Now poll in randomized order
    for _, casei := range pollorder {
        if ready(cas[casei]) {
            return casei
        }
    }
}
```

### For deterministic testing

Need to control `cheaprandn()` or seed it consistently.

### Code locations

| Concept | File | Function |
|---------|------|----------|
| selectgo | `select.go` | `selectgo(...)` |
| cheaprandn | `runtime.go` | `cheaprandn(n uint32) uint32` |

---

## 15. Channels

### Channel struct

```go
type hchan struct {
    qcount   uint      // elements in buffer
    dataqsiz uint      // buffer capacity
    buf      unsafe.Pointer  // circular buffer
    sendx    uint      // send index
    recvx    uint      // receive index
    recvq    waitq     // waiting receivers
    sendq    waitq     // waiting senders
    lock     mutex
}
```

### Send/Receive flow

**Send (`ch <- v`):**
1. Lock channel
2. If receiver waiting in `recvq` → copy directly, goready receiver
3. Else if buffer not full → copy to buffer
4. Else → gopark in `sendq`

**Receive (`<-ch`):**
1. Lock channel
2. If sender waiting in `sendq` → copy directly, goready sender
3. Else if buffer not empty → copy from buffer
4. Else → gopark in `recvq`

### Code locations

| Concept | File | Function |
|---------|------|----------|
| hchan struct | `chan.go` | `type hchan struct` |
| chansend | `chan.go` | `chansend(c *hchan, ep unsafe.Pointer, ...)` |
| chanrecv | `chan.go` | `chanrecv(c *hchan, ep unsafe.Pointer, ...)` |

---

## Summary: Sources of Non-Determinism

| Source | Cause | How to control |
|--------|-------|----------------|
| Multiple Ps | True parallelism | `GOMAXPROCS=1` |
| Async preemption | SIGURG after 10ms | `GODEBUG=asyncpreemptoff=1` |
| Select | Fisher-Yates shuffle | Seed/control `cheaprandn` |
| Work stealing | Random P order | `GOMAXPROCS=1` eliminates |
| Timers | Real time | synctest fake time |
| Netpoll | I/O timing | Mock/control I/O |
| Global runq fairness | `schedtick%61` | Predictable with single P |

---

## Deterministic Execution Recipe

```bash
GOMAXPROCS=1 GODEBUG=asyncpreemptoff=1 go test -v ./...
```

This gives you:
- Single P → no parallelism, no stealing
- No async preemption → only cooperative yields
- Deterministic order: runnext → runq (FIFO)

Remaining non-determinism: `select` statements only.
