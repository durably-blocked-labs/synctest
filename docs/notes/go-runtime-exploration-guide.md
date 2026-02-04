# Go Runtime Exploration Guide

A file-by-file guide to understanding Go's scheduler and synctest. Open these in order.

---

## Phase 1: Core Data Structures

### 1. `src/runtime/runtime2.go` — The Foundation

**Start here.** This file defines the three core types: G, M, P.

| Line Range | What to Look For |
|------------|------------------|
| **17-120** | G status constants (`_Gidle`, `_Grunnable`, `_Grunning`, `_Gwaiting`, `_Gdead`) |
| **473+** | `type g struct` — goroutine structure |
| **618+** | `type m struct` — machine/OS thread structure |
| **773+** | `type p struct` — processor structure |
| **1381-1399** | `isIdleInSynctest` array — defines durable vs non-durable blocking |

**Key fields to understand in `g`:**
```
stack       → [lo, hi) memory range, lives on HEAP
stackguard0 → checked for preemption/growth
sched       → saved CPU registers (gobuf)
atomicstatus → _Gidle, _Grunnable, _Grunning, _Gwaiting
waitreason  → why is this G blocked?
bubble      → synctest bubble association
```

**Key fields in `m`:**
```
g0      → scheduler goroutine (large fixed stack)
curg    → current user goroutine
p       → associated P (nil if not executing Go code)
cheaprand → per-M random state (KEY FOR DETERMINISM)
```

**Key fields in `p`:**
```
runq[256]  → circular buffer of runnable Gs
runnext    → VIP slot, bypasses queue
timers     → timer heap for this P
schedtick  → incremented on every scheduler call
```

---

### 2. `src/runtime/rand.go` — Randomness

**Why this matters:** Scheduler uses `cheaprand()` at key decision points.

| Line | What to Look For |
|------|------------------|
| **228+** | `func cheaprand()` — wyrand algorithm, per-M state |

```go
func cheaprand() uint32 {
    mp := getg().m
    mp.cheaprand += 0x53c5ca59
    hi, lo := bits.Mul32(mp.cheaprand, mp.cheaprand^0x74743c1b)
    return hi ^ lo
}
```

State lives in `mp.cheaprand`. Seeded from crypto source in `mrandinit()`.

---

## Phase 2: The Scheduler

### 3. `src/runtime/proc.go` — The Heart

**This is the biggest file.** Navigate by function, not linearly.

#### Goroutine Creation
| Line | Function | Purpose |
|------|----------|---------|
| **5295** | `newproc(&fn)` | Entry point for `go func()` |
| **5313** | `newproc1()` | Allocates/reuses G, sets up stack |

#### Run Queue Management
| Line | Function | Purpose |
|------|----------|---------|
| **7471** | `randomizeScheduler` constant | `= raceenabled` (only with `-race`) |
| **7478** | `runqput()` | Add G to P's queue |
| **7490** | | ↳ Randomization point #1: runnext vs queue |
| **7524** | `runqputslow()` | Overflow to global queue |
| **7541** | | ↳ Randomization point #2: Fisher-Yates shuffle |
| **7579** | `runqputbatch()` | Shuffle from global queue |

#### The Schedule Loop
| Line | Function | Purpose |
|------|----------|---------|
| **4135** | `schedule()` | Main loop, runs on g0 |
| **3389** | `findRunnable()` | Find next G to run |
| **3331** | `execute()` | Actually run a G |
| **3837** | `stealWork()` | Steal from other Ps |
| | | ↳ Randomization point #3: **ALWAYS random** |

#### Parking/Waking
| Line | Function | Purpose |
|------|----------|---------|
| **~3200** | `gopark()` | Park current G (block) |
| **~3250** | `goready()` | Wake a parked G |
| **1277-1320** | `casgstatus()` | Change G status, calls bubble.changegstatus |

#### Global State
| Line | What |
|------|------|
| **~200** | `var allm *m` — linked list of all Ms |
| | `var allp []*p` — all Ps |
| | `var sched schedt` — global scheduler state |

---

### 4. `src/runtime/select.go` — Select Statement

| Line | Function | Purpose |
|------|----------|---------|
| **121** | `selectgo()` | Implements `select {}` |
| **191** | | Randomization point #5: **ALWAYS random** (Go spec!) |

```go
// Poll order is ALWAYS randomized - this is a language guarantee
for i := range pollorder {
    j := cheaprandn(uint32(norder + 1))  // NOT guarded by randomizeScheduler!
    pollorder[norder] = pollorder[j]
    pollorder[j] = uint16(i)
    norder++
}
```

---

### 5. `src/runtime/chan.go` — Channels

| Line | What to Look For |
|------|------------------|
| **~30** | `type hchan struct` — channel structure |
| **116-118** | Bubble assignment: `c.bubble = getg().bubble` |
| **193** | Bubble isolation check (fatal if cross-bubble) |

**Key fields in `hchan`:**
```
buf      → circular buffer for buffered channels
sendq    → waiting senders (linked list of sudogs)
recvq    → waiting receivers
lock     → per-channel mutex
bubble   → synctest bubble (if created inside one)
```

**Send/Receive flow:**
1. Check for waiting receiver/sender → direct handoff
2. Check buffer space → enqueue/dequeue
3. Block → `gopark()` with appropriate waitreason

---

## Phase 3: Timers

### 6. `src/runtime/time.go` — Timer Implementation

| What to Look For |
|------------------|
| `timeSleep()` — entry point for `time.Sleep()` |
| Bubble check: uses `bubble.now` if in bubble, else `nanotime()` |
| Timer added to `bubble.timers` or `P.timers` |

```go
func timeSleep(ns int64) {
    gp := getg()
    var now int64
    if bubble := gp.bubble; bubble != nil {
        now = bubble.now  // Fake time!
    } else {
        now = nanotime()  // Real time
    }
    when := now + ns
    // Add timer...
}
```

---

## Phase 4: Synctest

### 7. `src/runtime/synctest.go` — Bubble Implementation

| Line | What to Look For |
|------|------------------|
| **14-39** | `type synctestBubble struct` |
| **43-107** | `changegstatus()` — updates running counter |
| **131-158** | `maybeWakeLocked()` — decides what to wake |
| **206-235** | Event loop — fires timers, advances time |
| **237-257** | Deadlock detection |
| **269-280** | `synctestidle_c` — should event loop park? |

**Key bubble fields:**
```
timers   → bubble's own timer heap (NOT P's!)
now      → fake time in nanoseconds
running  → non-durably-blocked goroutines (KEY!)
active   → event loop activity counter
total    → total goroutines (for deadlock detection)
root     → event loop goroutine
main     → user's test function goroutine
```

---

### 8. `src/runtime/sema.go` — Semaphores

Used by `sync.WaitGroup`, `sync.Cond`, `sync.Mutex`.

| What to Look For |
|------------------|
| `semacquire()` / `semrelease()` |
| How waitreason is set for different sync primitives |

---

## Exploration Order Summary

```
1. runtime2.go     → Understand G, M, P structures
       ↓
2. rand.go         → Understand cheaprand (source of non-determinism)
       ↓
3. proc.go         → Follow goroutine lifecycle:
   │                  newproc → runqput → schedule → findRunnable → execute
   │
   ├── runqput()        → Where randomization happens
   ├── stealWork()      → Work stealing (always random!)
   └── gopark/goready   → Blocking/waking
       ↓
4. select.go       → Select randomization (always random!)
       ↓
5. chan.go         → Channel ops, bubble isolation
       ↓
6. time.go         → Timer implementation, fake time hook
       ↓
7. synctest.go     → Bubble, running counter, deadlock detection
       ↓
8. sema.go         → Mutex/WaitGroup/Cond internals
```

---

## Quick Reference: Randomization Points

| # | File:Line | Function | Guarded by `-race`? |
|---|-----------|----------|---------------------|
| 1 | proc.go:7490 | `runqput()` | YES |
| 2 | proc.go:7541 | `runqputslow()` | YES |
| 3 | proc.go:3837 | `stealWork()` | **NO** — always random |
| 4 | proc.go:7579 | `runqputbatch()` | YES |
| 5 | select.go:191 | `selectgo()` | **NO** — always random (Go spec) |

---

## Quick Reference: Wait Reasons

**Durable (decrements `running`):**
- `waitReasonSleep`
- `waitReasonSynctestChanReceive/Send`
- `waitReasonSynctestSelect`
- `waitReasonSyncCondWait`
- `waitReasonSynctestWaitGroupWait`

**Non-durable (bubble stays "active"):**
- `waitReasonChanReceive/Send` (regular channels)
- `waitReasonSelect` (regular select)
- `waitReasonSyncMutexLock`
- `waitReasonIOWait`
