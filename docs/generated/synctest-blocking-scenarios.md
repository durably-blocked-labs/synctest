# Synctest Blocking Scenarios: Complete Call Graphs

This document details how synctest handles every type of durable blocking operation, with complete call graphs and state transitions.

**Source**: `go/src/runtime/synctest.go`, `go/src/runtime/proc.go`, `go/src/runtime/chan.go`, `go/src/runtime/select.go`, `go/src/runtime/time.go`

---

## 1. The Event Loop (Root Goroutine)

The event loop is the heart of synctest. It runs in the root goroutine that called `synctest.Run()`.

### 1.1 Event Loop Structure

```go
// go/src/runtime/synctest.go:206-235
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

### 1.2 The `synctestidle_c` Callback

This callback decides whether the event loop should actually park or continue immediately.

```go
// go/src/runtime/synctest.go:269-280
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

### 1.3 Event Loop State Machine

```
                    ┌─────────────────────────────────┐
                    │         LOOP RUNNING            │
                    │         active = 1              │
                    └───────────────┬─────────────────┘
                                    │
                                    ▼
                    ┌─────────────────────────────────┐
                    │      timers.check(now)          │
                    │      Fire ready timers          │
                    └───────────────┬─────────────────┘
                                    │
                                    ▼
                    ┌─────────────────────────────────┐
                    │   gopark(synctestidle_c)        │
                    └───────────────┬─────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
            running > 0                      running == 0
                    │                               │
                    ▼                               ▼
        ┌───────────────────────┐   ┌───────────────────────────┐
        │ ACTUALLY PARK         │   │ DON'T PARK                │
        │ active--  → active=0  │   │ active stays 1            │
        │ Wait for goready()    │   │ Continue immediately      │
        └───────────┬───────────┘   └─────────────┬─────────────┘
                    │                             │
                    │ (woken by                   │
                    │  maybeWakeLocked)           │
                    │                             │
                    ▼                             │
        ┌───────────────────────┐                │
        │ active++  → active=1  │                │
        └───────────┬───────────┘                │
                    │                             │
                    └──────────────┬──────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────────┐
                    │   next = timers.wakeTime()      │
                    │   bubble.now = next             │
                    │   (TIME ADVANCES!)              │
                    └───────────────┬─────────────────┘
                                    │
                                    ▼
                              Loop continues
```

### 1.4 Wake Chain

When a goroutine becomes durably blocked:

```
User goroutine parks (durably)
        │
        ▼
casgstatus(_Grunning → _Gwaiting)
        │
        ▼
bubble.changegstatus()
        │
        ▼
bubble.running--
        │
        ▼
maybeWakeLocked()
        │
        ├─► running > 0?  → return nil (don't wake)
        │
        └─► running == 0? → goready(bubble.root)
                                    │
                                    ▼
                           Event loop wakes!
                           Advances bubble.now
                           Fires ready timers
```

---

## 2. Durable Blocking: `time.Sleep`

### 2.1 Call Graph

```
time.Sleep(duration)
        │
        ▼
timeSleep(ns int64)                           [time.go]
        │
        ├─► gp := getg()
        │
        ├─► If gp.bubble != nil:
        │       Add timer to bubble.timers    [NOT P.timers!]
        │   Else:
        │       Add timer to P.timers
        │
        └─► gopark(resetForSleep, nil, waitReasonSleep, ...)
                │
                ▼
        park_m(gp)                            [proc.go]
                │
                ├─► casgstatus(gp, _Grunning, _Gwaiting)
                │       │
                │       └─► bubble.changegstatus(gp, old, new)
                │               │
                │               └─► bubble.running--
                │                   maybeWakeLocked() → maybe wake root
                │
                └─► schedule()  // Run something else
```

### 2.2 Timer Firing

```
Event loop: timers.check(bubble.now)
        │
        ▼
For each timer where timer.when <= bubble.now:
        │
        ├─► Remove from heap
        │
        └─► timer.f(timer.arg)
                │
                └─► goready(sleeping_gp)
                        │
                        ▼
                casgstatus(gp, _Gwaiting, _Grunnable)
                        │
                        └─► bubble.changegstatus()
                                │
                                └─► bubble.running++

                runqput(pp, gp, next=true)
```

### 2.3 Complete State Trace

```
Initial:
    running=2 (G1 main, G2 worker), active=0 (loop parked)
    bubble.now = T0
    bubble.timers = []

G1: time.Sleep(5s)
    │
    ├─► bubble.timers = [{when: T0+5s, wake: G1}]
    ├─► gopark() → running-- → running=1
    └─► maybeWakeLocked() → running>0, don't wake

G2: time.Sleep(3s)
    │
    ├─► bubble.timers = [{when: T0+3s, wake: G2},
    │                    {when: T0+5s, wake: G1}]  (heap ordered)
    ├─► gopark() → running-- → running=0
    └─► maybeWakeLocked() → running==0, goready(root)!

Event loop wakes:
    │
    ├─► active++ → active=1
    ├─► next = T0+3s
    ├─► bubble.now = T0+3s  ← TIME JUMP!
    ├─► timers.check():
    │       G2's timer ready → goready(G2) → running++ → running=1
    │
    └─► gopark(synctestidle_c):
            running==0 && active==1? NO (running=1)
            active-- → active=0
            Actually park

G2 runs, finishes, exits:
    │
    └─► running-- → running=0
        maybeWakeLocked() → goready(root)

Event loop wakes:
    │
    ├─► active++ → active=1
    ├─► next = T0+5s
    ├─► bubble.now = T0+5s  ← TIME JUMP!
    ├─► timers.check():
    │       G1's timer ready → goready(G1) → running=1
    │
    └─► gopark() → parks (running=1)

G1 runs, finishes...
```

---

## 3. Durable Blocking: Channel Receive

### 3.1 Call Graph: `<-ch` (No Sender Ready)

```
x := <-ch
        │
        ▼
chanrecv(c *hchan, ep unsafe.Pointer, block bool)     [chan.go:458]
        │
        ├─► lock(&c.lock)
        │
        ├─► Check bubble isolation:
        │       if c.bubble != nil && getg().bubble != c.bubble {
        │           fatal("recv on synctest channel from outside bubble")
        │       }
        │
        ├─► Check for sender in sendq:
        │       sg := c.sendq.dequeue()
        │       if sg != nil → direct receive (Case 1)
        │
        ├─► Check buffer:
        │       if c.qcount > 0 → receive from buffer (Case 2)
        │
        └─► Must block (Case 3):
                │
                ├─► gp := getg()
                ├─► mysg := acquireSudog()
                ├─► mysg.g = gp
                ├─► mysg.elem = ep  // Where to put received value
                ├─► c.recvq.enqueue(mysg)
                │
                └─► gopark(chanparkcommit, &c.lock,
                           waitReasonSynctestChanReceive, ...)
                        │
                        ▼
                park_m(gp):
                        │
                        ├─► casgstatus(_Grunning → _Gwaiting)
                        │       └─► bubble.running--
                        │           maybeWakeLocked()
                        │
                        ├─► chanparkcommit():
                        │       unlock(&c.lock)
                        │       return true
                        │
                        └─► schedule()
```

### 3.2 Waking: Sender Arrives

```
ch <- value
        │
        ▼
chansend(c *hchan, ep unsafe.Pointer, block bool)     [chan.go:160]
        │
        ├─► lock(&c.lock)
        │
        ├─► Check for receiver in recvq:
        │       sg := c.recvq.dequeue()
        │       if sg != nil:
        │           │
        │           ├─► recv(c, sg, ep)  // Copy value to receiver
        │           │       └─► memmove(sg.elem, ep, c.elemsize)
        │           │
        │           ├─► gp := sg.g
        │           │
        │           └─► goready(gp, 4)
        │                   │
        │                   ▼
        │               casgstatus(gp, _Gwaiting, _Grunnable)
        │                   └─► bubble.running++
        │               runqput(pp, gp, next=true)
        │
        └─► unlock(&c.lock)
```

### 3.3 Wait Reason

For channels inside a bubble, special wait reasons are used:

```go
// go/src/runtime/chan.go
if c.bubble != nil {
    waitreason = waitReasonSynctestChanReceive  // Durable!
} else {
    waitreason = waitReasonChanReceive  // Not durable
}
```

Only `waitReasonSynctestChanReceive` is in `isIdleInSynctest[]`.

---

## 4. Durable Blocking: Channel Send

### 4.1 Call Graph: `ch <- value` (No Receiver Ready, Buffer Full)

```
ch <- value
        │
        ▼
chansend(c *hchan, ep unsafe.Pointer, block bool)     [chan.go:160]
        │
        ├─► lock(&c.lock)
        │
        ├─► Check bubble isolation
        │
        ├─► Check for receiver in recvq:
        │       if sg != nil → direct send (Case 1)
        │
        ├─► Check buffer space:
        │       if c.qcount < c.dataqsiz → buffer send (Case 2)
        │
        └─► Must block (Case 3):
                │
                ├─► gp := getg()
                ├─► mysg := acquireSudog()
                ├─► mysg.g = gp
                ├─► mysg.elem = ep  // Value to send
                ├─► c.sendq.enqueue(mysg)
                │
                └─► gopark(chanparkcommit, &c.lock,
                           waitReasonSynctestChanSend, ...)
                        │
                        └─► bubble.running--
                            maybeWakeLocked()
```

### 4.2 Waking: Receiver Arrives

```
x := <-ch
        │
        ▼
chanrecv():
        │
        ├─► sg := c.sendq.dequeue()
        │
        ├─► send(c, sg, ep)  // Copy value from sender
        │
        └─► goready(sg.g)
                └─► bubble.running++
```

---

## 5. Durable Blocking: Select

### 5.1 Call Graph: `select` with Multiple Cases

```
select {
case x := <-ch1:
case ch2 <- y:
case <-ch3:
}
        │
        ▼
selectgo(cas0 *scase, order0 *uint16, ncases int)     [select.go:121]
        │
        ├─► Build scases array from cases
        │
        ├─► Generate random poll order (ALWAYS random!):
        │       for i := range scases {
        │           j := cheaprandn(uint32(i + 1))  // ← RANDOMIZATION
        │           pollorder[i], pollorder[j] = pollorder[j], pollorder[i]
        │       }
        │
        ├─► Generate lock order (sorted by channel address):
        │       // Prevents deadlock when locking multiple channels
        │
        ├─► Lock all channels in lock order
        │
        ├─► Pass 1: Check if any case is ready
        │       for _, casei := range pollorder {
        │           cas := &scases[casei]
        │           if cas.isReady() {
        │               // Found ready case!
        │               unlock all
        │               return casei
        │           }
        │       }
        │
        └─► Pass 2: No case ready, must block
                │
                ├─► Create sudog for each case
                ├─► Enqueue on each channel's sendq/recvq
                │
                └─► gopark(selparkcommit, nil,
                           waitReasonSynctestSelect, ...)
                        │
                        └─► bubble.running--
                            maybeWakeLocked()
```

### 5.2 Waking: One Case Becomes Ready

```
Some channel operation (send or recv):
        │
        ▼
Found our sudog in channel's queue:
        │
        ├─► Remove sudog from channel
        │
        ├─► sg.success = true
        │
        └─► goready(sg.g)
                │
                ▼
        bubble.running++

Goroutine wakes in selectgo():
        │
        ├─► Determine which case won
        │
        ├─► Remove sudogs from all OTHER channels
        │   (we were waiting on multiple)
        │
        └─► Return winning case index
```

### 5.3 Wait Reason

```go
if inBubble {
    waitreason = waitReasonSynctestSelect  // Durable
} else {
    waitreason = waitReasonSelect  // Not durable
}
```

---

## 6. Durable Blocking: `sync.Cond.Wait`

### 6.1 Call Graph

```
cond.Wait()
        │
        ▼
func (c *Cond) Wait()                                 [sync/cond.go]
        │
        ├─► c.checker.check()  // Verify lock held
        │
        ├─► t := runtime_notifyListAdd(&c.notify)
        │
        ├─► c.L.Unlock()
        │
        └─► runtime_notifyListWait(&c.notify, t)
                │
                ▼
        notifyListWait(l *notifyList, t uint32)       [runtime/sema.go]
                │
                └─► gopark(nil, nil,
                           waitReasonSyncCondWait, ...)
                        │
                        └─► bubble.running--
                            maybeWakeLocked()
```

### 6.2 Waking: `Signal()` or `Broadcast()`

```
cond.Signal()
        │
        ▼
runtime_notifyListNotifyOne(&c.notify)
        │
        └─► goready(waiting_gp)
                └─► bubble.running++

cond.Broadcast()
        │
        ▼
runtime_notifyListNotifyAll(&c.notify)
        │
        └─► for each waiter:
                goready(gp)
                bubble.running++
```

---

## 7. Durable Blocking: `sync.WaitGroup.Wait`

### 7.1 Call Graph

```
wg.Wait()
        │
        ▼
func (wg *WaitGroup) Wait()                           [sync/waitgroup.go]
        │
        ├─► Check if counter already 0 → return immediately
        │
        └─► runtime_Semacquire(&wg.sema)
                │
                ▼
        semacquire1(addr *uint32, ...)                [runtime/sema.go]
                │
                └─► gopark(nil, nil,
                           waitReasonSynctestWaitGroupWait, ...)
                        │
                        └─► bubble.running--
                            maybeWakeLocked()
```

### 7.2 Waking: Counter Reaches Zero

```
wg.Done()  // or wg.Add(-1)
        │
        ▼
func (wg *WaitGroup) Add(delta int)
        │
        ├─► Decrement counter
        │
        └─► if counter == 0:
                runtime_Semrelease(&wg.sema)
                        │
                        ▼
                goready(waiting_gp)
                        └─► bubble.running++
```

---

## 8. NOT Durable: `sync.Mutex.Lock`

### 8.1 Why Mutex Is NOT Durable

A mutex can be unlocked by ANY goroutine, including ones outside the bubble:

```go
var mu sync.Mutex

// In bubble:
go func() {
    mu.Lock()  // This blocks...
}()

// Outside bubble (hypothetically):
mu.Unlock()  // Could unlock it!
```

Because external code could release the lock, waiting on a mutex is NOT considered "durably blocked."

### 8.2 Wait Reason

```go
waitReasonSyncMutexLock  // NOT in isIdleInSynctest[]
```

If a goroutine blocks on mutex:
- `bubble.running` does NOT decrement
- Bubble stays "active"
- Time does NOT advance

### 8.3 Implication

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

---

## 9. NOT Durable: Network I/O

### 9.1 Why Network Is NOT Durable

Network operations depend on external systems:

```go
conn.Read(buf)  // Waiting for remote server
```

The remote server is outside the bubble, so this isn't durable.

### 9.2 Wait Reason

```go
waitReasonIOWait  // NOT in isIdleInSynctest[]
```

### 9.3 Implication

Don't do real I/O in synctest bubbles. Use mocks or interfaces.

---

## 10. The `isIdleInSynctest` Array

This array defines which wait reasons count as "durably blocked":

```go
// go/src/runtime/runtime2.go:1381-1399
var isIdleInSynctest = [...]bool{
    waitReasonZero:                  false,
    waitReasonGCAssistMarking:       false,
    waitReasonIOWait:                false,  // NOT durable
    waitReasonChanReceiveNilChan:    false,
    waitReasonChanSendNilChan:       false,
    waitReasonDumpingHeap:           false,
    waitReasonGarbageCollection:     false,
    waitReasonGarbageCollectionScan: false,
    waitReasonPanicWait:             false,
    waitReasonSelect:                false,  // Regular select
    waitReasonSelectNoCases:         false,
    waitReasonGCAssistWait:          false,
    waitReasonGCSweepWait:           false,
    waitReasonGCScavengeWait:        false,
    waitReasonChanReceive:           false,  // Regular chan
    waitReasonChanSend:              false,  // Regular chan
    waitReasonFinalizerWait:         false,
    waitReasonForceGCIdle:           false,
    waitReasonCoroutine:             false,
    waitReasonGCWorkerActive:        false,
    waitReasonGCWorkerIdle:          false,
    waitReasonPreempted:             false,
    waitReasonDebugCall:             false,
    waitReasonGCMarkTermination:     false,
    waitReasonStoppingTheWorld:      false,
    waitReasonFlushProcCaches:       false,
    waitReasonTraceGoroutineStatus:  false,
    waitReasonTraceProcStatus:       false,
    waitReasonPageTraceFlush:        false,
    waitReasonGCPointerInitialization: false,
    waitReasonGCMarkWorkerIdle:      false,
    waitReasonSyncMutexLock:         false,  // NOT durable
    waitReasonSyncRWMutexRLock:      false,  // NOT durable
    waitReasonSyncRWMutexLock:       false,  // NOT durable

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

---

## 11. Summary: Durable vs Non-Durable

| Operation | Wait Reason | Durable? | Why |
|-----------|-------------|----------|-----|
| `time.Sleep` | `waitReasonSleep` | **Yes** | Only time can wake |
| `<-ch` (bubble chan) | `waitReasonSynctestChanReceive` | **Yes** | Only bubble goroutines can send |
| `ch <-` (bubble chan) | `waitReasonSynctestChanSend` | **Yes** | Only bubble goroutines can recv |
| `select` (bubble chans) | `waitReasonSynctestSelect` | **Yes** | Only bubble activity |
| `sync.Cond.Wait` | `waitReasonSyncCondWait` | **Yes** | Signal comes from bubble |
| `sync.WaitGroup.Wait` | `waitReasonSynctestWaitGroupWait` | **Yes** | Done comes from bubble |
| `sync.Mutex.Lock` | `waitReasonSyncMutexLock` | **No** | Any goroutine can unlock |
| `sync.RWMutex` | `waitReasonSyncRWMutex*` | **No** | Any goroutine can unlock |
| Network I/O | `waitReasonIOWait` | **No** | External dependency |
| `<-ch` (non-bubble chan) | `waitReasonChanReceive` | **No** | External send possible |

---

## 12. Complete Example Trace

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

### Trace

```
T=0ms: synctest.Run() creates bubble
       bubble = {running: 1, active: 1, now: T0, total: 1}
       Event loop starts

T=0ms: Main goroutine (G1) starts
       bubble.running = 1 (already counted)

T=0ms: G1 creates channel
       ch.bubble = bubble

T=0ms: G1 spawns G2
       bubble.total++ → 2
       bubble.running++ → 2

T=0ms: Event loop: gopark(synctestidle_c)
       running==0 && active==1? NO (running=2)
       active-- → 0, actually park

T=0ms: G2 runs: time.Sleep(1s)
       Add timer {when: T0+1s, wake: G2}
       gopark() → running-- → running=1
       maybeWakeLocked() → running>0, don't wake

T=0ms: G1 runs: x := <-ch
       No sender, enqueue in ch.recvq
       gopark(waitReasonSynctestChanReceive)
       running-- → running=0
       maybeWakeLocked() → running==0, goready(root)!

T=0ms: Event loop wakes
       active++ → 1
       next = T0+1s
       bubble.now = T0+1s  ← TIME JUMPS 1 SECOND!
       timers.check():
           G2's timer ready → goready(G2)
           running++ → 1
       gopark(synctestidle_c)
           running==0? NO
           active-- → 0, park

T=0ms (real), T=1s (fake): G2 runs
       ch <- 42
       Found G1 in recvq
       Copy 42 to G1
       goready(G1) → running++ → 2
       G2 continues, exits
       running-- → 1
       total-- → 1? No wait, G2 hasn't exited yet

T=0ms (real): G1 wakes with x=42
       Continues, assertion passes, returns
       bubble.done = true
       running-- → 0 (if G2 done too)

T=0ms (real): G2 finishes
       total-- → 1 (only root remains)

T=0ms (real): Event loop checks
       total == 1? YES
       No deadlock, success!

Total real time: ~0ms
Total fake time: 1s
```

---

## 13. References

- `go/src/runtime/synctest.go` - Bubble implementation
- `go/src/runtime/proc.go` - gopark, goready, scheduler
- `go/src/runtime/chan.go` - Channel operations
- `go/src/runtime/select.go` - Select implementation
- `go/src/runtime/time.go` - Timer implementation
- `go/src/runtime/sema.go` - Semaphore (WaitGroup, Cond)
- `go/src/runtime/runtime2.go` - Wait reasons, isIdleInSynctest
