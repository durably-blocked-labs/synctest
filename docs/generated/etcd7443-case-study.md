# Case Study: etcd#7443 -- Channel & Lock Mixed Deadlock

## 1. Bug Overview

[etcd#7443](https://github.com/etcd-io/etcd/pull/7443) is a concurrent
close race in etcd's gRPC client load balancer. The `simpleBalancer`
coordinates address updates between a watcher goroutine (`lbWatcher`)
and per-address connection goroutines (`resetTransport`) using a
buffered channel (`notifyCh`, capacity 1) and a read-write mutex (`mu`).
Under a specific interleaving, `lbWatcher` calls a `down()` callback
that acquires `mu` and then attempts to send on `notifyCh` -- but
`lbWatcher` is the sole reader of `notifyCh`, and the buffer is already
full from a concurrent `Up()` call. The result is a circular wait:
the goroutine holding the mutex blocks on a channel send whose only
consumer is itself. All other goroutines needing the mutex -- including
`Close()` -- are blocked behind it.

The fix in etcd restructured the `down()` callback to avoid sending on
`notifyCh` while holding `mu`, eliminating the cross-primitive
dependency.

This bug is classified as **Blocking / Mixed Deadlock / Channel & Lock**
in the [GoBench](https://github.com/timmyyuan/gobench) benchmark suite.
No existing Go concurrency analysis tool detects it: Goleak detects
goroutine leaks, not deadlocks. GCatch models channel operations but
does not track cross-primitive dependencies with mutexes. GFuzz fuzzes
scheduling but lacks the systematic coverage to reliably reach depth-3
interleavings. GoAT relies on happens-before traces that do not capture
the channel-buffer-full blocking condition. The entire Channel & Lock
mixed deadlock category has less than 54% recall for every tool in the
GoBench evaluation.

Our system detects it in 74 runs under CHESS with context bound K=3,
and in a median of ~38 runs under randomized strategies.

---

## 2. Kernel Structure

The kernel code is at `bugs/gobench/etcd7443/etcd7443.go`. It distills
the essential types, goroutine topology, and synchronization structure
from etcd's gRPC client connection pool into 221 lines.

### 2.1 Types

**`simpleBalancer`** -- the load balancer:

```go
type simpleBalancer struct {
    addrs    []Address            // full address list: [0, 1, 2]
    notifyCh chan []Address        // buffered, capacity 1
    mu       sync.RWMutex         // guards pinAddr and closed
    closed   bool
    pinAddr  Address              // currently pinned address (0 = none)
}
```

`notifyCh` is the sole communication channel between producers
(`Up()`/`down()`) and the consumer (`lbWatcher`). Its capacity of 1
is critical: a single unread send fills the buffer, and the next send
blocks.

**`ClientConn`** -- the client connection:

```go
type ClientConn struct {
    dopts dialOptions
    mu    sync.RWMutex           // guards conns map
    conns map[Address]*addrConn
}
```

**`addrConn`** -- a per-address connection:

```go
type addrConn struct {
    mu    sync.Mutex
    cc    *ClientConn
    addr  Address
    dopts dialOptions
    down  func()                 // teardown callback returned by Up()
}
```

The `down` field is a closure returned by `simpleBalancer.Up()`. It
captures references to the balancer's `mu` and `notifyCh`. This closure
is the bridge between the mutex world and the channel world -- and the
source of the cross-primitive dependency that causes the deadlock.

### 2.2 The Up() / down() Callbacks

`Up()` is called by `resetTransport` when a connection is established.
It returns a `down()` closure called when the connection is torn down:

```go
func (b *simpleBalancer) Up(addr Address) func() {
    b.mu.Lock()
    defer b.mu.Unlock()
    if b.closed { return func() {} }
    if b.pinAddr == 0 {
        b.pinAddr = addr
        b.notifyCh <- []Address{addr}   // sends while holding mu
    }
    return func() {
        defer func() { if r := recover(); r != nil { return } }()
        b.mu.Lock()
        defer b.mu.Unlock()
        if b.pinAddr == addr {
            b.pinAddr = 0
            b.notifyCh <- b.addrs       // sends while holding mu
        }
    }
}
```

Both `Up()` and its returned `down()` closure acquire `b.mu` and then
send on `b.notifyCh`. This is the fundamental design flaw: the mutex
is held across a potentially blocking channel send.

### 2.3 Goroutine Topology

The test creates up to 8 goroutines:

| BGID | Role | Spawned by | Entry point |
|------|------|------------|-------------|
| B1 | tRunner | runtime | test infrastructure |
| B2 | test root | tRunner | `Workload(t)` |
| B3 | sb.Close | B2 | `go func() { defer close(closec); sb.Close() }()` |
| B4 | conn.Close | B2 | `go conn.Close()` |
| B5 | lbWatcher | Dial | `go cc.lbWatcher()` |
| B6 | resetTransport(0) | lbWatcher via resetAddrConn | `go ac.resetTransport()` |
| B7 | resetTransport(1) | lbWatcher via resetAddrConn | `go ac.resetTransport()` |
| B8 | resetTransport(2) | lbWatcher via resetAddrConn | `go ac.resetTransport()` |

B6, B7, B8 are created only if `lbWatcher` (B5) processes the initial
notification `[0, 1, 2]` before `sb.Close()` closes the channel. This
gives the test a bimodal structure: the FIFO path terminates with 5
goroutines; the bug path requires all 8.

### 2.4 Test Setup

```go
func Workload(t *testing.T) {
    sb := newSimpleBalancer()       // notifyCh pre-loaded with [0,1,2]
    conn := Dial(WithBalancer(sb))  // spawns lbWatcher (B5)
    closec := make(chan struct{})
    go func() {                     // B3: sb.Close goroutine
        defer close(closec)
        sb.Close()
    }()
    go conn.Close()                 // B4: conn.Close goroutine
    <-closec                        // B2 blocks until B3 returns
}
```

At the first scheduling decision, three goroutines are runnable:
`[B5, B3, B4]`. B2 is blocked on `<-closec`. B1 is blocked waiting
for B2. The relative ordering of these three goroutines -- and the
subsequent ordering of B6, B7, B8 -- determines whether the program
deadlocks.

---

## 3. FIFO Path (Safe)

Run 1 in the CHESS exploration follows FIFO scheduling at every
decision point. It completes in 5 steps with 5 goroutines. B6, B7, B8
are spawned but never get a chance to call `Up()` before the balancer
is closed.

Trace excerpt (run 1):

```
Step 0: runq=[B5,B3,B4]  index=0  chose B5  (lbWatcher)
Step 1: runq=[B3,B4]     index=0  chose B3  (sb.Close)
Step 2: runq=[B4]         index=0  chose B4  (conn.Close)
Step 3: runq=[B2]         index=0  chose B2  (test root)
Step 4: runq=[B1]         index=0  chose B1  (tRunner)
```

**Step 0 -- B5 (lbWatcher) runs first.** B5 receives `[0, 1, 2]`
from the pre-loaded `notifyCh` buffer. Acquires `cc.mu`, adds addresses
0, 1, 2 to `cc.conns`, releases `cc.mu`. Calls `resetAddrConn(0)`,
`resetAddrConn(1)`, `resetAddrConn(2)` -- each creates an `addrConn`,
registers it in `cc.conns`, and spawns a `resetTransport` goroutine
(B6, B7, B8). B5 loops back to `for range notifyCh` and blocks (buffer
empty, channel open).

**Step 1 -- B3 (sb.Close) runs.** Acquires `b.mu`, sets `closed = true`,
calls `close(notifyCh)`, sets `pinAddr = 0`, releases `b.mu`. The
deferred `close(closec)` fires, waking B2.

The `close(notifyCh)` causes B5's `for range` loop to exit. B5 is now
effectively done.

**Step 2 -- B4 (conn.Close) runs.** Acquires `cc.mu`, takes the
`conns` map, sets `cc.conns = nil`, releases `cc.mu`. Calls
`sb.Close()` -- `closed` is already true, returns immediately. Iterates
over saved `conns`, calling `tearDown()` on each `addrConn`. Since
B6/B7/B8 haven't run `resetTransport()` yet, each `ac.down` is nil.
`tearDown()` is a no-op. B4 returns.

**Steps 3-4 -- Cleanup.** B2 returns from `<-closec`. B1 finishes.
B6/B7/B8 eventually run `Up()`, see `closed == true`, and return no-op
`down` functions. They exit cleanly.

The FIFO path is safe because `sb.Close()` runs before any
`resetTransport` goroutine calls `Up()`. With `closed == true`, `Up()`
short-circuits: no send on `notifyCh`, no meaningful `pinAddr` update,
and the returned `down()` is a no-op.

---

## 4. Deadlock Path -- Run 74

Run 74 is the unique failing run in the CHESS exploration. It has 7
decision points, involves all 8 goroutines, and ends in deadlock.
Three of its first three decisions are non-FIFO (consuming the full
context budget K=3); the remaining four are FIFO, cascading into the
deadlock.

Trace (run 74, `etcd7443-chess-trace.jsonl`):

```
Step 0: runq=[B5,B3,B4]        index=1  chose B3  (non-FIFO)
Step 1: runq=[B8,B4,B5,B6,B7]  index=3  chose B6  (non-FIFO)
Step 2: runq=[B3,B4,B5,B7,B8]  index=4  chose B8  (non-FIFO)
Step 3: runq=[B3,B4,B5,B7]     index=0  chose B3  (FIFO)
Step 4: runq=[B4,B5,B7]        index=0  chose B4  (FIFO)
Step 5: runq=[B5,B7]           index=0  chose B5  (FIFO)
Step 6: runq=[B7]              index=0  chose B7  (FIFO)
```

The three non-FIFO decisions each serve a specific role in constructing
the deadlock. They are not arbitrary reorderings -- each one builds a
precondition that the subsequent steps depend on.

### 4.1 Phase 1: Constructing the Trap (Steps 0-2, Non-FIFO)

**Step 0 -- Non-FIFO decision 1: Delay B5 so resetTransport goroutines
and Close goroutines coexist.**

The runq is `[B5, B3, B4]`. FIFO would pick B5 (lbWatcher), which
would process the initial notification, close the channel, and
terminate safely (as in run 1). Instead, the scheduler picks B3
(index=1).

But B3's quantum does not complete before B5 runs -- the step 1 runq
includes B6/B7/B8, which can only exist after B5 processes the `[0,1,2]`
notification. This tells us that between steps 0 and 1, B5 ran as a
deterministic single-goroutine quantum: B5 read `[0,1,2]`, spawned
B6/B7/B8, and blocked on the next `notifyCh` receive. B3 also ran its
preamble but yielded before completing `sb.Close()`.

The critical effect: B5 has spawned B6/B7/B8, but the balancer is NOT
closed. The `resetTransport` goroutines and the `Close` goroutines are
now coexisting in the runq -- a situation that never arises under FIFO
scheduling.

**Step 1 -- Non-FIFO decision 2: A resetTransport goroutine runs Up()
before Close.**

Runq is `[B8, B4, B5, B6, B7]` (5 alternatives). FIFO would pick B8.
The scheduler picks B6 (index=3) -- `resetTransport` for address 0.

B6 calls `Up(0)`:

```
b.mu.Lock()                        // uncontended, acquires
closed == false                    // sb.Close() hasn't run yet
pinAddr == 0                       // no address pinned (zero value)
pinAddr = Address(0)               // pin address 0
b.notifyCh <- []Address{0}         // buffer was empty, now FULL
b.mu.Unlock()
return down closure (captures addr=0)
```

The send wakes B5 (blocked on receive). B6 exits. After this step:

- `notifyCh` buffer: **full** (contains `[0]`)
- `pinAddr`: `Address(0)`
- B5 is now runnable (woken by the send)
- B6 is done

**Step 2 -- Non-FIFO decision 3: A second resetTransport fills the
buffer again.**

Runq is `[B3, B4, B5, B7, B8]` (5 alternatives). FIFO would pick B3.
The scheduler picks B8 (index=4) -- `resetTransport` for address 2.

B8 calls `Up(2)`:

```
b.mu.Lock()                        // uncontended, acquires
closed == false                    // still not closed
pinAddr == Address(0)              // already pinned to address 0
pinAddr != 0                       // conditional is FALSE
                                   // (pinAddr is Address(0), which == 0
                                   //  only if Address(0) equals the zero value)
```

Here the `Address` type semantics matter. `type Address int` means
`Address(0)` is the zero value. The check `if b.pinAddr == 0` in `Up()`
is intended to mean "no address pinned", but `Address(0)` is a valid
address AND equals 0. After `Up(0)` set `pinAddr = Address(0)`, the
next call `Up(2)` sees `pinAddr == 0` as TRUE (because `Address(0) == 0`
in Go), so it re-pins to address 2 and sends:

```
pinAddr = Address(2)               // overwrite pin to address 2
b.notifyCh <- []Address{2}         // buffer is FULL -- BLOCKS
```

B8 blocks on the send while **holding `b.mu`**. This is the critical
lock acquisition: `b.mu` is now held by a goroutine that cannot make
progress until someone reads from `notifyCh`.

After step 2:

- `notifyCh` buffer: full (contains `[0]`)
- `b.mu`: **held by B8** (blocked on send)
- B8: blocked on channel send, holding `b.mu`
- B5: runnable (woken by step 1's send)
- B3, B4, B7: runnable

### 4.2 Phase 2: The Cascade (Steps 3-6, FIFO)

From this point, all decisions are FIFO. The deadlock emerges
inevitably from the state constructed by the three non-FIFO decisions.

**Step 3 -- B3 (sb.Close) runs.**

Runq is `[B3, B4, B5, B7]`. FIFO picks B3. B3 executes `sb.Close()`:

```
b.mu.Lock()    // B8 holds b.mu -- B3 BLOCKS on the mutex
```

B3 is now blocked on `b.mu`. The deferred `close(closec)` never fires.
B2 (test root) remains blocked on `<-closec` indefinitely.

**Step 4 -- B4 (conn.Close) runs.**

Runq is `[B4, B5, B7]`. FIFO picks B4. B4 executes `conn.Close()`:

```
cc.mu.Lock()                   // uncontended, acquires
conns := cc.conns              // saves the conns map
cc.conns = nil                 // prevents future resetAddrConn
cc.mu.Unlock()
cc.dopts.balancer.Close()      // calls sb.Close()
  -> b.mu.Lock()               // B8 holds b.mu -- B4 BLOCKS
```

B4 nullifies `cc.conns` (this matters for B5's behavior later), then
blocks on `b.mu` inside the nested `sb.Close()` call.

**Step 5 -- B5 (lbWatcher) runs.**

Runq is `[B5, B7]`. FIFO picks B5. B5 was woken by step 1's send and
reads `[0]` from the notifyCh buffer:

```
addrs := <-notifyCh            // reads [0], buffer now EMPTY
```

Reading from the buffer unblocks B8's pending send. B8's `[2]` enters
the buffer. B8 resumes:

```
// B8 continues in Up(2):
b.mu.Unlock()                  // releases b.mu
return down closure            // B8 exits
```

B8 releasing `b.mu` wakes one mutex waiter (B3 or B4). Meanwhile, B5
continues processing the `[0]` notification:

```
cc.mu.Lock()
// cc.conns was set to nil by B4 in step 4
// scanning nil map: no adds, no deletes
cc.mu.Unlock()
cc.resetAddrConn(Address(0))
  -> cc.mu.Lock()
  -> cc.conns == nil            // TRUE, early return
```

B5 loops back to `for range notifyCh`. Reads `[2]` from the buffer
(B8's send). Processes similarly -- `cc.conns` is nil, short-circuits.
Loops back. The channel may now be closed (if B3 acquired `b.mu` and
ran `close(notifyCh)` during B5's processing) or still open.

Between decision points, the goroutines unblocked by B8's `mu.Unlock()`
and B5's channel read run deterministically. The sequence resolves:
B8 exits, B3 or B4 acquires `b.mu`, and eventually B5 blocks or exits.
The trace shows that by step 6, the runq is `[B7]` -- meaning all
other goroutines have either completed or are durably blocked.

**Step 6 -- B7 (resetTransport(1)) runs.**

Runq is `[B7]`. Sole goroutine. B7 executes `resetTransport(1)`:

```
ac.mu.Lock()
ac.down = ac.cc.dopts.balancer.Up(ac.addr)    // Up(1)
  -> b.mu.Lock()               // contention depends on state
```

After B7's quantum, no goroutines remain runnable. The bubble detects
that all goroutines are durably blocked (some on `b.mu`, some on
channel operations, B2 on `<-closec`). **Deadlock.**

### 4.3 The Deadlock State

At termination, the blocked goroutines form a circular dependency:

```
B5 (lbWatcher):  blocked on notifyCh receive  OR  blocked on b.mu inside down()
B8:              completed (exited after Up(2))
B3 (sb.Close):   blocked on b.mu
B4 (conn.Close): blocked on b.mu (inside nested sb.Close)
B7 (resetTransport(1)): blocked on b.mu (inside Up(1))
B2 (test root):  blocked on <-closec (waiting for B3 to return)
B1 (tRunner):    blocked waiting for B2
```

The precise deadlock state depends on the ordering of intermediate
deterministic quanta between steps 5 and 6. In the general mechanism
(Section 5), the deadlock is always the same circular wait.

---

## 5. The Circular Wait

The deadlock is a textbook circular wait across two synchronization
primitives -- a mutex and a buffered channel:

```
lbWatcher (B5)                    resetTransport (B8)
    |                                   |
    | holds b.mu (inside down())        | blocked on b.mu (inside Up())
    | blocked on notifyCh send          |
    |                                   |
    +---- notifyCh buffer FULL <--------+
                 |
                 | only reader is lbWatcher
                 | lbWatcher can't read because it's inside down()
                 | down() can't return because the send blocks
                 v
             DEADLOCK
```

The circular dependency has four links:

1. **lbWatcher holds `b.mu`** -- acquired inside the `down()` closure
   during `tearDown()` processing.

2. **lbWatcher blocks on `notifyCh <- b.addrs`** -- the buffer is full
   because a concurrent `Up()` call already sent a notification.

3. **notifyCh can only be drained by lbWatcher** -- it is the sole
   goroutine executing `for range notifyCh`. No other goroutine reads
   from this channel.

4. **All other goroutines block on `b.mu`** -- `sb.Close()`,
   `conn.Close()` (which calls `sb.Close()`), and remaining
   `resetTransport` goroutines all need `b.mu` to proceed. Since
   lbWatcher's `down()` closure holds it, they are all stuck.

The circularity is unrecoverable because:

- lbWatcher cannot release `b.mu` until `down()` returns.
- `down()` cannot return until the channel send completes.
- The channel send cannot complete until someone reads from `notifyCh`.
- The only reader (`lbWatcher`) is stuck in `down()`.

This is a **mixed deadlock**: neither the mutex alone nor the channel
alone creates a cycle. The cycle exists only in the cross-product of
both primitives' wait-for graphs. This is precisely why tools that
analyze channels and locks independently (GCatch for channels, go vet
for locks) miss it.

---

## 6. CHESS Exploration

### 6.1 Tree Structure

The CHESS exploration with context bound K=3 produces a search tree of
74 nodes. The tree is rooted at the FIFO trace (run 1) and branches
at each decision point that has alternatives.

Data source: `charts/data/etcd7443-chess-tree.json`.

**Depth distribution:**

| Depth (K) | Runs | Cumulative | Passed | Failed |
|-----------|------|------------|--------|--------|
| 0 | 1 | 1 | 1 | 0 |
| 1 | 2 | 3 | 2 | 0 |
| 2 | 13 | 16 | 13 | 0 |
| 3 | 58 | 74 | 57 | **1** |

The single failing run (run 74) is at depth 3 -- the maximum allowed
by the context bound.

### 6.2 FIFO Trace Analysis

Run 1 (the FIFO trace, depth 0) has 5 steps and 2 branch points
(steps where `alternatives > 1`):

```
Step 0: runq=[B5,B3,B4]  3 alternatives  <-- branch point
Step 1: runq=[B3,B4]     2 alternatives  <-- branch point
Step 2: runq=[B4]         1 alternative
Step 3: runq=[B2]         1 alternative
Step 4: runq=[B1]         1 alternative
```

The 2 branch points at depth 0 generate 2 children at depth 1 (each
using one non-FIFO decision). From there, deeper explorations branch
further. Steps with only 1 alternative are deterministic and do not
contribute to branching.

### 6.3 Bug Trace Analysis

Run 74 (the failing trace, depth 3) has 7 steps. All three non-FIFO
decisions occur in the first three steps:

```
Step 0: 3 alternatives, chose index 1 (non-FIFO) -- B3 instead of B5
Step 1: 5 alternatives, chose index 3 (non-FIFO) -- B6 instead of B8
Step 2: 5 alternatives, chose index 4 (non-FIFO) -- B8 instead of B3
Step 3: 4 alternatives, chose index 0 (FIFO)
Step 4: 3 alternatives, chose index 0 (FIFO)
Step 5: 2 alternatives, chose index 0 (FIFO)
Step 6: 1 alternative,  chose index 0 (FIFO)
```

The non-FIFO decisions at steps 0-2 consume the entire context budget.
Steps 3-6 are forced to be FIFO. The deadlock emerges from the FIFO
continuation of the state constructed by the three non-FIFO decisions.

### 6.4 Why 74 Runs

CHESS performs iterative deepening: it exhaustively explores all traces
at depth K=0, then K=1, then K=2, then K=3.

- **K=0** (1 run): The FIFO trace. 5 steps, 2 branch points. Passes.

- **K=1** (2 runs): Each of the 2 branch points generates at most
  `alternatives - 1` children. Run 2 branches at step 0 (index=2,
  chose B4 instead of B5). Run 50 branches at step 0 (index=1, chose
  B3). Both pass. The different choice at step 0 changes the goroutine
  topology: choosing B3 or B4 first means B5 (lbWatcher) runs later,
  spawning B6/B7/B8 and creating a richer state space.

- **K=2** (13 runs): Runs 3-49 and 51, 73. These are traces with 2
  non-FIFO decisions. They explore various orderings of the Close and
  resetTransport goroutines but never achieve the specific three-step
  setup required for the deadlock. All pass.

- **K=3** (58 runs): Runs 4-18 (children of depth-2 parents), runs
  20-31, 34-48, 52-72, and run 74. These explore all three-non-FIFO-
  decision traces. 57 pass. Run 74 -- the last one explored -- fails.

The bug is at the boundary of the search space. CHESS must exhaust all
74 traces to find it. Every trace at depths 0, 1, and 2 is safe.

### 6.5 Ancestry of Run 74

The failing trace's ancestry in the search tree:

```
Run 1  (depth 0)  FIFO baseline
  -> Run 50 (depth 1)  branch at step 0, index=1 (chose B3)
    -> Run 72 (depth 2)  branch at step 8, (deep variation)
      -> Run 74 (depth 3)  branch at step 2, index=4 (chose B8) -- DEADLOCK
```

Run 74's parent is run 72 (Parent=72 in the tree data). Run 72 is at
depth 2 (NonFIFO=2) and branches at step 8. Run 74 introduces a third
non-FIFO decision at step 2 (BranchStep=2). This additional non-FIFO
decision at step 2 is the one that causes a resetTransport goroutine
to run `Up()` before the Close goroutines can shut down the balancer.

---

## 7. Strategy Comparison

### 7.1 Seed Sweep Results

Data source: `charts/data/seed-sweep.json`. Each randomized strategy
was run with 100 seeds. CHESS is deterministic (seed-independent).

| Strategy | Runs to bug | Min | Max | Deterministic? |
|----------|-------------|-----|-----|----------------|
| **CHESS K=3** | **74** | 74 | 74 | Yes |
| **Random** | avg 41 | 1 | 111 | No |
| **PCT-d2** | avg 38 | 1 | 83 | No |
| **PCT-d3** | avg 38 | 1 | 83 | No |

### 7.2 CHESS Analysis

CHESS explores 74 traces deterministically. The bug is always found on
the 74th (and last) trace. This is the worst case for CHESS: the bug
lives at the maximum depth (K=3) and is the last trace explored at that
depth.

The strength of CHESS is the guarantee: with K=3, it WILL find the bug
in exactly 74 runs. No seed dependence, no probability, no missed runs.
The cost is that it must explore all 73 safe traces first.

### 7.3 Random Walk Analysis

Random scheduling picks uniformly at each decision point. The bug
requires three specific non-FIFO decisions:

- Step 0: 1 correct choice out of 3 alternatives (index 1)
- Step 1: 1 correct choice out of 5 alternatives (index 3)
- Step 2: 1 correct choice out of 5 alternatives (index 4)

If the choices were independent, P(bug) = 1/3 x 1/5 x 1/5 = 1/75.
Expected runs: 75. The observed average of 41 is lower because some
interleavings that differ from the exact run-74 trace also trigger the
deadlock (the deadlock condition can be reached through multiple
scheduling paths). The minimum of 1 (lucky seed) and maximum of 111
show the high variance inherent in random search.

### 7.4 PCT Analysis

PCT (Probabilistic Concurrency Testing) assigns random priorities to
goroutines and changes priorities at d-1 randomly chosen steps. For
this bug:

- **PCT-d2** (1 priority change point): The theoretical lower bound
  is P >= 1/(N * k^1) where N is the number of goroutines and k is
  the number of steps. With N=8, k~7: P >= 1/56. Observed average: 38
  runs. PCT-d2 can find depth-3 bugs because priority assignments
  can accidentally produce the right ordering without explicitly
  targeting 3 change points.

- **PCT-d3** (2 priority change points): P >= 1/(N * k^2) = 1/(8 * 49)
  = 1/392. This bound is loose; the observed average of 38 runs is
  much better. The reason: the bug does not require precisely placed
  priority changes -- it requires a global ordering where certain
  goroutines precede others, which many priority assignments achieve.

The identical performance of PCT-d2 and PCT-d3 (avg 38, range 1-83)
suggests that for this bug, the depth parameter has minimal impact.
The bug is more sensitive to the initial priority assignment than to
the placement of priority change points.

### 7.5 Strategy Comparison Summary

| Property | CHESS K=3 | Random | PCT-d2 | PCT-d3 |
|----------|-----------|--------|--------|--------|
| Guarantee | Certain at K>=3 | Probabilistic | Probabilistic | Probabilistic |
| Worst case | 74 runs | unbounded | unbounded | unbounded |
| Average | 74 runs | 41 runs | 38 runs | 38 runs |
| Best case | 74 runs | 1 run | 1 run | 1 run |
| Useful for | Verification | Quick testing | Quick testing | Quick testing |

For a one-shot "find the bug fast" scenario, Random and PCT are
preferable (expected ~40 runs vs. CHESS's fixed 74). For a "prove no
bugs exist at depth <= K" scenario, CHESS is the only option: it
provides a completeness guarantee that randomized strategies cannot.

---

## 8. Why This Bug Is Hard

### 8.1 Depth 3

The bug requires exactly 3 non-FIFO scheduling decisions. Removing any
one of them causes the execution to terminate safely:

1. **Without non-FIFO decision 1** (step 0): lbWatcher runs first,
   processes `[0,1,2]`, and the channel is closed before any
   `resetTransport` calls `Up()`. No cross-primitive interaction.

2. **Without non-FIFO decision 2** (step 1): No `resetTransport`
   goroutine calls `Up()` with `pinAddr == 0`, so the buffer is never
   filled by an `Up()` call. `down()` either finds `pinAddr != addr`
   (no send) or sends into an empty buffer (non-blocking).

3. **Without non-FIFO decision 3** (step 2): The second `Up()` call
   doesn't happen, so the buffer isn't full when `down()` tries to
   send. Or `Close()` runs before the second `Up()`, preventing the
   lock-channel overlap.

Context bound K=2 is insufficient. The CHESS exploration exhaustively
proves this: all 16 runs at depths 0-2 pass. The DFS state space
tables (from `docs/research/algorithm-comparison.md`) confirm the
general principle:

| Context Bound | Traces Explored | Bug Found? |
|---------------|-----------------|------------|
| K=0 | 1 | No |
| K=1 | 3 | No |
| K=2 | 16 | No |
| K=3 | 74 | **Yes** |

### 8.2 Mixed Primitive Dependency

The deadlock involves two distinct synchronization primitives:

- **`sync.RWMutex`** (`b.mu`): held by the `down()` closure
- **Buffered channel** (`notifyCh`): blocked send inside `down()`

Neither primitive alone creates a cycle:

- The mutex has no cycle: there is only one mutex (`b.mu`), and no
  goroutine acquires it twice.
- The channel has no cycle: there is only one channel (`notifyCh`),
  and no goroutine both sends and receives on it... except that the
  `down()` closure sends while being called FROM `lbWatcher`, which is
  the reader. The channel "cycle" is indirect: the reader calls a
  function that sends.

The cycle exists only in the **cross-product** of both primitives' wait
graphs. This is why single-primitive analysis tools fail:

| Tool | What it analyzes | Why it misses this bug |
|------|-----------------|----------------------|
| **Goleak** | Goroutine leak detection | Detects leaked goroutines, not deadlocks |
| **GCatch** | Channel operations via static analysis | Does not model mutex interactions with channel sends |
| **GFuzz** | Scheduling fuzzing | Insufficient coverage for depth-3 interleavings |
| **GoAT** | Happens-before traces | Does not model buffer-full blocking condition |
| **go vet** | Lock ordering analysis | Sees only one mutex; no cycle in lock graph |

### 8.3 The Address(0) Ambiguity

A subtle aspect of the bug: `type Address int` means `Address(0)` is
both a valid address and the zero value. The `Up()` function uses
`b.pinAddr == 0` to mean "no address pinned", but after `Up(0)` sets
`pinAddr = Address(0)`, the condition `pinAddr == 0` is still true.
This allows a second `Up()` call to overwrite the pin and send another
notification, even though an address is already pinned.

This is not a separate bug but an amplifier: it makes the buffer-filling
scenario easier to trigger because multiple `Up()` calls can each send
a notification (instead of only the first one).

### 8.4 Self-Referential Channel Pattern

The root cause is a self-referential channel pattern: the sole reader
of `notifyCh` (lbWatcher) calls code that sends on `notifyCh` (the
`down()` closure). In the general case, this pattern is safe if:

1. The buffer is never full when `down()` sends, OR
2. Another goroutine can drain the buffer.

Both conditions fail under the deadlock interleaving:

1. The buffer IS full (a concurrent `Up()` filled it).
2. No other goroutine reads from `notifyCh` (lbWatcher is the only
   reader, and it's stuck inside `down()`).

This self-referential pattern is particularly insidious because it is
invisible in the source code's type structure. The `down()` closure is
stored as a `func()` in `addrConn.down` -- there is no type-level
indication that calling it may send on the same channel that the
caller is reading from. The dependency is hidden in the closure's
captured environment.

---

## 9. Detection Mechanism

### 9.1 How the Bubble Detects Deadlock

The synctest bubble detects the deadlock through its **durable blocking**
mechanism. At every scheduling decision point, the bubble tracks
which goroutines are runnable and which are durably blocked. When
no goroutines are runnable, no timers are pending, and no external
operations are in flight, the bubble declares deadlock.

In run 74, after step 6 (B7 blocks on `b.mu`), the bubble's state is:

| Goroutine | State | Wait reason |
|-----------|-------|-------------|
| B1 | blocked | Waiting for B2 (test completion) |
| B2 | blocked | Channel receive (`<-closec`) |
| B3 | blocked | Mutex lock (`b.mu.Lock()`) |
| B4 | blocked | Mutex lock (`b.mu.Lock()`) |
| B5 | blocked | Channel receive (`for range notifyCh`) |
| B6 | exited | (completed after `Up(0)`) |
| B7 | blocked | Mutex lock (`b.mu.Lock()`) |
| B8 | exited | (completed after `Up(2)`) |

Runnable set: empty. Timers: none. External: 0. The bubble correctly
identifies this as deadlock.

### 9.2 Why All Blocking Primitives Must Be Durable

The deadlock involves three distinct blocking primitives:

1. **Buffered channel send** (`b.notifyCh <- b.addrs`): The Go runtime
   parks the goroutine on the channel's send wait queue. This is a
   durable blocking point -- the goroutine cannot make progress without
   external action (a receive from the channel).

2. **Mutex lock** (`b.mu.Lock()`): The Go runtime parks the goroutine
   on the semaphore underlying the mutex. This is a durable blocking
   point -- the goroutine cannot make progress until the current holder
   releases the mutex.

3. **Unbuffered channel receive** (`<-closec`): B2 is parked on the
   channel's receive wait queue. This is a durable blocking point.

For the bubble to detect the deadlock, ALL three primitive types must
be recognized as durable blocking points. If any one were treated as
non-durable (e.g., if mutex contention were treated as a spin rather
than a park), the bubble would think a goroutine might still make
progress and would not declare deadlock.

The synctest runtime modifications mark all three as durable:

- Channel send/receive: `runtime/chan.go` -- `gopark` with bubble-aware
  wait reason
- Mutex: `sync.Mutex.Lock()` -> `runtime_SemacquireMutex` -> `gopark`
  with `waitReasonSemacquire`
- WaitGroup: `sync.WaitGroup.Wait()` -> `runtime_Semacquire` ->
  `gopark` (same semaphore mechanism)

### 9.3 What the Test Observes

The test function is invoked via `explorer.Test`:

```go
func TestEtcd7443(t *testing.T) {
    explorer.Test(t, Workload, &explorer.CHESS{Bound: 3})
}
```

`explorer.Test` runs the `Workload` function inside a synctest bubble,
exploring interleavings with context bound K=3. On run 74, the bubble
reports deadlock (all goroutines durably blocked). The explorer records
this as `passed: false, user_failed: true` and reports the failing
trace.

The `user_failed: true` flag distinguishes application-level deadlock
(the test's goroutines are stuck) from infrastructure failure. The
explorer can then output the exact trace -- the 7-step sequence of
scheduling decisions -- that reproduces the deadlock deterministically.
This trace is replayable: feeding the same decision prefix to the
bubble produces the same deadlock every time (Theorem 2.1, determinism
guarantee).

### 9.4 Comparison with Production Manifestation

In production etcd, this bug manifests as a hung gRPC connection that
never recovers. The client's `lbWatcher` goroutine is permanently stuck
inside `down()`, holding `b.mu`. All subsequent `Close()` calls block
on the mutex. The client connection is leaked. The only symptom is a
timeout at the application level -- there is no panic, no error message,
no log entry. The goroutine stack trace (if captured) shows `lbWatcher`
blocked on a channel send inside a mutex-protected section, but
connecting this to the root cause requires understanding the full
interleaving that produced the state.

Our system produces the exact interleaving (7 steps, 3 non-FIFO
decisions) in 27 microseconds of simulated time. The trace is
deterministically replayable and contains the complete scheduling
history needed to understand the bug.

---

## 10. Summary

| Property | Value |
|----------|-------|
| **Bug class** | Blocking / Mixed Deadlock / Channel & Lock |
| **Source** | etcd/etcd#7443 (gRPC client load balancer) |
| **Goroutines** | 8 (test root, lbWatcher, 2 Close, 3 resetTransport, tRunner) |
| **Primitives** | `sync.RWMutex` + buffered channel (cap 1) |
| **Bug depth** | 3 non-FIFO scheduling decisions |
| **CHESS runs** | 74 (deterministic, K=3) |
| **Random runs** | avg 41, min 1, max 111 (100 seeds) |
| **PCT-d2 runs** | avg 38, min 1, max 83 (100 seeds) |
| **PCT-d3 runs** | avg 38, min 1, max 83 (100 seeds) |
| **Existing tools** | 0/4 detect (Goleak, GCatch, GFuzz, GoAT all miss) |
| **Detection time** | ~27 microseconds simulated (run 74) |
| **Circular wait** | lbWatcher holds mu, blocks on channel send to self |
