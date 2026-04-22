# Depth-2 and Cross-Dimension Bug Examples

Microbenchmarks for the synctest interleaving explorer. Each example has
exactly one bug that requires multiple non-default decisions to trigger.
These demonstrate why context bound >= 2 and cross-dimension exploration
are necessary to find real concurrency bugs.

**Decision dimensions** (from model.md Section 6.3):
- **LOCAL (L)**: goroutine scheduling within a bubble (index into runq)
- **GLOBAL (G)**: message delivery order between bubbles (index into pending queue)
- **SELECT (S)**: case ordering in a Go select with multiple ready cases

**Default** = FIFO (index 0) for all dimensions. A **non-default decision**
has index != 0. The **context bound** limits non-default decisions per trace.

---

## Example L2: Bank Overdraft (Depth-2 Local)

**Pattern**: One node, three goroutines. Two non-FIFO scheduling decisions
at two consecutive yield points. Neither alone triggers the bug.

**Real-world pattern**: TOCTOU (time-of-check-to-time-of-use). Checking
inventory before purchase, file permissions before access, lock status
before acquiring. Maps to CVE-class filesystem, database, and API bugs.

### Pseudocode

```go
func Run() (balance int, count int) {
    acct := &Account{Balance: 100}
    gate := make(chan struct{})
    var wg sync.WaitGroup
    wg.Add(3)

    go func() {                              // W1, BGID 1 (deep in runq)
        defer wg.Done()
        if acct.Balance >= 60 {              // CHECK — stale if both run first
            <-gate                           // YIELD: block on approval
            acct.Balance -= 60               // ACT — uses stale check
            count++
        }
    }()

    go func() {                              // W2, BGID 2 (middle of runq)
        defer wg.Done()
        if acct.Balance >= 60 {              // CHECK
            <-gate                           // YIELD
            acct.Balance -= 60               // ACT
            count++
        }
    }()

    go func() {                              // Approver, BGID 3 (runnext = FIFO default)
        defer wg.Done()
        close(gate)                          // open gate for all waiters
    }()

    wg.Wait()
    return acct.Balance, count
}
```

### Why Depth 1 is Safe

**Only W1 before Approver** (non-default at decision 0): W1 checks
balance (100 >= 60), blocks on gate. Runq: [W2, Approver]. FIFO picks
Approver (runnext). Gate opens. W1 wakes, subtracts (balance=40). W2
runs, sees 40 < 60, fails. One withdrawal, balance=40. **Correct.**

**Only W2 before Approver** (non-default at decision 0, different index):
Same logic with roles swapped. Only one withdrawer passes the check
before Approver runs. **Correct.**

**Default execution**: Approver (runnext) runs first, opens gate. W1
runs, withdraws (balance=40). W2 runs, sees 40 < 60, fails. **Correct.**

### Triggering Trace (Depth 2)

```
Decision 0: runq=[Approver(runnext), W1, W2]
   pick index=1 (W1)                         ← NON-DEFAULT #1
   W1: Balance=100 >= 60 → true, blocks on gate
Decision 1: runq=[Approver(runnext), W2]
   pick index=1 (W2)                         ← NON-DEFAULT #2
   W2: Balance=100 >= 60 → true, blocks on gate
Decision 2: runq=[Approver]                  (forced, only choice)
   Approver: close(gate) → wakes W1, W2
Decision 3: runq=[W1, W2]
   W1: Balance -= 60 → Balance=40
Decision 4: runq=[W2]
   W2: Balance -= 60 → Balance=-20           ← BUG: overdraft
```

Both withdrawers pass the check with stale balance before Approver opens
the gate. Requires two non-FIFO decisions pushing Approver later twice.

### State Space

| Bound | Interleavings explored | Bug found? |
|-------|-----------------------|------------|
| 0 | 1 | No |
| 1 | 5 | No |
| **2** | **12** | **Yes** |
| 3 | 12 | Yes (already found) |

### Best Algorithm

**DFS with context bound 2** is optimal. The two non-FIFO decisions are
at the first two decision points, so DFS finds the bug on its first
depth-2 exploration. PCT with 2 priority change points at positions 0 and
1 has probability ~1/n^2 per run. POS does not help because the two
decisions are on independent goroutines with no causal dependency to
exploit.

---

## Example G2: Primary Failover with Stale Read (Depth-2 Global)

**Pattern**: Three nodes (Primary, Backup, Client). Two independent
message reorderings needed. Either reordering alone is safe because the
other ordering acts as a safety net.

**Real-world pattern**: Split-brain after primary failure. Found in
Redis Sentinel failover, PostgreSQL streaming replication, MongoDB
replica set elections. The fix: read from a quorum, or use read leases
tied to replication lag.

### Pseudocode

```go
// --- Primary (P) ---
// Replicates writes to Backup, then crashes.
func Primary(t Transport) {
    t.Send("B", Write{key: "x", val: 1})      // first write
    t.Send("B", Write{key: "x", val: 2})      // second write
    // P crashes after sending both
}

// --- Backup (B) ---
// Applies writes, accepts promotion, serves reads.
func Backup(t Transport) {
    state := map[string]int{}
    promoted := false
    for {
        select {
        case msg := <-t.RecvWrite():           // ← P: writes
            state[msg.Key] = msg.Val
        case <-t.RecvPromote():                // ← monitor: promote signal
            promoted = true
        case req := <-t.RecvRead():            // ← C: read request
            if promoted {
                t.Send("C", Reply{val: state[req.Key]})
            } else {
                t.Send("C", Reply{val: -1, err: "not primary"})
            }
        }
    }
}

// --- Client (C) ---
// Reads from Backup after failover. Retries on "not primary".
func Client(t Transport) {
    time.Sleep(2 * time.Second)                // wait for failover
    for {
        t.Send("B", Read{key: "x"})
        reply := t.Recv()
        if reply.Err == "not primary" {
            time.Sleep(100 * time.Millisecond) // retry
            continue
        }
        if reply.Val != 2 {
            panic("BUG: stale read")           // expected latest value
        }
        break
    }
}
```

### Why Depth 1 is Safe

**Only reorder #1** (Promote before Write(x=2)): B is promoted with
state[x]=1 (missing second write). But Write(x=2) is still pending.
Default FIFO delivers Write(x=2) next. B updates state[x]=2. Client
reads later: x=2. **Correct** — the pending write acts as a safety net.

**Only reorder #2** (Client Read before Write(x=2)): Under default
ordering, Promote arrives AFTER both writes. So when Client's Read
arrives early, B rejects it with "not primary" (not yet promoted).
The client sees an error, not stale data. Default ordering: Write(x=1),
Write(x=2), Promote, Read. Client gets x=2. **Correct** — B isn't
promoted yet, so the early read is rejected, not served with stale data.

**Default execution**: Write(x=1), Write(x=2), Promote, Read. B has
x=2 when promoted. Client reads x=2. **Correct.**

### Triggering Trace (Depth 2)

```
Round 0: P sends Write(x=1)→B, Write(x=2)→B. P crashes.
Round 1: Monitor sends Promote→B.
Round 2: Pending = [Write(x=1)→B, Write(x=2)→B, Promote→B].
         Deliver Write(x=1)→B (default, index=0). B: state[x]=1.
Round 3: Pending = [Write(x=2)→B, Promote→B].
         Deliver Promote→B (NON-DEFAULT #1, index=1).
         B: promoted=true, state[x]=1.
Round 4: Client sends Read(x)→B.
         Pending = [Write(x=2)→B, Read(x)→B].
         Deliver Read(x)→B (NON-DEFAULT #2, index=1).
         B: promoted=true, replies state[x]=1.
Round 5: Client receives 1, expects 2. BUG.
```

The two reorderings create a window: Promote arrives before the last
write (reorder #1), AND the client reads before the last write catches
up (reorder #2). Neither alone creates the inconsistency.

### State Space

| Bound | Interleavings | Bug found? | Why |
|-------|--------------|------------|-----|
| 0 | 1 | No | FIFO: all writes arrive before promote/read |
| 1 | O(n) | No | One reorder leaves the other safety net intact |
| **2** | **O(n^2)** | **Yes** | Both promote-before-write AND read-before-write |
| 3 | O(n^3) | Yes (already found) |

### Best Algorithm

**DFS with context bound 2** explores all pairs of non-default deliveries.
The two critical decisions are at rounds 3 and 4, which are adjacent.
DFS finds this efficiently by branching at round 3 (promote early), then
branching again at round 4 (read before write).

PCT with 2 priority change points works but has ~1/n^2 probability per
run. POS could help by recognizing that Promote and Write(x=2) are
concurrent (no causal order), reducing the effective search space.

---

## Example GL1: Limit Race (Cross-Dimension: 1 Global + 1 Local)

**Pattern**: Two nodes, one bubble. One non-default GLOBAL decision
(message delivery order) and one non-default LOCAL decision (goroutine
scheduling within the bubble). Neither alone triggers the bug.

**Real-world pattern**: Configuration update race. A security policy
change (rate limit, permission) is applied while a request is in flight.
The request observes a partially-applied configuration. Found in API
gateways, firewalls, database permission systems.

### Pseudocode

```go
// --- Node A (bank with configurable withdrawal limit) ---
func NodeA(transport Transport) {
    balance := 100
    limit := 100                               // max withdrawal amount
    gate := make(chan struct{})

    go func() {                                // G_withdraw (BGID 1)
        <-gate                                 // wait for admin to signal
        if balance >= limit {                  // check limit
            balance -= limit                   // withdraw
        }
    }()

    go func() {                                // G_admin (BGID 2, runnext)
        msg := transport.Recv()                // ExternalWait: admin message
        if msg.Type == "raise_limit" {
            limit = 200                        // raise limit to 200
        }
        close(gate)                            // signal withdrawal to proceed
        runtime.Gosched()                      // YIELD: G_withdraw is runnext, G_admin in runq
        limit = 100                            // restore default limit
    }()
}

// Node B sends: "no_change" (benign admin command)
// Node C sends: "raise_limit" (raises limit to 200)
// Both go to the same transport channel. G_admin receives whichever
// the orchestrator delivers first (GLOBAL decision).
```

**Invariant**: When limit is raised to 200, withdrawal should only
proceed if balance >= 200. With balance=100, the withdrawal must be
**rejected** when limit=200.

### Why Each Depth-1 Variation is Safe

All four combinations of global x local:

| Global decision | Local decision | limit at check | Withdraw? | Correct? |
|-----------------|----------------|----------------|-----------|----------|
| B: no\_change (default) | FIFO: G\_withdraw first | 100 | Yes (100>=100) | **Yes** |
| B: no\_change (default) | non-FIFO: G\_admin first | 100 | Yes (100>=100) | **Yes** |
| C: raise\_limit (non-default) | FIFO: G\_withdraw first | 200 | No (100<200) | **Yes** |
| C: raise\_limit (non-default) | non-FIFO: G\_admin first | 100 | Yes (100>=100) | **BUG** |

**Global non-default alone** (C's message + FIFO local): limit set to
200. After Gosched, FIFO picks G_withdraw (runnext). G_withdraw checks:
100 >= 200? No. Withdrawal rejected. **Correct** — the higher limit
correctly prevents the insufficient withdrawal.

**Local non-default alone** (B's message + non-FIFO local): limit stays
100 (no change). G_admin restores limit=100 (no-op). G_withdraw checks:
100 >= 100, withdraws. **Correct** — limit was never changed.

**Both non-default** (C's message + non-FIFO local): limit set to 200.
After Gosched, non-FIFO picks G_admin first. G_admin restores limit=100.
THEN G_withdraw runs: 100 >= 100, withdraws. **BUG** — the limit was
raised to 200 to prevent this withdrawal, but the premature restore
negated the safety check.

### Triggering Trace (Depth 2)

```
Round 1: Pending = [B:"no_change", C:"raise_limit"].
         Deliver C:"raise_limit" (NON-DEFAULT GLOBAL, index=1).
         G_admin wakes: limit=200.
         close(gate) → G_withdraw wakes (enters runnext).
         Gosched → G_admin enters runq tail.
         Local decision: runq = [G_withdraw(runnext), G_admin(tail)]
         Pick G_admin (NON-DEFAULT LOCAL, index=1).
         G_admin: limit=100 (restore).
         G_withdraw: 100 >= 100 → withdraw. balance=0.  ← BUG
```

### State Space

| Bound | Interleavings | Bug found? | Why |
|-------|--------------|------------|-----|
| 0 | 1 | No | Default msg + FIFO scheduling |
| 1 | O(g+m) | No | Global OR local non-default alone, never both |
| **2** | **O(g*m)** | **Yes** | Cross-dimensional: one global + one local |
| 3 | O((g+m)^3) | Yes (already found) |

where g = local branching points, m = global delivery alternatives.

### Best Algorithm

**DFS with cross-dimensional context bound 2** finds this by first
branching on the global delivery (C's message), then branching on the
local scheduling within the resulting execution. This requires the
explorer to track non-default decisions ACROSS both dimensions in a
single bound counter.

PCT does not naturally handle cross-dimensional bugs — it randomizes
within one dimension. A cross-dimensional PCT variant would need to
assign priorities across both message deliveries and goroutine scheduling.

POS helps if it recognizes the causal chain: C's message -> limit change
-> scheduling race. But the link between global and local dimensions is
indirect (state-mediated, not message-mediated), so POS may miss it.

---

## Example GS1: Cancel vs Data Select Race (Cross-Dimension: 1 Global + 1 Select)

**Pattern**: Two nodes, one bubble. One non-default GLOBAL decision
(which message is delivered) and one non-default SELECT decision (which
case wins when two are ready). Neither alone triggers the bug.

**Real-world pattern**: Event handler with priority inversion. A node
receives events on multiple channels and uses `select` to multiplex.
The select's random case ordering can cause a low-priority event to
preempt a high-priority one. Found in Go network servers, event loops,
message brokers (hashicorp/raft's leaderLoop has exactly this pattern).

### Pseudocode

```go
// --- Node A ---
func NodeA(transport Transport) {
    cancelCh := make(chan struct{})
    dataCh := make(chan int, 1)                 // BUFFERED: data always ready
    result := ""

    // Receiver: gets external message, may close cancelCh, always produces data
    go func() {                                // G1, BGID 1
        msg := transport.Recv()                // ExternalWait
        dataCh <- 42                           // always buffer data (non-blocking)
        if msg.Type == "cancel" {
            close(cancelCh)                    // cancel signal
        }
    }()

    // Handler: selects between data and cancel
    go func() {                                // G2, BGID 2 (runnext)
        transport.Recv()                       // ExternalWait: "ready" signal
        select {                               // SELECT decision point
        case <-cancelCh:                       // case 0 (default: wins)
            result = "cancelled"
        case v := <-dataCh:                    // case 1
            result = fmt.Sprintf("data:%d", v)
        }
    }()
}

// Node B sends two messages to A's transport (same channel):
//   1. Either Proceed{Type: "proceed"} or Cancel{Type: "cancel"} → G1
//   2. Ready{Type: "ready"} → G2
// The orchestrator delivers them one at a time. G1 gets the first,
// G2 gets the second. The GLOBAL decision is which message is first
// in the pending queue (proceed vs cancel).
//
// Sequence: G1 receives msg, buffers data on dataCh, optionally
// closes cancelCh. Bubble goes idle. G2 receives "ready", enters select.
// By the time G2's select fires, dataCh always has a value (buffered).
// cancelCh is ready ONLY if G1 received "cancel".
```

**Invariant**: If a cancel message was delivered, result must be
"cancelled". Cancel has priority (listed first in select).

### How the Select Works

G1 always runs first (receives first message). G1 buffers data on dataCh
(non-blocking, buffered channel) and optionally closes cancelCh. G1
finishes. Bubble goes idle. Second message ("ready") is delivered. G2
wakes, enters select.

If G1 received "proceed": dataCh has a value, cancelCh is NOT closed.
Select has **one ready case** (dataCh). The select is **forced** — no
SELECT decision. result = "data:42". Correct (no cancel sent).

If G1 received "cancel": dataCh has a value AND cancelCh is closed.
Select has **two ready cases**. The SELECT decision determines the
winner.
- Default select (case 0): cancelCh wins. result="cancelled". Correct.
- Non-default select (case 1): dataCh wins. result="data:42". **BUG** —
  cancel was sent but ignored.

### Why Each Depth-1 Variation is Safe

| Global decision | Select decision | cancelCh closed? | Cases ready | Result | Correct? |
|-----------------|-----------------|-------------------|------------|--------|----------|
| B: proceed (default) | (forced, 1 case) | No | dataCh only | "data:42" | **Yes** |
| C: cancel (non-default) | default (case 0) | Yes | both | "cancelled" | **Yes** |
| C: cancel (non-default) | non-default (case 1) | Yes | both | "data:42" | **BUG** |

**Global non-default alone** (cancel message + default select): cancelCh
closed. dataCh buffered. Two cases ready. Default select picks cancelCh
(case 0). result="cancelled". **Correct.**

**Select non-default alone** (proceed message + non-default select):
cancelCh not closed. Only dataCh ready. Select is forced (one case).
Non-default ordering has no effect. result="data:42". **Correct** (no
cancel was sent, so processing data is the right behavior).

**Both non-default** (cancel message + non-default select): cancelCh
closed. dataCh buffered. Two cases ready. Non-default select picks
dataCh (case 1). result="data:42". **BUG** — cancel was delivered but
the select chose the wrong case.

### Triggering Trace (Depth 2)

```
Round 1: Pending = [B:"proceed", C:"cancel"].
         Deliver C:"cancel" (NON-DEFAULT GLOBAL, index=1).
         G1 wakes: dataCh <- 42 (buffered), close(cancelCh).
         Bubble idle.
Round 2: Deliver "ready" to G2.
         G2 wakes, enters select.
         cancelCh ready (closed), dataCh ready (buffered value).
         SELECT picks case 1 / dataCh (NON-DEFAULT SELECT).
         result = "data:42".                     ← BUG: cancel ignored
```

### State Space

| Bound | Interleavings | Bug found? | Why |
|-------|--------------|------------|-----|
| 0 | 1 | No | Default message, select forced |
| 1 | O(m+s) | No | Cancel alone: default select picks cancel. Non-default select alone: only one case ready |
| **2** | **O(m*s)** | **Yes** | Cancel message + non-default select case |
| 3 | O((m+s)^3) | Yes (already found) |

where m = message delivery alternatives, s = select case alternatives.

### Best Algorithm

**DFS with cross-dimensional context bound 2** explores global delivery
alternatives and, within each resulting execution, select case
alternatives. The explorer must count non-default decisions across
both the GLOBAL and SELECT dimensions in a single bound.

This example is directly inspired by hashicorp/raft's `leaderLoop`,
where `commitCh` (step-down) and `applyCh` (new request) race in a
select. The bug from BUGS.md (Bug #6: RemoveLeader Apply-vs-StepDown
Race) is exactly this pattern: the select ordering determines whether
a request is incorrectly accepted after the leader should have stepped
down.

---

## Example G3_partition: Stale Read After Partition Heal (3 Nodes)

**Pattern**: Three nodes. A network partition isolates one node, creating
stale state. After healing, a client read arrives before the catch-up
replication. Requires two non-default decisions: partition injection
(drop a message) and delivery reordering (read before catch-up).

**Real-world pattern**: Stale read after partition heal. Found by Jepsen
in MongoDB (stale reads from former primaries), Redis Cluster
(split-brain writes), etcd (linearizability violations during leader
election). The fix: quorum reads, read leases, or epoch verification.

### Pseudocode

```go
// --- Leader (A) ---
func Leader(t Transport) {
    t.Send("B", Write{key: "x", val: 1})      // replicate to B
    t.Send("C", Write{key: "x", val: 1})      // replicate to C (may be dropped)
    t.Recv()                                   // ack from B
    // If C is partitioned, ack times out.
    // After partition heals, send catch-up:
    t.Send("C", CatchUp{key: "x", val: 1})
}

// --- Follower C (partitioned then healed) ---
func FollowerC(t Transport) {
    state := map[string]int{"x": 0}           // initial state

    go func() {                                // Replication handler
        for {
            msg := t.Recv()                    // ExternalWait
            state[msg.Key] = msg.Val
        }
    }()

    go func() {                                // Client read handler
        req := t.RecvClient()                  // ExternalWait
        t.Send("client", Reply{val: state[req.Key]})
    }()
}

// --- Client ---
func Client(t Transport) {
    time.Sleep(2 * time.Second)
    t.Send("C", Read{key: "x"})
    reply := t.Recv()
    if reply.Val != 1 {
        panic("BUG: stale read from C")
    }
}
```

### Decision Model for Partitions

A **partition** is modeled as a global decision: the orchestrator chooses
to "drop" a message instead of "deliver." This extends the delivery
decision from "which pending message to deliver" to "deliver OR drop."
The default is always "deliver."

| Decision | Type | Default | Non-default |
|----------|------|---------|-------------|
| Write(x=1)→C delivery | Drop/Deliver | Deliver | Drop (partition) |
| CatchUp→C vs Read→C ordering | Delivery order | CatchUp first (FIFO) | Read first |

### Why Depth 1 is Safe

**Only partition** (drop Write to C, FIFO remaining): C stays at x=0.
CatchUp(x=1) is delivered next (FIFO). C updates: state["x"]=1. Client
reads: x=1. **Correct** — catch-up fixes the stale state before the
client reads.

**Only read reorder** (no partition, deliver Read before CatchUp):
Without partition, Write(x=1) was delivered to C normally. C has
state["x"]=1. Client reads: x=1. **Correct** — C is already up-to-date,
so read ordering doesn't matter.

### Triggering Trace (Depth 2)

```
Round 1-3: Initial setup. A sends Write(x=1) to B (delivered, acked).
Round 4: A sends Write(x=1) to C.
         NON-DEFAULT #1: DROP the message (partition injection).
         C stays at state["x"]=0.
Round 5: Partition heals. A sends CatchUp(x=1) to C.
         Client sends Read(x) to C.
         Pending = [CatchUp(x=1)→C, Read(x)→C].
         NON-DEFAULT #2: Deliver Read→C first (index=1).
         C's client handler: state["x"]=0. Replies val=0.
Round 6: Deliver CatchUp(x=1)→C. C updates state["x"]=1. Too late.

CLIENT: expected val=1, got val=0. BUG.
```

### State Space

| Bound | Interleavings | Bug found? | Why |
|-------|--------------|------------|-----|
| 0 | 1 | No | No partition, FIFO delivery |
| 1 | O(n) | No | Partition alone: catch-up fixes it. Reorder alone: already up-to-date |
| **2** | **O(n^2)** | **Yes** | Partition + stale read before catch-up |
| 3 | O(n^3) | Yes (already found) |

### Best Algorithm

**DFS with fault injection + delivery exploration** at context bound 2.
The explorer must treat partition injection (drop) as a decision alongside
delivery ordering, with "deliver FIFO" as the default action at each
delivery point.

This is naturally modeled in our orchestrator: at each pending message,
the orchestrator can choose to deliver it, deliver a different message,
or drop it. Drop is a non-default action that costs one unit of context
bound.

PCT handles this well if fault injection is modeled as a priority change.
Jepsen's approach (random fault injection + random scheduling) finds this
probabilistically but not deterministically.

---

## Summary: Why Multi-Dimensional, Multi-Depth Exploration Matters

### Coverage Matrix

| Example | Dimensions | Non-default decisions | Depth | Single-dim finds it? | Depth-1 finds it? |
|---------|-----------|----------------------|-------|---------------------|-------------------|
| L2 | Local | 2 local | 2 | Yes (if local explored) | No |
| G2 | Global | 2 global | 2 | Yes (if global explored) | No |
| GL1 | Global + Local | 1 global + 1 local | 2 | **No** | **No** |
| GS1 | Global + Select | 1 global + 1 select | 2 | **No** | **No** |
| G3_partition | Global (delivery + fault) | 1 drop + 1 reorder | 2 | Yes (if faults explored) | No |

### Key Takeaways

1. **Depth 1 is insufficient for important bug classes.** All five
   examples require exactly depth 2. Depth-1 exploration catches simple
   races (single reorder, single scheduling swap) but misses bugs where
   two independent conditions must BOTH be violated.

2. **Single-dimension exploration misses cross-dimensional bugs.** GL1
   and GS1 demonstrate this: a global delivery reordering creates a
   WINDOW that a local scheduling or select decision exploits. An explorer
   that only varies message delivery (or only varies goroutine scheduling)
   will never find these bugs.

3. **Fault injection is a decision dimension.** G3_partition shows that
   network partitions (message drops) must be modeled as non-default
   decisions alongside delivery ordering. The same context-bounding
   framework applies: a partition costs one unit of bound, a delivery
   reorder costs another.

4. **Context bound 2 is the sweet spot.** Research (CHESS, PCT, Coyote)
   shows most real-world concurrency bugs manifest at depth 1-3, with a
   sharp dropoff after 3. Our examples confirm depth 2 is necessary and
   sufficient for important patterns.

### State Space Growth

| Bound | Typical state space | What it catches |
|-------|-------------------|----------------|
| 0 | 1 | Nothing (default execution only) |
| 1 | O(n) | Single-fault bugs, simple races |
| 2 | O(n^2) | Two-fault bugs, cross-dimensional races |
| 3 | O(n^3) | Three-fault bugs (rare in practice) |

The quadratic blowup at depth 2 is manageable for small systems (our
examples have n < 20 decision points). For larger systems, PCT provides
probabilistic coverage at depth 2 with O(1) cost per run (1/n^2
probability of hitting any specific depth-2 bug per run).
