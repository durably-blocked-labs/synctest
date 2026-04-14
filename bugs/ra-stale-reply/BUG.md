# `ra-stale-reply`

---

**Overview**

This package contains a Ricart-Agrawala style lock bug where a node accepts a
`Reply` from an earlier acquisition round and counts it toward a later
acquisition.

The scenario uses only ordinary protocol messages:

- `Request`
- `Reply`

The triggering network behavior is:

- one duplicated normal `Reply`
- reordered delivery of that duplicate

---

**Files**

- Implementation: [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-stale-reply/lock.go)
- Transport and duplicate injection: [transport.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-stale-reply/transport.go)
- Scenario and workload: [scenario.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-stale-reply/scenario.go)
- Tests: [bug_demo_test.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-stale-reply/bug_demo_test.go)
- Benchmarks: [bench_test.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-stale-reply/bench_test.go)

---

**Correct Behavior**

In a correct RA implementation:

- a node should enter the critical section only after receiving one valid reply
  from each peer for its current request
- replies from earlier rounds must not count toward a later request
- duplicate deliveries must not help a node reach quorum

Conceptually, each `Reply` should be matched against:

- the current acquisition round
- the current outstanding request
- the set of peers that have already replied for that round

---

**The Bug**

The bug is in [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-stale-reply/lock.go).

When `AcquireLock()` starts, the node resets:

- `repliesReceived`
- `repliedBy`
- `reqClock`

When `handleMessage(MsgReply)` runs, the node:

- ignores the reply if that sender was already counted in the current
  `repliedBy` map
- otherwise increments `repliesReceived`
- but does **not** verify that the reply belongs to the current request round

So the implementation deduplicates by sender within one acquisition, but it
does not validate the reply against the current round.

**Bug site:** [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-stale-reply/lock.go:113)

The defect is specifically that no round or epoch check is performed before the
reply is counted.

---

**Duplicate Injection**

The transport duplicates one normal `B -> A Reply`.

**Where the duplicate is configured**

[scenario.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-stale-reply/scenario.go:28)

```go
transports["B"].DuplicateNext("A", MsgReply)
```

This means the next `B -> A Reply` is enqueued twice.

**Where the duplicate is actually sent**

[transport.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-stale-reply/transport.go:95)

The second `enqueueSend(peer, to, msg)` in this block is the duplicate:

```go
if t.duplicate != nil && !t.duplicate.used && t.duplicate.to == to && t.duplicate.kind == msg.Kind {
	t.duplicate.used = true
	t.enqueueSend(peer, to, msg)
}
```

This is meant to model a realistic transport behavior:

- a normal reply is duplicated
- one copy can be delivered promptly
- the other copy can be delayed and reordered

---

**Scenario**

The workload in [scenario.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-stale-reply/scenario.go)
uses three nodes:

- `A`
- `B`
- `C`

**Roles**

- `A` is the node that miscounts a stale reply
- `B` is the node whose old reply is duplicated and later overlaps with `A`
- `C` provides the fresh reply that combines with the stale `B` reply

**Workload**

`A`:

- acquires once
- enters and leaves the critical section
- sleeps briefly
- acquires again

`B`:

- starts slightly later
- acquires once
- holds the critical section for a while

`C`:

- acts only as a responder

---

**Expected Failing Trace**

The intended bad execution has three phases.

**Phase 1: Round 1 completes normally**

1. `A -> B(Request)`
2. `A -> C(Request)`
3. `B -> A(Reply)` is sent
4. the transport enqueues a duplicated `B -> A(Reply)`
5. `C -> A(Reply)` is sent
6. one `B -> A(Reply)` and `C -> A(Reply)` are delivered
7. `A` reaches quorum for round 1 and releases normally

**Phase 2: The stale reply is kept pending**

8. the extra duplicated `B -> A(Reply)` is not delivered yet
9. `B` starts its own acquire
10. `B` enters the critical section
11. `A` starts round 2 and sends fresh `Request`s

**Phase 3: Round 2 fails**

12. `C -> A(Reply)` for round 2 is delivered
13. the delayed duplicated `B -> A(Reply)` from round 1 is delivered
14. buggy `A` counts that stale reply as valid for round 2
15. `A` reaches quorum too early
16. `A` enters the critical section while `B` is still holding it

That is the intended mutual exclusion violation.

---

**Why The Bug Happens**

Two conditions must line up:

1. **Protocol defect**
   `A` does not verify that an arriving reply belongs to its current request
   round.

2. **Network ordering**
   a duplicated `B -> A Reply` from round 1 is delayed until `A` is waiting in
   round 2.

Neither condition alone is enough:

- without the protocol defect, the stale reply would be ignored
- without the delayed duplicate, `A` would not receive an extra old reply to
  count in round 2

---

**Decision Structure**

The intended failure is primarily a **global-ordering** bug.

The important branching comes from message delivery order:

- one `B -> A Reply` should be delivered early in round 1
- the extra duplicate should be delayed until round 2
- `B` should already be in the critical section before `A`'s second acquire
  completes
- `C -> A Reply` for round 2 should combine with the delayed stale `B -> A
  Reply`

The scenario is not intended to require a special non-FIFO local scheduling
decision inside a node.

---

**Expected Search Behavior**

**Why PCT can do well**

PCT assigns priorities to message classes such as:

- `msg:B->A(Reply)`
- `msg:C->A(Reply)`
- `msg:A->B(Request)`
- `msg:A->C(Request)`

This bug depends on a cross-round causal chain, but still revolves around a
small set of important message classes. One favorable priority assignment can
therefore help line up the bad run across multiple phases.

**Why CHESS can be slower**

CHESS must explicitly enumerate delivery prefixes that realize:

- one early `B -> A Reply`
- one delayed `B -> A Reply`
- `B` entering the critical section before `A`'s second acquire completes

That can make CHESS sensitive to:

- the global bound
- the run budget
- whether the easiest failing path is truly global-only

---

**Current Empirical Notes**

This example is intended to support a benchmark profile where:

- the bug requires more than one run
- PCT is faster than CHESS
- the trace still looks like a realistic protocol/network failure

Current measurements should be interpreted carefully:

- if `PCT(d=2)` misses the bug but `PCT(d=3)` finds it, then the current
  scenario is behaving more like a depth-3 pattern than a depth-2 one
- if `CHESS(G+L)` finds the bug substantially earlier than `CHESS(GlobalOnly)`,
  then the current setup may admit an easier mixed global/local path than
  intended

---

**Tradeoffs**

**Strengths**

- uses only ordinary `Request` / `Reply` messages
- uses a realistic network-side perturbation: duplication plus delay
- the core defect is a believable protocol mistake: accepting a stale reply

**Weaknesses**

- the scenario still depends on deliberate workload shaping
- the exact benchmark profile is sensitive to timing and scheduling structure
- the easiest discovered failure path may not perfectly match the intended
  pure-global story

---

**Refinement Target**

If this example is tuned further, the target should be:

- PCT finds the bug faster than CHESS
- the bug is not found on run 1
- the failing trace is explainable using only realistic network ordering
  behavior
- the implementation remains limited to normal RA message types and normal RA
  node logic
