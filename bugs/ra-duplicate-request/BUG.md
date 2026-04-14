# `ra-duplicate-request`

---

**Overview**

This package contains a Ricart-Agrawala style bug triggered by a duplicated
normal `Request`.

The implementation has two protocol defects:

- a holder records the same deferred requester more than once
- a requester counts replies by raw total instead of unique sender

The network behavior is limited to:

- duplicating one ordinary `Request`
- exploring different delivery orders for otherwise normal `Request` and
  `Reply` messages

In its current form, this workload is **FIFO-safe**:

- the default run does not expose the bug
- the duplicated request becomes dangerous only when the search policy explores
  a less-default ordering
- the exact time-to-first-bug still depends on the current search budget and
  policy

---

**Files**

- Implementation: [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-duplicate-request/lock.go)
- Transport and duplicate injection: [transport.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-duplicate-request/transport.go)
- Scenario and workload: [scenario.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-duplicate-request/scenario.go)
- Tests: [bug_demo_test.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-duplicate-request/bug_demo_test.go)
- Benchmarks: [bench_test.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-duplicate-request/bench_test.go)

---

**Correct Behavior**

In a correct RA implementation:

- each requester should count at most one `Reply` from each peer
- a holder should defer each conflicting requester at most once
- a duplicated `Request` should not help a node reach quorum

Conceptually, quorum tracking should be set-based:

- deferred requesters should be unique by sender
- replies should be unique by sender

---

**The Bug**

The bug is in [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-duplicate-request/lock.go).

When a node receives a conflicting `Request`, it appends `msg.From` to
`deferred` without checking whether that peer is already present.

**Deferred-request bug site:** [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-duplicate-request/lock.go:123)

Later, when a node receives `Reply`, it increments `repliesReceived` as a raw
counter and does not deduplicate by sender.

**Reply-counting bug site:** [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-duplicate-request/lock.go:111)

Together, those defects let one duplicated request turn into two counted
replies from the same peer.

---

**Duplicate Injection**

The transport duplicates one normal `B -> A Request`.

**Where the duplicate is configured**

[scenario.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-duplicate-request/scenario.go:29)

```go
transports["B"].DuplicateNext("A", MsgRequest)
```

**Where the duplicate is actually sent**

[transport.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-duplicate-request/transport.go:97)

The second `enqueueSend(peer, to, msg)` in this block is the duplicate:

```go
if t.duplicate != nil && !t.duplicate.used && t.duplicate.to == to && t.duplicate.kind == msg.Kind {
	t.duplicate.used = true
	t.enqueueSend(peer, to, msg)
}
```

---

**Scenario**

The workload in [scenario.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-duplicate-request/scenario.go)
uses three nodes:

- `A`
- `B`
- `C`

**Roles**

- `A` acquires first and becomes the node that defers requests
- `B` is the duplicated requester and later miscounts replies
- `C` is the honest contender that should enter only after collecting real
  quorum

**Workload**

`A`:

- acquires first
- enters the critical section
- holds briefly
- releases

`B`:

- starts while `A` is still holding
- sends a duplicated request to `A`
- waits for quorum

`C`:

- also starts while `A` is still holding
- receives an honest reply from `B`
- waits for `A`

---

**Expected Failing Trace**

The intended bad execution in the current workload has three phases.

**Phase 1: `A` holds and both contenders queue behind it**

1. `A` acquires and enters the critical section
2. `B -> A(Request)` is sent and duplicated
3. `B -> C(Request)` is sent
4. `C -> A(Request)` is sent
5. `B` or `C` receive whatever non-conflicting replies they can get while `A`
   is still holding

**Phase 2: `A` records one contender once and the other contender twice**

6. `A` receives both copies of `B -> A(Request)` while still holding
7. `A` appends `B` twice to `deferred`
8. `A` also receives `C -> A(Request)` and appends `C` once

At this point the deferred queue contains the same requester multiple times.

**Phase 3: release turns the duplicate request into duplicate replies**

9. `A` releases
10. `A` flushes `deferred`
11. `A -> B(Reply)` is sent twice
12. `A -> C(Reply)` is sent once
13. `C` enters after collecting legitimate replies
14. `B` counts the two `A` replies as two distinct quorum contributions
15. `B` enters without ever receiving a real `C -> B(Reply)`

If `C` is still in the critical section at that point, mutual exclusion is
violated.

---

**Why The Bug Happens**

Three conditions must line up:

1. **Network duplication**
   `B -> A(Request)` is duplicated.

2. **Holder-side accounting defect**
   `A` stores duplicated deferred requesters more than once.

3. **Requester-side accounting defect**
   `B` counts replies by raw total instead of unique sender.

Without the duplicate request:

- `A` would send only one reply to `B`

Without the holder-side defect:

- the duplicate request would collapse to one deferred entry

Without the requester-side defect:

- duplicate replies from `A` would not satisfy quorum

---

**Decision Structure**

The underlying bug is primarily a **global-ordering** bug: duplicated normal
traffic plus delivery order is enough to trigger it.

Important delivery relationships are:

- both copies of `B -> A(Request)` must reach `A` before `A` releases
- `C` must already be waiting for `A` so that `A -> C(Reply)` can let `C`
  enter promptly after release
- `B` must receive both `A -> B(Reply)` messages before it receives a real
  `C -> B(Reply)`

The current workload does **not** need a special local scheduler choice.
The FIFO baseline stays safe because `B` starts late enough that the duplicated
request is no longer harmful in the default run.

---

**Current Search Behavior**

With the present timing:

- FIFO does not find the bug
- systematic search may find it, but the result depends on the configured
  policy and run budget

The key message classes are still the same:

- `msg:B->A(Request)`
- `msg:C->A(Request)`
- `msg:A->B(Reply)`
- `msg:A->C(Reply)`

So this package is still useful as a clean duplicate-request bug example.
It is closer to the intended “reordering matters” shape now, but it is still
not yet tuned into a stable benchmark where one policy consistently outperforms
another.

---

**Tradeoffs**

This example is cleaner than a benchmark that adds custom protocol messages,
because all traffic is still normal RA traffic:

- `Request`
- `Reply`

The one artificial element is the explicit network duplication rule in the
transport. That is still a realistic distributed-systems fault model.

The main remaining limitation is benchmark stability:

- the scenario is now FIFO-safe
- but the exact bug-finding performance still varies with the search harness

So this package is a good protocol example, but it still needs tuning if the
goal is a strong benchmark comparison between PCT and CHESS.
