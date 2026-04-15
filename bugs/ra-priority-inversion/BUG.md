# `ra-priority-inversion`

This package contains a Ricart-Agrawala style lock with a release-order bug.

The intended protocol is ordinary RA:

- `Request` messages are compared by Lamport timestamp, then sender id
- conflicting requests are deferred while a node is `Wanted` or `Held`
- deferred requesters are granted when the holder releases the lock

The defect is in the release path:

- deferred requesters are queued in arrival order
- `ReleaseLock()` drains that slice directly
- the code does not reorder deferred requests by timestamp priority before
  replying

That matters when a lower-priority contender reaches the deferred queue first.
If `E` arrives at `A` before higher-priority `B`, `A` replies to `E` first on
release even though `B` should have been prioritized.

## Files

- Implementation: [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-priority-inversion/lock.go)
- Transport and KV plumbing: [transport.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-priority-inversion/transport.go)
- Scenario: [scenario.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-priority-inversion/scenario.go)
- Tests: [simple_test.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-priority-inversion/simple_test.go), [bug_demo_test.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-priority-inversion/bug_demo_test.go)
- Benchmarks: [bench_test.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-priority-inversion/bench_test.go)

## Scenario

The workload uses five nodes:

- `A`
- `B`
- `C`
- `D`
- `E`

`A` acquires first and holds long enough for the two interesting contenders to
queue behind it. `C` and `D` are passive RA peers that only reply.

`B` starts before `E`, so `B` has the higher priority timestamp.

The scenario encodes that timestamp edge explicitly:

- `A` sends a harness-only control message to start `B`
- `B` sends a harness-only readiness message back to `A` after its requests are
  enqueued
- only then does `A` start `E`
- `B`'s request to `A` is held in the transport until `E` is already in flight,
  so `B` stays earlier by timestamp while `E` still lands first in `A`'s
  deferred queue

That makes `B` logically earlier while still letting the transport/search layer
reorder delivery so that `E`'s `Request` reaches `A` before `B`'s `Request`.

That creates the bad queue shape:

1. `A` records `E` in its deferred slice first
2. `A` records `B` later
3. `A` releases and flushes deferred replies in slice order
4. `A` replies to lower-priority `E` before replying to higher-priority `B`

## Detection

This package detects the protocol violation directly:

- when `A` releases, the harness watches the order of `A`'s deferred `Reply`
  messages
- if `A` sends `Reply(E)` before `Reply(B)`, the test fails immediately

That is a better fit for this bug than a lost-update check, because the defect
is a priority/fairness violation in release ordering rather than a mutual
exclusion failure.

## Search shape

This is intended to be a pure global-ordering example.

The interesting decisions are delivery order choices among ordinary `Request`
and `Reply` messages. There are no synthetic control messages and no
local-scheduling dependency in the bug path.
