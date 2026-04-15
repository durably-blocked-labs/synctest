# `ra-quorum-cascade`

This package contains a buggy Ricart-Agrawala lock implementation and a
four-node contention scenario that exposes the bug through ordinary
`Request`/`Reply` traffic.

## Core Bug

The protocol bug lives in [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-quorum-cascade/lock.go).

Correct RA behavior is:

- while a node is `Wanted` or `Held`, conflicting `Request`s are deferred
- deferred `Reply`s are sent only after `ReleaseLock()`

The injected bug is in `handleMessage(MsgReply)`:

- when the node receives its final quorum `Reply`
- it closes the acquire gate
- and it flushes `deferred` immediately, while the node is still only `Wanted`

That can hand out permission to another contender before the current holder has
actually left the critical section.

## Scenario Scaffolding

The rest of the package is intentionally ordinary:

- no synthetic control messages
- no node-internal replay hooks
- no benchmark-only protocol messages

The workload in [scenario.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-quorum-cascade/scenario.go) uses four simultaneous contenders:

- `A`
- `B`
- `C`
- `D`

Each node performs one read-modify-write in the critical section. The only
extra shaping is the ordinary message ordering provided by the orchestrator.

## Failure Mode

The intended bad execution is a cascade:

1. `A` reaches quorum first.
2. `A` prematurely flushes deferred requests.
3. `B` reaches quorum because of `A`'s premature reply and also flushes early.
4. `C` does the same because of `B`.
5. `D` does the same because of `C`.

The counter is then lower than expected because multiple critical sections
overlap.

## Why This Is a Global-Ordering Bug

The failure is driven by message delivery order:

- requests must be observed in a shape that leaves deferred work queued
- replies must arrive in an order that allows each node to reach quorum
- no local scheduling decision inside a node is required

The bug is therefore intended to be reachable with pure global ordering.
