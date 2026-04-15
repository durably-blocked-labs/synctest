# `ra-deferred-storm`

This package contains a buggy Ricart-Agrawala lock with a Roucairol-Carvalho
style permission cache and an eager re-entry optimization.

## Bug

The node implementation lives in [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-deferred-storm/lock.go).

The intended optimization is:

- keep cached permissions from peers until you send them a later `REPLY`
- on exit, drain deferred peers in sorted order
- if the application wants to acquire again, start the next round only after
  the drain has invalidated every cached permission that was handed back

The injected bug is in `ExitCS(reenter=true)`:

- it starts `go EnterCS()` after the first deferred reply is drained
- the remaining deferred drain is still in flight
- `inCS` and the old `Held` state are cleared only after the full drain ends

That means the eager round can snapshot a partially-invalidated permission
cache. It may decide it only needs one fresh reply, even though the rest of the
drain is about to give away more cached permissions.

## Why this is harder

This package is meant to be materially harder than the other RA examples.

The failure needs both:

- global reordering, so old deferred replies and new eager-round requests cross
- a local scheduling choice, so the eager goroutine runs mid-drain instead of
  after the drain has finished

With FIFO local scheduling, the eager goroutine usually starts too late and the
second round re-requests the full invalidated set. That leaves the bug latent.

## Scenario

The workload is shaped in [scenario.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-deferred-storm/scenario.go).

Nodes are `A`, `C`, `D`, `Z`, with `D` as the buggy node:

- `D` enters once, updates the counter, and exits with eager re-entry enabled
- `A` waits for an explicit `Start` message from `D`, then contends first
- `Z` does not start from a timer or transport-side mailbox; it starts only
  after it has actually received `A`'s ordinary RA `Request`
- `C` remains a normal RA peer, but also forwards one benchmark-only release
  control message to `Z`

The important execution shape is:

1. `D` enters its first critical section and sends `Start` to `A`.
2. `A` sends normal RA `Request`s. `Z` begins contending only after delivery of
   `A -> Z (Request)`, so `A` is causally first among the two non-buggy
   contenders.
3. `D` defers both `A` and `Z`, then exits and drains deferred peers in sorted
   order: `A`, then `Z`.
4. After replying to `A`, `D` launches `go EnterCS()` too early.
5. The eager round can therefore snapshot permission from `Z` as still cached,
   so it requests only `A`.
6. `A` does not exit on a delay. It waits until it has actually received both
   `Z`'s request and `D`'s second-round request, then exits and replies to both.
7. On exit, `A` also sends an `Arm` control message to `C`; `C` forwards a
   `Release` control message to `Z`, which keeps `Z` in the critical section
   until after `A` has exited.
8. If `A -> Z (Reply)` reaches `Z` before `A -> D (Reply)` reaches `D`, `Z`
   enters before `D` gets its final quorum reply. `D` then enters again using
   an incomplete quorum model, overlapping with `Z`.

The tests detect the overlap as a lost update: the final counter is below the
expected number of critical-section executions.

## Scaffolding

This package no longer uses `time.Sleep` or a transport-side control mailbox.
The scenario is driven by orchestrator-visible message dependencies.

Most of the shaping comes from ordinary RA traffic:

- `Z` starts only after delivery of `A`'s ordinary `Request`
- `A` waits for real `Request`s from `Z` and from `D`'s eager round before
  exiting

The package also includes three synthetic benchmark control messages handled
inside the node implementation:

- `Start` in [transport.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-deferred-storm/transport.go)
  and [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-deferred-storm/lock.go),
  used by `D` to release `A` into the scenario only after `D` has entered once
- `Arm` in those same files, used by `A` to trigger the final hold-release
  chain after it exits
- `Release` in those same files, forwarded by `C` to let `Z` leave its
  critical section

These control messages are benchmark scaffolding, not part of a normal
Ricart-Agrawala implementation. They exist only to replace timing-based holds
with message-visible causal dependencies.
