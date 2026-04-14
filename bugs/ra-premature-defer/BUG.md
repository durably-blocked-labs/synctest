# `ra-premature-defer`

This package contains a buggy Ricart-Agrawala style lock implementation and a
staged workload intended to expose a mutual exclusion violation under reordered
events.

## Bug

The node implementation lives in [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-premature-defer/lock.go).

The intended protocol rule is:

- while a node is `Wanted` or `Held`, conflicting `REQUEST`s are deferred
- deferred `REPLY`s should only be sent after `ReleaseLock()`

The injected bug is in `handleMessage(MsgReply)`:

- when the node receives the final quorum `REPLY`
- it closes the acquire gate
- and it incorrectly flushes `deferred` immediately, while it is still only
  `Wanted`

That means another contender can receive permission too early, before the first
node has actually finished its critical section.

## Benchmark-only additions

Several mechanisms in this directory were added only to shape the benchmark and
are not part of a normal Ricart-Agrawala implementation.

These are:

- `MsgArm` in [transport.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-premature-defer/transport.go)
- `MsgStart` in [transport.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-premature-defer/transport.go)
- `SendArm`, `SendStart`, `WaitForStart`, and related control wiring in
  [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-premature-defer/lock.go)
- the replayed stale reply state (`replayTo`, `replayTS`, `haveReplay`) and
  `SendStaleReply()` in
  [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-premature-defer/lock.go)
- the staged orchestration in
  [scenario.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-premature-defer/scenario.go)

These were introduced to make the workload harder for the orchestrator and to
force longer dependency chains. They should be read as benchmark scaffolding,
not as part of the core lock protocol.

More concretely:

- `Start` is a synthetic control message used to tell another node when to begin
  contending.
- `Arm` is a second synthetic control message used to force an extra ordered
  dependency before a `Start` is sent.
- the stale reply machinery lets `B` replay an old `Reply` to `A` in a later
  phase of the scenario; this is also artificial and exists only to shape the
  bug-finding workload.

So there are really two layers in this package:

- the RA bug itself: premature flushing of deferred replies at quorum
- the extra scenario machinery: synthetic control and replay behavior added only
  to make exploration behavior more interesting

## Scenario

The workload is shaped in [scenario.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-premature-defer/scenario.go).

The current setup is:

- `A` acquires once, releases, then waits for a later `Start` to acquire again
- `B` waits for a `Start`, acquires, enters the critical section, then sends:
  - a stale replayed `Reply` to `A`
  - an `Arm` control message to `C`
- `C` forwards control by sending `Start` messages

The point of this setup is to force the bug to depend on a longer ordering
prefix than the original easy version.

This also means the current package is no longer a minimal RA-only example. It
is an RA bug embedded inside an artificially staged harness.

## Tradeoffs

This example has value, but it also has clear limitations.

### What it is good for

It is still useful as an orchestrator stress test because it:

- creates a longer dependency chain than the simpler RA examples
- gives search policies a harder scheduling problem
- makes it easier to compare how `CHESS(GlobalOnly)`, `CHESS(G+L)`, `PCT`, and
  random search behave on a shaped workload

In that sense, it is a benchmark of search behavior under an artificial but
nontrivial interleaving problem.

### What it is not good for

It is not a clean benchmark of the original RA protocol, because the package
does not preserve the original system model.

The main reasons are:

- it introduces message types that do not exist in RA (`Arm`, `Start`)
- it adds node behavior that does not exist in RA (stale reply replay)
- it embeds benchmark-specific control logic directly into the node and
  scenario
- it shapes the workload around the bug rather than relying only on ordinary RA
  acquisitions plus network reorderings

So this package is not just "RA under reordered messages." It is "RA plus extra
benchmark scaffolding designed to manufacture a harder search problem."

### Relation to RPC / network injection

The extra machinery here should not be confused with ordinary network fault
injection.

Typical distributed-systems fault injection keeps the protocol implementation
fixed and perturbs delivery behavior, for example by:

- delaying messages
- reordering messages
- dropping messages
- duplicating messages
- partitioning links

By contrast, this package changes the protocol surface and node behavior
themselves. That makes it more invasive than normal RPC/network perturbation.

### Bottom line

This example is reasonable if the goal is:

- "stress the orchestrator with a shaped interleaving problem"

It is not ideal if the goal is:

- "demonstrate a realistic bug in a mostly unmodified RA implementation"

For a cleaner next example, the better standard is:

- use only ordinary RA `Request` / `Reply` messages
- keep the node logic close to the original protocol
- rely on realistic transport behaviors such as reorder, delay, drop, or
  duplicate delivery of normal messages
- avoid adding benchmark-only control RPCs or node-internal replay hooks

## Failure mode

The tests use an `inCS` counter to detect overlapping critical-section entry.

The bad execution shape is:

1. `A` reaches the point where it is waiting for its final permission.
2. Another contender's `REQUEST` reaches `A`, so `A` places that peer in
   `deferred`.
3. A later `REPLY` causes `A` to think it has quorum.
4. The buggy code flushes `deferred` before `A` has actually completed the
   critical section.
5. Two nodes can then overlap in the critical section.

## Important note

This directory originally targeted a pure global-ordering bug. The current
benchmark behavior shows that `CHESS(G+L)` can find a failure much earlier than
`CHESS(GlobalOnly)`, which is evidence that the present scenario is no longer a
clean pure-`G` example. In other words:

- the core protocol bug is still "premature deferred reply"
- but the current staged workload appears to admit an easier `G+L` failure path

So this package is best understood as:

- a buggy RA variant centered on premature deferred replies
- with a shaped benchmark scenario that makes pure-global search harder, but may
  also introduce local-scheduling-sensitive failure paths
