# `ra-ghost-grant`

This package models a Ricart-Agrawala composition failure caused by combining
two individually reasonable optimizations:

- Roucairol-Carvalho reply caching
- late join / dynamic membership

The failure is not that either idea is obviously wrong in isolation. The bug is
that they interact badly when membership updates are asynchronous.

## Scenario

Nodes:

- `A`
- `B`
- `C` as the late joiner
- `D`

Initial membership is only `A`, `B`, and `D`. Node `C` joins later by sending
ordinary `JOIN` messages.

Before `C` joins, `A` completes one lock acquisition against `B` and `D`. That
leaves `A` with cached replies from both peers. On its next acquisition, `A`
re-enters immediately without sending new `REQUEST`s.

The key bad ordering is:

1. `A` starts its second acquisition and immediately enters using cached replies
   from `B` and `D`.
2. `C` sends `JOIN` to `A`, `B`, and `D`, then sends its normal `REQUEST`s.
3. `B` and `D` process the join first, so they now treat `C` as part of the
   quorum.
4. `A` has still not processed `C`'s `JOIN`, so `A`'s peer set remains just
   `{B, D}`.
5. `A` receives `C`'s `REQUEST` before `C`'s delayed `JOIN`.
6. `A` handles `C` as an unknown sender and replies immediately rather than
   reconciling membership first.
7. `C` gets replies from `A`, `B`, and `D` and enters concurrently with `A`.

The workload detects this as a lost update: the final counter value is below
the expected total.

## Bug Site

The lock implementation is in [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-ghost-grant/lock.go).

The critical behavior is in `handleMessage(MsgRequest)`:

- if a requester is not in the node's current peer set
- the node replies immediately
- and it does not first incorporate that requester into the active membership

That behavior is defensible as a compatibility rule for late joiners. The
problem is that reply caching means `A` may also be running with a stale quorum
view at the same time. The combination lets `A` exclude `C` from its own
acquisition while still granting `C` permission to enter.

## Search Shape

This is intended to be a pure global-ordering bug.

- Depth target: `3`
- The important choices are message delivery order, not local goroutine
  scheduling.

The targeted scheduler in [bug_demo_test.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-ghost-grant/bug_demo_test.go)
delays `C -> A JOIN` while prioritizing `C`'s later request/reply path.
