# `ra-quorum-eclipse`

This package is intentionally not another Ricart-Agrawala variant.

The other `bugs/ra-*` directories model symmetric RA peers that exchange only
`Request` / `Reply` style permissions. This bug is different: the defect lives
inside a **quorum voter** that can revoke and re-grant a vote while two
requesters are contending. That is much closer to a Maekawa-style quorum lock
than to RA, so this directory uses:

- two contenders: `A`, `B`
- three quorum voters: `C`, `D`, `E`
- Maekawa-style messages: `Request`, `Grant`, `Failed`, `Inquire`,
  `Relinquish`, `Release`

## Why this package departs from the RA examples

To keep the model small while still demonstrating the revocation bug, the
implementation makes two deliberate simplifications:

- the system is asymmetric: requesters and quorum voters are separate roles
- contenders wait for responses from all three voters, but need only two live
  grants to enter the critical section

That second point is not a full production Maekawa implementation. It is a
minimal threshold model chosen so the revocation race is visible without
building the entire deadlock-avoidance protocol family.

## Bug

The bug lives in [lock.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-quorum-eclipse/lock.go).

Voter `D` starts by granting its vote to `A`. Later, higher-priority contender
`B` requests the same vote. `D` sends `Inquire` to `A`, and because `A` has
already seen a `Failed` from `E`, `A` sends `Relinquish` back to `D`.

At that point, `D` should do this in order:

1. tell the old holder `A` that the old vote is no longer valid (`Failed`)
2. grant the vote to the queued requester `B`

The buggy code does the reverse:

1. `Grant` to `B`
2. `Failed` to `A`

That creates an eclipse window where the same quorum vote effectively counts for
both contenders.

## Failing shape

The intended bad execution is:

1. `A` requests `E`, sees `E -> A(Failed)`, then requests `D`
2. `D` grants `A`
3. once `A` has both observed `E`'s failure and counted `D`'s grant, the
   harness emits a synthetic `A -> B(Control)` message
4. `B` waits for that control message before it begins contending for `C`, `D`,
   and `E`
5. `D` sends `Inquire` to `A`
6. `A` sends `Relinquish` to `D`
7. buggy `D` sends `Grant` to `B` before `Failed` to `A`
8. delayed voter `C` finally grants `A`
9. `A` still counts `D` and now has `C + D`
10. `B` has `D + E`
11. both enter the critical section

That `Control` message is not part of the lock protocol itself. It is a
scenario-only causal gate used to encode the dependency that previously had to
be approximated with timing: `B` must not start contention until `A` is already
in the vulnerable state where it has seen `E`'s failure and is still counting
`D`'s grant.

The black-box symptom is a lost update on the shared counter:

- both contenders read the same counter value
- both write back `value + 1`
- the final counter is below the expected number of successful critical-section
  entries

## Why the name

The old grant has not been revoked yet, but a new grant is already visible. The
new quorum temporarily eclipses the old one without actually invalidating it
first. That transient double-counting is the whole bug.
