# `ra-lease-expiry`

This package contains a minimal Ricart-Agrawala lock variant with a lease
watchdog bug.

## Core bug

The lock implementation is correct about ordinary `Request` / `Reply` traffic
and deferred replies, but it has one flawed extra behavior:

- once a node enters `Held`, it starts a lease watchdog with `time.After`
- if the node is still `Held` when the lease expires, the watchdog releases the
  lock and flushes deferred replies
- the application may still be blocked waiting for a delayed KV reply when that
  happens

That means a node can lose mutual exclusion even though it has not finished its
critical-section work yet.

The bad sequence is:

1. `A` acquires the lock and issues a KV `Get`
2. the KV reply to `A` is delayed
3. `A`'s lease watchdog fires while `A` is still blocked
4. `A` releases the lock early and unblocks deferred peers
5. `B` acquires the lock and updates the counter
6. `A` resumes, uses the stale value from its delayed `Get`, and writes last

The result is a lost update: the final counter is lower than expected.

## Harness-only delay injection

The only non-protocol mechanism in this directory is the one-shot delayed KV
reply configured on the transport in the tests and benchmark setup:

- `DelayNext(MsgKVGetReply, ...)` in [transport.go](/Users/allantan/Documents/Programming/research/synctest/bugs/ra-lease-expiry/transport.go)

This is not part of the lock protocol. It exists only to hold back one KV
reply long enough for the watchdog's `time.After` to fire.

## Scenario shape

The workload uses three nodes:

- `A` is the node whose lease expires while waiting for the delayed KV reply
- `B` is the second contender that enters after `A` releases early
- `C` is a passive RA peer

The package stays intentionally small:

- no synthetic control messages
- no duplicate reply injection
- no extra protocol rounds
- only ordinary RA traffic plus the delayed KV reply
