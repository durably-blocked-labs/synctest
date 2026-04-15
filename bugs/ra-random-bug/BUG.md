# `ra-random-bug`

This package contains a buggy Ricart-Agrawala lock implementation whose failure
has no fixed causal chain. The bug is reachable via many different execution
prefixes of varying length, so different search strategies explore different
paths to it and find it at different rates.

## Bug

The node implementation lives in [lock.go](lock.go).

The injected bug is in `handleMessage(MsgRequest)`, in the priority comparison:

```go
// correct
shouldDefer := n.state == Held ||
    (n.state == Wanted && (n.reqClock < msg.Timestamp ||
        (n.reqClock == msg.Timestamp && n.id < msg.From)))

// buggy
shouldDefer := n.state == Held ||
    (n.state == Wanted && (n.clock < msg.Timestamp ||
        (n.clock == msg.Timestamp && n.id < msg.From)))
```

`n.reqClock` is the Lamport timestamp at which this node sent its own request.
`n.clock` is the node's current Lamport clock, which advances with every
message received.

In correct RA, priority is determined by comparing REQUEST timestamps: the node
that requested earlier (lower `reqClock`) wins. Here, priority is instead
compared against the CURRENT clock, which has been advancing as replies arrive.

## How the window opens

When node A is in `Wanted` state and has been collecting replies, its `n.clock`
advances beyond `n.reqClock`. Suppose A's request went out at `reqClock = T`.
After receiving k replies (each carrying timestamps larger than T), `n.clock`
has grown to some value C > T.

Now node B's request arrives, carrying timestamp `t` where `T < t ≤ C`:

- Correct check: `n.reqClock(T) < msg.Timestamp(t)` → TRUE → A defers B
  (A requested earlier, A has priority, A correctly holds B back)
- Buggy check: `n.clock(C) < msg.Timestamp(t)` → FALSE (C ≥ t) → A replies
  to B immediately

A has just granted B a reply it should not have. If B collects replies from all
other peers, B can proceed to the critical section while A is also still working
toward it.

## Why there is no fixed causal chain

The trigger condition is `A.reqClock < B.Timestamp ≤ A.clock`. This window:

- does not exist when B's request arrives before any of A's replies have been
  processed (A.clock = A.reqClock, window is empty)
- grows by at least 1 for each reply A processes before B's request arrives
- can be entered at many different points depending on how many replies A has
  received

So the bug fires whenever B's request is delivered to A after at least one reply
has advanced A's clock past B's request timestamp. Different delivery orderings
create this condition at different depths:

| delivery order | required global decisions | window width |
|---|---|---|
| B's request arrives after 1 reply | short | narrow (just 1–2 timestamps) |
| B's request arrives after k replies | longer | wider (k timestamps) |
| B's request arrives before any reply | not triggering | zero |

There are many valid prefixes that each reach a triggering state. They differ in
which replies arrive first, how much A's clock has advanced, and where exactly
B's request is interleaved. None is strictly the "minimum" path; a short path
requires a narrower window and thus a more specific timestamp alignment, while
a longer path requires more decisions but admits a wider range of timestamps.

## How clock divergence accumulates across rounds

In a multi-round scenario (nodes acquiring more than once), the divergence
between `reqClock` and `clock` grows across rounds. Round 1 messages push
clocks higher; by round 2, `n.reqClock` and `n.clock` can differ by 10 or
more at the moment a competing request arrives. The window `(reqClock, clock]`
is then large, and many different request timestamps fall inside it.

This means:
- some runs trigger the bug in round 1 (narrow window, specific timing required)
- some runs trigger it in round 2 (wider window, looser timing requirement)
- some runs never trigger it (competing requests always arrive too early, before
  any replies)

The depth at which the bug manifests is variable and depends on accumulated
message history across rounds.

## Why strategies diverge

**CHESS (global-only)**: explores non-FIFO delivery orderings with context
bounding. It can find short paths by systematically delaying B's request
relative to some of A's replies. But the minimum window (after 1 reply) requires
B.Timestamp = A.reqClock + 1, which is a specific clock alignment. CHESS finds
this quickly if it exists; if clock values don't align that way, CHESS may need
more budget to find a triggering ordering.

**CHESS (G+L)**: adds local scheduling decisions. Whether B's request is
processed before or after A has updated its clock from a reply depends on which
goroutine runs first (local). This can shorten or lengthen the path. G+L may
find paths CHESS-global misses, or vice versa, depending on which ordering
combinations are budget-feasible.

**PCT**: randomly assigns thread priorities. The delivery order of A's replies
relative to B's request is determined by which goroutines run and in what
sequence—governed by random priority assignments. PCT has no systematic bias
toward the late-delivery ordering that opens the window, so it finds the bug in
proportion to how often random priority assignments happen to produce the right
delivery sequence. Different priority assignments produce different window widths
and thus different bug probabilities.

**Random**: uniform random over all delivery orderings. Finds the bug in
proportion to how large the triggering region is in the space of all
executions. In round 1 (narrow window), triggering sequences are rare. In
round 2+ (wider window), more sequences trigger it. Random search converges
faster in later rounds when it reaches them.

## Failure mode

The bad execution shape is:

1. A and B both call `AcquireLock`. A's `reqClock` is lower than B's (A has
   priority).
2. A processes some replies. Each reply advances `n.clock` past `n.reqClock`.
3. B's request arrives at A. B's timestamp falls in `(A.reqClock, A.clock]`.
4. Buggy check: `A.clock < B.Timestamp` is FALSE → A replies to B immediately.
5. B collects all N-1 replies including A's spurious one → B closes its gate →
   B enters the critical section.
6. A collects all N-1 replies → A enters the critical section.
7. A and B are simultaneously in the critical section. Mutual exclusion violated.

## What this is NOT

This is not a bug that requires a specific local scheduling race (goroutine
ordering within a node). The bug fires purely from global message delivery
ordering — specifically, the relative order in which A's replies and B's
request are delivered to A. Local scheduling within A's message handler is
sequential per the goroutine loop, so this is a pure global ordering bug.

The variance comes from the variable-width trigger window, not from an
independent local race. That distinguishes it from bugs like `ra-premature-defer`
(fixed causal chain) and the discarded version of this package (narrow fixed
local race requiring also a specific global setup).
