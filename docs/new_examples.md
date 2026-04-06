# Candidate Examples Beyond Raft

This document surveys smaller open-source distributed systems that would make good
orchestratorv2 test targets, and analyses the specific concurrency bugs that systematic
delivery reordering could expose.

---

## Candidate Repos

### Pure message-passing (easiest to wrap)

| Repo | Algorithm | Size | Notes |
|------|-----------|------|-------|
| [cheukwing/go-paxos](https://github.com/cheukwing/go-paxos) | Single-decree Paxos | ~6 files | Channel-based Proposer/Acceptor/Learner; few deps |
| [MotaOcimar/Ricart-Agrawala](https://github.com/MotaOcimar/Ricart-Agrawala) | Distributed mutex | ~300 lines | Pure message-passing, O(n²) msgs per CS entry |

### net/rpc based (same integration pattern as rafttest)

| Repo | Algorithm | Size | Notes |
|------|-----------|------|-------|
| [ianobermiller/gotwopc](https://github.com/ianobermiller/gotwopc) | Two-phase commit + KV | <500 lines | Concurrent fan-out prepare/commit |
| [Abdulsametileri/leader-election-bully-algorithm](https://github.com/Abdulsametileri/leader-election-bully-algorithm) | Bully election | Small | Simultaneous elections, in-flight message crossing |
| [despreston/go-craq](https://github.com/despreston/go-craq) | Chain Replication (CRAQ) | ~1000–1500 lines | Writes propagating while chain membership changes |

### Bonus: philosophically aligned

| Repo | Algorithm | Size | Notes |
|------|-----------|------|-------|
| [tangledbytes/go-vsr](https://github.com/tangledbytes/go-vsr) | Viewstamped Replication | Moderate | Ships its own deterministic simulator; good for comparison |

---

## Ricart-Agrawala: Interleaving Bug Analysis

Ricart-Agrawala is the highest-value starting point: it is tiny, pure message-passing,
and has a rich set of bugs that span from trivial data races to deep multi-step safety
violations. Below is a complete taxonomy.

### Algorithm recap

- To enter CS: broadcast `REQUEST(timestamp, pid)` to all peers; wait for `REPLY` from all n−1.
- On receiving `REQUEST(T_j, j)`: reply immediately if not wanting CS, or if the sender
  has higher priority `(T_j, j) < (T_i, i)` (lower timestamp wins; pid breaks ties);
  otherwise add sender to deferred queue.
- On CS exit: flush deferred queue (send all pending replies).

---

### Surface-level bugs (found by `-race` or trivial tests)

These require no delivery reordering — any concurrent execution exposes them.

**1. Non-atomic `replyCount` increment**

Two `REPLY` goroutines race on a shared counter without synchronisation. Both read 3,
both write 4; one reply is effectively lost. The process waits forever for quorum.

**2. Unprotected deferred queue**

The REQUEST handler (adds entries) and the CS-exit handler (drains entries) both touch
the deferred queue concurrently without a mutex. Concurrent map/slice mutation →
corruption or missed entries → safety or liveness violation.

---

### Medium-depth bugs (require specific scheduling windows)

These require a REQUEST to arrive inside a narrow window during the sender's own
CS-entry sequence. FIFO delivery never hits them; even a single non-FIFO delivery step
can trigger them.

**3. Non-atomic `wanting` + `myTimestamp` assignment**

A correct REQUEST handler must atomically read both `wanting` and `myTimestamp` to make
its priority decision. Many implementations set them in two separate steps:

```
A: sets wanting = true
A: [not yet written myTimestamp]
B's REQUEST arrives at A
A: wanting=true, myTimestamp=0  ← stale read
A: sets myTimestamp = 5         ← too late
```

With `myTimestamp=0`, A's priority comparison is inverted for this one REQUEST.
Depending on pid ordering, A either defers B when it should reply (liveness) or
replies when it should defer (safety — two processes enter CS simultaneously).

Requires: B's REQUEST delivered to A *between* A's two write steps.
Orchestrator bound needed: **1 non-FIFO delivery**.

**4. Partial broadcast + clock comparison race**

A broadcasts REQUEST by spawning one goroutine per peer. Those goroutines can fire in
any order. If B's REQUEST arrives at A *after* A's goroutine to B fires but *before*
A's goroutine to C fires, A handles B's REQUEST with an inconsistent view of its own
state (clock updated from B's message, but A's REQUEST not yet visible to C).

This does not directly cause a bug in the algorithm, but when combined with bug #3
(stale `myTimestamp`) the partial-broadcast window is the delivery ordering that lands
inside the dangerous zone.

---

### Deep bugs (multi-step delivery reordering, safety violations)

These require holding or reordering messages across a full CS round-trip. FIFO delivery
never exposes them. They are genuine safety violations in real implementations.

**5. Stale REPLY — no request-epoch tracking**

Most simple implementations use a plain `replyCount` counter with no request ID. This
creates a use-after-free of reply credits across CS epochs:

```
Round 1:
  A: CS attempt, timestamp=4; accumulates replies; enters CS
  A: exits CS; sends deferred reply to B  ← goroutine spawned but not yet scheduled

Round 2 (starts immediately):
  B: starts CS attempt, timestamp=6
  A: starts CS attempt, timestamp=8

Delivery reordering:
  A's stale deferred-reply goroutine (from Round 1 exit) fires NOW
  B receives a REPLY that was meant for Round 1
  B's replyCount++ (inflated)
  B accumulates enough "replies" without true quorum
  B enters CS while A is also in CS  ← safety violation
```

Requires: A's CS-exit reply goroutine to B to be delayed past B's next REQUEST
broadcast. The orchestrator must hold that outbound message for multiple delivery
steps — achievable with `GlobalBound(2)` or `GlobalBound(3)`.

**6. CS-exit atomicity (demonstrates correctness, not a bug)**

```
A: in CS, deferred = [B]
A: clears "in CS" flag
A: [drain not yet started]
C: REQUEST arrives at A → A replies immediately (not in CS, not wanting CS)
A: drains deferred, sends REPLY to B
→ B and C both received REPLYs from A
```

Looks dangerous, but is not a safety violation: C also needs a REPLY from B, and B
is in CS so B defers C. The algorithm's safety holds across the race. This scenario
is valuable as a **proof-of-correctness demonstration** — the orchestrator reaches this
ordering, finds no failure, and documents why the invariant holds despite the apparent
race on the CS flag. `GlobalBound(2)` reaches this ordering.

**7. Clock consistency violation enabling priority inversion**

Requires a specific 4-message reordering:

```
A: broadcasts REQUEST(5, A) to B and C via separate goroutines
Orchestrator delivers A→C first (C replies, not wanting CS)
B: concurrently broadcasts REQUEST(5, B) (same logical time)
Orchestrator delivers B→A before A→B
A: receives REQUEST(5, B); same timestamp; A.pid < B.pid → A defers B ✓
A→B finally delivered
B: receives REQUEST(5, A); (5,A) < (5,B) since A.pid < B.pid → B replies ✓
```

Under FIFO this scenario never arises. Under any reordering that delays A→B relative
to B→A, both nodes momentarily have asymmetric knowledge of each other's clocks.
The algorithm remains correct here, but if the implementation uses a non-total clock
comparison (e.g., skips pid tie-breaking), priority can invert → **deadlock**: A defers
B, B defers A, neither can enter CS.

Requires: `GlobalBound(2)` (delay A→B past B→A and B→C).

---

### Summary table

| # | Bug | Depth | Min reorderings | Violation type |
|---|-----|-------|-----------------|----------------|
| 1 | Non-atomic replyCount | Surface | 0 | Liveness |
| 2 | Unprotected deferred queue | Surface | 0 | Safety + Liveness |
| 3 | Non-atomic wanting + timestamp | Medium | 1 | Safety or Liveness |
| 4 | Partial broadcast window | Medium | 1 | Latent (amplifies #3) |
| 5 | Stale REPLY (no request IDs) | Deep | 2–3 | **Safety** |
| 6 | CS-exit flag race | Deep | 2 | Non-bug (correctness demo) |
| 7 | Clock asymmetry + priority inversion | Deep | 2 | Deadlock (liveness) |

Bugs #5 and #7 are the highest-value targets: both are genuine safety/liveness
violations, both require multi-step delivery reordering that FIFO never produces,
and both are the kind of bug that `Explore` with `GlobalBound(3)` is designed to find
systematically.
