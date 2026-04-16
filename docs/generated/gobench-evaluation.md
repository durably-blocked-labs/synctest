# GoBench Evaluation: Channel & Lock Category

## GoBench / GoKer

GoBench (Yuan et al., CGO 2021) is the standard benchmark suite for Go concurrency bug detection tools. It collects real-world blocking and non-blocking concurrency bugs from major Go projects (etcd, Kubernetes, gRPC, Moby, Istio, etc.) and distills each into a minimal reproducing kernel. The GoKer subset contains 103 kernel bugs across multiple synchronization categories. Each kernel preserves the original bug's root cause while stripping project-specific dependencies.

## Why Channel & Lock

Channel & Lock bugs require reasoning about the interaction between Go's channel operations and mutex/RWMutex locking — circular waits that span both primitives. This is the hardest category in GoKer. Existing tools specialize in one primitive or the other; none handle the cross-primitive interaction well.

Li (Edinburgh, 2023) evaluated four state-of-the-art tools against GoKer. Aggregate results from Table 4.2 for the Channel & Lock category (13 bugs):

| Tool | Detected | Recall |
|------|----------|--------|
| Goleak | 7/13 | 54% |
| GCatch | 5/13 | 38% |
| GFuzz | 4/13 | 31% |
| GoAT | 5/13 | 38% |

No tool exceeds 54% recall. Our approach: 10/13 detected (77%).

## Tool Comparison

| Tool | Approach | Why It Misses Channel & Lock |
|------|----------|------------------------------|
| Goleak | Post-mortem goroutine leak detection via stack inspection | Only detects leaked goroutines, not the deadlock mechanism; misses bugs where all goroutines block symmetrically |
| GCatch | Static path-sensitive analysis of channel operations | Models channels but not mutexes; cannot reason about lock-channel circular waits |
| GFuzz | Coverage-guided fuzzing with channel operation reordering | Random exploration; no systematic guarantee of hitting the specific interleaving; exponential in depth |
| GoAT | Dynamic trace analysis with happens-before on channel ops | Detects potential blocking from a single trace; misses interleavings that never appear under default scheduling |
| **Ours** | CHESS-style exhaustive search within synctest bubbles, context-bounded DFS over goroutine scheduling decisions | Systematic exploration of all interleavings up to bound K; misses only timer-dependent and instruction-level bugs (see below) |

## Per-Bug Results

All 13 Channel & Lock kernels ported into `explorer.Test()`. Each runs inside a synctest bubble with `GOMAXPROCS=1` and `asyncpreemptoff=1`. CHESS column shows which exploration run first triggers the bug. Bound is the minimum context bound K required.

| Project | Bug ID | Mechanism | Goroutines | CHESS Run | Bound | Detected |
|---------|--------|-----------|------------|-----------|-------|----------|
| grpc | #1353 | Mutex + unbuffered chan circular wait | 3 | 1 | 2 | Y |
| k8s | #6632 | WriteLock + resetChan deadlock | 3 | 2 | 2 | Y |
| etcd | #7902 | Lock around channel wait in election | 3 | 1 | 2 | Y |
| moby | #33781 | Goroutine leak on stop path | 3 | 1 | 2 | Y |
| etcd | #7443 | RWMutex + buffered chan circular wait | 5+ | 74 | 3 | Y |
| etcd | #6873 | Lock + channel interaction | TBD | 2 | 2 | Y |
| grpc | #1460 | Channel + mutex ordering | TBD | 1 | 2 | Y |
| istio | #16224 | Lock + channel deadlock | TBD | 1 | 2 | Y |
| k8s | #10182 | Channel + lock circular dependency | TBD | 3 | 2 | Y |
| k8s | #26980 | Lock + channel ordering | TBD | 2 | 2 | Y |
| moby | #28462 | Channel + mutex interaction | TBD | 6 | 2 | Y |
| etcd | #7492 | Timer-dependent deadlock | TBD | -- | -- | N |
| k8s | #1321 | Instruction-level preemption needed | TBD | -- | -- | N |
| serving | #2137 | Non-blocking-sync preemption needed | TBD | -- | -- | N |

moby#33781 is technically Channel & Context category; included for completeness.

etcd#7443 is the outlier: 74 runs at bound 3. The RWMutex + buffered channel combination creates a large branching factor with 5+ goroutines, pushing the bug deeper into the search space.

## Undetected Bugs

Three bugs fall outside the scheduling model. Each represents a distinct limitation.

### etcd#7492 — Timer-Scheduling Gap

The bug requires a ticker to fire while a goroutine holds a mutex. In synctest bubbles, fake time advances only when `running == 0` (all goroutines are durably blocked). A goroutine holding a mutex is not blocked — it is running or runnable. Therefore the timer cannot fire at the exact moment needed: between the mutex acquisition and the subsequent channel operation. This is a fundamental limitation of the bubble time model, not a search depth issue. No amount of context bounding helps.

### k8s#1321 — Instruction-Level Preemption

The bug requires preemption between `Watch()` and `Stop()` — two operations that execute within a single goroutine quantum with no yield point between them. The kernel has only 3 possible schedules total. Our model operates at goroutine-scheduling granularity: decisions happen when a goroutine yields (channel op, mutex acquire, Gosched). Without a yield point between the two calls, the explorer cannot place a context switch there. Detecting this bug requires instruction-level interleaving (e.g., Go's race detector granularity or explicit `Gosched()` injection).

### serving#2137 — Non-Blocking-Sync Atomicity

The bug requires preemption between `WaitGroup.Done()` and a subsequent buffered channel send. Both operations are non-blocking: `Done()` is an atomic decrement, and buffered sends with available capacity do not yield. They execute atomically within a single scheduling quantum. Like k8s#1321, this is an instruction-level bug invisible to goroutine-granularity scheduling. The two operations would need to be separated by a yield point for the explorer to insert a context switch.

### Common Thread

All three undetected bugs require preemption at a point where no goroutine scheduling decision occurs. The bubble model sees goroutines as indivisible between yield points. Bugs that require breaking this atomicity — whether via timer injection during execution, or preemption between non-blocking operations — are outside the model's observable granularity.

## Runtime Change: Mutex in isIdleInSynctest

Porting Channel & Lock bugs required a runtime modification. The `isIdleInSynctest` function determines whether a goroutine's wait reason counts as "durably blocked" for bubble time advancement. Originally, only channel operations and select were recognized as idle wait reasons.

Channel & Lock bugs block on `sync.Mutex` and `sync.RWMutex`. A goroutine waiting to acquire a mutex was not considered durably blocked, so the bubble's `running` counter never reached zero, and time advancement (and decision hooks) stalled.

The fix: add `waitReasonSemacquire` (the wait reason for mutex acquisition) to `isIdleInSynctest`. This tells the bubble that a goroutine parked on a mutex is durably blocked, enabling the scheduler to recognize yield points at lock acquisitions and explore interleavings that involve lock ordering.

Without this change, 0/13 Channel & Lock bugs are detectable — the bubble never recognizes a decision point involving mutex contention. With it, 10/13.
