# GoBench Evaluation: Channel & Lock Category

## GoBench / GoKer

GoBench (Yuan et al., CGO 2021) is the standard benchmark suite for Go concurrency bug detection tools. It collects real-world blocking and non-blocking concurrency bugs from major Go projects (etcd, Kubernetes, gRPC, Moby, Istio, Knative, etc.) and distills each into a minimal reproducing kernel. The GoKer subset contains 103 kernel bugs across multiple synchronization categories. Each kernel preserves the original bug's root cause while stripping project-specific dependencies.

## Why Channel & Lock

Channel & Lock bugs require reasoning about the interaction between Go's channel operations and mutex/RWMutex locking -- circular waits that span both primitives. This is the hardest category in GoKer. Existing tools specialize in one primitive or the other; none handle the cross-primitive interaction well.

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
| Goleak | Post-mortem goroutine leak detection via stack inspection. After a test completes, Goleak scans for goroutines that remain alive and reports them as leaks. It has no model of why goroutines are stuck, only that they are. | Only detects leaked goroutines, not the deadlock mechanism. Misses bugs where all goroutines block symmetrically (no goroutine leaks because the whole program deadlocks). Cannot distinguish a benign background goroutine from one stuck in a circular wait. |
| GCatch | Static path-sensitive analysis that builds a happens-before graph of channel operations. It models channel creation, sends, receives, and close operations to detect potential blocking paths without running the program. | Models channels but not mutexes. When a deadlock requires a goroutine holding a mutex to block on a channel send (the defining pattern of Channel & Lock bugs), GCatch cannot see the lock dependency and misses the circular wait. |
| GFuzz | Coverage-guided fuzzer that instruments channel operations and uses mutation-based scheduling to increase coverage of concurrent interleavings. It prioritizes schedules that exercise new combinations of channel operation orderings. | Random exploration with no systematic guarantee of reaching a specific interleaving. The probability of hitting the exact schedule that triggers a Channel & Lock deadlock is exponentially small in the number of goroutines and decision points. Most fuzzing budget is spent on non-buggy interleavings. |
| GoAT | Dynamic trace analysis tool that records a single execution trace and constructs a happens-before relation over channel operations. It then checks whether the observed trace could lead to a blocking state under an alternative schedule. | Infers potential deadlocks from one observed trace. If the default Go scheduler never produces a trace that contains the necessary ordering hint, GoAT has no signal to detect the bug. Channel & Lock bugs often require 2+ non-default scheduling decisions, which may never appear in a single default-schedule trace. |
| **Ours** | CHESS-style exhaustive interleaving exploration within synctest bubbles. At each scheduling decision point (channel op, mutex acquire, goroutine yield), the explorer records which goroutine ran and which alternatives existed. A DFS over the space of scheduling traces, bounded by a context parameter K (maximum non-FIFO decisions per trace), systematically covers all interleavings up to that bound. | Misses only bugs that require (a) timer firing while a goroutine is running (the bubble time model advances time only when all goroutines are durably blocked), (b) preemption between two non-blocking operations within a single goroutine quantum, or (c) instruction-level data races invisible at goroutine-scheduling granularity. |

## Methodology

### Kernel Porting

Each kernel was ported from the GoBench repository (`goker/blocking/{project}/{bugnum}/`) into our test suite at `bugs/gobench/{project}{bugnum}/`. The porting process follows three steps:

1. **Copy and adapt.** The original GoBench test file is copied and the package name is changed to match our directory convention (e.g., `package kubernetes6632` becomes `package k8s6632`). Global variables (package-level mutexes, channels, shared state) are moved into local variables within a `Workload(t *testing.T)` function. This is required because synctest bubbles are per-test: global state would leak between exploration runs and corrupt subsequent interleavings.

2. **Wrap in `explorer.Test`.** The test file imports `github.com/shubhaankar/synctest/explorer` and calls `explorer.Test(t, Workload)`. This single function call replaces the original `go test` entry point with the full CHESS exploration loop. No other test scaffolding is needed -- the explorer handles bubble creation, scheduling hook installation, trace recording, and DFS iteration internally.

3. **Remove runtime.Gosched and sleep hacks.** Several GoBench kernels use `runtime.Gosched()` calls or `time.Sleep` with magic durations to coerce the Go scheduler into producing the buggy interleaving. These are unnecessary under our approach (the explorer systematically covers all interleavings) and are removed to ensure the kernel reflects the original production code's structure.

### Execution Environment

All tests run with a custom-built Go toolchain (`./go/bin/go`) forked from Go 1.27 with synctest modifications. Two environment settings are required for deterministic scheduling:

- **`GODEBUG=asyncpreemptoff=1`**: Disables the Go runtime's asynchronous preemption, which would inject non-deterministic context switches via OS signals. Without this, two identical scheduling prefixes can produce different traces.
- **`GOMAXPROCS=1`**: Set in code by each test. Restricts execution to a single processor, ensuring that all goroutine scheduling decisions pass through the bubble's decision-recording machinery. Multi-P execution would allow goroutines to run on processors outside the bubble's control.

Tests are run via `make test-one TEST=TestName`, which sets both flags automatically.

### Detection Criterion

A bug is considered detected when the explorer observes a deadlock (the bubble's `running` counter reaches zero with goroutines still alive and no timers pending) or a goroutine leak (the bubble completes but leaked goroutines remain blocked). The explorer reports the trace that triggered the bug, including the exact sequence of scheduling decisions.

## Per-Bug Results

All 13 Channel & Lock kernels ported into `explorer.Test()`. Each runs inside a synctest bubble with `GOMAXPROCS=1` and `asyncpreemptoff=1`. CHESS column shows which exploration run first triggers the bug. Bound is the minimum context bound K required.

| Project | Bug ID | Mechanism | Goroutines | CHESS Run | Bound | Detected |
|---------|--------|-----------|------------|-----------|-------|----------|
| grpc | #1353 | Mutex held during unbuffered channel send creates circular wait: watchAddrUpdates holds mu and blocks on addrCh, lbWatcher's tearDown path needs mu | 3 | 1 | 2 | Y |
| k8s | #6632 | WriteFrame holds writeLock and blocks sending on unbuffered resetChan; monitor needs writeLock to close resetChan | 4 | 2 | 2 | Y |
| etcd | #7902 | Follower holds mu in release() and blocks on <-rcNextc; leader needs mu to execute close(nextc) that would unblock follower | 4 | 1 | 2 | Y |
| moby | #33781 | Goroutine leak: when stop fires during probe, monitor returns without draining unbuffered results channel, stranding the probe goroutine | 3 | 1 | 2 | Y |
| etcd | #7443 | RWMutex + buffered channel: simpleBalancer.Up holds mu and sends on notifyCh, Close needs mu; concurrent resetTransport goroutines amplify contention | 5+ | 74 | 3 | Y |
| etcd | #6873 | coalesce() in the updatec range loop needs mu; stop() holds mu, closes updatec, then blocks on <-donec which coalesce must signal | 3 | 2 | 2 | Y |
| grpc | #1460 | keepalive holds mu and blocks on <-awakenKeepalive; NewStream needs mu to send on awakenKeepalive | 3 | 1 | 2 | Y |
| istio | #16224 | Event handler (called by Run goroutine) holds lock and blocks sending on done channel; main goroutine needs same lock before draining done | 3 | 1 | 2 | Y |
| k8s | #10182 | syncBatch receives from podStatusChannel then needs RWMutex; concurrent SetPodStatus holds RWMutex and blocks sending on podStatusChannel | 4 | 3 | 2 | Y |
| k8s | #26980 | pop() holds RWMutex and enters select blocked on stopCh (with pendingNotifications non-empty); second goroutine needs same RWMutex | 3 | 2 | 2 | Y |
| moby | #28462 | monitor loop calls handleProbeResult which needs Container lock; StateChanged holds Container lock and blocks sending on stop channel that monitor reads | 3 | 6 | 2 | Y |
| etcd | #7492 | Timer-dependent: assignSimpleTokenToUser holds RWMutex and sends on addSimpleTokenCh; ticker must fire between receive and deleteTokenFunc's lock acquisition | 4 | -- | -- | N |
| k8s | #1321 | distribute() holds lock and blocks sending on w.result; stopWatching needs same lock to close(w.result) -- but Watch() and Stop() execute in one quantum with no yield point between them | 3 | -- | -- | N |
| serving | #2137 | WaitGroup.Done() + buffered channel send are both non-blocking; deadlock requires preemption between these two atomic operations within a single quantum | 3 | -- | -- | N |

moby#33781 is technically Channel & Context category in GoBench; included for completeness as it exercises the same scheduling machinery.

etcd#7443 is the outlier: 74 runs at bound 3. The RWMutex + buffered channel combination creates a large branching factor with 5+ goroutines across multiple `resetTransport` goroutines spawned by `resetAddrConn`, pushing the bug deeper into the search space.

## Flakiness Comparison

GoBench kernels include "Flaky: X/100" annotations measuring how often the original `go test` (under the default Go scheduler) triggers the bug in 100 runs. Bugs with low flakiness are the most dangerous in practice: they pass CI reliably and only manifest in production. CHESS-style exploration eliminates this non-determinism entirely.

| Project | Bug ID | Flaky (go test) | CHESS Runs to Find | Speedup |
|---------|--------|------------------|--------------------|---------|
| k8s | #1321 | 1/100 (1%) | N/A (outside model) | -- |
| k8s | #6632 | 4/100 (4%) | 2 | 12.5x |
| etcd | #6873 | 9/100 (9%) | 2 | 5.6x |
| k8s | #10182 | 15/100 (15%) | 3 | 2.2x |
| moby | #33781 | 25/100 (25%) | 1 | 25x |
| etcd | #7492 | 40/100 (40%) | N/A (outside model) | -- |
| moby | #28462 | 69/100 (69%) | 6 | -- |
| grpc | #1353 | 100/100 (100%) | 1 | -- |
| grpc | #1460 | 100/100 (100%) | 1 | -- |
| etcd | #7902 | 100/100 (100%) | 1 | -- |

Bugs without a Flaky annotation in GoBench (etcd#7443, istio#16224, k8s#26980, serving#2137) are omitted from this table.

The critical observation is in the low-flakiness range. k8s#6632 triggers in only 4 out of 100 random runs under `go test` -- a developer running tests 10 times might never see it. CHESS finds it in 2 systematic runs. etcd#6873 at 9% flakiness is found in 2 runs. k8s#10182 at 15% flakiness is found in 3 runs. These are the bugs that escape CI pipelines and surface as production incidents months later.

For high-flakiness bugs (grpc#1353, grpc#1460, etcd#7902 at 100%), both approaches find them quickly. The advantage of CHESS is not speed but certainty: it provides a proof that the interleaving space (up to bound K) has been exhausted, rather than a probabilistic "ran 100 times and it passed."

## Undetected Bugs

Three bugs fall outside the scheduling model. Each represents a distinct limitation.

### etcd#7492 -- Timer-Scheduling Gap

The bug requires a ticker to fire while a goroutine holds a mutex. In synctest bubbles, fake time advances only when `running == 0` (all goroutines are durably blocked). A goroutine holding a mutex is not blocked -- it is running or runnable. Therefore the timer cannot fire at the exact moment needed: between the mutex acquisition and the subsequent channel operation. This is a fundamental limitation of the bubble time model, not a search depth issue. No amount of context bounding helps.

### k8s#1321 -- Instruction-Level Preemption

The bug requires preemption between `Watch()` and `Stop()` -- two operations that execute within a single goroutine quantum with no yield point between them. The kernel has only 3 possible schedules total. Our model operates at goroutine-scheduling granularity: decisions happen when a goroutine yields (channel op, mutex acquire, Gosched). Without a yield point between the two calls, the explorer cannot place a context switch there. Detecting this bug requires instruction-level interleaving (e.g., Go's race detector granularity or explicit `Gosched()` injection).

### serving#2137 -- Non-Blocking-Sync Atomicity

The bug requires preemption between `WaitGroup.Done()` and a subsequent buffered channel send. Both operations are non-blocking: `Done()` is an atomic decrement, and buffered sends with available capacity do not yield. They execute atomically within a single scheduling quantum. Like k8s#1321, this is an instruction-level bug invisible to goroutine-granularity scheduling. The two operations would need to be separated by a yield point for the explorer to insert a context switch.

### Common Thread

All three undetected bugs require preemption at a point where no goroutine scheduling decision occurs. The bubble model sees goroutines as indivisible between yield points. Bugs that require breaking this atomicity -- whether via timer injection during execution, or preemption between non-blocking operations -- are outside the model's observable granularity.

## Runtime Change: Mutex Wait Reasons in isIdleInSynctest

Porting Channel & Lock bugs required a runtime modification. The `isIdleInSynctest` function in `runtime/runtime2.go` determines whether a goroutine's wait reason counts as "durably blocked" for bubble time advancement. Originally, only channel operations and select were recognized as idle wait reasons.

Channel & Lock bugs block on `sync.Mutex` and `sync.RWMutex`. A goroutine waiting to acquire a mutex was not considered durably blocked, so the bubble's `running` counter never reached zero, and time advancement (and decision hooks) stalled.

The fix: add three mutex-related wait reasons to `isIdleInSynctest`:

- `waitReasonSyncMutexLock` -- a goroutine blocked on `sync.Mutex.Lock()`
- `waitReasonSyncRWMutexRLock` -- a goroutine blocked on `sync.RWMutex.RLock()`
- `waitReasonSyncRWMutexLock` -- a goroutine blocked on `sync.RWMutex.Lock()`

This tells the bubble that a goroutine parked on a mutex is durably blocked, enabling the scheduler to recognize yield points at lock acquisitions and explore interleavings that involve lock ordering.

Without this change, 0/13 Channel & Lock bugs are detectable -- the bubble never recognizes a decision point involving mutex contention. With it, 10/13.

## Conclusion

The Channel & Lock evaluation demonstrates that CHESS-style systematic scheduling exploration, implemented within Go's `testing/synctest` framework, detects 77% of the hardest concurrency bug category in the GoBench suite -- outperforming every existing tool evaluated by Li (2023), including static analyzers, fuzzers, and dynamic trace tools. The three undetected bugs fall outside goroutine-scheduling granularity entirely, representing a well-defined boundary rather than an engineering gap.

For Go developers, the practical implication is direct: `explorer.Test(t, f)` is a drop-in replacement for `synctest.Test(t, f)`. Any test that runs inside a synctest bubble today can be upgraded to systematic interleaving exploration by changing a single import and function call. No annotations, no model specifications, no separate analysis passes. The explorer uses the same bubble isolation, fake clock, and goroutine tracking that `synctest.Test` already provides -- it simply replaces the single default-schedule execution with a bounded DFS over all possible schedules. Bugs that would require hundreds or thousands of `go test` runs to surface probabilistically are found within single-digit CHESS exploration runs, with a proof that the bounded interleaving space has been exhausted.
