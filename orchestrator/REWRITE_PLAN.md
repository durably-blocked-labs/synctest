# Orchestrator Rewrite Plan

This is the aggressive cleanup branch. It is allowed to break while we converge
on one correct distributed testing model.

The rename is already in progress:

- public distributed harness: `orchestrator/`
- internal bubble wrapper: `orchestrator/internal/distributed/`
- local-only explorer remains in `explorer/` for later

The goal is not incremental polish. The goal is to get to one correct DFS
engine quickly, with an API that makes later algorithms like PCT easy to add.

## Current Reality

What is stable enough to keep:

- the forked Go runtime can expose bubble-local scheduler choices
- the transport interception model is the right integration point
- the orchestrator package is the right home for the distributed harness
- the metrics pipeline is useful as an observer layer

What is not trustworthy yet:

- runtime idle/time ownership is still keyed off `externalWait`
- replay is not strict enough
- the trace model is still flat, not round-based
- `select` is still random in the current Go fork
- distributed exploration still hangs on real scenarios

Concrete failing signal today:

- `GODEBUG=asyncpreemptoff=1 ./go/bin/go test -count=1 -timeout 8s -run 'TestGateBug_ExploreGlobalOnly' ./ra-depth2`
  times out in the orchestrator wait/bridge path

Concrete passing signal today:

- `./go/bin/go test -run '^$' ./orchestrator/... ./explorer/... ./experiments/... ./ra-depth2 ./ra-distributed-lock ./rafttest ./examples/...`
  compiles
- `GODEBUG=asyncpreemptoff=1 ./go/bin/go test -count=1 -timeout 8s -run 'TestGateBug_FIFOPasses' ./ra-depth2`
  passes
- `GODEBUG=asyncpreemptoff=1 ./go/bin/go test -count=1 -timeout 8s -run 'TestOrchestratorMutualExclusion' ./ra-distributed-lock`
  passes

## Target Public API

The user-facing distributed API should collapse to one package and one mental
model:

```go
type Algorithm interface {
	Init() State
	Next(State, Trace) (State, *Plan, bool)
}

type NodeTransport interface {
	Addr() string
	Outbox() <-chan *PendingOp
}

type Orchestrator struct { ... }

func New() *Orchestrator
func (o *Orchestrator) AddNode(transport NodeTransport, run func(*testing.T), opts ...distributed.Option)
func (o *Orchestrator) Run(t *testing.T) (RecordedRun, bool)
func (o *Orchestrator) Replay(t *testing.T, rec RecordedRun) (RecordedRun, bool)
func (o *Orchestrator) Explore(t *testing.T, setup func(*Orchestrator), opts ...ExploreOption) bool
```

Short term, DFS can remain the only algorithm wired through `Explore`.
Long term, DFS and PCT must schedule over the same choice model.

## Package Decisions

Keep public:

- `orchestrator/`
- later, `explorer/` for local-only exploration

Keep internal:

- `orchestrator/internal/distributed/`

Keep as fixtures, not architecture truth:

- `rafttest/`
- `ra-distributed-lock/`
- `ra-depth2/`
- `examples/`

Use as runtime validation:

- `experiments/`

## Execution Plan

### Phase 1: Finish Rename And Surface Cleanup

Status:
- mostly done

Tasks:

- remove remaining `orchestratorv2` references from kept docs
- keep `distributed` internal-only
- update examples/tests/docs to point at `orchestrator`

Exit criteria:

- compile-only sweep stays green
- no code imports reference `orchestratorv2`

### Phase 2: Runtime Ownership Semantics

Files:

- `go/src/runtime/synctest.go`
- `go/src/runtime/proc.go`
- `go/src/testing/synctest/synctest.go`

Tasks:

1. Change orchestrator ownership checks from `externalWait > 0` to
   `onDecision != nil`.
2. Make idle-hook firing depend on hook presence plus actual bubble idleness.
3. Make timer auto-advance stop whenever the hook is installed.
4. Make `maybeWakeLocked` follow the same ownership rule.

Exit criteria:

- runtime behavior matches `docs/TODO_model_vs_reality.md` items 1, 2, 3, and 9
- no bubble auto-advances time while orchestrator control is active

### Phase 3: Strict Replay

Files:

- `go/src/runtime/proc.go`
- `orchestrator/orchestrator.go`

Tasks:

1. Stop clamping or falling through on replay divergence.
2. Make prefixed local scheduler choices fail loudly if the chosen runnable
   goroutine is not available.
3. Make global replay fail loudly if the chosen queue index is invalid.
4. Record replay divergence step and reason for metrics.

Exit criteria:

- replay either reproduces the same execution shape or returns a precise
  divergence
- no silent degradation from replay into exploration

### Phase 4: Round-Based Trace Model

Files:

- `orchestrator/orchestrator.go`
- `orchestrator/metrics.go`

Tasks:

1. Replace the disconnected `GlobalTrace + LocalTraces` story with a round-based
   trace structure.
2. Each round should capture:
   - which node ran
   - the local scheduler segment for that node
   - the global outcome that followed: deliver, time advance, or done
3. Keep derivable compatibility helpers only if they are cheap.

Exit criteria:

- replay and DFS can operate on one canonical trace model
- metrics can report round counts and round sizes
- docs/model and implementation no longer disagree on the shape of a complete
  execution

### Phase 5: Clean DFS Core

Files:

- `orchestrator/orchestrator.go`
- `orchestrator/internal/distributed/distributed.go`

Tasks:

1. Keep one exact execution engine.
2. Rebuild DFS on top of strict replay and the round-based trace.
3. Treat current `ExploreAll` as disposable unless pieces are clearly reusable.
4. Start with correct global DFS first.
5. Then layer in G+L exploration only after exact replay is trustworthy.

Exit criteria:

- FIFO run passes on the intended fixtures
- exact replay reproduces or fails loudly
- DFS finds the intended distributed bug under the intended bound
- no orchestrator hangs in the active RA depth-2 scenario

### Phase 6: Transport Shutdown Contract

Files:

- `rafttest/transport.go`
- `ra-distributed-lock/orchestrator_transport.go`
- `ra-depth2/transport.go`

Tasks:

1. Define and enforce the shutdown rule:
   every registered waiter completes exactly once.
2. Remove lossy close paths and silent drops where they violate that rule.
3. Add counters so we can prove closes and pending responses drain correctly.

Exit criteria:

- no shutdown-related deadlocks in distributed tests
- transports expose enough signal to debug close behavior

### Phase 7: Example Cleanup

We need one clear ladder of examples:

1. `experiments/`
   runtime invariants only
2. `examples/`
   tiny integration demos only
3. `ra-depth2/`
   primary G+L bug scenario
4. `ra-distributed-lock/`
   broader RA fixture / metrics harness
5. `rafttest/`
   real-system adapter case study, not the current priority

Tasks:

- prune or relabel examples that are just historical scratchpads
- keep each package responsible for one purpose
- stop using examples as architecture docs

### Phase 8: Gist Scenario

Target:

- implement the full RA depth-2 walkthrough from the April 7, 2026 gist:
  3 RA nodes (`A`, `B`, `C`) plus a network KV server

Required pieces:

1. transport layer that routes all cross-node ops through the orchestrator
2. RA implementation with separate app / handler / deferred-flusher goroutines
3. KV server bubble on the network
4. assertions that prove the lost-update bug
5. empirical comparison against the estimates in the gist

This scenario becomes the main proof point for:

- G-only DFS should not find the bug
- G+L DFS with depth 2 should find it
- later, PCT and random baselines can be compared on the same scenario

### Phase 9: Metrics

Keep metrics as observer-layer instrumentation, not engine semantics.

Must-have next metrics:

- replay divergence point and reason
- duplicate trace rate
- global branching histogram
- first-failure metrics
- decision-cap saturation
- round count and round sizes
- pending queue max and average
- transport send/recv/close counters

Later runtime metrics:

- frontier hook count
- idle hook count
- auto-advance count
- `SetTime` count
- `external` vs `externalWait`
- runq size histogram
- wait-reason histogram

### Phase 10: PCT

Do not start until DFS is correct on the same choice model.

PCT should be a new algorithm over the same frontier abstraction, not a new
engine.

## Current Priority Order

If time is tight, do exactly this:

1. runtime ownership semantics
2. strict replay
3. round-based trace
4. clean DFS core
5. transport shutdown contract
6. RA depth-2 gist scenario
7. metrics expansion
8. PCT

## Proof Checklist

We should not claim correctness until these are true:

- hook installed means orchestrator owns idle/time
- replay fails loudly on divergence
- FIFO run works on the active fixtures
- DFS finds the intended RA depth-2 bug at the expected bound
- transport shutdown completes every waiter exactly once
- trace model matches the documented model closely enough to explain runs
- select remains out of scope until it becomes a real scheduler decision point
