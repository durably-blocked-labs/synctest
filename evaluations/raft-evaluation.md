# HashiCorp Raft Evaluation

Part of the [Bubbles Evaluation Framework](Design.md).

## Open Questions

### Defining workloads

The proposed initial workload elects a leader and submits concurrent commands to a simple application state machine. FIFO, random, and DPOR scheduling can exercise the same workload with different execution and message-delivery orders. The existing DPOR implementation uses a node/endpoint dependency model; its coverage is limited to that model and the configured search bound.

- Which setup and lifecycle responsibilities should a reusable Raft wrapper handle beyond the existing Bubbles API?
- Which HashiCorp tests or scenarios are suitable as an initial workload corpus?

The existing API supports hand-written workloads. A node function is the `func(t *testing.T)` passed to `orch.AddNode(transport, nodeFn)`, which Bubbles executes inside that node's bubble. For Raft, this function starts the transport bridge, creates stores and the Raft instance, performs node-local workload actions, and handles shutdown. Goroutines created by the function belong to the same bubble.

`ExploreWith` invokes a setup callback before each execution. The callback creates fresh transports, connects peers, and registers node functions with `AddNode`. Bubbles then executes those functions inside their respective bubbles. Raft instances and their internal channels are constructed inside the node functions; transport wiring occurs in the setup callback. Immutable workload configuration can be shared across executions, while mutable stores, transports, and observations are recreated.

A thin Raft helper could encapsulate this setup and lifecycle code. The following example illustrates the existing exploration API; `setupRaftWorkload` represents a proposed helper rather than an existing function.

```go
orch := orchestrator.New()
orch.ExploreWith(t, setupRaftWorkload,
    &orchestrator.DPOR{Bound: 2},
    orchestrator.GlobalMaxRuns(100))
```

Candidate scenarios in `raft/raft_test.go` include `TestRaft_ApplyConcurrent`, `TestRaft_LeaderFail`, and `TestRaft_RemoveLeader`, with concurrent applies as the initial target. Their cluster helper, `makeCluster` in `raft/testing.go`, is internal to the Raft package and relies on its test infrastructure. These scenarios therefore require adaptation rather than direct execution through Bubbles. Exploring additional schedules can extend their coverage without introducing new application operations.

### Checking behavior with minimal integration friction

Raft's public APIs (`Apply`, `State`, `Leader`, and `Stats`) and its application-supplied finite state machine (FSM) provide observation points. Returned errors and panics reveal some failures but do not detect silent incorrect behavior. Recording applied commands permits cross-node history comparisons without a complete global snapshot: different commands applied at the same log index indicate a violation. Final-state comparisons must account for replicas that have not yet caught up.

A monotonically increasing sequence is a safety property, not a progress guarantee: it can remain unchanged forever. A progress check could require a submitted command to complete within a chosen virtual-time limit while a majority remains connected.

- What is the smallest amount of adapter code or instrumentation needed to collect these observations and check progress?
- Are existing application hooks sufficient, or would targeted internal instrumentation justify its additional integration cost?

One option is a recording FSM () whose `Apply(*raft.Log)` stores the log index and a copy of the command bytes in a per-node history. Comparing entries at shared indexes after execution checks agreement among observed applications through Raft's existing extension interface. This does not check every consensus property; properties requiring additional internal events may need targeted instrumentation.

The FSM represents the application state replicated by Raft. Raft handles consensus and calls `FSM.Apply` when a committed command is ready to execute. A key-value FSM, for example, interprets commands that update keys and values. A test can supply a recording FSM without integrating a full application; the interface also requires snapshot and restore support.

In-memory store inspection offers another observation mechanism for protocol-level checks. The cluster setup can retain store references and read entries through `FirstIndex`, `LastIndex`, and `GetLog` without modifying Raft. Wrappers around the `LogStore` and `StableStore` interfaces could record writes and deletions when execution histories are required.

Stored entries are not necessarily committed or applied. Raft can legitimately retain different uncommitted suffixes on different nodes, so comparing all stored entries would produce false failures. Committed-log agreement additionally requires consistent observations of commit indexes, while application behavior requires observations beyond the store. Store observations should occur inside the owning bubble at defined checkpoints or after execution has stopped. Command bytes must be copied because `InmemStore.GetLog` returns a shallow copy.

- Which properties can be checked through store inspection, and which require a recording FSM or commit-index observations?

Progress checks can wait on futures returned by `Apply` inside the node's bubble. The `Apply` timeout bounds enqueueing rather than the entire commit-and-apply operation; an end-to-end deadline requires a separate bubble-local timer competing with completion. Cluster setup, observation hooks, and completion checks should be reusable across scenarios to reduce integration effort.

### Determining completion and failure

Workload completion must be explicit so application failures can be distinguished from cleanup errors and execution-limit exhaustion. Exhausted budgets are inconclusive rather than successful outcomes.

- How should workloads express completion conditions and expected errors with minimal setup?

For concurrent applies, command identifiers can associate submissions with successful futures, returned errors, or operations still pending at the deadline. Successful futures provide an initial completion criterion; follower catch-up is a separate condition when required. Followers must remain active until the workload finishes. A reusable helper could manage this lifecycle while scenarios specify commands and expected outcomes. Errors such as applying to a non-leader may be expected and are not automatically bugs.

### Replay and model limitations

The initial evaluation will assess whether existing replay reproduces workload failures. Replay discrepancies may require stronger request identities or control over random timeout choices; source, destination, and RPC type alone do not distinguish repeated messages.

Initial workloads will use the current time policy, which delivers pending messages before advancing global time. Scenarios requiring timeouts while messages remain pending would motivate an extension to this model.

`ExploreWith` currently counts a failed execution as a bug only when `UserFailed` is set and the run has not diverged. Generic failure detection therefore requires an assessment of how deadlocks and panics are reported and counted, in addition to application assertions.

- Under what conditions does replay reproduce the same application failure?
- Which workloads expose limitations in the current time and delivery policy?
- How should application failures, replay divergence, framework failures, and exhausted budgets be classified?

## Initial evaluation harness

The exported `evaluations/raft` package provides a reusable setup for real
HashiCorp Raft nodes. Each execution creates fresh in-memory stores, recording
FSMs, and orchestrator-controlled transports. A workload is a per-node test
function invoked inside that node's bubble after Raft starts. The helper keeps
all nodes alive until every workload function finishes, then shuts the cluster
down.

Initial integration workloads cover leader election, command replication, and
concurrent command application. They are deliberately small seeds for longer
generated workloads rather than the final benchmark corpus.

A stale-term prototype uses an evaluation-only HashiCorp Raft mutation to
submit fresh- and stale-term `AppendEntries` messages concurrently. It remains
outside the initial framework change because publishing it requires a separate
Raft fork and submodule update.

Message fingerprinting is a separate extension point for feedback-guided
exploration. A transport-provided fingerprint should identify semantically
relevant message fields while remaining distinct from the resource identity
used by DPOR's dependency relation. Fingerprint sequences can then provide
coverage feedback or trace equivalence without assuming that messages with the
same fingerprint commute.
