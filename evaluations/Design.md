# Bubbles Evaluation Framework

## Goal

The evaluation framework runs finite distributed-system scenarios under multiple scheduling algorithms. The framework records assertion failures, bounded progress failures, bubble failures (deadlocks), coverage, and replayable traces.

We're starting our evaluation by targeting Hashicorp's RAFT


## Architecture

The framework has three layers:

1. A supervisor selects and operates evaluation jobs.
2. A worker runs one atomic `(scenario, algorithm)` job.
3. The existing orchestrator coordinates bubbles and executes one schedule at a time.

## Supervisor

The supervisor is the user-facing command. It expands the requested scenarios and algorithms into `(scenario, algorithm)` jobs, generates shared scenario inputs, starts isolated worker processes, and aggregates their status.

An attempt is one reproducible input to a scenario. Its number determines the seed, initial state, command sequence, topology plan, and Raft configuration. Every selected algorithm receives the same input for a given attempt number, so the only intended difference is how it schedules that input.

The supervisor owns:

- Job selection and parallelism.
- Wall-clock budgets.
- Shared scenario inputs for each attempt.
- Graceful interruption and forced termination.
- Resume policy.
- Campaign-level status and result indexing.

The worker owns:

- Algorithm exhaustion detection.
- Execution of complete schedules.
- Per-run persistence and replay artifacts.
- Algorithm checkpoints between schedules.
- Worker-level summaries.

This split keeps policy and process management out of the scheduling engine. The supervisor can stop, resume, or filter a large evaluation without changing how an individual schedule executes. Workers isolate algorithms from one another and save progress independently. The existing orchestrator remains focused on the smaller problem it already solves: coordinating bubbles during one execution and asking an algorithm which runnable operation to choose.

### Command-line flags

- `--scenario <name>` selects a scenario. It may be repeated. Omission selects all registered scenarios.
- `--algorithm <name>` selects an algorithm such as `chess`, `dpor`, `pct`, or `random`. It may be repeated. Omission selects all registered algorithms.
- `--attempt-min <n>` selects the first attempt number to run.
- `--attempt-max <n>` selects the last attempt number to run, inclusive. Every algorithm runs the same attempt range.
- `--parallel <n>` limits concurrent `(scenario, algorithm)` workers.
- `--duration <duration>` sets the wall-clock budget for each attempt. It applies equally to every algorithm.
- `--bound <n>` sets the context bound used by CHESS and DPOR.
- `--output <path>` selects the campaign result directory.
- `--resume` resumes compatible incomplete jobs from their checkpoints.
- `--grace-period <duration>` controls how long workers may finish their current schedule after interruption.
- `--stop-on-failure` stops a worker after its first non-divergent finding. The default is to record findings and continue.
- `--retain-traces <mode>` controls successful trace retention. Failure and bubble-failure traces are always retained.

Algorithm-specific parameters, such as PCT depth and maximum steps, belong in the saved job configuration. CLI flags may override them when needed.

### Interruption

The first interrupt asks every worker to stop after its current schedule, flush results, save a checkpoint, and write an interrupted summary. A second interrupt or grace-period expiry terminates remaining workers. Completed records remain usable even if a worker is forcibly terminated.

## Worker

A worker receives one scenario, one algorithm configuration, and the selected attempt range. Process isolation prevents algorithms from sharing runtime state, randomness, `testing.T` state, or output files.

For each attempt, the worker:

1. Restores or initializes the algorithm state.
2. Creates a fresh orchestrator setup for each schedule.
3. Runs one complete schedule.
4. Evaluates the resulting stable states and scenario result.
5. Appends the run record and required trace.
6. Saves the algorithm checkpoint.
7. Continues until exhaustion, a supervisor budget, or cancellation.

Every algorithm receives the same wall-clock budget for each attempt. CHESS and DPOR also use a context bound and may exhaust all schedules within that bound before time expires. PCT and Random continue generating schedules until the time budget expires. Cancellation is observed between schedules. Partially executing bubbles are not checkpointed.

### Wrapping the orchestrator

The worker wraps `orchestrator.Orchestrator.ExploreWith` rather than embedding campaign behavior into the orchestrator. The wrapper supplies:

- A scenario setup factory that recreates execution state.
- The selected scheduling algorithm.
- A stable-boundary observer for invariants and completion predicates.
- A run observer that converts `RunResult` into evaluation records.
- A cancellation check before starting the next schedule.

Evaluation findings should not call `t.Fatalf` as their primary reporting mechanism. They are structured run results so exploration can continue. A separate test adapter may fail a Go test after the evaluation completes.

### Results

Each completed schedule has one result:

- `no_failure`
- `program_failure`
- `framework_failure`
- `progress_budget_exceeded`

An optional message provides detail without expanding the main result type. Program failure details include invariant violations, test assertions, and bubble failures in the program under test. Framework failure details include orchestration, cleanup, persistence, and replay errors. A bubble failure does not initially distinguish panic from deadlock.

### Persistence

Each worker writes to its own directory:

```text
results/<scenario>/<algorithm>/
├── job.json
├── attempts.json
├── runs.jsonl
├── checkpoints/
├── traces/
└── summary.json
```

`job.json` records the scenario, algorithm configuration, schema version, repository revision, runtime revision, and time budget. `attempts.json` records the reproducible scenario input associated with each attempt number.

`runs.jsonl` receives one append-only record per completed schedule. A record includes the attempt, run number, result, optional detail, timing, decision counts, state and trace fingerprints, and replay-trace reference.

Failure and bubble-failure traces are always written under `traces/`. Successful traces may be omitted or sampled to control storage.

Checkpoints are atomically replaced between schedules. CHESS and DPOR checkpoints include pending prefixes and seen prefixes. PCT and Random can resume from their seed and completed-run count.

`summary.json` is atomically replaced when the worker exits and records totals, first findings, completed attempts, exhausted attempts, elapsed time, and termination reason.

JSONL preserves completed work during interruption and remains compatible with the existing chart pipeline. Graph generation is a separate command built on the persisted schema.

## HashiCorp Raft Workloads

The Raft integration translates the intent of the student-Raft tests rather than their harness-specific implementation. Absolute client-command indexes, encoded RPC byte thresholds, and probabilistic sleeps are not portable to HashiCorp Raft.

### Cluster abstraction

The scenario-facing cluster API separates actions, observations, invariants, and bounded progress conditions.

Actions include:

- Submit one or more commands.
- Disable or enable directional links.
- Isolate a node or partition node sets.
- Heal the network.
- Later, stop and restart nodes or request snapshots.

Observations are immutable cluster views captured only at stable scheduling boundaries. A node view includes whether it is running, Raft state, term, recognized leader, last index, commit index, applied index, copied FSM history, persistent log range, and connectivity.

Raft operations execute inside the owning node's bubble through a node-local agent. A scenario controller requests operations without directly invoking another bubble's Raft instance. Control actions take effect deterministically at stable boundaries and are recorded, but they are not choices explored by the scheduling algorithm.

### Network failures

Packet drops are not an independent scheduling dimension. A partition disables links deterministically.

When a link is disabled:

- Undelivered requests and responses crossing the link fail.
- Their RPC callers are unblocked with a transport error.
- Already delivered requests remain effective.
- Healing permits new RPCs but does not resurrect failed traffic.

This models an unavailable link without leaving pending RPCs that prevent virtual-time advancement.

### Safety invariants

Safety invariants run at every stable scheduling boundary:

- At most one node is leader in a given term.
- Node terms, commit indexes, and applied histories do not regress.
- Applied indexes do not exceed commit indexes.
- Commit indexes do not exceed last indexes.
- Nodes do not apply different commands at the same log index.
- A node does not apply the same index twice.
- Successfully completed commands remain present in later states.

Only nonblocking synchronized Raft APIs, copied FSM histories, and synchronized store reads are used by boundary observers. `Raft.Stats()` is unsuitable because it performs a potentially blocking configuration query.

### Progress conditions

Liveness is checked only at explicit scenario checkpoints under stated connectivity and fairness assumptions:

- A connected majority elects a leader.
- A command reaches the required replicas.
- A healed follower catches up.
- An isolated minority cannot complete a new command.

Exceeding a virtual-time or scheduling budget is reported as `progress_budget_exceeded`, not as a safety violation.

HashiCorp Raft's `ErrLeadershipLost` is treated as an unknown command result because the command may still commit. Successful apply futures must correspond to an applied command. Rejected or ambiguous submissions are recorded separately.

### Translation scope

The first implementation covers elections, re-elections, majority and minority partitions, basic and concurrent agreement, follower and leader failure, rejoin, and divergent-log repair.

Crash/restart persistence, finite Figure 8 and churn workloads, and snapshot installation follow after durable node lifecycle support. RPC counts become metrics. Encoded RPC byte assertions are omitted because the controlled transport passes typed values instead of production wire encodings.

## Orchestrator and Bubble Changes

The worker remains outside the core orchestrator, but the orchestrator needs several focused extensions.

- **Stable-boundary observation.** Add a callback invoked after all active bubbles report idle and before the next global delivery or time advance. It receives an immutable application-provided state snapshot and may record invariant violations or declare a scenario phase complete. It must not perform blocking Raft operations.
- **Deterministic control actions.** Allow the scenario controller to enqueue fixed actions that execute at stable boundaries. These actions are recorded in the execution history but are not presented to CHESS, DPOR, PCT, or Random as alternatives.
- **Cancellation and termination.** Allow `ExploreWith` to check a worker-owned cancellation signal before starting another schedule. Return an explicit termination reason such as exhausted, time limit, interrupted, failure, or infrastructure error. The orchestrator does not interrupt a running schedule during normal graceful shutdown. The worker process timeout remains the fallback for a schedule that never reaches a boundary.
- **Failure reporting.** Preserve bubble failures as structured `RunResult` results instead of silently excluding them from failure counts. The initial API may combine deadlocks and panics because `synctest.Explore` currently erases the distinction.
- **Full replay.** This is not required for the initial framework, but should be fixed later. Replay should accept the complete unified trace, including local and global decisions. Every RPC request and response needs a stable unique identity. Replay should validate the decision kind, node, chosen identity, and available alternative identities at each step, then report whether the original result reproduced or where execution diverged.
- **Algorithm checkpoints.** Checkpointable algorithms serialize and restore between-run search state. Checkpoints never include live bubble state. Restored configuration must match the scenario, attempt, algorithm, and runtime identifiers in the saved job configuration.
- **Transport topology.** The Raft transport needs synchronized directional link state and a way to fail queued or pending crossing RPCs when links are disabled. These failures must unblock both request and response waiters and remain deterministic under replay.
- **Cleanup.** Every schedule must close transports, unblock pending RPCs, stop Raft instances, and wait for bubble completion before the next setup begins. Cleanup failures are framework failures rather than application safety findings.
