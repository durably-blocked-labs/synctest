# Bounded independent trials

`Store` writes an exclusive directory containing an immutable versioned manifest
and checked, synced JSONL start/checkpoint/terminal records. The first persistence
error is sticky. Call and check `Close`. `orchestrator.ArtifactSink` supplies the
trial integration; legacy metrics observers are not replay artifacts.

Supply actual executable/runtime/source (including dirty-state) identities,
input digest, effective environment, policy, seed, and reset policy. A seed alone
is not replay identity. The maintained Raft adapter uses a 64-operation attached,
nonblocking outbox, a 16-envelope mailbox and at most 256 pending RPCs; exhaustion
is a resource outcome, not an assertion that Raft is faulty. Record those model
limits in the manifest's policy/reset description. Distributed execution requires
`GOMAXPROCS >= node count + 1` (and at least 2): frozen node roots retain their
owner Ps and the coordinator needs a spare host P. Insufficient capacity is a
pre-start infrastructure/configuration rejection, not an automatic increase.
Explicit `runtime.GOMAXPROCS(2)` overrides an environment value of 4.
Qualified transports explicitly declare `attached-bounded/v1`: no blocking or
detachment between publication registration and completion, and pending
publication count must be zero at every frozen snapshot. Detached/legacy
transports are rejected before targets start by strict replay/checked sinks;
`Publications.Wait` alone is not a supported detached-callback progress proof.
Default orchestration retains
delivery-before-time and non-FIFO deviation bounds, not preemption bounds.

`Inspect` rejects corruption, ordering errors, checksum mismatches and unsupported
versions. Explicit tail salvage returns only the verified prefix and marks it
incomplete. A killed in-progress trial may have only its synced start record;
there is no claim of incremental decision durability. No filesystem I/O occurs in
runtime scheduling hooks. Checkpoints are optional coordinator-side records.

`python3 campaign/runner.py tasks.json fresh-output --workers 2` runs finite,
explicitly independent tasks in separate process groups. Example task manifest:

```json
{"version":1,"tasks":[{"id":"seed-42","seed":42,"independent":true,"argv":["/absolute/path/to/test-worker"],"seconds":30,"max_bytes":1048576,"expected":["passed"],"exit_codes":[0]}]}
```

The worker receives `CAMPAIGN_TASK_DIR`, `CAMPAIGN_TASK_ID`, and
`CAMPAIGN_TASK_SEED`, plus `CAMPAIGN_TASK_MANIFEST_DIGEST`,
`CAMPAIGN_COMMAND_DIGEST` and `CAMPAIGN_INPUT_DIGEST`. Set `Manifest.Task` from
`TaskBindingFromEnv()` and set the actual `InputDigest` to the dispatched input
digest (an explicit task `input_digest`, or the digest of its `input` object).
Set top-level `Manifest.Seed` to the same effective seed as `Manifest.Task.Seed`;
both Go and runner validation reject disagreement.
The runner rejects cross-task/stale bindings, and pins the accepted full artifact
digest in the checkpoint to detect later replacement. It must create
`CAMPAIGN_TASK_DIR/artifact` with `Store`,
produce at least one complete terminal record, and exit with an allowed code.
Expected assertion failures need both an explicit allowed exit and a validated
assertion terminal record. Compile failures, timeouts, incomplete/unsafe terminals,
resource/infrastructure failures and zero exit without artifacts stop dispatch.

Workers default to one; task IDs/seeds and attempt paths are assigned before
dispatch, independently of completion order. `--resume` validates completed task
artifacts and never re-dispatches completed tasks. Failed/interrupted independent
tasks get new attempt directories, preserving prior evidence. `--only-task ID
--workers 1` uses a fresh destination for a single-task **rerun**; it is exact
trace replay only when that task's command explicitly invokes strict replay
against a pinned trace artifact. This resumes a
finite task list, **not a serialized DFS frontier**. The strict replay adapters
still require the worker to validate the original manifest before executing.
Schedule-only unified replay is exact for the recorded schedule/delivery stream,
not control of select or the whole execution. Explicit select control supports
bubble-owned channel cases only; host-owned outbox/control selects in the
Raft RPC adapter fail closed if select control is requested. Pending RPC and
response-forwarder waits use bubble-owned response/local-stop channels. Host
`Close` never sends to or closes those channels: the reattached bridge closes
local-stop. Closing a peer after request publication is an unsupported whole-trial
infrastructure stop, not a modeled response; ordinary teardown cancels first. PCT policy
`pct-all-decisions/v2` samples all decision steps including singleton steps; it
can change later seeded choices relative to the former singleton policy.

The host watchdog bounds wall time and checks output bytes while supervising;
checks have a polling window and are not an OS disk/RSS quota. Children are joined
on handled cancellation, with spawn/registration and containment cleanup protected
from interruption. Failed containment retains the lease. Resume rebinds every
immutable task field against the original manifest before dispatch; every terminal
is checked for completeness and safety. Checkpoints use unique atomic temporary
files, so an interrupted or stale temporary cannot block later publication.
A hard-killed runner leaves `runner.lock` deliberately:
resume is blocked until a supervisor proves child containment. Do not remove the
lease just to bypass an unjoined worker. `Ready.Wait` is not context-cancellable;
a stuck runtime readiness wait needs this process-level watchdog, not a leaked
waiter goroutine or in-process reset. Bubbles share a heap; no DPOR soundness or
long-campaign qualification is implied.
