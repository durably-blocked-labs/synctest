# Resource lifecycle scheduling probe

Two goroutines share a resource. The worker calls Claim, Read, Compute, Commit
and Release. Cleanup checks whether the resource is unused, then may close it
using that potentially stale observation. Closed resource operations return
`ErrClosed`. Both goroutines finish before the harness classifies the outcome.

This is an injected logical bug. The fixture adds no yields between operations;
a source function boundary is not automatically a scheduling frontier.

The one-shot `TestResourceGCWorker` is enabled only by absolute
`RESOURCEGC_REQUEST` and `RESOURCEGC_RESULT` paths. It executes one trial per
process, checks actual collection counters and saves both complete runtime
decisions and application actions/states. Expected application errors are data,
so an external DFS can continue. Runtime failures and limits remain incomplete.
The worker supports the original one-processor zero-GC baseline and a separate
two-processor host-driver treatment. Setup GC is outside the measured interval.
The host driver is created and signaled at the first actor entry in every
treatment, including its matched zero-GC control. The application joins its
actors and finishes the bubble before the host joins the GC driver. Thus driver
completion adds no application scheduling decisions. Divergent or unsafe trials
terminate without joining or starting another workload. It calls `runtime.GC`
0, 1, 2, 4 or 8 times; automatic collections remain disabled and counters must
match the request. Application operations contain no added GC calls or waits.

Build the package's test executable using a sealed custom GOROOT and the
repository's external-watchdog procedure. Run prefix DFS with:

```sh
python3 scripts/resourcegc-dfs.py \
  --worker /absolute/path/resourcegc.test \
  --goroot /absolute/path/sealed/go \
  --output /absolute/path/new-run-directory
```

An optional `--reference /absolute/path/model.json` validates observed
application transitions and complete action words against a separately
enumerated reference. The JSON contains `initial_state`, `states`, `transitions`
(`before`, `action`, `after`) and `histories` (`actions`, `error_at`).

DFS validates prefix identities, explores every exposed alternative and stores
pending work. `frontier_empty` means the exposed prefix tree was exhausted;
timeouts, limits, mismatches and unexpected collection counts prevent that result.
This does not establish coverage of all legal Go interleavings. Deterministic
startup decisions before hook installation are retained and checked; an
unhooked branching decision makes the run unsupported.

Generate the independent finite reference model, then run a bounded
characterization with the same compiled worker in all conditions:

```sh
python3 scripts/resourcegc-model.py > /absolute/path/reference-model.json
```

```sh
python3 scripts/resourcegc-sweep.py \
  --worker /absolute/path/resourcegc.test \
  --goroot /absolute/path/sealed/go \
  --reference /absolute/path/reference-model.json \
  --output /absolute/path/new-sweep-directory
```

The sweep retains rejected prefixes and separately tests baseline schedules and
within-treatment replay. An accepted scheduling identity is not counted as
reproduction unless the application observation also matches. The worklist
does not enumerate host-driver or collector timing, even when no collection
is requested. Only the original no-driver configuration retains the stronger
conditional prefix-tree exhaustion interpretation.

GC call windows and actor lifetimes use a test-only link to `runtime.nanotime`,
the real monotonic clock in the qualified runtime. Bubble `time.Now` uses
simulated time and cannot measure this overlap. Overlap of a `runtime.GC` call
with an actor lifetime is not evidence of stack scanning or suspension during
that lifetime. Requested, completed, overlapping calls and active-actor scans
must be reported separately; later calls may occur after both actors finish.
The saved native trace includes bubble shutdown. No events are filtered to make
zero-GC and nonzero-GC traces match. Host-driver and collector timing remain
uncontrolled.

## Experimental call and GC observations

The worker now requires the experimental `testing/synctest` observation API:
`EnableObservations`, `RecordObservation` and `Observations`. Use the pinned modified runtime and build a complete sealed GOROOT with
`scripts/build-go.sh` before compiling the worker.

Call entry/return marks and scheduler choices share a sequence with GC scan
and resume-request observations. Source names and stack frames are resolved
in the capturing process. Absolute PCs are diagnostic, not replay identities.
Recording allocates a bounded buffer and walks stacks, so comparisons describe
the instrumented workload. Overflow or incomplete publication invalidates an
exact timeline claim. BGID-zero records can refer to control/runtime goroutines
and must not be identified as a named application actor.

The runtime records owned preemption stacks, processor STW stop/resume events,
stack-scanning suspension and internal GC continuations. `gc_waiting` is the
runtime's world-stop flag; it can also be true for a non-collecting stop such as
`runtime.ReadMemStats`. The request field `stw_probes=1` runs that separate
zero-collection control, checked independently of forced-GC counters.

Internal pauses preserve the selected actor's turn while the collector actually
scans or wakes it. Explicit application blocking and `runtime.Gosched` remain
scheduling boundaries. This is cooperative exploration: a CPU spinner that
never blocks or yields can prevent a peer from running. Use fixed topology with
one P per bubble plus at least one ordinary host P. Dynamic processor resizing
and capacity changes during a run are unqualified. A GC attempt without host
capacity fails closed.

Run qualification tests separately in fresh, externally watched processes.
`TestAutomaticGCOwnershipQualification` requires an absolute
`RESOURCEGC_AUTO_OWNERSHIP_RESULT` output path. It checks real automatic
collections and that an allocating actor retains its turn. The scan and assist
fixtures distinguish an observed collector path from an inconclusive run;
a skipped fixture is not a passed qualification. The liveness build tag retains
only the process-terminal no-host-capacity check. Private fault-injection
runtime hooks are excluded from the published toolchain.

The finite resource model has 11 operation histories, four failing, and 27
states. Without added per-operation yields, this runtime exposes two histories,
one failing, and 11 observed states. Exhausting those exposed schedules does
not cover every operation-level interleaving. Compare full schedule identities
and every application snapshot across GC treatments, and report incomplete
trials separately. Historical evidence belongs to its original toolchain;
rebuild and rerun after runtime changes.

Render a saved worker result with:

```sh
python3 scripts/resourcegc-timeline.py /absolute/path/result.json
```

`TestObservationScanProbe` separately qualifies GC stack capture with a parked
actor and two processors: the runtime keeps GC workers off the bubble-owned
processor. A scan with `Stopped=false` is not evidence that GC interrupted a
running actor. The zero-GC DFS remains a separate one-processor configuration.
