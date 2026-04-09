# Model vs Reality — Changes Needed

Things we discover while writing the formal model that don't match the
current implementation. Each item needs a code change.

## 1. Timer auto-advance should check hook, not externalWait

**Model says:** If a decision hook is set, the orchestrator owns time. The bubble should never auto-advance.

**Reality:** The code at `synctest.go:536-540` only skips auto-advance when `externalWait > 0`. If the hook is set but `externalWait == 0` (e.g., between ExternalWait calls), the bubble auto-advances time without consulting the orchestrator.

**Fix:** Change the timer auto-advance guard from `if bubble.externalWait > 0` to `if bubble.onDecision != nil`. The hook being set means an orchestrator is in control.

**File:** `go/src/runtime/synctest.go` line 536-540

## 2. Idle hook should fire based on hook being set, not externalWait

**Model says:** The idle hook fires whenever the bubble is idle (`running == 0`, runq empty) and a decision hook is set. ExternalWait is irrelevant to idle detection — it's just the mechanism for crossing the bubble boundary.

**Reality:** The code at `synctest.go:489` checks `bubble.externalWait > 0 && bubble.onDecision != nil && runqempty(bubble.pp)`. The idle hook only fires when goroutines are in ExternalWait.

**Fix:** Change idle hook condition from `externalWait > 0 && onDecision != nil && runqempty` to `onDecision != nil && runqempty && running == 0`. The hook being set means an orchestrator exists. If the bubble is idle, tell the orchestrator — regardless of why goroutines are blocked.

**Consequence:** `externalWait` counter is still needed (reported in BubbleState so the orchestrator knows HOW goroutines are blocked) but it no longer gates the idle hook.

**File:** `go/src/runtime/synctest.go` line 489

## 3. Idle resolution priority order

**Model says:** The check order is:
1. `external > 0` → wait (bubble's responsibility, even if hook set)
2. Hook set + running == 0 + runq empty → idle hook fires
3. No hook + timers → auto-advance
4. No hook + no timers → deadlock

**Reality:** The code checks `externalWait > 0` before `external > 0` in some paths, and the idle hook condition is entangled with `externalWait`. Need to reorder to match the model's priority.

**File:** `go/src/runtime/synctest.go` synctestRunImpl main loop

## 4. Global trace should be index-based, not content-based

**Model says:** The global trace is a sequence of entries: Deliver(index), TimeAdvance(t), Done(node). Deliver uses an index into the pending queue. Replay follows the indices. The index is valid because the trace up to that point determines the queue contents.

**Reality:** `RecordedRun.GlobalTrace` uses `GlobalStep{Type, From, To, OpType, Time}` — content-based. `Replay` matches by `{From, To, Type}` via `findDeliveryOp`. This is fragile (two identical RPCs in the queue → wrong match). `Explore` uses `GlobalDecision{Index, QueueSize}` but only for the prefix, not the full recorded trace.

**Fix:** Unify. `RecordedRun` should carry `GlobalDecisions []GlobalDecision` alongside (or instead of) the content trace. Replay uses indices, not content matching. Delete `findDeliveryOp`.

**Note:** The orchestrator cleanup plan (REWRITE_PLAN.md) already describes this. Partially implemented by Allan's branch.

**File:** `orchestrator/orchestrator.go`

## 5. Done(node) not recorded as a global trace entry

**Model says:** Done is an observation recorded in the global trace so replay knows when to expect a bubble to exit.

**Reality:** StepDone IS recorded in GlobalTrace but not in GlobalDecisions. Replay doesn't use it for synchronization — it just logs it.

**Fix:** Ensure replay checks Done entries to avoid waiting for a bubble that already exited.

**File:** `orchestrator/orchestrator.go`

## 6. Complete trace should be round-based (flat sequence of rounds)

**Model says:** The complete trace is a flat sequence of rounds. Each round = (which node ran, its local trace segment, the global decision that followed). See Definition 6.3 in model.md.

**Reality:** `RecordedRun` has separate `GlobalTrace []GlobalStep` and `LocalTraces map[string][]Decision`. The local and global traces are disconnected — you can't tell which local decisions happened before which global decision. There's no round structure.

**Fix:** `RecordedRun` should be a flat `[]Round` where each Round carries: node name, local segment ([]Decision), and global decision (deliver index / time advance / done).

**File:** `orchestrator/orchestrator.go`

## 7. Select should be a true yield point with hook

**Model says:** Select with multiple ready cases should park the goroutine, fire the decision hook, and resume with the chosen case index — same mechanism as goroutine scheduling decisions.

**Reality:** Select uses `selectCounter` (seed-based deterministic counter). The goroutine does not park. Select ordering is deterministic for a given seed but not independently controllable per-decision.

**Fix:** In `selectgo`, when multiple cases are ready inside a bubble: park the goroutine, signal root, root calls hook with ready cases, hook returns chosen index, wake goroutine. Same pattern as `findRunnable` → root → hook for scheduling decisions.

**File:** `go/src/runtime/select.go`

## 8. Transport shutdown: makeRPC Phase 2 can hang

**Model says:** Every `respCh` registered in pending receives exactly one value. `Close()` drains all pending. Phase 2 (`resp := <-respCh`) always completes.

**Reality:** There are races between `Close()` draining and new `makeRPC` calls registering in pending. The TOCTOU checks between Phase 1 and Phase 2 are a workaround, not a fix. The pending-drain contract is not airtight.

**Fix:** Ensure `makeRPC` registers `respCh` BEFORE Phase 1. `Close()` sets a closed flag, closes closeCh, then drains pending. Any `makeRPC` that registered before `Close()` gets the error via drain. Any `makeRPC` that starts after `Close()` sees the closed flag and returns immediately.

**File:** `rafttest/transport.go`

## 9. maybeWakeLocked should check onDecision, not externalWait

**Model says:** The hook being set means an orchestrator exists. `maybeWakeLocked` should wake root when idle and hook set — regardless of `externalWait`.

**Reality:** `maybeWakeLocked` checks `externalWait > 0 && onDecision != nil`. Should just check `onDecision != nil`.

**Follows from:** Items 1-3 (same principle: hook set = orchestrator controls everything).

**File:** `go/src/runtime/synctest.go` maybeWakeLocked
