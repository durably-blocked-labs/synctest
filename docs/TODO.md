# TODO

## Runtime (`go/src/runtime`)

- **Deterministic `select` recording/replay.** Bubbled goroutines entering a
  `select` with multiple ready cases currently pick a case via the runtime's
  per-M `cheaprand()` — same PRNG as the scheduler, so the choice is
  controllable in principle, but the decision is not yet surfaced to the
  orchestrator as a `DecisionPoint`. Until this lands, any bug whose trigger
  requires a specific ready-case ordering in a contended `select` cannot be
  targeted by CHESS/PCT/DPOR. Prior in-flight work lived on the `go` fork
  (`2a009c6327 runtime: deterministic select, idleNote sleep, SetSelectOffset`)
  but was dropped from `synctest-explorer` during cleanup — the change touched
  `runtime/select.go`, `runtime/synctest.go`, `runtime/proc.go`,
  `internal/synctest/synctest.go`, `testing/synctest/synctest.go`. Re-land as a
  proper decision point with alternatives exposed through the hook state, same
  shape as goroutine-scheduling decisions.
