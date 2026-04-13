# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

Research project exploring deterministic scheduling and systematic interleaving exploration for Go concurrency testing. The core idea: Go's `testing/synctest` package provides time-deterministic "bubbles" (isolated execution environments), but scheduling remains non-deterministic due to `cheaprand()` in the Go runtime. This project modifies the Go runtime to make scheduling decisions observable, recordable, and replayable, then explores interleavings systematically using DFS with context bounding.

## Build & Test Commands

All commands use a **custom-built Go toolchain** at `./go/bin/go` (a forked Go 1.27 runtime with synctest modifications). The `go/` directory is a git submodule pointing to `durably-blocked-labs/go` on the `synctest-explorer` branch.

```bash
make build-go                    # Build custom Go runtime from go/src (required first time)
make test                        # Run all experiment tests (preemption enabled)
make test-det                    # Run all experiment tests (preemption disabled, deterministic)
make test-one TEST=TestName      # Run a specific test by name (preemption disabled)
make determinism                 # Run TestDeterminism1000 (validates deterministic scheduling)
```

Deterministic execution requires `GODEBUG=asyncpreemptoff=1` (set automatically by `test-det` and `test-one` targets). Tests set `GOMAXPROCS=1` in code.

## Architecture

### Three Go Modules

- **`explorer/`** — The main library. `explorer.Test(t, f)` is a drop-in replacement for `synctest.Test(t, f)` that explores scheduling interleavings via DFS with context bounding. `RunWithHook` provides low-level control for custom scheduling strategies.
- **`experiments/`** — Validation test suite proving that cheaprand() control enables deterministic scheduling. Tests decision recording/replay, frontier signaling, and race condition scenarios.
- **`examples/`** — Distributed simulation patterns (multi-bubble orchestration with `synctest.CallExternal()`).

### Forked Go Runtime (`go/` submodule)

Key modifications to the standard Go runtime:

- **`runtime/synctest.go`** — `synctestBubble` struct with decision recording (`bubbleDecision` array), decision fence for record/follow mode, and frontier hook signaling between g0 and root goroutine.
- **`testing/synctest/synctest.go`** — Public API: `Test()`, `Explore(prefix)`, `SetDecisionHook()`, `Wait()`, `CallExternal()`.
- **`runtime/proc.go`** — Scheduler integration: bubble-aware `runqput`, `runqputslow`, `stealWork` with cheaprand()-based randomization made controllable.
- **`runtime/rand.go`** — `cheaprand()` per-M PRNG that drives scheduler randomness.
- **`runtime/chan.go`, `runtime/select.go`** — Channel ops and select marked as durable blocking points for bubble tracking.

### Key Concepts

**Bubbles**: Isolated execution environments with their own fake clock, goroutine set, and durable-blocking tracker. Time advances only when all goroutines are durably blocked.

**Decision Model**: At each scheduler yield point where multiple goroutines are runnable, a `bubbleDecision` records the chosen goroutine index, the full runnable set, and goroutine metadata. Decisions can be pre-loaded as a prefix (follow mode) or made live via a hook (frontier mode).

**Exploration**: DFS over scheduling traces. At each decision point with alternatives, the explorer pushes new work items with modified prefixes onto a stack. Context bounding (default 2) limits non-FIFO decisions per trace — most concurrency bugs manifest with 1–2 non-default choices.
