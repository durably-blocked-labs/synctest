# Week 1: Project Setup & Initial Investigation

**Date**: Jan 27 - Feb 2, 2026
**Phase**: 1 (Validation)

## Goals for This Week

- [ ] Set up repository structure
- [ ] Build Go runtime from source
- [ ] Understand `cheaprand()` implementation
- [ ] Design first validation experiment

## Progress

### Repository Setup

- Created documentation structure in `docs/`
- Added reference papers to `papers/`
- Set up Go runtime fork as submodule
- Created changelog system

### Understanding the Codebase

Key files identified for Phase 1:

1. **`go/src/runtime/rand.go`** - Contains `cheaprand()` and `mrandinit()`
2. **`go/src/runtime/proc.go`** - Scheduler with randomization points
3. **`go/src/runtime/synctest.go`** - Bubble implementation

### Key Questions to Answer

1. Can we seed `cheaprand()` deterministically for goroutines in a bubble?
2. What other entropy sources exist beyond `cheaprand()`?
3. How do we trace/log scheduler decisions?

## Next Steps

1. Build the Go toolchain locally
2. Add instrumentation to log `cheaprand()` calls
3. Write simple test program (2-3 goroutines)
4. Run 1000 times with same seed, check for identical traces

## Notes

- `cheaprand()` state is per-M (OS thread), stored in `mp.cheaprand`
- Initialized in `mrandinit()` from cryptographic source
- `randomizeScheduler` constant is true when race detector enabled

## Open Questions

- Does GOMAXPROCS=1 help isolate scheduler behavior?
- How to handle M migration (goroutine moving between OS threads)?
- What happens when GC runs during bubble execution?
