// Package explorer explores scheduling interleavings of concurrent Go tests.
//
// The explorer runs a synctest workload through multiple bubble executions,
// each with a different scheduling interleaving. It uses depth-first search
// with context bounding to limit the exploration space while still catching
// most concurrency bugs.
//
// Usage is identical to synctest.Test — just replace the call:
//
//	// Before: runs once with default FIFO scheduling.
//	synctest.Test(t, func(t *testing.T) { ... })
//
//	// After: explores interleavings, stops at first failure.
//	explorer.Test(t, func(t *testing.T) { ... })
//
// For custom scheduling strategies (e.g., distributed simulation),
// use RunWithHook to control each scheduling decision via a callback.
package explorer

import (
	"fmt"
	"testing"
	"testing/synctest"
)

// Decision is a scheduling decision recorded within a bubble.
type Decision = synctest.Decision

// BubbleState describes the state of a bubble at a scheduling decision point.
type BubbleState = synctest.BubbleState

// DecisionHook is called at each frontier scheduling decision point.
// It receives the bubble state and returns the index of the goroutine
// to schedule next (0 = first in runq = FIFO default).
type DecisionHook func(state BubbleState) int32

// Option configures the explorer.
type Option func(*config)

type config struct {
	bound   int // context bound (max non-FIFO decisions per trace)
	maxRuns int // hard cap on bubble executions (0 = unlimited)
}

// Bound sets the context bound — the maximum number of scheduling
// decisions that may differ from the default FIFO order in any single
// interleaving. Most concurrency bugs manifest with 1–2 non-default
// choices. Default is 2.
func Bound(k int) Option {
	return func(c *config) { c.bound = k }
}

// MaxRuns sets a hard cap on the number of bubble executions.
// 0 means unlimited (default).
func MaxRuns(n int) Option {
	return func(c *config) { c.maxRuns = n }
}

// RunWithHook executes f in a new bubble with the given decision hook.
// The hook is called at every frontier scheduling decision point
// (i.e., where the runq has goroutines and no pre-loaded prefix applies).
//
// The hook is set inside the bubble before f runs. Infrastructure steps
// (bubble.main, tRunner) happen before the hook is active, but those
// always have runqSize=1 (no choice to make).
//
// Returns the full scheduling trace and whether the test passed.
// If the bubble panics (e.g., deadlock), returns (nil, false).
func RunWithHook(t *testing.T, f func(*testing.T), hook DecisionHook) ([]Decision, bool) {
	wrapped := func(t *testing.T) {
		synctest.SetDecisionHook(func(state synctest.BubbleState) int32 {
			if state.Idle {
				return -1
			}
			return hook(state)
		})
		f(t)
	}
	return synctest.Explore(t, wrapped, nil)
}

// Test explores scheduling interleavings of f using depth-first search
// with context bounding. f is a normal synctest workload — it receives
// a *testing.T and uses standard assertions (t.Error, t.Errorf, etc.).
//
// If any interleaving causes f to fail, Test reports the failing trace
// and stops. If all interleavings within the bound pass, Test succeeds.
//
// Each interleaving runs through the onDecision hook, validating the
// full frontier signaling path (g0 → root → g0 round-trip).
func Test(t *testing.T, f func(*testing.T), opts ...Option) {
	t.Helper()

	cfg := config{bound: 2}
	for _, o := range opts {
		o(&cfg)
	}

	type work struct {
		prefix  []Decision
		nonFIFO int // number of non-zero indices in prefix
	}

	stack := []work{{}} // start with empty prefix (baseline FIFO)
	runCount := 0

	for len(stack) > 0 {
		if cfg.maxRuns > 0 && runCount >= cfg.maxRuns {
			break
		}

		w := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		var trace []Decision
		var ok bool

		name := fmt.Sprintf("run_%d", runCount)
		runCount++

		// Build a hook that follows the plan (prefix) for known steps,
		// then returns 0 (FIFO) at the frontier.
		plan := w.prefix

		passed := t.Run(name, func(subT *testing.T) {
			subT.Helper()
			trace, ok = RunWithHook(subT, f, func(state BubbleState) int32 {
				step := int(state.Step)
				if step < len(plan) {
					return plan[step].Index
				}
				return 0 // FIFO at frontier
			})
			if !ok {
				if len(trace) > 0 {
					subT.Logf("failing interleaving (prefix len=%d):", len(w.prefix))
					for i, d := range trace {
						subT.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
							i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
					}
				}
				subT.Errorf("interleaving failed (prefix len=%d)", len(w.prefix))
			}
		})

		if !passed {
			return
		}

		// Find branching points in the frontier (past the prefix).
		// Only explore alternatives at positions >= len(prefix), since
		// earlier positions are already covered by ancestor explorations.
		for i := len(w.prefix); i < len(trace); i++ {
			d := trace[i]
			if d.RunqSize <= 1 {
				continue
			}
			for alt := int32(0); alt < d.RunqSize; alt++ {
				if alt == d.Index {
					continue
				}
				newNonFIFO := w.nonFIFO
				if alt != 0 {
					newNonFIFO++
				}
				if newNonFIFO > cfg.bound {
					continue
				}
				newPrefix := make([]Decision, i+1)
				copy(newPrefix, trace[:i+1])
				newPrefix[i].Index = alt
				stack = append(stack, work{prefix: newPrefix, nonFIFO: newNonFIFO})
			}
		}
	}

	t.Logf("explored %d interleavings, all passed (bound=%d)", runCount, cfg.bound)
}
