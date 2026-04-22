// Package explorer explores scheduling interleavings of concurrent Go tests.
//
// The explorer runs a synctest workload through multiple bubble executions,
// each with a different scheduling interleaving. It uses pluggable algorithms
// (CHESS, PCT, Random) from the orchestrator package to control scheduling
// decisions, while keeping a simple single-bubble execution model.
//
// Usage:
//
//	// CHESS (default): systematic DFS with context bound 2.
//	explorer.Test(t, func(t *testing.T) { ... })
//
//	// PCT: randomized priority-based scheduling.
//	explorer.Test(t, func(t *testing.T) { ... }, &explorer.PCT{Depth: 3})
//
//	// Random: uniform random scheduling.
//	explorer.Test(t, func(t *testing.T) { ... }, &explorer.Random{Seed: 1})
//
//	// Full-featured with options:
//	explorer.Explore(t, f, &explorer.PCT{Depth: 3}, explorer.MaxRuns(500))
//
// For custom scheduling strategies, use RunWithHook to control each
// scheduling decision via a callback.
package explorer

import (
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shubhaankar/synctest/orchestrator"
)

// Re-export algorithm types so single-bubble users only import explorer.
type (
	Algorithm         = orchestrator.Algorithm
	CHESS             = orchestrator.CHESS
	PCT               = orchestrator.PCT
	Random            = orchestrator.Random
	RunObserver       = orchestrator.RunObserver
	RunResult         = orchestrator.RunResult
	ExplorationResult = orchestrator.ExplorationResult
)

// Decision is a scheduling decision recorded within a bubble.
type Decision = synctest.Decision

// BubbleState describes the state of a bubble at a scheduling decision point.
type BubbleState = synctest.BubbleState

// DecisionHook is called at each frontier scheduling decision point.
// It receives the bubble state and returns the index of the goroutine
// to schedule next (0 = first in runq = FIFO default).
type DecisionHook func(state BubbleState) int32

// Option configures exploration.
type Option func(*config)

type config struct {
	maxRuns   int
	observers []RunObserver
}

// MaxRuns sets a hard cap on the number of bubble executions.
// 0 means unlimited (default).
func MaxRuns(n int) Option {
	return func(c *config) { c.maxRuns = n }
}

// WithObserver registers a callback invoked after each non-diverged run.
func WithObserver(fn RunObserver) Option {
	return func(c *config) { c.observers = append(c.observers, fn) }
}

// RunWithHook executes f in a new bubble with the given decision hook.
// The hook is called at every frontier scheduling decision point
// (i.e., where the runq has goroutines and no pre-loaded prefix applies).
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

// Explore runs f repeatedly under the given algorithm, collecting metrics.
// This is the full-featured entry point for single-bubble exploration.
//
// The algorithm controls scheduling at every decision point where multiple
// goroutines are runnable. CHESS explores systematically via DFS, PCT uses
// randomized priorities, Random picks uniformly.
//
// All failures — including deadlocks — are counted as bugs.
//
// Returns the exploration result (runs, failures, timing).
func Explore(t *testing.T, f func(*testing.T), algo Algorithm, opts ...Option) ExplorationResult {
	t.Helper()
	saved := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(saved) })

	var cfg config
	for _, o := range opts {
		o(&cfg)
	}

	start := time.Now()
	result := ExplorationResult{FirstBug: -1}

	for algo.BeforeRun() {
		if cfg.maxRuns > 0 && result.Runs >= cfg.maxRuns {
			break
		}

		var trace orchestrator.Trace
		var diverged bool
		runStart := time.Now()

		passed := t.Run(fmt.Sprintf("run_%d", result.Runs), func(subT *testing.T) {
			subT.Helper()
			_, ok := RunWithHook(subT, f, func(state BubbleState) int32 {
				alts := buildAlts(state)
				dp := orchestrator.DecisionPoint{
					Kind: orchestrator.Local,
					Step: int(state.Step),
					Node: "local",
					Alts: alts,
				}
				// Catch divergence panics from algo.Decide (e.g., CHESS prefix mismatch).
				// We set diverged via the closure, then re-panic to abort the bubble.
				// Note: synctest.Explore will swallow the re-panic (it recovers all
				// panics and returns ok=false). The diverged flag — not the panic — is
				// the actual signal we check after RunWithHook returns.
				defer func() {
					if r := recover(); r != nil {
						diverged = true
						panic(r)
					}
				}()
				idx := algo.Decide(dp)
				trace = append(trace, orchestrator.NewStep{
					Kind:         orchestrator.Local,
					Node:         "local",
					Index:        int32(idx),
					Alternatives: int32(len(alts)),
					ChosenID:     alts[idx].ID,
					ChosenBGID:   alts[idx].BGID,
					RunqBGIDs:    bgidsFromAlts(alts),
				})
				return int32(idx)
			})
			if !ok && !diverged {
				subT.Errorf("interleaving failed")
			}
		})

		elapsed := time.Since(runStart)
		rr := RunResult{
			Trace:          trace,
			Passed:         passed,
			Diverged:       diverged,
			DivergenceStep: -1,
			Elapsed:        elapsed,
		}
		if !passed && !diverged {
			rr.UserFailed = true
		}

		result.Runs++
		algo.AfterRun(rr)

		if diverged {
			continue
		}

		// Notify observers on non-diverged runs.
		if len(cfg.observers) > 0 {
			nonFIFO := 0
			for _, s := range trace {
				if s.Index != 0 {
					nonFIFO++
				}
			}
			for _, obs := range cfg.observers {
				obs(result.Runs, nonFIFO, elapsed, rr, passed)
			}
		}

		if !passed {
			result.Failed++
			if result.FirstBug == -1 {
				result.FirstBug = result.Runs
			}
			break
		}
	}

	result.Elapsed = time.Since(start)
	return result
}

// Test explores scheduling interleavings of f, stopping at the first failure.
// With no algorithm argument, uses CHESS with context bound 2.
//
// Accepts an optional Algorithm and/or Option arguments:
//
//	explorer.Test(t, f)                                    // CHESS default
//	explorer.Test(t, f, &explorer.CHESS{Bound: 3})         // CHESS with bound 3
//	explorer.Test(t, f, &explorer.PCT{Depth: 3, Seed: 1})  // PCT
//	explorer.Test(t, f, &explorer.Random{Seed: 1})          // Random
//	explorer.Test(t, f, explorer.MaxRuns(100))              // CHESS default + cap
func Test(t *testing.T, f func(*testing.T), args ...any) {
	t.Helper()

	var algo Algorithm
	var opts []Option

	for _, a := range args {
		switch v := a.(type) {
		case Algorithm:
			algo = v
		case Option:
			opts = append(opts, v)
		default:
			panic(fmt.Sprintf("explorer.Test: unsupported argument type %T", a))
		}
	}
	if algo == nil {
		algo = &CHESS{Bound: 2}
	}

	r := Explore(t, f, algo, opts...)
	if r.Failed > 0 {
		return // Explore already reported the failure via t.Run subtests
	}
	t.Logf("explored %d interleavings, all passed", r.Runs)
}

// buildAlts converts BubbleState goroutine info to orchestrator Alt entries.
func buildAlts(state BubbleState) []orchestrator.Alt {
	alts := make([]orchestrator.Alt, state.RunnableN)
	for i := int32(0); i < state.RunnableN; i++ {
		alts[i] = orchestrator.Alt{
			ID:   fmt.Sprintf("B%d", state.RunnableBgid[i]),
			BGID: state.RunnableBgid[i],
		}
	}
	return alts
}

// bgidsFromAlts extracts the BGID slice for trace recording.
func bgidsFromAlts(alts []orchestrator.Alt) []uint32 {
	bgids := make([]uint32, len(alts))
	for i, a := range alts {
		bgids[i] = a.BGID
	}
	return bgids
}
