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
	maxRuns       int
	observers     []RunObserver
	selectChoices bool
	sink          orchestrator.TrialSink
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

// WithTrialSink records start and terminal for every attempted execution.
func WithTrialSink(s orchestrator.TrialSink) Option { return func(c *config) { c.sink = s } }

// WithSelectChoices opts into explicit enabled-select-case decisions.
func WithSelectChoices() Option { return func(c *config) { c.selectChoices = true } }

// RunWithHook executes f in a new bubble with the given decision hook.
// The hook is called at every frontier scheduling decision point
// (i.e., where the runq has goroutines and no pre-loaded prefix applies).
//
// Returns a bounded scheduling trace and whether the test passed.
// Use RunWithHookResult to retain the exact failure and unsafe-worker status.
// If the bubble panics (e.g., deadlock), returns (nil, false).
func RunWithHook(t *testing.T, f func(*testing.T), hook DecisionHook) ([]Decision, bool) {
	r := RunWithHookResult(t, f, hook)
	return r.Trace, r.OK
}

// RunWithHookResult retains runtime causes and the unsafe-worker boundary.
func RunWithHookResult(t *testing.T, f func(*testing.T), hook DecisionHook) synctest.Result {
	return runWithHooks(t, f, hook, nil)
}

func runWithHooks(t *testing.T, f func(*testing.T), hook DecisionHook, selectHook func(synctest.SelectState) int32) synctest.Result {
	if !orchestrator.WorkerReusable() {
		return synctest.Result{Cause: fmt.Errorf("unsafe worker cannot start another bubble; terminate process"), WorkerUnsafe: true}
	}
	var workloadCause any
	result := synctest.ExploreResult(t, func(t *testing.T) {
		defer func() {
			if cause := recover(); cause != nil {
				workloadCause = cause
			}
		}()
		synctest.SetDecisionHook(func(state synctest.BubbleState) int32 {
			if state.Idle {
				return -1
			}
			return hook(state)
		})
		if selectHook != nil {
			synctest.SetSelectHook(selectHook)
		}
		f(t)
	}, nil)
	if workloadCause != nil {
		result.Cause = workloadCause
		result.OK = false
		result.WorkerUnsafe = true
	}
	if result.WorkerUnsafe {
		orchestrator.RetireWorker()
	}
	return result
}

// Explore runs f repeatedly under the given algorithm, collecting metrics.
// This is the full-featured entry point for single-bubble exploration.
//
// The algorithm controls scheduling at every decision point where multiple
// goroutines are runnable. CHESS explores systematically via DFS, PCT uses
// randomized priorities, Random picks uniformly.
//
// Assertion failures count as bugs; infrastructure/runtime failures stop the worker.
//
// Returns the exploration result (runs, failures, timing).
func Explore(t *testing.T, f func(*testing.T), algo Algorithm, opts ...Option) ExplorationResult {
	t.Helper()
	saved := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(saved)

	var cfg config
	for _, o := range opts {
		o(&cfg)
	}

	start := time.Now()
	result := ExplorationResult{FirstBug: -1}

	for cfg.maxRuns <= 0 || result.Runs < cfg.maxRuns {
		if !algo.BeforeRun() {
			if bounded, ok := algo.(interface{ SearchError() error }); ok {
				if err := bounded.SearchError(); err != nil {
					result.Incomplete++
					result.Cause = err
				}
			}
			break
		}

		if cfg.sink != nil {
			if err := cfg.sink.StartTrial(result.Runs + 1); err != nil {
				result.Incomplete++
				result.Cause = err
				break
			}
		}
		var trace orchestrator.Trace
		var runtimeResult synctest.Result
		var hookCause any
		runStart := time.Now()
		passed := t.Run(fmt.Sprintf("run_%d", result.Runs), func(subT *testing.T) {
			var selectHook func(synctest.SelectState) int32
			if cfg.selectChoices {
				selectHook = func(state synctest.SelectState) int32 {
					defer func() {
						if cause := recover(); cause != nil {
							hookCause = cause
							panic(cause)
						}
					}()
					dp := orchestrator.SelectDecisionPoint(state, len(trace), "local")
					idx := algo.Decide(dp)
					orchestrator.CheckDecision(dp, idx)
					trace = append(trace, orchestrator.RecordDecision(dp, idx))
					return dp.Alts[idx].CaseIndex
				}
			}
			runtimeResult = runWithHooks(subT, f, func(state BubbleState) int32 {
				dp := orchestrator.DecisionPoint{Kind: orchestrator.Local, Step: len(trace), Node: "local", Alts: buildAlts(state)}
				defer func() {
					if cause := recover(); cause != nil {
						hookCause = cause
						panic(cause)
					}
				}()
				idx := algo.Decide(dp)
				orchestrator.CheckDecision(dp, idx)
				ids := make([]string, len(dp.Alts))
				for i, a := range dp.Alts {
					ids[i] = a.ID
				}
				trace = append(trace, orchestrator.NewStep{Kind: orchestrator.Local, Node: "local", Index: int32(idx), Alternatives: int32(len(dp.Alts)), ChosenID: dp.Alts[idx].ID, ChosenBGID: dp.Alts[idx].BGID, RunqBGIDs: bgidsFromAlts(dp.Alts), AltIDs: ids})
				return int32(idx)
			}, selectHook)
			if !runtimeResult.OK {
				if _, diverged := orchestrator.IsReplayDivergence(hookCause); !diverged {
					subT.Errorf("interleaving failed: %v", runtimeResult.Cause)
				}
			}
		})
		elapsed := time.Since(runStart)
		rr := RunResult{Trace: trace, Passed: passed && runtimeResult.OK, DivergenceStep: -1, Elapsed: elapsed, Cause: runtimeResult.Cause, WorkerUnsafe: runtimeResult.WorkerUnsafe, Outcome: orchestrator.OutcomePassed, SelectControlled: cfg.selectChoices}
		if hookCause != nil {
			rr.Cause = hookCause
		}
		if rr.Cause != nil {
			rr.Passed = false
			if step, ok := orchestrator.IsReplayDivergence(rr.Cause); ok {
				rr.Diverged = true
				rr.DivergenceStep = step
				rr.Outcome = orchestrator.OutcomeDiverged
			} else if _, ok := rr.Cause.(interface {
				error
				Limit() string
			}); ok {
				rr.Outcome = orchestrator.OutcomeResourceLimit
			} else {
				rr.Outcome = orchestrator.OutcomeInfrastructure
			}
		} else if !rr.Passed {
			rr.UserFailed = true
			rr.Outcome = orchestrator.OutcomeAssertion
		}
		if validator, ok := algo.(interface{ ValidatePrefix() error }); ok && rr.Cause == nil {
			if err := validator.ValidatePrefix(); err != nil {
				rr.Passed = false
				rr.Cause = err
				rr.Diverged = true
				rr.Outcome = orchestrator.OutcomeDiverged
				rr.DivergenceStep, _ = orchestrator.IsReplayDivergence(err)
			}
		}

		if cfg.sink != nil {
			if err := cfg.sink.EndTrial(result.Runs+1, rr); err != nil {
				rr.PersistenceError = err
				rr.Passed = false
				if rr.Cause == nil {
					rr.UserFailed = false
					rr.Diverged = false
					rr.Cause = err
					rr.Outcome = orchestrator.OutcomeInfrastructure
				}
			}
		}
		result.Runs++
		algo.AfterRun(rr)

		if rr.Diverged {
			result.Diverged++
		}
		if rr.WorkerUnsafe || rr.PersistenceError != nil || (!rr.Passed && !rr.Diverged && !rr.UserFailed) {
			result.Incomplete++
			result.Cause = rr.Cause
			if rr.PersistenceError != nil {
				result.Cause = rr.PersistenceError
			}
		}

		// Notify observers on non-diverged runs.
		if len(cfg.observers) > 0 && !rr.Diverged {
			nonFIFO := 0
			for _, s := range trace {
				if s.Index != 0 {
					nonFIFO++
				}
			}
			for _, obs := range cfg.observers {
				obs(result.Runs, nonFIFO, elapsed, rr, rr.Passed)
			}
		}

		if rr.UserFailed {
			result.Failed++
			if result.FirstBug == -1 {
				result.FirstBug = result.Runs
			}
		}
		if bounded, ok := algo.(interface{ SearchError() error }); ok {
			if err := bounded.SearchError(); err != nil {
				result.Incomplete++
				result.Cause = err
				break
			}
		}
		if rr.UserFailed || rr.WorkerUnsafe || result.Incomplete > 0 {
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
	if !r.Successful() {
		if r.Incomplete > 0 {
			t.Errorf("incomplete exploration: %v", r.Cause)
		}
		return // Explore already reported the failure via t.Run subtests
	}
	t.Logf("explored %d interleavings (%d rejected prefixes)", r.Runs, r.Diverged)
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
