package orchestrator

import (
	"os"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// Opt-in executions must also use an external process-group watchdog: the OLD
// experimental runtime has blocked here even with Go's test timeout enabled.
func bookkeepingGlobalRuntimeGate(t *testing.T) {
	t.Helper()
	if os.Getenv("BUBBLES_GLOBAL_RUNTIME_REGRESSION") != "1" {
		t.Skip("global runtime integration blocked on OLD experimental runtime; opt in with BUBBLES_GLOBAL_RUNTIME_REGRESSION=1 under an external process-group watchdog")
	}
}

// A timer-only node parks without an external bridge. Deliveries target no node,
// so these tests exercise global recovery without the delivery retry protocol.
func setupBookkeepingDeliveries(o *Orchestrator) {
	tr := newStubTransport("timer")
	tr.outbox = make(chan *PendingOp, 2)
	for range 2 {
		tr.outbox <- &PendingOp{From: "timer", To: "sink", Type: "test", Execute: func() {}}
	}
	o.AddNode(tr, func(*testing.T) { time.Sleep(time.Hour) })
}

func bookkeepingProcessors(t *testing.T) {
	t.Helper()
	saved := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(saved) })
}

func TestRunForExploreRecoversGlobalDivergence(t *testing.T) {
	bookkeepingGlobalRuntimeGate(t)
	bookkeepingProcessors(t)
	o := New()
	setupBookkeepingDeliveries(o)
	o.initBubbles()
	globalCalls, divergenceStep := 0, -1
	rr := o.runForExplore(t, func(dp DecisionPoint) int {
		if dp.Kind == Global {
			globalCalls++
			if globalCalls == 2 {
				divergenceStep = dp.Step
				panic(replayDivergenceError{Step: dp.Step, Index: -1, QueueSize: dp.N()}) // Typed prefix mismatch, not an invalid algorithm result.
			}
		}
		return 0
	})
	if globalCalls != 2 || divergenceStep <= 0 {
		t.Fatalf("did not reach a nonzero global divergence: calls=%d step=%d", globalCalls, divergenceStep)
	}
	if !rr.Diverged || rr.DivergenceStep != divergenceStep || rr.Passed || rr.UserFailed {
		t.Fatalf("recovered result=%+v, want divergence at step %d", rr, divergenceStep)
	}
	for _, ctrl := range o.nodes {
		select {
		case <-ctrl.done:
		default:
			t.Fatal("timer node did not terminate during recovery cleanup")
		}
	}
}

func TestRunForExploreNoDivergenceSentinel(t *testing.T) {
	bookkeepingProcessors(t)
	rr := New().runForExplore(t, func(DecisionPoint) int { return 0 })
	if !rr.Passed || rr.Diverged || rr.DivergenceStep != -1 {
		t.Fatalf("ordinary result=%+v, want passed with divergence step -1", rr)
	}
}

type bookkeepingDivergenceAlgorithm struct {
	globalCalls int
	step        int
	results     []RunResult
}

func (a *bookkeepingDivergenceAlgorithm) BeforeRun() bool { return len(a.results) == 0 }
func (a *bookkeepingDivergenceAlgorithm) Decide(dp DecisionPoint) int {
	if dp.Kind == Global {
		a.globalCalls++
		if a.globalCalls == 2 {
			a.step = dp.Step
			panic(replayDivergenceError{Step: dp.Step, Index: -1, QueueSize: dp.N()})
		}
	}
	return 0
}
func (a *bookkeepingDivergenceAlgorithm) AfterRun(rr RunResult) {
	a.results = append(a.results, rr)
}

func TestExploreWithReportsGlobalDivergence(t *testing.T) {
	bookkeepingGlobalRuntimeGate(t)
	bookkeepingProcessors(t)
	a := &bookkeepingDivergenceAlgorithm{step: -1}
	observers := 0
	result := New().ExploreWith(t, setupBookkeepingDeliveries, a, GlobalMaxRuns(1),
		WithObserver(func(int, int, time.Duration, RunResult, bool) { observers++ }))
	if result.Runs != 1 || result.Failed != 0 || result.FirstBug != -1 {
		t.Fatalf("exploration result=%+v", result)
	}
	if len(a.results) != 1 || a.step <= 0 {
		t.Fatalf("AfterRun calls=%d, divergence step=%d", len(a.results), a.step)
	}
	if rr := a.results[0]; !rr.Diverged || rr.DivergenceStep != a.step || rr.UserFailed {
		t.Errorf("AfterRun result=%+v, want divergence at %d", rr, a.step)
	}
	if observers != 0 {
		t.Errorf("observers called %d times for a divergent run", observers)
	}
}

// Supply a fixed three-way search independently of runtime scheduling. The real
// driver must pair successful BeforeRun calls with AfterRun and preserve CHESS's
// pending prefixes between capped invocations.
type bookkeepingSearch struct {
	chess   CHESS
	before  int
	after   int
	chosen  int
	visited []int
}

func (a *bookkeepingSearch) BeforeRun() bool {
	if !a.chess.BeforeRun() {
		return false
	}
	a.before++
	a.chosen = a.chess.Decide(DecisionPoint{Kind: Global, Alts: []Alt{{ID: "a"}, {ID: "b"}, {ID: "c"}}})
	return true
}
func (a *bookkeepingSearch) Decide(DecisionPoint) int { return 0 }
func (a *bookkeepingSearch) AfterRun(rr RunResult) {
	a.after++
	a.visited = append(a.visited, a.chosen)
	rr.Trace = Trace{{Kind: Global, Index: int32(a.chosen), Alternatives: 3}}
	a.chess.AfterRun(rr)
}

func TestExploreWithCapPreservesPendingSearch(t *testing.T) {
	bookkeepingProcessors(t)
	for _, cap := range []int{1, 3} {
		t.Run(strconv.Itoa(cap), func(t *testing.T) {
			a := &bookkeepingSearch{chess: CHESS{Bound: 1}}
			for range 3 / cap {
				result := New().ExploreWith(t, func(*Orchestrator) {}, a, GlobalMaxRuns(cap))
				if result.Runs != cap || a.before != a.after {
					t.Errorf("cap=%d runs=%d BeforeRun=%d AfterRun=%d", cap, result.Runs, a.before, a.after)
				}
			}
			if !reflect.DeepEqual(a.visited, []int{0, 2, 1}) {
				t.Errorf("capped search visited %v, want [0 2 1]", a.visited)
			}
			if result := New().ExploreWith(t, func(*Orchestrator) {}, a, GlobalMaxRuns(1)); result.Runs != 0 {
				t.Errorf("exhausted search ran %d additional trials", result.Runs)
			}
		})
	}
}
