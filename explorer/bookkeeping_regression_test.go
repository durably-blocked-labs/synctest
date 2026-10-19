package explorer_test

import (
	"reflect"
	"runtime"
	"strconv"
	"testing"

	"github.com/shubhaankar/synctest/explorer"
	"github.com/shubhaankar/synctest/orchestrator"
)

// Keep the search deterministic and independent of runtime decision counts.
// The driver still executes and successfully terminates a real tiny bubble.
type bookkeepingSearch struct {
	chess   orchestrator.CHESS
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
	a.chosen = a.chess.Decide(orchestrator.DecisionPoint{
		Kind: orchestrator.Local,
		Node: "local",
		Alts: []orchestrator.Alt{{ID: "B5"}, {ID: "B6"}, {ID: "B7"}},
	})
	return true
}
func (a *bookkeepingSearch) Decide(orchestrator.DecisionPoint) int { return 0 }
func (a *bookkeepingSearch) AfterRun(rr explorer.RunResult) {
	a.after++
	a.visited = append(a.visited, a.chosen)
	rr.Trace = orchestrator.Trace{{Kind: orchestrator.Local, Node: "local", Index: int32(a.chosen), Alternatives: 3}}
	a.chess.AfterRun(rr)
}

func TestExploreCapPreservesPendingSearch(t *testing.T) {
	saved := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(saved)
	for _, cap := range []int{1, 3} {
		t.Run(strconv.Itoa(cap), func(t *testing.T) {
			a := &bookkeepingSearch{chess: orchestrator.CHESS{Bound: 1}}
			for range 3 / cap {
				result := explorer.Explore(t, func(*testing.T) {}, a, explorer.MaxRuns(cap))
				if result.Runs != cap || result.Failed != 0 || a.before != a.after {
					t.Errorf("cap=%d result=%+v BeforeRun=%d AfterRun=%d", cap, result, a.before, a.after)
				}
			}
			if !reflect.DeepEqual(a.visited, []int{0, 2, 1}) {
				t.Errorf("capped search visited %v, want [0 2 1]", a.visited)
			}
			if result := explorer.Explore(t, func(*testing.T) {}, a, explorer.MaxRuns(1)); result.Runs != 0 {
				t.Errorf("exhausted search ran %d additional trials", result.Runs)
			}
		})
	}
}

func TestExploreRestoresGOMAXPROCSOnReturn(t *testing.T) {
	saved := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(saved)
	for range 2 {
		a := &bookkeepingSearch{chess: orchestrator.CHESS{Bound: 1}}
		result := explorer.Explore(t, func(*testing.T) {}, a, explorer.MaxRuns(1))
		if result.Runs != 1 || result.Failed != 0 {
			t.Fatalf("tiny trial result=%+v", result)
		}
		// This assertion runs before the enclosing test's cleanup callbacks.
		if got := runtime.GOMAXPROCS(0); got != 4 {
			t.Fatalf("GOMAXPROCS after Explore returned=%d, want 4", got)
		}
	}
}
