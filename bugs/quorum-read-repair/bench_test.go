package quorumreadrepair

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/shubhaankar/synctest/orchestrator"
)

const benchMaxRuns = 500

func benchSetup(o *orchestrator.Orchestrator) {
	resetObservedOutcome()
	addFocusedReadRepairRace(o)
}

func benchDir() string {
	d := os.Getenv("BENCH_DIR")
	if d != "" {
		_ = os.MkdirAll(d, 0755)
	}
	return d
}

func observers(t *testing.T, policy string) []orchestrator.RunObserver {
	dir := benchDir()
	if dir == "" {
		return nil
	}
	var observers []orchestrator.RunObserver
	if f, err := os.Create(dir + "/" + policy + ".jsonl"); err == nil {
		t.Cleanup(func() { _ = f.Close() })
		observers = append(observers, orchestrator.NewJSONLObserver(f, policy))
	}
	if f, err := os.Create(dir + "/" + policy + "-trace.jsonl"); err == nil {
		t.Cleanup(func() { _ = f.Close() })
		observers = append(observers, orchestrator.NewDetailedObserver(f, policy))
	}
	return observers
}

func logBenchStart(t *testing.T, label string) {
	t.Helper()
	fmt.Printf("START %s\n", label)
}

func logBenchDone(t *testing.T, label string, runs int, firstBug int, elapsed time.Duration) {
	t.Helper()
	fmt.Printf("DONE %s: runs=%d first_bug=%d elapsed=%v\n", label, runs, firstBug, elapsed)
}

func requireFirstBug(t *testing.T, label string, firstBug int) {
	t.Helper()
	if firstBug == -1 {
		t.Fatalf("%s did not find bug within %d runs", label, benchMaxRuns)
	}
}

func runBenchmark(t *testing.T, label, policy string, algo orchestrator.Algorithm) (orchestrator.ExplorationResult, int) {
	t.Helper()
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()
	logBenchStart(t, label)

	firstBug := -1
	benchObservers := observers(t, policy)
	observe := func(runNum int, nonFIFO int, elapsed time.Duration, rr orchestrator.RunResult, passed bool) {
		bugFound := observedBugFound()
		passed = !bugFound
		rr.Passed = passed
		rr.UserFailed = bugFound
		if bugFound {
			if firstBug == -1 {
				firstBug = runNum
			}
		}
		for _, observer := range benchObservers {
			observer(runNum, nonFIFO, elapsed, rr, passed)
		}
	}

	opts := []orchestrator.ExploreOption{
		orchestrator.GlobalMaxRuns(benchMaxRuns),
		orchestrator.WithObserver(observe),
	}
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	logBenchDone(t, label, r.Runs, firstBug, r.Elapsed)
	return r, firstBug
}

func TestBench_CHESS_GlobalOnly(t *testing.T) {
	algo := &orchestrator.CHESS{Bound: 4, GlobalOnly: true}
	runBenchmark(t, "CHESS(G-only,k=4)", "chess-global", algo)
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-global-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			_ = f.Close()
		}
	}
}

func TestBench_CHESS_GL(t *testing.T) {
	algo := &orchestrator.CHESS{Bound: 4}
	_, firstBug := runBenchmark(t, "CHESS(G+L,k=4)", "chess-gl", algo)
	requireFirstBug(t, "CHESS(G+L,k=4)", firstBug)
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-gl-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			_ = f.Close()
		}
	}
}

func TestBench_PCT_d2(t *testing.T) {
	algo := &orchestrator.PCT{Depth: 2, MaxSteps: 256, Seed: 1}
	runBenchmark(t, "PCT(d=2)", "pct-d2", algo)
}

func TestBench_PCT_d3(t *testing.T) {
	algo := &orchestrator.PCT{Depth: 3, MaxSteps: 256, Seed: 1}
	runBenchmark(t, "PCT(d=3)", "pct-d3", algo)
}

func TestBench_Random(t *testing.T) {
	algo := &orchestrator.Random{Seed: 1}
	runBenchmark(t, "Random", "random", algo)
}
