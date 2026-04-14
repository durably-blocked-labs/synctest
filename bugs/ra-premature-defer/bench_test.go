package raprematuredefer

import (
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const benchMaxRuns = 500
const benchMaxRunsCHESSGlobal = 150
const benchMaxRunsCHESSGL = 375

func benchSetup(o *orchestrator.Orchestrator) {
	addrs := scenarioAddrs
	var inCS atomic.Int32
	var violated atomic.Bool
	transports := setupCluster(addrs)
	store := NewKVStore()
	addPrematureDeferNodes(o, addrs, transports, store, &inCS, &violated)
}

func benchDir() string {
	d := os.Getenv("BENCH_DIR")
	if d != "" {
		_ = os.MkdirAll(d, 0755)
	}
	return d
}

func observers(t *testing.T, policy string) []orchestrator.ExploreOption {
	dir := benchDir()
	if dir == "" {
		return nil
	}
	var opts []orchestrator.ExploreOption
	if f, err := os.Create(dir + "/" + policy + ".jsonl"); err == nil {
		t.Cleanup(func() { f.Close() })
		opts = append(opts, orchestrator.WithObserver(orchestrator.NewJSONLObserver(f, policy)))
	}
	if f, err := os.Create(dir + "/" + policy + "-trace.jsonl"); err == nil {
		t.Cleanup(func() { f.Close() })
		opts = append(opts, orchestrator.WithObserver(orchestrator.NewDetailedObserver(f, policy)))
	}
	return opts
}

func logBenchStart(t *testing.T, label string) {
	t.Helper()
	fmt.Printf("START %s\n", label)
}

func logBenchDone(t *testing.T, label string, runs int, firstBug int, elapsed interface{}) {
	t.Helper()
	fmt.Printf("DONE %s: runs=%d first_bug=%d elapsed=%v\n", label, runs, firstBug, elapsed)
}

func TestBench_CHESS_GlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(4)
	logBenchStart(t, "CHESS(G-only,k=4)")
	algo := &orchestrator.CHESS{Bound: 4, GlobalOnly: true}
	// This staged workload hits a non-converging prefix shortly after 150
	// global-only runs; cap earlier so package-local benchmark generation
	// completes instead of stalling on that tail.
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRunsCHESSGlobal)}, observers(t, "chess-global")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	logBenchDone(t, "CHESS(G-only,k=4)", r.Runs, r.FirstBug, r.Elapsed)
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-global-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			_ = f.Close()
		}
	}
}

func TestBench_CHESS_GL(t *testing.T) {
	runtime.GOMAXPROCS(4)
	logBenchStart(t, "CHESS(G+L,k=4)")
	algo := &orchestrator.CHESS{Bound: 4}
	// This workload also hits a non-converging CHESS(G+L) suffix late in the
	// run; cap before that point so benchmark-charts completes.
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRunsCHESSGL)}, observers(t, "chess-gl")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	logBenchDone(t, "CHESS(G+L,k=4)", r.Runs, r.FirstBug, r.Elapsed)
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-gl-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			_ = f.Close()
		}
	}
}

func TestBench_PCT_d2(t *testing.T) {
	runtime.GOMAXPROCS(4)
	logBenchStart(t, "PCT(d=2)")
	algo := &orchestrator.PCT{Depth: 2, MaxSteps: 256, Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "pct-d2")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	logBenchDone(t, "PCT(d=2)", r.Runs, r.FirstBug, r.Elapsed)
}

func TestBench_PCT_d3(t *testing.T) {
	runtime.GOMAXPROCS(4)
	logBenchStart(t, "PCT(d=3)")
	algo := &orchestrator.PCT{Depth: 3, MaxSteps: 256, Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "pct-d3")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	logBenchDone(t, "PCT(d=3)", r.Runs, r.FirstBug, r.Elapsed)
}

func TestBench_Random(t *testing.T) {
	runtime.GOMAXPROCS(4)
	logBenchStart(t, "Random")
	algo := &orchestrator.Random{Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "random")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	logBenchDone(t, "Random", r.Runs, r.FirstBug, r.Elapsed)
}
