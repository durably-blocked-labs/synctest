package rapriorityinversion

import (
	"os"
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const benchMaxRuns = 50

func requireFirstBug(t *testing.T, label string, firstBug int) {
	t.Helper()
	if firstBug == -1 {
		t.Fatalf("%s did not find bug within %d runs", label, benchMaxRuns)
	}
}

func benchSetup(o *orchestrator.Orchestrator) {
	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)
	addKVNode(o, transports)
	addPriorityInversionNodesChecked(o, addrs, transports)
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
		t.Cleanup(func() { _ = f.Close() })
		opts = append(opts, orchestrator.WithObserver(orchestrator.NewJSONLObserver(f, policy)))
	}
	if f, err := os.Create(dir + "/" + policy + "-trace.jsonl"); err == nil {
		t.Cleanup(func() { _ = f.Close() })
		opts = append(opts, orchestrator.WithObserver(orchestrator.NewDetailedObserver(f, policy)))
	}
	return opts
}

func TestBench_CHESS_GlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	algo := &orchestrator.CHESS{Bound: 3, GlobalOnly: true}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "chess-global")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("CHESS(G-only,k=3): %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
	requireFirstBug(t, "CHESS(G-only,k=3)", r.FirstBug)
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-global-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			_ = f.Close()
		}
	}
}

func TestBench_CHESS_GL(t *testing.T) {
	runtime.GOMAXPROCS(8)
	algo := &orchestrator.CHESS{Bound: 3}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "chess-gl")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("CHESS(G+L,k=3): %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
	requireFirstBug(t, "CHESS(G+L,k=3)", r.FirstBug)
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-gl-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			_ = f.Close()
		}
	}
}

func TestBench_PCT_d2(t *testing.T) {
	runtime.GOMAXPROCS(8)
	algo := &orchestrator.PCT{Depth: 2, MaxSteps: 256, Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "pct-d2")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("PCT(d=2): %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
	requireFirstBug(t, "PCT(d=2)", r.FirstBug)
}

func TestBench_PCT_d3(t *testing.T) {
	runtime.GOMAXPROCS(8)
	algo := &orchestrator.PCT{Depth: 3, MaxSteps: 256, Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "pct-d3")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("PCT(d=3): %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
	requireFirstBug(t, "PCT(d=3)", r.FirstBug)
}

func TestBench_Random(t *testing.T) {
	runtime.GOMAXPROCS(8)
	algo := &orchestrator.Random{Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "random")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("Random: %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
	requireFirstBug(t, "Random", r.FirstBug)
}
