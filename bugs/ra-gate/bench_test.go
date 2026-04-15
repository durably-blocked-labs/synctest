package ragate

import (
	"os"
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const benchMaxRuns = 500

func benchSetup(o *orchestrator.Orchestrator) {
	addrs := []string{"A", "B", "C"}
	transports := setupClusterWithKV(addrs)
	addKVNode(o, transports)
	addGateNodes(o, addrs, transports)
}

func benchDir() string {
	d := os.Getenv("BENCH_DIR")
	if d != "" {
		os.MkdirAll(d, 0755)
	}
	return d
}

func observers(t *testing.T, policy string) []orchestrator.ExploreOption {
	dir := benchDir()
	if dir == "" {
		return nil
	}
	var opts []orchestrator.ExploreOption
	// Summary JSONL
	if f, err := os.Create(dir + "/" + policy + ".jsonl"); err == nil {
		t.Cleanup(func() { f.Close() })
		opts = append(opts, orchestrator.WithObserver(orchestrator.NewJSONLObserver(f, policy)))
	}
	// Detailed trace JSONL
	if f, err := os.Create(dir + "/" + policy + "-trace.jsonl"); err == nil {
		t.Cleanup(func() { f.Close() })
		opts = append(opts, orchestrator.WithObserver(orchestrator.NewDetailedObserver(f, policy)))
	}
	return opts
}

func TestBench_CHESS_GlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	algo := &orchestrator.CHESS{Bound: 2, GlobalOnly: true}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "chess-global")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("CHESS(G-only,k=2): %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
	// Export CHESS tree.
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-global-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			f.Close()
		}
	}
}

func TestBench_CHESS_GL(t *testing.T) {
	runtime.GOMAXPROCS(8)
	algo := &orchestrator.CHESS{Bound: 2}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "chess-gl")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("CHESS(G+L,k=2): %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-gl-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			f.Close()
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
}

func TestBench_PCT_d3(t *testing.T) {
	runtime.GOMAXPROCS(8)
	algo := &orchestrator.PCT{Depth: 3, MaxSteps: 256, Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "pct-d3")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("PCT(d=3): %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
}

func TestBench_Random(t *testing.T) {
	runtime.GOMAXPROCS(8)
	algo := &orchestrator.Random{Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "random")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("Random: %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
}
