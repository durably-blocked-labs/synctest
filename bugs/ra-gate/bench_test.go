package ragate

import (
	"os"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const benchMaxRuns = 500

func benchSetup(o *orchestrator.Orchestrator) {
	addrs := []string{"A", "B", "C"}
	var inCS atomic.Int32
	transports := setupCluster(addrs)
	store := NewKVStore()
	addGateNodes(o, addrs, transports, store, &inCS)
}

func benchObserver(t *testing.T, policy string) []orchestrator.ExploreOption {
	dir := os.Getenv("BENCH_DIR")
	if dir == "" {
		return nil
	}
	os.MkdirAll(dir, 0755)
	f, err := os.Create(dir + "/" + policy + ".jsonl")
	if err != nil {
		return nil
	}
	t.Cleanup(func() { f.Close() })
	return []orchestrator.ExploreOption{
		orchestrator.WithObserver(orchestrator.NewJSONLObserver(f, policy)),
	}
}

func TestBench_CHESS_GlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(4)
	algo := &orchestrator.CHESS{Bound: 2, GlobalOnly: true}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, benchObserver(t, "chess-global")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("CHESS(G-only,k=2): %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
}

func TestBench_CHESS_GL(t *testing.T) {
	runtime.GOMAXPROCS(4)
	algo := &orchestrator.CHESS{Bound: 2}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, benchObserver(t, "chess-gl")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("CHESS(G+L,k=2): %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
}

func TestBench_PCT_d2(t *testing.T) {
	runtime.GOMAXPROCS(4)
	algo := &orchestrator.PCT{Depth: 2, MaxSteps: 256, Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, benchObserver(t, "pct-d2")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("PCT(d=2): %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
}

func TestBench_PCT_d3(t *testing.T) {
	runtime.GOMAXPROCS(4)
	algo := &orchestrator.PCT{Depth: 3, MaxSteps: 256, Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, benchObserver(t, "pct-d3")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("PCT(d=3): %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
}

func TestBench_Random(t *testing.T) {
	runtime.GOMAXPROCS(4)
	algo := &orchestrator.Random{Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, benchObserver(t, "random")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	t.Logf("Random: %d runs, first_bug=%d, elapsed=%s", r.Runs, r.FirstBug, r.Elapsed)
}
