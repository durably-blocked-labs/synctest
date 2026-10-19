// Package gobench_test benchmarks all GoBench kernel bugs with multiple
// scheduling strategies (CHESS, PCT, Random) and exports JSONL metrics.
//
// Run:
//
//	GODEBUG=asyncpreemptoff=1 BENCH_DIR=charts/data \
//	  ./go/bin/go test -v -run TestBench ./bugs/gobench/ -timeout 300s
package gobench_test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/shubhaankar/synctest/bugs/gobench/etcd6873"
	"github.com/shubhaankar/synctest/bugs/gobench/etcd7443"
	"github.com/shubhaankar/synctest/bugs/gobench/etcd7492"
	"github.com/shubhaankar/synctest/bugs/gobench/etcd7902"
	"github.com/shubhaankar/synctest/bugs/gobench/grpc1353"
	"github.com/shubhaankar/synctest/bugs/gobench/grpc1460"
	"github.com/shubhaankar/synctest/bugs/gobench/istio16224"
	"github.com/shubhaankar/synctest/bugs/gobench/k8s10182"
	"github.com/shubhaankar/synctest/bugs/gobench/k8s1321"
	"github.com/shubhaankar/synctest/bugs/gobench/k8s26980"
	"github.com/shubhaankar/synctest/bugs/gobench/k8s6632"
	"github.com/shubhaankar/synctest/bugs/gobench/moby28462"
	"github.com/shubhaankar/synctest/bugs/gobench/moby33781"
	"github.com/shubhaankar/synctest/bugs/gobench/serving2137"
	"github.com/shubhaankar/synctest/explorer"
	"github.com/shubhaankar/synctest/orchestrator"
)

const maxRuns = 500

type bug struct {
	name     string
	workload func(*testing.T)
	bound    int // CHESS bound (0 = default 2)
}

var bugs = []bug{
	// Original 5 (all detected).
	{"grpc1353", grpc1353.Workload, 0},
	{"k8s6632", k8s6632.Workload, 0},
	{"etcd7902", etcd7902.Workload, 0},
	{"moby33781", moby33781.Workload, 0},
	{"etcd7443", etcd7443.Workload, 3},
	// New 8 Channel & Lock bugs.
	{"etcd6873", etcd6873.Workload, 0},
	{"grpc1460", grpc1460.Workload, 0},
	{"istio16224", istio16224.Workload, 0},
	{"k8s10182", k8s10182.Workload, 0},
	{"k8s26980", k8s26980.Workload, 0},
	{"moby28462", moby28462.Workload, 3}, // busy-loop needs higher bound for reliable CHESS detection
	// Not detected at goroutine-scheduling granularity:
	{"k8s1321", k8s1321.Workload, 0},
	{"etcd7492", etcd7492.Workload, 0},
	{"serving2137", serving2137.Workload, 0},
}

type strategy struct {
	name string
	algo func(bound int) explorer.Algorithm
}

var strategies = []strategy{
	{"chess", func(b int) explorer.Algorithm { return &explorer.CHESS{Bound: b} }},
	{"pct-d2", func(_ int) explorer.Algorithm { return &explorer.PCT{Depth: 2, MaxSteps: 128, Seed: 1} }},
	{"pct-d3", func(_ int) explorer.Algorithm { return &explorer.PCT{Depth: 3, MaxSteps: 128, Seed: 1} }},
	{"random", func(_ int) explorer.Algorithm { return &explorer.Random{Seed: 1} }},
}

func benchDir() string {
	d := os.Getenv("BENCH_DIR")
	if d != "" {
		os.MkdirAll(d, 0o755)
	}
	return d
}

func observers(t *testing.T, tag string) []explorer.Option {
	dir := benchDir()
	if dir == "" {
		return nil
	}
	var opts []explorer.Option
	if f, err := os.Create(dir + "/" + tag + ".jsonl"); err == nil {
		t.Cleanup(func() { f.Close() })
		opts = append(opts, explorer.WithObserver(orchestrator.NewJSONLObserver(f, tag)))
	} else {
		t.Logf("warning: could not create %s/%s.jsonl: %v", dir, tag, err)
	}
	if f, err := os.Create(dir + "/" + tag + "-trace.jsonl"); err == nil {
		t.Cleanup(func() { f.Close() })
		opts = append(opts, explorer.WithObserver(orchestrator.NewDetailedObserver(f, tag)))
	} else {
		t.Logf("warning: could not create %s/%s-trace.jsonl: %v", dir, tag, err)
	}
	return opts
}

func TestBench(t *testing.T) {
	for _, b := range bugs {
		b := b
		bound := b.bound
		if bound == 0 {
			bound = 2
		}
		for _, s := range strategies {
			s := s
			tag := b.name + "-" + s.name
			t.Run(b.name+"/"+s.name, func(t *testing.T) {
				algo := s.algo(bound)
				opts := append([]explorer.Option{explorer.MaxRuns(maxRuns)},
					observers(t, tag)...)
				r := explorer.Explore(t, b.workload, algo, opts...)
				t.Logf("%s: %d runs, first_bug=%d, elapsed=%s",
					s.name, r.Runs, r.FirstBug, r.Elapsed)
				// Export CHESS tree for visualization.
				if chess, ok := algo.(*explorer.CHESS); ok {
					if dir := benchDir(); dir != "" {
						if f, err := os.Create(dir + "/" + tag + "-tree.json"); err == nil {
							orchestrator.WriteTreeJSON(f, chess.Tree())
							f.Close()
						}
					}
				}
			})
		}
	}
}

// TestSeedSweep runs Random and PCT with many seeds on etcd#7443 to show
// variance across seeds vs CHESS's deterministic behavior.
//
//	GODEBUG=asyncpreemptoff=1 BENCH_DIR=charts/data \
//	  ./go/bin/go test -v -run TestSeedSweep ./bugs/gobench/ -timeout 600s
func TestSeedSweep(t *testing.T) {
	const seeds = 100
	dir := benchDir()

	// CHESS: deterministic baseline.
	t.Run("chess", func(t *testing.T) {
		opts := []explorer.Option{explorer.MaxRuns(maxRuns)}
		opts = append(opts, observers(t, "seed-chess")...)
		r := explorer.Explore(t, etcd7443.Workload, &explorer.CHESS{Bound: 3}, opts...)
		t.Logf("CHESS: first_bug=%d (deterministic)", r.FirstBug)
	})

	// Seed sweep helper.
	type sweepResult struct {
		Seed     int64 `json:"seed"`
		Strategy string `json:"strategy"`
		FirstBug int   `json:"first_bug"` // -1 if not found
		Runs     int   `json:"runs"`
	}
	var results []sweepResult

	for _, strat := range []struct {
		name string
		algo func(seed int64) explorer.Algorithm
	}{
		{"random", func(s int64) explorer.Algorithm { return &explorer.Random{Seed: s} }},
		{"pct-d2", func(s int64) explorer.Algorithm { return &explorer.PCT{Depth: 2, MaxSteps: 128, Seed: s} }},
		{"pct-d3", func(s int64) explorer.Algorithm { return &explorer.PCT{Depth: 3, MaxSteps: 128, Seed: s} }},
	} {
		strat := strat
		t.Run(strat.name, func(t *testing.T) {
			for seed := int64(1); seed <= seeds; seed++ {
				seed := seed
				t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
					r := explorer.Explore(t, etcd7443.Workload,
						strat.algo(seed), explorer.MaxRuns(maxRuns))
					results = append(results, sweepResult{
						Seed: seed, Strategy: strat.name,
						FirstBug: r.FirstBug, Runs: r.Runs,
					})
					if r.FirstBug > 0 {
						t.Logf("%s seed=%d: first_bug=%d", strat.name, seed, r.FirstBug)
					} else {
						t.Logf("%s seed=%d: NOT FOUND in %d runs", strat.name, seed, maxRuns)
					}
				})
			}
		})
	}

	// Export seed sweep results as JSON.
	if dir != "" {
		if f, err := os.Create(dir + "/seed-sweep.json"); err == nil {
			enc := json.NewEncoder(f)
			enc.SetIndent("", "  ")
			enc.Encode(results)
			f.Close()
		}
	}
}
