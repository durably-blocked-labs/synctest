package orchestrator

import (
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"
)

// BenchmarkCase defines one algorithm to benchmark.
type BenchmarkCase struct {
	Name string
	Algo Algorithm
	Opts []ExploreOption // MaxRuns, Observer, etc.
}

// BenchmarkResult is the outcome of running one algorithm.
type BenchmarkResult struct {
	Name        string
	Runs        int
	Failed      int
	FirstBugRun int           // -1 if no bug found
	Elapsed     time.Duration
	BugFound    bool
}

// Benchmark runs each case against the same setup and returns results.
// Each case gets a fresh orchestrator. Results are logged as a table.
func Benchmark(
	t *testing.T,
	setup func(*Orchestrator),
	cases []BenchmarkCase,
) []BenchmarkResult {
	t.Helper()
	results := make([]BenchmarkResult, len(cases))

	for i, c := range cases {
		o := New()
		er := o.ExploreWith(t, setup, c.Algo, c.Opts...)
		results[i] = BenchmarkResult{
			Name:        c.Name,
			Runs:        er.Runs,
			Failed:      er.Failed,
			FirstBugRun: er.FirstBug,
			Elapsed:     er.Elapsed,
			BugFound:    er.Failed > 0,
		}
	}

	// Log comparison table.
	t.Logf("\n%-20s %8s %8s %12s %s", "Algorithm", "Runs", "1st Bug", "Elapsed", "Found?")
	t.Logf("%-20s %8s %8s %12s %s", "---------", "----", "-------", "-------", "------")
	for _, r := range results {
		bugStr := "-"
		if r.FirstBugRun > 0 {
			bugStr = fmt.Sprintf("%d", r.FirstBugRun)
		}
		found := "no"
		if r.BugFound {
			found = "YES"
		}
		t.Logf("%-20s %8d %8s %12s %s", r.Name, r.Runs, bugStr, r.Elapsed.Round(time.Millisecond), found)
	}

	return results
}

// WriteTreeJSON writes the CHESS exploration tree as JSON.
func WriteTreeJSON(w io.Writer, tree []RunNode) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(tree)
}

// WriteResultsJSON writes benchmark results as JSON.
func WriteResultsJSON(w io.Writer, results []BenchmarkResult) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}
