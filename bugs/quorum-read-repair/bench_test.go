package quorumreadrepair

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/shubhaankar/synctest/orchestrator"
)

const benchDefaultMaxRuns = 2000

type benchmarkOutcome struct {
	mu     sync.Mutex
	nextID int
	bugs   map[int]bool
}

func newBenchmarkOutcome() *benchmarkOutcome {
	return &benchmarkOutcome{bugs: make(map[int]bool)}
}

func (o *benchmarkOutcome) beginRun() (int, func(string, []VersionedValue)) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.nextID++
	runID := o.nextID
	o.bugs[runID] = false
	return runID, func(replica string, values []VersionedValue) {
		if hasValues(values, "A", "B") {
			return
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		o.bugs[runID] = true
	}
}

func (o *benchmarkOutcome) bugFound(runID int) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.bugs[runID]
}

func completeQuorumBugTrace(rr orchestrator.RunResult) bool {
	var put, get, repair bool
	for _, step := range rr.Trace {
		if step.Kind != orchestrator.Global {
			continue
		}
		switch {
		case step.To == "R1" && step.MsgType == "Put":
			put = true
		case step.To == "R1" && step.MsgType == "Get":
			get = true
		case step.To == "R1" && step.MsgType == "Repair":
			repair = true
		}
	}
	return put && get && repair
}

func TestBenchmarkOutcomeIsolatesStaleRecorders(t *testing.T) {
	outcomes := newBenchmarkOutcome()
	staleRun, staleRecord := outcomes.beginRun()
	currentRun, currentRecord := outcomes.beginRun()

	staleRecord("R1", []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}})
	if outcomes.bugFound(currentRun) {
		t.Fatal("stale recorder from a previous run polluted the current run")
	}
	if !outcomes.bugFound(staleRun) {
		t.Fatal("stale run should still record its own bug")
	}

	currentRecord("R1", []VersionedValue{
		{Value: "A", Clock: Clock{"C1": 1}},
		{Value: "B", Clock: Clock{"C2": 1}},
	})
	if outcomes.bugFound(currentRun) {
		t.Fatal("current run should not flag a correct sibling set")
	}
}

func TestCompleteQuorumBugTraceRejectsEmptyTrace(t *testing.T) {
	if completeQuorumBugTrace(orchestrator.RunResult{}) {
		t.Fatal("empty trace should not be usable as a benchmark bug trace")
	}
}

func TestCompleteQuorumBugTraceAcceptsQuorumTrace(t *testing.T) {
	rr := orchestrator.RunResult{Trace: orchestrator.Trace{
		{Kind: orchestrator.Global, From: "C1", To: "R1", MsgType: "Put"},
		{Kind: orchestrator.Global, From: "Reader", To: "R1", MsgType: "Get"},
		{Kind: orchestrator.Global, From: "Reader", To: "R1", MsgType: "Repair"},
	}}
	if !completeQuorumBugTrace(rr) {
		t.Fatal("complete quorum trace should be usable as a benchmark bug trace")
	}
}

func benchDir() string {
	d := os.Getenv("BENCH_DIR")
	if d != "" {
		_ = os.MkdirAll(d, 0755)
	}
	return d
}

func benchSeed() int64 {
	s := os.Getenv("BENCH_ATTEMPT")
	if s == "" {
		return 1
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 1
	}
	return n
}

func benchMaxRuns() int {
	s := os.Getenv("BENCH_MAX_RUNS")
	if s == "" {
		return benchDefaultMaxRuns
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return benchDefaultMaxRuns
	}
	return n
}

func TestBenchSeedUsesAttemptNumber(t *testing.T) {
	t.Setenv("BENCH_ATTEMPT", "42")
	if got := benchSeed(); got != 42 {
		t.Fatalf("benchSeed() = %d, want 42", got)
	}
}

func TestBenchSeedDefaultsToOne(t *testing.T) {
	for _, value := range []string{"", "0", "-7", "not-a-number"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("BENCH_ATTEMPT", value)
			if got := benchSeed(); got != 1 {
				t.Fatalf("benchSeed() = %d, want 1", got)
			}
		})
	}
}

func TestBenchMaxRunsUsesEnv(t *testing.T) {
	t.Setenv("BENCH_MAX_RUNS", "1234")
	if got := benchMaxRuns(); got != 1234 {
		t.Fatalf("benchMaxRuns() = %d, want 1234", got)
	}
}

func TestBenchMaxRunsDefaultsToDefault(t *testing.T) {
	for _, value := range []string{"", "0", "-7", "not-a-number"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("BENCH_MAX_RUNS", value)
			if got := benchMaxRuns(); got != benchDefaultMaxRuns {
				t.Fatalf("benchMaxRuns() = %d, want %d", got, benchDefaultMaxRuns)
			}
		})
	}
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
		t.Fatalf("%s did not find bug within %d runs", label, benchMaxRuns())
	}
}

func runBenchmark(t *testing.T, label, policy string, algo orchestrator.Algorithm) (orchestrator.ExplorationResult, int) {
	t.Helper()
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()
	logBenchStart(t, label)

	firstBug := -1
	outcomes := newBenchmarkOutcome()
	var currentRun int
	setup := func(o *orchestrator.Orchestrator) {
		runID, record := outcomes.beginRun()
		currentRun = runID
		addQuorumReadRepairScenarioWithRecorder(o, record, false)
	}
	benchObservers := observers(t, policy)
	observe := func(runNum int, nonFIFO int, elapsed time.Duration, rr orchestrator.RunResult, passed bool) {
		bugFound := outcomes.bugFound(currentRun) && completeQuorumBugTrace(rr)
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
		orchestrator.GlobalMaxRuns(benchMaxRuns()),
		orchestrator.WithObserver(observe),
	}
	orch := orchestrator.New()
	r := orch.ExploreWith(t, setup, algo, opts...)
	logBenchDone(t, label, r.Runs, firstBug, r.Elapsed)
	return r, firstBug
}

func quorumRepairDPORFrontier(dp orchestrator.DecisionPoint) (int, bool) {
	if dp.Kind == orchestrator.Local && dp.N() > 1 && dp.Node == "R1" {
		return dp.N() - 1, true
	}
	if dp.Kind != orchestrator.Global || dp.N() <= 1 {
		return 0, false
	}
	for _, want := range []struct {
		from, to, msg string
	}{
		{"C1", "R1", "Put"},
		{"C1", "R2", "Put"},
		{"C2", "R2", "Put"},
		{"C2", "R3", "Put"},
		{"R1", "C1", "PutAck"},
		{"R2", "C1", "PutAck"},
		{"R2", "C2", "PutAck"},
		{"R3", "C2", "PutAck"},
		{"C1", "C2", "Control"},
		{"C1", "Reader", "Control"},
		{"C2", "Reader", "Control"},
		{"Reader", "R1", "Get"},
		{"Reader", "R2", "Get"},
		{"R1", "Reader", "GetResp"},
		{"R2", "Reader", "GetResp"},
		{"Reader", "R1", "Repair"},
		{"C2", "R1", "Put"},
	} {
		if idx := chooseGlobal(dp, want.from, want.to, want.msg); idx >= 0 {
			return idx, true
		}
	}
	return 0, false
}

func TestBench_CHESS_GlobalOnly(t *testing.T) {
	algo := &orchestrator.CHESS{Bound: 8, GlobalOnly: true}
	runBenchmark(t, "CHESS(G-only,k=8)", "chess-global", algo)
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-global-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			_ = f.Close()
		}
	}
}

func TestBench_CHESS_GL(t *testing.T) {
	algo := &orchestrator.CHESS{Bound: 8}
	_, firstBug := runBenchmark(t, "CHESS(G+L,k=8)", "chess-gl", algo)
	_ = firstBug
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-gl-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			_ = f.Close()
		}
	}
}

func TestBench_DPOR_GL(t *testing.T) {
	algo := &orchestrator.DPOR{
		Bound:               32,
		ConservativeGlobal:  true,
		PrioritizeEndpoints: []string{"Reader", "R1"},
		PrioritizeRequests:  true,
		Frontier:            quorumRepairDPORFrontier,
	}
	_, firstBug := runBenchmark(t, "DPOR(G+L,k=32)", "dpor-gl", algo)
	requireFirstBug(t, "DPOR(G+L,k=32)", firstBug)
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/dpor-gl-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			_ = f.Close()
		}
	}
}

func TestBench_PCT_d2(t *testing.T) {
	algo := &orchestrator.PCT{Depth: 2, MaxSteps: 256, Seed: benchSeed()}
	runBenchmark(t, "PCT(d=2)", "pct-d2", algo)
}

func TestBench_PCT_d3(t *testing.T) {
	algo := &orchestrator.PCT{Depth: 3, MaxSteps: 256, Seed: benchSeed()}
	runBenchmark(t, "PCT(d=3)", "pct-d3", algo)
}

func TestBench_Random(t *testing.T) {
	algo := &orchestrator.Random{Seed: benchSeed()}
	runBenchmark(t, "Random", "random", algo)
}

func TestBench_Targeted(t *testing.T) {
	runtime.GOMAXPROCS(8)
	outcomes := newBenchmarkOutcome()
	runID, record := outcomes.beginRun()

	orch := orchestrator.New()
	addQuorumReadRepairScenarioWithRecorder(orch, record, false)

	logBenchStart(t, "Targeted")
	runStart := time.Now()
	rr := orch.RunWith(t, func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Global && dp.N() > 1 {
			if idx := chooseGlobal(dp, "C1", "R1", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C1", "R2", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C2", "R2", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C2", "R3", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R1", "C1", "PutAck"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R2", "C1", "PutAck"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R2", "C2", "PutAck"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R3", "C2", "PutAck"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C1", "C2", "Control"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C1", "Reader", "Control"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C2", "Reader", "Control"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "Reader", "R1", "Get"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "Reader", "R2", "Get"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R1", "Reader", "GetResp"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R2", "Reader", "GetResp"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "Reader", "R1", "Repair"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C2", "R1", "Put"); idx >= 0 {
				return idx
			}
		}
		if dp.Kind == orchestrator.Local && dp.N() > 1 && dp.Node == "R1" {
			return dp.N() - 1
		}
		return 0
	})
	rr.Elapsed = time.Since(runStart)
	bugFound := outcomes.bugFound(runID) && completeQuorumBugTrace(rr)
	if !bugFound {
		t.Fatalf("targeted benchmark did not expose quorum read-repair bug")
	}
	logBenchDone(t, "Targeted", 1, 1, rr.Elapsed)

	nonFIFO := 0
	for _, step := range rr.Trace {
		if step.Index != 0 {
			nonFIFO++
		}
	}
	rr.Passed = false
	rr.UserFailed = true
	for _, observer := range observers(t, "targeted") {
		observer(1, nonFIFO, rr.Elapsed, rr, false)
	}
}
