package explorer_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/shubhaankar/synctest/explorer"
	"github.com/shubhaankar/synctest/orchestrator"
)

type reviewSink struct{ startErr, endErr error }

func (s *reviewSink) StartTrial(int) error                       { return s.startErr }
func (s *reviewSink) EndTrial(int, orchestrator.RunResult) error { return s.endErr }

type budgetAfterRunAlgorithm struct {
	lifecycleAlgorithm
	budget error
}

func (a *budgetAfterRunAlgorithm) AfterRun(rr orchestrator.RunResult) {
	a.result = rr
	a.budget = errors.New("search frontier full")
}
func (a *budgetAfterRunAlgorithm) SearchError() error { return a.budget }

func TestReviewFailureAccountingProcesses(t *testing.T) {
	for _, scenario := range []string{"prestart-test", "failure-budget"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReviewFailureAccountingWorker$", "-test.timeout=8s")
			cmd.Env = append(os.Environ(), "BUBBLES_REVIEW_WORKER="+scenario)
			output, err := cmd.CombinedOutput()
			if scenario == "prestart-test" {
				if err == nil || !strings.Contains(string(output), "incomplete exploration: prestart sink failed") {
					t.Fatalf("prestart silently passed: %v\n%s", err, output)
				}
			} else if err != nil || !strings.Contains(string(output), "REVIEW_ACCOUNTING_OK") {
				t.Fatalf("worker: %v\n%s", err, output)
			}
		})
	}
}
func TestReviewFailureAccountingWorker(t *testing.T) {
	switch os.Getenv("BUBBLES_REVIEW_WORKER") {
	case "prestart-test":
		explorer.Test(t, func(*testing.T) { panic("must not start") }, explorer.WithTrialSink(&reviewSink{startErr: errors.New("prestart sink failed")}))
	case "failure-budget":
		a := &budgetAfterRunAlgorithm{}
		observed := 0
		r := explorer.Explore(t, func(t *testing.T) { t.Error("workload assertion") }, a, explorer.MaxRuns(1), explorer.WithObserver(func(_ int, _ int, _ time.Duration, rr orchestrator.RunResult, passed bool) {
			observed++
			if passed || rr.Passed || !rr.UserFailed {
				panic("observer lost failure")
			}
		}))
		if r.Failed != 1 || r.FirstBug != 1 || r.Incomplete != 1 || observed != 1 || r.Cause != a.budget {
			panic("completed failure lost to frontier budget")
		}
		os.Stdout.WriteString("REVIEW_ACCOUNTING_OK\n")
		os.Exit(0)
	default:
		t.Skip("subprocess fixture")
	}
}
func TestEndTrialFailureObserverGetsFinalStatus(t *testing.T) {
	cause := errors.New("terminal sink failed")
	observed := 0
	r := explorer.Explore(t, func(*testing.T) {}, &lifecycleAlgorithm{}, explorer.WithTrialSink(&reviewSink{endErr: cause}), explorer.WithObserver(func(_ int, _ int, _ time.Duration, rr orchestrator.RunResult, passed bool) {
		observed++
		if passed || rr.Passed || rr.Cause != cause {
			t.Errorf("stale observer: passed=%t rr=%+v", passed, rr)
		}
	}))
	if r.Incomplete != 1 || r.Cause != cause || observed != 1 {
		t.Fatalf("result=%+v observed=%d", r, observed)
	}
}
func TestBudgetExhaustedAlgorithmReuseIsIncomplete(t *testing.T) {
	a := &explorer.CHESS{Bound: 1, MaxFrontier: 1}
	a.BeforeRun()
	a.AfterRun(orchestrator.RunResult{Passed: true, Trace: orchestrator.Trace{{Kind: orchestrator.Local, Node: "local", Alternatives: 3, AltIDs: []string{"B1", "B2", "B3"}, Index: 0}}})
	started := false
	r := explorer.Explore(t, func(*testing.T) { started = true }, a)
	if r.Incomplete != 1 || r.Cause == nil || r.Runs != 0 || started {
		t.Fatalf("result=%+v started=%t", r, started)
	}
}

type lifecycleAlgorithm struct {
	decide func(orchestrator.DecisionPoint) int
	result orchestrator.RunResult
	ran    bool
}

func (a *lifecycleAlgorithm) BeforeRun() bool {
	if a.ran {
		return false
	}
	a.ran = true
	return true
}
func (a *lifecycleAlgorithm) Decide(dp orchestrator.DecisionPoint) int {
	if a.decide != nil {
		return a.decide(dp)
	}
	return 0
}
func (a *lifecycleAlgorithm) AfterRun(r orchestrator.RunResult) { a.result = r }

func TestLocalSelectUnifiedReplay(t *testing.T) {
	selected := 0
	workload := func(*testing.T) {
		a, b := make(chan int, 1), make(chan int, 1)
		a <- 1
		b <- 2
		select {
		case <-a:
			selected = 1
		case <-b:
			selected = 2
		}
	}
	algorithm := &lifecycleAlgorithm{decide: func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Select && dp.N() > 1 {
			return 1
		}
		return 0
	}}
	result := explorer.Explore(t, workload, algorithm, explorer.MaxRuns(1), explorer.WithSelectChoices())
	if !result.Successful() || !algorithm.result.Passed {
		t.Fatalf("result=%+v trial=%+v", result, algorithm.result)
	}
	selectEvents := 0
	for _, s := range algorithm.result.Trace {
		if s.Kind == orchestrator.Select {
			selectEvents++
			if s.Index != 1 || len(s.RunqBGIDs) != 0 {
				t.Fatalf("select encoded as schedule: %+v", s)
			}
		}
	}
	if selectEvents != 1 {
		t.Fatalf("select events %d", selectEvents)
	}
	want := selected
	rec := orchestrator.UnifiedReplay{Version: 1, SelectControl: true, Trace: algorithm.result.Trace, Terminal: algorithm.result.Outcome}
	selected = 0
	rr := explorer.ReplayUnified(t, workload, rec)
	if !rr.Passed || rr.Diverged || rr.WorkerUnsafe || selected != want {
		t.Fatalf("replay=%+v selected=%d want=%d", rr, selected, want)
	}
}

func TestExplorerFailureProcesses(t *testing.T) {
	for _, scenario := range []string{"panic", "negative", "upper"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExplorerFailureWorker$", "-test.timeout=10s")
			cmd.Env = append(os.Environ(), "BUBBLES_EXPLORER_WORKER="+scenario)
			output, err := cmd.CombinedOutput()
			if err != nil || !strings.Contains(string(output), "EXPLORER_RESULT_OK") {
				t.Fatalf("worker=%v\n%s", err, output)
			}
		})
	}
}
func TestExplorerFailureWorker(t *testing.T) {
	scenario := os.Getenv("BUBBLES_EXPLORER_WORKER")
	if scenario == "" {
		t.Skip("subprocess fixture")
	}
	cause := errors.New("exact algorithm defect")
	a := &lifecycleAlgorithm{decide: func(dp orchestrator.DecisionPoint) int {
		switch scenario {
		case "panic":
			panic(cause)
		case "negative":
			return -1
		default:
			return dp.N()
		}
	}}
	result := explorer.Explore(t, func(*testing.T) { runtime.Gosched() }, a, explorer.MaxRuns(3))
	rr := a.result
	if result.Successful() || result.Runs != 1 || result.Incomplete != 1 || rr.UserFailed || rr.Diverged || !rr.WorkerUnsafe || rr.Outcome != orchestrator.OutcomeInfrastructure {
		panic("failed algorithm misclassified")
	}
	if scenario == "panic" && rr.Cause != cause {
		panic("exact cause lost")
	}
	if orchestrator.WorkerReusable() {
		panic("unsafe worker reusable")
	}
	os.Stdout.WriteString("EXPLORER_RESULT_OK\n")
	os.Exit(0)
}
