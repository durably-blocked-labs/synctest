package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// Failed experimental runtime roots are disposable worker processes, never
// recovered and reused as subsequent tests in the same process.
func TestLifecycleOutcomeProcesses(t *testing.T) {
	for _, scenario := range []string{"local-panic", "local-index", "local-divergence", "root-panic", "limit", "unjoined", "setup-cleanup"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLifecycleOutcomeWorker$", "-test.timeout=10s")
			cmd.Env = append(os.Environ(), "BUBBLES_OUTCOME_WORKER="+scenario)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("worker: %v\n%s", err, output)
			}
			if !strings.Contains(string(output), "LIFECYCLE_RESULT ") {
				t.Fatalf("missing result: %s", output)
			}
		})
	}
}
func TestLifecycleOutcomeWorker(t *testing.T) {
	scenario := os.Getenv("BUBBLES_OUTCOME_WORKER")
	if scenario == "" {
		t.Skip("subprocess fixture")
	}
	runtime.GOMAXPROCS(2)
	if scenario == "setup-cleanup" {
		cause := fmt.Errorf("setup defect")
		sink := &failingTrialSink{}
		result := New().ExploreWith(t, func(o *Orchestrator) {
			o.AddNode(&setupCleanupFailureTransport{stubTransport: newStubTransport("a")}, func(*testing.T) { panic("must not start") })
			panic(cause)
		}, &Random{}, GlobalMaxRuns(1), WithTrialSink(sink))
		if result.Incomplete != 1 || sink.last.Cause != cause || sink.last.TeardownError == nil || !sink.last.WorkerUnsafe || WorkerReusable() {
			t.Fatalf("result=%+v terminal=%+v", result, sink.last)
		}
		next := New().RunWith(t, func(DecisionPoint) int { return 0 })
		if next.Passed || !next.WorkerUnsafe || next.Cause == nil {
			t.Fatalf("fresh orchestrator accepted unsafe worker: %+v", next)
		}
		fmt.Println("LIFECYCLE_RESULT setup-cleanup-retired")
		os.Exit(0)
	}
	if scenario == "unjoined" {
		testUnjoinedWorker(t)
		return
	}
	cause := &struct{ Message string }{"exact local/root cause"}
	o := New()
	o.AddNode(newStubTransport("a"), func(*testing.T) {
		if scenario == "root-panic" {
			panic(cause)
		}
		for i := 0; i < 20; i++ {
			runtime.Gosched()
		}
	})
	if scenario == "limit" {
		o.limits.Decisions = 2
	}
	wantStep := -1
	rr := o.RunWith(t, func(dp DecisionPoint) int {
		switch scenario {
		case "local-panic":
			panic(cause)
		case "local-index":
			return dp.N()
		case "local-divergence":
			if dp.Step >= 2 {
				wantStep = dp.Step
				panic(replayDivergenceError{Step: dp.Step, Detail: "local prefix mismatch"})
			}
		}
		return 0
	})
	if rr.Passed || rr.UserFailed || !rr.WorkerUnsafe || !o.unsafe {
		t.Fatalf("unsafe failure misclassified: %+v", rr)
	}
	switch scenario {
	case "local-panic":
		if rr.Cause != cause || rr.Outcome != OutcomeInfrastructure { // algorithm defect, not a workload assertion
			t.Fatalf("local cause lost: %+v", rr)
		}
	case "root-panic":
		if rr.Cause != cause || rr.Outcome != OutcomePanic {
			t.Fatalf("root cause lost: %+v", rr)
		}
	case "local-index":
		if rr.Outcome != OutcomeInfrastructure || rr.Diverged || !strings.Contains(fmt.Sprint(rr.Cause), "algorithm returned index") {
			t.Fatalf("index classified as divergence: %+v", rr)
		}
	case "local-divergence":
		if !rr.Diverged || rr.DivergenceStep != wantStep || wantStep < 2 {
			t.Fatalf("local divergence lost: %+v", rr)
		}
	case "limit":
		if rr.Outcome != OutcomeResourceLimit {
			t.Fatalf("limit lost: %+v", rr)
		}
	}
	// Demonstrate unsafe reset is refused before any new bubble is launched.
	next := o.RunWith(t, func(DecisionPoint) int { return 0 })
	if next.Passed || next.Cause == nil {
		t.Fatal("unsafe worker reset accepted")
	}
	b, _ := json.Marshal(struct {
		Outcome Outcome
		Unsafe  bool
	}{rr.Outcome, rr.WorkerUnsafe})
	fmt.Printf("LIFECYCLE_RESULT %s\n", b)
	os.Exit(0) // worker recycling is the containment boundary, not panic recovery
}

type setupCleanupFailureTransport struct{ *stubTransport }

func (*setupCleanupFailureTransport) Close() error { return fmt.Errorf("setup cleanup failed") }

type unjoinedTransport struct {
	*attachedPublicationTransport
	closes int
}

func (t *unjoinedTransport) Close() error { t.closes++; return nil } // deliberately fails to cancel its receiver
func testUnjoinedWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	sink := &failingTrialSink{}
	setups := 0
	never := make(chan struct{})
	tr := &unjoinedTransport{attachedPublicationTransport: &attachedPublicationTransport{stubTransport: newStubTransport("a")}}
	result := New().ExploreWith(t, func(o *Orchestrator) {
		setups++
		o.AddNode(tr, func(*testing.T) { synctest.ExternalWait(func() { <-never }) })
	}, &Random{}, GlobalMaxRuns(3), WithContext(ctx), WithTrialSink(sink))
	if result.Successful() || result.Runs != 1 || result.Incomplete != 1 || setups != 1 || tr.closes != 1 || !sink.last.WorkerUnsafe || sink.last.TeardownError == nil || WorkerReusable() {
		t.Fatalf("unsafe join accepted: %+v terminal=%+v setups=%d closes=%d", result, sink.last, setups, tr.closes)
	}
	fmt.Println("LIFECYCLE_RESULT unjoined-refused-next-setup")
	os.Exit(0)
}
