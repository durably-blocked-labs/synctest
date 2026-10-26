//go:build unix

package bubblegun

import (
	"errors"
	"flag"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var supervisorJob = flag.String("bubblegun-job", "", "supervisor fixture job")
var supervisorResume = flag.Bool("bubblegun-resume", false, "supervisor fixture resume")

// The supervisor launches this test binary as its worker. No controlled Go
// execution is involved: these fixtures test process and artifact handling.
func TestBubbleGunWorker(t *testing.T) {
	if *supervisorJob == "" {
		t.Skip("supervisor subprocess fixture")
	}
	dir := *supervisorJob
	data, err := os.ReadFile(filepath.Join(dir, "helper-mode"))
	if err != nil {
		t.Fatal(err)
	}
	mode := string(data)
	if mode == "descendant-grace-expiry" || mode == "descendant-second-signal" {
		signal.Ignore(syscall.SIGTERM)
	}
	if mode == "missing" {
		return
	}
	var job Job
	if err := readJSON(filepath.Join(dir, "job.json"), &job); err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(mode, "descendant-") {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSupervisorDescendant$")
		cmd.Env = append(os.Environ(), "BUBBLEGUN_HEARTBEAT="+filepath.Join(dir, "heartbeat"))
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "descendant-pid"), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
			t.Fatal(err)
		}
		waitForFile(t, filepath.Join(dir, "heartbeat"))
		if mode == "descendant-grace-expiry" || mode == "descendant-second-signal" {
			if err := os.WriteFile(filepath.Join(dir, "ready"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			for {
				time.Sleep(time.Hour)
			}
		}
		if mode == "descendant-timeout" {
			signal.Ignore(syscall.SIGTERM)
			for {
				time.Sleep(time.Hour)
			}
		}
		if mode == "descendant-graceful" {
			term := make(chan os.Signal, 1)
			signal.Notify(term, syscall.SIGTERM)
			defer signal.Stop(term)
			if err := os.WriteFile(filepath.Join(dir, "ready"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			<-term
			mode = "interrupted"
		}
	}
	record := RunRecord{Attempt: 1, Run: 1, Result: ResultNoFailure}
	summary := Summary{
		TotalRuns: 1, Totals: map[Result]int{ResultNoFailure: 1},
		CompletedAttempts: []int{1}, ExhaustedAttempts: []int{1},
		TerminationReason: TerminationExhausted,
	}
	exit := 0
	switch mode {
	case "malformed":
		if err := os.WriteFile(filepath.Join(dir, "summary.json"), []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	case "wrong-job":
		job.Scenario = "other"
		if err := WriteJSONAtomic(filepath.Join(dir, "job.json"), job); err != nil {
			t.Fatal(err)
		}
	case "unknown":
		summary.TerminationReason = "not-a-terminal-reason"
	case "counts":
		summary.TotalRuns = 2
	case "gap":
		record.Run = 2
	case "unfinished":
		summary.CompletedAttempts = nil
		summary.ExhaustedAttempts = nil
	case "failure":
		record.Result = ResultProgramFailure
		summary.Totals = map[Result]int{ResultProgramFailure: 1}
		summary.FirstFindings = []Finding{{Attempt: 1, Run: 1, Result: ResultProgramFailure}}
		summary.TerminationReason = TerminationFailure
		summary.ExhaustedAttempts = nil
		exit = 1
	case "continued-failure":
		record.Result = ResultProgramFailure
		summary.Totals = map[Result]int{ResultProgramFailure: 1}
		summary.FirstFindings = []Finding{{Attempt: 1, Run: 1, Result: ResultProgramFailure}}
		exit = 1
	case "resource-limit":
		record.Result = ResultProgressBudgetExceeded
		summary.Totals = map[Result]int{ResultProgressBudgetExceeded: 1}
		summary.TerminationReason = TerminationProgressBudgetExceeded
		summary.CompletedAttempts, summary.ExhaustedAttempts = nil, nil
		exit = 1
	case "interrupted":
		summary.TerminationReason = TerminationInterrupted
		summary.CompletedAttempts, summary.ExhaustedAttempts = nil, nil
	case "worker-error":
		summary.TerminationReason = TerminationError
		summary.TerminationDetail = "fixture persistence failure"
	case "exit-one":
		exit = 1
	case "exit-124":
		exit = 124
	}
	log, err := OpenRunLog(filepath.Join(dir, "runs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Append(record); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := WriteJSONAtomic(filepath.Join(dir, "summary.json"), summary); err != nil {
		t.Fatal(err)
	}
	if mode == "signaled" {
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		for {
			time.Sleep(time.Hour)
		}
	}
	if exit != 0 {
		os.Exit(exit)
	}
}

func TestSupervisorDescendant(t *testing.T) {
	path := os.Getenv("BUBBLEGUN_HEARTBEAT")
	if path == "" {
		t.Skip("supervisor descendant fixture")
	}
	signal.Ignore(syscall.SIGTERM)
	for n := 0; ; n++ {
		if err := os.WriteFile(path, []byte(strconv.Itoa(n)), 0o600); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func fixtureJob(t *testing.T, mode string) jobSpec {
	t.Helper()
	dir := t.TempDir()
	job := Job{Scenario: "fixture", Algorithm: AlgorithmConfig{Name: "random"}, AttemptMin: 1, AttemptMax: 1,
		TimeBudget: Duration(5 * time.Second), ShutdownGracePeriod: Duration(100 * time.Millisecond), StopOnFailure: true}
	if err := WriteJSONAtomic(filepath.Join(dir, "job.json"), job); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "helper-mode"), []byte(mode), 0o600); err != nil {
		t.Fatal(err)
	}
	return jobSpec{directory: dir, job: job}
}

func fixturePool() *workerPool {
	return newWorkerPool(Config{WorkerBinary: os.Args[0], Parallelism: 1, ShutdownGracePeriod: 100 * time.Millisecond})
}

func TestSupervisorRequiresMatchingTerminalArtifacts(t *testing.T) {
	for _, test := range []struct {
		mode string
		want string
	}{
		{"normal", ""}, {"failure", ""}, {"continued-failure", ""}, {"resource-limit", ""}, {"interrupted", ""},
		{"missing", "summary.json"}, {"malformed", "decode"}, {"wrong-job", "does not match"},
		{"unknown", "terminal reason"}, {"counts", "total"}, {"gap", "run order"},
		{"unfinished", "unfinished"}, {"worker-error", "fixture persistence failure"},
		{"exit-one", "exit status 1"}, {"exit-124", "exit status 124"}, {"signaled", "signal"},
	} {
		t.Run(test.mode, func(t *testing.T) {
			err := fixturePool().runJob(fixtureJob(t, test.mode))
			if test.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want error containing %q", err, test.want)
			}
		})
	}
}

func TestSupervisorStopsDescendants(t *testing.T) {
	for _, mode := range []string{"descendant-exit", "descendant-timeout", "descendant-graceful"} {
		t.Run(mode, func(t *testing.T) {
			spec := fixtureJob(t, mode)
			if mode == "descendant-timeout" {
				spec.job.TimeBudget = Duration(300 * time.Millisecond)
				if err := WriteJSONAtomic(filepath.Join(spec.directory, "job.json"), spec.job); err != nil {
					t.Fatal(err)
				}
			}
			pool := fixturePool()
			defer pool.signalActive(os.Kill)
			result := make(chan error, 1)
			go func() { result <- pool.runJob(spec) }()
			if mode == "descendant-graceful" {
				waitForFile(t, filepath.Join(spec.directory, "ready"))
				pool.stopAndSignal(syscall.SIGTERM)
			}
			err := <-result
			if mode == "descendant-timeout" {
				if !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatalf("got %v, want deadline error", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			assertHeartbeatStopped(t, filepath.Join(spec.directory, "heartbeat"))
		})
	}
}

func TestSupervisorStartAndStopAreAtomic(t *testing.T) {
	for iteration := range 24 {
		pool := fixturePool()
		cmd := exec.Command("/bin/sh", "-c", "exec sleep 10")
		if err := configureWorkerProcess(cmd); err != nil {
			t.Fatal(err)
		}
		var started error
		if iteration%2 == 0 {
			// Exercise the registered-before-stop side of the same boundary.
			started = pool.start(cmd)
			pool.stopAndSignal(syscall.SIGTERM)
		} else {
			var work sync.WaitGroup
			work.Add(2)
			go func() { defer work.Done(); started = pool.start(cmd) }()
			go func() { defer work.Done(); pool.stopAndSignal(syscall.SIGTERM) }()
			work.Wait()
		}
		if started == nil {
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				pool.signalActive(os.Kill)
				<-done
				t.Fatal("worker started concurrently with stop and escaped its signal")
			}
			pool.finish(cmd)
		} else if !errors.Is(started, errPoolStopping) {
			t.Fatal(started)
		}
		after := exec.Command("/bin/sh", "-c", "exit 0")
		if err := pool.start(after); !errors.Is(err, errPoolStopping) {
			t.Fatalf("start after stop: %v", err)
		}
	}
}

func TestSupervisorEscalatesSignalsToWholeGroup(t *testing.T) {
	for _, mode := range []string{"descendant-grace-expiry", "descendant-second-signal"} {
		t.Run(mode, func(t *testing.T) {
			spec := fixtureJob(t, mode)
			pool := fixturePool()
			signals, done, exited := make(chan os.Signal, 2), make(chan struct{}), make(chan struct{})
			go func() { defer close(exited); pool.handleSignals(signals, done) }()
			defer func() { close(done); <-exited; pool.signalActive(os.Kill) }()
			result := make(chan error, 1)
			go func() { result <- pool.runJob(spec) }()
			waitForFile(t, filepath.Join(spec.directory, "ready"))
			signals <- os.Interrupt
			<-pool.stopping
			if mode == "descendant-second-signal" {
				signals <- os.Interrupt
			}
			select {
			case err := <-result:
				if err == nil || !strings.Contains(err.Error(), "signal") {
					t.Fatalf("forced signal was not classified as worker failure: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("signal escalation did not stop worker")
			}
			assertHeartbeatStopped(t, filepath.Join(spec.directory, "heartbeat"))
		})
	}
}

func TestSupervisorSignalHandlerLifetime(t *testing.T) {
	for _, stopFirst := range []bool{false, true} {
		pool := fixturePool()
		signals, done, exited := make(chan os.Signal, 2), make(chan struct{}), make(chan struct{})
		go func() { defer close(exited); pool.handleSignals(signals, done) }()
		if stopFirst {
			signals <- os.Interrupt
			<-pool.stopping
		}
		close(done)
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Fatal("signal handler remained alive after supervisor completion")
		}
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func assertHeartbeatStopped(t *testing.T, path string) {
	t.Helper()
	time.Sleep(60 * time.Millisecond)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("descendant still writing after worker exit: %q -> %q", before, after)
	}
}
