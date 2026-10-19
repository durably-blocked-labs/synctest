package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/shubhaankar/synctest/evaluations/framework"
	rafter "github.com/shubhaankar/synctest/evaluations/raft"
)

type stringList []string

func (values *stringList) String() string {
	return strings.Join(*values, ",")
}

func (values *stringList) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func main() {
	var scenarios stringList
	var algorithms stringList
	flag.Var(&scenarios, "scenario", "scenario to run (repeatable)")
	flag.Var(&algorithms, "algorithm", "algorithm to run (repeatable)")
	attemptMin := flag.Int("attempt-min", 1, "first attempt number")
	attemptMax := flag.Int("attempt-max", 1, "last attempt number, inclusive")
	parallel := flag.Int("parallel", runtime.NumCPU(), "maximum concurrent workers")
	duration := flag.Duration("duration", 0, "wall-clock budget per attempt")
	bound := flag.Int("bound", 2, "CHESS and DPOR context bound")
	output := flag.String("output", "results", "campaign output directory")
	resume := flag.Bool("resume", false, "resume interrupted jobs")
	grace := flag.Duration("grace-period", 10*time.Second, "worker shutdown grace period")
	stopOnFailure := flag.Bool("stop-on-failure", false, "stop after first program failure")
	retainTraces := flag.String("retain-traces", "failures", "trace retention: failures or all")
	workerBinary := flag.String("worker-binary", "", "path to synctest evaluation worker")
	flag.Parse()

	registry := framework.NewRegistry()
	if err := rafter.RegisterScenarios(registry); err != nil {
		exitError(err)
	}
	if len(scenarios) == 0 {
		scenarios = registry.ScenarioNames()
	}
	if len(algorithms) == 0 {
		algorithms = stringList{"chess", "dpor", "pct", "random"}
	}
	if *workerBinary == "" {
		executable, err := os.Executable()
		if err != nil {
			exitError(err)
		}
		*workerBinary = filepath.Join(filepath.Dir(executable), "synctest-eval-worker")
	}
	algorithmConfigs := make([]framework.AlgorithmConfig, 0, len(algorithms))
	for _, name := range algorithms {
		config := framework.AlgorithmConfig{Name: name}
		if name == "chess" || name == "dpor" {
			value := *bound
			config.ContextBound = &value
		}
		algorithmConfigs = append(algorithmConfigs, config)
	}

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	err := framework.RunSupervisor(framework.SupervisorConfig{
		Scenarios: scenarios, Algorithms: algorithmConfigs,
		AttemptMin: *attemptMin, AttemptMax: *attemptMax,
		TimeBudget: *duration, Parallelism: *parallel, Output: *output,
		WorkerBinary: *workerBinary, GracePeriod: *grace,
		StopOnFailure: *stopOnFailure, RetainTraces: *retainTraces,
		Resume: *resume, Signals: signals,
	}, registry)
	if err != nil {
		exitError(err)
	}
}

func exitError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
