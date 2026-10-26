package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/shubhaankar/synctest/evaluations/bubblegun"
	raft "github.com/shubhaankar/synctest/evaluations/raft"
	"github.com/shubhaankar/synctest/evaluations/scenarios"
)

type stringList []string

func (values *stringList) String() string { return strings.Join(*values, ",") }

func (values *stringList) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func main() {
	registry := bubblegun.NewRegistry()
	registerScenarios(registry)
	config, err := collectFlags(registry)
	if err != nil {
		exitError(err)
	}
	if err := bubblegun.Run(config, registry); err != nil {
		exitError(err)
	}
}

func collectFlags(registry *bubblegun.Registry) (bubblegun.Config, error) {
	var scenarioNames stringList
	var algorithmNames stringList
	flag.Var(&scenarioNames, "scenario", "scenario to run (repeatable)")
	flag.Var(&algorithmNames, "algorithm", "algorithm to run (repeatable)")

	config := bubblegun.Config{}
	flag.IntVar(&config.AttemptMin, "attempt-min", 1, "first attempt number")
	flag.IntVar(&config.AttemptMax, "attempt-max", 1, "last attempt number, inclusive")
	flag.IntVar(&config.Parallelism, "parallel", runtime.NumCPU(), "maximum concurrent workers")
	flag.DurationVar(&config.TimeBudget, "duration", 0, "wall-clock budget per attempt")
	flag.StringVar(&config.Output, "output", "results", "campaign output directory")
	flag.BoolVar(&config.Resume, "resume", false, "resume interrupted jobs")
	flag.DurationVar(&config.ShutdownGracePeriod, "shutdown-grace-period", 10*time.Second, "time allowed for worker shutdown before a hard kill")
	flag.BoolVar(&config.StopOnFailure, "stop-on-failure", false, "stop after first program failure")
	flag.BoolVar(&config.Debug, "debug", false, "retain successful traces for debugging")

	bound := flag.Int("bound", 2, "CHESS and DPOR context bound")
	flag.StringVar(&config.WorkerBinary, "worker-binary", "", "path to Bubble Gun worker")
	flag.Parse()

	if len(scenarioNames) == 0 {
		config.Scenarios = registry.ScenarioNames()
	} else {
		config.Scenarios = scenarioNames
	}
	if len(algorithmNames) == 0 {
		algorithmNames = stringList{"chess", "dpor", "pct", "random"}
	}
	for _, name := range algorithmNames {
		algorithm := bubblegun.Algorithm(name)
		var options []bubblegun.AlgorithmOption
		if algorithm == bubblegun.AlgorithmCHESS || algorithm == bubblegun.AlgorithmDPOR {
			options = append(options, bubblegun.WithContextBound(*bound))
		}
		config.Algorithms = append(config.Algorithms, bubblegun.ConfigureAlgorithm(algorithm, options...))
	}

	if config.WorkerBinary == "" {
		executable, err := os.Executable()
		if err != nil {
			return config, err
		}
		config.WorkerBinary = filepath.Join(filepath.Dir(executable), "bubblegun-worker")
	}
	return config, nil
}

func registerScenarios(registry *bubblegun.Registry) {
	raft.RegisterScenarios(registry)
	scenarios.Register(registry)
}

func exitError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
