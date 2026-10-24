package bubblegun

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

type jobSpec struct {
	directory string
	job       Job
}

func prepareJobs(config Config, registry *Registry) ([]jobSpec, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	jobs := make([]jobSpec, 0, len(config.Scenarios)*len(config.Algorithms))
	for _, scenarioName := range config.Scenarios {
		scenario, ok := registry.Scenario(scenarioName)
		if !ok {
			return nil, fmt.Errorf("scenario %q is not registered", scenarioName)
		}
		attempts, err := generateAttempts(scenario, config.AttemptMin, config.AttemptMax)
		if err != nil {
			return nil, err
		}
		for _, algorithm := range config.Algorithms {
			if scenario.GlobalDecisionsOnly {
				algorithm.GlobalOnly = true
			}
			directory := filepath.Join(config.Output, filepath.FromSlash(scenarioName), string(algorithm.Name))
			job := Job{
				SchemaVersion: 1,
				Scenario:      scenarioName,
				Algorithm:     algorithm,
				AttemptMin:    config.AttemptMin,
				AttemptMax:    config.AttemptMax,
				TimeBudget:    Duration(config.TimeBudget),
				GracePeriod:   Duration(config.GracePeriod),
				StopOnFailure: config.StopOnFailure,
				RetainTraces:  config.RetainTraces,
			}
			if err := prepareJobDirectory(directory, job, attempts, config.Resume); err != nil {
				return nil, err
			}
			jobs = append(jobs, jobSpec{directory: directory, job: job})
		}
	}
	return jobs, nil
}

func generateAttempts(scenario Scenario, min, max int) ([]Attempt, error) {
	attempts := make([]Attempt, 0, max-min+1)
	for number := min; number <= max; number++ {
		input, err := scenario.Generate(number)
		if err != nil {
			return nil, fmt.Errorf("generate %s attempt %d: %w", scenario.Name, number, err)
		}
		attempts = append(attempts, Attempt{Number: number, Seed: int64(number), Input: input})
	}
	return attempts, nil
}

func validateConfig(config Config) error {
	if len(config.Scenarios) == 0 || len(config.Algorithms) == 0 {
		return fmt.Errorf("at least one scenario and algorithm are required")
	}
	if config.AttemptMin < 1 || config.AttemptMax < config.AttemptMin {
		return fmt.Errorf("invalid attempt range %d..%d", config.AttemptMin, config.AttemptMax)
	}
	if config.TimeBudget <= 0 || config.Parallelism < 1 || config.GracePeriod <= 0 {
		return fmt.Errorf("duration, parallelism, and grace period must be positive")
	}
	if config.Output == "" || config.WorkerBinary == "" {
		return fmt.Errorf("output and worker binary are required")
	}
	for _, algorithm := range config.Algorithms {
		if _, err := newAlgorithm(algorithm, 1); err != nil {
			return err
		}
	}
	if config.RetainTraces != RetainFailureTraces && config.RetainTraces != RetainAllTraces {
		return fmt.Errorf("unknown trace retention %q", config.RetainTraces)
	}
	return nil
}

func prepareJobDirectory(directory string, job Job, attempts []Attempt, resume bool) error {
	if resume {
		var existingJob Job
		if err := readJSON(filepath.Join(directory, "job.json"), &existingJob); err != nil {
			return fmt.Errorf("resume %s: %w", directory, err)
		}
		if !reflect.DeepEqual(existingJob, job) {
			return fmt.Errorf("resume configuration does not match %s", directory)
		}
		var existingAttempts []Attempt
		if err := readJSON(filepath.Join(directory, "attempts.json"), &existingAttempts); err != nil {
			return fmt.Errorf("resume %s: %w", directory, err)
		}
		if !reflect.DeepEqual(existingAttempts, attempts) {
			return fmt.Errorf("resume attempts do not match %s", directory)
		}
		return nil
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create job directory: %w", err)
	}
	if _, err := os.Stat(filepath.Join(directory, "runs.jsonl")); err == nil {
		return fmt.Errorf("results already exist in %s", directory)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := WriteJSONAtomic(filepath.Join(directory, "job.json"), job); err != nil {
		return err
	}
	return WriteJSONAtomic(filepath.Join(directory, "attempts.json"), attempts)
}
