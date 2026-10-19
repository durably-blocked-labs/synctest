package framework

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
)

type SupervisorConfig struct {
	Scenarios     []string
	Algorithms    []AlgorithmConfig
	AttemptMin    int
	AttemptMax    int
	TimeBudget    time.Duration
	Parallelism   int
	Output        string
	WorkerBinary  string
	GracePeriod   time.Duration
	StopOnFailure bool
	RetainTraces  string
	Resume        bool
	Signals       <-chan os.Signal
}

type supervisorJob struct {
	directory string
	job       Job
	attempts  []Attempt
}

func RunSupervisor(config SupervisorConfig, registry *Registry) error {
	if err := validateSupervisorConfig(config); err != nil {
		return err
	}
	jobs, err := prepareSupervisorJobs(config, registry)
	if err != nil {
		return err
	}

	jobQueue := make(chan supervisorJob)
	stopping := make(chan struct{})
	var stopOnce sync.Once
	var activeMu sync.Mutex
	active := make(map[*exec.Cmd]struct{})
	killActive := func(signal os.Signal) {
		activeMu.Lock()
		defer activeMu.Unlock()
		for command := range active {
			if command.Process != nil {
				_ = command.Process.Signal(signal)
			}
		}
	}

	if config.Signals != nil {
		go func() {
			first := true
			for range config.Signals {
				if first {
					first = false
					stopOnce.Do(func() { close(stopping) })
					killActive(syscall.SIGTERM)
					go func() {
						timer := time.NewTimer(config.GracePeriod)
						defer timer.Stop()
						<-timer.C
						killActive(os.Kill)
					}()
					continue
				}
				killActive(os.Kill)
			}
		}()
	}

	var wait sync.WaitGroup
	var errorsMu sync.Mutex
	var workerErrors []error
	for i := 0; i < config.Parallelism; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for job := range jobQueue {
				command := exec.Command(config.WorkerBinary,
					"-test.run=^TestEvaluationWorker$",
					"-test.v",
					"-evaluation-job="+job.directory,
				)
				if config.Resume {
					command.Args = append(command.Args, "-evaluation-resume")
				}
				command.Env = append(os.Environ(), "GODEBUG="+workerGODEBUG())
				logFile, err := os.OpenFile(filepath.Join(job.directory, "worker.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
				if err != nil {
					errorsMu.Lock()
					workerErrors = append(workerErrors, err)
					errorsMu.Unlock()
					continue
				}
				command.Stdout = logFile
				command.Stderr = logFile
				activeMu.Lock()
				active[command] = struct{}{}
				activeMu.Unlock()
				err = command.Run()
				activeMu.Lock()
				delete(active, command)
				activeMu.Unlock()
				_ = logFile.Close()
				if err != nil {
					if _, summaryErr := os.Stat(filepath.Join(job.directory, "summary.json")); summaryErr != nil {
						errorsMu.Lock()
						workerErrors = append(workerErrors, fmt.Errorf("%s/%s: %w", job.job.Scenario, job.job.Algorithm.Name, err))
						errorsMu.Unlock()
					}
				}
			}
		}()
	}

sendJobs:
	for _, job := range jobs {
		select {
		case <-stopping:
			break sendJobs
		case jobQueue <- job:
		}
	}
	close(jobQueue)
	wait.Wait()
	return errors.Join(workerErrors...)
}

func workerGODEBUG() string {
	settings := make([]string, 0)
	for _, setting := range strings.Split(os.Getenv("GODEBUG"), ",") {
		if setting == "" || strings.HasPrefix(setting, "asyncpreemptoff=") || strings.HasPrefix(setting, "randseednop=") {
			continue
		}
		settings = append(settings, setting)
	}
	settings = append(settings, "asyncpreemptoff=1", "randseednop=0")
	return strings.Join(settings, ",")
}

func validateSupervisorConfig(config SupervisorConfig) error {
	if len(config.Scenarios) == 0 {
		return fmt.Errorf("at least one scenario is required")
	}
	if len(config.Algorithms) == 0 {
		return fmt.Errorf("at least one algorithm is required")
	}
	if config.AttemptMin < 1 || config.AttemptMax < config.AttemptMin {
		return fmt.Errorf("invalid attempt range %d..%d", config.AttemptMin, config.AttemptMax)
	}
	if config.TimeBudget <= 0 {
		return fmt.Errorf("duration must be positive")
	}
	if config.Parallelism < 1 {
		return fmt.Errorf("parallelism must be positive")
	}
	if config.Output == "" || config.WorkerBinary == "" {
		return fmt.Errorf("output and worker binary are required")
	}
	if config.GracePeriod <= 0 {
		return fmt.Errorf("grace period must be positive")
	}
	return nil
}

func prepareSupervisorJobs(config SupervisorConfig, registry *Registry) ([]supervisorJob, error) {
	jobs := make([]supervisorJob, 0, len(config.Scenarios)*len(config.Algorithms))
	for _, scenarioName := range config.Scenarios {
		scenario, ok := registry.Scenario(scenarioName)
		if !ok {
			return nil, fmt.Errorf("scenario %q is not registered", scenarioName)
		}
		attempts := make([]Attempt, 0, config.AttemptMax-config.AttemptMin+1)
		for number := config.AttemptMin; number <= config.AttemptMax; number++ {
			input, err := scenario.Generate(number)
			if err != nil {
				return nil, fmt.Errorf("generate %s attempt %d: %w", scenarioName, number, err)
			}
			attempts = append(attempts, Attempt{Number: number, Seed: int64(number), Input: input})
		}
		for _, algorithm := range config.Algorithms {
			directory := filepath.Join(config.Output, filepath.FromSlash(scenarioName), algorithm.Name)
			job := Job{
				SchemaVersion: 1, Scenario: scenarioName, Algorithm: algorithm,
				AttemptMin: config.AttemptMin, AttemptMax: config.AttemptMax,
				TimeBudget: Duration(config.TimeBudget), GracePeriod: Duration(config.GracePeriod),
				StopOnFailure: config.StopOnFailure, RetainTraces: config.RetainTraces,
			}
			if err := prepareJobDirectory(directory, job, attempts, config.Resume); err != nil {
				return nil, err
			}
			jobs = append(jobs, supervisorJob{directory: directory, job: job, attempts: attempts})
		}
	}
	return jobs, nil
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
