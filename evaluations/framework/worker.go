package framework

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shubhaankar/synctest/orchestrator"
)

type WorkerConfig struct {
	Directory   string
	Resume      bool
	Interrupted <-chan struct{}
}

type algorithmParameters struct {
	GlobalOnly         bool `json:"global_only"`
	ConservativeGlobal bool `json:"conservative_global"`
	Depth              int  `json:"depth"`
	MaxSteps           int  `json:"max_steps"`
}

type traceRecord struct {
	Attempt     int                       `json:"attempt"`
	Run         int                       `json:"run"`
	Result      Result                    `json:"result"`
	Detail      string                    `json:"detail,omitempty"`
	Trace       orchestrator.Trace        `json:"trace"`
	GlobalTrace []orchestrator.GlobalStep `json:"global_trace"`
}

func RunWorker(t *testing.T, registry *Registry, config WorkerConfig) (Summary, error) {
	started := time.Now()
	summary := Summary{Totals: make(map[Result]int)}

	job, attempts, err := loadWorkerInputs(config.Directory)
	if err != nil {
		return summary, err
	}
	if job.AttemptMin < 1 || job.AttemptMax < job.AttemptMin {
		return summary, fmt.Errorf("invalid attempt range %d..%d", job.AttemptMin, job.AttemptMax)
	}
	if job.TimeBudget <= 0 {
		return summary, fmt.Errorf("time budget must be positive")
	}
	scenario, ok := registry.Scenario(job.Scenario)
	if !ok {
		return summary, fmt.Errorf("scenario %q is not registered", job.Scenario)
	}
	if err := os.MkdirAll(filepath.Join(config.Directory, "traces"), 0o755); err != nil {
		return summary, fmt.Errorf("create trace directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(config.Directory, "checkpoints"), 0o755); err != nil {
		return summary, fmt.Errorf("create checkpoint directory: %w", err)
	}
	existingRuns, err := loadExistingRuns(filepath.Join(config.Directory, "runs.jsonl"), config.Resume)
	if err != nil {
		return summary, err
	}
	if config.Resume {
		summaryPath := filepath.Join(config.Directory, "summary.json")
		if _, err := os.Stat(summaryPath); err == nil {
			if err := readJSON(summaryPath, &summary); err != nil {
				return summary, err
			}
		} else if !os.IsNotExist(err) {
			return summary, fmt.Errorf("inspect summary: %w", err)
		}
		if summary.Totals == nil {
			summary.Totals = make(map[Result]int)
		}
	}
	priorElapsed := time.Duration(summary.Elapsed)

	runs, err := OpenJSONLWriter(filepath.Join(config.Directory, "runs.jsonl"))
	if err != nil {
		return summary, err
	}
	defer runs.Close()

	for attemptNumber := job.AttemptMin; attemptNumber <= job.AttemptMax; attemptNumber++ {
		if config.Resume && containsInt(summary.CompletedAttempts, attemptNumber) {
			continue
		}
		attempt, ok := attempts[attemptNumber]
		if !ok {
			return summary, fmt.Errorf("attempt %d is missing from attempts.json", attemptNumber)
		}
		setup, err := scenario.Setup(attempt.Input)
		if err != nil {
			return summary, fmt.Errorf("configure attempt %d: %w", attemptNumber, err)
		}
		algorithm, err := newAlgorithm(job.Algorithm, attempt.Seed)
		if err != nil {
			return summary, err
		}
		checkpointPath := filepath.Join(config.Directory, "checkpoints", fmt.Sprintf("attempt-%06d.json", attemptNumber))
		if config.Resume {
			if err := restoreCheckpoint(checkpointPath, algorithm); err != nil {
				return summary, fmt.Errorf("restore attempt %d: %w", attemptNumber, err)
			}
		}

		deadline := time.Now().Add(time.Duration(job.TimeBudget))
		attemptRun := existingRuns[attemptNumber]
		stopCheck := func() (orchestrator.TerminationReason, bool) {
			select {
			case <-config.Interrupted:
				return orchestrator.TerminationInterrupted, true
			default:
			}
			if !time.Now().Before(deadline) {
				return orchestrator.TerminationTimeLimit, true
			}
			return "", false
		}
		persist := func(_ int, run orchestrator.RunResult) error {
			if run.Diverged {
				return saveCheckpoint(checkpointPath, algorithm)
			}
			attemptRun++
			result := workerResult(run)
			tracePath := ""
			if retainTrace(job.RetainTraces, result) {
				tracePath = filepath.Join("traces", fmt.Sprintf("attempt-%06d-run-%06d.json", attemptNumber, attemptRun))
				if err := WriteJSONAtomic(filepath.Join(config.Directory, tracePath), traceRecord{
					Attempt: attemptNumber, Run: attemptRun, Result: result, Detail: run.Detail,
					Trace: run.Trace, GlobalTrace: run.GlobalTrace,
				}); err != nil {
					return err
				}
			}
			record := buildRunRecord(attemptNumber, attemptRun, result, tracePath, run)
			if err := runs.Append(record); err != nil {
				return err
			}
			if err := saveCheckpoint(checkpointPath, algorithm); err != nil {
				return err
			}
			summary.TotalRuns++
			summary.Totals[result]++
			if result != ResultNoFailure && !hasFinding(summary.FirstFindings, attemptNumber, result) {
				summary.FirstFindings = append(summary.FirstFindings, Finding{
					Attempt: attemptNumber, Run: attemptRun, Result: result, Detail: run.Detail,
				})
			}
			summary.Elapsed = Duration(priorElapsed + time.Since(started))
			return WriteJSONAtomic(filepath.Join(config.Directory, "summary.json"), summary)
		}

		opts := []orchestrator.ExploreOption{
			orchestrator.WithStopCheck(stopCheck),
			orchestrator.WithScheduleObserver(persist),
		}
		if !job.StopOnFailure {
			opts = append(opts, orchestrator.ContinueAfterFailure())
		}
		exploration := orchestrator.New().ExploreWith(t, setup, algorithm, opts...)
		if exploration.Err != nil {
			summary.TerminationReason = TerminationInfrastructureError
			summary.TerminationDetail = exploration.Err.Error()
			summary.Elapsed = Duration(priorElapsed + time.Since(started))
			_ = WriteJSONAtomic(filepath.Join(config.Directory, "summary.json"), summary)
			return summary, exploration.Err
		}

		summary.TerminationReason = frameworkTermination(exploration.Termination)
		summary.Elapsed = Duration(priorElapsed + time.Since(started))
		if exploration.Termination != orchestrator.TerminationInterrupted {
			summary.CompletedAttempts = append(summary.CompletedAttempts, attemptNumber)
			if exploration.Termination == orchestrator.TerminationExhausted {
				summary.ExhaustedAttempts = append(summary.ExhaustedAttempts, attemptNumber)
			}
		}
		if err := WriteJSONAtomic(filepath.Join(config.Directory, "summary.json"), summary); err != nil {
			return summary, err
		}
		if exploration.Termination == orchestrator.TerminationInterrupted ||
			exploration.Termination == orchestrator.TerminationFailure {
			return summary, nil
		}
	}

	return summary, nil
}

func loadWorkerInputs(directory string) (Job, map[int]Attempt, error) {
	var job Job
	if err := readJSON(filepath.Join(directory, "job.json"), &job); err != nil {
		return job, nil, err
	}
	var values []Attempt
	if err := readJSON(filepath.Join(directory, "attempts.json"), &values); err != nil {
		return job, nil, err
	}
	attempts := make(map[int]Attempt, len(values))
	for _, attempt := range values {
		attempts[attempt.Number] = attempt
	}
	return job, attempts, nil
}

func readJSON(path string, target any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func loadExistingRuns(path string, resume bool) (map[int]int, error) {
	runs := make(map[int]int)
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return runs, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open existing runs: %w", err)
	}
	defer file.Close()
	if !resume {
		if info, err := file.Stat(); err != nil {
			return nil, fmt.Errorf("inspect existing runs: %w", err)
		} else if info.Size() > 0 {
			return nil, fmt.Errorf("runs already exist; use resume to continue")
		}
		return runs, nil
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record RunRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode existing run: %w", err)
		}
		if record.Run > runs[record.Attempt] {
			runs[record.Attempt] = record.Run
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read existing runs: %w", err)
	}
	return runs, nil
}

func newAlgorithm(config AlgorithmConfig, seed int64) (orchestrator.CheckpointableAlgorithm, error) {
	var parameters algorithmParameters
	if len(config.Parameters) > 0 {
		if err := json.Unmarshal(config.Parameters, &parameters); err != nil {
			return nil, fmt.Errorf("decode %s algorithm parameters: %w", config.Name, err)
		}
	}
	bound := 2
	if config.ContextBound != nil {
		bound = *config.ContextBound
	}
	switch strings.ToLower(config.Name) {
	case "chess":
		return &orchestrator.CHESS{Bound: bound, GlobalOnly: parameters.GlobalOnly}, nil
	case "dpor":
		return &orchestrator.DPOR{
			Bound: bound, GlobalOnly: parameters.GlobalOnly,
			ConservativeGlobal: parameters.ConservativeGlobal,
		}, nil
	case "pct":
		return &orchestrator.PCT{Depth: parameters.Depth, MaxSteps: parameters.MaxSteps, Seed: seed}, nil
	case "random":
		return &orchestrator.Random{Seed: seed}, nil
	default:
		return nil, fmt.Errorf("unknown algorithm %q", config.Name)
	}
}

func restoreCheckpoint(path string, algorithm orchestrator.CheckpointableAlgorithm) error {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return algorithm.UnmarshalCheckpoint(data)
}

func saveCheckpoint(path string, algorithm orchestrator.CheckpointableAlgorithm) error {
	data, err := algorithm.MarshalCheckpoint()
	if err != nil {
		return err
	}
	var checkpoint any
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return err
	}
	return WriteJSONAtomic(path, checkpoint)
}

func retainTrace(mode string, result Result) bool {
	return strings.EqualFold(mode, "all") || result != ResultNoFailure
}

func workerResult(run orchestrator.RunResult) Result {
	switch run.Outcome {
	case orchestrator.RunProgramFailure:
		return ResultProgramFailure
	case orchestrator.RunFrameworkFailure:
		return ResultFrameworkFailure
	case orchestrator.RunProgressBudgetExceeded:
		return ResultProgressBudgetExceeded
	default:
		return ResultNoFailure
	}
}

func buildRunRecord(attempt, runNumber int, result Result, tracePath string, run orchestrator.RunResult) RunRecord {
	record := RunRecord{
		Attempt: attempt, Run: runNumber, Result: result, Detail: run.Detail,
		Elapsed: Duration(run.Elapsed), ReplayTrace: tracePath,
	}
	var fingerprint strings.Builder
	for _, step := range run.Trace {
		if step.Alternatives > 1 {
			record.BranchPoints++
		}
		if step.Kind == orchestrator.Global {
			record.GlobalDecisions++
			if fingerprint.Len() > 0 {
				fingerprint.WriteByte(' ')
			}
			fmt.Fprintf(&fingerprint, "%s->%s(%s)", step.From, step.To, step.MsgType)
		} else {
			record.LocalDecisions++
		}
	}
	record.TotalDecisions = record.GlobalDecisions + record.LocalDecisions
	record.TraceFingerprint = fingerprint.String()
	return record
}

func hasFinding(findings []Finding, attempt int, result Result) bool {
	for _, finding := range findings {
		if finding.Attempt == attempt && finding.Result == result {
			return true
		}
	}
	return false
}

func containsInt(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func frameworkTermination(reason orchestrator.TerminationReason) TerminationReason {
	switch reason {
	case orchestrator.TerminationExhausted:
		return TerminationExhausted
	case orchestrator.TerminationTimeLimit, orchestrator.TerminationRunLimit:
		return TerminationTimeLimit
	case orchestrator.TerminationInterrupted:
		return TerminationInterrupted
	case orchestrator.TerminationFailure:
		return TerminationFailure
	default:
		return TerminationInfrastructureError
	}
}
