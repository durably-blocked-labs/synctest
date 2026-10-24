package bubblegun

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/shubhaankar/synctest/orchestrator"
)

type WorkerConfig struct {
	Directory   string
	Resume      bool
	Interrupted <-chan struct{}
	HardStop    func()
}

type traceRecord struct {
	Attempt     int                       `json:"attempt"`
	Run         int                       `json:"run"`
	Result      Result                    `json:"result"`
	Detail      string                    `json:"detail,omitempty"`
	Trace       orchestrator.Trace        `json:"trace"`
	GlobalTrace []orchestrator.GlobalStep `json:"global_trace"`
}

type workerAttempt struct {
	directory      string
	interrupted    <-chan struct{}
	debug          bool
	algorithm      orchestrator.CheckpointableAlgorithm
	checkpointPath string
	runLog         *RunLog
	summary        *Summary
	started        time.Time
	priorElapsed   time.Duration
	attemptNumber  int
	runNumber      int
	deadline       time.Time
}

type explorationWorker struct {
	t                  *testing.T
	config             WorkerConfig
	job                Job
	attempts           map[int]Attempt
	scenario           Scenario
	latestRunByAttempt map[int]int
	runLog             *RunLog
	summary            Summary
	started            time.Time
	priorElapsed       time.Duration
}

func RunWorker(t *testing.T, registry *Registry, config WorkerConfig) (Summary, error) {
	started := time.Now()
	summary := Summary{Totals: make(map[Result]int)}

	job, attempts, err := loadWorkerInputs(config.Directory)
	if err != nil {
		return summary, err
	}
	scenario, err := registry.Scenario(job.Scenario)
	if err != nil {
		return summary, err
	}
	processors := scenario.Processors
	if processors == 0 {
		processors = 4
	}
	runtime.GOMAXPROCS(processors)
	if err := os.MkdirAll(filepath.Join(config.Directory, "traces"), 0o755); err != nil {
		return summary, fmt.Errorf("create trace directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(config.Directory, "checkpoints"), 0o755); err != nil {
		return summary, fmt.Errorf("create checkpoint directory: %w", err)
	}
	latestRunByAttempt, err := loadRunNumbers(filepath.Join(config.Directory, "runs.jsonl"), config.Resume)
	if err != nil {
		return summary, err
	}
	if config.Resume {
		summaryPath := filepath.Join(config.Directory, "summary.json")
		if _, err := os.Stat(summaryPath); err == nil {
			if err := readJSON(summaryPath, &summary); err != nil {
				return summary, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return summary, fmt.Errorf("inspect summary: %w", err)
		}
		if summary.Totals == nil {
			summary.Totals = make(map[Result]int)
		}
	}
	runLog, err := OpenRunLog(filepath.Join(config.Directory, "runs.jsonl"))
	if err != nil {
		return summary, err
	}
	defer runLog.Close()
	worker := explorationWorker{
		t:                  t,
		config:             config,
		job:                job,
		attempts:           attempts,
		scenario:           scenario,
		latestRunByAttempt: latestRunByAttempt,
		runLog:             runLog,
		summary:            summary,
		started:            started,
		priorElapsed:       time.Duration(summary.Elapsed),
	}
	return worker.executeExplorations()
}

func (w *explorationWorker) executeExplorations() (Summary, error) {
	for attemptNumber := w.job.AttemptMin; attemptNumber <= w.job.AttemptMax; attemptNumber++ {
		if w.config.Resume && slices.Contains(w.summary.CompletedAttempts, attemptNumber) {
			continue
		}
		attempt, ok := w.attempts[attemptNumber]
		if !ok {
			return w.summary, fmt.Errorf("attempt %d is missing from attempts.json", attemptNumber)
		}
		setup, err := w.scenario.Setup(attempt.Input)
		if err != nil {
			return w.summary, fmt.Errorf("configure attempt %d: %w", attemptNumber, err)
		}
		algorithm, err := newAlgorithm(w.job.Algorithm, attempt.Seed)
		if err != nil {
			return w.summary, err
		}
		checkpointPath := filepath.Join(w.config.Directory, "checkpoints", fmt.Sprintf("attempt-%06d.json", attemptNumber))
		if w.config.Resume {
			if err := restoreCheckpoint(checkpointPath, algorithm); err != nil {
				return w.summary, fmt.Errorf("restore attempt %d: %w", attemptNumber, err)
			}
		}

		attemptState := &workerAttempt{
			directory:      w.config.Directory,
			interrupted:    w.config.Interrupted,
			debug:          w.job.Debug,
			algorithm:      algorithm,
			checkpointPath: checkpointPath,
			runLog:         w.runLog,
			summary:        &w.summary,
			started:        w.started,
			priorElapsed:   w.priorElapsed,
			attemptNumber:  attemptNumber,
			runNumber:      w.latestRunByAttempt[attemptNumber],
			deadline:       time.Now().Add(time.Duration(w.job.TimeBudget)),
		}
		var watchdog *time.Timer
		if w.config.HardStop != nil {
			watchdog = time.AfterFunc(time.Duration(w.job.TimeBudget+w.job.ShutdownGracePeriod), w.config.HardStop)
		}

		campaignAlgorithm := &workerAlgorithm{algorithm: algorithm, attempt: attemptState}
		var opts []orchestrator.ExploreOption
		if w.scenario.TrialLimits.Decisions != 0 || w.scenario.TrialLimits.PendingOperations != 0 {
			opts = append(opts, orchestrator.WithTrialLimits(w.scenario.TrialLimits))
		}
		if !w.job.StopOnFailure {
			opts = append(opts, orchestrator.ContinueAfterFailure())
		}
		exploration := orchestrator.New().ExploreWith(w.t, setup, campaignAlgorithm, opts...)
		if watchdog != nil {
			watchdog.Stop()
		}
		if campaignAlgorithm.err != nil {
			return finishWorkerError(w.summary, w.config.Directory, w.started, w.priorElapsed, campaignAlgorithm.err)
		}
		if exploration.Incomplete > 0 {
			return finishWorkerError(w.summary, w.config.Directory, w.started, w.priorElapsed,
				fmt.Errorf("exploration incomplete: %v", exploration.Cause))
		}

		w.summary.TerminationReason = campaignAlgorithm.termination
		if w.summary.TerminationReason == "" {
			if w.job.StopOnFailure && exploration.Failed > 0 {
				w.summary.TerminationReason = TerminationFailure
			} else {
				w.summary.TerminationReason = TerminationExhausted
			}
		}
		w.summary.Elapsed = Duration(w.priorElapsed + time.Since(w.started))
		if w.summary.TerminationReason != TerminationInterrupted {
			w.summary.CompletedAttempts = append(w.summary.CompletedAttempts, attemptNumber)
			if w.summary.TerminationReason == TerminationExhausted {
				w.summary.ExhaustedAttempts = append(w.summary.ExhaustedAttempts, attemptNumber)
			}
		}
		if err := WriteJSONAtomic(filepath.Join(w.config.Directory, "summary.json"), w.summary); err != nil {
			return w.summary, err
		}
		if w.summary.TerminationReason == TerminationInterrupted ||
			w.summary.TerminationReason == TerminationFailure {
			return w.summary, nil
		}
	}

	return w.summary, nil
}

func (a *workerAttempt) stopReason() TerminationReason {
	select {
	case <-a.interrupted:
		return TerminationInterrupted
	default:
	}
	if !time.Now().Before(a.deadline) {
		return TerminationTimeLimit
	}
	return ""
}

func (a *workerAttempt) persist(run orchestrator.RunResult) error {
	if run.Diverged {
		return saveCheckpoint(a.checkpointPath, a.algorithm)
	}
	a.runNumber++
	result := workerResult(run)
	detail := runDetail(run)
	tracePath := ""
	if retainTrace(a.debug, result) {
		tracePath = filepath.Join("traces", fmt.Sprintf("attempt-%06d-run-%06d.json", a.attemptNumber, a.runNumber))
		if err := WriteJSONAtomic(filepath.Join(a.directory, tracePath), traceRecord{
			Attempt: a.attemptNumber, Run: a.runNumber, Result: result, Detail: detail,
			Trace: run.Trace, GlobalTrace: run.GlobalTrace,
		}); err != nil {
			return err
		}
	}
	if err := a.runLog.Append(buildRunRecord(a.attemptNumber, a.runNumber, result, tracePath, run)); err != nil {
		return err
	}
	if err := saveCheckpoint(a.checkpointPath, a.algorithm); err != nil {
		return err
	}
	a.summary.TotalRuns++
	a.summary.Totals[result]++
	if result != ResultNoFailure && !hasFinding(a.summary.FirstFindings, a.attemptNumber, result) {
		a.summary.FirstFindings = append(a.summary.FirstFindings, Finding{
			Attempt: a.attemptNumber, Run: a.runNumber, Result: result, Detail: detail,
		})
	}
	a.summary.Elapsed = Duration(a.priorElapsed + time.Since(a.started))
	return WriteJSONAtomic(filepath.Join(a.directory, "summary.json"), a.summary)
}

func finishWorkerError(summary Summary, directory string, started time.Time, priorElapsed time.Duration, cause error) (Summary, error) {
	summary.TerminationReason = TerminationError
	summary.TerminationDetail = cause.Error()
	summary.Elapsed = Duration(priorElapsed + time.Since(started))
	if err := WriteJSONAtomic(filepath.Join(directory, "summary.json"), summary); err != nil {
		return summary, errors.Join(cause, err)
	}
	return summary, cause
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

func loadRunNumbers(path string, resume bool) (map[int]int, error) {
	latestByAttempt := make(map[int]int)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return latestByAttempt, nil
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
		return latestByAttempt, nil
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record RunRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode existing run: %w", err)
		}
		latestByAttempt[record.Attempt] = max(latestByAttempt[record.Attempt], record.Run)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read existing runs: %w", err)
	}
	return latestByAttempt, nil
}

func restoreCheckpoint(path string, algorithm orchestrator.CheckpointableAlgorithm) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
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
	return WriteJSONAtomic(path, json.RawMessage(data))
}

func retainTrace(debug bool, result Result) bool {
	return debug || result != ResultNoFailure
}

func workerResult(run orchestrator.RunResult) Result {
	switch run.Outcome {
	case orchestrator.OutcomeAssertion:
		return ResultProgramFailure
	case orchestrator.OutcomeResourceLimit:
		return ResultProgressBudgetExceeded
	case orchestrator.OutcomeInfrastructure, orchestrator.OutcomePanic,
		orchestrator.OutcomeCancelled, orchestrator.OutcomeUnsupported:
		return ResultFrameworkFailure
	default:
		return ResultNoFailure
	}
}

func buildRunRecord(attempt, runNumber int, result Result, tracePath string, run orchestrator.RunResult) RunRecord {
	record := RunRecord{
		Attempt: attempt, Run: runNumber, Result: result, Detail: runDetail(run),
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

func runDetail(run orchestrator.RunResult) string {
	if run.TeardownError != nil {
		return run.TeardownError.Error()
	}
	if run.PersistenceError != nil {
		return run.PersistenceError.Error()
	}
	if run.Cause != nil {
		return fmt.Sprint(run.Cause)
	}
	return ""
}

type workerAlgorithm struct {
	algorithm   orchestrator.CheckpointableAlgorithm
	attempt     *workerAttempt
	termination TerminationReason
	err         error
}

func (a *workerAlgorithm) BeforeRun() bool {
	if a.err != nil {
		return false
	}
	if reason := a.attempt.stopReason(); reason != "" {
		a.termination = reason
		return false
	}
	return a.algorithm.BeforeRun()
}

func (a *workerAlgorithm) Decide(point orchestrator.DecisionPoint) int {
	return a.algorithm.Decide(point)
}

func (a *workerAlgorithm) AfterRun(result orchestrator.RunResult) {
	a.algorithm.AfterRun(result)
	if err := a.attempt.persist(result); err != nil {
		a.err = err
	}
}

func (a *workerAlgorithm) SearchError() error {
	if bounded, ok := a.algorithm.(interface{ SearchError() error }); ok {
		return bounded.SearchError()
	}
	return nil
}

func (a *workerAlgorithm) ValidatePrefix() error {
	if validator, ok := a.algorithm.(interface{ ValidatePrefix() error }); ok {
		return validator.ValidatePrefix()
	}
	return nil
}
