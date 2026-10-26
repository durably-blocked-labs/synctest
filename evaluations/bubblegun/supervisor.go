package bubblegun

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Scenarios           []string
	Algorithms          []AlgorithmConfig
	AttemptMin          int
	AttemptMax          int
	TimeBudget          time.Duration
	Parallelism         int
	Output              string
	WorkerBinary        string
	ShutdownGracePeriod time.Duration
	StopOnFailure       bool
	Debug               bool
	Resume              bool
}

func Run(config Config, registry *Registry) error {
	jobs, err := prepareJobs(config, registry)
	if err != nil {
		return err
	}

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	pool := newWorkerPool(config)
	done, signalsDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(signalsDone)
		pool.handleSignals(signals, done)
	}()
	defer func() {
		close(done)
		<-signalsDone
	}()
	return pool.run(jobs)
}

type workerPool struct {
	config   Config
	stopping chan struct{}
	stopOnce sync.Once
	activeMu sync.Mutex
	active   map[*exec.Cmd]struct{}
}

func newWorkerPool(config Config) *workerPool {
	return &workerPool{
		config:   config,
		stopping: make(chan struct{}),
		active:   make(map[*exec.Cmd]struct{}),
	}
}

func (p *workerPool) run(jobs []jobSpec) error {
	queue := make(chan jobSpec)
	errorsCh := make(chan error, len(jobs))
	var workers sync.WaitGroup

	for range p.config.Parallelism {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range queue {
				if err := p.runJob(job); err != nil {
					errorsCh <- err
				}
			}
		}()
	}

sendJobs:
	for _, job := range jobs {
		select {
		case <-p.stopping:
			break sendJobs
		case queue <- job:
		}
	}
	close(queue)
	workers.Wait()
	close(errorsCh)

	var workerErrors []error
	for err := range errorsCh {
		workerErrors = append(workerErrors, err)
	}
	return errors.Join(workerErrors...)
}

func (p *workerPool) runJob(spec jobSpec) error {
	attempts := spec.job.AttemptMax - spec.job.AttemptMin + 1
	budget, grace := time.Duration(spec.job.TimeBudget), time.Duration(spec.job.ShutdownGracePeriod)
	if attempts <= 0 || budget <= 0 || grace <= 0 || int64(attempts) > int64((time.Duration(1<<63-1)-grace)/budget) {
		return errors.New("invalid or overflowing worker deadline")
	}
	args := []string{
		"-test.run=^TestBubbleGunWorker$",
		"-test.v",
		"-bubblegun-job=" + spec.directory,
	}
	if p.config.Resume {
		args = append(args, "-bubblegun-resume")
	}

	command := exec.Command(p.config.WorkerBinary, args...)
	command.Env = append(os.Environ(), "GODEBUG=asyncpreemptoff=1,randseednop=0")
	logFile, err := os.OpenFile(filepath.Join(spec.directory, "worker.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer logFile.Close()
	command.Stdout = logFile
	command.Stderr = logFile

	if err := configureWorkerProcess(command); err != nil {
		return err
	}
	if err := p.start(command); err != nil {
		if errors.Is(err, errPoolStopping) {
			return nil
		}
		return err
	}
	defer p.finish(command)
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	deadline := time.NewTimer(budget*time.Duration(attempts) + grace)
	defer deadline.Stop()
	select {
	case err = <-done:
	case <-deadline.C:
		_ = signalWorkerProcess(command, os.Kill)
		waitErr := <-done
		return fmt.Errorf("%s/%s: worker deadline exceeded: %w", spec.job.Scenario, spec.job.Algorithm.Name,
			errors.Join(os.ErrDeadlineExceeded, waitErr))
	}
	if workerAborted(err) {
		return fmt.Errorf("%s/%s: %w", spec.job.Scenario, spec.job.Algorithm.Name, err)
	}
	summary, summaryErr := validateWorkerSummary(spec)
	if summaryErr != nil {
		return fmt.Errorf("%s/%s: %w", spec.job.Scenario, spec.job.Algorithm.Name, errors.Join(err, summaryErr))
	}
	if summary.TerminationReason == TerminationError {
		return fmt.Errorf("%s/%s: worker error: %s", spec.job.Scenario, spec.job.Algorithm.Name, summary.TerminationDetail)
	}
	// The test binary can exit nonzero after a recorded target failure or an
	// intentionally bounded trial. The terminal summary, not exit zero alone,
	// establishes that the worker classified and persisted that outcome.
	if err != nil && summary.Totals[ResultProgramFailure]+summary.Totals[ResultFrameworkFailure]+summary.Totals[ResultProgressBudgetExceeded] == 0 {
		return fmt.Errorf("%s/%s: %w", spec.job.Scenario, spec.job.Algorithm.Name, err)
	}
	return nil
}

func (p *workerPool) handleSignals(signals <-chan os.Signal, done <-chan struct{}) {
	var grace *time.Timer
	var expired <-chan time.Time
	defer func() {
		if grace != nil {
			grace.Stop()
		}
	}()
	for {
		select {
		case <-done:
			return
		case _, ok := <-signals:
			if !ok {
				return
			}
			if grace == nil {
				p.stopAndSignal(syscall.SIGTERM)
				grace = time.NewTimer(p.config.ShutdownGracePeriod)
				expired = grace.C
			} else {
				p.signalActive(os.Kill)
			}
		case <-expired:
			p.signalActive(os.Kill)
			expired = nil
		}
	}
}

var errPoolStopping = errors.New("worker pool stopping")

func (p *workerPool) start(command *exec.Cmd) error {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	select {
	case <-p.stopping:
		return errPoolStopping
	default:
	}
	if err := command.Start(); err != nil {
		return err
	}
	p.active[command] = struct{}{}
	return nil
}

func (p *workerPool) finish(command *exec.Cmd) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	// A worker leader may exit while its descendants are still running.
	_ = signalWorkerProcess(command, os.Kill)
	delete(p.active, command)
}

func (p *workerPool) stopAndSignal(signal os.Signal) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	p.stopOnce.Do(func() { close(p.stopping) })
	p.signalActiveLocked(signal)
}

func (p *workerPool) signalActive(signal os.Signal) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	p.signalActiveLocked(signal)
}

func (p *workerPool) signalActiveLocked(signal os.Signal) {
	for command := range p.active {
		if command.Process != nil {
			_ = signalWorkerProcess(command, signal)
		}
	}
}

// validateWorkerSummary checks the independently written terminal summary
// against this job and its completed-run ledger. This detects inconsistent
// artifacts; it does not make the files a crash-atomic transaction.
func validateWorkerSummary(spec jobSpec) (Summary, error) {
	var summary Summary
	var job Job
	if err := readJSON(filepath.Join(spec.directory, "job.json"), &job); err != nil {
		return summary, err
	}
	if !reflect.DeepEqual(job, spec.job) {
		return summary, errors.New("worker job does not match prepared job")
	}
	if err := readJSON(filepath.Join(spec.directory, "summary.json"), &summary); err != nil {
		return summary, err
	}
	switch summary.TerminationReason {
	case TerminationExhausted, TerminationTimeLimit, TerminationInterrupted,
		TerminationFailure, TerminationProgressBudgetExceeded, TerminationError:
	default:
		return summary, fmt.Errorf("missing or unknown terminal reason %q", summary.TerminationReason)
	}
	if summary.TotalRuns < 0 || summary.Elapsed < 0 {
		return summary, errors.New("invalid negative summary count or elapsed time")
	}
	file, err := os.Open(filepath.Join(spec.directory, "runs.jsonl"))
	if err != nil {
		return summary, fmt.Errorf("read worker run ledger: %w", err)
	}
	defer file.Close()
	totals := make(map[Result]int)
	latest := make(map[int]int)
	findings := make(map[[2]int]Finding)
	for _, finding := range summary.FirstFindings {
		key := [2]int{finding.Attempt, finding.Run}
		if _, duplicate := findings[key]; duplicate || finding.Result == ResultNoFailure {
			return summary, errors.New("summary has invalid first findings")
		}
		findings[key] = finding
	}
	totalRuns := 0
	scanner := bufio.NewScanner(file)
	// Trace fingerprints can exceed Scanner's default 64 KiB limit.
	scanner.Buffer(make([]byte, 4096), 16<<20)
	for scanner.Scan() {
		var record RunRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return summary, fmt.Errorf("decode worker run ledger: %w", err)
		}
		if record.Attempt < job.AttemptMin || record.Attempt > job.AttemptMax || record.Run != latest[record.Attempt]+1 {
			return summary, errors.New("worker run ledger has invalid attempt or run order")
		}
		switch record.Result {
		case ResultNoFailure, ResultProgramFailure, ResultFrameworkFailure, ResultProgressBudgetExceeded:
		default:
			return summary, fmt.Errorf("unknown worker result %q", record.Result)
		}
		latest[record.Attempt] = record.Run
		totals[record.Result]++
		totalRuns++
		key := [2]int{record.Attempt, record.Run}
		if finding, ok := findings[key]; ok {
			if record.Result != finding.Result || record.Detail != finding.Detail {
				return summary, errors.New("summary finding does not match worker run ledger")
			}
			delete(findings, key)
		}
	}
	if err := scanner.Err(); err != nil {
		return summary, fmt.Errorf("read worker run ledger: %w", err)
	}
	if summary.TotalRuns != totalRuns {
		return summary, errors.New("summary total does not match worker run ledger")
	}
	for result, count := range summary.Totals {
		switch result {
		case ResultNoFailure, ResultProgramFailure, ResultFrameworkFailure, ResultProgressBudgetExceeded:
		default:
			return summary, fmt.Errorf("unknown summary result %q", result)
		}
		if count < 0 || count != totals[result] {
			return summary, errors.New("summary result counts do not match worker run ledger")
		}
		delete(totals, result)
	}
	if len(totals) != 0 {
		return summary, errors.New("summary omits worker results")
	}
	completed := make(map[int]bool)
	for _, attempt := range summary.CompletedAttempts {
		if attempt < job.AttemptMin || attempt > job.AttemptMax || completed[attempt] {
			return summary, errors.New("summary has invalid completed attempts")
		}
		completed[attempt] = true
	}
	exhausted := make(map[int]bool)
	for _, attempt := range summary.ExhaustedAttempts {
		if !completed[attempt] || exhausted[attempt] {
			return summary, errors.New("summary has invalid exhausted attempts")
		}
		exhausted[attempt] = true
	}
	if len(findings) != 0 {
		return summary, errors.New("summary finding does not match worker run ledger")
	}
	if summary.TerminationReason == TerminationExhausted || summary.TerminationReason == TerminationTimeLimit {
		if len(completed) != job.AttemptMax-job.AttemptMin+1 {
			return summary, errors.New("terminal summary omits unfinished attempts")
		}
	}
	if summary.TerminationReason == TerminationFailure &&
		(summary.Totals[ResultProgramFailure]+summary.Totals[ResultFrameworkFailure]+summary.Totals[ResultProgressBudgetExceeded] == 0 || !job.StopOnFailure) {
		return summary, errors.New("failure summary has no matching failure or stop-on-failure job")
	}
	return summary, nil
}
