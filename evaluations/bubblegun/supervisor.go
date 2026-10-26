package bubblegun

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
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
	go pool.handleSignals(signals)
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

	if err := command.Start(); err != nil {
		return err
	}
	p.track(command, true)
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	attempts := spec.job.AttemptMax - spec.job.AttemptMin + 1
	deadline := time.NewTimer(time.Duration(spec.job.TimeBudget)*time.Duration(attempts) + time.Duration(spec.job.ShutdownGracePeriod))
	select {
	case err = <-done:
		deadline.Stop()
	case <-deadline.C:
		_ = command.Process.Kill()
		err = <-done
	}
	p.track(command, false)
	if err == nil {
		return nil
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		if status, ok := exitError.ProcessState.Sys().(syscall.WaitStatus); ok && (status.Signaled() || exitError.ExitCode() == 124) {
			return fmt.Errorf("%s/%s: %w", spec.job.Scenario, spec.job.Algorithm.Name, err)
		}
	}
	var summary Summary
	if summaryErr := readJSON(filepath.Join(spec.directory, "summary.json"), &summary); summaryErr == nil {
		if summary.TerminationReason != TerminationError {
			return nil
		}
		if summary.TerminationDetail != "" {
			return fmt.Errorf("%s/%s: %s", spec.job.Scenario, spec.job.Algorithm.Name, summary.TerminationDetail)
		}
	}
	return fmt.Errorf("%s/%s: %w", spec.job.Scenario, spec.job.Algorithm.Name, err)
}

func (p *workerPool) handleSignals(signals <-chan os.Signal) {
	first := true
	for range signals {
		if first {
			first = false
			p.stopOnce.Do(func() { close(p.stopping) })
			p.signalActive(syscall.SIGTERM)
			time.AfterFunc(p.config.ShutdownGracePeriod, func() { p.signalActive(os.Kill) })
			continue
		}
		p.signalActive(os.Kill)
	}
}

func (p *workerPool) track(command *exec.Cmd, add bool) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	if add {
		p.active[command] = struct{}{}
		return
	}
	delete(p.active, command)
}

func (p *workerPool) signalActive(signal os.Signal) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	for command := range p.active {
		if command.Process != nil {
			_ = command.Process.Signal(signal)
		}
	}
}
