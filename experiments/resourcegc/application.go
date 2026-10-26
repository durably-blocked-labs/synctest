// Package resourcegc provides a deliberately faulty resource lifecycle workload.
// Its application actions are observations, not promises of runtime scheduling
// frontiers between adjacent source operations.
package resourcegc

import (
	"errors"
	"sync"
)

// ErrClosed is returned when the worker claims or uses a closed resource.
var ErrClosed = errors.New("resource closed")

// Snapshot is the application state after one observed action. Worker and
// Cleanup name the next action, or "done" when that actor has finished.
type Snapshot struct {
	Worker        string `json:"worker"`
	Cleanup       string `json:"cleanup"`
	InUse         bool   `json:"in_use"`
	Closed        bool   `json:"closed"`
	CloseDecision string `json:"close_decision"`
	Error         string `json:"error"`
	ReadValue     int    `json:"read_value"`
	ComputedValue int    `json:"computed_value"`
	Result        int    `json:"result"`
}

// Event identifies an application action and the resulting state.
type Event struct {
	Action string   `json:"action"`
	State  Snapshot `json:"state"`
}

// Observation records a completed workload without failing its enclosing test.
// WorkerError is empty or "ErrClosed"; an explorer can retain failures and keep
// searching before applying its final assertion outside the campaign.
type Observation struct {
	Events      []Event `json:"events"`
	WorkerError string  `json:"worker_error"`
	Result      int     `json:"result"`
	Completed   bool    `json:"completed"`
}

type resource struct {
	mu       sync.Mutex
	state    Snapshot
	events   []Event
	observer func(uint32)
}

// observe records a call boundary without changing the application event list.
// Callers invoke it outside mu; the optional observer must be concurrency-safe.
func (r *resource) observe(tag uint32) {
	if r.observer != nil {
		r.observer(tag)
	}
}

func newResource() *resource {
	r := &resource{
		state: Snapshot{
			Worker: "claim", Cleanup: "check", CloseDecision: "unset",
			ReadValue: -1, ComputedValue: -1,
		},
		events: make([]Event, 0, 8),
	}
	r.record("initial")
	return r
}

// record is called with mu held, except during private initialization.
func (r *resource) record(action string) {
	r.events = append(r.events, Event{Action: action, State: r.state})
}

func (r *resource) claim() error {
	r.observe(TagClaimEnter)
	defer r.observe(TagClaimReturn)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.Closed {
		r.state.Worker = "done"
		r.state.Error = "ErrClosed"
		r.record("claim:ErrClosed")
		return ErrClosed
	}
	r.state.InUse = true
	r.state.Worker = "read"
	r.record("claim")
	return nil
}

func (r *resource) read() error {
	r.observe(TagReadEnter)
	defer r.observe(TagReadReturn)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.Closed {
		r.state.Worker = "release"
		r.state.Error = "ErrClosed"
		r.record("read:ErrClosed")
		return ErrClosed
	}
	r.state.ReadValue = 21
	r.state.Worker = "compute"
	r.record("read")
	return nil
}

func (r *resource) compute() {
	r.observe(TagComputeEnter)
	defer r.observe(TagComputeReturn)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.ComputedValue = 2 * r.state.ReadValue
	r.state.Worker = "commit"
	r.record("compute")
}

func (r *resource) commit() error {
	r.observe(TagCommitEnter)
	defer r.observe(TagCommitReturn)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.Closed {
		r.state.Worker = "release"
		r.state.Error = "ErrClosed"
		r.record("commit:ErrClosed")
		return ErrClosed
	}
	r.state.Result = r.state.ComputedValue
	r.state.Worker = "release"
	r.record("commit")
	return nil
}

func (r *resource) release() {
	r.observe(TagReleaseEnter)
	defer r.observe(TagReleaseReturn)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.InUse = false
	r.state.Worker = "done"
	r.record("release")
}

// check records a decision that close deliberately fails to validate again.
func (r *resource) check() bool {
	r.observe(TagCheckEnter)
	defer r.observe(TagCheckReturn)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.InUse {
		r.state.CloseDecision = "no"
		r.state.Cleanup = "done"
		r.record("check:skip")
		return false
	}
	r.state.CloseDecision = "yes"
	r.state.Cleanup = "close"
	r.record("check:close")
	return true
}

func (r *resource) close() {
	r.observe(TagCloseEnter)
	defer r.observe(TagCloseReturn)
	r.mu.Lock()
	defer r.mu.Unlock()
	// Injected bug: the earlier unused check may be stale after a worker claim.
	r.state.Closed = true
	r.state.Cleanup = "done"
	r.record("close")
}

func (r *resource) runWorker() error {
	r.observe(TagWorkerEnter)
	defer r.observe(TagWorkerReturn)
	if err := r.claim(); err != nil {
		return err
	}
	defer r.release()
	if err := r.read(); err != nil {
		return err
	}
	r.compute()
	return r.commit()
}

func (r *resource) runCleanup() {
	r.observe(TagCleanupEnter)
	defer r.observe(TagCleanupReturn)
	if r.check() {
		r.close()
	}
}

func (r *resource) observation() Observation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Observation{
		Events: append([]Event(nil), r.events...), WorkerError: r.state.Error,
		Result:    r.state.Result,
		Completed: r.state.Worker == "done" && r.state.Cleanup == "done",
	}
}

// RunApplication starts both actors behind one gate and waits for both to finish.
// It adds no per-action yields or coordination. Call it inside the test bubble;
// collection controls and exploration belong to the enclosing harness.
func RunApplication() Observation {
	return RunApplicationWithObserver(nil)
}

// RunApplicationWithObserver also marks actor and operation entry/return with
// stable tags. The callback runs in the observed actor, outside the resource
// mutex. It must be concurrency-safe and should not allocate, block, yield, or
// perform I/O. Instrumentation itself may affect execution; it adds no explicit
// scheduling points. A return marker is emitted on both success and error.
func RunApplicationWithObserver(observer func(tag uint32)) Observation {
	r := newResource()
	r.observer = observer
	start := make(chan struct{})
	var actors sync.WaitGroup
	actors.Add(2)
	go func() {
		defer actors.Done()
		<-start
		_ = r.runWorker() // The returned error is also captured in the state.
	}()
	go func() {
		defer actors.Done()
		<-start
		r.runCleanup()
	}()
	close(start)
	actors.Wait()
	return r.observation()
}
