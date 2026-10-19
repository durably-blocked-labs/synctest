package orchestrator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TrialSink persists start before execution and terminal after stop/join.
// Implementations must retain their first write/sync/close error.
type TrialSink interface {
	StartTrial(int) error
	EndTrial(int, RunResult) error
}

func WithTrialSink(s TrialSink) ExploreOption {
	return func(c *exploreConfig) { c.sink = s; c.strict = true }
}
func WithTrialLimits(l TrialLimits) ExploreOption   { return func(c *exploreConfig) { c.limits = l } }
func WithContext(ctx context.Context) ExploreOption { return func(c *exploreConfig) { c.ctx = ctx } }
func WithSelectChoices() ExploreOption              { return func(c *exploreConfig) { c.selectChoices = true } }

func RequireAcknowledgements() ExploreOption { return func(c *exploreConfig) { c.strict = true } }

func classifyCause(rr *RunResult, cause any) {
	rr.Passed = false
	rr.UserFailed = false
	rr.Cause = cause
	if step, ok := IsReplayDivergence(cause); ok {
		rr.Diverged = true
		rr.DivergenceStep = step
		rr.Outcome = OutcomeDiverged
		return
	}
	rr.Diverged = false
	switch e := cause.(type) {
	case UnsupportedTransportError:
		rr.Outcome = OutcomeUnsupported
	case ResourceLimitError:
		rr.Outcome = OutcomeResourceLimit
	case interface {
		error
		Limit() string
	}:
		rr.Outcome = OutcomeResourceLimit
	case error:
		if e == context.Canceled {
			rr.Outcome = OutcomeCancelled
		} else {
			rr.Outcome = OutcomeInfrastructure
		}
	default:
		rr.Outcome = OutcomePanic
	}
}

// execute is the sole lifecycle owner for every started trial. A recovered
// runtime failure is not evidence that its goroutines were disposed.
func (o *Orchestrator) execute(t *testing.T, decide func(DecisionPoint) int) (rr RunResult) {
	rr.DivergenceStep = -1
	if !WorkerReusable() {
		rr.WorkerUnsafe = true
	}
	parent := o.ctx
	if parent == nil {
		parent = context.Background()
	}
	// Emergency host watchdog only: never a replay mismatch or simulated event.
	watchdog, stopWatchdog := context.WithTimeout(parent, 30*time.Second)
	defer stopWatchdog()
	ctx, cancel := context.WithCancelCause(watchdog)
	o.ctx = ctx
	defer func() {
		if cause := recover(); cause != nil {
			if failure, ok := cause.(nodeFailure); ok {
				c := failure.ctrl
				classifyCause(&rr, c.cause)
				if c.hookCause == nil && rr.Outcome == OutcomeInfrastructure {
					rr.Outcome = OutcomePanic
				}
				if c.hookCause != nil && rr.Outcome == OutcomePanic {
					rr.Outcome = OutcomeInfrastructure
				}
				rr.WorkerUnsafe = true
			} else {
				if failure, ok := cause.(algorithmFailure); ok {
					classifyCause(&rr, failure.cause)
					if rr.Outcome == OutcomePanic {
						rr.Outcome = OutcomeInfrastructure
					}
				} else {
					classifyCause(&rr, cause)
				}
			}
		}
		cancel(nil)
		if err := o.cleanupBubbles(); err != nil {
			rr.TeardownError = err
			rr.WorkerUnsafe = true
			if rr.Cause == nil {
				classifyCause(&rr, err)
			}
		}
		for _, c := range o.nodes {
			if c.started {
				select {
				case <-c.done:
					if c.unsafe {
						rr.WorkerUnsafe = true
					}
				default:
				}
			}
		}
		if rr.TeardownError == nil && !rr.WorkerUnsafe {
			rr.Trace = o.partial
			rr.GlobalTrace = o.trace
		}
		if rr.WorkerUnsafe {
			o.unsafe = true
			RetireWorker()
		}
		o.ctx = parent
	}()
	for _, addr := range o.order {
		if tr, ok := o.nodes[addr].transport.(interface {
			BindTrial(context.Context, func(error))
		}); ok {
			tr.BindTrial(ctx, func(err error) { cancel(err) })
		}
	}
	o.initBubbles()
	rr = o.run(t, decide)
	return rr
}

func (o *Orchestrator) enqueue(op *PendingOp) {
	if op == nil {
		panic(fmt.Errorf("transport published nil operation"))
	}
	if len(op.From)+len(op.To)+len(op.Type)+len(op.Occurrence) > 16384 {
		panic(ResourceLimitError{Resource: "operation identity bytes", Limit: 16384})
	}
	if len(o.schedulable) >= o.limits.PendingOperations {
		panic(ResourceLimitError{Resource: "pending operations", Limit: o.limits.PendingOperations})
	}
	o.schedulable = append(o.schedulable, op)
}

// ReplayGlobalOnly preserves the historical FIFO-local compatibility behavior.
// It is not exact replay; use ReplayUnified for the versioned G+L+select path.
func (o *Orchestrator) ReplayGlobalOnly(t *testing.T, rec RecordedRun) (RecordedRun, bool) {
	return o.Replay(t, rec)
}

func (o *Orchestrator) ReplayUnified(t *testing.T, rec UnifiedReplay) RunResult {
	r, err := NewStrictReplayer(rec)
	if err != nil {
		rr := RunResult{DivergenceStep: -1}
		classifyCause(&rr, err)
		return rr
	}
	for _, addr := range o.order {
		if err := validateTransport(addr, o.nodes[addr].transport); err != nil {
			rr := RunResult{DivergenceStep: -1}
			classifyCause(&rr, err)
			return rr
		}
	}

	previousSelect := o.selectChoices
	o.selectChoices = rec.SelectControl
	defer func() { o.selectChoices = previousSelect }()
	previous := o.strict
	o.strict = true
	defer func() { o.strict = previous }()
	rr := o.execute(t, r.Decide)
	if !rr.Diverged && !rr.WorkerUnsafe && rr.TeardownError == nil {
		if err = r.Finish(rr); err != nil {
			classifyCause(&rr, err)
		}
	}
	return rr
}

func (o *Orchestrator) prepare(setup func(*Orchestrator)) (cause any) {
	defer func() { cause = recover() }()
	o.nodes = make(map[string]*nodeCtrl)
	o.order = nil
	setup(o)
	return nil
}

type nodeFailure struct{ ctrl *nodeCtrl }
type algorithmFailure struct{ cause any }

func globalDecision(decide func(DecisionPoint) int, dp DecisionPoint) (idx int) {
	defer func() {
		if cause := recover(); cause != nil {
			panic(algorithmFailure{cause})
		}
	}()
	idx = decide(dp)
	CheckDecision(dp, idx)
	return idx
}

// recordDecision bounds retained trace metadata before copying alternatives.
// This is a conservative storage estimate, not a Go heap/RSS accounting claim.
func (o *Orchestrator) recordDecision(dp DecisionPoint, idx int) NewStep {
	bytes := 128 + len(dp.Node) + len(dp.Site)
	for _, a := range dp.Alts {
		bytes += 128 + len(a.ID) + len(a.Occurrence) + len(a.From) + len(a.To) + len(a.MsgType)
	}
	if bytes > o.limits.TraceBytes-o.traceBytes {
		panic(ResourceLimitError{Resource: "trace metadata bytes", Limit: o.limits.TraceBytes})
	}
	o.traceBytes += bytes
	return stepForDecision(dp, idx)
}
