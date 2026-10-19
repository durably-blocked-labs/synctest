package explorer

import (
	"runtime"
	"testing"
	"testing/synctest"

	"github.com/shubhaankar/synctest/orchestrator"
)

// ReplayUnified consumes every local/select event and checks the terminal
// result. It requires identical executable, inputs, environment and reset policy
// (validate the campaign manifest first). WorkerUnsafe requires process exit.
func ReplayUnified(t *testing.T, f func(*testing.T), rec orchestrator.UnifiedReplay) RunResult {
	saved := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(saved)
	rr := RunResult{DivergenceStep: -1, SelectControlled: rec.SelectControl}
	replayer, err := orchestrator.NewStrictReplayer(rec)
	if err != nil {
		rr.Cause = err
		rr.Outcome = orchestrator.OutcomeInfrastructure
		return rr
	}
	decide := func(dp orchestrator.DecisionPoint) int {
		idx := replayer.Decide(dp)
		rr.Trace = append(rr.Trace, orchestrator.RecordDecision(dp, idx))
		return idx
	}
	var selectHook func(synctest.SelectState) int32
	if rec.SelectControl {
		selectHook = func(s synctest.SelectState) int32 {
			dp := orchestrator.SelectDecisionPoint(s, len(rr.Trace), "local")
			return dp.Alts[decide(dp)].CaseIndex
		}
	}
	result := runWithHooks(t, f, func(s BubbleState) int32 {
		dp := orchestrator.DecisionPoint{Kind: orchestrator.Local, Step: len(rr.Trace), Node: "local", Alts: buildAlts(s)}
		return int32(decide(dp))
	}, selectHook)
	rr.Cause = result.Cause
	rr.WorkerUnsafe = result.WorkerUnsafe
	rr.Passed = result.OK
	rr.Outcome = orchestrator.OutcomePassed
	if rr.Cause != nil {
		rr.Outcome = orchestrator.OutcomePanic
		if step, ok := orchestrator.IsReplayDivergence(rr.Cause); ok {
			rr.Diverged = true
			rr.DivergenceStep = step
			rr.Outcome = orchestrator.OutcomeDiverged
		}
	} else if !rr.Passed {
		rr.UserFailed = true
		rr.Outcome = orchestrator.OutcomeAssertion
	}
	if !rr.Diverged && !rr.WorkerUnsafe {
		if err = replayer.Finish(rr); err != nil {
			rr.Passed = false
			rr.Diverged = true
			rr.Outcome = orchestrator.OutcomeDiverged
			rr.Cause = err
			rr.DivergenceStep, _ = orchestrator.IsReplayDivergence(err)
		}
	}
	return rr
}
