package orchestrator

import (
	"fmt"
	"github.com/shubhaankar/synctest/campaign"
)

// ArtifactSink adapts the checked campaign store; legacy metrics observers are
// optional callbacks, not a persistence or replay contract.
type ArtifactSink struct {
	Store        *campaign.Store
	MaxDecisions int
}

func (s *ArtifactSink) StartTrial(n int) error {
	return s.Store.Append("start", n, struct {
		ReplayVersion int `json:"replay_version"`
	}{ReplayVersion})
}
func (s *ArtifactSink) EndTrial(n int, rr RunResult) error {
	if s.MaxDecisions <= 0 || len(rr.Trace) > s.MaxDecisions {
		return ResourceLimitError{Resource: "artifact decisions", Limit: s.MaxDecisions}
	}
	cause := ""
	if rr.Cause != nil {
		cause = fmt.Sprint(rr.Cause)
	}
	return s.Store.Append("terminal", n, struct {
		Replay        UnifiedReplay `json:"replay"`
		Cause         string        `json:"cause,omitempty"`
		WorkerUnsafe  bool          `json:"worker_unsafe"`
		Complete      bool          `json:"complete"`
		TeardownError string        `json:"teardown_error,omitempty"`
		GlobalTrace   []GlobalStep  `json:"global_trace"`
	}{UnifiedReplay{Version: ReplayVersion, Trace: rr.Trace, Terminal: rr.Outcome, SelectControl: rr.SelectControlled, TerminalCause: cause, GlobalTrace: rr.GlobalTrace}, cause, rr.WorkerUnsafe, !rr.WorkerUnsafe && rr.TeardownError == nil && (rr.Outcome == OutcomePassed || rr.Outcome == OutcomeAssertion), causeDiagnostic(rr.TeardownError), rr.GlobalTrace})
}
