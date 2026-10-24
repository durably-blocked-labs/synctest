package bubblegun

import (
	"encoding/json"
	"fmt"
	"time"
)

type Result string

const (
	ResultNoFailure              Result = "no_failure"
	ResultProgramFailure         Result = "program_failure"
	ResultFrameworkFailure       Result = "framework_failure"
	ResultProgressBudgetExceeded Result = "progress_budget_exceeded"
)

type TerminationReason string

const (
	TerminationExhausted           TerminationReason = "exhausted"
	TerminationTimeLimit           TerminationReason = "time_limit"
	TerminationInterrupted         TerminationReason = "interrupted"
	TerminationFailure             TerminationReason = "failure"
	TerminationInfrastructureError TerminationReason = "infrastructure_error"
)

// Duration encodes a time.Duration as a human-readable string.
type Duration time.Duration

func (d Duration) String() string {
	return time.Duration(d).String()
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("decode duration: %w", err)
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", value, err)
	}
	*d = Duration(parsed)
	return nil
}

type Algorithm string

const (
	AlgorithmFIFO   Algorithm = "fifo"
	AlgorithmCHESS  Algorithm = "chess"
	AlgorithmDPOR   Algorithm = "dpor"
	AlgorithmPCT    Algorithm = "pct"
	AlgorithmRandom Algorithm = "random"
)

type AlgorithmConfig struct {
	Name               Algorithm `json:"name"`
	ContextBound       int       `json:"context_bound,omitempty"`
	GlobalOnly         bool      `json:"global_only,omitempty"`
	ConservativeGlobal bool      `json:"conservative_global,omitempty"`
	Depth              int       `json:"depth,omitempty"`
	MaxSteps           int       `json:"max_steps,omitempty"`
}

type TraceRetention string

const (
	RetainFailureTraces TraceRetention = "failures"
	RetainAllTraces     TraceRetention = "all"
)

type Job struct {
	SchemaVersion int             `json:"schema_version"`
	Scenario      string          `json:"scenario"`
	Algorithm     AlgorithmConfig `json:"algorithm"`
	AttemptMin    int             `json:"attempt_min"`
	AttemptMax    int             `json:"attempt_max"`
	TimeBudget    Duration        `json:"time_budget"`
	GracePeriod   Duration        `json:"grace_period"`
	StopOnFailure bool            `json:"stop_on_failure"`
	RetainTraces  TraceRetention  `json:"retain_traces"`
	RepositoryRev string          `json:"repository_revision"`
	RuntimeRev    string          `json:"runtime_revision"`
	RaftRev       string          `json:"raft_revision,omitempty"`
}

type Attempt struct {
	Number int             `json:"number"`
	Seed   int64           `json:"seed"`
	Input  json.RawMessage `json:"input"`
}

type RunRecord struct {
	Attempt          int      `json:"attempt"`
	Run              int      `json:"run"`
	Result           Result   `json:"result"`
	Detail           string   `json:"detail,omitempty"`
	Elapsed          Duration `json:"elapsed"`
	LogicalTime      Duration `json:"logical_time,omitempty"`
	TotalDecisions   int      `json:"total_decisions"`
	GlobalDecisions  int      `json:"global_decisions"`
	LocalDecisions   int      `json:"local_decisions"`
	BranchPoints     int      `json:"branch_points"`
	StateFingerprint string   `json:"state_fingerprint,omitempty"`
	TraceFingerprint string   `json:"trace_fingerprint,omitempty"`
	ReplayTrace      string   `json:"replay_trace,omitempty"`
}

type Finding struct {
	Attempt int    `json:"attempt"`
	Run     int    `json:"run"`
	Result  Result `json:"result"`
	Detail  string `json:"detail,omitempty"`
}

type Summary struct {
	TotalRuns         int               `json:"total_runs"`
	Totals            map[Result]int    `json:"totals"`
	FirstFindings     []Finding         `json:"first_findings,omitempty"`
	CompletedAttempts []int             `json:"completed_attempts,omitempty"`
	ExhaustedAttempts []int             `json:"exhausted_attempts,omitempty"`
	Elapsed           Duration          `json:"elapsed"`
	TerminationReason TerminationReason `json:"termination_reason"`
	TerminationDetail string            `json:"termination_detail,omitempty"`
}
