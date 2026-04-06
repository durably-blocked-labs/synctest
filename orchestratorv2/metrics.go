package orchestratorv2

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// RunRecord is the JSON-serializable snapshot of one exploration run.
// Written to a JSONL file by NewJSONLObserver; read by charts/charts.py.
type RunRecord struct {
	RunNum              int             `json:"run_num"`
	NonFIFO             int             `json:"non_fifo"`
	ElapsedNs           int64           `json:"elapsed_ns"`
	LogicalTimeNs       int64           `json:"logical_time_ns"`
	Passed              bool            `json:"passed"`
	GlobalDecisionCount int             `json:"global_decision_count"`
	LocalDecisionTotal  int             `json:"local_decision_total"`
	LocalDecisionByNode map[string]int  `json:"local_decision_by_node,omitempty"`
	QueueSizes          []int           `json:"queue_sizes"`
	TraceFingerprint    string          `json:"trace_fingerprint"`
	DeliverSeq          []DeliverRecord `json:"deliver_seq"`
}

// DeliverRecord captures one message delivery step in a run.
type DeliverRecord struct {
	From      string `json:"from"`
	To        string `json:"to"`
	OpType    string `json:"op_type"`
	Index     int    `json:"index"`
	QueueSize int    `json:"queue_size"`
}

const synctestBaseTimeNs = int64(946684800000000000)

// NewJSONLObserver returns a RunObserver that appends one JSON line per run to w.
// Each line is a RunRecord containing timing, decision counts, queue-size
// history, and the trace fingerprint. Designed for offline chart generation.
//
// Example:
//
//	f, _ := os.Create("explore.jsonl")
//	defer f.Close()
//	orch.Explore(t, setup, GlobalBound(2), WithObserver(NewJSONLObserver(f)))
func NewJSONLObserver(w io.Writer) RunObserver {
	enc := json.NewEncoder(w)
	return func(runNum int, nonFIFO int, elapsed time.Duration, rec RecordedRun, passed bool) {
		r := RunRecord{
			RunNum:              runNum,
			NonFIFO:             nonFIFO,
			ElapsedNs:           elapsed.Nanoseconds(),
			Passed:              passed,
			GlobalDecisionCount: len(rec.GlobalDecisions),
		}

		// Per-node local decision counts.
		r.LocalDecisionByNode = make(map[string]int, len(rec.LocalTraces))
		for addr, decisions := range rec.LocalTraces {
			n := len(decisions)
			r.LocalDecisionByNode[addr] = n
			r.LocalDecisionTotal += n
		}

		// Walk GlobalTrace to extract StepDeliver entries.
		// GlobalDecisions is parallel to StepDeliver entries (one record per delivery).
		dIdx := 0
		var fp strings.Builder
		for _, step := range rec.GlobalTrace {
			if step.Type == StepTimeAdvance && step.Time > synctestBaseTimeNs {
				r.LogicalTimeNs = step.Time - synctestBaseTimeNs
			}
			if step.Type != StepDeliver {
				continue
			}
			var idx, qs int
			if dIdx < len(rec.GlobalDecisions) {
				d := rec.GlobalDecisions[dIdx]
				idx = d.Index
				qs = d.QueueSize
			}
			r.QueueSizes = append(r.QueueSizes, qs)
			r.DeliverSeq = append(r.DeliverSeq, DeliverRecord{
				From:      step.From,
				To:        step.To,
				OpType:    step.OpType,
				Index:     idx,
				QueueSize: qs,
			})
			if fp.Len() > 0 {
				fp.WriteByte(' ')
			}
			fmt.Fprintf(&fp, "%s->%s(%s)", step.From, step.To, step.OpType)
			dIdx++
		}
		r.TraceFingerprint = fp.String()

		enc.Encode(r) //nolint:errcheck
	}
}

// ObserverFromEnv returns a WithObserver option when the METRICS_FILE
// environment variable is set, writing JSONL records to that path.
// Returns a no-op option when METRICS_FILE is unset.
//
// Wire into any Explore call to enable metrics collection via env:
//
//	orch.Explore(t, setup,
//	    GlobalBound(2),
//	    orchestratorv2.ObserverFromEnv(t),
//	)
//
// Then run the test with:
//
//	METRICS_FILE=charts/data/run.jsonl ./go/bin/go test -run TestName ./rafttest/...
func ObserverFromEnv(t *testing.T) ExploreOption {
	path := os.Getenv("METRICS_FILE")
	if path == "" {
		return func(*exploreConfig) {}
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Logf("metrics: failed to create directory %s: %v", dir, err)
			return func(*exploreConfig) {}
		}
	}
	f, err := os.Create(path)
	if err != nil {
		t.Logf("metrics: failed to create %s: %v", path, err)
		return func(*exploreConfig) {}
	}
	t.Cleanup(func() { f.Close() })
	return WithObserver(NewJSONLObserver(f))
}

// BoundFromEnv returns a GlobalBound option when the EXPLORE_K environment
// variable is set to a non-negative integer. Returns a no-op option otherwise.
// Overrides the default bound (or any explicit GlobalBound) set before it.
//
//	orch.Explore(t, setup, GlobalBound(2), orchestratorv2.BoundFromEnv())
//	# EXPLORE_K=1 drives exploration at k=1
func BoundFromEnv() ExploreOption {
	s := os.Getenv("EXPLORE_K")
	if s == "" {
		return func(*exploreConfig) {}
	}
	k, err := strconv.Atoi(s)
	if err != nil || k < 0 {
		return func(*exploreConfig) {}
	}
	return GlobalBound(k)
}

// MaxRunsFromEnv returns a GlobalMaxRuns option when the EXPLORE_MAX_RUNS
// environment variable is set to a positive integer.
func MaxRunsFromEnv() ExploreOption {
	s := os.Getenv("EXPLORE_MAX_RUNS")
	if s == "" {
		return func(*exploreConfig) {}
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return func(*exploreConfig) {}
	}
	return GlobalMaxRuns(n)
}
