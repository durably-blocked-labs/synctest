package orchestrator

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
	Policy                string                           `json:"policy"`
	RunNum                int                              `json:"run_num"`
	NonFIFO               int                              `json:"non_fifo"`
	ElapsedNs             int64                            `json:"elapsed_ns"`
	LogicalTimeNs         int64                            `json:"logical_time_ns"`
	Passed                bool                             `json:"passed"`
	TotalDecisions        int                              `json:"total_decisions"`
	GlobalDecisionCount   int                              `json:"global_decision_count"`
	LocalDecisionTotal    int                              `json:"local_decision_total"`
	LocalDecisionByNode   map[string]int                   `json:"local_decision_by_node,omitempty"`
	BranchPoints          int                              `json:"branch_points"`
	LocalTraceTotal       int                              `json:"local_trace_total,omitempty"`
	LocalTraceByNodeCount map[string]int                   `json:"local_trace_by_node_count,omitempty"`
	LocalTraceByNode      map[string][]LocalDecisionRecord `json:"local_trace_by_node,omitempty"`
	QueueSizes            []int                            `json:"queue_sizes"`
	TraceFingerprint      string                           `json:"trace_fingerprint"`
	DeliverSeq            []DeliverRecord                  `json:"deliver_seq"`
}

// DeliverRecord captures one message delivery step in a run.
type DeliverRecord struct {
	From      string `json:"from"`
	To        string `json:"to"`
	OpType    string `json:"op_type"`
	Index     int    `json:"index"`
	QueueSize int    `json:"queue_size"`
}

// LocalDecisionRecord is a compact JSON representation of one local
// scheduling decision.
type LocalDecisionRecord struct {
	Step       int      `json:"step"`
	Index      int32    `json:"index"`
	ChosenBgid uint32   `json:"chosen_bgid"`
	RunqSize   int32    `json:"runq_size"`
	RunqBgids  []uint32 `json:"runq_bgids,omitempty"`
}

const synctestBaseTimeNs = int64(946684800000000000)

// NewJSONLObserver returns a RunObserver that appends one JSON line per run to w.
// policy tags each record (e.g. "chess", "pct", "random"). Pass "" to omit.
func NewJSONLObserver(w io.Writer, policy string) RunObserver {
	enc := json.NewEncoder(w)
	return func(runNum int, nonFIFO int, elapsed time.Duration, rr RunResult, passed bool) {
		r := RunRecord{
			Policy:         policy,
			RunNum:         runNum,
			NonFIFO:        nonFIFO,
			ElapsedNs:      elapsed.Nanoseconds(),
			TotalDecisions: len(rr.Trace),
			Passed:         passed,
		}

		// Count global/local decisions and build per-node breakdown from unified trace.
		r.LocalDecisionByNode = make(map[string]int)
		r.LocalTraceByNodeCount = make(map[string]int)
		localStepIdx := 0
		for _, s := range rr.Trace {
			if s.Alternatives > 1 {
				r.BranchPoints++
			}
			if s.Kind == Global {
				r.GlobalDecisionCount++
			} else {
				r.LocalTraceByNodeCount[s.Node]++
				r.LocalTraceTotal++
				// Count meaningful decisions: >1 non-root goroutine runnable.
				nonRoot := 0
				for _, bg := range s.RunqBGIDs {
					if bg != 0 {
						nonRoot++
					}
				}
				if nonRoot > 1 {
					r.LocalDecisionByNode[s.Node]++
					r.LocalDecisionTotal++
				}
				localStepIdx++
			}
		}

		// Build local trace detail if requested.
		if includeLocalTrace(passed) {
			r.LocalTraceByNode = make(map[string][]LocalDecisionRecord)
			localStep := 0
			for _, s := range rr.Trace {
				if s.Kind != Local {
					continue
				}
				r.LocalTraceByNode[s.Node] = append(r.LocalTraceByNode[s.Node], LocalDecisionRecord{
					Step:       localStep,
					Index:      s.Index,
					ChosenBgid: s.ChosenBGID,
					RunqSize:   s.Alternatives,
					RunqBgids:  s.RunqBGIDs,
				})
				localStep++
			}
		}

		// Build global decisions list from trace for delivery indexing.
		var globalDecisions []struct{ idx, qs int }
		for _, s := range rr.Trace {
			if s.Kind == Global {
				globalDecisions = append(globalDecisions, struct{ idx, qs int }{int(s.Index), int(s.Alternatives)})
			}
		}

		// Walk GlobalTrace to extract delivery events and fingerprint.
		dIdx := 0
		var fp strings.Builder
		for _, step := range rr.GlobalTrace {
			if step.Type == StepTimeAdvance && step.Time > synctestBaseTimeNs {
				r.LogicalTimeNs = step.Time - synctestBaseTimeNs
			}
			if step.Type != StepDeliver {
				continue
			}
			var idx, qs int
			if dIdx < len(globalDecisions) {
				idx = globalDecisions[dIdx].idx
				qs = globalDecisions[dIdx].qs
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

func includeLocalTrace(passed bool) bool {
	switch strings.ToLower(os.Getenv("METRICS_INCLUDE_LOCAL_TRACE")) {
	case "1", "true", "yes", "all":
		return true
	case "fail", "failed", "failure":
		return !passed
	default:
		return false
	}
}

// ObserverFromEnv returns a WithObserver option when the METRICS_FILE
// environment variable is set.
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
	return WithObserver(NewJSONLObserver(f, ""))
}

// BoundFromEnv returns a GlobalBound option from the EXPLORE_K environment variable.
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

// MaxRunsFromEnv returns a GlobalMaxRuns option from EXPLORE_MAX_RUNS.
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
