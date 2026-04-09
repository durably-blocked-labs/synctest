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
	"testing/synctest"
	"time"
)

// RunRecord is the JSON-serializable snapshot of one exploration run.
// Written to a JSONL file by NewJSONLObserver; read by charts/charts.py.
type RunRecord struct {
	RunNum                int                              `json:"run_num"`
	NonFIFO               int                              `json:"non_fifo"`
	ElapsedNs             int64                            `json:"elapsed_ns"`
	LogicalTimeNs         int64                            `json:"logical_time_ns"`
	Passed                bool                             `json:"passed"`
	GlobalDecisionCount   int                              `json:"global_decision_count"`
	LocalDecisionTotal    int                              `json:"local_decision_total"`
	LocalDecisionByNode   map[string]int                   `json:"local_decision_by_node,omitempty"`
	LocalTraceTotal       int                              `json:"local_trace_total,omitempty"`
	LocalTraceByNodeCount map[string]int                   `json:"local_trace_by_node_count,omitempty"`
	LocalDecisionCapNode  map[string]bool                  `json:"local_decision_at_cap_by_node,omitempty"`
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

// LocalDecisionRecord is a compact JSON representation of one synctest
// scheduler decision inside a node bubble.
type LocalDecisionRecord struct {
	Step       int32    `json:"step"`
	Index      int32    `json:"index"`
	ChosenBgid uint32   `json:"chosen_bgid"`
	RunqSize   int32    `json:"runq_size"`
	RunqBgids  []uint32 `json:"runq_bgids,omitempty"`
	WaitReason uint8    `json:"wait_reason"`
}

const synctestBaseTimeNs = int64(946684800000000000)
const localDecisionRecordLimit = 512

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

		// Per-node local decision counts. LocalDecision* intentionally counts
		// only meaningful application-level scheduler choices: points where
		// more than one non-root bubble goroutine was runnable. Bgid 0 is the
		// synctest root/control-plane goroutine, not application work.
		r.LocalDecisionByNode = make(map[string]int, len(rec.LocalTraces))
		r.LocalTraceByNodeCount = make(map[string]int, len(rec.LocalTraces))
		r.LocalDecisionCapNode = make(map[string]bool, len(rec.LocalTraces))
		for addr, decisions := range rec.LocalTraces {
			traceLen := len(decisions)
			meaningful := meaningfulLocalDecisionCount(decisions)
			r.LocalTraceByNodeCount[addr] = traceLen
			r.LocalTraceTotal += traceLen
			r.LocalDecisionByNode[addr] = meaningful
			r.LocalDecisionTotal += meaningful
			if traceLen >= localDecisionRecordLimit {
				r.LocalDecisionCapNode[addr] = true
			}
		}
		if len(r.LocalDecisionCapNode) == 0 {
			r.LocalDecisionCapNode = nil
		}
		if includeLocalTrace(passed) {
			r.LocalTraceByNode = make(map[string][]LocalDecisionRecord, len(rec.LocalTraces))
			for addr, decisions := range rec.LocalTraces {
				trace := make([]LocalDecisionRecord, 0, len(decisions))
				for _, d := range decisions {
					n := int(d.RunqSize)
					if n > len(d.RunqBgids) {
						n = len(d.RunqBgids)
					}
					runqBgids := make([]uint32, 0, n)
					for i := 0; i < n; i++ {
						runqBgids = append(runqBgids, d.RunqBgids[i])
					}
					trace = append(trace, LocalDecisionRecord{
						Step:       d.Step,
						Index:      d.Index,
						ChosenBgid: d.ChosenBgid,
						RunqSize:   d.RunqSize,
						RunqBgids:  runqBgids,
						WaitReason: d.WaitReason,
					})
				}
				r.LocalTraceByNode[addr] = trace
			}
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

func meaningfulLocalDecisionCount(decisions []synctest.Decision) int {
	count := 0
	for _, d := range decisions {
		nonRootRunnable := 0
		n := int(d.RunqSize)
		if n > len(d.RunqBgids) {
			n = len(d.RunqBgids)
		}
		for i := 0; i < n; i++ {
			if d.RunqBgids[i] != 0 {
				nonRootRunnable++
			}
		}
		if nonRootRunnable > 1 {
			count++
		}
	}
	return count
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
// environment variable is set, writing JSONL records to that path.
// Returns a no-op option when METRICS_FILE is unset.
//
// Wire into any Explore call to enable metrics collection via env:
//
//	orch.Explore(t, setup,
//	    GlobalBound(2),
//	    orchestrator.ObserverFromEnv(t),
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
//	orch.Explore(t, setup, GlobalBound(2), orchestrator.BoundFromEnv())
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
