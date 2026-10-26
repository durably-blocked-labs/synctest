package resourcegc

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"testing"
	"testing/synctest"

	"github.com/shubhaankar/synctest/explorer"
)

const workerVersion = 1

// Wire protocol stays version1; this records the changed measurement boundary.
const workerHarnessScope = "application-bubble-host-driver-join-v2"
const maxWorkerDecisions = 512
const maxWorkerRunnable = 16

// ExpectedDecision uses bubble-local goroutine identities, never process PCs.
// BubbleState does not expose a portable scheduling-site identity.
type ExpectedDecision struct {
	Step         int      `json:"step"`
	OrderedBGIDs []uint32 `json:"ordered_bgids"`
	ChosenIndex  int32    `json:"chosen_index"`
}

type workerRequest struct {
	Version           int                `json:"version"`
	Mode              string             `json:"mode"`
	Prefix            []ExpectedDecision `json:"prefix"`
	Processors        int                `json:"processors,omitempty"`
	GCDriver          bool               `json:"gc_driver,omitempty"`
	ForcedCollections int                `json:"forced_collections,omitempty"`
	STWProbes         int                `json:"stw_probes,omitempty"`
}

type workerResult struct {
	Version             int                 `json:"version"`
	HarnessScope        string              `json:"harness_scope"`
	Status              string              `json:"status"`
	Completed           bool                `json:"completed"`
	ApplicationBug      bool                `json:"application_bug"`
	Application         Observation         `json:"application"`
	Decisions           []ExpectedDecision  `json:"decisions"`
	UncontrolledSteps   []ExpectedDecision  `json:"uncontrolled_steps"`
	RuntimeTrace        []explorer.Decision `json:"runtime_trace"`
	ForcedGCDelta       uint64              `json:"forced_gc_delta"`
	AutomaticGCDelta    uint64              `json:"automatic_gc_delta"`
	TotalGCDelta        uint64              `json:"total_gc_delta"`
	Cause               string              `json:"cause"`
	CauseClass          string              `json:"cause_class"`
	WorkerUnsafe        bool                `json:"worker_unsafe"`
	PrefixConsumed      int                 `json:"prefix_consumed"`
	Diverged            bool                `json:"diverged"`
	DecisionOverflow    bool                `json:"decision_overflow"`
	Timeline            []TimelineEvent     `json:"timeline"`
	ObservationOverflow bool                `json:"observation_overflow"`
	ObservationScope    string              `json:"observation_scope"`
	Processors          int                 `json:"processors"`
	GCTreatment         *GCTreatmentResult  `json:"gc_treatment,omitempty"`
	PartialTimeline     bool                `json:"partial_timeline,omitempty"`
}

type frontierSnapshot struct {
	step   int
	n      int32
	bgids  [maxWorkerRunnable]uint32
	chosen int32
}

// A single instance is allocated before the target starts. Hook failure paths
// only fill these fields and panic; they do no formatting, I/O or synchronization.
type workerHookError struct {
	reason string
	step   int
}

func (e *workerHookError) Error() string {
	return fmt.Sprintf("scheduling hook %s at decision %d", e.reason, e.step)
}

// TestResourceGCWorker executes exactly one trial in this process. Expected
// application errors are result data, so DFS can continue in another process.
// Runtime/prefix failures are never treated as successful target executions.
func TestResourceGCWorker(t *testing.T) {
	requestPath := os.Getenv("RESOURCEGC_REQUEST")
	resultPath := os.Getenv("RESOURCEGC_RESULT")
	if requestPath == "" && resultPath == "" {
		t.Skip("one-shot worker requires RESOURCEGC_REQUEST and RESOURCEGC_RESULT")
	}
	if !filepath.IsAbs(requestPath) || !filepath.IsAbs(resultPath) || filepath.Clean(requestPath) == filepath.Clean(resultPath) {
		t.Fatal("request and result must be different absolute paths")
	}

	requestFile, err := os.Open(requestPath)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(io.LimitReader(requestFile, 1<<20))
	decoder.DisallowUnknownFields()
	var request workerRequest
	err = decoder.Decode(&request)
	if err == nil {
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			err = fmt.Errorf("request must contain exactly one JSON value")
		}
	}
	closeErr := requestFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if request.Version != workerVersion || (request.Mode != "explore" && request.Mode != "strict") || len(request.Prefix) > maxWorkerDecisions {
		t.Fatal("unsupported worker version, mode or prefix length")
	}
	if request.Processors == 0 {
		request.Processors = 1
	}
	if request.Processors < 1 || request.Processors > 2 || request.ForcedCollections < 0 || request.ForcedCollections > 8 || request.STWProbes < 0 || request.STWProbes > 1 || (request.GCDriver && request.Processors != 2) || ((request.ForcedCollections != 0 || request.STWProbes != 0) && !request.GCDriver) {
		t.Fatal("unsupported processor count or GC treatment")
	}
	for i, expected := range request.Prefix {
		if expected.Step < 0 || expected.Step >= maxWorkerDecisions || (i > 0 && expected.Step <= request.Prefix[i-1].Step) || len(expected.OrderedBGIDs) < 1 || len(expected.OrderedBGIDs) > maxWorkerRunnable || expected.ChosenIndex < 0 || int(expected.ChosenIndex) >= len(expected.OrderedBGIDs) {
			t.Fatalf("invalid decision at prefix position %d", i)
		}
	}
	// Reserve the output before running. An existing artifact is never replaced.
	resultFile, err := os.OpenFile(resultPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer resultFile.Close()

	runtime.GOMAXPROCS(request.Processors)
	debug.SetGCPercent(-1)
	debug.SetMemoryLimit(1<<63 - 1)
	frontiers := make([]frontierSnapshot, maxWorkerDecisions)
	count, consumed := 0, 0
	hookError := &workerHookError{}
	result := workerResult{Version: workerVersion, HarnessScope: workerHarnessScope, Status: "not_started", Processors: request.Processors}
	var treatment *gcTreatment
	if request.GCDriver {
		treatment = newGCTreatment(request.ForcedCollections, request.STWProbes)
	}
	samples := []metrics.Sample{
		{Name: "/gc/cycles/forced:gc-cycles"},
		{Name: "/gc/cycles/automatic:gc-cycles"},
		{Name: "/gc/cycles/total:gc-cycles"},
	}
	metrics.Read(samples) // Warm metric metadata before the common setup GC.
	for _, sample := range samples {
		if sample.Value.Kind() != metrics.KindUint64 {
			t.Fatalf("unsupported metric %s", sample.Name)
		}
	}
	runtime.GC() // Common preparation; deliberately outside the measured interval.
	metrics.Read(samples)
	before := [3]uint64{samples[0].Value.Uint64(), samples[1].Value.Uint64(), samples[2].Value.Uint64()}

	var observations []synctest.Observation
	runtimeResult := explorer.RunWithHookResult(t, func(*testing.T) {
		synctest.EnableObservations()
		if treatment == nil {
			result.Application = RunApplicationWithObserver(synctest.RecordObservation)
		} else {
			result.Application = RunApplicationWithObserver(treatment.observe)
		}
		observations, result.ObservationOverflow = synctest.Observations()
	}, func(state explorer.BubbleState) int32 {
		// Preserve diagnostic evidence before a known hook failure unwinds
		// bubble membership. This copy happens only on an already invalid run.
		defer func() {
			if failure := recover(); failure != nil {
				observations, result.ObservationOverflow = synctest.Observations()
				result.PartialTimeline = true
				panic(failure)
			}
		}()
		if count == len(frontiers) {
			hookError.reason, hookError.step = "decision_capacity", count
			panic(hookError)
		}
		snapshot := &frontiers[count]
		snapshot.step, snapshot.n, snapshot.bgids = int(state.Step), state.RunnableN, state.RunnableBgid
		position := count
		count++
		if state.RunnableN < 1 || state.RunnableN > maxWorkerRunnable {
			hookError.reason, hookError.step = "runnable_capacity", position
			panic(hookError)
		}
		if position < len(request.Prefix) {
			expected := &request.Prefix[position]
			if expected.Step != int(state.Step) || len(expected.OrderedBGIDs) != int(state.RunnableN) {
				hookError.reason, hookError.step = "prefix_cardinality_or_step", position
				panic(hookError)
			}
			for i := 0; i < int(state.RunnableN); i++ {
				if expected.OrderedBGIDs[i] != state.RunnableBgid[i] {
					hookError.reason, hookError.step = "prefix_ordered_identity", position
					panic(hookError)
				}
			}
			snapshot.chosen = expected.ChosenIndex
			consumed++
		} else if request.Mode == "strict" {
			hookError.reason, hookError.step = "strict_extra_decision", position
			panic(hookError)
		}
		return snapshot.chosen
	})
	// The complete application bubble, including shutdown, has returned. Join
	// the host driver here so GC completion cannot add target scheduling choices.
	// Never wait on a survivor after divergence/unsafe retirement; that process
	// is terminal and the parent records its unsuccessful result.
	if treatment != nil && runtimeResult.OK && !runtimeResult.WorkerUnsafe && result.Application.Completed && consumed == len(request.Prefix) {
		joined := treatment.joinHost()
		result.GCTreatment = &joined
	}
	metrics.Read(samples)
	after := [3]uint64{samples[0].Value.Uint64(), samples[1].Value.Uint64(), samples[2].Value.Uint64()}
	result.ForcedGCDelta = after[0] - before[0]
	result.AutomaticGCDelta = after[1] - before[1]
	result.TotalGCDelta = after[2] - before[2]
	result.Timeline = symbolizeObservations(observations)
	result.ObservationScope = "enabled before application initialization; snapshot after joined application actors inside bubble; full raw bubble trace includes shutdown; host GC driver joined afterward outside bubble"
	result.PrefixConsumed = consumed
	result.RuntimeTrace = runtimeResult.Trace
	result.WorkerUnsafe = runtimeResult.WorkerUnsafe
	result.Decisions = make([]ExpectedDecision, count)
	for i, snapshot := range frontiers[:count] {
		n := int(snapshot.n)
		if n < 0 || n > maxWorkerRunnable {
			n = 0
		}
		result.Decisions[i] = ExpectedDecision{Step: snapshot.step, OrderedBGIDs: append([]uint32(nil), snapshot.bgids[:n]...), ChosenIndex: snapshot.chosen}
	}
	// Bubble/test bootstrap can precede hook installation. Preserve these raw
	// records and accept them only if they contain no scheduling alternative.
	// Every hook observation is matched by its native Step, not slice position.
	rawMismatch, uncontrolledBranch := false, false
	byStep := make([]int, len(runtimeResult.Trace))
	for i := range byStep {
		byStep[i] = -1
	}
	for i, snapshot := range frontiers[:count] {
		if snapshot.step < 0 || snapshot.step >= len(byStep) || byStep[snapshot.step] != -1 {
			rawMismatch = true
			continue
		}
		byStep[snapshot.step] = i
	}
	result.UncontrolledSteps = make([]ExpectedDecision, 0, len(runtimeResult.Trace))
	for i, raw := range runtimeResult.Trace {
		if raw.Kind != 0 || int(raw.Step) != i || raw.RunqSize < 1 || raw.RunqSize > maxWorkerRunnable || raw.Index < 0 || raw.Index >= raw.RunqSize || raw.ChosenBgid != raw.RunqBgids[raw.Index] {
			rawMismatch = true
			continue
		}
		if byStep[i] == -1 {
			result.UncontrolledSteps = append(result.UncontrolledSteps, ExpectedDecision{Step: i, OrderedBGIDs: append([]uint32(nil), raw.RunqBgids[:raw.RunqSize]...), ChosenIndex: raw.Index})
			if raw.RunqSize != 1 || raw.Index != 0 {
				uncontrolledBranch = true
			}
			continue
		}
		snapshot := frontiers[byStep[i]]
		if snapshot.n < 1 || snapshot.n > maxWorkerRunnable || snapshot.chosen < 0 || snapshot.chosen >= snapshot.n || raw.RunqSize != snapshot.n || raw.Index != snapshot.chosen || raw.ChosenBgid != snapshot.bgids[snapshot.chosen] {
			rawMismatch = true
			continue
		}
		for j := int32(0); j < snapshot.n; j++ {
			if raw.RunqBgids[j] != snapshot.bgids[j] {
				rawMismatch = true
				break
			}
		}
	}
	result.ApplicationBug = result.Application.WorkerError == "ErrClosed"
	result.Status = "completed"
	result.Completed = true
	switch {
	case runtimeResult.Cause != nil:
		result.Cause = fmt.Sprint(runtimeResult.Cause)
		result.CauseClass = "runtime"
		result.Status, result.Completed = "runtime_error", false
		if cause, ok := runtimeResult.Cause.(*workerHookError); ok {
			result.CauseClass = "hook"
			result.DecisionOverflow = cause.reason == "decision_capacity" || cause.reason == "runnable_capacity"
			result.Diverged = !result.DecisionOverflow
			if result.DecisionOverflow {
				result.Status = "decision_limit"
			} else {
				result.Status = "diverged"
			}
		} else if _, ok := runtimeResult.Cause.(interface{ Limit() string }); ok {
			result.Status, result.CauseClass = "runtime_limit", "runtime_limit"
		}
	case runtimeResult.WorkerUnsafe:
		result.Status, result.Completed, result.CauseClass = "worker_unsafe", false, "runtime"
		result.Cause = "runtime retired worker without a cause"
	case !runtimeResult.OK:
		result.Status, result.Completed, result.CauseClass = "runtime_not_ok", false, "runtime"
		result.Cause = "runtime reported unsuccessful execution without a cause"
	case rawMismatch:
		result.Status, result.Completed, result.Diverged, result.CauseClass = "runtime_trace_mismatch", false, true, "runtime_observation"
		result.Cause = "raw runtime decision record differs from the hook snapshot"
	case uncontrolledBranch:
		result.Status, result.Completed, result.CauseClass = "unsupported_branch_gap", false, "runtime_observation"
		result.Cause = "raw runtime trace contains an alternative outside the scheduling hook"
	case result.ObservationOverflow:
		result.Status, result.Completed, result.CauseClass = "observation_limit", false, "observation"
		result.Cause = "observation ring overflowed or contained unpublished entries"
	case consumed != len(request.Prefix):
		result.Status, result.Completed, result.Diverged, result.CauseClass = "diverged", false, true, "prefix"
		result.Cause = "execution ended before consuming the requested prefix"
	case !result.Application.Completed:
		result.Status, result.Completed, result.CauseClass = "application_incomplete", false, "application"
		result.Cause = "application did not report completion"
	case result.Application.WorkerError != "" && !result.ApplicationBug:
		result.Status, result.Completed, result.CauseClass = "unexpected_application_error", false, "application"
		result.Cause = result.Application.WorkerError
	case result.ForcedGCDelta != uint64(request.ForcedCollections) || result.AutomaticGCDelta != 0 || result.TotalGCDelta != uint64(request.ForcedCollections):
		result.Status, result.Completed, result.CauseClass = "gc_mismatch", false, "gc_treatment"
		result.Cause = "measured collection counts differ from requested treatment"
	case result.ApplicationBug:
		result.Status = "application_bug"
	}
	if err := json.NewEncoder(resultFile).Encode(result); err != nil {
		t.Fatal(err)
	}
	if err := resultFile.Sync(); err != nil {
		t.Fatal(err)
	}
	// No second bubble, retry or cleanup workload is run, including on unsafe
	// outcomes. The parent launches a fresh process for the next DFS prefix.
}
