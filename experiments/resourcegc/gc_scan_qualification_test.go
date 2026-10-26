package resourcegc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

const (
	scanQualificationActorEnter uint32 = 0xB001
	scanQualificationActorExit  uint32 = 0xB002
	scanQualificationPeer       uint32 = 0xB003
)

type scanQualificationResult struct {
	Scope                  string              `json:"scope"`
	Status                 string              `json:"status"`
	Cause                  string              `json:"cause,omitempty"`
	WorkerUnsafe           bool                `json:"worker_unsafe"`
	Chunks                 int                 `json:"chunks"`
	RequestedCollections   int                 `json:"requested_collections"`
	CompletedCollections   int                 `json:"completed_collections"`
	ForcedGCDelta          uint64              `json:"forced_gc_delta"`
	AutomaticGCDelta       uint64              `json:"automatic_gc_delta"`
	TotalGCDelta           uint64              `json:"total_gc_delta"`
	ActorBGID              uint32              `json:"actor_bgid"`
	PeerWhileActorActive   bool                `json:"peer_while_actor_active"`
	StoppedWorkScans       int                 `json:"stopped_work_scans"`
	ActorNormalPreemptions int                 `json:"actor_normal_preemptions"`
	ActorChecksum          uint64              `json:"actor_checksum"`
	ObservationOverflow    bool                `json:"observation_overflow"`
	Timeline               []TimelineEvent     `json:"timeline"`
	RuntimeTrace           []synctest.Decision `json:"runtime_trace"`
}

// TestGCScanRunningQualification executes one finite, process-isolated probe.
// Invoke only this test with -count=1 and an external process-group watchdog.
// This is not the resource-lifecycle application and does not assert replay or
// neutrality merely because a forced collection completed. A missing actual
// collector-stopped work-frame scan is explicitly inconclusive.
func TestGCScanRunningQualification(t *testing.T) {
	path := os.Getenv("RESOURCEGC_SCAN_RESULT")
	if path == "" {
		t.Skip("one-shot scan qualification requires RESOURCEGC_SCAN_RESULT")
	}
	if !filepath.IsAbs(path) {
		t.Fatal("RESOURCEGC_SCAN_RESULT must be an absolute fresh path")
	}
	chunks, err := scanQualificationInt("RESOURCEGC_SCAN_CHUNKS", 65536, 1024, 1048576)
	if err != nil {
		t.Fatal(err)
	}
	collections, err := scanQualificationInt("RESOURCEGC_SCAN_COLLECTIONS", 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// One bubble P and one host/scanner P. No subsequent trial or restoration
	// workload is run in this process, particularly after WorkerUnsafe.
	runtime.GOMAXPROCS(2)
	debug.SetGCPercent(-1)
	debug.SetMemoryLimit(1<<63 - 1)
	trigger, driverDone := make(chan struct{}), make(chan struct{})
	completed := 0
	go func() {
		<-trigger
		for i := 0; i < collections; i++ {
			runtime.GC()
			completed++
		}
		close(driverDone)
	}()
	samples := []metrics.Sample{
		{Name: "/gc/cycles/forced:gc-cycles"},
		{Name: "/gc/cycles/automatic:gc-cycles"},
		{Name: "/gc/cycles/total:gc-cycles"},
	}
	metrics.Read(samples)
	for _, sample := range samples {
		if sample.Value.Kind() != metrics.KindUint64 {
			t.Fatalf("unsupported metric %s", sample.Name)
		}
	}
	runtime.GC() // Identical preparation; excluded from the measured interval.
	metrics.Read(samples)
	before := [3]uint64{samples[0].Value.Uint64(), samples[1].Value.Uint64(), samples[2].Value.Uint64()}

	result := scanQualificationResult{
		Scope:  "finite running-actor stack-scan qualification; distinct from the resource application; peer activity alone does not prove a scan caused it",
		Chunks: chunks, RequestedCollections: collections,
	}
	var observations []synctest.Observation
	var active, peerDuring atomic.Bool
	var checksum uint64
	r := synctest.ExploreResult(t, func(*testing.T) {
		synctest.EnableObservations()
		peerStart := make(chan struct{})
		actorDone, peerDone := make(chan struct{}), make(chan struct{})
		go func() {
			<-peerStart
			peerDuring.Store(active.Load())
			synctest.RecordObservation(scanQualificationPeer)
			close(peerDone)
		}()
		go func() {
			checksum = scanQualificationActor(chunks, &active, peerStart, trigger)
			close(actorDone)
		}()
		<-actorDone
		<-peerDone
		// Foreign driver join is only after both application actors finish.
		synctest.External(func() { <-driverDone })
		observations, result.ObservationOverflow = synctest.Observations()
	}, nil)

	metrics.Read(samples)
	result.ForcedGCDelta = samples[0].Value.Uint64() - before[0]
	result.AutomaticGCDelta = samples[1].Value.Uint64() - before[1]
	result.TotalGCDelta = samples[2].Value.Uint64() - before[2]
	result.RuntimeTrace, result.WorkerUnsafe = r.Trace, r.WorkerUnsafe
	result.Timeline = symbolizeObservations(observations)
	if r.OK && !r.WorkerUnsafe {
		// Driver writes and actor result are read only after joined completion.
		result.CompletedCollections = completed
		result.PeerWhileActorActive = peerDuring.Load()
		result.ActorChecksum = checksum
	}
	for _, event := range result.Timeline {
		if event.Kind == "call" && event.Tag == scanQualificationActorEnter {
			result.ActorBGID = event.BGID
			break
		}
	}
	for _, event := range result.Timeline {
		if event.BGID != result.ActorBGID {
			continue
		}
		if event.Kind == "preempt" && !event.GCWaiting {
			result.ActorNormalPreemptions++
		}
		if event.Kind != "gc_scan_begin" || !event.Stopped || event.SelfScan {
			continue
		}
		for _, frame := range event.Stack {
			if strings.HasSuffix(frame.Function, ".scanQualificationChunk") || strings.HasSuffix(frame.Function, ".scanQualificationFold") {
				result.StoppedWorkScans++
				break
			}
		}
	}
	switch {
	case r.WorkerUnsafe || !r.OK:
		result.Status, result.Cause = "runtime_failure", fmt.Sprint(r.Cause)
	case result.ObservationOverflow:
		result.Status = "observation_incomplete"
	case result.ForcedGCDelta != uint64(collections) || result.AutomaticGCDelta != 0 || result.TotalGCDelta != uint64(collections):
		result.Status = "gc_count_mismatch"
	case collections == 0:
		result.Status = "completed_zero_gc_control"
	case result.StoppedWorkScans == 0:
		result.Status = "running_work_scan_not_observed"
	default:
		result.Status = "running_work_scan_observed"
	}
	if err := json.NewEncoder(file).Encode(result); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	t.Logf("status=%s stopped_work_scans=%d peer_while_actor_active=%v normal_preemptions=%d", result.Status, result.StoppedWorkScans, result.PeerWhileActorActive, result.ActorNormalPreemptions)
}

func scanQualificationInt(name string, fallback, minimum, maximum int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < minimum || n > maximum {
		return 0, fmt.Errorf("%s must be an integer in [%d,%d]", name, minimum, maximum)
	}
	return n, nil
}

//go:noinline
func scanQualificationActor(chunks int, active *atomic.Bool, peerStart, trigger chan struct{}) uint64 {
	synctest.RecordObservation(scanQualificationActorEnter)
	active.Store(true)
	close(peerStart)
	close(trigger)
	value := uint64(1)
	for i := 0; i < chunks; i++ {
		value = scanQualificationChunk(value)
	}
	active.Store(false)
	synctest.RecordObservation(scanQualificationActorExit)
	return value
}

// The fixed stack array and non-inlined fold preserve a real function-entry
// stack check for cooperative preemption. No per-chunk allocations, channel
// operations, deliberate yields, sleeps or application observations are added.
//
//go:noinline
func scanQualificationChunk(seed uint64) uint64 {
	var scratch [64]uint64
	for i := range scratch {
		seed = seed*6364136223846793005 + uint64(i) + 1
		scratch[i] = seed
	}
	return scanQualificationFold(&scratch, seed)
}

//go:noinline
func scanQualificationFold(scratch *[64]uint64, value uint64) uint64 {
	for i := range scratch {
		value ^= scratch[i] + uint64(i)
	}
	return value
}
