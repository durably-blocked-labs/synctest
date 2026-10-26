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
	"sync/atomic"
	"testing"
	"testing/synctest"
)

type automaticOwnershipResult struct {
	Scope                string              `json:"scope"`
	Mode                 string              `json:"mode"`
	Status               string              `json:"status"`
	Cause                string              `json:"cause,omitempty"`
	WorkerUnsafe         bool                `json:"worker_unsafe"`
	Completed            bool                `json:"completed"`
	ColdGCStartup        bool                `json:"cold_gc_startup"`
	AllocationCount      int                 `json:"allocation_count"`
	AllocatedBytes       int                 `json:"allocated_bytes"`
	InitialTotalGC       uint64              `json:"initial_total_gc"`
	ForcedGCDelta        uint64              `json:"forced_gc_delta"`
	AutomaticGCDelta     uint64              `json:"automatic_gc_delta"`
	TotalGCDelta         uint64              `json:"total_gc_delta"`
	ActorChecksum        uint64              `json:"actor_checksum"`
	PeerWhileActorActive bool                `json:"peer_while_actor_active"`
	ObservationOverflow  bool                `json:"observation_overflow"`
	Timeline             []TimelineEvent     `json:"timeline"`
	RuntimeTrace         []synctest.Decision `json:"runtime_trace"`
}

// TestAutomaticGCOwnershipQualification is an independent one-shot fixture.
// Run only this test, -count=1, with GOGC=off and an external process watchdog.
// It deliberately performs no setup GC: direct-first therefore qualifies cold
// GC-worker creation only when the initial measured total is actually zero.
// This does not modify the resource-lifecycle experiment or claim assist debt
// parking merely because allocations and automatic collections occurred.
func TestAutomaticGCOwnershipQualification(t *testing.T) {
	path := os.Getenv("RESOURCEGC_AUTO_OWNERSHIP_RESULT")
	if path == "" {
		t.Skip("explicit fresh automatic/direct ownership qualification required")
	}
	if !filepath.IsAbs(path) {
		t.Fatal("RESOURCEGC_AUTO_OWNERSHIP_RESULT must be an absolute fresh path")
	}
	mode := os.Getenv("RESOURCEGC_AUTO_OWNERSHIP_MODE")
	if mode == "" {
		mode = "automatic"
	}
	if mode != "automatic" && mode != "direct-first" {
		t.Fatal("RESOURCEGC_AUTO_OWNERSHIP_MODE must be automatic or direct-first")
	}
	allocations := 256
	if raw := os.Getenv("RESOURCEGC_AUTO_OWNERSHIP_ALLOCS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 64 || n > 4096 {
			t.Fatal("RESOURCEGC_AUTO_OWNERSHIP_ALLOCS must be in [64,4096]")
		}
		allocations = n
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	runtime.GOMAXPROCS(2)
	debug.SetGCPercent(-1)
	debug.SetMemoryLimit(1<<63 - 1)
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
	before := [3]uint64{samples[0].Value.Uint64(), samples[1].Value.Uint64(), samples[2].Value.Uint64()}
	result := automaticOwnershipResult{
		Scope: "separate finite allocation/direct-GC owner-return fixture; not resource-application coverage or proof of actual assist parking",
		Mode:  mode, InitialTotalGC: before[2], ColdGCStartup: before[2] == 0,
	}
	var active, peerDuring atomic.Bool
	var observations []synctest.Observation
	var actorCompleted bool
	var checksum uint64
	r := synctest.ExploreResult(t, func(*testing.T) {
		synctest.EnableObservations()
		peerStart := make(chan struct{})
		actorDone, peerDone := make(chan struct{}), make(chan struct{})
		go func() {
			<-peerStart
			peerDuring.Store(active.Load())
			synctest.RecordObservation(0xC003)
			close(peerDone)
		}()
		go func() {
			synctest.RecordObservation(0xC001)
			active.Store(true)
			close(peerStart)
			if mode == "direct-first" {
				// This public call's mark-completion waits remain legitimate
				// scheduling boundaries; peer activity is not itself failure.
				runtime.GC()
			} else {
				debug.SetGCPercent(25)
				checksum = automaticOwnershipAllocate(allocations)
				active.Store(false) // Exclude the explicit configuration wait below.
				// Disable further triggering and wait for current marking;
				// this is an explicit configuration call after allocation.
				debug.SetGCPercent(-1)
			}
			active.Store(false)
			// Loss of bubble membership here is surfaced by the observation
			// API rather than silently treating a host continuation as owned.
			synctest.RecordObservation(0xC002)
			actorCompleted = true
			close(actorDone)
		}()
		<-actorDone
		<-peerDone
		observations, result.ObservationOverflow = synctest.Observations()
	}, nil)
	metrics.Read(samples)
	result.ForcedGCDelta = samples[0].Value.Uint64() - before[0]
	result.AutomaticGCDelta = samples[1].Value.Uint64() - before[1]
	result.TotalGCDelta = samples[2].Value.Uint64() - before[2]
	result.WorkerUnsafe, result.RuntimeTrace = r.WorkerUnsafe, r.Trace
	result.Timeline = symbolizeObservations(observations)
	if r.OK && !r.WorkerUnsafe {
		result.Completed = actorCompleted
		result.ActorChecksum = checksum
		result.PeerWhileActorActive = peerDuring.Load()
	}
	if mode == "automatic" {
		result.AllocationCount, result.AllocatedBytes = allocations, allocations*(64<<10)
	}
	switch {
	case r.WorkerUnsafe || !r.OK:
		result.Status, result.Cause = "runtime_failure", fmt.Sprint(r.Cause)
	case result.ObservationOverflow:
		result.Status = "observation_incomplete"
	case !result.Completed:
		result.Status = "actor_incomplete"
	case mode == "automatic" && result.AutomaticGCDelta == 0:
		result.Status = "automatic_collection_not_observed"
	case mode == "automatic" && result.ForcedGCDelta != 0:
		result.Status = "unexpected_forced_collection"
	case mode == "direct-first" && result.ForcedGCDelta != 1:
		result.Status = "forced_collection_mismatch"
	case mode == "direct-first" && !result.ColdGCStartup:
		result.Status = "completed_direct_gc_but_not_cold_start"
	default:
		result.Status = "completed"
	}
	if err := json.NewEncoder(file).Encode(result); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	t.Logf("mode=%s status=%s cold=%v automatic=%d forced=%d peer_active=%v", mode, result.Status, result.ColdGCStartup, result.AutomaticGCDelta, result.ForcedGCDelta, result.PeerWhileActorActive)
	if result.Status != "completed" {
		t.Fatalf("ownership qualification incomplete: status=%s cause=%s", result.Status, result.Cause)
	}
	if mode == "automatic" && result.PeerWhileActorActive {
		t.Fatal("automatic GC changed the selected actor's turn")
	}

}

//go:noinline
func automaticOwnershipAllocate(count int) uint64 {
	// Fixed retained ring bounds live application buffers to 2 MiB. Total
	// allocation is finite (default16 MiB, configured maximum256 MiB).
	var retained [32][]byte
	var checksum uint64
	for i := 0; i < count; i++ {
		buffer := make([]byte, 64<<10)
		buffer[0], buffer[len(buffer)-1] = byte(i), byte(i>>8)
		checksum += uint64(buffer[0]) + uint64(buffer[len(buffer)-1])
		retained[i%len(retained)] = buffer
	}
	runtime.KeepAlive(retained)
	return checksum
}
