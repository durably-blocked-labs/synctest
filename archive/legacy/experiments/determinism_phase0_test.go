package experiments

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
)

// ============================================================================
// Phase 0.1 — Determinism Verification Tests
//
// These tests attack the foundational determinism claims:
//   T2.1: Record-replay determinism (identical prefix -> identical trace)
//   T2.2: BGID stability (goroutine IDs depend only on creation order)
//
// Run:
//   GODEBUG=asyncpreemptoff=1 ../go/bin/go test -v -count=1 -run TestPhase0 ./
// ============================================================================

// workload3G creates 3 goroutines that each yield twice via Gosched.
// This produces 5+ decision points (initial picks + re-picks after yields).
func workload3G(t *testing.T) {
	t.Helper()
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			runtime.Gosched()
		}()
	}
	wg.Wait()
}

// workload5G creates 5 goroutines with 3 yields each for a richer decision space.
func workload5G(t *testing.T) {
	t.Helper()
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			runtime.Gosched()
			runtime.Gosched()
		}()
	}
	wg.Wait()
}

// workloadChannels creates goroutines communicating via channels.
// This tests yield points from channel blocking, not just Gosched.
func workloadChannels(t *testing.T) {
	t.Helper()
	ch1 := make(chan int, 1)
	ch2 := make(chan int, 1)
	ch3 := make(chan int, 1)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		ch1 <- 1
		runtime.Gosched()
		v := <-ch2
		_ = v
	}()
	go func() {
		defer wg.Done()
		v := <-ch1
		ch2 <- v + 1
		runtime.Gosched()
	}()
	go func() {
		defer wg.Done()
		ch3 <- 42
		v := <-ch3
		_ = v
	}()
	wg.Wait()
}

// assertTracesIdentical compares two decision traces field-by-field.
// Returns nil if identical, or an error describing the first divergence.
func assertTracesIdentical(a, b []synctest.Decision) error {
	if len(a) != len(b) {
		return fmt.Errorf("length mismatch: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Step != b[i].Step {
			return fmt.Errorf("step %d: Step mismatch: %d vs %d", i, a[i].Step, b[i].Step)
		}
		if a[i].Index != b[i].Index {
			return fmt.Errorf("step %d: Index mismatch: %d vs %d", i, a[i].Index, b[i].Index)
		}
		if a[i].ChosenBgid != b[i].ChosenBgid {
			return fmt.Errorf("step %d: ChosenBgid mismatch: B%d vs B%d", i, a[i].ChosenBgid, b[i].ChosenBgid)
		}
		if a[i].RunqSize != b[i].RunqSize {
			return fmt.Errorf("step %d: RunqSize mismatch: %d vs %d", i, a[i].RunqSize, b[i].RunqSize)
		}
		for j := int32(0); j < a[i].RunqSize && j < 16; j++ {
			if a[i].RunqBgids[j] != b[i].RunqBgids[j] {
				return fmt.Errorf("step %d: RunqBgids[%d] mismatch: B%d vs B%d",
					i, j, a[i].RunqBgids[j], b[i].RunqBgids[j])
			}
		}
		if a[i].WaitReason != b[i].WaitReason {
			return fmt.Errorf("step %d: WaitReason mismatch: %d vs %d", i, a[i].WaitReason, b[i].WaitReason)
		}
	}
	return nil
}

// logTrace prints a trace for debugging.
func logTrace(t *testing.T, label string, trace []synctest.Decision) {
	t.Helper()
	t.Logf("%s: %d decisions", label, len(trace))
	for i, d := range trace {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v waitReason=%d",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize], d.WaitReason)
	}
}

// ============================================================================
// Test 1: Single-bubble replay determinism (T2.1)
//
// Record a trace, replay 100 times, assert IDENTICAL at every field.
// ============================================================================

func TestPhase0_SingleBubbleReplayDeterminism(t *testing.T) {
	runtime.GOMAXPROCS(1)

	// Sub-test A: 3 goroutines with Gosched (the simplest case)
	t.Run("3goroutines_gosched", func(t *testing.T) {
		recorded := synctest.Test(t, workload3G)
		if len(recorded) == 0 {
			t.Fatal("no decisions recorded")
		}
		logTrace(t, "baseline", recorded)

		// Verify >= 5 decision points (3 initial + 2 re-picks minimum)
		if len(recorded) < 5 {
			t.Fatalf("expected >= 5 decisions, got %d", len(recorded))
		}

		// Replay 100 times with the recorded prefix
		for run := 0; run < 100; run++ {
			replayed := synctest.Test(t, workload3G, recorded)
			if err := assertTracesIdentical(recorded, replayed); err != nil {
				logTrace(t, fmt.Sprintf("replay_%d", run), replayed)
				t.Fatalf("replay %d diverged: %v", run, err)
			}
		}
		t.Logf("PASS: 100 replays identical (%d decisions each)", len(recorded))
	})

	// Sub-test B: 5 goroutines with 3 yields (richer decision space)
	t.Run("5goroutines_gosched", func(t *testing.T) {
		recorded := synctest.Test(t, workload5G)
		if len(recorded) == 0 {
			t.Fatal("no decisions recorded")
		}
		logTrace(t, "baseline", recorded)

		for run := 0; run < 100; run++ {
			replayed := synctest.Test(t, workload5G, recorded)
			if err := assertTracesIdentical(recorded, replayed); err != nil {
				logTrace(t, fmt.Sprintf("replay_%d", run), replayed)
				t.Fatalf("replay %d diverged: %v", run, err)
			}
		}
		t.Logf("PASS: 100 replays identical (%d decisions each)", len(recorded))
	})

	// Sub-test C: channel-based communication (yield via channel block, not Gosched)
	t.Run("channels", func(t *testing.T) {
		recorded := synctest.Test(t, workloadChannels)
		if len(recorded) == 0 {
			t.Fatal("no decisions recorded")
		}
		logTrace(t, "baseline", recorded)

		for run := 0; run < 100; run++ {
			replayed := synctest.Test(t, workloadChannels, recorded)
			if err := assertTracesIdentical(recorded, replayed); err != nil {
				logTrace(t, fmt.Sprintf("replay_%d", run), replayed)
				t.Fatalf("replay %d diverged: %v", run, err)
			}
		}
		t.Logf("PASS: 100 replays identical (%d decisions each)", len(recorded))
	})
}

// ============================================================================
// Test 1 adversarial: record WITHOUT prefix, replay 100 times without prefix.
// If the default scheduling is deterministic (FIFO under asyncpreemptoff=1),
// all 100 runs should produce the same trace even without replay.
// This tests that the FIFO baseline itself is deterministic.
// ============================================================================

func TestPhase0_FIFOBaselineDeterminism(t *testing.T) {
	runtime.GOMAXPROCS(1)

	baseline := synctest.Test(t, workload5G)
	if len(baseline) == 0 {
		t.Fatal("no decisions recorded")
	}
	logTrace(t, "baseline", baseline)

	for run := 0; run < 100; run++ {
		trace := synctest.Test(t, workload5G)
		if err := assertTracesIdentical(baseline, trace); err != nil {
			logTrace(t, fmt.Sprintf("run_%d", run), trace)
			t.Fatalf("FIFO run %d diverged from baseline: %v", run, err)
		}
	}
	t.Logf("PASS: 100 FIFO runs identical (%d decisions each)", len(baseline))
}

// ============================================================================
// Test 1 adversarial: replay a NON-FIFO trace 100 times.
// First diverge at a branching point, then replay that divergent trace.
// ============================================================================

func TestPhase0_NonFIFOReplayDeterminism(t *testing.T) {
	runtime.GOMAXPROCS(1)

	// Record baseline
	baseline := synctest.Test(t, workload5G)
	if len(baseline) == 0 {
		t.Fatal("no decisions recorded")
	}

	// Find first branching point and make a non-FIFO choice
	divergeStep := -1
	for i, d := range baseline {
		if d.RunqSize > 1 {
			divergeStep = i
			break
		}
	}
	if divergeStep < 0 {
		t.Fatal("no branching point found in baseline")
	}

	// Create modified prefix choosing index=1 at diverge point
	prefix := make([]synctest.Decision, len(baseline))
	copy(prefix, baseline)
	prefix[divergeStep].Index = 1

	// Execute once with modified prefix to get the actual non-FIFO trace
	nonFIFOTrace := synctest.Test(t, workload5G, prefix)
	if len(nonFIFOTrace) == 0 {
		t.Fatal("non-FIFO run produced no decisions")
	}
	logTrace(t, "non-FIFO baseline", nonFIFOTrace)

	// Verify it actually diverged
	if nonFIFOTrace[divergeStep].ChosenBgid == baseline[divergeStep].ChosenBgid {
		t.Fatal("non-FIFO trace did not diverge at expected step")
	}

	// Now replay the non-FIFO trace 100 times
	for run := 0; run < 100; run++ {
		replayed := synctest.Test(t, workload5G, nonFIFOTrace)
		if err := assertTracesIdentical(nonFIFOTrace, replayed); err != nil {
			logTrace(t, fmt.Sprintf("replay_%d", run), replayed)
			t.Fatalf("non-FIFO replay %d diverged: %v", run, err)
		}
	}
	t.Logf("PASS: 100 non-FIFO replays identical (%d decisions)", len(nonFIFOTrace))
}

// ============================================================================
// Test 2: Cross-GOMAXPROCS determinism (T2.2)
//
// Record with GOMAXPROCS=4, replay with GOMAXPROCS=1 and vice versa.
// If P-pinning works correctly, BGIDs and traces must be identical because
// each bubble runs on exactly one P regardless of how many Ps exist.
// ============================================================================

func TestPhase0_CrossGOMAXPROCS_Determinism(t *testing.T) {
	// Sub-test A: Record at GOMAXPROCS=4, replay at GOMAXPROCS=1
	t.Run("record4_replay1", func(t *testing.T) {
		runtime.GOMAXPROCS(4)
		recorded := synctest.Test(t, workload5G)
		if len(recorded) == 0 {
			t.Fatal("no decisions recorded at GOMAXPROCS=4")
		}
		logTrace(t, "GOMAXPROCS=4 baseline", recorded)

		runtime.GOMAXPROCS(1)
		replayed := synctest.Test(t, workload5G, recorded)
		if err := assertTracesIdentical(recorded, replayed); err != nil {
			logTrace(t, "GOMAXPROCS=1 replay", replayed)
			t.Fatalf("cross-GOMAXPROCS divergence (4->1): %v", err)
		}
		t.Logf("PASS: GOMAXPROCS=4 trace replayed at GOMAXPROCS=1 (%d decisions)", len(recorded))
	})

	// Sub-test B: Record at GOMAXPROCS=1, replay at GOMAXPROCS=4
	t.Run("record1_replay4", func(t *testing.T) {
		runtime.GOMAXPROCS(1)
		recorded := synctest.Test(t, workload5G)
		if len(recorded) == 0 {
			t.Fatal("no decisions recorded at GOMAXPROCS=1")
		}
		logTrace(t, "GOMAXPROCS=1 baseline", recorded)

		runtime.GOMAXPROCS(4)
		replayed := synctest.Test(t, workload5G, recorded)
		if err := assertTracesIdentical(recorded, replayed); err != nil {
			logTrace(t, "GOMAXPROCS=4 replay", replayed)
			t.Fatalf("cross-GOMAXPROCS divergence (1->4): %v", err)
		}
		t.Logf("PASS: GOMAXPROCS=1 trace replayed at GOMAXPROCS=4 (%d decisions)", len(recorded))
	})

	// Sub-test C: Record at GOMAXPROCS=4, replay at GOMAXPROCS=4 100 times
	// (adversarial: stress P-pinning under concurrent Ps)
	t.Run("4to4_100x", func(t *testing.T) {
		runtime.GOMAXPROCS(4)
		baseline := synctest.Test(t, workload5G)
		if len(baseline) == 0 {
			t.Fatal("no decisions recorded")
		}

		for run := 0; run < 100; run++ {
			trace := synctest.Test(t, workload5G)
			if err := assertTracesIdentical(baseline, trace); err != nil {
				logTrace(t, fmt.Sprintf("run_%d", run), trace)
				t.Fatalf("GOMAXPROCS=4 run %d diverged: %v", run, err)
			}
		}
		t.Logf("PASS: 100 runs at GOMAXPROCS=4, all identical (%d decisions)", len(baseline))
	})

	// Sub-test D: Record at GOMAXPROCS=8 (extreme), replay at GOMAXPROCS=1
	t.Run("record8_replay1", func(t *testing.T) {
		runtime.GOMAXPROCS(8)
		recorded := synctest.Test(t, workload5G)
		if len(recorded) == 0 {
			t.Fatal("no decisions recorded at GOMAXPROCS=8")
		}

		runtime.GOMAXPROCS(1)
		replayed := synctest.Test(t, workload5G, recorded)
		if err := assertTracesIdentical(recorded, replayed); err != nil {
			logTrace(t, "GOMAXPROCS=8 baseline", recorded)
			logTrace(t, "GOMAXPROCS=1 replay", replayed)
			t.Fatalf("cross-GOMAXPROCS divergence (8->1): %v", err)
		}
		t.Logf("PASS: GOMAXPROCS=8 trace replayed at GOMAXPROCS=1 (%d decisions)", len(recorded))
	})
}

// ============================================================================
// Test 2 adversarial: record with GOMAXPROCS=4, record again fresh (no prefix)
// at GOMAXPROCS=1. Are the traces identical WITHOUT using replay?
// If yes: P-pinning means the number of Ps has NO effect on scheduling.
// If no: P count affects something (BGID assignment? runq ordering?).
// ============================================================================

func TestPhase0_CrossGOMAXPROCS_FreshRuns(t *testing.T) {
	runtime.GOMAXPROCS(4)
	trace4 := synctest.Test(t, workload5G)
	if len(trace4) == 0 {
		t.Fatal("no decisions at GOMAXPROCS=4")
	}
	logTrace(t, "GOMAXPROCS=4 fresh", trace4)

	runtime.GOMAXPROCS(1)
	trace1 := synctest.Test(t, workload5G)
	if len(trace1) == 0 {
		t.Fatal("no decisions at GOMAXPROCS=1")
	}
	logTrace(t, "GOMAXPROCS=1 fresh", trace1)

	err := assertTracesIdentical(trace4, trace1)
	if err != nil {
		t.Logf("NOTE: Fresh runs at GOMAXPROCS=4 vs 1 differ: %v", err)
		t.Logf("This means P count affects default scheduling even without explicit replay.")
		t.Logf("Replay still works (tested separately), but BGID assignment may depend on P count.")
		// This is informational, not necessarily a failure of T2.2 which is about
		// replay determinism, not about cross-P natural determinism.
		// But it IS a failure if BGIDs differ, since T2.2 says BGIDs depend only on creation order.
		if len(trace4) == len(trace1) {
			for i := range trace4 {
				if trace4[i].ChosenBgid != trace1[i].ChosenBgid {
					t.Errorf("BGID differs at step %d: GOMAXPROCS=4 B%d vs GOMAXPROCS=1 B%d",
						i, trace4[i].ChosenBgid, trace1[i].ChosenBgid)
				}
			}
		}
	} else {
		t.Logf("PASS: Fresh runs at GOMAXPROCS=4 and GOMAXPROCS=1 produce identical traces")
	}
}
