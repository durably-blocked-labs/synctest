package experiments

import (
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
)

// TestDecisionRecording verifies that scheduling decisions are recorded correctly
// at each yield point inside a synctest bubble.
//
// Run: GODEBUG=asyncpreemptoff=1 ../go/bin/go test -v -run TestDecisionRecording ./
func TestDecisionRecording(t *testing.T) {
	runtime.GOMAXPROCS(1)

	workload := func(t *testing.T) {
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

	trace := synctest.Test(t, workload)

	if len(trace) == 0 {
		t.Fatal("no decisions recorded")
	}

	t.Logf("recorded %d decisions", len(trace))
	for i, d := range trace {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
	}

	// Verify per-run invariants.
	for i, d := range trace {
		if d.Step != int32(i) {
			t.Fatalf("decision %d: step=%d, expected %d", i, d.Step, i)
		}
		if d.RunqSize < 1 {
			t.Fatalf("decision %d: runqSize=%d, expected >= 1", i, d.RunqSize)
		}
		found := false
		for j := int32(0); j < d.RunqSize && j < 16; j++ {
			if d.RunqBgids[j] == d.ChosenBgid {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("decision %d: chosenBgid=B%d not in runqBgids=%v",
				i, d.ChosenBgid, d.RunqBgids[:d.RunqSize])
		}
	}
}

// TestFollowDecisions verifies the unified record/follow model.
//
// Run 1: record a trace from a fresh bubble.
// Run 2: replay that trace in a new bubble. The scheduler follows the prefix.
// The two traces should be identical.
//
// Run: GODEBUG=asyncpreemptoff=1 ../go/bin/go test -v -run TestFollowDecisions ./
func TestFollowDecisions(t *testing.T) {
	runtime.GOMAXPROCS(1)

	workload := func(t *testing.T) {
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

	// Run 1: record.
	recorded := synctest.Test(t, workload)
	if len(recorded) == 0 {
		t.Fatal("run 1: no decisions recorded")
	}
	t.Logf("run 1 (record): %d decisions", len(recorded))
	for i, d := range recorded {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
	}

	// Run 2: follow. New bubble, pre-loaded with recorded trace.
	followed := synctest.Test(t, workload, recorded)
	if len(followed) == 0 {
		t.Fatal("run 2: no decisions recorded")
	}
	t.Logf("run 2 (follow): %d decisions", len(followed))
	for i, d := range followed {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
	}

	// Compare.
	if len(followed) != len(recorded) {
		t.Fatalf("decision count mismatch: recorded %d, followed %d",
			len(recorded), len(followed))
	}
	for i := range recorded {
		r := recorded[i]
		f := followed[i]
		if r.ChosenBgid != f.ChosenBgid {
			t.Fatalf("step %d: chosenBgid mismatch: recorded=B%d, followed=B%d",
				i, r.ChosenBgid, f.ChosenBgid)
		}
		if r.RunqSize != f.RunqSize {
			t.Fatalf("step %d: runqSize mismatch: recorded=%d, followed=%d",
				i, r.RunqSize, f.RunqSize)
		}
	}
	t.Logf("PASS: follow run produced identical decisions (%d decisions)", len(recorded))
}

// TestDivergentFollow proves that changing a decision index in the prefix
// causes a different goroutine to be picked, producing a divergent trace.
//
// The workload creates 3 goroutines (B3, B4, B5) that each Gosched twice.
// At step 2 the runq is [B5, B3, B4] (size 3). The default trace picks
// index=0 (B5). This test changes it to index=1 (B3) and verifies:
//   - Steps 0-1 are identical (before the divergence point)
//   - Step 2 picks B3 instead of B5
//   - The trace completes without deadlock
//
// Run: GODEBUG=asyncpreemptoff=1 ../go/bin/go test -v -run TestDivergentFollow ./
func TestDivergentFollow(t *testing.T) {
	runtime.GOMAXPROCS(1)

	workload := func(t *testing.T) {
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

	// Run 1: record the default (FIFO) trace.
	baseline := synctest.Test(t, workload)
	if len(baseline) == 0 {
		t.Fatal("no decisions recorded")
	}
	t.Logf("baseline: %d decisions", len(baseline))
	for i, d := range baseline {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
	}

	// Find the first decision point with runqSize > 1 — that's where we diverge.
	divergeStep := -1
	for i, d := range baseline {
		if d.RunqSize > 1 {
			divergeStep = i
			break
		}
	}
	if divergeStep < 0 {
		t.Fatal("no decision point with runqSize > 1 found")
	}
	t.Logf("diverging at step %d (runqSize=%d, baseline picks B%d at index=0)",
		divergeStep, baseline[divergeStep].RunqSize, baseline[divergeStep].ChosenBgid)

	// Build a modified prefix: copy baseline, change the diverge step to index=1.
	prefix := make([]synctest.Decision, len(baseline))
	copy(prefix, baseline)
	prefix[divergeStep].Index = 1

	expectedBgid := baseline[divergeStep].RunqBgids[1]
	t.Logf("modified prefix: step %d picks index=1 (expecting B%d)", divergeStep, expectedBgid)

	// Run 2: follow the modified prefix.
	diverged := synctest.Test(t, workload, prefix)
	if len(diverged) == 0 {
		t.Fatal("divergent run: no decisions recorded")
	}
	t.Logf("diverged: %d decisions", len(diverged))
	for i, d := range diverged {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
	}

	// Verify: steps before divergence are identical.
	for i := 0; i < divergeStep; i++ {
		if baseline[i].ChosenBgid != diverged[i].ChosenBgid {
			t.Fatalf("step %d (before divergence): chosenBgid mismatch: baseline=B%d, diverged=B%d",
				i, baseline[i].ChosenBgid, diverged[i].ChosenBgid)
		}
	}

	// Verify: at the diverge step, a different goroutine was picked.
	if diverged[divergeStep].ChosenBgid == baseline[divergeStep].ChosenBgid {
		t.Fatalf("step %d: same goroutine picked (B%d) — divergence failed!",
			divergeStep, diverged[divergeStep].ChosenBgid)
	}
	if diverged[divergeStep].ChosenBgid != expectedBgid {
		t.Fatalf("step %d: expected B%d, got B%d",
			divergeStep, expectedBgid, diverged[divergeStep].ChosenBgid)
	}

	t.Logf("PASS: diverged at step %d — baseline picked B%d, diverged picked B%d",
		divergeStep, baseline[divergeStep].ChosenBgid, diverged[divergeStep].ChosenBgid)
}

// TestPPinning verifies that P pinning makes bubbles deterministic under GOMAXPROCS>1.
//
// Runs the same workload 20 times with GOMAXPROCS=4 and verifies all traces are identical.
// Without P pinning, schedtick%61 and stealWork cause occasional non-determinism.
//
// Run: GODEBUG=asyncpreemptoff=1 GOMAXPROCS=4 ../go/bin/go test -v -run TestPPinning ./
func TestPPinning(t *testing.T) {
	runtime.GOMAXPROCS(4) // multiple Ps — the whole point

	workload := func(t *testing.T) {
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

	// Record baseline.
	baseline := synctest.Test(t, workload)
	if len(baseline) == 0 {
		t.Fatal("no decisions recorded")
	}
	t.Logf("baseline: %d decisions", len(baseline))
	for i, d := range baseline {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
	}

	// Run 20 more times, compare each to baseline.
	for run := 1; run <= 20; run++ {
		trace := synctest.Test(t, workload)
		if len(trace) != len(baseline) {
			t.Logf("run %d: %d decisions", run, len(trace))
			for i, d := range trace {
				t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
					i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
			}
			t.Fatalf("run %d: decision count %d != baseline %d", run, len(trace), len(baseline))
		}
		for i := range baseline {
			if trace[i].ChosenBgid != baseline[i].ChosenBgid {
				t.Fatalf("run %d step %d: chosenBgid=B%d != baseline B%d",
					run, i, trace[i].ChosenBgid, baseline[i].ChosenBgid)
			}
			if trace[i].RunqSize != baseline[i].RunqSize {
				t.Fatalf("run %d step %d: runqSize=%d != baseline %d",
					run, i, trace[i].RunqSize, baseline[i].RunqSize)
			}
		}
	}
	t.Logf("PASS: 20 runs with GOMAXPROCS=4 all identical (%d decisions each)", len(baseline))
}
