package explorer_test

import (
	"runtime"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/shubhaankar/synctest/explorer"
)

// TestExploreFindsBug proves that prefix-based divergence finds a race
// condition that the default FIFO scheduling misses.
//
// Workload: two goroutines withdraw from a shared balance.
//   - B3 (created first, in runq tail): check → Gosched → write (vulnerable)
//   - B4 (created second, runnext): check → write (atomic, no yield)
//
// FIFO picks B4 first (runnext). B4 atomically withdraws. B3 sees the
// reduced balance and skips. Safe.
//
// Non-FIFO (index=1 at step 2) picks B3 first. B3 checks (true), yields.
// B4 runs, also checks (true), writes. B3 resumes and writes again.
// Balance goes negative. Race!
func TestExploreFindsBug(t *testing.T) {
	runtime.GOMAXPROCS(1)

	var balance int
	workload := func(t *testing.T) {
		balance = 100
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { // B3 — vulnerable (yield between check and write)
			defer wg.Done()
			if balance >= 60 {
				runtime.Gosched()
				balance -= 60
			}
		}()
		go func() { // B4 — atomic (no yield)
			defer wg.Done()
			if balance >= 60 {
				balance -= 60
			}
		}()
		wg.Wait()
	}

	// Run 1: baseline FIFO — should be safe.
	baseline := synctest.Test(t, workload)
	baselineBalance := balance
	if baselineBalance < 0 {
		t.Fatalf("baseline FIFO should be safe, got balance=%d", baselineBalance)
	}
	t.Logf("baseline FIFO: balance=%d (safe), %d decisions", baselineBalance, len(baseline))
	for i, d := range baseline {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
	}

	// Find first branching point (runqSize > 1).
	branchIdx := -1
	for i, d := range baseline {
		if d.RunqSize > 1 {
			branchIdx = i
			break
		}
	}
	if branchIdx < 0 {
		t.Fatal("no branching point found in baseline")
	}
	t.Logf("first branching point: step %d (runqSize=%d)", branchIdx, baseline[branchIdx].RunqSize)

	// Run 2: non-FIFO — pick index 1 at the branching point.
	prefix := make([]synctest.Decision, branchIdx+1)
	copy(prefix, baseline[:branchIdx+1])
	prefix[branchIdx].Index = 1

	diverged := synctest.Test(t, workload, prefix)
	divergedBalance := balance
	t.Logf("non-FIFO (index=1 at step %d): balance=%d, %d decisions",
		branchIdx, divergedBalance, len(diverged))
	for i, d := range diverged {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
	}

	if divergedBalance >= 0 {
		t.Fatalf("expected race (negative balance) but got balance=%d", divergedBalance)
	}
	t.Logf("PASS: non-FIFO interleaving triggers race (balance=%d) that FIFO misses", divergedBalance)
}

// TestRunWithHook verifies that RunWithHook correctly passes scheduling
// decisions through the onDecision hook.
//
// A FIFO hook should produce the same trace as a baseline run.
// A non-FIFO hook should produce a different interleaving.
func TestRunWithHook(t *testing.T) {
	runtime.GOMAXPROCS(1)

	workload := func(t *testing.T) {
		var wg sync.WaitGroup
		for range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				runtime.Gosched()
			}()
		}
		wg.Wait()
	}

	// FIFO hook — should produce valid trace.
	fifoTrace, fifoOK := explorer.RunWithHook(t, workload, func(state synctest.BubbleState) int32 {
		return 0
	})
	if !fifoOK {
		t.Fatal("FIFO hook run failed")
	}
	if len(fifoTrace) == 0 {
		t.Fatal("FIFO hook produced no decisions")
	}
	t.Logf("FIFO hook: %d decisions", len(fifoTrace))
	for i, d := range fifoTrace {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d",
			i, d.Index, d.ChosenBgid, d.RunqSize)
	}

	// Non-FIFO hook — pick index 1 when there's a real choice.
	nonFIFOTrace, nonFIFOOK := explorer.RunWithHook(t, workload, func(state synctest.BubbleState) int32 {
		if state.RunnableN > 1 {
			return 1
		}
		return 0
	})
	if !nonFIFOOK {
		t.Fatal("non-FIFO hook run failed")
	}
	t.Logf("non-FIFO hook: %d decisions", len(nonFIFOTrace))
	for i, d := range nonFIFOTrace {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d",
			i, d.Index, d.ChosenBgid, d.RunqSize)
	}

	// Verify divergence: at least one decision differs.
	anyDiffer := false
	minLen := len(fifoTrace)
	if len(nonFIFOTrace) < minLen {
		minLen = len(nonFIFOTrace)
	}
	for i := 0; i < minLen; i++ {
		if fifoTrace[i].ChosenBgid != nonFIFOTrace[i].ChosenBgid {
			anyDiffer = true
			t.Logf("divergence at step %d: FIFO=B%d non-FIFO=B%d",
				i, fifoTrace[i].ChosenBgid, nonFIFOTrace[i].ChosenBgid)
			break
		}
	}
	if !anyDiffer {
		t.Fatal("non-FIFO hook produced identical trace to FIFO — hook not controlling decisions")
	}
	t.Logf("PASS: RunWithHook correctly routes decisions through the hook")
}

// TestExploreAllSafe runs explorer.Test on a workload where every
// interleaving is safe. Verifies that the DFS explores multiple
// interleavings and all pass.
func TestExploreAllSafe(t *testing.T) {
	runtime.GOMAXPROCS(1)

	explorer.Test(t, func(t *testing.T) {
		var wg sync.WaitGroup
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				runtime.Gosched()
			}()
		}
		wg.Wait()
	})
}

// TestExploreBound verifies that the context bound controls exploration size.
func TestExploreBound(t *testing.T) {
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

	// Bound 0: only baseline FIFO (1 run).
	t.Run("bound_0", func(t *testing.T) {
		explorer.Test(t, workload, &explorer.CHESS{Bound: 0})
	})

	// Bound 1: more interleavings.
	t.Run("bound_1", func(t *testing.T) {
		explorer.Test(t, workload, &explorer.CHESS{Bound: 1})
	})

	// MaxRuns cap.
	t.Run("max_3", func(t *testing.T) {
		explorer.Test(t, workload, explorer.MaxRuns(3))
	})
}
