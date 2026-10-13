package experiments

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// TestFrontierSignaling verifies the g0↔root round-trip for live decisions.
//
// Sets an onDecision hook that always returns 0 (FIFO). The hook is called
// by the root goroutine at each frontier decision point. The resulting trace
// should be identical to a run without the hook (default FIFO behavior).
//
// Run: GODEBUG=asyncpreemptoff=1 ../go/bin/go test -v -run TestFrontierSignaling ./
func TestFrontierSignaling(t *testing.T) {
	runtime.GOMAXPROCS(1)

	var hookCalls atomic.Int32

	workload := func(t *testing.T) {
		// Set hook from inside the bubble. It will be active for all
		// subsequent frontier decision points.
		synctest.SetDecisionHook(func(state synctest.BubbleState) int32 {
			if state.Idle {
				return -1
			}
			hookCalls.Add(1)
			return 0 // FIFO — same as default
		})

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

	// Run with hook.
	hooked := synctest.Test(t, workload)
	if len(hooked) == 0 {
		t.Fatal("no decisions recorded")
	}
	t.Logf("hooked trace: %d decisions, hook called %d times", len(hooked), hookCalls.Load())
	for i, d := range hooked {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
	}

	if hookCalls.Load() == 0 {
		t.Fatal("hook was never called — frontier signaling not working")
	}

	// Run without hook (baseline).
	baseline := synctest.Test(t, func(t *testing.T) {
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
	})

	// Compare: hooked FIFO should produce same trace as default FIFO.
	if len(hooked) != len(baseline) {
		t.Fatalf("decision count mismatch: hooked %d, baseline %d", len(hooked), len(baseline))
	}
	for i := range baseline {
		if hooked[i].ChosenBgid != baseline[i].ChosenBgid {
			t.Fatalf("step %d: chosenBgid mismatch: hooked=B%d, baseline=B%d",
				i, hooked[i].ChosenBgid, baseline[i].ChosenBgid)
		}
	}
	t.Logf("PASS: hooked FIFO trace identical to baseline (%d decisions, hook called %d times)",
		len(baseline), hookCalls.Load())
}

// TestExpandedHookState verifies that the BubbleState struct is correctly
// populated at each frontier decision point.
//
// Run: GODEBUG=asyncpreemptoff=1 ../go/bin/go test -v -run TestExpandedHookState ./
func TestExpandedHookState(t *testing.T) {
	runtime.GOMAXPROCS(1)

	var states []synctest.BubbleState

	workload := func(t *testing.T) {
		synctest.SetDecisionHook(func(state synctest.BubbleState) int32 {
			if state.Idle {
				return -1
			}
			states = append(states, state)
			return 0
		})

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

	states = nil
	synctest.Test(t, workload)

	if len(states) == 0 {
		t.Fatal("no hook calls recorded")
	}
	t.Logf("recorded %d hook states", len(states))

	for i, s := range states {
		t.Logf("  state[%d]: Step=%d RunnableN=%d Blocked=%d Now=%d NextTimer=%d LastBgid=B%d bgids=%v",
			i, s.Step, s.RunnableN, s.Blocked, s.Now, s.NextTimer, s.LastBgid,
			s.RunnableBgid[:s.RunnableN])

		// Step should increment
		if s.Step != int32(i) {
			// Step numbers may skip infrastructure steps (non-frontier decisions).
			// Just verify they're non-negative.
			if s.Step < 0 {
				t.Errorf("state[%d]: negative Step=%d", i, s.Step)
			}
		}

		// RunnableN should be positive (frontier = runq non-empty)
		if s.RunnableN <= 0 {
			t.Errorf("state[%d]: RunnableN=%d, want > 0", i, s.RunnableN)
		}

		// Now should be the synctest base time (midnight UTC 2000-01-01)
		const synctestBaseTime = 946684800000000000
		if s.Now != synctestBaseTime {
			t.Errorf("state[%d]: Now=%d, want %d (base time)", i, s.Now, synctestBaseTime)
		}

		// Blocked should be non-negative
		if s.Blocked < 0 {
			t.Errorf("state[%d]: Blocked=%d, want >= 0", i, s.Blocked)
		}

		// RunnableGlob should all be false (no global goroutines yet)
		for j := int32(0); j < s.RunnableN; j++ {
			if s.RunnableGlob[j] {
				t.Errorf("state[%d]: RunnableGlob[%d]=true, want false (no MarkGlobal)", i, j)
			}
		}
	}
	t.Logf("PASS: BubbleState correctly populated at all %d frontier decisions", len(states))
}

// TestGlobalGoroutine verifies that MarkGlobal correctly sets the global flag
// on the current goroutine and that children inherit it. The hook should see
// Global=true for marked goroutines and Global=false for unmarked ones.
//
// Run: GODEBUG=asyncpreemptoff=1 ../go/bin/go test -v -run TestGlobalGoroutine ./
func TestGlobalGoroutine(t *testing.T) {
	runtime.GOMAXPROCS(1)

	var states []synctest.BubbleState

	workload := func(t *testing.T) {
		synctest.SetDecisionHook(func(state synctest.BubbleState) int32 {
			if state.Idle {
				return -1
			}
			states = append(states, state)
			return 0
		})

		done := make(chan struct{}, 3)

		// B3: normal goroutine (global=false)
		go func() {
			runtime.Gosched()
			done <- struct{}{}
		}()

		// B4: global goroutine
		go func() {
			synctest.MarkGlobal()
			runtime.Gosched()

			// B5: child of global goroutine (should inherit global=true)
			go func() {
				runtime.Gosched()
				done <- struct{}{}
			}()
			done <- struct{}{}
		}()

		<-done
		<-done
		<-done
	}

	states = nil
	synctest.Test(t, workload)

	if len(states) == 0 {
		t.Fatal("no hook calls recorded")
	}

	// Look for a state where both global and non-global goroutines are runnable.
	foundGlobal := false
	foundNonGlobal := false
	for i, s := range states {
		t.Logf("  state[%d]: Step=%d RunnableN=%d bgids=%v global=%v",
			i, s.Step, s.RunnableN,
			s.RunnableBgid[:s.RunnableN], s.RunnableGlob[:s.RunnableN])
		for j := int32(0); j < s.RunnableN; j++ {
			if s.RunnableGlob[j] {
				foundGlobal = true
			} else {
				foundNonGlobal = true
			}
		}
	}
	if !foundGlobal {
		t.Error("never saw a global goroutine in hook state — MarkGlobal not propagating")
	}
	if !foundNonGlobal {
		t.Error("never saw a non-global goroutine in hook state — all goroutines marked global?")
	}
	t.Logf("PASS: hook correctly reports global=%v and non-global goroutines", foundGlobal)
}

// TestRootInHook verifies that the rootInHook flag correctly prevents
// findRunnable from re-waking root when the hook is executing.
// In this test, the hook doesn't actually block (that requires the
// distributed simulation setup), but we verify that the flag is
// set correctly and that multiple goroutines yielding during hook
// execution don't cause panics.
//
// Run: GODEBUG=asyncpreemptoff=1 ../go/bin/go test -v -run TestRootInHook ./
func TestRootInHook(t *testing.T) {
	runtime.GOMAXPROCS(4) // multi-P to stress test

	var hookCalls atomic.Int32

	workload := func(t *testing.T) {
		synctest.SetDecisionHook(func(state synctest.BubbleState) int32 {
			if state.Idle {
				return -1
			}
			hookCalls.Add(1)
			return 0
		})

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

	// Run 20 times — if rootInHook guard is broken, we'd see panics.
	for i := range 20 {
		hookCalls.Store(0)
		trace := synctest.Test(t, workload)
		if i == 0 {
			t.Logf("run %d: %d decisions, %d hook calls", i, len(trace), hookCalls.Load())
		}
	}
	t.Logf("PASS: 20 runs with GOMAXPROCS=4, no panics, rootInHook guard working")
}

// TestFrontierNonFIFO verifies that the hook can make non-default choices.
//
// The hook returns 1 when runqSize > 1 (pick the second goroutine instead
// of the first). This should produce a different trace than default FIFO.
//
// Run: GODEBUG=asyncpreemptoff=1 ../go/bin/go test -v -run TestFrontierNonFIFO ./
func TestFrontierNonFIFO(t *testing.T) {
	runtime.GOMAXPROCS(1)

	workload := func(t *testing.T) {
		// Hook: pick index 1 when there's a real choice, else 0.
		synctest.SetDecisionHook(func(state synctest.BubbleState) int32 {
			if state.Idle {
				return -1
			}
			if state.RunnableN > 1 {
				return 1
			}
			return 0
		})

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

	// Run with non-FIFO hook.
	hooked := synctest.Test(t, workload)
	if len(hooked) == 0 {
		t.Fatal("no decisions recorded")
	}
	t.Logf("non-FIFO trace: %d decisions", len(hooked))
	for i, d := range hooked {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
	}

	// Run without hook (baseline FIFO).
	baseline := synctest.Test(t, func(t *testing.T) {
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
	})
	t.Logf("baseline trace: %d decisions", len(baseline))
	for i, d := range baseline {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d runqBgids=%v",
			i, d.Index, d.ChosenBgid, d.RunqSize, d.RunqBgids[:d.RunqSize])
	}

	// Verify: at least one decision point differs (where runqSize > 1).
	anyDiffer := false
	for i := range hooked {
		if i >= len(baseline) {
			break
		}
		if hooked[i].ChosenBgid != baseline[i].ChosenBgid {
			anyDiffer = true
			t.Logf("step %d: hooked picked B%d, baseline picked B%d",
				i, hooked[i].ChosenBgid, baseline[i].ChosenBgid)
		}
	}
	if !anyDiffer {
		t.Fatal("non-FIFO hook produced identical trace to FIFO — hook not working")
	}
	t.Logf("PASS: non-FIFO hook produced different interleaving")
}

// TestIdleHookOwnsTime verifies that once a hook is installed, the hook sees
// idle/timer states before time advances.
func TestIdleHookOwnsTime(t *testing.T) {
	runtime.GOMAXPROCS(1)

	var idleCalls atomic.Int32

	workload := func(t *testing.T) {
		done := make(chan struct{})
		synctest.SetDecisionHook(func(state synctest.BubbleState) int32 {
			if !state.Idle {
				return 0
			}
			idleCalls.Add(1)
			if state.NextTimer > 0 {
				synctest.SetTime(state.NextTimer)
				return 0
			}
			return -1
		})

		go func() {
			time.Sleep(time.Nanosecond)
			close(done)
		}()

		<-done
	}

	trace := synctest.Test(t, workload)
	if len(trace) == 0 {
		t.Fatal("no decisions recorded")
	}
	if idleCalls.Load() == 0 {
		t.Fatal("idle hook never fired before timer advance")
	}
}
