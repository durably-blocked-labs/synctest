package experiments

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestManualRace shows how to manually trigger a race condition
// by inserting runtime.Gosched() at the exact "bad" spot.
//
//	GOMAXPROCS=1 GODEBUG=asyncpreemptoff=1 ./go/bin/go test -v -run TestManualRace ./experiments/manual_race_test.go
func TestManualRace(t *testing.T) {
	runtime.GOMAXPROCS(1)

	balance := 100
	var wg sync.WaitGroup
	wg.Add(2)

	withdraw := func(id int, shouldYield bool) {
		defer wg.Done()

		// CHECK
		if balance >= 60 {
			t.Logf("G%d: check passed (balance=%d)", id, balance)

			// === THE RACE WINDOW ===
			if shouldYield {
				runtime.Gosched() // "preempt me here"
			}

			// ACT
			balance = balance - 60
			t.Logf("G%d: withdrew 60, balance=%d", id, balance)
		} else {
			t.Logf("G%d: check failed (balance=%d)", id, balance)
		}
	}

	t.Log("=== Without yield (no race) ===")
	go withdraw(1, false)
	go withdraw(2, false)
	wg.Wait()
	t.Logf("Final balance: %d\n", balance)

	// Reset
	balance = 100
	wg.Add(2)

	t.Log("=== With yield after check (triggers race) ===")
	go withdraw(1, true) // G1 yields after check
	go withdraw(2, true) // G2 yields after check
	wg.Wait()
	t.Logf("Final balance: %d", balance)

	if balance < 0 {
		t.Logf("SUCCESS: Race triggered, balance went negative!")
	}
}

// TestAllSchedules explores ALL possible interleavings of 4 yield points.
//
//	GOMAXPROCS=1 GODEBUG=asyncpreemptoff=1 ./go/bin/go test -v -run TestAllSchedules ./experiments/manual_race_test.go
//
// Yield points (BEFORE each memory op):
//
//	[yield] → G1 reads balance (check)
//	[yield] → G1 writes balance (subtract)
//	[yield] → G2 reads balance (check)
//	[yield] → G2 writes balance (subtract)
func TestAllSchedules(t *testing.T) {
	runtime.GOMAXPROCS(1)

	schedules := []struct {
		name   string
		order  []string
		expect string
	}{
		{"G1 fully, G2 fully", []string{"G1.read", "G1.write", "G2.read", "G2.write"}, "ok"},
		{"G2 fully, G1 fully", []string{"G2.read", "G2.write", "G1.read", "G1.write"}, "ok"},
		{"reads then writes", []string{"G1.read", "G2.read", "G1.write", "G2.write"}, "race"},
		{"reads then writes reversed", []string{"G1.read", "G2.read", "G2.write", "G1.write"}, "race"},
		{"G1.read G1.write G2.read G2.write", []string{"G1.read", "G1.write", "G2.read", "G2.write"}, "ok"},
		{"G2.read G1.read G2.write G1.write", []string{"G2.read", "G1.read", "G2.write", "G1.write"}, "race"},
	}

	for _, sched := range schedules {
		t.Run(sched.name, func(t *testing.T) {
			result := runSchedule(t, sched.order)

			if result < 0 && sched.expect == "race" {
				t.Logf("RACE confirmed (balance=%d)", result)
			} else if result >= 0 && sched.expect == "ok" {
				t.Logf("OK confirmed (balance=%d)", result)
			} else {
				t.Errorf("Unexpected: expected %s, got balance=%d", sched.expect, result)
			}
		})
	}
}

func runSchedule(t *testing.T, schedule []string) int {
	balance := 100
	g1CheckPassed := false
	g2CheckPassed := false

	// Control channels
	do := map[string]chan struct{}{
		"G1.read":  make(chan struct{}),
		"G1.write": make(chan struct{}),
		"G2.read":  make(chan struct{}),
		"G2.write": make(chan struct{}),
	}
	done := map[string]chan struct{}{
		"G1.read":  make(chan struct{}),
		"G1.write": make(chan struct{}),
		"G2.read":  make(chan struct{}),
		"G2.write": make(chan struct{}),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// G1
	go func() {
		defer wg.Done()

		<-do["G1.read"]
		g1CheckPassed = balance >= 60
		t.Logf("  G1.read: balance=%d pass=%v", balance, g1CheckPassed)
		done["G1.read"] <- struct{}{}

		<-do["G1.write"]
		if g1CheckPassed {
			balance -= 60
			t.Logf("  G1.write: balance=%d", balance)
		} else {
			t.Logf("  G1.write: skipped")
		}
		done["G1.write"] <- struct{}{}
	}()

	// G2
	go func() {
		defer wg.Done()

		<-do["G2.read"]
		g2CheckPassed = balance >= 60
		t.Logf("  G2.read: balance=%d pass=%v", balance, g2CheckPassed)
		done["G2.read"] <- struct{}{}

		<-do["G2.write"]
		if g2CheckPassed {
			balance -= 60
			t.Logf("  G2.write: balance=%d", balance)
		} else {
			t.Logf("  G2.write: skipped")
		}
		done["G2.write"] <- struct{}{}
	}()

	// Run the schedule
	t.Logf("Schedule: %v", schedule)
	for _, op := range schedule {
		do[op] <- struct{}{}
		<-done[op]
	}

	wg.Wait()
	return balance
}

// TestAsyncPreemptOff verifies async preemption behavior.
//
//	# WITH preemption disabled (deterministic - B runs fully before A)
//	GOMAXPROCS=1 GODEBUG=asyncpreemptoff=1 ./go/bin/go test -v -run TestAsyncPreemptOff ./experiments/manual_race_test.go
//
//	# WITH preemption enabled (B gets preempted mid-execution)
//	GOMAXPROCS=1 ./go/bin/go test -v -run TestAsyncPreemptOff ./experiments/manual_race_test.go
//
// Expected:
//
//	asyncpreemptoff=1: [B.start, B.end, A.start, A.end] (B finishes before A starts)
//	asyncpreemptoff=0: [B.start, A.start, A.end, B.end] (B preempted, A runs)

func TestAsyncPreemptOff(t *testing.T) {
	runtime.GOMAXPROCS(1)

	// Use atomic writes to avoid mutex yield points
	var order [4]int32
	var orderIdx int32

	var wg sync.WaitGroup
	wg.Add(2)

	// G_A - spawned first, goes to runq
	go func() {
		defer wg.Done()
		// Record start - use atomic to avoid yield
		idx := atomicAdd(&orderIdx)
		order[idx] = 1 // A.start

		// A does minimal work
		idx = atomicAdd(&orderIdx)
		order[idx] = 2 // A.end
	}()

	// G_B - spawned second, goes to runnext (runs first)
	go func() {
		defer wg.Done()
		// Record start
		idx := atomicAdd(&orderIdx)
		order[idx] = 3 // B.start

		// Long CPU-bound loop (~50-100ms)
		// Must be long enough to trigger sysmon (checks every 10ms)
		// Use result to prevent compiler optimization
		sum := uint64(0)
		for i := uint64(0); i < 500_000_000; i++ {
			sum += i
		}
		// don't optimize away
		runtime.KeepAlive(sum)

		// Record end
		idx = atomicAdd(&orderIdx)
		order[idx] = 4 // B.end
	}()

	wg.Wait()

	// Decode: 1=A.start, 2=A.end, 3=B.start, 4=B.end
	names := map[int32]string{1: "A.start", 2: "A.end", 3: "B.start", 4: "B.end"}
	result := make([]string, 4)
	for i := 0; i < 4; i++ {
		result[i] = names[order[i]]
	}
	t.Logf("Execution order: %v", result)

	// Analyze
	if order[0] == 3 && order[1] == 4 { // B.start, B.end first
		t.Logf("NO PREEMPTION: B ran to completion before A")
	} else if order[0] == 3 && order[1] == 1 { // B.start, A.start
		t.Logf("PREEMPTED: B was interrupted, A ran")
	}
}

func atomicAdd(p *int32) int {
	return int(atomic.AddInt32(p, 1) - 1)
}

// TestDeterminism1000 runs a scheduling test 1000 times and verifies
// that the execution order is identical every single time.
//
//	GOMAXPROCS=1 GODEBUG=asyncpreemptoff=1 ./go/bin/go test -v -run TestDeterminism1000 ./experiments/manual_race_test.go
//
// This proves that with GOMAXPROCS=1 + asyncpreemptoff=1, scheduling is deterministic.
func TestDeterminism1000(t *testing.T) {
	runtime.GOMAXPROCS(1)
	const iterations = 1000

	// runOnce executes the goroutine scheduling and returns the order
	// Uses atomics instead of mutex to allow true parallelism
	runOnce := func() [6]int32 {
		var order [6]int32
		var idx int32
		var wg sync.WaitGroup

		wg.Add(3)

		// Spawn G1, G2, G3
		go func() {
			defer wg.Done()
			order[atomic.AddInt32(&idx, 1)-1] = 1
			order[atomic.AddInt32(&idx, 1)-1] = 1
		}()

		go func() {
			defer wg.Done()
			order[atomic.AddInt32(&idx, 1)-1] = 2
			order[atomic.AddInt32(&idx, 1)-1] = 2
		}()

		go func() {
			defer wg.Done()
			order[atomic.AddInt32(&idx, 1)-1] = 3
			order[atomic.AddInt32(&idx, 1)-1] = 3
		}()

		wg.Wait()
		return order
	}

	// Run first iteration to establish baseline
	baseline := runOnce()
	t.Logf("Baseline order: %v", baseline)

	// Run 999 more times and compare
	mismatches := 0
	for i := 1; i < iterations; i++ {
		order := runOnce()

		// Compare with baseline
		if order != baseline {
			mismatches++
			if mismatches <= 5 { // only log first 5
				t.Logf("Iteration %d: order mismatch %v vs %v", i, order, baseline)
			}
		}
	}

	if mismatches == 0 {
		t.Logf("SUCCESS: All %d iterations produced identical order", iterations)
	} else {
		t.Errorf("FAILED: %d/%d iterations had different order (non-deterministic!)", mismatches, iterations)
	}
}
