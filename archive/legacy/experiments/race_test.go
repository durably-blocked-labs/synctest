package experiments

import (
	"runtime"
	"sync"
	"testing"
)

// TestDeterministicScheduling demonstrates that with GOMAXPROCS=1
// and no select statements, execution order is deterministic.
//
//	GOMAXPROCS=1 GODEBUG=asyncpreemptoff=1 ./go/bin/go test -v -run TestDeterministicScheduling ./experiments/race_test.go
//
// All runs should produce identical output.

func TestDeterministicScheduling(t *testing.T) {
	runtime.GOMAXPROCS(1)

	var order []int
	var mu sync.Mutex

	record := func(id int) {
		mu.Lock()
		order = append(order, id)
		mu.Unlock()
	}

	var wg sync.WaitGroup
	wg.Add(3)

	// Launch 3 goroutines
	go func() {
		defer wg.Done()
		record(1)
		record(1)
		record(1)
	}()

	go func() {
		defer wg.Done()
		record(2)
		record(2)
		record(2)
	}()

	go func() {
		defer wg.Done()
		record(3)
		record(3)
		record(3)
	}()

	wg.Wait()

	t.Logf("Execution order: %v", order)

	// With GOMAXPROCS=1 and no preemption, order should be consistent
	// (though which goroutine runs first depends on runqueue order)
}

// TestDataRace demonstrates a classic check-then-act race.
//
//	GOMAXPROCS=1 GODEBUG=asyncpreemptoff=1 ./go/bin/go test -v -run TestDataRace ./experiments/race_test.go
//
// The bug: both goroutines check balance >= 60, both pass,
// both subtract, resulting in negative balance.
//
// With GOMAXPROCS=1, this race is HARDER to trigger because
// there's no true parallelism. The first goroutine usually
// completes before the second runs.

func TestDataRace(t *testing.T) {
	runtime.GOMAXPROCS(1)

	balance := 100

	var wg sync.WaitGroup
	wg.Add(2)

	// Both try to withdraw 60 from balance of 100
	// Correct behavior: only one succeeds, balance = 40
	// Bug behavior: both succeed, balance = -20

	withdraw := func(id int) {
		defer wg.Done()

		// CHECK
		if balance >= 60 {
			// RACE WINDOW: if preempted here, both see balance=100

			// Read current balance
			current := balance

			// ACT
			balance = current - 60
			t.Logf("G%d: withdrew 60, balance now %d", id, balance)
		} else {
			t.Logf("G%d: insufficient funds, balance=%d", id, balance)
		}
	}

	go withdraw(1)
	go withdraw(2)

	wg.Wait()

	t.Logf("Final balance: %d", balance)

	if balance < 0 {
		t.Errorf("BUG: negative balance %d (race condition triggered)", balance)
	}
}

// TestDataRaceWithYieldPoints adds explicit yield points (channel ops)
// to create opportunities for the scheduler to switch goroutines.
//
//	GOMAXPROCS=1 GODEBUG=asyncpreemptoff=1 ./go/bin/go test -v -run TestDataRaceWithYieldPoints ./experiments/race_test.go
//
// This makes the race MORE likely to manifest even with GOMAXPROCS=1.

func TestDataRaceWithYieldPoints(t *testing.T) {
	runtime.GOMAXPROCS(1)

	balance := 100
	checkpoint := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)

	withdraw := func(id int) {
		defer wg.Done()

		// CHECK
		if balance >= 60 {
			t.Logf("G%d: balance check passed (balance=%d)", id, balance)

			// YIELD POINT: this is where scheduler CAN switch
			// G1 sends, blocks waiting for receiver
			// Scheduler switches to G2
			if id == 1 {
				checkpoint <- struct{}{} // G1 yields here
			} else {
				<-checkpoint // G2 yields here
			}

			// Both goroutines reach here having seen balance >= 60
			current := balance

			// ACT
			balance = current - 60
			t.Logf("G%d: withdrew 60, balance now %d", id, balance)
		} else {
			t.Logf("G%d: insufficient funds, balance=%d", id, balance)
		}
	}

	go withdraw(1)
	go withdraw(2)

	wg.Wait()

	t.Logf("Final balance: %d", balance)

	if balance < 0 {
		t.Logf("Race condition triggered! Balance went negative.")
	}
}

// TestRunqueueOrder demonstrates the order goroutines are picked from runqueue.
//
//	GOMAXPROCS=1 GODEBUG=asyncpreemptoff=1 ./go/bin/go test -v -run TestRunqueueOrder ./experiments/race_test.go
//
// With GOMAXPROCS=1 and no -race flag:
// - New goroutines go to runnext (displacing previous to runq tail)
// - Last spawned runs first (LIFO via runnext)

func TestRunqueueOrder(t *testing.T) {
	runtime.GOMAXPROCS(1)

	var order []int
	var mu sync.Mutex
	done := make(chan struct{})

	record := func(id int) {
		mu.Lock()
		order = append(order, id)
		mu.Unlock()
	}

	// Spawn goroutines in order: 1, 2, 3
	// Expected runqueue state after spawning:
	//   runnext: G3 (last spawned)
	//   runq: [G1, G2]
	//
	// So execution order should be: main blocks → G3 → G1 → G2

	go func() { record(1); <-done }()
	go func() { record(2); <-done }()
	go func() { record(3); <-done }()

	// Give goroutines a chance to run
	// (main yields by receiving from channel)
	runtime.Gosched()

	// Unblock all
	close(done)
	runtime.Gosched()

	t.Logf("Spawn order: [1, 2, 3]")
	t.Logf("Execution order: %v", order)
	t.Logf("Expected (runnext LIFO): [3, 1, 2] or similar")
}
