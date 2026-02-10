package examples

// This file demonstrates how to build a distributed simulation orchestrator
// using synctest bubbles. Each bubble represents one node. The orchestrator
// controls cross-node scheduling decisions via channel-based hooks.
//
// The pattern:
//   1. Each bubble gets a BubbleControl channel pair (Req/Resp).
//   2. The hook checks if any runnable goroutine is global.
//   3. Local-only decisions return 0 (FIFO) immediately.
//   4. Global decisions are forwarded to the orchestrator, which responds.
//
// This is a reference implementation for the partner's distributed
// simulation testing system.

import (
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
)

// BubbleControl is the channel pair connecting a bubble's hook to the orchestrator.
type BubbleControl struct {
	ID   int
	Req  chan synctest.BubbleState // hook → orchestrator: bubble state at decision point
	Resp chan int32                // orchestrator → hook: index to schedule
	Done chan struct{}             // bubble → orchestrator: bubble finished
}

// makeHook returns a decision hook that:
//   - handles local-only decisions immediately (FIFO)
//   - forwards global decisions to the orchestrator via the channel pair
func makeHook(ctrl *BubbleControl) func(synctest.BubbleState) int32 {
	return func(state synctest.BubbleState) int32 {
		// Check if any runnable goroutine is global.
		hasGlobal := false
		for i := int32(0); i < state.RunnableN; i++ {
			if state.RunnableGlob[i] {
				hasGlobal = true
				break
			}
		}

		// Local-only: no orchestrator involvement needed.
		if !hasGlobal {
			return 0 // FIFO
		}

		// Global: ask orchestrator to decide.
		ctrl.Req <- state
		return <-ctrl.Resp
	}
}

// TestDistributedSimulation demonstrates two bubbles (nodes) with a
// shared orchestrator controlling cross-node scheduling.
//
// Node A: has a local goroutine and a "global" goroutine (simulating RPC).
// Node B: has a local goroutine and a "global" goroutine.
// Orchestrator: receives global decisions from both, picks FIFO (index 0).
//
// Run: GODEBUG=asyncpreemptoff=1 ../go/bin/go test -v -run TestDistributedSimulation .
func TestDistributedSimulation(t *testing.T) {
	ctrl0 := &BubbleControl{
		ID:   0,
		Req:  make(chan synctest.BubbleState, 1),
		Resp: make(chan int32, 1),
		Done: make(chan struct{}),
	}
	ctrl1 := &BubbleControl{
		ID:   1,
		Req:  make(chan synctest.BubbleState, 1),
		Resp: make(chan int32, 1),
		Done: make(chan struct{}),
	}

	// Orchestrator: receives global decisions from both bubbles.
	// In a real system, this would be a gRPC proxy making cross-node
	// scheduling decisions. Here we just pick FIFO (index 0) always.
	var orchestratorCalls int
	var orchestratorMu sync.Mutex
	orchDone := make(chan struct{})
	go func() {
		defer close(orchDone)
		done0, done1 := false, false
		for !done0 || !done1 {
			select {
			case state := <-ctrl0.Req:
				orchestratorMu.Lock()
				orchestratorCalls++
				t.Logf("orchestrator: bubble 0, step=%d, runnableN=%d, bgids=%v, global=%v",
					state.Step, state.RunnableN,
					state.RunnableBgid[:state.RunnableN],
					state.RunnableGlob[:state.RunnableN])
				orchestratorMu.Unlock()
				ctrl0.Resp <- 0 // FIFO
			case state := <-ctrl1.Req:
				orchestratorMu.Lock()
				orchestratorCalls++
				t.Logf("orchestrator: bubble 1, step=%d, runnableN=%d, bgids=%v, global=%v",
					state.Step, state.RunnableN,
					state.RunnableBgid[:state.RunnableN],
					state.RunnableGlob[:state.RunnableN])
				orchestratorMu.Unlock()
				ctrl1.Resp <- 0 // FIFO
			case <-ctrl0.Done:
				done0 = true
			case <-ctrl1.Done:
				done1 = true
			}
		}
	}()

	// Node A workload
	nodeA := func(t *testing.T) {
		synctest.SetDecisionHook(makeHook(ctrl0))

		var wg sync.WaitGroup

		// Local goroutine (not global)
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.Gosched()
		}()

		// Global goroutine (simulates RPC handler)
		wg.Add(1)
		go func() {
			synctest.CallExternal(func() {
				// In a real system, this would be an RPC call.
				// The orchestrator sees this goroutine as global.
				runtime.Gosched()
			})
			wg.Done()
		}()

		wg.Wait()
	}

	// Node B workload
	nodeB := func(t *testing.T) {
		synctest.SetDecisionHook(makeHook(ctrl1))

		var wg sync.WaitGroup

		// Local goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.Gosched()
		}()

		// Global goroutine
		wg.Add(1)
		go func() {
			synctest.CallExternal(func() {
				runtime.Gosched()
			})
			wg.Done()
		}()

		wg.Wait()
	}

	// Run both bubbles in parallel — each is pinned to its own P.
	var bubbleWg sync.WaitGroup
	var trace0, trace1 []synctest.Decision

	bubbleWg.Add(2)
	go func() {
		defer bubbleWg.Done()
		trace0 = synctest.Test(t, nodeA)
		ctrl0.Done <- struct{}{}
	}()
	go func() {
		defer bubbleWg.Done()
		trace1 = synctest.Test(t, nodeB)
		ctrl1.Done <- struct{}{}
	}()
	bubbleWg.Wait()
	<-orchDone

	t.Logf("bubble 0: %d decisions", len(trace0))
	for i, d := range trace0 {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d",
			i, d.Index, d.ChosenBgid, d.RunqSize)
	}
	t.Logf("bubble 1: %d decisions", len(trace1))
	for i, d := range trace1 {
		t.Logf("  step=%d index=%d chosenBgid=B%d runqSize=%d",
			i, d.Index, d.ChosenBgid, d.RunqSize)
	}

	orchestratorMu.Lock()
	calls := orchestratorCalls
	orchestratorMu.Unlock()

	if calls == 0 {
		t.Fatal("orchestrator was never called — global goroutines not triggering forwarding")
	}
	t.Logf("PASS: orchestrator called %d times across both bubbles (parallel)", calls)
}
