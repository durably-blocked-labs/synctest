// Package localreplayer demonstrates record/replay of scheduling decisions
// using synctest bubbles. A concurrent program's scheduling trace can be
// recorded and replayed to verify determinism, or modified to explore
// alternative interleavings.
package localreplayer

import (
	"fmt"
	"runtime"
	"sync"
)

// Run executes a concurrent workload where three workers race to process
// tasks from a shared queue. The order in which workers pick up tasks
// depends on the scheduler's goroutine ordering, making the output
// scheduling-dependent.
//
// Returns a log of events in the order they occurred.
func Run() []string {
	var mu sync.Mutex
	var log []string

	appendLog := func(msg string) {
		mu.Lock()
		log = append(log, msg)
		mu.Unlock()
	}

	// Shared task queue. Workers pull tasks from this slice.
	tasks := []string{"alpha", "beta", "gamma", "delta"}
	var taskMu sync.Mutex
	nextTask := 0

	claimTask := func() (string, bool) {
		taskMu.Lock()
		defer taskMu.Unlock()
		if nextTask >= len(tasks) {
			return "", false
		}
		t := tasks[nextTask]
		nextTask++
		return t, true
	}

	var wg sync.WaitGroup

	// Launch three workers. Each worker tries to claim and process tasks.
	for w := 1; w <= 3; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			appendLog(fmt.Sprintf("worker-%d:start", id))
			runtime.Gosched() // decision point: all workers may be runnable

			for {
				task, ok := claimTask()
				if !ok {
					break
				}
				appendLog(fmt.Sprintf("worker-%d:process(%s)", id, task))
				runtime.Gosched() // decision point between tasks
			}

			appendLog(fmt.Sprintf("worker-%d:done", id))
		}(w)
	}

	wg.Wait()
	return log
}
