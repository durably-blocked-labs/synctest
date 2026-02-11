// Package replayerscheduler demonstrates finding a race bug by iterating
// scheduling interleavings. A task-processing program has a race between
// a preparer (computes output, sets done flag) and a processor (checks done
// flag, reads output). Under FIFO scheduling the preparer runs first
// (it's the most recently created goroutine → runnext). Under non-FIFO
// interleavings the processor may run before the preparer sets done.
package replayerscheduler

import (
	"runtime"
	"sync"
)

// Task represents a job being prepared and processed.
type Task struct {
	ID     int
	Input  string
	Output string
	Done   bool
}

// RunRace runs the task-processing race scenario.
//
// Goroutine creation order determines the initial runq layout:
//
//	B3 = padding1 (created first → runq position 1)
//	B4 = padding2 (created second → runq position 2)
//	B5 = processor (created third → runq position 3)
//	B6 = preparer (created last → runnext = position 0)
//
// Under FIFO (index 0), the preparer (B6) runs first and sets Done=true
// before the processor (B5) checks. To reach B5 at position 3, the
// explorer must try three alternative indices at step 0 (index 1, 2, 3),
// finding the race on the third modification.
//
// Returns (output, found). found=true means the processor saw Done=true.
func RunRace() (output string, found bool) {
	task := &Task{ID: 1, Input: "raw-data"}
	var wg sync.WaitGroup

	wg.Add(4)

	// B3: padding — occupies runq position 1.
	go func() {
		defer wg.Done()
		runtime.Gosched()
	}()

	// B4: padding — occupies runq position 2.
	go func() {
		defer wg.Done()
		runtime.Gosched()
	}()

	// B5: processor — occupies runq position 3.
	// Checks the done flag and reads output.
	go func() {
		defer wg.Done()
		if task.Done {
			output = task.Output
			found = true
		}
	}()

	// B6: preparer — runnext (position 0), runs first under FIFO.
	// Computes output, then sets done flag.
	go func() {
		defer wg.Done()
		task.Output = "processed:" + task.Input
		task.Done = true
	}()

	wg.Wait()
	return
}
