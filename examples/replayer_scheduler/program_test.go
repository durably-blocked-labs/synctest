package replayerscheduler

import (
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
)

func logTrace(t *testing.T, label string, trace []synctest.Decision) {
	t.Helper()
	t.Logf("%s (%d decisions):", label, len(trace))
	for _, d := range trace {
		bgids := make([]string, d.RunqSize)
		for j := int32(0); j < d.RunqSize; j++ {
			bgids[j] = fmt.Sprintf("B%d", d.RunqBgids[j])
		}
		t.Logf("  step=%-3d index=%d chosenBgid=B%-3d runqSize=%d runq=[%s]",
			d.Step, d.Index, d.ChosenBgid, d.RunqSize, strings.Join(bgids, ", "))
	}
}

// TestFindRace explores scheduling interleavings depth-first until it
// finds an interleaving that triggers the overdraft bug.
//
// The exploration uses context-bounded DFS: at each run, alternatives
// at NEW frontier decision points (past the prefix) are pushed onto
// a stack for future exploration. A "context bound" limits how many
// non-FIFO scheduling decisions any single interleaving may contain.
//
// Why depth > 1 is needed:
//
//	Under FIFO, the approver (runnext, created last) runs first and
//	opens the gate before any withdrawer starts. One non-FIFO decision
//	lets ONE withdrawer run before the approver — that withdrawer blocks
//	at the gate, the approver opens it, and only one withdrawal occurs.
//	The overdraft requires BOTH withdrawers to check balance=100 before
//	the approver opens the gate, which takes TWO non-FIFO decisions.
//
// Expected: the DFS finds the overdraft within 3 runs (FIFO baseline,
// one-withdrawer deviation, two-withdrawer deviation).
func TestFindRace(t *testing.T) {
	var lastBalance int
	var lastWithdrawals int

	// Workload: run the program, no assertions (we check externally).
	workload := func(t *testing.T) {
		lastBalance, lastWithdrawals = Run()
	}

	// Context bound: max non-FIFO decisions per interleaving.
	// The overdraft needs exactly 2 (both withdrawers before the approver).
	const bound = 2

	type work struct {
		prefix  []synctest.Decision
		nonFIFO int // number of non-zero indices in prefix
	}

	stack := []work{{}} // empty prefix = FIFO baseline
	runNum := 0

	for len(stack) > 0 {
		w := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		trace, _ := synctest.Explore(t, workload, w.prefix)

		label := fmt.Sprintf("Run %d", runNum)
		t.Logf("=== %s (prefix=%d decisions, nonFIFO=%d) ===", label, len(w.prefix), w.nonFIFO)
		logTrace(t, label, trace)
		t.Logf("%s: balance=%d withdrawals=%d", label, lastBalance, lastWithdrawals)
		runNum++

		if lastBalance < 0 {
			t.Logf("")
			t.Logf("========================================")
			t.Logf("OVERDRAFT FOUND on %s!", label)
			t.Logf("Both withdrawers checked balance=100 before")
			t.Logf("either subtracted → balance went negative.")
			t.Logf("========================================")
			return
		}

		// Push alternatives at frontier decisions (past the prefix).
		for i := len(w.prefix); i < len(trace); i++ {
			d := trace[i]
			if d.RunqSize <= 1 {
				continue
			}
			for alt := int32(0); alt < d.RunqSize; alt++ {
				if alt == d.Index {
					continue
				}
				newNonFIFO := w.nonFIFO
				if alt != 0 {
					newNonFIFO++
				}
				if newNonFIFO > bound {
					continue
				}
				newPrefix := make([]synctest.Decision, i+1)
				copy(newPrefix, trace[:i+1])
				newPrefix[i].Index = alt
				stack = append(stack, work{prefix: newPrefix, nonFIFO: newNonFIFO})
			}
		}
	}

	t.Fatalf("explored %d interleavings without finding overdraft (bound=%d)", runNum, bound)
}
