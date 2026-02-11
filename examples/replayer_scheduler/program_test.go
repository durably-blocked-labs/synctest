package replayerscheduler

// This test demonstrates finding a race bug by iterating scheduling
// interleavings. It runs the task-processing program under FIFO (default),
// then tries alternative goroutine choices at each multi-choice decision
// point until it finds an interleaving that exposes the race.
//
// Run:
//   GOROOT=../../go GODEBUG=asyncpreemptoff=1 ../../go/bin/go test -v -count=1 .
//
// The test is EXPECTED TO FAIL — it demonstrates finding the bug.
// Expected output: ~4 runs (baseline + 3 modifications before bug found).

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

func TestFindRace(t *testing.T) {
	var lastOutput string
	var lastFound bool

	workload := func(t *testing.T) {
		lastOutput, lastFound = RunRace()
		if !lastFound {
			t.Errorf("RACE: processor saw Done=false (preparer had not finished)")
		} else if lastOutput != "processed:raw-data" {
			t.Errorf("RACE: wrong output: %q", lastOutput)
		}
	}

	// ------------------------------------------------------------------
	// Run 0: FIFO baseline. Preparer (B6, runnext) runs first → PASS.
	// ------------------------------------------------------------------
	t.Log("=== Run 0: FIFO baseline ===")
	baseTrace, ok := synctest.Explore(t, workload, nil)
	if !ok {
		t.Fatalf("FIFO baseline FAILED (unexpected): found=%v output=%q", lastFound, lastOutput)
	}
	logTrace(t, "Run 0 trace", baseTrace)
	t.Logf("Run 0: PASSED (found=%v output=%q)", lastFound, lastOutput)

	// ------------------------------------------------------------------
	// Iterate: at each multi-choice decision point, try every alternative
	// index (1, 2, ..., runqSize-1). Move to the next step only after
	// exhausting all alternatives at the current step. The race is found
	// when the processor (deep in the runq) is picked before the preparer.
	// ------------------------------------------------------------------
	runNum := 1
	for i, d := range baseTrace {
		if d.RunqSize <= 1 || d.Index != 0 {
			continue
		}

		for alt := int32(1); alt < d.RunqSize; alt++ {
			prefix := make([]synctest.Decision, i+1)
			copy(prefix, baseTrace[:i+1])
			prefix[i].Index = alt

			altBgid := d.RunqBgids[alt]
			t.Logf("=== Run %d: step %d index %d→%d (pick B%d instead of B%d) ===",
				runNum, d.Step, d.Index, alt, altBgid, d.ChosenBgid)

			trace, ok := synctest.Explore(t, workload, prefix)
			logTrace(t, fmt.Sprintf("Run %d trace", runNum), trace)
			t.Logf("Run %d: found=%v output=%q ok=%v", runNum, lastFound, lastOutput, ok)

			if !ok {
				t.Logf("")
				t.Logf("========================================")
				t.Logf("RACE FOUND on run %d!", runNum)
				t.Logf("Changed step %d: picked B%d instead of B%d",
					d.Step, altBgid, d.ChosenBgid)
				t.Logf("Processor ran before preparer set Done=true.")
				t.Logf("========================================")
				t.Fatalf("RACE FOUND: found=%v output=%q on run %d (step %d, index %d)",
					lastFound, lastOutput, runNum, d.Step, alt)
			}

			t.Logf("Run %d: PASSED", runNum)
			runNum++
		}
	}

	t.Logf("explored %d schedule modifications without finding the race", runNum-1)
}
