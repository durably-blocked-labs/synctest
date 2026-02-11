package localreplayer

import (
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
)

// TestRecord runs the concurrent program inside a bubble, records
// the full scheduling trace, and prints every decision with bgids,
// step numbers, and runq state.
func TestRecord(t *testing.T) {
	var output []string
	trace := synctest.Test(t, func(t *testing.T) {
		output = Run()
	})

	t.Logf("program output (%d entries):", len(output))
	for i, entry := range output {
		t.Logf("  [%d] %s", i, entry)
	}

	t.Logf("\nscheduling trace (%d decisions):", len(trace))
	for _, d := range trace {
		bgids := make([]string, d.RunqSize)
		for j := int32(0); j < d.RunqSize; j++ {
			bgids[j] = fmt.Sprintf("B%d", d.RunqBgids[j])
		}
		t.Logf("  step=%-3d index=%d chosenBgid=B%-3d runqSize=%d runq=[%s]",
			d.Step, d.Index, d.ChosenBgid, d.RunqSize, strings.Join(bgids, ", "))
	}

	if len(trace) < 3 {
		t.Fatalf("expected at least 3 scheduling decisions, got %d", len(trace))
	}
	t.Logf("\nrecorded %d decisions successfully", len(trace))
}

// TestReplay records a trace and replays it as a prefix. Verifies:
//  1. Same number of decisions.
//  2. Same bgids at every step (deterministic replay).
//  3. Same program output.
func TestReplay(t *testing.T) {
	// Phase 1: Record.
	var output1 []string
	trace1 := synctest.Test(t, func(t *testing.T) {
		output1 = Run()
	})

	t.Logf("recorded trace: %d decisions", len(trace1))

	// Phase 2: Replay with the recorded trace as prefix.
	var output2 []string
	trace2 := synctest.Test(t, func(t *testing.T) {
		output2 = Run()
	}, trace1)

	t.Logf("replayed trace: %d decisions", len(trace2))

	// Verify 1: Same number of decisions.
	if len(trace1) != len(trace2) {
		t.Fatalf("decision count mismatch: recorded=%d replayed=%d", len(trace1), len(trace2))
	}

	// Verify 2: Same bgids at every step.
	for i := range trace1 {
		if trace1[i].ChosenBgid != trace2[i].ChosenBgid {
			t.Errorf("step %d: bgid mismatch: recorded=B%d replayed=B%d",
				trace1[i].Step, trace1[i].ChosenBgid, trace2[i].ChosenBgid)
		}
		if trace1[i].Index != trace2[i].Index {
			t.Errorf("step %d: index mismatch: recorded=%d replayed=%d",
				trace1[i].Step, trace1[i].Index, trace2[i].Index)
		}
	}

	// Verify 3: Same program output.
	if len(output1) != len(output2) {
		t.Fatalf("output length mismatch: recorded=%d replayed=%d", len(output1), len(output2))
	}
	for i := range output1 {
		if output1[i] != output2[i] {
			t.Errorf("output[%d] mismatch: recorded=%q replayed=%q", i, output1[i], output2[i])
		}
	}

	t.Logf("replay matches: %d decisions, %d output entries", len(trace1), len(output1))
}

// TestModifySchedule records a trace, finds a decision point with
// RunqSize > 1, modifies the prefix to choose a different goroutine,
// and replays. Verifies the modified replay produces at least one
// different bgid (different scheduling) or different program output.
func TestModifySchedule(t *testing.T) {
	// Phase 1: Record baseline.
	var output1 []string
	trace1 := synctest.Test(t, func(t *testing.T) {
		output1 = Run()
	})

	t.Logf("baseline: %d decisions, %d output entries", len(trace1), len(output1))

	// Find the first decision point with RunqSize > 1 where we can
	// choose a different goroutine.
	modIdx := -1
	var altIndex int32
	for i, d := range trace1 {
		if d.RunqSize > 1 {
			// Pick an alternative index (different from the one chosen).
			for candidate := int32(0); candidate < d.RunqSize; candidate++ {
				if candidate != d.Index {
					modIdx = i
					altIndex = candidate
					break
				}
			}
			if modIdx >= 0 {
				break
			}
		}
	}
	if modIdx < 0 {
		t.Fatal("no decision point with RunqSize > 1 found -- program has no scheduling choices")
	}

	t.Logf("modifying step %d: index %d -> %d (choosing B%d instead of B%d)",
		trace1[modIdx].Step, trace1[modIdx].Index, altIndex,
		trace1[modIdx].RunqBgids[altIndex], trace1[modIdx].ChosenBgid)

	// Build modified prefix: same as baseline up to modIdx, but with
	// the modified index at modIdx. After the modified prefix, the
	// scheduler falls back to FIFO.
	modifiedPrefix := make([]synctest.Decision, modIdx+1)
	copy(modifiedPrefix, trace1[:modIdx+1])
	modifiedPrefix[modIdx].Index = altIndex

	// Phase 2: Replay with modified prefix.
	var output2 []string
	trace2 := synctest.Test(t, func(t *testing.T) {
		output2 = Run()
	}, modifiedPrefix)

	t.Logf("modified replay: %d decisions", len(trace2))

	// Verify: at least one bgid differs OR program output differs.
	bgidDiffers := false
	minLen := len(trace1)
	if len(trace2) < minLen {
		minLen = len(trace2)
	}
	for i := 0; i < minLen; i++ {
		if trace1[i].ChosenBgid != trace2[i].ChosenBgid {
			bgidDiffers = true
			t.Logf("  step %d: bgid changed B%d -> B%d",
				trace1[i].Step, trace1[i].ChosenBgid, trace2[i].ChosenBgid)
		}
	}
	if len(trace1) != len(trace2) {
		bgidDiffers = true
		t.Logf("  decision count changed: %d -> %d", len(trace1), len(trace2))
	}

	outputDiffers := false
	if len(output1) != len(output2) {
		outputDiffers = true
	} else {
		for i := range output1 {
			if output1[i] != output2[i] {
				outputDiffers = true
				break
			}
		}
	}

	if !bgidDiffers && !outputDiffers {
		t.Fatal("modified prefix produced identical trace AND output -- modification had no effect")
	}

	if bgidDiffers {
		t.Logf("scheduling changed (at least one bgid differs)")
	}
	if outputDiffers {
		t.Logf("program output changed:")
		t.Logf("  baseline: %v", output1)
		t.Logf("  modified: %v", output2)
	}

	t.Logf("modification at step %d successfully altered the interleaving", trace1[modIdx].Step)
}
