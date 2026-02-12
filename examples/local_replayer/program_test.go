package localreplayer

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"testing/synctest"
)

// names is the goroutine creation order in Run().
var names = []string{"alice", "bob", "carol"}

// logAssignments prints the name → BGID → item mapping.
// BGIDs are assigned monotonically at creation, so sorting the worker
// BGIDs gives us creation order = names order.
func logAssignments(t *testing.T, label string, trace []synctest.Decision, result map[string]string) {
	t.Helper()

	// Collect worker BGIDs from the first multi-choice decision.
	var bgids []int
	for _, d := range trace {
		if d.RunqSize > 1 {
			for j := int32(0); j < d.RunqSize; j++ {
				bgids = append(bgids, int(d.RunqBgids[j]))
			}
			break
		}
	}
	sort.Ints(bgids)

	t.Logf("%s:", label)
	for i, name := range names {
		if i < len(bgids) {
			t.Logf("  %s = B%d → %s", name, bgids[i], result[name])
		} else {
			t.Logf("  %s → %s", name, result[name])
		}
	}
}

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

// TestRecord runs the program inside a bubble and prints the full trace
// with BGIDs at every scheduling decision.
func TestRecord(t *testing.T) {
	var result map[string]string
	trace := synctest.Test(t, func(t *testing.T) {
		result = Run(t.Logf)
	})

	logAssignments(t, "assignments", trace, result)
	logTrace(t, "scheduling trace", trace)

	if len(trace) < 3 {
		t.Fatalf("expected at least 3 decisions, got %d", len(trace))
	}
	t.Logf("recorded %d decisions", len(trace))
}

// TestReplay records a trace and replays it. Verifies:
//  1. Same number of decisions
//  2. Same BGID at every step (deterministic BGID assignment)
//  3. Same program output
func TestReplay(t *testing.T) {
	// Phase 1: Record.
	var result1 map[string]string
	trace1 := synctest.Test(t, func(t *testing.T) {
		result1 = Run(t.Logf)
	})

	t.Logf("recorded %d decisions", len(trace1))
	logTrace(t, "record", trace1)

	// Phase 2: Replay with the recorded trace as prefix.
	var result2 map[string]string
	trace2 := synctest.Test(t, func(t *testing.T) {
		result2 = Run(t.Logf)
	}, trace1)

	t.Logf("replayed %d decisions", len(trace2))
	logTrace(t, "replay", trace2)

	// Verify: same number of decisions.
	if len(trace1) != len(trace2) {
		t.Fatalf("decision count mismatch: recorded=%d replayed=%d",
			len(trace1), len(trace2))
	}

	// Verify: same BGID at every step.
	for i := range trace1 {
		if trace1[i].ChosenBgid != trace2[i].ChosenBgid {
			t.Errorf("step %d: BGID mismatch recorded=B%d replayed=B%d",
				i, trace1[i].ChosenBgid, trace2[i].ChosenBgid)
		}
	}

	// Verify: same program output.
	for k, v1 := range result1 {
		if v2, ok := result2[k]; !ok || v1 != v2 {
			t.Errorf("output mismatch for %s: recorded=%q replayed=%q", k, v1, v2)
		}
	}

	t.Logf("PASS: replay produced identical BGIDs and output")
}

// TestModifySchedule records a trace, modifies one decision to pick a
// different goroutine, and replays. Proves scheduling affects output.
func TestModifySchedule(t *testing.T) {
	// Phase 1: Record baseline.
	var result1 map[string]string
	trace1 := synctest.Test(t, func(t *testing.T) {
		result1 = Run(t.Logf)
	})
	logAssignments(t, "baseline assignments", trace1, result1)
	logTrace(t, "baseline", trace1)

	// Find first multi-choice decision point.
	modIdx := -1
	var altIndex int32
	for i, d := range trace1 {
		if d.RunqSize > 1 {
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
		t.Fatal("no multi-choice decision point found")
	}

	t.Logf("modifying step %d: index %d→%d (B%d instead of B%d)",
		trace1[modIdx].Step, trace1[modIdx].Index, altIndex,
		trace1[modIdx].RunqBgids[altIndex], trace1[modIdx].ChosenBgid)

	// Build modified prefix.
	prefix := make([]synctest.Decision, modIdx+1)
	copy(prefix, trace1[:modIdx+1])
	prefix[modIdx].Index = altIndex

	// Phase 2: Replay with modified prefix.
	var result2 map[string]string
	trace2 := synctest.Test(t, func(t *testing.T) {
		result2 = Run(t.Logf)
	}, prefix)

	logAssignments(t, "modified assignments", trace2, result2)
	logTrace(t, "modified", trace2)

	// Verify: at least one BGID or output differs.
	bgidDiffers := false
	minLen := len(trace1)
	if len(trace2) < minLen {
		minLen = len(trace2)
	}
	for i := 0; i < minLen; i++ {
		if trace1[i].ChosenBgid != trace2[i].ChosenBgid {
			bgidDiffers = true
			t.Logf("  step %d: B%d → B%d",
				trace1[i].Step, trace1[i].ChosenBgid, trace2[i].ChosenBgid)
		}
	}

	outputDiffers := false
	for k := range result1 {
		if result1[k] != result2[k] {
			outputDiffers = true
			break
		}
	}

	if !bgidDiffers && !outputDiffers {
		t.Fatal("modified prefix had no effect on scheduling or output")
	}

	t.Logf("PASS: modified schedule changed the interleaving")
}
