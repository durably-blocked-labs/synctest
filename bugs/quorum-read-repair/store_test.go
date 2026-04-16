package quorumreadrepair

import (
	"reflect"
	"testing"
)

func TestCorrectMergePreservesConcurrentSiblings(t *testing.T) {
	existing := []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}}
	incoming := []VersionedValue{{Value: "B", Clock: Clock{"C2": 1}}}

	got := mergeSiblings(existing, incoming)

	assertValues(t, got, []VersionedValue{
		{Value: "A", Clock: Clock{"C1": 1}},
		{Value: "B", Clock: Clock{"C2": 1}},
	})
}

func TestCorrectMergeDropsDominatedValue(t *testing.T) {
	existing := []VersionedValue{{Value: "old", Clock: Clock{"C1": 1}}}
	incoming := []VersionedValue{{Value: "new", Clock: Clock{"C1": 2}}}

	got := mergeSiblings(existing, incoming)

	assertValues(t, got, []VersionedValue{
		{Value: "new", Clock: Clock{"C1": 2}},
	})
}

func TestBuggyRepairMergeDropsConcurrentSibling(t *testing.T) {
	existing := []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}}
	incoming := []VersionedValue{{Value: "B", Clock: Clock{"C2": 1}}}

	got := buggyRepairMerge(existing, incoming)

	if len(got) != 1 {
		t.Fatalf("buggyRepairMerge returned %d values, want 1", len(got))
	}
}

func assertValues(t *testing.T, got, want []VersionedValue) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(got) = %d, want %d; got=%#v want=%#v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Value != want[i].Value {
			t.Fatalf("got[%d].Value = %q, want %q; got=%#v want=%#v", i, got[i].Value, want[i].Value, got, want)
		}
		if !reflect.DeepEqual(got[i].Clock, want[i].Clock) {
			t.Fatalf("got[%d].Clock = %#v, want %#v; got=%#v want=%#v", i, got[i].Clock, want[i].Clock, got, want)
		}
	}
}
