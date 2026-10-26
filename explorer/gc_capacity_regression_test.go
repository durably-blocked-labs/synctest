package explorer

import (
	"runtime"
	"testing"
)

func TestExploreProvidesHostCapacityForGC(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	completed := false
	result := Explore(t, func(t *testing.T) {
		if runtime.GOMAXPROCS(0) < 2 {
			t.Fatal("no spare host processor")
		}
		runtime.GC()
		completed = true
	}, &Random{Seed: 1}, MaxRuns(1))
	if !result.Successful() || result.Runs != 1 || result.Cause != nil || !completed {
		t.Fatalf("GC workload incomplete: result=%+v completed=%v", result, completed)
	}
	if got := runtime.GOMAXPROCS(0); got != 1 {
		t.Fatalf("processor setting not restored: %d", got)
	}
}
