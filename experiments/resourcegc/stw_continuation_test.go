package resourcegc

import (
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/shubhaankar/synctest/explorer"
)

// Qualification is process-isolated by the external runner. A failed runtime
// is not reused for another case. These probes do not change the application.
func TestSTWContinuationLockedActor(t *testing.T)         { qualifyActorSTW(t, true, true) }
func TestSTWContinuationWithoutObservations(t *testing.T) { qualifyActorSTW(t, false, false) }

func qualifyActorSTW(t *testing.T, locked, observed bool) {
	runtime.GOMAXPROCS(2)
	debug.SetGCPercent(-1)
	driver := newGCTreatment(0, 1)
	var app Observation
	var treatment GCTreatmentResult
	var events []synctest.Observation
	var overflow bool
	r := explorer.RunWithHookResult(t, func(*testing.T) {
		if observed {
			synctest.EnableObservations()
		}
		app = RunApplicationWithObserver(func(tag uint32) {
			if locked && tag == TagCleanupEnter {
				runtime.LockOSThread()
			}
			driver.observe(tag)
			if locked && tag == TagCleanupReturn {
				runtime.UnlockOSThread()
			}
		})
		treatment = driver.join()
		if observed {
			events, overflow = synctest.Observations()
		}
	}, func(explorer.BubbleState) int32 { return 0 })
	if !r.OK || r.WorkerUnsafe || !app.Completed || treatment.CompletedSTW != 1 || overflow {
		t.Fatalf("qualification failed: OK=%v unsafe=%v app=%v STW=%d overflow=%v cause=%v", r.OK, r.WorkerUnsafe, app.Completed, treatment.CompletedSTW, overflow, r.Cause)
	}
	counts := map[uint32]int{}
	for _, d := range r.Trace {
		counts[d.ChosenBgid]++
	}
	if counts[3] != 1 || counts[4] != 1 {
		t.Fatalf("STW introduced actor decisions: worker=%d cleanup=%d", counts[3], counts[4])
	}
	if observed {
		continued := false
		for _, e := range events {
			if e.Kind == 9 && e.BGID == 4 {
				continued = true
			}
		}
		if !continued {
			t.Fatal("locked actor probe did not capture a continued STW interruption")
		}
	}
}

func TestSTWContinuationInsideHook(t *testing.T) {
	runtime.GOMAXPROCS(2)
	debug.SetGCPercent(-1)
	start, done := make(chan struct{}), make(chan struct{})
	var finished, inHook atomic.Bool
	var stats runtime.MemStats
	go func() { <-start; runtime.ReadMemStats(&stats); finished.Store(true); close(done) }()
	var app Observation
	var events []synctest.Observation
	var overflow bool
	first := true
	var pcs [8]uintptr
	r := explorer.RunWithHookResult(t, func(*testing.T) {
		synctest.EnableObservations()
		app = RunApplicationWithObserver(func(tag uint32) {
			if inHook.Load() {
				panic("application actor ran while hook was suspended")
			}
			synctest.RecordObservation(tag)
		})
		synctest.External(func() { <-done })
		events, overflow = synctest.Observations()
	}, func(explorer.BubbleState) int32 {
		if first {
			first = false
			inHook.Store(true)
			close(start)
			deadline := realNanotime() + 1e9
			for !finished.Load() {
				// Stack walking supplies cooperative safe points without
				// adding target decisions or filling the observation ring.
				runtime.Callers(0, pcs[:])
				if realNanotime() > deadline {
					panic("host STW did not finish during hook")
				}
			}
			inHook.Store(false)
		}
		return 0
	})
	if !r.OK || r.WorkerUnsafe || !app.Completed || overflow {
		t.Fatalf("hook qualification failed: OK=%v unsafe=%v app=%v overflow=%v cause=%v", r.OK, r.WorkerUnsafe, app.Completed, overflow, r.Cause)
	}
	preempted, continued := false, false
	for _, e := range symbolizeObservations(events) {
		if e.Kind == "preempt" && e.BGID == 0 && e.STWReason == 7 {
			preempted = true
		}
		if e.Kind == "world_stop_continued" && e.BGID == 0 {
			continued = true
		}
	}
	if !preempted || !continued {
		t.Fatalf("hook interruption not qualified: preempted=%v continued=%v", preempted, continued)
	}
}
