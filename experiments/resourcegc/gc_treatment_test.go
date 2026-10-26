package resourcegc

import (
	"runtime"
	"sync/atomic"
	"testing/synctest"
	_ "unsafe"
)

// realNanotime bypasses the bubble simulated clock. This test-only link is
// tied to the exact qualified runtime pin (runtime/time_nofake.go).
//
//go:linkname realNanotime runtime.nanotime
func realNanotime() int64

// CollectionWindow brackets a runtime.GC call, not the collector's internal
// phases. Actor overlap with this interval does not establish interruption.
type CollectionWindow struct {
	BeginNS int64 `json:"begin_ns"`
	EndNS   int64 `json:"end_ns"`
}

type ActorWindow struct {
	Actor   string `json:"actor"`
	BeginNS int64  `json:"begin_ns"`
	EndNS   int64  `json:"end_ns"`
}

type GCTreatmentResult struct {
	Requested    int                `json:"requested"`
	Completed    int                `json:"completed"`
	Calls        []CollectionWindow `json:"calls"`
	Actors       []ActorWindow      `json:"actors"`
	Scope        string             `json:"scope"`
	RequestedSTW int                `json:"requested_stw"`
	CompletedSTW int                `json:"completed_stw"`
	STWCalls     []CollectionWindow `json:"stw_calls"`
}

// The driver lives outside the bubble on the spare host P. Only the existing
// observer signals it; application control flow has no GC calls or new waits.
// Even the zero-GC control creates, signals and joins this same driver.
type gcTreatment struct {
	epoch     int64
	start     chan struct{}
	done      chan struct{}
	triggered atomic.Bool
	begin     [2]atomic.Int64
	end       [2]atomic.Int64
	result    GCTreatmentResult
	stats     runtime.MemStats
}

func newGCTreatment(count, stwProbes int) *gcTreatment {
	g := &gcTreatment{epoch: realNanotime(), start: make(chan struct{}), done: make(chan struct{}),
		result: GCTreatmentResult{Requested: count, Calls: make([]CollectionWindow, count),
			RequestedSTW: stwProbes, STWCalls: make([]CollectionWindow, stwProbes),
			Scope: "host-triggered forced collections; call windows are not GC-phase boundaries"}}
	go func() {
		<-g.start
		for i := range g.result.Calls {
			g.result.Calls[i].BeginNS = realNanotime() - g.epoch
			runtime.GC()
			g.result.Calls[i].EndNS = realNanotime() - g.epoch
			g.result.Completed++
		}
		for i := range g.result.STWCalls {
			g.result.STWCalls[i].BeginNS = realNanotime() - g.epoch
			// Causal control: the pinned runtime stops the world to read
			// statistics, without requesting a garbage collection.
			runtime.ReadMemStats(&g.stats)
			g.result.STWCalls[i].EndNS = realNanotime() - g.epoch
			g.result.CompletedSTW++
		}
		close(g.done)
	}()
	return g
}

func (g *gcTreatment) observe(tag uint32) {
	synctest.RecordObservation(tag)
	actor, enter, leave := -1, false, false
	switch tag {
	case TagWorkerEnter:
		actor, enter = 0, true
	case TagWorkerReturn:
		actor, leave = 0, true
	case TagCleanupEnter:
		actor, enter = 1, true
	case TagCleanupReturn:
		actor, leave = 1, true
	}
	if enter {
		g.begin[actor].Store(realNanotime() - g.epoch)
		if g.triggered.CompareAndSwap(false, true) {
			close(g.start)
		}
	} else if leave {
		g.end[actor].Store(realNanotime() - g.epoch)
	}
}

func (g *gcTreatment) join() GCTreatmentResult {
	// A host-owned channel needs the explicit external wrapper. Joining only
	// happens after application completion and does not gate an application op.
	synctest.External(func() { <-g.done })
	return g.completedResult()
}

// joinHost is called after a successful observed bubble returns. Channel-close
// synchronization covers the driver's results; this wait is outside its trace.
func (g *gcTreatment) joinHost() GCTreatmentResult {
	<-g.done
	return g.completedResult()
}

func (g *gcTreatment) completedResult() GCTreatmentResult {
	result := g.result
	for i, actor := range []string{"worker", "cleanup"} {
		result.Actors = append(result.Actors, ActorWindow{actor, g.begin[i].Load(), g.end[i].Load()})
	}
	return result
}
