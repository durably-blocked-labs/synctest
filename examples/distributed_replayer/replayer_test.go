package distributedreplayer

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
)

// GlobalEvent represents one event in the unified global schedule.
// Only network (store) operations and node completions are tracked;
// intra-bubble scheduling decisions are handled automatically (FIFO).
type GlobalEvent struct {
	Node     string // "A" or "B"
	Type     string // "net" (store operation) or "done" (node completed)
	NetOp    string // "GET" or "SET" (for net events)
	NetKey   string // key (for net events)
	NetValue string // value sent/received (for net events)
}

func (e GlobalEvent) String() string {
	switch e.Type {
	case "done":
		return fmt.Sprintf("{%s done}", e.Node)
	case "net":
		return fmt.Sprintf("{%s %s %s=%q}", e.Node, e.NetOp, e.NetKey, e.NetValue)
	default:
		return fmt.Sprintf("{%s %s}", e.Node, e.Type)
	}
}

// bubbleCtrl connects a bubble's hook and network channels to the orchestrator.
type bubbleCtrl struct {
	name   string
	req    chan synctest.BubbleState // hook -> auto-responder (buffered 1)
	resp   chan int32                // auto-responder -> hook (buffered 1)
	done   chan struct{}             // bubble -> orchestrator (unbuffered)
	outbox chan Request              // node -> orchestrator: store requests (unbuffered)
	inbox  chan Response             // orchestrator -> node: store responses (buffered 1)
}

func newBubbleCtrl(name string) *bubbleCtrl {
	return &bubbleCtrl{
		name:   name,
		req:    make(chan synctest.BubbleState, 1),
		resp:   make(chan int32, 1),
		done:   make(chan struct{}),
		outbox: make(chan Request),    // unbuffered: node blocks until orch reads
		inbox:  make(chan Response, 1), // buffered 1: orch can pre-load response
	}
}

// makeHook returns a decision hook that forwards state to the auto-responder.
func makeHook(ctrl *bubbleCtrl) func(synctest.BubbleState) int32 {
	return func(state synctest.BubbleState) int32 {
		ctrl.req <- state
		return <-ctrl.resp
	}
}

// startAutoResponder launches a goroutine that automatically responds to
// all hook requests with FIFO (index 0). Hook events are not part of the
// global schedule — they're infrastructure. Stops when done is closed.
func startAutoResponder(ctrl *bubbleCtrl) {
	go func() {
		for {
			select {
			case <-ctrl.req:
				ctrl.resp <- 0 // always FIFO
			case <-ctrl.done:
				return
			}
		}
	}()
}

// runBubble launches a synctest bubble that runs IncrementCounter.
func runBubble(t *testing.T, ctrl *bubbleCtrl) []synctest.Decision {
	io := NodeIO{
		Outbox: ctrl.outbox,
		Inbox:  ctrl.inbox,
	}
	trace := synctest.Test(t, func(t *testing.T) {
		synctest.SetDecisionHook(makeHook(ctrl))
		IncrementCounter(io)
	})
	return trace
}

// --- Orchestrator ---

// serviceRequest reads one store request from the node's outbox,
// processes it against the store, sends the response to the node's inbox,
// and records the event.
func serviceRequest(
	ctrl *bubbleCtrl,
	store *Store,
	logf func(string, ...any),
) GlobalEvent {
	req := <-ctrl.outbox
	logf("  %s: %s %s", ctrl.name, req.Method, req.Key)

	var resp Response
	var ev GlobalEvent

	switch req.Method {
	case "GET":
		v := store.data[req.Key]
		resp = Response{Value: v, OK: v != ""}
		ev = GlobalEvent{
			Node: ctrl.name, Type: "net",
			NetOp: "GET", NetKey: req.Key, NetValue: v,
		}
	case "SET":
		store.data[req.Key] = req.Value
		resp = Response{OK: true}
		ev = GlobalEvent{
			Node: ctrl.name, Type: "net",
			NetOp: "SET", NetKey: req.Key, NetValue: req.Value,
		}
	}

	ctrl.inbox <- resp
	return ev
}

// waitDone waits for a node to signal completion and returns the event.
func waitDone(ctrl *bubbleCtrl) GlobalEvent {
	<-ctrl.done
	return GlobalEvent{Node: ctrl.name, Type: "done"}
}

// recordSequential records a global schedule by running A's store ops
// to completion before B's. Each node does GET then SET, so:
//   - A: GET(counter=""), SET(counter="1")
//   - B: GET(counter="1"), SET(counter="2")
//
// Result: counter=2 (no race).
func recordSequential(
	ctrlA, ctrlB *bubbleCtrl,
	store *Store,
	logf func(string, ...any),
) []GlobalEvent {
	var trace []GlobalEvent

	// Drain A: GET, SET, done.
	logf("orch: === drain A ===")
	trace = append(trace, serviceRequest(ctrlA, store, logf))
	trace = append(trace, serviceRequest(ctrlA, store, logf))
	trace = append(trace, waitDone(ctrlA))

	// Drain B: GET, SET, done.
	logf("orch: === drain B ===")
	trace = append(trace, serviceRequest(ctrlB, store, logf))
	trace = append(trace, serviceRequest(ctrlB, store, logf))
	trace = append(trace, waitDone(ctrlB))

	return trace
}

// replaySchedule follows a global schedule exactly.
func replaySchedule(
	ctrlA, ctrlB *bubbleCtrl,
	store *Store,
	schedule []GlobalEvent,
	logf func(string, ...any),
) []GlobalEvent {
	var trace []GlobalEvent

	for i, ev := range schedule {
		ctrl := ctrlA
		if ev.Node == "B" {
			ctrl = ctrlB
		}

		switch ev.Type {
		case "done":
			logf("  replay[%d]: waiting %s done", i, ev.Node)
			trace = append(trace, waitDone(ctrl))
			logf("  replay[%d]: %s done", i, ev.Node)

		case "net":
			logf("  replay[%d]: waiting %s %s", i, ev.Node, ev.NetOp)
			got := serviceRequest(ctrl, store, logf)
			trace = append(trace, got)
		}
	}

	return trace
}

// interleaveSchedule reorders a sequential schedule into an interleaved
// one where both nodes read before either writes.
//
// Sequential: A-GET, A-SET, A-done, B-GET, B-SET, B-done
// Interleaved: A-GET, B-GET, A-SET, B-SET, A-done, B-done
//
// The interleaved order causes both nodes to read "" (counter not set),
// so both write "1" — exposing the lost-update bug (counter=1 instead of 2).
func interleaveSchedule(original []GlobalEvent) []GlobalEvent {
	var aEvents, bEvents []GlobalEvent
	for _, ev := range original {
		switch ev.Node {
		case "A":
			aEvents = append(aEvents, ev)
		case "B":
			bEvents = append(bEvents, ev)
		}
	}

	// Round-robin interleave.
	var interleaved []GlobalEvent
	ai, bi := 0, 0
	for ai < len(aEvents) || bi < len(bEvents) {
		if ai < len(aEvents) {
			interleaved = append(interleaved, aEvents[ai])
			ai++
		}
		if bi < len(bEvents) {
			interleaved = append(interleaved, bEvents[bi])
			bi++
		}
	}
	return interleaved
}

// --- Tests ---

func TestDistributedReplay(t *testing.T) {
	runtime.GOMAXPROCS(4)

	var globalTrace []GlobalEvent

	// Run 1: Record — sequential (drain A then B), expect counter=2.
	t.Run("record", func(t *testing.T) {
		store := NewStore()
		ctrlA := newBubbleCtrl("A")
		ctrlB := newBubbleCtrl("B")

		startAutoResponder(ctrlA)
		startAutoResponder(ctrlB)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			runBubble(t, ctrlA)
			close(ctrlA.done)
		}()
		go func() {
			defer wg.Done()
			runBubble(t, ctrlB)
			close(ctrlB.done)
		}()

		globalTrace = recordSequential(ctrlA, ctrlB, store, t.Logf)
		wg.Wait()

		counter := store.data["counter"]
		t.Logf("counter=%s", counter)
		logSchedule(t, "global", globalTrace)

		if counter != "2" {
			t.Fatalf("expected counter=2 (sequential), got %s", counter)
		}
	})

	if t.Failed() {
		return
	}

	// Run 2: Replay — same schedule, expect same result.
	t.Run("replay", func(t *testing.T) {
		store := NewStore()
		ctrlA := newBubbleCtrl("A")
		ctrlB := newBubbleCtrl("B")

		startAutoResponder(ctrlA)
		startAutoResponder(ctrlB)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			runBubble(t, ctrlA)
			close(ctrlA.done)
		}()
		go func() {
			defer wg.Done()
			runBubble(t, ctrlB)
			close(ctrlB.done)
		}()

		replayTrace := replaySchedule(ctrlA, ctrlB, store, globalTrace, t.Logf)
		wg.Wait()

		counter := store.data["counter"]
		t.Logf("counter=%s", counter)
		logSchedule(t, "replay", replayTrace)

		if counter != "2" {
			t.Fatalf("replay: expected counter=2, got %s", counter)
		}
	})

	if t.Failed() {
		return
	}

	// Run 3: Modified replay — interleaved schedule, expect counter=1 (bug).
	t.Run("modified_replay", func(t *testing.T) {
		modified := interleaveSchedule(globalTrace)
		t.Logf("=== Modified (interleaved) schedule ===")
		logSchedule(t, "modified", modified)

		store := NewStore()
		ctrlA := newBubbleCtrl("A")
		ctrlB := newBubbleCtrl("B")

		startAutoResponder(ctrlA)
		startAutoResponder(ctrlB)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			runBubble(t, ctrlA)
			close(ctrlA.done)
		}()
		go func() {
			defer wg.Done()
			runBubble(t, ctrlB)
			close(ctrlB.done)
		}()

		replayTrace := replaySchedule(ctrlA, ctrlB, store, modified, t.Logf)
		wg.Wait()

		counter := store.data["counter"]
		t.Logf("counter=%s (expected 1 — lost update bug)", counter)
		logSchedule(t, "actual", replayTrace)

		if counter != "1" {
			t.Fatalf("modified replay: expected counter=1 (lost update), got %s", counter)
		}
	})
}

func logSchedule(t *testing.T, label string, trace []GlobalEvent) {
	t.Helper()
	t.Logf("%s schedule (%d events):", label, len(trace))
	for i, ev := range trace {
		t.Logf("  [%d] %s", i, ev)
	}
}
