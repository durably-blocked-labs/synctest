package orchestrator

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestDistributedRequiresSpareHostPBeforeStart(t *testing.T) {
	saved := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(saved)
	started := false
	sink := &failingTrialSink{}
	result := New().ExploreWith(t, func(o *Orchestrator) {
		for _, name := range []string{"a", "b"} {
			tr := &attachedPublicationTransport{stubTransport: newStubTransport(name)}
			o.AddNode(tr, func(*testing.T) { started = true })
		}
	}, &Random{}, GlobalMaxRuns(1), WithTrialSink(sink))
	cause, ok := sink.last.Cause.(ConfigurationError)
	if !ok || cause.Available != 2 || cause.Required != 3 || started || result.Incomplete != 1 || sink.ends != 1 || sink.last.WorkerUnsafe || sink.last.Outcome != OutcomeInfrastructure || !WorkerReusable() {
		t.Fatalf("result=%+v terminal=%+v started=%t", result, sink.last, started)
	}
}

type detachedPublicationTransport struct {
	*stubTransport
	publications Publications
}

func (t *detachedPublicationTransport) PublicationPolicy() string { return "detached-post-park/v1" }
func (t *detachedPublicationTransport) PendingPublications() int  { return t.publications.Pending() }

func TestDetachedPublicationRejectedBeforeStart(t *testing.T) {
	saved := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(saved)
	for _, legacy := range []bool{false, true} {
		started := false
		sink := &failingTrialSink{}
		r := New().ExploreWith(t, func(o *Orchestrator) {
			var tr NodeTransport = newStubTransport("a")
			if !legacy {
				tr = &detachedPublicationTransport{stubTransport: tr.(*stubTransport)}
			}
			o.AddNode(tr, func(*testing.T) { started = true })
		}, &Random{}, GlobalMaxRuns(2), WithTrialSink(sink))
		if r.Successful() || r.Runs != 1 || r.Incomplete != 1 || started || sink.last.Outcome != OutcomeUnsupported || sink.starts != 1 || sink.ends != 1 {
			t.Fatalf("legacy=%t result=%+v started=%t terminal=%+v", legacy, r, started, sink.last)
		}
		if _, ok := sink.last.Cause.(UnsupportedTransportError); !ok {
			t.Fatalf("untyped rejection: %T", sink.last.Cause)
		}
	}
}

type attachedPublicationTransport struct {
	*stubTransport
	publications Publications
}

func (t *attachedPublicationTransport) PublicationPolicy() string { return AttachedPublicationV1 }
func (t *attachedPublicationTransport) PendingPublications() int  { return t.publications.Pending() }

func TestAttachedNoWorkDeliveryPrecedesTime(t *testing.T) {
	saved := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(saved)
	tr := &attachedPublicationTransport{stubTransport: newStubTransport("a")}
	sink := &failingTrialSink{}
	delivered := false
	o := New()
	result := o.ExploreWith(t, func(o *Orchestrator) {
		o.AddNode(tr, func(t *testing.T) {
			id, complete := tr.publications.Begin("a")
			tr.outbox <- &PendingOp{From: "a", To: "a", Type: "no-work", Occurrence: id, Deliver: func(context.Context) (DeliveryReceipt, error) {
				if o.globalTime != 0 {
					panic("time advanced before attached publication")
				}
				delivered = true
				return DeliveryReceipt{}, nil
			}}
			complete()
			time.Sleep(time.Hour)
			if !delivered {
				t.Error("timer overtook delivery")
			}
		})
	}, &Random{}, GlobalMaxRuns(1), WithTrialSink(sink))
	if !result.Successful() || !delivered {
		t.Fatalf("%+v", result)
	}
	events := sink.last.GlobalTrace
	if len(events) < 2 || events[0].Type != StepDeliver || events[1].Type != StepTimeAdvance {
		t.Fatalf("delivery-before-time changed: %+v", events)
	}
}

// This gated PROCESS characterization retains the cause of the failed detached
// publication barrier: readiness registers the callback continuation on its
// frozen owner queue, but does not run Complete. Qualified execution rejects it.
func TestUnsupportedDetachedPublicationCharacterization(t *testing.T) {
	if os.Getenv("BUBBLES_DETACHED_CHARACTERIZATION") != "1" {
		t.Skip("unsupported detached post-park publication; explicit subprocess characterization only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDetachedPublicationWorker$", "-test.timeout=8s")
	cmd.Env = append(os.Environ(), "BUBBLES_DETACHED_WORKER=1")
	output, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "DETACHED_READY_PENDING=1") {
		t.Fatalf("characterization %v\n%s", err, output)
	}
}
func TestDetachedPublicationWorker(t *testing.T) {
	if os.Getenv("BUBBLES_DETACHED_WORKER") != "1" {
		t.Skip("subprocess fixture")
	}
	runtime.GOMAXPROCS(2)
	idle := make(chan struct{}, 1)
	resume := make(chan struct{})
	release := make(chan struct{})
	tokens := make(chan *synctest.Ready, 1)
	var p Publications
	go synctest.ExploreResult(t, func(*testing.T) {
		synctest.SetDecisionHook(func(s synctest.BubbleState) int32 {
			if s.Idle {
				idle <- struct{}{}
				<-resume
			}
			return 0
		})
		_, complete := p.Begin("a")
		ready := synctest.NewReady()
		tokens <- ready
		synctest.ExternalWaitReady(func() { <-release; complete() }, ready)
	}, nil)
	ready := <-tokens
	<-idle
	close(release)
	ready.Wait()
	if p.Pending() != 1 {
		panic("readiness unexpectedly proved callback completion")
	}
	fmt.Println("DETACHED_READY_PENDING=1")
	os.Exit(0) // unresumed owner is intentionally process-contained
}

func TestEqualTimerDeadlinesAdvanceOnce(t *testing.T) {
	saved := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(saved)
	sink := &failingTrialSink{}
	result := New().ExploreWith(t, func(o *Orchestrator) {
		for _, name := range []string{"a", "b"} {
			tr := &attachedPublicationTransport{stubTransport: newStubTransport(name)}
			o.AddNode(tr, func(*testing.T) { time.Sleep(time.Hour) })
		}
	}, &Random{}, GlobalMaxRuns(1), WithTrialSink(sink))
	if !result.Successful() {
		t.Fatalf("%+v", result)
	}
	advances := 0
	var previous int64
	for _, event := range sink.last.GlobalTrace {
		if event.Type == StepTimeAdvance {
			advances++
			if event.Time < previous {
				t.Fatal("fake time moved backward")
			}
			previous = event.Time
		}
	}
	if advances != 1 {
		t.Fatalf("equal deadlines advanced %d times: %+v", advances, sink.last.GlobalTrace)
	}
}
