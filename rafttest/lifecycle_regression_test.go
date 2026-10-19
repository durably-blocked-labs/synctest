package rafttest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/shubhaankar/synctest/orchestrator"
)

func TestAcknowledgedTransportIdleBridgeCloses(t *testing.T) {
	saved := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(saved)
	tr := NewRaftTransport("a")
	o := orchestrator.New()
	rr := o.ExploreWith(t, func(o *orchestrator.Orchestrator) { o.AddNode(tr, func(*testing.T) { tr.StartBridge() }) }, &orchestrator.Random{Seed: 1}, orchestrator.GlobalMaxRuns(1), orchestrator.RequireAcknowledgements())
	if !rr.Successful() || rr.Runs != 1 {
		t.Fatalf("%+v", rr)
	}
	select {
	case <-tr.closeCh:
	default:
		t.Fatal("bridge transport not closed")
	}
}

type captureAlgorithm struct {
	decide func(orchestrator.DecisionPoint) int
	result orchestrator.RunResult
	ran    bool
}

func (a *captureAlgorithm) BeforeRun() bool {
	if a.ran {
		return false
	}
	a.ran = true
	return true
}
func (a *captureAlgorithm) Decide(dp orchestrator.DecisionPoint) int {
	if a.decide != nil {
		return a.decide(dp)
	}
	return 0
}
func (a *captureAlgorithm) AfterRun(r orchestrator.RunResult) { a.result = r }

func TestAcknowledgedRPCAndUnifiedReplay(t *testing.T) {
	saved := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(saved)
	setup := func(o *orchestrator.Orchestrator) {
		a, b := NewRaftTransport("a"), NewRaftTransport("b")
		a.Connect(b)
		b.Connect(a)
		o.AddNode(a, func(t *testing.T) {
			a.StartBridge()
			var response raft.RequestVoteResponse
			if err := a.RequestVote("b", "b", &raft.RequestVoteRequest{Term: 1}, &response); err != nil {
				t.Errorf("RPC: %v", err)
			}
			if !response.Granted || response.Term != 1 {
				t.Errorf("response: %+v", response)
			}
		})
		o.AddNode(b, func(*testing.T) {
			b.StartBridge()
			go func() { rpc := <-b.Consumer(); rpc.Respond(&raft.RequestVoteResponse{Term: 1, Granted: true}, nil) }()
		})
	}
	o := orchestrator.New()
	algo := &captureAlgorithm{}
	result := o.ExploreWith(t, setup, algo, orchestrator.GlobalMaxRuns(1), orchestrator.RequireAcknowledgements())
	if !result.Successful() || !algo.result.Passed {
		t.Fatalf("result=%+v trial=%+v", result, algo.result)
	}
	rec := orchestrator.UnifiedReplay{Version: orchestrator.ReplayVersion, SelectControl: false, Trace: algo.result.Trace, Terminal: algo.result.Outcome, GlobalTrace: algo.result.GlobalTrace}
	replay := orchestrator.New()
	setup(replay)
	rr := replay.ReplayUnified(t, rec)
	if !rr.Passed || rr.Diverged || rr.WorkerUnsafe {
		t.Fatalf("replay: %+v", rr)
	}
}

func TestAttachedPublicationBoundAndCancellation(t *testing.T) {
	tr := NewRaftTransport("a")
	for i := 0; i < cap(tr.outbox); i++ {
		if err := tr.publish(&orchestrator.PendingOp{}); err != nil {
			t.Fatal(err)
		}
	}
	err := tr.publish(&orchestrator.PendingOp{})
	var limit orchestrator.ResourceLimitError
	if !errors.As(err, &limit) || limit.Resource != "outbox" {
		t.Fatalf("%T %v", err, err)
	}
	if err = tr.WaitPublications(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for len(tr.outbox) > 0 {
		op := <-tr.outbox
		if op.Occurrence == "" || seen[op.Occurrence] {
			t.Fatal("occurrence missing/duplicate")
		}
		seen[op.Occurrence] = true
	}
	if err = tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err = tr.publish(&orchestrator.PendingOp{}); !errors.Is(err, ErrTransportClosed) {
		t.Fatal(err)
	}
	if err = tr.WaitPublications(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestClosedPeerDeliveryDoesNotWriteFullSenderMailbox(t *testing.T) {
	sender, peer := NewRaftTransport("sender"), NewRaftTransport("peer")
	for i := 0; i < cap(sender.mailbox); i++ {
		sender.mailbox <- envelope{}
	}
	for i := 0; i < cap(peer.mailbox); i++ {
		peer.mailbox <- envelope{}
	}
	sender.Close()
	peer.Close()
	receipt, err := peer.deliver(context.Background(), envelope{kind: envelopeRequest})
	if !errors.Is(err, ErrTransportClosed) || receipt.WaitReady != nil || receipt.Consumed != nil {
		t.Fatalf("closed delivery: %+v %v", receipt, err)
	}
	if len(sender.mailbox) != cap(sender.mailbox) {
		t.Fatal("closed delivery touched sender mailbox")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = peer.deliver(ctx, envelope{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestDelayedConsumerFailsExplicitlyWhenFull(t *testing.T) {
	if os.Getenv("BUBBLES_FULL_CONSUMER_WORKER") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.v", "-test.run=^TestDelayedConsumerFailsExplicitlyWhenFull$", "-test.timeout=8s")
		cmd.Env = append(os.Environ(), "BUBBLES_FULL_CONSUMER_WORKER=1")
		output, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(output), "FULL_CONSUMER_EXPLICIT") {
			t.Fatalf("worker %v\n%s", err, output)
		}
		t.Logf("%s", output)
		return
	}
	saved := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(saved)
	a, b := NewRaftTransport("a"), NewRaftTransport("b")
	a.Connect(b)
	b.Connect(a)
	algorithm := &captureAlgorithm{}
	result := orchestrator.New().ExploreWith(t, func(o *orchestrator.Orchestrator) {
		o.AddNode(a, func(*testing.T) {
			a.StartBridge()
			for i := 0; i < 18; i++ {
				go func() {
					var response raft.RequestVoteResponse
					_ = a.RequestVote("b", "b", &raft.RequestVoteRequest{}, &response)
				}()
				runtime.Gosched() // stagger publication below the runtime runnable-set cap
			}
		})
		o.AddNode(b, func(*testing.T) { b.StartBridge(); time.Sleep(time.Hour) })
	}, algorithm, orchestrator.GlobalMaxRuns(1), orchestrator.RequireAcknowledgements())
	var limit orchestrator.ResourceLimitError
	err, _ := algorithm.result.Cause.(error)
	if result.Incomplete != 1 || !errors.As(err, &limit) || limit.Resource != "internal consumer" || algorithm.result.TeardownError != nil {
		t.Fatalf("result=%+v trial=%+v", result, algorithm.result)
	}
	if algorithm.result.WorkerUnsafe && orchestrator.WorkerReusable() {
		t.Fatal("unsafe burst teardown permitted reset")
	}
	// Waking this burst may exceed the runtime's 16-runnable safety cap.
	// Never reuse that worker, even though the original queue failure is typed.
	fmt.Printf("FULL_CONSUMER_EXPLICIT unsafe=%t cause=%v\n", algorithm.result.WorkerUnsafe, algorithm.result.Cause)
	os.Exit(0)
}

func TestAdapterHostSelectControlFailsClosed(t *testing.T) {
	if os.Getenv("BUBBLES_HOST_SELECT_WORKER") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAdapterHostSelectControlFailsClosed$", "-test.timeout=8s")
		cmd.Env = append(os.Environ(), "BUBBLES_HOST_SELECT_WORKER=1")
		output, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(output), "HOST_SELECT_FAIL_CLOSED") {
			t.Fatalf("worker %v\n%s", err, output)
		}
		return
	}
	saved := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(saved)
	a, b := NewRaftTransport("a"), NewRaftTransport("b")
	a.Connect(b)
	b.Connect(a)
	algorithm := &captureAlgorithm{}
	result := orchestrator.New().ExploreWith(t, func(o *orchestrator.Orchestrator) {
		o.AddNode(a, func(*testing.T) {
			a.StartBridge()
			var response raft.RequestVoteResponse
			_ = a.RequestVote("b", "b", &raft.RequestVoteRequest{}, &response)
		})
		o.AddNode(b, func(*testing.T) { b.StartBridge() })
	}, algorithm, orchestrator.GlobalMaxRuns(1), orchestrator.RequireAcknowledgements(), orchestrator.WithSelectChoices())
	if result.Incomplete != 1 || result.Failed != 0 || algorithm.result.UserFailed || !strings.Contains(fmt.Sprint(algorithm.result.Cause), "controlled select requires bubble-owned channels") {
		t.Fatalf("result=%+v trial=%+v", result, algorithm.result)
	}
	fmt.Println("HOST_SELECT_FAIL_CLOSED")
	os.Exit(0)
}

func TestPeerCloseAfterPublicationStopsWholeTrial(t *testing.T) {
	saved := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(saved)
	a, b := NewRaftTransport("a"), NewRaftTransport("b")
	a.Connect(b)
	b.Connect(a)
	algorithm := &captureAlgorithm{decide: func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Global {
			b.Close()
		}
		return 0
	}}
	result := orchestrator.New().ExploreWith(t, func(o *orchestrator.Orchestrator) {
		o.AddNode(a, func(*testing.T) {
			a.StartBridge()
			var response raft.RequestVoteResponse
			_ = a.RequestVote("b", "b", &raft.RequestVoteRequest{}, &response)
		})
		o.AddNode(b, func(*testing.T) { b.StartBridge() })
	}, algorithm, orchestrator.GlobalMaxRuns(1), orchestrator.RequireAcknowledgements())
	rr := algorithm.result
	if result.Incomplete != 1 || rr.Outcome != orchestrator.OutcomeInfrastructure || rr.UserFailed || rr.WorkerUnsafe || rr.TeardownError != nil || !strings.Contains(fmt.Sprint(rr.Cause), "closed after publication") {
		t.Fatalf("result=%+v trial=%+v", result, rr)
	}
	for _, step := range rr.GlobalTrace {
		if step.Type == orchestrator.StepDeliver || step.Type == orchestrator.StepTimeAdvance {
			t.Fatal("work continued after causal peer close")
		}
	}
}

func TestPrepublicationClosedPeerReturnsError(t *testing.T) {
	a, b := NewRaftTransport("a"), NewRaftTransport("b")
	a.Connect(b)
	b.Close()
	_, err := a.makeRPC("b", &raft.RequestVoteRequest{}, nil)
	if err == nil || !strings.Contains(err.Error(), "closed") || len(a.outbox) != 0 {
		t.Fatalf("err=%v publications=%d", err, len(a.outbox))
	}
}

func TestGlobalAlgorithmPanicPreservesExactCause(t *testing.T) {
	saved := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(saved)
	a, b := NewRaftTransport("a"), NewRaftTransport("b")
	a.Connect(b)
	b.Connect(a)
	cause := &struct{ Message string }{"global algorithm defect"}
	algorithm := &captureAlgorithm{decide: func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Global {
			panic(cause)
		}
		return 0
	}}
	result := orchestrator.New().ExploreWith(t, func(o *orchestrator.Orchestrator) {
		o.AddNode(a, func(*testing.T) {
			a.StartBridge()
			var response raft.RequestVoteResponse
			_ = a.RequestVote("b", "b", &raft.RequestVoteRequest{}, &response)
		})
		o.AddNode(b, func(*testing.T) { b.StartBridge() })
	}, algorithm, orchestrator.GlobalMaxRuns(1), orchestrator.RequireAcknowledgements())
	if result.Incomplete != 1 || algorithm.result.Cause != cause || algorithm.result.Outcome != orchestrator.OutcomeInfrastructure || algorithm.result.WorkerUnsafe {
		t.Fatalf("result=%+v trial=%+v", result, algorithm.result)
	}
}

func TestInfrastructureFailureCancelsPendingRPC(t *testing.T) {
	saved := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(saved)
	a, b := NewRaftTransport("a"), NewRaftTransport("b")
	a.Connect(b)
	b.Connect(a)
	cause := fmt.Errorf("coordinator algorithm defect")
	algorithm := &captureAlgorithm{decide: func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Global {
			panic(cause)
		}
		return 0
	}}
	o := orchestrator.New()
	setups := 0
	result := o.ExploreWith(t, func(o *orchestrator.Orchestrator) {
		setups++
		o.AddNode(a, func(t *testing.T) {
			a.StartBridge()
			var response raft.RequestVoteResponse
			if err := a.RequestVote("b", "b", &raft.RequestVoteRequest{}, &response); err == nil {
				t.Error("pending RPC did not receive close outcome")
			}
		})
		o.AddNode(b, func(*testing.T) { b.StartBridge() })
	}, algorithm, orchestrator.GlobalMaxRuns(2), orchestrator.RequireAcknowledgements())
	if result.Successful() || result.Incomplete != 1 || setups != 1 || algorithm.result.Cause != cause || algorithm.result.TeardownError != nil {
		t.Fatalf("result=%+v trial=%+v setups=%d", result, algorithm.result, setups)
	}
	for _, tr := range []*RaftTransport{a, b} {
		tr.pendingMu.Lock()
		pending := len(tr.pending)
		tr.pendingMu.Unlock()
		if pending != 0 {
			t.Fatalf("%s has %d pending RPCs", tr.localAddr, pending)
		}
	}
}
