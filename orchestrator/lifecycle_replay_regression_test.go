package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPublicationWaitsForRegisteredSend(t *testing.T) {
	var p Publications
	id, complete := p.Begin("node")
	if id != "node:1" {
		t.Fatal(id)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("pending publication ignored", err)
	}
	complete()
	complete()
	if err := p.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	next, done := p.Begin("node")
	if next == id {
		t.Fatal("occurrence reused")
	}
	done()
}
func replayFixture() UnifiedReplay {
	dp := DecisionPoint{Kind: Global, Alts: []Alt{{ID: "class", Occurrence: "a:1"}, {ID: "class", Occurrence: "a:2"}}}
	local := DecisionPoint{Kind: Local, Node: "a", Alts: []Alt{{ID: "B2", BGID: 2}, {ID: "B3", BGID: 3}}}
	return UnifiedReplay{Version: 1, Trace: Trace{stepForDecision(dp, 1), stepForDecision(local, 1)}, Terminal: OutcomePassed}
}
func TestStrictReplayIdentityAndConsumption(t *testing.T) {
	rec := replayFixture()
	r, err := NewStrictReplayer(rec)
	if err != nil {
		t.Fatal(err)
	}
	global := DecisionPoint{Kind: Global, Alts: []Alt{{ID: "class", Occurrence: "a:1"}, {ID: "class", Occurrence: "a:2"}}}
	if r.Decide(global) != 1 {
		t.Fatal("global nonFIFO lost")
	}
	if err = r.Finish(RunResult{Outcome: OutcomePassed}); err == nil {
		t.Fatal("unused suffix accepted")
	}
	if r.Decide(DecisionPoint{Kind: Local, Node: "a", Alts: []Alt{{ID: "B2"}, {ID: "B3"}}}) != 1 {
		t.Fatal("local nonFIFO lost")
	}
	if err = r.Finish(RunResult{Outcome: OutcomeAssertion}); err == nil {
		t.Fatal("terminal mismatch accepted")
	}
	if err = r.Finish(RunResult{Outcome: OutcomePassed}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"occurrence", "node", "kind", "extra"} {
		t.Run(name, func(t *testing.T) {
			r, _ := NewStrictReplayer(rec)
			dp := global
			switch name {
			case "occurrence":
				dp.Alts = []Alt{{ID: "class", Occurrence: "a:2"}, {ID: "class", Occurrence: "a:1"}}
			case "node":
				dp.Node = "wrong"
			case "kind":
				dp.Kind = Local
			case "extra":
				r.next = len(rec.Trace)
			}
			defer func() {
				if _, ok := IsReplayDivergence(recover()); !ok {
					t.Error("mismatch was not divergence")
				}
			}()
			r.Decide(dp)
		})
	}
	for _, idx := range []int32{-1, 2} {
		bad := replayFixture()
		bad.Trace[0].Index = idx
		if _, err = NewStrictReplayer(bad); err == nil {
			t.Fatal("invalid index accepted")
		}
	}
}
func TestClosedOutboxAndNilOperation(t *testing.T) {
	tr := newStubTransport("a")
	close(tr.outbox)
	o := New()
	o.AddNode(tr, func(*testing.T) {})
	o.drainOutbox("a")
	defer func() {
		if recover() == nil {
			t.Error("nil operation accepted")
		}
	}()
	o.enqueue(nil)
}
func TestCleanupRecognizesCloseAndReportsUnjoined(t *testing.T) {
	tr := &closeTransport{stubTransport: newStubTransport("a")}
	o := New()
	o.AddNode(tr, func(*testing.T) {})
	o.initBubbles()
	c := o.nodes["a"]
	c.started = true // synthetic root never acknowledges completion
	start := time.Now()
	if err := o.cleanupBubbles(); err == nil {
		t.Fatal("unjoined node accepted")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("unbounded join")
	}
	if tr.closed != 1 {
		t.Fatal("Close not recognized")
	}
	if err := o.cleanupBubbles(); err == nil {
		t.Fatal("unjoined repeat accepted")
	}
	if tr.closed != 1 {
		t.Fatal("duplicate stop")
	}
}

type closeTransport struct {
	*stubTransport
	closed int
}

func (c *closeTransport) Close() error { c.closed++; return nil }
func TestClassifyRetainsExactCause(t *testing.T) {
	cause := errors.New("algorithm failure")
	rr := RunResult{Passed: true}
	classifyCause(&rr, cause)
	if rr.Cause != cause || rr.Passed || rr.UserFailed || rr.Diverged || rr.Outcome != OutcomeInfrastructure {
		t.Fatalf("%+v", rr)
	}
	classifyCause(&rr, replayDivergenceError{Step: 7})
	if !rr.Diverged || rr.DivergenceStep != 7 {
		t.Fatalf("%+v", rr)
	}
	if (ExplorationResult{Incomplete: 1}).Successful() {
		t.Fatal("incomplete exploration claimed success")
	}
}
