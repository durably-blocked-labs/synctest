package orchestrator

import (
	"runtime"
	"sync/atomic"
	"testing"
)

type stubTransport struct {
	addr       string
	outbox     chan *PendingOp
	shutdowns  atomic.Int32
}

func newStubTransport(addr string) *stubTransport {
	return &stubTransport{
		addr:   addr,
		outbox: make(chan *PendingOp, 1),
	}
}

func (t *stubTransport) Addr() string               { return t.addr }
func (t *stubTransport) Outbox() <-chan *PendingOp  { return t.outbox }
func (t *stubTransport) Shutdown()                  { t.shutdowns.Add(1) }

func TestExploreWithCleansUpSuccessfulRuns(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const runs = 3
	var transports []*stubTransport

	orch := New()
	algo := &Random{Seed: 1}
	result := orch.ExploreWith(t, func(o *Orchestrator) {
		tr := newStubTransport("node")
		transports = append(transports, tr)
		o.AddNode(tr, func(t *testing.T) {})
	}, algo, GlobalMaxRuns(runs))

	if result.Runs != runs {
		t.Fatalf("runs=%d, want %d", result.Runs, runs)
	}
	if len(transports) != runs {
		t.Fatalf("created %d transports, want %d", len(transports), runs)
	}
	for i, tr := range transports {
		if got := tr.shutdowns.Load(); got != 1 {
			t.Fatalf("transport %d shutdowns=%d, want 1", i, got)
		}
	}
}
