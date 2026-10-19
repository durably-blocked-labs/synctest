package orchestrator

import (
	"reflect"
	"testing"
)

func bookkeepingRecoveryOwner() (*Orchestrator, *stubTransport) {
	tr := newStubTransport("cleanup")
	o := New()
	o.AddNode(tr, func(*testing.T) {})
	return o, tr
}

func TestRecoverForExploreReturnsDivergence(t *testing.T) {
	o, tr := bookkeepingRecoveryOwner()
	rr := o.recoverForExplore(func() RunResult {
		panic(replayDivergenceError{Step: 37, Index: 4, QueueSize: 2})
	})
	if !rr.Diverged || rr.DivergenceStep != 37 || rr.Passed || rr.UserFailed {
		t.Errorf("recovery result=%+v, want divergence at step 37", rr)
	}
	if got := tr.shutdowns.Load(); got != 1 {
		t.Errorf("recovery shutdowns=%d, want 1", got)
	}
}

func TestRecoverForExplorePreservesOrdinaryResult(t *testing.T) {
	o, tr := bookkeepingRecoveryOwner()
	want := RunResult{Passed: true, DivergenceStep: -1, Trace: Trace{{Kind: Global, ChosenID: "message"}}}
	if got := o.recoverForExplore(func() RunResult { return want }); !reflect.DeepEqual(got, want) {
		t.Errorf("ordinary result=%+v, want %+v", got, want)
	}
	if got := tr.shutdowns.Load(); got != 0 {
		t.Errorf("ordinary return shutdowns=%d, want 0", got)
	}
}

func TestRecoverForExploreRepanicsUnknownCause(t *testing.T) {
	o, tr := bookkeepingRecoveryOwner()
	cause := &struct{ message string }{"algorithm defect"}
	defer func() {
		if got := recover(); got != cause {
			t.Errorf("panic=%v, want exact cause %v", got, cause)
		}
		if got := tr.shutdowns.Load(); got != 0 {
			t.Errorf("unknown panic shutdowns=%d, want 0 (existing ownership)", got)
		}
	}()
	o.recoverForExplore(func() RunResult { panic(cause) })
	t.Error("unknown panic was swallowed")
}
