package resourcegc

import (
	"errors"
	"reflect"
	"testing"
)

func TestLifecycleErrorPaths(t *testing.T) {
	tests := []struct {
		name    string
		actions []string
		failed  string
	}{
		{"closed before claim", []string{"check", "close", "claim"}, "claim"},
		{"closed before read", []string{"check", "claim", "close", "read", "release"}, "read"},
		{"closed before compute", []string{"check", "claim", "read", "close", "compute", "commit", "release"}, "commit"},
		{"closed before commit", []string{"check", "claim", "read", "compute", "close", "commit", "release"}, "commit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newResource()
			var failed string
			for _, action := range tt.actions {
				err := takeAction(r, action)
				if err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Fatalf("%s returned %v", action, err)
					}
					if failed != "" {
						t.Fatalf("second error at %s", action)
					}
					failed = action
				}
			}
			got := r.observation()
			if failed != tt.failed || got.WorkerError != "ErrClosed" || got.Result != 0 || !got.Completed {
				t.Fatalf("failed action %q, observation %+v", failed, got)
			}
			if got.Events[len(got.Events)-1].State.InUse {
				t.Fatal("failed worker retained resource ownership")
			}
		})
	}
}

func TestCleanupSkipsOwnedResource(t *testing.T) {
	r := newResource()
	if err := r.claim(); err != nil {
		t.Fatal(err)
	}
	if r.check() {
		t.Fatal("cleanup decided to close an owned resource")
	}
	if err := r.read(); err != nil {
		t.Fatal(err)
	}
	r.compute()
	if err := r.commit(); err != nil {
		t.Fatal(err)
	}
	r.release()
	got := r.observation()
	if got.WorkerError != "" || got.Result != 42 || !got.Completed {
		t.Fatalf("observation %+v", got)
	}
	state := got.Events[len(got.Events)-1].State
	if state.Closed || state.InUse || state.CloseDecision != "no" {
		t.Fatalf("final state %+v", state)
	}
}

func TestStaleCloseAfterCommitSucceeds(t *testing.T) {
	r := newResource()
	if !r.check() {
		t.Fatal("unused resource was not selected for cleanup")
	}
	for _, action := range []string{"claim", "read", "compute", "commit", "close", "release"} {
		if err := takeAction(r, action); err != nil {
			t.Fatalf("%s: %v", action, err)
		}
	}
	got := r.observation()
	if got.WorkerError != "" || got.Result != 42 || !got.Completed {
		t.Fatalf("observation %+v", got)
	}
	if !got.Events[len(got.Events)-1].State.Closed {
		t.Fatal("cleanup did not execute its captured decision")
	}
}

func TestRunApplicationReturnsCompletedObservation(t *testing.T) {
	got := RunApplication()
	if !got.Completed || len(got.Events) < 4 || len(got.Events) > 8 {
		t.Fatalf("incomplete or malformed observation %+v", got)
	}
	wantInitial := Snapshot{
		Worker: "claim", Cleanup: "check", CloseDecision: "unset",
		ReadValue: -1, ComputedValue: -1,
	}
	if got.Events[0].Action != "initial" || !reflect.DeepEqual(got.Events[0].State, wantInitial) {
		t.Fatalf("initial event %+v", got.Events[0])
	}
	final := got.Events[len(got.Events)-1].State
	if final.Worker != "done" || final.Cleanup != "done" || final.InUse {
		t.Fatalf("final state %+v", final)
	}
	switch got.WorkerError {
	case "":
		if got.Result != 42 {
			t.Fatalf("successful worker result %d", got.Result)
		}
	case "ErrClosed":
		if got.Result != 0 {
			t.Fatalf("failed worker result %d", got.Result)
		}
	default:
		t.Fatalf("unexpected error %q", got.WorkerError)
	}
}

// takeAction lets unit tests arrange application phases without adding test
// synchronization or scheduling points to the concurrent workload itself.
func takeAction(r *resource, action string) error {
	switch action {
	case "claim":
		return r.claim()
	case "read":
		return r.read()
	case "compute":
		r.compute()
	case "commit":
		return r.commit()
	case "release":
		r.release()
	case "check":
		r.check()
	case "close":
		r.close()
	default:
		panic("unknown application action: " + action)
	}
	return nil
}
