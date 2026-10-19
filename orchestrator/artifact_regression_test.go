package orchestrator

import (
	"errors"
	"runtime"
	"testing"
)

type failingTrialSink struct {
	startErr, endErr error
	starts, ends     int
	last             RunResult
}

func (s *failingTrialSink) StartTrial(int) error              { s.starts++; return s.startErr }
func (s *failingTrialSink) EndTrial(_ int, r RunResult) error { s.ends++; s.last = r; return s.endErr }
func TestSinkFailureStopsBeforeNextSetup(t *testing.T) {
	saved := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(saved)
	cause := errors.New("persistence failure")
	for _, where := range []string{"start", "terminal"} {
		t.Run(where, func(t *testing.T) {
			sink := &failingTrialSink{}
			if where == "start" {
				sink.startErr = cause
			} else {
				sink.endErr = cause
			}
			setups := 0
			r := New().ExploreWith(t, func(*Orchestrator) { setups++ }, &Random{Seed: 1}, GlobalMaxRuns(3), WithTrialSink(sink))
			if r.Successful() || r.Incomplete != 1 || r.Cause != cause {
				t.Fatalf("%+v", r)
			}
			want := 0
			if where == "terminal" {
				want = 1
			}
			if setups != want || sink.starts != 1 || sink.ends != want {
				t.Fatalf("setups=%d starts=%d ends=%d", setups, sink.starts, sink.ends)
			}
		})
	}
}
func TestSetupFailureWritesTerminalAndStops(t *testing.T) {
	cause := errors.New("reset failed")
	sink := &failingTrialSink{}
	setups := 0
	r := New().ExploreWith(t, func(*Orchestrator) { setups++; panic(cause) }, &Random{}, GlobalMaxRuns(3), WithTrialSink(sink))
	if r.Successful() || r.Cause != cause || setups != 1 || sink.starts != 1 || sink.ends != 1 || sink.last.Cause != cause {
		t.Fatalf("result=%+v sink=%+v setups=%d", r, sink, setups)
	}
}
