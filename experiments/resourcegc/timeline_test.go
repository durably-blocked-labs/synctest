package resourcegc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"testing/synctest"
)

// SourceFrame is symbolized in the process that captured the PC. Absolute PCs
// are retained as diagnostics and are never used as portable replay identity.
type SourceFrame struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
}

type TimelineEvent struct {
	Sequence            uint64           `json:"sequence"`
	Kind                string           `json:"kind"`
	BGID                uint32           `json:"bgid"`
	NativeStep          int32            `json:"native_step"`
	Tag                 uint32           `json:"tag,omitempty"`
	PC                  uintptr          `json:"pc,omitempty"`
	GCCycle             uint32           `json:"gc_cycle"`
	Stopped             bool             `json:"stopped"`
	SelfScan            bool             `json:"self_scan"`
	StatusBeforeSuspend uint32           `json:"status_before_suspend"`
	Call                *CallTagInfo     `json:"call,omitempty"`
	CapturedSite        *SourceFrame     `json:"captured_site,omitempty"`
	Stack               []SourceFrame    `json:"stack,omitempty"`
	GCWaiting           bool             `json:"gc_waiting,omitempty"`
	PreemptStop         bool             `json:"preempt_stop,omitempty"`
	SyncSafePoint       bool             `json:"sync_safe_point,omitempty"`
	AsyncSafePoint      bool             `json:"async_safe_point,omitempty"`
	STWReason           uint8            `json:"stw_reason,omitempty"`
	Control             *ControlSnapshot `json:"control,omitempty"`
}

type ControlQueuedG struct {
	BGID      uint32       `json:"bgid"`
	IsRoot    bool         `json:"is_root"`
	Member    bool         `json:"member"`
	StartPC   uintptr      `json:"start_pc"`
	StartSite *SourceFrame `json:"start_site,omitempty"`
}

type ControlSnapshot struct {
	Queue         []ControlQueuedG `json:"queue"`
	RootStatus    uint32           `json:"root_status"`
	RootInHook    bool             `json:"root_in_hook"`
	DecisionReady bool             `json:"decision_ready"`
	Signal        uint32           `json:"signal"`
	Running       int32            `json:"running"`
	Active        int32            `json:"active"`
	External      int32            `json:"external"`
}

func symbolizeObservations(observations []synctest.Observation) []TimelineEvent {
	result := make([]TimelineEvent, 0, len(observations))
	for _, observation := range observations {
		kind := "unknown"
		switch observation.Kind {
		case 1:
			kind = "schedule"
		case 2:
			kind = "call"
		case 3:
			kind = "gc_scan_begin"
		case 4:
			kind = "gc_resume_requested"
		case 5:
			kind = "preempt"
		case 6:
			kind = "preempt_request"
		case 7:
			kind = "world_stop_wait"
		case 8:
			kind = "world_stop_resumed"
		case 9:
			kind = "world_stop_continued"
		case 10:
			kind = "control_snapshot"
		case 11:
			kind = "scan_suspend"
		case 12:
			kind = "runtime_continued"
		case 13:
			kind = "gc_internal_pause"
		}
		event := TimelineEvent{Sequence: observation.Sequence, Kind: kind,
			BGID: observation.BGID, NativeStep: observation.NativeStep, Tag: observation.Tag,
			PC: observation.PC, GCCycle: observation.GCCycle, Stopped: observation.Stopped}
		event.SelfScan, event.StatusBeforeSuspend = observation.SelfScan, observation.StatusBeforeSuspend
		// Diagnostic runtime extensions are optional. Reflection happens only
		// after the workload, retaining compatibility with the prior sealed API.
		value := reflect.ValueOf(observation)
		if flag := value.FieldByName("GCWaiting"); flag.IsValid() {
			event.GCWaiting = flag.Bool()
			event.PreemptStop = value.FieldByName("PreemptStop").Bool()
			event.SyncSafePoint = value.FieldByName("SyncSafePoint").Bool()
			event.AsyncSafePoint = value.FieldByName("AsyncSafePoint").Bool()
			event.STWReason = uint8(value.FieldByName("STWReason").Uint())
		}
		if size := value.FieldByName("ControlQueueN"); size.IsValid() && kind == "control_snapshot" {
			c := &ControlSnapshot{RootStatus: uint32(value.FieldByName("ControlRootStatus").Uint()),
				RootInHook: value.FieldByName("ControlRootInHook").Bool(), DecisionReady: value.FieldByName("ControlDecisionReady").Bool(),
				Signal: uint32(value.FieldByName("ControlSignal").Uint()), Running: int32(value.FieldByName("ControlRunning").Int()),
				Active: int32(value.FieldByName("ControlActive").Int()), External: int32(value.FieldByName("ControlExternal").Int())}
			rootMask, memberMask := value.FieldByName("ControlRootMask").Uint(), value.FieldByName("ControlMemberMask").Uint()
			for i := 0; i < int(size.Uint()); i++ {
				g := ControlQueuedG{BGID: uint32(value.FieldByName("ControlQueueBGIDs").Index(i).Uint()), IsRoot: rootMask&(1<<i) != 0,
					Member: memberMask&(1<<i) != 0, StartPC: uintptr(value.FieldByName("ControlQueueStarts").Index(i).Uint())}
				if f := runtime.FuncForPC(g.StartPC); f != nil {
					file, line := f.FileLine(g.StartPC)
					g.StartSite = &SourceFrame{f.Name(), file, line}
				}
				c.Queue = append(c.Queue, g)
			}
			event.Control = c
		}
		if info, ok := LookupCallTag(observation.Tag); ok && kind == "call" {
			event.Call = &info
		}
		if observation.PC != 0 && observation.StackN == 0 {
			if function := runtime.FuncForPC(observation.PC); function != nil {
				file, line := function.FileLine(observation.PC - 1)
				event.CapturedSite = &SourceFrame{Function: function.Name(), File: file, Line: line}
			}
		}
		if observation.StackN > 0 {
			frames := runtime.CallersFrames(observation.StackPCs[:int(observation.StackN)])
			for {
				frame, more := frames.Next()
				event.Stack = append(event.Stack, SourceFrame{Function: frame.Function, File: frame.File, Line: frame.Line})
				if !more {
					break
				}
			}
			if len(event.Stack) != 0 {
				site := event.Stack[0]
				event.CapturedSite = &site
			}
		}
		result = append(result, event)
	}
	return result
}

// TestObservationScanProbe exercises collection while an application actor is
// parked. It qualifies capture and symbolization; it does not demonstrate GC
// preempting a running actor or perturbing the resource application.
func TestObservationScanProbe(t *testing.T) {
	// The runtime skips background mark workers on a bubble-owned P. Leave a
	// host P available; this qualification is separate from the one-P baseline.
	runtime.GOMAXPROCS(2)
	var observations []synctest.Observation
	var overflow bool
	r := synctest.ExploreResult(t, func(*testing.T) {
		synctest.EnableObservations()
		ready := make(chan struct{})
		release := make(chan struct{})
		done := make(chan struct{})
		go observationParkedActor(ready, release, done)
		<-ready
		runtime.GC()
		close(release)
		<-done
		observations, overflow = synctest.Observations()
	}, nil)
	if !r.OK || r.WorkerUnsafe {
		t.Fatalf("probe failed: %v unsafe=%v", r.Cause, r.WorkerUnsafe)
	}
	if overflow {
		t.Fatal("observation capacity/publication failure")
	}
	decoded := symbolizeObservations(observations)
	matched := false
	for _, event := range decoded {
		if event.Kind != "gc_scan_begin" {
			continue
		}
		for _, frame := range event.Stack {
			if frame.Function == "github.com/shubhaankar/synctest/experiments/resourcegc.observationParkedActor" {
				matched = true
				t.Logf("captured BGID=%d stopped=%v %s:%d %s", event.BGID, event.Stopped, frame.File, frame.Line, frame.Function)
			}
		}
	}
	if !matched {
		t.Fatal("no captured GC stack contains the parked application actor")
	}
	if path := os.Getenv("RESOURCEGC_PROBE_RESULT"); path != "" {
		if !filepath.IsAbs(path) {
			t.Fatal("probe result must be an absolute path")
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		data := struct {
			Scope               string          `json:"scope"`
			Processors          int             `json:"processors"`
			Timeline            []TimelineEvent `json:"timeline"`
			ObservationOverflow bool            `json:"observation_overflow"`
		}{"parked actor scan qualification; not running-actor interruption evidence", 2, decoded, overflow}
		if err = json.NewEncoder(file).Encode(data); err != nil {
			t.Fatal(err)
		}
		if err = file.Sync(); err != nil {
			t.Fatal(err)
		}
	}
}

//go:noinline
func observationParkedActor(ready, release, done chan struct{}) {
	synctest.RecordObservation(1001)
	close(ready)
	<-release
	synctest.RecordObservation(1002)
	close(done)
}

func TestObservationOverflowIsReported(t *testing.T) {
	var overflow bool
	r := synctest.ExploreResult(t, func(*testing.T) {
		synctest.EnableObservations()
		for i := 0; i < 4097; i++ {
			synctest.RecordObservation(2001)
		}
		_, overflow = synctest.Observations()
	}, nil)
	if !r.OK || r.WorkerUnsafe {
		t.Fatalf("overflow probe failed: %v", r.Cause)
	}
	if !overflow {
		t.Fatal("truncated observations were not marked incomplete")
	}
}
