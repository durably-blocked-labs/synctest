package etcdkernels

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	"github.com/shubhaankar/synctest/experiments/etcdkernels/etcd6873"
	"github.com/shubhaankar/synctest/experiments/etcdkernels/etcd7443"
	"github.com/shubhaankar/synctest/experiments/etcdkernels/etcd7902"
	"github.com/shubhaankar/synctest/explorer"
	"github.com/shubhaankar/synctest/orchestrator"
)

type kernelChoice struct {
	BGIDs  []uint32 `json:"bgids"`
	Chosen int      `json:"chosen"`
}

type kernelRequest struct {
	Kernel string         `json:"kernel"`
	Mode   string         `json:"mode"`
	Seed   int64          `json:"seed"`
	Depth  int            `json:"depth"`
	Prefix []kernelChoice `json:"prefix"`
}

type kernelResult struct {
	Kernel       string              `json:"kernel"`
	Mode         string              `json:"mode"`
	OK           bool                `json:"ok"`
	WorkerUnsafe bool                `json:"worker_unsafe"`
	Cause        string              `json:"cause"`
	Choices      []kernelChoice      `json:"choices"`
	Trace        []explorer.Decision `json:"trace"`
	ReplayExact  bool                `json:"replay_exact"`
	Stack        string              `json:"stack,omitempty"`
}

// TestKernelWorker executes exactly one reduced kernel in one process. A caller
// supplies fresh request/result paths and is responsible for the external
// timeout and for discarding this process after WorkerUnsafe.
func TestKernelWorker(t *testing.T) {
	requestPath, resultPath := os.Getenv("BUBBLES_ETCD_REQUEST"), os.Getenv("BUBBLES_ETCD_RESULT")
	if requestPath == "" && resultPath == "" {
		t.Skip("one-shot worker")
	}
	if requestPath == "" || resultPath == "" {
		t.Fatal("request and result paths are both required")
	}
	data, err := os.ReadFile(requestPath)
	if err != nil {
		t.Fatal(err)
	}
	var request kernelRequest
	if err := json.Unmarshal(data, &request); err != nil {
		t.Fatal(err)
	}
	var workload func(*testing.T)
	switch request.Kernel {
	case "6873":
		workload = etcd6873.Workload
	case "7443":
		workload = etcd7443.Workload
	case "7902":
		workload = etcd7902.Workload
	default:
		t.Fatalf("unknown kernel %q", request.Kernel)
	}
	var algorithm explorer.Algorithm
	switch request.Mode {
	case "fifo", "prefix", "replay":
	case "random":
		algorithm = &explorer.Random{Seed: request.Seed}
	case "pct":
		algorithm = &explorer.PCT{Seed: request.Seed, Depth: request.Depth, MaxSteps: 128}
	default:
		t.Fatalf("unknown mode %q", request.Mode)
	}
	if algorithm != nil && !algorithm.BeforeRun() {
		t.Fatal("algorithm refused its first run")
	}
	previous := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(previous)
	choiceLog, err := openChoiceJournal(resultPath + ".choices.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer choiceLog.close()
	choices := make([]kernelChoice, 0, 32)
	run := explorer.RunWithHookResult(t, workload, func(state explorer.BubbleState) int32 {
		ids := append([]uint32(nil), state.RunnableBgid[:state.RunnableN]...)
		step := len(choices)
		var chosen int
		if step < len(request.Prefix) {
			if !reflect.DeepEqual(ids, request.Prefix[step].BGIDs) {
				panic(fmt.Sprintf("prefix identity mismatch at choice %d: got %v, want %v", step, ids, request.Prefix[step].BGIDs))
			}
			chosen = request.Prefix[step].Chosen
		} else if request.Mode == "replay" {
			panic(fmt.Sprintf("unexpected choice %d after complete replay prefix", step))
		} else if algorithm != nil {
			alts := make([]orchestrator.Alt, len(ids))
			for i, id := range ids {
				alts[i] = orchestrator.Alt{ID: "B" + strconv.FormatUint(uint64(id), 10), BGID: id}
			}
			chosen = algorithm.Decide(orchestrator.DecisionPoint{Kind: orchestrator.Local, Node: "local", Step: step, Alts: alts})
		}
		if chosen < 0 || chosen >= len(ids) {
			panic(fmt.Sprintf("invalid choice %d at step %d with %d alternatives", chosen, step, len(ids)))
		}
		decision := kernelChoice{BGIDs: ids, Chosen: chosen}
		choices = append(choices, decision)
		if err := choiceLog.append(decision); err != nil {
			panic(fmt.Sprintf("save decision: %v", err))
		}
		return int32(chosen)
	})
	result := kernelResult{Kernel: request.Kernel, Mode: request.Mode, OK: run.OK,
		WorkerUnsafe: run.WorkerUnsafe, Cause: fmt.Sprint(run.Cause), Choices: choices,
		Trace: run.Trace, ReplayExact: request.Mode != "replay" || len(choices) == len(request.Prefix)}
	if run.WorkerUnsafe {
		stack := make([]byte, 512<<10)
		result.Stack = string(stack[:runtime.Stack(stack, true)])
	}
	output, err := os.OpenFile(resultPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(output).Encode(result); err != nil {
		output.Close()
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
}
