package resourcegc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/shubhaankar/synctest/explorer"
)

const (
	assistActorEnter  uint32 = 4001
	assistActorReturn uint32 = 4002
	assistPeerEnter   uint32 = 4003
)

type assistProbeNode struct {
	left, right *assistProbeNode
	value       [8]uint64
}

// These are separate process-isolated qualifications. Natural allocation is
// deliberately not claimed to force either path: a missing diagnostic is an
// inconclusive skip, not evidence that the runtime continuation worked.
func TestGCAssistYieldContinuation(t *testing.T) { qualifyAssistContinuation(t, 1, 4) }
func TestGCAssistParkContinuation(t *testing.T)  { qualifyAssistContinuation(t, 2, 5) }

func qualifyAssistContinuation(t *testing.T, wantedPause, wantedContinuation uint32) {
	runtime.GOMAXPROCS(2) // The bubble P does not run background GC workers.
	previousGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previousGC)

	// Retain a finite pointer-rich graph to give the host collector real work.
	// No finalizers, weak pointers, fabricated credit, or collector state edits.
	graph := make([]assistProbeNode, 1<<17)
	for i := range graph {
		graph[i].left = &graph[(i+1)%len(graph)]
		graph[i].right = &graph[(i*17+3)%len(graph)]
		graph[i].value[0] = uint64(i)
	}
	runtime.GC() // Common setup collection, before the host trigger is armed.
	startGC, driverDone := make(chan struct{}), make(chan struct{})
	var collections atomic.Uint32
	go func() {
		<-startGC
		for i := 0; i < 4; i++ {
			runtime.GC()
			collections.Add(1)
		}
		close(driverDone)
	}()

	var actorFinished, peerRanEarly atomic.Bool
	var events []synctest.Observation
	var overflow bool
	var checksum uint64
	result := explorer.RunWithHookResult(t, func(*testing.T) {
		synctest.EnableObservations()
		start := make(chan struct{})
		var actors sync.WaitGroup
		actors.Add(2)
		// In this fixture's testing/synctest wrapper B1 and B2 are control
		// goroutines; the following actor and peer are B3 and B4. Check the
		// recorded markers below rather than silently assuming this mapping.
		go func() {
			defer actors.Done()
			<-start
			synctest.RecordObservation(assistActorEnter)
			close(startGC)
			checksum = allocateAssistProbe(graph)
			actorFinished.Store(true)
			synctest.RecordObservation(assistActorReturn)
		}()
		go func() {
			defer actors.Done()
			<-start
			if !actorFinished.Load() {
				peerRanEarly.Store(true)
			}
			synctest.RecordObservation(assistPeerEnter)
		}()
		close(start)
		actors.Wait()
		events, overflow = synctest.Observations()
	}, func(state explorer.BubbleState) int32 {
		for i := int32(0); i < state.RunnableN; i++ {
			if state.RunnableBgid[i] == 3 {
				return i
			}
		}
		return 0
	})
	if result.WorkerUnsafe || !result.OK {
		// An unsafe worker is process-terminal. Do not wait for a survivor
		// or attempt another qualification in this process.
		t.Fatalf("assist probe runtime failure: OK=%v unsafe=%v cause=%v", result.OK, result.WorkerUnsafe, result.Cause)
	}
	// Join on the host only after the observed bubble has retired. This does
	// not add an External reattachment to the target scheduling trace.
	<-driverDone
	runtime.KeepAlive(graph)

	pauses, continuations, actorChoices := 0, 0, 0
	var actorBGID, peerBGID uint32
	var enterSequence, returnSequence, peerSequence uint64
	for _, event := range events {
		if event.Kind == 2 {
			switch event.Tag {
			case assistActorEnter:
				actorBGID, enterSequence = event.BGID, event.Sequence
			case assistActorReturn:
				returnSequence = event.Sequence
			case assistPeerEnter:
				peerBGID, peerSequence = event.BGID, event.Sequence
			}
		}
	}
	// The marker IDs above are distinct from runtime reason tags. Require a
	// matching continuation after every observed selected-path interruption.
	pending := false
	pairingError := ""
	for _, event := range events {
		if event.BGID != actorBGID || event.Sequence <= enterSequence || event.Sequence >= returnSequence {
			continue
		}
		if event.Kind == 13 && event.Tag == wantedPause {
			if pending {
				pairingError = "assist pause repeated before its internal continuation"
			}
			pauses++
			pending = true
		}
		if event.Kind == 12 && event.Tag == wantedContinuation {
			if !pending {
				pairingError = "assist continuation lacks its matching pause"
			}
			continuations++
			pending = false
		}
	}
	for _, decision := range result.Trace {
		if decision.ChosenBgid == actorBGID {
			actorChoices++
		}
	}

	// Write all raw records before classifying missing coverage or a semantic
	// failure. The caller supplies a fresh absolute artifact path per process.
	if path := os.Getenv("RESOURCEGC_ASSIST_RESULT"); path != "" {
		if !filepath.IsAbs(path) {
			t.Fatal("RESOURCEGC_ASSIST_RESULT must be absolute")
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		artifact := struct {
			WantedPause        uint32                 `json:"wanted_pause"`
			WantedContinuation uint32                 `json:"wanted_continuation"`
			Pauses             int                    `json:"pauses"`
			Continuations      int                    `json:"continuations"`
			ActorChoices       int                    `json:"actor_choices"`
			Collections        uint32                 `json:"completed_collections"`
			PeerRanEarly       bool                   `json:"peer_ran_early"`
			Overflow           bool                   `json:"observation_overflow"`
			Checksum           uint64                 `json:"checksum"`
			PairingError       string                 `json:"pairing_error"`
			Raw                []synctest.Observation `json:"observations"`
			Decisions          []explorer.Decision    `json:"runtime_trace"`
			Scope              string                 `json:"scope"`
		}{wantedPause, wantedContinuation, pauses, continuations, actorChoices,
			collections.Load(), peerRanEarly.Load(), overflow, checksum, pairingError, events, result.Trace,
			"finite natural-allocation assist qualification; snapshot after actors, before host driver join; no missing-path success"}
		encodeErr := json.NewEncoder(file).Encode(artifact)
		syncErr := file.Sync()
		closeErr := file.Close()
		if encodeErr != nil || syncErr != nil || closeErr != nil {
			t.Fatalf("saving assist artifact: encode=%v sync=%v close=%v", encodeErr, syncErr, closeErr)
		}
	}
	if overflow || actorBGID != 3 || peerBGID != 4 || enterSequence == 0 || returnSequence <= enterSequence || peerSequence <= returnSequence || !actorFinished.Load() || peerRanEarly.Load() || actorChoices != 1 || collections.Load() != 4 {
		t.Fatalf("assist continuation invariant: overflow=%v actor=%d peer=%d sequences=%d/%d/%d early=%v actorChoices=%d collections=%d", overflow, actorBGID, peerBGID, enterSequence, returnSequence, peerSequence, peerRanEarly.Load(), actorChoices, collections.Load())
	}
	if pauses == 0 {
		t.Skipf("INCONCLUSIVE: natural allocation did not capture assist pause tag %d; no continuation claim", wantedPause)
	}
	if pairingError != "" || pending || continuations != pauses {
		t.Fatalf("assist continuation coverage: pauses=%d continuations=%d pending=%v error=%s", pauses, continuations, pending, pairingError)
	}
	t.Logf("captured %d assist pauses/continuations, one actor choice, peer after actor", pauses)
}

//go:noinline
func allocateAssistProbe(graph []assistProbeNode) uint64 {
	var roots [64]*assistProbeNode
	var checksum uint64
	// 2048 x 256 nodes is finite (roughly 40 MiB of allocated nodes). Only
	// 64 batches remain retained, in addition to the setup pointer graph.
	for batch := 0; batch < 2048; batch++ {
		nodes := make([]assistProbeNode, 256)
		for i := range nodes {
			nodes[i].left = &graph[(batch+i)%len(graph)]
			if i != 0 {
				nodes[i].right = &nodes[i-1]
			}
			nodes[i].value[0] = uint64(batch + i)
			checksum += nodes[i].value[0]
		}
		roots[batch%len(roots)] = &nodes[len(nodes)-1]
	}
	runtime.KeepAlive(roots)
	return checksum
}
