package orchestrator

import (
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
)

// eventLog is a thread-safe recorder for verifying cross-bubble event ordering.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) record(event string) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// assertBefore checks that event a appears before event b in the log.
func assertBefore(t *testing.T, events []string, a, b string) {
	t.Helper()
	ai, bi := -1, -1
	for i, e := range events {
		if e == a && ai == -1 {
			ai = i
		}
		if e == b && bi == -1 {
			bi = i
		}
	}
	if ai == -1 {
		t.Errorf("event %q not found in %v", a, events)
		return
	}
	if bi == -1 {
		t.Errorf("event %q not found in %v", b, events)
		return
	}
	if ai >= bi {
		t.Errorf("expected %q before %q, but got idx %d >= %d\nevents: %v", a, b, ai, bi, events)
	}
}

// TestClientServer verifies basic cross-bubble ping/pong routing
// and that the From field is set correctly by the orchestrator.
func TestClientServer(t *testing.T) {
	orch := New()

	orch.AddBubble("client", func(t *testing.T, node *Node) {
		node.Send("server", "ping")
		resp := node.Recv()
		if resp.From != "server" {
			t.Errorf("resp.From = %q, want %q", resp.From, "server")
		}
		if resp.Payload != "pong" {
			t.Errorf("resp.Payload = %v, want %q", resp.Payload, "pong")
		}
	})

	orch.AddBubble("server", func(t *testing.T, node *Node) {
		msg := node.Recv()
		if msg.From != "client" {
			t.Errorf("msg.From = %q, want %q", msg.From, "client")
		}
		if msg.Payload != "ping" {
			t.Errorf("msg.Payload = %v, want %q", msg.Payload, "ping")
		}
		node.Send(msg.From, "pong")
	})

	orch.Run(t)
}

// TestTurnBasedOrdering verifies the core turn-based invariant:
// when the client sends a request, the server must process and respond
// before the client's Recv completes. An event log records the causal
// ordering from both bubbles.
func TestTurnBasedOrdering(t *testing.T) {
	log := &eventLog{}
	orch := New()

	record := func(event string) {
		synctest.CallExternal(func() { log.record(event) })
	}

	orch.AddBubble("client", func(t *testing.T, node *Node) {
		record("client:pre-send")
		node.Send("server", "req")
		resp := node.Recv()
		record("client:recv-done")
		if resp.Payload != "resp" {
			t.Errorf("resp.Payload = %v, want %q", resp.Payload, "resp")
		}
	})

	orch.AddBubble("server", func(t *testing.T, node *Node) {
		msg := node.Recv()
		record("server:processing")
		node.Send(msg.From, "resp")
	})

	orch.Run(t)

	events := log.snapshot()
	// The client's send causally precedes the server's receive.
	assertBefore(t, events, "client:pre-send", "server:processing")
	// The server processes before the client gets the response.
	assertBefore(t, events, "server:processing", "client:recv-done")
}

// TestMultiHopRPC verifies a three-node RPC chain: A -> B -> C -> B -> A.
// B acts as a proxy, forwarding A's request to C and relaying C's response.
func TestMultiHopRPC(t *testing.T) {
	log := &eventLog{}
	orch := New()

	record := func(event string) {
		synctest.CallExternal(func() { log.record(event) })
	}

	orch.AddBubble("A", func(t *testing.T, node *Node) {
		record("A:send")
		node.Send("B", "hello")
		resp := node.Recv()
		record("A:recv:" + resp.Payload.(string))
	})

	orch.AddBubble("B", func(t *testing.T, node *Node) {
		msg := node.Recv()
		record("B:recv:" + msg.Payload.(string))
		// Forward to C.
		node.Send("C", "forwarded")
		cResp := node.Recv()
		record("B:recv:" + cResp.Payload.(string))
		// Respond to A.
		node.Send(msg.From, "final")
	})

	orch.AddBubble("C", func(t *testing.T, node *Node) {
		msg := node.Recv()
		record("C:recv:" + msg.Payload.(string))
		node.Send(msg.From, "from-C")
	})

	orch.Run(t)

	events := log.snapshot()
	// Full causal chain through all three nodes.
	assertBefore(t, events, "A:send", "B:recv:hello")
	assertBefore(t, events, "B:recv:hello", "C:recv:forwarded")
	assertBefore(t, events, "C:recv:forwarded", "B:recv:from-C")
	assertBefore(t, events, "B:recv:from-C", "A:recv:final")
}

// TestSequentialRPCs verifies that multiple round-trip RPCs on the same
// pair of bubbles complete correctly with responses arriving in order.
func TestSequentialRPCs(t *testing.T) {
	orch := New()
	requests := []string{"first", "second", "third"}
	responses := make([]string, 0, len(requests))

	orch.AddBubble("client", func(t *testing.T, node *Node) {
		for _, req := range requests {
			node.Send("server", req)
			resp := node.Recv()
			responses = append(responses, resp.Payload.(string))
		}
	})

	orch.AddBubble("server", func(t *testing.T, node *Node) {
		for range len(requests) {
			msg := node.Recv()
			node.Send(msg.From, "re:"+msg.Payload.(string))
		}
	})

	orch.Run(t)

	expected := []string{"re:first", "re:second", "re:third"}
	if len(responses) != len(expected) {
		t.Fatalf("got %d responses, want %d", len(responses), len(expected))
	}
	for i, want := range expected {
		if responses[i] != want {
			t.Errorf("responses[%d] = %q, want %q", i, responses[i], want)
		}
	}
}

// TestRPCConvenience verifies that the RPC helper (Send+Recv) works
// and returns the correct From and Payload.
func TestRPCConvenience(t *testing.T) {
	orch := New()
	var result string

	orch.AddBubble("client", func(t *testing.T, node *Node) {
		resp := node.RPC("server", "question")
		result = resp.Payload.(string)
		if resp.From != "server" {
			t.Errorf("resp.From = %q, want %q", resp.From, "server")
		}
	})

	orch.AddBubble("server", func(t *testing.T, node *Node) {
		msg := node.Recv()
		node.Send(msg.From, "answer:"+msg.Payload.(string))
	})

	orch.Run(t)

	if result != "answer:question" {
		t.Errorf("RPC result = %q, want %q", result, "answer:question")
	}
}

// setupClientServer creates a fresh orchestrator with client/server bubbles
// for record/replay tests.
func setupClientServer() *Orchestrator {
	orch := New()

	orch.AddBubble("client", func(t *testing.T, node *Node) {
		node.Send("server", "ping")
		resp := node.Recv()
		if resp.From != "server" {
			t.Errorf("resp.From = %q, want %q", resp.From, "server")
		}
		if resp.Payload != "pong" {
			t.Errorf("resp.Payload = %v, want %q", resp.Payload, "pong")
		}
	})

	orch.AddBubble("server", func(t *testing.T, node *Node) {
		msg := node.Recv()
		if msg.From != "client" {
			t.Errorf("msg.From = %q, want %q", msg.From, "client")
		}
		node.Send(msg.From, "pong")
	})

	return orch
}

// setupMultiHop creates a fresh orchestrator with A→B→C→B→A bubbles.
func setupMultiHop() *Orchestrator {
	orch := New()

	orch.AddBubble("A", func(t *testing.T, node *Node) {
		node.Send("B", "hello")
		node.Recv()
	})

	orch.AddBubble("B", func(t *testing.T, node *Node) {
		msg := node.Recv()
		node.Send("C", "forwarded")
		node.Recv()
		node.Send(msg.From, "final")
	})

	orch.AddBubble("C", func(t *testing.T, node *Node) {
		msg := node.Recv()
		node.Send(msg.From, "from-C")
	})

	return orch
}

// causalTrace extracts the outbox and done events from a trace, which form
// the deterministic "causal" subsequence. Hook events are incidental scheduling
// decisions that may vary in count and position between runs.
func causalTrace(trace []GlobalStep) []GlobalStep {
	var causal []GlobalStep
	for _, s := range trace {
		if s.Event == EventOutbox || s.Event == EventDone {
			causal = append(causal, s)
		}
	}
	return causal
}

// assertCausalTracesEqual checks that the outbox/done subsequences of two
// traces are identical. Hook events are excluded from comparison since they
// may differ between record and replay modes.
func assertCausalTracesEqual(t *testing.T, recorded, replayed []GlobalStep) {
	t.Helper()
	rc := causalTrace(recorded)
	rp := causalTrace(replayed)
	if len(rc) != len(rp) {
		t.Fatalf("causal trace length mismatch: recorded=%d, replayed=%d", len(rc), len(rp))
	}
	for i := range rc {
		if rc[i] != rp[i] {
			t.Errorf("causal trace[%d] mismatch: recorded=%+v, replayed=%+v", i, rc[i], rp[i])
		}
	}
}

// TestRecordReplayClientServer records a client-server trace and replays it,
// asserting the replayed trace is identical.
func TestRecordReplayClientServer(t *testing.T) {
	// Record.
	orch1 := setupClientServer()
	recorded, ok1 := orch1.RunExplore(t, nil)
	if !ok1 {
		t.Fatal("record run failed")
	}
	t.Logf("recorded %d steps", len(recorded))
	for i, s := range recorded {
		t.Logf("  step[%d]: bubble=%q event=%d hookReply=%d", i, s.Bubble, s.Event, s.HookReply)
	}

	// Replay.
	orch2 := setupClientServer()
	replayed, ok2 := orch2.RunExplore(t, recorded)
	if !ok2 {
		t.Fatal("replay run failed")
	}

	assertCausalTracesEqual(t, recorded, replayed)
}

// TestRecordReplayMultiHop records a 3-node A→B→C→B→A trace and replays it.
func TestRecordReplayMultiHop(t *testing.T) {
	// Record.
	orch1 := setupMultiHop()
	recorded, ok1 := orch1.RunExplore(t, nil)
	if !ok1 {
		t.Fatal("record run failed")
	}
	t.Logf("recorded %d steps", len(recorded))
	for i, s := range recorded {
		t.Logf("  step[%d]: bubble=%q event=%d hookReply=%d", i, s.Bubble, s.Event, s.HookReply)
	}

	// Replay.
	orch2 := setupMultiHop()
	replayed, ok2 := orch2.RunExplore(t, recorded)
	if !ok2 {
		t.Fatal("replay run failed")
	}
	// print the repalyed trace
	for i, s := range replayed {
		t.Logf("  step[%d]: bubble=%q event=%d hookReply=%d", i, s.Bubble, s.Event, s.HookReply)
	}

	assertCausalTracesEqual(t, recorded, replayed)
}

// TestRecordTraceStructure records a client-server trace and asserts
// structural properties of the trace.
func TestRecordTraceStructure(t *testing.T) {
	orch := setupClientServer()
	trace, ok := orch.RunExplore(t, nil)
	if !ok {
		t.Fatal("run failed")
	}

	if len(trace) == 0 {
		t.Fatal("trace is empty")
	}

	// Log the full trace for debugging.
	for i, s := range trace {
		name := []string{"outbox", "hook", "done"}[s.Event]
		t.Logf("trace[%d]: %s %s (hookReply=%d)", i, s.Bubble, name, s.HookReply)
	}

	// Check that both bubbles appear in the trace.
	bubbleSeen := map[string]bool{}
	for _, s := range trace {
		bubbleSeen[s.Bubble] = true
	}
	for _, name := range []string{"client", "server"} {
		if !bubbleSeen[name] {
			t.Errorf("bubble %q not found in trace", name)
		}
	}

	// Check that we have at least one of each event type.
	eventCounts := map[EventType]int{}
	for _, s := range trace {
		eventCounts[s.Event]++
	}
	for _, et := range []EventType{EventOutbox, EventHook, EventDone} {
		if eventCounts[et] == 0 {
			t.Errorf("no events of type %d in trace", et)
		}
	}

	// Check there are exactly 2 done events (one per bubble).
	if eventCounts[EventDone] != 2 {
		t.Errorf("expected 2 done events, got %d", eventCounts[EventDone])
	}

	// In the causal trace (outbox+done only), done events come at the end.
	causal := causalTrace(trace)
	seenDone := false
	for _, s := range causal {
		if seenDone && s.Event != EventDone {
			t.Errorf("non-done causal event %+v after a done event", s)
		}
		if s.Event == EventDone {
			seenDone = true
		}
	}

	// Check that each bubble's done event comes after all its outbox events.
	for _, name := range []string{"client", "server"} {
		lastOutbox := -1
		doneIdx := -1
		for i, s := range trace {
			if s.Bubble != name {
				continue
			}
			if s.Event == EventOutbox {
				lastOutbox = i
			}
			if s.Event == EventDone {
				doneIdx = i
			}
		}
		if lastOutbox >= 0 && doneIdx >= 0 && lastOutbox >= doneIdx {
			t.Errorf("bubble %q: outbox at %d after done at %d", name, lastOutbox, doneIdx)
		}
	}
}

// --- KV store tests ---
//
// These tests use an in-memory key-value store bubble to demonstrate
// stateful cross-bubble interactions where message ordering determines
// the final system state.

// kvOp represents a key-value store operation for cross-bubble RPC.
type kvOp struct {
	Op    string // "get", "put"
	Key   string
	Value any // value for put; response value for get result
}

// TestKVStoreReadAfterWrite verifies that a value written by one bubble
// is visible to a subsequent reader, demonstrating causal ordering through
// the orchestrator's message routing.
func TestKVStoreReadAfterWrite(t *testing.T) {
	orch := New()
	var readResult any

	orch.AddBubble("store", func(t *testing.T, node *Node) {
		kv := map[string]any{}
		for i := 0; i < 2; i++ { // 1 put from writer + 1 get from reader
			msg := node.Recv()
			op := msg.Payload.(kvOp)
			switch op.Op {
			case "put":
				kv[op.Key] = op.Value
				node.Send(msg.From, kvOp{Op: "ack"})
			case "get":
				node.Send(msg.From, kvOp{Op: "result", Key: op.Key, Value: kv[op.Key]})
			}
		}
	})

	orch.AddBubble("writer", func(t *testing.T, node *Node) {
		node.RPC("store", kvOp{Op: "put", Key: "x", Value: "hello"})
		node.RPC("reader", "written") // notify reader, wait for ack
	})

	orch.AddBubble("reader", func(t *testing.T, node *Node) {
		msg := node.Recv()            // wait for writer notification
		node.Send(msg.From, "ack")    // ack so writer can finish
		resp := node.RPC("store", kvOp{Op: "get", Key: "x"})
		synctest.CallExternal(func() {
			readResult = resp.Payload.(kvOp).Value
		})
	})

	orch.Run(t)

	if readResult != "hello" {
		t.Errorf("read x = %v, want %q", readResult, "hello")
	}
}

// TestKVStoreConditionalUpdate verifies read-modify-write: a client reads
// a counter, increments it, and writes it back. Two cycles produce counter=2.
func TestKVStoreConditionalUpdate(t *testing.T) {
	orch := New()
	var finalCounter int

	orch.AddBubble("store", func(t *testing.T, node *Node) {
		kv := map[string]any{"counter": 0}
		for i := 0; i < 4; i++ { // 2 gets + 2 puts
			msg := node.Recv()
			op := msg.Payload.(kvOp)
			switch op.Op {
			case "get":
				node.Send(msg.From, kvOp{Op: "result", Key: op.Key, Value: kv[op.Key]})
			case "put":
				kv[op.Key] = op.Value
				node.Send(msg.From, kvOp{Op: "ack"})
			}
		}
		synctest.CallExternal(func() { finalCounter = kv["counter"].(int) })
	})

	orch.AddBubble("client", func(t *testing.T, node *Node) {
		for i := 0; i < 2; i++ {
			resp := node.RPC("store", kvOp{Op: "get", Key: "counter"})
			val := resp.Payload.(kvOp).Value.(int)
			node.RPC("store", kvOp{Op: "put", Key: "counter", Value: val + 1})
		}
	})

	orch.Run(t)

	if finalCounter != 2 {
		t.Errorf("counter = %d, want 2", finalCounter)
	}
}

// TestKVStoreBankTransfer verifies a multi-step transaction: a transfer bubble
// reads alice's balance, debits her, reads bob's balance, credits him.
// An auditor verifies the conservation invariant (total balance unchanged).
func TestKVStoreBankTransfer(t *testing.T) {
	orch := New()
	var auditAlice, auditBob int

	orch.AddBubble("bank", func(t *testing.T, node *Node) {
		accounts := map[string]int{"alice": 100, "bob": 50}
		for i := 0; i < 6; i++ { // 4 from transfer + 2 from auditor
			msg := node.Recv()
			op := msg.Payload.(kvOp)
			switch op.Op {
			case "get":
				node.Send(msg.From, kvOp{Op: "result", Key: op.Key, Value: accounts[op.Key]})
			case "put":
				accounts[op.Key] = op.Value.(int)
				node.Send(msg.From, kvOp{Op: "ack"})
			}
		}
	})

	orch.AddBubble("transfer", func(t *testing.T, node *Node) {
		// Read alice's balance.
		resp := node.RPC("bank", kvOp{Op: "get", Key: "alice"})
		aliceBal := resp.Payload.(kvOp).Value.(int)

		// Debit alice by 30.
		node.RPC("bank", kvOp{Op: "put", Key: "alice", Value: aliceBal - 30})

		// Read bob's balance.
		resp = node.RPC("bank", kvOp{Op: "get", Key: "bob"})
		bobBal := resp.Payload.(kvOp).Value.(int)

		// Credit bob by 30.
		node.RPC("bank", kvOp{Op: "put", Key: "bob", Value: bobBal + 30})

		// Notify auditor that transfer is complete (RPC to get ack).
		node.RPC("auditor", "transfer-complete")
	})

	orch.AddBubble("auditor", func(t *testing.T, node *Node) {
		msg := node.Recv()         // wait for transfer notification
		node.Send(msg.From, "ack") // ack so transfer can finish

		aliceResp := node.RPC("bank", kvOp{Op: "get", Key: "alice"})
		bobResp := node.RPC("bank", kvOp{Op: "get", Key: "bob"})
		synctest.CallExternal(func() {
			auditAlice = aliceResp.Payload.(kvOp).Value.(int)
			auditBob = bobResp.Payload.(kvOp).Value.(int)
		})
	})

	orch.Run(t)

	total := auditAlice + auditBob
	if total != 150 {
		t.Errorf("total balance = %d, want 150 (alice=%d, bob=%d)", total, auditAlice, auditBob)
	}
	if auditAlice != 70 {
		t.Errorf("alice balance = %d, want 70", auditAlice)
	}
	if auditBob != 80 {
		t.Errorf("bob balance = %d, want 80", auditBob)
	}
}

// TestKVStoreBankTransferReplay records a bank transfer trace and replays it,
// verifying that the same final balances are produced deterministically.
func TestKVStoreBankTransferReplay(t *testing.T) {
	type balances struct{ alice, bob int }

	setup := func() (*Orchestrator, *balances) {
		orch := New()
		bal := &balances{}

		orch.AddBubble("bank", func(t *testing.T, node *Node) {
			accounts := map[string]int{"alice": 100, "bob": 50}
			for i := 0; i < 6; i++ {
				msg := node.Recv()
				op := msg.Payload.(kvOp)
				switch op.Op {
				case "get":
					node.Send(msg.From, kvOp{Op: "result", Key: op.Key, Value: accounts[op.Key]})
				case "put":
					accounts[op.Key] = op.Value.(int)
					node.Send(msg.From, kvOp{Op: "ack"})
				}
			}
			synctest.CallExternal(func() {
				bal.alice = accounts["alice"]
				bal.bob = accounts["bob"]
			})
		})

		orch.AddBubble("transfer", func(t *testing.T, node *Node) {
			resp := node.RPC("bank", kvOp{Op: "get", Key: "alice"})
			aliceBal := resp.Payload.(kvOp).Value.(int)
			node.RPC("bank", kvOp{Op: "put", Key: "alice", Value: aliceBal - 30})

			resp = node.RPC("bank", kvOp{Op: "get", Key: "bob"})
			bobBal := resp.Payload.(kvOp).Value.(int)
			node.RPC("bank", kvOp{Op: "put", Key: "bob", Value: bobBal + 30})

			node.RPC("auditor", "done") // notify + wait for ack
		})

		orch.AddBubble("auditor", func(t *testing.T, node *Node) {
			msg := node.Recv()
			node.Send(msg.From, "ack")
			node.RPC("bank", kvOp{Op: "get", Key: "alice"})
			node.RPC("bank", kvOp{Op: "get", Key: "bob"})
		})

		return orch, bal
	}

	// Record.
	orch1, bal1 := setup()
	recorded, ok := orch1.RunExplore(t, nil)
	if !ok {
		t.Fatal("record run failed")
	}
	t.Logf("recorded: alice=%d, bob=%d, total=%d, steps=%d",
		bal1.alice, bal1.bob, bal1.alice+bal1.bob, len(recorded))

	// Replay.
	orch2, bal2 := setup()
	replayed, ok := orch2.RunExplore(t, recorded)
	if !ok {
		t.Fatal("replay run failed")
	}

	if bal2.alice != bal1.alice || bal2.bob != bal1.bob {
		t.Errorf("replay balances differ: got alice=%d,bob=%d want alice=%d,bob=%d",
			bal2.alice, bal2.bob, bal1.alice, bal1.bob)
	}
	assertCausalTracesEqual(t, recorded, replayed)
}

// TestKVStoreMultiKeyTransaction verifies a cross-key transaction pattern:
// a client atomically swaps the values of two keys by reading both, then
// writing both in reverse. The event log proves the causal ordering.
func TestKVStoreMultiKeyTransaction(t *testing.T) {
	log := &eventLog{}
	orch := New()

	record := func(event string) {
		synctest.CallExternal(func() { log.record(event) })
	}

	orch.AddBubble("store", func(t *testing.T, node *Node) {
		kv := map[string]any{"a": "alpha", "b": "beta"}
		for i := 0; i < 4; i++ { // 2 gets + 2 puts
			msg := node.Recv()
			op := msg.Payload.(kvOp)
			switch op.Op {
			case "get":
				record(fmt.Sprintf("store:get:%s=%v", op.Key, kv[op.Key]))
				node.Send(msg.From, kvOp{Op: "result", Key: op.Key, Value: kv[op.Key]})
			case "put":
				record(fmt.Sprintf("store:put:%s=%v", op.Key, op.Value))
				kv[op.Key] = op.Value
				node.Send(msg.From, kvOp{Op: "ack"})
			}
		}
	})

	orch.AddBubble("swapper", func(t *testing.T, node *Node) {
		// Read both keys.
		record("swapper:read-a")
		respA := node.RPC("store", kvOp{Op: "get", Key: "a"})
		valA := respA.Payload.(kvOp).Value

		record("swapper:read-b")
		respB := node.RPC("store", kvOp{Op: "get", Key: "b"})
		valB := respB.Payload.(kvOp).Value

		// Write them swapped.
		record("swapper:write-a")
		node.RPC("store", kvOp{Op: "put", Key: "a", Value: valB})
		record("swapper:write-b")
		node.RPC("store", kvOp{Op: "put", Key: "b", Value: valA})
	})

	orch.Run(t)

	events := log.snapshot()
	// Verify the full causal chain: reads happen before writes,
	// and the store sees operations in the correct order.
	assertBefore(t, events, "swapper:read-a", "store:get:a=alpha")
	assertBefore(t, events, "store:get:a=alpha", "swapper:read-b")
	assertBefore(t, events, "swapper:read-b", "store:get:b=beta")
	assertBefore(t, events, "store:get:b=beta", "swapper:write-a")
	assertBefore(t, events, "swapper:write-a", "store:put:a=beta")
	assertBefore(t, events, "store:put:a=beta", "swapper:write-b")
	assertBefore(t, events, "swapper:write-b", "store:put:b=alpha")
}
