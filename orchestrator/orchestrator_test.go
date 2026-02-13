package orchestrator

import (
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
