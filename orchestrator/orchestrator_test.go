package orchestrator

import "testing"

// TestClientServer verifies basic cross-bubble message routing.
// The client sends "ping" to the server, and the server replies "pong".
// Run using "make test pkg=orchestrator"
func TestClientServer(t *testing.T) {
	orch := New()

	orch.AddBubble("client", func(t *testing.T, node *Node) {
		node.Send("server", "ping")
		resp := node.Recv()
		if resp.Payload != "pong" {
			t.Fatalf("expected pong, got %v", resp.Payload)
		}
		t.Logf("client: got %v", resp.Payload)
	})

	orch.AddBubble("server", func(t *testing.T, node *Node) {
		msg := node.Recv()
		if msg.Payload != "ping" {
			t.Fatalf("expected ping, got %v", msg.Payload)
		}
		t.Logf("server: got %v from %s, replying pong", msg.Payload, msg.From)
		node.Send(msg.From, "pong")
	})

	orch.Run(t)
}
