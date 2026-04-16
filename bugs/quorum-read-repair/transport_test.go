package quorumreadrepair

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestTransportSmoke_FIFODeliversPut(t *testing.T) {
	runtime.GOMAXPROCS(8)

	a := NewOrchestratorTransport("A")
	b := NewOrchestratorTransport("B")
	a.Connect(b)
	b.Connect(a)

	orch := orchestrator.New()
	orch.AddNode(a, func(t *testing.T) {
		a.StartBridge()
		a.Send("B", Message{Kind: MsgPut, From: "A", Key: "x", Values: []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}}})
		a.Shutdown()
	})
	orch.AddNode(b, func(t *testing.T) {
		b.StartBridge()
		msg := <-b.Mailbox()
		if msg.Kind != MsgPut || msg.From != "A" || msg.Key != "x" {
			t.Fatalf("delivered message = %#v", msg)
		}
		b.Shutdown()
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("transport smoke run failed")
	}
}
