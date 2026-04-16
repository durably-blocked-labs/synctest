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

func TestTransportSendFreezesPayload(t *testing.T) {
	runtime.GOMAXPROCS(8)

	a := NewOrchestratorTransport("A")
	b := NewOrchestratorTransport("B")
	a.Connect(b)
	b.Connect(a)

	orch := orchestrator.New()
	orch.AddNode(a, func(t *testing.T) {
		a.StartBridge()
		msg := Message{
			Kind: MsgPut,
			From: "A",
			Key:  "x",
			Values: []VersionedValue{{
				Value: "original",
				Clock: Clock{"C1": 1},
			}},
		}
		a.Send("B", msg)
		msg.Values[0].Value = "mutated"
		msg.Values[0].Clock["C1"] = 99
		a.Shutdown()
	})
	orch.AddNode(b, func(t *testing.T) {
		b.StartBridge()
		msg := <-b.Mailbox()
		if got, want := msg.Values[0].Value, "original"; got != want {
			t.Fatalf("message value = %q, want %q", got, want)
		}
		if got, want := msg.Values[0].Clock["C1"], 1; got != want {
			t.Fatalf("message clock = %d, want %d", got, want)
		}
		b.Shutdown()
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("transport payload freeze run failed")
	}
}

func TestTransportControlRoutesToControlMailbox(t *testing.T) {
	runtime.GOMAXPROCS(8)

	a := NewOrchestratorTransport("A")
	b := NewOrchestratorTransport("B")
	a.Connect(b)
	b.Connect(a)

	orch := orchestrator.New()
	orch.AddNode(a, func(t *testing.T) {
		a.StartBridge()
		a.SendControl("B", "ctrl-1")
		a.Shutdown()
	})
	orch.AddNode(b, func(t *testing.T) {
		b.StartBridge()
		msg := <-b.ControlMailbox()
		if msg.Kind != MsgControl || msg.Control != "ctrl-1" || msg.From != "A" {
			t.Fatalf("control message = %#v", msg)
		}
		select {
		case msg := <-b.Mailbox():
			t.Fatalf("unexpected regular mailbox message = %#v", msg)
		default:
		}
		b.Shutdown()
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("transport control routing run failed")
	}
}
