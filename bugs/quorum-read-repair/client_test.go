package quorumreadrepair

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestClientFIFO_PutAndReadRepairs(t *testing.T) {
	runtime.GOMAXPROCS(8)

	addrs := []string{"R1", "R2", "R3", "C1"}
	transports := setupCluster(addrs)
	orch := orchestrator.New()
	addReplicas(orch, []string{"R1", "R2"}, transports)

	orch.AddNode(transports["C1"], func(t *testing.T) {
		tr := transports["C1"]
		tr.StartBridge()
		client := NewClient("C1", tr, []string{"R1", "R2", "R3"})
		client.Put("x", "A")
		got := client.GetAndRepair("x", "read-1")
		assertValues(t, got, []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}})
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("client FIFO run failed")
	}

	transports["C1"].Shutdown()
	transports["R1"].Shutdown()
	transports["R2"].Shutdown()
}

func TestClientQuorum_PutAndReadRepairsWithUnavailableReplica(t *testing.T) {
	runtime.GOMAXPROCS(8)

	addrs := []string{"R1", "R2", "R3", "C1"}
	transports := setupCluster(addrs)
	orch := orchestrator.New()
	addReplicas(orch, []string{"R1", "R2"}, transports)

	orch.AddNode(transports["C1"], func(t *testing.T) {
		tr := transports["C1"]
		tr.StartBridge()
		client := NewClient("C1", tr, []string{"R1", "R2", "R3"})
		client.Put("x", "A")
		got := client.GetAndRepair("x", "read-1")
		assertValues(t, got, []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}})
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("client quorum run failed")
	}

	repairs := drainTransportMailbox(transports["R3"])
	if !containsRepair(repairs, "read-1-repair-R3") {
		t.Fatalf("repair to R3 not delivered; got=%#v", repairs)
	}

	transports["C1"].Shutdown()
	transports["R1"].Shutdown()
	transports["R2"].Shutdown()
}

func TestNewClientUsesPerInstanceInbox(t *testing.T) {
	c1 := NewClient("C1", NewOrchestratorTransport("C1"), []string{"R1"})
	c2 := NewClient("C2", NewOrchestratorTransport("C2"), []string{"R1"})

	if c1.inbox == nil || c2.inbox == nil {
		t.Fatal("client inbox was not initialized")
	}
	if c1.inbox == c2.inbox {
		t.Fatal("client inbox should be per-instance, not shared")
	}
	if c1.inbox.transport != c1.tr || c2.inbox.transport != c2.tr {
		t.Fatal("client inbox transport should match the owning client transport")
	}
}

func drainTransportMailbox(tr *OrchestratorTransport) []Message {
	var msgs []Message
	for {
		select {
		case msg := <-tr.mailbox:
			msgs = append(msgs, msg)
		default:
			return msgs
		}
	}
}

func containsRepair(msgs []Message, requestID string) bool {
	for _, msg := range msgs {
		if msg.Kind == MsgRepair && msg.RequestID == requestID {
			return true
		}
	}
	return false
}
