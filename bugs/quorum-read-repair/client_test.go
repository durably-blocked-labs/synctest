package quorumreadrepair

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestClientFIFO_PutAndReadRepairs(t *testing.T) {
	runtime.GOMAXPROCS(8)

	addrs := []string{"R1", "R2", "C1"}
	transports := setupCluster(addrs)
	orch := orchestrator.New()
	addReplicas(orch, []string{"R1", "R2"}, transports)

	orch.AddNode(transports["C1"], func(t *testing.T) {
		tr := transports["C1"]
		tr.StartBridge()
		client := NewClient("C1", tr, []string{"R1", "R2"})
		client.Put("x", "A")
		got := client.GetAndRepair("x", "read-1")
		assertValues(t, got, []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}})
		shutdownTransports(transports, addrs)
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("client FIFO run failed")
	}
}

func TestClientQuorum_PutAndReadRepairsStaleReplica(t *testing.T) {
	runtime.GOMAXPROCS(8)

	addrs := []string{"R1", "R2", "R3", "C1"}
	transports := setupCluster(addrs)
	orch := orchestrator.New()
	addReplicas(orch, []string{"R1", "R2", "R3"}, transports)

	orch.AddNode(transports["C1"], func(t *testing.T) {
		tr := transports["C1"]
		tr.StartBridge()
		writer := NewClient("C1", tr, []string{"R1", "R2"})
		writer.Put("x", "A")

		reader := NewClient("C1", tr, []string{"R1", "R2", "R3"})
		got := reader.GetAndRepair("x", "read-1")
		assertValues(t, got, []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}})
		msg, ok := reader.inbox.waitFor(MsgRepairAck, "read-1-repair-R3")
		if !ok || msg.From != "R3" {
			t.Fatalf("repair ack from R3 = %#v, ok=%v", msg, ok)
		}
		shutdownTransports(transports, addrs)
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("client quorum run failed")
	}
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

func shutdownTransports(transports map[string]*OrchestratorTransport, addrs []string) {
	for _, addr := range addrs {
		transports[addr].Shutdown()
	}
}
