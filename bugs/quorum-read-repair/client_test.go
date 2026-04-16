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
	addReplicas(orch, []string{"R1", "R2", "R3"}, transports)

	orch.AddNode(transports["C1"], func(t *testing.T) {
		tr := transports["C1"]
		tr.StartBridge()
		client := NewClient("C1", tr, []string{"R1", "R2", "R3"})
		client.Put("x", "A")
		got := client.GetAndRepair("x", "read-1")
		assertValues(t, got, []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}})
		tr.Shutdown()
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("client FIFO run failed")
	}
}
