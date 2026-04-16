package quorumreadrepair

import (
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

var replicaAddrs = []string{"R1", "R2", "R3"}
var clientAddrs = []string{"C1", "C2", "Reader"}

var outcome struct {
	mu             sync.Mutex
	values         map[string][]string
	firstBugValues map[string][]string
	bug            bool
}

func resetObservedOutcome() {
	outcome.mu.Lock()
	defer outcome.mu.Unlock()
	outcome.values = make(map[string][]string)
	outcome.firstBugValues = nil
	outcome.bug = false
}

func observedBugFound() bool {
	outcome.mu.Lock()
	defer outcome.mu.Unlock()
	return outcome.bug
}

func lastObservedValues() map[string][]string {
	outcome.mu.Lock()
	defer outcome.mu.Unlock()

	if outcome.firstBugValues != nil {
		return cloneOutcomeValues(outcome.firstBugValues)
	}
	return cloneOutcomeValues(outcome.values)
}

func setupCluster(addrs []string) map[string]*OrchestratorTransport {
	transports := make(map[string]*OrchestratorTransport, len(addrs))
	for _, addr := range addrs {
		transports[addr] = NewOrchestratorTransport(addr)
	}
	for _, tr := range transports {
		for _, peer := range transports {
			if tr != peer {
				tr.Connect(peer)
			}
		}
	}
	return transports
}

func addReplicas(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) map[string]*Replica {
	replicas := make(map[string]*Replica, len(addrs))
	for _, addr := range addrs {
		addr := addr
		tr := transports[addr]
		replica := NewReplica(addr, tr)
		replicas[addr] = replica
		orch.AddNode(tr, func(t *testing.T) {
			tr.StartBridge()
			replica.Start()
		})
	}
	return replicas
}

func addQuorumReadRepairScenario(orch *orchestrator.Orchestrator, checked bool) {
	all := append([]string{}, replicaAddrs...)
	all = append(all, clientAddrs...)
	transports := setupCluster(all)
	replicas := addReplicas(orch, replicaAddrs, transports)

	orch.AddNode(transports["C1"], func(t *testing.T) {
		tr := transports["C1"]
		tr.StartBridge()
		client := NewClient("C1", tr, replicaAddrs)
		requestID := client.Put("x", "A")
		waitForExtraPutAcks(client, requestID, len(replicaAddrs)-quorum)
		tr.SendControl("C2", "c1-all-done")
		tr.SendControl("Reader", "c1-done")
	})

	orch.AddNode(transports["C2"], func(t *testing.T) {
		tr := transports["C2"]
		tr.StartBridge()
		tr.WaitControl("c1-all-done")
		client := NewClient("C2", tr, []string{"R2", "R3"})
		client.Put("x", "B")
		tr.SendControl("Reader", "c2-done")
		tr.Send("R1", Message{
			Kind:      MsgPut,
			From:      "C2",
			RequestID: "C2-late-R1",
			Key:       "x",
			Values: []VersionedValue{{
				Value: "B",
				Clock: Clock{"C2": 1},
			}},
		})
	})

	orch.AddNode(transports["Reader"], func(t *testing.T) {
		tr := transports["Reader"]
		tr.StartBridge()
		tr.WaitControl("c1-done")
		tr.WaitControl("c2-done")

		client := NewClient("Reader", tr, replicaAddrs)
		client.GetAndRepair("x", "read-1")
		final := client.GetAndRepair("x", "read-2")
		if len(final) == 0 {
			return
		}
		recordOutcome("reader", final)
		recordReplicaOutcomes(replicas)
		if checked && observedBugFound() {
			t.Errorf("lost concurrent sibling or divergent replicas: got %v want every replica [A B]", lastObservedValues())
		}
	})
}

func addFocusedReadRepairRace(orch *orchestrator.Orchestrator) {
	addrs := []string{"R1", "Writer", "Repairer", "Checker"}
	transports := setupCluster(addrs)
	replicas := addReplicas(orch, []string{"R1"}, transports)
	replicas["R1"].store["x"] = []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}}

	orch.AddNode(transports["Writer"], func(t *testing.T) {
		tr := transports["Writer"]
		tr.StartBridge()
		tr.Send("R1", Message{
			Kind:      MsgPut,
			From:      "Writer",
			RequestID: "late-put",
			Key:       "x",
			Values: []VersionedValue{{
				Value: "B",
				Clock: Clock{"C2": 1},
			}},
		})
		inbox := clientInbox{transport: tr}
		inbox.waitFor(MsgPutAck, "late-put")
		tr.SendControl("Checker", "writer-done")
	})

	orch.AddNode(transports["Repairer"], func(t *testing.T) {
		tr := transports["Repairer"]
		tr.StartBridge()
		tr.Send("R1", Message{
			Kind:      MsgRepair,
			From:      "Repairer",
			RequestID: "repair",
			Key:       "x",
			Values: []VersionedValue{
				{Value: "A", Clock: Clock{"C1": 1}},
				{Value: "B", Clock: Clock{"C2": 1}},
			},
		})
		inbox := clientInbox{transport: tr}
		inbox.waitFor(MsgRepairAck, "repair")
		tr.SendControl("Checker", "repair-done")
	})

	orch.AddNode(transports["Checker"], func(t *testing.T) {
		tr := transports["Checker"]
		tr.StartBridge()
		tr.WaitControl("writer-done")
		tr.WaitControl("repair-done")
		recordOutcome("R1", replicas["R1"].Snapshot("x"))
	})
}

func waitForExtraPutAcks(client *Client, requestID string, extra int) {
	for i := 0; i < extra; i++ {
		if _, ok := client.inbox.waitFor(MsgPutAck, requestID); !ok {
			return
		}
	}
}

func recordReplicaOutcomes(replicas map[string]*Replica) {
	for _, addr := range replicaAddrs {
		recordOutcome(addr, replicas[addr].Snapshot("x"))
	}
}

func recordOutcome(replica string, values []VersionedValue) {
	got := valuesOnly(values)

	outcome.mu.Lock()
	defer outcome.mu.Unlock()
	outcome.values[replica] = got
	if !stringSetEqual(got, []string{"A", "B"}) {
		if !outcome.bug {
			outcome.firstBugValues = cloneOutcomeValues(outcome.values)
		}
		outcome.bug = true
	}
}

func cloneOutcomeValues(values map[string][]string) map[string][]string {
	out := make(map[string][]string, len(values))
	for replica, replicaValues := range values {
		out[replica] = append([]string(nil), replicaValues...)
	}
	return out
}

func valuesOnly(values []VersionedValue) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, value.Value)
	}
	sort.Strings(out)
	return out
}

func hasValues(values []VersionedValue, want ...string) bool {
	return stringSetEqual(valuesOnly(values), want)
}

func stringSetEqual(got, want []string) bool {
	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	sort.Strings(got)
	sort.Strings(want)
	return strings.Join(got, ",") == strings.Join(want, ",")
}
