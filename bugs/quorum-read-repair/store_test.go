package quorumreadrepair

import (
	"reflect"
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestCorrectMergePreservesConcurrentSiblings(t *testing.T) {
	existing := []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}}
	incoming := []VersionedValue{{Value: "B", Clock: Clock{"C2": 1}}}

	got := mergeSiblings(existing, incoming)

	assertValues(t, got, []VersionedValue{
		{Value: "A", Clock: Clock{"C1": 1}},
		{Value: "B", Clock: Clock{"C2": 1}},
	})
}

func TestCorrectMergeDropsDominatedValue(t *testing.T) {
	existing := []VersionedValue{{Value: "old", Clock: Clock{"C1": 1}}}
	incoming := []VersionedValue{{Value: "new", Clock: Clock{"C1": 2}}}

	got := mergeSiblings(existing, incoming)

	assertValues(t, got, []VersionedValue{
		{Value: "new", Clock: Clock{"C1": 2}},
	})
}

func TestCorrectMergePreservesConcurrentSamePayloadDeterministicOrder(t *testing.T) {
	existing := []VersionedValue{{Value: "A", Clock: Clock{"C2": 1}}}
	incoming := []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}}

	got := mergeSiblings(existing, incoming)

	assertValues(t, got, []VersionedValue{
		{Value: "A", Clock: Clock{"C1": 1}},
		{Value: "A", Clock: Clock{"C2": 1}},
	})
}

func TestCorrectMergeDeepCopiesInputs(t *testing.T) {
	existing := []VersionedValue{{Value: "left", Clock: Clock{"C1": 1}}}
	incoming := []VersionedValue{{Value: "right", Clock: Clock{"C2": 1}}}

	got := mergeSiblings(existing, incoming)

	existing[0].Value = "mutated-left"
	existing[0].Clock["C1"] = 99
	incoming[0].Value = "mutated-right"
	incoming[0].Clock["C2"] = 88

	assertValues(t, got, []VersionedValue{
		{Value: "left", Clock: Clock{"C1": 1}},
		{Value: "right", Clock: Clock{"C2": 1}},
	})
}

func TestBuggyRepairMergeDropsConcurrentSibling(t *testing.T) {
	existing := []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}}
	incoming := []VersionedValue{{Value: "B", Clock: Clock{"C2": 1}}}

	got := buggyRepairMerge(existing, incoming)

	if len(got) != 1 {
		t.Fatalf("buggyRepairMerge returned %d values, want 1", len(got))
	}
}

func TestBuggyRepairMergeDeterministicTieWinner(t *testing.T) {
	existing := []VersionedValue{{Value: "A", Clock: Clock{"C2": 2}}}
	incoming := []VersionedValue{{Value: "A", Clock: Clock{"C1": 2}}}

	got := buggyRepairMerge(existing, incoming)

	assertValues(t, got, []VersionedValue{
		{Value: "A", Clock: Clock{"C2": 2}},
	})
}

func TestReplicaFIFO_NormalWritesPreserveSiblings(t *testing.T) {
	runtime.GOMAXPROCS(8)

	r := NewOrchestratorTransport("R1")
	c := NewOrchestratorTransport("C")
	r.Connect(c)
	c.Connect(r)

	orch := orchestrator.New()
	orch.AddNode(r, func(t *testing.T) {
		r.StartBridge()
		replica := NewReplica("R1", r)
		replica.Start()
	})
	orch.AddNode(c, func(t *testing.T) {
		c.StartBridge()
		c.Send("R1", Message{Kind: MsgPut, RequestID: "put-a", Key: "x", Values: []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}}})
		c.Send("R1", Message{Kind: MsgPut, RequestID: "put-b", Key: "x", Values: []VersionedValue{{Value: "B", Clock: Clock{"C2": 1}}}})

		waitForAck(t, c, MsgPutAck, "put-a")
		waitForAck(t, c, MsgPutAck, "put-b")

		c.Send("R1", Message{Kind: MsgGet, RequestID: "get", Key: "x"})
		resp := waitForResponse(t, c, MsgGetResp, "get")
		assertValues(t, resp.Values, []VersionedValue{
			{Value: "A", Clock: Clock{"C1": 1}},
			{Value: "B", Clock: Clock{"C2": 1}},
		})
		c.Shutdown()
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("replica FIFO run failed")
	}
}

func assertValues(t *testing.T, got, want []VersionedValue) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(got) = %d, want %d; got=%#v want=%#v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Value != want[i].Value {
			t.Fatalf("got[%d].Value = %q, want %q; got=%#v want=%#v", i, got[i].Value, want[i].Value, got, want)
		}
		if !reflect.DeepEqual(got[i].Clock, want[i].Clock) {
			t.Fatalf("got[%d].Clock = %#v, want %#v; got=%#v want=%#v", i, got[i].Clock, want[i].Clock, got, want)
		}
	}
}

func waitForAck(t *testing.T, tr *OrchestratorTransport, kind MsgKind, requestID string) Message {
	t.Helper()
	return waitForMessage(t, tr, kind, requestID)
}

func waitForResponse(t *testing.T, tr *OrchestratorTransport, kind MsgKind, requestID string) Message {
	t.Helper()
	return waitForMessage(t, tr, kind, requestID)
}

func waitForMessage(t *testing.T, tr *OrchestratorTransport, kind MsgKind, requestID string) Message {
	t.Helper()
	for msg := range tr.Mailbox() {
		if msg.Kind == kind && msg.RequestID == requestID {
			return msg
		}
	}
	t.Fatalf("mailbox closed before %s %s", msgKindName(kind), requestID)
	return Message{}
}
