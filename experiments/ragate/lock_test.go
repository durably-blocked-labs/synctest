package ragate

import "testing"

type sentMessage struct {
	to  string
	msg Message
}

type recordingTransport struct {
	sent []sentMessage
}

func (tr *recordingTransport) Send(to string, msg Message) {
	tr.sent = append(tr.sent, sentMessage{to: to, msg: msg})
}

func (*recordingTransport) Mailbox() <-chan Message { return nil }

// waitingForLastReply constructs a protocol phase, not a goroutine schedule.
// A already has B's permission and is deferring B's own request.
func waitingForLastReply() (*GateRANode, *recordingTransport) {
	tr := &recordingTransport{}
	n := NewGateRANode("A", tr, []string{"B", "C"})
	n.state = Wanted
	n.clock = 2
	n.reqClock = 1
	n.repliesReceived = 1
	n.gate = make(chan struct{})
	n.deferred = []string{"B"}
	return n, tr
}

func TestLastReplyNotifiesBeforeOwnership(t *testing.T) {
	n, _ := waitingForLastReply()
	n.handleMessage(Message{Kind: MsgReply, From: "C", Timestamp: 2})
	if n.state != Wanted {
		t.Fatalf("notification changed acquisition state: got %v, want Wanted", n.state)
	}
	select {
	case <-n.gate:
	default:
		t.Fatal("last permission did not open gate")
	}
}

func TestFlusherBeforeOwnershipGrantsPrematurePermission(t *testing.T) {
	n, tr := waitingForLastReply()
	var diagnostics []string
	n.OnViolation = func(detail string) { diagnostics = append(diagnostics, detail) }
	n.handleMessage(Message{Kind: MsgReply, From: "C", Timestamp: 2})
	n.deferredFlusher()
	if n.state != Wanted {
		t.Fatalf("flusher changed state: got %v, want Wanted", n.state)
	}
	if len(tr.sent) != 1 || tr.sent[0].to != "B" || tr.sent[0].msg.Kind != MsgReply || tr.sent[0].msg.From != "A" {
		t.Fatalf("premature permission = %+v, want one A->B Reply", tr.sent)
	}
	if len(diagnostics) != 1 {
		t.Fatalf("diagnostics = %v, want one premature-permission diagnostic", diagnostics)
	}
	n.ReleaseLock()
	if len(tr.sent) != 1 {
		t.Fatalf("release duplicated an already flushed reply: %+v", tr.sent)
	}
}

func TestPublishedOwnershipKeepsPermissionDeferredUntilRelease(t *testing.T) {
	n, tr := waitingForLastReply()
	n.OnViolation = func(detail string) { t.Fatalf("unexpected diagnostic: %s", detail) }
	// This phase represents App having published ownership before the helper
	// inspects it. The test does not claim to drive that concurrent ordering.
	n.state = Held
	close(n.gate)
	n.deferredFlusher()
	if len(tr.sent) != 0 || len(n.deferred) != 1 {
		t.Fatalf("permission changed while Held: sent=%+v deferred=%v", tr.sent, n.deferred)
	}
	n.ReleaseLock()
	if n.state != Released || len(tr.sent) != 1 || tr.sent[0].to != "B" || tr.sent[0].msg.Kind != MsgReply {
		t.Fatalf("release did not grant deferred permission: state=%v sent=%+v", n.state, tr.sent)
	}
}

func TestEmptyDeferredQueueDoesNotReportPrematurePermission(t *testing.T) {
	n, tr := waitingForLastReply()
	n.deferred = nil
	n.OnViolation = func(detail string) { t.Fatalf("unexpected diagnostic: %s", detail) }
	n.handleMessage(Message{Kind: MsgReply, From: "C", Timestamp: 2})
	n.deferredFlusher()
	if len(tr.sent) != 0 {
		t.Fatalf("sent permission without a deferred request: %+v", tr.sent)
	}
}
