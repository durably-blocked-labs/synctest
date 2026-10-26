package ragate

// MsgKind identifies the two RA protocol messages.
type MsgKind int

const (
	MsgRequest MsgKind = iota
	MsgReply
)

// Message carries a request priority or reply timestamp.
type Message struct {
	Kind      MsgKind
	From      string
	Timestamp int
}

// nodeTransport is the protocol-facing interface. A Bubbles adapter must also
// implement the orchestrator's qualified publication and delivery contract.
type nodeTransport interface {
	Send(to string, msg Message)
	Mailbox() <-chan Message
}
