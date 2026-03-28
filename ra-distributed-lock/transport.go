package lock

import "sync"

type MsgKind int

const (
	MsgRequest MsgKind = iota
	MsgReply
)

// KV protocol message kinds. Explicit values avoid collision with RA iota.
const (
	MsgKVGet   MsgKind = 10
	MsgKVPut   MsgKind = 11
	MsgKVDone  MsgKind = 12
	MsgKVReply MsgKind = 13
)

type Message struct {
	Kind      MsgKind
	From      string
	Timestamp int
	Key       string // KV: key for Get/Put operations
	Value     int    // KV: value for Put/Reply operations
}

// Network is a central in-process message router.
type Network struct {
	mu    sync.Mutex
	nodes map[string]*Transport
}

func NewNetwork() *Network {
	return &Network{nodes: make(map[string]*Transport)}
}

func (n *Network) AddNode(addr string) *Transport {
	t := &Transport{
		Addr:    addr,
		network: n,
		mailbox: make(chan Message, 64),
	}
	n.mu.Lock()
	n.nodes[addr] = t
	n.mu.Unlock()
	return t
}

func (n *Network) Send(to string, msg Message) {
	n.mu.Lock()
	t, ok := n.nodes[to]
	n.mu.Unlock()
	if ok {
		t.mailbox <- msg
	}
}

// Transport is a per-node handle to the network.
type Transport struct {
	Addr    string
	network *Network
	mailbox chan Message
}

func (t *Transport) Send(to string, msg Message) {
	t.network.Send(to, msg)
}

func (t *Transport) Mailbox() <-chan Message {
	return t.mailbox
}

func (t *Transport) Recv() Message {
	return <-t.mailbox
}
