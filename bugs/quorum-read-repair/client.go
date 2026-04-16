package quorumreadrepair

import (
	"fmt"
	"sync"
)

const quorum = 2

type Client struct {
	addr     string
	tr       *OrchestratorTransport
	replicas []string
	seq      int
	inbox    *clientInbox
}

func NewClient(addr string, tr *OrchestratorTransport, replicas []string) *Client {
	return &Client{
		addr:     addr,
		tr:       tr,
		replicas: append([]string(nil), replicas...),
		inbox:    &clientInbox{transport: tr},
	}
}

func (c *Client) Put(key, value string) {
	c.inbox.start()
	c.seq++
	requestID := fmt.Sprintf("%s-put-%d", c.addr, c.seq)
	version := VersionedValue{
		Value: value,
		Clock: Clock{c.addr: c.seq},
	}

	for _, replica := range c.replicas {
		c.tr.Send(replica, Message{
			Kind:      MsgPut,
			From:      c.addr,
			RequestID: requestID,
			Key:       key,
			Values:    []VersionedValue{version},
		})
	}

	acks := 0
	for acks < quorum {
		msg := c.inbox.waitFor(MsgPutAck, requestID)
		if msg.Kind == MsgPutAck && msg.RequestID == requestID {
			acks++
		}
	}
}

func (c *Client) GetAndRepair(key, requestID string) []VersionedValue {
	c.inbox.start()
	for _, replica := range c.replicas {
		c.tr.Send(replica, Message{
			Kind:      MsgGet,
			From:      c.addr,
			RequestID: requestID,
			Key:       key,
		})
	}

	responses := make(map[string][]VersionedValue, len(c.replicas))
	var merged []VersionedValue
	for len(responses) < quorum {
		msg := c.inbox.waitFor(MsgGetResp, requestID)
		if msg.Kind != MsgGetResp || msg.RequestID != requestID {
			continue
		}
		responses[msg.From] = cloneValues(msg.Values)
		merged = mergeSiblings(merged, msg.Values)
	}

	for _, replica := range c.replicas {
		if !sameValues(responses[replica], merged) {
			c.tr.Send(replica, Message{
				Kind:      MsgRepair,
				From:      c.addr,
				RequestID: requestID + "-repair-" + replica,
				Key:       key,
				Values:    cloneValues(merged),
			})
		}
	}

	return merged
}

func sameValues(a, b []VersionedValue) bool {
	a = mergeSiblings(nil, a)
	b = mergeSiblings(nil, b)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Value != b[i].Value || !clockEqual(a[i].Clock, b[i].Clock) {
			return false
		}
	}
	return true
}

type clientInbox struct {
	once      sync.Once
	mu        sync.Mutex
	cond      *sync.Cond
	transport *OrchestratorTransport
	pending   []Message
	closed    bool
}

func (i *clientInbox) start() {
	i.once.Do(func() {
		i.cond = sync.NewCond(&i.mu)
		go i.run()
	})
}

func (i *clientInbox) waitFor(kind MsgKind, requestID string) Message {
	i.start()
	i.mu.Lock()
	defer i.mu.Unlock()

	for {
		for idx, msg := range i.pending {
			if msg.Kind == kind && msg.RequestID == requestID {
				i.pending = append(i.pending[:idx], i.pending[idx+1:]...)
				return msg
			}
		}

		if i.closed {
			return Message{}
		}
		i.cond.Wait()
	}
}

func (i *clientInbox) run() {
	for msg := range i.transport.Mailbox() {
		i.mu.Lock()
		i.pending = append(i.pending, msg)
		i.cond.Broadcast()
		i.mu.Unlock()
	}
	i.mu.Lock()
	i.closed = true
	i.cond.Broadcast()
	i.mu.Unlock()
}
