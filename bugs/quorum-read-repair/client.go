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
}

func NewClient(addr string, tr *OrchestratorTransport, replicas []string) *Client {
	return &Client{
		addr:     addr,
		tr:       tr,
		replicas: append([]string(nil), replicas...),
	}
}

func (c *Client) Put(key, value string) {
	c.seq++
	requestID := fmt.Sprintf("%s-put-%d", c.addr, c.seq)
	version := VersionedValue{
		Value: value,
		Clock: Clock{c.addr: c.seq},
	}

	inbox := newClientInbox(c.tr)
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
	for acks < len(c.replicas) {
		msg := inbox.waitFor(MsgPutAck, requestID)
		if msg.Kind == MsgPutAck && msg.RequestID == requestID {
			acks++
		}
	}
}

func (c *Client) GetAndRepair(key, requestID string) []VersionedValue {
	inbox := newClientInbox(c.tr)
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
	for len(responses) < len(c.replicas) {
		msg := inbox.waitFor(MsgGetResp, requestID)
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
	transport *OrchestratorTransport
	pending   []Message
}

var clientInboxByTransport sync.Map

func newClientInbox(tr *OrchestratorTransport) *clientInbox {
	if inbox, ok := clientInboxByTransport.Load(tr); ok {
		return inbox.(*clientInbox)
	}

	inbox := &clientInbox{transport: tr}
	actual, _ := clientInboxByTransport.LoadOrStore(tr, inbox)
	return actual.(*clientInbox)
}

func (i *clientInbox) waitFor(kind MsgKind, requestID string) Message {
	for {
		for idx, msg := range i.pending {
			if msg.Kind == kind && msg.RequestID == requestID {
				i.pending = append(i.pending[:idx], i.pending[idx+1:]...)
				return msg
			}
		}

		msg, ok := <-i.transport.Mailbox()
		if !ok {
			return Message{}
		}
		i.pending = append(i.pending, msg)
	}
}
