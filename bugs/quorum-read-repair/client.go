package quorumreadrepair

import (
	"fmt"
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

func (c *Client) Put(key, value string) string {
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
		msg, ok := c.inbox.waitFor(MsgPutAck, requestID)
		if !ok {
			return requestID
		}
		if msg.Kind == MsgPutAck && msg.RequestID == requestID {
			acks++
		}
	}
	c.inbox.drainAvailable()
	return requestID
}

func (c *Client) GetAndRepair(key, requestID string) []VersionedValue {
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
		msg, ok := c.inbox.waitFor(MsgGetResp, requestID)
		if !ok {
			return merged
		}
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

	c.inbox.drainAvailable()
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

func (i *clientInbox) waitFor(kind MsgKind, requestID string) (Message, bool) {
	for {
		for idx, msg := range i.pending {
			if msg.Kind == kind && msg.RequestID == requestID {
				i.pending = append(i.pending[:idx], i.pending[idx+1:]...)
				return msg, true
			}
		}

		msg, ok := <-i.transport.Mailbox()
		if !ok {
			return Message{}, false
		}
		i.pending = append(i.pending, msg)
	}
}

func (i *clientInbox) drainAvailable() {
	for {
		select {
		case msg, ok := <-i.transport.Mailbox():
			if !ok {
				return
			}
			i.pending = append(i.pending, msg)
		default:
			return
		}
	}
}
