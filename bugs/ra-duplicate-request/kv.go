package raduplicaterequest

// NetworkKVStore is a key-value store that runs as a separate bubble in the
// orchestrator. It receives Get/Put requests as messages and sends replies,
// making all KV operations visible to the orchestrator for reordering.
type NetworkKVStore struct {
	data      map[string]int
	transport *OrchestratorTransport
}

func NewNetworkKVStore(transport *OrchestratorTransport) *NetworkKVStore {
	return &NetworkKVStore{
		data:      make(map[string]int),
		transport: transport,
	}
}

// Serve processes KV requests until the transport shuts down.
func (s *NetworkKVStore) Serve() {
	for msg := range s.transport.Mailbox() {
		switch msg.Kind {
		case MsgKVGet:
			s.transport.Send(msg.From, Message{
				Kind:  MsgKVGetReply,
				From:  s.transport.Addr(),
				Key:   msg.Key,
				Value: s.data[msg.Key],
			})
		case MsgKVPut:
			s.data[msg.Key] = msg.Value
			s.transport.Send(msg.From, Message{
				Kind: MsgKVPutReply,
				From: s.transport.Addr(),
			})
		}
	}
}

// KVClient sends KV operations as messages through the orchestrator transport.
type KVClient struct {
	transport *OrchestratorTransport
	kvAddr    string
}

func NewKVClient(transport *OrchestratorTransport, kvAddr string) *KVClient {
	return &KVClient{transport: transport, kvAddr: kvAddr}
}

func (c *KVClient) Get(key string) int {
	c.transport.Send(c.kvAddr, Message{
		Kind: MsgKVGet,
		From: c.transport.Addr(),
		Key:  key,
	})
	reply := <-c.transport.KVMailbox()
	return reply.Value
}

func (c *KVClient) Put(key string, value int) {
	c.transport.Send(c.kvAddr, Message{
		Kind:  MsgKVPut,
		From:  c.transport.Addr(),
		Key:   key,
		Value: value,
	})
	<-c.transport.KVMailbox()
}
