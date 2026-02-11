// Package distributedreplayer demonstrates a two-node distributed counter
// with deterministic record-and-replay scheduling via synctest bubbles.
package distributedreplayer

// Request is a store operation sent from a node to the orchestrator.
type Request struct {
	Method string // "GET" or "SET"
	Key    string
	Value  string // for SET
}

// Response is the result of a store operation, sent from the orchestrator
// back to the node.
type Response struct {
	Value string
	OK    bool
}

// Store is an in-memory KV store. The orchestrator accesses its data map
// directly (no channel-based RPC needed since the orchestrator IS the
// store's executor).
type Store struct {
	data map[string]string
}

// NewStore creates a new empty Store.
func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}
