package distributedreplayer

import (
	"strconv"
	"testing/synctest"
)

// NodeIO is the pair of channels a node uses to communicate with the
// orchestrator. Both channels are created outside the bubble, so all
// operations on them must use synctest.External.
type NodeIO struct {
	Outbox chan<- Request  // node sends store requests here
	Inbox  <-chan Response // node receives store responses here
}

// nodeGet sends a GET request through the orchestrator and waits for
// the response. Each direction (send, then receive) is wrapped in
// synctest.External so the bubble parks properly while blocked on the
// external channel. Between the two External calls the goroutine
// re-enters the runq and the scheduling hook fires, giving the
// orchestrator a chance to interleave with the other node.
func nodeGet(io NodeIO, key string) string {
	// Step 1: send GET request. Blocks until orchestrator reads from outbox.
	synctest.External(func() {
		io.Outbox <- Request{Method: "GET", Key: key}
	})
	// -- hook fires here (goroutine re-entered runq) --
	// Step 2: receive response. Blocks until orchestrator sends to inbox.
	var val string
	synctest.External(func() {
		resp := <-io.Inbox
		val = resp.Value
	})
	return val
}

// nodeSet sends a SET request through the orchestrator and waits for
// the acknowledgment. Same two-phase External pattern as nodeGet.
func nodeSet(io NodeIO, key, value string) {
	// Step 1: send SET request.
	synctest.External(func() {
		io.Outbox <- Request{Method: "SET", Key: key, Value: value}
	})
	// -- hook fires here --
	// Step 2: receive acknowledgment.
	synctest.External(func() {
		<-io.Inbox
	})
}

// IncrementCounter reads "counter" from the store, increments it by 1,
// and writes it back. This is a read-modify-write cycle with no atomicity
// guarantee: if two nodes execute concurrently, both may read "0" and
// write "1", losing an increment.
//
// The node communicates with the store through the orchestrator:
//   - Outbox: sends requests (GET/SET)
//   - Inbox: receives responses
//
// The orchestrator controls when to service each outbox/inbox, which
// determines the cross-node interleaving.
func IncrementCounter(io NodeIO) {
	val := nodeGet(io, "counter")
	n, _ := strconv.Atoi(val)
	nodeSet(io, "counter", strconv.Itoa(n+1))
}
