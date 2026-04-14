package rastalereply

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestSimpleStaleReply(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs
	transports := setupCluster(addrs)
	store := NewKVStore()

	orch := orchestrator.New()
	addStaleReplyNodes(orch, addrs, transports, store, nil)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("run failed")
	}
}
