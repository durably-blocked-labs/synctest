package raduplicaterequest

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestSimpleDuplicateRequest(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs
	transports := setupCluster(addrs)
	store := NewKVStore()

	orch := orchestrator.New()
	addDuplicateRequestNodes(orch, addrs, transports, store, nil, nil, false)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("run failed")
	}
}
