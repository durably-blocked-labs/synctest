package raprematuredefer

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestSimplePrematureDefer(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs

	transports := setupCluster(addrs)
	store := NewKVStore()

	orch := orchestrator.New()
	addPrematureDeferNodes(orch, addrs, transports, store, nil, nil)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("orchestrator run failed")
	}
}
