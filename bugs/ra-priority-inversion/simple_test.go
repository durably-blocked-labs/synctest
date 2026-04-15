package rapriorityinversion

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestSimplePriorityInversion(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addPriorityInversionNodes(orch, addrs, transports)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("run failed")
	}
}
