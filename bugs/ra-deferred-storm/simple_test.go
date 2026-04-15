package radeferredstorm

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestSimpleDeferredStorm(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	transports := setupClusterWithKV(addrs)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addDeferredStormNodesUnchecked(orch, addrs, transports)

	_, ok := orch.Run(t)
	if ok {
		t.Log("default run did not expose the deferred-storm bug")
		return
	}
	t.Log("default run exposed the deferred-storm bug")
}
