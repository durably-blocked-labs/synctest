package radeferredstorm

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestDeferredStorm_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addDeferredStormNodes(o, addrs, transports)
	}, orchestrator.GlobalBound(4), orchestrator.GlobalMaxRuns(1500))

	if ok {
		t.Fatal("ExploreAll did not find the deferred-storm lost-update bug")
	}
}
