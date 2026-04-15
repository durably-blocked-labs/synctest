package radeferredstorm

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestSimpleStorm(t *testing.T) {
	runtime.GOMAXPROCS(8)
	transports := setupClusterWithKV(stormAddrs)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addStormNodes(orch, stormAddrs, transports)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("orchestrator run failed")
	}
}

// @stance: creative
