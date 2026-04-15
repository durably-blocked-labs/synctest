package raquorumcascade

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestSimpleQuorumCascade(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)
	addKVNode(orch, transports)
	addQuorumCascadeNodes(orch, addrs, transports)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("orchestrator run failed")
	}
}
