package raquorumeclipse

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestSimpleQuorumEclipse(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	transports := setupClusterWithKV(addrs)
	orch := orchestrator.New()
	addKVNode(orch, transports)
	addQuorumEclipseNodes(orch, addrs, transports)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("orchestrator run failed")
	}
}
