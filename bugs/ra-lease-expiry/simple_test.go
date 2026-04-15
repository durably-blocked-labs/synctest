package raleaseexpiry

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestSimpleLeaseExpiry(t *testing.T) {
	runtime.GOMAXPROCS(8)

	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addLeaseExpiryNodes(orch, addrs, transports, 2)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("orchestrator run failed")
	}
}
