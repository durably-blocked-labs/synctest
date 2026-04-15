package raleaseexpiry

import (
	"runtime"
	"testing"
	"time"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestLeaseExpiry_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(8)

	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)
	transports["A"].DelayNext(MsgKVGetReply, 50*time.Millisecond)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addLeaseExpiryNodes(orch, addrs, transports, activeContenders)

	_, ok := orch.Run(t)
	if ok {
		t.Fatal("expected lease-expiry schedule to expose the lost-update bug")
	}
}
