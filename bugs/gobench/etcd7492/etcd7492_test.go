package etcd7492

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

// TestEtcd7492 explores the etcd#7492 deadlock.
// NOT DETECTED: requires ticker to fire while a goroutine holds simpleTokensMu.
// In synctest, the fake clock only advances when all goroutines are idle,
// so the ticker can't fire during lock-holding. Needs timer-aware interleaving.
func TestEtcd7492(t *testing.T) {
	explorer.Test(t, Workload)
}
