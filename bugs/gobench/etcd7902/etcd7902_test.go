package etcd7902

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

func TestEtcd7902(t *testing.T) {
	explorer.Test(t, Workload)
}
