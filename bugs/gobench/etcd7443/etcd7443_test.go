package etcd7443

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

func TestEtcd7443(t *testing.T) {
	explorer.Test(t, Workload, &explorer.CHESS{Bound: 3})
}
