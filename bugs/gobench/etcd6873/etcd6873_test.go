package etcd6873

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

func TestEtcd6873(t *testing.T) {
	explorer.Test(t, Workload)
}
