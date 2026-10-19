package grpc1460

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

func TestGrpc1460(t *testing.T) {
	explorer.Test(t, Workload)
}
