package grpc1353

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

func TestGrpc1353(t *testing.T) {
	explorer.Test(t, Workload)
}
