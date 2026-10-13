package istio16224

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

func TestIstio16224(t *testing.T) {
	explorer.Test(t, Workload)
}
