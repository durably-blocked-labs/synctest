package moby33781

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

func TestMoby33781(t *testing.T) {
	explorer.Test(t, Workload)
}
