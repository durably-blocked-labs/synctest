package moby28462

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

func TestMoby28462(t *testing.T) {
	explorer.Test(t, Workload)
}
