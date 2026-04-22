package k8s26980

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

func TestKubernetes26980(t *testing.T) {
	explorer.Test(t, Workload)
}
