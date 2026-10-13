package k8s10182

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

func TestKubernetes10182(t *testing.T) {
	explorer.Test(t, Workload)
}
