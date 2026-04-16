package k8s6632

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

func TestKubernetes6632(t *testing.T) {
	explorer.Test(t, Workload)
}
