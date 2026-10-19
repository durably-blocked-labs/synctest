package worker

import (
	"github.com/shubhaankar/synctest/evaluations/framework"
	rafter "github.com/shubhaankar/synctest/evaluations/raft"
)

func Registry() (*framework.Registry, error) {
	registry := framework.NewRegistry()
	if err := rafter.RegisterScenarios(registry); err != nil {
		return nil, err
	}
	return registry, nil
}
