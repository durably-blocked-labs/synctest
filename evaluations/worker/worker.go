package worker

import (
	"github.com/shubhaankar/synctest/evaluations/bubblegun"
	rafter "github.com/shubhaankar/synctest/evaluations/raft"
	"github.com/shubhaankar/synctest/evaluations/scenarios"
)

func Registry() *bubblegun.Registry {
	registry := bubblegun.NewRegistry()
	rafter.RegisterScenarios(registry)
	scenarios.Register(registry)
	return registry
}
