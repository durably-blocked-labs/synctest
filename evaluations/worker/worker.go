package worker

import (
	"github.com/shubhaankar/synctest/evaluations/bubblegun"
	rafter "github.com/shubhaankar/synctest/evaluations/raft"
)

func Registry() *bubblegun.Registry {
	registry := bubblegun.NewRegistry()
	rafter.RegisterScenarios(registry)
	return registry
}
