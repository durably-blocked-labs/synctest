package bubblegun

import (
	"fmt"

	"github.com/shubhaankar/synctest/orchestrator"
)

type Algorithm string

const (
	AlgorithmFIFO   Algorithm = "fifo"
	AlgorithmCHESS  Algorithm = "chess"
	AlgorithmDPOR   Algorithm = "dpor"
	AlgorithmPCT    Algorithm = "pct"
	AlgorithmRandom Algorithm = "random"
)

type AlgorithmConfig struct {
	Name         Algorithm `json:"name"`
	ContextBound int       `json:"context_bound,omitempty"`
	Depth        int       `json:"depth,omitempty"`
	MaxSteps     int       `json:"max_steps,omitempty"`
}

type AlgorithmOption func(*AlgorithmConfig)

func ConfigureAlgorithm(name Algorithm, options ...AlgorithmOption) AlgorithmConfig {
	config := AlgorithmConfig{Name: name}
	for _, option := range options {
		option(&config)
	}
	return config
}

func WithContextBound(bound int) AlgorithmOption {
	return func(config *AlgorithmConfig) { config.ContextBound = bound }
}

func WithDepth(depth int) AlgorithmOption {
	return func(config *AlgorithmConfig) { config.Depth = depth }
}

func WithMaxSteps(maxSteps int) AlgorithmOption {
	return func(config *AlgorithmConfig) { config.MaxSteps = maxSteps }
}

func newAlgorithm(config AlgorithmConfig, seed int64) (orchestrator.CheckpointableAlgorithm, error) {
	bound := config.ContextBound
	if bound == 0 {
		bound = 2
	}

	switch config.Name {
	case AlgorithmFIFO:
		return &fifoAlgorithm{}, nil
	case AlgorithmCHESS:
		return &orchestrator.CHESS{Bound: bound}, nil
	case AlgorithmDPOR:
		return &orchestrator.DPOR{Bound: bound}, nil
	case AlgorithmPCT:
		return &orchestrator.PCT{Depth: config.Depth, MaxSteps: config.MaxSteps, Seed: seed}, nil
	case AlgorithmRandom:
		return &orchestrator.Random{Seed: seed}, nil
	default:
		return nil, fmt.Errorf("unknown algorithm %q", config.Name)
	}
}
