package framework

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/shubhaankar/synctest/orchestrator"
)

type Scenario struct {
	Name     string
	Generate func(attempt int) (json.RawMessage, error)
	Setup    func(input json.RawMessage) (func(*orchestrator.Orchestrator), error)
}

type Registry struct {
	mu        sync.RWMutex
	scenarios map[string]Scenario
}

func NewRegistry() *Registry {
	return &Registry{scenarios: make(map[string]Scenario)}
}

func (r *Registry) RegisterScenario(scenario Scenario) error {
	if scenario.Name == "" {
		return fmt.Errorf("scenario name is required")
	}
	if scenario.Generate == nil || scenario.Setup == nil {
		return fmt.Errorf("scenario %q requires generate and setup functions", scenario.Name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.scenarios[scenario.Name]; exists {
		return fmt.Errorf("scenario %q is already registered", scenario.Name)
	}
	r.scenarios[scenario.Name] = scenario
	return nil
}

func (r *Registry) Scenario(name string) (Scenario, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	scenario, ok := r.scenarios[name]
	return scenario, ok
}

func (r *Registry) ScenarioNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.scenarios))
	for name := range r.scenarios {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
