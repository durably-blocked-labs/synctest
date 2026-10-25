package bubblegun

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/shubhaankar/synctest/orchestrator"
)

type Scenario struct {
	Name        string
	Processors  int
	TrialLimits orchestrator.TrialLimits
	Generate    func(attempt int) (json.RawMessage, error)
	Setup       func(input json.RawMessage) (func(*orchestrator.Orchestrator), error)
}

type Registry struct {
	scenarios map[string]Scenario
}

func NewRegistry() *Registry {
	return &Registry{scenarios: make(map[string]Scenario)}
}

func (r *Registry) RegisterScenario(scenario Scenario) {
	r.scenarios[scenario.Name] = scenario
}

func (r *Registry) Scenario(name string) (Scenario, error) {
	scenario, ok := r.scenarios[name]
	if !ok {
		return Scenario{}, fmt.Errorf("scenario %q is not registered", name)
	}
	if scenario.Name == "" {
		return Scenario{}, fmt.Errorf("registered scenario has no name")
	}
	if scenario.Generate == nil || scenario.Setup == nil {
		return Scenario{}, fmt.Errorf("scenario %q requires generate and setup functions", name)
	}
	return scenario, nil
}

func (r *Registry) ScenarioNames() []string {
	names := make([]string, 0, len(r.scenarios))
	for name := range r.scenarios {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
