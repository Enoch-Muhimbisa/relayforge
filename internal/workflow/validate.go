package workflow

import (
	"fmt"
	"sort"
	"strings"
)

// Validate rejects malformed dependency graphs and returns deterministic topological order.
func Validate(def Definition) ([]Step, error) {
	if strings.TrimSpace(def.ID) == "" { return nil, fmt.Errorf("workflow id is required") }
	if strings.TrimSpace(def.Name) == "" { return nil, fmt.Errorf("workflow name is required") }
	if len(def.Steps) == 0 { return nil, fmt.Errorf("workflow must contain at least one step") }
	byID := make(map[string]Step, len(def.Steps))
	for _, step := range def.Steps {
		if strings.TrimSpace(step.ID) == "" { return nil, fmt.Errorf("step id is required") }
		if strings.TrimSpace(step.Name) == "" { return nil, fmt.Errorf("step %q name is required", step.ID) }
		if _, exists := byID[step.ID]; exists { return nil, fmt.Errorf("duplicate step id %q", step.ID) }
		if step.Kind != "task" && step.Kind != "delay" { return nil, fmt.Errorf("step %q has unsupported kind %q", step.ID, step.Kind) }
		byID[step.ID] = step
	}
	indegree := make(map[string]int, len(byID))
	children := make(map[string][]string, len(byID))
	for _, step := range def.Steps {
		seen := map[string]bool{}
		for _, dep := range step.DependsOn {
			if _, exists := byID[dep]; !exists { return nil, fmt.Errorf("step %q depends on unknown step %q", step.ID, dep) }
			if dep == step.ID { return nil, fmt.Errorf("step %q cannot depend on itself", step.ID) }
			if seen[dep] { return nil, fmt.Errorf("step %q repeats dependency %q", step.ID, dep) }
			seen[dep] = true
			indegree[step.ID]++
			children[dep] = append(children[dep], step.ID)
		}
	}
	ready := make([]string, 0)
	for id, degree := range indegree { if degree == 0 { ready = append(ready, id) } }
	sort.Strings(ready)
	order := make([]Step, 0, len(def.Steps))
	for len(ready) > 0 {
		id := ready[0]; ready = ready[1:]
		order = append(order, byID[id])
		for _, child := range children[id] {
			indegree[child]--
			if indegree[child] == 0 { ready = append(ready, child); sort.Strings(ready) }
		}
	}
	if len(order) != len(def.Steps) { return nil, fmt.Errorf("workflow contains a dependency cycle") }
	return order, nil
}
