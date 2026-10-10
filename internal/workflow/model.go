package workflow

import "time"

type Status string
const (
	StatusQueued Status = "queued"
	StatusRunning Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed Status = "failed"
	StatusCancelled Status = "cancelled"
)
type Step struct {
	ID string `json:"id"`
	Name string `json:"name"`
	Kind string `json:"kind"`
	DependsOn []string `json:"dependsOn,omitempty"`
	Config map[string]string `json:"config,omitempty"`
}
type Definition struct {
	ID string `json:"id"`
	Name string `json:"name"`
	Description string `json:"description"`
	Version int `json:"version"`
	Steps []Step `json:"steps"`
}
type StepResult struct {
	StepID string `json:"stepId"`
	Status Status `json:"status"`
	StartedAt time.Time `json:"startedAt,omitempty"`
	FinishedAt time.Time `json:"finishedAt,omitempty"`
	Attempt int `json:"attempt"`
	Output string `json:"output,omitempty"`
	Error string `json:"error,omitempty"`
}
type Run struct {
	ID string `json:"id"`
	WorkflowID string `json:"workflowId"`
	WorkflowName string `json:"workflowName"`
	Status Status `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
	StartedAt time.Time `json:"startedAt,omitempty"`
	FinishedAt time.Time `json:"finishedAt,omitempty"`
	StepResults []StepResult `json:"stepResults"`
	Error string `json:"error,omitempty"`
}
