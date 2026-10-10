package engine

import (
	"testing"
	"context"
	"sync"
	"fmt"
	"time"
	"github.com/Enoch-Muhimbisa/relayforge/internal/store"
	"github.com/Enoch-Muhimbisa/relayforge/internal/workflow"
)
func TestRunReachesTerminalState(t *testing.T){repo:=store.NewMemory();e:=New(repo,Config{MaxConcurrent:2,RetryLimit:1});defer e.Close();run,err:=e.Start("release-check");if err!=nil{t.Fatal(err)};deadline:=time.Now().Add(3*time.Second);for time.Now().Before(deadline){got,err:=repo.GetRun(run.ID);if err!=nil{t.Fatal(err)};if got.Status==workflow.StatusSucceeded{if len(got.StepResults)!=4{t.Fatalf("got %d step results",len(got.StepResults))};return};if got.Status==workflow.StatusFailed{t.Fatalf("run failed: %s",got.Error)};time.Sleep(10*time.Millisecond)};t.Fatal("run did not finish")}
func TestStartRejectsUnknownWorkflow(t *testing.T){e:=New(store.NewMemory(),Config{});defer e.Close();if _,err:=e.Start("missing");err==nil{t.Fatal("expected error")}}


func TestWorkerPoolPicksUpQueuedRunCreatedBeforeStartup(t *testing.T) {
	repo := store.NewMemory()
	run := workflow.Run{
		ID: "queued-before-engine-start", WorkflowID: "release-check",
		WorkflowName: "Release readiness", Status: workflow.StatusQueued,
		CreatedAt: time.Now().UTC(), StepResults: []workflow.StepResult{},
	}
	if err := repo.CreateRun(run); err != nil { t.Fatal(err) }
	e := New(repo, Config{MaxConcurrent: 2, RetryLimit: 1})
	defer e.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := repo.GetRun(run.ID)
		if err != nil { t.Fatal(err) }
		if got.Status == workflow.StatusSucceeded {
			if len(got.StepResults) != 4 { t.Fatalf("got %d step results", len(got.StepResults)) }
			return
		}
		if got.Status == workflow.StatusFailed { t.Fatalf("recovered run failed: %s", got.Error) }
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker pool did not pick up pre-existing queued run")
}


type recordingExecutor struct {
	mu sync.Mutex
	calls map[string]int
	keys map[string][]string
	failFirst map[string]bool
}

func (x *recordingExecutor) Execute(ctx context.Context, step workflow.Step, key string) (string, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.calls == nil { x.calls = map[string]int{} }
	if x.keys == nil { x.keys = map[string][]string{} }
	x.calls[step.ID]++
	x.keys[step.ID] = append(x.keys[step.ID], key)
	if x.failFirst != nil && x.failFirst[step.ID] && x.calls[step.ID] == 1 {
		return "", fmt.Errorf("injected transient failure")
	}
	return "ok:" + step.ID, nil
}

func TestRecoverySkipsCheckpointedStepsAndReusesIdempotencyKey(t *testing.T) {
	repo := store.NewMemory()
	runID := "recovery-checkpoint-test"
	run := workflow.Run{
		ID: runID, WorkflowID: "release-check", WorkflowName: "Release readiness",
		Status: workflow.StatusQueued, CreatedAt: time.Now().UTC(),
		StepResults: []workflow.StepResult{
			{StepID: "lint", Status: workflow.StatusSucceeded, Attempt: 1, IdempotencyKey: stepIdempotencyKey(runID, "lint"), Output: "already done"},
			{StepID: "unit", Status: workflow.StatusRunning, Attempt: 1, IdempotencyKey: stepIdempotencyKey(runID, "unit")},
		},
	}
	if err := repo.CreateRun(run); err != nil { t.Fatal(err) }
	executor := &recordingExecutor{}
	e := New(repo, Config{MaxConcurrent: 1, RetryLimit: 1, Executor: executor})
	defer e.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := repo.GetRun(runID)
		if err != nil { t.Fatal(err) }
		if got.Status == workflow.StatusSucceeded {
			executor.mu.Lock()
			defer executor.mu.Unlock()
			if executor.calls["lint"] != 0 { t.Fatalf("checkpointed lint step executed %d times", executor.calls["lint"]) }
			if executor.calls["unit"] != 1 { t.Fatalf("recovered unit step executed %d times", executor.calls["unit"]) }
			if got.StepResults[1].IdempotencyKey != stepIdempotencyKey(runID, "unit") { t.Fatalf("unit idempotency key changed: %q", got.StepResults[1].IdempotencyKey) }
			for _, result := range got.StepResults {
				if result.Status != workflow.StatusSucceeded { t.Fatalf("step %q ended %q", result.StepID, result.Status) }
				if result.IdempotencyKey == "" { t.Fatalf("step %q has no idempotency key", result.StepID) }
			}
			return
		}
		if got.Status == workflow.StatusFailed { t.Fatalf("recovered run failed: %s", got.Error) }
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("recovered run did not finish")
}

func TestRetriesReuseTheSameIdempotencyKey(t *testing.T) {
	repo := store.NewMemory()
	executor := &recordingExecutor{failFirst: map[string]bool{"lint": true}}
	e := New(repo, Config{MaxConcurrent: 1, RetryLimit: 1, RetryDelay: time.Millisecond, Executor: executor})
	defer e.Close()
	run, err := e.Start("release-check")
	if err != nil { t.Fatal(err) }
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := repo.GetRun(run.ID)
		if err != nil { t.Fatal(err) }
		if got.Status == workflow.StatusSucceeded {
			executor.mu.Lock()
			defer executor.mu.Unlock()
			if executor.calls["lint"] != 2 { t.Fatalf("lint attempts = %d, want 2", executor.calls["lint"]) }
			keys := executor.keys["lint"]
			if len(keys) != 2 || keys[0] != keys[1] { t.Fatalf("retry idempotency keys differ: %v", keys) }
			if got.StepResults[0].Attempt != 2 { t.Fatalf("persisted attempts = %d, want 2", got.StepResults[0].Attempt) }
			return
		}
		if got.Status == workflow.StatusFailed { t.Fatalf("run failed: %s", got.Error) }
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("run did not finish")
}
