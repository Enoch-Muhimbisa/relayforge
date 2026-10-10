package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/Enoch-Muhimbisa/relayforge/internal/store"
	"github.com/Enoch-Muhimbisa/relayforge/internal/workflow"
)

// StepExecutor receives a stable idempotency key for every logical step. External
// integrations should pass this key to the downstream service when it supports deduplication.
type StepExecutor interface {
	Execute(context.Context, workflow.Step, string) (string, error)
}

type Config struct {
	MaxConcurrent int
	RetryLimit    int
	RetryDelay    time.Duration
	StepTimeout   time.Duration
	Executor      StepExecutor
}

const defaultLeaseDuration = 2 * time.Minute

type Engine struct {
	repo     store.Repository
	cfg      Config
	executor StepExecutor
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	leaseFor time.Duration
}

func New(repo store.Repository, cfg Config) *Engine {
	if cfg.MaxConcurrent < 1 { cfg.MaxConcurrent = 1 }
	if cfg.RetryLimit < 0 { cfg.RetryLimit = 0 }
	if cfg.StepTimeout <= 0 { cfg.StepTimeout = 30 * time.Second }
	if cfg.Executor == nil { cfg.Executor = demoExecutor{} }
	ctx, cancel := context.WithCancel(context.Background())
	seed := make([]byte, 8)
	if _, err := rand.Read(seed); err != nil { copy(seed, []byte(fmt.Sprintf("%016x", time.Now().UnixNano()))) }
	prefix := hex.EncodeToString(seed)
	e := &Engine{repo: repo, cfg: cfg, executor: cfg.Executor, ctx: ctx, cancel: cancel, leaseFor: defaultLeaseDuration}
	for i := 0; i < cfg.MaxConcurrent; i++ {
		workerID := fmt.Sprintf("rf-%s-%d", prefix, i)
		e.wg.Add(1)
		go e.workerLoop(workerID)
	}
	return e
}

func (e *Engine) Close() { e.cancel(); e.wg.Wait() }

func (e *Engine) Start(id string) (workflow.Run, error) {
	def, err := e.repo.GetWorkflow(id)
	if err != nil { return workflow.Run{}, err }
	if _, err = workflow.Validate(def); err != nil { return workflow.Run{}, err }
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil { return workflow.Run{}, fmt.Errorf("generate run id: %w", err) }
	run := workflow.Run{ID: hex.EncodeToString(idBytes), WorkflowID: def.ID, WorkflowName: def.Name, Status: workflow.StatusQueued, CreatedAt: time.Now().UTC(), StepResults: []workflow.StepResult{}}
	if err = e.repo.CreateRun(run); err != nil { return workflow.Run{}, err }
	return run, nil
}

func (e *Engine) workerLoop(workerID string) {
	defer e.wg.Done()
	for {
		if e.ctx.Err() != nil { return }
		run, err := e.repo.ClaimRun(e.ctx, workerID, e.leaseFor)
		if err != nil {
			if e.ctx.Err() != nil { return }
			if err == store.ErrNoPendingRun {
				if !wait(e.ctx, 100*time.Millisecond) { return }
			} else if !wait(e.ctx, 500*time.Millisecond) { return }
			continue
		}
		e.execute(run, workerID)
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select { case <-timer.C: return true; case <-ctx.Done(): return false }
}

func (e *Engine) execute(run workflow.Run, workerID string) {
	def, err := e.repo.GetWorkflow(run.WorkflowID)
	if err != nil {
		run.Status = workflow.StatusFailed
		run.Error = fmt.Sprintf("workflow definition unavailable: %v", err)
		run.FinishedAt = time.Now().UTC()
		_ = e.repo.UpdateClaimedRun(run, workerID, e.leaseFor)
		return
	}
	order, err := workflow.Validate(def)
	if err != nil {
		run.Status = workflow.StatusFailed
		run.Error = err.Error()
		run.FinishedAt = time.Now().UTC()
		_ = e.repo.UpdateClaimedRun(run, workerID, e.leaseFor)
		return
	}

	// Completed step results are durable checkpoints. On recovery they are reused,
	// while interrupted steps are retried with the same idempotency key.
	results := make(map[string]workflow.StepResult, len(run.StepResults))
	for _, previous := range run.StepResults { results[previous.StepID] = previous }

	for _, step := range order {
		if previous, ok := results[step.ID]; ok && previous.Status == workflow.StatusSucceeded {
			continue
		}
		if e.ctx.Err() != nil {
			run.Status = workflow.StatusCancelled
			run.Error = "engine shutting down"
			run.FinishedAt = time.Now().UTC()
			run.StepResults = ordered(order, results)
			_ = e.repo.UpdateClaimedRun(run, workerID, e.leaseFor)
			return
		}
		blocked := false
		for _, dep := range step.DependsOn {
			if results[dep].Status != workflow.StatusSucceeded { blocked = true; break }
		}
		if blocked {
			results[step.ID] = workflow.StepResult{StepID: step.ID, Status: workflow.StatusCancelled, Error: "dependency did not succeed"}
			run.StepResults = ordered(order, results)
			if err := e.repo.UpdateClaimedRun(run, workerID, e.leaseFor); err != nil { return }
			continue
		}

		key := stepIdempotencyKey(run.ID, step.ID)
		previous := results[step.ID]
		results[step.ID] = workflow.StepResult{
			StepID: step.ID, Status: workflow.StatusRunning,
			StartedAt: time.Now().UTC(), Attempt: previous.Attempt,
			IdempotencyKey: key,
		}
		run.StepResults = ordered(order, results)
		// Persist intent before executing side effects. A crash after this point reuses
		// the same key, allowing an idempotency-aware handler to deduplicate the retry.
		if err := e.repo.UpdateClaimedRun(run, workerID, e.leaseFor); err != nil { return }

		result := e.runStep(step, key, previous.Attempt)
		results[step.ID] = result
		run.StepResults = ordered(order, results)
		if err := e.repo.UpdateClaimedRun(run, workerID, e.leaseFor); err != nil { return }

		if result.Status == workflow.StatusCancelled {
			run.Status = workflow.StatusCancelled
			run.Error = result.Error
			run.FinishedAt = time.Now().UTC()
			run.StepResults = ordered(order, results)
			_ = e.repo.UpdateClaimedRun(run, workerID, e.leaseFor)
			return
		}
		if result.Status == workflow.StatusFailed {
			run.Status = workflow.StatusFailed
			run.Error = fmt.Sprintf("step %q failed: %s", step.ID, result.Error)
			run.FinishedAt = time.Now().UTC()
			_ = e.repo.UpdateClaimedRun(run, workerID, e.leaseFor)
			return
		}
	}
	run.StepResults = ordered(order, results)
	run.Status = workflow.StatusSucceeded
	run.FinishedAt = time.Now().UTC()
	_ = e.repo.UpdateClaimedRun(run, workerID, e.leaseFor)
}

func stepIdempotencyKey(runID, stepID string) string {
	sum := sha256.Sum256([]byte("relayforge-step-v1:" + runID + ":" + stepID))
	return "rf1_" + hex.EncodeToString(sum[:])
}

func (e *Engine) runStep(step workflow.Step, key string, priorAttempts int) workflow.StepResult {
	r := workflow.StepResult{StepID: step.ID, Status: workflow.StatusRunning, StartedAt: time.Now().UTC(), Attempt: priorAttempts, IdempotencyKey: key}
	for retry := 1; retry <= e.cfg.RetryLimit+1; retry++ {
		r.Attempt = priorAttempts + retry
		ctx, cancel := context.WithTimeout(e.ctx, e.cfg.StepTimeout)
		output, err := e.executor.Execute(ctx, step, key)
		cancel()
		if err == nil {
			r.Status = workflow.StatusSucceeded
			r.Output = output
			if r.Output == "" { r.Output = "Step completed" }
			r.FinishedAt = time.Now().UTC()
			return r
		}
		r.Error = err.Error()
		if e.ctx.Err() != nil {
			r.Status = workflow.StatusCancelled
			r.FinishedAt = time.Now().UTC()
			return r
		}
		if retry <= e.cfg.RetryLimit && !wait(e.ctx, e.cfg.RetryDelay) {
			r.Status = workflow.StatusCancelled
			r.FinishedAt = time.Now().UTC()
			return r
		}
	}
	r.Status = workflow.StatusFailed
	r.FinishedAt = time.Now().UTC()
	return r
}

type demoExecutor struct{}

func (demoExecutor) Execute(ctx context.Context, step workflow.Step, _ string) (string, error) {
	switch step.Kind {
	case "task":
		if step.Config["fail"] == "true" { return "", fmt.Errorf("configured task failure") }
		select {
		case <-time.After(80 * time.Millisecond):
			output := step.Config["message"]
			if output == "" { output = "Step completed" }
			return output, nil
		case <-ctx.Done(): return "", ctx.Err()
		}
	case "delay":
		d, err := time.ParseDuration(step.Config["duration"])
		if err != nil { d = 100 * time.Millisecond }
		if d < 0 { return "", fmt.Errorf("delay must not be negative") }
		select {
		case <-time.After(d): return "Step completed", nil
		case <-ctx.Done(): return "", ctx.Err()
		}
	default:
		return "", fmt.Errorf("unsupported step kind %q", step.Kind)
	}
}

func ordered(order []workflow.Step, results map[string]workflow.StepResult) []workflow.StepResult {
	out := make([]workflow.StepResult, 0, len(results))
	for _, step := range order { if result, ok := results[step.ID]; ok { out = append(out, result) } }
	return out
}
