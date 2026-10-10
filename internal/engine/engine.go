package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/Enoch-Muhimbisa/relayforge/internal/store"
	"github.com/Enoch-Muhimbisa/relayforge/internal/workflow"
)

type Config struct {
	MaxConcurrent int
	RetryLimit    int
	RetryDelay    time.Duration
}

const defaultLeaseDuration = 2 * time.Minute

type Engine struct {
	repo      store.Repository
	cfg       Config
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	leaseFor  time.Duration
	workerIDs []string
}

// New starts a fixed worker pool. Every worker claims work from the repository;
// PostgreSQL's row lock and lease owner fence competing processes.
func New(repo store.Repository, cfg Config) *Engine {
	if cfg.MaxConcurrent < 1 {
		cfg.MaxConcurrent = 1
	}
	if cfg.RetryLimit < 0 {
		cfg.RetryLimit = 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	seed := make([]byte, 8)
	if _, err := rand.Read(seed); err != nil {
		copy(seed, []byte(fmt.Sprintf("%016x", time.Now().UnixNano())))
	}
	prefix := hex.EncodeToString(seed)
	e := &Engine{
		repo: repo, cfg: cfg, ctx: ctx, cancel: cancel,
		leaseFor: defaultLeaseDuration, workerIDs: make([]string, cfg.MaxConcurrent),
	}
	for i := range e.workerIDs {
		e.workerIDs[i] = fmt.Sprintf("rf-%s-%d", prefix, i)
		e.wg.Add(1)
		go e.workerLoop(e.workerIDs[i])
	}
	return e
}

func (e *Engine) Close() {
	e.cancel()
	e.wg.Wait()
}

func (e *Engine) Start(id string) (workflow.Run, error) {
	def, err := e.repo.GetWorkflow(id)
	if err != nil {
		return workflow.Run{}, err
	}
	if _, err = workflow.Validate(def); err != nil {
		return workflow.Run{}, err
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return workflow.Run{}, fmt.Errorf("generate run id: %w", err)
	}
	run := workflow.Run{
		ID: hex.EncodeToString(idBytes), WorkflowID: def.ID, WorkflowName: def.Name,
		Status: workflow.StatusQueued, CreatedAt: time.Now().UTC(),
		StepResults: []workflow.StepResult{},
	}
	if err = e.repo.CreateRun(run); err != nil {
		return workflow.Run{}, err
	}
	return run, nil
}

func (e *Engine) workerLoop(workerID string) {
	defer e.wg.Done()
	for {
		if e.ctx.Err() != nil {
			return
		}
		run, err := e.repo.ClaimRun(e.ctx, workerID, e.leaseFor)
		if err != nil {
			if e.ctx.Err() != nil {
				return
			}
			if err == store.ErrNoPendingRun {
				if !wait(e.ctx, 100*time.Millisecond) {
					return
				}
				continue
			}
			if !wait(e.ctx, 500*time.Millisecond) {
				return
			}
			continue
		}
		e.execute(run, workerID)
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
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

	results := map[string]workflow.StepResult{}
	for _, step := range order {
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
			if results[dep].Status != workflow.StatusSucceeded {
				blocked = true
				break
			}
		}
		if blocked {
			results[step.ID] = workflow.StepResult{
				StepID: step.ID, Status: workflow.StatusCancelled,
				Error: "dependency did not succeed",
			}
			continue
		}

		result := e.runStep(step)
		results[step.ID] = result
		run.StepResults = ordered(order, results)
		if result.Status == workflow.StatusCancelled {
			run.Status = workflow.StatusCancelled
			run.Error = result.Error
			run.FinishedAt = time.Now().UTC()
			_ = e.repo.UpdateClaimedRun(run, workerID, e.leaseFor)
			return
		}
		if err := e.repo.UpdateClaimedRun(run, workerID, e.leaseFor); err != nil {
			return // Lease was lost; a newer owner is responsible for the run.
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

func (e *Engine) runStep(step workflow.Step) workflow.StepResult {
	r := workflow.StepResult{StepID: step.ID, Status: workflow.StatusRunning, StartedAt: time.Now().UTC()}
	for attempt := 1; attempt <= e.cfg.RetryLimit+1; attempt++ {
		r.Attempt = attempt
		ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
		err := executeStep(ctx, step)
		cancel()
		if err == nil {
			r.Status = workflow.StatusSucceeded
			r.Output = step.Config["message"]
			if r.Output == "" {
				r.Output = "Step completed"
			}
			r.FinishedAt = time.Now().UTC()
			return r
		}
		r.Error = err.Error()
		if e.ctx.Err() != nil {
			r.Status = workflow.StatusCancelled
			r.FinishedAt = time.Now().UTC()
			return r
		}
		if attempt <= e.cfg.RetryLimit {
			if !wait(e.ctx, e.cfg.RetryDelay) {
				r.Status = workflow.StatusCancelled
				r.FinishedAt = time.Now().UTC()
				return r
			}
		}
	}
	r.Status = workflow.StatusFailed
	r.FinishedAt = time.Now().UTC()
	return r
}

func executeStep(ctx context.Context, s workflow.Step) error {
	switch s.Kind {
	case "task":
		if s.Config["fail"] == "true" {
			return fmt.Errorf("configured task failure")
		}
		select {
		case <-time.After(80 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case "delay":
		d, err := time.ParseDuration(s.Config["duration"])
		if err != nil {
			d = 100 * time.Millisecond
		}
		if d < 0 {
			return fmt.Errorf("delay must not be negative")
		}
		select {
		case <-time.After(d):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	default:
		return fmt.Errorf("unsupported step kind %q", s.Kind)
	}
}

func ordered(order []workflow.Step, results map[string]workflow.StepResult) []workflow.StepResult {
	out := make([]workflow.StepResult, 0, len(results))
	for _, s := range order {
		if r, ok := results[s.ID]; ok {
			out = append(out, r)
		}
	}
	return out
}
