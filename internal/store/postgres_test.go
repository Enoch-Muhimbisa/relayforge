package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Enoch-Muhimbisa/relayforge/internal/workflow"
)

func TestPostgresRunSurvivesStoreReopen(t *testing.T) {
	url := os.Getenv("RELAYFORGE_TEST_DATABASE_URL")
	if url == "" { t.Skip("RELAYFORGE_TEST_DATABASE_URL is not set") }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first, err := NewPostgres(ctx, url)
	if err != nil { t.Fatal(err) }
	run := workflow.Run{
		ID: fmt.Sprintf("integration-%d", time.Now().UnixNano()),
		WorkflowID: "release-check",
		WorkflowName: "Release readiness",
		Status: workflow.StatusQueued,
		CreatedAt: time.Now().UTC(),
		StepResults: []workflow.StepResult{{StepID: "lint", Status: workflow.StatusSucceeded, Attempt: 1, IdempotencyKey: "rf1_checkpoint-persisted", Output: "checkpointed"}},
	}
	if err := first.CreateRun(run); err != nil { first.Close(); t.Fatal(err) }
	first.Close()

	second, err := NewPostgres(ctx, url)
	if err != nil { t.Fatal(err) }
	defer second.Close()
	got, err := second.GetRun(run.ID)
	if err != nil { t.Fatal(err) }
	if got.ID != run.ID || got.WorkflowID != run.WorkflowID || got.Status != workflow.StatusQueued {
		t.Fatalf("persisted run mismatch: got %+v", got)
	}
	// Keep this fixture out of the pending queue for subsequent lease tests.
	got.Status = workflow.StatusSucceeded
	got.FinishedAt = time.Now().UTC()
	if err := second.UpdateRun(got); err != nil { t.Fatal(err) }
}


func TestPostgresLeaseReclaimsExpiredRunAndFencesOldWorker(t *testing.T) {
	url := os.Getenv("RELAYFORGE_TEST_DATABASE_URL")
	if url == "" { t.Skip("RELAYFORGE_TEST_DATABASE_URL is not set") }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	repo, err := NewPostgres(ctx, url)
	if err != nil { t.Fatal(err) }
	defer repo.Close()

	run := workflow.Run{
		ID: fmt.Sprintf("lease-integration-%d", time.Now().UnixNano()),
		WorkflowID: "release-check", WorkflowName: "Release readiness",
		Status: workflow.StatusQueued, CreatedAt: time.Now().UTC(),
		StepResults: []workflow.StepResult{},
	}
	if err := repo.CreateRun(run); err != nil { t.Fatal(err) }
	first, err := repo.ClaimRun(ctx, "worker-a", 25*time.Millisecond)
	if err != nil { t.Fatal(err) }
	time.Sleep(50 * time.Millisecond)
	second, err := repo.ClaimRun(ctx, "worker-b", time.Second)
	if err != nil { t.Fatal(err) }
	if second.ID != run.ID { t.Fatalf("claimed %q, want %q", second.ID, run.ID) }
	if err := repo.UpdateClaimedRun(first, "worker-a", time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale worker update error = %v, want ErrLeaseLost", err)
	}
	second.Status = workflow.StatusSucceeded
	second.FinishedAt = time.Now().UTC()
	if err := repo.UpdateClaimedRun(second, "worker-b", time.Second); err != nil { t.Fatal(err) }
}
