package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Enoch-Muhimbisa/relayforge/internal/workflow"
)

func TestMemoryLeaseFencesStaleWorkerAndReclaimsExpiredRun(t *testing.T) {
	repo := NewMemory()
	run := workflow.Run{ID: "lease-test", WorkflowID: "release-check", Status: workflow.StatusQueued, CreatedAt: time.Now().UTC()}
	if err := repo.CreateRun(run); err != nil { t.Fatal(err) }

	first, err := repo.ClaimRun(context.Background(), "worker-a", 20*time.Millisecond)
	if err != nil { t.Fatal(err) }
	if first.Status != workflow.StatusRunning { t.Fatalf("claim status = %q", first.Status) }
	time.Sleep(35 * time.Millisecond)

	second, err := repo.ClaimRun(context.Background(), "worker-b", time.Second)
	if err != nil { t.Fatal(err) }
	if second.ID != run.ID { t.Fatalf("claimed %q, want %q", second.ID, run.ID) }
	if err := repo.UpdateClaimedRun(first, "worker-a", time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale worker update error = %v, want ErrLeaseLost", err)
	}
	second.Status = workflow.StatusSucceeded
	second.FinishedAt = time.Now().UTC()
	if err := repo.UpdateClaimedRun(second, "worker-b", time.Second); err != nil { t.Fatal(err) }
	if _, err := repo.ClaimRun(context.Background(), "worker-c", time.Second); !errors.Is(err, ErrNoPendingRun) {
		t.Fatalf("claim after terminal state = %v, want ErrNoPendingRun", err)
	}
}
