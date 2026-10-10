package store

import (
	"context"
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
		StepResults: []workflow.StepResult{},
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
}
