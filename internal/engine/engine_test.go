package engine

import (
	"testing"
	"time"
	"github.com/redeemedwinner92-dotcom/relayforge/internal/store"
	"github.com/redeemedwinner92-dotcom/relayforge/internal/workflow"
)
func TestRunReachesTerminalState(t *testing.T){repo:=store.NewMemory();e:=New(repo,Config{MaxConcurrent:2,RetryLimit:1});defer e.Close();run,err:=e.Start("release-check");if err!=nil{t.Fatal(err)};deadline:=time.Now().Add(3*time.Second);for time.Now().Before(deadline){got,err:=repo.GetRun(run.ID);if err!=nil{t.Fatal(err)};if got.Status==workflow.StatusSucceeded{if len(got.StepResults)!=4{t.Fatalf("got %d step results",len(got.StepResults))};return};if got.Status==workflow.StatusFailed{t.Fatalf("run failed: %s",got.Error)};time.Sleep(10*time.Millisecond)};t.Fatal("run did not finish")}
func TestStartRejectsUnknownWorkflow(t *testing.T){e:=New(store.NewMemory(),Config{});defer e.Close();if _,err:=e.Start("missing");err==nil{t.Fatal("expected error")}}
