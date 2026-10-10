package engine

import (
	"context"
	"fmt"
	"sync"
	"time"
	"github.com/google/uuid"
	"github.com/redeemedwinner92-dotcom/relayforge/internal/store"
	"github.com/redeemedwinner92-dotcom/relayforge/internal/workflow"
)
type Config struct{MaxConcurrent int;RetryLimit int;RetryDelay time.Duration}
type Engine struct{repo store.Repository;cfg Config;slots chan struct{};ctx context.Context;cancel context.CancelFunc;wg sync.WaitGroup}
func New(repo store.Repository,cfg Config)*Engine{if cfg.MaxConcurrent<1{cfg.MaxConcurrent=1};if cfg.RetryLimit<0{cfg.RetryLimit=0};ctx,cancel:=context.WithCancel(context.Background());return &Engine{repo:repo,cfg:cfg,slots:make(chan struct{},cfg.MaxConcurrent),ctx:ctx,cancel:cancel}}
func(e *Engine)Close(){e.cancel();e.wg.Wait()}
func(e *Engine)Start(id string)(workflow.Run,error){def,err:=e.repo.GetWorkflow(id);if err!=nil{return workflow.Run{},err};if _,err=workflow.Validate(def);err!=nil{return workflow.Run{},err};run:=workflow.Run{ID:uuid.NewString(),WorkflowID:def.ID,WorkflowName:def.Name,Status:workflow.StatusQueued,CreatedAt:time.Now().UTC(),StepResults:[]workflow.StepResult{}};if err=e.repo.CreateRun(run);err!=nil{return workflow.Run{},err};e.wg.Add(1);go func(){defer e.wg.Done();e.execute(def,run.ID)}();return run,nil}
func(e *Engine)execute(def workflow.Definition,id string){
	select{case e.slots<-struct{}{}:case <-e.ctx.Done():return};defer func(){<-e.slots}()
	run,err:=e.repo.GetRun(id);if err!=nil{return};run.Status=workflow.StatusRunning;run.StartedAt=time.Now().UTC();_ = e.repo.UpdateRun(run)
	order,err:=workflow.Validate(def);if err!=nil{run.Status=workflow.StatusFailed;run.Error=err.Error();run.FinishedAt=time.Now().UTC();_ = e.repo.UpdateRun(run);return}
	results:=map[string]workflow.StepResult{}
	for _,step:=range order{
		select{case <-e.ctx.Done():run.Status=workflow.StatusCancelled;run.Error="engine shutting down";run.FinishedAt=time.Now().UTC();run.StepResults=ordered(order,results);_ = e.repo.UpdateRun(run);return;default:}
		blocked:=false;for _,dep:=range step.DependsOn{if results[dep].Status!=workflow.StatusSucceeded{blocked=true;break}}
		if blocked{results[step.ID]=workflow.StepResult{StepID:step.ID,Status:workflow.StatusCancelled,Error:"dependency did not succeed"};continue}
		result:=e.runStep(step);results[step.ID]=result;run.StepResults=ordered(order,results);_ = e.repo.UpdateRun(run)
		if result.Status==workflow.StatusFailed{run.Status=workflow.StatusFailed;run.Error=fmt.Sprintf("step %q failed: %s",step.ID,result.Error);run.FinishedAt=time.Now().UTC();_ = e.repo.UpdateRun(run);return}
	}
	run.StepResults=ordered(order,results);run.Status=workflow.StatusSucceeded;run.FinishedAt=time.Now().UTC();_ = e.repo.UpdateRun(run)
}
func(e *Engine)runStep(step workflow.Step)workflow.StepResult{
	r:=workflow.StepResult{StepID:step.ID,Status:workflow.StatusRunning,StartedAt:time.Now().UTC()}
	for attempt:=1;attempt<=e.cfg.RetryLimit+1;attempt++{r.Attempt=attempt;ctx,cancel:=context.WithTimeout(e.ctx,30*time.Second);err:=executeStep(ctx,step);cancel();if err==nil{r.Status=workflow.StatusSucceeded;r.Output=step.Config["message"];if r.Output==""{r.Output="Step completed"};r.FinishedAt=time.Now().UTC();return r};r.Error=err.Error();if attempt<=e.cfg.RetryLimit{select{case <-time.After(e.cfg.RetryDelay):case <-e.ctx.Done():r.Status=workflow.StatusCancelled;r.FinishedAt=time.Now().UTC();return r}}}
	r.Status=workflow.StatusFailed;r.FinishedAt=time.Now().UTC();return r
}
func executeStep(ctx context.Context,s workflow.Step)error{switch s.Kind{case "task":if s.Config["fail"]=="true"{return fmt.Errorf("configured task failure")};select{case <-time.After(80*time.Millisecond):return nil;case <-ctx.Done():return ctx.Err()};case "delay":d,err:=time.ParseDuration(s.Config["duration"]);if err!=nil{d=100*time.Millisecond};if d<0{return fmt.Errorf("delay must not be negative")};select{case <-time.After(d):return nil;case <-ctx.Done():return ctx.Err()};default:return fmt.Errorf("unsupported step kind %q",s.Kind)}}
func ordered(order []workflow.Step,results map[string]workflow.StepResult)[]workflow.StepResult{out:=make([]workflow.StepResult,0,len(results));for _,s:=range order{if r,ok:=results[s.ID];ok{out=append(out,r)}};return out}
