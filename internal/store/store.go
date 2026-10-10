package store

import (
	"errors"
	"context"
	"time"
	"sort"
	"sync"
	"github.com/Enoch-Muhimbisa/relayforge/internal/workflow"
)
var (
	ErrNotFound = errors.New("not found")
	ErrNoPendingRun = errors.New("no pending run")
	ErrLeaseLost = errors.New("run lease lost")
)
type Repository interface {
	ListWorkflows() []workflow.Definition
	GetWorkflow(string) (workflow.Definition,error)
	CreateRun(workflow.Run) error
	GetRun(string) (workflow.Run,error)
	ListRuns(int) []workflow.Run
	UpdateRun(workflow.Run) error
	ClaimRun(context.Context, string, time.Duration) (workflow.Run, error)
	UpdateClaimedRun(workflow.Run, string, time.Duration) error
}
type runLease struct { workerID string; until time.Time; duration time.Duration }
type Memory struct { mu sync.RWMutex; workflows map[string]workflow.Definition; runs map[string]workflow.Run; leases map[string]runLease }
func NewMemory()*Memory{
	m:=&Memory{workflows:map[string]workflow.Definition{},runs:map[string]workflow.Run{},leases:map[string]runLease{}}
	for _,d:=range catalog(){m.workflows[d.ID]=d};return m
}
func(m *Memory)ListWorkflows()[]workflow.Definition{m.mu.RLock();defer m.mu.RUnlock();out:=make([]workflow.Definition,0,len(m.workflows));for _,v:=range m.workflows{out=append(out,v)};sort.Slice(out,func(i,j int)bool{return out[i].ID<out[j].ID});return out}
func(m *Memory)GetWorkflow(id string)(workflow.Definition,error){m.mu.RLock();defer m.mu.RUnlock();v,ok:=m.workflows[id];if !ok{return workflow.Definition{},ErrNotFound};return v,nil}
func(m *Memory)CreateRun(r workflow.Run)error{m.mu.Lock();defer m.mu.Unlock();if _,ok:=m.runs[r.ID];ok{return errors.New("run already exists")};m.runs[r.ID]=r;return nil}
func(m *Memory)GetRun(id string)(workflow.Run,error){m.mu.RLock();defer m.mu.RUnlock();v,ok:=m.runs[id];if !ok{return workflow.Run{},ErrNotFound};v.StepResults=append([]workflow.StepResult(nil),v.StepResults...);return v,nil}
func(m *Memory)ListRuns(limit int)[]workflow.Run{m.mu.RLock();defer m.mu.RUnlock();out:=make([]workflow.Run,0,len(m.runs));for _,v:=range m.runs{v.StepResults=append([]workflow.StepResult(nil),v.StepResults...);out=append(out,v)};sort.Slice(out,func(i,j int)bool{return out[i].CreatedAt.After(out[j].CreatedAt)});if limit>0&&len(out)>limit{out=out[:limit]};return out}
func(m *Memory)UpdateRun(r workflow.Run)error{m.mu.Lock();defer m.mu.Unlock();if _,ok:=m.runs[r.ID];!ok{return ErrNotFound};r.StepResults=append([]workflow.StepResult(nil),r.StepResults...);m.runs[r.ID]=r;return nil}
func catalog()[]workflow.Definition{return []workflow.Definition{
{ID:"release-check",Name:"Release readiness",Description:"Dependency-aware release gate with parallel checks.",Version:1,Steps:[]workflow.Step{
{ID:"lint",Name:"Static analysis",Kind:"task",Config:map[string]string{"message":"Static analysis passed"}},
{ID:"unit",Name:"Unit test suite",Kind:"task",DependsOn:[]string{"lint"},Config:map[string]string{"message":"Unit tests passed"}},
{ID:"contract",Name:"Contract checks",Kind:"task",DependsOn:[]string{"lint"},Config:map[string]string{"message":"API contracts verified"}},
{ID:"package",Name:"Package artifact",Kind:"task",DependsOn:[]string{"unit","contract"},Config:map[string]string{"message":"Artifact assembled"}},
}},
{ID:"data-refresh",Name:"Data refresh pipeline",Description:"Fan-out/fan-in pipeline for downstream datasets.",Version:1,Steps:[]workflow.Step{
{ID:"extract",Name:"Extract source",Kind:"task",Config:map[string]string{"message":"Source snapshot extracted"}},
{ID:"normalize",Name:"Normalize records",Kind:"task",DependsOn:[]string{"extract"},Config:map[string]string{"message":"Records normalized"}},
{ID:"quality",Name:"Quality checks",Kind:"task",DependsOn:[]string{"extract"},Config:map[string]string{"message":"Quality threshold met"}},
{ID:"publish",Name:"Publish dataset",Kind:"task",DependsOn:[]string{"normalize","quality"},Config:map[string]string{"message":"Dataset published"}},
}},
{ID:"heartbeat",Name:"Service heartbeat",Description:"Short sequential flow exercising state transitions.",Version:1,Steps:[]workflow.Step{
{ID:"probe",Name:"Probe endpoint",Kind:"task",Config:map[string]string{"message":"Probe completed"}},
{ID:"settle",Name:"Settle window",Kind:"delay",DependsOn:[]string{"probe"},Config:map[string]string{"duration":"300ms"}},
{ID:"record",Name:"Record observation",Kind:"task",DependsOn:[]string{"settle"},Config:map[string]string{"message":"Observation recorded"}},
}},
}}


// ClaimRun atomically leases the oldest queued run or reclaims an expired lease.
func (m *Memory) ClaimRun(ctx context.Context, workerID string, leaseDuration time.Duration) (workflow.Run, error) {
	if err := ctx.Err(); err != nil { return workflow.Run{}, err }
	if leaseDuration <= 0 { leaseDuration = 2 * time.Minute }
	m.mu.Lock()
	defer m.mu.Unlock()
	var selected workflow.Run
	found := false
	for _, candidate := range m.runs {
		lease, leased := m.leases[candidate.ID]
		eligible := candidate.Status == workflow.StatusQueued || (candidate.Status == workflow.StatusRunning && (!leased || !time.Now().Before(lease.until)))
		if !eligible { continue }
		if !found || candidate.CreatedAt.Before(selected.CreatedAt) {
			selected, found = candidate, true
		}
	}
	if !found { return workflow.Run{}, ErrNoPendingRun }
	selected.Status = workflow.StatusRunning
	if selected.StartedAt.IsZero() { selected.StartedAt = time.Now().UTC() }
	selected.StepResults = append([]workflow.StepResult(nil), selected.StepResults...)
	m.runs[selected.ID] = selected
	m.leases[selected.ID] = runLease{workerID: workerID, until: time.Now().Add(leaseDuration), duration: leaseDuration}
	return selected, nil
}

// UpdateClaimedRun fences stale workers and renews a live lease on each checkpoint.
func (m *Memory) UpdateClaimedRun(run workflow.Run, workerID string, leaseDuration time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	lease, ok := m.leases[run.ID]
	if !ok || lease.workerID != workerID || !time.Now().Before(lease.until) {
		return ErrLeaseLost
	}
	run.StepResults = append([]workflow.StepResult(nil), run.StepResults...)
	m.runs[run.ID] = run
	if run.Status == workflow.StatusRunning {
		if leaseDuration <= 0 { leaseDuration = lease.duration }
		lease.duration = leaseDuration
		lease.until = time.Now().Add(leaseDuration)
		m.leases[run.ID] = lease
	} else {
		delete(m.leases, run.ID)
	}
	return nil
}
