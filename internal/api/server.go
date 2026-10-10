package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"
	"github.com/Enoch-Muhimbisa/relayforge/internal/engine"
	"github.com/Enoch-Muhimbisa/relayforge/internal/store"
)
type Server struct{repo store.Repository;engine *engine.Engine}
func New(repo store.Repository,e *engine.Engine)*Server{return &Server{repo:repo,engine:e}}
func(s *Server)Handler()http.Handler{m:=http.NewServeMux();m.HandleFunc("GET /api/v1/health",s.health);m.HandleFunc("GET /api/v1/workflows",s.workflows);m.HandleFunc("GET /api/v1/workflows/{id}",s.workflow);m.HandleFunc("POST /api/v1/workflows/{id}/runs",s.start);m.HandleFunc("GET /api/v1/runs",s.runs);m.HandleFunc("GET /api/v1/runs/{id}",s.run);return headers(logRequests(m))}
func(s *Server)health(w http.ResponseWriter,r *http.Request){jsonOut(w,200,map[string]any{"status":"ok","service":"relayforge-api","time":time.Now().UTC()})}
func(s *Server)workflows(w http.ResponseWriter,r *http.Request){jsonOut(w,200,map[string]any{"items":s.repo.ListWorkflows()})}
func(s *Server)workflow(w http.ResponseWriter,r *http.Request){v,e:=s.repo.GetWorkflow(r.PathValue("id"));if e!=nil{errorOut(w,404,"workflow_not_found","Workflow does not exist");return};jsonOut(w,200,v)}
func(s *Server)start(w http.ResponseWriter,r *http.Request){v,e:=s.engine.Start(r.PathValue("id"));if e!=nil{if errors.Is(e,store.ErrNotFound){errorOut(w,404,"workflow_not_found","Workflow does not exist");return};errorOut(w,422,"workflow_invalid",e.Error());return};jsonOut(w,202,v)}
func(s *Server)runs(w http.ResponseWriter,r *http.Request){limit:=50;if raw:=r.URL.Query().Get("limit");raw!=""{n,e:=strconv.Atoi(raw);if e!=nil||n<1||n>200{errorOut(w,400,"invalid_limit","limit must be between 1 and 200");return};limit=n};jsonOut(w,200,map[string]any{"items":s.repo.ListRuns(limit)})}
func(s *Server)run(w http.ResponseWriter,r *http.Request){v,e:=s.repo.GetRun(r.PathValue("id"));if e!=nil{errorOut(w,404,"run_not_found","Run does not exist");return};jsonOut(w,200,v)}
func jsonOut(w http.ResponseWriter,status int,v any){w.Header().Set("Content-Type","application/json; charset=utf-8");w.WriteHeader(status);_ = json.NewEncoder(w).Encode(v)}
func errorOut(w http.ResponseWriter,status int,code,msg string){jsonOut(w,status,map[string]any{"error":map[string]string{"code":code,"message":msg}})}
func headers(next http.Handler)http.Handler{return http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){w.Header().Set("X-Content-Type-Options","nosniff");w.Header().Set("X-Frame-Options","DENY");w.Header().Set("Referrer-Policy","strict-origin-when-cross-origin");w.Header().Set("Permissions-Policy","camera=(), microphone=(), geolocation=()");origin:=r.Header.Get("Origin");if origin=="http://localhost:5173"||origin=="http://127.0.0.1:5173"{w.Header().Set("Access-Control-Allow-Origin",origin);w.Header().Add("Vary","Origin");w.Header().Set("Access-Control-Allow-Methods","GET, POST, OPTIONS");w.Header().Set("Access-Control-Allow-Headers","Content-Type, X-Request-ID")};if r.Method=="OPTIONS"{w.WriteHeader(http.StatusNoContent);return};next.ServeHTTP(w,r)})}
func logRequests(next http.Handler)http.Handler{return http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){start:=time.Now();next.ServeHTTP(w,r);log.Printf("method=%s path=%s elapsed=%s",r.Method,r.URL.Path,time.Since(start).Round(time.Millisecond))})}
