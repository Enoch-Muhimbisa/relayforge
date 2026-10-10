package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Enoch-Muhimbisa/relayforge/internal/api"
	"github.com/Enoch-Muhimbisa/relayforge/internal/engine"
	"github.com/Enoch-Muhimbisa/relayforge/internal/store"
)

func main() {
	repo := store.NewMemory()
	executor := engine.New(repo, engine.Config{MaxConcurrent: 4, RetryLimit: 2, RetryDelay: 250 * time.Millisecond})
	defer executor.Close()
	addr := os.Getenv("RELAYFORGE_ADDR")
	if addr == "" { addr = ":8080" }
	server := &http.Server{Addr: addr, Handler: api.New(repo, executor).Handler(), ReadHeaderTimeout: 5*time.Second, ReadTimeout: 10*time.Second, WriteTimeout: 15*time.Second, IdleTimeout: 60*time.Second}
	log.Printf("RelayForge API listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed { log.Fatal(err) }
}
