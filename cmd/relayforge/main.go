package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Enoch-Muhimbisa/relayforge/internal/api"
	"github.com/Enoch-Muhimbisa/relayforge/internal/engine"
	"github.com/Enoch-Muhimbisa/relayforge/internal/store"
)

func main() {
	var repo store.Repository
	closeStore := func() {}
	if databaseURL := os.Getenv("RELAYFORGE_DATABASE_URL"); databaseURL != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		persistent, err := store.NewPostgres(ctx, databaseURL)
		if err != nil { log.Fatalf("initialize PostgreSQL storage: %v", err) }
		repo = persistent
		closeStore = persistent.Close
		log.Printf("storage=postgres")
	} else {
		repo = store.NewMemory()
		log.Printf("storage=memory (run history will not survive restart)")
	}
	defer closeStore()

	executor := engine.New(repo, engine.Config{MaxConcurrent: 4, RetryLimit: 2, RetryDelay: 250 * time.Millisecond})
	defer executor.Close()
	addr := os.Getenv("RELAYFORGE_ADDR")
	if addr == "" { addr = ":8080" }
	server := &http.Server{Addr: addr, Handler: api.New(repo, executor).Handler(), ReadHeaderTimeout: 5*time.Second, ReadTimeout: 10*time.Second, WriteTimeout: 15*time.Second, IdleTimeout: 60*time.Second}
	log.Printf("RelayForge API listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed { log.Fatal(err) }
}
