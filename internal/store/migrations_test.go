package store

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestPostgresSchemaMigrationsAreRecorded(t *testing.T) {
	url := os.Getenv("RELAYFORGE_TEST_DATABASE_URL")
	if url == "" { t.Skip("RELAYFORGE_TEST_DATABASE_URL is not set") }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	repo, err := NewPostgres(ctx, url)
	if err != nil { t.Fatal(err) }
	defer repo.Close()
	rows, err := repo.pool.Query(ctx, "SELECT version FROM relayforge_schema_migrations ORDER BY version")
	if err != nil { t.Fatal(err) }
	defer rows.Close()
	var versions []int
	for rows.Next() { var version int; if err := rows.Scan(&version); err != nil { t.Fatal(err) }; versions = append(versions, version) }
	if err := rows.Err(); err != nil { t.Fatal(err) }
	if len(versions) < 2 || versions[0] != 1 || versions[1] != 2 { t.Fatalf("migration versions = %v, want at least [1 2]", versions) }
}
