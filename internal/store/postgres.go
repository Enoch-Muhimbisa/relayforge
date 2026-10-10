package store

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "time"

    "github.com/Enoch-Muhimbisa/relayforge/internal/workflow"
    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"
)

// Postgres persists run state. Workflow definitions remain a versioned built-in catalog.
type Postgres struct { pool *pgxpool.Pool }

func NewPostgres(ctx context.Context, databaseURL string) (*Postgres, error) {
    cfg, err := pgxpool.ParseConfig(databaseURL)
    if err != nil { return nil, fmt.Errorf("parse database URL: %w", err) }
    cfg.MaxConns = 12
    cfg.MinConns = 1
    pool, err := pgxpool.NewWithConfig(ctx, cfg)
    if err != nil { return nil, fmt.Errorf("open PostgreSQL pool: %w", err) }
    p := &Postgres{pool: pool}
    if err := pool.Ping(ctx); err != nil { pool.Close(); return nil, fmt.Errorf("connect to PostgreSQL: %w", err) }
    if err := p.migrate(ctx); err != nil { pool.Close(); return nil, err }
    return p, nil
}

func (p *Postgres) Close() { p.pool.Close() }

func (p *Postgres) migrate(ctx context.Context) error {
    _, err := p.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS relayforge_runs (id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, payload JSONB NOT NULL)")
    if err != nil { return fmt.Errorf("migrate RelayForge run table: %w", err) }
    _, err = p.pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS relayforge_runs_created_at_idx ON relayforge_runs (created_at DESC)")
    if err != nil { return fmt.Errorf("index RelayForge run history: %w", err) }
    _, err = p.pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS relayforge_runs_status_idx ON relayforge_runs (status)")
    if err != nil { return fmt.Errorf("index RelayForge run status: %w", err) }
    return nil
}

func (p *Postgres) ListWorkflows() []workflow.Definition { return catalog() }
func (p *Postgres) GetWorkflow(id string) (workflow.Definition, error) {
    for _, def := range catalog() { if def.ID == id { return def, nil } }
    return workflow.Definition{}, ErrNotFound
}

func (p *Postgres) CreateRun(run workflow.Run) error {
    payload, err := json.Marshal(run); if err != nil { return fmt.Errorf("encode run: %w", err) }
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
    _, err = p.pool.Exec(ctx, "INSERT INTO relayforge_runs (id, workflow_id, status, created_at, payload) VALUES ($1,$2,$3,$4,$5)", run.ID, run.WorkflowID, run.Status, run.CreatedAt, payload)
    if err != nil { return fmt.Errorf("create run: %w", err) }; return nil
}

func (p *Postgres) GetRun(id string) (workflow.Run, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
    var payload []byte
    err := p.pool.QueryRow(ctx, "SELECT payload FROM relayforge_runs WHERE id=$1", id).Scan(&payload)
    if errors.Is(err, pgx.ErrNoRows) { return workflow.Run{}, ErrNotFound }
    if err != nil { return workflow.Run{}, fmt.Errorf("get run: %w", err) }
    var run workflow.Run; if err := json.Unmarshal(payload, &run); err != nil { return workflow.Run{}, fmt.Errorf("decode run: %w", err) }; return run, nil
}

func (p *Postgres) ListRuns(limit int) []workflow.Run {
    if limit < 1 { limit = 50 }; if limit > 200 { limit = 200 }
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
    rows, err := p.pool.Query(ctx, "SELECT payload FROM relayforge_runs ORDER BY created_at DESC LIMIT $1", limit)
    if err != nil { return []workflow.Run{} }; defer rows.Close()
    out := make([]workflow.Run, 0, limit)
    for rows.Next() { var payload []byte; if rows.Scan(&payload) != nil { continue }; var run workflow.Run; if json.Unmarshal(payload, &run) == nil { out = append(out, run) } }
    return out
}

func (p *Postgres) UpdateRun(run workflow.Run) error {
    payload, err := json.Marshal(run); if err != nil { return fmt.Errorf("encode run: %w", err) }
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
    tag, err := p.pool.Exec(ctx, "UPDATE relayforge_runs SET status=$2, payload=$3 WHERE id=$1", run.ID, run.Status, payload)
    if err != nil { return fmt.Errorf("update run: %w", err) }; if tag.RowsAffected() == 0 { return ErrNotFound }; return nil
}


// ClaimRun uses row-level locking to claim queued work or reclaim an expired worker lease.
// SKIP LOCKED allows multiple API/worker processes to compete without double-claiming a row.
func (p *Postgres) ClaimRun(ctx context.Context, workerID string, leaseDuration time.Duration) (workflow.Run, error) {
	if leaseDuration <= 0 { leaseDuration = 2 * time.Minute }
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil { return workflow.Run{}, fmt.Errorf("begin run claim: %w", err) }
	defer tx.Rollback(ctx)

	var id string
	var payload []byte
	err = tx.QueryRow(ctx, `SELECT id, payload FROM relayforge_runs
		WHERE status = 'queued'
		   OR (status = 'running' AND (lease_until IS NULL OR lease_until <= now()))
		ORDER BY created_at ASC
		LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&id, &payload)
	if errors.Is(err, pgx.ErrNoRows) { return workflow.Run{}, ErrNoPendingRun }
	if err != nil { return workflow.Run{}, fmt.Errorf("select pending run: %w", err) }
	var run workflow.Run
	if err := json.Unmarshal(payload, &run); err != nil { return workflow.Run{}, fmt.Errorf("decode pending run: %w", err) }
	run.Status = workflow.StatusRunning
	if run.StartedAt.IsZero() { run.StartedAt = time.Now().UTC() }
	payload, err = json.Marshal(run)
	if err != nil { return workflow.Run{}, fmt.Errorf("encode claimed run: %w", err) }
	_, err = tx.Exec(ctx, `UPDATE relayforge_runs
		SET status = 'running', worker_id = $2,
		    lease_until = now() + ($3 * interval '1 second'), payload = $4
		WHERE id = $1`, id, workerID, leaseDuration.Seconds(), payload)
	if err != nil { return workflow.Run{}, fmt.Errorf("assign run lease: %w", err) }
	if err := tx.Commit(ctx); err != nil { return workflow.Run{}, fmt.Errorf("commit run claim: %w", err) }
	return run, nil
}

// UpdateClaimedRun rejects writes from a worker whose lease has expired or been reclaimed.
func (p *Postgres) UpdateClaimedRun(run workflow.Run, workerID string, leaseDuration time.Duration) error {
	if leaseDuration <= 0 { leaseDuration = 2 * time.Minute }
	payload, err := json.Marshal(run)
	if err != nil { return fmt.Errorf("encode claimed run: %w", err) }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := p.pool.Exec(ctx, `UPDATE relayforge_runs
		SET status = $2, payload = $3,
		    worker_id = CASE WHEN $2 = 'running' THEN worker_id ELSE NULL END,
		    lease_until = CASE WHEN $2 = 'running' THEN now() + ($5 * interval '1 second') ELSE NULL END
		WHERE id = $1 AND worker_id = $4 AND status = 'running' AND lease_until > now()`,
		run.ID, run.Status, payload, workerID, leaseDuration.Seconds())
	if err != nil { return fmt.Errorf("checkpoint claimed run: %w", err) }
	if result.RowsAffected() == 0 { return ErrLeaseLost }
	return nil
}
