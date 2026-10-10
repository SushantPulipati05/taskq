package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrDuplicate means a job with the same queue and idempotency key already exists,
// so nothing new was enqueued.
var ErrDuplicate = errors.New("duplicate job")

// Querier is anything that can run a query: the connection pool (*pgxpool.Pool)
// or an open transaction (pgx.Tx). Accepting this interface lets Enqueue run
// inside the caller's transaction, which is what makes the outbox pattern work.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// NewJob describes a job to enqueue.
type NewJob struct {
	Queue          string // defaults to "default"
	Kind           string // which handler runs it, e.g. "email"
	Payload        any    // marshalled to JSON
	Priority       int    // higher runs first
	IdempotencyKey string // optional; same key in the same queue is enqueued only once
}

const enqueueSQL = `
INSERT INTO jobs (queue, kind, payload, priority, idempotency_key)
VALUES ($1, $2, $3, $4, NULLIF($5, ''))
ON CONFLICT (queue, idempotency_key) DO NOTHING
RETURNING id`

// Enqueue inserts a job using db, which may be the pool or a transaction. When db
// is a transaction, the job becomes visible to workers only if that transaction
// commits, and disappears if it rolls back.
func Enqueue(ctx context.Context, db Querier, j NewJob) (int64, error) {
	if j.Queue == "" {
		j.Queue = "default"
	}
	payload, err := json.Marshal(j.Payload)
	if err != nil {
		return 0, fmt.Errorf("enqueue %s: marshal payload: %w", j.Kind, err)
	}

	var id int64
	err = db.QueryRow(ctx, enqueueSQL, j.Queue, j.Kind, payload, j.Priority, j.IdempotencyKey).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// ON CONFLICT DO NOTHING inserted nothing, so RETURNING returned no row.
		return 0, fmt.Errorf("enqueue %s key %q: %w", j.Kind, j.IdempotencyKey, ErrDuplicate)
	}
	if err != nil {
		return 0, fmt.Errorf("enqueue %s: %w", j.Kind, err)
	}
	return id, nil
}
