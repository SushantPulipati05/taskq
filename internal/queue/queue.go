package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Job struct {
	ID          int64
	Queue       string
	Kind        string
	Payload     json.RawMessage
	Priority    int
	Attempts    int
	MaxAttempts int
	LockedUntil time.Time
}

type Queue struct {
    pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Queue { 
	return &Queue{
		pool: pool,
	}
 }

 const claimSQL = `UPDATE jobs
SET    status       = 'running',
       locked_by    = $2,
       locked_until = now() + make_interval(secs => $3),
       attempts     = attempts + 1,
       updated_at   = now()
WHERE  id = (
    SELECT id
    FROM   jobs
    WHERE  status = 'queued'
      AND  queue  = $1
      AND  run_at <= now()
    ORDER  BY priority DESC, run_at, id
    LIMIT  1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, queue, kind, payload, priority, attempts, max_attempts, locked_until;`

func (q *Queue) Claim(ctx context.Context, queue, workerID string, lease time.Duration) (*Job, error) {
	var j Job
	err := q.pool.QueryRow(ctx, claimSQL, queue, workerID, lease.Seconds()).Scan(
		&j.ID,
		&j.Queue,
		&j.Kind,
		&j.Payload,
		&j.Priority,
		&j.Attempts,
		&j.MaxAttempts,
		&j.LockedUntil,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // queue is empty: not an error
	}
	if err != nil {
		return nil, fmt.Errorf("claim job: %w", err)
	}
	return &j, nil
}