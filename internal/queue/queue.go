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

// ErrLeaseLost means the worker no longer holds the job: its lease expired and the
// job was reclaimed, or it was already finished. The worker must not treat its
// result as recorded.
var ErrLeaseLost = errors.New("lease lost")

// Job is a claimed job, as returned by Claim.
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

// Queue runs job-queue operations against Postgres.
type Queue struct {
	pool *pgxpool.Pool
}

// New returns a Queue that uses the given connection pool.
func New(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

const claimSQL = `
UPDATE jobs
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
RETURNING id, queue, kind, payload, priority, attempts, max_attempts, locked_until`

const completeSQL = `
UPDATE jobs
SET    status       = 'done',
       locked_by    = NULL,
       locked_until = NULL,
       updated_at   = now()
WHERE  id = $1
  AND  locked_by = $2
  AND  status = 'running'`

// failSQL records a failed attempt. If attempts are used up the job goes to 'dead';
// otherwise it goes back to 'queued' with run_at pushed out by 2^attempts seconds
// (2s, 4s, 8s, ...). Claim already incremented attempts, so attempts here is
// "how many tries have happened so far".
const failSQL = `
UPDATE jobs
SET    status = CASE
                    WHEN attempts >= max_attempts THEN 'dead'
                    ELSE 'queued'
                END,
       run_at = CASE
                    WHEN attempts >= max_attempts THEN run_at
                    ELSE now() + make_interval(secs => power(2, attempts))
                END,
       last_error   = $3,
       locked_by    = NULL,
       locked_until = NULL,
       updated_at   = now()
WHERE  id = $1
  AND  locked_by = $2
  AND  status = 'running'`

// reapSQL returns jobs whose lease expired to the queue. An expired lease means the
// worker crashed or hung, so the attempt counts as used: a job that keeps crashing
// its worker eventually goes to 'dead' instead of crashing workers forever.
const reapSQL = `
UPDATE jobs
SET    status = CASE
                    WHEN attempts >= max_attempts THEN 'dead'
                    ELSE 'queued'
                END,
       run_at       = now(),
       last_error   = 'lease expired: worker ' || locked_by || ' stopped responding',
       locked_by    = NULL,
       locked_until = NULL,
       updated_at   = now()
WHERE  status = 'running'
  AND  locked_until < now()`

// Claim atomically takes the next runnable job from the given queue and leases it
// to workerID for the lease duration. It returns (nil, nil) when no job is available.
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

// Complete marks a job done, but only if workerID still holds its lease.
// It returns ErrLeaseLost (wrapped) if the worker no longer owns the job.
func (q *Queue) Complete(ctx context.Context, jobID int64, workerID string) error {
	tag, err := q.pool.Exec(ctx, completeSQL, jobID, workerID)
	if err != nil {
		return fmt.Errorf("complete job %d: %w", jobID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("complete job %d: %w", jobID, ErrLeaseLost)
	}
	return nil
}

// Fail records a failed attempt for a job workerID holds. The job is retried later
// with exponential backoff, or marked dead once max_attempts is reached.
// It returns ErrLeaseLost (wrapped) if the worker no longer owns the job.
func (q *Queue) Fail(ctx context.Context, jobID int64, workerID string, jobErr error) error {
	msg := "unknown error"
	if jobErr != nil {
		msg = jobErr.Error()
	}

	tag, err := q.pool.Exec(ctx, failSQL, jobID, workerID, msg)
	if err != nil {
		return fmt.Errorf("fail job %d: %w", jobID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("fail job %d: %w", jobID, ErrLeaseLost)
	}
	return nil
}

// Reap returns jobs whose lease has expired back to the queue (or to dead, if their
// attempts are used up). It returns how many jobs were reclaimed.
func (q *Queue) Reap(ctx context.Context) (int64, error) {
	tag, err := q.pool.Exec(ctx, reapSQL)
	if err != nil {
		return 0, fmt.Errorf("reap expired leases: %w", err)
	}
	return tag.RowsAffected(), nil
}
