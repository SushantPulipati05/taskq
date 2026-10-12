package queue_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SushantPulipati05/taskq/internal/queue"
	"github.com/SushantPulipati05/taskq/internal/testdb"
)

const lease = 30 * time.Second

// enqueue is a test helper that adds a job and fails the test on error.
func enqueue(t *testing.T, pool *pgxpool.Pool, j queue.NewJob) int64 {
	t.Helper()
	id, err := queue.Enqueue(context.Background(), pool, j)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return id
}

// status reads a job's status straight from the table.
func status(t *testing.T, pool *pgxpool.Pool, id int64) string {
	t.Helper()
	var s string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM jobs WHERE id = $1`, id).Scan(&s); err != nil {
		t.Fatalf("read status of job %d: %v", id, err)
	}
	return s
}

func TestClaimEmptyQueueReturnsNil(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool)

	job, err := q.Claim(context.Background(), "default", "w1", lease)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if job != nil {
		t.Fatalf("expected no job, got job %d", job.ID)
	}
}

func TestClaimOrdersByPriorityThenAge(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool)
	ctx := context.Background()

	low := enqueue(t, pool, queue.NewJob{Kind: "email", Priority: 0})
	high := enqueue(t, pool, queue.NewJob{Kind: "email", Priority: 10})
	low2 := enqueue(t, pool, queue.NewJob{Kind: "email", Priority: 0})

	for _, want := range []int64{high, low, low2} {
		job, err := q.Claim(ctx, "default", "w1", lease)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if job == nil || job.ID != want {
			t.Fatalf("claimed %v, want job %d", job, want)
		}
	}
}

// The core guarantee: many workers claiming at once never get the same job.
func TestConcurrentClaimsNeverDuplicate(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool)
	ctx := context.Background()

	const jobs, workers = 300, 20
	for i := 0; i < jobs; i++ {
		enqueue(t, pool, queue.NewJob{Kind: "email"})
	}

	var (
		mu      sync.Mutex
		claimed = make(map[int64]string) // job ID -> worker that claimed it
		wg      sync.WaitGroup
		errs    = make(chan error, workers)
	)
	for w := 0; w < workers; w++ {
		workerID := "w" + string(rune('A'+w))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				job, err := q.Claim(ctx, "default", workerID, lease)
				if err != nil {
					errs <- err
					return
				}
				if job == nil {
					return // queue drained
				}
				mu.Lock()
				if other, dup := claimed[job.ID]; dup {
					mu.Unlock()
					errs <- errors.New("job claimed twice by " + other + " and " + workerID)
					return
				}
				claimed[job.ID] = workerID
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if len(claimed) != jobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(claimed), jobs)
	}
}

func TestCompleteRequiresLease(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool)
	ctx := context.Background()

	id := enqueue(t, pool, queue.NewJob{Kind: "email"})
	if _, err := q.Claim(ctx, "default", "owner", lease); err != nil {
		t.Fatalf("claim: %v", err)
	}

	if err := q.Complete(ctx, id, "intruder"); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("complete by non-owner: got %v, want ErrLeaseLost", err)
	}
	if got := status(t, pool, id); got != "running" {
		t.Fatalf("non-owner changed status to %q", got)
	}

	if err := q.Complete(ctx, id, "owner"); err != nil {
		t.Fatalf("complete by owner: %v", err)
	}
	if got := status(t, pool, id); got != "done" {
		t.Fatalf("status = %q, want done", got)
	}
}

func TestFailRetriesWithBackoffThenDies(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool)
	ctx := context.Background()

	id := enqueue(t, pool, queue.NewJob{Kind: "email"})
	// Allow only two attempts.
	if _, err := pool.Exec(ctx, `UPDATE jobs SET max_attempts = 2 WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	// Attempt 1 fails: back to queued, scheduled in the future, error saved.
	if _, err := q.Claim(ctx, "default", "w1", lease); err != nil {
		t.Fatal(err)
	}
	if err := q.Fail(ctx, id, "w1", errors.New("smtp timeout")); err != nil {
		t.Fatalf("fail: %v", err)
	}
	var st, lastErr string
	var inFuture bool
	err := pool.QueryRow(ctx, `SELECT status, last_error, run_at > now() FROM jobs WHERE id = $1`, id).
		Scan(&st, &lastErr, &inFuture)
	if err != nil {
		t.Fatal(err)
	}
	if st != "queued" || lastErr != "smtp timeout" || !inFuture {
		t.Fatalf("after 1st failure: status=%q last_error=%q run_at_in_future=%v", st, lastErr, inFuture)
	}

	// Backoff means it can't be claimed yet.
	if job, err := q.Claim(ctx, "default", "w1", lease); err != nil || job != nil {
		t.Fatalf("claimed during backoff: job=%v err=%v", job, err)
	}

	// Skip the backoff wait, then fail attempt 2: attempts used up, so dead.
	if _, err := pool.Exec(ctx, `UPDATE jobs SET run_at = now() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Claim(ctx, "default", "w1", lease); err != nil {
		t.Fatal(err)
	}
	if err := q.Fail(ctx, id, "w1", errors.New("smtp timeout again")); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if got := status(t, pool, id); got != "dead" {
		t.Fatalf("status = %q, want dead", got)
	}
}

// End-to-end lease story: a worker's lease expires, the reaper reclaims the job,
// another worker takes it, and the original worker is fenced out.
func TestReapReclaimsExpiredLeaseAndFencesOldWorker(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool)
	ctx := context.Background()

	id := enqueue(t, pool, queue.NewJob{Kind: "email"})
	if _, err := q.Claim(ctx, "default", "slow", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond) // let the lease expire

	n, err := q.Reap(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 1 {
		t.Fatalf("reaped %d jobs, want 1", n)
	}

	job, err := q.Claim(ctx, "default", "fresh", lease)
	if err != nil || job == nil || job.ID != id {
		t.Fatalf("fresh worker claim: job=%v err=%v", job, err)
	}
	if job.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (the reaped attempt counts)", job.Attempts)
	}

	if err := q.Complete(ctx, id, "slow"); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("slow worker complete: got %v, want ErrLeaseLost", err)
	}
	if err := q.Complete(ctx, id, "fresh"); err != nil {
		t.Fatalf("fresh worker complete: %v", err)
	}
}

func TestReapLeavesValidLeasesAlone(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool)
	ctx := context.Background()

	id := enqueue(t, pool, queue.NewJob{Kind: "email"})
	if _, err := q.Claim(ctx, "default", "w1", lease); err != nil {
		t.Fatal(err)
	}
	n, err := q.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 || status(t, pool, id) != "running" {
		t.Fatalf("reaper touched a job with a valid lease (reaped=%d)", n)
	}
}

func TestEnqueueIdempotencyKey(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()

	j := queue.NewJob{Kind: "email", IdempotencyKey: "order-1-confirmation"}
	if _, err := queue.Enqueue(ctx, pool, j); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if _, err := queue.Enqueue(ctx, pool, j); !errors.Is(err, queue.ErrDuplicate) {
		t.Fatalf("second enqueue: got %v, want ErrDuplicate", err)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("%d jobs stored, want 1", n)
	}
}

// The outbox guarantee: a job enqueued inside a transaction disappears if the
// transaction rolls back.
func TestEnqueueInRolledBackTransactionLeavesNoJob(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, tx, queue.NewJob{Kind: "email"}); err != nil {
		t.Fatalf("enqueue in tx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d jobs after rollback, want 0", n)
	}
}
