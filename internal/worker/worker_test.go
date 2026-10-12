package worker_test

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SushantPulipati05/taskq/internal/queue"
	"github.com/SushantPulipati05/taskq/internal/testdb"
	"github.com/SushantPulipati05/taskq/internal/worker"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPoolProcessesAllJobs(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool)
	ctx := context.Background()

	const jobs = 50
	for i := 0; i < jobs; i++ {
		if _, err := queue.Enqueue(ctx, pool, queue.NewJob{Kind: "noop"}); err != nil {
			t.Fatal(err)
		}
	}

	var handled atomic.Int64
	p := worker.NewPool(q, worker.Config{
		Queue: "default", Concurrency: 4, Lease: 30 * time.Second, PollInterval: 20 * time.Millisecond,
	}, quietLogger())
	p.Handle("noop", func(ctx context.Context, job *queue.Job) error {
		handled.Add(1)
		return nil
	})

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { p.Run(runCtx); close(done) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var left int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status <> 'done'`).Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d jobs still not done after 10s", left)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	<-done

	if got := handled.Load(); got != jobs {
		t.Fatalf("handler ran %d times, want %d (each job exactly once in a crash-free run)", got, jobs)
	}
}

// Graceful shutdown: cancelling the context while a job is running lets that job
// finish and be recorded as done, instead of being aborted.
func TestGracefulShutdownFinishesInFlightJob(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool)
	ctx := context.Background()

	id, err := queue.Enqueue(ctx, pool, queue.NewJob{Kind: "slow"})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	p := worker.NewPool(q, worker.Config{
		Queue: "default", Concurrency: 1, Lease: 30 * time.Second, PollInterval: 20 * time.Millisecond,
	}, quietLogger())
	p.Handle("slow", func(ctx context.Context, job *queue.Job) error {
		close(started)
		select {
		case <-time.After(300 * time.Millisecond): // the "work"
			return nil
		case <-ctx.Done():
			return ctx.Err() // would mean shutdown aborted the job: the bug we test for
		}
	})

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { p.Run(runCtx); close(done) }()

	<-started
	stop() // "Ctrl+C" while the job is mid-flight

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pool did not stop within 5s")
	}

	var st string
	if err := pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, id).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != "done" {
		t.Fatalf("in-flight job ended as %q, want done", st)
	}
}
