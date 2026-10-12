// Command bench measures Taskq's throughput: how many jobs per second the worker
// pool completes at different concurrency levels, using a handler that does no
// work, so the result measures the queue itself (claim + complete in Postgres).
//
//	go run ./cmd/bench -jobs 20000 -concurrency 1,4,8,16,32
//
// It only touches jobs in the "bench" queue, so your other data is left alone.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SushantPulipati05/taskq/internal/queue"
	"github.com/SushantPulipati05/taskq/internal/worker"
)

const benchQueue = "bench"

func main() {
	jobs := flag.Int("jobs", 20000, "jobs to process per run")
	levels := flag.String("concurrency", "1,4,8,16,32", "comma-separated worker counts to test")
	flag.Parse()

	ctx := context.Background()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL environment variable is empty")
		os.Exit(1)
	}

	fmt.Printf("%-12s %-10s %-12s %-12s\n", "workers", "jobs", "seconds", "jobs/sec")
	for _, s := range strings.Split(*levels, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 1 {
			fmt.Fprintf(os.Stderr, "bad concurrency value %q\n", s)
			os.Exit(1)
		}
		elapsed, err := runOnce(ctx, databaseURL, n, *jobs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "run with %d workers: %v\n", n, err)
			os.Exit(1)
		}
		fmt.Printf("%-12d %-10d %-12.2f %-12.0f\n", n, *jobs, elapsed.Seconds(), float64(*jobs)/elapsed.Seconds())
	}
}

// runOnce loads `jobs` no-op jobs, runs `concurrency` workers until all are done,
// and returns how long that took.
func runOnce(ctx context.Context, databaseURL string, concurrency, jobs int) (time.Duration, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return 0, err
	}
	// Each worker needs its own connection while it queries. pgxpool's default
	// maximum is small (4, or the number of CPUs), so without this, extra workers
	// would just queue up waiting for a connection and the test would be wrong.
	cfg.MaxConns = int32(concurrency + 2)

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return 0, err
	}
	defer pool.Close()

	if err := reset(ctx, pool, jobs); err != nil {
		return 0, err
	}

	q := queue.New(pool)
	// Only warnings and errors: logging every job would itself slow the test down.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	workers := worker.NewPool(q, worker.Config{
		Queue:        benchQueue,
		Concurrency:  concurrency,
		Lease:        30 * time.Second,
		PollInterval: 50 * time.Millisecond,
		// Reaper and depth sampling off: we only want to measure claim + complete.
	}, logger)
	workers.Handle("noop", func(ctx context.Context, job *queue.Job) error { return nil })

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})

	start := time.Now()
	go func() {
		workers.Run(runCtx)
		close(done)
	}()

	// Poll until no bench jobs are left unfinished, then stop the workers.
	for {
		var left int64
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM jobs WHERE queue = $1 AND status IN ('queued', 'running')`,
			benchQueue).Scan(&left)
		if err != nil {
			stop()
			return 0, fmt.Errorf("count remaining: %w", err)
		}
		if left == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)

	stop()
	<-done
	return elapsed, nil
}

// reset deletes old bench jobs, cleans up the dead rows they leave behind, and
// inserts a fresh batch of no-op jobs in one statement.
func reset(ctx context.Context, pool *pgxpool.Pool, jobs int) error {
	if _, err := pool.Exec(ctx, `DELETE FROM jobs WHERE queue = $1`, benchQueue); err != nil {
		return fmt.Errorf("delete old bench jobs: %w", err)
	}
	// DELETE leaves dead rows and index entries behind until VACUUM removes them;
	// without this, every run would be slower than the one before it.
	if _, err := pool.Exec(ctx, `VACUUM (ANALYZE) jobs`); err != nil {
		return fmt.Errorf("vacuum: %w", err)
	}
	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (queue, kind) SELECT $1, 'noop' FROM generate_series(1, $2::int)`,
		benchQueue, jobs)
	if err != nil {
		return fmt.Errorf("insert bench jobs: %w", err)
	}
	return nil
}