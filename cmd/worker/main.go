package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SushantPulipati05/taskq/internal/queue"
	"github.com/SushantPulipati05/taskq/internal/worker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Error("DATABASE_URL environment variable is empty")
		os.Exit(1)
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		logger.Error("create pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		logger.Error("ping", "err", err)
		os.Exit(1)
	}
	logger.Info("connected to postgres")

	q := queue.New(pool)
	workers := worker.NewPool(q, worker.Config{
		Queue:        "default",
		Concurrency:  4,
		Lease:        30 * time.Second,
		PollInterval: time.Second,
	}, logger)

	workers.Handle("email", sendEmail)
	workers.Handle("urgent-email", sendEmail)

	logger.Info("starting workers", "queue", "default", "concurrency", 4)
	workers.Run(ctx) // blocks; Ctrl+C kills the process for now (graceful shutdown is Step 5)
}

// emailPayload is what an "email" job carries in its payload column.
type emailPayload struct {
	To string `json:"to"`
}

// sendEmail is a fake handler: it takes 200–800ms and fails about 1 in 5 times,
// so you can watch retries, backoff and dead jobs happen.
func sendEmail(ctx context.Context, job *queue.Job) error {
	var p emailPayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return fmt.Errorf("bad payload: %w", err)
	}

	work := time.Duration(200+rand.IntN(600)) * time.Millisecond
	select {
	case <-time.After(work):
	case <-ctx.Done():
		return ctx.Err() // lease ran out or we're shutting down
	}

	if rand.IntN(5) == 0 {
		return errors.New("smtp timeout (simulated)")
	}
	return nil
}