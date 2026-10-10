package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SushantPulipati05/taskq/internal/queue"
)

func main() {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is empty")
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping: %v", err)
	}
	fmt.Println("connected to postgres")

	var queued int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status = 'queued'").Scan(&queued)
	if err != nil {
		log.Fatalf("count queued jobs: %v", err)
	}
	fmt.Printf("queued jobs: %d\n", queued)

	
	q := queue.New(pool)
	job, err := q.Claim(ctx, "default", "worker-1", 30*time.Second)
	if err != nil {
		log.Fatalf("claim: %v", err)
	}
	if job == nil {
		fmt.Println("no jobs available")
		return
	}
	fmt.Printf("claimed job %d (kind=%s, priority=%d, attempt %d/%d, lease until %s)\n",
		job.ID, job.Kind, job.Priority, job.Attempts, job.MaxAttempts,
		job.LockedUntil.Format(time.RFC3339))
}