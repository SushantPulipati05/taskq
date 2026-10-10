package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
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
}