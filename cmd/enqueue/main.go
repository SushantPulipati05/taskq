// Command enqueue places demo orders. Each order and its confirmation-email job
// are written in one transaction (the transactional outbox pattern).
//
//	go run ./cmd/enqueue -n 10        place 10 orders
//	go run ./cmd/enqueue -crash       fail before commit: no order, no job
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SushantPulipati05/taskq/internal/orders"
)

func main() {
	n := flag.Int("n", 1, "number of orders to place")
	crash := flag.Bool("crash", false, "simulate a crash after the inserts but before COMMIT")
	flag.Parse()

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

	for i := 1; i <= *n; i++ {
		email := fmt.Sprintf("customer%d@example.com", i)
		orderID, jobID, err := orders.PlaceOrder(ctx, pool, email, 49900, *crash) // ₹499.00

		if errors.Is(err, orders.ErrSimulatedCrash) {
			logger.Warn("crashed before commit; transaction rolled back", "email", email)
			continue
		}
		if err != nil {
			logger.Error("place order", "email", email, "err", err)
			os.Exit(1)
		}
		logger.Info("order placed", "order_id", orderID, "job_id", jobID, "email", email)
	}
}
