// Package orders is a tiny example of business code that uses Taskq's
// transactional outbox: saving an order and enqueueing its email are atomic.
package orders

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SushantPulipati05/taskq/internal/queue"
)

// ErrSimulatedCrash is returned when PlaceOrder is asked to fail on purpose
// between saving the order and committing, to demonstrate the rollback.
var ErrSimulatedCrash = errors.New("simulated crash before commit")

// PlaceOrder saves an order and enqueues its confirmation email in one transaction.
// Either both are saved or neither is. If crashBeforeCommit is true, it fails after
// both inserts but before COMMIT, so you can check that nothing was saved.
func PlaceOrder(ctx context.Context, pool *pgxpool.Pool, email string, amountPaise int64, crashBeforeCommit bool) (orderID, jobID int64, err error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin: %w", err)
	}
	// If anything below returns early, roll back. After a successful Commit,
	// Rollback does nothing, so it is always safe to defer.
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx,
		`INSERT INTO orders (customer_email, amount_paise) VALUES ($1, $2) RETURNING id`,
		email, amountPaise,
	).Scan(&orderID)
	if err != nil {
		return 0, 0, fmt.Errorf("insert order: %w", err)
	}

	// Same transaction (tx), not the pool: this is the whole point.
	jobID, err = queue.Enqueue(ctx, tx, queue.NewJob{
		Kind: "email",
		Payload: map[string]any{
			"to":       email,
			"order_id": orderID,
		},
		// One confirmation email per order, even if this code is retried.
		IdempotencyKey: fmt.Sprintf("order-%d-confirmation", orderID),
	})
	if err != nil {
		return 0, 0, fmt.Errorf("enqueue confirmation: %w", err)
	}

	if crashBeforeCommit {
		return 0, 0, ErrSimulatedCrash // deferred Rollback undoes BOTH inserts
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit: %w", err)
	}
	return orderID, jobID, nil
}

// compile-time check that a transaction can be passed to queue.Enqueue.
var _ queue.Querier = (pgx.Tx)(nil)
