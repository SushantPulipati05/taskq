package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SushantPulipati05/taskq/internal/queue"
	"github.com/SushantPulipati05/taskq/internal/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const shutdownTimeout = 40 * time.Second

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	// ctx is cancelled when the process receives Ctrl+C (os.Interrupt) or SIGTERM,
	// the signal Docker and Kubernetes send when they stop a container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
		Queue:         "default",
		Concurrency:   4,
		Lease:         30 * time.Second,
		PollInterval:  time.Second,
		ReapInterval:  10 * time.Second,
		DepthInterval: 5 * time.Second,
	}, logger)

	workers.Handle("email", sendEmail)
	workers.Handle("urgent-email", sendEmail)

	// Serve Prometheus metrics. ":2112" listens on all interfaces so the Prometheus
	// container can reach it via host.docker.internal.
	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":2112"
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server", "err", err)
		}
	}()
	logger.Info("serving metrics", "addr", metricsAddr, "path", "/metrics")

	logger.Info("starting workers", "queue", "default", "concurrency", 4)

	// Run the pool in the background so main can watch for the shutdown signal.
	done := make(chan struct{})
	go func() {
		workers.Run(ctx) // returns once every worker has stopped
		close(done)
	}()

	<-ctx.Done() // block here until Ctrl+C / SIGTERM
	logger.Info("shutdown signal received; finishing in-flight jobs", "timeout", shutdownTimeout)

	// Restore normal Ctrl+C behaviour: pressing it a second time kills the process
	// immediately, in case you don't want to wait.
	stop()

	select {
	case <-done:
		logger.Info("all workers stopped cleanly")

		// Workers are done; stop the metrics server too (wait up to 5s for any
		// in-progress scrape to finish).
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
			logger.Warn("metrics server shutdown", "err", err)
		}
	case <-time.After(shutdownTimeout):
		// Some job is taking too long. Exit anyway; its lease will expire and the
		// reaper (Step 6) will put it back in the queue.
		logger.Warn("timed out waiting for workers; exiting")
		os.Exit(1)
	} // blocks; Ctrl+C kills the process for now (graceful shutdown is Step 5)
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
