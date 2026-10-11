// Package worker runs a pool of goroutines that claim jobs from the queue,
// run the matching handler, and record the outcome.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/SushantPulipati05/taskq/internal/queue"
)

// Handler does the actual work for one job kind. Returning nil marks the job done;
// returning an error records a failed attempt (retry with backoff, or dead).
// Handlers must respect ctx: it is cancelled when the lease runs out.
type Handler func(ctx context.Context, job *queue.Job) error

// Config controls how the pool claims and runs jobs.
type Config struct {
	Queue        string        // which queue to claim from
	Concurrency  int           // number of worker goroutines
	Lease        time.Duration // how long a claim is valid; also the handler timeout
	PollInterval time.Duration // how long to wait when the queue is empty
	ReapInterval time.Duration // how often to reclaim jobs with expired leases (0 = off)
	DepthInterval time.Duration // how often to sample queue depth for metrics (0 = off)
}

// Pool is a set of workers sharing one queue and one set of handlers.
type Pool struct {
	q        *queue.Queue
	cfg      Config
	handlers map[string]Handler
	log      *slog.Logger
}

// NewPool creates a worker pool. Register handlers with Handle before calling Run.
func NewPool(q *queue.Queue, cfg Config, log *slog.Logger) *Pool {
	return &Pool{
		q:        q,
		cfg:      cfg,
		handlers: make(map[string]Handler),
		log:      log,
	}
}

// Handle registers the handler for a job kind. It must be called before Run:
// the handlers map is read by many goroutines and is not safe to change later.
func (p *Pool) Handle(kind string, h Handler) {
	p.handlers[kind] = h
}

// Run starts Concurrency workers and blocks until all of them have stopped.
// Workers stop when ctx is cancelled.
func (p *Pool) Run(ctx context.Context) {
	host, _ := os.Hostname()

	var wg sync.WaitGroup
	for i := 1; i <= p.cfg.Concurrency; i++ {
		// Unique across processes and machines, so two worker processes never
		// share an ID (Complete/Fail rely on locked_by to check ownership).
		workerID := fmt.Sprintf("%s-%d-w%d", host, os.Getpid(), i)

		wg.Add(1)
		go func() {
			defer wg.Done()
			p.loop(ctx, workerID)
		}()
	}

		// One goroutine samples queue depth for the taskq_queue_depth gauge.
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.depthLoop(ctx)
	}()

	// One reaper per process. If several worker processes run, each has a reaper;
	// that's safe because the reap UPDATE is atomic, so a job is reclaimed once.
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.reapLoop(ctx)
	}()
	wg.Wait()
}

// reapLoop periodically returns jobs whose lease expired (their worker crashed or
// hung) to the queue, until ctx is cancelled.
func (p *Pool) reapLoop(ctx context.Context) {
	if p.cfg.ReapInterval <= 0 {
		return
	}
	log := p.log.With("component", "reaper")

	for {
		sleep(ctx, p.cfg.ReapInterval)
		if ctx.Err() != nil {
			return
		}

		n, err := p.q.Reap(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("reap failed", "err", err)
			continue
		}
				if n > 0 {
			jobsReaped.Add(float64(n))
			log.Warn("reclaimed jobs with expired leases", "count", n)
		}
	}
}

// depthLoop periodically records how many jobs are queued and running, so the
// taskq_queue_depth gauge shows whether workers are keeping up.
func (p *Pool) depthLoop(ctx context.Context) {
	if p.cfg.DepthInterval <= 0 {
		return
	}
	for {
		queued, running, err := p.q.Depth(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.log.Error("sample queue depth", "err", err)
		} else {
			queueDepth.WithLabelValues("queued").Set(float64(queued))
			queueDepth.WithLabelValues("running").Set(float64(running))
		}

		sleep(ctx, p.cfg.DepthInterval)
		if ctx.Err() != nil {
			return
		}
	}
}

// loop is one worker: claim, process, repeat, until ctx is cancelled.
func (p *Pool) loop(ctx context.Context, workerID string) {
	log := p.log.With("worker", workerID)
	log.Info("worker started")
	defer log.Info("worker stopped")

	for {
		if ctx.Err() != nil {
			return
		}

				claimStart := time.Now()
		job, err := p.q.Claim(ctx, p.cfg.Queue, workerID, p.cfg.Lease)
		claimDuration.WithLabelValues(claimResult(job, err)).Observe(time.Since(claimStart).Seconds())

		if err != nil {
			if ctx.Err() != nil {
				return // shutting down; the error is just the cancellation
			}
			log.Error("claim failed", "err", err)
			sleep(ctx, p.cfg.PollInterval) // don't hammer a struggling database
			continue
		}
		if job == nil {
			sleep(ctx, p.cfg.PollInterval) // queue empty: wait before asking again
			continue
		}

		p.process(ctx, log, workerID, job)
	}
}

// process runs the handler for one claimed job and records the result.
func (p *Pool) process(ctx context.Context, log *slog.Logger, workerID string, job *queue.Job) {
	log = log.With("job_id", job.ID, "kind", job.Kind, "attempt", job.Attempts)
	start := time.Now()

	jobErr := p.runHandler(ctx, job)
		jobDuration.WithLabelValues(job.Kind).Observe(time.Since(start).Seconds())

	// Record the outcome with a context that is NOT cancelled when ctx is, so a job
	// that finished just as we were shutting down still gets marked done/failed.
	// The 5s timeout stops a dead database from blocking the worker forever.
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if jobErr == nil {
				if err := p.q.Complete(recordCtx, job.ID, workerID); err != nil {
			jobsProcessed.WithLabelValues(job.Kind, recordErrOutcome(err)).Inc()
			logRecordErr(log, "complete", err)
			return
		}
		jobsProcessed.WithLabelValues(job.Kind, "done").Inc()
		log.Info("job done", "duration", time.Since(start))
		return
	}

		if err := p.q.Fail(recordCtx, job.ID, workerID, jobErr); err != nil {
		jobsProcessed.WithLabelValues(job.Kind, recordErrOutcome(err)).Inc()
		logRecordErr(log, "fail", err)
		return
	}
	jobsProcessed.WithLabelValues(job.Kind, "failed").Inc()
	log.Warn("job failed", "err", jobErr, "max_attempts", job.MaxAttempts, "duration", time.Since(start))
}

// runHandler finds and runs the handler, turning a missing handler or a panic
// into an ordinary error so one bad job can't crash the whole worker process.
func (p *Pool) runHandler(ctx context.Context, job *queue.Job) (err error) {
	h, ok := p.handlers[job.Kind]
	if !ok {
		return fmt.Errorf("no handler registered for kind %q", job.Kind)
	}

	// The handler's context ignores shutdown (WithoutCancel): on Ctrl+C we let the
	// current job finish instead of aborting it halfway and wasting an attempt.
	// It is still limited to one lease, after which another worker may reclaim the job.
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.Lease)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()

	return h(hctx, job)
}

func logRecordErr(log *slog.Logger, op string, err error) {
	if errors.Is(err, queue.ErrLeaseLost) {
		log.Warn("lease lost; outcome not recorded", "op", op, "err", err)
		return
	}
	log.Error("could not record outcome", "op", op, "err", err)
}

// sleep waits for d, or returns early if ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}


// claimResult labels a claim for the claim-latency histogram.
func claimResult(job *queue.Job, err error) string {
	switch {
	case err != nil:
		return "error"
	case job == nil:
		return "empty"
	default:
		return "job"
	}
}