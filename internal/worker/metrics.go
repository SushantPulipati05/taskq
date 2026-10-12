package worker

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/SushantPulipati05/taskq/internal/queue"
)

// Prometheus metrics for the worker pool. promauto registers each one with the
// default registry, which the /metrics HTTP endpoint (in main.go) serves.
//
// Label rule: labels must have a small, fixed set of values (kind, outcome, result).
// Never use a job ID or error message as a label: every distinct value creates a
// new time series, and millions of series would overwhelm Prometheus.
var (
	jobsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "taskq_jobs_processed_total",
		Help: "Jobs finished by a worker, by kind and outcome (done, failed, lease_lost, record_error).",
	}, []string{"kind", "outcome"})

	jobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "taskq_job_duration_seconds",
		Help:    "Time spent running a job's handler.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 14), // 5ms .. ~41s
	}, []string{"kind"})

	claimDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "taskq_claim_duration_seconds",
		Help:    "Latency of the claim query, by result (job, empty, error).",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16), // 0.1ms .. ~3.3s
	}, []string{"result"})

	jobsReaped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "taskq_jobs_reaped_total",
		Help: "Jobs whose lease expired and were returned to the queue (or marked dead) by the reaper.",
	})

	queueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "taskq_queue_depth",
		Help: "Jobs currently in each state (queued or running), sampled periodically.",
	}, []string{"status"})
)

// recordErrOutcome turns an error from Complete/Fail into an outcome label.
func recordErrOutcome(err error) string {
	if errors.Is(err, queue.ErrLeaseLost) {
		return "lease_lost"
	}
	return "record_error"
}
