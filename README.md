# Taskq

![CI](https://github.com/SushantPulipati05/taskq/actions/workflows/ci.yml/badge.svg)

A durable background-job queue in Go, built on PostgreSQL alone, with no Redis, Kafka or RabbitMQ.
Workers claim jobs with `SELECT ... FOR UPDATE SKIP LOCKED`, hold them under a lease, retry
with exponential backoff, and are observable through Prometheus metrics.

I built it to learn how to get correctness right under concurrency and failure:
row locking, transactions, leases, at-least-once delivery, and measuring
performance instead of guessing.

## Features

- **Concurrent claiming without double-processing:** one atomic
  `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)` statement, so two workers
  never get the same job and never wait on each other.
- **Leases with fencing:** a claimed job is leased to one worker (`locked_by`, `locked_until`).
  Complete and Fail only succeed if that worker still holds the lease, so a worker that lost its
  lease can't overwrite another worker's result.
- **Retries with exponential backoff:** failed jobs return to the queue after 2^attempts seconds,
  and are marked `dead` after `max_attempts` (default 5) with the last error saved.
- **Reaper:** jobs whose worker crashed are reclaimed once their lease expires. A reclaimed attempt
  counts toward `max_attempts`, so a job that keeps crashing workers ends up `dead`.
- **Graceful shutdown:** on Ctrl+C / SIGTERM, workers stop claiming new jobs, let in-flight jobs
  finish and record their result, then exit (with a timeout longer than the lease).
- **Transactional outbox:** `queue.Enqueue` accepts a transaction, so business data and its job
  are committed together or not at all. Idempotency keys (`ON CONFLICT DO NOTHING`) stop
  duplicate enqueues.
- **Database-enforced invariants:** CHECK constraints make invalid states impossible to store,
  e.g. a job is `running` *if and only if* it holds a lease.
- **Observability:** Prometheus metrics (job outcomes, job duration and claim latency
  histograms for p50/p99, queue depth, reaped jobs) and structured logs with `log/slog`.

## Architecture

```
                 ┌──────────────────────────── PostgreSQL ───────────────────────────┐
 producer        │                                                                    │
 (cmd/enqueue) ──┼─► BEGIN; INSERT order; INSERT job; COMMIT   (transactional outbox) │
                 │                                                                    │
                 │   jobs table  ──  partial index: status = 'queued'   (claim)      │
                 │               ──  partial index: status = 'running'  (reaper)     │
                 └───────▲──────────────────────────────▲─────────────────────────────┘
                         │ claim / complete / fail       │ reclaim expired leases
          ┌──────────────┴───────────────┐        ┌──────┴──────┐
          │  worker pool (cmd/worker)     │        │   reaper     │
          │  N goroutines:                │        │  every 10 s  │
          │  claim → handler → complete   │        └─────────────┘
          │                  └→ fail (retry / dead)
          └──────────────┬───────────────┘
                         │ /metrics (:2112)
                   Prometheus (:9090)
```

Job lifecycle:

```
queued ──claim──► running ──complete──► done
  ▲                  │
  ├── fail (retry) ──┤
  │                  └── fail (attempts used up) ──► dead
  └── reaper (lease expired) ── running
```

## Guarantees and non-guarantees

- **At-least-once delivery.** Every job runs at least once. A job *can* run twice: if a worker
  is slower than its lease, the reaper hands the job to another worker. Fencing stops the slow
  worker from corrupting the job's state, but it can't undo side effects it already performed.
  **Handlers must be idempotent** (e.g. pass the job ID as an idempotency key to the email or
  payment provider).
- **No exactly-once.** Exactly-once is impossible when a worker can crash between doing the work
  and recording it.
- **Ordering:** jobs are claimed by priority, then `run_at`, then id. This is not a strict FIFO
  guarantee across concurrent workers.

## Quick start

Requires Go 1.22+, Docker, and [goose](https://github.com/pressly/goose)
(`go install github.com/pressly/goose/v3/cmd/goose@latest`).

```bash
docker compose up -d                      # Postgres 16 + Prometheus

export DATABASE_URL="postgres://taskq:taskq123@localhost:5433/taskqdb?sslmode=disable"
goose -dir migrations postgres "$DATABASE_URL" up

go run ./cmd/enqueue -n 100               # place 100 demo orders (each enqueues an email job)
go run ./cmd/worker                       # 4 workers; Ctrl+C for graceful shutdown
```

Metrics: http://localhost:2112/metrics · Prometheus UI: http://localhost:9090

(PowerShell: use `$env:DATABASE_URL = "..."` instead of `export`.)

## Benchmarks

Measured on a Windows desktop, with Postgres 16 running in Docker Desktop (WSL2) on the same machine.
Full query plans and raw results are in [docs/measurements.md](docs/measurements.md).

### Claim query: why the index is partial

1,000,000 jobs (999,500 done, 500 queued), median of 3 runs of `EXPLAIN (ANALYZE, BUFFERS)`:

| Index on `(queue, priority DESC, run_at, id)` | Plan                        | Time      | Pages read | Index size |
|-----------------------------------------------|-----------------------------|-----------|------------|------------|
| **Partial** (`WHERE status = 'queued'`)       | Index Scan, stops at row 1  | **0.19 ms** | ~23      | **48 kB**  |
| None                                          | Seq Scan + Sort             | 92.5 ms   | ~17,260    | —          |
| Full (same columns, all rows)                 | Index Scan, filters 999,500 rows | ~306 ms | ~23,260 | 47 MB   |

The full index was *slower than no index*. Finished jobs sort first in it, so Postgres
checked and discarded 999,500 of them (one random table read each) before reaching a queued job.
The partial index contains only queued jobs, so it stays ~1000× smaller and its first entry
is always the answer.

### Throughput

No-op handler (measures claim + complete only), 20,000 jobs per run (`go run ./cmd/bench`):

| Workers | Jobs/sec (default, durable commits) | Jobs/sec (`synchronous_commit=off`) |
|---------|-------------------------------------|-------------------------------------|
| 1       | 175                                 | 921                                 |
| 4       | 1,011                               | 3,511                               |
| 8       | 1,796                               | 5,216                               |
| 16      | 2,533                               | 6,947                               |
| 32      | 4,383                               | —                                   |

Findings:
- **The disk flush dominates.** At 1 worker, ~80% of each job's time (≈4.6 of 5.7 ms) was
  waiting for the commit to be flushed to disk (`fsync`). Each job needs two commits.
- **Group commit makes concurrency super-linear.** 4 workers were 5.8× faster than 1, because
  concurrent commits share a single disk flush. With `synchronous_commit=off` (nothing to share),
  4 workers gave 3.8×.
- **Plateau at ~4,200–4,400 jobs/sec.** Throughput stopped growing past ~48–64 workers
  (64 → 80 workers: +1%). Postgres, the Docker VM and the workers all share one machine, so the
  ceiling is a property of this setup, not of Taskq alone.
  Benchmarks were warmed up and repeated: a cold first run measured ~40% lower.
- Taskq keeps durable commits. `synchronous_commit=off` is ~5× faster at low concurrency, but a
  crash can lose recently "committed" job state.

## Metrics

| Metric                            | Type      | Labels           |
|-----------------------------------|-----------|------------------|
| `taskq_jobs_processed_total`      | counter   | kind, outcome (done, failed, lease_lost, record_error) |
| `taskq_job_duration_seconds`      | histogram | kind             |
| `taskq_claim_duration_seconds`    | histogram | result (job, empty, error) |
| `taskq_queue_depth`               | gauge     | status (queued, running) |
| `taskq_jobs_reaped_total`         | counter   | —                |

Example: p99 job duration
`histogram_quantile(0.99, sum by (le) (rate(taskq_job_duration_seconds_bucket[1m])))`

## Design decisions

- **Postgres as the queue:** jobs can be enqueued in the same transaction as business data
  (the outbox), with no second system to run, back up or keep consistent.
- **One atomic claim statement:** a separate SELECT then UPDATE leaves a gap where two workers
  can pick the same job (a check-then-act race).
- **`text` + CHECK instead of an enum** for status: easier to change later.
- **Partial indexes** for both the claim and reaper queries, with sizes proportional to active
  work, not to history.
- **Money as integer paise** in the demo `orders` table, never floats.

## Limitations and future work

- **No batching:** each job costs two round trips and two commits. Claiming N jobs per query
  would raise throughput, at the cost of more work lost when a worker dies.
- **Polling:** idle workers poll every second. Postgres `LISTEN/NOTIFY` could wake them instantly.
- **No lease extension (heartbeat):** jobs must finish within one lease.
- **Unbounded backoff:** 2^attempts is fine at 5 attempts but should be capped and jittered.
- **No retention:** finished jobs stay in the table forever. They don't slow claims (partial
  index), but the table grows. A periodic cleanup or partitioning by time would fix that.
- **Test coverage is focused on the queue's guarantees:** integration tests against a real
  Postgres cover concurrent claims (20 workers, 300 jobs, no duplicates), priority ordering, lease
  fencing, retry/backoff/dead, the reaper, idempotency keys, outbox rollback and graceful
  shutdown. They run with the race detector in CI on every push. Not yet covered: the metrics,
  the HTTP server and the demo `orders` code.

For production, use a mature library such as [River](https://riverqueue.com). This project
exists to understand the problems those libraries solve.

## Project structure

```
cmd/worker      worker pool + metrics server
cmd/enqueue     demo producer (orders + email jobs via the outbox)
cmd/bench       throughput benchmark
internal/queue  claim, complete, fail, reap, enqueue, depth (all SQL lives here)
internal/worker pool, handlers, graceful shutdown, reaper, metrics
internal/orders demo business code using the transactional outbox
migrations      goose migrations
docs            benchmark details
```