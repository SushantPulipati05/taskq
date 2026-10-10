-- +goose Up

CREATE TABLE jobs (
    -- why: identity bigint is compact, ordered (good index locality) and easy to read in logs/psql.
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- why: workers subscribe to a queue; kind tells the worker which handler to run.
    queue           text        NOT NULL DEFAULT 'default',
    kind            text        NOT NULL,

    -- why: jsonb is validated JSON stored in binary form; payload shape varies by kind.
    payload         jsonb       NOT NULL DEFAULT '{}',

    -- why: text + CHECK instead of an enum; adding a status later is a simple constraint change,
    -- while ALTER TYPE on enums has sharp edges.
    status          text        NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued', 'running', 'done', 'dead')),

    -- why: convention for this project: HIGHER number = MORE urgent.
    priority        int         NOT NULL DEFAULT 0,

    attempts        int         NOT NULL DEFAULT 0,
    max_attempts    int         NOT NULL DEFAULT 5 CHECK (max_attempts > 0),

    -- why: earliest time the job may run. Needed for retry backoff
    -- (a failed job goes back to 'queued' with run_at in the future).
    run_at          timestamptz NOT NULL DEFAULT now(),

    -- why: the lease. Set only while a worker holds the job; if locked_until passes,
    -- the worker is presumed dead and the job can be reclaimed (Day 2).
    locked_by       text,
    locked_until    timestamptz,

    last_error      text,

    -- why: lets a producer enqueue "the same job" twice safely (e.g. a retried webhook).
    -- NULLs never conflict in a UNIQUE constraint, so jobs without a key are unaffected.
    idempotency_key text,

    -- why: timestamptz stores an absolute instant; plain timestamp loses the timezone.
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT jobs_attempts_range
        CHECK (attempts >= 0 AND attempts <= max_attempts),

    -- why: a job holds a lease if and only if it is running. Catches bugs where
    -- a job is marked done but still looks locked, or running with no owner.
    CONSTRAINT jobs_lease_iff_running
        CHECK ((status = 'running') = (locked_by IS NOT NULL AND locked_until IS NOT NULL)),

    CONSTRAINT jobs_queue_idempotency_key_uniq
        UNIQUE (queue, idempotency_key)
);

-- Partial index for the claim query.
-- why partial: only 'queued' rows are ever claimed. Done/dead rows pile up forever,
-- but they never enter this index, so it stays small and hot in memory, and a row
-- leaves the index the moment it stops being queued.
-- The claim query must filter `status = 'queued' AND queue = $1` and
-- ORDER BY priority DESC, run_at, id  -- exactly this index's order,
-- so Postgres can walk the index and stop at the first unlocked row (no Sort node).
CREATE INDEX jobs_claim_idx
    ON jobs (queue, priority DESC, run_at, id)
    WHERE status = 'queued';

-- +goose Down
DROP INDEX IF EXISTS jobs_claim_idx;
DROP TABLE IF EXISTS jobs;
