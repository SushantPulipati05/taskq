-- Claim one job atomically.
--
-- Parameters (used from Go with pgx):
--   $1  queue name           e.g. 'default'
--   $2  worker id            e.g. 'worker-A'
--   $3  lease length (secs)  e.g. 30
--
-- How it works:
--   1. The inner SELECT finds the best runnable job: queued, in this queue, run_at reached,
--      ordered exactly like jobs_claim_idx (priority DESC, run_at, id) so Postgres can walk
--      the index and stop at the first match without sorting.
--   2. FOR UPDATE SKIP LOCKED locks that row; rows already locked by other workers are skipped,
--      so concurrent workers each get a different job and nobody waits.
--   3. The outer UPDATE marks it running and sets the lease, in the SAME statement, so there is
--      no gap between "found it" and "took it" where another worker or a crash could interfere.
--   4. RETURNING hands the claimed row back to the worker. Zero rows = nothing to do.

UPDATE jobs
SET    status       = 'running',
       locked_by    = $2,
       locked_until = now() + make_interval(secs => $3),
       attempts     = attempts + 1,
       updated_at   = now()
WHERE  id = (
    SELECT id
    FROM   jobs
    WHERE  status = 'queued'
      AND  queue  = $1
      AND  run_at <= now()
    ORDER  BY priority DESC, run_at, id
    LIMIT  1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, queue, kind, payload, priority, attempts, max_attempts, locked_until;


hij

njk