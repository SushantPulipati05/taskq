-- +goose Up
-- The reaper looks for running jobs whose lease has expired. Like the claim index,
-- this is partial: only 'running' rows are in it, so it stays tiny even when the
-- table holds millions of done jobs.
CREATE INDEX jobs_reap_idx ON jobs (locked_until) WHERE status = 'running';

-- +goose Down
DROP INDEX IF EXISTS jobs_reap_idx;