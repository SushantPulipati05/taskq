// Package testdb gives each test its own empty, fully migrated Postgres schema,
// so tests never touch real data and can run in parallel without interfering.
package testdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// New returns a connection pool whose search_path points at a brand-new schema
// with all migrations applied. The schema is dropped when the test finishes.
// Tests are skipped if TEST_DATABASE_URL is not set.
func New(t *testing.T) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping database test")
	}
	ctx := context.Background()

	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	// Every connection in this pool sees only the test's schema, so the
	// unqualified table names in the migrations and queries land there.
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 30
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to schema: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Logf("drop schema %s: %v", schema, err)
		}
		admin.Close()
	})

	applyMigrations(t, pool)
	return pool
}

// applyMigrations runs the "-- +goose Up" section of every file in migrations/,
// in filename (timestamp) order, the same order goose uses.
func applyMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	// Find migrations/ relative to this source file, so tests work from any package.
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations")

	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no migrations found in %s (err: %v)", dir, err)
	}
	sort.Strings(files)

	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		text := strings.ReplaceAll(string(raw), "\r\n", "\n")
		up, _, found := strings.Cut(text, "-- +goose Down")
		if !found {
			t.Fatalf("%s: no '-- +goose Down' marker", filepath.Base(f))
		}
		if _, err := pool.Exec(context.Background(), up); err != nil {
			t.Fatalf("apply %s: %v", filepath.Base(f), err)
		}
	}
}
