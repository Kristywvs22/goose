package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

// MigrationRunner handles running migrations with advisory locking pinned to a single connection.
type MigrationRunner struct {
	db *sql.DB
}

// NewMigrationRunner creates a new MigrationRunner.
func NewMigrationRunner(db *sql.DB) *MigrationRunner {
	return &MigrationRunner{db: db}
}

// RunMigrations executes migrations sequentially by acquiring a session-level advisory lock.
func (r *MigrationRunner) RunMigrations(ctx context.Context, lockID int64) error {
	// Obtain a dedicated connection from the pool to pin the session-level lock
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to obtain dedicated connection: %w", err)
	}
	defer conn.Close()

	// Acquire session-level advisory lock
	log.Printf("Attempting to acquire advisory lock %d...", lockID)
	_, err = conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", lockID)
	if err != nil {
		return fmt.Errorf("failed to acquire advisory lock: %w", err)
	}
	log.Printf("Advisory lock %d acquired successfully.", lockID)

	// Ensure the lock is released when we are done
	defer func() {
		log.Printf("Releasing advisory lock %d...", lockID)
		_, unlockErr := conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", lockID)
		if unlockErr != nil {
			log.Printf("Error releasing advisory lock %d: %v", lockID, unlockErr)
		} else {
			log.Printf("Advisory lock %d released successfully.", lockID)
		}
	}()

	// Start migration transaction on the same pinned connection
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p) // re-throw panic after rollback
		} else if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	// Execute migrations inside the transaction
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS goose_db_version (
			id serial PRIMARY KEY,
			version_id bigint NOT NULL,
			is_applied boolean NOT NULL,
			tstamp timestamp with time zone DEFAULT now()
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to run migration: %w", err)
	}

	// Simulate migration work
	time.Sleep(100 * time.Millisecond)

	return nil
}

func main() {
	fmt.Println("Goose Advisory Locking Runner")
	// Example usage and verification setup
	// In a real scenario, db would be initialized with a postgres driver.
	// db, err := sql.Open("postgres", "postgres://user:pass@localhost/db?sslmode=disable")
	// runner := NewMigrationRunner(db)
	// runner.RunMigrations(context.Background(), 12345)
}
