package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
)

// Migrate applies the necessary migrations to the database.
func Migrate(db *sql.DB, dir string, target int) error {
	// Get a dedicated connection from the pool
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err!= nil {
		return err
	}
	defer func() {
		// Ensure the connection is closed and returned to the pool
		if err := conn.Close(); err!= nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}()

	// Wrap the migration logic in a function to handle panics
	var resultErr error
	func() {
		defer func() {
			if r := recover(); r!= nil {
				resultErr = errors.Errorf("migration panic: %v", r)
			}
		}()

		// Acquire the advisory lock
		if err := acquireAdvisoryLock(conn); err!= nil {
			resultErr = err
			return
		}

		// Perform the migrations
		if err := performMigrations(conn, dir, target); err!= nil {
			resultErr = err
			return
		}

		// Release the advisory lock
		if err := releaseAdvisoryLock(conn); err!= nil {
			resultErr = err
			return
		}
	}()

	return resultErr
}

// acquireAdvisoryLock acquires the PostgreSQL advisory lock.
func acquireAdvisoryLock(conn *sql.Conn) error {
	_, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_lock(1)")
	return err
}

// releaseAdvisoryLock releases the PostgreSQL advisory lock.
func releaseAdvisoryLock(conn *sql.Conn) error {
	_, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock(1)")
	return err
}

// performMigrations performs the actual migrations.
func performMigrations(conn *sql.Conn, dir string, target int) error {
	// Your existing migration logic here, but using the dedicated connection
	// For example:
	// tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{})
	// if err!= nil {
	//	return err
	// }
	// defer tx.Rollback()
	// // Apply migrations
	// if err := applyMigrations(tx, dir, target); err!= nil {
	//	return err
	// }
	// if err := tx.Commit(); err!= nil {
	//	return err
	// }
	return nil
}
