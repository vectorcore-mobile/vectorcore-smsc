// Package sqlite implements the store.Store interface using modernc.org/sqlite
// via database/sql.  Intended for testing and demo deployments; not recommended
// for production.
package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

// DB holds the sql.DB handle and implements store.Store.
type DB struct {
	db     *sql.DB
	notify *notifier
}

// Open opens (or creates) a SQLite database at the given path and enables
// WAL journal mode for concurrent reads.
func Open(ctx context.Context, dsn string, pollInterval time.Duration) (*DB, error) {
	// modernc sqlite DSN may need _pragma options appended
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %q: %w", dsn, err)
	}
	// WAL mode for concurrent readers
	if _, err := sqlDB.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("enable WAL: %w", err)
	}
	if _, err := sqlDB.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("enable FK: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	// SQLite is single-writer; limit pool to 1 writer connection
	sqlDB.SetMaxOpenConns(1)

	// Auto-apply schema (all statements use CREATE TABLE IF NOT EXISTS)
	if _, err := sqlDB.ExecContext(ctx, schema); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("apply sqlite schema: %w", err)
	}
	// Additive migrations — ignore errors for existing DBs.
	sqlDB.ExecContext(ctx, `ALTER TABLE smpp_server_accounts ADD COLUMN name TEXT NOT NULL DEFAULT ''`)
	sqlDB.ExecContext(ctx, `UPDATE smpp_server_accounts SET name = system_id WHERE COALESCE(name,'') = ''`)
	sqlDB.ExecContext(ctx, `ALTER TABLE ims_registrations ADD COLUMN imsi TEXT`)
	sqlDB.ExecContext(ctx, `ALTER TABLE routing_rules RENAME COLUMN egress_peer_id TO egress_peer`)
	// diameter_peers: add applications (JSON array) alongside legacy application column.
	sqlDB.ExecContext(ctx, `ALTER TABLE diameter_peers ADD COLUMN applications TEXT NOT NULL DEFAULT '[]'`)
	// Migrate existing single-value rows to JSON array form.
	sqlDB.ExecContext(ctx, `UPDATE diameter_peers SET applications = json_array(application) WHERE applications = '[]'`)
	sqlDB.ExecContext(ctx, `ALTER TABLE smpp_clients ADD COLUMN transport TEXT NOT NULL DEFAULT 'tcp'`)
	sqlDB.ExecContext(ctx, `ALTER TABLE smpp_clients ADD COLUMN verify_server_cert INTEGER NOT NULL DEFAULT 0`)
	sqlDB.ExecContext(ctx, `ALTER TABLE smpp_clients ADD COLUMN source_addr_ton INTEGER`)
	sqlDB.ExecContext(ctx, `ALTER TABLE smpp_clients ADD COLUMN source_addr_npi INTEGER`)
	sqlDB.ExecContext(ctx, `ALTER TABLE smpp_clients ADD COLUMN dest_addr_ton INTEGER`)
	sqlDB.ExecContext(ctx, `ALTER TABLE smpp_clients ADD COLUMN dest_addr_npi INTEGER`)
	sqlDB.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN udh BLOB`)
	sqlDB.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN encoding INTEGER`)
	sqlDB.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN route_cursor INTEGER NOT NULL DEFAULT 0`)
	sqlDB.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN alert_correlation_id TEXT`)
	sqlDB.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN deferred_reason TEXT`)
	sqlDB.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN deferred_interface TEXT`)
	sqlDB.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN serving_node_at_deferral TEXT`)
	sqlDB.ExecContext(ctx, `ALTER TABLE subscribers ADD COLUMN mme_number TEXT`)
	sqlDB.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN kind TEXT NOT NULL DEFAULT 'sm'`)
	sqlDB.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN ref_message_id TEXT`)
	sqlDB.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN raw_tpdu BLOB`)
	sqlDB.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN smpp_registered_delivery INTEGER`)
	sqlDB.ExecContext(ctx, `UPDATE smpp_server_accounts
		SET default_route_id = NULL
		WHERE default_route_id IN (SELECT id FROM routing_rules WHERE egress_iface = 'sgd')`)
	sqlDB.ExecContext(ctx, `DELETE FROM routing_rules WHERE egress_iface = 'sgd'`)
	sqlDB.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_messages_alert_corr ON messages (alert_correlation_id)`)
	// Crash recovery: messages stuck in DISPATCHED state (server died mid-send)
	// are reset to QUEUED so the retry scheduler re-attempts them.
	sqlDB.ExecContext(ctx, `UPDATE messages SET status='QUEUED', next_retry_at=datetime('now') WHERE status='DISPATCHED'`)
	// Migration 002: S6c-to-SGd MME name mapping table.
	sqlDB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS sgd_mme_mappings (
		id          TEXT PRIMARY KEY,
		s6c_result  TEXT NOT NULL UNIQUE,
		sgd_host    TEXT NOT NULL,
		enabled     INTEGER NOT NULL DEFAULT 1,
		created_at  TEXT NOT NULL DEFAULT (datetime('now')),
		updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
	)`)

	n := newNotifier(sqlDB, pollInterval)
	go n.run(ctx)

	return &DB{db: sqlDB, notify: n}, nil
}

// Close shuts down the database.
func (db *DB) Close() error {
	db.notify.stop()
	return db.db.Close()
}

// Subscribe registers ch to receive ChangeEvents for table via polling.
func (db *DB) Subscribe(ctx context.Context, table string, ch chan<- ChangeEvent) error {
	db.notify.subscribe(table, ch)
	return nil
}
