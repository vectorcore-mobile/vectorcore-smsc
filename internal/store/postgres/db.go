// Package postgres implements the store.Store interface using pgx/v5.
package postgres

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

// DB holds the connection pool and implements store.Store.
type DB struct {
	pool   *pgxpool.Pool
	notify *notifier
}

// Open creates a pgx connection pool and starts the LISTEN/NOTIFY notifier.
func Open(ctx context.Context, dsn string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply postgres schema: %w", err)
	}

	n, err := newNotifier(ctx, dsn)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("notifier: %w", err)
	}

	db := &DB{pool: pool, notify: n}
	// Additive migrations — ignore errors if already applied.
	pool.Exec(ctx, `ALTER TABLE smpp_server_accounts ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT ''`)
	pool.Exec(ctx, `UPDATE smpp_server_accounts SET name = system_id WHERE COALESCE(name,'') = ''`)
	pool.Exec(ctx, `ALTER TABLE routing_rules RENAME COLUMN egress_peer_id TO egress_peer`)
	pool.Exec(ctx, `ALTER TABLE routing_rules ALTER COLUMN egress_peer TYPE TEXT USING egress_peer::text`)
	// diameter_peers: add applications (JSON array text) alongside legacy application column.
	pool.Exec(ctx, `ALTER TABLE diameter_peers ADD COLUMN IF NOT EXISTS applications TEXT NOT NULL DEFAULT '[]'`)
	pool.Exec(ctx, `UPDATE diameter_peers SET applications = json_build_array(application)::text WHERE applications = '[]'`)
	pool.Exec(ctx, `ALTER TABLE smpp_clients ADD COLUMN IF NOT EXISTS transport TEXT NOT NULL DEFAULT 'tcp'`)
	pool.Exec(ctx, `ALTER TABLE smpp_clients ADD COLUMN IF NOT EXISTS verify_server_cert BOOLEAN NOT NULL DEFAULT false`)
	pool.Exec(ctx, `ALTER TABLE smpp_clients ADD COLUMN IF NOT EXISTS source_addr_ton INT`)
	pool.Exec(ctx, `ALTER TABLE smpp_clients ADD COLUMN IF NOT EXISTS source_addr_npi INT`)
	pool.Exec(ctx, `ALTER TABLE smpp_clients ADD COLUMN IF NOT EXISTS dest_addr_ton INT`)
	pool.Exec(ctx, `ALTER TABLE smpp_clients ADD COLUMN IF NOT EXISTS dest_addr_npi INT`)
	pool.Exec(ctx, `ALTER TABLE messages ADD COLUMN IF NOT EXISTS udh BYTEA`)
	pool.Exec(ctx, `ALTER TABLE messages ADD COLUMN IF NOT EXISTS encoding SMALLINT`)
	pool.Exec(ctx, `ALTER TABLE messages ADD COLUMN IF NOT EXISTS route_cursor INT NOT NULL DEFAULT 0`)
	pool.Exec(ctx, `ALTER TABLE messages ADD COLUMN IF NOT EXISTS alert_correlation_id TEXT`)
	pool.Exec(ctx, `ALTER TABLE messages ADD COLUMN IF NOT EXISTS deferred_reason TEXT`)
	pool.Exec(ctx, `ALTER TABLE messages ADD COLUMN IF NOT EXISTS deferred_interface TEXT`)
	pool.Exec(ctx, `ALTER TABLE messages ADD COLUMN IF NOT EXISTS serving_node_at_deferral TEXT`)
	pool.Exec(ctx, `ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS mme_number TEXT`)
	pool.Exec(ctx, `ALTER TABLE messages ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'sm'`)
	pool.Exec(ctx, `ALTER TABLE messages ADD COLUMN IF NOT EXISTS ref_message_id UUID`)
	pool.Exec(ctx, `ALTER TABLE messages ADD COLUMN IF NOT EXISTS raw_tpdu BYTEA`)
	pool.Exec(ctx, `UPDATE smpp_server_accounts
		SET default_route_id = NULL
		WHERE default_route_id IN (SELECT id FROM routing_rules WHERE egress_iface = 'sgd')`)
	pool.Exec(ctx, `DELETE FROM routing_rules WHERE egress_iface = 'sgd'`)
	pool.Exec(ctx, `DO $$
BEGIN
	IF EXISTS (
		SELECT 1 FROM pg_constraint
		WHERE conrelid = 'messages'::regclass
		  AND conname = 'messages_status_check'
	) THEN
		ALTER TABLE messages DROP CONSTRAINT messages_status_check;
	END IF;
	ALTER TABLE messages
		ADD CONSTRAINT messages_status_check
		CHECK (status IN ('QUEUED','DISPATCHED','WAIT_TIMER','WAIT_EVENT','WAIT_TIMER_EVENT','DELIVERED','FAILED','EXPIRED'));
END $$`)
	pool.Exec(ctx, `DO $$
BEGIN
	IF EXISTS (
		SELECT 1 FROM pg_constraint
		WHERE conrelid = 'routing_rules'::regclass
		  AND conname = 'routing_rules_egress_iface_check'
	) THEN
		ALTER TABLE routing_rules DROP CONSTRAINT routing_rules_egress_iface_check;
	END IF;
	ALTER TABLE routing_rules
		ADD CONSTRAINT routing_rules_egress_iface_check
		CHECK (egress_iface IN ('sip3gpp','sipsimple','smpp'));
END $$`)
	pool.Exec(ctx, `DROP INDEX IF EXISTS idx_messages_next_retry_at`)
	pool.Exec(ctx, `DROP INDEX IF EXISTS idx_messages_expiry_at`)
	pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_messages_alert_corr ON messages (alert_correlation_id)`)
	pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_messages_next_retry_at ON messages (next_retry_at) WHERE status IN ('QUEUED','WAIT_TIMER','WAIT_TIMER_EVENT')`)
	pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_messages_expiry_at ON messages (expiry_at) WHERE status IN ('QUEUED','WAIT_TIMER','WAIT_EVENT','WAIT_TIMER_EVENT')`)
	// Crash recovery: messages stuck in DISPATCHED state (server died mid-send)
	// are reset to QUEUED so the retry scheduler re-attempts them.
	pool.Exec(ctx, `UPDATE messages SET status='QUEUED', next_retry_at=now() WHERE status='DISPATCHED'`)
	// Migration 002: S6c-to-SGd MME name mapping table.
	pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS sgd_mme_mappings (
		id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		s6c_result    TEXT NOT NULL UNIQUE,
		sgd_host      TEXT NOT NULL,
		enabled       BOOLEAN NOT NULL DEFAULT true,
		created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
	)`)
	pool.Exec(ctx, `DROP TRIGGER IF EXISTS sgd_mme_mappings_notify ON sgd_mme_mappings`)
	pool.Exec(ctx, `CREATE TRIGGER sgd_mme_mappings_notify
		AFTER INSERT OR UPDATE OR DELETE ON sgd_mme_mappings
		FOR EACH ROW EXECUTE FUNCTION notify_change()`)
	return db, nil
}

// Close shuts down the pool and notifier.
func (db *DB) Close() error {
	db.notify.close()
	db.pool.Close()
	return nil
}

// Subscribe registers ch to receive ChangeEvents for table.
func (db *DB) Subscribe(ctx context.Context, table string, ch chan<- ChangeEvent) error {
	return db.notify.subscribe(ctx, table, ch)
}
