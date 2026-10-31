-- VectorCore SMSC — SQLite initial schema
-- No LISTEN/NOTIFY triggers; hot-reload uses polling instead.
-- UUIDs stored as TEXT. Timestamps as TEXT (ISO 8601). Booleans as INTEGER.

CREATE TABLE IF NOT EXISTS smpp_server_accounts (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL DEFAULT '',
    system_id        TEXT NOT NULL UNIQUE,
    password_hash    TEXT NOT NULL,
    allowed_ip       TEXT,
    bind_type        TEXT NOT NULL CHECK (bind_type IN ('transmitter','receiver','transceiver')),
    throughput_limit INTEGER NOT NULL DEFAULT 0,
    default_route_id TEXT,
    enabled          INTEGER NOT NULL DEFAULT 1,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS smpp_clients (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    host                TEXT NOT NULL,
    port                INTEGER NOT NULL,
    transport           TEXT NOT NULL DEFAULT 'tcp' CHECK (transport IN ('tcp','tls')),
    verify_server_cert  INTEGER NOT NULL DEFAULT 0,
    system_id           TEXT NOT NULL,
    password            TEXT NOT NULL,
    bind_type           TEXT NOT NULL CHECK (bind_type IN ('transmitter','receiver','transceiver')),
    reconnect_interval  TEXT NOT NULL DEFAULT '10s',
    throughput_limit    INTEGER NOT NULL DEFAULT 0,
    source_addr_ton     INTEGER,
    source_addr_npi     INTEGER,
    dest_addr_ton       INTEGER,
    dest_addr_npi       INTEGER,
    enabled             INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sip_peers (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    address     TEXT NOT NULL,
    port        INTEGER NOT NULL DEFAULT 5060,
    transport   TEXT NOT NULL DEFAULT 'udp' CHECK (transport IN ('udp','tcp','tls')),
    domain      TEXT NOT NULL,
    auth_user   TEXT,
    auth_pass   TEXT,
    enabled     INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS diameter_peers (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    host            TEXT NOT NULL,
    realm           TEXT NOT NULL,
    port            INTEGER NOT NULL DEFAULT 3868,
    transport       TEXT NOT NULL DEFAULT 'tcp' CHECK (transport IN ('tcp','sctp')),
    application     TEXT NOT NULL DEFAULT 'sgd',
    applications    TEXT NOT NULL DEFAULT '["sgd"]',
    enabled         INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sf_policies (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    max_retries     INTEGER NOT NULL DEFAULT 8,
    retry_schedule  TEXT NOT NULL DEFAULT '[30,300,1800,3600,3600,3600,3600,3600]',
    max_ttl         TEXT NOT NULL DEFAULT '48h',
    vp_override     TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS routing_rules (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    priority         INTEGER NOT NULL,
    match_src_iface  TEXT,
    match_src_peer   TEXT,
    match_dst_prefix TEXT,
    match_msisdn_min TEXT,
    match_msisdn_max TEXT,
    egress_iface     TEXT NOT NULL CHECK (egress_iface IN ('sip3gpp','sipsimple','smpp')),
    egress_peer   TEXT,
    sf_policy_id     TEXT REFERENCES sf_policies(id),
    enabled          INTEGER NOT NULL DEFAULT 1,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ims_registrations (
    id           TEXT PRIMARY KEY,
    msisdn       TEXT NOT NULL UNIQUE,
    imsi         TEXT,
    sip_aor      TEXT NOT NULL,
    contact_uri  TEXT NOT NULL,
    s_cscf       TEXT NOT NULL,
    registered   INTEGER NOT NULL DEFAULT 1,
    expiry       TEXT NOT NULL,
    updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS subscribers (
    id              TEXT PRIMARY KEY,
    msisdn          TEXT NOT NULL UNIQUE,
    imsi            TEXT,
    ims_registered  INTEGER NOT NULL DEFAULT 0,
    lte_attached    INTEGER NOT NULL DEFAULT 0,
    mme_number      TEXT,
    mme_host        TEXT,
    mwd_set         INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS messages (
    id               TEXT PRIMARY KEY,
    tp_mr            INTEGER,
    smpp_msgid       TEXT,
    origin_iface     TEXT NOT NULL,
    origin_peer      TEXT,
    egress_iface     TEXT,
    egress_peer      TEXT,
    route_cursor     INTEGER NOT NULL DEFAULT 0,
    src_msisdn       TEXT,
    dst_msisdn       TEXT,
    alert_correlation_id    TEXT,
    deferred_reason         TEXT,
    deferred_interface      TEXT,
    serving_node_at_deferral TEXT,
    payload          BLOB,
    udh              BLOB,
    encoding         INTEGER,
    dcs              INTEGER,
    status           TEXT NOT NULL DEFAULT 'QUEUED'
                         CHECK (status IN ('QUEUED','DISPATCHED','WAIT_TIMER','WAIT_EVENT','WAIT_TIMER_EVENT','DELIVERED','FAILED','EXPIRED')),
    retry_count      INTEGER NOT NULL DEFAULT 0,
    next_retry_at    TEXT,
    dr_required      INTEGER NOT NULL DEFAULT 0,
    submitted_at     TEXT NOT NULL DEFAULT (datetime('now')),
    expiry_at        TEXT,
    delivered_at     TEXT,
    kind             TEXT NOT NULL DEFAULT 'sm',
    ref_message_id   TEXT,
    raw_tpdu         BLOB
);

CREATE TABLE IF NOT EXISTS message_segments (
    id           TEXT PRIMARY KEY,
    src_msisdn   TEXT NOT NULL,
    concat_ref   INTEGER NOT NULL,
    total_segs   INTEGER NOT NULL,
    segment_num  INTEGER NOT NULL,
    payload      BLOB NOT NULL,
    origin_iface TEXT NOT NULL,
    origin_peer  TEXT,
    received_at  TEXT NOT NULL DEFAULT (datetime('now')),
    expiry_at    TEXT NOT NULL,
    UNIQUE (src_msisdn, concat_ref, segment_num)
);

CREATE TABLE IF NOT EXISTS delivery_reports (
    id            TEXT PRIMARY KEY,
    message_id    TEXT NOT NULL REFERENCES messages(id),
    status        TEXT NOT NULL CHECK (status IN ('DELIVRD','FAILED','EXPIRED','UNDELIV')),
    egress_iface  TEXT NOT NULL,
    raw_receipt   TEXT,
    reported_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_messages_status          ON messages (status);
CREATE INDEX IF NOT EXISTS idx_messages_dst_msisdn      ON messages (dst_msisdn);
CREATE INDEX IF NOT EXISTS idx_messages_alert_corr      ON messages (alert_correlation_id);
CREATE INDEX IF NOT EXISTS idx_messages_next_retry_at   ON messages (next_retry_at);
CREATE INDEX IF NOT EXISTS idx_messages_expiry_at       ON messages (expiry_at);
CREATE INDEX IF NOT EXISTS idx_ims_registrations_msisdn ON ims_registrations (msisdn);
CREATE INDEX IF NOT EXISTS idx_subscribers_msisdn       ON subscribers (msisdn);

-- Auto-update updated_at on IMS registrations
CREATE TRIGGER IF NOT EXISTS ims_registrations_updated_at
    AFTER UPDATE ON ims_registrations
    FOR EACH ROW
BEGIN
    UPDATE ims_registrations SET updated_at = datetime('now') WHERE id = NEW.id;
END;

-- Auto-update updated_at on subscribers
CREATE TRIGGER IF NOT EXISTS subscribers_updated_at
    AFTER UPDATE ON subscribers
    FOR EACH ROW
BEGIN
    UPDATE subscribers SET updated_at = datetime('now') WHERE id = NEW.id;
END;
