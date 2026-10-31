CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS smpp_server_accounts (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    system_id        TEXT NOT NULL UNIQUE,
    password_hash    TEXT NOT NULL,
    allowed_ip       INET,
    bind_type        TEXT NOT NULL CHECK (bind_type IN ('transmitter','receiver','transceiver')),
    throughput_limit INT NOT NULL DEFAULT 0,
    default_route_id UUID,
    enabled          BOOLEAN NOT NULL DEFAULT true,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS smpp_clients (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT NOT NULL UNIQUE,
    host                TEXT NOT NULL,
    port                INT NOT NULL,
    transport           TEXT NOT NULL DEFAULT 'tcp' CHECK (transport IN ('tcp','tls')),
    verify_server_cert  BOOLEAN NOT NULL DEFAULT false,
    system_id           TEXT NOT NULL,
    password            TEXT NOT NULL,
    bind_type           TEXT NOT NULL CHECK (bind_type IN ('transmitter','receiver','transceiver')),
    reconnect_interval  INTERVAL NOT NULL DEFAULT '10s',
    throughput_limit    INT NOT NULL DEFAULT 0,
    source_addr_ton     INT,
    source_addr_npi     INT,
    dest_addr_ton       INT,
    dest_addr_npi       INT,
    enabled             BOOLEAN NOT NULL DEFAULT true,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sip_peers (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,
    address     TEXT NOT NULL,
    port        INT NOT NULL DEFAULT 5060,
    transport   TEXT NOT NULL DEFAULT 'udp' CHECK (transport IN ('udp','tcp','tls')),
    domain      TEXT NOT NULL,
    auth_user   TEXT,
    auth_pass   TEXT,
    enabled     BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS diameter_peers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE,
    host            TEXT NOT NULL,
    realm           TEXT NOT NULL,
    port            INT NOT NULL DEFAULT 3868,
    transport       TEXT NOT NULL DEFAULT 'tcp' CHECK (transport IN ('tcp','sctp')),
    application     TEXT NOT NULL CHECK (application IN ('sgd','sh','s6c')),
    applications    TEXT NOT NULL DEFAULT '["sgd"]',
    enabled         BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sf_policies (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE,
    max_retries     INT NOT NULL DEFAULT 8,
    retry_schedule  JSONB NOT NULL DEFAULT '[30,300,1800,3600,3600,3600,3600,3600]',
    max_ttl         INTERVAL NOT NULL DEFAULT '48 hours',
    vp_override     INTERVAL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS routing_rules (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name             TEXT NOT NULL,
    priority         INT NOT NULL,
    match_src_iface  TEXT,
    match_src_peer   TEXT,
    match_dst_prefix TEXT,
    match_msisdn_min TEXT,
    match_msisdn_max TEXT,
    egress_iface     TEXT NOT NULL CHECK (egress_iface IN ('sip3gpp','sipsimple','smpp')),
    egress_peer      TEXT,
    sf_policy_id     UUID REFERENCES sf_policies(id),
    enabled          BOOLEAN NOT NULL DEFAULT true,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ims_registrations (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    msisdn       TEXT NOT NULL UNIQUE,
    sip_aor      TEXT NOT NULL,
    contact_uri  TEXT NOT NULL,
    s_cscf       TEXT NOT NULL,
    registered   BOOLEAN NOT NULL DEFAULT true,
    expiry       TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS subscribers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    msisdn          TEXT NOT NULL UNIQUE,
    imsi            TEXT,
    ims_registered  BOOLEAN NOT NULL DEFAULT false,
    lte_attached    BOOLEAN NOT NULL DEFAULT false,
    mme_number      TEXT,
    mme_host        TEXT,
    mwd_set         BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS messages (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tp_mr            INT,
    smpp_msgid       TEXT,
    origin_iface     TEXT NOT NULL,
    origin_peer      TEXT,
    egress_iface     TEXT,
    egress_peer      TEXT,
    route_cursor     INT NOT NULL DEFAULT 0,
    src_msisdn       TEXT,
    dst_msisdn       TEXT,
    alert_correlation_id    TEXT,
    deferred_reason         TEXT,
    deferred_interface      TEXT,
    serving_node_at_deferral TEXT,
    payload          BYTEA,
    udh              BYTEA,
    encoding         SMALLINT,
    dcs              SMALLINT,
    status           TEXT NOT NULL DEFAULT 'QUEUED'
                         CHECK (status IN ('QUEUED','DISPATCHED','WAIT_TIMER','WAIT_EVENT','WAIT_TIMER_EVENT','DELIVERED','FAILED','EXPIRED')),
    retry_count      INT NOT NULL DEFAULT 0,
    next_retry_at    TIMESTAMPTZ,
    dr_required      BOOLEAN NOT NULL DEFAULT false,
    submitted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expiry_at        TIMESTAMPTZ,
    delivered_at     TIMESTAMPTZ,
    kind             TEXT NOT NULL DEFAULT 'sm',
    ref_message_id   UUID,
    raw_tpdu         BYTEA,
    smpp_registered_delivery SMALLINT
);

CREATE TABLE IF NOT EXISTS message_segments (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    src_msisdn   TEXT NOT NULL,
    concat_ref   INT NOT NULL,
    total_segs   INT NOT NULL,
    segment_num  INT NOT NULL,
    payload      BYTEA NOT NULL,
    origin_iface TEXT NOT NULL,
    origin_peer  TEXT,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expiry_at    TIMESTAMPTZ NOT NULL,
    UNIQUE (src_msisdn, concat_ref, segment_num)
);

CREATE TABLE IF NOT EXISTS delivery_reports (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id    UUID NOT NULL REFERENCES messages(id),
    status        TEXT NOT NULL CHECK (status IN ('DELIVRD','FAILED','EXPIRED','UNDELIV')),
    egress_iface  TEXT NOT NULL,
    raw_receipt   TEXT,
    reported_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_default_route'
    ) THEN
        ALTER TABLE smpp_server_accounts
            ADD CONSTRAINT fk_default_route FOREIGN KEY (default_route_id) REFERENCES routing_rules(id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_messages_status ON messages (status);
CREATE INDEX IF NOT EXISTS idx_messages_dst_msisdn ON messages (dst_msisdn);
CREATE INDEX IF NOT EXISTS idx_messages_next_retry_at ON messages (next_retry_at) WHERE status IN ('QUEUED','WAIT_TIMER','WAIT_TIMER_EVENT');
CREATE INDEX IF NOT EXISTS idx_messages_expiry_at ON messages (expiry_at) WHERE status IN ('QUEUED','WAIT_TIMER','WAIT_EVENT','WAIT_TIMER_EVENT');
CREATE INDEX IF NOT EXISTS idx_ims_registrations_msisdn ON ims_registrations (msisdn);
CREATE INDEX IF NOT EXISTS idx_subscribers_msisdn ON subscribers (msisdn);

CREATE OR REPLACE FUNCTION notify_change() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(TG_TABLE_NAME || '_changed', TG_OP);
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS smpp_server_accounts_notify ON smpp_server_accounts;
CREATE TRIGGER smpp_server_accounts_notify
    AFTER INSERT OR UPDATE OR DELETE ON smpp_server_accounts
    FOR EACH ROW EXECUTE FUNCTION notify_change();

DROP TRIGGER IF EXISTS smpp_clients_notify ON smpp_clients;
CREATE TRIGGER smpp_clients_notify
    AFTER INSERT OR UPDATE OR DELETE ON smpp_clients
    FOR EACH ROW EXECUTE FUNCTION notify_change();

DROP TRIGGER IF EXISTS sip_peers_notify ON sip_peers;
CREATE TRIGGER sip_peers_notify
    AFTER INSERT OR UPDATE OR DELETE ON sip_peers
    FOR EACH ROW EXECUTE FUNCTION notify_change();

DROP TRIGGER IF EXISTS diameter_peers_notify ON diameter_peers;
CREATE TRIGGER diameter_peers_notify
    AFTER INSERT OR UPDATE OR DELETE ON diameter_peers
    FOR EACH ROW EXECUTE FUNCTION notify_change();

DROP TRIGGER IF EXISTS routing_rules_notify ON routing_rules;
CREATE TRIGGER routing_rules_notify
    AFTER INSERT OR UPDATE OR DELETE ON routing_rules
    FOR EACH ROW EXECUTE FUNCTION notify_change();

DROP TRIGGER IF EXISTS sf_policies_notify ON sf_policies;
CREATE TRIGGER sf_policies_notify
    AFTER INSERT OR UPDATE OR DELETE ON sf_policies
    FOR EACH ROW EXECUTE FUNCTION notify_change();

DROP TRIGGER IF EXISTS ims_registrations_notify ON ims_registrations;
CREATE TRIGGER ims_registrations_notify
    AFTER INSERT OR UPDATE OR DELETE ON ims_registrations
    FOR EACH ROW EXECUTE FUNCTION notify_change();

CREATE TABLE IF NOT EXISTS sgd_mme_mappings (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    s6c_result    TEXT NOT NULL UNIQUE,
    sgd_host      TEXT NOT NULL,
    enabled       BOOLEAN NOT NULL DEFAULT true,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS sgd_mme_mappings_notify ON sgd_mme_mappings;
CREATE TRIGGER sgd_mme_mappings_notify
    AFTER INSERT OR UPDATE OR DELETE ON sgd_mme_mappings
    FOR EACH ROW EXECUTE FUNCTION notify_change();
