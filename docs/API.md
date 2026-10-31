# API Reference

Base URL:

- `http://<host>:8080`

Primary API namespace:

- `/api/v1`

OpenAPI is the source of truth for schemas:

- `GET /api/v1/openapi.json`
- `GET /api/v1/docs`
- `GET /api/v1/schemas/...`

## Operational Endpoints

### Status

- `GET /api/v1/status`
- `GET /api/v1/status/peers`

```bash
curl -s http://localhost:8080/api/v1/status | jq
curl -s http://localhost:8080/api/v1/status/peers | jq
```

Example `status` response:

```json
{
  "version": "0.4.0b",
  "uptime": "2m34s",
  "uptime_sec": 154.1,
  "started_at": "2026-04-23T10:00:00Z",
  "message_counts": {
    "queued": 1,
    "dispatched": 0,
    "delivered": 12,
    "failed": 1,
    "expired": 0
  }
}
```

Example `status/peers` response:

```json
[
  {
    "name": "msc0",
    "type": "smpp_client",
    "state": "BOUND",
    "transport": "tcp",
    "system_id": "smsc",
    "bind_type": "transceiver",
    "remote_addr": "10.90.250.42:2775",
    "connected_at": "2026-04-23T10:01:00Z"
  },
  {
    "name": "3342012832",
    "type": "sip_ims",
    "state": "REGISTERED",
    "system_id": "sip:+3342012832@example.org",
    "remote_addr": "sip:ue@10.0.0.44:5060",
    "application": "scscf1.example.org",
    "connected_at": "2026-04-23T10:02:00Z",
    "expiry_at": "2026-04-23T10:32:00Z"
  }
]
```

Peer `type` values currently emitted:

- `smpp_server`
- `smpp_client`
- `diameter_peer`
- `sip_ims`

### Metrics, Health, and UI

- `GET /metrics`
- `GET /health`
- `GET /ui/`

```bash
curl -s http://localhost:8080/metrics
curl -s http://localhost:8080/health
```

Example metric lines:

```text
smsc_messages_in_total{interface="smpp"} 12
smsc_messages_out_total{interface="sip3gpp"} 10
smsc_store_forward_queued 2
smsc_store_forward_retried_total 4
smsc_diameter_peers_connected 1
```

If the UI bundle was not built before the binary was compiled, `/ui/` serves a placeholder page instead of the React SPA.

## Configuration Resources

### SMPP Server Accounts

Endpoints:

- `GET /api/v1/smpp/server/accounts`
- `GET /api/v1/smpp/server/accounts/{id}`
- `POST /api/v1/smpp/server/accounts`
- `PUT /api/v1/smpp/server/accounts/{id}`
- `DELETE /api/v1/smpp/server/accounts/{id}`

Create/update fields:

- `name`
- `system_id`
- `password`
- `allowed_ip`
- `bind_type`: `transmitter | receiver | transceiver`
- `throughput_limit`
- `enabled`

Notes:

- plaintext `password` is hashed on save
- password hashes are never returned by the API
- delete returns `409` if the account is still referenced by a routing rule

Example create:

```bash
curl -s -X POST http://localhost:8080/api/v1/smpp/server/accounts \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "test-account",
    "system_id": "test",
    "password": "secret",
    "allowed_ip": "192.168.105.20",
    "bind_type": "transceiver",
    "throughput_limit": 0,
    "enabled": true
  }'
```

### SMPP Outbound Clients

Endpoints:

- `GET /api/v1/smpp/clients`
- `GET /api/v1/smpp/clients/{id}`
- `POST /api/v1/smpp/clients`
- `PUT /api/v1/smpp/clients/{id}`
- `DELETE /api/v1/smpp/clients/{id}`

Create/update fields:

- `name`
- `host`
- `port`
- `transport`: `tcp | tls`
- `verify_server_cert`
- `system_id`
- `password`
- `bind_type`: `transmitter | receiver | transceiver`
- `reconnect_interval`
- `throughput_limit`
- `enabled`

Notes:

- outbound client passwords are not returned by the API
- `verify_server_cert` only matters when `transport` is `tls`
- TLS CA configuration comes from `smpp.outbound_client_tls.server_ca_file`
- delete returns `409` if the client is still referenced by a routing rule

Example response:

```json
[
  {
    "id": "7aefb5d0-3498-44d2-a373-8f2f86ff3de3",
    "name": "msc0",
    "host": "10.90.250.42",
    "port": 2775,
    "transport": "tls",
    "verify_server_cert": true,
    "system_id": "smsc",
    "bind_type": "transceiver",
    "reconnect_interval": "10s",
    "throughput_limit": 0,
    "enabled": true
  }
]
```

### SIP SIMPLE Peers

Endpoints:

- `GET /api/v1/sip/peers`
- `GET /api/v1/sip/peers/{id}`
- `POST /api/v1/sip/peers`
- `PUT /api/v1/sip/peers/{id}`
- `DELETE /api/v1/sip/peers/{id}`

Create/update fields:

- `name`
- `address`
- `port`
- `transport`: `udp | tcp | tls`
- `domain`
- `auth_user`
- `auth_pass`
- `enabled`

Notes:

- `auth_pass` is accepted on write but never returned on read
- delete returns `409` if the peer is still referenced by a routing rule

### Diameter Peers

Endpoints:

- `GET /api/v1/diameter/peers`
- `GET /api/v1/diameter/peers/{id}`
- `POST /api/v1/diameter/peers`
- `PUT /api/v1/diameter/peers/{id}`
- `DELETE /api/v1/diameter/peers/{id}`

Create/update fields:

- `name`
- `host`
- `realm`
- `port`
- `transport`: `tcp | sctp`
- `applications`: one or more of `sgd`, `sh`, `s6c`
- `enabled`

Notes:

- the runtime uses enabled `sh` and `s6c` peers for HSS-facing lookups
- enabled `sgd` peers are used for outbound SGd delivery
- delete returns `409` if the peer is still referenced by a routing rule

Example create:

```bash
curl -s -X POST http://localhost:8080/api/v1/diameter/peers \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "dra01.epc.mnc435.mcc311.3gppnetwork.org",
    "host": "10.90.250.35",
    "realm": "epc.mnc435.mcc311.3gppnetwork.org",
    "port": 3868,
    "transport": "sctp",
    "applications": ["sh", "sgd"],
    "enabled": true
  }'
```

### Routing Rules

Endpoints:

- `GET /api/v1/routing/rules`
- `GET /api/v1/routing/rules/{id}`
- `POST /api/v1/routing/rules`
- `PUT /api/v1/routing/rules/{id}`
- `DELETE /api/v1/routing/rules/{id}`

Create/update fields:

- `name`
- `priority`
- `match_src_iface`
- `match_src_peer`
- `match_dst_prefix`
- `match_msisdn_min`
- `match_msisdn_max`
- `egress_iface`: `smpp | sipsimple`
- `egress_peer`
- `sf_policy_id`
- `enabled`

Notes:

- routing rules are fallback-only
- `sgd` is built in and is no longer a valid routing-rule `egress_iface`
- legacy `sgd` rules are removed during store startup cleanup

### Store-And-Forward Policies

Endpoints:

- `GET /api/v1/routing/policies`
- `GET /api/v1/routing/policies/{id}`
- `POST /api/v1/routing/policies`
- `PUT /api/v1/routing/policies/{id}`
- `DELETE /api/v1/routing/policies/{id}`

Create/update fields:

- `name`
- `max_retries`
- `retry_schedule`: array of seconds
- `max_ttl`: Go duration string
- `vp_override`: Go duration string or empty

Notes:

- delete returns `409` if the policy is still referenced by a routing rule
- `vp_override` is persisted as an optional duration and may be `null` on reads

Example create:

```bash
curl -s -X POST http://localhost:8080/api/v1/routing/policies \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "default",
    "max_retries": 8,
    "retry_schedule": [30, 300, 1800, 3600],
    "max_ttl": "48h",
    "vp_override": ""
  }'
```

### Subscribers

Endpoints:

- `GET /api/v1/subscribers`
- `GET /api/v1/subscribers/{id}`
- `POST /api/v1/subscribers`
- `PUT /api/v1/subscribers/{id}`
- `DELETE /api/v1/subscribers/{id}`

Create/update fields:

- `msisdn`
- `imsi`
- `ims_registered`
- `lte_attached`
- `mme_number_for_mt_sms`
- `mme_host`
- `mwd_set`

Notes:

- `POST /api/v1/subscribers` is an upsert by subscriber identity

Example upsert:

```bash
curl -s -X POST http://localhost:8080/api/v1/subscribers \
  -H 'Content-Type: application/json' \
  -d '{
    "msisdn": "3342012832",
    "imsi": "311435123456789",
    "ims_registered": false,
    "lte_attached": true,
    "mme_number_for_mt_sms": "+15551230000",
    "mme_host": "mme01.epc.example.org",
    "mwd_set": false
  }'
```

### S6c To SGd MME Mappings

Endpoints:

- `GET /api/v1/sgd/mme-mappings`
- `GET /api/v1/sgd/mme-mappings/{id}`
- `POST /api/v1/sgd/mme-mappings`
- `PUT /api/v1/sgd/mme-mappings/{id}`
- `DELETE /api/v1/sgd/mme-mappings/{id}`

Fields:

- `s6c_result`: MME hostname returned by `S6c`
- `sgd_host`: hostname to use for outbound `SGd`
- `enabled`

These mappings are applied during built-in SGd route resolution.

## Message And Delivery Data

### Message Status Values

Current stored message statuses:

- `QUEUED`
- `DISPATCHED`
- `WAIT_TIMER`
- `WAIT_EVENT`
- `WAIT_TIMER_EVENT`
- `DELIVERED`
- `FAILED`
- `EXPIRED`

### Messages

Endpoints:

- `GET /api/v1/messages?limit=100`
- `GET /api/v1/messages/{id}`
- `GET /api/v1/messages/queue?limit=100&src_msisdn=...&dst_msisdn=...&origin_peer=...`
- `DELETE /api/v1/messages/queue/{id}`

Notes:

- `/api/v1/messages` returns recent messages across all states
- `/api/v1/messages/queue` returns only `QUEUED`, `DISPATCHED`, `WAIT_TIMER`, `WAIT_EVENT`, and `WAIT_TIMER_EVENT`
- queue deletion is only allowed for those queue-visible states

Representative message fields:

- `id`
- `tp_mr`
- `smpp_msg_id`
- `origin_iface`
- `origin_peer`
- `egress_iface`
- `egress_peer`
- `route_cursor`
- `src_msisdn`
- `dst_msisdn`
- `alert_correlation_id`
- `deferred_reason`
- `deferred_interface`
- `serving_node_at_deferral`
- `payload`
- `udh`
- `encoding`
- `dcs`
- `status`
- `retry_count`
- `next_retry_at`
- `dr_required`
- `submitted_at`
- `expiry_at`
- `delivered_at`
- `kind` — `sm` for ordinary messages, `status_report` for SMSC-generated SMS-STATUS-REPORTs
- `ref_message_id` — `status_report` only: the `id` of the message being reported on
- `raw_tpdu` — pre-encoded TP-DATA sent verbatim on egress (base64), when set

Example queue query:

```bash
curl -s "http://localhost:8080/api/v1/messages/queue?limit=20&dst_msisdn=3342012832" | jq
```

Example response:

```json
[
  {
    "id": "6ff75e5c-bd8e-468f-8c17-8396d6124e38",
    "origin_iface": "smpp",
    "origin_peer": "test",
    "egress_iface": "sgd",
    "egress_peer": "mme01.epc.example.org",
    "route_cursor": 2,
    "src_msisdn": "3342021234",
    "dst_msisdn": "3342012832",
    "alert_correlation_id": "c21zYzp...",
    "deferred_reason": "sgd_delivery",
    "deferred_interface": "sgd",
    "status": "WAIT_TIMER_EVENT",
    "retry_count": 1,
    "next_retry_at": "2026-04-23T10:30:00Z",
    "dr_required": true,
    "submitted_at": "2026-04-23T10:29:00Z",
    "expiry_at": "2026-04-30T10:29:00Z"
  }
]
```

### Delivery Reports

Endpoints:

- `GET /api/v1/delivery-reports?limit=100`
- `GET /api/v1/delivery-reports/{id}`

Example:

```bash
curl -s "http://localhost:8080/api/v1/delivery-reports?limit=20" | jq
```

Example response:

```json
[
  {
    "id": "747fcb2e-21f7-4739-a041-868c5b9ca50e",
    "message_id": "6ff75e5c-bd8e-468f-8c17-8396d6124e38",
    "status": "DELIVRD",
    "egress_iface": "sip3gpp",
    "raw_receipt": "id:6ff75e5c-bd8e-468f-8c17-8396d6124e38 stat:DELIVRD",
    "reported_at": "2026-04-23T10:29:01Z"
  }
]
```
