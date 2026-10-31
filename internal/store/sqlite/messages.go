package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

func (db *DB) SaveMessage(ctx context.Context, msg store.Message) error {
	const q = `
			INSERT INTO messages (
				id, tp_mr, smpp_msgid, origin_iface, origin_peer,
				egress_iface, egress_peer, route_cursor, src_msisdn, dst_msisdn,
				alert_correlation_id, deferred_reason, deferred_interface, serving_node_at_deferral,
				payload, udh, encoding, dcs, status, retry_count,
				next_retry_at, dr_required, submitted_at, expiry_at,
				kind, ref_message_id, raw_tpdu, smpp_registered_delivery
			) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	_, err := db.db.ExecContext(ctx, q,
		msg.ID, msg.TPMR, msg.SMPPMsgID, msg.OriginIface, msg.OriginPeer,
		msg.EgressIface, msg.EgressPeer, msg.RouteCursor, msg.SrcMSISDN, msg.DstMSISDN,
		msg.AlertCorrelationID, msg.DeferredReason, msg.DeferredInterface, msg.ServingNodeAtDeferral,
		msg.Payload, msg.UDH, msg.Encoding, msg.DCS, msg.Status, msg.RetryCount,
		nullableTime(msg.NextRetryAt),
		boolInt(msg.DRRequired),
		msg.SubmittedAt.UTC().Format(time.RFC3339),
		nullableTime(msg.ExpiryAt),
		store.NormalizeMessageKind(msg.Kind),
		nullableString(msg.RefMessageID),
		msg.RawTPDU,
		msg.SMPPRegisteredDelivery,
	)
	if err != nil {
		return fmt.Errorf("save message %s: %w", msg.ID, err)
	}
	return nil
}

func (db *DB) UpdateMessageRouting(ctx context.Context, id, egressIface, egressPeer string) error {
	_, err := db.db.ExecContext(ctx,
		`UPDATE messages SET egress_iface=?, egress_peer=? WHERE id=?`,
		egressIface, egressPeer, id)
	if err != nil {
		return fmt.Errorf("update message routing %s: %w", id, err)
	}
	return nil
}

func (db *DB) UpdateMessageDeferred(ctx context.Context, id, deferredReason, deferredInterface, servingNodeAtDeferral string, routeCursor int) error {
	_, err := db.db.ExecContext(ctx,
		`UPDATE messages
		   SET deferred_reason=?, deferred_interface=?, serving_node_at_deferral=?, route_cursor=?
		 WHERE id=?`,
		deferredReason, deferredInterface, servingNodeAtDeferral, routeCursor, id)
	if err != nil {
		return fmt.Errorf("update message deferred %s: %w", id, err)
	}
	return nil
}

func (db *DB) UpdateMessageStatus(ctx context.Context, id, status string) error {
	var err error
	if status == store.MessageStatusDelivered {
		now := time.Now().UTC().Format(time.RFC3339)
		_, err = db.db.ExecContext(ctx,
			`UPDATE messages SET status=?, delivered_at=? WHERE id=?`,
			status, now, id)
	} else {
		_, err = db.db.ExecContext(ctx,
			`UPDATE messages SET status=? WHERE id=?`, status, id)
	}
	if err != nil {
		return fmt.Errorf("update message status %s: %w", id, err)
	}
	return nil
}

func (db *DB) ClaimMessageForDispatch(ctx context.Context, id string, allowedStatuses []string) (bool, error) {
	if len(allowedStatuses) == 0 {
		return false, nil
	}
	args := make([]any, 0, len(allowedStatuses)+3)
	args = append(args, store.MessageStatusDispatched, id)
	placeholders := make([]string, 0, len(allowedStatuses))
	for _, status := range allowedStatuses {
		placeholders = append(placeholders, "?")
		args = append(args, status)
	}
	q := `UPDATE messages SET status=? WHERE id=? AND status IN (` + strings.Join(placeholders, ",") + `)`
	res, err := db.db.ExecContext(ctx, q, args...)
	if err != nil {
		return false, fmt.Errorf("claim message for dispatch %s: %w", id, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim message for dispatch %s: rows affected: %w", id, err)
	}
	return rows > 0, nil
}

func (db *DB) UpdateMessageRetry(ctx context.Context, id string, retryCount int, nextRetryAt time.Time, routeCursor int) error {
	_, err := db.db.ExecContext(ctx,
		`UPDATE messages SET status=?, retry_count=?, next_retry_at=?, route_cursor=? WHERE id=?`,
		store.MessageStatusWaitTimer, retryCount,
		nextRetryAt.UTC().Format(time.RFC3339), routeCursor, id)
	if err != nil {
		return fmt.Errorf("update message retry %s: %w", id, err)
	}
	return nil
}

func (db *DB) UpdateMessageExpiryCap(ctx context.Context, id string, expiryAt time.Time) error {
	_, err := db.db.ExecContext(ctx,
		`UPDATE messages
		    SET expiry_at = CASE
		        WHEN expiry_at IS NULL OR expiry_at > ? THEN ?
		        ELSE expiry_at
		    END
		  WHERE id=?`,
		expiryAt.UTC().Format(time.RFC3339),
		expiryAt.UTC().Format(time.RFC3339),
		id,
	)
	if err != nil {
		return fmt.Errorf("update message expiry cap %s: %w", id, err)
	}
	return nil
}

func (db *DB) RequeueMessageForAlert(ctx context.Context, id string, nextRetryAt time.Time, routeCursor int, deferredReason string, allowedStatuses []string) (bool, error) {
	if len(allowedStatuses) == 0 {
		return false, nil
	}
	args := make([]any, 0, len(allowedStatuses)+7)
	args = append(args,
		store.MessageStatusWaitTimer,
		nextRetryAt.UTC().Format(time.RFC3339),
		routeCursor,
		deferredReason,
		deferredReason,
		id,
	)
	placeholders := make([]string, 0, len(allowedStatuses))
	for _, status := range allowedStatuses {
		placeholders = append(placeholders, "?")
		args = append(args, status)
	}
	q := `UPDATE messages
		SET status=?, next_retry_at=?, route_cursor=?,
		    deferred_reason=CASE WHEN ? <> '' THEN ? ELSE deferred_reason END
		WHERE id=? AND status IN (` + strings.Join(placeholders, ",") + `)`
	res, err := db.db.ExecContext(ctx, q, args...)
	if err != nil {
		return false, fmt.Errorf("requeue message for alert %s: %w", id, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("requeue message for alert %s: rows affected: %w", id, err)
	}
	return rows > 0, nil
}

// Timestamps are stored as RFC3339 ("2006-01-02T15:04:05Z"), which does not
// compare correctly as text against datetime('now') ("2006-01-02 15:04:05"),
// so due-time checks compare julianday() values instead.
func (db *DB) ListRetryableMessages(ctx context.Context) ([]store.Message, error) {
	const q = `
			SELECT id, tp_mr, smpp_msgid, origin_iface, origin_peer,
			       egress_iface, egress_peer, route_cursor, src_msisdn, dst_msisdn,
			       alert_correlation_id, deferred_reason, deferred_interface, serving_node_at_deferral,
			       payload, udh, encoding, dcs, status, retry_count, next_retry_at,
			       dr_required, submitted_at, expiry_at,
			       kind, ref_message_id, raw_tpdu, delivered_at, smpp_registered_delivery
		FROM messages
		WHERE status IN ('QUEUED','WAIT_TIMER','WAIT_TIMER_EVENT')
		  AND (next_retry_at IS NULL OR julianday(next_retry_at) <= julianday('now'))
		ORDER BY next_retry_at ASC LIMIT 100`
	return sqliteScanMessages(db, ctx, q)
}

func (db *DB) ListExpiredMessages(ctx context.Context) ([]store.Message, error) {
	const q = `
			SELECT id, tp_mr, smpp_msgid, origin_iface, origin_peer,
			       egress_iface, egress_peer, route_cursor, src_msisdn, dst_msisdn,
			       alert_correlation_id, deferred_reason, deferred_interface, serving_node_at_deferral,
			       payload, udh, encoding, dcs, status, retry_count, next_retry_at,
			       dr_required, submitted_at, expiry_at,
			       kind, ref_message_id, raw_tpdu, delivered_at, smpp_registered_delivery
		FROM messages
		WHERE status IN ('QUEUED','WAIT_TIMER','WAIT_EVENT','WAIT_TIMER_EVENT')
		  AND expiry_at IS NOT NULL AND julianday(expiry_at) <= julianday('now')
		LIMIT 100`
	return sqliteScanMessages(db, ctx, q)
}

func (db *DB) GetMessage(ctx context.Context, id string) (*store.Message, error) {
	const q = `
			SELECT id, tp_mr, smpp_msgid, origin_iface, origin_peer,
			       egress_iface, egress_peer, route_cursor, src_msisdn, dst_msisdn,
			       alert_correlation_id, deferred_reason, deferred_interface, serving_node_at_deferral,
			       payload, udh, encoding, dcs, status, retry_count, next_retry_at,
		       dr_required, submitted_at, expiry_at,
			       kind, ref_message_id, raw_tpdu, delivered_at, smpp_registered_delivery
		FROM messages WHERE id=?`
	msgs, err := sqliteScanMessages(db, ctx, q, id)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	return &msgs[0], nil
}

func (db *DB) DeleteMessage(ctx context.Context, id string) error {
	_, err := db.db.ExecContext(ctx, `DELETE FROM messages WHERE id=?`, id)
	if err != nil {
		return fmt.Errorf("delete message %s: %w", id, err)
	}
	return nil
}

func sqliteScanMessages(db *DB, ctx context.Context, q string, args ...any) ([]store.Message, error) {
	rows, err := db.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()

	var msgs []store.Message
	for rows.Next() {
		var m store.Message
		var drInt int
		var nextRetryStr, submittedStr, expiryStr, deliveredStr *string
		var tpmr *int
		var encoding *int
		var dcs *int
		var kind, refMessageID *string

		err := rows.Scan(
			&m.ID, &tpmr, &m.SMPPMsgID, &m.OriginIface, &m.OriginPeer,
			&m.EgressIface, &m.EgressPeer, &m.RouteCursor, &m.SrcMSISDN, &m.DstMSISDN,
			&m.AlertCorrelationID, &m.DeferredReason, &m.DeferredInterface, &m.ServingNodeAtDeferral,
			&m.Payload, &m.UDH, &encoding, &dcs, &m.Status,
			&m.RetryCount, &nextRetryStr,
			&drInt, &submittedStr, &expiryStr,
			&kind, &refMessageID, &m.RawTPDU, &deliveredStr, &m.SMPPRegisteredDelivery,
		)
		if err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		m.TPMR = tpmr
		if encoding != nil {
			m.Encoding = *encoding
		}
		if dcs != nil {
			m.DCS = *dcs
		}
		m.DRRequired = drInt != 0
		m.Kind = store.MessageKindSM
		if kind != nil {
			m.Kind = store.NormalizeMessageKind(*kind)
		}
		if refMessageID != nil {
			m.RefMessageID = *refMessageID
		}
		if submittedStr != nil {
			m.SubmittedAt, _ = time.Parse(time.RFC3339, *submittedStr)
		}
		if nextRetryStr != nil {
			t, _ := time.Parse(time.RFC3339, *nextRetryStr)
			m.NextRetryAt = &t
		}
		if expiryStr != nil {
			t, _ := time.Parse(time.RFC3339, *expiryStr)
			m.ExpiryAt = &t
		}
		if deliveredStr != nil {
			if t, err := time.Parse(time.RFC3339, *deliveredStr); err == nil {
				m.DeliveredAt = &t
			}
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

func (db *DB) CountMessagesByStatus(ctx context.Context) (map[string]int64, error) {
	rows, err := db.db.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM messages GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count messages by status: %w", err)
	}
	defer rows.Close()
	counts := make(map[string]int64)
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		counts[status] = n
	}
	return counts, rows.Err()
}

func (db *DB) ListMessages(ctx context.Context, limit int) ([]store.Message, error) {
	if limit <= 0 {
		limit = 100
	}
	const q = `
			SELECT id, tp_mr, smpp_msgid, origin_iface, origin_peer,
			       egress_iface, egress_peer, route_cursor, src_msisdn, dst_msisdn,
			       alert_correlation_id, deferred_reason, deferred_interface, serving_node_at_deferral,
			       payload, udh, encoding, dcs, status, retry_count, next_retry_at,
			       dr_required, submitted_at, expiry_at,
			       kind, ref_message_id, raw_tpdu, delivered_at, smpp_registered_delivery
	FROM messages ORDER BY submitted_at DESC LIMIT ?`
	return sqliteScanMessages(db, ctx, q, limit)
}

func (db *DB) ListFilteredMessages(ctx context.Context, filter store.MessageFilter) ([]store.Message, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}

	conds := make([]string, 0, 4)
	args := make([]any, 0, 8)

	if len(filter.Statuses) > 0 {
		placeholders := make([]string, 0, len(filter.Statuses))
		for _, status := range filter.Statuses {
			if status == "" {
				continue
			}
			placeholders = append(placeholders, "?")
			args = append(args, status)
		}
		if len(placeholders) > 0 {
			conds = append(conds, "status IN ("+strings.Join(placeholders, ",")+")")
		}
	}
	if filter.SrcMSISDN != "" {
		conds = append(conds, "src_msisdn LIKE ?")
		args = append(args, "%"+filter.SrcMSISDN+"%")
	}
	if filter.DstMSISDN != "" {
		conds = append(conds, "dst_msisdn LIKE ?")
		args = append(args, "%"+filter.DstMSISDN+"%")
	}
	if filter.OriginPeer != "" {
		conds = append(conds, "origin_peer LIKE ?")
		args = append(args, "%"+filter.OriginPeer+"%")
	}
	if filter.AlertCorrelationID != "" {
		conds = append(conds, "alert_correlation_id = ?")
		args = append(args, filter.AlertCorrelationID)
	}
	if filter.DeferredInterface != "" {
		conds = append(conds, "deferred_interface = ?")
		args = append(args, filter.DeferredInterface)
	}

	q := `
			SELECT id, tp_mr, smpp_msgid, origin_iface, origin_peer,
			       egress_iface, egress_peer, route_cursor, src_msisdn, dst_msisdn,
			       alert_correlation_id, deferred_reason, deferred_interface, serving_node_at_deferral,
			       payload, udh, encoding, dcs, status, retry_count, next_retry_at,
			       dr_required, submitted_at, expiry_at,
			       kind, ref_message_id, raw_tpdu, delivered_at, smpp_registered_delivery
		FROM messages`
	if len(conds) > 0 {
		q += "\n\t\tWHERE " + strings.Join(conds, " AND ")
	}
	q += "\n\t\tORDER BY COALESCE(next_retry_at, submitted_at) ASC, submitted_at DESC LIMIT ?"
	args = append(args, limit)
	return sqliteScanMessages(db, ctx, q, args...)
}

func (db *DB) ListDeliveryReports(ctx context.Context, limit int) ([]store.DeliveryReport, error) {
	if limit <= 0 {
		limit = 100
	}
	const q = `
		SELECT id, message_id, status, egress_iface, COALESCE(raw_receipt,''), reported_at
		FROM delivery_reports ORDER BY reported_at DESC LIMIT ?`
	return sqliteScanDeliveryReports(db, ctx, q, limit)
}

func (db *DB) GetDeliveryReport(ctx context.Context, id string) (*store.DeliveryReport, error) {
	const q = `
		SELECT id, message_id, status, egress_iface, COALESCE(raw_receipt,''), reported_at
		FROM delivery_reports WHERE id = ?`
	drs, err := sqliteScanDeliveryReports(db, ctx, q, id)
	if err != nil {
		return nil, err
	}
	if len(drs) == 0 {
		return nil, nil
	}
	return &drs[0], nil
}

func sqliteScanDeliveryReports(db *DB, ctx context.Context, q string, args ...any) ([]store.DeliveryReport, error) {
	rows, err := db.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query delivery_reports: %w", err)
	}
	defer rows.Close()
	var drs []store.DeliveryReport
	for rows.Next() {
		var dr store.DeliveryReport
		var reportedStr string
		if err := rows.Scan(&dr.ID, &dr.MessageID, &dr.Status, &dr.EgressIface, &dr.RawReceipt, &reportedStr); err != nil {
			return nil, fmt.Errorf("scan delivery_report: %w", err)
		}
		dr.ReportedAt, _ = time.Parse(time.RFC3339, reportedStr)
		drs = append(drs, dr)
	}
	return drs, rows.Err()
}

func (db *DB) SaveDeliveryReport(ctx context.Context, dr store.DeliveryReport) error {
	const q = `
		INSERT INTO delivery_reports (id, message_id, status, egress_iface, raw_receipt, reported_at)
		VALUES (?,?,?,?,?,?)`
	_, err := db.db.ExecContext(ctx, q,
		dr.ID, dr.MessageID, dr.Status, dr.EgressIface, dr.RawReceipt,
		dr.ReportedAt.UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("save delivery_report: %w", err)
	}
	return nil
}

// nullableString stores an empty string as NULL.
func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// nullableTime converts a *time.Time to a string for SQLite, or nil.
func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}
