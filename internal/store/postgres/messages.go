package postgres

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
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,
				$25,$26,$27,$28
			)`
	_, err := db.pool.Exec(ctx, q,
		msg.ID, msg.TPMR, msg.SMPPMsgID, msg.OriginIface, msg.OriginPeer,
		msg.EgressIface, msg.EgressPeer, msg.RouteCursor, msg.SrcMSISDN, msg.DstMSISDN,
		msg.AlertCorrelationID, msg.DeferredReason, msg.DeferredInterface, msg.ServingNodeAtDeferral,
		msg.Payload, msg.UDH, msg.Encoding, msg.DCS, msg.Status,
		msg.RetryCount, msg.NextRetryAt, msg.DRRequired, msg.SubmittedAt,
		msg.ExpiryAt,
		store.NormalizeMessageKind(msg.Kind),
		nullableUUID(msg.RefMessageID),
		msg.RawTPDU,
		msg.SMPPRegisteredDelivery,
	)
	if err != nil {
		return fmt.Errorf("save message %s: %w", msg.ID, err)
	}
	return nil
}

func (db *DB) UpdateMessageRouting(ctx context.Context, id, egressIface, egressPeer string) error {
	_, err := db.pool.Exec(ctx,
		`UPDATE messages SET egress_iface=$2, egress_peer=$3 WHERE id=$1`,
		id, egressIface, egressPeer)
	if err != nil {
		return fmt.Errorf("update message routing %s: %w", id, err)
	}
	return nil
}

func (db *DB) UpdateMessageDeferred(ctx context.Context, id, deferredReason, deferredInterface, servingNodeAtDeferral string, routeCursor int) error {
	_, err := db.pool.Exec(ctx,
		`UPDATE messages
		    SET deferred_reason=$2, deferred_interface=$3, serving_node_at_deferral=$4, route_cursor=$5
		  WHERE id=$1`,
		id, deferredReason, deferredInterface, servingNodeAtDeferral, routeCursor)
	if err != nil {
		return fmt.Errorf("update message deferred %s: %w", id, err)
	}
	return nil
}

func (db *DB) UpdateMessageStatus(ctx context.Context, id, status string) error {
	var err error
	if status == store.MessageStatusDelivered {
		now := time.Now()
		_, err = db.pool.Exec(ctx,
			`UPDATE messages SET status=$2, delivered_at=$3 WHERE id=$1`,
			id, status, now)
	} else {
		_, err = db.pool.Exec(ctx,
			`UPDATE messages SET status=$2 WHERE id=$1`, id, status)
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
	args := make([]any, 0, len(allowedStatuses)+2)
	args = append(args, id, store.MessageStatusDispatched)
	placeholders := make([]string, 0, len(allowedStatuses))
	for i, status := range allowedStatuses {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+3))
		args = append(args, status)
	}
	q := `UPDATE messages SET status=$2 WHERE id=$1 AND status IN (` + strings.Join(placeholders, ",") + `)`
	tag, err := db.pool.Exec(ctx, q, args...)
	if err != nil {
		return false, fmt.Errorf("claim message for dispatch %s: %w", id, err)
	}
	return tag.RowsAffected() > 0, nil
}

func (db *DB) UpdateMessageRetry(ctx context.Context, id string, retryCount int, nextRetryAt time.Time, routeCursor int) error {
	_, err := db.pool.Exec(ctx,
		`UPDATE messages SET status=$2, retry_count=$3, next_retry_at=$4, route_cursor=$5 WHERE id=$1`,
		id, store.MessageStatusWaitTimer, retryCount, nextRetryAt, routeCursor)
	if err != nil {
		return fmt.Errorf("update message retry %s: %w", id, err)
	}
	return nil
}

func (db *DB) UpdateMessageExpiryCap(ctx context.Context, id string, expiryAt time.Time) error {
	_, err := db.pool.Exec(ctx,
		`UPDATE messages
		    SET expiry_at = CASE
		        WHEN expiry_at IS NULL OR expiry_at > $2 THEN $2
		        ELSE expiry_at
		    END
		  WHERE id=$1`,
		id, expiryAt)
	if err != nil {
		return fmt.Errorf("update message expiry cap %s: %w", id, err)
	}
	return nil
}

func (db *DB) RequeueMessageForAlert(ctx context.Context, id string, nextRetryAt time.Time, routeCursor int, deferredReason string, allowedStatuses []string) (bool, error) {
	if len(allowedStatuses) == 0 {
		return false, nil
	}
	args := make([]any, 0, len(allowedStatuses)+6)
	args = append(args, id, store.MessageStatusWaitTimer, nextRetryAt, routeCursor, deferredReason, deferredReason)
	placeholders := make([]string, 0, len(allowedStatuses))
	for i, status := range allowedStatuses {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+7))
		args = append(args, status)
	}
	q := `UPDATE messages
		SET status=$2, next_retry_at=$3, route_cursor=$4,
		    deferred_reason=CASE WHEN $5 <> '' THEN $6 ELSE deferred_reason END
		WHERE id=$1 AND status IN (` + strings.Join(placeholders, ",") + `)`
	tag, err := db.pool.Exec(ctx, q, args...)
	if err != nil {
		return false, fmt.Errorf("requeue message for alert %s: %w", id, err)
	}
	return tag.RowsAffected() > 0, nil
}

func (db *DB) ListRetryableMessages(ctx context.Context) ([]store.Message, error) {
	const q = `
			SELECT id, tp_mr, smpp_msgid, origin_iface, origin_peer,
			       egress_iface, egress_peer, route_cursor, src_msisdn, dst_msisdn,
			       alert_correlation_id, deferred_reason, deferred_interface, serving_node_at_deferral,
			       payload, udh, encoding, dcs, status, retry_count, next_retry_at,
			       dr_required, submitted_at, expiry_at,
			       kind, ref_message_id::text, raw_tpdu, delivered_at, smpp_registered_delivery
		FROM messages
		WHERE status IN ('QUEUED','WAIT_TIMER','WAIT_TIMER_EVENT')
		  AND (next_retry_at IS NULL OR next_retry_at <= now())
		ORDER BY next_retry_at ASC NULLS FIRST
		LIMIT 100`
	return scanMessages(db, ctx, q)
}

func (db *DB) ListExpiredMessages(ctx context.Context) ([]store.Message, error) {
	const q = `
			SELECT id, tp_mr, smpp_msgid, origin_iface, origin_peer,
			       egress_iface, egress_peer, route_cursor, src_msisdn, dst_msisdn,
			       alert_correlation_id, deferred_reason, deferred_interface, serving_node_at_deferral,
			       payload, udh, encoding, dcs, status, retry_count, next_retry_at,
			       dr_required, submitted_at, expiry_at,
			       kind, ref_message_id::text, raw_tpdu, delivered_at, smpp_registered_delivery
		FROM messages
		WHERE status IN ('QUEUED','WAIT_TIMER','WAIT_EVENT','WAIT_TIMER_EVENT')
		  AND expiry_at IS NOT NULL AND expiry_at <= now()
		LIMIT 100`
	return scanMessages(db, ctx, q)
}

func (db *DB) GetMessage(ctx context.Context, id string) (*store.Message, error) {
	const q = `
			SELECT id, tp_mr, smpp_msgid, origin_iface, origin_peer,
			       egress_iface, egress_peer, route_cursor, src_msisdn, dst_msisdn,
			       alert_correlation_id, deferred_reason, deferred_interface, serving_node_at_deferral,
			       payload, udh, encoding, dcs, status, retry_count, next_retry_at,
			       dr_required, submitted_at, expiry_at,
			       kind, ref_message_id::text, raw_tpdu, delivered_at, smpp_registered_delivery
		FROM messages WHERE id = $1`
	msgs, err := scanMessages(db, ctx, q, id)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	return &msgs[0], nil
}

func (db *DB) DeleteMessage(ctx context.Context, id string) error {
	_, err := db.pool.Exec(ctx, `DELETE FROM messages WHERE id = $1::uuid`, id)
	if err != nil {
		return fmt.Errorf("delete message %s: %w", id, err)
	}
	return nil
}

func scanMessages(db *DB, ctx context.Context, q string, args ...any) ([]store.Message, error) {
	rows, err := db.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()

	var msgs []store.Message
	for rows.Next() {
		var m store.Message
		var smppMsgID *string
		var originPeer *string
		var egressIface *string
		var egressPeer *string
		var srcMSISDN *string
		var dstMSISDN *string
		var alertCorrelationID *string
		var deferredReason *string
		var deferredInterface *string
		var servingNodeAtDeferral *string
		var encoding *int
		var dcs *int
		var kind *string
		var refMessageID *string
		err := rows.Scan(
			&m.ID, &m.TPMR, &smppMsgID, &m.OriginIface, &originPeer,
			&egressIface, &egressPeer, &m.RouteCursor, &srcMSISDN, &dstMSISDN,
			&alertCorrelationID, &deferredReason, &deferredInterface, &servingNodeAtDeferral,
			&m.Payload, &m.UDH, &encoding, &dcs, &m.Status,
			&m.RetryCount, &m.NextRetryAt,
			&m.DRRequired, &m.SubmittedAt, &m.ExpiryAt,
			&kind, &refMessageID, &m.RawTPDU, &m.DeliveredAt, &m.SMPPRegisteredDelivery,
		)
		if err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		if smppMsgID != nil {
			m.SMPPMsgID = *smppMsgID
		}
		if originPeer != nil {
			m.OriginPeer = *originPeer
		}
		if egressIface != nil {
			m.EgressIface = *egressIface
		}
		if egressPeer != nil {
			m.EgressPeer = *egressPeer
		}
		if srcMSISDN != nil {
			m.SrcMSISDN = *srcMSISDN
		}
		if dstMSISDN != nil {
			m.DstMSISDN = *dstMSISDN
		}
		if alertCorrelationID != nil {
			m.AlertCorrelationID = *alertCorrelationID
		}
		if deferredReason != nil {
			m.DeferredReason = *deferredReason
		}
		if deferredInterface != nil {
			m.DeferredInterface = *deferredInterface
		}
		if servingNodeAtDeferral != nil {
			m.ServingNodeAtDeferral = *servingNodeAtDeferral
		}
		if encoding != nil {
			m.Encoding = *encoding
		}
		if dcs != nil {
			m.DCS = *dcs
		}
		m.Kind = store.MessageKindSM
		if kind != nil {
			m.Kind = store.NormalizeMessageKind(*kind)
		}
		if refMessageID != nil {
			m.RefMessageID = *refMessageID
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

func (db *DB) CountMessagesByStatus(ctx context.Context) (map[string]int64, error) {
	rows, err := db.pool.Query(ctx,
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
			       kind, ref_message_id::text, raw_tpdu, delivered_at, smpp_registered_delivery
	FROM messages ORDER BY submitted_at DESC LIMIT $1`
	return scanMessages(db, ctx, q, limit)
}

func (db *DB) ListFilteredMessages(ctx context.Context, filter store.MessageFilter) ([]store.Message, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}

	conds := make([]string, 0, 4)
	args := make([]any, 0, 8)
	nextArg := 1

	if len(filter.Statuses) > 0 {
		placeholders := make([]string, 0, len(filter.Statuses))
		for _, status := range filter.Statuses {
			if status == "" {
				continue
			}
			placeholders = append(placeholders, fmt.Sprintf("$%d", nextArg))
			args = append(args, status)
			nextArg++
		}
		if len(placeholders) > 0 {
			conds = append(conds, "status IN ("+strings.Join(placeholders, ",")+")")
		}
	}
	if filter.SrcMSISDN != "" {
		conds = append(conds, fmt.Sprintf("src_msisdn ILIKE $%d", nextArg))
		args = append(args, "%"+filter.SrcMSISDN+"%")
		nextArg++
	}
	if filter.DstMSISDN != "" {
		conds = append(conds, fmt.Sprintf("dst_msisdn ILIKE $%d", nextArg))
		args = append(args, "%"+filter.DstMSISDN+"%")
		nextArg++
	}
	if filter.OriginPeer != "" {
		conds = append(conds, fmt.Sprintf("origin_peer ILIKE $%d", nextArg))
		args = append(args, "%"+filter.OriginPeer+"%")
		nextArg++
	}
	if filter.AlertCorrelationID != "" {
		conds = append(conds, fmt.Sprintf("alert_correlation_id = $%d", nextArg))
		args = append(args, filter.AlertCorrelationID)
		nextArg++
	}
	if filter.DeferredInterface != "" {
		conds = append(conds, fmt.Sprintf("deferred_interface = $%d", nextArg))
		args = append(args, filter.DeferredInterface)
		nextArg++
	}

	q := `
			SELECT id, tp_mr, smpp_msgid, origin_iface, origin_peer,
			       egress_iface, egress_peer, route_cursor, src_msisdn, dst_msisdn,
			       alert_correlation_id, deferred_reason, deferred_interface, serving_node_at_deferral,
			       payload, udh, encoding, dcs, status, retry_count, next_retry_at,
			       dr_required, submitted_at, expiry_at,
			       kind, ref_message_id::text, raw_tpdu, delivered_at, smpp_registered_delivery
		FROM messages`
	if len(conds) > 0 {
		q += "\n\t\tWHERE " + strings.Join(conds, " AND ")
	}
	q += fmt.Sprintf("\n\t\tORDER BY COALESCE(next_retry_at, submitted_at) ASC NULLS FIRST, submitted_at DESC LIMIT $%d", nextArg)
	args = append(args, limit)
	return scanMessages(db, ctx, q, args...)
}

func (db *DB) ListDeliveryReports(ctx context.Context, limit int) ([]store.DeliveryReport, error) {
	if limit <= 0 {
		limit = 100
	}
	const q = `
		SELECT id, message_id, status, egress_iface, COALESCE(raw_receipt,''), reported_at
		FROM delivery_reports ORDER BY reported_at DESC LIMIT $1`
	return scanDeliveryReports(db, ctx, q, limit)
}

func (db *DB) GetDeliveryReport(ctx context.Context, id string) (*store.DeliveryReport, error) {
	const q = `
		SELECT id, message_id, status, egress_iface, COALESCE(raw_receipt,''), reported_at
		FROM delivery_reports WHERE id = $1::uuid`
	drs, err := scanDeliveryReports(db, ctx, q, id)
	if err != nil {
		return nil, err
	}
	if len(drs) == 0 {
		return nil, nil
	}
	return &drs[0], nil
}

func scanDeliveryReports(db *DB, ctx context.Context, q string, args ...any) ([]store.DeliveryReport, error) {
	rows, err := db.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query delivery_reports: %w", err)
	}
	defer rows.Close()
	var drs []store.DeliveryReport
	for rows.Next() {
		var dr store.DeliveryReport
		if err := rows.Scan(&dr.ID, &dr.MessageID, &dr.Status, &dr.EgressIface, &dr.RawReceipt, &dr.ReportedAt); err != nil {
			return nil, fmt.Errorf("scan delivery_report: %w", err)
		}
		drs = append(drs, dr)
	}
	return drs, rows.Err()
}

func (db *DB) SaveDeliveryReport(ctx context.Context, dr store.DeliveryReport) error {
	const q = `
		INSERT INTO delivery_reports (id, message_id, status, egress_iface, raw_receipt, reported_at)
		VALUES ($1,$2,$3,$4,$5,$6)`
	_, err := db.pool.Exec(ctx, q,
		dr.ID, dr.MessageID, dr.Status, dr.EgressIface, dr.RawReceipt, dr.ReportedAt)
	if err != nil {
		return fmt.Errorf("save delivery_report: %w", err)
	}
	return nil
}

// nullableUUID stores an empty string as NULL so it is not cast to UUID.
func nullableUUID(v string) any {
	if v == "" {
		return nil
	}
	return v
}
