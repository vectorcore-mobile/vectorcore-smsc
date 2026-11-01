package sqlite

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

func TestSaveMessageRoundTripsTPMR(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "smsc.db"), time.Hour)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	mr := 0x2A
	for _, m := range []store.Message{
		{ID: "with-mr", TPMR: &mr, OriginIface: "sgd", SrcMSISDN: "989100000555", DstMSISDN: "989100000111", Status: store.MessageStatusDispatched, SubmittedAt: time.Now()},
		{ID: "without-mr", OriginIface: "smpp", SrcMSISDN: "989100000555", DstMSISDN: "989100000111", Status: store.MessageStatusDispatched, SubmittedAt: time.Now()},
	} {
		if err := db.SaveMessage(ctx, m); err != nil {
			t.Fatalf("SaveMessage(%s) error = %v", m.ID, err)
		}
	}

	got, err := db.GetMessage(ctx, "with-mr")
	if err != nil || got == nil {
		t.Fatalf("GetMessage(with-mr) = %v, %v", got, err)
	}
	if got.TPMR == nil || *got.TPMR != mr {
		t.Fatalf("TPMR = %v, want %d", got.TPMR, mr)
	}

	got, err = db.GetMessage(ctx, "without-mr")
	if err != nil || got == nil {
		t.Fatalf("GetMessage(without-mr) = %v, %v", got, err)
	}
	if got.TPMR != nil {
		t.Fatalf("TPMR = %d, want nil", *got.TPMR)
	}
}

func TestSaveMessageRoundTripsKindRefAndRawTPDU(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "smsc.db"), time.Hour)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	raw := []byte{0x06, 0x2A, 0x0C, 0x91, 0x89, 0x19, 0x00, 0x00, 0x10, 0x11}
	if err := db.SaveMessage(ctx, store.Message{
		ID: "sr-1", Kind: store.MessageKindStatusReport, RefMessageID: "orig-1", RawTPDU: raw,
		OriginIface: "internal", DstMSISDN: "989100000555", Status: store.MessageStatusDispatched, SubmittedAt: time.Now(),
	}); err != nil {
		t.Fatalf("SaveMessage(sr-1) error = %v", err)
	}
	if err := db.SaveMessage(ctx, store.Message{
		ID: "sm-1", OriginIface: "sgd", DstMSISDN: "989100000111", Status: store.MessageStatusDispatched, SubmittedAt: time.Now(),
	}); err != nil {
		t.Fatalf("SaveMessage(sm-1) error = %v", err)
	}

	got, err := db.GetMessage(ctx, "sr-1")
	if err != nil || got == nil {
		t.Fatalf("GetMessage(sr-1) = %v, %v", got, err)
	}
	if got.Kind != store.MessageKindStatusReport || got.RefMessageID != "orig-1" || !bytes.Equal(got.RawTPDU, raw) {
		t.Fatalf("status report = kind %q ref %q raw % X", got.Kind, got.RefMessageID, got.RawTPDU)
	}

	got, err = db.GetMessage(ctx, "sm-1")
	if err != nil || got == nil {
		t.Fatalf("GetMessage(sm-1) = %v, %v", got, err)
	}
	if got.Kind != store.MessageKindSM || got.RefMessageID != "" || got.RawTPDU != nil {
		t.Fatalf("sm = kind %q ref %q raw % X, want sm/empty/nil", got.Kind, got.RefMessageID, got.RawTPDU)
	}
}

func TestSaveMessageRoundTripsSMPPRegisteredDelivery(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "smsc.db"), time.Hour)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	failureOnly := 0x02
	if err := db.SaveMessage(ctx, store.Message{
		ID: "smpp-1", OriginIface: "smpp", DstMSISDN: "15551230002", Status: store.MessageStatusDispatched,
		SubmittedAt: time.Now(), SMPPRegisteredDelivery: &failureOnly,
	}); err != nil {
		t.Fatalf("SaveMessage(smpp-1) error = %v", err)
	}
	if err := db.SaveMessage(ctx, store.Message{
		ID: "sgd-1", OriginIface: "sgd", DstMSISDN: "15551230002", Status: store.MessageStatusDispatched, SubmittedAt: time.Now(),
	}); err != nil {
		t.Fatalf("SaveMessage(sgd-1) error = %v", err)
	}

	got, err := db.GetMessage(ctx, "smpp-1")
	if err != nil || got == nil {
		t.Fatalf("GetMessage(smpp-1) = %v, %v", got, err)
	}
	if got.SMPPRegisteredDelivery == nil || *got.SMPPRegisteredDelivery != failureOnly {
		t.Fatalf("smpp-1 SMPPRegisteredDelivery = %v, want 2", got.SMPPRegisteredDelivery)
	}
	got, err = db.GetMessage(ctx, "sgd-1")
	if err != nil || got == nil {
		t.Fatalf("GetMessage(sgd-1) = %v, %v", got, err)
	}
	if got.SMPPRegisteredDelivery != nil {
		t.Fatalf("sgd-1 SMPPRegisteredDelivery = %d, want nil", *got.SMPPRegisteredDelivery)
	}
}

// Rows written by older versions can hold NULL in text columns; reading
// them must not fail (which would break every list query that includes them).
func TestGetMessageReadsNullTextColumns(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "smsc.db"), time.Hour)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.db.ExecContext(ctx,
		`INSERT INTO messages (id, origin_iface, status, submitted_at) VALUES ('nulls-1', 'smpp', 'QUEUED', ?)`,
		time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	got, err := db.GetMessage(ctx, "nulls-1")
	if err != nil || got == nil {
		t.Fatalf("GetMessage(nulls-1) = %v, %v", got, err)
	}
	if got.SMPPMsgID != "" || got.OriginPeer != "" || got.EgressIface != "" || got.DstMSISDN != "" || got.DeferredReason != "" {
		t.Fatalf("NULL columns read as %+v, want empty strings", got)
	}
	msgs, err := db.ListMessages(ctx, 10)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("ListMessages() = %d rows, %v; want 1 row", len(msgs), err)
	}
}

// An existing database created before the kind/ref_message_id/raw_tpdu
// columns must be upgraded in place by Open, with old rows read as "sm".
func TestOpenUpgradesLegacyMessagesTable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "smsc.db")

	db, err := Open(ctx, path, time.Hour)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, col := range []string{"kind", "ref_message_id", "raw_tpdu", "smpp_registered_delivery"} {
		if _, err := db.db.ExecContext(ctx, "ALTER TABLE messages DROP COLUMN "+col); err != nil {
			t.Fatalf("drop column %s: %v", col, err)
		}
	}
	if _, err := db.db.ExecContext(ctx,
		`INSERT INTO messages (
			id, smpp_msgid, origin_iface, origin_peer, egress_iface, egress_peer,
			src_msisdn, dst_msisdn, alert_correlation_id, deferred_reason,
			deferred_interface, serving_node_at_deferral, status, submitted_at
		) VALUES ('legacy-1', '', 'smpp', '', '', '', '', '', '', '', '', '', 'QUEUED', ?)`,
		time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	db.Close()

	db, err = Open(ctx, path, time.Hour)
	if err != nil {
		t.Fatalf("re-Open() error = %v", err)
	}
	defer db.Close()

	got, err := db.GetMessage(ctx, "legacy-1")
	if err != nil || got == nil {
		t.Fatalf("GetMessage(legacy-1) = %v, %v", got, err)
	}
	if got.Kind != store.MessageKindSM {
		t.Fatalf("legacy Kind = %q, want %q", got.Kind, store.MessageKindSM)
	}
	if got.SMPPRegisteredDelivery != nil {
		t.Fatalf("legacy SMPPRegisteredDelivery = %d, want nil", *got.SMPPRegisteredDelivery)
	}
}

func TestGetMessageLoadsDeliveredAt(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "smsc.db"), time.Hour)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if err := db.SaveMessage(ctx, store.Message{
		ID: "m1", OriginIface: "sgd", DstMSISDN: "989100000111", Status: store.MessageStatusDispatched, SubmittedAt: time.Now(),
	}); err != nil {
		t.Fatalf("SaveMessage() error = %v", err)
	}
	got, err := db.GetMessage(ctx, "m1")
	if err != nil || got == nil {
		t.Fatalf("GetMessage() = %v, %v", got, err)
	}
	if got.DeliveredAt != nil {
		t.Fatalf("DeliveredAt = %v before delivery, want nil", got.DeliveredAt)
	}

	before := time.Now().UTC().Add(-time.Second)
	if err := db.UpdateMessageStatus(ctx, "m1", store.MessageStatusDelivered); err != nil {
		t.Fatalf("UpdateMessageStatus() error = %v", err)
	}
	got, err = db.GetMessage(ctx, "m1")
	if err != nil || got == nil {
		t.Fatalf("GetMessage() = %v, %v", got, err)
	}
	if got.DeliveredAt == nil || got.DeliveredAt.Before(before) {
		t.Fatalf("DeliveredAt = %v, want approximately now", got.DeliveredAt)
	}
}

// Regression: RFC3339 timestamps must be compared as times, not text, so a
// retry or expiry due earlier the same UTC day is picked up immediately.
func TestDueMessagesAreSelectedOnTheSameDay(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "smsc.db"), time.Hour)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)
	for _, m := range []store.Message{
		{ID: "retry-due", Status: store.MessageStatusWaitTimer, NextRetryAt: &past},
		{ID: "retry-later", Status: store.MessageStatusWaitTimer, NextRetryAt: &future},
		{ID: "expiry-due", Status: store.MessageStatusWaitEvent, ExpiryAt: &past},
		{ID: "expiry-later", Status: store.MessageStatusWaitEvent, ExpiryAt: &future},
	} {
		m.OriginIface = "sgd"
		m.SubmittedAt = time.Now()
		if err := db.SaveMessage(ctx, m); err != nil {
			t.Fatalf("SaveMessage(%s) error = %v", m.ID, err)
		}
	}
	// Rows requeued at startup carry SQLite's own datetime('now') format.
	if _, err := db.db.ExecContext(ctx, `UPDATE messages SET next_retry_at=datetime('now', '-1 minute') WHERE id='retry-later'`); err != nil {
		t.Fatalf("set datetime() format: %v", err)
	}

	ids := func(msgs []store.Message) map[string]bool {
		out := map[string]bool{}
		for _, m := range msgs {
			out[m.ID] = true
		}
		return out
	}

	retry, err := db.ListRetryableMessages(ctx)
	if err != nil {
		t.Fatalf("ListRetryableMessages() error = %v", err)
	}
	if got := ids(retry); !got["retry-due"] || !got["retry-later"] || len(got) != 2 {
		t.Fatalf("retryable = %v, want retry-due and retry-later (datetime format)", got)
	}

	expired, err := db.ListExpiredMessages(ctx)
	if err != nil {
		t.Fatalf("ListExpiredMessages() error = %v", err)
	}
	if got := ids(expired); !got["expiry-due"] || len(got) != 1 {
		t.Fatalf("expired = %v, want only expiry-due", got)
	}
}
