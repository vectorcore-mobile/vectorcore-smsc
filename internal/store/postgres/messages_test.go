package postgres

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

// openTestDB connects to the database named by SMSC_TEST_POSTGRES_DSN and
// skips the test when it is unset. The schema is applied by Open.
func openTestDB(t *testing.T) *DB {
	t.Helper()
	dsn := os.Getenv("SMSC_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SMSC_TEST_POSTGRES_DSN not set")
	}
	db, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSaveMessageRoundTripsStatusReportFields(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	origID := "6ff75e5c-bd8e-468f-8c17-8396d6124e38"
	reportID := "0b0e4c8e-5a8a-4d4f-9f55-2d7f4a0f1c01"
	t.Cleanup(func() {
		_ = db.DeleteMessage(ctx, origID)
		_ = db.DeleteMessage(ctx, reportID)
	})

	mr := 0x2A
	if err := db.SaveMessage(ctx, store.Message{
		ID: origID, TPMR: &mr, OriginIface: "sgd", SrcMSISDN: "989100000555", DstMSISDN: "989100000111",
		Status: store.MessageStatusDispatched, DRRequired: true, SubmittedAt: time.Now(),
	}); err != nil {
		t.Fatalf("SaveMessage(orig) error = %v", err)
	}
	raw := []byte{0x06, 0x2A, 0x0C, 0x91, 0x89, 0x19, 0x00, 0x00, 0x10, 0x11}
	if err := db.SaveMessage(ctx, store.Message{
		ID: reportID, Kind: store.MessageKindStatusReport, RefMessageID: origID, RawTPDU: raw,
		OriginIface: "internal", DstMSISDN: "989100000555", Status: store.MessageStatusDispatched, SubmittedAt: time.Now(),
	}); err != nil {
		t.Fatalf("SaveMessage(report) error = %v", err)
	}

	got, err := db.GetMessage(ctx, origID)
	if err != nil || got == nil {
		t.Fatalf("GetMessage(orig) = %v, %v", got, err)
	}
	if got.Kind != store.MessageKindSM || got.RefMessageID != "" || got.RawTPDU != nil {
		t.Fatalf("orig = kind %q ref %q raw % X, want sm/empty/nil", got.Kind, got.RefMessageID, got.RawTPDU)
	}
	if got.TPMR == nil || *got.TPMR != mr {
		t.Fatalf("orig TPMR = %v, want %d", got.TPMR, mr)
	}
	if got.DeliveredAt != nil {
		t.Fatalf("orig DeliveredAt = %v before delivery, want nil", got.DeliveredAt)
	}
	before := time.Now().Add(-time.Second)
	if err := db.UpdateMessageStatus(ctx, origID, store.MessageStatusDelivered); err != nil {
		t.Fatalf("UpdateMessageStatus() error = %v", err)
	}
	if got, err = db.GetMessage(ctx, origID); err != nil || got == nil {
		t.Fatalf("GetMessage(orig) = %v, %v", got, err)
	}
	if got.DeliveredAt == nil || got.DeliveredAt.Before(before) {
		t.Fatalf("orig DeliveredAt = %v, want approximately now", got.DeliveredAt)
	}

	got, err = db.GetMessage(ctx, reportID)
	if err != nil || got == nil {
		t.Fatalf("GetMessage(report) = %v, %v", got, err)
	}
	if got.Kind != store.MessageKindStatusReport || got.RefMessageID != origID || !bytes.Equal(got.RawTPDU, raw) {
		t.Fatalf("report = kind %q ref %q raw % X", got.Kind, got.RefMessageID, got.RawTPDU)
	}
}

func TestSaveMessageRoundTripsSMPPRegisteredDelivery(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	smppID := "5d1f0f6e-3c1a-4b8e-9a51-7d2c9e0b6a11"
	sgdID := "a3c2b1d0-8e7f-4a6b-9c5d-1e2f3a4b5c6d"
	t.Cleanup(func() {
		_ = db.DeleteMessage(ctx, smppID)
		_ = db.DeleteMessage(ctx, sgdID)
	})

	failureOnly := 0x02
	if err := db.SaveMessage(ctx, store.Message{
		ID: smppID, OriginIface: "smpp", DstMSISDN: "15551230002", Status: store.MessageStatusDispatched,
		SubmittedAt: time.Now(), SMPPRegisteredDelivery: &failureOnly,
	}); err != nil {
		t.Fatalf("SaveMessage(smpp) error = %v", err)
	}
	if err := db.SaveMessage(ctx, store.Message{
		ID: sgdID, OriginIface: "sgd", DstMSISDN: "15551230002", Status: store.MessageStatusDispatched, SubmittedAt: time.Now(),
	}); err != nil {
		t.Fatalf("SaveMessage(sgd) error = %v", err)
	}

	got, err := db.GetMessage(ctx, smppID)
	if err != nil || got == nil {
		t.Fatalf("GetMessage(smpp) = %v, %v", got, err)
	}
	if got.SMPPRegisteredDelivery == nil || *got.SMPPRegisteredDelivery != failureOnly {
		t.Fatalf("smpp SMPPRegisteredDelivery = %v, want 2", got.SMPPRegisteredDelivery)
	}
	got, err = db.GetMessage(ctx, sgdID)
	if err != nil || got == nil {
		t.Fatalf("GetMessage(sgd) = %v, %v", got, err)
	}
	if got.SMPPRegisteredDelivery != nil {
		t.Fatalf("sgd SMPPRegisteredDelivery = %d, want nil", *got.SMPPRegisteredDelivery)
	}
}
