package tpdu

import (
	"bytes"
	"testing"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
)

func TestEncodeStatusReportKnownVector(t *testing.T) {
	scts := time.Date(2026, 10, 30, 17, 52, 38, 0, time.UTC)
	dt := time.Date(2026, 10, 30, 17, 52, 45, 0, time.UTC)

	got, err := EncodeStatusReport(0x2A, codec.Address{MSISDN: "989100000111"}, scts, dt, STReceivedBySME)
	if err != nil {
		t.Fatalf("EncodeStatusReport() error = %v", err)
	}

	want := []byte{
		0x06,                                           // TP-MTI=STATUS-REPORT, TP-MMS=1, TP-SRQ=0
		0x2A,                                           // TP-MR
		0x0C, 0x91, 0x89, 0x19, 0x00, 0x00, 0x10, 0x11, // TP-RA 989100000111, international/E.164
		0x62, 0x01, 0x03, 0x71, 0x25, 0x83, 0x00, // TP-SCTS 26-10-30 17:52:38 +00
		0x62, 0x01, 0x03, 0x71, 0x25, 0x54, 0x00, // TP-DT   26-10-30 17:52:45 +00
		0x00, // TP-ST received by SME
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeStatusReport() =\n % X\nwant\n % X", got, want)
	}
}

func TestEncodeStatusReportRoundTrip(t *testing.T) {
	scts := time.Date(2026, 10, 30, 17, 52, 38, 0, time.UTC)
	dt := time.Date(2026, 10, 30, 18, 0, 1, 0, time.UTC)

	for _, st := range []byte{STReceivedBySME, STPermRemoteProcedureError, STPermValidityPeriodExpired} {
		data, err := EncodeStatusReport(0xFF, codec.Address{MSISDN: "989100000111"}, scts, dt, st)
		if err != nil {
			t.Fatalf("EncodeStatusReport(st=0x%02X) error = %v", st, err)
		}
		msg, err := Decode(data)
		if err != nil {
			t.Fatalf("Decode(st=0x%02X) error = %v", st, err)
		}
		if msg.TPMR != 0xFF {
			t.Errorf("TPMR = 0x%02X, want 0xFF", msg.TPMR)
		}
		if msg.Source.MSISDN != "989100000111" {
			t.Errorf("TP-RA = %q, want 989100000111", msg.Source.MSISDN)
		}
		if msg.TPStatus != st {
			t.Errorf("TPStatus = 0x%02X, want 0x%02X", msg.TPStatus, st)
		}
		if !msg.Timestamp.Equal(scts) {
			t.Errorf("SCTS = %v, want %v", msg.Timestamp, scts)
		}
		if !msg.DischargeTime.Equal(dt) {
			t.Errorf("DT = %v, want %v", msg.DischargeTime, dt)
		}
	}
}

func TestEncodeStatusReportOddLengthRecipientPadsWithF(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	data, err := EncodeStatusReport(0x01, codec.Address{MSISDN: "15551234567"}, now, now, STReceivedBySME)
	if err != nil {
		t.Fatalf("EncodeStatusReport() error = %v", err)
	}
	// len(11) | TOA | 6 BCD octets, last one F-padded
	wantRA := []byte{0x0B, 0x91, 0x51, 0x55, 0x21, 0x43, 0x65, 0xF7}
	if got := data[2 : 2+len(wantRA)]; !bytes.Equal(got, wantRA) {
		t.Fatalf("TP-RA = % X, want % X", got, wantRA)
	}
	msg, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if msg.Source.MSISDN != "15551234567" {
		t.Fatalf("TP-RA = %q, want 15551234567", msg.Source.MSISDN)
	}
}

func TestEncodeStatusReportPreservesTimezones(t *testing.T) {
	scts := time.Date(2026, 10, 30, 12, 0, 0, 0, time.FixedZone("", -5*3600))
	dt := time.Date(2026, 10, 30, 21, 30, 0, 0, time.FixedZone("", 3*3600+30*60))

	data, err := EncodeStatusReport(0x10, codec.Address{MSISDN: "989100000111"}, scts, dt, STReceivedBySME)
	if err != nil {
		t.Fatalf("EncodeStatusReport() error = %v", err)
	}
	// -05:00 = 20 quarters -> BCD 0x02 | sign 0x08; +03:30 = 14 quarters -> 0x41
	if got := data[16]; got != 0x0A {
		t.Errorf("SCTS TZ octet = 0x%02X, want 0x0A", got)
	}
	if got := data[23]; got != 0x41 {
		t.Errorf("DT TZ octet = 0x%02X, want 0x41", got)
	}

	msg, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if !msg.Timestamp.Equal(scts) {
		t.Errorf("SCTS = %v, want %v", msg.Timestamp, scts)
	}
	if !msg.DischargeTime.Equal(dt) {
		t.Errorf("DT = %v, want %v", msg.DischargeTime, dt)
	}
}

func TestEncodeStatusReportRejectsEmptyRecipient(t *testing.T) {
	now := time.Now()
	if _, err := EncodeStatusReport(0x01, codec.Address{}, now, now, STReceivedBySME); err == nil {
		t.Fatal("EncodeStatusReport accepted an empty TP-RA")
	}
	if _, err := EncodeStatusReport(0x01, codec.Address{Alpha: "VECTOR"}, now, now, STReceivedBySME); err == nil {
		t.Fatal("EncodeStatusReport accepted an alphanumeric-only TP-RA")
	}
}

func TestEncodeMTUsesRawTPDUVerbatim(t *testing.T) {
	raw := []byte{0x06, 0x2A, 0x0C, 0x91, 0x89, 0x19, 0x00, 0x00, 0x10, 0x11}
	msg := &codec.Message{RawTPDU: raw, Text: "ignored", Source: codec.Address{MSISDN: "1"}}

	got, err := EncodeMT(msg)
	if err != nil {
		t.Fatalf("EncodeMT() error = %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("EncodeMT() = % X, want % X", got, raw)
	}
	got[0] = 0xFF
	if msg.RawTPDU[0] != 0x06 {
		t.Fatal("EncodeMT() result aliases msg.RawTPDU")
	}
}

func TestEncodeMTFallsBackToDeliver(t *testing.T) {
	msg := &codec.Message{
		Source:    codec.Address{MSISDN: "989100000555"},
		Text:      "hi",
		Encoding:  codec.EncodingGSM7,
		Timestamp: time.Date(2026, 10, 30, 17, 52, 38, 0, time.UTC),
	}
	want, err := EncodeDeliver(msg)
	if err != nil {
		t.Fatalf("EncodeDeliver() error = %v", err)
	}
	got, err := EncodeMT(msg)
	if err != nil {
		t.Fatalf("EncodeMT() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeMT() = % X, want % X", got, want)
	}
}

func TestEncodeMTRejectsMSToSCRawTPDU(t *testing.T) {
	// TP-MTI=01 is SMS-SUBMIT, which never travels SC→MS.
	if _, err := EncodeMT(&codec.Message{RawTPDU: []byte{0x01, 0x00}}); err == nil {
		t.Fatal("EncodeMT accepted an SMS-SUBMIT as MT TP-DATA")
	}
}
