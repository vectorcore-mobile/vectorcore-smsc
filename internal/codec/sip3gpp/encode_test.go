package sip3gpp

import (
	"bytes"
	"testing"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	"github.com/svinson1121/vectorcore-smsc/internal/numbering"
)

func TestEncodeRPAddressStripsLeadingPlus(t *testing.T) {
	got := encodeRPAddress("+15550000000")
	if len(got) < 3 {
		t.Fatalf("encoded RP address too short: %v", got)
	}
	if got[0] == 0x00 {
		t.Fatalf("expected non-empty RP address for +E.164 input: %v", got)
	}
	if got[1] != 0x91 {
		t.Fatalf("unexpected TOA: got 0x%02x want 0x91", got[1])
	}
}

func TestEncodeRPAddressRejectsNonDigits(t *testing.T) {
	got := encodeRPAddress("smsc")
	if len(got) != 1 || got[0] != 0x00 {
		t.Fatalf("expected empty RP address for invalid input, got %v", got)
	}
}

func TestDecodeRPDataUsesTPDestinationAddressForRecipient(t *testing.T) {
	tpData, err := tpdu.EncodeSubmit(&codec.Message{
		Destination: codec.Address{
			MSISDN: "6752012860",
			TON:    numbering.TONUnknown,
			NPI:    0x01,
		},
		Text:     "hello",
		Encoding: codec.EncodingGSM7,
	})
	if err != nil {
		t.Fatalf("EncodeSubmit() error = %v", err)
	}

	body := []byte{rpMTIDataMStoSC, 0x7a}
	body = append(body, encodeRPAddress("")...)
	body = append(body, encodeRPAddress("15550000000")...)
	body = append(body, byte(len(tpData)))
	body = append(body, tpData...)

	msg, err := Decode(body)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got, want := msg.Destination.MSISDN, "6752012860"; got != want {
		t.Fatalf("Destination.MSISDN = %q, want TP-DA %q", got, want)
	}
	if got, want := msg.Destination.TON, numbering.TONUnknown; got != want {
		t.Fatalf("Destination.TON = %d, want %d", got, want)
	}
	if got, want := msg.Destination.NPI, byte(0x01); got != want {
		t.Fatalf("Destination.NPI = %d, want %d", got, want)
	}
}

func TestEncodeCarriesRawTPDUInRPUserData(t *testing.T) {
	raw := []byte{0x06, 0x2A, 0x0C, 0x91, 0x89, 0x19, 0x00, 0x00, 0x10, 0x11,
		0x62, 0x01, 0x03, 0x71, 0x25, 0x83, 0x00, 0x62, 0x01, 0x03, 0x71, 0x25, 0x54, 0x00, 0x00}

	body, err := Encode(&codec.Message{Kind: codec.MessageKindStatusReport, RawTPDU: raw}, 0x07, "15550000000")
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if got, want := body[0], byte(rpMTIDataSCtoMS); got != want {
		t.Fatalf("RP-MTI = 0x%02X, want 0x%02X", got, want)
	}
	if got := body[1]; got != 0x07 {
		t.Fatalf("RP-MR = 0x%02X, want 0x07", got)
	}
	// RP-UD is the tail: length octet followed by the TP-DATA.
	rpUD := body[len(body)-len(raw)-1:]
	if int(rpUD[0]) != len(raw) || !bytes.Equal(rpUD[1:], raw) {
		t.Fatalf("RP-UD = % X, want len %d + % X", rpUD, len(raw), raw)
	}
}

func rpData(rpMTI byte, tpData []byte) []byte {
	// RP-MTI, RP-MR, empty RP-OA, empty RP-DA, RP-UD
	return append([]byte{rpMTI, 0x05, 0x00, 0x00, byte(len(tpData))}, tpData...)
}

func TestDecodeRPDataUsesDirectionForTPMTI(t *testing.T) {
	command := []byte{0x02, 0x07, 0x00, 0x00, 0x2A, 0x0C, 0x91, 0x89, 0x19, 0x00, 0x00, 0x10, 0x11, 0x00}
	if msg, err := Decode(rpData(rpMTIDataMStoSC, command)); err == nil {
		t.Fatalf("MO RP-DATA with SMS-COMMAND decoded as %+v, want rejection", msg)
	}

	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	report, err := tpdu.EncodeStatusReport(0x2A, codec.Address{MSISDN: "989100000111"}, now, now, tpdu.STReceivedBySME)
	if err != nil {
		t.Fatalf("EncodeStatusReport() error = %v", err)
	}
	msg, err := Decode(rpData(rpMTIDataSCtoMS, report))
	if err != nil || msg.TPMR != 0x2A || msg.TPStatus != tpdu.STReceivedBySME {
		t.Fatalf("MT RP-DATA with SMS-STATUS-REPORT = %+v, %v", msg, err)
	}
}
