package sgd

import (
	"bytes"
	"testing"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

func TestDecodeTFRUsesUserIdentifierMSISDNAsSource(t *testing.T) {
	msg := &codec.Message{
		Destination: codec.Address{MSISDN: "15551230001"},
		Text:        "test",
		Encoding:    codec.EncodingGSM7,
	}
	tpData, err := tpdu.EncodeSubmit(msg)
	if err != nil {
		t.Fatalf("EncodeSubmit() error = %v", err)
	}

	uid, err := dcodec.NewGrouped(
		dcodec.CodeUserIdentifier,
		dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP,
				dcodec.FlagMandatory|dcodec.FlagVendorSpecific, encodeBCDMSISDN("3342012832")),
		},
	)
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}

	wire := dcodec.NewRequest(dcodec.CmdMTForwardShortMessage, dcodec.App3GPP_SGd)
	wire.Add(
		dcodec.NewOctetString(dcodec.CodeSMRPUI, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific, tpData),
		uid,
		dcodec.NewOctetString(dcodec.CodeSCAddress, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific, []byte("15550000001")),
	)

	decoded, err := DecodeTFR(wire.Build())
	if err != nil {
		t.Fatalf("DecodeTFR() error = %v", err)
	}
	if got, want := decoded.Source.MSISDN, "3342012832"; got != want {
		t.Fatalf("Source.MSISDN = %q, want %q", got, want)
	}
	if got, want := decoded.Destination.MSISDN, "15551230001"; got != want {
		t.Fatalf("Destination.MSISDN = %q, want %q", got, want)
	}
}

func TestDecodeSCAddressAcceptsASCIIDigits(t *testing.T) {
	if got, want := decodeSCAddress([]byte("15550000001")), "15550000001"; got != want {
		t.Fatalf("decodeSCAddress() = %q, want %q", got, want)
	}
}

func TestDecodeBCDMSISDNLengthPrefixedWithoutTONNPI(t *testing.T) {
	data := []byte{0x05, 0x33, 0x24, 0x10, 0x82, 0x23}
	if got, want := decodeBCDMSISDN(data), "3342012832"; got != want {
		t.Fatalf("decodeBCDMSISDN() = %q, want %q", got, want)
	}
}

func TestEncodeSCAddressUsesASCIIDigitsWhenConfigured(t *testing.T) {
	got := encodeSCAddress("+15550000000", "ascii_digits")
	want := []byte("15550000000")
	if !bytes.Equal(got, want) {
		t.Fatalf("encodeSCAddress() = %x, want %x", got, want)
	}
}

func buildUserIdentifierRequest(t *testing.T, tpData []byte, imsi, msisdn string) *dcodec.Message {
	t.Helper()
	uid, err := dcodec.NewGrouped(
		dcodec.CodeUserIdentifier,
		dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewString(dcodec.CodeUserName, 0, dcodec.FlagMandatory, imsi),
			dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP,
				dcodec.FlagMandatory|dcodec.FlagVendorSpecific, encodeBCDMSISDN(msisdn)),
		},
	)
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}
	wire := dcodec.NewRequest(dcodec.CmdMOForwardShortMessage, dcodec.App3GPP_SGd)
	wire.Add(
		dcodec.NewOctetString(dcodec.CodeSMRPUI, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific, tpData),
		uid,
	)
	return wire.Build()
}

func TestDecodeTFRAttributesUserIdentifierIMSIToSourceForSubmit(t *testing.T) {
	tpData, err := tpdu.EncodeSubmit(&codec.Message{
		Destination: codec.Address{MSISDN: "989100000111"},
		Text:        "test",
		Encoding:    codec.EncodingGSM7,
		TPMR:        0x2A,
	})
	if err != nil {
		t.Fatalf("EncodeSubmit() error = %v", err)
	}

	decoded, err := DecodeTFR(buildUserIdentifierRequest(t, tpData, "432600000000555", "989100000555"))
	if err != nil {
		t.Fatalf("DecodeTFR() error = %v", err)
	}
	if got, want := decoded.Source.IMSI, "432600000000555"; got != want {
		t.Fatalf("Source.IMSI = %q, want %q", got, want)
	}
	if got := decoded.Destination.IMSI; got != "" {
		t.Fatalf("Destination.IMSI = %q, want empty", got)
	}
	if got, want := decoded.Source.MSISDN, "989100000555"; got != want {
		t.Fatalf("Source.MSISDN = %q, want %q", got, want)
	}
	if got, want := decoded.TPMR, byte(0x2A); got != want {
		t.Fatalf("TPMR = 0x%02X, want 0x%02X", got, want)
	}
}

func TestDecodeOFRAttributesUserIdentifierIMSIToDestinationForDeliver(t *testing.T) {
	tpData, err := tpdu.EncodeDeliver(&codec.Message{
		Source:   codec.Address{MSISDN: "989100000555"},
		Text:     "test",
		Encoding: codec.EncodingGSM7,
	})
	if err != nil {
		t.Fatalf("EncodeDeliver() error = %v", err)
	}

	decoded, err := DecodeOFR(buildUserIdentifierRequest(t, tpData, "432600000000111", "989100000111"))
	if err != nil {
		t.Fatalf("DecodeOFR() error = %v", err)
	}
	if got, want := decoded.Destination.IMSI, "432600000000111"; got != want {
		t.Fatalf("Destination.IMSI = %q, want %q", got, want)
	}
	if got := decoded.Source.IMSI; got != "" {
		t.Fatalf("Source.IMSI = %q, want empty", got)
	}
}
