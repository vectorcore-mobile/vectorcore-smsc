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
				dcodec.FlagMandatory|dcodec.FlagVendorSpecific, EncodeMSISDN("3342012832", MSISDNEncodingTBCD)),
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

func TestDecodeSCAddressAcceptsASCIIDigitsOnlyWhenConfigured(t *testing.T) {
	ascii := []byte("15550000001")
	if got, want := DecodeSCAddress(ascii, "ascii_digits"), "15550000001"; got != want {
		t.Fatalf("DecodeSCAddress(ascii_digits) = %q, want %q", got, want)
	}
	// The same octets are valid TBCD; without ascii_digits they decode as TBCD.
	if got, want := DecodeSCAddress(ascii, "tbcd"), "1353535303030303030313"; got != want {
		t.Fatalf("DecodeSCAddress(tbcd) = %q, want %q", got, want)
	}
}

func TestDecodeSCAddressTBCD(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{"odd length with F filler", []byte{0x51, 0x55, 0x00, 0x00, 0x00, 0xF0}, "15550000000"},
		// Second digit 8: first octet has the high bit set and must not be
		// mistaken for a nature-of-address octet.
		{"country code 98", []byte{0x89, 0x19, 0x00, 0x00, 0x10, 0x00}, "989100000100"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DecodeSCAddress(tc.data, "tbcd"); got != tc.want {
				t.Fatalf("DecodeSCAddress() = %q, want %q", got, tc.want)
			}
		})
	}
}

// Issue #2: the MSISDN AVP is plain TBCD (TS 29.329 §6.3.2) and must be
// decoded without guessing at length or nature-of-address octets.
func TestDecodeMSISDNStrictTBCD(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{"country code 98", []byte{0x89, 0x19, 0x00, 0x00, 0x50, 0x55}, "989100000555"},
		{"country code 49", []byte{0x94, 0x51, 0x23, 0x45, 0x67}, "4915325476"},
		{"odd length with F filler", []byte{0x33, 0x24, 0x10, 0x82, 0xF3}, "334201283"},
		// 0x01 is smaller than the value length; it is the digits "10".
		{"first octet smaller than length", []byte{0x01, 0x23, 0x45, 0x67}, "10325476"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DecodeMSISDN(tc.data, MSISDNEncodingTBCD); got != tc.want {
				t.Fatalf("DecodeMSISDN() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDecodeMSISDNLegacyPrefixed(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{"len | NOA | TBCD (vectorcore-mme)", []byte{0x07, 0x91, 0x89, 0x19, 0x00, 0x00, 0x50, 0x55}, "989100000555"},
		{"len | TBCD", []byte{0x05, 0x33, 0x24, 0x10, 0x82, 0x23}, "3342012832"},
		{"NOA | TBCD", []byte{0x91, 0x33, 0x24, 0x10, 0x82, 0x23}, "3342012832"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DecodeMSISDN(tc.data, MSISDNEncodingLegacyPrefixed); got != tc.want {
				t.Fatalf("DecodeMSISDN() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEncodeMSISDN(t *testing.T) {
	if got, want := EncodeMSISDN("+989100000555", MSISDNEncodingTBCD), []byte{0x89, 0x19, 0x00, 0x00, 0x50, 0x55}; !bytes.Equal(got, want) {
		t.Fatalf("EncodeMSISDN(tbcd) = % X, want % X", got, want)
	}
	if got, want := EncodeMSISDN("334201283", MSISDNEncodingTBCD), []byte{0x33, 0x24, 0x10, 0x82, 0xF3}; !bytes.Equal(got, want) {
		t.Fatalf("EncodeMSISDN(tbcd, odd) = % X, want % X", got, want)
	}
	if got, want := EncodeMSISDN("989100000555", MSISDNEncodingLegacyPrefixed), []byte{0x07, 0x91, 0x89, 0x19, 0x00, 0x00, 0x50, 0x55}; !bytes.Equal(got, want) {
		t.Fatalf("EncodeMSISDN(legacy) = % X, want % X", got, want)
	}
	for _, enc := range []MSISDNEncoding{MSISDNEncodingTBCD, MSISDNEncodingLegacyPrefixed} {
		if got := DecodeMSISDN(EncodeMSISDN("989100000555", enc), enc); got != "989100000555" {
			t.Fatalf("round trip %s = %q", enc, got)
		}
	}
}

func TestNormalizeMSISDNEncoding(t *testing.T) {
	for in, want := range map[string]MSISDNEncoding{
		"": MSISDNEncodingTBCD, "tbcd": MSISDNEncodingTBCD, " Legacy_Prefixed ": MSISDNEncodingLegacyPrefixed, "bogus": MSISDNEncodingTBCD,
	} {
		if got := NormalizeMSISDNEncoding(in); got != want {
			t.Fatalf("NormalizeMSISDNEncoding(%q) = %q, want %q", in, got, want)
		}
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
				dcodec.FlagMandatory|dcodec.FlagVendorSpecific, EncodeMSISDN(msisdn, MSISDNEncodingTBCD)),
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

// Issue #2 end to end: a spec-compliant MME sends the originator as plain
// TBCD; the decoded SMS must keep the country code.
func TestDecodeTFRKeepsOriginatorCountryCode(t *testing.T) {
	tpData, err := tpdu.EncodeSubmit(&codec.Message{
		Destination: codec.Address{MSISDN: "989100000111"},
		Text:        "test",
		Encoding:    codec.EncodingGSM7,
	})
	if err != nil {
		t.Fatalf("EncodeSubmit() error = %v", err)
	}
	uid, err := dcodec.NewGrouped(dcodec.CodeUserIdentifier, dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewString(dcodec.CodeUserName, 0, dcodec.FlagMandatory, "432600000000555"),
			dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP,
				dcodec.FlagMandatory|dcodec.FlagVendorSpecific, []byte{0x89, 0x19, 0x00, 0x00, 0x50, 0x55}),
		})
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}
	wire := dcodec.NewRequest(dcodec.CmdMOForwardShortMessage, dcodec.App3GPP_SGd)
	wire.Add(dcodec.NewOctetString(dcodec.CodeSMRPUI, dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific, tpData), uid)

	decoded, err := DecodeTFR(wire.Build())
	if err != nil {
		t.Fatalf("DecodeTFR() error = %v", err)
	}
	if got, want := decoded.Source.MSISDN, "989100000555"; got != want {
		t.Fatalf("Source.MSISDN = %q, want %q", got, want)
	}
}

// TP-MTI 0x02 from the UE is SMS-COMMAND, not a status report: an MO-Forward
// carrying one is rejected instead of being routed as a bogus message.
func TestDecodeTFRRejectsSMSCommand(t *testing.T) {
	command := []byte{0x02, 0x07, 0x00, 0x00, 0x2A, 0x0C, 0x91, 0x89, 0x19, 0x00, 0x00, 0x10, 0x11, 0x00}
	wire := dcodec.NewRequest(dcodec.CmdMOForwardShortMessage, dcodec.App3GPP_SGd)
	wire.Add(dcodec.NewOctetString(dcodec.CodeSMRPUI, dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific, command))
	if msg, err := DecodeTFR(wire.Build()); err == nil {
		t.Fatalf("DecodeTFR() = %+v, want SMS-COMMAND rejection", msg)
	}
}
