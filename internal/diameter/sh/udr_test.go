package sh

import (
	"bytes"
	"testing"

	sgdcodec "github.com/svinson1121/vectorcore-smsc/internal/codec/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

func TestBuildUDRRequestIncludesRequired3GPPAVPs(t *testing.T) {
	msg, err := buildUDRRequest(
		diameter.Config{LocalFQDN: "smsc.example.net", LocalRealm: "example.net"},
		"hss.example.net",
		"example.net",
		"3342012832",
		sgdcodec.MSISDNEncodingTBCD,
		"smsc.example.net;1;1",
		0x11223344,
	)
	if err != nil {
		t.Fatalf("buildUDRRequest() error = %v", err)
	}
	if !msg.IsRequest() {
		t.Fatal("expected UDR to be a request")
	}
	if got, want := msg.Header.CommandCode, dcodec.CmdUserData; got != want {
		t.Fatalf("command = %d, want %d", got, want)
	}
	if got, want := msg.Header.AppID, dcodec.App3GPP_Sh; got != want {
		t.Fatalf("app_id = %d, want %d", got, want)
	}
	requiredBase := []uint32{
		dcodec.CodeSessionID,
		dcodec.CodeAuthApplicationID,
		dcodec.CodeAuthSessionState,
		dcodec.CodeOriginHost,
		dcodec.CodeOriginRealm,
		dcodec.CodeDestinationHost,
		dcodec.CodeDestinationRealm,
	}
	for _, code := range requiredBase {
		if msg.FindAVP(code, 0) == nil {
			t.Fatalf("missing required base AVP %d", code)
		}
	}
	if avp := msg.FindAVP(dcodec.CodeAuthSessionState, 0); avp == nil {
		t.Fatal("missing Auth-Session-State")
	} else if got, _ := avp.Uint32(); got != dcodec.AuthSessionStateNoStateMaintained {
		t.Fatalf("Auth-Session-State = %d, want %d", got, dcodec.AuthSessionStateNoStateMaintained)
	}
	if avp := msg.FindAVP(dcodec.CodeDataReference, dcodec.Vendor3GPP); avp == nil {
		t.Fatal("missing Data-Reference")
	} else if got, _ := avp.Uint32(); got != dcodec.DataRefIMSUserState {
		t.Fatalf("Data-Reference = %d, want %d", got, dcodec.DataRefIMSUserState)
	}
	userIdentity := msg.FindAVP(dcodec.CodeUserIdentity, dcodec.Vendor3GPP)
	if userIdentity == nil {
		t.Fatal("missing User-Identity")
	}
	children, err := dcodec.DecodeGrouped(userIdentity)
	if err != nil {
		t.Fatalf("DecodeGrouped(User-Identity) error = %v", err)
	}
	var msisdnFound bool
	for _, child := range children {
		if child.Code == dcodec.CodeMSISDN && child.VendorID == dcodec.Vendor3GPP {
			msisdnFound = true
		}
	}
	if !msisdnFound {
		t.Fatal("User-Identity missing MSISDN AVP")
	}
}

func udrMSISDN(t *testing.T, msisdn string, enc sgdcodec.MSISDNEncoding) []byte {
	t.Helper()
	msg, err := buildUDRRequest(
		diameter.Config{LocalFQDN: "smsc.example.net", LocalRealm: "example.net"},
		"hss.example.net", "example.net", msisdn, enc, "smsc.example.net;1;1", 1,
	)
	if err != nil {
		t.Fatalf("buildUDRRequest() error = %v", err)
	}
	userIdentity := msg.FindAVP(dcodec.CodeUserIdentity, dcodec.Vendor3GPP)
	if userIdentity == nil {
		t.Fatal("missing User-Identity")
	}
	children, err := dcodec.DecodeGrouped(userIdentity)
	if err != nil {
		t.Fatalf("DecodeGrouped(User-Identity) error = %v", err)
	}
	for _, child := range children {
		if child.Code == dcodec.CodeMSISDN && child.VendorID == dcodec.Vendor3GPP {
			return child.Data
		}
	}
	t.Fatal("User-Identity missing MSISDN AVP")
	return nil
}

// TS 29.329 §6.3.2: the MSISDN AVP is plain TBCD with no length or NOA octet.
func TestBuildUDRRequestMSISDNPlainTBCD(t *testing.T) {
	tests := []struct {
		msisdn string
		want   []byte
	}{
		{"989100000555", []byte{0x89, 0x19, 0x00, 0x00, 0x50, 0x55}},
		{"491532547600", []byte{0x94, 0x51, 0x23, 0x45, 0x67, 0x00}},
		{"105800000001", []byte{0x01, 0x85, 0x00, 0x00, 0x00, 0x10}},
		{"334201283", []byte{0x33, 0x24, 0x10, 0x82, 0xF3}},
		{"+12345678901", []byte{0x21, 0x43, 0x65, 0x87, 0x09, 0xF1}},
	}
	for _, tc := range tests {
		if got := udrMSISDN(t, tc.msisdn, sgdcodec.MSISDNEncodingTBCD); !bytes.Equal(got, tc.want) {
			t.Errorf("MSISDN(%q) = % X, want % X", tc.msisdn, got, tc.want)
		}
	}
}

func TestBuildUDRRequestMSISDNLegacyPrefixed(t *testing.T) {
	want := []byte{0x07, 0x91, 0x21, 0x43, 0x65, 0x87, 0x09, 0xF1}
	for _, msisdn := range []string{"12345678901", "+12345678901"} {
		if got := udrMSISDN(t, msisdn, sgdcodec.MSISDNEncodingLegacyPrefixed); !bytes.Equal(got, want) {
			t.Errorf("MSISDN(%q) = % X, want % X", msisdn, got, want)
		}
	}
}

func TestClientDefaultsToPlainTBCD(t *testing.T) {
	c := &Client{msisdnEncoding: sgdcodec.MSISDNEncodingTBCD}
	c.SetMSISDNEncoding("")
	if c.msisdnEncoding != sgdcodec.MSISDNEncodingTBCD {
		t.Fatalf("empty setting = %q, want tbcd", c.msisdnEncoding)
	}
	c.SetMSISDNEncoding("legacy_prefixed")
	if c.msisdnEncoding != sgdcodec.MSISDNEncodingLegacyPrefixed {
		t.Fatalf("legacy setting = %q, want legacy_prefixed", c.msisdnEncoding)
	}
}

func TestParseUDAExperimentalResultReturnsUnknownUser(t *testing.T) {
	exp, err := dcodec.NewGrouped(
		dcodec.CodeExperimentalResult, 0, dcodec.FlagMandatory,
		[]*dcodec.AVP{
			dcodec.NewUint32(dcodec.CodeVendorID, 0, dcodec.FlagMandatory, dcodec.Vendor3GPP),
			dcodec.NewUint32(dcodec.CodeExperimentalResultCode, 0, dcodec.FlagMandatory, 5001),
		},
	)
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}
	msg := &dcodec.Message{AVPs: []*dcodec.AVP{exp}}

	_, err = parseUDA(msg)
	if err == nil {
		t.Fatal("parseUDA() error = nil, want unknown user error")
	}
	unknown, ok := err.(*ErrUnknownUser)
	if !ok {
		t.Fatalf("parseUDA() error = %T, want *ErrUnknownUser", err)
	}
	if got, want := unknown.ResultCode, uint32(5001); got != want {
		t.Fatalf("ResultCode = %d, want %d", got, want)
	}
}

func TestParseUDAExperimentalResultReturnsUnsupportedUserData(t *testing.T) {
	exp, err := dcodec.NewGrouped(
		dcodec.CodeExperimentalResult, 0, dcodec.FlagMandatory,
		[]*dcodec.AVP{
			dcodec.NewUint32(dcodec.CodeVendorID, 0, dcodec.FlagMandatory, dcodec.Vendor3GPP),
			dcodec.NewUint32(dcodec.CodeExperimentalResultCode, 0, dcodec.FlagMandatory, 5009),
		},
	)
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}
	msg := &dcodec.Message{AVPs: []*dcodec.AVP{exp}}

	_, err = parseUDA(msg)
	if err == nil {
		t.Fatal("parseUDA() error = nil, want unsupported user data error")
	}
	unsupported, ok := err.(*ErrUnsupportedUserData)
	if !ok {
		t.Fatalf("parseUDA() error = %T, want *ErrUnsupportedUserData", err)
	}
	if got, want := unsupported.ResultCode, uint32(5009); got != want {
		t.Fatalf("ResultCode = %d, want %d", got, want)
	}
}
