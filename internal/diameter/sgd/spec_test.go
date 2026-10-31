package sgd

import (
	"bytes"
	"testing"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	sgdcodec "github.com/svinson1121/vectorcore-smsc/internal/codec/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

func TestBuildAlertSCRequestIncludesRequiredAVPs(t *testing.T) {
	msg := buildAlertSCRequest(
		diameter.Config{LocalFQDN: "smsc.example.net", LocalRealm: "example.net"},
		"mme01.epc.example.net",
		"epc.example.net",
		"3342012832",
		"+15550000000",
		"tbcd",
	)

	if !msg.IsRequest() {
		t.Fatal("expected Alert-SC to be a request")
	}
	if got, want := msg.Header.CommandCode, dcodec.CmdAlertServiceCentre; got != want {
		t.Fatalf("command = %d, want %d", got, want)
	}
	if got, want := msg.Header.AppID, dcodec.App3GPP_SGd; got != want {
		t.Fatalf("app_id = %d, want %d", got, want)
	}
	requiredBase := []uint32{
		dcodec.CodeSessionID,
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
	if msg.FindAVP(dcodec.CodeMSISDN, dcodec.Vendor3GPP) == nil {
		t.Fatal("missing MSISDN AVP")
	}
	if msg.FindAVP(dcodec.CodeSCAddress, dcodec.Vendor3GPP) == nil {
		t.Fatal("missing SC-Address AVP")
	}
}

func TestBuildOFRRequestIncludesRequiredAVPs(t *testing.T) {
	canonical := &codec.Message{}
	canonical.Source.MSISDN = "15551234567"
	canonical.Destination.MSISDN = "3342012832"
	canonical.Destination.IMSI = "311435000070570"
	canonical.Destination.MMENumber = "15550000001"
	canonical.TPMR = 7
	canonical.DCS = 0
	tpData, err := tpdu.EncodeDeliver(canonical)
	if err != nil {
		t.Fatalf("EncodeDeliver() error = %v", err)
	}
	avps, err := sgdcodec.EncodeOFR(canonical, "+15550000000", "tbcd")
	if err != nil {
		t.Fatalf("EncodeOFR() error = %v", err)
	}
	msg := buildOFRRequest("smsc.example.net", "example.net", "mme01.epc.example.net", "epc.example.net", avps)

	if !msg.IsRequest() {
		t.Fatal("expected OFR to be a request")
	}
	if got, want := msg.Header.CommandCode, dcodec.CmdMTForwardShortMessage; got != want {
		t.Fatalf("command = %d, want %d", got, want)
	}
	if got, want := msg.Header.AppID, dcodec.App3GPP_SGd; got != want {
		t.Fatalf("app_id = %d, want %d", got, want)
	}
	requiredBase := []uint32{
		dcodec.CodeSessionID,
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
	if avp := msg.FindAVP(dcodec.CodeSMRPUI, dcodec.Vendor3GPP); avp == nil {
		t.Fatal("missing SM-RP-UI")
	} else if string(avp.Data) != string(tpData) {
		t.Fatal("SM-RP-UI payload mismatch")
	}
	if msg.FindAVP(dcodec.CodeSMRPMTI, dcodec.Vendor3GPP) != nil {
		t.Fatal("unexpected SM-RP-MTI AVP on normal MT SGd request")
	}
	if msg.FindAVP(dcodec.CodeSCAddress, dcodec.Vendor3GPP) == nil {
		t.Fatal("missing SC-Address AVP")
	}
	if avp := msg.FindAVP(dcodec.CodeSCAddress, dcodec.Vendor3GPP); avp != nil {
		want := []byte{0x51, 0x55, 0x00, 0x00, 0x00, 0xF0}
		if !bytes.Equal(avp.Data, want) {
			t.Fatalf("SC-Address bytes = %x, want %x", avp.Data, want)
		}
	}
	if avp := msg.FindAVP(dcodec.CodeUserName, 0); avp == nil {
		t.Fatal("missing User-Name AVP")
	} else if got, want := string(avp.Data), canonical.Destination.IMSI; got != want {
		t.Fatalf("User-Name = %q, want %q", got, want)
	}
	if avp := msg.FindAVP(dcodec.CodeMMENumberForMTSMSServing, dcodec.Vendor3GPP); avp == nil {
		t.Fatal("missing MME-Number-for-MT-SMS AVP on MT SGd request")
	} else {
		want := []byte{0x51, 0x55, 0x00, 0x00, 0x00, 0xF1}
		if !bytes.Equal(avp.Data, want) {
			t.Fatalf("MME-Number-for-MT-SMS bytes = %x, want %x", avp.Data, want)
		}
	}
	if msg.FindAVP(dcodec.CodeMMENumberForMTSMS, dcodec.Vendor3GPP) != nil {
		t.Fatal("unexpected legacy MME-Number-for-MT-SMS AVP 1607 on MT SGd request")
	}
	if msg.FindAVP(dcodec.CodeMSISDN, dcodec.Vendor3GPP) != nil {
		t.Fatal("unexpected MSISDN AVP on MT SGd request")
	}
}

func TestBuildOFRRequestUsesProvidedSCAddress(t *testing.T) {
	canonical := &codec.Message{}
	canonical.Source.MSISDN = "15551234567"
	canonical.Destination.MSISDN = "3324108223"
	canonical.Destination.IMSI = "311435000070570"
	canonical.Destination.MMENumber = "15550000001"
	canonical.TPMR = 7
	canonical.DCS = 0

	avps, err := sgdcodec.EncodeOFR(canonical, "+15550000000", "tbcd")
	if err != nil {
		t.Fatalf("EncodeOFR() error = %v", err)
	}
	msg := buildOFRRequest("smsc.example.net", "example.net", "mme01.epc.example.net", "epc.example.net", avps)

	scAddr := msg.FindAVP(dcodec.CodeSCAddress, dcodec.Vendor3GPP)
	if scAddr == nil {
		t.Fatal("missing SC-Address AVP")
	}
	want := []byte{0x51, 0x55, 0x00, 0x00, 0x00, 0xF0}
	if !bytes.Equal(scAddr.Data, want) {
		t.Fatalf("SC-Address bytes = %x, want %x", scAddr.Data, want)
	}
}

func TestBuildOFRRequestEncodesSCAddressAsASCIIDigitsWhenConfigured(t *testing.T) {
	canonical := &codec.Message{}
	canonical.Source.MSISDN = "15551234567"
	canonical.Destination.MSISDN = "3324108223"
	canonical.Destination.IMSI = "311435000070570"
	canonical.Destination.MMENumber = "15550000001"

	avps, err := sgdcodec.EncodeOFR(canonical, "+15550000000", "ascii_digits")
	if err != nil {
		t.Fatalf("EncodeOFR() error = %v", err)
	}
	msg := buildOFRRequest("smsc.example.net", "example.net", "mme01.epc.example.net", "epc.example.net", avps)

	scAddr := msg.FindAVP(dcodec.CodeSCAddress, dcodec.Vendor3GPP)
	if scAddr == nil {
		t.Fatal("missing SC-Address AVP")
	}
	want := []byte("15550000000")
	if !bytes.Equal(scAddr.Data, want) {
		t.Fatalf("SC-Address bytes = %x, want %x", scAddr.Data, want)
	}
}

func TestBuildOFRRequestCarriesStatusReportForOriginalSender(t *testing.T) {
	scts := time.Date(2026, 10, 30, 17, 52, 38, 0, time.UTC)
	dt := time.Date(2026, 10, 30, 17, 52, 45, 0, time.UTC)
	report, err := tpdu.EncodeStatusReport(0x2A, codec.Address{MSISDN: "989100000111"}, scts, dt, tpdu.STReceivedBySME)
	if err != nil {
		t.Fatalf("EncodeStatusReport() error = %v", err)
	}

	canonical := &codec.Message{
		Kind:    codec.MessageKindStatusReport,
		RawTPDU: report,
	}
	canonical.Destination.MSISDN = "989100000555"
	canonical.Destination.IMSI = "432600000000555"
	canonical.Destination.MMENumber = "15550000001"

	avps, err := sgdcodec.EncodeOFR(canonical, "+15550000000", "tbcd")
	if err != nil {
		t.Fatalf("EncodeOFR() error = %v", err)
	}
	msg := buildOFRRequest("smsc.example.net", "example.net", "mme-a.epc.example.net", "epc.example.net", avps)

	if got, want := msg.Header.CommandCode, dcodec.CmdMTForwardShortMessage; got != want {
		t.Fatalf("command = %d, want %d (MT-Forward-Short-Message)", got, want)
	}
	avp := msg.FindAVP(dcodec.CodeSMRPUI, dcodec.Vendor3GPP)
	if avp == nil {
		t.Fatal("missing SM-RP-UI")
	}
	if !bytes.Equal(avp.Data, report) {
		t.Fatalf("SM-RP-UI = % X, want status report % X", avp.Data, report)
	}
	decoded, err := tpdu.Decode(avp.Data)
	if err != nil {
		t.Fatalf("decode SM-RP-UI: %v", err)
	}
	if decoded.TPMR != 0x2A || decoded.TPStatus != tpdu.STReceivedBySME || decoded.Source.MSISDN != "989100000111" {
		t.Fatalf("decoded report = MR 0x%02X ST 0x%02X RA %q", decoded.TPMR, decoded.TPStatus, decoded.Source.MSISDN)
	}
	if avp := msg.FindAVP(dcodec.CodeUserName, 0); avp == nil || string(avp.Data) != "432600000000555" {
		t.Fatalf("User-Name = %v, want original sender IMSI 432600000000555", avp)
	}
	if msg.FindAVP(dcodec.CodeMMENumberForMTSMSServing, dcodec.Vendor3GPP) == nil {
		t.Fatal("missing MME-Number-for-MT-SMS AVP")
	}
	if msg.FindAVP(dcodec.CodeSCAddress, dcodec.Vendor3GPP) == nil {
		t.Fatal("missing SC-Address AVP")
	}
}
