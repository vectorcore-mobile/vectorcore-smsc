package s6c

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"

	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

func TestEncodeDecodeTBCDRoundTrip(t *testing.T) {
	input := "+15550000000"
	encoded := encodeTBCD(input)
	if got, want := decodeTBCD(encoded), "15550000000"; got != want {
		t.Fatalf("decodeTBCD(encodeTBCD(%q)) = %q, want %q", input, got, want)
	}
}

func TestParseSRIAnswerAttachedSubscriber(t *testing.T) {
	servingNode, err := dcodec.NewGrouped(
		dcodec.CodeServingNode,
		dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewString(dcodec.CodeMMEName, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, "s6a-vpc-si-01.epc.mnc435.mcc311.3gppnetwork.org"),
			dcodec.NewOctetString(dcodec.CodeMMENumberForMTSMSServing, dcodec.Vendor3GPP, dcodec.FlagVendorSpecific, encodeTBCD("15550000001")),
			dcodec.NewString(dcodec.CodeMMERealm, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, "epc.mnc435.mcc311.3gppnetwork.org"),
		},
	)
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}

	msg := &dcodec.Message{
		Header: dcodec.Header{
			CommandCode: dcodec.CmdSendRoutingInfoSM,
			AppID:       dcodec.App3GPP_S6c,
		},
		AVPs: []*dcodec.AVP{
			dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, "smsc.example;123;1"),
			dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, dcodec.DiameterSuccess),
			dcodec.NewString(dcodec.CodeUserName, 0, dcodec.FlagMandatory, "311435000070570"),
			dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, encodeTBCD("3342012832")),
			servingNode,
		},
	}

	info, err := parseSRIAnswer(msg)
	if err != nil {
		t.Fatalf("parseSRIAnswer() error = %v", err)
	}
	if got, want := info.IMSI, "311435000070570"; got != want {
		t.Fatalf("IMSI = %q, want %q", got, want)
	}
	if got, want := info.MSISDN, "3342012832"; got != want {
		t.Fatalf("MSISDN = %q, want %q", got, want)
	}
	if !info.Attached {
		t.Fatal("Attached = false, want true")
	}
	if got, want := info.MMEName, "s6a-vpc-si-01.epc.mnc435.mcc311.3gppnetwork.org"; got != want {
		t.Fatalf("MMEName = %q, want %q", got, want)
	}
	if got, want := info.MMERealm, "epc.mnc435.mcc311.3gppnetwork.org"; got != want {
		t.Fatalf("MMERealm = %q, want %q", got, want)
	}
	if got, want := info.MMENumber, "15550000001"; got != want {
		t.Fatalf("MMENumber = %q, want %q", got, want)
	}
}

func TestParseSRIAnswerServingNodeMMENumberUsesE164Digits(t *testing.T) {
	servingNode, err := dcodec.NewGrouped(
		dcodec.CodeServingNode,
		dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewString(dcodec.CodeMMEName, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, "s6a-vpc-si-01.epc.mnc435.mcc311.3gppnetwork.org"),
			dcodec.NewOctetString(dcodec.CodeMMENumberForMTSMSServing, dcodec.Vendor3GPP, dcodec.FlagVendorSpecific, []byte{0x51, 0x55, 0x00, 0x00, 0x00, 0xF1}),
			dcodec.NewString(dcodec.CodeMMERealm, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, "epc.mnc435.mcc311.3gppnetwork.org"),
		},
	)
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}

	msg := &dcodec.Message{
		Header: dcodec.Header{
			CommandCode: dcodec.CmdSendRoutingInfoSM,
			AppID:       dcodec.App3GPP_S6c,
		},
		AVPs: []*dcodec.AVP{
			dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, "smsc.example;123;3"),
			dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, dcodec.DiameterSuccess),
			servingNode,
		},
	}

	info, err := parseSRIAnswer(msg)
	if err != nil {
		t.Fatalf("parseSRIAnswer() error = %v", err)
	}
	if got, want := info.MMENumber, "15550000001"; got != want {
		t.Fatalf("MMENumber = %q, want %q", got, want)
	}
}

func TestNormalizeE164Address(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "digits", input: "15550000001", want: "15550000001"},
		{name: "plus digits", input: "+15550000001", want: "15550000001"},
		{name: "tbcd hex text", input: "5155000000f1", want: "15550000001"},
		{name: "raw tbcd bytes", input: string([]byte{0x51, 0x55, 0x00, 0x00, 0x00, 0xF1}), want: "15550000001"},
	}

	for _, tt := range tests {
		if got := NormalizeE164Address(tt.input); got != tt.want {
			t.Fatalf("%s: NormalizeE164Address(%q) = %q, want %q", tt.name, tt.input, got, tt.want)
		}
	}
}

func TestDecodeTBCDForSCAddress(t *testing.T) {
	if got, want := decodeTBCD([]byte{0x51, 0x55, 0x00, 0x00, 0x00, 0xF1}), "15550000001"; got != want {
		t.Fatalf("decodeTBCD(sc-address) = %q, want %q", got, want)
	}
}

func TestParseSRIAnswerUnattachedSubscriber(t *testing.T) {
	msg := &dcodec.Message{
		Header: dcodec.Header{
			CommandCode: dcodec.CmdSendRoutingInfoSM,
			AppID:       dcodec.App3GPP_S6c,
		},
		AVPs: []*dcodec.AVP{
			dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, "smsc.example;123;2"),
			dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, dcodec.DiameterSuccess),
			dcodec.NewString(dcodec.CodeUserName, 0, dcodec.FlagMandatory, "311435000070570"),
			dcodec.NewUint32(dcodec.CodeMWDStatusS6c, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, dcodec.MWDStatusMNRF),
		},
	}

	info, err := parseSRIAnswer(msg)
	if err != nil {
		t.Fatalf("parseSRIAnswer() error = %v", err)
	}
	if info.Attached {
		t.Fatal("Attached = true, want false")
	}
	if got, want := info.MWDStatus, dcodec.MWDStatusMNRF; got != want {
		t.Fatalf("MWDStatus = %d, want %d", got, want)
	}
}

func TestParseAlertServiceCentreUsesUserIdentifierAndCorrelation(t *testing.T) {
	userIdentifier, err := dcodec.NewGrouped(
		dcodec.CodeUserIdentifier,
		dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewString(dcodec.CodeUserName, 0, dcodec.FlagMandatory, "311435000070570"),
			dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, encodeTBCD("3342012832")),
		},
	)
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}
	servingNode, err := dcodec.NewGrouped(
		dcodec.CodeServingNode,
		dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewString(dcodec.CodeMMEName, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, "mme01.example.net"),
		},
	)
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}
	corrPayload := []byte("smsc:message-123")
	msg := &dcodec.Message{
		Header: dcodec.Header{
			CommandCode: dcodec.CmdAlertServiceCentre,
			AppID:       dcodec.App3GPP_S6c,
		},
		AVPs: []*dcodec.AVP{
			dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, "smsc.example;123;4"),
			dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, "hss.example.net"),
			dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, "example.net"),
			dcodec.NewOctetString(dcodec.CodeSCAddressS6c, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, encodeTBCD("15550000000")),
			userIdentifier,
			dcodec.NewOctetString(dcodec.CodeSMSMICorrelationID, dcodec.Vendor3GPP, dcodec.FlagVendorSpecific, corrPayload),
			servingNode,
			dcodec.NewUint32(dcodec.CodeSMSGMSCAlertEvent, dcodec.Vendor3GPP, dcodec.FlagVendorSpecific, 1),
		},
	}

	req, err := parseAlertServiceCentre(msg)
	if err != nil {
		t.Fatalf("parseAlertServiceCentre() error = %v", err)
	}
	if got, want := req.IMSI, "311435000070570"; got != want {
		t.Fatalf("IMSI = %q, want %q", got, want)
	}
	if got, want := req.MSISDN, "3342012832"; got != want {
		t.Fatalf("MSISDN = %q, want %q", got, want)
	}
	if got, want := req.AlertCorrelationID, base64.StdEncoding.EncodeToString(corrPayload); got != want {
		t.Fatalf("AlertCorrelationID = %q, want %q", got, want)
	}
	if got, want := req.ServingNode, "mme01.example.net"; got != want {
		t.Fatalf("ServingNode = %q, want %q", got, want)
	}
}

func TestBuildUserIdentifierIncludesBothIMSIAndMSISDN(t *testing.T) {
	uid := buildUserIdentifier("311435000070570", "3342012832")
	if uid == nil {
		t.Fatal("buildUserIdentifier() = nil")
	}
	children, err := dcodec.DecodeGrouped(uid)
	if err != nil {
		t.Fatalf("DecodeGrouped() error = %v", err)
	}
	if len(children) != 2 {
		t.Fatalf("len(children) = %d, want 2", len(children))
	}
}

func TestDecodeAlertCorrelationIDRoundTrip(t *testing.T) {
	raw := []byte("smsc:message-123")
	encoded := base64.StdEncoding.EncodeToString(raw)
	got, err := decodeAlertCorrelationID(encoded)
	if err != nil {
		t.Fatalf("decodeAlertCorrelationID() error = %v", err)
	}
	if string(got) != string(raw) {
		t.Fatalf("decodeAlertCorrelationID() = %q, want %q", got, raw)
	}
}

func TestParseAlertServiceCentreMaximumUEAvailabilityTimeOptional(t *testing.T) {
	ts := uint32(time.Now().Unix() + 2208988800)
	msg := &dcodec.Message{
		Header: dcodec.Header{CommandCode: dcodec.CmdAlertServiceCentre, AppID: dcodec.App3GPP_S6c},
		AVPs: []*dcodec.AVP{
			dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, "smsc.example;123;5"),
			dcodec.NewUint32(dcodec.CodeMaximumUEAvailabilityTime, dcodec.Vendor3GPP, dcodec.FlagVendorSpecific, ts),
		},
	}
	req, err := parseAlertServiceCentre(msg)
	if err != nil {
		t.Fatalf("parseAlertServiceCentre() error = %v", err)
	}
	if req.MaximumUEAvailabilityTime == nil {
		t.Fatal("MaximumUEAvailabilityTime = nil, want value")
	}
}

func experimentalResult(t *testing.T, code uint32) *dcodec.AVP {
	t.Helper()
	avp, err := dcodec.NewGrouped(dcodec.CodeExperimentalResult, 0, dcodec.FlagMandatory, []*dcodec.AVP{
		dcodec.NewUint32(dcodec.CodeVendorID, 0, dcodec.FlagMandatory, dcodec.Vendor3GPP),
		dcodec.NewUint32(dcodec.CodeExperimentalResultCode, 0, dcodec.FlagMandatory, code),
	})
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}
	return avp
}

func TestParseResultReadsExperimentalResult(t *testing.T) {
	cases := []struct {
		name             string
		avps             []*dcodec.AVP
		wantCode         uint32
		wantExperimental bool
	}{
		{"result-code", []*dcodec.AVP{dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, dcodec.DiameterSuccess)}, dcodec.DiameterSuccess, false},
		{"experimental-result", []*dcodec.AVP{experimentalResult(t, 5558)}, 5558, true},
		{"neither", nil, dcodec.DiameterUnableToComply, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := dcodec.NewRequest(dcodec.CmdReportSMDeliveryStatus, dcodec.App3GPP_S6c)
			b.Add(tc.avps...)
			code, experimental := parseResult(b.Build())
			if code != tc.wantCode || experimental != tc.wantExperimental {
				t.Fatalf("parseResult() = %d, %v; want %d, %v", code, experimental, tc.wantCode, tc.wantExperimental)
			}
		})
	}
}

func TestParseAnswersClassifyHSSErrors(t *testing.T) {
	answer := func(avps ...*dcodec.AVP) *dcodec.Message {
		b := dcodec.NewRequest(dcodec.CmdSendRoutingInfoSM, dcodec.App3GPP_S6c)
		b.Add(avps...)
		return b.Build()
	}

	// 3GPP DIAMETER_ERROR_USER_UNKNOWN → subscriber unknown.
	_, err := parseSRIAnswer(answer(experimentalResult(t, 5001)))
	var unknown *ErrUnknownUser
	if !errors.As(err, &unknown) || unknown.ResultCode != 5001 {
		t.Fatalf("SRI-SM user unknown: err = %v, want ErrUnknownUser(5001)", err)
	}

	// Base Result-Code 5001 is DIAMETER_AVP_UNSUPPORTED, not user unknown.
	_, err = parseSRIAnswer(answer(dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, 5001)))
	var result *ErrResult
	if !errors.As(err, &result) || result.Experimental {
		t.Fatalf("SRI-SM base 5001: err = %v, want non-experimental ErrResult", err)
	}

	// MWD list full on RSDS is an error, but not "subscriber unknown".
	_, err = parseRSDSAnswer(answer(experimentalResult(t, 5558)))
	if !errors.As(err, &result) || result.ResultCode != 5558 || !result.Experimental || result.Command != "RSDS" {
		t.Fatalf("RSDS MWD list full: err = %v, want ErrResult(RSDS, 5558, experimental)", err)
	}
	if errors.As(err, &unknown) {
		t.Fatal("MWD list full must not be reported as an unknown subscriber")
	}

	// HSS-side failure on SRI-SM.
	_, err = parseSRIAnswer(answer(dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, dcodec.DiameterUnableToComply)))
	if !errors.As(err, &result) || result.ResultCode != dcodec.DiameterUnableToComply {
		t.Fatalf("SRI-SM unable to comply: err = %v, want ErrResult(5012)", err)
	}
}

func TestBuildSMDeliveryOutcomeUsesMMEDeliveryOutcome(t *testing.T) {
	diag := uint32(1)
	outcome, err := buildSMDeliveryOutcome(dcodec.SMDeliveryCauseAbsentUser, &diag)
	if err != nil {
		t.Fatalf("buildSMDeliveryOutcome() error = %v", err)
	}
	if outcome.Code != dcodec.CodeSMDeliveryOutcomeS6c || outcome.VendorID != dcodec.Vendor3GPP {
		t.Fatalf("outer AVP = %d/%d, want SM-Delivery-Outcome", outcome.Code, outcome.VendorID)
	}
	nodes, err := dcodec.DecodeGrouped(outcome)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("decode SM-Delivery-Outcome = %v, %v", nodes, err)
	}
	if nodes[0].Code != dcodec.CodeMMEDeliveryOutcome || nodes[0].VendorID != dcodec.Vendor3GPP {
		t.Fatalf("node outcome AVP = %d, want MME-Delivery-Outcome (3317)", nodes[0].Code)
	}
	inner, err := dcodec.DecodeGrouped(nodes[0])
	if err != nil {
		t.Fatalf("decode MME-Delivery-Outcome: %v", err)
	}
	var gotCause, gotDiag uint32
	for _, a := range inner {
		v, _ := a.Uint32()
		switch a.Code {
		case dcodec.CodeSMDeliveryCause:
			gotCause = v
		case dcodec.CodeAbsentUserDiagnosticSM:
			gotDiag = v
		}
	}
	if gotCause != dcodec.SMDeliveryCauseAbsentUser || gotDiag != 1 {
		t.Fatalf("cause/diagnostic = %d/%d, want %d/1", gotCause, gotDiag, dcodec.SMDeliveryCauseAbsentUser)
	}
}

// MME-Number-for-MT-SMS is plain TBCD. A number whose bytes look like a
// length+NOA prefix ("1058...": 01 85 ...) must not lose its first digits.
func TestParseSRIAnswerMMENumberStrictTBCD(t *testing.T) {
	servingNode, err := dcodec.NewGrouped(dcodec.CodeServingNode, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewString(dcodec.CodeMMEName, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, "mme-a.epc.example.net"),
			dcodec.NewOctetString(dcodec.CodeMMENumberForMTSMSS6c, dcodec.Vendor3GPP, dcodec.FlagVendorSpecific, encodeTBCD("105800000001")),
		})
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}
	msg := &dcodec.Message{AVPs: []*dcodec.AVP{
		dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, dcodec.DiameterSuccess),
		servingNode,
	}}
	info, err := parseSRIAnswer(msg)
	if err != nil {
		t.Fatalf("parseSRIAnswer() error = %v", err)
	}
	if info.MMENumber != "105800000001" {
		t.Fatalf("MMENumber = %q, want 105800000001", info.MMENumber)
	}
}

func TestDecodeMMENumber(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{"ASCII digits (Cisco-style deployments)", []byte("15550000001"), "15550000001"},
		{"plain TBCD", encodeTBCD("15550000001"), "15550000001"},
		{"TBCD that looks like len+NOA (issue #2)", encodeTBCD("105800000001"), "105800000001"},
		{"TBCD containing 3s", encodeTBCD("13320003"), "13320003"},
		{"TBCD country code 98", encodeTBCD("989100000001"), "989100000001"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := decodeMMENumber(tc.data); got != tc.want {
				t.Fatalf("decodeMMENumber(% X) = %q, want %q", tc.data, got, tc.want)
			}
		})
	}
}

func TestParseSRIAnswerAcceptsASCIIMMENumber(t *testing.T) {
	servingNode, err := dcodec.NewGrouped(dcodec.CodeServingNode, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewString(dcodec.CodeMMEName, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, "mme-cisco.epc.example.net"),
			dcodec.NewOctetString(dcodec.CodeMMENumberForMTSMSS6c, dcodec.Vendor3GPP, dcodec.FlagVendorSpecific, []byte("15550000001")),
		})
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}
	info, err := parseSRIAnswer(&dcodec.Message{AVPs: []*dcodec.AVP{
		dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, dcodec.DiameterSuccess),
		servingNode,
	}})
	if err != nil {
		t.Fatalf("parseSRIAnswer() error = %v", err)
	}
	if info.MMENumber != "15550000001" {
		t.Fatalf("MMENumber = %q, want 15550000001", info.MMENumber)
	}
}
