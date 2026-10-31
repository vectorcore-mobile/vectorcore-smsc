package sgd

import (
	"encoding/base64"
	"errors"
	"testing"

	sgdcodec "github.com/svinson1121/vectorcore-smsc/internal/codec/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

func TestSelectPeerForMMELockedPrefersDirectMMEPeer(t *testing.T) {
	s := NewServer("tcp", ":0", "smsc.example.net", "example.net", "tbcd", nil)
	direct := diameter.NewPeer(diameter.Config{Name: "mme-direct", Application: "sgd"})
	proxy := diameter.NewPeer(diameter.Config{Name: "dra01", Application: "sgd"})
	proxy.RemoteFQDN = "dra01.example.net"

	s.peers["mme01.example.net"] = direct
	s.peers[proxy.RemoteFQDN] = proxy
	s.outboundPeers["dra01"] = proxy

	got, viaProxy := s.selectPeerForMMELocked("mme01.example.net")
	if got != direct {
		t.Fatalf("selected peer = %p, want direct %p", got, direct)
	}
	if viaProxy {
		t.Fatal("viaProxy = true, want false")
	}
}

func TestSelectPeerForMMELockedFallsBackToActiveOutboundProxy(t *testing.T) {
	s := NewServer("tcp", ":0", "smsc.example.net", "example.net", "tbcd", nil)
	proxy := diameter.NewPeer(diameter.Config{Name: "dra01", Application: "sgd"})
	proxy.RemoteFQDN = "dra01.example.net"
	proxy.RemoteRealm = "example.net"

	s.outboundPeers["dra01"] = proxy
	s.peers[proxy.RemoteFQDN] = proxy

	got, viaProxy := s.selectPeerForMMELocked("mme01.example.net")
	if got != proxy {
		t.Fatalf("selected peer = %p, want proxy %p", got, proxy)
	}
	if !viaProxy {
		t.Fatal("viaProxy = false, want true")
	}
	if !s.HasPeerForMME("mme01.example.net") {
		t.Fatal("HasPeerForMME() = false, want true via proxy fallback")
	}
}

func TestDestinationRealmForPeerFallsBackToMMEHostRealm(t *testing.T) {
	if got, want := destinationRealmForPeer("mme01.epc.example.net", nil), "epc.example.net"; got != want {
		t.Fatalf("destinationRealmForPeer() = %q, want %q", got, want)
	}
}

func TestRoutePeerForMMEReturnsProxyDetails(t *testing.T) {
	s := NewServer("tcp", ":0", "smsc.example.net", "example.net", "tbcd", nil)
	proxy := diameter.NewPeer(diameter.Config{Name: "dra01", Application: "sgd"})
	proxy.RemoteFQDN = "dra01.example.net"

	s.outboundPeers["dra01"] = proxy
	s.peers[proxy.RemoteFQDN] = proxy

	peerName, viaProxy, ok := s.RoutePeerForMME("mme01.example.net")
	if !ok {
		t.Fatal("RoutePeerForMME() ok = false, want true")
	}
	if got, want := peerName, "dra01.example.net"; got != want {
		t.Fatalf("peerName = %q, want %q", got, want)
	}
	if !viaProxy {
		t.Fatal("viaProxy = false, want true")
	}
}

func TestDispatchRoutesOFAAnswerToPendingSender(t *testing.T) {
	s := NewServer("tcp", ":0", "smsc.example.net", "example.net", "tbcd", nil)
	replyCh := make(chan *dcodec.Message, 1)
	hopByHop := uint32(12345)
	s.trackPendingOFR(hopByHop, replyCh)
	defer s.untrackPendingOFR(hopByHop)

	ans := &dcodec.Message{
		Header: dcodec.Header{
			Flags:       dcodec.FlagProxiable,
			CommandCode: dcodec.CmdMTForwardShortMessage,
			AppID:       dcodec.App3GPP_SGd,
			HopByHop:    hopByHop,
		},
		AVPs: []*dcodec.AVP{
			dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, dcodec.DiameterAVPUnsupported),
		},
	}

	s.dispatch(nil, ans)

	select {
	case got := <-replyCh:
		if got != ans {
			t.Fatal("pending OFA mismatch")
		}
	default:
		t.Fatal("pending OFA was not delivered to sender")
	}
}

func TestCompleteMORemovesPendingEntry(t *testing.T) {
	s := NewServer("tcp", ":0", "smsc.example.net", "example.net", "tbcd", nil)
	p := diameter.NewPeer(diameter.Config{Name: "mme01", Application: "sgd"})
	req := dcodec.NewRequest(dcodec.CmdMOForwardShortMessage, dcodec.App3GPP_SGd).Build()

	s.trackPendingMO("message-123", p, req)
	if !s.CompleteMO("message-123", []byte{0x03, 0x01}) {
		t.Fatal("CompleteMO() = false, want true")
	}
	if s.CompleteMO("message-123", []byte{0x03, 0x01}) {
		t.Fatal("CompleteMO() second call = true, want false")
	}
}

func TestMapISCResultToSGdAnswerBuildsSubmitReportAck(t *testing.T) {
	resultCode, smRPUI := mapISCResultToSGdAnswer([]byte{0x02, 0x01, 0x41, 0x02, 0x00, 0x00})
	if got, want := resultCode, dcodec.DiameterSuccess; got != want {
		t.Fatalf("resultCode = %d, want %d", got, want)
	}
	if len(smRPUI) != 9 {
		t.Fatalf("len(smRPUI) = %d, want 9", len(smRPUI))
	}
	if got, want := smRPUI[0], byte(0x01); got != want {
		t.Fatalf("SM-RP-UI[0] = 0x%02x, want 0x%02x", got, want)
	}
	if got, want := smRPUI[1], byte(0x00); got != want {
		t.Fatalf("SM-RP-UI[1] = 0x%02x, want 0x%02x", got, want)
	}
}

func TestAuthSessionStateFromRequestUsesRequestValue(t *testing.T) {
	req := dcodec.NewRequest(dcodec.CmdMOForwardShortMessage, dcodec.App3GPP_SGd)
	req.Add(dcodec.NewUint32(dcodec.CodeAuthSessionState, 0, dcodec.FlagMandatory, 7))
	avp := authSessionStateFromRequest(req.Build())
	if got, err := avp.Uint32(); err != nil || got != 7 {
		t.Fatalf("authSessionStateFromRequest() = (%d, %v), want (7, nil)", got, err)
	}
}

func TestAuthSessionStateFromRequestDefaultsToNoStateMaintained(t *testing.T) {
	avp := authSessionStateFromRequest(nil)
	if got, err := avp.Uint32(); err != nil || got != dcodec.AuthSessionStateNoStateMaintained {
		t.Fatalf("default auth session state = (%d, %v), want (%d, nil)", got, err, dcodec.AuthSessionStateNoStateMaintained)
	}
}

func TestParseAlertServiceCentreReadsCorrelationAndSCAddress(t *testing.T) {
	msg := &dcodec.Message{
		Header: dcodec.Header{CommandCode: dcodec.CmdAlertServiceCentre, AppID: dcodec.App3GPP_SGd},
		AVPs: []*dcodec.AVP{
			dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, "smsc.example;123;alr"),
			dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, "mme01.example.net"),
			dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, "example.net"),
			dcodec.NewString(dcodec.CodeUserName, 0, dcodec.FlagMandatory, "311435000070570"),
			dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, sgdcodec.EncodeMSISDN("3342012832", sgdcodec.MSISDNEncodingTBCD)),
			dcodec.NewOctetString(dcodec.CodeSCAddress, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, []byte("15550000000")),
			dcodec.NewOctetString(dcodec.CodeSMSMICorrelationID, dcodec.Vendor3GPP, dcodec.FlagVendorSpecific, []byte("smsc:message-123")),
		},
	}

	req, err := parseAlertServiceCentre(msg, sgdcodec.AddressOptions{SCAddress: "ascii_digits"})
	if err != nil {
		t.Fatalf("parseAlertServiceCentre() error = %v", err)
	}
	if got, want := req.MSISDN, "3342012832"; got != want {
		t.Fatalf("MSISDN = %q, want %q", got, want)
	}
	if got, want := req.SCAddress, "15550000000"; got != want {
		t.Fatalf("SCAddress = %q, want %q", got, want)
	}
	if got, want := req.AlertCorrelationID, base64.StdEncoding.EncodeToString([]byte("smsc:message-123")); got != want {
		t.Fatalf("AlertCorrelationID = %q, want %q", got, want)
	}
}

func TestOFAErrorClassifiesAnswer(t *testing.T) {
	expResult := func(code uint32) *dcodec.AVP {
		avp, err := dcodec.NewGrouped(dcodec.CodeExperimentalResult, 0, dcodec.FlagMandatory, []*dcodec.AVP{
			dcodec.NewUint32(dcodec.CodeVendorID, 0, dcodec.FlagMandatory, dcodec.Vendor3GPP),
			dcodec.NewUint32(dcodec.CodeExperimentalResultCode, 0, dcodec.FlagMandatory, code),
		})
		if err != nil {
			t.Fatalf("NewGrouped() error = %v", err)
		}
		return avp
	}
	failureCause := func(cause uint32) *dcodec.AVP {
		avp, err := dcodec.NewGrouped(dcodec.CodeSMDeliveryFailureCause, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, []*dcodec.AVP{
			dcodec.NewUint32(dcodec.CodeSMEnumeratedDeliveryFailureCause, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, cause),
		})
		if err != nil {
			t.Fatalf("NewGrouped() error = %v", err)
		}
		return avp
	}
	answer := func(avps ...*dcodec.AVP) *dcodec.Message {
		b := dcodec.NewRequest(dcodec.CmdMTForwardShortMessage, dcodec.App3GPP_SGd)
		b.Add(avps...)
		return b.Build()
	}

	if err := ofaError(answer(dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, dcodec.DiameterSuccess))); err != nil {
		t.Fatalf("success: err = %v, want nil", err)
	}

	cases := []struct {
		name      string
		ans       *dcodec.Message
		wantCode  uint32
		wantExp   bool
		wantCause *uint32
	}{
		{"base 3002", answer(dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, dcodec.DiameterUnableToDeliver)), dcodec.DiameterUnableToDeliver, false, nil},
		{"absent user", answer(expResult(ResultErrorAbsentUser)), ResultErrorAbsentUser, true, nil},
		{"memory full", answer(expResult(ResultErrorSMDeliveryFailure), failureCause(DeliveryFailureMemoryCapacityExceeded)), ResultErrorSMDeliveryFailure, true, u32(DeliveryFailureMemoryCapacityExceeded)},
		{"protocol error", answer(expResult(ResultErrorSMDeliveryFailure), failureCause(DeliveryFailureEquipmentProtocolError)), ResultErrorSMDeliveryFailure, true, u32(DeliveryFailureEquipmentProtocolError)},
		{"no result", answer(), dcodec.DiameterUnableToComply, false, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var e *OFAResultError
			if !errors.As(ofaError(tc.ans), &e) {
				t.Fatalf("err is not *OFAResultError")
			}
			if e.ResultCode != tc.wantCode || e.Experimental != tc.wantExp {
				t.Fatalf("got %d/%v, want %d/%v", e.ResultCode, e.Experimental, tc.wantCode, tc.wantExp)
			}
			switch {
			case tc.wantCause == nil && e.DeliveryFailureCause != nil:
				t.Fatalf("DeliveryFailureCause = %d, want nil", *e.DeliveryFailureCause)
			case tc.wantCause != nil && (e.DeliveryFailureCause == nil || *e.DeliveryFailureCause != *tc.wantCause):
				t.Fatalf("DeliveryFailureCause = %v, want %d", e.DeliveryFailureCause, *tc.wantCause)
			}
		})
	}
}

func u32(v uint32) *uint32 { return &v }

// An ALR's MSISDN is decoded per the configured encoding, and always to bare
// digits so it matches stored destination numbers.
func TestParseAlertServiceCentreMSISDNEncodings(t *testing.T) {
	alr := func(msisdn []byte) *dcodec.Message {
		return &dcodec.Message{
			Header: dcodec.Header{CommandCode: dcodec.CmdAlertServiceCentre, AppID: dcodec.App3GPP_SGd},
			AVPs: []*dcodec.AVP{
				dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, "mme;1;alr"),
				dcodec.NewString(dcodec.CodeUserName, 0, dcodec.FlagMandatory, "432600000000555"),
				dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, msisdn),
				dcodec.NewOctetString(dcodec.CodeSCAddress, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, []byte{0x51, 0x55, 0x00, 0x00, 0x00, 0xF0}),
			},
		}
	}
	cases := []struct {
		name   string
		msisdn []byte
		enc    sgdcodec.MSISDNEncoding
	}{
		{"tbcd", []byte{0x89, 0x19, 0x00, 0x00, 0x50, 0x55}, sgdcodec.MSISDNEncodingTBCD},
		{"legacy prefixed", []byte{0x07, 0x91, 0x89, 0x19, 0x00, 0x00, 0x50, 0x55}, sgdcodec.MSISDNEncodingLegacyPrefixed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := parseAlertServiceCentre(alr(tc.msisdn), sgdcodec.AddressOptions{MSISDN: tc.enc, SCAddress: "tbcd"})
			if err != nil {
				t.Fatalf("parseAlertServiceCentre() error = %v", err)
			}
			if req.MSISDN != "989100000555" {
				t.Fatalf("MSISDN = %q, want 989100000555 (bare digits)", req.MSISDN)
			}
			if req.SCAddress != "15550000000" {
				t.Fatalf("SCAddress = %q, want 15550000000", req.SCAddress)
			}
		})
	}
}

func TestServerMSISDNEncodingDefaultsToTBCD(t *testing.T) {
	s := NewServer("tcp", "127.0.0.1:0", "smsc", "realm", "tbcd", nil)
	if got := s.addressOptions().MSISDN; got != sgdcodec.MSISDNEncodingTBCD {
		t.Fatalf("default MSISDN encoding = %q, want tbcd", got)
	}
	s.SetMSISDNEncoding("legacy_prefixed")
	if got := s.addressOptions().MSISDN; got != sgdcodec.MSISDNEncodingLegacyPrefixed {
		t.Fatalf("MSISDN encoding = %q, want legacy_prefixed", got)
	}
}
