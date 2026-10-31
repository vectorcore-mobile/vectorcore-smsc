package s6c

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

// diameterErrorUserUnknown is DIAMETER_ERROR_USER_UNKNOWN (TS 29.338), sent
// in Experimental-Result with Vendor-Id 10415. As a base Result-Code, 5001 is
// DIAMETER_AVP_UNSUPPORTED instead.
const diameterErrorUserUnknown uint32 = 5001

// ErrUnknownUser means the HSS does not know the subscriber.
type ErrUnknownUser struct {
	ResultCode uint32
}

func (e *ErrUnknownUser) Error() string {
	return fmt.Sprintf("s6c: HSS returned result-code %d (subscriber unknown)", e.ResultCode)
}

// ErrResult is any other non-success S6c answer, e.g. MWD list full (5558)
// or an HSS-side failure. Unlike ErrUnknownUser it says nothing about
// whether the subscriber exists.
type ErrResult struct {
	Command      string
	ResultCode   uint32
	Experimental bool // code came from Experimental-Result (vendor 10415)
}

func (e *ErrResult) Error() string {
	kind := "result-code"
	if e.Experimental {
		kind = "experimental-result-code"
	}
	return fmt.Sprintf("s6c: %s answer %s %d", e.Command, kind, e.ResultCode)
}

// answerError classifies a non-success answer.
func answerError(command string, resultCode uint32, experimental bool) error {
	if experimental && resultCode == diameterErrorUserUnknown {
		return &ErrUnknownUser{ResultCode: resultCode}
	}
	return &ErrResult{Command: command, ResultCode: resultCode, Experimental: experimental}
}

func parseSRIAnswer(msg *dcodec.Message) (*RoutingInfo, error) {
	resultCode, experimental := parseResult(msg)
	if resultCode != dcodec.DiameterSuccess {
		return nil, answerError("SRI-SM", resultCode, experimental)
	}

	info := &RoutingInfo{
		SessionID:  parseStringAVP(msg, dcodec.CodeSessionID, 0),
		ResultCode: resultCode,
		IMSI:       parseStringAVP(msg, dcodec.CodeUserName, 0),
	}
	if msisdn := msg.FindAVP(dcodec.CodeMSISDN, dcodec.Vendor3GPP); msisdn != nil {
		info.MSISDN = decodeTBCD(msisdn.Data)
	}
	if mwd := msg.FindAVP(dcodec.CodeMWDStatusS6c, dcodec.Vendor3GPP); mwd != nil {
		info.MWDStatus, _ = mwd.Uint32()
	}
	if servingNode := msg.FindAVP(dcodec.CodeServingNode, dcodec.Vendor3GPP); servingNode != nil {
		children, err := dcodec.DecodeGrouped(servingNode)
		if err != nil {
			return nil, fmt.Errorf("s6c: decode Serving-Node: %w", err)
		}
		for _, child := range children {
			switch {
			case child.Code == dcodec.CodeMMEName && child.VendorID == dcodec.Vendor3GPP:
				info.MMEName = string(child.Data)
			case (child.Code == dcodec.CodeMMENumberForMTSMSS6c || child.Code == dcodec.CodeMMENumberForMTSMSServing) && child.VendorID == dcodec.Vendor3GPP:
				info.MMENumber = NormalizeE164Address(string(child.Data))
			case child.Code == dcodec.CodeMMERealm && child.VendorID == dcodec.Vendor3GPP:
				info.MMERealm = string(child.Data)
			}
		}
		info.Attached = info.MMEName != ""
	}
	return info, nil
}

func parseRSDSAnswer(msg *dcodec.Message) (*ReportDeliveryResult, error) {
	resultCode, experimental := parseResult(msg)
	result := &ReportDeliveryResult{
		SessionID:  parseStringAVP(msg, dcodec.CodeSessionID, 0),
		ResultCode: resultCode,
	}
	if result.ResultCode != dcodec.DiameterSuccess {
		return nil, answerError("RSDS", resultCode, experimental)
	}
	if mwd := msg.FindAVP(dcodec.CodeMWDStatusS6c, dcodec.Vendor3GPP); mwd != nil {
		result.MWDStatus, _ = mwd.Uint32()
	}
	return result, nil
}

func parseAlertServiceCentre(msg *dcodec.Message) (AlertServiceCentreRequest, error) {
	req := AlertServiceCentreRequest{
		SessionID:   parseStringAVP(msg, dcodec.CodeSessionID, 0),
		OriginHost:  parseStringAVP(msg, dcodec.CodeOriginHost, 0),
		OriginRealm: parseStringAVP(msg, dcodec.CodeOriginRealm, 0),
	}
	if req.SessionID == "" {
		return AlertServiceCentreRequest{}, fmt.Errorf("missing Session-Id")
	}
	req.IMSI, req.MSISDN = parseUserIdentifier(msg)
	if req.IMSI == "" {
		req.IMSI = parseStringAVP(msg, dcodec.CodeUserName, 0)
	}
	if scAddr := msg.FindAVP(dcodec.CodeSCAddressS6c, dcodec.Vendor3GPP); scAddr != nil {
		req.SCAddress = decodeTBCD(scAddr.Data)
	}
	if req.MSISDN == "" {
		if msisdn := msg.FindAVP(dcodec.CodeMSISDN, dcodec.Vendor3GPP); msisdn != nil {
			req.MSISDN = decodeTBCD(msisdn.Data)
		}
	}
	if corr := msg.FindAVP(dcodec.CodeSMSMICorrelationID, dcodec.Vendor3GPP); corr != nil && len(corr.Data) > 0 {
		req.AlertCorrelationID = base64.StdEncoding.EncodeToString(corr.Data)
	}
	if alertReason := msg.FindAVP(dcodec.CodeAlertReason, dcodec.Vendor3GPP); alertReason != nil {
		req.AlertReason, _ = alertReason.Uint32()
	}
	if servingNode := msg.FindAVP(dcodec.CodeServingNode, dcodec.Vendor3GPP); servingNode != nil {
		req.ServingNode = parseServingNodeMME(servingNode)
	}
	if maxAvail := msg.FindAVP(dcodec.CodeMaximumUEAvailabilityTime, dcodec.Vendor3GPP); maxAvail != nil && len(maxAvail.Data) == 4 {
		if secs, err := maxAvail.Uint32(); err == nil {
			t := decodeDiameterTime(secs)
			req.MaximumUEAvailabilityTime = &t
		}
	}
	if alertEvent := msg.FindAVP(dcodec.CodeSMSGMSCAlertEvent, dcodec.Vendor3GPP); alertEvent != nil {
		req.SMSGMSCAlertEvent, _ = alertEvent.Uint32()
	}
	if diag := msg.FindAVP(dcodec.CodeAbsentUserDiagnosticSM, dcodec.Vendor3GPP); diag != nil {
		req.AbsentUserDiagnosticSM, _ = diag.Uint32()
	}
	return req, nil
}

func sendAlertServiceCentreAnswer(peer *diameter.Peer, req *dcodec.Message, resultCode uint32) error {
	cfg := peer.Cfg()
	b := dcodec.NewAnswer(req)
	b.Add(
		dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, parseStringAVP(req, dcodec.CodeSessionID, 0)),
		dcodec.NewUint32(dcodec.CodeAuthSessionState, 0, dcodec.FlagMandatory, dcodec.AuthSessionStateNoStateMaintained),
		dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, resultCode),
		dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, cfg.LocalFQDN),
		dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, cfg.LocalRealm),
	)
	return peer.Send(b.Build())
}

func parseResultCode(msg *dcodec.Message) uint32 {
	code, _ := parseResult(msg)
	return code
}

// parseResult returns the answer's Result-Code or, when absent, the
// Experimental-Result-Code (3GPP application errors such as user unknown or
// MWD list full are sent that way). experimental reports which one was used.
// A missing or malformed result yields DIAMETER_UNABLE_TO_COMPLY.
func parseResult(msg *dcodec.Message) (code uint32, experimental bool) {
	if rc := msg.FindAVP(dcodec.CodeResultCode, 0); rc != nil {
		if v, err := rc.Uint32(); err == nil {
			return v, false
		}
	}
	if exp := msg.FindAVP(dcodec.CodeExperimentalResult, 0); exp != nil {
		if children, err := dcodec.DecodeGrouped(exp); err == nil {
			for _, child := range children {
				if child.Code == dcodec.CodeExperimentalResultCode && child.VendorID == 0 {
					if v, err := child.Uint32(); err == nil {
						return v, true
					}
				}
			}
		}
	}
	return dcodec.DiameterUnableToComply, false
}

func parseStringAVP(msg *dcodec.Message, code, vendorID uint32) string {
	avp := msg.FindAVP(code, vendorID)
	if avp == nil {
		return ""
	}
	return string(avp.Data)
}

func parseUserIdentifier(msg *dcodec.Message) (imsi, msisdn string) {
	uid := msg.FindAVP(dcodec.CodeUserIdentifier, dcodec.Vendor3GPP)
	if uid == nil {
		return "", ""
	}
	children, err := dcodec.DecodeGrouped(uid)
	if err != nil {
		return "", ""
	}
	for _, child := range children {
		switch {
		case child.Code == dcodec.CodeUserName && child.VendorID == 0:
			if val, err := child.String(); err == nil {
				imsi = val
			}
		case child.Code == dcodec.CodeMSISDN && child.VendorID == dcodec.Vendor3GPP:
			msisdn = decodeTBCD(child.Data)
		}
	}
	return imsi, msisdn
}

func buildUserIdentifier(imsi, msisdn string) *dcodec.AVP {
	children := make([]*dcodec.AVP, 0, 2)
	if imsi != "" {
		children = append(children, dcodec.NewString(dcodec.CodeUserName, 0, dcodec.FlagMandatory, imsi))
	}
	if msisdn != "" {
		children = append(children, dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, encodeTBCD(msisdn)))
	}
	if len(children) == 0 {
		return nil
	}
	uid, err := dcodec.NewGrouped(dcodec.CodeUserIdentifier, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, children)
	if err != nil {
		return nil
	}
	return uid
}

func decodeAlertCorrelationID(encoded string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(encoded)
}

func decodeDiameterTime(v uint32) time.Time {
	const diameterTimeOffset = 2208988800
	if v <= diameterTimeOffset {
		return time.Unix(0, 0).UTC()
	}
	return time.Unix(int64(v-diameterTimeOffset), 0).UTC()
}

func parseServingNodeMME(avp *dcodec.AVP) string {
	if avp == nil {
		return ""
	}
	children, err := dcodec.DecodeGrouped(avp)
	if err != nil {
		return ""
	}
	for _, child := range children {
		if child.Code == dcodec.CodeMMEName && child.VendorID == dcodec.Vendor3GPP {
			if val, err := child.String(); err == nil {
				return val
			}
			return string(child.Data)
		}
	}
	return ""
}

func encodeTBCD(msisdn string) []byte {
	msisdn = strings.TrimPrefix(msisdn, "+")
	if len(msisdn)%2 != 0 {
		msisdn += "F"
	}
	out := make([]byte, len(msisdn)/2)
	for i := 0; i < len(msisdn); i += 2 {
		out[i/2] = (digitNibble(msisdn[i+1]) << 4) | digitNibble(msisdn[i])
	}
	return out
}

func decodeTBCD(data []byte) string {
	var out []byte
	for _, octet := range data {
		lo := octet & 0x0F
		hi := (octet >> 4) & 0x0F
		out = appendDigit(out, lo)
		out = appendDigit(out, hi)
	}
	return string(out)
}

// decodeE164Digits normalizes Diameter octet strings that represent an E.164
// address. Some AVPs carry raw TBCD digits, while others include a leading
// length and TON/NPI byte. In both cases we persist just the digit string.
func decodeE164Digits(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	if len(data) >= 2 && data[0] < byte(len(data)) && data[1]&0x80 != 0 {
		return decodeTBCD(data[2:])
	}
	return decodeTBCD(data)
}

// NormalizeE164Address converts an E.164 address into the persisted digit-only
// form used internally. It accepts already-normalized digits, optional leading
// '+', raw TBCD bytes coerced to string, and hex text such as "5155000000f1".
func NormalizeE164Address(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if isDigitString(raw) {
		return raw
	}
	if strings.HasPrefix(raw, "+") && isDigitString(raw[1:]) {
		return raw[1:]
	}
	if isHexAddress(raw) {
		if decoded, err := hex.DecodeString(raw); err == nil {
			return decodeE164Digits(decoded)
		}
	}
	return decodeE164Digits([]byte(raw))
}

func isDigitString(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func isHexAddress(s string) bool {
	if len(s) == 0 || len(s)%2 != 0 {
		return false
	}
	hasHexAlpha := false
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] >= '0' && s[i] <= '9':
		case s[i] >= 'a' && s[i] <= 'f':
			hasHexAlpha = true
		case s[i] >= 'A' && s[i] <= 'F':
			hasHexAlpha = true
		default:
			return false
		}
	}
	return hasHexAlpha
}

func digitNibble(d byte) byte {
	if d >= '0' && d <= '9' {
		return d - '0'
	}
	return 0x0F
}

func appendDigit(out []byte, nibble byte) []byte {
	if nibble <= 9 {
		return append(out, '0'+nibble)
	}
	return out
}
