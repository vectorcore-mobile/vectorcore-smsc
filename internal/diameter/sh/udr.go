// Package sh implements the Diameter Sh interface (3GPP TS 29.328) client.
// The SMSC uses Sh as an Application Server to query IMS registration state
// from the HSS when a local cache miss occurs.
package sh

import (
	"encoding/xml"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	sgdcodec "github.com/svinson1121/vectorcore-smsc/internal/codec/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

const udrTimeout = 5 * time.Second

// IMSUserState values from Sh-Data XML (TS 29.328 §6.3.16)
const (
	IMSUserStateRegistered   = 0
	IMSUserStateUnregistered = 1
)

// UserDataResult holds the parsed fields from a Sh UDA User-Data XML response.
type UserDataResult struct {
	MSISDN     string
	SIPAoR     string // first SIP URI public identity
	SCSCFName  string
	Registered bool
}

// Client manages a Sh connection to the HSS and correlates UDR/UDA exchanges.
type Client struct {
	peer           *diameter.Peer
	msisdnEncoding sgdcodec.MSISDNEncoding

	mu      sync.Mutex
	pending map[uint32]chan *dcodec.Message // keyed by HopByHop
}

// NewClient wraps a Peer as a Sh client.
func NewClient(peer *diameter.Peer) *Client {
	c := &Client{
		peer:           peer,
		msisdnEncoding: sgdcodec.MSISDNEncodingTBCD,
		pending:        make(map[uint32]chan *dcodec.Message),
	}
	peer.AddOnMessageHandler(c.dispatch)
	return c
}

// Peer returns the underlying diameter.Peer (so callers can call Start/Stop).
func (c *Client) Peer() *diameter.Peer { return c.peer }

// SetMSISDNEncoding sets the wire format of the UDR User-Identity MSISDN:
// "tbcd" (TS 29.329 §6.3.2, default) or "legacy_prefixed" (len|NOA|TBCD, for
// HSSs that still expect the historical format).
func (c *Client) SetMSISDNEncoding(v string) {
	c.msisdnEncoding = sgdcodec.NormalizeMSISDNEncoding(v)
}

// LookupIMSState sends a UDR for the given MSISDN and returns parsed results.
// Returns typed Sh errors when the HSS rejects the request.
func (c *Client) LookupIMSState(msisdn string) (*UserDataResult, error) {
	if c.peer.State() != diameter.StateOpen {
		return nil, fmt.Errorf("sh: peer not OPEN (state=%s)", c.peer.State())
	}

	hopByHop := dcodec.NextHopByHop()
	sessionID := dcodec.NewSessionID(c.peer.Cfg().LocalFQDN)
	msg, err := buildUDRRequest(c.peer.Cfg(), c.peer.RemoteFQDN, c.peer.RemoteRealm, msisdn, c.msisdnEncoding, sessionID, hopByHop)
	if err != nil {
		return nil, err
	}

	slog.Debug("sh UDR sending",
		"peer", c.peer.Name(),
		"msisdn", msisdn,
		"session_id", sessionID,
		"hop_by_hop", hopByHop,
	)

	replyCh := make(chan *dcodec.Message, 1)
	c.mu.Lock()
	c.pending[hopByHop] = replyCh
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, hopByHop)
		c.mu.Unlock()
	}()

	if err := c.peer.Send(msg); err != nil {
		return nil, fmt.Errorf("sh: send UDR: %w", err)
	}

	select {
	case uda := <-replyCh:
		logAttrs := []any{
			"peer", c.peer.Name(),
			"session_id", sessionID,
			"hop_by_hop", hopByHop,
		}
		if resultCode, ok := udaResultCode(uda); ok {
			logAttrs = append(logAttrs, "result_code", resultCode)
		}
		slog.Debug("sh UDA received", logAttrs...)
		return parseUDA(uda)
	case <-time.After(udrTimeout):
		slog.Debug("sh UDR timeout",
			"peer", c.peer.Name(),
			"msisdn", msisdn,
			"session_id", sessionID,
			"hop_by_hop", hopByHop,
		)
		return nil, fmt.Errorf("sh: UDR timeout after %s", udrTimeout)
	}
}

func buildUDRRequest(cfg diameter.Config, remoteFQDN, remoteRealm, msisdn string, msisdnEncoding sgdcodec.MSISDNEncoding, sessionID string, hopByHop uint32) (*dcodec.Message, error) {
	msisdnBCD := sgdcodec.EncodeMSISDN(msisdn, msisdnEncoding)
	userIdentity, err := dcodec.NewGrouped(
		dcodec.CodeUserIdentity, dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP,
				dcodec.FlagMandatory|dcodec.FlagVendorSpecific, msisdnBCD),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("sh: build User-Identity: %w", err)
	}

	b := dcodec.NewRequest(dcodec.CmdUserData, dcodec.App3GPP_Sh)
	b.Add(
		dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, sessionID),
		dcodec.NewUint32(dcodec.CodeAuthApplicationID, 0, dcodec.FlagMandatory, dcodec.App3GPP_Sh),
		dcodec.NewUint32(dcodec.CodeAuthSessionState, 0, dcodec.FlagMandatory, dcodec.AuthSessionStateNoStateMaintained),
		dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, cfg.LocalFQDN),
		dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, cfg.LocalRealm),
		dcodec.NewString(dcodec.CodeDestinationHost, 0, dcodec.FlagMandatory, remoteFQDN),
		dcodec.NewString(dcodec.CodeDestinationRealm, 0, dcodec.FlagMandatory, remoteRealm),
		userIdentity,
		dcodec.NewUint32(dcodec.CodeDataReference, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific, dcodec.DataRefIMSUserState),
	)

	msg := b.Build()
	msg.Header.HopByHop = hopByHop
	return msg, nil
}

// dispatch is wired as peer.OnMessage to route UDA responses to pending callers.
func (c *Client) dispatch(p *diameter.Peer, msg *dcodec.Message) {
	if msg.Header.CommandCode != dcodec.CmdUserData || msg.IsRequest() {
		return
	}
	hopByHop := msg.Header.HopByHop
	c.mu.Lock()
	ch, ok := c.pending[hopByHop]
	c.mu.Unlock()
	if ok {
		select {
		case ch <- msg:
		default:
		}
	}
}

// parseUDA extracts registration state from a UDA message.
func parseUDA(msg *dcodec.Message) (*UserDataResult, error) {
	if resultCode, ok := udaResultCode(msg); ok && resultCode != dcodec.DiameterSuccess {
		return nil, shResultError(resultCode)
	}

	// User-Data AVP (vendor 10415, code 702) contains Sh-Data XML
	udAVP := msg.FindAVP(dcodec.CodeUserData, dcodec.Vendor3GPP)
	if udAVP == nil {
		return nil, fmt.Errorf("sh: UDA missing User-Data AVP")
	}
	xmlData := string(udAVP.Data)
	return parseShDataXML(xmlData)
}

func udaResultCode(msg *dcodec.Message) (uint32, bool) {
	if rc := msg.FindAVP(dcodec.CodeResultCode, 0); rc != nil {
		code, err := rc.Uint32()
		if err == nil {
			return code, true
		}
	}
	if exp := msg.FindAVP(dcodec.CodeExperimentalResult, 0); exp != nil {
		children, err := dcodec.DecodeGrouped(exp)
		if err == nil {
			for _, child := range children {
				if child.Code != dcodec.CodeExperimentalResultCode || child.VendorID != 0 {
					continue
				}
				code, err := child.Uint32()
				if err == nil {
					return code, true
				}
			}
		}
	}
	return 0, false
}

// parseShDataXML extracts fields from the Sh-Data XML document.
// Handles both the TS 29.328 Sh-Data format produced by VectorCore HSS.
func parseShDataXML(xmlData string) (*UserDataResult, error) {
	type PublicIdentifiers struct {
		Identities []string `xml:"IMSPublicIdentity"`
		MSISDN     string   `xml:"MSISDN"`
	}
	type ShIMSData struct {
		SCSCFName string `xml:"SCSCFName"`
	}
	type ShData struct {
		XMLName           xml.Name          `xml:"Sh-Data"`
		PublicIdentifiers PublicIdentifiers `xml:"PublicIdentifiers"`
		IMSUserState      int               `xml:"IMSUserState"`
		ShIMSData         ShIMSData         `xml:"ShIMSData"`
	}

	var shData ShData
	if err := xml.Unmarshal([]byte(xmlData), &shData); err != nil {
		return nil, fmt.Errorf("sh: parse Sh-Data XML: %w", err)
	}

	result := &UserDataResult{
		MSISDN:     shData.PublicIdentifiers.MSISDN,
		SCSCFName:  shData.ShIMSData.SCSCFName,
		Registered: shData.IMSUserState == IMSUserStateRegistered,
	}

	// Pick the first SIP URI from the public identities list
	for _, id := range shData.PublicIdentifiers.Identities {
		if strings.HasPrefix(id, "sip:") || strings.HasPrefix(id, "sips:") {
			result.SIPAoR = id
			break
		}
	}

	return result, nil
}

type ErrUnknownUser struct {
	ResultCode uint32
}

func (e *ErrUnknownUser) Error() string {
	return fmt.Sprintf("sh: HSS returned result-code %d (subscriber unknown or error)", e.ResultCode)
}

type ErrUnsupportedUserData struct {
	ResultCode uint32
}

func (e *ErrUnsupportedUserData) Error() string {
	return fmt.Sprintf("sh: HSS returned result-code %d (unsupported user data)", e.ResultCode)
}

type ErrResultCode struct {
	ResultCode uint32
}

func (e *ErrResultCode) Error() string {
	return fmt.Sprintf("sh: HSS returned result-code %d", e.ResultCode)
}

func shResultError(resultCode uint32) error {
	switch resultCode {
	case 5001:
		return &ErrUnknownUser{ResultCode: resultCode}
	case 5009:
		return &ErrUnsupportedUserData{ResultCode: resultCode}
	default:
		return &ErrResultCode{ResultCode: resultCode}
	}
}
