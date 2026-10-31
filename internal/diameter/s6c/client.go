// Package s6c implements the Diameter S6c interface (3GPP TS 29.338) client.
package s6c

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

const requestTimeout = 5 * time.Second

// RoutingInfo holds the SRI-SM routing data returned by the HSS.
type RoutingInfo struct {
	IMSI       string
	MSISDN     string
	Attached   bool
	MMEName    string
	MMENumber  string
	MMERealm   string
	MWDStatus  uint32
	SessionID  string
	ResultCode uint32
}

// ReportDeliveryResult holds the RSDS answer details.
type ReportDeliveryResult struct {
	SessionID  string
	ResultCode uint32
	MWDStatus  uint32
}

// AlertServiceCentreRequest is the decoded inbound ALSC payload.
type AlertServiceCentreRequest struct {
	SessionID                 string
	OriginHost                string
	OriginRealm               string
	MSISDN                    string
	IMSI                      string
	SCAddress                 string
	AlertCorrelationID        string
	AlertReason               uint32
	ServingNode               string
	MaximumUEAvailabilityTime *time.Time
	SMSGMSCAlertEvent         uint32
	AbsentUserDiagnosticSM    uint32
}

// Client manages S6c requests over a shared Diameter peer.
type Client struct {
	peer   *diameter.Peer
	scAddr string

	mu      sync.Mutex
	pending map[uint32]chan *dcodec.Message

	onAlertServiceCentre func(AlertServiceCentreRequest) error
}

// NewClient wraps a Peer as an S6c client.
func NewClient(peer *diameter.Peer, scAddr string) *Client {
	c := &Client{
		peer:    peer,
		scAddr:  scAddr,
		pending: make(map[uint32]chan *dcodec.Message),
	}
	peer.AddOnMessageHandler(c.dispatch)
	return c
}

// Peer returns the underlying Diameter peer.
func (c *Client) Peer() *diameter.Peer { return c.peer }

// SetOnAlertServiceCentre installs the ALSC callback.
func (c *Client) SetOnAlertServiceCentre(fn func(AlertServiceCentreRequest) error) {
	c.mu.Lock()
	c.onAlertServiceCentre = fn
	c.mu.Unlock()
}

// LookupRouting sends SRI-SM for the given MSISDN.
func (c *Client) LookupRouting(msisdn string) (*RoutingInfo, error) {
	if c.peer.State() != diameter.StateOpen {
		return nil, fmt.Errorf("s6c: peer not OPEN (state=%s)", c.peer.State())
	}

	cfg := c.peer.Cfg()
	hopByHop := dcodec.NextHopByHop()
	sessionID := dcodec.NewSessionID(cfg.LocalFQDN)

	b := dcodec.NewRequest(dcodec.CmdSendRoutingInfoSM, dcodec.App3GPP_S6c)
	b.Add(
		dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, sessionID),
		dcodec.NewUint32(dcodec.CodeAuthSessionState, 0, dcodec.FlagMandatory, dcodec.AuthSessionStateNoStateMaintained),
		dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, cfg.LocalFQDN),
		dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, cfg.LocalRealm),
		dcodec.NewString(dcodec.CodeDestinationHost, 0, dcodec.FlagMandatory, c.peer.RemoteFQDN),
		dcodec.NewString(dcodec.CodeDestinationRealm, 0, dcodec.FlagMandatory, c.peer.RemoteRealm),
		dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, encodeTBCD(msisdn)),
		dcodec.NewUint32(dcodec.CodeSMRPMTIS6c, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, dcodec.SMRPMTIS6cDeliver),
	)
	if c.scAddr != "" {
		b.Add(dcodec.NewOctetString(dcodec.CodeSCAddressS6c, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, encodeTBCD(c.scAddr)))
	}

	msg := b.Build()
	msg.Header.HopByHop = hopByHop

	slog.Debug("s6c SRI-SM sending",
		"peer", c.peer.Name(),
		"msisdn", msisdn,
		"session_id", sessionID,
		"hop_by_hop", hopByHop,
	)

	replyCh := make(chan *dcodec.Message, 1)
	c.trackPending(hopByHop, replyCh)
	defer c.untrackPending(hopByHop)

	if err := c.peer.Send(msg); err != nil {
		return nil, fmt.Errorf("s6c: send SRI-SM: %w", err)
	}

	select {
	case ans := <-replyCh:
		resultCode, experimental := parseResult(ans)
		slog.Debug("s6c SRI-SM answer received",
			"peer", c.peer.Name(),
			"msisdn", msisdn,
			"session_id", sessionID,
			"hop_by_hop", hopByHop,
			"result_code", resultCode,
			"experimental", experimental,
		)
		return parseSRIAnswer(ans)
	case <-time.After(requestTimeout):
		return nil, fmt.Errorf("s6c: SRI-SM timeout after %s", requestTimeout)
	}
}

// ReportDelivery sends RSDS for the given subscriber outcome using the current
// Rel-15-oriented HSS contract.
func (c *Client) ReportDelivery(msisdn, imsi, scAddr, alertCorrelationID string, cause uint32, diagnostic *uint32) (*ReportDeliveryResult, error) {
	if c.peer.State() != diameter.StateOpen {
		return nil, fmt.Errorf("s6c: peer not OPEN (state=%s)", c.peer.State())
	}

	cfg := c.peer.Cfg()
	hopByHop := dcodec.NextHopByHop()
	sessionID := dcodec.NewSessionID(cfg.LocalFQDN)

	deliveryOutcome, err := buildSMDeliveryOutcome(cause, diagnostic)
	if err != nil {
		return nil, err
	}

	b := dcodec.NewRequest(dcodec.CmdReportSMDeliveryStatus, dcodec.App3GPP_S6c)
	b.Add(
		dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, sessionID),
		dcodec.NewUint32(dcodec.CodeAuthSessionState, 0, dcodec.FlagMandatory, dcodec.AuthSessionStateNoStateMaintained),
		dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, cfg.LocalFQDN),
		dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, cfg.LocalRealm),
		dcodec.NewString(dcodec.CodeDestinationRealm, 0, dcodec.FlagMandatory, c.peer.RemoteRealm),
		dcodec.NewOctetString(dcodec.CodeSCAddressS6c, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, encodeTBCD(scAddr)),
		dcodec.NewUint32(dcodec.CodeSMRPMTIS6c, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, dcodec.SMRPMTIS6cDeliver),
		deliveryOutcome,
	)
	if userIdentifier := buildUserIdentifier(imsi, msisdn); userIdentifier != nil {
		b.Add(userIdentifier)
	}
	if alertCorrelationID != "" {
		if payload, err := decodeAlertCorrelationID(alertCorrelationID); err == nil && len(payload) > 0 {
			b.Add(dcodec.NewOctetString(dcodec.CodeSMSMICorrelationID, dcodec.Vendor3GPP, dcodec.FlagVendorSpecific, payload))
		} else if err != nil {
			slog.Warn("s6c RSDS invalid alert correlation id", "peer", c.peer.Name(), "alert_correlation_id", alertCorrelationID, "err", err)
		}
	}

	msg := b.Build()
	msg.Header.HopByHop = hopByHop

	slog.Debug("s6c RSDS sending",
		"peer", c.peer.Name(),
		"msisdn", msisdn,
		"imsi", imsi,
		"session_id", sessionID,
		"hop_by_hop", hopByHop,
		"cause", cause,
	)

	replyCh := make(chan *dcodec.Message, 1)
	c.trackPending(hopByHop, replyCh)
	defer c.untrackPending(hopByHop)

	if err := c.peer.Send(msg); err != nil {
		return nil, fmt.Errorf("s6c: send RSDS: %w", err)
	}

	select {
	case ans := <-replyCh:
		resultCode, experimental := parseResult(ans)
		slog.Debug("s6c RSDS answer received",
			"peer", c.peer.Name(),
			"msisdn", msisdn,
			"imsi", imsi,
			"session_id", sessionID,
			"hop_by_hop", hopByHop,
			"result_code", resultCode,
			"experimental", experimental,
		)
		return parseRSDSAnswer(ans)
	case <-time.After(requestTimeout):
		return nil, fmt.Errorf("s6c: RSDS timeout after %s", requestTimeout)
	}
}

// buildSMDeliveryOutcome builds SM-Delivery-Outcome for RSDS. The SMSC
// reports RSDS only for MT attempts via SGd, i.e. via the MME, so the cause
// goes in MME-Delivery-Outcome (TS 29.338 §7.3.17).
func buildSMDeliveryOutcome(cause uint32, diagnostic *uint32) (*dcodec.AVP, error) {
	children := []*dcodec.AVP{
		dcodec.NewUint32(dcodec.CodeSMDeliveryCause, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, cause),
	}
	if diagnostic != nil {
		children = append(children,
			dcodec.NewUint32(dcodec.CodeAbsentUserDiagnosticSM, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, *diagnostic),
		)
	}
	nodeOutcome, err := dcodec.NewGrouped(dcodec.CodeMMEDeliveryOutcome, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, children)
	if err != nil {
		return nil, fmt.Errorf("s6c: build MME-Delivery-Outcome: %w", err)
	}
	outcome, err := dcodec.NewGrouped(dcodec.CodeSMDeliveryOutcomeS6c, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, []*dcodec.AVP{nodeOutcome})
	if err != nil {
		return nil, fmt.Errorf("s6c: build SM-Delivery-Outcome: %w", err)
	}
	return outcome, nil
}

func (c *Client) dispatch(p *diameter.Peer, msg *dcodec.Message) {
	if msg.Header.AppID != dcodec.App3GPP_S6c {
		return
	}
	switch {
	case msg.IsRequest() && msg.Header.CommandCode == dcodec.CmdAlertServiceCentre:
		c.handleAlertServiceCentre(msg)
	case !msg.IsRequest():
		c.mu.Lock()
		ch := c.pending[msg.Header.HopByHop]
		c.mu.Unlock()
		if ch != nil {
			select {
			case ch <- msg:
			default:
			}
		}
	}
}

func (c *Client) handleAlertServiceCentre(msg *dcodec.Message) {
	req, err := parseAlertServiceCentre(msg)
	if err != nil {
		slog.Warn("s6c ALSC parse failed", "peer", c.peer.Name(), "err", err)
		_ = sendAlertServiceCentreAnswer(c.peer, msg, dcodec.DiameterUnableToComply)
		return
	}

	c.mu.Lock()
	fn := c.onAlertServiceCentre
	c.mu.Unlock()

	resultCode := dcodec.DiameterSuccess
	if fn != nil {
		if err := fn(req); err != nil {
			slog.Warn("s6c ALSC handler failed",
				"peer", c.peer.Name(),
				"msisdn", req.MSISDN,
				"imsi", req.IMSI,
				"err", err,
			)
			resultCode = dcodec.DiameterUnableToComply
		}
	}
	slog.Debug("s6c ALSC answering",
		"peer", c.peer.Name(),
		"msisdn", req.MSISDN,
		"imsi", req.IMSI,
		"result_code", resultCode,
	)
	_ = sendAlertServiceCentreAnswer(c.peer, msg, resultCode)
}

func (c *Client) trackPending(hopByHop uint32, replyCh chan *dcodec.Message) {
	c.mu.Lock()
	c.pending[hopByHop] = replyCh
	c.mu.Unlock()
}

func (c *Client) untrackPending(hopByHop uint32) {
	c.mu.Lock()
	delete(c.pending, hopByHop)
	c.mu.Unlock()
}
