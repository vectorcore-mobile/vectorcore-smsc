// Package sgd implements the Diameter SGd interface (3GPP TS 29.338) for
// IP-SM-GW operation. It handles inbound OFR (MO) requests from the MME,
// correlates answers to outbound TFR (MT) sends, and sends RSR
// (delivery report) and Alert-SC back.
//
// Naming note: 3GPP TS 29.338 calls MO-Forward-Short-Message "OFR/OFA" and
// MT-Forward-Short-Message "TFR/TFA". Several identifiers in this codebase use
// the two names the other way round:
//
//	SendOFR, EncodeOFR, buildOFRRequest, OFAResultError → MT-Forward (3GPP TFR/TFA)
//	HandleTFR, DecodeTFR, EncodeTFR                     → MO-Forward (3GPP OFR)
//
// The Diameter command codes (dcodec.CmdMTForwardShortMessage,
// dcodec.CmdMOForwardShortMessage) are authoritative; log messages keep the
// historical names.
package sgd

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	sgdcodec "github.com/svinson1121/vectorcore-smsc/internal/codec/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

const gracefulStopTimeout = 5 * time.Second

// DefaultTFATimeout is how long SendOFR waits for the MME's
// MT-Forward-Short-Message-Answer. It covers paging the UE and the NAS
// round trip, and must stay above the MME's own SMS transaction timeout
// (30s by default in vectorcore-mme) so the MME's answer, success or
// failure, arrives before the SMSC gives up and retries.
const DefaultTFATimeout = 40 * time.Second

// OnMessageFunc is called for each decoded MO/MT message.
type OnMessageFunc func(msg *codec.Message, peerName string)

// OnAlertServiceCentreFunc is called for inbound SGd Alert-Service-Centre requests.
type OnAlertServiceCentreFunc func(req AlertServiceCentreRequest) error

// Server listens for inbound Diameter SGd connections from MMEs and can also
// send MT messages (TFR) back to connected MMEs.
type Server struct {
	listenAddr     string
	transport      string
	localFQDN      string
	localRealm     string
	scAddrEncoding string
	tfaTimeout     time.Duration
	onMsg          OnMessageFunc
	onAlertSC      OnAlertServiceCentreFunc

	// Active peers keyed by RemoteFQDN (or remote addr for inbound before CEA).
	mu            sync.RWMutex
	peers         map[string]*diameter.Peer
	outboundPeers map[string]*diameter.Peer // keyed by configured name
	pendingOFR    map[uint32]chan *dcodec.Message
	pendingMO     map[string]pendingMORequest
}

type pendingMORequest struct {
	peer *diameter.Peer
	req  *dcodec.Message
}

// TS 29.338 §7.4.3 SGd experimental result codes (Experimental-Result,
// Vendor-Id 10415) returned in MT-Forward-Short-Message-Answer.
const (
	ResultErrorAbsentUser           uint32 = 5550
	ResultErrorUserBusyForMTSMS     uint32 = 5551
	ResultErrorFacilityNotSupported uint32 = 5552
	ResultErrorIllegalUser          uint32 = 5553
	ResultErrorIllegalEquipment     uint32 = 5554
	ResultErrorSMDeliveryFailure    uint32 = 5555
)

// SM-Enumerated-Delivery-Failure-Cause values (TS 29.338 §7.3.5), sent with
// DIAMETER_ERROR_SM_DELIVERY_FAILURE.
const (
	DeliveryFailureMemoryCapacityExceeded uint32 = 0
	DeliveryFailureEquipmentProtocolError uint32 = 1
	DeliveryFailureEquipmentNotSMEquipped uint32 = 2
	DeliveryFailureUnknownServiceCentre   uint32 = 3
	DeliveryFailureSCCongestion           uint32 = 4
	DeliveryFailureInvalidSMEAddress      uint32 = 5
	DeliveryFailureUserNotSCUser          uint32 = 6
)

// OFAResultError captures a non-success answer to an MT-Forward request
// (see the naming note in the package doc).
type OFAResultError struct {
	ResultCode   uint32
	Experimental bool // ResultCode came from Experimental-Result (vendor 10415)
	// DeliveryFailureCause is SM-Enumerated-Delivery-Failure-Cause when the
	// MME sent SM-Delivery-Failure-Cause (with ResultErrorSMDeliveryFailure).
	DeliveryFailureCause *uint32
}

func (e *OFAResultError) Error() string {
	kind := "result-code"
	if e.Experimental {
		kind = "experimental-result-code"
	}
	if e.DeliveryFailureCause != nil {
		return fmt.Sprintf("sgd: OFA returned %s %d (delivery failure cause %d)", kind, e.ResultCode, *e.DeliveryFailureCause)
	}
	return fmt.Sprintf("sgd: OFA returned %s %d", kind, e.ResultCode)
}

// ofaError returns nil for a successful MT-Forward answer, otherwise an
// *OFAResultError describing the failure.
func ofaError(ans *dcodec.Message) error {
	code, experimental, ok := resultCodeExperimental(ans)
	if !ok {
		return &OFAResultError{ResultCode: dcodec.DiameterUnableToComply}
	}
	if code == dcodec.DiameterSuccess && !experimental {
		return nil
	}
	e := &OFAResultError{ResultCode: code, Experimental: experimental}
	if grp := ans.FindAVP(dcodec.CodeSMDeliveryFailureCause, dcodec.Vendor3GPP); grp != nil {
		if children, err := dcodec.DecodeGrouped(grp); err == nil {
			for _, child := range children {
				if child.Code == dcodec.CodeSMEnumeratedDeliveryFailureCause && child.VendorID == dcodec.Vendor3GPP {
					if v, err := child.Uint32(); err == nil {
						e.DeliveryFailureCause = &v
					}
				}
			}
		}
	}
	return e
}

// NewServer creates an SGd server.
func NewServer(transport, listenAddr, localFQDN, localRealm, scAddrEncoding string, onMsg OnMessageFunc) *Server {
	s := &Server{
		listenAddr:     listenAddr,
		transport:      transport,
		localFQDN:      localFQDN,
		localRealm:     localRealm,
		scAddrEncoding: scAddrEncoding,
		tfaTimeout:     DefaultTFATimeout,
		onMsg:          onMsg,
		peers:          make(map[string]*diameter.Peer),
		outboundPeers:  make(map[string]*diameter.Peer),
		pendingOFR:     make(map[uint32]chan *dcodec.Message),
		pendingMO:      make(map[string]pendingMORequest),
	}
	return s
}

// AddOutboundPeer starts an outbound Diameter connection to a configured SGd peer.
// If a peer with the same name is already tracked, it is stopped first.
func (s *Server) AddOutboundPeer(ctx context.Context, name, host string, port int, transport, peerFQDN, peerRealm string) {
	var oldPeer *diameter.Peer
	s.mu.Lock()
	if old, ok := s.outboundPeers[name]; ok {
		oldPeer = old
		delete(s.outboundPeers, name)
	}
	s.mu.Unlock()
	if oldPeer != nil {
		oldPeer.StopGraceful(gracefulStopTimeout)
	}

	cfg := diameter.Config{
		Name:        name,
		Host:        host,
		Port:        port,
		Transport:   transport,
		AppID:       dcodec.App3GPP_SGd,
		Application: "sgd",
		LocalFQDN:   s.localFQDN,
		LocalRealm:  s.localRealm,
		PeerFQDN:    peerFQDN,
		PeerRealm:   peerRealm,
	}
	p := diameter.NewPeer(cfg)
	p.AddOnMessageHandler(func(peer *diameter.Peer, msg *dcodec.Message) {
		s.dispatch(peer, msg)
	})
	p.AddOnOpenHandler(func(peer *diameter.Peer) {
		key := peer.RemoteFQDN
		if key == "" {
			key = name
		}
		s.mu.Lock()
		s.peers[key] = peer
		s.mu.Unlock()
		slog.Info("diameter SGd outbound peer connected", "name", name, "fqdn", key)
	})
	p.AddOnCloseHandler(func(peer *diameter.Peer) {
		s.unregisterPeer(name, peer)
	})

	s.mu.Lock()
	s.outboundPeers[name] = p
	s.mu.Unlock()

	p.Start(ctx)
	slog.Info("diameter SGd outbound peer dialing", "name", name, "host", host, "port", port)
}

// AttachOutboundPeer registers an already-created peer as an SGd outbound peer.
// The caller owns the peer lifecycle and is responsible for calling Start/Stop.
func (s *Server) AttachOutboundPeer(name string, p *diameter.Peer) {
	s.mu.Lock()
	s.outboundPeers[name] = p
	s.mu.Unlock()

	p.AddOnMessageHandler(func(peer *diameter.Peer, msg *dcodec.Message) {
		if msg.Header.AppID != dcodec.App3GPP_SGd {
			return
		}
		s.dispatch(peer, msg)
	})
	p.AddOnOpenHandler(func(peer *diameter.Peer) {
		key := peer.RemoteFQDN
		if key == "" {
			key = name
		}
		s.mu.Lock()
		s.peers[key] = peer
		s.mu.Unlock()
		slog.Info("diameter SGd shared outbound peer connected", "name", name, "fqdn", key)
	})
	p.AddOnCloseHandler(func(peer *diameter.Peer) {
		s.unregisterPeer(name, peer)
	})
}

// DetachOutboundPeer unregisters a peer previously attached via AttachOutboundPeer.
func (s *Server) DetachOutboundPeer(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.outboundPeers[name]; ok {
		delete(s.outboundPeers, name)
		s.unregisterPeerLocked(name, p)
	}
}

// RemoveOutboundPeer stops and removes a previously added outbound peer.
func (s *Server) RemoveOutboundPeer(name string) {
	var p *diameter.Peer
	s.mu.Lock()
	if peer, ok := s.outboundPeers[name]; ok {
		p = peer
		delete(s.outboundPeers, name)
		s.unregisterPeerLocked(name, p)
	}
	s.mu.Unlock()
	if p != nil {
		p.StopGraceful(gracefulStopTimeout)
	}
}

// PeerStatus describes the runtime state of a Diameter SGd peer.
type PeerStatus struct {
	Name        string     `json:"name"`
	Application string     `json:"application"`
	State       string     `json:"state"`
	ConnectedAt *time.Time `json:"connected_at,omitempty"`
}

// PeerStatuses returns the runtime state of all SGd peers — both configured
// outbound peers and inbound connections that have completed CER/CEA.
func (s *Server) PeerStatuses() []PeerStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]PeerStatus, 0, len(s.outboundPeers)+len(s.peers))

	// Outbound (configured).
	outboundFQDNs := make(map[string]bool, len(s.outboundPeers))
	for name, p := range s.outboundPeers {
		fqdn := p.RemoteFQDN
		if fqdn == "" {
			fqdn = name
		}
		outboundFQDNs[fqdn] = true
		outboundFQDNs[name] = true
		out = append(out, PeerStatus{
			Name:        name,
			Application: p.Config().Application,
			State:       p.State().String(),
			ConnectedAt: p.ConnectedAt(),
		})
	}

	// Inbound peers that completed CEA (keyed by FQDN — skip TCP-addr-only entries
	// and anything already represented by an outbound peer).
	for key, p := range s.peers {
		fqdn := p.RemoteFQDN
		if fqdn == "" {
			continue // CER/CEA not yet complete; skip
		}
		if outboundFQDNs[key] || outboundFQDNs[fqdn] {
			continue // already represented above
		}
		if key != fqdn {
			continue // TCP addr entry — the FQDN entry covers it
		}
		out = append(out, PeerStatus{
			Name:        fqdn,
			Application: "sgd",
			State:       p.State().String(),
			ConnectedAt: p.ConnectedAt(),
		})
	}

	return out
}

// SetOnMessage sets the message callback after server creation.
func (s *Server) SetOnMessage(fn OnMessageFunc) {
	s.mu.Lock()
	s.onMsg = fn
	s.mu.Unlock()
}

// SetTFATimeout sets how long SendOFR waits for the MT-Forward answer.
// Non-positive values keep the current timeout.
func (s *Server) SetTFATimeout(d time.Duration) {
	if d <= 0 {
		return
	}
	s.mu.Lock()
	s.tfaTimeout = d
	s.mu.Unlock()
}

// SetOnAlertServiceCentre sets the callback for inbound SGd ALR requests.
func (s *Server) SetOnAlertServiceCentre(fn OnAlertServiceCentreFunc) {
	s.mu.Lock()
	s.onAlertSC = fn
	s.mu.Unlock()
}

// SendOFR sends an MT-Forward-Short-Message (TFR) to the MME identified by
// mmeHost. It implements the forwarder.SGdSender interface.
func (s *Server) SendOFR(ctx context.Context, msg *codec.Message, mmeHost, scAddr string) error {
	s.mu.RLock()
	p, viaProxy := s.selectPeerForMMELocked(mmeHost)
	tfaTimeout := s.tfaTimeout
	s.mu.RUnlock()
	if p == nil {
		return fmt.Errorf("sgd: no active peer for MME %q", mmeHost)
	}

	avps, err := sgdcodec.EncodeOFR(msg, scAddr, s.scAddrEncoding)
	if err != nil {
		return fmt.Errorf("sgd OFR encode: %w", err)
	}

	req := buildOFRRequest(s.localFQDN, s.localRealm, mmeHost, destinationRealmForPeer(mmeHost, p), avps)
	replyCh := make(chan *dcodec.Message, 1)
	s.trackPendingOFR(req.Header.HopByHop, replyCh)
	defer s.untrackPendingOFR(req.Header.HopByHop)

	if err := p.Send(req); err != nil {
		return fmt.Errorf("sgd OFR send: %w", err)
	}
	slog.Info("sgd OFR sent",
		"id", msg.ID,
		"kind", msg.KindOrDefault(),
		"ref_id", msg.RefMessageID,
		"tp_mr", msg.TPMR,
		"mme", mmeHost,
		"dst", msg.Destination.MSISDN,
		"via_proxy", viaProxy,
		"peer", peerLogName(p),
		"hop_by_hop", req.Header.HopByHop,
	)

	select {
	case ans := <-replyCh:
		resultCode := resultCode(ans)
		ofaErr := ofaError(ans)
		slog.Debug("sgd OFA received",
			"id", msg.ID,
			"kind", msg.KindOrDefault(),
			"ref_id", msg.RefMessageID,
			"mme", mmeHost,
			"dst", msg.Destination.MSISDN,
			"via_proxy", viaProxy,
			"peer", peerLogName(p),
			"hop_by_hop", req.Header.HopByHop,
			"result_code", resultCode,
			"err", ofaErr,
		)
		return ofaErr
	case <-time.After(tfaTimeout):
		slog.Warn("sgd OFA timeout",
			"id", msg.ID,
			"kind", msg.KindOrDefault(),
			"ref_id", msg.RefMessageID,
			"mme", mmeHost,
			"dst", msg.Destination.MSISDN,
			"hop_by_hop", req.Header.HopByHop,
			"timeout", tfaTimeout,
		)
		return fmt.Errorf("sgd: OFA timeout after %s", tfaTimeout)
	}
}

func (s *Server) HasPeerForMME(mmeHost string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, _ := s.selectPeerForMMELocked(mmeHost)
	return p != nil
}

func (s *Server) RoutePeerForMME(mmeHost string) (peerName string, viaProxy bool, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, viaProxy := s.selectPeerForMMELocked(mmeHost)
	if p == nil {
		return "", false, false
	}
	return peerLogName(p), viaProxy, true
}

func (s *Server) selectPeerForMMELocked(mmeHost string) (*diameter.Peer, bool) {
	if p := s.peers[mmeHost]; p != nil {
		return p, false
	}

	for name, p := range s.outboundPeers {
		if p == nil {
			continue
		}
		if s.peerIsActiveLocked(name, p) {
			return p, true
		}
	}

	for key, p := range s.peers {
		if p == nil || key == mmeHost {
			continue
		}
		if key == p.RemoteFQDN && p.RemoteFQDN != "" {
			return p, true
		}
	}

	return nil, false
}

func (s *Server) peerIsActiveLocked(name string, p *diameter.Peer) bool {
	if active := s.peers[name]; active == p {
		return true
	}
	if p.RemoteFQDN != "" && s.peers[p.RemoteFQDN] == p {
		return true
	}
	return false
}

func destinationRealmForPeer(mmeHost string, p *diameter.Peer) string {
	if p != nil && p.RemoteRealm != "" {
		return p.RemoteRealm
	}
	for i := 0; i < len(mmeHost); i++ {
		if mmeHost[i] == '.' && i+1 < len(mmeHost) {
			return mmeHost[i+1:]
		}
	}
	return ""
}

func peerLogName(p *diameter.Peer) string {
	if p == nil {
		return ""
	}
	if p.RemoteFQDN != "" {
		return p.RemoteFQDN
	}
	return p.Name()
}

func (s *Server) trackPendingOFR(hopByHop uint32, replyCh chan *dcodec.Message) {
	s.mu.Lock()
	s.pendingOFR[hopByHop] = replyCh
	s.mu.Unlock()
}

func (s *Server) untrackPendingOFR(hopByHop uint32) {
	s.mu.Lock()
	delete(s.pendingOFR, hopByHop)
	s.mu.Unlock()
}

func (s *Server) trackPendingMO(messageID string, p *diameter.Peer, req *dcodec.Message) {
	s.mu.Lock()
	s.pendingMO[messageID] = pendingMORequest{peer: p, req: req}
	s.mu.Unlock()
}

func (s *Server) finishPendingMO(messageID string, resultCode uint32, smRPUI []byte) bool {
	s.mu.Lock()
	pending, ok := s.pendingMO[messageID]
	if ok {
		delete(s.pendingMO, messageID)
	}
	s.mu.Unlock()
	if !ok {
		return false
	}

	extra := []*dcodec.AVP(nil)
	if len(smRPUI) > 0 {
		extra = append(extra, dcodec.NewOctetString(
			dcodec.CodeSMRPUI,
			dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
			smRPUI,
		))
	}
	sendAnswerWithAVPs(pending.peer, pending.req, resultCode, extra...)
	return true
}

// CompleteMO completes a pending inbound SGd MO request using the correlated
// downstream RP result body from ISC.
func (s *Server) CompleteMO(messageID string, rpBody []byte) bool {
	resultCode, smRPUI := mapISCResultToSGdAnswer(rpBody)
	return s.finishPendingMO(messageID, resultCode, smRPUI)
}

func (s *Server) handleMOForwardRequest(p *diameter.Peer, req *dcodec.Message) {
	msg, err := sgdcodec.DecodeTFR(req)
	if err != nil {
		slog.Error("sgd TFR decode failed", "peer", p.RemoteFQDN, "err", err)
		sendAnswer(p, req, dcodec.DiameterUnableToComply)
		return
	}

	msg.IngressPeer = p.RemoteFQDN
	msg.IngressInterface = codec.InterfaceSGd
	msg.CorrelationID = newSessionID()

	slog.Info("sgd TFR received (MO)",
		"peer", p.RemoteFQDN,
		"src", msg.Source.MSISDN,
		"dst", msg.Destination.MSISDN,
		"encoding", msg.Encoding,
		"corr_id", msg.CorrelationID,
	)
	slog.Debug("sgd TFR decoded",
		"peer", p.RemoteFQDN,
		"src", msg.Source.MSISDN,
		"dst", msg.Destination.MSISDN,
		"tp_mr", msg.TPMR,
		"binary_len", len(msg.Binary),
		"corr_id", msg.CorrelationID,
	)

	if s.onMsg == nil {
		sendAnswer(p, req, dcodec.DiameterUnableToComply)
		return
	}

	// Accept the MO once the SMSC has taken ownership of processing; delivery
	// continues asynchronously via store-and-forward and downstream interface retries.
	go s.onMsg(msg, p.RemoteFQDN)
	sendAnswer(p, req, dcodec.DiameterSuccess)
}

func mapISCResultToSGdAnswer(body []byte) (uint32, []byte) {
	if len(body) == 0 {
		return dcodec.DiameterUnableToComply, nil
	}
	switch body[0] & 0x07 {
	case 0x02, 0x03:
		return dcodec.DiameterSuccess, encodeSubmitReportAck(time.Now().UTC())
	default:
		return dcodec.DiameterUnableToComply, nil
	}
}

func resultCode(msg *dcodec.Message) uint32 {
	code, ok := resultCodeOK(msg)
	if !ok {
		return dcodec.DiameterUnableToComply
	}
	return code
}

func resultCodeOK(msg *dcodec.Message) (uint32, bool) {
	code, _, ok := resultCodeExperimental(msg)
	return code, ok
}

// resultCodeExperimental returns the Result-Code or, when absent, the
// Experimental-Result-Code, and whether the latter was used.
func resultCodeExperimental(msg *dcodec.Message) (code uint32, experimental, ok bool) {
	if msg == nil {
		return 0, false, false
	}
	if rc := msg.FindAVP(dcodec.CodeResultCode, 0); rc != nil {
		if code, err := rc.Uint32(); err == nil {
			return code, false, true
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
					return code, true, true
				}
			}
		}
	}
	return 0, false, false
}

func buildOFRRequest(localFQDN, localRealm, mmeHost, mmeRealm string, avps []*dcodec.AVP) *dcodec.Message {
	b := dcodec.NewRequest(dcodec.CmdMTForwardShortMessage, dcodec.App3GPP_SGd)
	b.Add(
		dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, newSessionID()),
		dcodec.NewUint32(dcodec.CodeAuthSessionState, 0, dcodec.FlagMandatory, dcodec.AuthSessionStateNoStateMaintained),
		dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, localFQDN),
		dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, localRealm),
		dcodec.NewString(dcodec.CodeDestinationHost, 0, dcodec.FlagMandatory, mmeHost),
		dcodec.NewString(dcodec.CodeDestinationRealm, 0, dcodec.FlagMandatory, mmeRealm),
	)
	b.Add(avps...)
	return b.Build()
}

// ListenAndServe accepts Diameter TCP connections from MMEs.
// Each connection gets a Peer FSM (inbound mode via direct session).
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := diameter.Listen(s.transport, s.listenAddr)
	if err != nil {
		return err
	}
	slog.Info("diameter SGd server listening", "transport", s.transport, "addr", s.listenAddr)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("diameter SGd accept error", "err", err)
			return err
		}
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	remote := conn.RemoteAddr().String()
	slog.Info("diameter SGd inbound connection", "remote", remote)

	cfg := diameter.Config{
		Name:        remote,
		LocalFQDN:   s.localFQDN,
		LocalRealm:  s.localRealm,
		AppID:       dcodec.App3GPP_SGd,
		Application: "sgd",
	}
	p := diameter.NewPeer(cfg)
	p.AddOnMessageHandler(func(peer *diameter.Peer, msg *dcodec.Message) {
		s.dispatch(peer, msg)
	})
	// Track by remote TCP addr initially; re-register by RemoteFQDN after CER/CEA.
	s.mu.Lock()
	s.peers[remote] = p
	s.mu.Unlock()

	p.AddOnOpenHandler(func(peer *diameter.Peer) {
		if peer.RemoteFQDN != "" {
			s.mu.Lock()
			s.peers[peer.RemoteFQDN] = peer
			s.mu.Unlock()
			slog.Info("diameter SGd peer registered", "fqdn", peer.RemoteFQDN)
		}
	})
	p.AddOnCloseHandler(func(peer *diameter.Peer) {
		s.unregisterPeer(remote, peer)
	})

	// Run inbound session — blocks until connection drops.
	p.RunInbound(ctx, conn)

	// Remove from peer registry on disconnect.
	s.mu.Lock()
	delete(s.peers, remote)
	if p.RemoteFQDN != "" {
		delete(s.peers, p.RemoteFQDN)
	}
	s.mu.Unlock()
	slog.Info("diameter SGd peer disconnected", "remote", remote)
}

func (s *Server) unregisterPeer(name string, p *diameter.Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unregisterPeerLocked(name, p)
}

func (s *Server) unregisterPeerLocked(name string, p *diameter.Peer) {
	delete(s.peers, name)
	if p.RemoteFQDN != "" {
		delete(s.peers, p.RemoteFQDN)
	}
}

// dispatch routes application messages to the correct handler.
func (s *Server) dispatch(p *diameter.Peer, msg *dcodec.Message) {
	cmd := msg.Header.CommandCode
	isReq := msg.IsRequest()

	switch {
	case cmd == dcodec.CmdMTForwardShortMessage && isReq:
		HandleOFR(p, msg, s.onMsg)
	case cmd == dcodec.CmdMTForwardShortMessage && !isReq:
		s.mu.RLock()
		ch := s.pendingOFR[msg.Header.HopByHop]
		s.mu.RUnlock()
		if ch != nil {
			select {
			case ch <- msg:
			default:
			}
			return
		}
	case cmd == dcodec.CmdMOForwardShortMessage && isReq:
		s.handleMOForwardRequest(p, msg)
	case cmd == dcodec.CmdAlertServiceCentre && isReq:
		s.handleAlertServiceCentreRequest(p, msg)
	default:
		slog.Debug("diameter SGd unhandled command",
			"cmd", cmd, "request", isReq)
	}
}
