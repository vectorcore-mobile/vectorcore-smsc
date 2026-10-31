package isc

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"

	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/sip3gpp"
	"github.com/svinson1121/vectorcore-smsc/internal/registry"
)

// Sender delivers MT SMS to IMS UEs via the S-CSCF ISC interface.
type Sender struct {
	client   *sipgo.Client
	scAddr   string // Our SMSC E.164 address, placed in RP-OA
	sipLocal string // Our SIP URI (From / P-Asserted-Identity)
	cfg      Settings
	rpMRSeq  atomic.Uint32
}

type Settings struct {
	AcceptContact           string
	MTRequestDisposition    string
	SubmitReportDisposition string
}

// NewSender creates a Sender using the given sipgo client.
// sipLocal is the SMSC's own SIP URI, e.g. "sip:smsc.ims.mnc435.mcc311.3gppnetwork.org".
func NewSender(client *sipgo.Client, scAddr, sipLocal string, cfg Settings) *Sender {
	return &Sender{client: client, scAddr: scAddr, sipLocal: sipLocal, cfg: cfg}
}

// Send delivers msg to the IMS UE described by reg.
func (s *Sender) Send(ctx context.Context, msg *codec.Message, reg *registry.Registration) error {
	rpMR := byte(s.rpMRSeq.Add(1) & 0xFF)

	body, err := sip3gpp.Encode(msg, rpMR, s.scAddr)
	if err != nil {
		return fmt.Errorf("encode sip3gpp: %w", err)
	}

	req, err := s.buildRequest(msg, reg, body)
	if err != nil {
		return err
	}

	tx, err := s.client.TransactionRequest(ctx, req)
	if err != nil {
		return fmt.Errorf("send SIP MESSAGE: %w", err)
	}

	for {
		select {
		case resp := <-tx.Responses():
			if resp == nil {
				return fmt.Errorf("no response from S-CSCF")
			}
			code := resp.StatusCode
			if code < 200 {
				// Provisional (1xx) — wait for the final response.
				continue
			}
			slog.Info("ISC MT delivered",
				"id", msg.ID,
				"kind", msg.KindOrDefault(),
				"ref_id", msg.RefMessageID,
				"dst", msg.Destination.MSISDN,
				"scscf", reg.SCSCF,
				"status", code,
			)
			if code < 300 {
				return nil
			}
			return fmt.Errorf("S-CSCF returned %d %s", code, resp.Reason)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Sender) buildRequest(msg *codec.Message, reg *registry.Registration, body []byte) (*sip.Request, error) {
	// Parse the target SIP URI (SIPAOR of the UE)
	var targetURI sip.Uri
	if err := sip.ParseUri(reg.SIPAOR, &targetURI); err != nil {
		return nil, fmt.Errorf("parse target URI %q: %w", reg.SIPAOR, err)
	}

	req := sip.NewRequest(sip.MESSAGE, targetURI)

	fromURI, fromValue, err := buildFromURI(strings.TrimSpace(s.sipLocal), targetURI.Host, reg.SCSCF)
	if err != nil {
		return nil, err
	}
	fromParams := sip.NewParams()
	fromParams.Add("tag", newTag())
	req.AppendHeader(&sip.FromHeader{Address: fromURI, Params: fromParams})
	callIDValue := msg.CorrelationID
	if callIDValue == "" {
		callIDValue = msg.ID
	}
	if callIDValue != "" {
		callID := sip.CallIDHeader(callIDValue)
		req.AppendHeader(&callID)
	}

	req.AppendHeader(sip.NewHeader("Content-Type", sip3gpp.ContentType))
	if v := strings.TrimSpace(s.cfg.MTRequestDisposition); v != "" {
		req.AppendHeader(sip.NewHeader("Request-Disposition", v))
	}
	if v := strings.TrimSpace(s.cfg.AcceptContact); v != "" {
		req.AppendHeader(sip.NewHeader("Accept-Contact", v))
	}

	// P-Asserted-Identity: SMSC SIP URI (required for DR correlation per TS 24.341)
	pai := fmt.Sprintf("<%s>", fromValue)
	req.AppendHeader(sip.NewHeader("P-Asserted-Identity", pai))
	slog.Debug("ISC outbound identity",
		"from", fromValue,
		"pai", pai,
		"dst", reg.SIPAOR,
		"scscf", reg.SCSCF,
	)

	req.SetBody(body)

	// Route to the S-CSCF that owns this registration
	if reg.SCSCF != "" {
		routeStr := fmt.Sprintf("sip:%s;lr", reg.SCSCF)
		var routeURI sip.Uri
		if err := sip.ParseUri(routeStr, &routeURI); err == nil {
			req.AppendHeader(&sip.RouteHeader{Address: routeURI})
		}
	}

	return req, nil
}

func buildFromURI(explicit, targetHost, scscf string) (sip.Uri, string, error) {
	candidates := []string{
		explicit,
		fmt.Sprintf("sip:smsc@%s", targetHost),
		fmt.Sprintf("sip:smsc@%s", scscf),
		"sip:smsc@localhost",
	}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		var fromURI sip.Uri
		if err := sip.ParseUri(candidate, &fromURI); err == nil {
			return fromURI, candidate, nil
		}
	}
	return sip.Uri{}, "", fmt.Errorf("unable to build valid SIP From URI")
}

func newTag() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "smsc"
	}
	return fmt.Sprintf("%x", b[:])
}
