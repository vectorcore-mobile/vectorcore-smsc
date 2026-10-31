// Package dr handles cross-interface delivery report correlation.
// When a message originates on interface A and is delivered on interface B,
// the correlator generates the appropriate DR format for interface A and
// arranges for it to be sent back to the originating peer.
package dr

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	smppcodec "github.com/svinson1121/vectorcore-smsc/internal/codec/smpp"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
	s6cdiam "github.com/svinson1121/vectorcore-smsc/internal/diameter/s6c"
	"github.com/svinson1121/vectorcore-smsc/internal/numbering"
	"github.com/svinson1121/vectorcore-smsc/internal/smpp"
	smppClient "github.com/svinson1121/vectorcore-smsc/internal/smpp/client"
	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

// SMPPRegistry is used to send DR deliver_sm back to the originating ESME.
type SMPPRegistry interface {
	GetByName(name string) *smpp.Link
}

// S6cReporter sends terminal MT outcomes to the HSS over S6c.
type S6cReporter interface {
	ReportDelivery(msisdn, imsi, scAddr, alertCorrelationID string, cause uint32, diagnostic *uint32) (*s6cdiam.ReportDeliveryResult, error)
}

// Dispatcher submits a new SMSC-generated message (an SMS-STATUS-REPORT)
// into the normal forwarding pipeline.
type Dispatcher interface {
	Dispatch(ctx context.Context, msg *codec.Message)
}

// Correlator generates delivery reports for delivered or failed messages
// and routes them back to the originating interface.
type Correlator struct {
	st         store.Store
	smppMgr    *smppClient.Manager
	smppReg    SMPPRegistry
	s6c        S6cReporter
	dispatcher Dispatcher
	scAddr     string
}

// New creates a Correlator.
func New(st store.Store, smppMgr *smppClient.Manager, smppReg SMPPRegistry, scAddr string) *Correlator {
	return &Correlator{st: st, smppMgr: smppMgr, smppReg: smppReg, scAddr: scAddr}
}

// SetS6cReporter swaps the active S6c reporter as peer selection changes.
func (c *Correlator) SetS6cReporter(r S6cReporter) { c.s6c = r }

// SetDispatcher sets the pipeline used to deliver SMS-STATUS-REPORTs. It is
// a setter because the forwarder is constructed after the correlator.
func (c *Correlator) SetDispatcher(d Dispatcher) { c.dispatcher = d }

// DrStatus values passed to Report.
const (
	StatusDelivered = "DELIVRD"
	StatusFailed    = "FAILED"
	StatusExpired   = "EXPIRED"
	StatusUndeliv   = "UNDELIV"
)

// Report records a delivery report for the given message and, if the origin
// interface supports it, sends the DR back to the originating peer.
func (c *Correlator) Report(ctx context.Context, m store.Message, status string) {
	dr := store.DeliveryReport{
		ID:          newUUID(),
		MessageID:   m.ID,
		Status:      status,
		EgressIface: m.EgressIface,
		RawReceipt:  fmt.Sprintf("id:%s stat:%s", m.ID, status),
		ReportedAt:  time.Now(),
	}
	if err := c.st.SaveDeliveryReport(ctx, dr); err != nil {
		slog.Error("correlator: save delivery report", "id", m.ID, "err", err)
	}
	c.reportS6c(ctx, m, status)

	if store.NormalizeMessageKind(m.Kind) == store.MessageKindStatusReport {
		// The outcome of a status report is recorded but never itself
		// reported on: no SMPP receipt and no further STATUS-REPORT.
		slog.Info("correlator: status report outcome",
			"id", m.ID,
			"ref_id", m.RefMessageID,
			"dst", m.DstMSISDN,
			"egress", m.EgressIface,
			"status", status,
		)
		return
	}

	switch m.OriginIface {
	case "smpp":
		c.reportSMPP(ctx, m, status)
	case "sgd", "sip3gpp":
		c.reportStatusReport(ctx, m, status)
	default:
		// SIP SIMPLE IMDN generation is not implemented; log only.
		slog.Info("correlator: DR recorded",
			"id", m.ID,
			"origin", m.OriginIface,
			"egress", m.EgressIface,
			"status", status,
		)
	}
}

func (c *Correlator) reportS6c(ctx context.Context, m store.Message, status string) {
	if m.EgressIface != "sgd" {
		return
	}
	c.sendRSDS(ctx, m, s6cCauseForStatus(status), status)
}

// ReportDeferred sends RSDS when an SGd MT attempt is deferred because the
// subscriber can't take the message now: cause is absent-user or
// memory-capacity-exceeded. The HSS then stores Message Waiting Data (MNRF or
// MCEF) and alerts the SMSC (ALR) when the subscriber is reachable again or
// has memory available (TS 23.040 §10.1, TS 29.338 §5.3.2.4). The forwarder
// calls this once per episode, not on every retry.
func (c *Correlator) ReportDeferred(ctx context.Context, m store.Message, cause uint32) {
	status := "deferred_absent"
	if cause == dcodec.SMDeliveryCauseMemoryCapacityExceeded {
		status = "deferred_memory_full"
	}
	c.sendRSDS(ctx, m, cause, status)
}

// sendRSDS reports an MT outcome for m's destination to the HSS over S6c.
func (c *Correlator) sendRSDS(ctx context.Context, m store.Message, cause uint32, status string) {
	if c.s6c == nil || c.scAddr == "" {
		return
	}

	sub, err := c.st.GetSubscriber(ctx, m.DstMSISDN)
	if err != nil {
		slog.Warn("correlator: S6c subscriber lookup failed", "id", m.ID, "dst", m.DstMSISDN, "err", err)
		return
	}

	var imsi string
	if sub != nil {
		imsi = sub.IMSI
	}

	if _, err := c.s6c.ReportDelivery(m.DstMSISDN, imsi, c.scAddr, m.AlertCorrelationID, cause, nil); err != nil {
		slog.Warn("correlator: S6c RSDS failed",
			"id", m.ID,
			"dst", m.DstMSISDN,
			"imsi", imsi,
			"status", status,
			"cause", cause,
			"err", err,
		)
		return
	}

	slog.Info("correlator: S6c RSDS sent",
		"id", m.ID,
		"dst", m.DstMSISDN,
		"imsi", imsi,
		"sc_addr", c.scAddr,
		"status", status,
		"cause", cause,
	)
}

func s6cCauseForStatus(status string) uint32 {
	switch status {
	case StatusDelivered:
		return dcodec.SMDeliveryCauseSuccessfulTransfer
	default:
		return dcodec.SMDeliveryCauseAbsentUser
	}
}

// reportStatusReport builds an SMS-STATUS-REPORT for a handset-originated SM
// whose sender requested one (TP-SRR) and dispatches it as a new MT message
// to the original sender. The report is routed like any MT (fresh S6c lookup
// of the sender, store-and-forward, retries) and is independent of the
// original message's transaction.
func (c *Correlator) reportStatusReport(ctx context.Context, m store.Message, status string) {
	if !m.DRRequired {
		return
	}
	if c.dispatcher == nil {
		slog.Warn("correlator: status report requested but no dispatcher configured", "id", m.ID)
		return
	}
	if m.SrcMSISDN == "" || m.DstMSISDN == "" || m.TPMR == nil {
		slog.Warn("correlator: cannot build status report: original message incomplete",
			"id", m.ID,
			"src", m.SrcMSISDN,
			"dst", m.DstMSISDN,
			"has_tp_mr", m.TPMR != nil,
		)
		return
	}

	now := time.Now().UTC()
	// TP-DT is the delivery time for a delivered SM, otherwise the time the
	// SC gave up on it.
	dt := now
	if status == StatusDelivered && m.DeliveredAt != nil {
		dt = *m.DeliveredAt
	}
	scts := m.SubmittedAt
	if scts.IsZero() {
		scts = dt
	}
	st := tpStatusForDR(status)
	mr := byte(*m.TPMR)

	tpData, err := tpdu.EncodeStatusReport(mr, codec.Address{MSISDN: m.DstMSISDN}, scts, dt, st)
	if err != nil {
		slog.Error("correlator: encode status report failed", "id", m.ID, "err", err)
		return
	}

	report := &codec.Message{
		ID:           newUUID(),
		Kind:         codec.MessageKindStatusReport,
		RefMessageID: m.ID,
		RawTPDU:      tpData,
		TPMR:         mr,
		Source:       codec.Address{MSISDN: strings.TrimPrefix(c.scAddr, "+")},
		Destination: codec.Address{
			MSISDN: m.SrcMSISDN,
			TON:    numbering.TONInternational,
			NPI:    0x01,
		},
		IngressInterface: codec.InterfaceInternal,
		Timestamp:        now,
	}

	slog.Info("correlator: dispatching SMS-STATUS-REPORT",
		"id", report.ID,
		"ref_id", m.ID,
		"tp_mr", mr,
		"dst", m.SrcMSISDN,
		"recipient", m.DstMSISDN,
		"status", status,
		"tp_st", fmt.Sprintf("0x%02X", st),
	)
	// Dispatch asynchronously: delivery to the sender involves an S6c lookup
	// and an SGd round trip that must not hold up the original message.
	go c.dispatcher.Dispatch(ctx, report)
}

// tpStatusForDR maps a final DR status to TP-ST (3GPP TS 23.040 §9.2.3.15).
func tpStatusForDR(status string) byte {
	switch status {
	case StatusDelivered:
		return tpdu.STReceivedBySME
	case StatusExpired:
		return tpdu.STPermValidityPeriodExpired
	case StatusFailed, StatusUndeliv:
		// Routing failed permanently (e.g. unknown subscriber).
		return tpdu.STPermNotObtainable
	default:
		return tpdu.STPermRemoteProcedureError
	}
}

// reportSMPP sends an SMPP deliver_sm delivery receipt back to the originating ESME.
func (c *Correlator) reportSMPP(ctx context.Context, m store.Message, status string) {
	if c.smppMgr == nil && c.smppReg == nil {
		return
	}
	if m.OriginPeer == "" {
		return
	}
	// DRRequired is set for registered_delivery mode 01 (receipt on success
	// and failure). Without it no success receipt is sent. Failure receipts
	// are still sent, because mode 10 (failure only) is not recorded separately.
	if status == StatusDelivered && !m.DRRequired {
		slog.Debug("correlator: SMPP success receipt not requested", "peer", m.OriginPeer, "id", m.ID)
		return
	}

	msgID := m.SMPPMsgID
	if msgID == "" {
		msgID = m.ID
	}

	pdu := smppcodec.EncodeDeliveryReceipt(msgID, m.SrcMSISDN, m.DstMSISDN, status, "000")
	pdu.SequenceNumber = 0

	// Try the named outbound client link first.
	if c.smppMgr != nil {
		if _, err := c.smppMgr.SendViaPeer(m.OriginPeer, pdu); err == nil {
			slog.Info("correlator: SMPP DR sent via client", "peer", m.OriginPeer, "id", m.ID)
			return
		}
	}

	// Fall back to the inbound server link (the ESME that submitted the message).
	if c.smppReg != nil {
		link := c.smppReg.GetByName(m.OriginPeer)
		if link != nil {
			if err := link.Send(pdu); err != nil {
				slog.Warn("correlator: SMPP DR via server link failed",
					"peer", m.OriginPeer, "id", m.ID, "err", err)
			} else {
				slog.Info("correlator: SMPP DR queued via server link", "peer", m.OriginPeer, "id", m.ID)
			}
			return
		}
	}

	slog.Warn("correlator: no SMPP link found for DR", "peer", m.OriginPeer, "id", m.ID)
}

func newUUID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
