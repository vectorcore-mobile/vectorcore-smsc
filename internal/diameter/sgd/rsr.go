package sgd

import (
	"context"
	"fmt"
	"log/slog"

	sgdcodec "github.com/svinson1121/vectorcore-smsc/internal/codec/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

// SendRSR sends a Report-SM-Delivery-Status-Request to the HSS via the S6c
// interface.  outcome: 0=success (SMSGWSuccessful), 1=absent, 2=other-error.
//
// In the current phase this is sent to the SGd peer (MME) that forwarded the
// original request.  Full S6c support (separate HSS connection) is Phase 5.
func SendRSR(ctx context.Context, p *diameter.Peer, msisdn, scAddr, scAddrEncoding string, msisdnEncoding sgdcodec.MSISDNEncoding, outcome uint32) error {
	avps, err := sgdcodec.EncodeRSR(msisdn, scAddr, scAddrEncoding, msisdnEncoding, outcome)
	if err != nil {
		return fmt.Errorf("sgd RSR encode: %w", err)
	}

	b := dcodec.NewRequest(dcodec.CmdReportSMDeliveryStatus, dcodec.App3GPP_SGd)
	b.Add(
		dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, newSessionID()),
		dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, p.Cfg().LocalFQDN),
		dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, p.Cfg().LocalRealm),
		dcodec.NewString(dcodec.CodeDestinationHost, 0, dcodec.FlagMandatory, p.RemoteFQDN),
		dcodec.NewString(dcodec.CodeDestinationRealm, 0, dcodec.FlagMandatory, p.RemoteRealm),
	)
	b.Add(avps...)

	if err := p.Send(b.Build()); err != nil {
		return fmt.Errorf("sgd RSR send: %w", err)
	}

	slog.Info("sgd RSR sent",
		"peer", p.RemoteFQDN,
		"msisdn", msisdn,
		"outcome", outcome,
	)
	return nil
}
