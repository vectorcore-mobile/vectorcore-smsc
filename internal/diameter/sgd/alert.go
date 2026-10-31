package sgd

import (
	"context"
	"fmt"
	"log/slog"

	sgdcodec "github.com/svinson1121/vectorcore-smsc/internal/codec/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

// SendAlertSC sends an Alert-Service-Centre-Request (ALR) to the MME.
// This is triggered when a UE re-attaches and there are deferred MT messages
// waiting in the store-and-forward queue.
// msisdn is the subscriber E.164; scAddr is our SMSC address.
func SendAlertSC(ctx context.Context, p *diameter.Peer, msisdn, scAddr, scAddrEncoding string, msisdnEncoding sgdcodec.MSISDNEncoding) error {
	msg := buildAlertSCRequest(p.Cfg(), p.RemoteFQDN, p.RemoteRealm, msisdn, scAddr, scAddrEncoding, msisdnEncoding)
	if err := p.Send(msg); err != nil {
		return fmt.Errorf("sgd Alert-SC send: %w", err)
	}

	slog.Info("sgd Alert-SC sent",
		"peer", p.RemoteFQDN,
		"msisdn", msisdn,
	)
	return nil
}

func buildAlertSCRequest(cfg diameter.Config, remoteFQDN, remoteRealm, msisdn, scAddr, scAddrEncoding string, msisdnEncoding sgdcodec.MSISDNEncoding) *dcodec.Message {
	b := dcodec.NewRequest(dcodec.CmdAlertServiceCentre, dcodec.App3GPP_SGd)
	b.Add(
		dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, newSessionID()),
		dcodec.NewUint32(dcodec.CodeAuthSessionState, 0, dcodec.FlagMandatory, dcodec.AuthSessionStateNoStateMaintained),
		dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, cfg.LocalFQDN),
		dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, cfg.LocalRealm),
		dcodec.NewString(dcodec.CodeDestinationHost, 0, dcodec.FlagMandatory, remoteFQDN),
		dcodec.NewString(dcodec.CodeDestinationRealm, 0, dcodec.FlagMandatory, remoteRealm),
		dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
			sgdcodec.EncodeMSISDN(msisdn, msisdnEncoding)),
		dcodec.NewOctetString(dcodec.CodeSCAddress, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
			encodeSCAddress(scAddr, scAddrEncoding)),
	)
	return b.Build()
}
