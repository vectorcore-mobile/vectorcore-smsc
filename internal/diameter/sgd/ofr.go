package sgd

import (
	"log/slog"

	sgdcodec "github.com/svinson1121/vectorcore-smsc/internal/codec/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

// HandleOFR processes an MO-Forward-Short-Message-Request from the MME.
// It decodes the SM-RP-UI, fires the OnMessage callback, and returns OFA.
func HandleOFR(p *diameter.Peer, req *dcodec.Message, onMsg OnMessageFunc, opts sgdcodec.AddressOptions) {
	msg, err := sgdcodec.DecodeOFRWith(req, opts)
	if err != nil {
		slog.Error("sgd OFR decode failed", "peer", p.RemoteFQDN, "err", err)
		sendAnswer(p, req, dcodec.DiameterUnableToComply)
		return
	}

	msg.IngressPeer = p.RemoteFQDN

	slog.Info("sgd OFR received",
		"peer", p.RemoteFQDN,
		"src", msg.Source.MSISDN,
		"dst", msg.Destination.MSISDN,
		"encoding", msg.Encoding,
	)
	slog.Debug("sgd OFR decoded",
		"peer", p.RemoteFQDN,
		"src", msg.Source.MSISDN,
		"dst", msg.Destination.MSISDN,
		"tp_mr", msg.TPMR,
		"binary_len", len(msg.Binary),
	)

	sendAnswer(p, req, dcodec.DiameterSuccess)

	if onMsg != nil {
		onMsg(msg, p.RemoteFQDN)
	}
}
