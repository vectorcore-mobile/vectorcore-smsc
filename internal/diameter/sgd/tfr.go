package sgd

import (
	"log/slog"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	sgdcodec "github.com/svinson1121/vectorcore-smsc/internal/codec/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

// HandleTFR processes an MT-Forward-Short-Message answer for an outbound send.
// The reply is correlated elsewhere; this helper only decodes the payload shape.
func HandleTFR(p *diameter.Peer, req *dcodec.Message, onMsg OnMessageFunc, opts sgdcodec.AddressOptions) {
	msg, err := sgdcodec.DecodeTFRWith(req, opts)
	if err != nil {
		slog.Error("sgd TFR decode failed", "peer", p.RemoteFQDN, "err", err)
		sendAnswer(p, req, dcodec.DiameterUnableToComply)
		return
	}

	msg.IngressPeer = p.RemoteFQDN
	msg.IngressInterface = codec.InterfaceSGd

	slog.Info("sgd TFR received (MO)",
		"peer", p.RemoteFQDN,
		"src", msg.Source.MSISDN,
		"dst", msg.Destination.MSISDN,
		"encoding", msg.Encoding,
	)
	slog.Debug("sgd TFR decoded",
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
