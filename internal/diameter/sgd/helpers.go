package sgd

import (
	"log/slog"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

// newSessionID generates a unique Diameter Session-Id string.
func newSessionID() string { return dcodec.NewSessionID("vectorcore-smsc") }

// sendAnswer sends a simple answer with just Result-Code, Origin-Host, Origin-Realm.
func sendAnswer(p *diameter.Peer, req *dcodec.Message, resultCode uint32) {
	sendAnswerWithAVPs(p, req, resultCode)
}

func sendAnswerWithAVPs(p *diameter.Peer, req *dcodec.Message, resultCode uint32, extra ...*dcodec.AVP) {
	b := dcodec.NewAnswer(req)
	if sessionID := req.FindAVP(dcodec.CodeSessionID, 0); sessionID != nil {
		b.Add(dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, string(sessionID.Data)))
	}
	b.Add(
		dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, resultCode),
		authSessionStateFromRequest(req),
		dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, p.Cfg().LocalFQDN),
		dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, p.Cfg().LocalRealm),
	)
	b.Add(extra...)
	if err := p.Send(b.Build()); err != nil {
		slog.Warn("sgd answer send failed",
			"peer", p.RemoteFQDN,
			"command_code", req.Header.CommandCode,
			"hop_by_hop", req.Header.HopByHop,
			"result_code", resultCode,
			"err", err,
		)
		return
	}
	slog.Debug("sgd answer sent",
		"peer", p.RemoteFQDN,
		"command_code", req.Header.CommandCode,
		"hop_by_hop", req.Header.HopByHop,
		"result_code", resultCode,
	)
}

func authSessionStateFromRequest(req *dcodec.Message) *dcodec.AVP {
	if req != nil {
		if avp := req.FindAVP(dcodec.CodeAuthSessionState, 0); avp != nil {
			if v, err := avp.Uint32(); err == nil {
				return dcodec.NewUint32(dcodec.CodeAuthSessionState, 0, dcodec.FlagMandatory, v)
			}
		}
	}
	return dcodec.NewUint32(dcodec.CodeAuthSessionState, 0, dcodec.FlagMandatory, dcodec.AuthSessionStateNoStateMaintained)
}

// encodeSCAddress mirrors codec/sgd's SC-Address encoder for use within
// this package.
func encodeSCAddress(sc, encoding string) []byte {
	if sc == "" {
		return nil
	}
	if sc[0] == '+' {
		sc = sc[1:]
	}
	if encoding == "ascii_digits" {
		return []byte(sc)
	}
	return encodeBCDDigits(sc)
}

func encodeBCDDigits(digits string) []byte {
	size := (len(digits) + 1) / 2
	buf := make([]byte, size)
	for i := 0; i < len(digits); i++ {
		d := digits[i] - '0'
		if i%2 == 0 {
			buf[i/2] = d
		} else {
			buf[i/2] |= d << 4
		}
	}
	if len(digits)%2 != 0 {
		buf[size-1] |= 0xF0
	}
	return buf
}

func encodeSubmitReportAck(now time.Time) []byte {
	return append([]byte{0x01, 0x00}, encodeSCTS(now)...)
}

func encodeSCTS(t time.Time) []byte {
	_, offset := t.Zone()
	quarters := offset / (15 * 60)
	neg := quarters < 0
	if neg {
		quarters = -quarters
	}
	tz := encodeBCDByte(byte(quarters))
	if neg {
		tz |= 0x08
	}
	return []byte{
		encodeBCDByte(byte(t.Year() % 100)),
		encodeBCDByte(byte(t.Month())),
		encodeBCDByte(byte(t.Day())),
		encodeBCDByte(byte(t.Hour())),
		encodeBCDByte(byte(t.Minute())),
		encodeBCDByte(byte(t.Second())),
		tz,
	}
}

func encodeBCDByte(v byte) byte {
	return ((v % 10) << 4) | (v / 10)
}
