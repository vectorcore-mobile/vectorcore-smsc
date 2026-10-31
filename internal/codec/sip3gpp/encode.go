package sip3gpp

import (
	"fmt"
	"strings"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
)

// Encode serialises a canonical Message into an application/vnd.3gpp.sms
// RP-DATA body ready for use as a SIP MESSAGE body on the ISC interface.
//
// The message is encoded as SMS-DELIVER (MT direction, SC→MS), or carries
// msg.RawTPDU verbatim when set (e.g. an SMS-STATUS-REPORT).
// rpMR is the RP message reference to use (caller manages sequence).
// scAddr is the SMSC address to set as RP-OA (may be empty).
func Encode(msg *codec.Message, rpMR byte, scAddr string) ([]byte, error) {
	tpData, err := tpdu.EncodeMT(msg)
	if err != nil {
		return nil, fmt.Errorf("encode MT TP-DATA: %w", err)
	}

	// Build RP-DATA (SC→MS)
	var out []byte

	// RP-MTI (SC→MS = 0x00) | spare
	out = append(out, rpMTIDataSCtoMS)

	// RP-MR
	out = append(out, rpMR)

	// RP-OA (SC address)
	out = append(out, encodeRPAddress(scAddr)...)

	// RP-DA (empty — destination is in TP-DA)
	out = append(out, 0x00)

	// RP-UD: length + TP-DATA
	if len(tpData) > 233 {
		return nil, fmt.Errorf("TP-DATA too long for single RP-DATA: %d bytes", len(tpData))
	}
	out = append(out, byte(len(tpData)))
	out = append(out, tpData...)

	return out, nil
}

// EncodeMO serialises a canonical Message into an RP-DATA body for the
// MO direction (MS→SC), used when the SMSC needs to forward a SUBMIT.
func EncodeMO(msg *codec.Message, rpMR byte, scAddr string) ([]byte, error) {
	tpData, err := tpdu.EncodeSubmit(msg)
	if err != nil {
		return nil, fmt.Errorf("encode TP-SUBMIT: %w", err)
	}

	var out []byte
	out = append(out, rpMTIDataMStoSC)
	out = append(out, rpMR)
	// RP-OA empty (originator in TP layer)
	out = append(out, 0x00)
	// RP-DA (SMSC address)
	out = append(out, encodeRPAddress(scAddr)...)
	out = append(out, byte(len(tpData)))
	out = append(out, tpData...)
	return out, nil
}

// EncodeRPAck serialises an RP-ACK body.
// Per TS 24.011 the mandatory fields are just the RP message type and message reference.
func EncodeRPAck(rpMR byte, networkToMS bool) []byte {
	if networkToMS {
		return []byte{rpMTIACKSCtoMS, rpMR}
	}
	return []byte{rpMTIACKMStoSC, rpMR}
}

// encodeRPAddress encodes an E.164 MSISDN string as an RP address IE.
// Returns a zero-length IE (0x00) for an empty address.
func encodeRPAddress(msisdn string) []byte {
	normalized, err := normalizeAddressDigits(msisdn)
	if err != nil || normalized == "" {
		return []byte{0x00}
	}

	bcd, _ := encodeBCDLocal(normalized)

	// TOA: international (0x91)
	toa := byte(0x91)
	// length = 1 (TOA) + len(bcd)
	length := 1 + len(bcd)
	out := make([]byte, 1+length)
	out[0] = byte(length)
	out[1] = toa
	copy(out[2:], bcd)
	return out
}

func normalizeAddressDigits(msisdn string) (string, error) {
	msisdn = strings.TrimSpace(msisdn)
	if msisdn == "" {
		return "", nil
	}
	msisdn = strings.TrimPrefix(msisdn, "+")

	var digits strings.Builder
	digits.Grow(len(msisdn))
	for _, c := range msisdn {
		if c < '0' || c > '9' {
			return "", fmt.Errorf("non-digit %q in address", c)
		}
		digits.WriteRune(c)
	}
	return digits.String(), nil
}

// encodeBCDLocal converts a digit string to semi-octet BCD.
func encodeBCDLocal(digits string) ([]byte, error) {
	n := len(digits)
	out := make([]byte, (n+1)/2)
	for i, c := range digits {
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("non-digit %q in address", c)
		}
		d := byte(c - '0')
		if i%2 == 0 {
			out[i/2] = d
		} else {
			out[i/2] |= d << 4
		}
	}
	if n%2 == 1 {
		out[len(out)-1] |= 0xF0
	}
	return out, nil
}
