// Package sip3gpp handles encoding and decoding of application/vnd.3gpp.sms
// SIP MESSAGE bodies as used on the ISC interface (3GPP TS 24.341).
//
// The body carries an RP-DATA message (3GPP TS 24.011) which in turn
// contains the TP-DATA (SMS-SUBMIT or SMS-DELIVER).
package sip3gpp

import (
	"fmt"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
)

// RP-MTI values per 3GPP TS 24.011 Table 8.3 (bits 2-0 of first octet).
const (
	rpMTIDataMStoSC  = 0x00 // RP-DATA MS → network (MO)
	rpMTIDataSCtoMS  = 0x01 // RP-DATA Network → MS (MT)
	rpMTIACKMStoSC   = 0x02 // RP-ACK  MS → network
	rpMTIACKSCtoMS   = 0x03 // RP-ACK  Network → MS
	rpMTIErrorMStoSC = 0x04 // RP-ERROR MS → network
	rpMTIErrorSCtoMS = 0x05 // RP-ERROR Network → MS
	rpMTISMMA        = 0x06 // RP-SMMA MS → network
)

// ContentType is the MIME type for this codec.
const ContentType = "application/vnd.3gpp.sms"

// Decode parses an application/vnd.3gpp.sms body into a canonical Message.
// The body is expected to be an RP-DATA PDU per 3GPP TS 24.011 §7.3.
// If the body does not begin with a recognised RP-MTI, it is treated as raw
// TP-DATA (fallback for non-standard implementations).
func Decode(body []byte) (*codec.Message, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("empty vnd.3gpp.sms body")
	}

	mti := body[0] & 0x07
	switch mti {
	case rpMTIDataMStoSC, rpMTIDataSCtoMS:
		return decodeRPData(body)
	case rpMTIACKMStoSC, rpMTIACKSCtoMS:
		// RP-ACK: delivery acknowledgment — no TPDU to decode.
		return nil, nil
	case rpMTIErrorMStoSC, rpMTIErrorSCtoMS:
		// RP-ERROR: delivery failure notification — no TPDU to decode.
		return nil, nil
	case rpMTISMMA:
		// RP-SMMA: memory available notification — no TPDU.
		return nil, nil
	default:
		// Fallback: treat as raw TP-DATA
		return tpdu.Decode(body)
	}
}

// decodeRPData parses an RP-DATA message and extracts the TP-DATA payload.
//
// RP-DATA structure (3GPP TS 24.011 §7.3.1):
//
//	Octet 0:  RP-MTI (bits 2-0)
//	Octet 1:  RP-MR  (message reference)
//	Octet 2+: RP-OA  (originator address: length byte + address data)
//	Octet N+: RP-DA  (destination address: length byte + address data)
//	Octet M+: RP-UD  (user data: length byte + TP-DATA)
func decodeRPData(data []byte) (*codec.Message, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("RP-DATA too short: %d bytes", len(data))
	}

	pos := 2 // skip MTI + MR

	// RP-OA
	oaAddr, n, err := decodeRPAddress(data, pos)
	if err != nil {
		return nil, fmt.Errorf("decode RP-OA: %w", err)
	}
	pos += n

	// RP-DA
	daAddr, n, err := decodeRPAddress(data, pos)
	if err != nil {
		return nil, fmt.Errorf("decode RP-DA: %w", err)
	}
	pos += n

	// RP-UD length
	if pos >= len(data) {
		return nil, fmt.Errorf("RP-DATA truncated before RP-UD")
	}
	rpUDLen := int(data[pos])
	pos++

	if pos+rpUDLen > len(data) {
		return nil, fmt.Errorf("RP-UD length %d exceeds data", rpUDLen)
	}

	tpData := data[pos : pos+rpUDLen]
	// RP-MTI gives the direction, which TP-MTI needs: 0x02 from the UE is
	// SMS-COMMAND, towards the UE it is SMS-STATUS-REPORT.
	decodeTPDU := tpdu.DecodeMT
	if data[0]&0x07 == rpMTIDataMStoSC {
		decodeTPDU = tpdu.DecodeMO
	}
	msg, err := decodeTPDU(tpData)
	if err != nil {
		return nil, fmt.Errorf("decode TP-DATA: %w", err)
	}
	msg.RPMR = data[1]

	// Supplement address information from RP layer if TP layer is sparse
	if msg.Source.MSISDN == "" && oaAddr != "" {
		msg.Source.MSISDN = oaAddr
	}
	if msg.Destination.MSISDN == "" && daAddr != "" {
		msg.Destination.MSISDN = daAddr
	}

	return msg, nil
}

// decodeRPAddress reads an RP address IE starting at data[pos].
// Returns the E.164 MSISDN string (may be empty), bytes consumed, and error.
//
// RP address structure:
//
//	Octet 0: length of contents in octets (0 = empty address)
//	If length > 0:
//	  Octet 1: Type of Number / Numbering Plan (same bit layout as TP address TOA)
//	  Octets 2+: semi-octet BCD digits
func decodeRPAddress(data []byte, pos int) (string, int, error) {
	if pos >= len(data) {
		return "", 0, fmt.Errorf("RP address out of bounds at pos %d", pos)
	}
	length := int(data[pos])
	if length == 0 {
		return "", 1, nil
	}
	if pos+1+length > len(data) {
		return "", 0, fmt.Errorf("RP address data truncated")
	}

	toa := data[pos+1]
	ton := (toa >> 4) & 0x07

	// BCD digits occupy length-1 octets (1 octet is the TOA)
	bcdOctets := length - 1
	bcdData := data[pos+2 : pos+1+length]

	// Count actual digits from BCD (each octet = 2 digits, trailing 0xF ignored)
	numDigits := bcdOctets * 2
	if bcdOctets > 0 && (bcdData[bcdOctets-1]>>4)&0x0F == 0x0F {
		numDigits--
	}

	msisdn := decodeBCD(bcdData, numDigits)
	_ = ton // international flag informational only

	return msisdn, 1 + length, nil
}

// decodeBCD converts semi-octet BCD bytes to a digit string.
func decodeBCD(data []byte, numDigits int) string {
	digits := make([]byte, 0, numDigits)
	for _, b := range data {
		lo := b & 0x0F
		hi := (b >> 4) & 0x0F
		if lo <= 9 {
			digits = append(digits, '0'+lo)
		}
		if hi <= 9 && len(digits) < numDigits {
			digits = append(digits, '0'+hi)
		}
	}
	return string(digits)
}
