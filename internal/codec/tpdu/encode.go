package tpdu

import (
	"fmt"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
)

// EncodeMT returns the TP-DATA to send SC→MS for msg. Pre-encoded TP-DATA in
// msg.RawTPDU (e.g. an SMS-STATUS-REPORT) is returned verbatim; otherwise an
// SMS-DELIVER is encoded from the message fields.
func EncodeMT(msg *codec.Message) ([]byte, error) {
	if len(msg.RawTPDU) == 0 {
		return EncodeDeliver(msg)
	}
	switch msg.RawTPDU[0] & 0x03 {
	case mtiSMSDeliver, mtiSMSStatusReport:
		return append([]byte(nil), msg.RawTPDU...), nil
	default:
		return nil, fmt.Errorf("raw TP-DATA has TP-MTI 0x%02X, not valid SC→MS", msg.RawTPDU[0]&0x03)
	}
}

// EncodeDeliver encodes a canonical Message into an SMS-DELIVER TP-DATA byte slice.
// SMS-DELIVER is sent MT (from SMSC to UE) per 3GPP TS 23.040 §9.2.2.1.
func EncodeDeliver(msg *codec.Message) ([]byte, error) {
	var out []byte

	// Build TP-OA (originating address)
	oa, err := encodeAddress(msg.Source)
	if err != nil {
		return nil, fmt.Errorf("encode TP-OA: %w", err)
	}

	// Preserve raw UDH when available so binary payloads keep all IEs intact.
	udh := encodeUDH(msg)
	ud, udl, err := encodeUD(msg, udh)
	if err != nil {
		return nil, fmt.Errorf("encode TP-UD: %w", err)
	}

	// Byte 0: TP-MTI=00 (DELIVER) | TP-MMS=1 (bit2) | TP-UDHI (bit6)
	b0 := byte(mtiSMSDeliver) | 0x04 // MMS: more messages to send = 1
	if msg.TPSRRequired {
		b0 |= 0x20 // TP-SRI
	}
	if len(udh) > 0 {
		b0 |= 0x40 // TP-UDHI
	}
	out = append(out, b0)

	// TP-OA
	out = append(out, oa...)

	// TP-PID
	out = append(out, 0x00)

	// TP-DCS — preserve the original inbound DCS byte verbatim
	out = append(out, dcsForEgress(msg))

	// TP-SCTS (7 bytes)
	ts := msg.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	out = append(out, encodeSCTS(ts)...)

	// TP-UDL
	out = append(out, byte(udl))

	// TP-UD
	out = append(out, ud...)

	return out, nil
}

// EncodeSubmit encodes a canonical Message into an SMS-SUBMIT TP-DATA byte slice.
// Used when the SMSC forwards MO SMS to another interface that expects a SUBMIT.
func EncodeSubmit(msg *codec.Message) ([]byte, error) {
	var out []byte

	da, err := encodeAddress(msg.Destination)
	if err != nil {
		return nil, fmt.Errorf("encode TP-DA: %w", err)
	}

	udh := encodeUDH(msg)
	ud, udl, err := encodeUD(msg, udh)
	if err != nil {
		return nil, fmt.Errorf("encode TP-UD: %w", err)
	}

	// Byte 0: TP-MTI=01 (SUBMIT) | TP-VPF=10 (relative, bit4) | TP-UDHI (bit6)
	b0 := byte(mtiSMSSubmit)
	if msg.TPSRRequired {
		b0 |= 0x20 // TP-SRR
	}
	if len(udh) > 0 {
		b0 |= 0x40 // TP-UDHI
	}
	var vpf byte
	if msg.ValidityPeriod != nil {
		vpf = 0x02 // relative
		b0 |= vpf << 3
	}
	out = append(out, b0)

	// TP-MR
	out = append(out, msg.TPMR)

	// TP-DA
	out = append(out, da...)

	// TP-PID
	out = append(out, 0x00)

	// TP-DCS — preserve the original inbound DCS byte verbatim
	out = append(out, dcsForEgress(msg))

	// TP-VP (relative, 1 byte) if present
	if vpf == 0x02 && msg.ValidityPeriod != nil {
		out = append(out, encodeVPRelative(*msg.ValidityPeriod))
	}

	// TP-UDL
	out = append(out, byte(udl))

	// TP-UD
	out = append(out, ud...)

	return out, nil
}

// EncodeStatusReport encodes an SMS-STATUS-REPORT TP-DATA byte slice
// (3GPP TS 23.040 §9.2.2.3), sent MT from the SC to the originator of the
// referenced SMS-SUBMIT.
//
//	mr        TP-MR of the original SMS-SUBMIT
//	recipient TP-RA: the recipient the original SM was addressed to
//	scts      TP-SCTS: when the SC received the original SM
//	dt        TP-DT: when the SM was delivered or the SC gave up
//	st        TP-ST: one of the ST* constants
//
// No TP-PI is included, so the report carries no PID/DCS/UD.
func EncodeStatusReport(mr byte, recipient codec.Address, scts, dt time.Time, st byte) ([]byte, error) {
	if recipient.MSISDN == "" {
		return nil, fmt.Errorf("encode TP-RA: recipient address required")
	}
	ra, err := encodeAddress(codec.Address{
		MSISDN: recipient.MSISDN,
		TON:    recipient.TON,
		NPI:    recipient.NPI,
	})
	if err != nil {
		return nil, fmt.Errorf("encode TP-RA: %w", err)
	}

	out := make([]byte, 0, 2+len(ra)+7+7+1)

	// Byte 0: TP-MTI=10 (STATUS-REPORT) | TP-MMS=1 (bit2, no more messages)
	// TP-SRQ=0 (report is the result of an SMS-SUBMIT), TP-UDHI=0.
	out = append(out, byte(mtiSMSStatusReport)|0x04)
	out = append(out, mr)
	out = append(out, ra...)
	out = append(out, encodeSCTS(scts)...)
	out = append(out, encodeSCTS(dt)...)
	out = append(out, st)
	return out, nil
}

// dcsForEgress returns the DCS byte to embed in the outbound TP-DATA.
// When the message was received on any inbound interface, msg.DCS carries the
// original wire value (e.g. 0xF5 for binary class-1 WAP push) and is used
// verbatim.  Only when msg.DCS is zero (message originated internally) does
// it fall back to BuildDCS to derive a minimal DCS from the encoding type.
func dcsForEgress(msg *codec.Message) byte {
	if msg.DCS != 0 {
		return msg.DCS
	}
	return BuildDCS(msg.Encoding)
}

// encodeUD builds the TP-UD bytes and returns them along with the UDL value.
// For GSM7, UDL = number of septets. For all others, UDL = number of octets.
func encodeUD(msg *codec.Message, udh []byte) (ud []byte, udl int, err error) {
	switch msg.Encoding {
	case codec.EncodingGSM7:
		packed, septets := EncodeGSM7(msg.Text)
		if len(udh) > 0 {
			// Prepend UDH and recalculate packing with fill bits
			ud, udl = packGSM7WithUDH(udh, msg.Text)
		} else {
			ud = packed
			udl = septets
		}

	case codec.EncodingUCS2:
		payload := EncodeUCS2(msg.Text)
		if len(udh) > 0 {
			ud = append(udh, payload...)
		} else {
			ud = payload
		}
		udl = len(ud)

	case codec.EncodingBinary:
		if len(udh) > 0 {
			ud = append(udh, msg.Binary...)
		} else {
			ud = msg.Binary
		}
		udl = len(ud)

	default:
		return nil, 0, fmt.Errorf("unsupported encoding: %d", msg.Encoding)
	}
	return ud, udl, nil
}

func encodeUDH(msg *codec.Message) []byte {
	if msg != nil && msg.UDH != nil && len(msg.UDH.Raw) > 0 {
		udh := make([]byte, len(msg.UDH.Raw))
		copy(udh, msg.UDH.Raw)
		return udh
	}
	if msg != nil && msg.Concat != nil {
		return buildConcatUDH(msg.Concat)
	}
	return nil
}

// packGSM7WithUDH prepends the UDH, adds fill bits so the text starts on a
// septet boundary, then packs everything into octets.
// Returns the packed bytes and the total UDL in septets.
func packGSM7WithUDH(udh []byte, text string) ([]byte, int) {
	udhLen := len(udh)
	// Fill bits to align to septet boundary
	fillBits := (7 - (udhLen*8)%7) % 7
	// Total septets = fill + UDH septets (ceil) + text septets
	udhSeptets := (udhLen*8 + fillBits) / 7

	textSeptets := make([]byte, 0, len(text)*2)
	for _, r := range []rune(text) {
		if b, ok := runeToGSM7[r]; ok {
			textSeptets = append(textSeptets, b)
		} else if b, ok := runeToGSM7Ext[r]; ok {
			textSeptets = append(textSeptets, 0x1B, b)
		} else {
			textSeptets = append(textSeptets, runeToGSM7['?'])
		}
	}

	totalSeptets := udhSeptets + len(textSeptets)
	totalBits := udhLen*8 + fillBits + len(textSeptets)*7
	out := make([]byte, (totalBits+7)/8)

	// Copy UDH bytes directly
	copy(out, udh)

	// Pack text septets starting at bit offset udhLen*8 + fillBits
	bitPos := udhLen*8 + fillBits
	for _, s := range textSeptets {
		byteIdx := bitPos / 8
		bitIdx := bitPos % 8
		out[byteIdx] |= s << uint(bitIdx)
		if bitIdx > 1 && byteIdx+1 < len(out) {
			out[byteIdx+1] |= s >> uint(8-bitIdx)
		}
		bitPos += 7
	}

	return out, totalSeptets
}

// encodeAddress converts an Address to TP address bytes (len + TOA + BCD).
func encodeAddress(addr codec.Address) ([]byte, error) {
	if addr.Alpha != "" {
		return encodeAlphaAddress(addr.Alpha)
	}
	digits := addr.MSISDN
	if digits == "" {
		return []byte{0x00, 0x80}, nil // empty address
	}

	// Default TOA: international E.164
	ton := addr.TON
	npi := addr.NPI
	if ton == 0 && npi == 0 {
		ton = 0x01 // international
		npi = 0x01 // ISDN/E.164
	}
	toa := byte(0x80) | (ton << 4) | npi

	bcd, err := encodeBCD(digits)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 2+len(bcd))
	out[0] = byte(len(digits))
	out[1] = toa
	copy(out[2:], bcd)
	return out, nil
}

// encodeAlphaAddress encodes an alphanumeric sender ID as a GSM7 address field.
func encodeAlphaAddress(alpha string) ([]byte, error) {
	packed, septets := EncodeGSM7(alpha)
	// TOA: type=alphanumeric (101), NPI=unknown (0000)
	toa := byte(0xD0) // 1 101 0000
	out := make([]byte, 2+len(packed))
	out[0] = byte(septets) // numDigits = number of septets
	out[1] = toa
	copy(out[2:], packed)
	return out, nil
}

// encodeBCD converts a digit string to semi-octet BCD bytes.
func encodeBCD(digits string) ([]byte, error) {
	n := len(digits)
	out := make([]byte, (n+1)/2)
	for i, c := range digits {
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("non-digit character %q in address", c)
		}
		d := byte(c - '0')
		if i%2 == 0 {
			out[i/2] = d
		} else {
			out[i/2] |= d << 4
		}
	}
	if n%2 == 1 {
		out[len(out)-1] |= 0xF0 // pad with 0xF
	}
	return out, nil
}

// buildConcatUDH builds a UDH byte slice for the given ConcatInfo.
// Uses a 16-bit reference IE (0x08) to avoid ref number collisions.
func buildConcatUDH(c *codec.ConcatInfo) []byte {
	// UDHL(1) + IEI(1) + IEL(1) + ref_hi(1) + ref_lo(1) + total(1) + seq(1) = 7 bytes
	return []byte{
		0x06, // UDHL
		0x08, // IEI: concat 16-bit ref
		0x04, // IEL
		byte(c.Ref >> 8),
		byte(c.Ref),
		c.Total,
		c.Sequence,
	}
}

// encodeVPRelative converts a duration to a relative VP byte.
func encodeVPRelative(d time.Duration) byte {
	mins := int(d.Minutes())
	switch {
	case mins <= 720: // ≤ 12 hours: (VP+1) * 5 min
		v := mins/5 - 1
		if v < 0 {
			v = 0
		}
		return byte(v)
	case mins <= 1440: // 12–24 hours: 144 + (VP-143) * 30 min
		v := 143 + (mins-720)/30
		if v > 167 {
			v = 167
		}
		return byte(v)
	case mins <= 30240: // 2–30 days
		days := mins / 1440
		v := 166 + days
		if v > 196 {
			v = 196
		}
		return byte(v)
	default: // weeks
		weeks := mins / (7 * 1440)
		v := 192 + weeks
		if v > 255 {
			v = 255
		}
		return byte(v)
	}
}

// encodeSCTS encodes a time.Time as a 7-byte SCTS field.
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

// encodeBCDByte swaps the nibbles: input 23 → 0x32.
func encodeBCDByte(v byte) byte {
	return ((v % 10) << 4) | (v / 10)
}
