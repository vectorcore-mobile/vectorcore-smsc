package tpdu

import (
	"fmt"
	"strings"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
)

// TP-MTI values (bits 1-0 of first octet)
const (
	mtiSMSDeliver      = 0x00
	mtiSMSSubmit       = 0x01
	mtiSMSStatusReport = 0x02
	mtiSMSCommand      = 0x02 // same value, direction-dependent
)

// Decode parses raw TP-DATA bytes into a canonical Message.
// It handles SMS-SUBMIT (MO from UE) and SMS-DELIVER (MT to UE).
func Decode(data []byte) (*codec.Message, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("tpdu too short: %d bytes", len(data))
	}

	mti := data[0] & 0x03
	switch mti {
	case mtiSMSSubmit:
		return decodeSubmit(data)
	case mtiSMSDeliver:
		return decodeDeliver(data)
	case mtiSMSStatusReport:
		return decodeStatusReport(data)
	default:
		return nil, fmt.Errorf("unsupported TP-MTI: 0x%02X", mti)
	}
}

// decodeSubmit parses an SMS-SUBMIT PDU (3GPP TS 23.040 §9.2.2.2).
func decodeSubmit(data []byte) (*codec.Message, error) {
	// Byte 0: TP-MTI(1-0) | TP-RD(2) | TP-VPF(4-3) | TP-SRR(5) | TP-UDHI(6) | TP-RP(7)
	hasUDHI := data[0]&0x40 != 0
	srr := data[0]&0x20 != 0
	vpf := (data[0] >> 3) & 0x03

	if len(data) < 2 {
		return nil, fmt.Errorf("submit too short")
	}
	tpmr := data[1]

	// TP-DA
	pos := 2
	da, n, err := decodeAddress(data, pos)
	if err != nil {
		return nil, fmt.Errorf("decode TP-DA: %w", err)
	}
	pos += n

	if pos+2 > len(data) {
		return nil, fmt.Errorf("submit truncated before PID/DCS")
	}
	// TP-PID
	pos++ // skip PID (byte ignored for routing)
	// TP-DCS
	dcs := data[pos]
	pos++

	// TP-VP
	var vp *time.Duration
	vp, n, err = decodeVP(data, pos, vpf)
	if err != nil {
		return nil, fmt.Errorf("decode TP-VP: %w", err)
	}
	pos += n

	// TP-UDL + TP-UD
	msg, err := decodeUD(data, pos, dcs, hasUDHI)
	if err != nil {
		return nil, fmt.Errorf("decode TP-UD: %w", err)
	}

	msg.TPMR = tpmr
	msg.TPSRRequired = srr
	msg.ValidityPeriod = vp
	msg.Destination = da
	msg.DCS = dcs
	msg.Timestamp = time.Now().UTC()
	return msg, nil
}

// decodeDeliver parses an SMS-DELIVER PDU (3GPP TS 23.040 §9.2.2.1).
func decodeDeliver(data []byte) (*codec.Message, error) {
	hasUDHI := data[0]&0x40 != 0
	srr := data[0]&0x20 != 0

	pos := 1
	// TP-OA
	oa, n, err := decodeAddress(data, pos)
	if err != nil {
		return nil, fmt.Errorf("decode TP-OA: %w", err)
	}
	pos += n

	if pos+2 > len(data) {
		return nil, fmt.Errorf("deliver truncated before PID/DCS")
	}
	pos++ // TP-PID
	dcs := data[pos]
	pos++

	// TP-SCTS (7 bytes)
	if pos+7 > len(data) {
		return nil, fmt.Errorf("deliver truncated before SCTS")
	}
	ts := decodeSCTS(data[pos : pos+7])
	pos += 7

	msg, err := decodeUD(data, pos, dcs, hasUDHI)
	if err != nil {
		return nil, fmt.Errorf("decode TP-UD: %w", err)
	}

	msg.Source = oa
	msg.DCS = dcs
	msg.TPSRRequired = srr
	msg.Timestamp = ts
	return msg, nil
}

// decodeStatusReport parses an SMS-STATUS-REPORT PDU (3GPP TS 23.040 §9.2.2.3).
func decodeStatusReport(data []byte) (*codec.Message, error) {
	if len(data) < 14 {
		return nil, fmt.Errorf("status report too short: %d bytes", len(data))
	}
	tpmr := data[1]

	pos := 2
	ra, n, err := decodeAddress(data, pos)
	if err != nil {
		return nil, fmt.Errorf("decode TP-RA: %w", err)
	}
	pos += n

	// Two seven-octet timestamps are followed by the mandatory TP-ST octet.
	if pos+15 > len(data) {
		return nil, fmt.Errorf("status report truncated")
	}
	// TP-SCTS (7 bytes)
	scts := decodeSCTS(data[pos : pos+7])
	pos += 7
	// TP-DT (7 bytes) — discharge time
	dt := decodeSCTS(data[pos : pos+7])
	pos += 7
	tpST := data[pos]

	msg := &codec.Message{
		TPMR:          tpmr,
		Source:        ra,
		Timestamp:     scts,
		DischargeTime: dt,
		TPStatus:      tpST,
	}
	return msg, nil
}

// decodeAddress parses a TP address field starting at data[pos].
// Returns the Address, the number of bytes consumed, and any error.
func decodeAddress(data []byte, pos int) (codec.Address, int, error) {
	if pos >= len(data) {
		return codec.Address{}, 0, fmt.Errorf("address out of bounds")
	}
	numDigits := int(data[pos])
	pos++
	if pos >= len(data) {
		return codec.Address{}, 1, fmt.Errorf("address TOA out of bounds")
	}
	toa := data[pos]
	pos++

	// Number of octets needed to hold numDigits BCD digits
	numOctets := (numDigits + 1) / 2
	if pos+numOctets > len(data) {
		return codec.Address{}, 2, fmt.Errorf("address digits out of bounds")
	}

	ton := (toa >> 4) & 0x07
	npi := toa & 0x0F

	var msisdn string
	if ton == 0x05 {
		// Alphanumeric (encoded as GSM7 in the address field)
		alphaSeptets := (numDigits * 4) / 7
		msisdn = DecodeGSM7(data[pos:pos+numOctets], alphaSeptets, 0)
	} else {
		msisdn = decodeBCD(data[pos:pos+numOctets], numDigits)
		if ton == 0x01 { // international
			msisdn = strings.TrimPrefix(msisdn, "+")
		}
	}

	addr := codec.Address{
		MSISDN: msisdn,
		TON:    ton,
		NPI:    npi,
	}
	if ton == 0x05 {
		addr.Alpha = msisdn
		addr.MSISDN = ""
	}

	return addr, 2 + numOctets, nil
}

// decodeVP parses the TP-VP field per VPF.
// vpf: 0=not present, 1=enhanced, 2=relative, 3=absolute
// Returns the decoded duration (nil if not present), bytes consumed, and error.
func decodeVP(data []byte, pos int, vpf byte) (*time.Duration, int, error) {
	switch vpf {
	case 0x00: // not present
		return nil, 0, nil

	case 0x02: // relative (1 byte)
		if pos >= len(data) {
			return nil, 0, fmt.Errorf("VP relative out of bounds")
		}
		d := decodeVPRelative(data[pos])
		return &d, 1, nil

	case 0x03: // absolute (7 bytes SCTS format)
		if pos+7 > len(data) {
			return nil, 0, fmt.Errorf("VP absolute out of bounds")
		}
		// Absolute VP — we convert to a duration from now
		abs := decodeSCTS(data[pos : pos+7])
		d := time.Until(abs)
		if d < 0 {
			d = 0
		}
		return &d, 7, nil

	case 0x01: // enhanced (7 bytes)
		if pos+7 > len(data) {
			return nil, 0, fmt.Errorf("VP enhanced out of bounds")
		}
		// Enhanced VP — parse as relative for now
		d := decodeVPRelative(data[pos+1])
		return &d, 7, nil

	default:
		return nil, 0, fmt.Errorf("unknown VPF: 0x%02X", vpf)
	}
}

// decodeVPRelative converts the relative VP byte to a time.Duration.
// Per 3GPP TS 23.040 §9.2.3.12.1.
func decodeVPRelative(vp byte) time.Duration {
	switch {
	case vp <= 143:
		return time.Duration(vp+1) * 5 * time.Minute
	case vp <= 167:
		return (12*time.Hour + time.Duration(vp-143)*30*time.Minute)
	case vp <= 196:
		return time.Duration(vp-166) * 24 * time.Hour
	default:
		return time.Duration(vp-192) * 7 * 24 * time.Hour
	}
}

// decodeUD parses TP-UDL and TP-UD into a Message.
func decodeUD(data []byte, pos int, dcs byte, hasUDHI bool) (*codec.Message, error) {
	if pos >= len(data) {
		return nil, fmt.Errorf("UDL out of bounds")
	}
	udl := int(data[pos])
	pos++

	enc := ParseDCS(dcs)

	msg := &codec.Message{
		DCS:      dcs,
		Encoding: enc,
	}

	// UDH parsing
	udhOctets := 0
	if hasUDHI && pos < len(data) {
		udhLen := int(data[pos]) // UDHL (length of UDH, not including the UDHL byte itself)
		if pos+1+udhLen > len(data) {
			return nil, fmt.Errorf("UDH extends beyond UD")
		}
		raw := data[pos : pos+1+udhLen]
		udh, err := decodeUDH(raw)
		if err != nil {
			return nil, fmt.Errorf("decode UDH: %w", err)
		}
		msg.UDH = udh
		if udh.Concat != nil {
			msg.Concat = udh.Concat
		}
		udhOctets = 1 + udhLen
	}

	// Payload
	switch enc {
	case codec.EncodingGSM7:
		// UDL is the total number of septets, including the UDH and its
		// alignment fill bits.
		requiredOctets := (udl*7 + 7) / 8
		if requiredOctets > len(data)-pos {
			return nil, fmt.Errorf("GSM7 user data truncated: need %d octets, have %d", requiredOctets, len(data)-pos)
		}
		udhSeptets := (udhOctets*8 + 6) / 7
		if udhSeptets > udl {
			return nil, fmt.Errorf("UDH requires %d septets, exceeds UDL %d", udhSeptets, udl)
		}
		msg.Text = DecodeGSM7(data[pos:pos+requiredOctets], udl-udhSeptets, udhOctets)

	case codec.EncodingUCS2:
		// UDL is the total number of octets, including the UDH.
		if udl > len(data)-pos {
			return nil, fmt.Errorf("UCS2 user data truncated: need %d octets, have %d", udl, len(data)-pos)
		}
		if udhOctets > udl {
			return nil, fmt.Errorf("UDH length %d exceeds UDL %d", udhOctets, udl)
		}
		udEnd := pos + udl
		payload := data[pos+udhOctets : udEnd]
		msg.Text = DecodeUCS2(payload)

	case codec.EncodingBinary:
		if udl > len(data)-pos {
			return nil, fmt.Errorf("binary user data truncated: need %d octets, have %d", udl, len(data)-pos)
		}
		if udhOctets > udl {
			return nil, fmt.Errorf("UDH length %d exceeds UDL %d", udhOctets, udl)
		}
		udEnd := pos + udl
		payload := data[pos+udhOctets : udEnd]
		msg.Binary = make([]byte, len(payload))
		copy(msg.Binary, payload)

	default:
		if udl > len(data)-pos {
			return nil, fmt.Errorf("user data truncated: need %d octets, have %d", udl, len(data)-pos)
		}
		if udhOctets > udl {
			return nil, fmt.Errorf("UDH length %d exceeds UDL %d", udhOctets, udl)
		}
		msg.Text = string(data[pos+udhOctets : pos+udl])
	}

	return msg, nil
}

// decodeUDH parses the User Data Header (starting from the UDHL byte).
func decodeUDH(raw []byte) (*codec.UDH, error) {
	udh := &codec.UDH{Raw: raw}
	if len(raw) < 1 {
		return udh, nil
	}
	udhl := int(raw[0])
	ie := raw[1:]
	if len(ie) < udhl {
		return nil, fmt.Errorf("UDH IEI data truncated")
	}
	ie = ie[:udhl]

	for len(ie) >= 2 {
		iei := ie[0]
		iel := int(ie[1])
		if 2+iel > len(ie) {
			break
		}
		val := ie[2 : 2+iel]

		switch iei {
		case 0x00: // Concat 8-bit reference
			if iel == 3 {
				udh.Concat = &codec.ConcatInfo{
					Ref:      uint16(val[0]),
					Total:    val[1],
					Sequence: val[2],
				}
			}
		case 0x08: // Concat 16-bit reference
			if iel == 4 {
				udh.Concat = &codec.ConcatInfo{
					Ref:      uint16(val[0])<<8 | uint16(val[1]),
					Total:    val[2],
					Sequence: val[3],
				}
			}
		}
		ie = ie[2+iel:]
	}
	return udh, nil
}

// decodeBCD converts semi-octet BCD bytes to a digit string.
// Each octet contains two digits: low nibble = first, high nibble = second.
// Trailing 0xF nibble is stripped.
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

// decodeSCTS parses a 7-byte Service Centre Time Stamp into a time.Time.
// Each nibble-pair is BCD; timezone byte: bits 0-6 = offset in quarter-hours, bit 7 = negative.
func decodeSCTS(b []byte) time.Time {
	if len(b) < 7 {
		return time.Time{}
	}
	year := int(bcdByte(b[0])) + 2000
	month := time.Month(bcdByte(b[1]))
	day := int(bcdByte(b[2]))
	hour := int(bcdByte(b[3]))
	min := int(bcdByte(b[4]))
	sec := int(bcdByte(b[5]))

	tzByte := b[6]
	neg := tzByte&0x08 != 0 // bit 3 of the high nibble (GSM spec uses bit 3)
	tzQuarters := int(bcdByte(tzByte & 0xF7))
	tzOffset := tzQuarters * 15 // minutes
	if neg {
		tzOffset = -tzOffset
	}
	loc := time.FixedZone("", tzOffset*60)
	return time.Date(year, month, day, hour, min, sec, 0, loc)
}

func bcdByte(b byte) byte {
	return (b&0x0F)*10 + ((b >> 4) & 0x0F)
}
