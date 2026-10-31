// Package smppcodec converts between SMPP PDUs and the canonical codec.Message type.
package smppcodec

import (
	"fmt"
	"strings"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	"github.com/svinson1121/vectorcore-smsc/internal/smpp"
)

// DecodeSM converts a submit_sm or deliver_sm PDU to a canonical Message.
// Works for both command IDs since their body format is identical.
func DecodeSM(pdu *smpp.PDU) (*codec.Message, error) {
	if pdu.CommandID != smpp.CmdSubmitSM && pdu.CommandID != smpp.CmdDeliverSM {
		return nil, fmt.Errorf("smppcodec: expected submit_sm or deliver_sm, got 0x%08X", pdu.CommandID)
	}

	msg := &codec.Message{
		Timestamp: time.Now().UTC(),
		DCS:       pdu.DataCoding,
		Encoding:  tpdu.ParseDCS(pdu.DataCoding),
	}

	// Source address
	msg.Source = decodeAddress(pdu.SourceAddr, pdu.SourceAddrTON, pdu.SourceAddrNPI)

	// Destination address
	msg.Destination = decodeAddress(pdu.DestinationAddr, pdu.DestAddrTON, pdu.DestAddrNPI)

	// Payload — prefer TLV message_payload for long messages
	payload := pdu.Payload()

	// UDH — indicated by ESMClass bit 6
	hasUDHI := pdu.ESMClass&smpp.ESMClassUDHI != 0
	var udhOctets int
	if hasUDHI && len(payload) > 0 {
		udhl := int(payload[0])
		if len(payload) < 1+udhl {
			return nil, fmt.Errorf("smppcodec: UDHI payload truncated: have %d want %d", len(payload), 1+udhl)
		}
		raw := payload[:1+udhl]
		udh, err := decodeUDH(raw)
		if err == nil {
			msg.UDH = udh
			if udh.Concat != nil {
				msg.Concat = udh.Concat
			}
		}
		udhOctets = 1 + udhl
	}

	// SAR TLV concat (alternative to UDH)
	if msg.Concat == nil {
		if ref, ok := pdu.TLV(smpp.TLVSARMsgRefNum); ok && len(ref) >= 2 {
			total, _ := pdu.TLV(smpp.TLVSARTotalSegments)
			seq, _ := pdu.TLV(smpp.TLVSARSegmentSeqnum)
			if len(total) >= 1 && len(seq) >= 1 {
				msg.Concat = &codec.ConcatInfo{
					Ref:      uint16(ref[0])<<8 | uint16(ref[1]),
					Total:    total[0],
					Sequence: seq[0],
				}
			}
		}
	}

	// Decode text payload
	switch msg.Encoding {
	case codec.EncodingGSM7:
		// UDL in submit_sm is byte count of ShortMessage, but septets = octets * 8 / 7
		// The tpdu GSM7 decoder handles the pack/unpack correctly
		septets := (len(payload)*8 - udhOctets*8) / 7
		if udhOctets > 0 {
			fillBits := (7 - (udhOctets*8)%7) % 7
			septets = (len(payload)*8 - udhOctets*8 - fillBits) / 7
		}
		msg.Text = tpdu.DecodeGSM7(payload, septets, udhOctets)

	case codec.EncodingUCS2:
		if len(payload) < udhOctets {
			return nil, fmt.Errorf("smppcodec: UDH exceeds payload: have %d want at least %d", len(payload), udhOctets)
		}
		msg.Text = tpdu.DecodeUCS2(payload[udhOctets:])

	case codec.EncodingBinary:
		if len(payload) < udhOctets {
			return nil, fmt.Errorf("smppcodec: UDH exceeds payload: have %d want at least %d", len(payload), udhOctets)
		}
		msg.Binary = make([]byte, len(payload)-udhOctets)
		copy(msg.Binary, payload[udhOctets:])

	default:
		if len(payload) < udhOctets {
			return nil, fmt.Errorf("smppcodec: UDH exceeds payload: have %d want at least %d", len(payload), udhOctets)
		}
		msg.Text = string(payload[udhOctets:])
	}

	// Delivery report requested
	msg.TPSRRequired = pdu.RegisteredDelivery&smpp.RegDeliverySuccess != 0
	rd := pdu.RegisteredDelivery
	msg.SMPPRegisteredDelivery = &rd

	// Validity period
	if vp := parseVP(pdu.ValidityPeriod); vp != nil {
		msg.ValidityPeriod = vp
	}

	return msg, nil
}

// DecodeDeliveryReceipt parses a deliver_sm that is a delivery receipt
// (ESMClass & 0x04 != 0). Returns a Message with DR fields populated.
// The short message body has the format:
//
//	id:MSGID sub:NNN dlvrd:NNN submit date:... done date:... stat:DELIVRD err:EEE
func DecodeDeliveryReceipt(pdu *smpp.PDU) (*codec.Message, error) {
	msg := &codec.Message{
		Timestamp: time.Now().UTC(),
	}
	msg.Source = decodeAddress(pdu.SourceAddr, pdu.SourceAddrTON, pdu.SourceAddrNPI)
	msg.Destination = decodeAddress(pdu.DestinationAddr, pdu.DestAddrTON, pdu.DestAddrNPI)

	// SMPP message ID from TLV (preferred) or receipt body
	if id := pdu.TLVString(smpp.TLVReceiptedMessageID); id != "" {
		msg.ID = id
	} else {
		body := string(pdu.Payload())
		msg.ID = parseReceiptField(body, "id")
	}

	return msg, nil
}

// decodeAddress maps SMPP address fields to a codec.Address.
func decodeAddress(addr string, ton, npi byte) codec.Address {
	a := codec.Address{
		TON: ton,
		NPI: npi,
	}
	switch ton {
	case 0x05: // alphanumeric
		a.Alpha = addr
	default:
		// Strip leading + if present (E.164 stored without +)
		a.MSISDN = strings.TrimPrefix(addr, "+")
	}
	return a
}

// decodeUDH parses a UDH starting at the UDHL byte.
func decodeUDH(raw []byte) (*codec.UDH, error) {
	udh := &codec.UDH{Raw: raw}
	if len(raw) < 1 {
		return udh, nil
	}
	udhl := int(raw[0])
	ie := raw[1:]
	if len(ie) < udhl {
		return nil, fmt.Errorf("UDH truncated")
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
		case 0x00: // 8-bit concat ref
			if iel == 3 {
				udh.Concat = &codec.ConcatInfo{
					Ref:      uint16(val[0]),
					Total:    val[1],
					Sequence: val[2],
				}
			}
		case 0x08: // 16-bit concat ref
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

// parseVP parses an SMPP validity period string.
// Returns nil if empty or unparseable.
// Relative format: YYMMDDHHmmss000R
func parseVP(vp string) *time.Duration {
	if vp == "" {
		return nil
	}
	if len(vp) >= 1 && vp[len(vp)-1] == 'R' && len(vp) >= 13 {
		// Relative: first 12 digits are YYMMDDHHmmss
		raw := vp[:12]
		if len(raw) == 12 {
			d := parseDuration(raw)
			if d > 0 {
				return &d
			}
		}
	}
	return nil
}

func parseDuration(s string) time.Duration {
	if len(s) < 12 {
		return 0
	}
	parse := func(i, n int) int {
		v := 0
		for j := 0; j < n; j++ {
			v = v*10 + int(s[i+j]-'0')
		}
		return v
	}
	years := parse(0, 2)
	months := parse(2, 2)
	days := parse(4, 2)
	hours := parse(6, 2)
	mins := parse(8, 2)
	secs := parse(10, 2)

	total := time.Duration(years)*365*24*time.Hour +
		time.Duration(months)*30*24*time.Hour +
		time.Duration(days)*24*time.Hour +
		time.Duration(hours)*time.Hour +
		time.Duration(mins)*time.Minute +
		time.Duration(secs)*time.Second
	return total
}

// parseReceiptField extracts a named field from a DR receipt body.
func parseReceiptField(body, field string) string {
	needle := field + ":"
	idx := strings.Index(body, needle)
	if idx < 0 {
		return ""
	}
	rest := body[idx+len(needle):]
	if sp := strings.Index(rest, " "); sp >= 0 {
		return rest[:sp]
	}
	return rest
}
