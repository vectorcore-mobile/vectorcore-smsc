package smppcodec

import (
	"fmt"
	"strings"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	"github.com/svinson1121/vectorcore-smsc/internal/smpp"
)

// EncodeDeliverSM encodes a canonical Message as a deliver_sm PDU for sending
// to an ESME bound as receiver or transceiver (MT delivery, or forwarding
// an MO message to a downstream ESME).
func EncodeDeliverSM(msg *codec.Message) (*smpp.PDU, error) {
	return encodeSM(msg, smpp.CmdDeliverSM)
}

// EncodeSubmitSM encodes a canonical Message as a submit_sm PDU for sending
// via an outbound SMPP client connection (forwarding to a remote SMSC).
func EncodeSubmitSM(msg *codec.Message) (*smpp.PDU, error) {
	return encodeSM(msg, smpp.CmdSubmitSM)
}

// EncodeDeliveryReceipt builds a deliver_sm delivery receipt PDU for sending
// back to the originating ESME.  msgID is the SMPP message ID of the original
// submit_sm; status is "DELIVRD", "UNDELIV", "EXPIRED", or "FAILED".
func EncodeDeliveryReceipt(origMsgID, srcAddr, dstAddr, status, errCode string) *smpp.PDU {
	body := fmt.Sprintf("id:%s sub:001 dlvrd:001 submit date:0001010000 done date:0001010000 stat:%s err:%s",
		origMsgID, status, errCode)

	pdu := &smpp.PDU{
		CommandID:          smpp.CmdDeliverSM,
		CommandStatus:      smpp.ESME_ROK,
		ServiceType:        "",
		SourceAddrTON:      0x01,
		SourceAddrNPI:      0x01,
		SourceAddr:         srcAddr,
		DestAddrTON:        0x01,
		DestAddrNPI:        0x01,
		DestinationAddr:    dstAddr,
		ESMClass:           smpp.ESMClassDeliverReceipt,
		RegisteredDelivery: 0x00,
		DataCoding:         0x00,
		ShortMessage:       []byte(body),
	}
	pdu.SMLength = byte(len(pdu.ShortMessage))
	pdu.SetTLVString(smpp.TLVReceiptedMessageID, origMsgID)
	return pdu
}

func encodeSM(msg *codec.Message, cmdID uint32) (*smpp.PDU, error) {
	pdu := &smpp.PDU{
		CommandID:     cmdID,
		CommandStatus: smpp.ESME_ROK,
		ServiceType:   "",
	}

	// Source
	pdu.SourceAddrTON, pdu.SourceAddrNPI, pdu.SourceAddr = encodeAddress(msg.Source)

	// Destination
	pdu.DestAddrTON, pdu.DestAddrNPI, pdu.DestinationAddr = encodeAddress(msg.Destination)

	// Registered delivery
	if msg.TPSRRequired {
		pdu.RegisteredDelivery = smpp.RegDeliverySuccess | smpp.RegDeliveryFailure
	}

	// Preserve the original DCS byte so message class, coding group flags,
	// and other DCS metadata (e.g. 0xF5 = binary class 1 for WAP push) are
	// not lost when forwarding.  Fall back to BuildDCS only when no inbound
	// DCS was recorded (e.g. messages originated via the REST API).
	pdu.DataCoding = dcsForEgress(msg)

	// Encode payload
	switch msg.Encoding {
	case codec.EncodingGSM7:
		if msg.Concat != nil {
			udh := buildConcatUDH(msg.Concat)
			packed, septets := tpdu.EncodeGSM7(msg.Text)
			_ = septets
			payload := append(udh, packed...)
			pdu.ESMClass = smpp.ESMClassUDHI
			pdu.ShortMessage = payload
		} else {
			packed, _ := tpdu.EncodeGSM7(msg.Text)
			pdu.ShortMessage = packed
		}

	case codec.EncodingUCS2:
		payload := tpdu.EncodeUCS2(msg.Text)
		if msg.Concat != nil {
			udh := buildConcatUDH(msg.Concat)
			pdu.ESMClass = smpp.ESMClassUDHI
			pdu.ShortMessage = append(udh, payload...)
		} else {
			pdu.ShortMessage = payload
		}

	case codec.EncodingBinary:
		pdu.ShortMessage = msg.Binary

	default:
		return nil, fmt.Errorf("smppcodec: unsupported encoding %d", msg.Encoding)
	}

	// For long messages use TLV message_payload instead of ShortMessage
	if len(pdu.ShortMessage) > 254 {
		pdu.SetTLV(smpp.TLVMessagePayload, pdu.ShortMessage)
		pdu.ShortMessage = nil
	}

	pdu.SMLength = byte(len(pdu.ShortMessage))
	return pdu, nil
}

// encodeAddress maps a codec.Address to SMPP address fields.
func encodeAddress(addr codec.Address) (ton, npi byte, addrStr string) {
	if addr.Alpha != "" {
		return 0x05, 0x00, addr.Alpha // alphanumeric
	}
	msisdn := strings.TrimPrefix(addr.MSISDN, "+")
	t := addr.TON
	n := addr.NPI
	if t == 0 && n == 0 {
		t = 0x01 // international
		n = 0x01 // ISDN/E.164
	}
	return t, n, msisdn
}

// dcsForEgress returns the DCS byte to use on outbound SMPP PDUs.
// When the message was received on any inbound interface, msg.DCS carries the
// original wire value and is used verbatim.  Only when msg.DCS is zero (e.g.
// a message originated internally via the REST API) does it fall back to
// deriving a minimal DCS from the encoding type.
func dcsForEgress(msg *codec.Message) byte {
	if msg.DCS != 0 {
		return msg.DCS
	}
	return tpdu.BuildDCS(msg.Encoding)
}

// buildConcatUDH builds a 16-bit reference concat UDH.
func buildConcatUDH(c *codec.ConcatInfo) []byte {
	return []byte{
		0x06,             // UDHL
		0x08,             // IEI: 16-bit ref
		0x04,             // IEL
		byte(c.Ref >> 8), // ref high
		byte(c.Ref),      // ref low
		c.Total,
		c.Sequence,
	}
}
