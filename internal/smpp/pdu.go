// Package smpp provides the SMPP v3.4 wire protocol: PDU definitions,
// encode/decode, and the low-level Conn wrapper.
// Adapted from VectorCore SMPP Router (same author).
package smpp

// Command IDs (big-endian uint32 in wire format).
const (
	CmdBindReceiver        uint32 = 0x00000001
	CmdBindTransmitter     uint32 = 0x00000002
	CmdQuerySM             uint32 = 0x00000003
	CmdSubmitSM            uint32 = 0x00000004
	CmdDeliverSM           uint32 = 0x00000005
	CmdUnbind              uint32 = 0x00000006
	CmdSubmitMulti         uint32 = 0x00000021
	CmdBindTransceiver     uint32 = 0x00000009
	CmdEnquireLink         uint32 = 0x00000015
	CmdAlertNotification   uint32 = 0x00000102
	CmdSubmitSMResp        uint32 = 0x80000004
	CmdDeliverSMResp       uint32 = 0x80000005
	CmdBindReceiverResp    uint32 = 0x80000001
	CmdBindTransmitterResp uint32 = 0x80000002
	CmdBindTransceiverResp uint32 = 0x80000009
	CmdEnquireLinkResp     uint32 = 0x80000015
	CmdUnbindResp          uint32 = 0x80000006
	CmdGenericNack         uint32 = 0x80000000
)

// Command status codes.
const (
	ESME_ROK        uint32 = 0x00000000
	ESME_RINVBNDSTS uint32 = 0x00000004
	ESME_RSYSERR    uint32 = 0x00000008
	ESME_RINVSYSID  uint32 = 0x0000000E
	ESME_RINVPASWD  uint32 = 0x0000000F
	ESME_RTHROTTLED uint32 = 0x00000058
	ESME_RINVDSTADR uint32 = 0x0000000B
)

// Optional parameter (TLV) tags used by the SMSC.
const (
	TLVDestAddrSubunit    uint16 = 0x0005
	TLVSourceAddrSubunit  uint16 = 0x000D
	TLVReceiptedMessageID uint16 = 0x001E // DR correlation
	TLVSARMsgRefNum       uint16 = 0x020C // SAR concat reference
	TLVSARTotalSegments   uint16 = 0x020E
	TLVSARSegmentSeqnum   uint16 = 0x020F
	TLVNetworkErrorCode   uint16 = 0x0423 // DR error detail
	TLVMessagePayload     uint16 = 0x0424 // long messages (> 254 bytes)
	TLVMessageState       uint16 = 0x0427 // DR delivery state
)

// MessageState values for TLVMessageState.
const (
	MsgStateEnroute     byte = 1
	MsgStateDelivered   byte = 2
	MsgStateExpired     byte = 3
	MsgStateDeleted     byte = 4
	MsgStateUndelivered byte = 5
	MsgStateAccepted    byte = 6
	MsgStateUnknown     byte = 7
	MsgStateRejected    byte = 8
)

// ESMClass bit flags (bits 6 = UDHI, bits 2-0 = message type).
const (
	ESMClassUDHI           byte = 0x40 // UDH indicator
	ESMClassDeliverReceipt byte = 0x04 // this is a delivery receipt
)

// RegisteredDelivery bit flags.
const (
	RegDeliverySuccess byte = 0x01 // request receipt on success
	RegDeliveryFailure byte = 0x02 // request receipt on failure
)

// PDU represents a decoded SMPP v3.4 PDU.
type PDU struct {
	// Header (always present, 16 bytes on the wire)
	CommandLength  uint32
	CommandID      uint32
	CommandStatus  uint32
	SequenceNumber uint32

	// Bind request/response body
	SystemID         string
	Password         string
	SystemType       string
	InterfaceVersion byte
	AddrTON          byte
	AddrNPI          byte
	AddressRange     string

	// submit_sm / deliver_sm body
	ServiceType          string
	SourceAddrTON        byte
	SourceAddrNPI        byte
	SourceAddr           string
	DestAddrTON          byte
	DestAddrNPI          byte
	DestinationAddr      string
	ESMClass             byte
	ProtocolID           byte
	PriorityFlag         byte
	ScheduleDeliveryTime string
	ValidityPeriod       string
	RegisteredDelivery   byte
	ReplaceIfPresentFlag byte
	DataCoding           byte
	SMDefaultMsgID       byte
	SMLength             byte
	ShortMessage         []byte

	// submit_sm_resp / deliver_sm_resp
	MessageID string

	// Optional parameters (TLVs).  Keyed by tag number.
	TLVs map[uint16][]byte
}

// TLV returns the value for the given tag, and whether it was present.
func (p *PDU) TLV(tag uint16) ([]byte, bool) {
	if p.TLVs == nil {
		return nil, false
	}
	v, ok := p.TLVs[tag]
	return v, ok
}

// TLVString returns the TLV value as a string (trimming any null terminator).
func (p *PDU) TLVString(tag uint16) string {
	v, ok := p.TLV(tag)
	if !ok {
		return ""
	}
	s := string(v)
	if len(s) > 0 && s[len(s)-1] == 0 {
		s = s[:len(s)-1]
	}
	return s
}

// SetTLV sets an optional TLV parameter.
func (p *PDU) SetTLV(tag uint16, value []byte) {
	if p.TLVs == nil {
		p.TLVs = make(map[uint16][]byte)
	}
	p.TLVs[tag] = value
}

// SetTLVString sets a TLV to a null-terminated string value.
func (p *PDU) SetTLVString(tag uint16, s string) {
	b := make([]byte, len(s)+1)
	copy(b, s)
	p.SetTLV(tag, b)
}

// Payload returns the message payload: TLVMessagePayload if set, else ShortMessage.
func (p *PDU) Payload() []byte {
	if v, ok := p.TLV(TLVMessagePayload); ok && len(v) > 0 {
		return v
	}
	return p.ShortMessage
}
