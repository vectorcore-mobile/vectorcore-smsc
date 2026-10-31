package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"time"
)

// Message flag bits
const (
	FlagRequest    = 0x80
	FlagProxiable  = 0x40
	FlagError      = 0x20
	FlagRetransmit = 0x10

	HeaderLen = 20
)

// Command codes
const (
	CmdCapabilitiesExchange uint32 = 257
	CmdDeviceWatchdog       uint32 = 280
	CmdDisconnectPeer       uint32 = 282

	// SGd (3GPP TS 29.338)
	CmdMOForwardShortMessage  uint32 = 8388645 // OFR — MO-Forward-Short-Message-Request
	CmdMTForwardShortMessage  uint32 = 8388646 // TFR — MT-Forward-Short-Message-Request
	CmdSendRoutingInfoSM      uint32 = 8388647 // SRI-SM — Send-Routing-Info-for-SM-Request
	CmdReportSMDeliveryStatus uint32 = 8388649 // RSR — Report-SM-Delivery-Status-Request
	CmdAlertServiceCentre     uint32 = 8388648 // ALR — Alert-Service-Centre-Request
)

// Application IDs
const (
	AppDiameterCommon uint32 = 0
	AppRelayAgent     uint32 = 0xFFFFFFFF
	App3GPP_S6c       uint32 = 16777312
	App3GPP_SGd       uint32 = 16777313
	App3GPP_Sh        uint32 = 16777217
)

// 3GPP Vendor ID
const Vendor3GPP uint32 = 10415

// Result codes
const (
	DiameterSuccess         uint32 = 2001
	DiameterUnableToDeliver uint32 = 3002
	DiameterTooBusy         uint32 = 3004
	DiameterAVPUnsupported  uint32 = 5001
	DiameterUnableToComply  uint32 = 5012
)

// Header is the 20-byte Diameter message header.
type Header struct {
	Version     uint8
	Length      uint32
	Flags       byte
	CommandCode uint32
	AppID       uint32
	HopByHop    uint32
	EndToEnd    uint32
}

// Message is a decoded Diameter message.
type Message struct {
	Header Header
	AVPs   []*AVP
}

// IsRequest returns true if the R flag is set.
func (m *Message) IsRequest() bool {
	return m.Header.Flags&FlagRequest != 0
}

// Encode encodes the message to wire format.
func (m *Message) Encode() ([]byte, error) {
	var avpBytes []byte
	for _, a := range m.AVPs {
		enc, err := Encode(a)
		if err != nil {
			return nil, fmt.Errorf("message: encoding AVP %d: %w", a.Code, err)
		}
		avpBytes = append(avpBytes, enc...)
	}
	totalLen := HeaderLen + len(avpBytes)
	buf := make([]byte, totalLen)
	buf[0] = 1
	buf[1] = byte(totalLen >> 16)
	buf[2] = byte(totalLen >> 8)
	buf[3] = byte(totalLen)
	buf[4] = m.Header.Flags
	buf[5] = byte(m.Header.CommandCode >> 16)
	buf[6] = byte(m.Header.CommandCode >> 8)
	buf[7] = byte(m.Header.CommandCode)
	binary.BigEndian.PutUint32(buf[8:12], m.Header.AppID)
	binary.BigEndian.PutUint32(buf[12:16], m.Header.HopByHop)
	binary.BigEndian.PutUint32(buf[16:20], m.Header.EndToEnd)
	copy(buf[HeaderLen:], avpBytes)
	return buf, nil
}

// DecodeMessage reads a full Diameter message from r.
func DecodeMessage(r io.Reader) (*Message, error) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, fmt.Errorf("message: reading header: %w", err)
	}
	if hdr[0] != 1 {
		return nil, fmt.Errorf("message: unsupported version %d", hdr[0])
	}
	totalLen := uint32(hdr[1])<<16 | uint32(hdr[2])<<8 | uint32(hdr[3])
	if totalLen < HeaderLen {
		return nil, fmt.Errorf("message: length %d too short", totalLen)
	}
	rest := make([]byte, totalLen-4)
	if _, err := io.ReadFull(r, rest); err != nil {
		return nil, fmt.Errorf("message: reading body: %w", err)
	}
	full := make([]byte, totalLen)
	copy(full[:4], hdr)
	copy(full[4:], rest)
	return decodeBytes(full)
}

func decodeBytes(b []byte) (*Message, error) {
	if len(b) < HeaderLen {
		return nil, errors.New("message: buffer too short")
	}
	m := &Message{}
	m.Header.Version = b[0]
	m.Header.Length = uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	m.Header.Flags = b[4]
	m.Header.CommandCode = uint32(b[5])<<16 | uint32(b[6])<<8 | uint32(b[7])
	m.Header.AppID = binary.BigEndian.Uint32(b[8:12])
	m.Header.HopByHop = binary.BigEndian.Uint32(b[12:16])
	m.Header.EndToEnd = binary.BigEndian.Uint32(b[16:20])
	if int(m.Header.Length) > len(b) {
		return nil, fmt.Errorf("message: declared length %d > buffer %d", m.Header.Length, len(b))
	}
	var err error
	m.AVPs, err = DecodeAll(b[HeaderLen:m.Header.Length])
	if err != nil {
		return nil, fmt.Errorf("message: AVP decode: %w", err)
	}
	return m, nil
}

// FindAVP returns the first AVP matching code and vendorID.
func (m *Message) FindAVP(code, vendorID uint32) *AVP {
	for _, a := range m.AVPs {
		if a.Code == code && a.VendorID == vendorID {
			return a
		}
	}
	return nil
}

// FindAVPs returns all AVPs matching code and vendorID.
func (m *Message) FindAVPs(code, vendorID uint32) []*AVP {
	var result []*AVP
	for _, a := range m.AVPs {
		if a.Code == code && a.VendorID == vendorID {
			result = append(result, a)
		}
	}
	return result
}

// ── Builder ──────────────────────────────────────────────────────────────────

var (
	hopByHopCounter  uint32
	endToEndBase     uint32
	endToEndCounter  uint32
	originStateIDVal uint32
)

func init() {
	hopByHopCounter = rand.Uint32()
	startTime := uint32(time.Now().Unix()) & 0x00FFFFFF
	endToEndBase = startTime << 8
	originStateIDVal = uint32(time.Now().Unix())
}

// NextHopByHop returns the next hop-by-hop identifier.
func NextHopByHop() uint32 { return atomic.AddUint32(&hopByHopCounter, 1) }

// NextEndToEnd returns the next end-to-end identifier.
func NextEndToEnd() uint32 {
	cnt := atomic.AddUint32(&endToEndCounter, 1) & 0xFF
	return endToEndBase | cnt
}

// OriginStateID returns the process-lifetime origin state ID.
func OriginStateID() uint32 { return atomic.LoadUint32(&originStateIDVal) }

// Builder constructs a Diameter message.
type Builder struct{ msg *Message }

// NewRequest creates a new request message builder.
func NewRequest(commandCode, appID uint32) *Builder {
	return &Builder{msg: &Message{Header: Header{
		Version:     1,
		Flags:       FlagRequest | FlagProxiable,
		CommandCode: commandCode,
		AppID:       appID,
		HopByHop:    NextHopByHop(),
		EndToEnd:    NextEndToEnd(),
	}}}
}

// NewAnswer creates an answer builder from a request.
func NewAnswer(req *Message) *Builder {
	flags := req.Header.Flags &^ FlagRequest &^ FlagRetransmit
	return &Builder{msg: &Message{Header: Header{
		Version:     1,
		Flags:       flags,
		CommandCode: req.Header.CommandCode,
		AppID:       req.Header.AppID,
		HopByHop:    req.Header.HopByHop,
		EndToEnd:    req.Header.EndToEnd,
	}}}
}

// Add appends AVPs to the message.
func (b *Builder) Add(avps ...*AVP) *Builder {
	b.msg.AVPs = append(b.msg.AVPs, avps...)
	return b
}

// NonProxiable clears the Proxiable flag (for CER/CEA/DWR/DWA/DPR/DPA).
func (b *Builder) NonProxiable() *Builder {
	b.msg.Header.Flags &^= FlagProxiable
	return b
}

// Build returns the constructed Message.
func (b *Builder) Build() *Message { return b.msg }
