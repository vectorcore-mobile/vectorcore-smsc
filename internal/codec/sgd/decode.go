// Package sgd decodes and encodes Diameter SGd (3GPP TS 29.338) messages
// to and from the canonical codec.Message type.
//
// SGd carries SM-RP-UI (OctetString) which contains raw TP-DATA bytes.
// The tpdu package handles the TP-layer decode/encode.
package sgd

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

// DecodeOFR decodes an MO-Forward-Short-Message-Request (OFR) from the MME.
// Returns the canonical Message extracted from the SM-RP-UI AVP.
func DecodeOFR(msg *dcodec.Message) (*codec.Message, error) {
	return decodeSMRPUI(msg, codec.InterfaceSGd)
}

// DecodeTFR decodes an MT-Forward-Short-Message-Request (TFR).
func DecodeTFR(msg *dcodec.Message) (*codec.Message, error) {
	return decodeSMRPUI(msg, codec.InterfaceSGd)
}

func decodeSMRPUI(msg *dcodec.Message, iface codec.InterfaceType) (*codec.Message, error) {
	// SM-RP-UI is a vendor AVP (vendor 10415, code 3101)
	smRPUI := msg.FindAVP(dcodec.CodeSMRPUI, dcodec.Vendor3GPP)
	if smRPUI == nil {
		return nil, fmt.Errorf("sgd: SM-RP-UI AVP not found")
	}

	// SM-RP-UI contains raw TP-DATA bytes (no RP layer header in SGd)
	canonical, err := tpdu.Decode(smRPUI.Data)
	if err != nil {
		return nil, fmt.Errorf("sgd: tpdu decode: %w", err)
	}
	canonical.IngressInterface = iface

	imsi, msisdn := decodeUserIdentifier(msg)
	// The User-Identifier names the served UE: the originator of an MO
	// SMS-SUBMIT, or the recipient of an MT SMS-DELIVER.
	if isSMSSubmit(smRPUI.Data) {
		if canonical.Source.IMSI == "" {
			canonical.Source.IMSI = imsi
		}
	} else if canonical.Destination.IMSI == "" {
		canonical.Destination.IMSI = imsi
	}

	// For MO SMS-SUBMIT over SGd, TPDU carries only TP-DA. The subscriber
	// originator must come from User-Identifier/MSISDN rather than SC-Address.
	if canonical.Source.MSISDN == "" {
		canonical.Source.MSISDN = strings.TrimPrefix(msisdn, "+")
	}
	if canonical.Destination.MSISDN == "" {
		canonical.Destination.MSISDN = msisdn
	}

	return canonical, nil
}

// isSMSSubmit reports whether raw TP-DATA carries an SMS-SUBMIT (TP-MTI=01).
func isSMSSubmit(tpData []byte) bool {
	return len(tpData) > 0 && tpData[0]&0x03 == 0x01
}

func decodeUserIdentifier(msg *dcodec.Message) (imsi, msisdn string) {
	if msg == nil {
		return "", ""
	}

	if uid := msg.FindAVP(dcodec.CodeUserIdentifier, dcodec.Vendor3GPP); uid != nil {
		if children, err := dcodec.DecodeGrouped(uid); err == nil {
			for _, child := range children {
				switch {
				case child.Code == dcodec.CodeUserName && child.VendorID == 0:
					if val, err := child.String(); err == nil {
						imsi = val
					}
				case child.Code == dcodec.CodeMSISDN && child.VendorID == dcodec.Vendor3GPP:
					msisdn = decodeBCDMSISDN(child.Data)
				}
			}
		}
	}

	if msisdn == "" {
		if a := msg.FindAVP(dcodec.CodeMSISDN, dcodec.Vendor3GPP); a != nil {
			msisdn = decodeBCDMSISDN(a.Data)
		}
	}
	if imsi == "" {
		if a := msg.FindAVP(dcodec.CodeUserName, 0); a != nil {
			if val, err := a.String(); err == nil {
				imsi = val
			}
		}
	}

	return imsi, msisdn
}

// decodeBCDMSISDN decodes a BCD-encoded MSISDN AVP value.
// The AVP data is a packed BCD string optionally prefixed with a TON/NPI byte.
// For SGd MSISDN AVP (TS 29.338 §7.3.11), format is per TS 23.003 §12.1:
//
//	length byte | nature-of-address byte | BCD digits (semi-octets, F as filler)
func decodeBCDMSISDN(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	offset := 0
	international := false

	// Peers are inconsistent here. We see:
	//   len | TON/NPI | BCD digits
	//   len | BCD digits
	//   TON/NPI | BCD digits
	// Support all three so the first digit octet is not lost.
	if data[0] < byte(len(data)) {
		offset = 1
	}
	if offset < len(data) && data[offset]&0x80 != 0 {
		noa := data[offset]
		offset++
		international = (noa>>4)&0x7 == 1
	}

	digits := decodeBCDDigits(data[offset:])
	if international {
		return "+" + digits
	}
	return digits
}

// decodeSCAddress decodes an SGd SC-Address AVP.
// Standards-compliant SGd encodes this as raw TBCD digits without
// the length / TON-NPI prefix used by the MSISDN AVP.
func decodeSCAddress(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	if isASCIIDigits(data) {
		return string(data)
	}
	if data[0] < 0x80 {
		return decodeBCDDigits(data)
	}
	return decodeBCDMSISDN(data)
}

func isASCIIDigits(data []byte) bool {
	for _, b := range data {
		if b < '0' || b > '9' {
			return false
		}
	}
	return len(data) > 0
}

// decodeBCDDigits decodes packed semi-octet BCD, stopping at 0xF filler.
func decodeBCDDigits(data []byte) string {
	buf := make([]byte, 0, len(data)*2)
	for _, b := range data {
		lo := b & 0x0F
		hi := (b >> 4) & 0x0F
		if lo <= 9 {
			buf = append(buf, '0'+lo)
		}
		if hi <= 9 {
			buf = append(buf, '0'+hi)
		}
	}
	return string(buf)
}

// encodeBCDMSISDN encodes an MSISDN (with or without leading +) as BCD for SGd AVP.
func encodeBCDMSISDN(msisdn string) []byte {
	international := false
	if len(msisdn) > 0 && msisdn[0] == '+' {
		international = true
		msisdn = msisdn[1:]
	}
	noa := byte(0x81) // international = 0x91, national = 0x81
	if international {
		noa = 0x91
	}
	digits := encodeBCDDigits(msisdn)
	result := make([]byte, 1+1+len(digits))
	result[0] = byte(1 + len(digits)) // length
	result[1] = noa
	copy(result[2:], digits)
	return result
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
		buf[size-1] |= 0xF0 // filler
	}
	return buf
}

type SCAddressEncoding string

const (
	SCAddressEncodingTBCD        SCAddressEncoding = "tbcd"
	SCAddressEncodingASCIIDigits SCAddressEncoding = "ascii_digits"
)

// encodeSCAddress encodes a Service Centre address for SM-RP-OA.
func encodeSCAddress(sc string, encoding string) []byte {
	if sc == "" {
		return nil
	}
	if sc[0] == '+' {
		sc = sc[1:]
	}
	if normalizeSCAddressEncoding(encoding) == SCAddressEncodingASCIIDigits {
		return []byte(sc)
	}
	return encodeBCDDigits(sc)
}

func normalizeSCAddressEncoding(v string) SCAddressEncoding {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", string(SCAddressEncodingTBCD):
		return SCAddressEncodingTBCD
	case string(SCAddressEncodingASCIIDigits):
		return SCAddressEncodingASCIIDigits
	default:
		return SCAddressEncodingTBCD
	}
}

// uint16BE returns a big-endian uint16 from 2 bytes.
func uint16BE(b []byte) uint16 {
	return binary.BigEndian.Uint16(b)
}
