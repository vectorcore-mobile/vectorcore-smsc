package sgd

import "strings"

// MSISDNEncoding selects the wire format of SGd MSISDN AVPs.
//
// TS 29.338 uses the MSISDN AVP of TS 29.329 §6.3.2: the international E.164
// number as a plain TBCD string, with no length or nature-of-address octet.
// Some deployed peers (including vectorcore-mme at the time of writing) send
// and expect "length | NOA | TBCD" instead. The two can't be told apart
// reliably from the bytes alone: TBCD of a number whose second digit is 8 or
// 9 (e.g. country code 98, 49, 39) starts with an octet that looks like a
// NOA. So the format is configured, not guessed.
type MSISDNEncoding string

const (
	// MSISDNEncodingTBCD is the standards-compliant plain TBCD string.
	MSISDNEncodingTBCD MSISDNEncoding = "tbcd"
	// MSISDNEncodingLegacyPrefixed decodes the historical lenient formats
	// (len|NOA|TBCD, len|TBCD, NOA|TBCD) and encodes len|NOA(0x91)|TBCD.
	MSISDNEncodingLegacyPrefixed MSISDNEncoding = "legacy_prefixed"
)

// NormalizeMSISDNEncoding maps a configured value to an MSISDNEncoding.
// Empty and unknown values select the standards-compliant TBCD format.
func NormalizeMSISDNEncoding(v string) MSISDNEncoding {
	if MSISDNEncoding(strings.ToLower(strings.TrimSpace(v))) == MSISDNEncodingLegacyPrefixed {
		return MSISDNEncodingLegacyPrefixed
	}
	return MSISDNEncodingTBCD
}

// AddressOptions carries the configured SGd address encodings.
type AddressOptions struct {
	MSISDN    MSISDNEncoding
	SCAddress string // SC-Address encoding: "tbcd" (default) or "ascii_digits"
}

// DecodeMSISDN decodes an SGd MSISDN AVP value to E.164 digits without a
// leading "+".
func DecodeMSISDN(data []byte, enc MSISDNEncoding) string {
	if len(data) == 0 {
		return ""
	}
	if enc == MSISDNEncodingLegacyPrefixed {
		return decodeLegacyPrefixedMSISDN(data)
	}
	return decodeBCDDigits(data)
}

// EncodeMSISDN encodes E.164 digits (an optional leading "+" is ignored) as
// an SGd MSISDN AVP value.
func EncodeMSISDN(msisdn string, enc MSISDNEncoding) []byte {
	msisdn = strings.TrimPrefix(msisdn, "+")
	digits := encodeBCDDigits(msisdn)
	if enc != MSISDNEncodingLegacyPrefixed {
		return digits
	}
	out := make([]byte, 2+len(digits))
	out[0] = byte(1 + len(digits)) // length of NOA + digits
	out[1] = 0x91                  // international, ISDN/E.164
	copy(out[2:], digits)
	return out
}

// decodeLegacyPrefixedMSISDN is the historical lenient decoder, kept for
// peers configured as legacy_prefixed. It accepts:
//
//	len | NOA | TBCD
//	len | TBCD
//	NOA | TBCD
func decodeLegacyPrefixedMSISDN(data []byte) string {
	offset := 0
	if data[0] < byte(len(data)) {
		offset = 1
	}
	if offset < len(data) && data[offset]&0x80 != 0 {
		offset++
	}
	return decodeBCDDigits(data[offset:])
}

// DecodeSCAddress decodes an SGd SC-Address AVP (TS 29.338 §6.3.3.1, plain
// TBCD). ASCII digits are accepted only when the SMSC is configured with
// sgd_sc_address_encoding: ascii_digits, since octets 0x30-0x39 are also
// valid TBCD.
func DecodeSCAddress(data []byte, scAddrEncoding string) string {
	if len(data) == 0 {
		return ""
	}
	if normalizeSCAddressEncoding(scAddrEncoding) == SCAddressEncodingASCIIDigits && isASCIIDigits(data) {
		return string(data)
	}
	return decodeBCDDigits(data)
}
