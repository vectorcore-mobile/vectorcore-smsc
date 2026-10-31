package tpdu

import "testing"

func FuzzDecode(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x01})
	f.Add([]byte{0x01, 0x00, 0x00, 0x81, 0x00, 0x00, 0x00})
	f.Add([]byte{0x00, 0x00, 0x81, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0x02, 0x00, 0x00, 0x81, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	// Well-formed SMS-STATUS-REPORT (see TestEncodeStatusReportKnownVector).
	f.Add([]byte{0x06, 0x2A, 0x0C, 0x91, 0x89, 0x19, 0x00, 0x00, 0x10, 0x11, 0x62, 0x01, 0x03, 0x71, 0x25, 0x83, 0x00, 0x62, 0x01, 0x03, 0x71, 0x25, 0x54, 0x00, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Decode(data)
	})
}

func FuzzGSM7RoundTrip(f *testing.F) {
	f.Add("")
	f.Add("hello")
	f.Add("[]{}^~|€")
	f.Add("not representable: 🙂")

	f.Fuzz(func(t *testing.T, text string) {
		packed, septets := EncodeGSM7(text)
		decoded := DecodeGSM7(packed, septets, 0)

		// Encoding is intentionally lossy for non-GSM7 runes. A second
		// encode/decode must nevertheless be stable.
		repacked, reseptets := EncodeGSM7(decoded)
		redecoded := DecodeGSM7(repacked, reseptets, 0)
		if redecoded != decoded {
			t.Fatalf("GSM7 encoding is not stable: first %q, second %q", decoded, redecoded)
		}
	})
}

func FuzzUCS2RoundTrip(f *testing.F) {
	f.Add("")
	f.Add("hello")
	f.Add("☺😳")
	f.Add(string([]byte{0xff, 0xfe, 0xfd}))

	f.Fuzz(func(t *testing.T, text string) {
		decoded := DecodeUCS2(EncodeUCS2(text))
		redecoded := DecodeUCS2(EncodeUCS2(decoded))
		if redecoded != decoded {
			t.Fatalf("UCS2 encoding is not stable: first %q, second %q", decoded, redecoded)
		}
	})
}
