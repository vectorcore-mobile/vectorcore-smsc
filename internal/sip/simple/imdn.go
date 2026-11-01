// Package simple implements SIP SIMPLE inter-site messaging (RFC 3428)
// with RFC 5438 IMDN delivery notifications.
package simple

import (
	"fmt"
	"time"
)

// IMDNStatus values per RFC 5438 §7.2.
const (
	IMDNDelivered = "delivered"
	IMDNFailed    = "failed"
	IMDNProcessed = "processed"
	IMDNDisplayed = "displayed"
)

// IMDNBody generates a message/imdn+xml body per RFC 5438.
// messageID is the original-message-id from the inbound MESSAGE.
func IMDNBody(messageID, status string) []byte {
	ts := time.Now().UTC().Format(time.RFC3339)
	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<imdn xmlns="urn:ietf:params:xml:ns:imdn">
  <message-id>%s</message-id>
  <datetime>%s</datetime>
  <delivery-notification>
    <status>
      <%s/>
    </status>
  </delivery-notification>
</imdn>`, messageID, ts, status)
	return []byte(body)
}

const IMDNContentType = "message/imdn+xml"

// ParseIMDNMessageID extracts the <message-id> value from an imdn+xml body.
// Returns empty string if not found (no full XML parser — simple text scan).
func ParseIMDNMessageID(body []byte) string {
	s := string(body)
	const open = "<message-id>"
	const close = "</message-id>"
	start := indexOf(s, open)
	if start < 0 {
		return ""
	}
	start += len(open)
	end := indexOf(s[start:], close)
	if end < 0 {
		return ""
	}
	return s[start : start+end]
}

func indexOf(s, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
