package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadSupportsNewSMPPTLSLayout(t *testing.T) {
	cfg := loadTestConfig(t, `
smpp:
  server:
    address: "0.0.0.0"
    port: 2775
  server_tls:
    address: "127.0.0.1"
    port: 3550
    cert_file: "/tmp/server.crt"
    key_file: "/tmp/server.key"
    verify_client_cert: true
    client_ca_file: "/tmp/client-ca.crt"
  outbound_client_tls:
    server_ca_file: "/tmp/server-ca.crt"
`)

	if got := cfg.SMPPServerTLSListen(); got != "127.0.0.1:3550" {
		t.Fatalf("SMPPServerTLSListen() = %q", got)
	}
	if got := cfg.SMPPServerTLSCertFile(); got != "/tmp/server.crt" {
		t.Fatalf("SMPPServerTLSCertFile() = %q", got)
	}
	if got := cfg.SMPPServerTLSKeyFile(); got != "/tmp/server.key" {
		t.Fatalf("SMPPServerTLSKeyFile() = %q", got)
	}
	if got := cfg.SMPPServerTLSClientCAFile(); got != "/tmp/client-ca.crt" {
		t.Fatalf("SMPPServerTLSClientCAFile() = %q", got)
	}
	if got := cfg.SMPPOutboundServerCAFile(); got != "/tmp/server-ca.crt" {
		t.Fatalf("SMPPOutboundServerCAFile() = %q", got)
	}
}

func TestLoadParsesS6cCacheTTL(t *testing.T) {
	cfg := loadTestConfig(t, `
diameter:
  s6c_cache_ttl: 300s
`)

	if got := cfg.Diameter.S6CCacheTTL; got != 300*time.Second {
		t.Fatalf("S6CCacheTTL = %v, want 300s", got)
	}
}

func TestLoadNumberingStripsLeadingPlusFromDefaultCountryCode(t *testing.T) {
	cfg := loadTestConfig(t, `
numbering:
  default_country_code: "+33"
  local_national_length: 9
`)

	if got := cfg.Numbering.DefaultCountryCode; got != "33" {
		t.Fatalf("DefaultCountryCode = %q, want %q", got, "33")
	}
	if got := cfg.Numbering.LocalNationalLength; got != 9 {
		t.Fatalf("LocalNationalLength = %d, want 9", got)
	}
}

func loadTestConfig(t *testing.T, body string) *Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%q): %v", path, err)
	}
	return cfg
}

func TestLoadSGdTFATimeout(t *testing.T) {
	if got := loadTestConfig(t, "smsc:\n  address: \"+15550000000\"\n").SMSC.SGdTFATimeout; got != 40*time.Second {
		t.Fatalf("default SGdTFATimeout = %v, want 40s", got)
	}
	if got := loadTestConfig(t, "smsc:\n  sgd_tfa_timeout: 45s\n").SMSC.SGdTFATimeout; got != 45*time.Second {
		t.Fatalf("SGdTFATimeout = %v, want 45s", got)
	}
}

func TestLoadSGdMSISDNEncoding(t *testing.T) {
	if got := loadTestConfig(t, "smsc:\n  address: \"+15550000000\"\n").SMSC.SGdMSISDNEncoding; got != "tbcd" {
		t.Fatalf("default SGdMSISDNEncoding = %q, want tbcd", got)
	}
	if got := loadTestConfig(t, "smsc:\n  sgd_msisdn_encoding: legacy_prefixed\n").SMSC.SGdMSISDNEncoding; got != "legacy_prefixed" {
		t.Fatalf("SGdMSISDNEncoding = %q, want legacy_prefixed", got)
	}
}
