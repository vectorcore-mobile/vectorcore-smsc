package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	SMSC      SMSCConfig      `yaml:"smsc"`
	Numbering NumberingConfig `yaml:"numbering"`
	SMPP      SMPPConfig      `yaml:"smpp"`
	SIP       SIPConfig       `yaml:"sip"`
	Diameter  DiameterConfig  `yaml:"diameter"`
	Database  DatabaseConfig  `yaml:"database"`
	API       APIConfig       `yaml:"api"`
	Log       LogConfig       `yaml:"log"`
}

type LogConfig struct {
	// File is the path to write logs to in addition to stderr.
	// Leave empty to log to stderr only.
	File string `yaml:"file"`
	// Level overrides the -d flag: debug | info | warn | error
	Level string `yaml:"level"`
}

type SMSCConfig struct {
	Address              string        `yaml:"address"`                 // SMSC E.164 / GT — used across all interfaces
	SGdSCAddressEncoding string        `yaml:"sgd_sc_address_encoding"` // tbcd | ascii_digits
	MaxQueueLifetime     time.Duration `yaml:"max_queue_lifetime"`      // global max queued/waiting lifetime
	SGdTFATimeout        time.Duration `yaml:"sgd_tfa_timeout"`         // wait for MT-Forward answer (TFA) from the MME
}

type NumberingConfig struct {
	DefaultCountryCode  string `yaml:"default_country_code"`
	LocalNationalLength int    `yaml:"local_national_length"`
}

type SMPPConfig struct {
	Server            SMPPServerConfig            `yaml:"server"`
	ServerTLS         SMPPServerTLSConfig         `yaml:"server_tls"`
	OutboundClientTLS SMPPOutboundClientTLSConfig `yaml:"outbound_client_tls"`
}

type SMPPServerConfig struct {
	Address             string        `yaml:"address"`
	Port                int           `yaml:"port"`
	Listen              string        `yaml:"listen"` // legacy; overridden by address+port if set
	MaxConnections      int           `yaml:"max_connections"`
	EnquireLinkInterval time.Duration `yaml:"enquire_link_interval"`
	ResponseTimeout     time.Duration `yaml:"response_timeout"`
}

type SMPPServerTLSConfig struct {
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
	Listen  string `yaml:"listen"` // optional explicit listener; address+port preferred

	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`

	RequireClientCert bool   `yaml:"require_client_cert"`
	VerifyClientCert  bool   `yaml:"verify_client_cert"`
	ClientCAFile      string `yaml:"client_ca_file"`
}

type SMPPOutboundClientTLSConfig struct {
	ServerCAFile string `yaml:"server_ca_file"`
}

type SIPConfig struct {
	Address   string       `yaml:"address"`
	Port      int          `yaml:"port"`
	Listen    string       `yaml:"listen"`    // legacy; overridden by address+port if set
	FQDN      string       `yaml:"fqdn"`      // explicit SIP identity host/domain
	Transport string       `yaml:"transport"` // udp | tcp
	ISC       SIPISCConfig `yaml:"isc"`
}

type SIPISCConfig struct {
	AcceptContact                  string `yaml:"accept_contact"`
	MTRequestDisposition           string `yaml:"mt_request_disposition"`
	SubmitReportRequestDisposition string `yaml:"submit_report_request_disposition"`
}

type DiameterConfig struct {
	Address     string        `yaml:"address"`
	Port        int           `yaml:"port"`
	Listen      string        `yaml:"listen"`    // legacy; overridden by address+port if set
	Transport   string        `yaml:"transport"` // tcp | sctp
	LocalFQDN   string        `yaml:"local_fqdn"`
	LocalRealm  string        `yaml:"local_realm"`
	S6CCacheTTL time.Duration `yaml:"s6c_cache_ttl"`
}

type DatabaseConfig struct {
	Driver       string        `yaml:"driver"` // postgres | sqlite
	DSN          string        `yaml:"dsn"`
	PollInterval time.Duration `yaml:"poll_interval"` // SQLite only
}

type APIConfig struct {
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
	Listen  string `yaml:"listen"` // legacy; overridden by address+port if set
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	cfg.applyDefaults()
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	c.Numbering.DefaultCountryCode = strings.TrimPrefix(strings.TrimSpace(c.Numbering.DefaultCountryCode), "+")
	if c.SMSC.SGdSCAddressEncoding == "" {
		c.SMSC.SGdSCAddressEncoding = "tbcd"
	}
	if c.SMSC.MaxQueueLifetime == 0 {
		c.SMSC.MaxQueueLifetime = 7 * 24 * time.Hour
	}
	if c.SMSC.SGdTFATimeout <= 0 {
		c.SMSC.SGdTFATimeout = 40 * time.Second // keep above the MME's SMS transaction timeout
	}
	// Build Listen addresses from address+port fields when provided.
	if c.SMPP.Server.Address != "" || c.SMPP.Server.Port != 0 {
		addr := c.SMPP.Server.Address
		if addr == "" {
			addr = "0.0.0.0"
		}
		port := c.SMPP.Server.Port
		if port == 0 {
			port = 2775
		}
		c.SMPP.Server.Listen = net.JoinHostPort(addr, strconv.Itoa(port))
	}
	if c.SMPP.Server.Listen == "" {
		c.SMPP.Server.Listen = "0.0.0.0:2775"
	}
	if c.SMPP.Server.MaxConnections == 0 {
		c.SMPP.Server.MaxConnections = 50
	}
	if c.SMPP.Server.EnquireLinkInterval == 0 {
		c.SMPP.Server.EnquireLinkInterval = 30 * time.Second
	}
	if c.SMPP.Server.ResponseTimeout == 0 {
		c.SMPP.Server.ResponseTimeout = 10 * time.Second
	}
	if c.SMPP.ServerTLS.Address != "" || c.SMPP.ServerTLS.Port != 0 {
		addr := c.SMPP.ServerTLS.Address
		if addr == "" {
			addr = "0.0.0.0"
		}
		port := c.SMPP.ServerTLS.Port
		if port == 0 {
			port = 3550
		}
		c.SMPP.ServerTLS.Listen = net.JoinHostPort(addr, strconv.Itoa(port))
	}
	if c.SIP.Address != "" || c.SIP.Port != 0 {
		addr := c.SIP.Address
		if addr == "" {
			addr = "0.0.0.0"
		}
		port := c.SIP.Port
		if port == 0 {
			port = 5060
		}
		c.SIP.Listen = net.JoinHostPort(addr, strconv.Itoa(port))
	}
	if c.SIP.Listen == "" {
		c.SIP.Listen = "0.0.0.0:5060"
	}
	if c.SIP.Transport == "" {
		c.SIP.Transport = "udp"
	}
	if c.SIP.ISC.AcceptContact == "" {
		c.SIP.ISC.AcceptContact = "*;+g.3gpp.smsip"
	}
	if c.SIP.ISC.MTRequestDisposition == "" {
		c.SIP.ISC.MTRequestDisposition = "no-fork"
	}
	if c.SIP.ISC.SubmitReportRequestDisposition == "" {
		c.SIP.ISC.SubmitReportRequestDisposition = "no-fork"
	}
	if c.Diameter.Address != "" || c.Diameter.Port != 0 {
		addr := c.Diameter.Address
		if addr == "" {
			addr = "0.0.0.0"
		}
		port := c.Diameter.Port
		if port == 0 {
			port = 3868
		}
		c.Diameter.Listen = net.JoinHostPort(addr, strconv.Itoa(port))
	}
	if c.Diameter.Listen == "" {
		c.Diameter.Listen = "0.0.0.0:3868"
	}
	if c.Diameter.Transport == "" {
		c.Diameter.Transport = "tcp"
	}
	if c.Diameter.LocalFQDN == "" {
		c.Diameter.LocalFQDN = "smsc.local"
	}
	if c.Diameter.LocalRealm == "" {
		c.Diameter.LocalRealm = "local"
	}
	if c.Diameter.S6CCacheTTL == 0 {
		c.Diameter.S6CCacheTTL = 300 * time.Second
	}
	if c.SIP.FQDN == "" {
		// Prefer explicit Diameter identity when SIP identity isn't set.
		c.SIP.FQDN = c.Diameter.LocalFQDN
	}
	if c.Database.Driver == "" {
		c.Database.Driver = "postgres"
	}
	if c.Database.PollInterval == 0 {
		c.Database.PollInterval = 2 * time.Second
	}
	if c.API.Address != "" || c.API.Port != 0 {
		addr := c.API.Address
		if addr == "" {
			addr = "0.0.0.0"
		}
		port := c.API.Port
		if port == 0 {
			port = 8080
		}
		c.API.Listen = net.JoinHostPort(addr, strconv.Itoa(port))
	}
	if c.API.Listen == "" {
		c.API.Listen = "0.0.0.0:8080"
	}
}

func (c *Config) SMPPServerTLSListen() string {
	return c.SMPP.ServerTLS.Listen
}

func (c *Config) SMPPServerTLSCertFile() string {
	return c.SMPP.ServerTLS.CertFile
}

func (c *Config) SMPPServerTLSKeyFile() string {
	return c.SMPP.ServerTLS.KeyFile
}

func (c *Config) SMPPServerTLSRequireClientCert() bool {
	return c.SMPP.ServerTLS.RequireClientCert
}

func (c *Config) SMPPServerTLSVerifyClientCert() bool {
	return c.SMPP.ServerTLS.VerifyClientCert
}

func (c *Config) SMPPServerTLSClientCAFile() string {
	return c.SMPP.ServerTLS.ClientCAFile
}

func (c *Config) SMPPOutboundServerCAFile() string {
	return c.SMPP.OutboundClientTLS.ServerCAFile
}
