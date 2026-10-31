// Package store defines the storage interface and model types used across
// all database backends.
package store

import (
	"context"
	"time"
)

// ChangeEvent is emitted by a backend whenever a watched table is modified.
type ChangeEvent struct {
	Table     string // table name, e.g. "routing_rules"
	Operation string // INSERT | UPDATE | DELETE
}

// IMSRegistration mirrors the ims_registrations table.
type IMSRegistration struct {
	ID         string    `json:"id"`
	MSISDN     string    `json:"msisdn"`
	IMSI       string    `json:"imsi"`
	SIPAOR     string    `json:"sip_aor"`
	ContactURI string    `json:"contact_uri"`
	SCSCF      string    `json:"s_cscf"`
	Registered bool      `json:"registered"`
	Expiry     time.Time `json:"expiry"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Subscriber mirrors the subscribers table.
type Subscriber struct {
	ID            string    `json:"id"`
	MSISDN        string    `json:"msisdn"`
	IMSI          string    `json:"imsi"`
	IMSRegistered bool      `json:"ims_registered"`
	LTEAttached   bool      `json:"lte_attached"`
	MMENumber     string    `json:"mme_number_for_mt_sms"`
	MMEHost       string    `json:"mme_host"`
	MWDSet        bool      `json:"mwd_set"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// SMPPServerAccount mirrors the smpp_server_accounts table.
type SMPPServerAccount struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	SystemID        string    `json:"system_id"`
	PasswordHash    string    `json:"-"` // never expose bcrypt hash over API
	AllowedIP       string    `json:"allowed_ip"`
	BindType        string    `json:"bind_type"`
	ThroughputLimit int       `json:"throughput_limit"`
	DefaultRouteID  string    `json:"default_route_id"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// SMPPClient mirrors the smpp_clients table.
type SMPPClient struct {
	ID                string        `json:"id"`
	Name              string        `json:"name"`
	Host              string        `json:"host"`
	Port              int           `json:"port"`
	Transport         string        `json:"transport"`
	VerifyServerCert  bool          `json:"verify_server_cert"`
	SystemID          string        `json:"system_id"`
	Password          string        `json:"-"` // never expose decrypted password over API
	BindType          string        `json:"bind_type"`
	ReconnectInterval time.Duration `json:"reconnect_interval"`
	ThroughputLimit   int           `json:"throughput_limit"`
	SourceAddrTON     *int          `json:"source_addr_ton"`
	SourceAddrNPI     *int          `json:"source_addr_npi"`
	DestAddrTON       *int          `json:"dest_addr_ton"`
	DestAddrNPI       *int          `json:"dest_addr_npi"`
	Enabled           bool          `json:"enabled"`
	CreatedAt         time.Time     `json:"created_at"`
	UpdatedAt         time.Time     `json:"updated_at"`
}

// SIPPeer mirrors the sip_peers table.
type SIPPeer struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Address   string    `json:"address"`
	Port      int       `json:"port"`
	Transport string    `json:"transport"`
	Domain    string    `json:"domain"`
	AuthUser  string    `json:"auth_user"`
	AuthPass  string    `json:"-"` // never expose credentials over API
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// DiameterPeer mirrors the diameter_peers table.
type DiameterPeer struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Host         string    `json:"host"`
	Realm        string    `json:"realm"`
	Port         int       `json:"port"`
	Transport    string    `json:"transport"`
	Applications []string  `json:"applications"` // e.g. ["sgd","sh"] — DRA proxy may handle multiple
	Enabled      bool      `json:"enabled"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// SFPolicy mirrors the sf_policies table.
type SFPolicy struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	MaxRetries    int            `json:"max_retries"`
	RetrySchedule []int          `json:"retry_schedule"`
	MaxTTL        time.Duration  `json:"max_ttl"`
	VPOverride    *time.Duration `json:"vp_override"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

// RoutingRule mirrors the routing_rules table.
type RoutingRule struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Priority       int       `json:"priority"`
	MatchSrcIface  string    `json:"match_src_iface"`
	MatchSrcPeer   string    `json:"match_src_peer"`
	MatchDstPrefix string    `json:"match_dst_prefix"`
	MatchMSISDNMin string    `json:"match_msisdn_min"`
	MatchMSISDNMax string    `json:"match_msisdn_max"`
	EgressIface    string    `json:"egress_iface"`
	EgressPeer     string    `json:"egress_peer"`
	SFPolicyID     string    `json:"sf_policy_id"`
	Enabled        bool      `json:"enabled"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// MessageStatus values for the messages table.
const (
	MessageStatusQueued         = "QUEUED"
	MessageStatusDispatched     = "DISPATCHED"
	MessageStatusWaitTimer      = "WAIT_TIMER"
	MessageStatusWaitEvent      = "WAIT_EVENT"
	MessageStatusWaitTimerEvent = "WAIT_TIMER_EVENT"
	MessageStatusDelivered      = "DELIVERED"
	MessageStatusFailed         = "FAILED"
	MessageStatusExpired        = "EXPIRED"
)

// MessageKind values for the messages table.
const (
	MessageKindSM           = "sm"            // ordinary short message
	MessageKindStatusReport = "status_report" // SC-generated SMS-STATUS-REPORT
)

// NormalizeMessageKind maps an unset kind to MessageKindSM.
func NormalizeMessageKind(kind string) string {
	if kind == "" {
		return MessageKindSM
	}
	return kind
}

// Message mirrors the messages table.
type Message struct {
	ID                    string     `json:"id"`
	TPMR                  *int       `json:"tp_mr"`
	SMPPMsgID             string     `json:"smpp_msg_id"`
	OriginIface           string     `json:"origin_iface"`
	OriginPeer            string     `json:"origin_peer"`
	EgressIface           string     `json:"egress_iface"`
	EgressPeer            string     `json:"egress_peer"`
	RouteCursor           int        `json:"route_cursor"`
	SrcMSISDN             string     `json:"src_msisdn"`
	DstMSISDN             string     `json:"dst_msisdn"`
	AlertCorrelationID    string     `json:"alert_correlation_id"`
	DeferredReason        string     `json:"deferred_reason"`
	DeferredInterface     string     `json:"deferred_interface"`
	ServingNodeAtDeferral string     `json:"serving_node_at_deferral"`
	Payload               []byte     `json:"payload,omitempty"`
	UDH                   []byte     `json:"udh,omitempty"`
	Encoding              int        `json:"encoding"`
	DCS                   int        `json:"dcs"`
	Status                string     `json:"status"`
	RetryCount            int        `json:"retry_count"`
	NextRetryAt           *time.Time `json:"next_retry_at"`
	DRRequired            bool       `json:"dr_required"`
	SubmittedAt           time.Time  `json:"submitted_at"`
	ExpiryAt              *time.Time `json:"expiry_at"`
	DeliveredAt           *time.Time `json:"delivered_at"`
	Kind                  string     `json:"kind"`                     // MessageKind*; "" is read as MessageKindSM
	RefMessageID          string     `json:"ref_message_id,omitempty"` // status_report: the SM being reported on
	RawTPDU               []byte     `json:"raw_tpdu,omitempty"`       // pre-encoded TP-DATA sent verbatim on egress
}

// MessageFilter narrows list queries for operational views.
type MessageFilter struct {
	Statuses           []string
	SrcMSISDN          string
	DstMSISDN          string
	OriginPeer         string
	AlertCorrelationID string
	DeferredInterface  string
	Limit              int
}

// SGDMMEMapping mirrors the sgd_mme_mappings table.
// It maps an MME hostname returned by S6c (the S6a FQDN) to the SGd FQDN
// used for Diameter SGd message delivery.
type SGDMMEMapping struct {
	ID        string    `json:"id"`
	S6CResult string    `json:"s6c_result"`
	SGDHost   string    `json:"sgd_host"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// DeliveryReport mirrors the delivery_reports table.
type DeliveryReport struct {
	ID          string    `json:"id"`
	MessageID   string    `json:"message_id"`
	Status      string    `json:"status"`
	EgressIface string    `json:"egress_iface"`
	RawReceipt  string    `json:"raw_receipt"`
	ReportedAt  time.Time `json:"reported_at"`
}

// Store is the database abstraction used by all subsystems.
type Store interface {
	// IMS registration cache
	GetIMSRegistration(ctx context.Context, msisdn string) (*IMSRegistration, error)
	UpsertIMSRegistration(ctx context.Context, reg IMSRegistration) error
	DeleteIMSRegistration(ctx context.Context, msisdn string) error
	ListIMSRegistrations(ctx context.Context) ([]IMSRegistration, error)

	// Subscriber records
	GetSubscriber(ctx context.Context, msisdn string) (*Subscriber, error)
	UpsertSubscriber(ctx context.Context, sub Subscriber) error

	// SMPP server accounts
	GetSMPPServerAccount(ctx context.Context, systemID string) (*SMPPServerAccount, error)
	ListSMPPServerAccounts(ctx context.Context) ([]SMPPServerAccount, error)

	// SMPP outbound clients
	ListSMPPClients(ctx context.Context) ([]SMPPClient, error)

	// SIP SIMPLE peers
	ListSIPPeers(ctx context.Context) ([]SIPPeer, error)
	GetSIPPeer(ctx context.Context, name string) (*SIPPeer, error)

	// Diameter peers
	ListDiameterPeers(ctx context.Context) ([]DiameterPeer, error)
	GetDiameterPeer(ctx context.Context, name string) (*DiameterPeer, error)

	// Routing rules (sorted by priority ascending)
	ListRoutingRules(ctx context.Context) ([]RoutingRule, error)

	// SF policies
	GetSFPolicy(ctx context.Context, id string) (*SFPolicy, error)

	// Messages (store-and-forward)
	SaveMessage(ctx context.Context, msg Message) error
	UpdateMessageRouting(ctx context.Context, id, egressIface, egressPeer string) error
	UpdateMessageDeferred(ctx context.Context, id, deferredReason, deferredInterface, servingNodeAtDeferral string, routeCursor int) error
	UpdateMessageStatus(ctx context.Context, id, status string) error
	ClaimMessageForDispatch(ctx context.Context, id string, allowedStatuses []string) (bool, error)
	UpdateMessageRetry(ctx context.Context, id string, retryCount int, nextRetryAt time.Time, routeCursor int) error
	UpdateMessageExpiryCap(ctx context.Context, id string, expiryAt time.Time) error
	RequeueMessageForAlert(ctx context.Context, id string, nextRetryAt time.Time, routeCursor int, deferredReason string, allowedStatuses []string) (bool, error)
	ListRetryableMessages(ctx context.Context) ([]Message, error)
	ListExpiredMessages(ctx context.Context) ([]Message, error)
	GetMessage(ctx context.Context, id string) (*Message, error)
	DeleteMessage(ctx context.Context, id string) error

	// Delivery reports
	SaveDeliveryReport(ctx context.Context, dr DeliveryReport) error

	// ── Full CRUD for operational config ────────────────────────────────────

	// SMPP server accounts — full CRUD
	GetSMPPServerAccountByID(ctx context.Context, id string) (*SMPPServerAccount, error)
	CreateSMPPServerAccount(ctx context.Context, a SMPPServerAccount) error
	UpdateSMPPServerAccount(ctx context.Context, a SMPPServerAccount) error
	DeleteSMPPServerAccount(ctx context.Context, id string) error

	// SMPP outbound clients — full CRUD
	GetSMPPClient(ctx context.Context, id string) (*SMPPClient, error)
	CreateSMPPClient(ctx context.Context, c SMPPClient) error
	UpdateSMPPClient(ctx context.Context, c SMPPClient) error
	DeleteSMPPClient(ctx context.Context, id string) error

	// SIP SIMPLE peers — full CRUD
	ListAllSIPPeers(ctx context.Context) ([]SIPPeer, error)
	GetSIPPeerByID(ctx context.Context, id string) (*SIPPeer, error)
	CreateSIPPeer(ctx context.Context, p SIPPeer) error
	UpdateSIPPeer(ctx context.Context, p SIPPeer) error
	DeleteSIPPeer(ctx context.Context, id string) error

	// Diameter peers — full CRUD
	ListAllDiameterPeers(ctx context.Context) ([]DiameterPeer, error)
	GetDiameterPeerByID(ctx context.Context, id string) (*DiameterPeer, error)
	CreateDiameterPeer(ctx context.Context, p DiameterPeer) error
	UpdateDiameterPeer(ctx context.Context, p DiameterPeer) error
	DeleteDiameterPeer(ctx context.Context, id string) error

	// Routing rules — full CRUD
	ListAllRoutingRules(ctx context.Context) ([]RoutingRule, error)
	GetRoutingRule(ctx context.Context, id string) (*RoutingRule, error)
	CreateRoutingRule(ctx context.Context, r RoutingRule) error
	UpdateRoutingRule(ctx context.Context, r RoutingRule) error
	DeleteRoutingRule(ctx context.Context, id string) error

	// SF policies — full CRUD
	ListSFPolicies(ctx context.Context) ([]SFPolicy, error)
	CreateSFPolicy(ctx context.Context, p SFPolicy) error
	UpdateSFPolicy(ctx context.Context, p SFPolicy) error
	DeleteSFPolicy(ctx context.Context, id string) error

	// Subscribers — extended
	ListSubscribers(ctx context.Context) ([]Subscriber, error)
	GetSubscriberByID(ctx context.Context, id string) (*Subscriber, error)
	DeleteSubscriber(ctx context.Context, id string) error

	// Messages — API views
	ListMessages(ctx context.Context, limit int) ([]Message, error)
	ListFilteredMessages(ctx context.Context, filter MessageFilter) ([]Message, error)
	// CountMessagesByStatus returns the count of messages in each status bucket.
	// The returned map contains all statuses present in the table; missing = 0.
	CountMessagesByStatus(ctx context.Context) (map[string]int64, error)

	// Delivery reports — API views
	ListDeliveryReports(ctx context.Context, limit int) ([]DeliveryReport, error)
	GetDeliveryReport(ctx context.Context, id string) (*DeliveryReport, error)

	// SGd MME mappings — full CRUD
	ListSGDMMEMappings(ctx context.Context) ([]SGDMMEMapping, error)
	GetSGDMMEMappingByID(ctx context.Context, id string) (*SGDMMEMapping, error)
	CreateSGDMMEMapping(ctx context.Context, m SGDMMEMapping) error
	UpdateSGDMMEMapping(ctx context.Context, m SGDMMEMapping) error
	DeleteSGDMMEMapping(ctx context.Context, id string) error

	// Hot-reload subscription.
	Subscribe(ctx context.Context, table string, ch chan<- ChangeEvent) error

	// Lifecycle
	Close() error
}
