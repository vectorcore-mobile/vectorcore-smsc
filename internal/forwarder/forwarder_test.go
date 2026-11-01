package forwarder

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter/s6c"
	diametersgd "github.com/svinson1121/vectorcore-smsc/internal/diameter/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/numbering"
	"github.com/svinson1121/vectorcore-smsc/internal/registry"
	"github.com/svinson1121/vectorcore-smsc/internal/routing"
	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

type forwarderTestStore struct {
	subscriber    *store.Subscriber
	sipPeers      map[string]store.SIPPeer
	sfPolicies    map[string]store.SFPolicy
	saved         *store.Message
	routeIface    string
	routePeer     string
	routingCalls  int
	statusUpdates []string
	retryCount    int
	routeCursor   int
	nextRetryAt   *time.Time
	expiryCapAt   *time.Time
	claimBlocked  bool
	deferred      *struct {
		reason      string
		iface       string
		servingNode string
		routeCursor int
	}
}

func (s *forwarderTestStore) GetIMSRegistration(context.Context, string) (*store.IMSRegistration, error) {
	return nil, nil
}
func (s *forwarderTestStore) UpsertIMSRegistration(context.Context, store.IMSRegistration) error {
	return nil
}
func (s *forwarderTestStore) DeleteIMSRegistration(context.Context, string) error { return nil }
func (s *forwarderTestStore) ListIMSRegistrations(context.Context) ([]store.IMSRegistration, error) {
	return nil, nil
}
func (s *forwarderTestStore) GetSubscriber(context.Context, string) (*store.Subscriber, error) {
	return s.subscriber, nil
}
func (s *forwarderTestStore) UpsertSubscriber(_ context.Context, sub store.Subscriber) error {
	cp := sub
	s.subscriber = &cp
	return nil
}
func (s *forwarderTestStore) GetSMPPServerAccount(context.Context, string) (*store.SMPPServerAccount, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) ListSMPPServerAccounts(context.Context) ([]store.SMPPServerAccount, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) ListSMPPClients(context.Context) ([]store.SMPPClient, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) ListSIPPeers(context.Context) ([]store.SIPPeer, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetSIPPeer(_ context.Context, name string) (*store.SIPPeer, error) {
	if peer, ok := s.sipPeers[name]; ok {
		cp := peer
		return &cp, nil
	}
	return nil, nil
}
func (s *forwarderTestStore) ListDiameterPeers(context.Context) ([]store.DiameterPeer, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetDiameterPeer(context.Context, string) (*store.DiameterPeer, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) ListRoutingRules(context.Context) ([]store.RoutingRule, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetSFPolicy(_ context.Context, id string) (*store.SFPolicy, error) {
	if s.sfPolicies == nil {
		return nil, nil
	}
	pol, ok := s.sfPolicies[id]
	if !ok {
		return nil, nil
	}
	cp := pol
	return &cp, nil
}
func (s *forwarderTestStore) SaveMessage(_ context.Context, msg store.Message) error {
	cp := msg
	s.saved = &cp
	return nil
}
func (s *forwarderTestStore) UpdateMessageRouting(_ context.Context, _ string, egressIface, egressPeer string) error {
	s.routeIface = egressIface
	s.routePeer = egressPeer
	s.routingCalls++
	if s.saved != nil {
		s.saved.EgressIface = egressIface
		s.saved.EgressPeer = egressPeer
	}
	return nil
}
func (s *forwarderTestStore) UpdateMessageStatus(_ context.Context, _ string, status string) error {
	s.statusUpdates = append(s.statusUpdates, status)
	return nil
}
func (s *forwarderTestStore) ClaimMessageForDispatch(_ context.Context, _ string, _ []string) (bool, error) {
	if s.claimBlocked {
		return false, nil
	}
	s.statusUpdates = append(s.statusUpdates, store.MessageStatusDispatched)
	return true, nil
}
func (s *forwarderTestStore) UpdateMessageRetry(_ context.Context, _ string, retryCount int, nextRetryAt time.Time, routeCursor int) error {
	s.retryCount = retryCount
	s.routeCursor = routeCursor
	s.nextRetryAt = &nextRetryAt
	return nil
}
func (s *forwarderTestStore) UpdateMessageExpiryCap(_ context.Context, _ string, expiryAt time.Time) error {
	s.expiryCapAt = &expiryAt
	return nil
}
func (s *forwarderTestStore) UpdateMessageDeferred(_ context.Context, _ string, deferredReason, deferredInterface, servingNodeAtDeferral string, routeCursor int) error {
	s.deferred = &struct {
		reason      string
		iface       string
		servingNode string
		routeCursor int
	}{
		reason:      deferredReason,
		iface:       deferredInterface,
		servingNode: servingNodeAtDeferral,
		routeCursor: routeCursor,
	}
	return nil
}
func (s *forwarderTestStore) RequeueMessageForAlert(_ context.Context, _ string, nextRetryAt time.Time, routeCursor int, deferredReason string, _ []string) (bool, error) {
	s.routeCursor = routeCursor
	s.nextRetryAt = &nextRetryAt
	if deferredReason != "" {
		s.deferred = &struct {
			reason      string
			iface       string
			servingNode string
			routeCursor int
		}{
			reason:      deferredReason,
			routeCursor: routeCursor,
		}
	}
	return true, nil
}
func (s *forwarderTestStore) ListRetryableMessages(context.Context) ([]store.Message, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) ListExpiredMessages(context.Context) ([]store.Message, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetMessage(_ context.Context, id string) (*store.Message, error) {
	if s.saved != nil && s.saved.ID == id {
		cp := *s.saved
		return &cp, nil
	}
	return nil, nil
}
func (s *forwarderTestStore) DeleteMessage(context.Context, string) error { panic("unexpected call") }
func (s *forwarderTestStore) SaveDeliveryReport(context.Context, store.DeliveryReport) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetSMPPServerAccountByID(context.Context, string) (*store.SMPPServerAccount, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) CreateSMPPServerAccount(context.Context, store.SMPPServerAccount) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) UpdateSMPPServerAccount(context.Context, store.SMPPServerAccount) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) DeleteSMPPServerAccount(context.Context, string) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetSMPPClient(context.Context, string) (*store.SMPPClient, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) CreateSMPPClient(context.Context, store.SMPPClient) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) UpdateSMPPClient(context.Context, store.SMPPClient) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) DeleteSMPPClient(context.Context, string) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) ListAllSIPPeers(context.Context) ([]store.SIPPeer, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetSIPPeerByID(context.Context, string) (*store.SIPPeer, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) CreateSIPPeer(context.Context, store.SIPPeer) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) UpdateSIPPeer(context.Context, store.SIPPeer) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) DeleteSIPPeer(context.Context, string) error { panic("unexpected call") }
func (s *forwarderTestStore) ListAllDiameterPeers(context.Context) ([]store.DiameterPeer, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetDiameterPeerByID(context.Context, string) (*store.DiameterPeer, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) CreateDiameterPeer(context.Context, store.DiameterPeer) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) UpdateDiameterPeer(context.Context, store.DiameterPeer) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) DeleteDiameterPeer(context.Context, string) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) ListAllRoutingRules(context.Context) ([]store.RoutingRule, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetRoutingRule(context.Context, string) (*store.RoutingRule, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) CreateRoutingRule(context.Context, store.RoutingRule) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) UpdateRoutingRule(context.Context, store.RoutingRule) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) DeleteRoutingRule(context.Context, string) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) ListSFPolicies(context.Context) ([]store.SFPolicy, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) CreateSFPolicy(context.Context, store.SFPolicy) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) UpdateSFPolicy(context.Context, store.SFPolicy) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) DeleteSFPolicy(context.Context, string) error { panic("unexpected call") }
func (s *forwarderTestStore) ListSubscribers(context.Context) ([]store.Subscriber, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetSubscriberByID(context.Context, string) (*store.Subscriber, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) DeleteSubscriber(context.Context, string) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) ListMessages(context.Context, int) ([]store.Message, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) ListFilteredMessages(context.Context, store.MessageFilter) ([]store.Message, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) CountMessagesByStatus(context.Context) (map[string]int64, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) ListDeliveryReports(context.Context, int) ([]store.DeliveryReport, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetDeliveryReport(context.Context, string) (*store.DeliveryReport, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) Subscribe(context.Context, string, chan<- store.ChangeEvent) error {
	return nil
}
func (s *forwarderTestStore) Close() error { return nil }
func (s *forwarderTestStore) ListSGDMMEMappings(context.Context) ([]store.SGDMMEMapping, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) GetSGDMMEMappingByID(context.Context, string) (*store.SGDMMEMapping, error) {
	panic("unexpected call")
}
func (s *forwarderTestStore) CreateSGDMMEMapping(context.Context, store.SGDMMEMapping) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) UpdateSGDMMEMapping(context.Context, store.SGDMMEMapping) error {
	panic("unexpected call")
}
func (s *forwarderTestStore) DeleteSGDMMEMapping(context.Context, string) error {
	panic("unexpected call")
}

type fakeISCSender struct {
	calls int
	err   error
	msgs  []*codec.Message
}

func (f *fakeISCSender) Send(_ context.Context, msg *codec.Message, _ *registry.Registration) error {
	f.calls++
	if msg != nil {
		cp := *msg
		if msg.Binary != nil {
			cp.Binary = append([]byte(nil), msg.Binary...)
		}
		if msg.UDH != nil {
			cp.UDH = &codec.UDH{Raw: append([]byte(nil), msg.UDH.Raw...)}
		}
		f.msgs = append(f.msgs, &cp)
	}
	return f.err
}

type fakeSimpleSender struct {
	calls int
	err   error
	errs  []error
	msgs  []*codec.Message
}

func (f *fakeSimpleSender) Send(_ context.Context, msg *codec.Message, _ store.SIPPeer) error {
	f.calls++
	if msg != nil {
		cp := *msg
		if msg.ValidityPeriod != nil {
			d := *msg.ValidityPeriod
			cp.ValidityPeriod = &d
		}
		f.msgs = append(f.msgs, &cp)
	}
	if idx := f.calls - 1; idx >= 0 && idx < len(f.errs) && f.errs[idx] != nil {
		return f.errs[idx]
	}
	return f.err
}

type fakeS6cClient struct {
	info  *s6c.RoutingInfo
	err   error
	calls int
}

func (f *fakeS6cClient) LookupRouting(msisdn string) (*s6c.RoutingInfo, error) {
	f.calls++
	return f.info, f.err
}

type fakeSGdSender struct {
	lastSCAddr string
	lastMME    string
	lastMsg    *codec.Message
	err        error
}

func (f *fakeSGdSender) SendOFR(_ context.Context, msg *codec.Message, mmeHost, scAddr string) error {
	f.lastMME = mmeHost
	f.lastSCAddr = scAddr
	f.lastMsg = msg
	return f.err
}
func (f *fakeSGdSender) HasPeerForMME(string) bool { return true }

func durationPtr(d time.Duration) *time.Duration { return &d }

func newTestRegistry(t *testing.T, st store.Store) *registry.Registry {
	t.Helper()
	reg, err := registry.New(context.Background(), st)
	if err != nil {
		t.Fatalf("registry.New() error = %v", err)
	}
	return reg
}

func TestNormalizeIngressRecipientUsesConfiguredNationalContext(t *testing.T) {
	f := New(Config{
		DefaultCountryCode:  "1",
		LocalNationalLength: 10,
	})
	msg := &codec.Message{
		Destination: codec.Address{
			MSISDN: "6752012860",
			TON:    numbering.TONUnknown,
			NPI:    0x01,
		},
	}

	f.normalizeIngressRecipient(msg)

	if got, want := msg.Destination.MSISDN, "16752012860"; got != want {
		t.Fatalf("Destination.MSISDN = %q, want %q", got, want)
	}
	if got, want := msg.Destination.TON, numbering.TONUnknown; got != want {
		t.Fatalf("Destination.TON = %d, want %d", got, want)
	}
	if got, want := msg.Destination.NPI, byte(0x01); got != want {
		t.Fatalf("Destination.NPI = %d, want %d", got, want)
	}
}

func TestSelectRouteUsesS6cOnlyForFallbackCandidates(t *testing.T) {
	st := &forwarderTestStore{
		sipPeers: map[string]store.SIPPeer{
			"simple1": {Name: "simple1"},
		},
	}
	reg := newTestRegistry(t, st)
	s6cClient := &fakeS6cClient{}
	reg.SetS6cClient(s6cClient)

	engine := routing.NewEngine()
	engine.Reload([]store.RoutingRule{
		{Name: "simple", Priority: 10, MatchSrcIface: "smpp", MatchDstPrefix: "334", EgressIface: "sipsimple", EgressPeer: "simple1"},
	})

	f := New(Config{
		Registry:     reg,
		Engine:       engine,
		Store:        st,
		SimpleSender: &fakeSimpleSender{},
		SGdSender:    &fakeSGdSender{},
	})

	route, err := f.selectRoute(context.Background(), &codec.Message{
		IngressInterface: codec.InterfaceSMPP,
		Destination:      codec.Address{MSISDN: "3342012860"},
	}, 3)
	if err != nil {
		t.Fatalf("selectRoute() error = %v", err)
	}
	if got, want := route.egressIface, "sipsimple"; got != want {
		t.Fatalf("egressIface = %q, want %q", got, want)
	}
	if got := s6cClient.calls; got != 0 {
		t.Fatalf("S6c calls = %d, want 0", got)
	}
}

func TestPersistMessageStoresBinaryMetadataForSMPPIngress(t *testing.T) {
	st := &forwarderTestStore{}
	f := &Forwarder{st: st}
	now := time.Now().UTC()

	msg := &codec.Message{
		ID:               "msg-1",
		IngressInterface: codec.InterfaceSMPP,
		Encoding:         codec.EncodingBinary,
		DCS:              0x04,
		Source:           codec.Address{MSISDN: "3342012832"},
		Destination:      codec.Address{MSISDN: "3342012860"},
		UDH:              &codec.UDH{Raw: []byte{0x0b, 0x05, 0x04, 0x0b, 0x84, 0x23, 0xf0, 0x00, 0x03, 0x42, 0x02, 0x01}},
		Binary:           []byte{0x01, 0x02, 0x03},
	}

	f.persistMessage(context.Background(), msg, "sip3gpp", "peer1", store.MessageStatusQueued, now)

	if st.saved == nil {
		t.Fatal("message was not saved")
	}
	if got, want := st.saved.Encoding, int(codec.EncodingBinary); got != want {
		t.Fatalf("saved encoding = %d, want %d", got, want)
	}
	if got, want := st.saved.DCS, 4; got != want {
		t.Fatalf("saved dcs = %d, want %d", got, want)
	}
	if string(st.saved.Payload) != string(msg.Binary) {
		t.Fatalf("saved payload = %x, want %x", st.saved.Payload, msg.Binary)
	}
	if string(st.saved.UDH) != string(msg.UDH.Raw) {
		t.Fatalf("saved UDH = %x, want %x", st.saved.UDH, msg.UDH.Raw)
	}
}

func TestPersistMessageStoresTextPayloadForRetry(t *testing.T) {
	st := &forwarderTestStore{}
	f := &Forwarder{st: st}
	now := time.Now().UTC()

	msg := &codec.Message{
		ID:               "msg-text-1",
		IngressInterface: codec.InterfaceSMPP,
		Encoding:         codec.EncodingUCS2,
		DCS:              0x08,
		Source:           codec.Address{MSISDN: "3342012832"},
		Destination:      codec.Address{MSISDN: "3342012860"},
		Text:             "hello over retry",
	}

	f.persistMessage(context.Background(), msg, "sip3gpp", "peer1", store.MessageStatusQueued, now)

	if st.saved == nil {
		t.Fatal("message was not saved")
	}
	if got, want := string(st.saved.Payload), msg.Text; got != want {
		t.Fatalf("saved payload text = %q, want %q", got, want)
	}
}

func TestStoreToCodecMessageRestoresBinaryEncodingAndUDH(t *testing.T) {
	m := store.Message{
		ID:          "msg-2",
		OriginIface: string(codec.InterfaceSMPP),
		EgressIface: string(codec.InterfaceSIP3GPP),
		SrcMSISDN:   "3342012832",
		DstMSISDN:   "3342012860",
		Payload:     []byte{0xde, 0xad},
		UDH:         []byte{0x06, 0x08, 0x04, 0x12, 0x34, 0x02, 0x01},
		Encoding:    int(codec.EncodingBinary),
		DCS:         0x04,
	}

	msg := storeToCodecMessage(m)
	if got, want := msg.Encoding, codec.EncodingBinary; got != want {
		t.Fatalf("encoding = %v, want %v", got, want)
	}
	if string(msg.Binary) != string(m.Payload) {
		t.Fatalf("binary payload = %x, want %x", msg.Binary, m.Payload)
	}
	if msg.UDH == nil || string(msg.UDH.Raw) != string(m.UDH) {
		t.Fatalf("UDH = %x, want %x", msg.UDH.Raw, m.UDH)
	}
}

func TestStoreToCodecMessageRestoresTextPayload(t *testing.T) {
	m := store.Message{
		ID:          "msg-text-2",
		OriginIface: string(codec.InterfaceSMPP),
		EgressIface: string(codec.InterfaceSIP3GPP),
		SrcMSISDN:   "3342012832",
		DstMSISDN:   "3342012860",
		Payload:     []byte("hello over retry"),
		Encoding:    int(codec.EncodingUCS2),
		DCS:         0x08,
	}

	msg := storeToCodecMessage(m)
	if got, want := msg.Encoding, codec.EncodingUCS2; got != want {
		t.Fatalf("encoding = %v, want %v", got, want)
	}
	if got, want := msg.Text, "hello over retry"; got != want {
		t.Fatalf("text = %q, want %q", got, want)
	}
	if len(msg.Binary) != 0 {
		t.Fatalf("binary payload = %x, want empty", msg.Binary)
	}
}

func TestStoreToCodecMessageRestoresAlertCorrelationID(t *testing.T) {
	m := store.Message{
		ID:                 "msg-corr-1",
		OriginIface:        string(codec.InterfaceSMPP),
		AlertCorrelationID: "corr-123",
	}

	msg := storeToCodecMessage(m)
	if got, want := msg.CorrelationID, "corr-123"; got != want {
		t.Fatalf("correlation_id = %q, want %q", got, want)
	}
}

func TestStoreToCodecMessageRestoresRemainingValidityPeriodFromExpiry(t *testing.T) {
	expiry := time.Now().Add(10 * time.Minute)
	m := store.Message{
		ID:       "msg-expiry-1",
		ExpiryAt: &expiry,
	}

	msg := storeToCodecMessage(m)
	if msg.ValidityPeriod == nil {
		t.Fatal("expected validity period to be restored from expiry")
	}
	if *msg.ValidityPeriod <= 0 {
		t.Fatalf("validity period = %v, want positive", *msg.ValidityPeriod)
	}
	if *msg.ValidityPeriod > 10*time.Minute || *msg.ValidityPeriod < 9*time.Minute {
		t.Fatalf("validity period = %v, want about 10m remaining", *msg.ValidityPeriod)
	}
}

func TestSelectRouteFallsThroughFromSGdToNextRule(t *testing.T) {
	st := &forwarderTestStore{
		sipPeers: map[string]store.SIPPeer{
			"simple1": {Name: "simple1"},
		},
	}
	reg := newTestRegistry(t, st)
	s6cClient := &fakeS6cClient{info: &s6c.RoutingInfo{Attached: false}}
	reg.SetS6cClient(s6cClient)

	engine := routing.NewEngine()
	engine.Reload([]store.RoutingRule{
		{Name: "simple-second", Priority: 20, MatchSrcIface: "smpp", MatchDstPrefix: "334", EgressIface: "sipsimple", EgressPeer: "simple1"},
	})

	f := New(Config{
		Registry:     reg,
		Engine:       engine,
		Store:        st,
		SimpleSender: &fakeSimpleSender{},
		SGdSender:    &fakeSGdSender{},
	})

	route, err := f.selectRoute(context.Background(), &codec.Message{
		IngressInterface: codec.InterfaceSMPP,
		Destination:      codec.Address{MSISDN: "3342012860"},
	}, 0)
	if err != nil {
		t.Fatalf("selectRoute() error = %v", err)
	}
	if got, want := route.egressIface, "sipsimple"; got != want {
		t.Fatalf("egressIface = %q, want %q", got, want)
	}
	if got, want := route.egressPeer, "simple1"; got != want {
		t.Fatalf("egressPeer = %q, want %q", got, want)
	}
	if got := s6cClient.calls; got != 1 {
		t.Fatalf("S6c calls = %d, want 1", got)
	}
}

func TestSelectRouteRefreshesS6cBeforeSGdWhenSubscriberCacheClaimsAttached(t *testing.T) {
	st := &forwarderTestStore{
		subscriber: &store.Subscriber{
			MSISDN:        "3342012832",
			IMSI:          "311435000070570",
			LTEAttached:   true,
			MMENumber:     "15550000001",
			MMEHost:       "stale-mme.epc.mnc435.mcc311.3gppnetwork.org",
			IMSRegistered: false,
			UpdatedAt:     time.Now().Add(-301 * time.Second),
		},
		sipPeers: map[string]store.SIPPeer{
			"simple1": {Name: "simple1"},
		},
	}
	reg := newTestRegistry(t, st)
	s6cClient := &fakeS6cClient{info: &s6c.RoutingInfo{Attached: false}}
	reg.SetS6cClient(s6cClient)

	engine := routing.NewEngine()
	engine.Reload([]store.RoutingRule{
		{Name: "simple-second", Priority: 20, MatchSrcIface: "smpp", MatchDstPrefix: "334", EgressIface: "sipsimple", EgressPeer: "simple1"},
	})

	f := New(Config{
		Registry:     reg,
		Engine:       engine,
		Store:        st,
		SimpleSender: &fakeSimpleSender{},
		SGdSender:    &fakeSGdSender{},
	})

	route, err := f.selectRoute(context.Background(), &codec.Message{
		IngressInterface: codec.InterfaceSMPP,
		Destination:      codec.Address{MSISDN: "3342012832"},
	}, 0)
	if err != nil {
		t.Fatalf("selectRoute() error = %v", err)
	}
	if got, want := route.egressIface, "sipsimple"; got != want {
		t.Fatalf("egressIface = %q, want %q", got, want)
	}
	if got := s6cClient.calls; got != 1 {
		t.Fatalf("S6c calls = %d, want 1", got)
	}
	if st.subscriber == nil {
		t.Fatal("subscriber cache was not updated")
	}
	if st.subscriber.LTEAttached {
		t.Fatalf("LTEAttached = %v, want false after S6c refresh", st.subscriber.LTEAttached)
	}
}

func TestSelectRouteUsesFreshS6cCacheForSGdCandidate(t *testing.T) {
	st := &forwarderTestStore{
		subscriber: &store.Subscriber{
			MSISDN:        "3342012832",
			IMSI:          "311435000070570",
			LTEAttached:   true,
			MMENumber:     "15550000001",
			MMEHost:       "fresh-mme.epc.mnc435.mcc311.3gppnetwork.org",
			IMSRegistered: false,
			UpdatedAt:     time.Now().Add(-299 * time.Second),
		},
	}
	reg := newTestRegistry(t, st)
	s6cClient := &fakeS6cClient{info: &s6c.RoutingInfo{Attached: false}}
	reg.SetS6cClient(s6cClient)

	engine := routing.NewEngine()

	f := New(Config{
		Registry:         reg,
		Engine:           engine,
		Store:            st,
		SGdSender:        &fakeSGdSender{},
		MaxQueueLifetime: 2 * time.Hour,
	})

	route, err := f.selectRoute(context.Background(), &codec.Message{
		IngressInterface: codec.InterfaceSMPP,
		Destination:      codec.Address{MSISDN: "3342012832"},
	}, 0)
	if err != nil {
		t.Fatalf("selectRoute() error = %v", err)
	}
	if got, want := route.egressIface, "sgd"; got != want {
		t.Fatalf("egressIface = %q, want %q", got, want)
	}
	if got, want := route.egressPeer, "fresh-mme.epc.mnc435.mcc311.3gppnetwork.org"; got != want {
		t.Fatalf("egressPeer = %q, want %q", got, want)
	}
	if got, want := route.sgdMMENum, "15550000001"; got != want {
		t.Fatalf("sgdMMENum = %q, want %q", got, want)
	}
	if got := s6cClient.calls; got != 0 {
		t.Fatalf("S6c calls = %d, want 0", got)
	}
}

func TestSelectRouteUsesFreshS6cCacheHexMMENumberForSGdCandidate(t *testing.T) {
	st := &forwarderTestStore{
		subscriber: &store.Subscriber{
			MSISDN:        "3342012832",
			IMSI:          "311435000070570",
			LTEAttached:   true,
			MMENumber:     "5155000000f1",
			MMEHost:       "fresh-mme.epc.mnc435.mcc311.3gppnetwork.org",
			IMSRegistered: false,
			UpdatedAt:     time.Now().Add(-299 * time.Second),
		},
	}
	reg := newTestRegistry(t, st)
	s6cClient := &fakeS6cClient{info: &s6c.RoutingInfo{Attached: false}}
	reg.SetS6cClient(s6cClient)

	engine := routing.NewEngine()

	sgdSender := &fakeSGdSender{}
	f := New(Config{
		Registry:  reg,
		Engine:    engine,
		Store:     st,
		SGdSender: sgdSender,
		SCAddr:    "15550000000",
	})

	msg := &codec.Message{
		IngressInterface: codec.InterfaceSMPP,
		Destination:      codec.Address{MSISDN: "3342012832"},
	}
	route, err := f.selectRoute(context.Background(), msg, 0)
	if err != nil {
		t.Fatalf("selectRoute() error = %v", err)
	}
	if got, want := route.sgdMMENum, "15550000001"; got != want {
		t.Fatalf("sgdMMENum = %q, want %q", got, want)
	}
	if got := s6cClient.calls; got != 0 {
		t.Fatalf("S6c calls = %d, want 0", got)
	}

	if err := f.deliverSelectedRoute(context.Background(), msg, route); err != nil {
		t.Fatalf("deliverSelectedRoute() error = %v", err)
	}
	if got, want := sgdSender.lastSCAddr, "15550000000"; got != want {
		t.Fatalf("SCAddress = %q, want %q", got, want)
	}
}

func TestDeliverSGdUsesConfiguredSCAddressEvenWhenMMENumberPresent(t *testing.T) {
	sgdSender := &fakeSGdSender{}
	f := &Forwarder{
		scAddr:    "15550000000",
		sgdSender: sgdSender,
	}

	msg := &codec.Message{
		Destination: codec.Address{
			MSISDN:    "3342012832",
			IMSI:      "311435000070570",
			MMENumber: "15550000001",
		},
	}

	if err := f.deliverSGd(context.Background(), msg, "mme01.example.net"); err != nil {
		t.Fatalf("deliverSGd() error = %v", err)
	}
	if got, want := sgdSender.lastSCAddr, "15550000000"; got != want {
		t.Fatalf("SCAddress = %q, want %q", got, want)
	}
	if got, want := sgdSender.lastMME, "mme01.example.net"; got != want {
		t.Fatalf("MME = %q, want %q", got, want)
	}
}

func TestDeliverSGdUsesConfiguredSCAddress(t *testing.T) {
	sgdSender := &fakeSGdSender{}
	f := &Forwarder{
		scAddr:    "15550000000",
		sgdSender: sgdSender,
	}

	msg := &codec.Message{
		Destination: codec.Address{
			MSISDN:    "3342012832",
			IMSI:      "311435000070570",
			MMENumber: "5155000000f1",
		},
	}

	if err := f.deliverSGd(context.Background(), msg, "mme01.example.net"); err != nil {
		t.Fatalf("deliverSGd() error = %v", err)
	}
	if got, want := sgdSender.lastSCAddr, "15550000000"; got != want {
		t.Fatalf("SCAddress = %q, want %q", got, want)
	}
}

func TestRetryDispatchTriesNextRuleWithinSameRetryPass(t *testing.T) {
	st := &forwarderTestStore{
		sipPeers: map[string]store.SIPPeer{
			"simple1": {Name: "simple1"},
			"simple2": {Name: "simple2"},
		},
	}
	reg := newTestRegistry(t, st)

	engine := routing.NewEngine()
	engine.Reload([]store.RoutingRule{
		{Name: "rule-1", Priority: 10, MatchSrcIface: "smpp", MatchDstPrefix: "334", EgressIface: "sipsimple", EgressPeer: "simple1"},
		{Name: "rule-2", Priority: 20, MatchSrcIface: "smpp", MatchDstPrefix: "334", EgressIface: "sipsimple", EgressPeer: "simple2"},
	})

	simpleSender := &fakeSimpleSender{errs: []error{context.DeadlineExceeded, nil}}
	f := New(Config{
		Registry:     reg,
		Engine:       engine,
		Store:        st,
		SimpleSender: simpleSender,
	})
	retries := NewRetryScheduler(f, time.Second)

	retries.dispatch(context.Background(), store.Message{
		ID:          "msg-1",
		OriginIface: string(codec.InterfaceSMPP),
		DstMSISDN:   "3342012860",
		SrcMSISDN:   "3342012832",
	})

	if got := st.retryCount; got != 0 {
		t.Fatalf("retryCount = %d, want 0", got)
	}
	if got, want := st.routeIface, "sipsimple"; got != want {
		t.Fatalf("routeIface = %q, want %q", got, want)
	}
	if got, want := st.routePeer, "simple2"; got != want {
		t.Fatalf("routePeer = %q, want %q", got, want)
	}
	if got, want := st.routingCalls, 1; got != want {
		t.Fatalf("routingCalls = %d, want %d", got, want)
	}
	if got, want := simpleSender.calls, 2; got != want {
		t.Fatalf("simple sender calls = %d, want %d", got, want)
	}
	if len(st.statusUpdates) == 0 || st.statusUpdates[len(st.statusUpdates)-1] != store.MessageStatusDelivered {
		t.Fatalf("final status = %v, want delivered", st.statusUpdates)
	}
}

func TestRetryDispatchPreservesTextPayloadForISC(t *testing.T) {
	st := &forwarderTestStore{}
	reg := newTestRegistry(t, st)
	if err := reg.Upsert(context.Background(), registry.Registration{
		MSISDN:     "3342012860",
		SIPAOR:     "sip:3342012860@example.com",
		SCSCF:      "scscf.example.com",
		Registered: true,
		Expiry:     time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("registry upsert: %v", err)
	}

	iscSender := &fakeISCSender{}
	f := New(Config{
		Registry:  reg,
		Engine:    routing.NewEngine(),
		Store:     st,
		ISCSender: iscSender,
	})
	retries := NewRetryScheduler(f, time.Second)

	retries.dispatch(context.Background(), store.Message{
		ID:          "msg-isc-1",
		OriginIface: string(codec.InterfaceSMPP),
		SrcMSISDN:   "3342012832",
		DstMSISDN:   "3342012860",
		Payload:     []byte("hello over retry"),
		Encoding:    int(codec.EncodingUCS2),
		DCS:         0x08,
	})

	if got, want := iscSender.calls, 1; got != want {
		t.Fatalf("ISC sender calls = %d, want %d", got, want)
	}
	if len(iscSender.msgs) != 1 {
		t.Fatalf("captured messages = %d, want 1", len(iscSender.msgs))
	}
	if got, want := iscSender.msgs[0].Text, "hello over retry"; got != want {
		t.Fatalf("retried text = %q, want %q", got, want)
	}
	if len(iscSender.msgs[0].Binary) != 0 {
		t.Fatalf("retried binary = %x, want empty", iscSender.msgs[0].Binary)
	}
	if len(st.statusUpdates) == 0 || st.statusUpdates[len(st.statusUpdates)-1] != store.MessageStatusDelivered {
		t.Fatalf("final status = %v, want delivered", st.statusUpdates)
	}
}

func TestRetryDispatchSkipsWhenMessageAlreadyClaimed(t *testing.T) {
	st := &forwarderTestStore{claimBlocked: true}
	reg := newTestRegistry(t, st)

	simpleSender := &fakeSimpleSender{}
	f := New(Config{
		Registry:     reg,
		Engine:       routing.NewEngine(),
		Store:        st,
		SimpleSender: simpleSender,
	})
	retries := NewRetryScheduler(f, time.Second)

	retries.dispatch(context.Background(), store.Message{
		ID:          "msg-claimed-1",
		OriginIface: string(codec.InterfaceSMPP),
		DstMSISDN:   "3342012860",
		SrcMSISDN:   "3342012832",
		Status:      store.MessageStatusWaitTimer,
	})

	if got := simpleSender.calls; got != 0 {
		t.Fatalf("simple sender calls = %d, want 0", got)
	}
	if len(st.statusUpdates) != 0 {
		t.Fatalf("status updates = %v, want none", st.statusUpdates)
	}
}

func TestDispatchMarksSGdLookupDeferralForBuiltInSGdRoute(t *testing.T) {
	st := &forwarderTestStore{}
	reg := newTestRegistry(t, st)
	s6cClient := &fakeS6cClient{info: &s6c.RoutingInfo{Attached: false}}
	reg.SetS6cClient(s6cClient)

	engine := routing.NewEngine()

	f := New(Config{
		Registry:         reg,
		Engine:           engine,
		Store:            st,
		SGdSender:        &fakeSGdSender{},
		MaxQueueLifetime: 2 * time.Hour,
	})

	f.Dispatch(context.Background(), &codec.Message{
		ID:               "msg-sgd-defer-1",
		IngressInterface: codec.InterfaceSMPP,
		Source:           codec.Address{MSISDN: "3342012832"},
		Destination:      codec.Address{MSISDN: "3342012860"},
	})

	if st.deferred == nil {
		t.Fatal("expected deferred metadata to be recorded")
	}
	if got, want := st.deferred.reason, "sgd_lookup"; got != want {
		t.Fatalf("deferred reason = %q, want %q", got, want)
	}
	if got, want := st.deferred.iface, string(codec.InterfaceSGd); got != want {
		t.Fatalf("deferred interface = %q, want %q", got, want)
	}
	if got, want := st.deferred.routeCursor, 2; got != want {
		t.Fatalf("deferred route cursor = %d, want %d", got, want)
	}
	if got, want := st.retryCount, 1; got != want {
		t.Fatalf("retryCount = %d, want %d", got, want)
	}
	if got, want := st.routeCursor, 2; got != want {
		t.Fatalf("scheduled route cursor = %d, want %d", got, want)
	}
	if st.expiryCapAt == nil {
		t.Fatal("expected expiry cap to be recorded")
	}
	if delta := st.expiryCapAt.Sub(st.saved.SubmittedAt); delta < (2*time.Hour-time.Second) || delta > (2*time.Hour+time.Second) {
		t.Fatalf("expiry cap delta = %v, want about 2h", delta)
	}
}

func TestDispatchMarksSGdAlertWaitWhenMMEReturnsUnableToDeliver(t *testing.T) {
	st := &forwarderTestStore{}
	reg := newTestRegistry(t, st)
	s6cClient := &fakeS6cClient{info: &s6c.RoutingInfo{
		Attached:  true,
		IMSI:      "311435300070599",
		MMEName:   "mme01.example.net",
		MMENumber: "15550000001",
	}}
	reg.SetS6cClient(s6cClient)

	sgdSender := &fakeSGdSender{err: &diametersgd.OFAResultError{ResultCode: dcodec.DiameterUnableToDeliver}}
	f := New(Config{
		Registry:  reg,
		Engine:    routing.NewEngine(),
		Store:     st,
		SGdSender: sgdSender,
	})

	f.Dispatch(context.Background(), &codec.Message{
		ID:               "msg-sgd-wait-1",
		IngressInterface: codec.InterfaceSMPP,
		Source:           codec.Address{MSISDN: "3342012832"},
		Destination:      codec.Address{MSISDN: "3342012860"},
	})

	if st.deferred == nil {
		t.Fatal("expected deferred metadata to be recorded")
	}
	if got, want := st.deferred.reason, "sgd_delivery"; got != want {
		t.Fatalf("deferred reason = %q, want %q", got, want)
	}
	if got, want := st.deferred.iface, string(codec.InterfaceSGd); got != want {
		t.Fatalf("deferred interface = %q, want %q", got, want)
	}
	if len(st.statusUpdates) == 0 {
		t.Fatal("expected status updates")
	}
	last := st.statusUpdates[len(st.statusUpdates)-1]
	if last != store.MessageStatusWaitTimerEvent {
		t.Fatalf("final status = %q, want %q", last, store.MessageStatusWaitTimerEvent)
	}
}

func TestDispatchUsesSFPolicyMaxTTLOverGlobalQueueLifetime(t *testing.T) {
	st := &forwarderTestStore{
		sipPeers: map[string]store.SIPPeer{
			"simple1": {Name: "simple1"},
		},
		sfPolicies: map[string]store.SFPolicy{
			"policy-1": {
				ID:     "policy-1",
				Name:   "short",
				MaxTTL: 15 * time.Minute,
			},
		},
	}
	reg := newTestRegistry(t, st)
	engine := routing.NewEngine()
	engine.Reload([]store.RoutingRule{
		{
			Name:           "simple",
			Priority:       10,
			MatchSrcIface:  "smpp",
			MatchDstPrefix: "334",
			EgressIface:    "sipsimple",
			EgressPeer:     "simple1",
			SFPolicyID:     "policy-1",
		},
	})

	f := New(Config{
		Registry:         reg,
		Engine:           engine,
		Store:            st,
		SimpleSender:     &fakeSimpleSender{err: context.DeadlineExceeded},
		MaxQueueLifetime: 7 * 24 * time.Hour,
	})

	f.Dispatch(context.Background(), &codec.Message{
		ID:               "msg-policy-ttl-1",
		IngressInterface: codec.InterfaceSMPP,
		Source:           codec.Address{MSISDN: "3342012832"},
		Destination:      codec.Address{MSISDN: "3342012860"},
	})

	if st.expiryCapAt == nil {
		t.Fatal("expected expiry cap to be recorded")
	}
	if delta := st.expiryCapAt.Sub(st.saved.SubmittedAt); delta < (15*time.Minute-time.Second) || delta > (15*time.Minute+time.Second) {
		t.Fatalf("expiry cap delta = %v, want about 15m", delta)
	}
}

func TestDispatchAppliesSFPolicyVPOverrideToFallbackSend(t *testing.T) {
	st := &forwarderTestStore{
		sipPeers: map[string]store.SIPPeer{
			"simple1": {Name: "simple1"},
		},
		sfPolicies: map[string]store.SFPolicy{
			"policy-1": {
				ID:         "policy-1",
				Name:       "vp-short",
				MaxTTL:     48 * time.Hour,
				VPOverride: durationPtr(30 * time.Minute),
			},
		},
	}
	reg := newTestRegistry(t, st)
	engine := routing.NewEngine()
	engine.Reload([]store.RoutingRule{
		{
			Name:           "simple",
			Priority:       10,
			MatchSrcIface:  "smpp",
			MatchDstPrefix: "334",
			EgressIface:    "sipsimple",
			EgressPeer:     "simple1",
			SFPolicyID:     "policy-1",
		},
	})

	simpleSender := &fakeSimpleSender{}
	f := New(Config{
		Registry:         reg,
		Engine:           engine,
		Store:            st,
		SimpleSender:     simpleSender,
		MaxQueueLifetime: 7 * 24 * time.Hour,
	})

	originalVP := 2 * time.Hour
	f.Dispatch(context.Background(), &codec.Message{
		ID:               "msg-vp-override-1",
		IngressInterface: codec.InterfaceSMPP,
		Source:           codec.Address{MSISDN: "3342012832"},
		Destination:      codec.Address{MSISDN: "3342012860"},
		ValidityPeriod:   &originalVP,
	})

	if got, want := simpleSender.calls, 1; got != want {
		t.Fatalf("simple sender calls = %d, want %d", got, want)
	}
	if len(simpleSender.msgs) != 1 || simpleSender.msgs[0].ValidityPeriod == nil {
		t.Fatal("expected captured message validity period")
	}
	got := *simpleSender.msgs[0].ValidityPeriod
	if got < (30*time.Minute-time.Second) || got > (30*time.Minute+time.Second) {
		t.Fatalf("captured validity period = %v, want about 30m", got)
	}
}

func TestDispatchPersistsTPMRForTPDUOrigins(t *testing.T) {
	cases := []struct {
		name    string
		ingress codec.InterfaceType
		want    *int
	}{
		{name: "sgd", ingress: codec.InterfaceSGd, want: intPtrForTest(0x2A)},
		{name: "sip3gpp", ingress: codec.InterfaceSIP3GPP, want: intPtrForTest(0x2A)},
		{name: "smpp", ingress: codec.InterfaceSMPP, want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &forwarderTestStore{}
			reg := newTestRegistry(t, st)
			reg.SetS6cClient(&fakeS6cClient{info: &s6c.RoutingInfo{Attached: false}})

			f := New(Config{
				Registry:         reg,
				Engine:           routing.NewEngine(),
				Store:            st,
				SGdSender:        &fakeSGdSender{},
				MaxQueueLifetime: time.Hour,
			})

			f.Dispatch(context.Background(), &codec.Message{
				ID:               "msg-tpmr-" + tc.name,
				IngressInterface: tc.ingress,
				TPMR:             0x2A,
				Source:           codec.Address{MSISDN: "989100000555"},
				Destination:      codec.Address{MSISDN: "989100000111"},
			})

			if st.saved == nil {
				t.Fatal("expected message to be persisted")
			}
			switch {
			case tc.want == nil && st.saved.TPMR != nil:
				t.Fatalf("TPMR = %d, want nil", *st.saved.TPMR)
			case tc.want != nil && st.saved.TPMR == nil:
				t.Fatalf("TPMR = nil, want %d", *tc.want)
			case tc.want != nil && *st.saved.TPMR != *tc.want:
				t.Fatalf("TPMR = %d, want %d", *st.saved.TPMR, *tc.want)
			}
		})
	}
}

func TestStoreToCodecMessageRestoresTPMR(t *testing.T) {
	msg := storeToCodecMessage(store.Message{ID: "m1", TPMR: intPtrForTest(0x2A)})
	if got, want := msg.TPMR, byte(0x2A); got != want {
		t.Fatalf("TPMR = 0x%02X, want 0x%02X", got, want)
	}
}

func intPtrForTest(v int) *int { return &v }

func TestDispatchPersistsMessageKindFields(t *testing.T) {
	st := &forwarderTestStore{}
	reg := newTestRegistry(t, st)
	reg.SetS6cClient(&fakeS6cClient{info: &s6c.RoutingInfo{Attached: false}})
	f := New(Config{
		Registry:         reg,
		Engine:           routing.NewEngine(),
		Store:            st,
		SGdSender:        &fakeSGdSender{},
		MaxQueueLifetime: time.Hour,
	})

	raw := []byte{0x06, 0x2A}
	in := &codec.Message{
		ID:           "sr-1",
		Kind:         codec.MessageKindStatusReport,
		RefMessageID: "orig-1",
		RawTPDU:      raw,
		Destination:  codec.Address{MSISDN: "989100000555"},
	}
	f.Dispatch(context.Background(), in)

	if st.saved == nil {
		t.Fatal("expected message to be persisted")
	}
	if st.saved.Kind != store.MessageKindStatusReport || st.saved.RefMessageID != "orig-1" || !bytes.Equal(st.saved.RawTPDU, raw) {
		t.Fatalf("saved = kind %q ref %q raw % X", st.saved.Kind, st.saved.RefMessageID, st.saved.RawTPDU)
	}
	in.RawTPDU[0] = 0xFF
	if st.saved.RawTPDU[0] != 0x06 {
		t.Fatal("persisted RawTPDU aliases the caller's slice")
	}

	st = &forwarderTestStore{}
	reg = newTestRegistry(t, st)
	reg.SetS6cClient(&fakeS6cClient{info: &s6c.RoutingInfo{Attached: false}})
	f = New(Config{Registry: reg, Engine: routing.NewEngine(), Store: st, SGdSender: &fakeSGdSender{}, MaxQueueLifetime: time.Hour})
	f.Dispatch(context.Background(), &codec.Message{ID: "sm-1", Destination: codec.Address{MSISDN: "989100000111"}})
	if st.saved.Kind != store.MessageKindSM {
		t.Fatalf("default Kind = %q, want %q", st.saved.Kind, store.MessageKindSM)
	}
}

func TestStoreToCodecMessageRestoresKindFields(t *testing.T) {
	raw := []byte{0x06, 0x2A}
	msg := storeToCodecMessage(store.Message{
		ID: "sr-1", Kind: store.MessageKindStatusReport, RefMessageID: "orig-1", RawTPDU: raw,
	})
	if msg.Kind != codec.MessageKindStatusReport || msg.RefMessageID != "orig-1" || !bytes.Equal(msg.RawTPDU, raw) {
		t.Fatalf("restored = kind %q ref %q raw % X", msg.Kind, msg.RefMessageID, msg.RawTPDU)
	}

	if got := storeToCodecMessage(store.Message{ID: "legacy"}).Kind; got != codec.MessageKindSM {
		t.Fatalf("legacy Kind = %q, want %q", got, codec.MessageKindSM)
	}
}

func TestSelectRouteNeverUsesFallbackRulesForStatusReport(t *testing.T) {
	newForwarder := func() (*Forwarder, *fakeS6cClient) {
		st := &forwarderTestStore{sipPeers: map[string]store.SIPPeer{"simple1": {Name: "simple1"}}}
		reg := newTestRegistry(t, st)
		s6cClient := &fakeS6cClient{info: &s6c.RoutingInfo{Attached: false}}
		reg.SetS6cClient(s6cClient)
		engine := routing.NewEngine()
		engine.Reload([]store.RoutingRule{
			{Name: "simple-any", Priority: 10, MatchDstPrefix: "989", EgressIface: "sipsimple", EgressPeer: "simple1"},
		})
		return New(Config{
			Registry:     reg,
			Engine:       engine,
			Store:        st,
			SimpleSender: &fakeSimpleSender{},
			SGdSender:    &fakeSGdSender{},
		}), s6cClient
	}

	// Control: an ordinary SM to the same unattached subscriber falls back to SIP SIMPLE.
	f, _ := newForwarder()
	route, err := f.selectRoute(context.Background(), &codec.Message{
		Destination: codec.Address{MSISDN: "989100000555"},
	}, 0)
	if err != nil || route == nil || route.egressIface != "sipsimple" {
		t.Fatalf("SM selectRoute() = %+v, %v; want sipsimple", route, err)
	}

	// A status report must not use the fallback rule.
	f, _ = newForwarder()
	route, err = f.selectRoute(context.Background(), &codec.Message{
		Kind:        codec.MessageKindStatusReport,
		RawTPDU:     []byte{0x06, 0x2A},
		Destination: codec.Address{MSISDN: "989100000555"},
	}, 0)
	if err == nil {
		t.Fatalf("status report selectRoute() = %+v, want no usable route", route)
	}
}

func TestDispatchSendsStatusReportRawTPDUViaSGd(t *testing.T) {
	st := &forwarderTestStore{}
	reg := newTestRegistry(t, st)
	reg.SetS6cClient(&fakeS6cClient{info: &s6c.RoutingInfo{
		IMSI:      "432600000000555",
		Attached:  true,
		MMEName:   "mme-a.epc.example.net",
		MMENumber: "15550000001",
	}})
	sgd := &fakeSGdSender{}
	f := New(Config{
		Registry:         reg,
		Engine:           routing.NewEngine(),
		Store:            st,
		SGdSender:        sgd,
		MaxQueueLifetime: time.Hour,
	})

	raw := []byte{0x06, 0x2A, 0x0C, 0x91, 0x89, 0x19, 0x00, 0x00, 0x10, 0x11}
	f.Dispatch(context.Background(), &codec.Message{
		ID:           "sr-1",
		Kind:         codec.MessageKindStatusReport,
		RefMessageID: "orig-1",
		RawTPDU:      raw,
		Destination:  codec.Address{MSISDN: "989100000555"},
	})

	if sgd.lastMsg == nil {
		t.Fatal("expected status report to be sent via SGd")
	}
	if got, want := sgd.lastMME, "mme-a.epc.example.net"; got != want {
		t.Fatalf("MME = %q, want %q", got, want)
	}
	if got, want := sgd.lastMsg.Destination.IMSI, "432600000000555"; got != want {
		t.Fatalf("Destination.IMSI = %q, want %q (the original sender)", got, want)
	}
	if !bytes.Equal(sgd.lastMsg.RawTPDU, raw) {
		t.Fatalf("RawTPDU = % X, want % X", sgd.lastMsg.RawTPDU, raw)
	}
}

func TestDispatchDoesNotRenormalizeStatusReportDestination(t *testing.T) {
	st := &forwarderTestStore{}
	reg := newTestRegistry(t, st)
	reg.SetS6cClient(&fakeS6cClient{info: &s6c.RoutingInfo{Attached: false}})
	f := New(Config{
		Registry:            reg,
		Engine:              routing.NewEngine(),
		Store:               st,
		SGdSender:           &fakeSGdSender{},
		MaxQueueLifetime:    time.Hour,
		DefaultCountryCode:  "98",
		LocalNationalLength: 12, // same length as the sender's international number
	})

	f.Dispatch(context.Background(), &codec.Message{
		ID:               "sr-1",
		Kind:             codec.MessageKindStatusReport,
		IngressInterface: codec.InterfaceInternal,
		RawTPDU:          []byte{0x06, 0x2A},
		Destination:      codec.Address{MSISDN: "989100000555", TON: 0x01, NPI: 0x01},
	})

	if got, want := st.saved.DstMSISDN, "989100000555"; got != want {
		t.Fatalf("DstMSISDN = %q, want %q", got, want)
	}
	if got, want := st.saved.OriginIface, "internal"; got != want {
		t.Fatalf("OriginIface = %q, want %q", got, want)
	}
	if st.saved.TPMR != nil {
		t.Fatalf("TPMR = %d, want nil for an internal message", *st.saved.TPMR)
	}
}

// An HSS failure on SRI-SM (anything but user-unknown) must not fall back to a
// stale cached serving MME; the SGd candidate is unavailable until S6c works.
func TestSelectRouteDoesNotUseStaleMMEWhenS6cFails(t *testing.T) {
	st := &forwarderTestStore{
		subscriber: &store.Subscriber{
			MSISDN:      "989100000111",
			IMSI:        "432600000000111",
			LTEAttached: true,
			MMEHost:     "stale-mme.epc.example.net",
			MMENumber:   "15550000002",
			UpdatedAt:   time.Now().Add(-time.Hour), // older than the S6c cache TTL
		},
	}
	reg := newTestRegistry(t, st)
	reg.SetS6cClient(&fakeS6cClient{err: &s6c.ErrResult{Command: "SRI-SM", ResultCode: dcodec.DiameterUnableToComply}})
	sgd := &fakeSGdSender{}
	f := New(Config{Registry: reg, Engine: routing.NewEngine(), Store: st, SGdSender: sgd})

	route, err := f.selectRoute(context.Background(), &codec.Message{
		Destination: codec.Address{MSISDN: "989100000111"},
	}, 2) // sgd-built-in
	if err == nil {
		t.Fatalf("selectRoute() = %+v, want S6c lookup failure", route)
	}
	if !strings.Contains(err.Error(), "S6c lookup failed") {
		t.Fatalf("selectRoute() error = %v, want S6c lookup failure", err)
	}
}

func TestClassifySGdResult(t *testing.T) {
	cause := func(v uint32) *uint32 { return &v }
	cases := []struct {
		name string
		err  *diametersgd.OFAResultError
		want routeOutcome
	}{
		{"base 3002 absent", &diametersgd.OFAResultError{ResultCode: dcodec.DiameterUnableToDeliver}, routeOutcomeWaitEvent},
		{"base 3004 busy", &diametersgd.OFAResultError{ResultCode: dcodec.DiameterTooBusy}, routeOutcomeBusy},
		{"base 5012", &diametersgd.OFAResultError{ResultCode: dcodec.DiameterUnableToComply}, routeOutcomeWaitTimer},
		{"5550 absent", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorAbsentUser, Experimental: true}, routeOutcomeWaitEvent},
		{"5551 busy", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorUserBusyForMTSMS, Experimental: true}, routeOutcomeBusy},
		{"5552 facility", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorFacilityNotSupported, Experimental: true}, routeOutcomeWaitTimer},
		{"5553 illegal user", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorIllegalUser, Experimental: true}, routeOutcomeRejected},
		{"5554 illegal equipment", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorIllegalEquipment, Experimental: true}, routeOutcomeRejected},
		{"5555 no cause", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorSMDeliveryFailure, Experimental: true}, routeOutcomeWaitTimer},
		{"5555 memory full", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorSMDeliveryFailure, Experimental: true, DeliveryFailureCause: cause(diametersgd.DeliveryFailureMemoryCapacityExceeded)}, routeOutcomeWaitEvent},
		{"5555 congestion", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorSMDeliveryFailure, Experimental: true, DeliveryFailureCause: cause(diametersgd.DeliveryFailureSCCongestion)}, routeOutcomeWaitTimer},
		{"5555 protocol error", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorSMDeliveryFailure, Experimental: true, DeliveryFailureCause: cause(diametersgd.DeliveryFailureEquipmentProtocolError)}, routeOutcomeRejected},
		{"5555 not SM equipped", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorSMDeliveryFailure, Experimental: true, DeliveryFailureCause: cause(diametersgd.DeliveryFailureEquipmentNotSMEquipped)}, routeOutcomeRejected},
		{"5555 unknown cause value", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorSMDeliveryFailure, Experimental: true, DeliveryFailureCause: cause(99)}, routeOutcomeWaitTimer},
		// Base 5550 is not the 3GPP absent-user code.
		{"base 5550", &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorAbsentUser}, routeOutcomeWaitTimer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifySGdResult(tc.err); got != tc.want {
				t.Fatalf("classifySGdResult() = %d, want %d", got, tc.want)
			}
		})
	}
}

type captureReporter struct {
	mu       sync.Mutex
	reports  []string
	deferred []uint32
}

func (r *captureReporter) Report(_ context.Context, _ store.Message, status string) {
	r.mu.Lock()
	r.reports = append(r.reports, status)
	r.mu.Unlock()
}

func (r *captureReporter) ReportDeferred(_ context.Context, _ store.Message, cause uint32) {
	r.mu.Lock()
	r.deferred = append(r.deferred, cause)
	r.mu.Unlock()
}

func dispatchWithSGdError(t *testing.T, sgdErr error) (*forwarderTestStore, *captureReporter) {
	t.Helper()
	st := &forwarderTestStore{}
	reg := newTestRegistry(t, st)
	reg.SetS6cClient(&fakeS6cClient{info: &s6c.RoutingInfo{
		IMSI: "432600000000111", Attached: true, MMEName: "mme-b.epc.example.net", MMENumber: "15550000002",
	}})
	rep := &captureReporter{}
	f := New(Config{
		Registry:         reg,
		Engine:           routing.NewEngine(),
		Store:            st,
		SGdSender:        &fakeSGdSender{err: sgdErr},
		Reporter:         rep,
		MaxQueueLifetime: time.Hour,
	})
	f.Dispatch(context.Background(), &codec.Message{
		ID:               "m1",
		IngressInterface: codec.InterfaceSGd,
		Source:           codec.Address{MSISDN: "989100000555"},
		Destination:      codec.Address{MSISDN: "989100000111", TON: 0x01},
	})
	return st, rep
}

func TestDispatchSGdBusyRetriesSoonWithoutUsingBudget(t *testing.T) {
	before := time.Now()
	st, rep := dispatchWithSGdError(t, &diametersgd.OFAResultError{ResultCode: diametersgd.ResultErrorUserBusyForMTSMS, Experimental: true})

	if st.nextRetryAt == nil {
		t.Fatal("busy answer scheduled no retry")
	}
	if d := st.nextRetryAt.Sub(before); d < sgdBusyRetryDelay-time.Second || d > sgdBusyRetryDelay+time.Second {
		t.Fatalf("next retry in %v, want about %v", d, sgdBusyRetryDelay)
	}
	if st.retryCount != 0 {
		t.Fatalf("retryCount = %d, want 0 (busy doesn't consume the retry budget)", st.retryCount)
	}
	if len(rep.reports) != 0 || len(rep.deferred) != 0 {
		t.Fatalf("reports = %v, deferred = %v; want none for busy", rep.reports, rep.deferred)
	}
}

func TestDispatchSGdRejectionFailsImmediately(t *testing.T) {
	cause := diametersgd.DeliveryFailureEquipmentProtocolError
	st, rep := dispatchWithSGdError(t, &diametersgd.OFAResultError{
		ResultCode: diametersgd.ResultErrorSMDeliveryFailure, Experimental: true, DeliveryFailureCause: &cause,
	})

	// The IMS candidates before SGd were unavailable (would normally mean
	// "retry later"), but a handset rejection is final.
	if got := st.statusUpdates[len(st.statusUpdates)-1]; got != store.MessageStatusFailed {
		t.Fatalf("final status = %s, want FAILED", got)
	}
	if st.nextRetryAt != nil {
		t.Fatalf("rejected message scheduled a retry at %v", st.nextRetryAt)
	}
	if fmt.Sprint(rep.reports) != "[FAILED]" {
		t.Fatalf("reports = %v, want [FAILED]", rep.reports)
	}
}

func TestDispatchSGdMemoryFullReportsMCEF(t *testing.T) {
	cause := diametersgd.DeliveryFailureMemoryCapacityExceeded
	st, rep := dispatchWithSGdError(t, &diametersgd.OFAResultError{
		ResultCode: diametersgd.ResultErrorSMDeliveryFailure, Experimental: true, DeliveryFailureCause: &cause,
	})

	if got := st.statusUpdates[len(st.statusUpdates)-1]; got != store.MessageStatusWaitTimerEvent {
		t.Fatalf("final status = %s, want WAIT_TIMER_EVENT", got)
	}
	if fmt.Sprint(rep.deferred) != fmt.Sprint([]uint32{dcodec.SMDeliveryCauseMemoryCapacityExceeded}) {
		t.Fatalf("deferred RSDS causes = %v, want [memory capacity exceeded]", rep.deferred)
	}
}

type concurrencySGdSender struct {
	mu        sync.Mutex
	active    map[string]int
	maxActive map[string]int
	total     int
	maxTotal  int
}

func (s *concurrencySGdSender) SendOFR(_ context.Context, msg *codec.Message, _, _ string) error {
	key := msg.Destination.IMSI
	s.mu.Lock()
	if s.active == nil {
		s.active, s.maxActive = map[string]int{}, map[string]int{}
	}
	s.active[key]++
	s.total++
	if s.active[key] > s.maxActive[key] {
		s.maxActive[key] = s.active[key]
	}
	if s.total > s.maxTotal {
		s.maxTotal = s.total
	}
	s.mu.Unlock()

	time.Sleep(30 * time.Millisecond)

	s.mu.Lock()
	s.active[key]--
	s.total--
	s.mu.Unlock()
	return nil
}

func TestDeliverSGdSerialisesPerDestination(t *testing.T) {
	sender := &concurrencySGdSender{}
	f := New(Config{SGdSender: sender})

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		for _, imsi := range []string{"432600000000111", "432600000000555"} {
			wg.Add(1)
			go func(imsi string) {
				defer wg.Done()
				msg := &codec.Message{Destination: codec.Address{IMSI: imsi}}
				if err := f.deliverSGd(context.Background(), msg, "mme"); err != nil {
					t.Errorf("deliverSGd() error = %v", err)
				}
			}(imsi)
		}
	}
	wg.Wait()

	for imsi, max := range sender.maxActive {
		if max != 1 {
			t.Errorf("max concurrent MT to %s = %d, want 1", imsi, max)
		}
	}
	if sender.maxTotal < 2 {
		t.Errorf("max concurrent MT overall = %d, want different destinations in parallel", sender.maxTotal)
	}
}

// The HSS answering SRI-SM with user-unknown is definitive: the message fails
// at once (no retries, no SGd attempt) instead of waiting out its lifetime,
// and a stale cached serving MME is never used.
func TestDispatchFailsWhenHSSReportsUnknownSubscriber(t *testing.T) {
	st := &forwarderTestStore{
		subscriber: &store.Subscriber{
			MSISDN:      "989100000111",
			IMSI:        "432600000000111",
			LTEAttached: true,
			MMEHost:     "stale-mme.epc.example.net",
			MMENumber:   "15550000002",
			UpdatedAt:   time.Now().Add(-time.Hour), // older than the S6c cache TTL
		},
	}
	reg := newTestRegistry(t, st)
	reg.SetS6cClient(&fakeS6cClient{err: &s6c.ErrUnknownUser{}})
	sgd := &fakeSGdSender{}
	rep := &captureReporter{}
	f := New(Config{
		Registry:         reg,
		Engine:           routing.NewEngine(),
		Store:            st,
		SGdSender:        sgd,
		Reporter:         rep,
		MaxQueueLifetime: time.Hour,
	})
	f.Dispatch(context.Background(), &codec.Message{
		ID:               "m-unknown",
		IngressInterface: codec.InterfaceSMPP,
		Source:           codec.Address{MSISDN: "3342012832"},
		Destination:      codec.Address{MSISDN: "989100000111"},
	})

	if sgd.lastMsg != nil {
		t.Fatalf("SGd send to %q, want none for an unknown subscriber", sgd.lastMME)
	}
	if n := len(st.statusUpdates); n == 0 || st.statusUpdates[n-1] != store.MessageStatusFailed {
		t.Fatalf("status updates = %v, want final FAILED", st.statusUpdates)
	}
	if st.nextRetryAt != nil || st.deferred != nil {
		t.Fatalf("retry at %v / deferred %+v, want neither", st.nextRetryAt, st.deferred)
	}
	if len(rep.reports) != 1 || rep.reports[0] != "FAILED" {
		t.Fatalf("reports = %v, want [FAILED]", rep.reports)
	}
}

// User-unknown only rules out the SGd route: a matching fallback rule (e.g.
// SMPP/SIP SIMPLE towards another network) is still tried.
func TestDispatchUnknownSubscriberFallsBackToNextRoute(t *testing.T) {
	st := &forwarderTestStore{
		sipPeers: map[string]store.SIPPeer{"simple1": {Name: "simple1"}},
	}
	reg := newTestRegistry(t, st)
	reg.SetS6cClient(&fakeS6cClient{err: &s6c.ErrUnknownUser{}})
	engine := routing.NewEngine()
	engine.Reload([]store.RoutingRule{{
		Name:           "offnet",
		Priority:       10,
		MatchSrcIface:  "smpp",
		MatchDstPrefix: "334",
		EgressIface:    "sipsimple",
		EgressPeer:     "simple1",
	}})
	sgd := &fakeSGdSender{}
	simple := &fakeSimpleSender{}
	f := New(Config{
		Registry:         reg,
		Engine:           engine,
		Store:            st,
		SGdSender:        sgd,
		SimpleSender:     simple,
		MaxQueueLifetime: time.Hour,
	})
	f.Dispatch(context.Background(), &codec.Message{
		ID:               "m-offnet",
		IngressInterface: codec.InterfaceSMPP,
		Source:           codec.Address{MSISDN: "3342012832"},
		Destination:      codec.Address{MSISDN: "3342012860"},
	})

	if sgd.lastMsg != nil {
		t.Fatal("unexpected SGd send")
	}
	if simple.calls != 1 {
		t.Fatalf("SIP SIMPLE sends = %d, want 1", simple.calls)
	}
	if n := len(st.statusUpdates); n == 0 || st.statusUpdates[n-1] != store.MessageStatusDelivered {
		t.Fatalf("status updates = %v, want final DELIVERED", st.statusUpdates)
	}
}
