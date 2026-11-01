// Package e2e runs the SMSC's SGd, forwarding and delivery-report pipeline
// end to end against fake MMEs (real Diameter peers over loopback TCP) and
// a fake HSS, using a real SQLite store.
package e2e

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/sip3gpp"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
	s6cdiam "github.com/svinson1121/vectorcore-smsc/internal/diameter/s6c"
	sgdsrv "github.com/svinson1121/vectorcore-smsc/internal/diameter/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/dr"
	"github.com/svinson1121/vectorcore-smsc/internal/forwarder"
	"github.com/svinson1121/vectorcore-smsc/internal/registry"
	"github.com/svinson1121/vectorcore-smsc/internal/routing"
	"github.com/svinson1121/vectorcore-smsc/internal/store"
	"github.com/svinson1121/vectorcore-smsc/internal/store/sqlite"
)

const (
	smscFQDN  = "smsc.example.net"
	smscRealm = "example.net"
	scAddr    = "+15550000000"

	mmeRealm = "epc.example.net"
	mmeAHost = "mme-a.epc.example.net" // serves the sender
	mmeBHost = "mme-b.epc.example.net" // serves the destination

	senderIMSI   = "432600000000555"
	senderMSISDN = "989100000555"
	destIMSI     = "432600000000111"
	destMSISDN   = "989100000111"

	waitTimeout = 10 * time.Second
)

// ── Fake HSS (S6c) ────────────────────────────────────────────────────────────

type fakeHSS struct {
	mu      sync.Mutex
	subs    map[string]*s6cdiam.RoutingInfo
	lookups map[string]int
	rsds    map[string][]uint32 // msisdn → SM-Delivery-Cause of each RSDS
}

func newFakeHSS() *fakeHSS {
	return &fakeHSS{
		subs: map[string]*s6cdiam.RoutingInfo{
			senderMSISDN: {IMSI: senderIMSI, Attached: true, MMEName: mmeAHost, MMENumber: "15550000001"},
			destMSISDN:   {IMSI: destIMSI, Attached: true, MMEName: mmeBHost, MMENumber: "15550000002"},
		},
		lookups: map[string]int{},
		rsds:    map[string][]uint32{},
	}
}

func (h *fakeHSS) LookupRouting(msisdn string) (*s6cdiam.RoutingInfo, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lookups[msisdn]++
	info, ok := h.subs[msisdn]
	if !ok {
		return nil, &s6cdiam.ErrUnknownUser{}
	}
	cp := *info
	return &cp, nil
}

func (h *fakeHSS) ReportDelivery(msisdn, _, _, _ string, cause uint32, _ *uint32) (*s6cdiam.ReportDeliveryResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rsds[msisdn] = append(h.rsds[msisdn], cause)
	return &s6cdiam.ReportDeliveryResult{ResultCode: dcodec.DiameterSuccess}, nil
}

// remove makes the HSS answer user-unknown for msisdn from now on.
func (h *fakeHSS) remove(msisdn string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subs, msisdn)
}

func (h *fakeHSS) lookupCount(msisdn string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lookups[msisdn]
}

func (h *fakeHSS) rsdsCauses(msisdn string) []uint32 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]uint32(nil), h.rsds[msisdn]...)
}

// ── Fake MME ──────────────────────────────────────────────────────────────────

// fakeMME is a real Diameter SGd peer that connects to the SMSC, sends MO
// requests, and answers every MT-Forward-Short-Message-Request (TFR) with a
// configurable result code.
type fakeMME struct {
	t    *testing.T
	host string
	peer *diameter.Peer

	mu         sync.Mutex
	resultCode uint32
	tfrs       chan *dcodec.Message
	moAnswers  chan *dcodec.Message

	// Optional realism (see setRealistic / setExperimental).
	experimental bool    // send resultCode as Experimental-Result (vendor 10415)
	failureCause *uint32 // SM-Enumerated-Delivery-Failure-Cause to include
	answerDelay  time.Duration
	oneMTPerIMSI bool // like vectorcore-mme: a second concurrent TFR for an IMSI gets 3004
	inFlight     map[string]bool
	busyAnswers  int
	legacyMSISDN bool // send MSISDN as len|0x91|TBCD like vectorcore-mme today
}

func startFakeMME(ctx context.Context, t *testing.T, host string, port int) *fakeMME {
	t.Helper()
	m := &fakeMME{
		t:          t,
		host:       host,
		resultCode: dcodec.DiameterSuccess,
		tfrs:       make(chan *dcodec.Message, 16),
		moAnswers:  make(chan *dcodec.Message, 16),
	}
	m.peer = diameter.NewPeer(diameter.Config{
		Name:        host,
		Host:        "127.0.0.1",
		Port:        port,
		Transport:   "tcp",
		Application: "sgd",
		AppID:       dcodec.App3GPP_SGd,
		LocalFQDN:   host,
		LocalRealm:  mmeRealm,
		PeerFQDN:    smscFQDN,
		PeerRealm:   smscRealm,
	})
	m.peer.AddOnMessageHandler(func(p *diameter.Peer, msg *dcodec.Message) {
		switch {
		case msg.Header.CommandCode == dcodec.CmdMTForwardShortMessage && msg.IsRequest():
			imsi := ""
			if a := msg.FindAVP(dcodec.CodeUserName, 0); a != nil {
				imsi = string(a.Data)
			}
			m.mu.Lock()
			rc, experimental, failureCause, delay := m.resultCode, m.experimental, m.failureCause, m.answerDelay
			busy := m.oneMTPerIMSI && m.inFlight[imsi]
			if busy {
				m.busyAnswers++
			} else if m.oneMTPerIMSI {
				m.inFlight[imsi] = true
			}
			m.mu.Unlock()
			m.tfrs <- msg
			if busy {
				m.sendTFA(p, msg, dcodec.DiameterTooBusy, false, nil)
				return
			}
			if rc == 0 {
				return // silent MME: never answers
			}
			answer := func() {
				m.sendTFA(p, msg, rc, experimental, failureCause)
				m.mu.Lock()
				delete(m.inFlight, imsi)
				m.mu.Unlock()
			}
			if delay > 0 {
				go func() { time.Sleep(delay); answer() }()
			} else {
				answer()
			}
		case msg.Header.CommandCode == dcodec.CmdMOForwardShortMessage && !msg.IsRequest():
			m.moAnswers <- msg
		}
	})
	m.peer.Start(ctx)
	t.Cleanup(m.peer.Stop)
	return m
}

// sendTFA answers an MT-Forward request.
func (m *fakeMME) sendTFA(p *diameter.Peer, req *dcodec.Message, rc uint32, experimental bool, failureCause *uint32) {
	ans := dcodec.NewAnswer(req)
	if sid := req.FindAVP(dcodec.CodeSessionID, 0); sid != nil {
		ans.Add(dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, string(sid.Data)))
	}
	if experimental {
		exp, err := dcodec.NewGrouped(dcodec.CodeExperimentalResult, 0, dcodec.FlagMandatory, []*dcodec.AVP{
			dcodec.NewUint32(dcodec.CodeVendorID, 0, dcodec.FlagMandatory, dcodec.Vendor3GPP),
			dcodec.NewUint32(dcodec.CodeExperimentalResultCode, 0, dcodec.FlagMandatory, rc),
		})
		if err != nil {
			m.t.Errorf("%s: build Experimental-Result: %v", m.host, err)
			return
		}
		ans.Add(exp)
	} else {
		ans.Add(dcodec.NewUint32(dcodec.CodeResultCode, 0, dcodec.FlagMandatory, rc))
	}
	if failureCause != nil {
		cause, err := dcodec.NewGrouped(dcodec.CodeSMDeliveryFailureCause, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, []*dcodec.AVP{
			dcodec.NewUint32(dcodec.CodeSMEnumeratedDeliveryFailureCause, dcodec.Vendor3GPP, dcodec.FlagMandatory|dcodec.FlagVendorSpecific, *failureCause),
		})
		if err != nil {
			m.t.Errorf("%s: build SM-Delivery-Failure-Cause: %v", m.host, err)
			return
		}
		ans.Add(cause)
	}
	ans.Add(
		dcodec.NewUint32(dcodec.CodeAuthSessionState, 0, dcodec.FlagMandatory, dcodec.AuthSessionStateNoStateMaintained),
		dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, m.host),
		dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, mmeRealm),
	)
	if err := p.Send(ans.Build()); err != nil {
		m.t.Errorf("%s: send TFA: %v", m.host, err)
	}
}

// setRealistic makes the MME behave like vectorcore-mme today: one MT
// transaction per IMSI (a concurrent TFR gets 3004) and answers that take
// delay, so transactions really overlap.
func (m *fakeMME) setRealistic(delay time.Duration) {
	m.mu.Lock()
	m.oneMTPerIMSI = true
	m.inFlight = map[string]bool{}
	m.answerDelay = delay
	m.mu.Unlock()
}

// setExperimental makes TFAs carry rc as a TS 29.338 Experimental-Result,
// optionally with SM-Delivery-Failure-Cause.
func (m *fakeMME) setExperimental(rc uint32, failureCause *uint32) {
	m.mu.Lock()
	m.resultCode, m.experimental, m.failureCause = rc, true, failureCause
	m.mu.Unlock()
}

func (m *fakeMME) busyCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.busyAnswers
}

// setResultCode sets the TFA Result-Code; 0 means never answer.
func (m *fakeMME) setResultCode(rc uint32) {
	m.mu.Lock()
	m.resultCode = rc
	m.mu.Unlock()
}

// sendMO sends an MO-Forward-Short-Message-Request carrying an SMS-SUBMIT
// from the sender UE and waits for the SMSC's answer.
func (m *fakeMME) sendMO(t *testing.T, submit *codec.Message) {
	t.Helper()
	tpData, err := tpdu.EncodeSubmit(submit)
	if err != nil {
		t.Fatalf("EncodeSubmit() error = %v", err)
	}
	uid, err := dcodec.NewGrouped(dcodec.CodeUserIdentifier, dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewString(dcodec.CodeUserName, 0, dcodec.FlagMandatory, senderIMSI),
			dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP,
				dcodec.FlagMandatory|dcodec.FlagVendorSpecific, m.encodeMSISDN(senderMSISDN)),
		})
	if err != nil {
		t.Fatalf("NewGrouped() error = %v", err)
	}
	req := dcodec.NewRequest(dcodec.CmdMOForwardShortMessage, dcodec.App3GPP_SGd).Add(
		dcodec.NewString(dcodec.CodeSessionID, 0, dcodec.FlagMandatory, dcodec.NewSessionID(m.host)),
		dcodec.NewUint32(dcodec.CodeAuthSessionState, 0, dcodec.FlagMandatory, dcodec.AuthSessionStateNoStateMaintained),
		dcodec.NewString(dcodec.CodeOriginHost, 0, dcodec.FlagMandatory, m.host),
		dcodec.NewString(dcodec.CodeOriginRealm, 0, dcodec.FlagMandatory, mmeRealm),
		dcodec.NewString(dcodec.CodeDestinationHost, 0, dcodec.FlagMandatory, smscFQDN),
		dcodec.NewString(dcodec.CodeDestinationRealm, 0, dcodec.FlagMandatory, smscRealm),
		dcodec.NewOctetString(dcodec.CodeSCAddress, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific, []byte("15550000000")),
		uid,
		dcodec.NewOctetString(dcodec.CodeSMRPUI, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific, tpData),
	).Build()
	if err := m.peer.Send(req); err != nil {
		t.Fatalf("%s: send MO: %v", m.host, err)
	}
	select {
	case ans := <-m.moAnswers:
		if rc := ans.FindAVP(dcodec.CodeResultCode, 0); rc == nil {
			t.Fatalf("%s: MO answer has no Result-Code", m.host)
		} else if v, _ := rc.Uint32(); v != dcodec.DiameterSuccess {
			t.Fatalf("%s: MO answer Result-Code = %d, want 2001", m.host, v)
		}
	case <-time.After(waitTimeout):
		t.Fatalf("%s: no answer to MO request", m.host)
	}
}

// nextTFR waits for the next MT TFR received by this MME.
func (m *fakeMME) nextTFR(t *testing.T) *dcodec.Message {
	t.Helper()
	select {
	case req := <-m.tfrs:
		return req
	case <-time.After(waitTimeout):
		t.Fatalf("%s: expected an MT-Forward-Short-Message-Request", m.host)
		return nil
	}
}

func (m *fakeMME) expectNoTFR(t *testing.T, wait time.Duration) {
	t.Helper()
	select {
	case req := <-m.tfrs:
		t.Fatalf("%s: unexpected MT-Forward-Short-Message-Request: %+v", m.host, req.Header)
	case <-time.After(wait):
	}
}

// encodeMSISDN encodes the MSISDN AVP as a spec-compliant MME does (plain
// TBCD, TS 29.329 §6.3.2), or with vectorcore-mme's current len|0x91 prefix
// when the MME is set to legacy mode.
func (m *fakeMME) encodeMSISDN(msisdn string) []byte {
	m.mu.Lock()
	legacy := m.legacyMSISDN
	m.mu.Unlock()
	digits := make([]byte, (len(msisdn)+1)/2)
	for i, c := range msisdn {
		d := byte(c - '0')
		if i%2 == 0 {
			digits[i/2] = d
		} else {
			digits[i/2] |= d << 4
		}
	}
	if len(msisdn)%2 == 1 {
		digits[len(digits)-1] |= 0xF0
	}
	if !legacy {
		return digits
	}
	return append([]byte{byte(1 + len(digits)), 0x91}, digits...)
}

// ── SMSC under test ──────────────────────────────────────────────────────────

type harness struct {
	ctx  context.Context
	fwd  *forwarder.Forwarder
	st   *sqlite.DB
	hss  *fakeHSS
	mmeA *fakeMME
	mmeB *fakeMME
}

type harnessOptions struct {
	maxQueueLifetime time.Duration // default 1h
	tfaTimeout       time.Duration // default sgd.DefaultTFATimeout
	expirySweep      time.Duration // >0 runs the expiry sweeper at this interval
	retryInterval    time.Duration // >0 runs the retry scheduler at this interval
	msisdnEncoding   string        // SMSC sgd_msisdn_encoding; default tbcd
}

func startHarness(t *testing.T) *harness {
	t.Helper()
	return startHarnessWith(t, harnessOptions{})
}

func startHarnessWith(t *testing.T, opts harnessOptions) *harness {
	t.Helper()
	if opts.maxQueueLifetime == 0 {
		opts.maxQueueLifetime = time.Hour
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	st, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "smsc.db"), time.Hour)
	if err != nil {
		t.Fatalf("sqlite.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	hss := newFakeHSS()
	reg, err := registry.New(ctx, st)
	if err != nil {
		t.Fatalf("registry.New() error = %v", err)
	}
	reg.SetS6cClient(hss)

	port := freePort(t)
	srv := sgdsrv.NewServer("tcp", fmt.Sprintf("127.0.0.1:%d", port), smscFQDN, smscRealm, "tbcd", nil)
	srv.SetTFATimeout(opts.tfaTimeout)
	srv.SetMSISDNEncoding(opts.msisdnEncoding)

	correlator := dr.New(st, nil, nil, scAddr)
	correlator.SetS6cReporter(hss)
	fwd := forwarder.New(forwarder.Config{
		Registry:         reg,
		Engine:           routing.NewEngine(),
		Store:            st,
		SCAddr:           scAddr,
		SGdSender:        srv,
		Reporter:         correlator,
		MaxQueueLifetime: opts.maxQueueLifetime,
	})
	correlator.SetDispatcher(fwd)
	if opts.expirySweep > 0 {
		// Same wiring as cmd/smsc: expired messages that requested a report get one.
		sweeper := forwarder.NewExpirySweeper(fwd, opts.expirySweep, func(ctx context.Context, m store.Message) {
			correlator.Report(ctx, m, dr.StatusExpired)
		})
		go sweeper.Run(ctx)
	}
	if opts.retryInterval > 0 {
		go forwarder.NewRetryScheduler(fwd, opts.retryInterval).Run(ctx)
	}
	srv.SetOnMessage(func(msg *codec.Message, _ string) { fwd.Dispatch(ctx, msg) })

	go func() { _ = srv.ListenAndServe(ctx) }()

	h := &harness{
		ctx:  ctx,
		fwd:  fwd,
		st:   st,
		hss:  hss,
		mmeA: startFakeMME(ctx, t, mmeAHost, port),
		mmeB: startFakeMME(ctx, t, mmeBHost, port),
	}
	waitFor(t, "both MMEs connected", func() bool {
		return srv.HasPeerForMME(mmeAHost) && srv.HasPeerForMME(mmeBHost) &&
			h.mmeA.peer.State() == diameter.StateOpen && h.mmeB.peer.State() == diameter.StateOpen
	})
	// HasPeerForMME falls back to any open peer, so wait for both to be
	// registered under their own Origin-Host before sending traffic.
	waitFor(t, "MMEs registered by FQDN", func() bool {
		a, viaA, okA := srv.RoutePeerForMME(mmeAHost)
		b, viaB, okB := srv.RoutePeerForMME(mmeBHost)
		return okA && okB && !viaA && !viaB && a == mmeAHost && b == mmeBHost
	})
	return h
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (h *harness) messages(t *testing.T) []store.Message {
	t.Helper()
	msgs, err := h.st.ListMessages(context.Background(), 100)
	if err != nil {
		t.Fatalf("ListMessages() error = %v", err)
	}
	return msgs
}

func (h *harness) messagesOfKind(t *testing.T, kind string) []store.Message {
	t.Helper()
	var out []store.Message
	for _, m := range h.messages(t) {
		if m.Kind == kind {
			out = append(out, m)
		}
	}
	return out
}

// waitForStatus waits until count messages of kind have the given status.
func (h *harness) waitForStatus(t *testing.T, kind, status string, count int) []store.Message {
	t.Helper()
	var matched []store.Message
	waitFor(t, fmt.Sprintf("%d %s message(s) in %s", count, kind, status), func() bool {
		matched = matched[:0]
		for _, m := range h.messagesOfKind(t, kind) {
			if m.Status == status {
				matched = append(matched, m)
			}
		}
		return len(matched) == count
	})
	return matched
}

func submitTo(dst string, mr byte, srr bool) *codec.Message {
	return &codec.Message{
		Destination:  codec.Address{MSISDN: dst},
		Text:         "hello from 555",
		Encoding:     codec.EncodingGSM7,
		TPMR:         mr,
		TPSRRequired: srr,
	}
}

func userName(t *testing.T, req *dcodec.Message) string {
	t.Helper()
	avp := req.FindAVP(dcodec.CodeUserName, 0)
	if avp == nil {
		t.Fatal("TFR has no User-Name")
	}
	return string(avp.Data)
}

func smRPUI(t *testing.T, req *dcodec.Message) []byte {
	t.Helper()
	avp := req.FindAVP(dcodec.CodeSMRPUI, dcodec.Vendor3GPP)
	if avp == nil {
		t.Fatal("TFR has no SM-RP-UI")
	}
	return avp.Data
}

func sessionID(req *dcodec.Message) string {
	if avp := req.FindAVP(dcodec.CodeSessionID, 0); avp != nil {
		return string(avp.Data)
	}
	return ""
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// Sender UE → MME-A → SMSC → HSS → MME-B → destination UE, then
// SMSC → HSS (sender lookup) → MME-A → sender UE with SMS-STATUS-REPORT.
func TestSGdStatusReportDeliveredToOriginalSender(t *testing.T) {
	h := startHarness(t)

	h.mmeA.sendMO(t, submitTo(destMSISDN, 0x2A, true))

	// Leg 1: the SMS-DELIVER reaches the destination MME.
	deliverReq := h.mmeB.nextTFR(t)
	if got := userName(t, deliverReq); got != destIMSI {
		t.Fatalf("leg 1 User-Name = %q, want destination IMSI %s", got, destIMSI)
	}
	deliverData := smRPUI(t, deliverReq)
	if mti := deliverData[0] & 0x03; mti != 0x00 {
		t.Fatalf("leg 1 TP-MTI = %d, want SMS-DELIVER (0)", mti)
	}
	deliver, err := tpdu.Decode(deliverData)
	if err != nil {
		t.Fatalf("decode leg 1 SMS-DELIVER: %v", err)
	}
	if deliver.Source.MSISDN != senderMSISDN || !deliver.TPSRRequired {
		t.Fatalf("leg 1 SMS-DELIVER = OA %q SRI %v, want OA %s with TP-SRI set", deliver.Source.MSISDN, deliver.TPSRRequired, senderMSISDN)
	}

	// Leg 2: a new TFR carrying an SMS-STATUS-REPORT reaches the sender's MME.
	reportReq := h.mmeA.nextTFR(t)
	if got := userName(t, reportReq); got != senderIMSI {
		t.Fatalf("leg 2 User-Name = %q, want sender IMSI %s", got, senderIMSI)
	}
	if sessionID(reportReq) == "" || sessionID(reportReq) == sessionID(deliverReq) {
		t.Fatalf("leg 2 Session-Id %q must be new (leg 1 was %q)", sessionID(reportReq), sessionID(deliverReq))
	}
	if reportReq.Header.HopByHop == deliverReq.Header.HopByHop {
		t.Fatal("leg 2 reused leg 1's hop-by-hop identifier")
	}
	reportData := smRPUI(t, reportReq)
	if mti := reportData[0] & 0x03; mti != 0x02 {
		t.Fatalf("leg 2 TP-MTI = %d, want SMS-STATUS-REPORT (2)", mti)
	}
	report, err := tpdu.Decode(reportData)
	if err != nil {
		t.Fatalf("decode leg 2 SMS-STATUS-REPORT: %v", err)
	}
	if report.TPMR != 0x2A {
		t.Errorf("TP-MR = 0x%02X, want original 0x2A", report.TPMR)
	}
	if report.Source.MSISDN != destMSISDN {
		t.Errorf("TP-RA = %q, want %s", report.Source.MSISDN, destMSISDN)
	}
	if report.TPStatus != tpdu.STReceivedBySME {
		t.Errorf("TP-ST = 0x%02X, want 0x00 (received by SME)", report.TPStatus)
	}

	// Both transactions complete independently, and are linked in the store.
	orig := h.waitForStatus(t, store.MessageKindSM, store.MessageStatusDelivered, 1)[0]
	sr := h.waitForStatus(t, store.MessageKindStatusReport, store.MessageStatusDelivered, 1)[0]
	if orig.TPMR == nil || *orig.TPMR != 0x2A || !orig.DRRequired {
		t.Errorf("original row TP-MR %v / dr_required %v, want 0x2A / true", orig.TPMR, orig.DRRequired)
	}
	if orig.SrcMSISDN != senderMSISDN || orig.DstMSISDN != destMSISDN || orig.EgressPeer != mmeBHost {
		t.Errorf("original row = src %q dst %q egress %q", orig.SrcMSISDN, orig.DstMSISDN, orig.EgressPeer)
	}
	if sr.RefMessageID != orig.ID {
		t.Errorf("status report ref_message_id = %q, want %q", sr.RefMessageID, orig.ID)
	}
	if sr.DstMSISDN != senderMSISDN || sr.OriginIface != string(codec.InterfaceInternal) || sr.EgressPeer != mmeAHost {
		t.Errorf("status report row = dst %q origin %q egress %q", sr.DstMSISDN, sr.OriginIface, sr.EgressPeer)
	}

	// The sender was looked up through S6c, and both MT legs were reported.
	if h.hss.lookupCount(senderMSISDN) == 0 {
		t.Error("expected an S6c lookup for the original sender")
	}
	waitFor(t, "RSDS for both MT legs", func() bool {
		return len(h.hss.rsdsCauses(destMSISDN)) == 1 && len(h.hss.rsdsCauses(senderMSISDN)) == 1
	})

	// Nothing further: the report's own outcome does not produce another one.
	h.mmeA.expectNoTFR(t, 300*time.Millisecond)
	h.mmeB.expectNoTFR(t, 0)
}

func TestSGdNoStatusReportWhenNotRequested(t *testing.T) {
	h := startHarness(t)

	h.mmeA.sendMO(t, submitTo(destMSISDN, 0x2B, false))

	h.mmeB.nextTFR(t)
	h.waitForStatus(t, store.MessageKindSM, store.MessageStatusDelivered, 1)
	h.mmeA.expectNoTFR(t, 500*time.Millisecond)
	if n := len(h.messages(t)); n != 1 {
		t.Fatalf("message rows = %d, want 1", n)
	}
	if n := h.hss.lookupCount(senderMSISDN); n != 0 {
		t.Fatalf("S6c lookups for sender = %d, want 0", n)
	}
}

// Each segment of a concatenated SMS has its own TP-MR and must get its own
// status report carrying that TP-MR.
func TestSGdConcatenatedSMSGetsOneStatusReportPerSegment(t *testing.T) {
	h := startHarness(t)

	mrs := []byte{0x10, 0x11, 0x12}
	for i, mr := range mrs {
		part := submitTo(destMSISDN, mr, true)
		part.Text = fmt.Sprintf("part %d", i+1)
		part.Concat = &codec.ConcatInfo{Ref: 0x0A0B, Total: 3, Sequence: uint8(i + 1)}
		h.mmeA.sendMO(t, part)
	}

	for range mrs {
		h.mmeB.nextTFR(t)
	}
	var gotMRs []int
	for range mrs {
		report, err := tpdu.Decode(smRPUI(t, h.mmeA.nextTFR(t)))
		if err != nil {
			t.Fatalf("decode status report: %v", err)
		}
		if report.TPStatus != tpdu.STReceivedBySME || report.Source.MSISDN != destMSISDN {
			t.Fatalf("status report = ST 0x%02X RA %q", report.TPStatus, report.Source.MSISDN)
		}
		gotMRs = append(gotMRs, int(report.TPMR))
	}
	sort.Ints(gotMRs)
	if want := []int{0x10, 0x11, 0x12}; fmt.Sprint(gotMRs) != fmt.Sprint(want) {
		t.Fatalf("status report TP-MRs = %v, want %v", gotMRs, want)
	}

	// Each report is linked to the segment whose TP-MR it carries.
	origs := h.waitForStatus(t, store.MessageKindSM, store.MessageStatusDelivered, 3)
	reports := h.waitForStatus(t, store.MessageKindStatusReport, store.MessageStatusDelivered, 3)
	mrByID := map[string]int{}
	for _, o := range origs {
		if o.TPMR == nil {
			t.Fatalf("segment %s has no TP-MR", o.ID)
		}
		mrByID[o.ID] = *o.TPMR
	}
	for _, r := range reports {
		mr, ok := mrByID[r.RefMessageID]
		if !ok {
			t.Fatalf("report %s references unknown message %q", r.ID, r.RefMessageID)
		}
		decoded, err := tpdu.Decode(r.RawTPDU)
		if err != nil {
			t.Fatalf("decode stored report: %v", err)
		}
		if int(decoded.TPMR) != mr {
			t.Fatalf("report for segment with TP-MR 0x%02X carries TP-MR 0x%02X", mr, decoded.TPMR)
		}
		delete(mrByID, r.RefMessageID)
	}
	h.mmeA.expectNoTFR(t, 300*time.Millisecond)
}

// If the sender's MME cannot deliver the report, the report is stored for
// retry like any MT, and the original message's state is unaffected.
func TestSGdStatusReportDeferredWhenSenderUnreachable(t *testing.T) {
	h := startHarness(t)
	h.mmeA.setResultCode(dcodec.DiameterUnableToDeliver)

	h.mmeA.sendMO(t, submitTo(destMSISDN, 0x2C, true))

	h.mmeB.nextTFR(t)
	h.mmeA.nextTFR(t)

	orig := h.waitForStatus(t, store.MessageKindSM, store.MessageStatusDelivered, 1)[0]
	// Same treatment as any MT refused with 3002: wait for Alert-SC and retry on a timer.
	sr := h.waitForStatus(t, store.MessageKindStatusReport, store.MessageStatusWaitTimerEvent, 1)[0]
	if sr.NextRetryAt == nil {
		t.Fatal("deferred status report has no retry scheduled")
	}
	if sr.RefMessageID != orig.ID {
		t.Fatalf("status report ref_message_id = %q, want %q", sr.RefMessageID, orig.ID)
	}
	if sr.DeferredInterface != "sgd" || sr.ServingNodeAtDeferral != mmeAHost {
		t.Fatalf("status report deferral = iface %q node %q, want sgd / %s", sr.DeferredInterface, sr.ServingNodeAtDeferral, mmeAHost)
	}
	if len(sr.RawTPDU) == 0 {
		t.Fatal("deferred status report lost its TPDU")
	}
	// The original stays DELIVERED; nothing more is sent to either MME.
	h.mmeA.expectNoTFR(t, 300*time.Millisecond)
	h.mmeB.expectNoTFR(t, 0)
	if got := h.messagesOfKind(t, store.MessageKindSM)[0].Status; got != store.MessageStatusDelivered {
		t.Fatalf("original status = %s, want DELIVERED", got)
	}
}

// Destination absent until the message expires: the sender gets a report
// saying the validity period expired.
func TestSGdStatusReportOnExpiry(t *testing.T) {
	h := startHarnessWith(t, harnessOptions{
		maxQueueLifetime: time.Second,
		expirySweep:      100 * time.Millisecond,
	})
	h.mmeB.setResultCode(dcodec.DiameterUnableToDeliver)

	before := time.Now().UTC().Add(-time.Second)
	h.mmeA.sendMO(t, submitTo(destMSISDN, 0x2D, true))
	h.mmeB.nextTFR(t)

	report, err := tpdu.Decode(smRPUI(t, h.mmeA.nextTFR(t)))
	if err != nil {
		t.Fatalf("decode status report: %v", err)
	}
	if report.TPStatus != tpdu.STPermValidityPeriodExpired {
		t.Fatalf("TP-ST = 0x%02X, want 0x46 (validity period expired)", report.TPStatus)
	}
	if report.TPMR != 0x2D || report.Source.MSISDN != destMSISDN {
		t.Fatalf("report = MR 0x%02X RA %q, want 0x2D / %s", report.TPMR, report.Source.MSISDN, destMSISDN)
	}
	if report.DischargeTime.Before(before) {
		t.Fatalf("TP-DT = %v, want the expiry time", report.DischargeTime)
	}

	orig := h.waitForStatus(t, store.MessageKindSM, store.MessageStatusExpired, 1)[0]
	sr := h.waitForStatus(t, store.MessageKindStatusReport, store.MessageStatusDelivered, 1)[0]
	if sr.RefMessageID != orig.ID {
		t.Fatalf("status report ref_message_id = %q, want %q", sr.RefMessageID, orig.ID)
	}
	// The HSS was told the destination was absent when delivery was deferred;
	// expiry itself sends nothing further.
	if causes := h.hss.rsdsCauses(destMSISDN); fmt.Sprint(causes) != fmt.Sprint([]uint32{dcodec.SMDeliveryCauseAbsentUser}) {
		t.Fatalf("destination RSDS causes = %v, want exactly one absent-user", causes)
	}
}

// A missing TFA is not a final outcome: the message waits for retry and no
// status report is sent.
func TestSGdTFATimeoutIsNotReportedAsFailure(t *testing.T) {
	h := startHarnessWith(t, harnessOptions{tfaTimeout: 300 * time.Millisecond})
	h.mmeB.setResultCode(0) // destination MME never answers

	h.mmeA.sendMO(t, submitTo(destMSISDN, 0x2E, true))
	h.mmeB.nextTFR(t)

	orig := h.waitForStatus(t, store.MessageKindSM, store.MessageStatusWaitTimer, 1)[0]
	if orig.NextRetryAt == nil {
		t.Fatal("timed-out message has no retry scheduled")
	}
	h.mmeA.expectNoTFR(t, 500*time.Millisecond)
	if n := h.hss.lookupCount(senderMSISDN); n != 0 {
		t.Fatalf("S6c lookups for sender = %d, want 0", n)
	}
	if n := len(h.messagesOfKind(t, store.MessageKindStatusReport)); n != 0 {
		t.Fatalf("status report rows = %d, want 0", n)
	}
}

// An absent destination is reported to the HSS (RSDS absent-user) once per
// absent episode: on the first deferral, not on timer retries, and again
// after an HSS alert finds the subscriber still absent. Delivery clears it.
func TestSGdAbsentDestinationReportedToHSSOncePerEpisode(t *testing.T) {
	ctx := context.Background()
	h := startHarnessWith(t, harnessOptions{retryInterval: 50 * time.Millisecond})
	h.mmeB.setResultCode(dcodec.DiameterUnableToDeliver)
	absent, success := dcodec.SMDeliveryCauseAbsentUser, dcodec.SMDeliveryCauseSuccessfulTransfer
	wantRSDS := func(want ...uint32) {
		t.Helper()
		waitFor(t, fmt.Sprintf("destination RSDS causes %v", want), func() bool {
			return fmt.Sprint(h.hss.rsdsCauses(destMSISDN)) == fmt.Sprint(want)
		})
	}

	h.mmeA.sendMO(t, submitTo(destMSISDN, 0x30, false))
	h.mmeB.nextTFR(t)
	orig := h.waitForStatus(t, store.MessageKindSM, store.MessageStatusWaitTimerEvent, 1)[0]
	wantRSDS(absent) // 1. first deferral → HSS stores MWD

	// 2. Timer retry, still absent: no repeat RSDS.
	if err := h.st.UpdateMessageRetry(ctx, orig.ID, 1, time.Now().Add(-time.Second), orig.RouteCursor); err != nil {
		t.Fatalf("UpdateMessageRetry() error = %v", err)
	}
	h.mmeB.nextTFR(t)
	h.waitForStatus(t, store.MessageKindSM, store.MessageStatusWaitEvent, 1)
	time.Sleep(200 * time.Millisecond)
	wantRSDS(absent)

	// 3. HSS alert arrives (the HSS clears its MWD), retry still absent: report again.
	alertRetry := func() {
		t.Helper()
		ok, err := h.st.RequeueMessageForAlert(ctx, orig.ID, time.Now().Add(-time.Second), orig.RouteCursor, "sgd_alert_retry",
			[]string{store.MessageStatusWaitEvent, store.MessageStatusWaitTimerEvent, store.MessageStatusWaitTimer})
		if err != nil || !ok {
			t.Fatalf("RequeueMessageForAlert() = %v, %v", ok, err)
		}
	}
	alertRetry()
	h.mmeB.nextTFR(t)
	wantRSDS(absent, absent)

	// 4. Subscriber reachable, next alert delivers: HSS told successful transfer.
	h.mmeB.setResultCode(dcodec.DiameterSuccess)
	waitFor(t, "message waiting again after the failed alert retry", func() bool {
		m, err := h.st.GetMessage(ctx, orig.ID)
		return err == nil && m != nil && m.Status != store.MessageStatusDispatched
	})
	alertRetry()
	h.mmeB.nextTFR(t)
	h.waitForStatus(t, store.MessageKindSM, store.MessageStatusDelivered, 1)
	wantRSDS(absent, absent, success)
}

// Against an MME that allows one MT per subscriber (like vectorcore-mme),
// a concatenated SMS and its status reports must be delivered one at a time
// per handset, never provoking a busy answer.
func TestSGdConcatenatedSMSSerialisedPerSubscriber(t *testing.T) {
	h := startHarness(t)
	h.mmeA.setRealistic(150 * time.Millisecond)
	h.mmeB.setRealistic(150 * time.Millisecond)

	for i, mr := range []byte{0x40, 0x41, 0x42} {
		part := submitTo(destMSISDN, mr, true)
		part.Text = fmt.Sprintf("part %d", i+1)
		part.Concat = &codec.ConcatInfo{Ref: 0x0B0C, Total: 3, Sequence: uint8(i + 1)}
		h.mmeA.sendMO(t, part)
	}

	h.waitForStatus(t, store.MessageKindSM, store.MessageStatusDelivered, 3)
	h.waitForStatus(t, store.MessageKindStatusReport, store.MessageStatusDelivered, 3)
	if n := h.mmeB.busyCount(); n != 0 {
		t.Fatalf("destination MME answered busy %d time(s); MTs to one subscriber were not serialised", n)
	}
	if n := h.mmeA.busyCount(); n != 0 {
		t.Fatalf("sender MME answered busy %d time(s); status reports to one subscriber were not serialised", n)
	}
}

// A handset that rejects the status report (TS 29.338 SM delivery failure,
// equipment protocol error) is final: the report fails at once instead of
// being retried until the queue lifetime runs out.
func TestSGdStatusReportRejectedByHandsetFailsImmediately(t *testing.T) {
	h := startHarness(t)
	cause := sgdsrv.DeliveryFailureEquipmentProtocolError
	h.mmeA.setExperimental(sgdsrv.ResultErrorSMDeliveryFailure, &cause)

	h.mmeA.sendMO(t, submitTo(destMSISDN, 0x50, true))
	h.mmeB.nextTFR(t)
	h.mmeA.nextTFR(t)

	h.waitForStatus(t, store.MessageKindSM, store.MessageStatusDelivered, 1)
	sr := h.waitForStatus(t, store.MessageKindStatusReport, store.MessageStatusFailed, 1)[0]
	if sr.NextRetryAt != nil {
		t.Fatalf("rejected status report has a retry scheduled at %v", sr.NextRetryAt)
	}
	// No retry, and the failed report doesn't generate a report of its own.
	h.mmeA.expectNoTFR(t, 300*time.Millisecond)
}

// With sgd_msisdn_encoding: legacy_prefixed the SMSC still interoperates with
// an MME that sends len|0x91|TBCD (vectorcore-mme today): the sender keeps
// its country code and gets its status report.
func TestSGdLegacyPrefixedMSISDNPeer(t *testing.T) {
	h := startHarnessWith(t, harnessOptions{msisdnEncoding: "legacy_prefixed"})
	h.mmeA.mu.Lock()
	h.mmeA.legacyMSISDN = true
	h.mmeA.mu.Unlock()

	h.mmeA.sendMO(t, submitTo(destMSISDN, 0x60, true))
	deliver, err := tpdu.Decode(smRPUI(t, h.mmeB.nextTFR(t)))
	if err != nil {
		t.Fatalf("decode SMS-DELIVER: %v", err)
	}
	if deliver.Source.MSISDN != senderMSISDN {
		t.Fatalf("TP-OA = %q, want %s", deliver.Source.MSISDN, senderMSISDN)
	}
	if got := userName(t, h.mmeA.nextTFR(t)); got != senderIMSI {
		t.Fatalf("status report User-Name = %q, want %s", got, senderIMSI)
	}
}

// A handset-originated SM over IMS (ISC, RP-DATA in a SIP MESSAGE) with
// TP-SRR set gets its SMS-STATUS-REPORT like an SGd-originated one. Neither
// party is IMS-registered here, so both MT legs go out over SGd.
func TestIMSOriginatedStatusReportDelivered(t *testing.T) {
	h := startHarness(t)

	submit := submitTo(destMSISDN, 0x61, true)
	body, err := sip3gpp.EncodeMO(submit, 0x07, scAddr)
	if err != nil {
		t.Fatalf("sip3gpp.EncodeMO() error = %v", err)
	}
	// What the ISC handler does with the MESSAGE body.
	msg, err := sip3gpp.Decode(body)
	if err != nil || msg == nil {
		t.Fatalf("sip3gpp.Decode() = %v, %v", msg, err)
	}
	msg.IngressInterface = codec.InterfaceSIP3GPP
	msg.IngressPeer = "scscf.ims.example.net"
	msg.Source.MSISDN = senderMSISDN // from P-Asserted-Identity
	if msg.TPMR != 0x61 || !msg.TPSRRequired {
		t.Fatalf("decoded SUBMIT = TP-MR 0x%02X SRR %v, want 0x61 / true", msg.TPMR, msg.TPSRRequired)
	}
	go h.fwd.Dispatch(h.ctx, msg)

	h.mmeB.nextTFR(t)
	reportReq := h.mmeA.nextTFR(t)
	if got := userName(t, reportReq); got != senderIMSI {
		t.Fatalf("status report User-Name = %q, want sender IMSI %s", got, senderIMSI)
	}
	report, err := tpdu.Decode(smRPUI(t, reportReq))
	if err != nil {
		t.Fatalf("decode SMS-STATUS-REPORT: %v", err)
	}
	if report.TPMR != 0x61 || report.Source.MSISDN != destMSISDN || report.TPStatus != tpdu.STReceivedBySME {
		t.Fatalf("status report = TP-MR 0x%02X RA %q ST 0x%02X, want 0x61 / %s / 0x00",
			report.TPMR, report.Source.MSISDN, report.TPStatus, destMSISDN)
	}

	orig := h.waitForStatus(t, store.MessageKindSM, store.MessageStatusDelivered, 1)[0]
	if orig.OriginIface != string(codec.InterfaceSIP3GPP) || orig.TPMR == nil || *orig.TPMR != 0x61 {
		t.Fatalf("original row = origin %q TP-MR %v, want sip3gpp / 0x61", orig.OriginIface, orig.TPMR)
	}
	sr := h.waitForStatus(t, store.MessageKindStatusReport, store.MessageStatusDelivered, 1)[0]
	if sr.RefMessageID != orig.ID {
		t.Fatalf("status report ref_message_id = %q, want %q", sr.RefMessageID, orig.ID)
	}
	h.mmeA.expectNoTFR(t, 300*time.Millisecond)
}

// When the destination's handset refuses the SM outright, the sender's
// status report says why (TP-ST from the MT-Forward failure cause) rather
// than a generic "not obtainable".
func TestSGdStatusReportCarriesRejectionCause(t *testing.T) {
	h := startHarness(t)
	h.mmeB.setExperimental(sgdsrv.ResultErrorIllegalEquipment, nil)

	h.mmeA.sendMO(t, submitTo(destMSISDN, 0x62, true))
	h.mmeB.nextTFR(t)
	report, err := tpdu.Decode(smRPUI(t, h.mmeA.nextTFR(t)))
	if err != nil {
		t.Fatalf("decode SMS-STATUS-REPORT: %v", err)
	}
	if report.TPMR != 0x62 || report.TPStatus != tpdu.STPermIncompatibleDestination {
		t.Fatalf("status report = TP-MR 0x%02X ST 0x%02X, want 0x62 / 0x41 (incompatible destination)",
			report.TPMR, report.TPStatus)
	}
	h.waitForStatus(t, store.MessageKindSM, store.MessageStatusFailed, 1)
	h.waitForStatus(t, store.MessageKindStatusReport, store.MessageStatusDelivered, 1)
}

// If the HSS no longer knows the original sender when its status report is
// sent, the report fails at once: it is not retried, does not generate
// further traffic, and the original message is unaffected.
func TestSGdStatusReportSenderUnknownToHSS(t *testing.T) {
	h := startHarness(t)
	h.hss.remove(senderMSISDN)

	h.mmeA.sendMO(t, submitTo(destMSISDN, 0x63, true))
	h.mmeB.nextTFR(t)
	h.waitForStatus(t, store.MessageKindSM, store.MessageStatusDelivered, 1)

	waitFor(t, "S6c lookup of the sender", func() bool { return h.hss.lookupCount(senderMSISDN) > 0 })
	h.mmeA.expectNoTFR(t, 500*time.Millisecond)
	h.mmeB.expectNoTFR(t, 0)

	reports := h.messagesOfKind(t, store.MessageKindStatusReport)
	if len(reports) != 1 {
		t.Fatalf("status report rows = %d, want 1", len(reports))
	}
	if sr := reports[0]; sr.Status != store.MessageStatusFailed || sr.NextRetryAt != nil {
		t.Fatalf("status report = status %s next_retry %v, want FAILED with no retry", sr.Status, sr.NextRetryAt)
	}
	if got := h.hss.lookupCount(senderMSISDN); got != 1 {
		t.Fatalf("S6c lookups of the sender = %d, want 1", got)
	}
}
