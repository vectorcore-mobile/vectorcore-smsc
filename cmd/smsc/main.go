package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/svinson1121/vectorcore-smsc/internal/api"
	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/config"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter/s6c"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter/sh"
	"github.com/svinson1121/vectorcore-smsc/internal/dr"
	"github.com/svinson1121/vectorcore-smsc/internal/forwarder"
	"github.com/svinson1121/vectorcore-smsc/internal/metrics"
	"github.com/svinson1121/vectorcore-smsc/internal/registry"
	"github.com/svinson1121/vectorcore-smsc/internal/routing"
	"github.com/svinson1121/vectorcore-smsc/internal/sgdmap"
	sipserver "github.com/svinson1121/vectorcore-smsc/internal/sip"
	"github.com/svinson1121/vectorcore-smsc/internal/sip/isc"
	"github.com/svinson1121/vectorcore-smsc/internal/sip/simple"
	"github.com/svinson1121/vectorcore-smsc/internal/smpp"
	smppClient "github.com/svinson1121/vectorcore-smsc/internal/smpp/client"
	smppServer "github.com/svinson1121/vectorcore-smsc/internal/smpp/server"
	"github.com/svinson1121/vectorcore-smsc/internal/store"
	postgresstore "github.com/svinson1121/vectorcore-smsc/internal/store/postgres"
	sqlitestore "github.com/svinson1121/vectorcore-smsc/internal/store/sqlite"
)

// version is injected by the Makefile via -ldflags.
// It falls back to "dev" when built outside the Makefile.
var version = "dev"

func appIDsForDiameterPeer(apps []string) []uint32 {
	appIDs := make([]uint32, 0, len(apps))
	for _, app := range apps {
		switch app {
		case "s6c":
			appIDs = append(appIDs, dcodec.App3GPP_S6c)
		case "sgd":
			appIDs = append(appIDs, dcodec.App3GPP_SGd)
		case "sh":
			appIDs = append(appIDs, dcodec.App3GPP_Sh)
		}
	}
	return appIDs
}

func sameAppIDSet(current diameter.Config, want []uint32) bool {
	got := make([]uint32, 0, len(current.AppIDs)+1)
	if current.AppID != 0 {
		got = append(got, current.AppID)
	}
	got = append(got, current.AppIDs...)

	normalize := func(in []uint32) []uint32 {
		seen := make(map[uint32]struct{}, len(in))
		out := make([]uint32, 0, len(in))
		for _, v := range in {
			if v == 0 {
				continue
			}
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			out = append(out, v)
		}
		slices.Sort(out)
		return out
	}

	return slices.Equal(normalize(got), normalize(want))
}

func shouldReplaceHSSPeer(current *diameter.Peer, currentHasSGd bool, want *store.DiameterPeer, wantHasSGd bool) bool {
	return shouldReplaceDiameterPeer(current, currentHasSGd, want, wantHasSGd, want.Applications)
}

func buildInboundSMPPTLSConfig(cfg *config.Config) (*tls.Config, error) {
	if cfg.SMPPServerTLSListen() == "" {
		return nil, nil
	}
	certFile := cfg.SMPPServerTLSCertFile()
	keyFile := cfg.SMPPServerTLSKeyFile()
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("smpp.server_tls requires cert_file and key_file")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load SMPP TLS keypair: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	verifyClientCert := cfg.SMPPServerTLSVerifyClientCert()
	requireClientCert := cfg.SMPPServerTLSRequireClientCert()
	clientCAFile := cfg.SMPPServerTLSClientCAFile()
	if verifyClientCert {
		if clientCAFile == "" {
			return nil, fmt.Errorf("smpp.server_tls.verify_client_cert requires smpp.server_tls.client_ca_file")
		}
		pem, err := os.ReadFile(clientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read SMPP client CA file %q: %w", clientCAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("parse SMPP client CA file %q: no certificates found", clientCAFile)
		}
		tlsCfg.ClientCAs = pool
	}

	switch {
	case requireClientCert && verifyClientCert:
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	case requireClientCert:
		tlsCfg.ClientAuth = tls.RequireAnyClientCert
	case verifyClientCert:
		tlsCfg.ClientAuth = tls.VerifyClientCertIfGiven
	}

	return tlsCfg, nil
}

func shouldReplaceDiameterPeer(current *diameter.Peer, currentHasSGd bool, want *store.DiameterPeer, wantHasSGd bool, wantApps []string) bool {
	if current == nil {
		return want != nil
	}
	if want == nil {
		return true
	}
	cfg := current.Config()
	return current.Name() != want.Name ||
		cfg.Host != want.Host ||
		cfg.Port != want.Port ||
		cfg.Transport != want.Transport ||
		cfg.PeerRealm != want.Realm ||
		currentHasSGd != wantHasSGd ||
		!sameAppIDSet(cfg, appIDsForDiameterPeer(wantApps))
}

func main() {
	cfgPath := flag.String("c", "config.yaml", "path to config file")
	debug := flag.Bool("d", false, "enable debug logging (overrides config)")
	showVersion := flag.Bool("v", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("VectorCore SMSC %s\n", version)
		os.Exit(0)
	}

	// Load config early so we can use log.file/log.level at startup.
	// Full config re-load happens inside run(); this is just for logging setup.
	var logFile, logLevel string
	if earlycfg, err := config.Load(*cfgPath); err == nil {
		logFile = earlycfg.Log.File
		logLevel = earlycfg.Log.Level
	}
	closer := setupLogging(*debug, logFile, logLevel)
	if closer != nil {
		defer closer()
	}

	if err := run(*cfgPath); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(cfgPath string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// ── Metrics ───────────────────────────────────────────────────────────────
	m := metrics.New()

	// ── Database ─────────────────────────────────────────────────────────────
	st, err := openStore(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	// ── IMS registry ──────────────────────────────────────────────────────────
	reg, err := registry.New(ctx, st)
	if err != nil {
		return fmt.Errorf("init registry: %w", err)
	}
	reg.SetS6cCacheTTL(cfg.Diameter.S6CCacheTTL)

	// ── Routing engine ────────────────────────────────────────────────────────
	routingEngine := routing.NewEngine()
	if _, err := routing.NewLoader(ctx, st, routingEngine); err != nil {
		return fmt.Errorf("routing loader: %w", err)
	}

	// ── SGd MME name mapper ───────────────────────────────────────────────────
	sgdMapper, err := sgdmap.NewMapper(ctx, st)
	if err != nil {
		return fmt.Errorf("sgd mme mapper: %w", err)
	}

	// ── Shared SMPP link registry ─────────────────────────────────────────────
	smppReg := smpp.NewRegistry()

	// ── SMPP Server auth ──────────────────────────────────────────────────────
	smppAuth, err := smppServer.NewAuthenticator(ctx, st)
	if err != nil {
		return fmt.Errorf("smpp auth: %w", err)
	}
	smppAccCh := make(chan store.ChangeEvent, 8)
	if err := st.Subscribe(ctx, "smpp_server_accounts", smppAccCh); err != nil {
		return fmt.Errorf("subscribe smpp_server_accounts: %w", err)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-smppAccCh:
				smppAuth.Reload(ctx)
			}
		}
	}()

	// ── SIP Server ──────────────────────────────────────────────────────────
	sipSrv, err := sipserver.NewServer(cfg.SIP.Transport, cfg.SIP.Listen, cfg.SIP.FQDN)
	if err != nil {
		return fmt.Errorf("init SIP server: %w", err)
	}
	sipClient, err := sipSrv.NewClient()
	if err != nil {
		return fmt.Errorf("create SIP client: %w", err)
	}

	scAddr := cfg.SMSC.Address
	iscSettings := isc.Settings{
		AcceptContact:           cfg.SIP.ISC.AcceptContact,
		MTRequestDisposition:    cfg.SIP.ISC.MTRequestDisposition,
		SubmitReportDisposition: cfg.SIP.ISC.SubmitReportRequestDisposition,
	}

	sipLocalURI := fmt.Sprintf("sip:smsc@%s", cfg.SIP.FQDN)
	iscSender := isc.NewSender(sipClient, scAddr, sipLocalURI, iscSettings)
	simpleSender := simple.NewSender(sipClient, cfg.SIP.FQDN)

	// ── Diameter SGd server ─────────────────────────────────────────────────
	sgdServer := sgd.NewServer(
		cfg.Diameter.Transport,
		cfg.Diameter.Listen,
		cfg.Diameter.LocalFQDN,
		cfg.Diameter.LocalRealm,
		cfg.SMSC.SGdSCAddressEncoding,
		nil, // set below
	)
	sgdServer.SetTFATimeout(cfg.SMSC.SGdTFATimeout)
	sgdServer.SetMSISDNEncoding(cfg.SMSC.SGdMSISDNEncoding)

	// ── SMPP Client Manager (not started yet — callback set after forwarder) ──
	clientMgr := smppClient.NewManager(st, smppReg, nil, smppClient.TLSOptions{
		OutboundCAFile: cfg.SMPPOutboundServerCAFile(),
	})

	// ── DR Correlator ─────────────────────────────────────────────────────────
	correlator := dr.New(st, clientMgr, smppReg, scAddr)

	// ── Forwarder ────────────────────────────────────────────────────────────
	fwd := forwarder.New(forwarder.Config{
		Registry:            reg,
		Engine:              routingEngine,
		Store:               st,
		SCAddr:              scAddr,
		Metrics:             m,
		ISCSender:           iscSender,
		SimpleSender:        simpleSender,
		SMPPManager:         clientMgr,
		SGdSender:           sgdServer,
		Reporter:            correlator,
		SGDMapper:           sgdMapper,
		MaxQueueLifetime:    cfg.SMSC.MaxQueueLifetime,
		DefaultCountryCode:  cfg.Numbering.DefaultCountryCode,
		LocalNationalLength: cfg.Numbering.LocalNationalLength,
	})
	// SMS-STATUS-REPORTs generated by the correlator are delivered as new MT
	// messages through the forwarder.
	correlator.SetDispatcher(fwd)

	// ── Retry Scheduler ───────────────────────────────────────────────────────
	retrySched := forwarder.NewRetryScheduler(fwd, 15*time.Second)
	go retrySched.Run(ctx)

	// ── Expiry Sweeper ────────────────────────────────────────────────────────
	sweeper := forwarder.NewExpirySweeper(fwd, 60*time.Second,
		func(expCtx context.Context, m store.Message) {
			correlator.Report(expCtx, m, dr.StatusExpired)
		},
	)
	go sweeper.Run(ctx)

	// ── IMS registration cleanup ──────────────────────────────────────────────
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				regs, err := st.ListIMSRegistrations(ctx)
				if err != nil {
					slog.Error("list ims registrations for cleanup", "err", err)
					continue
				}
				now := time.Now()
				var removed int
				for _, r := range regs {
					if r.Registered && now.Before(r.Expiry) {
						continue
					}
					if err := st.DeleteIMSRegistration(ctx, r.MSISDN); err != nil {
						slog.Error("delete expired ims registration", "msisdn", r.MSISDN, "err", err)
						continue
					}
					removed++
				}
				if removed > 0 {
					slog.Info("expired ims registrations removed", "count", removed)
				}
			}
		}
	}()

	// ── Wire ingress callbacks now that forwarder is ready ────────────────────

	// SGd server: TFR (MO from MME)
	sgdServer.SetOnMessage(func(msg *codec.Message, _ string) {
		fwd.Dispatch(ctx, msg)
	})

	// SMPP outbound clients: deliver_sm / MO from downstream SMSCs
	clientMgr.SetOnMessage(func(msg *codec.Message, _ *smpp.Link, clientName string) {
		msg.IngressInterface = codec.InterfaceSMPP
		msg.IngressPeer = clientName
		fwd.Dispatch(ctx, msg)
	})

	// Now start client manager so sessions get the correct callback.
	if err := clientMgr.Start(ctx); err != nil {
		return fmt.Errorf("smpp client manager: %w", err)
	}

	// SMPP server: submit_sm from connected ESMEs
	onSMPPServerMessage := func(msg *codec.Message, _ *smpp.Link, account store.SMPPServerAccount) {
		msg.IngressInterface = codec.InterfaceSMPP
		msg.IngressPeer = account.SystemID
		fwd.Dispatch(ctx, msg)
	}
	smppListener := smppServer.NewListener(
		cfg.SMPP.Server.Listen,
		smppAuth,
		smppReg,
		smppServer.SessionConfig{MaxConnections: cfg.SMPP.Server.MaxConnections, Transport: "tcp"},
		onSMPPServerMessage,
	)
	var smppTLSListener *smppServer.Listener
	if cfg.SMPPServerTLSListen() != "" {
		tlsCfg, err := buildInboundSMPPTLSConfig(cfg)
		if err != nil {
			return fmt.Errorf("configure SMPP TLS listener: %w", err)
		}
		smppTLSListener = smppServer.NewTLSListener(
			cfg.SMPPServerTLSListen(),
			smppAuth,
			smppReg,
			smppServer.SessionConfig{MaxConnections: cfg.SMPP.Server.MaxConnections, Transport: "tls"},
			tlsCfg,
			onSMPPServerMessage,
		)
	}

	// SIP ISC handler: SIP MESSAGE from S-CSCF
	msgHandler := &isc.MessageHandler{
		OnMessage: func(msg *codec.Message) {
			fwd.Dispatch(ctx, msg)
		},
		OnResult: func(inReplyTo string, body []byte) {
			if !sgdServer.CompleteMO(inReplyTo, body) {
				slog.Debug("ISC result had no pending SGd MO match", "in_reply_to", inReplyTo)
			}
		},
		Client:   sipClient,
		SIPLocal: sipLocalURI,
		Settings: iscSettings,
	}
	regHandler := &isc.RegisterHandler{Registry: reg}

	sipSrv.OnRequest(sip.REGISTER, regHandler.HandleRegister)
	sipSrv.OnRequest(sip.NOTIFY, regHandler.HandleNotify)

	// SIP SIMPLE handler: inter-site peers
	simpleHandler := &simple.MessageHandler{
		OnMessage: func(msg *codec.Message) {
			fwd.Dispatch(ctx, msg)
		},
	}

	// Route SIP MESSAGE by Content-Type.
	sipSrv.OnRequest(sip.MESSAGE, func(req *sip.Request, tx sip.ServerTransaction) {
		ct := ""
		if ctHdr := req.GetHeader("Content-Type"); ctHdr != nil {
			ct = ctHdr.Value()
		}
		if len(ct) >= 19 && ct[:19] == "application/vnd.3gp" {
			msgHandler.Handle(req, tx)
		} else {
			simpleHandler.Handle(req, tx)
		}
	})

	// Hot-reload: sip_peers
	sipPeersCh := make(chan store.ChangeEvent, 8)
	if err := st.Subscribe(ctx, "sip_peers", sipPeersCh); err != nil {
		return fmt.Errorf("subscribe sip_peers: %w", err)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-sipPeersCh:
				slog.Debug("sip_peers changed")
			}
		}
	}()

	// ── Diameter HSS clients (Sh and S6c may be separate or shared) ────────────
	type desiredDiameterPeer struct {
		peer   store.DiameterPeer
		apps   []string
		hasSGd bool
	}
	activeHSSPeers := map[string]*diameter.Peer{}
	activeHSSHasSGd := map[string]bool{}
	activeShClients := map[string]*sh.Client{}
	activeS6cClients := map[string]*s6c.Client{}
	const diameterGracefulStopTimeout = 5 * time.Second

	diameterPeerRuntimeEqual := func(a, b store.DiameterPeer) bool {
		return a.Name == b.Name &&
			a.Host == b.Host &&
			a.Realm == b.Realm &&
			a.Port == b.Port &&
			a.Transport == b.Transport &&
			slices.Equal(a.Applications, b.Applications) &&
			a.Enabled == b.Enabled
	}

	triggerAlertRetry := func(source, msisdn, imsi, alertCorrelationID, deferredInterface string) error {
		if alertCorrelationID == "" && msisdn == "" {
			return fmt.Errorf("%s ALR missing subscriber identity", source)
		}

		filter := store.MessageFilter{
			Statuses: []string{
				store.MessageStatusQueued,
				store.MessageStatusWaitTimer,
				store.MessageStatusWaitEvent,
				store.MessageStatusWaitTimerEvent,
			},
			AlertCorrelationID: alertCorrelationID,
			DstMSISDN:          msisdn,
			DeferredInterface:  deferredInterface,
			Limit:              1000,
		}
		if alertCorrelationID != "" {
			filter.DstMSISDN = ""
		}

		msgs, err := st.ListFilteredMessages(ctx, filter)
		if err != nil {
			return fmt.Errorf("list deferred messages for %s: %w", msisdn, err)
		}
		if len(msgs) == 0 && alertCorrelationID != "" && msisdn != "" {
			fallbackFilter := store.MessageFilter{
				Statuses: []string{
					store.MessageStatusQueued,
					store.MessageStatusWaitTimer,
					store.MessageStatusWaitEvent,
					store.MessageStatusWaitTimerEvent,
				},
				DstMSISDN:         msisdn,
				DeferredInterface: deferredInterface,
				Limit:             1000,
			}
			msgs, err = st.ListFilteredMessages(ctx, fallbackFilter)
			if err != nil {
				return fmt.Errorf("list deferred fallback messages for %s: %w", msisdn, err)
			}
		}

		var requeued int
		var skipped int
		now := time.Now()
		for _, msg := range msgs {
			deferredReason := ""
			if source == "sgd" && deferredInterface == string(codec.InterfaceSGd) {
				deferredReason = "sgd_alert_retry"
			}
			ok, err := st.RequeueMessageForAlert(ctx, msg.ID, now, msg.RouteCursor, deferredReason, []string{
				store.MessageStatusQueued,
				store.MessageStatusWaitTimer,
				store.MessageStatusWaitEvent,
				store.MessageStatusWaitTimerEvent,
			})
			if err != nil {
				slog.Warn(source+" ALR requeue failed", "message_id", msg.ID, "msisdn", msisdn, "alert_correlation_id", alertCorrelationID, "err", err)
				continue
			}
			if !ok {
				skipped++
				slog.Debug(source+" ALR stale or already claimed", "message_id", msg.ID, "msisdn", msisdn, "alert_correlation_id", alertCorrelationID)
				continue
			}
			requeued++
		}

		slog.Info(source+" ALR processed",
			"msisdn", msisdn,
			"imsi", imsi,
			"alert_correlation_id", alertCorrelationID,
			"deferred_interface", deferredInterface,
			"requeued", requeued,
			"skipped", skipped,
		)
		return nil
	}
	triggerAlertSCRetry := func(req s6c.AlertServiceCentreRequest) error {
		return triggerAlertRetry("s6c", req.MSISDN, req.IMSI, req.AlertCorrelationID, "")
	}
	triggerSGdAlertRetry := func(req sgd.AlertServiceCentreRequest) error {
		return triggerAlertRetry("sgd", req.MSISDN, req.IMSI, req.AlertCorrelationID, string(codec.InterfaceSGd))
	}
	sgdServer.SetOnAlertServiceCentre(triggerSGdAlertRetry)

	syncHSSPeers := func() {
		peers, err := st.ListDiameterPeers(ctx)
		if err != nil {
			slog.Error("reload diameter peers", "err", err)
			return
		}

		var wantSh *store.DiameterPeer
		var wantS6c *store.DiameterPeer
		for i := range peers {
			if !peers[i].Enabled {
				continue
			}
			if wantSh == nil && slices.Contains(peers[i].Applications, "sh") {
				wantSh = &peers[i]
			}
			if wantS6c == nil && slices.Contains(peers[i].Applications, "s6c") {
				wantS6c = &peers[i]
			}
		}

		desired := map[string]desiredDiameterPeer{}
		addDesired := func(dp *store.DiameterPeer, app string) {
			if dp == nil {
				return
			}
			entry, ok := desired[dp.Name]
			if !ok {
				entry = desiredDiameterPeer{
					peer:   *dp,
					apps:   []string{},
					hasSGd: slices.Contains(dp.Applications, "sgd"),
				}
			}
			if !slices.Contains(entry.apps, app) {
				entry.apps = append(entry.apps, app)
			}
			if entry.hasSGd && !slices.Contains(entry.apps, "sgd") {
				entry.apps = append(entry.apps, "sgd")
			}
			desired[dp.Name] = entry
		}
		addDesired(wantSh, "sh")
		addDesired(wantS6c, "s6c")

		for name, peer := range activeHSSPeers {
			want, ok := desired[name]
			if ok && !shouldReplaceDiameterPeer(peer, activeHSSHasSGd[name], &want.peer, want.hasSGd, want.apps) {
				continue
			}
			if activeHSSHasSGd[name] {
				sgdServer.DetachOutboundPeer(name)
			}
			peer.StopGraceful(diameterGracefulStopTimeout)
			delete(activeHSSPeers, name)
			delete(activeHSSHasSGd, name)
			delete(activeShClients, name)
			delete(activeS6cClients, name)
		}

		for name, want := range desired {
			if _, ok := activeHSSPeers[name]; ok {
				continue
			}

			primaryApp := "sh"
			primaryAppID := dcodec.App3GPP_Sh
			if !slices.Contains(want.apps, "sh") && slices.Contains(want.apps, "s6c") {
				primaryApp = "s6c"
				primaryAppID = dcodec.App3GPP_S6c
			}
			peerApps := appIDsForDiameterPeer(want.apps)
			peerCfg := diameter.Config{
				Name:        want.peer.Name,
				Host:        want.peer.Host,
				Port:        want.peer.Port,
				Transport:   want.peer.Transport,
				Application: primaryApp,
				AppID:       primaryAppID,
				AppIDs:      peerApps,
				LocalFQDN:   cfg.Diameter.LocalFQDN,
				LocalRealm:  cfg.Diameter.LocalRealm,
				PeerFQDN:    want.peer.Host,
				PeerRealm:   want.peer.Realm,
			}
			p := diameter.NewPeer(peerCfg)
			if want.hasSGd {
				sgdServer.AttachOutboundPeer(want.peer.Name, p)
			}
			if slices.Contains(want.apps, "sh") {
				shClient := sh.NewClient(p)
				shClient.SetMSISDNEncoding(cfg.SMSC.ShMSISDNEncoding)
				activeShClients[name] = shClient
			}
			if slices.Contains(want.apps, "s6c") {
				s6cClient := s6c.NewClient(p, scAddr)
				s6cClient.SetOnAlertServiceCentre(triggerAlertSCRetry)
				activeS6cClients[name] = s6cClient
			}
			p.Start(ctx)
			activeHSSPeers[name] = p
			activeHSSHasSGd[name] = want.hasSGd
			slog.Info("diameter HSS client started", "peer", want.peer.Name, "host", want.peer.Host, "applications", want.apps)
		}

		reg.SetShClient(nil)
		if wantSh != nil {
			if client := activeShClients[wantSh.Name]; client != nil {
				reg.SetShClient(client)
			}
		}

		reg.SetS6cClient(nil)
		correlator.SetS6cReporter(nil)
		if wantS6c != nil {
			if client := activeS6cClients[wantS6c.Name]; client != nil {
				reg.SetS6cClient(client)
				correlator.SetS6cReporter(client)
			}
		}
	}

	// syncSGdPeers starts outbound connections to all enabled SGd peers and
	// stops connections for peers that are no longer present/enabled.
	activeSGd := map[string]bool{}
	activeSGdCfg := map[string]store.DiameterPeer{}
	syncSGdPeers := func() {
		peers, err := st.ListDiameterPeers(ctx)
		if err != nil {
			slog.Error("reload sgd peers", "err", err)
			return
		}
		want := map[string]bool{}
		for _, dp := range peers {
			if slices.Contains(dp.Applications, "sgd") && dp.Enabled {
				if activeHSSPeers[dp.Name] != nil && activeHSSHasSGd[dp.Name] {
					want[dp.Name] = true
					if activeSGd[dp.Name] {
						sgdServer.RemoveOutboundPeer(dp.Name)
						delete(activeSGd, dp.Name)
						delete(activeSGdCfg, dp.Name)
					}
					continue
				}
				want[dp.Name] = true
				if activeSGd[dp.Name] && diameterPeerRuntimeEqual(activeSGdCfg[dp.Name], dp) {
					continue
				}
				if activeSGd[dp.Name] {
					sgdServer.RemoveOutboundPeer(dp.Name)
				}
				if !activeSGd[dp.Name] {
					activeSGd[dp.Name] = true
				}
				sgdServer.AddOutboundPeer(ctx, dp.Name, dp.Host, dp.Port, dp.Transport, dp.Host, dp.Realm)
				activeSGdCfg[dp.Name] = dp
			}
		}
		// Remove peers that were deleted or disabled.
		for name := range activeSGd {
			if !want[name] {
				sgdServer.RemoveOutboundPeer(name)
				delete(activeSGd, name)
				delete(activeSGdCfg, name)
			}
		}
	}

	syncHSSPeers()
	syncSGdPeers()

	// Hot-reload: diameter_peers
	diamPeersCh := make(chan store.ChangeEvent, 8)
	if err := st.Subscribe(ctx, "diameter_peers", diamPeersCh); err != nil {
		return fmt.Errorf("subscribe diameter_peers: %w", err)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-diamPeersCh:
				slog.Info("diameter_peers changed — reloading")
				syncHSSPeers()
				syncSGdPeers()
			}
		}
	}()

	// ── Metrics gauge updater ─────────────────────────────────────────────────
	go func() {
		updateGauges := func() {
			// Count bound SMPP sessions
			var smppBound float64
			for _, link := range smppReg.All() {
				if link.State() == smpp.StateBound {
					smppBound++
				}
			}
			m.SMPPSessions.Set(smppBound)
			// Count open Diameter peers
			var diamOpen float64
			hssPeers := make([]*diameter.Peer, 0, len(activeHSSPeers))
			for _, peer := range activeHSSPeers {
				hssPeers = append(hssPeers, peer)
			}
			for _, peer := range aggregateDiameterRuntimePeers(sgdServer.PeerStatuses(), hssPeers) {
				if peer.State == "OPEN" {
					diamOpen++
				}
			}
			m.DiameterPeers.Set(diamOpen)
			// SIP SIMPLE peers: count enabled peers from sip_peers table.
			if peers, err := st.ListSIPPeers(ctx); err == nil {
				var active float64
				for _, p := range peers {
					if p.Enabled {
						active++
					}
				}
				m.SIPPeers.Set(active)
			}
			// Store-and-forward queue depth and expired count (DB-authoritative).
			if counts, err := st.CountMessagesByStatus(ctx); err == nil {
				queuedInFlight := counts[store.MessageStatusQueued] +
					counts[store.MessageStatusDispatched] +
					counts[store.MessageStatusWaitTimer] +
					counts[store.MessageStatusWaitEvent] +
					counts[store.MessageStatusWaitTimerEvent]
				m.SFQueued.Set(float64(queuedInFlight))
				m.SFExpired.Set(float64(counts[store.MessageStatusExpired]))
			}
		}

		updateGauges()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				updateGauges()
			}
		}
	}()

	// ── REST API server ───────────────────────────────────────────────────────
	apiSrv := api.New(st, version)
	apiSrv.SetPeerStatusFunc(func() []api.PeerInfo {
		var out []api.PeerInfo
		smppAccountNames := map[string]string{}
		if accounts, err := st.ListSMPPServerAccounts(context.Background()); err == nil {
			for _, acc := range accounts {
				if acc.Name != "" {
					smppAccountNames[acc.SystemID] = acc.Name
				}
			}
		}
		// SMPP links (server-side ESMEs and client-side sessions)
		for _, link := range smppReg.All() {
			t := "smpp_server"
			if link.Mode == "client" {
				t = "smpp_client"
			}
			name := link.Name
			if t == "smpp_server" {
				if mapped := smppAccountNames[link.SystemID]; mapped != "" {
					name = mapped
				} else if name == "" {
					name = link.SystemID
				}
			}
			at := link.ConnectedAt
			out = append(out, api.PeerInfo{
				Name:        name,
				Type:        t,
				State:       link.State().String(),
				Transport:   link.Transport,
				SystemID:    link.SystemID,
				BindType:    link.BindType,
				RemoteAddr:  link.RemoteAddr,
				ConnectedAt: &at,
			})
		}
		hssPeers := make([]*diameter.Peer, 0, len(activeHSSPeers))
		for _, peer := range activeHSSPeers {
			hssPeers = append(hssPeers, peer)
		}
		out = append(out, diameterRuntimePeerInfo(aggregateDiameterRuntimePeers(sgdServer.PeerStatuses(), hssPeers))...)
		// IMS registrations — show each active UE as a sip_ims peer
		if regs, err := st.ListIMSRegistrations(context.Background()); err == nil {
			for _, r := range regs {
				if !r.Registered || time.Now().After(r.Expiry) {
					continue
				}
				updatedAt := r.UpdatedAt
				expiryAt := r.Expiry
				out = append(out, api.PeerInfo{
					Name:        r.MSISDN,
					ConnectedAt: &updatedAt,
					ExpiryAt:    &expiryAt,
					Type:        "sip_ims",
					State:       "REGISTERED",
					SystemID:    r.SIPAOR,
					RemoteAddr:  r.ContactURI,
					Application: r.SCSCF,
				})
			}
		}
		return out
	})

	// ── Start listeners ───────────────────────────────────────────────────────
	errCh := make(chan error, 5)

	go func() { errCh <- sipSrv.ListenAndServe(ctx, cfg.SIP.Transport, cfg.SIP.Listen) }()
	go func() { errCh <- smppListener.ListenAndServe(ctx) }()
	if smppTLSListener != nil {
		go func() { errCh <- smppTLSListener.ListenAndServe(ctx) }()
	}
	go func() { errCh <- sgdServer.ListenAndServe(ctx) }()
	go func() { errCh <- apiSrv.Start(ctx, cfg.API.Listen) }()

	slog.Info("VectorCore SMSC started",
		"sip", cfg.SIP.Listen,
		"smpp", cfg.SMPP.Server.Listen,
		"smpp_tls", cfg.SMPPServerTLSListen(),
		"diameter", cfg.Diameter.Listen,
		"api", cfg.API.Listen,
		"db", cfg.Database.Driver,
		"version", version,
	)

	select {
	case <-ctx.Done():
		slog.Info("shutting down")
		shutdownCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
		defer sc()
		_ = shutdownCtx
		return nil
	case err := <-errCh:
		return err
	}
}

func openStore(ctx context.Context, cfg *config.Config) (store.Store, error) {
	switch cfg.Database.Driver {
	case "postgres":
		return postgresstore.Open(ctx, cfg.Database.DSN)
	case "sqlite":
		return sqlitestore.Open(ctx, cfg.Database.DSN, cfg.Database.PollInterval)
	default:
		return nil, fmt.Errorf("unsupported database driver: %q", cfg.Database.Driver)
	}
}

// setupLogging configures the global slog logger.
// debug flag takes precedence; log.level in config is a secondary override.
// If logFile is non-empty the logger tees output to that file as well as stderr.
// Returns a closer that must be called on exit, or nil.
func setupLogging(debug bool, logFile, logLevel string) (closer func()) {
	level := slog.LevelInfo
	switch logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	if debug {
		level = slog.LevelDebug // -d flag always wins
	}

	var w io.Writer = os.Stderr
	if logFile != "" {
		if err := os.MkdirAll(filepath.Dir(logFile), 0755); err == nil {
			f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err == nil {
				w = io.MultiWriter(os.Stderr, f)
				closer = func() { f.Close() }
			} else {
				fmt.Fprintf(os.Stderr, "warning: cannot open log file %q: %v\n", logFile, err)
			}
		} else {
			fmt.Fprintf(os.Stderr, "warning: cannot create log dir for %q: %v\n", logFile, err)
		}
	}

	h := slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))
	return closer
}
