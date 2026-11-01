// Package forwarder dispatches canonical Messages to egress interfaces based
// on routing decisions, persists them for store-and-forward, and triggers
// delivery report generation on completion.
package forwarder

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	smppcodec "github.com/svinson1121/vectorcore-smsc/internal/codec/smpp"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/diameter/s6c"
	diametersgd "github.com/svinson1121/vectorcore-smsc/internal/diameter/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/metrics"
	"github.com/svinson1121/vectorcore-smsc/internal/numbering"
	"github.com/svinson1121/vectorcore-smsc/internal/registry"
	"github.com/svinson1121/vectorcore-smsc/internal/routing"
	"github.com/svinson1121/vectorcore-smsc/internal/sgdmap"
	"github.com/svinson1121/vectorcore-smsc/internal/smpp"
	smppClient "github.com/svinson1121/vectorcore-smsc/internal/smpp/client"
	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

// ISCSender sends MT SMS to IMS UEs via the SIP ISC interface.
type ISCSender interface {
	Send(ctx context.Context, msg *codec.Message, reg *registry.Registration) error
}

// SimpleSender sends MT SMS to inter-site SIP SIMPLE peers.
type SimpleSender interface {
	Send(ctx context.Context, msg *codec.Message, peer store.SIPPeer) error
}

// SGdSender sends MT SMS to an LTE UE via the Diameter SGd interface.
type SGdSender interface {
	SendOFR(ctx context.Context, msg *codec.Message, mmeHost, scAddr string) error
}

// DeliveryReporter records terminal outcomes once delivery is complete.
type DeliveryReporter interface {
	Report(ctx context.Context, m store.Message, status string)
}

// deferredReporter is implemented by reporters that tell the HSS (RSDS)
// when an SGd delivery waits for an unreachable subscriber. cause is an
// SM-Delivery-Cause (absent user or memory capacity exceeded).
type deferredReporter interface {
	ReportDeferred(ctx context.Context, m store.Message, cause uint32)
}

// sgdBusyRetryDelay is the retry delay after the MME reports another MT SMS in
// progress for the subscriber. Busy retries don't consume the retry budget.
const sgdBusyRetryDelay = 10 * time.Second

type selectedRoute struct {
	routeIndex  int
	egressIface string
	egressPeer  string
	sgdIMSI     string
	sgdMMENum   string
	imsReg      *registry.Registration
	sfPolicyID  string
	ruleName    string
	priority    int
}

type routeOutcome int

const (
	routeOutcomeDelivered routeOutcome = iota
	routeOutcomeTryNext
	routeOutcomeWaitTimer
	routeOutcomeWaitEvent
	routeOutcomePermanent
	// routeOutcomeBusy: the destination has another MT in progress; retry soon.
	routeOutcomeBusy
	// routeOutcomeRejected: a definitive refusal for this subscriber/message
	// (e.g. the handset rejected the TPDU). Ends routing immediately as FAILED,
	// even when other candidates were merely unavailable.
	routeOutcomeRejected
)

type passOutcome int

const (
	passOutcomeDelivered passOutcome = iota
	passOutcomeWaitTimer
	passOutcomeWaitEvent
	passOutcomeWaitTimerEvent
	passOutcomeFailed
)

type routingPassResult struct {
	outcome        passOutcome
	deliveredRoute *selectedRoute
	lastAttempted  *selectedRoute
	eventRoute     *selectedRoute
	timerRoute     *selectedRoute
	lastErr        error
	// retryAfter overrides the timer-retry delay (and is not counted as a
	// retry) when set, e.g. after an SGd busy answer.
	retryAfter time.Duration
}

// Forwarder dispatches messages to the correct egress interface.
type Forwarder struct {
	reg                 *registry.Registry
	engine              *routing.Engine
	st                  store.Store
	scAddr              string // Our SMSC address for RP-OA / SGd SC-Address
	m                   *metrics.M
	sgdMapper           *sgdmap.Mapper // optional; nil = no MME name translation
	maxQueueLifetime    time.Duration
	defaultCountryCode  string
	localNationalLength int

	// Egress senders — all optional; nil means that interface is not available.
	iscSender    ISCSender
	simpleSender SimpleSender
	smppMgr      *smppClient.Manager
	sgdSender    SGdSender
	sgdDestLocks keyedLocks // serialises SGd MT per destination
	reporter     DeliveryReporter
}

// Config holds the dependencies wired into the Forwarder.
type Config struct {
	Registry            *registry.Registry
	Engine              *routing.Engine
	Store               store.Store
	SCAddr              string
	Metrics             *metrics.M
	ISCSender           ISCSender
	SimpleSender        SimpleSender
	SMPPManager         *smppClient.Manager
	SGdSender           SGdSender
	Reporter            DeliveryReporter
	SGDMapper           *sgdmap.Mapper // optional S6c→SGd MME name translator
	MaxQueueLifetime    time.Duration
	DefaultCountryCode  string
	LocalNationalLength int
}

// New creates a Forwarder.
func New(cfg Config) *Forwarder {
	return &Forwarder{
		reg:                 cfg.Registry,
		engine:              cfg.Engine,
		st:                  cfg.Store,
		scAddr:              cfg.SCAddr,
		m:                   cfg.Metrics,
		sgdMapper:           cfg.SGDMapper,
		maxQueueLifetime:    cfg.MaxQueueLifetime,
		defaultCountryCode:  cfg.DefaultCountryCode,
		localNationalLength: cfg.LocalNationalLength,
		iscSender:           cfg.ISCSender,
		simpleSender:        cfg.SimpleSender,
		smppMgr:             cfg.SMPPManager,
		sgdSender:           cfg.SGdSender,
		reporter:            cfg.Reporter,
	}
}

// Dispatch routes msg to the correct egress interface.
// Every message is written to the database before the network send, so a crash
// between receipt and delivery leaves the message in QUEUED state for retry.
// On success the record is updated to DELIVERED; on non-terminal outcomes the
// message moves into one of the waiting states and is later resumed by the
// retry scheduler or alert-triggered wakeup path.
func (f *Forwarder) Dispatch(ctx context.Context, msg *codec.Message) {
	if msg.ID == "" {
		msg.ID = newUUID()
	}
	if msg.CorrelationID == "" {
		msg.CorrelationID = defaultAlertCorrelationID(msg.ID)
	}
	if f.m != nil {
		f.m.MessagesIn.WithLabelValues(string(msg.IngressInterface)).Inc()
	}

	f.normalizeIngressRecipient(msg)

	now := time.Now()
	f.persistMessage(ctx, msg, "", "", store.MessageStatusDispatched, now)

	result := f.runRoutingPass(ctx, msg, 0)
	f.finalizeRoutingPass(ctx, msg, now, 0, 1, result, "forwarder")
}

func (f *Forwarder) normalizeIngressRecipient(msg *codec.Message) {
	if msg == nil {
		return
	}
	raw := msg.Destination.MSISDN
	normalized := numbering.NormalizeRecipientMSISDN(raw, msg.Destination.TON, f.defaultCountryCode, f.localNationalLength)
	msg.Destination.MSISDN = normalized
	slog.Debug("forwarder: normalized ingress recipient MSISDN",
		"raw_tp_da_digits", raw,
		"tp_da_ton", msg.Destination.TON,
		"tp_da_npi", msg.Destination.NPI,
		"default_country_code", f.defaultCountryCode,
		"local_national_length", f.localNationalLength,
		"normalized_recipient_msisdn", normalized,
	)
}

func (f *Forwarder) tryRoutes(ctx context.Context, msg *codec.Message, startCursor int) (*selectedRoute, *selectedRoute, error) {
	result := f.runRoutingPass(ctx, msg, startCursor)
	if result.outcome == passOutcomeDelivered {
		return result.deliveredRoute, result.deliveredRoute, nil
	}
	if result.lastErr != nil {
		return nil, result.lastAttempted, result.lastErr
	}
	return nil, result.lastAttempted, fmt.Errorf("routing pass ended with outcome %d", result.outcome)
}

func (f *Forwarder) runRoutingPass(ctx context.Context, msg *codec.Message, startCursor int) routingPassResult {
	totalCandidates := f.candidateCount(msg)
	if totalCandidates <= 0 {
		return routingPassResult{outcome: passOutcomeWaitTimer}
	}

	decisions := f.fallbackDecisions(msg)
	base := modulo(startCursor, totalCandidates)
	var sawRetry bool
	var sawPermanent bool
	var sawBusy bool
	// The IMS candidates (0, 1) being unavailable is retryable, since the
	// subscriber may register later, unless the HSS doesn't know the
	// subscriber at all, which rules out IMS as well.
	var sawIMSUnavailable bool
	var sawUnknownSubscriber bool
	var lastErr error
	var lastAttempted *selectedRoute

	for offset := 0; offset < totalCandidates; offset++ {
		routeIndex := (base + offset) % totalCandidates
		route, reason := f.resolveCandidateAt(ctx, msg, decisions, routeIndex)
		if reason != "" {
			slog.Debug("forwarder: routing candidate skipped",
				"dst", msg.Destination.MSISDN,
				"route_index", routeIndex,
				"reason", reason,
			)
			if planned := f.plannedRouteAt(msg, routeIndex); planned != nil {
				lastAttempted = planned
				lastErr = errors.New(reason)
			}
			switch {
			case reason == sgdReasonUnknownSubscriber:
				// Definitive for SGd: try the next route, but don't retry this one.
				sawPermanent = true
				sawUnknownSubscriber = true
			case routeIndex < 2:
				sawIMSUnavailable = true
			default:
				sawRetry = true
			}
			continue
		}

		slog.Debug("forwarder: routing candidate selected",
			"dst", msg.Destination.MSISDN,
			"route_index", route.routeIndex,
			"rule", route.ruleName,
			"priority", route.priority,
			"egress", route.egressIface,
			"peer", route.egressPeer,
		)

		outcome, err := f.attemptRoute(ctx, msg, route)
		switch outcome {
		case routeOutcomeDelivered:
			return routingPassResult{
				outcome:        passOutcomeDelivered,
				deliveredRoute: route,
				lastAttempted:  route,
			}
		case routeOutcomeWaitEvent:
			pass := passOutcomeWaitEvent
			if sawRetry || sawPermanent || sawIMSUnavailable {
				pass = passOutcomeWaitTimerEvent
			}
			return routingPassResult{
				outcome:       pass,
				eventRoute:    route,
				lastAttempted: route,
				lastErr:       err,
			}
		case routeOutcomeWaitTimer:
			sawRetry = true
			lastAttempted = route
			lastErr = err
		case routeOutcomePermanent:
			sawPermanent = true
			lastAttempted = route
			lastErr = err
		case routeOutcomeBusy:
			sawRetry = true
			sawBusy = true
			lastAttempted = route
			lastErr = err
		case routeOutcomeRejected:
			return routingPassResult{
				outcome:       passOutcomeFailed,
				lastAttempted: route,
				lastErr:       err,
			}
		case routeOutcomeTryNext:
			sawRetry = true
			lastAttempted = route
			lastErr = err
		}
	}

	if sawIMSUnavailable && !sawUnknownSubscriber {
		sawRetry = true
	}
	switch {
	case sawRetry:
		result := routingPassResult{
			outcome:       passOutcomeWaitTimer,
			timerRoute:    lastAttempted,
			lastAttempted: lastAttempted,
			lastErr:       lastErr,
		}
		if sawBusy {
			result.retryAfter = sgdBusyRetryDelay
		}
		return result
	case sawPermanent:
		return routingPassResult{
			outcome:       passOutcomeFailed,
			lastAttempted: lastAttempted,
			lastErr:       lastErr,
		}
	default:
		return routingPassResult{
			outcome: passOutcomeFailed,
			lastErr: fmt.Errorf("no usable route for dst=%s", msg.Destination.MSISDN),
		}
	}
}

func (f *Forwarder) attemptRoute(ctx context.Context, msg *codec.Message, route *selectedRoute) (routeOutcome, error) {
	msg.EgressInterface = codec.InterfaceType(route.egressIface)
	restore := f.applyRoutePolicy(ctx, msg, route)
	defer restore()
	if err := f.deliverSelectedRoute(ctx, msg, route); err != nil {
		outcome := classifyRouteError(route, err)
		slog.Warn("forwarder: candidate delivery failed",
			"dst", msg.Destination.MSISDN,
			"route_index", route.routeIndex,
			"egress", route.egressIface,
			"peer", route.egressPeer,
			"outcome", outcome,
			"err", err,
		)
		return outcome, err
	}
	return routeOutcomeDelivered, nil
}

func (f *Forwarder) tryRouteAt(ctx context.Context, msg *codec.Message, routeCursor int) (*selectedRoute, error) {
	decisions := f.fallbackDecisions(msg)
	totalCandidates := len(decisions) + 3
	if totalCandidates == 0 {
		return nil, fmt.Errorf("no usable route for dst=%s", msg.Destination.MSISDN)
	}
	routeIndex := modulo(routeCursor, totalCandidates)
	route, reason := f.resolveCandidateAt(ctx, msg, decisions, routeIndex)
	if reason != "" || route == nil {
		if reason == "" {
			reason = "candidate unavailable"
		}
		return nil, fmt.Errorf("route[%d] unavailable: %s", routeIndex, reason)
	}
	msg.EgressInterface = codec.InterfaceType(route.egressIface)
	if err := f.deliverSelectedRoute(ctx, msg, route); err != nil {
		return route, err
	}
	return route, nil
}

func (f *Forwarder) persistDeliveredRoute(ctx context.Context, messageID string, egress codec.InterfaceType, peer string) error {
	if messageID == "" || egress == "" {
		return nil
	}
	return f.st.UpdateMessageRouting(ctx, messageID, string(egress), peer)
}

func (f *Forwarder) finalizeRoutingPass(ctx context.Context, msg *codec.Message, now time.Time, startCursor, nextRetryCount int, result routingPassResult, logPrefix string) {
	switch result.outcome {
	case passOutcomeDelivered:
		route := result.deliveredRoute
		if route == nil {
			route = result.lastAttempted
		}
		if route != nil {
			if err := f.persistDeliveredRoute(ctx, msg.ID, msg.EgressInterface, route.egressPeer); err != nil {
				slog.Error(logPrefix+": persist delivered route failed", "id", msg.ID, "err", err)
			}
		}
		_ = f.st.UpdateMessageStatus(ctx, msg.ID, store.MessageStatusDelivered)
		if f.reporter != nil {
			if stored, err := f.st.GetMessage(ctx, msg.ID); err == nil && stored != nil {
				f.reporter.Report(ctx, *stored, "DELIVRD")
			}
		}
	case passOutcomeWaitTimer:
		slog.Warn(logPrefix+": no usable route",
			"dst", msg.Destination.MSISDN,
			"src_iface", msg.IngressInterface,
			"err", result.lastErr,
		)
		f.markDeferred(ctx, msg.ID, msg, startCursor, result.lastAttempted)
		f.applyDeferredExpiryCap(ctx, msg.ID, now, result.lastAttempted)
		next := now.Add(30 * time.Second)
		retryCount := nextRetryCount
		if result.retryAfter > 0 {
			next = now.Add(result.retryAfter)
			retryCount = nextRetryCount - 1 // busy retries don't consume the budget
		}
		routeCursor := deferredRouteCursor(result.lastAttempted)
		if err := f.st.UpdateMessageRetry(ctx, msg.ID, retryCount, next, routeCursor); err != nil {
			slog.Error(logPrefix+": schedule retry failed", "id", msg.ID, "err", err)
		}
	case passOutcomeWaitEvent:
		f.markDeferred(ctx, msg.ID, msg, startCursor, result.eventRoute)
		f.applyDeferredExpiryCap(ctx, msg.ID, now, result.eventRoute)
		if err := f.st.UpdateMessageStatus(ctx, msg.ID, store.MessageStatusWaitEvent); err != nil {
			slog.Error(logPrefix+": mark wait event failed", "id", msg.ID, "err", err)
		}
		f.reportSGdAbsent(ctx, msg.ID, "", result.eventRoute, result.lastErr)
	case passOutcomeWaitTimerEvent:
		f.markDeferred(ctx, msg.ID, msg, startCursor, result.eventRoute)
		f.applyDeferredExpiryCap(ctx, msg.ID, now, result.eventRoute)
		next := now.Add(30 * time.Second)
		routeCursor := deferredRouteCursor(result.eventRoute)
		if err := f.st.UpdateMessageRetry(ctx, msg.ID, nextRetryCount, next, routeCursor); err != nil {
			slog.Error(logPrefix+": schedule timer-event retry failed", "id", msg.ID, "err", err)
			return
		}
		if err := f.st.UpdateMessageStatus(ctx, msg.ID, store.MessageStatusWaitTimerEvent); err != nil {
			slog.Error(logPrefix+": mark wait timer-event failed", "id", msg.ID, "err", err)
		}
		f.reportSGdAbsent(ctx, msg.ID, "", result.eventRoute, result.lastErr)
	case passOutcomeFailed:
		slog.Warn(logPrefix+": routing failed permanently",
			"dst", msg.Destination.MSISDN,
			"src_iface", msg.IngressInterface,
			"err", result.lastErr,
		)
		_ = f.st.UpdateMessageStatus(ctx, msg.ID, store.MessageStatusFailed)
		f.reportFailed(ctx, msg.ID, result.lastErr)
	}
}

// reportSGdAbsent tells the HSS that an SGd delivery of messageID is waiting
// for an unreachable subscriber, so it stores MWD and alerts the SMSC later.
// priorDeferredReason is the message's deferred_reason before this attempt;
// "sgd_delivery" means the HSS was already told during this absent episode,
// so retries don't repeat the RSDS. After an alert retry ("sgd_alert_retry")
// the HSS has cleared its MWD, so a renewed failure is reported again.
func (f *Forwarder) reportSGdAbsent(ctx context.Context, messageID, priorDeferredReason string, route *selectedRoute, deliveryErr error) {
	if route == nil || route.egressIface != string(codec.InterfaceSGd) || priorDeferredReason == "sgd_delivery" {
		return
	}
	reporter, ok := f.reporter.(deferredReporter)
	if !ok {
		return
	}
	stored, err := f.st.GetMessage(ctx, messageID)
	if err != nil || stored == nil {
		slog.Warn("forwarder: RSDS absent skipped, message not found", "id", messageID, "err", err)
		return
	}
	reporter.ReportDeferred(ctx, *stored, deferredDeliveryCause(deliveryErr))
}

// deferredDeliveryCause maps the SGd failure that deferred delivery to the
// SM-Delivery-Cause reported to the HSS: memory capacity exceeded (HSS sets
// MCEF and alerts on memory available) or absent user (MNRF, alert on attach).
func deferredDeliveryCause(err error) uint32 {
	var ofaErr *diametersgd.OFAResultError
	if errors.As(err, &ofaErr) && ofaErr.Experimental &&
		ofaErr.ResultCode == diametersgd.ResultErrorSMDeliveryFailure &&
		ofaErr.DeliveryFailureCause != nil &&
		*ofaErr.DeliveryFailureCause == diametersgd.DeliveryFailureMemoryCapacityExceeded {
		return dcodec.SMDeliveryCauseMemoryCapacityExceeded
	}
	return dcodec.SMDeliveryCauseAbsentUser
}

func (f *Forwarder) deferredLifetimeForRoute(ctx context.Context, route *selectedRoute) time.Duration {
	lifetime := f.maxQueueLifetime
	pol := f.routePolicy(ctx, route)
	if pol != nil && pol.MaxTTL > 0 {
		return pol.MaxTTL
	}
	return lifetime
}

func (f *Forwarder) applyDeferredExpiryCap(ctx context.Context, messageID string, submittedAt time.Time, route *selectedRoute) {
	if messageID == "" {
		return
	}
	var expiryAt *time.Time
	if lifetime := f.deferredLifetimeForRoute(ctx, route); lifetime > 0 {
		t := submittedAt.Add(lifetime)
		expiryAt = &t
	}
	if pol := f.routePolicy(ctx, route); pol != nil && pol.VPOverride != nil && *pol.VPOverride > 0 {
		t := submittedAt.Add(*pol.VPOverride)
		if expiryAt == nil || t.Before(*expiryAt) {
			expiryAt = &t
		}
	}
	if expiryAt == nil {
		return
	}
	if err := f.st.UpdateMessageExpiryCap(ctx, messageID, *expiryAt); err != nil {
		slog.Warn("forwarder: update expiry cap failed", "id", messageID, "err", err)
	}
}

func (f *Forwarder) routePolicy(ctx context.Context, route *selectedRoute) *store.SFPolicy {
	if route == nil || route.sfPolicyID == "" {
		return nil
	}
	pol, err := f.st.GetSFPolicy(ctx, route.sfPolicyID)
	if err != nil || pol == nil {
		return nil
	}
	return pol
}

func (f *Forwarder) applyRoutePolicy(ctx context.Context, msg *codec.Message, route *selectedRoute) func() {
	pol := f.routePolicy(ctx, route)
	if pol == nil || pol.VPOverride == nil {
		return func() {}
	}
	original := msg.ValidityPeriod
	override := *pol.VPOverride
	msg.ValidityPeriod = &override
	return func() {
		msg.ValidityPeriod = original
	}
}

func (f *Forwarder) plannedRouteAt(msg *codec.Message, startCursor int) *selectedRoute {
	decisions := f.fallbackDecisions(msg)
	totalCandidates := len(decisions) + 3
	if totalCandidates == 0 {
		return nil
	}
	routeIndex := modulo(startCursor, totalCandidates)
	switch {
	case routeIndex == 0:
		return &selectedRoute{
			routeIndex:  routeIndex,
			egressIface: string(codec.InterfaceSIP3GPP),
			ruleName:    "ims-local",
			priority:    -2,
		}
	case routeIndex == 1:
		return &selectedRoute{
			routeIndex:  routeIndex,
			egressIface: string(codec.InterfaceSIP3GPP),
			ruleName:    "ims-sh",
			priority:    -1,
		}
	case routeIndex == 2:
		return &selectedRoute{
			routeIndex:  routeIndex,
			egressIface: string(codec.InterfaceSGd),
			ruleName:    "sgd-built-in",
			priority:    0,
		}
	default:
		decisionIdx := routeIndex - 3
		if decisionIdx < 0 || decisionIdx >= len(decisions) {
			return nil
		}
		decision := decisions[decisionIdx]
		return &selectedRoute{
			routeIndex:  routeIndex,
			egressIface: decision.EgressIface,
			egressPeer:  decision.EgressPeer,
			sfPolicyID:  decision.SFPolicyID,
			ruleName:    decision.RuleName,
			priority:    decision.Priority,
		}
	}
}

func (f *Forwarder) deliverSelectedRoute(ctx context.Context, msg *codec.Message, route *selectedRoute) error {
	if f.m != nil {
		f.m.MessagesOut.WithLabelValues(route.egressIface).Inc()
	}
	msg.EgressInterface = codec.InterfaceType(route.egressIface)
	switch route.egressIface {
	case string(codec.InterfaceSIP3GPP):
		return f.deliverISC(ctx, msg, route.imsReg)
	case string(codec.InterfaceSIPSimple):
		return f.deliverSimple(ctx, msg, route.egressPeer)
	case string(codec.InterfaceSMPP):
		return f.deliverSMPP(ctx, msg, route.egressPeer)
	case string(codec.InterfaceSGd):
		msg.Destination.IMSI = route.sgdIMSI
		msg.Destination.MMENumber = route.sgdMMENum
		return f.deliverSGd(ctx, msg, route.egressPeer)
	default:
		return fmt.Errorf("unknown egress interface %q", route.egressIface)
	}
}

func (f *Forwarder) candidateCount(msg *codec.Message) int {
	return len(f.fallbackDecisions(msg)) + 3
}

func (f *Forwarder) selectRoute(ctx context.Context, msg *codec.Message, startCursor int) (*selectedRoute, error) {
	decisions := f.fallbackDecisions(msg)
	totalCandidates := len(decisions) + 3
	slog.Debug("forwarder: routing candidates matched",
		"dst", msg.Destination.MSISDN,
		"count", len(decisions),
		"cursor", startCursor,
	)

	var skipped []string
	base := modulo(startCursor, totalCandidates)
	for offset := 0; offset < totalCandidates; offset++ {
		routeIndex := (base + offset) % totalCandidates
		route, reason := f.resolveCandidateAt(ctx, msg, decisions, routeIndex)
		if reason != "" {
			slog.Debug("forwarder: routing candidate skipped",
				"dst", msg.Destination.MSISDN,
				"route_index", routeIndex,
				"reason", reason,
			)
			skipped = append(skipped, fmt.Sprintf("route[%d]: %s", routeIndex, reason))
			continue
		}
		slog.Debug("forwarder: routing candidate selected",
			"dst", msg.Destination.MSISDN,
			"route_index", route.routeIndex,
			"rule", route.ruleName,
			"priority", route.priority,
			"egress", route.egressIface,
			"peer", route.egressPeer,
		)
		return route, nil
	}

	slog.Debug("forwarder: no more matching rules",
		"dst", msg.Destination.MSISDN,
		"matched", len(decisions),
	)
	return nil, fmt.Errorf("no usable matching route: %s", strings.Join(skipped, "; "))
}

func (f *Forwarder) resolveCandidateAt(ctx context.Context, msg *codec.Message, decisions []routing.Decision, routeIndex int) (*selectedRoute, string) {
	switch {
	case routeIndex == 0:
		reg, ok := f.reg.Lookup(msg.Destination.MSISDN)
		if !ok || reg == nil || !reg.Registered || reg.SCSCF == "" {
			return nil, "subscriber not locally IMS registered"
		}
		return &selectedRoute{
			routeIndex:  routeIndex,
			egressIface: string(codec.InterfaceSIP3GPP),
			egressPeer:  reg.SCSCF,
			imsReg:      reg,
			ruleName:    "ims-local",
			priority:    -2,
		}, ""
	case routeIndex == 1:
		reg, err := f.reg.ShRefresh(ctx, msg.Destination.MSISDN)
		if err != nil {
			slog.Warn("forwarder: Sh lookup error, continuing to routing rules",
				"dst", msg.Destination.MSISDN, "err", err)
			return nil, fmt.Sprintf("Sh lookup failed: %v", err)
		}
		if reg == nil || !reg.Registered || reg.SCSCF == "" {
			return nil, "subscriber not IMS registered via Sh"
		}
		return &selectedRoute{
			routeIndex:  routeIndex,
			egressIface: string(codec.InterfaceSIP3GPP),
			egressPeer:  reg.SCSCF,
			imsReg:      reg,
			ruleName:    "ims-sh",
			priority:    -1,
		}, ""
	case routeIndex == 2:
		return f.resolveCandidate(ctx, msg, routing.Decision{
			RuleName:    "sgd-built-in",
			Priority:    0,
			EgressIface: string(codec.InterfaceSGd),
		}, routeIndex)
	default:
		decisionIdx := routeIndex - 3
		if decisionIdx < 0 || decisionIdx >= len(decisions) {
			return nil, "no routing rule at index"
		}
		decision := decisions[decisionIdx]
		slog.Debug("forwarder: evaluating routing candidate",
			"dst", msg.Destination.MSISDN,
			"route_index", routeIndex,
			"rule", decision.RuleName,
			"priority", decision.Priority,
			"egress", decision.EgressIface,
			"peer", decision.EgressPeer,
		)
		return f.resolveCandidate(ctx, msg, decision, routeIndex)
	}
}

func (f *Forwarder) fallbackDecisions(msg *codec.Message) []routing.Decision {
	// An SMS-STATUS-REPORT is a TPDU for the originating handset; it can only
	// be delivered over a TPDU-carrying interface (ISC or SGd), never via the
	// SMPP / SIP SIMPLE fallback rules.
	if msg.Kind == codec.MessageKindStatusReport {
		return nil
	}
	decisions, err := f.engine.RouteAll(msg)
	if err != nil {
		return nil
	}
	filtered := make([]routing.Decision, 0, len(decisions))
	for _, decision := range decisions {
		switch decision.EgressIface {
		case string(codec.InterfaceSMPP), string(codec.InterfaceSIPSimple):
			filtered = append(filtered, decision)
		}
	}
	return filtered
}

func (f *Forwarder) resolveCandidate(ctx context.Context, msg *codec.Message, decision routing.Decision, routeIndex int) (*selectedRoute, string) {
	route := &selectedRoute{
		routeIndex:  routeIndex,
		egressIface: decision.EgressIface,
		egressPeer:  decision.EgressPeer,
		sfPolicyID:  decision.SFPolicyID,
		ruleName:    decision.RuleName,
		priority:    decision.Priority,
	}

	switch decision.EgressIface {
	case string(codec.InterfaceSIP3GPP):
		reg, ok := f.reg.Lookup(msg.Destination.MSISDN)
		if !ok || reg == nil {
			return nil, "subscriber not locally IMS registered"
		}
		if reg == nil || !reg.Registered || reg.SCSCF == "" {
			return nil, "subscriber not IMS registered"
		}
		route.egressPeer = reg.SCSCF
		route.imsReg = reg
		return route, ""
	case string(codec.InterfaceSIPSimple):
		if f.simpleSender == nil {
			return nil, "SIP SIMPLE sender not configured"
		}
		if decision.EgressPeer == "" {
			return nil, "SIP SIMPLE peer not specified"
		}
		peer, err := f.st.GetSIPPeer(ctx, decision.EgressPeer)
		if err != nil || peer == nil {
			return nil, fmt.Sprintf("SIP SIMPLE peer %q not found", decision.EgressPeer)
		}
		return route, ""
	case string(codec.InterfaceSMPP):
		if f.smppMgr == nil {
			return nil, "SMPP manager not configured"
		}
		if decision.EgressPeer == "" {
			return nil, "SMPP peer not specified"
		}
		if !f.smppMgr.HasPeer(decision.EgressPeer) {
			return nil, fmt.Sprintf("SMPP peer %q not active", decision.EgressPeer)
		}
		return route, ""
	case string(codec.InterfaceSGd):
		mmeHost, imsi, mmeNumber, reason := f.resolveSGdTarget(ctx, msg.Destination.MSISDN)
		if reason != "" {
			return nil, reason
		}
		if checker, ok := f.sgdSender.(interface{ HasPeerForMME(string) bool }); ok && !checker.HasPeerForMME(mmeHost) {
			return nil, fmt.Sprintf("no active SGd peer for MME %q", mmeHost)
		}
		if inspector, ok := f.sgdSender.(interface {
			RoutePeerForMME(string) (string, bool, bool)
		}); ok {
			if peerName, viaProxy, found := inspector.RoutePeerForMME(mmeHost); found {
				slog.Debug("forwarder: SGd route resolved",
					"dst", msg.Destination.MSISDN,
					"mme", mmeHost,
					"peer", peerName,
					"via_proxy", viaProxy,
				)
			}
		}
		msg.Destination.IMSI = imsi
		msg.Destination.MMENumber = mmeNumber
		route.egressPeer = mmeHost
		route.sgdIMSI = imsi
		route.sgdMMENum = mmeNumber
		return route, ""
	default:
		return nil, fmt.Sprintf("unsupported egress interface %q", decision.EgressIface)
	}
}

func classifyRouteError(route *selectedRoute, err error) routeOutcome {
	if route == nil || err == nil {
		return routeOutcomeTryNext
	}
	switch route.egressIface {
	case string(codec.InterfaceSIP3GPP), string(codec.InterfaceSIPSimple):
		if code, ok := sipResponseCodeFromError(err); ok && code == 404 {
			return routeOutcomePermanent
		}
		return routeOutcomeWaitTimer
	case string(codec.InterfaceSGd):
		var ofaErr *diametersgd.OFAResultError
		if errors.As(err, &ofaErr) {
			return classifySGdResult(ofaErr)
		}
		return routeOutcomeWaitTimer
	default:
		return routeOutcomeWaitTimer
	}
}

// classifySGdResult maps an MT-Forward failure (TS 29.338 §5.3.2.4/§7.4.3)
// to a routing outcome.
func classifySGdResult(e *diametersgd.OFAResultError) routeOutcome {
	if !e.Experimental {
		switch e.ResultCode {
		case dcodec.DiameterUnableToDeliver: // used by MMEs for absent subscribers
			return routeOutcomeWaitEvent
		case dcodec.DiameterTooBusy:
			return routeOutcomeBusy
		}
		return routeOutcomeWaitTimer
	}
	switch e.ResultCode {
	case diametersgd.ResultErrorAbsentUser:
		return routeOutcomeWaitEvent
	case diametersgd.ResultErrorUserBusyForMTSMS:
		return routeOutcomeBusy
	case diametersgd.ResultErrorIllegalUser, diametersgd.ResultErrorIllegalEquipment:
		return routeOutcomeRejected
	case diametersgd.ResultErrorSMDeliveryFailure:
		if e.DeliveryFailureCause == nil {
			return routeOutcomeWaitTimer
		}
		switch *e.DeliveryFailureCause {
		case diametersgd.DeliveryFailureMemoryCapacityExceeded:
			return routeOutcomeWaitEvent // wait for the memory-available alert
		case diametersgd.DeliveryFailureSCCongestion:
			return routeOutcomeWaitTimer
		case diametersgd.DeliveryFailureEquipmentProtocolError,
			diametersgd.DeliveryFailureEquipmentNotSMEquipped,
			diametersgd.DeliveryFailureUnknownServiceCentre,
			diametersgd.DeliveryFailureInvalidSMEAddress,
			diametersgd.DeliveryFailureUserNotSCUser:
			return routeOutcomeRejected
		}
	}
	return routeOutcomeWaitTimer
}

func sipResponseCodeFromError(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	msg := err.Error()
	idx := strings.Index(msg, " returned ")
	if idx < 0 {
		return 0, false
	}
	var code int
	if _, scanErr := fmt.Sscanf(msg[idx+10:], "%d", &code); scanErr != nil {
		return 0, false
	}
	return code, true
}

func (r *selectedRoute) nextRouteCursor() int {
	return r.routeIndex + 1
}

func modulo(v, size int) int {
	if size <= 0 {
		return 0
	}
	v %= size
	if v < 0 {
		v += size
	}
	return v
}

// sgdReasonUnknownSubscriber is the candidate-skip reason when the HSS
// answers SRI-SM with user-unknown (Experimental-Result 5001). Unlike other
// S6c failures it is definitive: the SGd route is not retried, and the
// message fails unless another route delivers it.
const sgdReasonUnknownSubscriber = "S6c: subscriber unknown to the HSS"

func (f *Forwarder) resolveSGdTarget(ctx context.Context, msisdn string) (string, string, string, string) {
	if f.sgdSender == nil {
		return "", "", "", "SGd sender not configured"
	}
	s6cSub, s6cErr := f.reg.S6cLookup(ctx, msisdn)
	if s6cErr != nil {
		var unknown *s6c.ErrUnknownUser
		if errors.As(s6cErr, &unknown) {
			return "", "", "", sgdReasonUnknownSubscriber
		}
		return "", "", "", fmt.Sprintf("S6c lookup failed: %v", s6cErr)
	}
	if s6cSub == nil || !s6cSub.LTEAttached || s6cSub.MMEHost == "" {
		return "", "", "", "S6c did not return a serving MME"
	}
	if s6cSub.IMSI == "" {
		return "", "", "", "S6c did not return an IMSI"
	}
	if s6cSub.MMENumber == "" {
		return "", "", "", "S6c did not return MME-Number-for-MT-SMS"
	}
	return f.applyMMEMapping(s6cSub.MMEHost), s6cSub.IMSI, s6cSub.MMENumber, ""
}

// applyMMEMapping translates an MME hostname via the S6c→SGd mapping table.
// If no mapper is configured or no enabled entry matches, the original host is returned.
func (f *Forwarder) applyMMEMapping(mmeHost string) string {
	if f.sgdMapper == nil {
		return mmeHost
	}
	return f.sgdMapper.Map(mmeHost)
}

// persistMessage writes a new message record to the store.
// Errors are logged but do not abort delivery — best effort on DB write failure.
func (f *Forwarder) persistMessage(ctx context.Context, msg *codec.Message,
	egressIface, egressPeer, status string, now time.Time) {

	var expiry *time.Time
	if msg.ValidityPeriod != nil {
		t := now.Add(*msg.ValidityPeriod)
		expiry = &t
	}

	sm := store.Message{
		ID:                 msg.ID,
		TPMR:               tpmrForStore(msg),
		OriginIface:        string(msg.IngressInterface),
		OriginPeer:         msg.IngressPeer,
		SMPPMsgID:          msg.SMPPMsgID,
		EgressIface:        egressIface,
		EgressPeer:         egressPeer,
		RouteCursor:        0,
		SrcMSISDN:          msg.Source.MSISDN,
		DstMSISDN:          msg.Destination.MSISDN,
		AlertCorrelationID: msg.CorrelationID,
		Encoding:           int(msg.Encoding),
		DCS:                int(msg.DCS),
		Status:             status,
		RetryCount:         0,
		DRRequired:         msg.TPSRRequired,
		SubmittedAt:        now,
		ExpiryAt:           expiry,
		Kind:               store.NormalizeMessageKind(string(msg.Kind)),
		RefMessageID:       msg.RefMessageID,
	}
	sm.SMPPRegisteredDelivery = smppRegisteredDeliveryForStore(msg)
	if len(msg.RawTPDU) > 0 {
		sm.RawTPDU = append([]byte(nil), msg.RawTPDU...)
	}
	switch msg.Encoding {
	case codec.EncodingBinary:
		if len(msg.Binary) > 0 {
			sm.Payload = append([]byte(nil), msg.Binary...)
		}
	default:
		if msg.Text != "" {
			sm.Payload = []byte(msg.Text)
		}
	}
	if len(msg.Binary) > 0 && len(sm.Payload) == 0 {
		sm.Payload = append([]byte(nil), msg.Binary...)
	}
	if msg.UDH != nil && len(msg.UDH.Raw) > 0 {
		sm.UDH = append([]byte(nil), msg.UDH.Raw...)
	}
	if err := f.st.SaveMessage(ctx, sm); err != nil {
		slog.Error("forwarder: persist message failed", "id", msg.ID, "err", err)
	}
}

// smppRegisteredDeliveryForStore returns the SMPP registered_delivery field
// to persist, or nil for messages that did not arrive over SMPP.
func smppRegisteredDeliveryForStore(msg *codec.Message) *int {
	if msg.SMPPRegisteredDelivery == nil {
		return nil
	}
	v := int(*msg.SMPPRegisteredDelivery)
	return &v
}

// tpmrForStore returns the TP-MR to persist, or nil when the ingress interface
// does not carry an SMS-SUBMIT TPDU (so NULL means "no TP-MR", not MR=0).
// The stored TP-MR is required to build an SMS-STATUS-REPORT for the sender.
func tpmrForStore(msg *codec.Message) *int {
	switch msg.IngressInterface {
	case codec.InterfaceSGd, codec.InterfaceSIP3GPP:
		mr := int(msg.TPMR)
		return &mr
	default:
		return nil
	}
}

func defaultAlertCorrelationID(messageID string) string {
	return base64.StdEncoding.EncodeToString([]byte("smsc:" + messageID))
}

func deferredRouteCursor(route *selectedRoute) int {
	if route == nil {
		return 0
	}
	if route.egressIface == string(codec.InterfaceSGd) {
		return route.routeIndex
	}
	return route.nextRouteCursor()
}

func (f *Forwarder) markDeferred(ctx context.Context, messageID string, msg *codec.Message, startCursor int, route *selectedRoute) {
	if messageID == "" {
		return
	}
	if route == nil {
		route = f.plannedRouteAt(msg, startCursor)
	}
	reason := "route_lookup"
	deferredInterface := ""
	servingNode := ""
	routeCursor := 0
	if route != nil {
		routeCursor = deferredRouteCursor(route)
		if route.egressIface == string(codec.InterfaceSGd) {
			reason = "sgd_lookup"
			deferredInterface = route.egressIface
			if route.egressPeer != "" {
				reason = "sgd_delivery"
				servingNode = route.egressPeer
			}
		}
	}
	if err := f.st.UpdateMessageDeferred(ctx, messageID, reason, deferredInterface, servingNode, routeCursor); err != nil {
		slog.Warn("forwarder: update deferred metadata failed", "id", messageID, "err", err)
	}
}

// ── Egress helpers ────────────────────────────────────────────────────────────

func (f *Forwarder) deliverISC(ctx context.Context, msg *codec.Message, reg *registry.Registration) error {
	if f.iscSender == nil {
		return fmt.Errorf("ISC sender not configured")
	}
	if err := f.iscSender.Send(ctx, msg, reg); err != nil {
		return err
	}
	return nil
}

func (f *Forwarder) deliverSimple(ctx context.Context, msg *codec.Message, peerName string) error {
	if f.simpleSender == nil {
		return fmt.Errorf("SIP SIMPLE sender not configured")
	}
	peer, err := f.st.GetSIPPeer(ctx, peerName)
	if err != nil || peer == nil {
		return fmt.Errorf("SIP SIMPLE peer %q not found", peerName)
	}
	if err := f.simpleSender.Send(ctx, msg, *peer); err != nil {
		return err
	}
	return nil
}

func (f *Forwarder) deliverSMPP(ctx context.Context, msg *codec.Message, peerName string) error {
	if f.smppMgr == nil {
		return fmt.Errorf("SMPP manager not configured")
	}
	pdu, err := smppcodec.EncodeDeliverSM(msg)
	if err != nil {
		return fmt.Errorf("encode deliver_sm: %w", err)
	}
	pdu.SequenceNumber = 0 // assigned by Link.SendAndWait
	resp, err := f.smppMgr.SendViaPeer(peerName, pdu)
	if err != nil {
		return err
	}
	if resp.CommandStatus != smpp.ESME_ROK {
		return fmt.Errorf("SMPP peer %s returned status 0x%08X", peerName, resp.CommandStatus)
	}
	return nil
}

func (f *Forwarder) deliverSGd(ctx context.Context, msg *codec.Message, mmeHost string) error {
	if f.sgdSender == nil {
		return fmt.Errorf("SGd sender not configured")
	}
	// Only one MT transaction per subscriber is possible at the MME
	// (TS 24.011 CP transactions), so concurrent MTs to the same UE, such as
	// concatenated parts or their status reports, wait their turn here
	// instead of colliding with a busy answer.
	key := msg.Destination.IMSI
	if key == "" {
		key = msg.Destination.MSISDN
	}
	release, err := f.sgdDestLocks.acquire(ctx, key)
	if err != nil {
		return err
	}
	defer release()
	if err := f.sgdSender.SendOFR(ctx, msg, mmeHost, f.scAddr); err != nil {
		return err
	}
	return nil
}

func newUUID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
