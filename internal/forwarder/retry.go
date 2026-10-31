package forwarder

import (
	"context"
	"log/slog"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

// RetryScheduler polls for retryable messages and re-dispatches them.
// It respects the SF policy attached to each message's routing rule.
type RetryScheduler struct {
	f        *Forwarder
	interval time.Duration
}

// NewRetryScheduler creates a RetryScheduler that polls every interval.
func NewRetryScheduler(f *Forwarder, interval time.Duration) *RetryScheduler {
	return &RetryScheduler{f: f, interval: interval}
}

// Run starts the retry loop and blocks until ctx is cancelled.
func (r *RetryScheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

func (r *RetryScheduler) tick(ctx context.Context) {
	msgs, err := r.f.st.ListRetryableMessages(ctx)
	if err != nil {
		slog.Error("retry scheduler: list retryable messages", "err", err)
		return
	}
	for _, m := range msgs {
		r.dispatch(ctx, m)
	}
}

func (r *RetryScheduler) dispatch(ctx context.Context, m store.Message) {
	claimed, err := r.f.st.ClaimMessageForDispatch(ctx, m.ID, []string{
		store.MessageStatusQueued,
		store.MessageStatusWaitTimer,
		store.MessageStatusWaitTimerEvent,
	})
	if err != nil {
		slog.Error("retry: mark dispatched failed", "id", m.ID, "err", err)
		return
	}
	if !claimed {
		slog.Debug("retry: message already claimed or no longer retryable", "id", m.ID, "status", m.Status)
		return
	}

	msg := storeToCodecMessage(m)
	result := r.retryMessage(ctx, m, msg)
	if result.outcome != passOutcomeDelivered {
		if r.f.m != nil {
			r.f.m.SFRetried.Inc()
		}
		r.finalizeNonDelivered(ctx, m, msg, result)
		return
	}
	route := result.deliveredRoute
	slog.Info("retry: delivered", "id", m.ID, "egress", route.egressIface, "peer", route.egressPeer)
	if err := r.f.persistDeliveredRoute(ctx, m.ID, msg.EgressInterface, route.egressPeer); err != nil {
		slog.Error("retry: persist delivered route failed", "id", m.ID, "err", err)
	}
	_ = r.f.st.UpdateMessageStatus(ctx, m.ID, store.MessageStatusDelivered)
	if r.f.reporter != nil {
		if stored, err := r.f.st.GetMessage(ctx, m.ID); err == nil && stored != nil {
			r.f.reporter.Report(ctx, *stored, "DELIVRD")
		}
	}
}

func (r *RetryScheduler) retryMessage(ctx context.Context, m store.Message, msg *codec.Message) routingPassResult {
	if m.DeferredReason == "sgd_alert_retry" && m.DeferredInterface == string(codec.InterfaceSGd) {
		route, err := r.f.tryRouteAt(ctx, msg, m.RouteCursor)
		if err != nil {
			result := routingPassResult{
				outcome:       passOutcomeWaitTimer,
				lastAttempted: route,
				timerRoute:    route,
				lastErr:       err,
			}
			switch classifyRouteError(route, err) {
			case routeOutcomeRejected:
				result.outcome = passOutcomeFailed
			case routeOutcomeBusy:
				result.retryAfter = sgdBusyRetryDelay
			}
			return result
		}
		return routingPassResult{
			outcome:        passOutcomeDelivered,
			deliveredRoute: route,
			lastAttempted:  route,
		}
	}
	return r.f.runRoutingPass(ctx, msg, m.RouteCursor)
}

func (r *RetryScheduler) finalizeNonDelivered(ctx context.Context, m store.Message, msg *codec.Message, result routingPassResult) {
	switch result.outcome {
	case passOutcomeWaitTimer:
		r.f.markDeferred(ctx, m.ID, msg, m.RouteCursor, result.lastAttempted)
		r.f.applyDeferredExpiryCap(ctx, m.ID, m.SubmittedAt, result.lastAttempted)
		if result.retryAfter > 0 {
			// Busy: retry soon without consuming the retry budget.
			if err := r.f.st.UpdateMessageRetry(ctx, m.ID, m.RetryCount, time.Now().Add(result.retryAfter), deferredRouteCursor(result.lastAttempted)); err != nil {
				slog.Error("retry: update busy retry failed", "id", m.ID, "err", err)
			}
		} else {
			r.scheduleOrFail(ctx, m, result.lastAttempted)
		}
		// An alert-triggered retry that finds the subscriber absent again is
		// returned as a timer wait; the HSS cleared its MWD on the alert, so
		// report the renewed absence.
		if classifyRouteError(result.lastAttempted, result.lastErr) == routeOutcomeWaitEvent {
			r.f.reportSGdAbsent(ctx, m.ID, m.DeferredReason, result.lastAttempted, result.lastErr)
		}
	case passOutcomeWaitEvent:
		r.f.markDeferred(ctx, m.ID, msg, m.RouteCursor, result.eventRoute)
		r.f.applyDeferredExpiryCap(ctx, m.ID, m.SubmittedAt, result.eventRoute)
		if err := r.f.st.UpdateMessageStatus(ctx, m.ID, store.MessageStatusWaitEvent); err != nil {
			slog.Error("retry: mark wait event failed", "id", m.ID, "err", err)
		}
		r.f.reportSGdAbsent(ctx, m.ID, m.DeferredReason, result.eventRoute, result.lastErr)
	case passOutcomeWaitTimerEvent:
		r.f.markDeferred(ctx, m.ID, msg, m.RouteCursor, result.eventRoute)
		r.f.applyDeferredExpiryCap(ctx, m.ID, m.SubmittedAt, result.eventRoute)
		r.scheduleOrFail(ctx, m, result.eventRoute)
		if err := r.f.st.UpdateMessageStatus(ctx, m.ID, store.MessageStatusWaitTimerEvent); err != nil {
			slog.Error("retry: mark wait timer-event failed", "id", m.ID, "err", err)
		}
		r.f.reportSGdAbsent(ctx, m.ID, m.DeferredReason, result.eventRoute, result.lastErr)
	case passOutcomeFailed:
		slog.Warn("retry: max retries exceeded, marking failed", "id", m.ID, "err", result.lastErr)
		_ = r.f.st.UpdateMessageStatus(ctx, m.ID, store.MessageStatusFailed)
		if r.f.reporter != nil {
			if stored, err := r.f.st.GetMessage(ctx, m.ID); err == nil && stored != nil {
				r.f.reporter.Report(ctx, *stored, "FAILED")
			}
		}
	}
}

func (r *RetryScheduler) scheduleOrFail(ctx context.Context, m store.Message, route *selectedRoute) {
	// Compute next retry using SF policy if available.
	next := r.nextRetry(ctx, m, route)
	if next == nil {
		// Max retries exceeded or no policy — fail permanently.
		slog.Warn("retry: max retries exceeded, marking failed", "id", m.ID)
		_ = r.f.st.UpdateMessageStatus(ctx, m.ID, store.MessageStatusFailed)
		if r.f.reporter != nil {
			if stored, err := r.f.st.GetMessage(ctx, m.ID); err == nil && stored != nil {
				r.f.reporter.Report(ctx, *stored, "FAILED")
			}
		}
		return
	}

	if err := r.f.st.UpdateMessageRetry(ctx, m.ID, m.RetryCount+1, *next, deferredRouteCursor(route)); err != nil {
		slog.Error("retry: update retry failed", "id", m.ID, "err", err)
	}
}

// nextRetry returns the absolute time of the next retry attempt, or nil if
// the message has exhausted its retries.
func (r *RetryScheduler) nextRetry(ctx context.Context, m store.Message, route *selectedRoute) *time.Time {
	// Default: simple exponential-ish schedule without a named policy.
	defaultSchedule := []int{30, 300, 1800, 3600, 3600, 3600, 3600, 3600}

	schedule := defaultSchedule
	maxRetries := len(defaultSchedule)

	// Try to load the SF policy referenced by the routing rule.
	if sfPolicyID := r.sfPolicyForMessage(ctx, m, route); sfPolicyID != "" {
		if pol, err := r.f.st.GetSFPolicy(ctx, sfPolicyID); err == nil && pol != nil {
			schedule = pol.RetrySchedule
			maxRetries = pol.MaxRetries
		}
	}

	attempt := m.RetryCount + 1 // this will be the Nth attempt (0-indexed RetryCount)
	if attempt > maxRetries {
		return nil
	}

	idx := attempt - 1
	if idx >= len(schedule) {
		idx = len(schedule) - 1
	}
	delay := time.Duration(schedule[idx]) * time.Second
	t := time.Now().Add(delay)
	return &t
}

// sfPolicyForMessage looks up the SF policy ID from the routing engine for this message.
// Returns empty string if no policy is configured.
func (r *RetryScheduler) sfPolicyForMessage(ctx context.Context, m store.Message, route *selectedRoute) string {
	if route != nil && route.sfPolicyID != "" {
		return route.sfPolicyID
	}
	msg := storeToCodecMessage(m)
	if planned := r.f.plannedRouteAt(msg, m.RouteCursor); planned != nil && planned.sfPolicyID != "" {
		return planned.sfPolicyID
	}
	selected, err := r.f.selectRoute(ctx, msg, m.RouteCursor)
	if err != nil || selected == nil {
		return ""
	}
	return selected.sfPolicyID
}

// storeToCodecMessage converts a store.Message back to a codec.Message for re-dispatch.
func storeToCodecMessage(m store.Message) *codec.Message {
	msg := &codec.Message{
		ID:               m.ID,
		IngressInterface: codec.InterfaceType(m.OriginIface),
		EgressInterface:  codec.InterfaceType(m.EgressIface),
		IngressPeer:      m.OriginPeer,
		CorrelationID:    m.AlertCorrelationID,
		Encoding:         tpdu.ParseDCS(byte(m.DCS)),
		DCS:              byte(m.DCS),
	}
	if m.Encoding != 0 {
		msg.Encoding = codec.Encoding(m.Encoding)
	}
	msg.Source.MSISDN = m.SrcMSISDN
	msg.Destination.MSISDN = m.DstMSISDN
	if len(m.Payload) > 0 {
		if msg.Encoding == codec.EncodingBinary {
			msg.Binary = append([]byte(nil), m.Payload...)
		} else {
			msg.Text = string(m.Payload)
		}
	}
	if len(m.UDH) > 0 {
		msg.UDH = &codec.UDH{Raw: append([]byte(nil), m.UDH...)}
	}
	if m.TPMR != nil {
		msg.TPMR = byte(*m.TPMR)
	}
	msg.Kind = codec.MessageKind(store.NormalizeMessageKind(m.Kind))
	msg.RefMessageID = m.RefMessageID
	if len(m.RawTPDU) > 0 {
		msg.RawTPDU = append([]byte(nil), m.RawTPDU...)
	}
	if m.ExpiryAt != nil {
		t := *m.ExpiryAt
		msg.Expiry = &t
		if remaining := time.Until(t); remaining > 0 {
			msg.ValidityPeriod = &remaining
		}
	}
	return msg
}
