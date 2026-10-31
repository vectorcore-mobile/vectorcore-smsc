package forwarder

import (
	"context"
	"log/slog"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

// ExpirySweeper runs on a fixed interval and expires messages that have
// exceeded their validity period or maximum TTL.
type ExpirySweeper struct {
	f        *Forwarder
	interval time.Duration
	onExpiry func(ctx context.Context, msg store.Message) // optional DR callback
}

// NewExpirySweeper creates an ExpirySweeper.
// onExpiry is called for each expired message; it records the outcome and
// sends whatever delivery report the message's origin asked for.
func NewExpirySweeper(f *Forwarder, interval time.Duration, onExpiry func(context.Context, store.Message)) *ExpirySweeper {
	return &ExpirySweeper{f: f, interval: interval, onExpiry: onExpiry}
}

// Run starts the sweep loop and blocks until ctx is cancelled.
func (e *ExpirySweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.sweep(ctx)
		}
	}
}

func (e *ExpirySweeper) sweep(ctx context.Context) {
	msgs, err := e.f.st.ListExpiredMessages(ctx)
	if err != nil {
		slog.Error("expiry sweep: list expired messages", "err", err)
		return
	}
	for _, m := range msgs {
		slog.Info("expiry sweep: expiring message",
			"id", m.ID,
			"dst", m.DstMSISDN,
			"egress", m.EgressIface,
			"retry_count", m.RetryCount,
		)
		if err := e.f.st.UpdateMessageStatus(ctx, m.ID, store.MessageStatusExpired); err != nil {
			slog.Error("expiry sweep: mark expired", "id", m.ID, "err", err)
			continue
		}
		// SFExpired is a gauge polled from DB; no per-event increment needed.
		if e.onExpiry != nil {
			e.onExpiry(ctx, m)
		}
	}
}
