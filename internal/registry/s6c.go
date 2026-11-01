package registry

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/diameter/s6c"
	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

// S6cLookup checks the local subscriber cache first. If no usable attached MME
// is already known, it queries the HSS via SRI-SM, updates the subscriber
// record, and returns the refreshed state. A user-unknown answer from the HSS
// is returned as *s6c.ErrUnknownUser, never as the stale cached record.
func (r *Registry) S6cLookup(ctx context.Context, msisdn string) (*store.Subscriber, error) {
	sub, err := r.st.GetSubscriber(ctx, msisdn)
	if err != nil {
		return nil, err
	}

	r.mu.RLock()
	ttl := r.s6cTTL
	s6cc := r.s6cClient
	r.mu.RUnlock()

	if sub != nil && sub.LTEAttached && sub.MMEHost != "" && sub.IMSI != "" && s6cCacheFresh(sub, ttl) {
		sub.MMENumber = s6c.NormalizeE164Address(sub.MMENumber)
		slog.Debug("registry: S6c lookup (cached)",
			"msisdn", msisdn,
			"mme", sub.MMEHost,
			"mme_number", sub.MMENumber,
			"age", cacheAge(sub),
			"ttl", ttl,
		)
		return sub, nil
	}
	if s6cc == nil {
		return sub, nil
	}

	if sub != nil && sub.LTEAttached && sub.MMEHost != "" && sub.IMSI != "" && ttl > 0 {
		slog.Debug("registry: S6c lookup", "msisdn", msisdn, "cache", "expired", "age", cacheAge(sub), "ttl", ttl)
	} else {
		slog.Debug("registry: S6c lookup", "msisdn", msisdn)
	}

	info, err := s6cc.LookupRouting(msisdn)
	if err != nil {
		var unknown *s6c.ErrUnknownUser
		if isS6cUnknown(err, &unknown) {
			slog.Debug("registry: S6c lookup unknown subscriber", "msisdn", msisdn)
		}
		return nil, err
	}

	refreshed := store.Subscriber{MSISDN: msisdn}
	if sub != nil {
		refreshed = *sub
	}
	refreshed.MSISDN = msisdn
	refreshed.IMSI = info.IMSI
	refreshed.LTEAttached = info.Attached
	refreshed.MMEHost = info.MMEName
	refreshed.MMENumber = s6c.NormalizeE164Address(info.MMENumber)
	refreshed.MWDSet = info.MWDStatus != 0

	if err := r.st.UpsertSubscriber(ctx, refreshed); err != nil {
		slog.Warn("registry: S6c upsert failed", "msisdn", msisdn, "err", err)
	} else {
		slog.Info("registry: S6c populated",
			"msisdn", msisdn,
			"imsi", refreshed.IMSI,
			"mme", refreshed.MMEHost,
			"mme_number", refreshed.MMENumber,
			"attached", refreshed.LTEAttached,
			"mwd_set", refreshed.MWDSet,
		)
	}

	return &refreshed, nil
}

func s6cCacheFresh(sub *store.Subscriber, ttl time.Duration) bool {
	if sub == nil || ttl <= 0 || sub.UpdatedAt.IsZero() {
		return false
	}
	return cacheAge(sub) <= ttl
}

func cacheAge(sub *store.Subscriber) time.Duration {
	if sub == nil || sub.UpdatedAt.IsZero() {
		return 0
	}
	return time.Since(sub.UpdatedAt)
}

func isS6cUnknown(err error, target **s6c.ErrUnknownUser) bool {
	if err == nil {
		return false
	}
	return errors.As(err, target)
}
