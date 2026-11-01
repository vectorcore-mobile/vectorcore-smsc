// Package routing implements the message routing decision engine.
// Rules are loaded from the database and held in an atomically-swapped
// in-memory slice sorted by priority (lowest number = highest priority).
package routing

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

// Decision is the result of a routing evaluation.
type Decision struct {
	RuleName    string
	Priority    int
	EgressIface string // sip3gpp | sipsimple | smpp | sgd
	EgressPeer  string // peer name / system_id (empty = use default)
	SFPolicyID  string // SF policy to apply on failure (empty = no SF)
}

// Engine holds the in-memory rule table and evaluates routing decisions.
type Engine struct {
	// rules is an atomically-swapped *[]store.RoutingRule
	rules unsafe.Pointer
}

// NewEngine creates an Engine with an empty rule table.
func NewEngine() *Engine {
	e := &Engine{}
	empty := []store.RoutingRule{}
	atomic.StorePointer(&e.rules, unsafe.Pointer(&empty))
	return e
}

// Reload replaces the rule table atomically.
func (e *Engine) Reload(rules []store.RoutingRule) {
	atomic.StorePointer(&e.rules, unsafe.Pointer(&rules))
	slog.Info("routing engine reloaded", "rules", len(rules))
}

// Route evaluates routing rules for msg.
//
// The decision tree is:
//  1. IMS-registered → EgressIface=sip3gpp  (handled by forwarder, not here)
//  2. IMS via Sh     → handled by forwarder, not here
//  3. LTE-attached   → EgressIface=sgd       (handled by forwarder, not here)
//  4. Routing rules  → fallback routes only
//
// Route only handles step 4; steps 1-3 are checked by the Forwarder
// before calling Route, since they require live registry/subscriber lookups.
func (e *Engine) Route(msg *codec.Message) (*Decision, error) {
	decisions, err := e.RouteAll(msg)
	if err != nil {
		return nil, err
	}
	d := decisions[0]
	slog.Debug("routing rule matched",
		"rule", d.RuleName,
		"priority", d.Priority,
		"egress", d.EgressIface,
		"peer", d.EgressPeer,
	)
	return &d, nil
}

// RouteAll returns every matching routing decision in priority order.
func (e *Engine) RouteAll(msg *codec.Message) ([]Decision, error) {
	rules := *(*[]store.RoutingRule)(atomic.LoadPointer(&e.rules))
	var decisions []Decision

	for _, r := range rules {
		if matches(r, msg) {
			decisions = append(decisions, Decision{
				RuleName:    r.Name,
				Priority:    r.Priority,
				EgressIface: r.EgressIface,
				EgressPeer:  r.EgressPeer,
				SFPolicyID:  r.SFPolicyID,
			})
		}
	}
	if len(decisions) == 0 {
		return nil, fmt.Errorf("routing: no matching rule for dst=%s src_iface=%s",
			msg.Destination.MSISDN, msg.IngressInterface)
	}
	return decisions, nil
}

// RouteWithContext loads from store and evaluates.  Convenience wrapper used
// by the Loader to expose a context-aware call to callers that don't cache rules.
func (e *Engine) RouteWithContext(_ context.Context, msg *codec.Message) (*Decision, error) {
	return e.Route(msg)
}

// matches returns true if all non-empty match criteria on r match msg.
func matches(r store.RoutingRule, msg *codec.Message) bool {
	// Source interface
	if r.MatchSrcIface != "" {
		if string(msg.IngressInterface) != r.MatchSrcIface {
			return false
		}
	}
	// Source peer
	if r.MatchSrcPeer != "" && msg.IngressPeer != r.MatchSrcPeer {
		return false
	}
	// Destination prefix
	if r.MatchDstPrefix != "" {
		dst := strings.TrimPrefix(msg.Destination.MSISDN, "+")
		pfx := strings.TrimPrefix(r.MatchDstPrefix, "+")
		if !strings.HasPrefix(dst, pfx) {
			return false
		}
	}
	// MSISDN range
	if r.MatchMSISDNMin != "" || r.MatchMSISDNMax != "" {
		dst := strings.TrimPrefix(msg.Destination.MSISDN, "+")
		if r.MatchMSISDNMin != "" {
			min := strings.TrimPrefix(r.MatchMSISDNMin, "+")
			if dst < min {
				return false
			}
		}
		if r.MatchMSISDNMax != "" {
			max := strings.TrimPrefix(r.MatchMSISDNMax, "+")
			if dst > max {
				return false
			}
		}
	}
	return true
}
