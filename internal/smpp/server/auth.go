// Package server implements the SMPP server listener and per-connection session FSM.
package server

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"golang.org/x/crypto/bcrypt"

	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

// AuthResult is returned by Authenticator.Authenticate.
type AuthResult struct {
	Account store.SMPPServerAccount
	Allowed bool
	Reason  string
}

// Authenticator validates SMPP bind credentials against the database.
// It maintains a local cache that is refreshed on every bind attempt to
// pick up account changes without requiring a reconnect.
type Authenticator struct {
	st store.Store

	mu       sync.RWMutex
	accounts map[string]store.SMPPServerAccount // keyed by system_id
}

// NewAuthenticator creates an Authenticator and loads the initial account list.
func NewAuthenticator(ctx context.Context, st store.Store) (*Authenticator, error) {
	a := &Authenticator{
		st:       st,
		accounts: make(map[string]store.SMPPServerAccount),
	}
	if err := a.reload(ctx); err != nil {
		return nil, err
	}
	return a, nil
}

// Reload refreshes the in-memory account cache from the database.
// Called by the server's hot-reload goroutine on smpp_server_accounts_changed.
func (a *Authenticator) Reload(ctx context.Context) {
	if err := a.reload(ctx); err != nil {
		slog.Error("smpp auth reload failed", "err", err)
	}
}

func (a *Authenticator) reload(ctx context.Context) error {
	accs, err := a.st.ListSMPPServerAccounts(ctx)
	if err != nil {
		return err
	}
	m := make(map[string]store.SMPPServerAccount, len(accs))
	for _, acc := range accs {
		m[acc.SystemID] = acc
	}
	a.mu.Lock()
	a.accounts = m
	a.mu.Unlock()
	slog.Info("smpp accounts loaded", "count", len(m))
	return nil
}

// Authenticate checks system_id, password (bcrypt), enabled flag, and optional
// IP whitelist.  remoteAddr is the TCP remote address ("ip:port").
func (a *Authenticator) Authenticate(systemID, password, remoteAddr string) AuthResult {
	a.mu.RLock()
	acc, ok := a.accounts[systemID]
	a.mu.RUnlock()

	if !ok {
		return AuthResult{Reason: "unknown system_id"}
	}
	if !acc.Enabled {
		return AuthResult{Reason: "account disabled"}
	}
	if err := bcrypt.CompareHashAndPassword([]byte(acc.PasswordHash), []byte(password)); err != nil {
		return AuthResult{Reason: "invalid password"}
	}
	if acc.AllowedIP != "" {
		clientIP, _, err := net.SplitHostPort(remoteAddr)
		if err != nil {
			clientIP = remoteAddr
		}
		if clientIP != acc.AllowedIP {
			return AuthResult{Reason: "IP not allowed"}
		}
	}
	return AuthResult{Account: acc, Allowed: true}
}
