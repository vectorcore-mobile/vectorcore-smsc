package forwarder

import (
	"context"
	"sync"
)

// keyedLocks provides one mutual-exclusion lock per key, acquired with a
// context. Entries are removed when no one holds or waits for them. The zero
// value is ready to use.
type keyedLocks struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	ch   chan struct{} // capacity 1: holding a token means holding the lock
	refs int           // holders + waiters
}

// acquire blocks until the lock for key is held or ctx is done. The returned
// release func must be called exactly once.
func (k *keyedLocks) acquire(ctx context.Context, key string) (func(), error) {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = make(map[string]*keyedLock)
	}
	l := k.locks[key]
	if l == nil {
		l = &keyedLock{ch: make(chan struct{}, 1)}
		k.locks[key] = l
	}
	l.refs++
	k.mu.Unlock()

	select {
	case l.ch <- struct{}{}:
		return func() {
			<-l.ch
			k.unref(key, l)
		}, nil
	case <-ctx.Done():
		k.unref(key, l)
		return nil, ctx.Err()
	}
}

func (k *keyedLocks) unref(key string, l *keyedLock) {
	k.mu.Lock()
	l.refs--
	if l.refs == 0 && k.locks[key] == l {
		delete(k.locks, key)
	}
	k.mu.Unlock()
}
