package sqlite

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

// ChangeEvent is re-exported so callers only import this package.
type ChangeEvent = store.ChangeEvent

// notifier polls each subscribed table for changes by tracking the maximum
// updated_at timestamp seen so far.
type notifier struct {
	db           *sql.DB
	pollInterval time.Duration

	mu   sync.Mutex
	subs map[string][]chan<- ChangeEvent // table → subscriber channels
	seen map[string]string               // table → last seen updated_at (ISO8601)

	stopCh chan struct{}
	once   sync.Once
}

func newNotifier(db *sql.DB, pollInterval time.Duration) *notifier {
	d := pollInterval
	if d <= 0 {
		d = 2 * time.Second
	}
	return &notifier{
		db:           db,
		pollInterval: d,
		subs:         make(map[string][]chan<- ChangeEvent),
		seen:         make(map[string]string),
		stopCh:       make(chan struct{}),
	}
}

func (n *notifier) subscribe(table string, ch chan<- ChangeEvent) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.subs[table] = append(n.subs[table], ch)
	// Seed the baseline so the first poll doesn't fire spuriously for
	// tables that already have rows.
	if _, seeded := n.seen[table]; !seeded {
		row := n.db.QueryRowContext(context.Background(),
			"SELECT COALESCE(MAX(updated_at),'') FROM "+table)
		var maxUpdated string
		if err := row.Scan(&maxUpdated); err == nil {
			n.seen[table] = maxUpdated
		}
	}
}

func (n *notifier) run(ctx context.Context) {
	ticker := time.NewTicker(n.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.poll(ctx)
		}
	}
}

func (n *notifier) poll(ctx context.Context) {
	n.mu.Lock()
	tables := make([]string, 0, len(n.subs))
	for t := range n.subs {
		tables = append(tables, t)
	}
	n.mu.Unlock()

	for _, table := range tables {
		n.checkTable(ctx, table)
	}
}

func (n *notifier) checkTable(ctx context.Context, table string) {
	row := n.db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(updated_at),'') FROM "+table)
	var maxUpdated string
	if err := row.Scan(&maxUpdated); err != nil {
		slog.Warn("sqlite poll error", "table", table, "err", err)
		return
	}

	n.mu.Lock()
	last := n.seen[table]
	if maxUpdated != "" && maxUpdated > last {
		n.seen[table] = maxUpdated
		chs := n.subs[table]
		n.mu.Unlock()
		ev := ChangeEvent{Table: table, Operation: "UPDATE"}
		for _, ch := range chs {
			select {
			case ch <- ev:
			default:
			}
		}
	} else {
		n.mu.Unlock()
	}
}

func (n *notifier) stop() {
	n.once.Do(func() { close(n.stopCh) })
}
