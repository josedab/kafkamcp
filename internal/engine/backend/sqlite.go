package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	_ "modernc.org/sqlite" // pure-Go SQLite driver, no cgo
)

// SQLite is a Backend backed by an embedded SQLite database opened in WAL
// mode. It is intended for single-node KafkaMCP deployments that need
// durability for engine state across restarts.
//
// A single table (`kv`) is created with the schema:
//
//	CREATE TABLE kv (
//	    namespace TEXT NOT NULL,
//	    key       TEXT NOT NULL,
//	    value     BLOB NOT NULL,
//	    PRIMARY KEY (namespace, key)
//	);
//
// All access goes through the standard `database/sql` connection pool,
// which is set to a single connection to keep WAL writers serialised and
// to avoid `SQLITE_BUSY` under concurrent load.
type SQLite struct {
	db   *sql.DB
	path string

	mu     sync.Mutex
	closed bool
}

// SQLiteOptions configures a [SQLite] backend.
type SQLiteOptions struct {
	// Path is the on-disk filename of the SQLite database. Required.
	Path string
}

// OpenSQLite opens (and lazily migrates) a SQLite-backed [Backend].
func OpenSQLite(opts SQLiteOptions) (*SQLite, error) {
	if opts.Path == "" {
		return nil, errors.New("kafkamcp/engine/backend: sqlite path is required")
	}

	// Reject any path containing parent traversals; engines should pass
	// fully-resolved paths from validated config. We check the raw input
	// before cleaning because filepath.Clean collapses traversals.
	if strings.Contains(opts.Path, "..") {
		return nil, fmt.Errorf("kafkamcp/engine/backend: sqlite path must not contain '..': %s", opts.Path)
	}
	clean := filepath.Clean(opts.Path)

	// WAL mode + busy timeout: single-writer/multi-reader concurrency
	// with bounded waits. _foreign_keys is enabled defensively; we don't
	// use foreign keys today but future schema additions might.
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)", clean)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("kafkamcp/engine/backend: open sqlite: %w", err)
	}

	// Pin the pool to a single connection. modernc.org/sqlite is safe
	// for concurrent use, but WAL writers must still serialise; using
	// one connection makes that explicit and avoids `SQLITE_BUSY`
	// surprises under load.
	db.SetMaxOpenConns(1)

	b := &SQLite{db: db, path: clean}
	if err := b.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return b, nil
}

const schemaDDL = `
CREATE TABLE IF NOT EXISTS kv (
    namespace TEXT NOT NULL,
    key       TEXT NOT NULL,
    value     BLOB NOT NULL,
    PRIMARY KEY (namespace, key)
);
CREATE INDEX IF NOT EXISTS kv_ns_prefix ON kv(namespace, key);
`

func (b *SQLite) migrate() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := b.db.ExecContext(ctx, schemaDDL); err != nil {
		return fmt.Errorf("kafkamcp/engine/backend: migrate: %w", err)
	}
	return nil
}

// Path returns the on-disk path of the SQLite database. Useful for tests.
func (b *SQLite) Path() string { return b.path }

// Get implements Backend.
func (b *SQLite) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if b.isClosed() {
		return nil, ErrClosed
	}
	var v []byte
	err := b.db.QueryRowContext(ctx, `SELECT value FROM kv WHERE namespace = ? AND key = ?`, namespace, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("kafkamcp/engine/backend: get %s/%s: %w", namespace, key, err)
	}
	return v, nil
}

// Set implements Backend.
func (b *SQLite) Set(ctx context.Context, namespace, key string, value []byte) error {
	if b.isClosed() {
		return ErrClosed
	}
	_, err := b.db.ExecContext(ctx, `
INSERT INTO kv (namespace, key, value) VALUES (?, ?, ?)
ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value
`, namespace, key, value)
	if err != nil {
		return fmt.Errorf("kafkamcp/engine/backend: set %s/%s: %w", namespace, key, err)
	}
	return nil
}

// Delete implements Backend.
func (b *SQLite) Delete(ctx context.Context, namespace, key string) error {
	if b.isClosed() {
		return ErrClosed
	}
	_, err := b.db.ExecContext(ctx, `DELETE FROM kv WHERE namespace = ? AND key = ?`, namespace, key)
	if err != nil {
		return fmt.Errorf("kafkamcp/engine/backend: delete %s/%s: %w", namespace, key, err)
	}
	return nil
}

// List implements Backend.
func (b *SQLite) List(ctx context.Context, namespace, prefix string) ([]Entry, error) {
	if b.isClosed() {
		return nil, ErrClosed
	}
	var (
		rows *sql.Rows
		err  error
	)
	if prefix == "" {
		rows, err = b.db.QueryContext(ctx, `SELECT key, value FROM kv WHERE namespace = ?`, namespace)
	} else {
		// LIKE pattern needs '%' suffix; escape SQL LIKE wildcards in
		// the supplied prefix so callers can pass arbitrary bytes.
		esc := escapeLike(prefix)
		rows, err = b.db.QueryContext(ctx,
			`SELECT key, value FROM kv WHERE namespace = ? AND key LIKE ? ESCAPE '\'`,
			namespace, esc+"%")
	}
	if err != nil {
		return nil, fmt.Errorf("kafkamcp/engine/backend: list %s/%s: %w", namespace, prefix, err)
	}
	defer rows.Close()

	var out []Entry
	for rows.Next() {
		var (
			k string
			v []byte
		)
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("kafkamcp/engine/backend: scan: %w", err)
		}
		out = append(out, Entry{Namespace: namespace, Key: k, Value: v})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kafkamcp/engine/backend: rows: %w", err)
	}
	return out, nil
}

// Close implements Backend.
func (b *SQLite) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	return b.db.Close()
}

func (b *SQLite) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// escapeLike escapes the SQLite LIKE wildcard characters ('%' and '_')
// using '\' as the escape character (see the ESCAPE clause in List).
func escapeLike(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}
