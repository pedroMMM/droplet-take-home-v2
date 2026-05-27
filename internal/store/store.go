package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/pedroMMM/droplet-take-home-v2/internal/models"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	_ "modernc.org/sqlite"
)

var tracer = otel.Tracer("webhook-delivery/store")

const schema = `
CREATE TABLE IF NOT EXISTS webhooks (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    secret TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(id),
    webhook_id TEXT NOT NULL REFERENCES webhooks(id),
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at DATETIME,
    last_attempt_at DATETIME,
    last_status_code INTEGER,
    last_error TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_deliveries_status_next ON deliveries(status, next_attempt_at);
`

// Store wraps a SQLite database with typed accessors for webhooks, events,
// and deliveries.
type Store struct {
	db *sql.DB
}

// Stats summarizes counts across the persistence layer.
type Stats struct {
	TotalWebhooks   int
	ActiveWebhooks  int
	TotalEvents     int
	TotalDeliveries int
	ByStatus        map[models.DeliveryStatus]int
}

// Open opens (or creates) the SQLite database at path, applies the schema,
// and configures pragmas (WAL + foreign keys).
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// modernc sqlite is single-connection-friendly; allow more reads, but
	// keep writes serialized via WAL.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set wal: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set foreign_keys: %w", err)
	}

	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

// --- time helpers ---------------------------------------------------------

func fmtTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	// Try a couple of layouts that SQLite returns.
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, l := range layouts {
		t, err := time.Parse(l, s)
		if err == nil {
			return t, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return fmtTime(*t)
}

func nullableInt(i *int) any {
	if i == nil {
		return nil
	}
	return *i
}

// --- webhooks -------------------------------------------------------------

// CreateWebhook inserts a new webhook row.
func (s *Store) CreateWebhook(ctx context.Context, w *models.Webhook) error {
	ctx, span := tracer.Start(ctx, "store.CreateWebhook")
	defer span.End()
	if w.CreatedAt.IsZero() {
		w.CreatedAt = time.Now().UTC()
	}
	active := 0
	if w.Active {
		active = 1
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO webhooks (id, url, secret, active, created_at) VALUES (?, ?, ?, ?, ?)`,
		w.ID, w.URL, w.Secret, active, fmtTime(w.CreatedAt),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("insert webhook: %w", err)
	}
	return nil
}

func (s *Store) scanWebhook(scanner interface {
	Scan(...any) error
}) (*models.Webhook, error) {
	var (
		w         models.Webhook
		active    int
		createdAt string
	)
	if err := scanner.Scan(&w.ID, &w.URL, &w.Secret, &active, &createdAt); err != nil {
		return nil, err
	}
	w.Active = active != 0
	t, err := parseTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse webhook created_at: %w", err)
	}
	w.CreatedAt = t
	return &w, nil
}

// GetWebhook returns the webhook with the given id, or sql.ErrNoRows if missing.
func (s *Store) GetWebhook(ctx context.Context, id string) (*models.Webhook, error) {
	ctx, span := tracer.Start(ctx, "store.GetWebhook")
	defer span.End()
	row := s.db.QueryRowContext(ctx,
		`SELECT id, url, secret, active, created_at FROM webhooks WHERE id = ?`, id)
	w, err := s.scanWebhook(row)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return w, nil
}

// ListWebhooks returns all webhooks ordered by creation time descending.
func (s *Store) ListWebhooks(ctx context.Context) ([]*models.Webhook, error) {
	ctx, span := tracer.Start(ctx, "store.ListWebhooks")
	defer span.End()
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, url, secret, active, created_at FROM webhooks ORDER BY created_at DESC`)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("query webhooks: %w", err)
	}
	defer rows.Close()

	var out []*models.Webhook
	for rows.Next() {
		w, err := s.scanWebhook(rows)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return out, nil
}

// ListActiveWebhooks returns only active webhooks.
func (s *Store) ListActiveWebhooks(ctx context.Context) ([]*models.Webhook, error) {
	ctx, span := tracer.Start(ctx, "store.ListActiveWebhooks")
	defer span.End()
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, url, secret, active, created_at FROM webhooks WHERE active = 1 ORDER BY created_at DESC`)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("query active webhooks: %w", err)
	}
	defer rows.Close()

	var out []*models.Webhook
	for rows.Next() {
		w, err := s.scanWebhook(rows)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return out, nil
}

// DeactivateWebhook flips active to 0. Returns sql.ErrNoRows if id missing.
func (s *Store) DeactivateWebhook(ctx context.Context, id string) error {
	ctx, span := tracer.Start(ctx, "store.DeactivateWebhook")
	defer span.End()
	res, err := s.db.ExecContext(ctx, `UPDATE webhooks SET active = 0 WHERE id = ?`, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("deactivate webhook: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if n == 0 {
		span.RecordError(sql.ErrNoRows)
		span.SetStatus(codes.Error, sql.ErrNoRows.Error())
		return sql.ErrNoRows
	}
	return nil
}

// --- events ---------------------------------------------------------------

// CreateEvent inserts a new event row.
func (s *Store) CreateEvent(ctx context.Context, e *models.Event) error {
	ctx, span := tracer.Start(ctx, "store.CreateEvent")
	defer span.End()
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now().UTC()
	}
	if len(e.Payload) == 0 {
		e.Payload = []byte("null")
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO events (id, type, payload, created_at) VALUES (?, ?, ?, ?)`,
		e.ID, e.Type, string(e.Payload), fmtTime(e.CreatedAt),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// GetEvent returns the event with id, or sql.ErrNoRows.
func (s *Store) GetEvent(ctx context.Context, id string) (*models.Event, error) {
	ctx, span := tracer.Start(ctx, "store.GetEvent")
	defer span.End()
	row := s.db.QueryRowContext(ctx,
		`SELECT id, type, payload, created_at FROM events WHERE id = ?`, id)
	var (
		e         models.Event
		payload   string
		createdAt string
	)
	if err := row.Scan(&e.ID, &e.Type, &payload, &createdAt); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	e.Payload = []byte(payload)
	t, err := parseTime(createdAt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("parse event created_at: %w", err)
	}
	e.CreatedAt = t
	return &e, nil
}

// --- deliveries -----------------------------------------------------------

// CreateDelivery inserts a new delivery row.
func (s *Store) CreateDelivery(ctx context.Context, d *models.Delivery) error {
	ctx, span := tracer.Start(ctx, "store.CreateDelivery")
	defer span.End()
	if d.CreatedAt.IsZero() {
		d.CreatedAt = time.Now().UTC()
	}
	if d.Status == "" {
		d.Status = models.StatusPending
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO deliveries (
            id, event_id, webhook_id, status, attempt_count,
            next_attempt_at, last_attempt_at, last_status_code, last_error, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		d.ID, d.EventID, d.WebhookID, string(d.Status), d.AttemptCount,
		nullableTime(d.NextAttemptAt), nullableTime(d.LastAttemptAt),
		nullableInt(d.LastStatusCode), d.LastError, fmtTime(d.CreatedAt),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("insert delivery: %w", err)
	}
	return nil
}

func (s *Store) scanDelivery(scanner interface {
	Scan(...any) error
}) (*models.Delivery, error) {
	var (
		d              models.Delivery
		status         string
		nextAttemptAt  sql.NullString
		lastAttemptAt  sql.NullString
		lastStatusCode sql.NullInt64
		createdAt      string
	)
	if err := scanner.Scan(
		&d.ID, &d.EventID, &d.WebhookID, &status, &d.AttemptCount,
		&nextAttemptAt, &lastAttemptAt, &lastStatusCode, &d.LastError, &createdAt,
	); err != nil {
		return nil, err
	}
	d.Status = models.DeliveryStatus(status)
	if nextAttemptAt.Valid {
		t, err := parseTime(nextAttemptAt.String)
		if err != nil {
			return nil, fmt.Errorf("parse next_attempt_at: %w", err)
		}
		d.NextAttemptAt = &t
	}
	if lastAttemptAt.Valid {
		t, err := parseTime(lastAttemptAt.String)
		if err != nil {
			return nil, fmt.Errorf("parse last_attempt_at: %w", err)
		}
		d.LastAttemptAt = &t
	}
	if lastStatusCode.Valid {
		code := int(lastStatusCode.Int64)
		d.LastStatusCode = &code
	}
	t, err := parseTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse delivery created_at: %w", err)
	}
	d.CreatedAt = t
	return &d, nil
}

const deliveryColumns = `id, event_id, webhook_id, status, attempt_count,
    next_attempt_at, last_attempt_at, last_status_code, last_error, created_at`

// GetDelivery returns the delivery by id, or sql.ErrNoRows.
func (s *Store) GetDelivery(ctx context.Context, id string) (*models.Delivery, error) {
	ctx, span := tracer.Start(ctx, "store.GetDelivery")
	defer span.End()
	row := s.db.QueryRowContext(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE id = ?`, id)
	d, err := s.scanDelivery(row)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return d, nil
}

// ListDeliveries returns all deliveries; if status is non-empty, filters by status.
func (s *Store) ListDeliveries(ctx context.Context) ([]*models.Delivery, error) {
	ctx, span := tracer.Start(ctx, "store.ListDeliveries")
	defer span.End()
	out, err := s.queryDeliveries(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries ORDER BY created_at DESC`)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return out, nil
}

// ListDeliveriesByStatus returns deliveries filtered by a single status.
func (s *Store) ListDeliveriesByStatus(ctx context.Context, status models.DeliveryStatus) ([]*models.Delivery, error) {
	ctx, span := tracer.Start(ctx, "store.ListDeliveriesByStatus")
	defer span.End()
	out, err := s.queryDeliveries(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE status = ? ORDER BY created_at DESC`,
		string(status))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return out, nil
}

// ListDeliveriesByEvent returns deliveries for a given event id.
func (s *Store) ListDeliveriesByEvent(ctx context.Context, eventID string) ([]*models.Delivery, error) {
	ctx, span := tracer.Start(ctx, "store.ListDeliveriesByEvent")
	defer span.End()
	out, err := s.queryDeliveries(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE event_id = ? ORDER BY created_at DESC`,
		eventID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return out, nil
}

// ListOverdueDeliveries returns pending/failed deliveries whose next_attempt_at <= now.
// Pending deliveries with NULL next_attempt_at are also returned (newly created).
func (s *Store) ListOverdueDeliveries(ctx context.Context, now time.Time) ([]*models.Delivery, error) {
	ctx, span := tracer.Start(ctx, "store.ListOverdueDeliveries")
	defer span.End()
	out, err := s.queryDeliveries(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries
         WHERE (status = 'pending' OR status = 'failed')
           AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
         ORDER BY created_at ASC`,
		fmtTime(now))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return out, nil
}

func (s *Store) queryDeliveries(ctx context.Context, query string, args ...any) ([]*models.Delivery, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query deliveries: %w", err)
	}
	defer rows.Close()

	var out []*models.Delivery
	for rows.Next() {
		d, err := s.scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// UpdateDelivery overwrites all mutable fields of an existing delivery row.
func (s *Store) UpdateDelivery(ctx context.Context, d *models.Delivery) error {
	ctx, span := tracer.Start(ctx, "store.UpdateDelivery")
	defer span.End()
	res, err := s.db.ExecContext(ctx,
		`UPDATE deliveries SET
            status = ?,
            attempt_count = ?,
            next_attempt_at = ?,
            last_attempt_at = ?,
            last_status_code = ?,
            last_error = ?
         WHERE id = ?`,
		string(d.Status), d.AttemptCount,
		nullableTime(d.NextAttemptAt), nullableTime(d.LastAttemptAt),
		nullableInt(d.LastStatusCode), d.LastError, d.ID,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("update delivery: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if n == 0 {
		span.RecordError(sql.ErrNoRows)
		span.SetStatus(codes.Error, sql.ErrNoRows.Error())
		return sql.ErrNoRows
	}
	return nil
}

// --- stats ----------------------------------------------------------------

// GetStats returns aggregate counts across the database.
func (s *Store) GetStats(ctx context.Context) (*Stats, error) {
	ctx, span := tracer.Start(ctx, "store.GetStats")
	defer span.End()
	stats := &Stats{
		ByStatus: map[models.DeliveryStatus]int{
			models.StatusPending:      0,
			models.StatusDelivering:   0,
			models.StatusSuccess:      0,
			models.StatusFailed:       0,
			models.StatusDeadLettered: 0,
		},
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(active), 0) FROM webhooks`).
		Scan(&stats.TotalWebhooks, &stats.ActiveWebhooks); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("webhook stats: %w", err)
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM events`).Scan(&stats.TotalEvents); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("event stats: %w", err)
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM deliveries`).Scan(&stats.TotalDeliveries); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("delivery total: %w", err)
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM deliveries GROUP BY status`)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("delivery status group: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		stats.ByStatus[models.DeliveryStatus(status)] = n
	}
	if err := rows.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return stats, nil
}

// IsNotFound returns true if err indicates a missing row.
func IsNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
