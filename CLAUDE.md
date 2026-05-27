# Webhook Delivery Service — CLAUDE.md

## Project

Production-grade webhook delivery service in Go. Registers webhook URLs, ingests events, delivers to all registered endpoints with at-least-once guarantees.

## Stack

- **Language**: Go 1.26+
- **Router**: `github.com/go-chi/chi/v5`
- **Persistence**: SQLite via `modernc.org/sqlite` (pure Go, no CGO)
- **IDs**: `github.com/google/uuid`
- **Logging**: stdlib `log/slog` (structured JSON)
- **Tracing**: OpenTelemetry (`go.opentelemetry.io/otel`) — stdout by default, OTLP if `OTEL_EXPORTER_OTLP_ENDPOINT` is set
- **Test harness**: stdlib `net/http/httptest` — real local HTTP servers that receive actual webhook POSTs end-to-end

## Directory Structure

```
.
├── cmd/
│   ├── server/main.go       # main entrypoint, wires everything
│   └── harness/main.go      # e2e test harness using httptest servers
├── internal/
│   ├── models/              # domain types (Webhook, Event, Delivery)
│   ├── store/               # SQLite persistence layer
│   ├── delivery/            # delivery engine, worker pool, retry scheduler
│   └── api/                 # HTTP handlers, middleware
├── Makefile
├── README.md
└── CLAUDE.md
```

## API

```
POST   /webhooks              register webhook URL
GET    /webhooks              list registered webhooks
DELETE /webhooks/{id}         deregister webhook
POST   /events                ingest event (triggers delivery)
GET    /events/{id}           get event + delivery statuses
GET    /deliveries            list all deliveries (filterable)
GET    /deliveries/{id}       single delivery detail
GET    /status                health check + aggregate metrics
```

## Domain Models

```go
type Webhook struct {
    ID        string
    URL       string
    Secret    string    // for HMAC signing; optional
    Active    bool
    CreatedAt time.Time
}

type Event struct {
    ID        string
    Type      string         // e.g. "order.created"
    Payload   json.RawMessage
    CreatedAt time.Time
}

type Delivery struct {
    ID            string
    EventID       string
    WebhookID     string
    Status        DeliveryStatus  // pending | delivering | success | failed | dead_lettered
    AttemptCount  int
    NextAttemptAt *time.Time
    LastAttemptAt *time.Time
    LastStatusCode *int
    LastError     string
    CreatedAt     time.Time
}
```

## SQLite Schema

```sql
CREATE TABLE webhooks (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    secret TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE events (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE deliveries (
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

CREATE INDEX idx_deliveries_status_next ON deliveries(status, next_attempt_at);
```

## Delivery Engine

- **Worker pool**: configurable concurrency (default 10 goroutines)
- **Queue**: buffered channel fed by two sources:
  1. New deliveries created on event ingest
  2. Background retry scheduler (polls DB every 5s for overdue retries)
- **Retry schedule** (exponential backoff, max 5 attempts):
  - Attempt 1: immediate
  - Attempt 2: +10s
  - Attempt 3: +60s
  - Attempt 4: +5m
  - Attempt 5: +30m
  - After attempt 5: status → `dead_lettered`
- **Success**: HTTP 2xx from target → status `success`
- **Failure**: non-2xx or network error → status `failed`, schedule retry
- **Timeout**: 10s per delivery attempt

## HMAC Signing

When a webhook has a non-empty `secret`, sign every delivery:

```
X-Webhook-Signature: sha256=<hex(HMAC-SHA256(secret, body))>
X-Webhook-Timestamp: <unix seconds>
X-Webhook-Event-ID: <event id>
X-Webhook-Delivery-ID: <delivery id>
```

Receivers can verify authenticity using the signature and timestamp (reject if >5 min old).

## Observability

`GET /status` returns:

```json
{
  "status": "ok",
  "uptime_seconds": 3600,
  "webhooks": {"total": 5, "active": 4},
  "events": {"total": 1200},
  "deliveries": {
    "total": 4800,
    "pending": 10,
    "success": 4750,
    "failed": 30,
    "dead_lettered": 10
  },
  "delivery_success_rate": 0.9979
}
```

Slog JSON to stdout. Every delivery attempt logged with: delivery_id, webhook_id, event_id, attempt, status_code, duration_ms, error.

## Test Harness (`cmd/harness`)

Uses `net/http/httptest.NewServer` to spin up real local HTTP receivers, no mocking:

1. Start N httptest servers (some healthy, some failing, some flaky)
2. Register each as a webhook via API
3. POST M events to `/events`
4. Poll `/deliveries` until all delivered or dead-lettered
5. Assert delivery counts, retry behavior, HMAC signatures
6. Print summary report

Test scenarios:
- Happy path: all webhooks healthy, all events delivered
- Retry path: webhook initially returns 500, succeeds on attempt 3
- Dead letter path: webhook always fails → dead_lettered after 5 attempts
- Slow webhook: responds in 9s (within timeout)
- Timeout webhook: responds in 11s (exceeds timeout)
- HMAC verification: receiver validates signature

## Configuration (env vars)

```
PORT              HTTP listen port (default: 8080)
DATABASE_PATH     SQLite file path (default: ./webhook.db)
WORKER_COUNT      Delivery worker goroutines (default: 10)
RETRY_POLL_INTERVAL  Retry scheduler poll interval (default: 5s)
```

## Build

```bash
make build       # builds ./bin/server and ./bin/harness
make run         # runs server on :8080
make harness     # runs e2e test harness against running server
make test        # go test ./...
```

## Key Design Decisions

- **Pure Go SQLite** (`modernc.org/sqlite`): no CGO required, single binary, easy deployment
- **httptest for harness**: real HTTP round-trips, no mocking magic — tests actual delivery logic including retries, timeouts, HMAC
- **DB-backed retry queue**: survives server restart; retry scheduler re-enqueues on startup
- **At-least-once semantics**: delivery may succeed but DB update fail — idempotency key (`delivery_id`) in headers lets receivers deduplicate
- **Dead-letter visible in API**: operators can inspect and requeue (future) rather than silently dropping
