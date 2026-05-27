# Webhook Delivery Service

A production-grade webhook delivery service in Go. Register endpoints, ingest events, and get at-least-once delivery with retries, HMAC signing, and full observability.

## Features

- Register and manage webhook endpoints
- Ingest events that fan out to all active webhooks
- Persistent delivery queue — survives restarts
- Exponential backoff retries (5 attempts) with dead-letter tracking
- HMAC-SHA256 request signing for endpoint authentication
- Structured JSON logging + metrics endpoint
- End-to-end test harness using real local HTTP servers

## Quick Start

```bash
make build
make run        # starts server on :8080
```

In another terminal:

```bash
# Register a webhook
curl -X POST http://localhost:8080/webhooks \
  -H "Content-Type: application/json" \
  -d '{"url": "https://your-endpoint.example.com/hook", "secret": "optional-secret"}'

# Ingest an event
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"type": "order.created", "payload": {"order_id": "123", "amount": 9900}}'

# Check delivery status
curl http://localhost:8080/status
```

## API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/webhooks` | Register a webhook URL |
| `GET` | `/webhooks` | List all registered webhooks |
| `DELETE` | `/webhooks/{id}` | Deregister a webhook |
| `POST` | `/events` | Ingest an event (triggers delivery) |
| `GET` | `/events/{id}` | Get event details + delivery statuses |
| `GET` | `/deliveries` | List all deliveries |
| `GET` | `/deliveries/{id}` | Single delivery detail |
| `GET` | `/status` | Health check + aggregate metrics |

### Register a Webhook

```bash
curl -X POST http://localhost:8080/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://your-endpoint.example.com/hook",
    "secret": "my-secret"
  }'
```

```json
{
  "id": "01930c4a-...",
  "url": "https://your-endpoint.example.com/hook",
  "active": true,
  "created_at": "2026-05-27T17:00:00Z"
}
```

### Ingest an Event

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "type": "order.created",
    "payload": {"order_id": "123", "amount": 9900}
  }'
```

```json
{
  "id": "01930c4b-...",
  "type": "order.created",
  "created_at": "2026-05-27T17:00:01Z"
}
```

Delivery to all active webhooks begins immediately in the background.

### Status / Metrics

```bash
curl http://localhost:8080/status
```

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

## Delivery Semantics

**At-least-once delivery.** Every event is delivered to every active webhook. If a delivery fails, it is retried with exponential backoff:

| Attempt | Delay after failure |
|---------|---------------------|
| 1 | immediate |
| 2 | +10s |
| 3 | +60s |
| 4 | +5m |
| 5 | +30m |
| — | dead_lettered |

A delivery succeeds on any HTTP 2xx response. Non-2xx responses and network errors are treated as failures. Each attempt times out after 10 seconds.

Dead-lettered deliveries are visible in `/deliveries` so operators can investigate.

## HMAC Signing

When a webhook is registered with a `secret`, every request includes a signature:

```
X-Webhook-Signature:   sha256=<hex(HMAC-SHA256(secret, body))>
X-Webhook-Timestamp:   <unix timestamp>
X-Webhook-Event-ID:    <event id>
X-Webhook-Delivery-ID: <delivery id>
```

Verify on the receiver side:

```go
mac := hmac.New(sha256.New, []byte(secret))
mac.Write(body)
expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
if !hmac.Equal([]byte(expected), []byte(r.Header.Get("X-Webhook-Signature"))) {
    // reject
}
```

Reject requests where `X-Webhook-Timestamp` is more than 5 minutes old to prevent replay attacks. The `X-Webhook-Delivery-ID` header is an idempotency key — use it to deduplicate in case a delivery succeeds but the server fails before recording it.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `PORT` | `8080` | HTTP listen port |
| `DATABASE_PATH` | `./webhook.db` | SQLite database file |
| `WORKER_COUNT` | `10` | Concurrent delivery workers |
| `RETRY_POLL_INTERVAL` | `5s` | How often the retry scheduler polls |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(unset)* | If set, exports traces via OTLP HTTP (e.g. `http://localhost:4318`) |

## Test Harness

The harness spins up real local HTTP servers (no mocking) and exercises the full delivery pipeline end-to-end:

```bash
# In one terminal:
make run

# In another:
make harness
```

Scenarios covered:

- **Happy path** — all webhooks healthy, all events delivered on first attempt
- **Retry path** — webhook returns 500 twice, succeeds on attempt 3
- **Dead letter** — webhook always fails, delivery dead-lettered after 5 attempts
- **Slow webhook** — responds in 9s (within the 10s timeout)
- **Timeout** — responds in 11s, delivery fails and retries
- **HMAC verification** — receiver validates the signature header

## Build

```bash
make build     # ./bin/server and ./bin/harness
make run       # server on :8080
make harness   # e2e test harness (server must be running)
make test      # go test ./...
```

## Design Notes

**Pure Go SQLite** (`modernc.org/sqlite`) — no CGO, single static binary, trivial deployment.

**DB-backed retry queue** — delivery state lives in SQLite, so retries survive server restarts. The retry scheduler polls for overdue deliveries on startup, so nothing gets lost.

**Real HTTP in the test harness** — `net/http/httptest.NewServer` creates actual listening servers. The harness exercises the real delivery path including timeouts, connection errors, and HMAC verification — no mocking layer to lie to you.

**Dead letters are observable, not silent** — failed deliveries after max retries are marked `dead_lettered` and remain visible in the API. Inspect them, requeue them (future), or alert on them.

**OpenTelemetry tracing** — every HTTP request, store operation, and delivery attempt produces a span. The HTTP server uses `otelhttp` middleware; the delivery HTTP client wraps its transport with `otelhttp.NewTransport` so outbound calls are traced too. By default traces go to stdout. Point `OTEL_EXPORTER_OTLP_ENDPOINT` at a collector (Jaeger, Tempo, etc.) for production use.

## Stack

- Go 1.26+
- [chi](https://github.com/go-chi/chi) — HTTP router
- [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) — pure Go SQLite
- [google/uuid](https://github.com/google/uuid) — UUIDs
- [OpenTelemetry Go](https://opentelemetry.io/docs/languages/go/) — distributed tracing
- `log/slog` — structured JSON logging (stdlib)
- `net/http/httptest` — test harness servers (stdlib)
