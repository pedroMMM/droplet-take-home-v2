package delivery

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/pedroMMM/droplet-take-home-v2/internal/models"
	"github.com/pedroMMM/droplet-take-home-v2/internal/store"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("webhook-delivery/delivery")

// Engine is a worker-pool + retry scheduler that drains a queue of pending
// deliveries from the store and pushes them over HTTP.
type Engine struct {
	store   *store.Store
	queue   chan string
	workers int
	client  *http.Client
	logger  *slog.Logger

	retryPollInterval time.Duration

	nowFunc func() time.Time

	wg       sync.WaitGroup
	stopOnce sync.Once
	stopCh   chan struct{}
}

// New constructs an Engine. Use Start to begin processing.
func New(s *store.Store, workers int) *Engine {
	if workers <= 0 {
		workers = 10
	}
	return &Engine{
		store:             s,
		queue:             make(chan string, workers*10),
		workers:           workers,
		retryPollInterval: 5 * time.Second,
		client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
		logger:  slog.Default(),
		nowFunc: time.Now,
		stopCh:  make(chan struct{}),
	}
}

// now returns the engine's current time in UTC, using the injected clock.
func (e *Engine) now() time.Time {
	return e.nowFunc().UTC()
}

// WithClock sets a custom time source (for testing).
func (e *Engine) WithClock(fn func() time.Time) *Engine {
	e.nowFunc = fn
	return e
}

// SetLogger overrides the default logger.
func (e *Engine) SetLogger(l *slog.Logger) {
	if l != nil {
		e.logger = l
	}
}

// SetRetryPollInterval overrides how often the scheduler polls for overdue
// deliveries.
func (e *Engine) SetRetryPollInterval(d time.Duration) {
	if d > 0 {
		e.retryPollInterval = d
	}
}

// Start launches the worker pool and the retry scheduler goroutine.
// Start returns immediately; Stop blocks until everything drains.
func (e *Engine) Start(ctx context.Context) {
	for i := 0; i < e.workers; i++ {
		e.wg.Add(1)
		go e.worker(ctx, i)
	}
	e.wg.Add(1)
	go e.scheduler(ctx)
}

// Stop signals all goroutines to exit and waits for them.
func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
	})
	e.wg.Wait()
}

// Enqueue submits a delivery id to the worker queue. If the queue is full,
// the delivery is dropped here but the retry scheduler will pick it up.
func (e *Engine) Enqueue(deliveryID string) {
	select {
	case e.queue <- deliveryID:
	default:
		// Queue full — the retry scheduler will re-discover this delivery
		// on its next tick.
		e.logger.Warn("delivery queue full, dropping enqueue",
			"delivery_id", deliveryID)
	}
}

// worker drains the queue and processes deliveries.
func (e *Engine) worker(ctx context.Context, _ int) {
	defer e.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		case deliveryID := <-e.queue:
			e.processDelivery(ctx, deliveryID)
		}
	}
}

// scheduler periodically re-enqueues overdue deliveries from the store.
func (e *Engine) scheduler(ctx context.Context) {
	defer e.wg.Done()
	ticker := time.NewTicker(e.retryPollInterval)
	defer ticker.Stop()

	// First run immediately so newly-started servers pick up backlog quickly.
	e.requeueOverdue(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.requeueOverdue(ctx)
		}
	}
}

func (e *Engine) requeueOverdue(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "delivery.scheduler.requeue")
	defer span.End()
	overdue, err := e.store.ListOverdueDeliveries(ctx, e.now())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		e.logger.Error("scheduler: list overdue deliveries failed", "error", err)
		return
	}
	for _, d := range overdue {
		e.Enqueue(d.ID)
	}
}

// backoffFor returns the wait duration for the next retry after the given
// attempt has just failed. attempt is the post-increment attempt count.
func backoffFor(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 10 * time.Second
	case 2:
		return 60 * time.Second
	case 3:
		return 5 * time.Minute
	case 4:
		return 30 * time.Minute
	default:
		return 30 * time.Minute
	}
}

const maxAttempts = 5

// processDelivery loads a delivery + its webhook + event, performs the HTTP
// POST, and updates the row with the result.
func (e *Engine) processDelivery(ctx context.Context, deliveryID string) {
	ctx, span := tracer.Start(ctx, "delivery.process")
	defer span.End()
	span.SetAttributes(attribute.String("delivery.id", deliveryID))

	d, err := e.store.GetDelivery(ctx, deliveryID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		e.logger.Error("load delivery", "delivery_id", deliveryID, "error", err)
		return
	}

	span.SetAttributes(
		attribute.String("webhook.id", d.WebhookID),
		attribute.String("event.id", d.EventID),
	)

	// Only process pending/failed.
	if d.Status != models.StatusPending && d.Status != models.StatusFailed {
		return
	}

	webhook, err := e.store.GetWebhook(ctx, d.WebhookID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		e.logger.Error("load webhook", "delivery_id", deliveryID,
			"webhook_id", d.WebhookID, "error", err)
		return
	}
	if !webhook.Active {
		// Webhook deactivated after delivery was created — leave as-is.
		return
	}

	event, err := e.store.GetEvent(ctx, d.EventID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		e.logger.Error("load event", "delivery_id", deliveryID,
			"event_id", d.EventID, "error", err)
		return
	}

	// Mark as delivering. Increment attempt count.
	now := e.now()
	d.Status = models.StatusDelivering
	d.AttemptCount++
	d.LastAttemptAt = &now
	if err := e.store.UpdateDelivery(ctx, d); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		e.logger.Error("mark delivering", "delivery_id", deliveryID, "error", err)
		return
	}

	statusCode, attemptErr, duration := e.attempt(ctx, webhook, event, d)

	// Record outcome.
	if statusCode > 0 {
		d.LastStatusCode = &statusCode
	} else {
		d.LastStatusCode = nil
	}
	if attemptErr != nil {
		d.LastError = attemptErr.Error()
	} else {
		d.LastError = ""
	}

	success := attemptErr == nil && statusCode >= 200 && statusCode < 300

	if success {
		d.Status = models.StatusSuccess
		d.NextAttemptAt = nil
	} else if d.AttemptCount >= maxAttempts {
		d.Status = models.StatusDeadLettered
		d.NextAttemptAt = nil
	} else {
		d.Status = models.StatusFailed
		next := e.now().Add(backoffFor(d.AttemptCount))
		d.NextAttemptAt = &next
	}

	if err := e.store.UpdateDelivery(ctx, d); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		e.logger.Error("update delivery result", "delivery_id", deliveryID, "error", err)
		return
	}

	if d.Status == models.StatusDeadLettered {
		span.SetStatus(codes.Error, "delivery dead-lettered after max attempts")
	}

	logArgs := []any{
		"delivery_id", d.ID,
		"webhook_id", d.WebhookID,
		"event_id", d.EventID,
		"attempt", d.AttemptCount,
		"status_code", statusCode,
		"duration_ms", duration.Milliseconds(),
		"final_status", string(d.Status),
	}
	if attemptErr != nil {
		logArgs = append(logArgs, "error", attemptErr.Error())
	}
	if success {
		e.logger.Info("delivery success", logArgs...)
	} else if d.Status == models.StatusDeadLettered {
		e.logger.Warn("delivery dead-lettered", logArgs...)
	} else {
		e.logger.Warn("delivery failed", logArgs...)
	}
}

// attempt performs a single HTTP delivery. Returns status code (0 if no
// response), an error (nil on transport success even if non-2xx), and the
// wall-clock duration of the request.
func (e *Engine) attempt(ctx context.Context, w *models.Webhook, ev *models.Event, d *models.Delivery) (int, error, time.Duration) {
	ctx, span := tracer.Start(ctx, "delivery.attempt")
	defer span.End()
	span.SetAttributes(
		attribute.String("url", w.URL),
		attribute.Int("attempt_count", d.AttemptCount),
	)

	body := []byte(ev.Payload)
	if len(body) == 0 {
		body = []byte("null")
	}

	reqCtx, cancel := context.WithTimeout(ctx, e.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("build request: %w", err), 0
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Event-ID", ev.ID)
	req.Header.Set("X-Webhook-Delivery-ID", d.ID)
	req.Header.Set("X-Webhook-Timestamp", strconv.FormatInt(e.now().Unix(), 10))
	if ev.Type != "" {
		req.Header.Set("X-Webhook-Event-Type", ev.Type)
	}
	if w.Secret != "" {
		req.Header.Set("X-Webhook-Signature", signature(w.Secret, body))
	}

	start := time.Now()
	resp, err := e.client.Do(req)
	duration := time.Since(start)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err, duration
	}
	defer resp.Body.Close()
	// Drain body so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	span.SetAttributes(attribute.Int("status_code", resp.StatusCode))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		nonOK := errors.New("non-2xx response: " + strconv.Itoa(resp.StatusCode))
		span.RecordError(nonOK)
		span.SetStatus(codes.Error, nonOK.Error())
		return resp.StatusCode, nonOK, duration
	}
	return resp.StatusCode, nil, duration
}

func signature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
