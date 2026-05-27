package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/pedroMMM/droplet-take-home-v2/internal/models"
	"github.com/pedroMMM/droplet-take-home-v2/internal/store"
)

func newStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestWebhookCRUD(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	wh := &models.Webhook{
		ID:        uuid.New().String(),
		URL:       "https://example.com/hook",
		Secret:    "secret",
		Active:    true,
		CreatedAt: time.Now().UTC(),
	}
	if err := s.CreateWebhook(ctx, wh); err != nil {
		t.Fatalf("create webhook: %v", err)
	}

	got, err := s.GetWebhook(ctx, wh.ID)
	if err != nil {
		t.Fatalf("get webhook: %v", err)
	}
	if got.URL != wh.URL {
		t.Errorf("URL mismatch: got %q want %q", got.URL, wh.URL)
	}
	if !got.Active {
		t.Errorf("expected webhook active=true")
	}

	list, err := s.ListWebhooks(ctx)
	if err != nil {
		t.Fatalf("list webhooks: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 webhook, got %d", len(list))
	}

	if err := s.DeactivateWebhook(ctx, wh.ID); err != nil {
		t.Fatalf("deactivate webhook: %v", err)
	}

	got2, err := s.GetWebhook(ctx, wh.ID)
	if err != nil {
		t.Fatalf("get webhook after deactivate: %v", err)
	}
	if got2.Active {
		t.Errorf("expected webhook Active=false after deactivate, got true")
	}

	active, err := s.ListActiveWebhooks(ctx)
	if err != nil {
		t.Fatalf("list active webhooks: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected 0 active webhooks, got %d", len(active))
	}
}

func TestEventCRUD(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	ev := &models.Event{
		ID:        uuid.New().String(),
		Type:      "test.created",
		Payload:   []byte(`{"hello":"world"}`),
		CreatedAt: time.Now().UTC(),
	}
	if err := s.CreateEvent(ctx, ev); err != nil {
		t.Fatalf("create event: %v", err)
	}

	got, err := s.GetEvent(ctx, ev.ID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if got.Type != ev.Type {
		t.Errorf("Type mismatch: got %q want %q", got.Type, ev.Type)
	}
	if string(got.Payload) != string(ev.Payload) {
		t.Errorf("Payload mismatch: got %q want %q", string(got.Payload), string(ev.Payload))
	}
}

func TestDeliveryCRUD(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	wh := &models.Webhook{
		ID:        uuid.New().String(),
		URL:       "https://example.com/hook",
		Active:    true,
		CreatedAt: time.Now().UTC(),
	}
	if err := s.CreateWebhook(ctx, wh); err != nil {
		t.Fatalf("create webhook: %v", err)
	}
	ev := &models.Event{
		ID:        uuid.New().String(),
		Type:      "t",
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}
	if err := s.CreateEvent(ctx, ev); err != nil {
		t.Fatalf("create event: %v", err)
	}

	now := time.Now().UTC()
	d := &models.Delivery{
		ID:            uuid.New().String(),
		EventID:       ev.ID,
		WebhookID:     wh.ID,
		Status:        models.StatusPending,
		NextAttemptAt: &now,
		CreatedAt:     now,
	}
	if err := s.CreateDelivery(ctx, d); err != nil {
		t.Fatalf("create delivery: %v", err)
	}

	got, err := s.GetDelivery(ctx, d.ID)
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if got.Status != models.StatusPending {
		t.Errorf("status mismatch: got %s want %s", got.Status, models.StatusPending)
	}

	// Update status fields.
	code := 200
	last := time.Now().UTC()
	got.Status = models.StatusSuccess
	got.AttemptCount = 2
	got.LastStatusCode = &code
	got.LastAttemptAt = &last
	got.LastError = "none"
	got.NextAttemptAt = nil
	if err := s.UpdateDelivery(ctx, got); err != nil {
		t.Fatalf("update delivery: %v", err)
	}

	updated, err := s.GetDelivery(ctx, d.ID)
	if err != nil {
		t.Fatalf("get delivery after update: %v", err)
	}
	if updated.Status != models.StatusSuccess {
		t.Errorf("status not updated: got %s", updated.Status)
	}
	if updated.AttemptCount != 2 {
		t.Errorf("attempt_count not updated: got %d", updated.AttemptCount)
	}
	if updated.LastStatusCode == nil || *updated.LastStatusCode != 200 {
		t.Errorf("last_status_code not updated: %+v", updated.LastStatusCode)
	}
	if updated.NextAttemptAt != nil {
		t.Errorf("next_attempt_at expected nil, got %v", updated.NextAttemptAt)
	}
}

func TestListOverdueDeliveries(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	wh := &models.Webhook{ID: uuid.New().String(), URL: "https://example.com", Active: true}
	if err := s.CreateWebhook(ctx, wh); err != nil {
		t.Fatalf("create webhook: %v", err)
	}
	ev := &models.Event{ID: uuid.New().String(), Type: "t", Payload: []byte(`{}`)}
	if err := s.CreateEvent(ctx, ev); err != nil {
		t.Fatalf("create event: %v", err)
	}

	past := time.Now().Add(-1 * time.Hour).UTC()
	future := time.Now().Add(1 * time.Hour).UTC()

	overdue := &models.Delivery{
		ID:            uuid.New().String(),
		EventID:       ev.ID,
		WebhookID:     wh.ID,
		Status:        models.StatusPending,
		NextAttemptAt: &past,
	}
	notYet := &models.Delivery{
		ID:            uuid.New().String(),
		EventID:       ev.ID,
		WebhookID:     wh.ID,
		Status:        models.StatusPending,
		NextAttemptAt: &future,
	}
	if err := s.CreateDelivery(ctx, overdue); err != nil {
		t.Fatalf("create overdue: %v", err)
	}
	if err := s.CreateDelivery(ctx, notYet); err != nil {
		t.Fatalf("create future: %v", err)
	}

	list, err := s.ListOverdueDeliveries(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("list overdue: %v", err)
	}

	var sawOverdue, sawFuture bool
	for _, d := range list {
		if d.ID == overdue.ID {
			sawOverdue = true
		}
		if d.ID == notYet.ID {
			sawFuture = true
		}
	}
	if !sawOverdue {
		t.Errorf("expected overdue delivery to appear in list")
	}
	if sawFuture {
		t.Errorf("did not expect future-scheduled delivery in list")
	}
}

func TestGetStats(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	wh := &models.Webhook{ID: uuid.New().String(), URL: "https://example.com", Active: true}
	if err := s.CreateWebhook(ctx, wh); err != nil {
		t.Fatalf("create webhook: %v", err)
	}
	ev := &models.Event{ID: uuid.New().String(), Type: "t", Payload: []byte(`{}`)}
	if err := s.CreateEvent(ctx, ev); err != nil {
		t.Fatalf("create event: %v", err)
	}

	// Create deliveries in various statuses.
	mkDelivery := func(status models.DeliveryStatus) {
		t.Helper()
		d := &models.Delivery{
			ID:        uuid.New().String(),
			EventID:   ev.ID,
			WebhookID: wh.ID,
			Status:    status,
		}
		if err := s.CreateDelivery(ctx, d); err != nil {
			t.Fatalf("create delivery (%s): %v", status, err)
		}
	}
	mkDelivery(models.StatusPending)
	mkDelivery(models.StatusSuccess)
	mkDelivery(models.StatusSuccess)
	mkDelivery(models.StatusFailed)
	mkDelivery(models.StatusDeadLettered)

	stats, err := s.GetStats(ctx)
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}

	if stats.TotalWebhooks != 1 {
		t.Errorf("TotalWebhooks: got %d want 1", stats.TotalWebhooks)
	}
	if stats.ActiveWebhooks != 1 {
		t.Errorf("ActiveWebhooks: got %d want 1", stats.ActiveWebhooks)
	}
	if stats.TotalEvents != 1 {
		t.Errorf("TotalEvents: got %d want 1", stats.TotalEvents)
	}
	if stats.TotalDeliveries != 5 {
		t.Errorf("TotalDeliveries: got %d want 5", stats.TotalDeliveries)
	}
	if stats.ByStatus[models.StatusPending] != 1 {
		t.Errorf("ByStatus[pending]: got %d want 1", stats.ByStatus[models.StatusPending])
	}
	if stats.ByStatus[models.StatusSuccess] != 2 {
		t.Errorf("ByStatus[success]: got %d want 2", stats.ByStatus[models.StatusSuccess])
	}
	if stats.ByStatus[models.StatusFailed] != 1 {
		t.Errorf("ByStatus[failed]: got %d want 1", stats.ByStatus[models.StatusFailed])
	}
	if stats.ByStatus[models.StatusDeadLettered] != 1 {
		t.Errorf("ByStatus[dead_lettered]: got %d want 1", stats.ByStatus[models.StatusDeadLettered])
	}
}
