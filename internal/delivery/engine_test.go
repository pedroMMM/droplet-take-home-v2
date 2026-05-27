package delivery

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/pedroMMM/droplet-take-home-v2/internal/models"
	"github.com/pedroMMM/droplet-take-home-v2/internal/store"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// seed creates a webhook, event, and a pending delivery row in the store and
// returns the delivery id. The webhook URL is set to url and the secret to
// secret.
func seed(t *testing.T, s *store.Store, url, secret string) (whID, evID, delID string) {
	t.Helper()
	ctx := context.Background()
	wh := &models.Webhook{
		ID:        uuid.New().String(),
		URL:       url,
		Secret:    secret,
		Active:    true,
		CreatedAt: time.Now().UTC(),
	}
	if err := s.CreateWebhook(ctx, wh); err != nil {
		t.Fatalf("create webhook: %v", err)
	}
	ev := &models.Event{
		ID:        uuid.New().String(),
		Type:      "test.event",
		Payload:   []byte(`{"hello":"world"}`),
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
	return wh.ID, ev.ID, d.ID
}

func TestSuccessfulDelivery(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, _, delID := seed(t, s, srv.URL, "")

	fixedTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	eng := New(s, 1)
	eng.WithClock(func() time.Time { return fixedTime })

	eng.processDelivery(ctx, delID)

	d, err := s.GetDelivery(ctx, delID)
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if d.Status != models.StatusSuccess {
		t.Errorf("status: got %s want %s", d.Status, models.StatusSuccess)
	}
	if d.AttemptCount != 1 {
		t.Errorf("attempt_count: got %d want 1", d.AttemptCount)
	}
	if hits.Load() != 1 {
		t.Errorf("server hits: got %d want 1", hits.Load())
	}
	if d.NextAttemptAt != nil {
		t.Errorf("expected next_attempt_at nil on success, got %v", d.NextAttemptAt)
	}
}

func TestFailedDeliverySchedulesRetry(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, _, delID := seed(t, s, srv.URL, "")

	fixedTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	eng := New(s, 1)
	eng.WithClock(func() time.Time { return fixedTime })

	eng.processDelivery(ctx, delID)

	d, err := s.GetDelivery(ctx, delID)
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if d.Status != models.StatusFailed {
		t.Errorf("status: got %s want %s", d.Status, models.StatusFailed)
	}
	if d.AttemptCount != 1 {
		t.Errorf("attempt_count: got %d want 1", d.AttemptCount)
	}
	if d.NextAttemptAt == nil {
		t.Fatalf("expected next_attempt_at to be set")
	}
	wantNext := fixedTime.Add(10 * time.Second)
	diff := d.NextAttemptAt.Sub(wantNext)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("next_attempt_at: got %v want ~%v (diff %v)", d.NextAttemptAt, wantNext, diff)
	}
	if d.LastStatusCode == nil || *d.LastStatusCode != http.StatusInternalServerError {
		t.Errorf("last_status_code: got %+v want 500", d.LastStatusCode)
	}
}

// TestDeadLetterAfterMaxAttempts processes a delivery through 5 failing attempts
// using a fake clock to skip over backoff periods without sleeping.
func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, _, delID := seed(t, s, srv.URL, "")

	fakeNow := time.Now().UTC()
	eng := New(s, 1)
	eng.WithClock(func() time.Time { return fakeNow })

	var d *models.Delivery
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		eng.processDelivery(ctx, delID)

		var err error
		d, err = s.GetDelivery(ctx, delID)
		if err != nil {
			t.Fatalf("attempt %d: get delivery: %v", attempt, err)
		}

		if attempt < maxAttempts {
			if d.Status != models.StatusFailed {
				t.Errorf("attempt %d: want %s, got %s", attempt, models.StatusFailed, d.Status)
			}
			if d.NextAttemptAt == nil {
				t.Fatalf("attempt %d: expected next_attempt_at set", attempt)
			}
			// Advance fake clock past the scheduled retry time.
			fakeNow = d.NextAttemptAt.Add(time.Second).UTC()
		}
	}

	if d.Status != models.StatusDeadLettered {
		t.Errorf("after %d attempts: want %s, got %s", maxAttempts, models.StatusDeadLettered, d.Status)
	}
	if d.NextAttemptAt != nil {
		t.Errorf("dead_lettered delivery should have nil next_attempt_at, got %v", d.NextAttemptAt)
	}
	if d.AttemptCount != maxAttempts {
		t.Errorf("attempt_count: got %d want %d", d.AttemptCount, maxAttempts)
	}
}

func TestHMACSignaturePresent(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)

	const secret = "test-secret"

	var gotSig string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSig = r.Header.Get("X-Webhook-Signature")
		body, _ := io.ReadAll(r.Body)
		gotBody = body
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, _, delID := seed(t, s, srv.URL, secret)

	eng := New(s, 1)
	eng.processDelivery(ctx, delID)

	if gotSig == "" {
		t.Fatalf("expected X-Webhook-Signature header to be set")
	}
	if !strings.HasPrefix(gotSig, "sha256=") {
		t.Errorf("expected signature prefix sha256=, got %q", gotSig)
	}

	// Verify the signature ourselves.
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(gotBody)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if gotSig != want {
		t.Errorf("signature mismatch:\n got  %s\n want %s", gotSig, want)
	}

	d, err := s.GetDelivery(ctx, delID)
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if d.Status != models.StatusSuccess {
		t.Errorf("status: got %s want %s", d.Status, models.StatusSuccess)
	}
}

func TestDeliveryTimeout(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)

	// Server that hangs longer than the engine's client timeout. Close the
	// unblock channel before srv.Close so the in-flight handler returns and
	// the test server can shut down cleanly.
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(block)

	_, _, delID := seed(t, s, srv.URL, "")

	eng := New(s, 1)
	// Shorten timeout so the test stays fast.
	eng.client.Timeout = 500 * time.Millisecond

	eng.processDelivery(ctx, delID)

	d, err := s.GetDelivery(ctx, delID)
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if d.Status != models.StatusFailed {
		t.Errorf("status: got %s want %s", d.Status, models.StatusFailed)
	}
	if d.LastError == "" {
		t.Errorf("expected last_error to be populated on timeout")
	}
}
