package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pedroMMM/droplet-take-home-v2/internal/api"
	"github.com/pedroMMM/droplet-take-home-v2/internal/delivery"
	"github.com/pedroMMM/droplet-take-home-v2/internal/models"
	"github.com/pedroMMM/droplet-take-home-v2/internal/store"
)

func setup(t *testing.T) (http.Handler, *store.Store) {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng := delivery.New(st, 1)
	eng.WithClock(func() time.Time { return time.Now() })
	h := api.New(st, eng, time.Now())
	return h, st
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	switch v := body.(type) {
	case nil:
		reader = bytes.NewReader(nil)
	case string:
		reader = bytes.NewReader([]byte(v))
	case []byte:
		reader = bytes.NewReader(v)
	default:
		buf, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(buf)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateWebhook_Valid(t *testing.T) {
	h, _ := setup(t)
	rec := doJSON(t, h, http.MethodPost, "/webhooks", map[string]string{
		"url": "https://example.com/hook",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status: got %d want %d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	id, _ := resp["id"].(string)
	if id == "" {
		t.Errorf("expected id in response, got %v", resp)
	}
}

func TestCreateWebhook_InvalidURL(t *testing.T) {
	h, _ := setup(t)
	rec := doJSON(t, h, http.MethodPost, "/webhooks", map[string]string{
		"url": "not-a-url",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestListWebhooks(t *testing.T) {
	h, _ := setup(t)
	for i, url := range []string{"https://a.example/x", "https://b.example/x"} {
		rec := doJSON(t, h, http.MethodPost, "/webhooks", map[string]string{"url": url})
		if rec.Code != http.StatusCreated {
			t.Fatalf("create webhook %d: got %d body=%s", i, rec.Code, rec.Body.String())
		}
	}

	rec := doJSON(t, h, http.MethodGet, "/webhooks", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status: got %d body=%s", rec.Code, rec.Body.String())
	}
	var list []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("expected 2 webhooks, got %d", len(list))
	}
}

func TestDeleteWebhook(t *testing.T) {
	h, _ := setup(t)
	rec := doJSON(t, h, http.MethodPost, "/webhooks", map[string]string{
		"url": "https://example.com/hook",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: got %d", rec.Code)
	}
	var created map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	id, _ := created["id"].(string)

	delRec := doJSON(t, h, http.MethodDelete, "/webhooks/"+id, nil)
	if delRec.Code != http.StatusNoContent {
		t.Fatalf("delete: got %d body=%s", delRec.Code, delRec.Body.String())
	}

	listRec := doJSON(t, h, http.MethodGet, "/webhooks", nil)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list: got %d", listRec.Code)
	}
	var list []map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	for _, wh := range list {
		if active, _ := wh["active"].(bool); active {
			t.Errorf("expected no active webhooks, got %+v", wh)
		}
	}
}

func TestCreateEvent(t *testing.T) {
	h, _ := setup(t)
	rec := doJSON(t, h, http.MethodPost, "/events", map[string]any{
		"type":    "user.created",
		"payload": map[string]any{"id": "u-1"},
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create event: got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if id, _ := resp["id"].(string); id == "" {
		t.Errorf("expected id in response, got %v", resp)
	}
}

func TestCreateEvent_InvalidPayload(t *testing.T) {
	h, _ := setup(t)
	// Truly invalid JSON.
	rec := doJSON(t, h, http.MethodPost, "/events", `{"type":"t","payload":{broken`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestGetStatus(t *testing.T) {
	h, _ := setup(t)
	rec := doJSON(t, h, http.MethodGet, "/status", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if status, _ := resp["status"].(string); status != "ok" {
		t.Errorf("status: got %q want %q", status, "ok")
	}
}

func TestGetEventWithDeliveries(t *testing.T) {
	h, st := setup(t)

	// Create webhook so an event will fan out into a delivery.
	whRec := doJSON(t, h, http.MethodPost, "/webhooks", map[string]string{
		"url": "https://example.com/hook",
	})
	if whRec.Code != http.StatusCreated {
		t.Fatalf("create webhook: got %d", whRec.Code)
	}

	evRec := doJSON(t, h, http.MethodPost, "/events", map[string]any{
		"type":    "user.created",
		"payload": map[string]any{"id": "u-1"},
	})
	if evRec.Code != http.StatusCreated {
		t.Fatalf("create event: got %d body=%s", evRec.Code, evRec.Body.String())
	}
	var ev models.Event
	if err := json.Unmarshal(evRec.Body.Bytes(), &ev); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if ev.ID == "" {
		t.Fatalf("created event missing id: %s", evRec.Body.String())
	}

	getRec := doJSON(t, h, http.MethodGet, "/events/"+ev.ID, nil)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get event: got %d body=%s", getRec.Code, getRec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := resp["event"]; !ok {
		t.Errorf("response missing 'event' key: %v", resp)
	}
	deliveries, ok := resp["deliveries"].([]any)
	if !ok {
		t.Fatalf("response missing 'deliveries' array, got %v", resp)
	}
	if len(deliveries) == 0 {
		t.Errorf("expected at least one delivery for fanned-out event")
	}

	// Sanity check that the delivery row was actually persisted.
	list, err := st.ListDeliveriesByEvent(context.Background(), ev.ID)
	if err != nil {
		t.Fatalf("store list: %v", err)
	}
	if len(list) == 0 {
		t.Errorf("expected at least one stored delivery for event")
	}
}
