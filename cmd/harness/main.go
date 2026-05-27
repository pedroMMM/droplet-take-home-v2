package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"time"
)

// scenarioResult captures the outcome of a single scenario.
type scenarioResult struct {
	Name   string
	Pass   bool
	Notes  string
	Took   time.Duration
	Detail string
}

var (
	serverURL string
	logger    *slog.Logger
)

func main() {
	logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	serverURL = os.Getenv("SERVER_URL")
	if serverURL == "" {
		serverURL = "http://localhost:8080"
	}

	logger.Info("harness starting", "server_url", serverURL)

	if !waitForServer(30 * time.Second) {
		logger.Error("server is not reachable", "server_url", serverURL)
		os.Exit(1)
	}

	results := []scenarioResult{
		runScenario("happy_path", scenarioHappyPath),
		runScenario("retry_path", scenarioRetry),
		runScenario("dead_letter_path", scenarioDeadLetterProgress),
		runScenario("hmac_verification", scenarioHMAC),
		runScenario("status_endpoint", scenarioStatus),
	}

	allPass := true
	fmt.Println()
	fmt.Println("=== Harness Summary ===")
	fmt.Printf("%-22s  %-6s  %-9s  %s\n", "Scenario", "Result", "Duration", "Notes")
	fmt.Printf("%-22s  %-6s  %-9s  %s\n", "--------", "------", "--------", "-----")
	for _, r := range results {
		status := "PASS"
		if !r.Pass {
			status = "FAIL"
			allPass = false
		}
		fmt.Printf("%-22s  %-6s  %-9s  %s\n", r.Name, status, r.Took.Round(time.Millisecond).String(), r.Notes)
		if r.Detail != "" {
			fmt.Printf("    detail: %s\n", r.Detail)
		}
	}
	fmt.Println()
	if !allPass {
		fmt.Println("RESULT: FAIL")
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS")
}

func runScenario(name string, fn func() scenarioResult) scenarioResult {
	logger.Info("scenario start", "name", name)
	start := time.Now()
	res := fn()
	res.Name = name
	res.Took = time.Since(start)
	if res.Pass {
		logger.Info("scenario pass", "name", name, "duration", res.Took.String(), "notes", res.Notes)
	} else {
		logger.Warn("scenario fail", "name", name, "duration", res.Took.String(),
			"notes", res.Notes, "detail", res.Detail)
	}
	return res
}

func waitForServer(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(serverURL + "/status")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// --- shared HTTP helpers --------------------------------------------------

func postJSON(path string, body any, out any) (int, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequest(http.MethodPost, serverURL+path, bytes.NewReader(buf))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	if out != nil && len(rb) > 0 {
		if err := json.Unmarshal(rb, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w (body=%s)", err, string(rb))
		}
	}
	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("status %d: %s", resp.StatusCode, string(rb))
	}
	return resp.StatusCode, nil
}

func getJSON(path string, out any) (int, error) {
	resp, err := http.Get(serverURL + path)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	if out != nil && len(rb) > 0 {
		if err := json.Unmarshal(rb, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w (body=%s)", err, string(rb))
		}
	}
	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("status %d: %s", resp.StatusCode, string(rb))
	}
	return resp.StatusCode, nil
}

type webhookResp struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}

type eventResp struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

type deliveryResp struct {
	ID             string     `json:"id"`
	EventID        string     `json:"event_id"`
	WebhookID      string     `json:"webhook_id"`
	Status         string     `json:"status"`
	AttemptCount   int        `json:"attempt_count"`
	NextAttemptAt  *time.Time `json:"next_attempt_at,omitempty"`
	LastAttemptAt  *time.Time `json:"last_attempt_at,omitempty"`
	LastStatusCode *int       `json:"last_status_code,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
}

type eventWithDeliveries struct {
	Event      *eventResp      `json:"event"`
	Deliveries []*deliveryResp `json:"deliveries"`
}

func registerWebhook(url, secret string) (*webhookResp, error) {
	var wh webhookResp
	_, err := postJSON("/webhooks", map[string]string{"url": url, "secret": secret}, &wh)
	if err != nil {
		return nil, err
	}
	return &wh, nil
}

func postEvent(eventType string, payload any) (*eventResp, error) {
	var ev eventResp
	_, err := postJSON("/events", map[string]any{
		"type":    eventType,
		"payload": payload,
	}, &ev)
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

func deliveriesForEvent(eventID string) ([]*deliveryResp, error) {
	var out eventWithDeliveries
	if _, err := getJSON("/events/"+eventID, &out); err != nil {
		return nil, err
	}
	return out.Deliveries, nil
}

// pollForDeliveries waits up to timeout for predicate to return true on every
// delivery for the given event id. Returns the last fetched deliveries.
func pollForDeliveries(eventID string, timeout time.Duration, predicate func([]*deliveryResp) bool) ([]*deliveryResp, error) {
	deadline := time.Now().Add(timeout)
	var last []*deliveryResp
	for time.Now().Before(deadline) {
		ds, err := deliveriesForEvent(eventID)
		if err != nil {
			return nil, err
		}
		last = ds
		if len(ds) > 0 && predicate(ds) {
			return ds, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return last, errors.New("poll timed out")
}

// --- scenarios ------------------------------------------------------------

func scenarioHappyPath() scenarioResult {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	wh, err := registerWebhook(srv.URL, "")
	if err != nil {
		return scenarioResult{Notes: "register failed", Detail: err.Error()}
	}

	events := []*eventResp{}
	for i := range 3 {
		ev, err := postEvent("order.created", map[string]any{"i": i})
		if err != nil {
			return scenarioResult{Notes: "post event failed", Detail: err.Error()}
		}
		events = append(events, ev)
	}

	for _, ev := range events {
		ds, err := pollForDeliveries(ev.ID, 30*time.Second, func(ds []*deliveryResp) bool {
			for _, d := range ds {
				if d.WebhookID == wh.ID && d.Status != "success" {
					return false
				}
			}
			return true
		})
		if err != nil {
			return scenarioResult{Notes: "deliveries did not succeed", Detail: fmt.Sprintf("event=%s last=%+v err=%v", ev.ID, ds, err)}
		}
	}
	return scenarioResult{Pass: true, Notes: "3 events delivered on first attempt"}
}

func scenarioRetry() scenarioResult {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	wh, err := registerWebhook(srv.URL, "")
	if err != nil {
		return scenarioResult{Notes: "register failed", Detail: err.Error()}
	}

	ev, err := postEvent("retry.test", map[string]string{"hello": "world"})
	if err != nil {
		return scenarioResult{Notes: "post event failed", Detail: err.Error()}
	}

	// Backoff: 1st fail at +0s, retry at +10s. 2nd fail at +10s, retry at +60s.
	// So success expected ~70s after creation. Use 120s timeout to be safe.
	ds, err := pollForDeliveries(ev.ID, 120*time.Second, func(ds []*deliveryResp) bool {
		for _, d := range ds {
			if d.WebhookID == wh.ID && d.Status == "success" {
				return true
			}
		}
		return false
	})
	if err != nil {
		return scenarioResult{Notes: "delivery did not eventually succeed",
			Detail: fmt.Sprintf("hits=%d last=%+v err=%v", hits.Load(), ds, err)}
	}

	// Find our delivery and check attempt_count.
	for _, d := range ds {
		if d.WebhookID == wh.ID && d.Status == "success" {
			if d.AttemptCount < 3 {
				return scenarioResult{Notes: "succeeded but attempt_count < 3",
					Detail: fmt.Sprintf("attempt_count=%d hits=%d", d.AttemptCount, hits.Load())}
			}
			return scenarioResult{Pass: true,
				Notes: fmt.Sprintf("succeeded after %d attempts (hits=%d)", d.AttemptCount, hits.Load())}
		}
	}
	return scenarioResult{Notes: "could not find delivery for webhook"}
}

func scenarioDeadLetterProgress() scenarioResult {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	wh, err := registerWebhook(srv.URL, "")
	if err != nil {
		return scenarioResult{Notes: "register failed", Detail: err.Error()}
	}

	ev, err := postEvent("dead.letter.test", map[string]string{"will": "fail"})
	if err != nil {
		return scenarioResult{Notes: "post event failed", Detail: err.Error()}
	}

	// Wait long enough for at least 2 attempts (initial + first retry at +10s).
	ds, err := pollForDeliveries(ev.ID, 45*time.Second, func(ds []*deliveryResp) bool {
		for _, d := range ds {
			if d.WebhookID == wh.ID && d.AttemptCount >= 2 {
				return true
			}
		}
		return false
	})
	if err != nil {
		return scenarioResult{Notes: "failing webhook did not show retry progress",
			Detail: fmt.Sprintf("last=%+v err=%v", ds, err)}
	}
	for _, d := range ds {
		if d.WebhookID == wh.ID {
			if d.Status != "failed" && d.Status != "dead_lettered" {
				return scenarioResult{Notes: "delivery in unexpected state",
					Detail: fmt.Sprintf("status=%s attempt_count=%d", d.Status, d.AttemptCount)}
			}
			return scenarioResult{
				Pass:  true,
				Notes: fmt.Sprintf("retries observed: attempt_count=%d status=%s (full dead-letter takes ~45 min)", d.AttemptCount, d.Status),
			}
		}
	}
	return scenarioResult{Notes: "could not find delivery for webhook"}
}

func scenarioHMAC() scenarioResult {
	const secret = "test-secret"
	var (
		hmacOK   atomic.Bool
		sawAuth  atomic.Bool
		captured atomic.Value // string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		sig := r.Header.Get("X-Webhook-Signature")
		sawAuth.Store(sig != "")
		captured.Store(sig)

		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(body)
		expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if !hmac.Equal([]byte(expected), []byte(sig)) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		hmacOK.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	wh, err := registerWebhook(srv.URL, secret)
	if err != nil {
		return scenarioResult{Notes: "register failed", Detail: err.Error()}
	}

	ev, err := postEvent("signed.event", map[string]string{"top": "secret"})
	if err != nil {
		return scenarioResult{Notes: "post event failed", Detail: err.Error()}
	}

	ds, err := pollForDeliveries(ev.ID, 30*time.Second, func(ds []*deliveryResp) bool {
		for _, d := range ds {
			if d.WebhookID == wh.ID && d.Status == "success" {
				return true
			}
		}
		return false
	})
	if err != nil {
		sig, _ := captured.Load().(string)
		return scenarioResult{Notes: "HMAC delivery did not succeed",
			Detail: fmt.Sprintf("saw_auth=%v hmac_ok=%v signature=%q last=%+v err=%v",
				sawAuth.Load(), hmacOK.Load(), sig, ds, err)}
	}
	if !hmacOK.Load() {
		return scenarioResult{Notes: "delivery succeeded but receiver did not validate hmac"}
	}
	return scenarioResult{Pass: true, Notes: "delivery succeeded with valid X-Webhook-Signature"}
}

func scenarioStatus() scenarioResult {
	var resp map[string]any
	code, err := getJSON("/status", &resp)
	if err != nil {
		return scenarioResult{Notes: "status fetch failed", Detail: err.Error()}
	}
	if code != http.StatusOK {
		return scenarioResult{Notes: "status returned non-200", Detail: fmt.Sprintf("code=%d", code)}
	}
	requiredKeys := []string{"status", "uptime_seconds", "webhooks", "events", "deliveries", "delivery_success_rate"}
	for _, k := range requiredKeys {
		if _, ok := resp[k]; !ok {
			return scenarioResult{Notes: "status missing field", Detail: "missing " + k}
		}
	}
	return scenarioResult{Pass: true, Notes: "status returned all expected fields"}
}
