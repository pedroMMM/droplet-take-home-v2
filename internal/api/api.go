package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/pedroMMM/droplet-take-home-v2/internal/delivery"
	"github.com/pedroMMM/droplet-take-home-v2/internal/models"
	"github.com/pedroMMM/droplet-take-home-v2/internal/store"
)

var tracer = otel.Tracer("webhook-delivery/api")

// Server bundles the HTTP routes with their dependencies.
type Server struct {
	store     *store.Store
	engine    *delivery.Engine
	startTime time.Time
	logger    *slog.Logger
}

// New builds an http.Handler with all routes wired.
func New(s *store.Store, eng *delivery.Engine, startTime time.Time) http.Handler {
	srv := &Server{
		store:     s,
		engine:    eng,
		startTime: startTime,
		logger:    slog.Default(),
	}
	return srv.routes()
}

func (s *Server) routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)

	r.Post("/webhooks", s.createWebhook)
	r.Get("/webhooks", s.listWebhooks)
	r.Delete("/webhooks/{id}", s.deleteWebhook)

	r.Post("/events", s.createEvent)
	r.Get("/events/{id}", s.getEvent)

	r.Get("/deliveries", s.listDeliveries)
	r.Get("/deliveries/{id}", s.getDelivery)

	r.Get("/status", s.getStatus)

	// Wrap the whole router with otelhttp for automatic span-per-request.
	return otelhttp.NewHandler(r, "http.server",
		otelhttp.WithSpanNameFormatter(func(op string, r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}),
	)
}

// --- helpers --------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

// --- webhook handlers -----------------------------------------------------

type createWebhookRequest struct {
	URL    string `json:"url"`
	Secret string `json:"secret"`
}

type webhookResponse struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}

func toWebhookResponse(w *models.Webhook) webhookResponse {
	return webhookResponse{
		ID:        w.ID,
		URL:       w.URL,
		Active:    w.Active,
		CreatedAt: w.CreatedAt,
	}
}

func (s *Server) createWebhook(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "api.createWebhook")
	defer span.End()

	var body createWebhookRequest
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body: "+err.Error())
		return
	}
	body.URL = strings.TrimSpace(body.URL)
	if body.URL == "" {
		writeError(w, http.StatusBadRequest, "url is required")
		return
	}
	parsed, err := url.ParseRequestURI(body.URL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		writeError(w, http.StatusBadRequest, "url is not valid")
		return
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		writeError(w, http.StatusBadRequest, "url must be http or https")
		return
	}

	wh := &models.Webhook{
		ID:        uuid.New().String(),
		URL:       body.URL,
		Secret:    body.Secret,
		Active:    true,
		CreatedAt: time.Now().UTC(),
	}
	if err := s.store.CreateWebhook(ctx, wh); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("create webhook", "error", err)
		writeError(w, http.StatusInternalServerError, "could not create webhook")
		return
	}
	writeJSON(w, http.StatusCreated, toWebhookResponse(wh))
}

func (s *Server) listWebhooks(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListWebhooks(r.Context())
	if err != nil {
		s.logger.Error("list webhooks", "error", err)
		writeError(w, http.StatusInternalServerError, "could not list webhooks")
		return
	}
	out := make([]webhookResponse, 0, len(list))
	for _, wh := range list {
		out = append(out, toWebhookResponse(wh))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) deleteWebhook(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	if err := s.store.DeactivateWebhook(r.Context(), id); err != nil {
		if store.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "webhook not found")
			return
		}
		s.logger.Error("deactivate webhook", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "could not deactivate webhook")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- event handlers -------------------------------------------------------

type createEventRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (s *Server) createEvent(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "api.createEvent")
	defer span.End()

	var body createEventRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body: "+err.Error())
		return
	}
	if len(body.Payload) == 0 {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}
	// Validate payload is itself valid JSON.
	var probe any
	if err := json.Unmarshal(body.Payload, &probe); err != nil {
		writeError(w, http.StatusBadRequest, "payload is not valid json: "+err.Error())
		return
	}

	event := &models.Event{
		ID:        uuid.New().String(),
		Type:      body.Type,
		Payload:   body.Payload,
		CreatedAt: time.Now().UTC(),
	}
	span.SetAttributes(attribute.String("event.id", event.ID))
	if err := s.store.CreateEvent(ctx, event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("create event", "error", err)
		writeError(w, http.StatusInternalServerError, "could not create event")
		return
	}

	hooks, err := s.store.ListActiveWebhooks(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("list active webhooks", "error", err)
		writeError(w, http.StatusInternalServerError, "could not list webhooks")
		return
	}

	now := time.Now().UTC()
	for _, h := range hooks {
		d := &models.Delivery{
			ID:            uuid.New().String(),
			EventID:       event.ID,
			WebhookID:     h.ID,
			Status:        models.StatusPending,
			NextAttemptAt: &now, // immediately eligible
			CreatedAt:     time.Now().UTC(),
		}
		if err := s.store.CreateDelivery(ctx, d); err != nil {
			s.logger.Error("create delivery", "event_id", event.ID,
				"webhook_id", h.ID, "error", err)
			continue
		}
		s.engine.Enqueue(d.ID)
	}

	writeJSON(w, http.StatusCreated, event)
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "api.getEvent")
	defer span.End()

	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	event, err := s.store.GetEvent(ctx, id)
	if err != nil {
		if store.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "event not found")
			return
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("get event", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "could not get event")
		return
	}
	deliveries, err := s.store.ListDeliveriesByEvent(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("list deliveries by event", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "could not list deliveries")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event":      event,
		"deliveries": deliveries,
	})
}

// --- delivery handlers ----------------------------------------------------

func (s *Server) listDeliveries(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	var (
		list []*models.Delivery
		err  error
	)
	if status != "" {
		list, err = s.store.ListDeliveriesByStatus(r.Context(), models.DeliveryStatus(status))
	} else {
		list, err = s.store.ListDeliveries(r.Context())
	}
	if err != nil {
		s.logger.Error("list deliveries", "error", err)
		writeError(w, http.StatusInternalServerError, "could not list deliveries")
		return
	}
	if list == nil {
		list = []*models.Delivery{}
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) getDelivery(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	d, err := s.store.GetDelivery(r.Context(), id)
	if err != nil {
		if store.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "delivery not found")
			return
		}
		s.logger.Error("get delivery", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "could not get delivery")
		return
	}
	writeJSON(w, http.StatusOK, d)
}

// --- status ---------------------------------------------------------------

type statusCounts struct {
	Total        int `json:"total"`
	Pending      int `json:"pending"`
	Delivering   int `json:"delivering"`
	Success      int `json:"success"`
	Failed       int `json:"failed"`
	DeadLettered int `json:"dead_lettered"`
}

type statusResponse struct {
	Status              string         `json:"status"`
	UptimeSeconds       int64          `json:"uptime_seconds"`
	Webhooks            map[string]int `json:"webhooks"`
	Events              map[string]int `json:"events"`
	Deliveries          statusCounts   `json:"deliveries"`
	DeliverySuccessRate float64        `json:"delivery_success_rate"`
}

func (s *Server) getStatus(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "api.getStatus")
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stats, err := s.store.GetStats(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			writeError(w, http.StatusGatewayTimeout, "stats timed out")
			return
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("get stats", "error", err)
		writeError(w, http.StatusInternalServerError, "could not get stats")
		return
	}

	counts := statusCounts{
		Total:        stats.TotalDeliveries,
		Pending:      stats.ByStatus[models.StatusPending],
		Delivering:   stats.ByStatus[models.StatusDelivering],
		Success:      stats.ByStatus[models.StatusSuccess],
		Failed:       stats.ByStatus[models.StatusFailed],
		DeadLettered: stats.ByStatus[models.StatusDeadLettered],
	}

	rate := 0.0
	if counts.Total > 0 {
		rate = float64(counts.Success) / float64(counts.Total)
		rate = math.Round(rate*10000) / 10000
	}

	resp := statusResponse{
		Status:        "ok",
		UptimeSeconds: int64(time.Since(s.startTime).Seconds()),
		Webhooks: map[string]int{
			"total":  stats.TotalWebhooks,
			"active": stats.ActiveWebhooks,
		},
		Events: map[string]int{
			"total": stats.TotalEvents,
		},
		Deliveries:          counts,
		DeliverySuccessRate: rate,
	}
	writeJSON(w, http.StatusOK, resp)
}
