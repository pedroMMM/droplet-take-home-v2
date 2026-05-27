package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/pedroMMM/droplet-take-home-v2/internal/api"
	"github.com/pedroMMM/droplet-take-home-v2/internal/delivery"
	"github.com/pedroMMM/droplet-take-home-v2/internal/store"
	"github.com/pedroMMM/droplet-take-home-v2/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	shutdownTelemetry, err := telemetry.Setup(context.Background())
	if err != nil {
		logger.Error("init telemetry", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(shutdownCtx); err != nil {
			logger.Error("shutdown telemetry", "error", err)
		}
	}()

	port := envOrDefault("PORT", "8080")
	dbPath := envOrDefault("DATABASE_PATH", "./webhook.db")
	workerCount := envInt("WORKER_COUNT", 10)
	retryPoll := envDuration("RETRY_POLL_INTERVAL", 5*time.Second)

	logger.Info("starting webhook delivery service",
		"port", port,
		"db_path", dbPath,
		"workers", workerCount,
		"retry_poll", retryPoll.String(),
	)

	st, err := store.Open(dbPath)
	if err != nil {
		logger.Error("open store", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	engineCtx, engineCancel := context.WithCancel(context.Background())
	defer engineCancel()

	engine := delivery.New(st, workerCount)
	engine.SetLogger(logger)
	engine.SetRetryPollInterval(retryPoll)
	engine.Start(engineCtx)

	handler := api.New(st, engine, time.Now())

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Run server in background.
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// Wait for shutdown signal.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-stop:
		logger.Info("shutdown signal received", "signal", sig.String())
	case err := <-serverErr:
		logger.Error("http server error", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}

	engineCancel()
	engine.Stop()
	logger.Info("shutdown complete")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}
