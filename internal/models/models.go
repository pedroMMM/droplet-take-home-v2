package models

import (
	"encoding/json"
	"time"
)

type DeliveryStatus string

const (
	StatusPending      DeliveryStatus = "pending"
	StatusDelivering   DeliveryStatus = "delivering"
	StatusSuccess      DeliveryStatus = "success"
	StatusFailed       DeliveryStatus = "failed"
	StatusDeadLettered DeliveryStatus = "dead_lettered"
)

type Webhook struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Secret    string    `json:"secret,omitempty"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}

type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

type Delivery struct {
	ID             string         `json:"id"`
	EventID        string         `json:"event_id"`
	WebhookID      string         `json:"webhook_id"`
	Status         DeliveryStatus `json:"status"`
	AttemptCount   int            `json:"attempt_count"`
	NextAttemptAt  *time.Time     `json:"next_attempt_at,omitempty"`
	LastAttemptAt  *time.Time     `json:"last_attempt_at,omitempty"`
	LastStatusCode *int           `json:"last_status_code,omitempty"`
	LastError      string         `json:"last_error,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
}
