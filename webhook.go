package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

type workflowJobEvent struct {
	Action      string      `json:"action"` // queued, in_progress, completed
	WorkflowJob WorkflowJob `json:"workflow_job"`
	Repository  struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

func webhookHandler(c *Controller, cfg *Config, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 25<<20))
		if err != nil {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}
		if !validSignature(cfg.Webhook.Secret, body, r.Header.Get("X-Hub-Signature-256")) {
			log.Warn("webhook with invalid signature", "remote", r.RemoteAddr)
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		switch r.Header.Get("X-GitHub-Event") {
		case "ping":
			log.Info("webhook ping received")
		case "workflow_job":
			var ev workflowJobEvent
			if err := json.Unmarshal(body, &ev); err != nil {
				http.Error(w, "bad payload", http.StatusBadRequest)
				return
			}
			// Answer fast: GitHub times webhooks out after 10s.
			switch ev.Action {
			case "queued":
				c.JobQueued(ev.WorkflowJob, ev.Repository.FullName)
			case "in_progress":
				c.JobInProgress(ev.WorkflowJob)
			case "completed":
				c.JobCompleted(ev.WorkflowJob)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func validSignature(secret string, body []byte, header string) bool {
	sig, ok := strings.CutPrefix(header, "sha256=")
	if !ok {
		return false
	}
	got, err := hex.DecodeString(sig)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hmac.Equal(got, mac.Sum(nil))
}
