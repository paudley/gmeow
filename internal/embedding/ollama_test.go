// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestModelTagMatches(t *testing.T) {
	cases := []struct {
		have, want string
		match      bool
	}{
		{"nomic-embed-text", "nomic-embed-text", true},
		{"nomic-embed-text:latest", "nomic-embed-text", true},
		{"nomic-embed-text", "nomic-embed-text:latest", true},
		{"nomic-embed-text:v1.5", "nomic-embed-text", false},
		{"other", "nomic-embed-text", false},
	}
	for _, c := range cases {
		if got := modelTagMatches(c.have, c.want); got != c.match {
			t.Fatalf("modelTagMatches(%q,%q)=%v want %v", c.have, c.want, got, c.match)
		}
	}
}

func TestOllamaConfigDefaultsAndEndpoint(t *testing.T) {
	b := &OllamaBackend{
		cfg:     OllamaConfig{}.withDefaults(),
		baseURL: "http://127.0.0.1:11434",
	}
	if b.cfg.Model != defaultOllamaModel || b.cfg.Host != defaultOllamaHost {
		t.Fatalf("defaults not applied: %+v", b.cfg)
	}
	if b.Endpoint() != "http://127.0.0.1:11434/v1/embeddings" {
		t.Fatalf("unexpected endpoint %q", b.Endpoint())
	}
}

func TestNewOllamaBackendMissingBinary(t *testing.T) {
	_, err := NewOllamaBackend(OllamaConfig{Binary: "gmeow-no-such-ollama-binary-xyz"})
	if err == nil {
		t.Fatal("expected an error when the ollama binary is absent")
	}
	if !strings.Contains(err.Error(), "install Ollama") {
		t.Fatalf("error should guide installation, got: %v", err)
	}
}

func newTestOllamaBackend(server *httptest.Server) *OllamaBackend {
	return &OllamaBackend{
		cfg:     OllamaConfig{Model: "nomic-embed-text"}.withDefaults(),
		client:  server.Client(),
		baseURL: server.URL,
	}
}

func TestWaitHealthyAndModelPresent(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/version":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"version":"0.0.0"}`))
			case "/api/tags":
				_, _ = w.Write(
					[]byte(
						`{"models":[{"name":"nomic-embed-text:latest","model":"nomic-embed-text:latest"}]}`,
					),
				)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}),
	)
	defer server.Close()

	b := newTestOllamaBackend(server)
	if err := b.WaitHealthy(context.Background(), 2*time.Second); err != nil {
		t.Fatalf("WaitHealthy: %v", err)
	}
	present, err := b.modelPresent(context.Background())
	if err != nil {
		t.Fatalf("modelPresent: %v", err)
	}
	if !present {
		t.Fatal("expected nomic-embed-text to be reported present")
	}
}

func TestModelAbsentAndUnhealthyTimeout(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/tags" {
				_, _ = w.Write([]byte(`{"models":[{"name":"some-other-model:latest"}]}`))
				return
			}
			w.WriteHeader(http.StatusNotFound) // /api/version 404 -> never healthy
		}),
	)
	defer server.Close()

	b := newTestOllamaBackend(server)
	present, err := b.modelPresent(context.Background())
	if err != nil || present {
		t.Fatalf("expected model absent, got present=%v err=%v", present, err)
	}
	if err := b.WaitHealthy(context.Background(), 300*time.Millisecond); err == nil {
		t.Fatal("expected WaitHealthy to time out when /api/version 404s")
	}
}
