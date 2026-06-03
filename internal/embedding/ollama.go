// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

// OllamaConfig configures a gmeow-managed Ollama embedding backend. gmeow can
// supervise its OWN ollama process (own host + model dir) so the embedding
// endpoint is provided and controlled rather than required from the operator.
type OllamaConfig struct {
	// Binary is the ollama executable (name on PATH or an absolute path).
	Binary string
	// Host is the OLLAMA_HOST the managed server binds, e.g. "127.0.0.1:11434".
	Host string
	// Model is the Ollama model tag to embed with, e.g. "nomic-embed-text".
	Model string
	// ModelsDir, if set, is the OLLAMA_MODELS directory so model weights live
	// under gmeow's control (default: ollama's own location).
	ModelsDir string
}

const (
	defaultOllamaBinary = "ollama"
	defaultOllamaHost   = "127.0.0.1:11434"
	defaultOllamaModel  = "nomic-embed-text"
	ollamaRestartDelay  = 3 * time.Second
)

func (c OllamaConfig) withDefaults() OllamaConfig {
	if c.Binary == "" {
		c.Binary = defaultOllamaBinary
	}

	if c.Host == "" {
		c.Host = defaultOllamaHost
	}

	if c.Model == "" {
		c.Model = defaultOllamaModel
	}

	return c
}

// OllamaBackend supervises a gmeow-owned `ollama serve` subprocess and ensures
// the embedding model is present. The EMBEDDING service points its HTTP embedder
// at Endpoint()/Model(). Run() restarts the subprocess if it exits, so a crash
// recovers instead of stalling the import (the failure mode of the hand-run
// server).
type OllamaBackend struct {
	cfg    OllamaConfig
	client *http.Client
	// baseURL is the http base (scheme+host); separated for testability so the
	// health/tags probes can target a mock server.
	baseURL string
}

// NewOllamaBackend validates that the ollama binary is resolvable and returns a
// backend. It does not start anything; call Run to supervise the server.
func NewOllamaBackend(cfg OllamaConfig) (*OllamaBackend, error) {
	cfg = cfg.withDefaults()
	if _, err := exec.LookPath(cfg.Binary); err != nil {
		return nil, fmt.Errorf(
			"ollama binary %q not found: install Ollama (https://ollama.com/download "+
				"or `curl -fsSL https://ollama.com/install.sh | sh`) or run the bundled "+
				"deploy/embedding docker compose instead: %w",
			cfg.Binary, err,
		)
	}

	return &OllamaBackend{
		cfg:     cfg,
		client:  &http.Client{Timeout: 30 * time.Second},
		baseURL: "http://" + cfg.Host,
	}, nil
}

// Endpoint is the OpenAI-compatible embeddings URL the HTTP embedder calls.
func (b *OllamaBackend) Endpoint() string { return b.baseURL + "/v1/embeddings" }

// Model is the model tag to pass in embedding requests.
func (b *OllamaBackend) Model() string { return b.cfg.Model }

// Run supervises `ollama serve`: it starts the server and restarts it (after a
// short delay) whenever it exits, until ctx is cancelled. It returns when ctx is
// done. Run blocks; call it in a goroutine.
func (b *OllamaBackend) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		cmd := exec.CommandContext(ctx, b.cfg.Binary, "serve")
		cmd.Env = b.serveEnv()
		cmd.Stdout = os.Stderr // ollama logs to its own stream; surface for ops
		cmd.Stderr = os.Stderr

		_ = cmd.Run() // returns when the server exits or ctx cancels

		if ctx.Err() != nil {
			return ctx.Err()
		}
		// The server exited on its own (crash/restart). Pause, then relaunch.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ollamaRestartDelay):
		}
	}
}

func (b *OllamaBackend) serveEnv() []string {
	env := append(os.Environ(), "OLLAMA_HOST="+b.cfg.Host)
	if b.cfg.ModelsDir != "" {
		env = append(env, "OLLAMA_MODELS="+b.cfg.ModelsDir)
	}

	return env
}

// WaitHealthy polls the server's version endpoint until it responds or timeout.
func (b *OllamaBackend) WaitHealthy(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		if b.ping(ctx) {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf(
				"ollama server at %s did not become healthy within %s",
				b.cfg.Host,
				timeout,
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (b *OllamaBackend) ping(ctx context.Context) bool {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		b.baseURL+"/api/version",
		nil,
	)
	if err != nil {
		return false
	}

	response, err := b.client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()

	return response.StatusCode == http.StatusOK
}

// EnsureModel makes sure the configured model is pulled. It checks the server's
// model list first (no-op if present) and otherwise runs `ollama pull`.
func (b *OllamaBackend) EnsureModel(ctx context.Context) error {
	present, err := b.modelPresent(ctx)
	if err != nil {
		return err
	}

	if present {
		return nil
	}

	cmd := exec.CommandContext(ctx, b.cfg.Binary, "pull", b.cfg.Model)
	cmd.Env = b.serveEnv()
	cmd.Stdout = os.Stderr

	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ollama pull %s: %w", b.cfg.Model, err)
	}

	return nil
}

// modelPresent reports whether the configured model tag is already available.
func (b *OllamaBackend) modelPresent(ctx context.Context) (bool, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		b.baseURL+"/api/tags",
		nil,
	)
	if err != nil {
		return false, err
	}

	response, err := b.client.Do(request)
	if err != nil {
		return false, fmt.Errorf("list ollama models: %w", err)
	}
	defer response.Body.Close()

	var decoded struct {
		Models []struct {
			Name string `json:"name"`

			Model string `json:"model"`
		} `json:"models"`
	}
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return false, fmt.Errorf("decode ollama model list: %w", err)
	}

	for _, m := range decoded.Models {
		if modelTagMatches(m.Name, b.cfg.Model) || modelTagMatches(m.Model, b.cfg.Model) {
			return true, nil
		}
	}

	return false, nil
}

// modelTagMatches compares an ollama model tag against the configured model,
// treating a missing ":latest" suffix as equal ("nomic-embed-text" ==
// "nomic-embed-text:latest").
func modelTagMatches(have, want string) bool {
	norm := func(s string) string { return strings.TrimSuffix(s, ":latest") }

	return norm(have) == norm(want)
}
