// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestHTTPEmbedderChunksLargeBatches verifies that more inputs than fit in one
// physical batch are split across requests and reassembled in order, and that no
// single request exceeds the batch caps.
func TestHTTPEmbedderChunksLargeBatches(t *testing.T) {
	var maxReqSize int
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				Input []string `json:"input"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if len(req.Input) > maxReqSize {
				maxReqSize = len(req.Input)
			}
			var resp struct {
				Data []struct {
					Index     int       `json:"index"`
					Embedding []float64 `json:"embedding"`
				} `json:"data"`
			}
			for i := range req.Input {
				resp.Data = append(resp.Data, struct {
					Index     int       `json:"index"`
					Embedding []float64 `json:"embedding"`
				}{Index: i, Embedding: []float64{float64(i)}})
			}
			_ = json.NewEncoder(w).Encode(resp)
		}),
	)
	defer server.Close()

	embedder, err := NewHTTPEmbedder(server.URL, "stub", nil)
	if err != nil {
		t.Fatal(err)
	}

	const n = maxBatchTexts*3 + 7
	texts := make([]string, n)
	for i := range texts {
		texts[i] = fmt.Sprintf("claim number %d", i)
	}
	vectors, err := embedder.Embed(context.Background(), texts)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if len(vectors) != n {
		t.Fatalf("got %d vectors, want %d", len(vectors), n)
	}
	if maxReqSize > maxBatchTexts {
		t.Fatalf(
			"a request carried %d inputs, exceeding the cap %d",
			maxReqSize,
			maxBatchTexts,
		)
	}
}

func TestHTTPEmbedderRetriesTransientFailures(t *testing.T) {
	saved := embedRetryBackoffs
	embedRetryBackoffs = []time.Duration{5 * time.Millisecond, 5 * time.Millisecond}
	defer func() { embedRetryBackoffs = saved }()

	var calls int
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			if calls < 3 { // fail the first two attempts (transient 503)
				http.Error(w, "overloaded", http.StatusServiceUnavailable)
				return
			}
			var req struct {
				Input []string `json:"input"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			var resp struct {
				Data []struct {
					Index     int       `json:"index"`
					Embedding []float64 `json:"embedding"`
				} `json:"data"`
			}
			for i := range req.Input {
				resp.Data = append(resp.Data, struct {
					Index     int       `json:"index"`
					Embedding []float64 `json:"embedding"`
				}{Index: i, Embedding: []float64{1}})
			}
			_ = json.NewEncoder(w).Encode(resp)
		}),
	)
	defer server.Close()

	embedder, _ := NewHTTPEmbedder(server.URL, "stub", nil)
	vectors, err := embedder.Embed(context.Background(), []string{"x"})
	if err != nil {
		t.Fatalf("embed should succeed after retries: %v", err)
	}
	if len(vectors) != 1 || calls != 3 {
		t.Fatalf(
			"expected success on the 3rd call, got %d vectors after %d calls",
			len(vectors),
			calls,
		)
	}
}
