// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

func TestEmbeddingAnalyzerCallsConfiguredEndpoint(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putAnalyzerTextObject(t, ctx, store, "semantic signal")
	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			var payload map[string]any
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload["model"] != "test-embed" {
				t.Fatalf("unexpected model payload: %#v", payload)
			}
			if payload["input"] != "semantic signal" {
				t.Fatalf("unexpected input payload: %#v", payload)
			}
			_, _ = writer.Write([]byte(`{"data":[{"embedding":[0.25,0.5,0.75]}]}`))
		}),
	)
	defer server.Close()
	analyzer, err := NewEmbeddingAnalyzer(EmbeddingConfig{
		Endpoint: server.URL,
		Model:    "test-embed",
	})
	if err != nil {
		t.Fatal(err)
	}
	annotation, err := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}
	if annotation.Data["model"] != "test-embed" {
		t.Fatalf("unexpected embedding annotation: %#v", annotation.Data)
	}
	if annotation.Data["dimensions"] != 3 {
		t.Fatalf("unexpected embedding dimensions: %#v", annotation.Data)
	}
}

func TestSummaryAnalyzerProducesExplicitPlaceholderForEmptyText(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putAnalyzerTextObject(t, ctx, store, "   ")
	analyzer := SummaryAnalyzer{}
	annotation, err := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}
	if annotation.Data["status"] != "placeholder" {
		t.Fatalf("expected placeholder summary, got %#v", annotation.Data)
	}
}

func putAnalyzerTextObject(
	t *testing.T,
	ctx context.Context,
	store filestore.Store,
	text string,
) contracts.ObjectDigest {
	t.Helper()
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader(text),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file", Version: "1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
