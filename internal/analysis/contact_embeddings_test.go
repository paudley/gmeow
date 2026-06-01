// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestAnalyzeContactsSkipsCurrentEmbeddingWhenNotForced(t *testing.T) {
	ctx := context.Background()
	analyzer, endpointCalls := testContactEmbeddingAnalyzer(t)
	store := &contactEmbeddingStore{
		inputs: []contracts.ContactAnalysisInputResult{{
			ContactID: "contact:alice",
			InputText: "Alice Example alice@example.test",
		}},
		current: true,
	}

	response, err := AnalyzeContacts(
		ctx,
		store,
		analyzer,
		contracts.ContactAnalysisRequest{},
	)
	if err != nil {
		t.Fatalf("AnalyzeContacts returned error: %v", err)
	}

	if response.Analyzed != 0 || response.Skipped != 1 {
		t.Fatalf(
			"response counts = analyzed %d skipped %d",
			response.Analyzed,
			response.Skipped,
		)
	}
	if got := endpointCalls.Load(); got != 0 {
		t.Fatalf("embedding endpoint calls = %d, want 0", got)
	}
	if len(store.upserts) != 0 {
		t.Fatalf("unexpected upserts for current contact: %#v", store.upserts)
	}
	if len(response.Results) != 1 || response.Results[0].Status != "current" {
		t.Fatalf("expected current result, got %#v", response.Results)
	}
}

func TestAnalyzeContactsForcedRecomputesCurrentEmbedding(t *testing.T) {
	ctx := context.Background()
	analyzer, endpointCalls := testContactEmbeddingAnalyzer(t)
	store := &contactEmbeddingStore{
		inputs: []contracts.ContactAnalysisInputResult{{
			ContactID:        "contact:alice",
			InputText:        "Alice Example alice@example.test",
			FactCount:        2,
			MessageCount:     3,
			ParticipantCount: 4,
		}},
		current: true,
	}

	response, err := AnalyzeContacts(
		ctx,
		store,
		analyzer,
		contracts.ContactAnalysisRequest{Forced: true},
	)
	if err != nil {
		t.Fatalf("AnalyzeContacts returned error: %v", err)
	}

	if response.Analyzed != 1 || response.Skipped != 0 {
		t.Fatalf(
			"response counts = analyzed %d skipped %d",
			response.Analyzed,
			response.Skipped,
		)
	}
	if got := endpointCalls.Load(); got != 1 {
		t.Fatalf("embedding endpoint calls = %d, want 1", got)
	}
	if len(store.upserts) != 1 {
		t.Fatalf("upsert count = %d, want 1", len(store.upserts))
	}
	record := store.upserts[0]
	if record.Status != "complete" || len(record.Vector) != 3 {
		t.Fatalf("unexpected embedding record: %#v", record)
	}
}

func TestPreviewContactInputPreservesUTF8(t *testing.T) {
	preview := previewContactInput(strings.Repeat("é", 130))
	if !utf8.ValidString(preview) {
		t.Fatalf("preview is not valid UTF-8: %q", preview)
	}
	if len(preview) > 240 {
		t.Fatalf("preview exceeded byte limit: %d", len(preview))
	}
}

func testContactEmbeddingAnalyzer(
	t *testing.T,
) (*EmbeddingAnalyzer, *atomic.Int64) {
	t.Helper()

	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		_ *http.Request,
	) {
		calls.Add(1)
		_, _ = writer.Write([]byte(`{"data":[{"embedding":[0.1,0.2,0.3]}]}`))
	}))
	t.Cleanup(server.Close)

	analyzer, err := NewEmbeddingAnalyzer(EmbeddingConfig{
		Endpoint: server.URL,
		Model:    "test-contact-embed",
	})
	if err != nil {
		t.Fatalf("NewEmbeddingAnalyzer returned error: %v", err)
	}

	return analyzer, &calls
}

type contactEmbeddingStore struct {
	inputs  []contracts.ContactAnalysisInputResult
	upserts []contracts.ContactEmbeddingUpsert
	current bool
}

func (store *contactEmbeddingStore) ContactAnalysisInputs(
	context.Context,
	contracts.ContactAnalysisInputRequest,
) (contracts.ContactAnalysisInputResponse, error) {
	return contracts.ContactAnalysisInputResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       append([]contracts.ContactAnalysisInputResult{}, store.inputs...),
		Total:         len(store.inputs),
		Limit:         len(store.inputs),
	}, nil
}

func (store *contactEmbeddingStore) ContactAnalysisStatus(
	_ context.Context,
	request contracts.ContactAnalysisStatusRequest,
) (contracts.ContactAnalysisStatusResponse, error) {
	if !store.current {
		return contracts.ContactAnalysisStatusResponse{
			SchemaVersion: contracts.SchemaVersionPhase00,
		}, nil
	}

	return contracts.ContactAnalysisStatusResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results: []contracts.ContactAnalysisStatusResult{{
			ContactID:       request.ContactIDs[0],
			AnalyzerName:    request.AnalyzerName,
			AnalyzerVersion: request.AnalyzerVersion,
			Status:          "complete",
			Model:           request.Model,
			InputHash:       request.InputHashes[0],
		}},
		Total: 1,
		Limit: 1,
	}, nil
}

func (store *contactEmbeddingStore) StoreContactEmbedding(
	_ context.Context,
	record contracts.ContactEmbeddingUpsert,
) error {
	store.upserts = append(store.upserts, record)

	return nil
}
