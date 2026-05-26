// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

const EmbeddingName = "embedding.endpoint"

type EmbeddingConfig struct {
	Endpoint string
	Model    string
	Client   *http.Client
}

type EmbeddingAnalyzer struct {
	endpoint string
	model    string
	client   *http.Client
}

func NewEmbeddingAnalyzer(config EmbeddingConfig) (*EmbeddingAnalyzer, error) {
	if strings.TrimSpace(config.Endpoint) == "" {
		return nil, errors.New("embedding endpoint is required")
	}
	if strings.TrimSpace(config.Model) == "" {
		return nil, errors.New("embedding model is required")
	}
	client := config.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &EmbeddingAnalyzer{
		endpoint: config.Endpoint,
		model:    config.Model,
		client:   client,
	}, nil
}

func (analyzer *EmbeddingAnalyzer) Spec() contracts.AnalyzerSpec {
	return contracts.AnalyzerSpec{
		Name:               EmbeddingName,
		Version:            Phase04Version,
		OutputSections:     []string{"embeddings"},
		Deterministic:      false,
		WorkerKind:         "go",
		RequiredInputs:     []string{"text"},
		IdempotencyFormula: "digest+analyzer+version",
	}
}

func (analyzer *EmbeddingAnalyzer) Analyze(
	ctx context.Context,
	store ObjectStore,
	job contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	content, manifest, err := readObject(ctx, store, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}
	text := extractText(content, manifest.MediaType)
	if text == "" {
		return contracts.Annotation{}, errors.New("embedding input text is empty")
	}
	vector, err := analyzer.embed(ctx, text)
	if err != nil {
		return contracts.Annotation{}, err
	}
	return contracts.Annotation{Data: map[string]any{
		"model":      analyzer.model,
		"dimensions": len(vector),
		"embedding":  vector,
		"input_kind": "extracted_text",
	}}, nil
}

func (analyzer *EmbeddingAnalyzer) embed(
	ctx context.Context,
	text string,
) ([]float64, error) {
	body, err := json.Marshal(map[string]any{
		"model": analyzer.model,
		"input": text,
	})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		analyzer.endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := analyzer.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("call embedding endpoint: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("embedding endpoint returned %s", response.Status)
	}
	var decoded embeddingResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode embedding response: %w", err)
	}
	if len(decoded.Data) == 0 || len(decoded.Data[0].Embedding) == 0 {
		return nil, errors.New("embedding endpoint returned no vector")
	}
	return decoded.Data[0].Embedding, nil
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
}

var _ Analyzer = (*EmbeddingAnalyzer)(nil)
