// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

const summaryInputRuneLimit = 4000

type SummaryConfig struct {
	Client   *http.Client
	Endpoint string
	Model    string
}

type SummaryAnalyzer struct {
	client   *http.Client
	endpoint string
	gate     modelGate
	model    string
}

func NewSummaryAnalyzer(config SummaryConfig) (*SummaryAnalyzer, error) {
	if strings.TrimSpace(config.Endpoint) == "" {
		return nil, errors.New("summary endpoint is required")
	}
	if strings.TrimSpace(config.Model) == "" {
		return nil, errors.New("summary model is required")
	}

	client := config.Client
	if client == nil {
		client = &http.Client{Timeout: modelRequestTimeout}
	}

	return &SummaryAnalyzer{
		client:   client,
		endpoint: config.Endpoint,
		gate:     newModelGate(),
		model:    config.Model,
	}, nil
}

func (analyzer *SummaryAnalyzer) Spec() contracts.AnalyzerSpec {
	return contracts.AnalyzerSpec{
		Name:               SummaryName,
		Version:            Phase04Version,
		OutputSections:     []string{"summary"},
		Deterministic:      false,
		WorkerKind:         "go",
		RequiredInputs:     []string{"text"},
		IdempotencyFormula: "digest+analyzer+version",
	}
}

func (analyzer *SummaryAnalyzer) Analyze(
	ctx context.Context,
	store ObjectStore,
	job contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	manifest, err := store.ReadManifest(ctx, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}

	text, _, err := analysisText(ctx, store, job.ObjectDigest, manifest)
	if err != nil {
		return contracts.Annotation{}, err
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return contracts.Annotation{Data: map[string]any{
			"status":     "skipped",
			"reason":     "empty_text",
			"model":      analyzer.model,
			"input_kind": "extracted_text",
		}}, nil
	}
	text, truncated := truncateRunes(text, summaryInputRuneLimit)

	var summary summaryPayload
	err = analyzer.gate.withLock(
		ctx,
		modelRequestTimeout,
		func(callCtx context.Context) error {
			var summaryErr error
			summary, summaryErr = analyzer.summarize(callCtx, text)
			return summaryErr
		},
	)
	if err != nil {
		return contracts.Annotation{}, err
	}

	return contracts.Annotation{Data: map[string]any{
		"status":     "complete",
		"summary":    summary.Summary,
		"bullets":    summary.Bullets,
		"model":      analyzer.model,
		"input_kind": "extracted_text",
		"truncated":  truncated,
	}}, nil
}

func (analyzer *SummaryAnalyzer) summarize(
	ctx context.Context,
	text string,
) (summaryPayload, error) {
	body, err := json.Marshal(chatCompletionRequest{
		Model: analyzer.model,
		Messages: []chatCompletionMessage{
			{
				Role: "system",
				Content: "Return strict JSON only with keys summary and bullets. " +
					"summary must be one concise sentence. bullets must contain up to three short strings.",
			},
			{
				Role:    "user",
				Content: "Summarize this email for an operator:\n\n" + text,
			},
		},
		Temperature: 0,
	})
	if err != nil {
		return summaryPayload{}, err
	}

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		analyzer.endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return summaryPayload{}, err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := analyzer.client.Do(request)
	if err != nil {
		return summaryPayload{}, fmt.Errorf("call summary endpoint: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		if len(bytes.TrimSpace(detail)) == 0 {
			return summaryPayload{}, fmt.Errorf("summary endpoint returned %s", response.Status)
		}

		return summaryPayload{}, fmt.Errorf(
			"summary endpoint returned %s: %s",
			response.Status,
			strings.TrimSpace(string(detail)),
		)
	}

	var decoded chatCompletionResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return summaryPayload{}, fmt.Errorf("decode summary response: %w", err)
	}
	if len(decoded.Choices) == 0 {
		return summaryPayload{}, errors.New("summary endpoint returned no choices")
	}

	var summary summaryPayload
	content := strings.TrimSpace(decoded.Choices[0].Message.Content)
	if err := json.Unmarshal([]byte(stripJSONFence(content)), &summary); err != nil {
		return summaryPayload{}, fmt.Errorf(
			"summary endpoint returned non-json content: %w",
			err,
		)
	}
	if err := validateSummary(summary, text); err != nil {
		return summaryPayload{}, err
	}

	return summary, nil
}

func stripJSONFence(content string) string {
	content = strings.TrimSpace(content)
	if !strings.HasPrefix(content, "```") {
		return content
	}

	lines := strings.Split(content, "\n")
	if len(lines) < 3 {
		return content
	}
	if strings.TrimSpace(lines[len(lines)-1]) != "```" {
		return content
	}

	return strings.TrimSpace(strings.Join(lines[1:len(lines)-1], "\n"))
}

func validateSummary(summary summaryPayload, input string) error {
	summary.Summary = strings.TrimSpace(summary.Summary)
	if summary.Summary == "" {
		return errors.New("summary endpoint returned empty summary")
	}
	if strings.Contains(input, summary.Summary) && len([]rune(summary.Summary)) > 80 {
		return errors.New("summary endpoint echoed input instead of summarizing")
	}
	if len([]rune(summary.Summary)) > 500 {
		return errors.New("summary endpoint returned overlong summary")
	}
	if len(summary.Bullets) > 3 {
		return errors.New("summary endpoint returned too many bullets")
	}
	for _, bullet := range summary.Bullets {
		if strings.TrimSpace(bullet) == "" {
			return errors.New("summary endpoint returned empty bullet")
		}
	}

	return nil
}

type chatCompletionRequest struct {
	Model       string                  `json:"model"`
	Messages    []chatCompletionMessage `json:"messages"`
	Temperature float64                 `json:"temperature"`
}

type chatCompletionMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message chatCompletionMessage `json:"message"`
	} `json:"choices"`
}

type summaryPayload struct {
	Summary string   `json:"summary"`
	Bullets []string `json:"bullets"`
}

var _ Analyzer = (*SummaryAnalyzer)(nil)
