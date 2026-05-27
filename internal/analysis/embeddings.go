// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"unicode"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	EmbeddingName             = "embedding.endpoint"
	embeddingInputRuneLimit   = 2000
	embeddingChunkTargetRunes = 1200
	embeddingChunkMinLetters  = 16
)

type EmbeddingConfig struct {
	Client   *http.Client
	Endpoint string
	Model    string
}

type EmbeddingAnalyzer struct {
	client   *http.Client
	endpoint string
	gate     modelGate
	model    string
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
		client = &http.Client{Timeout: modelRequestTimeout}
	}

	return &EmbeddingAnalyzer{
		endpoint: config.Endpoint,
		model:    config.Model,
		client:   client,
		gate:     newModelGate(),
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
	manifest, err := store.ReadManifest(ctx, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}

	segments, err := embeddingSegments(ctx, store, job.ObjectDigest, manifest)
	if err != nil {
		return contracts.Annotation{}, err
	}
	if len(segments) == 0 {
		return contracts.Annotation{Data: map[string]any{
			"status":     "skipped",
			"reason":     "empty_text",
			"model":      analyzer.model,
			"input_kind": "natural_language_segments",
		}}, nil
	}

	embeddings := make([]any, 0, len(segments))
	for _, segment := range segments {
		text, truncated := truncateRunes(segment.Text, embeddingInputRuneLimit)
		var vector []float64
		err = analyzer.gate.withLock(
			ctx,
			modelRequestTimeout,
			func(callCtx context.Context) error {
				var embedErr error
				vector, embedErr = analyzer.embed(callCtx, text)
				return embedErr
			},
		)
		if err != nil {
			return contracts.Annotation{}, err
		}

		embeddings = append(embeddings, map[string]any{
			"id":            embeddingSegmentID(job.ObjectDigest, segment, text),
			"kind":          segment.Kind,
			"model":         analyzer.model,
			"dimensions":    len(vector),
			"vector":        vector,
			"source_digest": string(segment.SourceDigest),
			"ordinal":       segment.Ordinal,
			"text_preview":  previewText(text),
			"truncated":     truncated,
		})
	}

	return contracts.Annotation{Data: map[string]any{
		"status":     "complete",
		"model":      analyzer.model,
		"embeddings": embeddings,
		"input_kind": "natural_language_segments",
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
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		if len(bytes.TrimSpace(detail)) == 0 {
			return nil, fmt.Errorf("embedding endpoint returned %s", response.Status)
		}

		return nil, fmt.Errorf(
			"embedding endpoint returned %s: %s",
			response.Status,
			strings.TrimSpace(string(detail)),
		)
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

func truncateRunes(text string, limit int) (string, bool) {
	if limit <= 0 {
		return text, false
	}

	count := 0
	for index := range text {
		if count == limit {
			return text[:index], true
		}
		count++
	}

	return text, false
}

type embeddingSegment struct {
	SourceDigest contracts.ObjectDigest
	Text         string
	Kind         string
	Ordinal      int
}

func embeddingSegments(
	ctx context.Context,
	store ObjectStore,
	digest contracts.ObjectDigest,
	manifest contracts.Manifest,
) ([]embeddingSegment, error) {
	if !manifest.Compound.IsCompound {
		content, _, err := readObject(ctx, store, digest)
		if err != nil {
			return nil, err
		}

		return chunkEmbeddingText(
			"body_chunk",
			digest,
			extractText(content, manifest.MediaType),
		), nil
	}

	headerText := ""
	bodyTexts := []string{}
	attachmentTexts := []embeddingSegment{}
	for _, part := range manifest.Compound.Parts {
		content, partManifest, err := readObject(ctx, store, part.Digest)
		if err != nil {
			return nil, err
		}

		switch part.Role {
		case "rfc822_headers":
			header, err := parseStoredHeaders(content)
			if err == nil {
				headerText = semanticHeaderText(header)
			}
		case "email_body", "body", "text":
			bodyTexts = append(bodyTexts, extractText(content, partManifest.MediaType))
		case "attachment":
			for _, segment := range chunkEmbeddingText(
				"attachment_chunk",
				part.Digest,
				extractText(content, partManifest.MediaType),
			) {
				segment.Ordinal = part.Order*1000 + segment.Ordinal
				attachmentTexts = append(attachmentTexts, segment)
			}
		}
	}

	segments := []embeddingSegment{}
	summary := naturalLanguageJoin(append(
		[]string{semanticTitles(manifest), headerText},
		bodyTexts...,
	))
	if usableEmbeddingText(summary) {
		segments = append(segments, embeddingSegment{
			Kind:         "email_summary",
			SourceDigest: digest,
			Ordinal:      0,
			Text:         summary,
		})
	}
	if usableEmbeddingText(headerText) {
		segments = append(segments, embeddingSegment{
			Kind:         "header",
			SourceDigest: digest,
			Ordinal:      0,
			Text:         headerText,
		})
	}

	body := naturalLanguageJoin(bodyTexts)
	segments = append(segments, chunkEmbeddingText("body_chunk", digest, body)...)
	segments = append(segments, attachmentTexts...)

	sort.SliceStable(segments, func(left, right int) bool {
		if segments[left].Kind != segments[right].Kind {
			return segments[left].Kind < segments[right].Kind
		}

		return segments[left].Ordinal < segments[right].Ordinal
	})

	return segments, nil
}

func semanticTitles(manifest contracts.Manifest) string {
	parts := make([]string, 0, len(manifest.Titles))
	for _, title := range manifest.Titles {
		parts = append(parts, title.Value)
	}

	return naturalLanguageJoin(parts)
}

func semanticHeaderText(header mailHeader) string {
	values := []string{}
	for _, name := range []string{"Subject", "From", "To", "Cc", "Date"} {
		value := strings.TrimSpace(header.Get(name))
		if value == "" {
			continue
		}
		values = append(values, name+": "+value)
	}

	return naturalLanguageJoin(values)
}

type mailHeader interface {
	Get(string) string
}

func chunkEmbeddingText(
	kind string,
	sourceDigest contracts.ObjectDigest,
	text string,
) []embeddingSegment {
	text = naturalLanguageJoin([]string{text})
	if !usableEmbeddingText(text) {
		return nil
	}

	runes := []rune(text)
	if len(runes) <= embeddingChunkTargetRunes {
		return []embeddingSegment{{
			Kind:         kind,
			SourceDigest: sourceDigest,
			Ordinal:      0,
			Text:         text,
		}}
	}

	segments := []embeddingSegment{}
	for start, ordinal := 0, 0; start < len(runes); ordinal++ {
		end := int(math.Min(float64(start+embeddingChunkTargetRunes), float64(len(runes))))
		if end < len(runes) {
			end = chunkBoundary(runes, start, end)
		}
		chunk := strings.TrimSpace(string(runes[start:end]))
		if usableEmbeddingText(chunk) {
			segments = append(segments, embeddingSegment{
				Kind:         kind,
				SourceDigest: sourceDigest,
				Ordinal:      ordinal,
				Text:         chunk,
			})
		}
		start = end
	}

	return segments
}

func chunkBoundary(runes []rune, start, fallback int) int {
	for index := fallback; index > start+embeddingChunkTargetRunes/2; index-- {
		switch runes[index-1] {
		case '.', '!', '?', '\n':
			return index
		}
	}
	for index := fallback; index > start+embeddingChunkTargetRunes/2; index-- {
		if unicode.IsSpace(runes[index-1]) {
			return index
		}
	}

	return fallback
}

func naturalLanguageJoin(parts []string) string {
	cleaned := []string{}
	for _, part := range parts {
		part = normalizeWhitespace(part)
		if part != "" {
			cleaned = append(cleaned, part)
		}
	}

	return strings.Join(cleaned, "\n")
}

func usableEmbeddingText(text string) bool {
	letters := 0
	for _, char := range text {
		if unicode.IsLetter(char) {
			letters++
		}
	}

	return letters >= embeddingChunkMinLetters
}

func embeddingSegmentID(
	objectDigest contracts.ObjectDigest,
	segment embeddingSegment,
	text string,
) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		string(objectDigest),
		segment.Kind,
		string(segment.SourceDigest),
		fmt.Sprint(segment.Ordinal),
		text,
	}, "\x00")))

	return hex.EncodeToString(sum[:])
}

func previewText(text string) string {
	preview, _ := truncateRunes(normalizeWhitespace(text), 240)

	return preview
}

var _ Analyzer = (*EmbeddingAnalyzer)(nil)
