// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	EmbeddingName             = "embedding.endpoint"
	EmbeddingModel            = "nomic-embed-text"
	embeddingInputRuneLimit   = 2000
	embeddingChunkTargetRunes = 1200
	embeddingChunkMinLetters  = 16
)

type EmbeddingConfig struct {
	Model     string
	Namespace string
	Service   EmbeddingService
}

type EmbeddingService interface {
	EmbedNamespace(
		ctx context.Context,
		namespace string,
		texts []string,
	) ([][]float32, int, error)
}

type EmbeddingAnalyzer struct {
	gate      modelGate
	model     string
	namespace string
	service   EmbeddingService
}

func NewEmbeddingAnalyzer(config EmbeddingConfig) (*EmbeddingAnalyzer, error) {
	if config.Service == nil {
		return nil, errors.New("embedding service is required")
	}

	model := strings.TrimSpace(config.Model)
	if model == "" {
		model = EmbeddingModel
	}

	return &EmbeddingAnalyzer{
		model:     model,
		gate:      newModelGate(),
		namespace: strings.TrimSpace(config.Namespace),
		service:   config.Service,
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

func (analyzer *EmbeddingAnalyzer) Model() string {
	return analyzer.model
}

func (analyzer *EmbeddingAnalyzer) EmbedText(
	ctx context.Context,
	text string,
) ([]float64, string, bool, error) {
	truncatedText, truncated := truncateRunes(text, embeddingInputRuneLimit)
	var vector []float64
	err := analyzer.gate.withLock(
		ctx,
		modelRequestTimeout,
		func(callCtx context.Context) error {
			var embedErr error
			vector, embedErr = analyzer.embed(callCtx, truncatedText)

			return embedErr
		},
	)
	if err != nil {
		return nil, "", false, err
	}

	return vector, truncatedText, truncated, nil
}

func (analyzer *EmbeddingAnalyzer) embed(
	ctx context.Context,
	text string,
) ([]float64, error) {
	vectors, _, err := analyzer.service.EmbedNamespace(
		ctx,
		analyzer.namespace,
		[]string{strings.ToValidUTF8(text, "")},
	)
	if err != nil {
		return nil, fmt.Errorf("call embedding service: %w", err)
	}
	if len(vectors) == 0 || len(vectors[0]) == 0 {
		return nil, errors.New("embedding service returned no vector")
	}

	return float32VectorToFloat64(vectors[0]), nil
}

func float32VectorToFloat64(values []float32) []float64 {
	converted := make([]float64, len(values))
	for index, value := range values {
		converted[index] = float64(value)
	}

	return converted
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

	bodyTexts := []string{}
	for _, part := range manifest.Compound.Parts {
		content, partManifest, err := readObject(ctx, store, part.Digest)
		if err != nil {
			return nil, err
		}

		switch part.Role {
		case "email_body", "body", "text":
			bodyTexts = append(bodyTexts, extractText(content, partManifest.MediaType))
		}
	}

	segments := []embeddingSegment{}
	body := naturalLanguageJoin(bodyTexts)
	segments = append(segments, chunkEmbeddingText("body_chunk", digest, body)...)

	return segments, nil
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
