// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/mail"
	"regexp"
	"sort"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	TextExtractName = "text.extract"
	HeadersName     = "rfc822.headers"
	MetadataName    = "metadata.extract"
	GraphFactsName  = "graph.facts"
	SummaryName     = "summary.centroid"
	Phase04Version  = "phase04"
)

type TextExtractAnalyzer struct{}

func (TextExtractAnalyzer) Spec() contracts.AnalyzerSpec {
	return contracts.AnalyzerSpec{
		Name:    TextExtractName,
		Version: Phase04Version,
		MediaTypes: []string{
			"text/plain",
			"text/html",
			"message/rfc822",
			"application/json",
		},
		OutputSections:     []string{"text"},
		Deterministic:      true,
		WorkerKind:         "go",
		ContentRoles:       nil,
		RequiredInputs:     []string{"blob"},
		Dependencies:       nil,
		Priority:           0,
		IdempotencyFormula: "digest+analyzer+version",
	}
}

func (TextExtractAnalyzer) Analyze(
	ctx context.Context,
	store ObjectStore,
	job contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	content, manifest, err := readObject(ctx, store, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}
	text := extractText(content, manifest.MediaType)
	return contracts.Annotation{
		Data: map[string]any{
			"text":       text,
			"byte_count": len(content),
			"char_count": len([]rune(text)),
			"media_type": manifest.MediaType,
		},
	}, nil
}

type RFC822HeaderAnalyzer struct{}

func (RFC822HeaderAnalyzer) Spec() contracts.AnalyzerSpec {
	return contracts.AnalyzerSpec{
		Name:               HeadersName,
		Version:            Phase04Version,
		MediaTypes:         []string{"message/rfc822", "text/rfc822-headers"},
		OutputSections:     []string{"headers", "graph"},
		Deterministic:      true,
		WorkerKind:         "go",
		RequiredInputs:     []string{"blob"},
		IdempotencyFormula: "digest+analyzer+version",
	}
}

func (RFC822HeaderAnalyzer) Analyze(
	ctx context.Context,
	store ObjectStore,
	job contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	content, _, err := readObject(ctx, store, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}
	message, err := mail.ReadMessage(bytes.NewReader(content))
	if err != nil {
		return contracts.Annotation{}, err
	}
	data := map[string]any{
		"message_id":  message.Header.Get("Message-Id"),
		"subject":     message.Header.Get("Subject"),
		"from":        message.Header.Get("From"),
		"to":          message.Header.Get("To"),
		"cc":          message.Header.Get("Cc"),
		"date":        message.Header.Get("Date"),
		"references":  message.Header.Get("References"),
		"in_reply_to": message.Header.Get("In-Reply-To"),
		"headers":     normalizedHeaders(message.Header),
	}
	if date, err := message.Header.Date(); err == nil {
		data["date_rfc3339"] = date.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	return contracts.Annotation{Data: data}, nil
}

type MetadataAnalyzer struct{}

func (MetadataAnalyzer) Spec() contracts.AnalyzerSpec {
	return contracts.AnalyzerSpec{
		Name:               MetadataName,
		Version:            Phase04Version,
		OutputSections:     []string{"metadata"},
		Deterministic:      true,
		WorkerKind:         "go",
		IdempotencyFormula: "digest+analyzer+version",
	}
}

func (MetadataAnalyzer) Analyze(
	ctx context.Context,
	store ObjectStore,
	job contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	manifest, err := store.ReadManifest(ctx, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}
	return contracts.Annotation{Data: map[string]any{
		"object_id":          manifest.ObjectID,
		"identity_strategy":  manifest.IdentityStrategy,
		"media_type":         manifest.MediaType,
		"size":               manifest.Size,
		"content_roles":      append([]string(nil), manifest.ContentRoles...),
		"facets":             facetKinds(manifest.Facets),
		"provenance_count":   len(manifest.Provenance),
		"relationship_count": len(manifest.Relationships),
		"part_count":         len(manifest.Compound.Parts),
	}}, nil
}

type GraphFactAnalyzer struct{}

func (GraphFactAnalyzer) Spec() contracts.AnalyzerSpec {
	return contracts.AnalyzerSpec{
		Name:               GraphFactsName,
		Version:            Phase04Version,
		OutputSections:     []string{"graph"},
		Deterministic:      true,
		WorkerKind:         "go",
		IdempotencyFormula: "digest+analyzer+version",
	}
}

func (GraphFactAnalyzer) Analyze(
	ctx context.Context,
	store ObjectStore,
	job contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	manifest, err := store.ReadManifest(ctx, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}
	facts := make(
		[]map[string]any,
		0,
		len(manifest.Provenance)+len(manifest.Relationships),
	)
	for _, provenance := range manifest.Provenance {
		if provenance.SourceKind == "" {
			continue
		}
		facts = append(facts, map[string]any{
			"subject":   string(manifest.ObjectDigest),
			"predicate": "observed_from",
			"object":    provenance.SourceKind + ":" + provenance.SourceName,
		})
	}
	for _, relationship := range manifest.Relationships {
		facts = append(facts, map[string]any{
			"subject":   string(relationship.From),
			"predicate": relationship.Type,
			"object":    string(relationship.To),
			"role":      relationship.Role,
		})
	}
	sort.SliceStable(facts, func(left, right int) bool {
		return facts[left]["predicate"].(string) < facts[right]["predicate"].(string)
	})
	return contracts.Annotation{Data: map[string]any{"facts": facts}}, nil
}

type SummaryAnalyzer struct{}

func (SummaryAnalyzer) Spec() contracts.AnalyzerSpec {
	return contracts.AnalyzerSpec{
		Name:               SummaryName,
		Version:            Phase04Version,
		OutputSections:     []string{"summary"},
		Deterministic:      true,
		WorkerKind:         "go",
		RequiredInputs:     []string{"blob"},
		IdempotencyFormula: "digest+analyzer+version",
	}
}

func (SummaryAnalyzer) Analyze(
	ctx context.Context,
	store ObjectStore,
	job contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	content, manifest, err := readObject(ctx, store, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}
	text := extractText(content, manifest.MediaType)
	summary := firstSentences(text, 2)
	status := "complete"
	if summary == "" {
		status = "placeholder"
	}
	return contracts.Annotation{Data: map[string]any{
		"status":     status,
		"summary":    summary,
		"algorithm":  "extractive_first_sentences",
		"media_type": manifest.MediaType,
	}}, nil
}

func DefaultRegistry() (*Registry, error) {
	return NewRegistry(
		TextExtractAnalyzer{},
		RFC822HeaderAnalyzer{},
		MetadataAnalyzer{},
		GraphFactAnalyzer{},
		SummaryAnalyzer{},
	)
}

func readObject(
	ctx context.Context,
	store ObjectStore,
	digest contracts.ObjectDigest,
) ([]byte, contracts.Manifest, error) {
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		return nil, contracts.Manifest{}, err
	}
	reader, err := store.Open(ctx, digest)
	if err != nil {
		return nil, contracts.Manifest{}, err
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, contracts.Manifest{}, err
	}
	return content, manifest, nil
}

func extractText(content []byte, mediaType string) string {
	text := string(content)
	base, _, err := mime.ParseMediaType(mediaType)
	if err == nil {
		mediaType = base
	}
	switch mediaType {
	case "text/html":
		return normalizeWhitespace(stripTags(text))
	case "application/json":
		var value any
		if json.Unmarshal(content, &value) == nil {
			encoded, err := json.Marshal(value)
			if err == nil {
				return string(encoded)
			}
		}
		return normalizeWhitespace(text)
	case "message/rfc822":
		message, err := mail.ReadMessage(bytes.NewReader(content))
		if err != nil {
			return normalizeWhitespace(text)
		}
		body, err := io.ReadAll(message.Body)
		if err != nil {
			return ""
		}
		return normalizeWhitespace(string(body))
	default:
		return normalizeWhitespace(text)
	}
}

func normalizedHeaders(header mail.Header) map[string][]string {
	result := map[string][]string{}
	for key, values := range header {
		canonical := httpHeaderCanonical(key)
		result[canonical] = append([]string(nil), values...)
	}
	return result
}

func httpHeaderCanonical(value string) string {
	parts := strings.Split(strings.ToLower(value), "-")
	for index, part := range parts {
		if part == "" {
			continue
		}
		parts[index] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, "-")
}

func facetKinds(facets []contracts.Facet) []string {
	kinds := make([]string, 0, len(facets))
	for _, facet := range facets {
		if facet.FacetKind() != "" {
			kinds = append(kinds, facet.FacetKind())
		}
	}
	sort.Strings(kinds)
	return kinds
}

var (
	tagPattern        = regexp.MustCompile(`<[^>]+>`)
	whitespacePattern = regexp.MustCompile(`\s+`)
)

func stripTags(value string) string {
	return tagPattern.ReplaceAllString(value, " ")
}

func normalizeWhitespace(value string) string {
	return strings.TrimSpace(whitespacePattern.ReplaceAllString(value, " "))
}

func firstSentences(text string, limit int) string {
	text = normalizeWhitespace(text)
	if text == "" || limit <= 0 {
		return ""
	}
	endCount := 0
	for index, char := range text {
		switch char {
		case '.', '!', '?':
			endCount++
			if endCount >= limit {
				return strings.TrimSpace(text[:index+1])
			}
		}
	}
	return text
}
