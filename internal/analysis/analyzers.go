// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"bytes"
	"context"
	"encoding/json"
	stdhtml "html"
	"io"
	"mime"
	"net/mail"
	"regexp"
	"sort"
	"strings"

	nethtml "golang.org/x/net/html"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	TextExtractName = "text.extract"
	HeadersName     = "rfc822.headers"
	MetadataName    = "metadata.extract"
	GraphFactsName  = "graph.facts"
	SummaryName     = "summary.model"
	Phase04Version  = "phase04-email-v2"
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
	manifest, err := store.ReadManifest(ctx, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}

	text, byteCount, err := analysisText(ctx, store, job.ObjectDigest, manifest)
	if err != nil {
		return contracts.Annotation{}, err
	}

	return contracts.Annotation{
		Data: map[string]any{
			"text":       text,
			"byte_count": byteCount,
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
	manifest, err := store.ReadManifest(ctx, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}
	if !manifest.Compound.IsCompound && !isRFC822HeaderMediaType(manifest.MediaType) {
		return contracts.Annotation{Data: map[string]any{
			"status":     "skipped",
			"reason":     "not_rfc822_headers",
			"media_type": manifest.MediaType,
		}}, nil
	}

	content, _, err := readHeaderObject(ctx, store, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}

	header, err := parseStoredHeaders(content)
	if err != nil {
		return contracts.Annotation{}, err
	}

	data := map[string]any{
		"message_id":  header.Get("Message-Id"),
		"subject":     header.Get("Subject"),
		"from":        header.Get("From"),
		"to":          header.Get("To"),
		"cc":          header.Get("Cc"),
		"date":        header.Get("Date"),
		"references":  header.Get("References"),
		"in_reply_to": header.Get("In-Reply-To"),
		"headers":     normalizedHeaders(header),
	}
	if date, err := header.Date(); err == nil {
		data["date_rfc3339"] = date.UTC().Format("2006-01-02T15:04:05Z07:00")
	}

	return contracts.Annotation{Data: data}, nil
}

func isRFC822HeaderMediaType(mediaType string) bool {
	base, _, err := mime.ParseMediaType(mediaType)
	if err == nil {
		mediaType = base
	}

	switch mediaType {
	case "message/rfc822", "text/rfc822-headers":
		return true
	default:
		return false
	}
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

func DefaultRegistry() (*Registry, error) {
	return NewRegistry(
		TextExtractAnalyzer{},
		RFC822HeaderAnalyzer{},
		MetadataAnalyzer{},
		GraphFactAnalyzer{},
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

func readHeaderObject(
	ctx context.Context,
	store ObjectStore,
	digest contracts.ObjectDigest,
) ([]byte, contracts.Manifest, error) {
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		return nil, contracts.Manifest{}, err
	}

	if manifest.Compound.IsCompound {
		for _, part := range manifest.Compound.Parts {
			if part.Role != "rfc822_headers" {
				continue
			}

			return readObject(ctx, store, part.Digest)
		}
	}

	content, manifest, err := readObject(ctx, store, digest)
	if err != nil {
		return nil, contracts.Manifest{}, err
	}

	return content, manifest, nil
}

func analysisText(
	ctx context.Context,
	store ObjectStore,
	digest contracts.ObjectDigest,
	manifest contracts.Manifest,
) (string, int, error) {
	cacheKey := extractedTextKey(digest, manifest)
	if cached, ok := extractedTextCache.Get(cacheKey); ok {
		return cached.text, cached.bytes, nil
	}

	text, byteCount, err := computeAnalysisText(ctx, store, digest, manifest)
	if err != nil {
		return "", 0, err
	}

	extractedTextCache.Put(cacheKey, extractedText{text: text, bytes: byteCount})

	return text, byteCount, nil
}

func computeAnalysisText(
	ctx context.Context,
	store ObjectStore,
	digest contracts.ObjectDigest,
	manifest contracts.Manifest,
) (string, int, error) {
	if !manifest.Compound.IsCompound {
		content, _, err := readObject(ctx, store, digest)
		if err != nil {
			return "", 0, err
		}

		return extractText(content, manifest.MediaType), len(content), nil
	}

	parts := []string{manifest.ObjectID}
	byteCount := 0
	for _, title := range manifest.Titles {
		parts = append(parts, title.Value)
	}
	for _, facet := range manifest.Facets {
		if len(facet.Metadata) == 0 {
			continue
		}
		encoded, err := json.Marshal(facet.Metadata)
		if err == nil {
			parts = append(parts, string(encoded))
		}
	}

	for _, part := range manifest.Compound.Parts {
		if !analysisTextPartRole(part.Role) {
			continue
		}

		content, partManifest, err := readObject(ctx, store, part.Digest)
		if err != nil {
			return "", 0, err
		}
		byteCount += len(content)

		if part.Role == "rfc822_headers" {
			header, err := parseStoredHeaders(content)
			if err == nil {
				parts = append(parts, headerText(header))
				continue
			}
		}

		parts = append(parts, extractText(content, partManifest.MediaType))
	}

	return normalizeWhitespace(strings.Join(parts, "\n")), byteCount, nil
}

func analysisTextPartRole(role string) bool {
	switch role {
	case "email_body", "rfc822_headers", "text", "body":
		return true
	default:
		return false
	}
}

func extractText(content []byte, mediaType string) string {
	text := string(content)

	base, _, err := mime.ParseMediaType(mediaType)
	if err == nil {
		mediaType = base
	}

	switch mediaType {
	case "text/html":
		return normalizeWhitespace(stripHTML(text))
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
	case "text/rfc822-headers":
		header, err := parseStoredHeaders(content)
		if err != nil {
			return normalizeWhitespace(text)
		}

		return normalizeWhitespace(headerText(header))
	default:
		return normalizeWhitespace(text)
	}
}

func parseStoredHeaders(content []byte) (mail.Header, error) {
	var pairs []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(content, &pairs); err == nil && len(pairs) > 0 {
		header := mail.Header{}
		for _, pair := range pairs {
			if strings.TrimSpace(pair.Name) != "" {
				header[pair.Name] = append(header[pair.Name], pair.Value)
			}
		}

		return header, nil
	}

	values := map[string]string{}
	if err := json.Unmarshal(content, &values); err == nil && len(values) > 0 {
		header := mail.Header{}
		for name, value := range values {
			header[name] = append(header[name], value)
		}

		return header, nil
	}

	raw := append([]byte(nil), content...)
	raw = append(raw, []byte("\r\n\r\n")...)
	message, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}

	return message.Header, nil
}

func headerText(header mail.Header) string {
	names := make([]string, 0, len(header))
	for name := range header {
		names = append(names, name)
	}
	sort.Strings(names)

	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+": "+strings.Join(header[name], " "))
	}

	return strings.Join(parts, "\n")
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

var whitespacePattern = regexp.MustCompile(`\s+`)

func stripHTML(value string) string {
	root, err := nethtml.Parse(strings.NewReader(value))
	if err != nil {
		return stdhtml.UnescapeString(value)
	}

	var builder strings.Builder
	writeHTMLText(&builder, root, false)

	return builder.String()
}

func writeHTMLText(builder *strings.Builder, node *nethtml.Node, skip bool) {
	if node.Type == nethtml.ElementNode {
		switch strings.ToLower(node.Data) {
		case "script", "style", "template", "noscript":
			skip = true
		case "br",
			"p",
			"div",
			"li",
			"tr",
			"td",
			"th",
			"section",
			"article",
			"header",
			"footer":
			builder.WriteByte(' ')
		}
	}

	if !skip && node.Type == nethtml.TextNode {
		builder.WriteString(stdhtml.UnescapeString(node.Data))
		builder.WriteByte(' ')
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		writeHTMLText(builder, child, skip)
	}

	if !skip && node.Type == nethtml.ElementNode {
		switch strings.ToLower(node.Data) {
		case "p", "div", "li", "tr", "td", "th", "section", "article", "header", "footer":
			builder.WriteByte(' ')
		}
	}
}

func normalizeWhitespace(value string) string {
	return strings.TrimSpace(whitespacePattern.ReplaceAllString(value, " "))
}
