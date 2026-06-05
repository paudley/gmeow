// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/embedding"
)

// stubEmbedder is a deterministic stand-in for the model endpoint: equal claim
// texts map to equal unit vectors, so records about the same person (shared
// claims) pool to nearby centroids — no live endpoint needed in tests.
type stubEmbedder struct{ dim int }

func (s stubEmbedder) Embed(
	_ context.Context,
	texts []string,
) ([]embedding.Vector, error) {
	out := make([]embedding.Vector, len(texts))
	for i, text := range texts {
		v := make(embedding.Vector, s.dim)
		var seed uint32 = 2166136261
		for _, b := range []byte(text) {
			seed = (seed ^ uint32(b)) * 16777619
		}
		for j := range v {
			seed = seed*1664525 + 1013904223
			v[j] = float32(int32(seed%2000)-1000) / 1000.0
		}
		out[i] = embedding.Normalize(v)
	}

	return out, nil
}

// newTestResolver builds an in-process embedding.Service (deterministic vectors,
// counter ULIDs) that satisfies contactio.Resolver.
func newTestResolver() Resolver {
	service := embedding.NewService(
		embedding.NewResolver(
			embedding.NewMemoryCache(),
			stubEmbedder{dim: embedding.FullDim},
		),
		embedding.NewEntityIndex(embedding.FullDim, embedding.CoarseDim),
		"stub",
	)
	var counter int
	service.SetIDSource(
		func() string { counter++; return fmt.Sprintf("01E%04d", counter) },
	)

	return service
}

func TestResolveImportProducesEntitySubjectedDeltaRecords(t *testing.T) {
	ctx := context.Background()
	resolver := newTestResolver()
	const threshold = 0.5
	prov := SourceProvenance{
		Location:      "/imports/paudley.ttl",
		ModifiedAt:    time.Date(2002, 6, 30, 12, 0, 0, 0, time.UTC),
		ContentDigest: "sha256:deadbeef",
		IngestedAt:    time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC),
	}

	rooted := []byte(`@prefix gmeow: <https://blackcatinformatics.ca/gmeow/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
<https://example.test/#paudley> a foaf:Person ;
    foaf:name "Patrick Audley" ;
    gmeow:hasEmail <mailto:paudley@blackcat.ca> ;
    gmeow:affiliation "Blackcat Informatics" .
`)

	records, result, err := ResolveImport(
		ctx,
		resolver,
		threshold,
		threshold,
		FormatRDF,
		"lod",
		rooted,
		ImportOptions{ImportLevel: 10},
		prov,
	)
	if err != nil {
		t.Fatalf("resolve import (rooted): %v", err)
	}
	if len(records) != 1 || records[0].IsNoop || !records[0].IsNew {
		t.Fatalf("rooted import: want 1 new record, got %+v", records)
	}
	entity := records[0].Entity
	if len(result.Contacts) != 1 || result.Contacts[0] != entity {
		t.Fatalf(
			"result.Contacts should be the resolved entity, got %v (entity %s)",
			result.Contacts,
			entity,
		)
	}
	content := records[0].Content
	if !strings.Contains(content, EntityPrefix+entity) {
		t.Fatalf("delta record not subjected to the entity IRI:\n%s", content)
	}
	// The entity's first record must carry rdf:type foaf:Person so the QUERY
	// projection detects the entity as a contact (the entity-as-contact surface).
	if !strings.Contains(content, "22-rdf-syntax-ns#type") ||
		!strings.Contains(content, "foaf/0.1/Person") {
		t.Fatalf(
			"delta record missing the rdf:type foaf:Person triple (projection won't surface the entity):\n%s",
			content,
		)
	}
	// Four-clock provenance: a gmeow:Source carries the CARRIER time (mtime), a
	// gmeow:ImportActivity the TRANSACTION time, and claims a derived terminus-ante-
	// quem — but NEVER the old observedAt-as-validity stamp.
	if strings.Contains(content, "observedAt") {
		t.Fatalf("delta still emits the conflated observedAt stamp:\n%s", content)
	}
	for _, want := range []string{
		"sourceModifiedAt", "2002-06-30", // carrier time on the Source
		"ImportActivity", "ingestedAt", "2026-06-05", // transaction time
		"recordedNoLaterThan", // per-claim derived bound (no source assertion)
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("delta record missing four-clock term %q:\n%s", want, content)
		}
	}

	// Re-import the exact same graph -> NOOP, nothing to persist.
	noop, _, err := ResolveImport(
		ctx,
		resolver,
		threshold,
		threshold,
		FormatRDF,
		"lod",
		rooted,
		ImportOptions{ImportLevel: 10},
		prov,
	)
	if err != nil {
		t.Fatalf("resolve import (re-run): %v", err)
	}
	if len(noop) != 1 || !noop[0].IsNoop || noop[0].Content != "" {
		t.Fatalf("re-import should be a NOOP with no content, got %+v", noop)
	}
	if noop[0].Entity != entity {
		t.Fatalf("re-import resolved to %s, want same entity %s", noop[0].Entity, entity)
	}
}
