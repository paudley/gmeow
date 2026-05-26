// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"os"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

func TestPostgresRebuildProjectsFilestore(t *testing.T) {
	dsn := os.Getenv("GMEOW_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GMEOW_TEST_POSTGRES_DSN is required for QUERY integration tests")
	}
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello apollo"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "integration",
			ExternalID: "apollo-1",
		}},
		Relationships: []contracts.Relationship{
			{
				Type: "mentions",
				From: contracts.ObjectDigest(
					"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				),
				To: contracts.ObjectDigest(
					"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				),
				Role: "topic",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		Kind:         "analysis",
		AnalyzerName: "summary",
		AnalyzerVer:  "1",
		Data:         map[string]any{"summary": "apollo integration"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		Kind:         "analysis",
		AnalyzerName: "entities",
		AnalyzerVer:  "1",
		Data:         map[string]any{"entities": []any{"apollo"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteSourceCursor(ctx, contracts.SourceCursor{
		SourceKind: "fixture",
		SourceName: "integration",
		Cursor:     map[string]any{"offset": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	config := Config{ConnString: dsn, MigrationsDir: "../../../migrations/query"}
	if err := Migrate(ctx, config); err != nil {
		t.Fatal(err)
	}
	index, err := New(ctx, config, store)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	report, err := index.RebuildReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Projected != 1 || report.Failed != 0 {
		t.Fatalf("unexpected rebuild report: %#v", report)
	}
	cursors, err := index.SourceCursors(ctx, contracts.SourceCursorRequest{
		SourceNames: []string{"integration"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cursors.Cursors) != 1 ||
		cursors.Cursors[0].SourceKind != "fixture" ||
		cursors.Cursors[0].Cursor["offset"] != "1" {
		t.Fatalf("unexpected source cursor response: %#v", cursors)
	}
	analysis, err := index.AnalysisStatus(ctx, contracts.AnalysisStatusRequest{
		ObjectDigests: []contracts.ObjectDigest{digest},
		Analyzers: []contracts.AnalyzerSpec{
			{Name: "summary", Version: "2"},
			{Name: "entities", Version: "1"},
			{Name: "keywords", Version: "1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(analysis.Statuses) != 3 ||
		analysis.Statuses[0].Status != "stale" ||
		analysis.Statuses[1].Status != "complete" ||
		analysis.Statuses[2].Status != "missing" {
		t.Fatalf("unexpected analysis status response: %#v", analysis)
	}
	response, err := index.Search(ctx, contracts.SearchRequest{
		Query:  "apollo",
		Facets: []string{"file"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Total != 1 || response.Results[0].ObjectDigest != digest {
		t.Fatalf("unexpected search response: %#v", response)
	}
	filtered, err := index.Search(ctx, contracts.SearchRequest{
		Query: "apollo",
		Provenance: contracts.ProvenanceFilter{
			SourceNames: []string{"integration"},
			ExternalIDs: []string{"apollo-1"},
		},
		Relationships: contracts.RelationshipFilter{
			Types: []string{"mentions"},
			Roles: []string{"topic"},
		},
		AnalyzerNames: []string{"summary"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Total != 1 || filtered.Results[0].ObjectDigest != digest {
		t.Fatalf("unexpected filtered search response: %#v", filtered)
	}
	missing, err := index.Search(ctx, contracts.SearchRequest{
		Query: "apollo",
		Provenance: contracts.ProvenanceFilter{
			SourceNames: []string{"other"},
		},
		AnalyzerNames: []string{"summary"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if missing.Total != 0 {
		t.Fatalf("non-matching provenance filter returned results: %#v", missing)
	}
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	embeddingDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader:       strings.NewReader(`{"vector":[0.1,0.2,0.3]}`),
		MediaType:    "application/json",
		ContentRoles: []string{"analysis_output"},
		Facets:       []contracts.Facet{{Kind: "analysis_output"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest.Graph = []contracts.GraphFact{{
		Subject:   "gmeow:entity/Apollo",
		Predicate: "gmeow:mentions",
		Object:    "gmeow:entity/Mission",
	}}
	manifest.Embeddings = []contracts.EmbeddingRef{{
		Model:        "fixture-ref",
		ObjectDigest: embeddingDigest,
		Dimensions:   3,
	}}
	if err := index.Project(ctx, manifest, []contracts.Annotation{{
		ObjectDigest: digest,
		Kind:         "analysis",
		AnalyzerName: "embedding",
		Data: map[string]any{
			"embeddings": []any{map[string]any{
				"model":         "fixture",
				"object_digest": string(digest),
				"dimensions":    float64(3),
				"vector":        []any{float64(0.1), float64(0.2), float64(0.3)},
			}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	vector, err := index.VectorSearch(ctx, contracts.VectorSearchRequest{
		Model:      "fixture",
		Dimensions: 3,
		Vector:     []float32{0.1, 0.2, 0.3},
		Facets:     []string{"file"},
		Limit:      5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(vector.Results) != 1 || vector.Results[0].ObjectDigest != digest {
		t.Fatalf("unexpected vector response: %#v", vector)
	}
	referencedVector, err := index.VectorSearch(ctx, contracts.VectorSearchRequest{
		Model:  "fixture-ref",
		Vector: []float32{0.1, 0.2, 0.3},
		Facets: []string{"file"},
		Limit:  5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(referencedVector.Results) != 1 ||
		referencedVector.Results[0].ObjectDigest != digest {
		t.Fatalf("unexpected referenced vector response: %#v", referencedVector)
	}
	otherDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello artemis"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	otherManifest, err := store.ReadManifest(ctx, otherDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Project(ctx, otherManifest, []contracts.Annotation{{
		ObjectDigest: otherDigest,
		Kind:         "analysis",
		AnalyzerName: "embedding",
		Data: map[string]any{
			"embeddings": []any{map[string]any{
				"model":         "fixture",
				"object_digest": string(otherDigest),
				"dimensions":    float64(2),
				"vector":        []any{float64(0.1), float64(0.2)},
			}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	inferredDimension, err := index.VectorSearch(ctx, contracts.VectorSearchRequest{
		Model:  "fixture",
		Vector: []float32{0.1, 0.2, 0.3},
		Facets: []string{"file"},
		Limit:  5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(inferredDimension.Results) != 1 ||
		inferredDimension.Results[0].ObjectDigest != digest {
		t.Fatalf("unexpected inferred-dimension vector response: %#v", inferredDimension)
	}
	wrongDimension, err := index.VectorSearch(ctx, contracts.VectorSearchRequest{
		Model:      "fixture",
		Dimensions: 2,
		Vector:     []float32{0.1, 0.2, 0.3},
		Facets:     []string{"file"},
		Limit:      5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(wrongDimension.Results) != 0 {
		t.Fatalf("wrong vector dimensions returned results: %#v", wrongDimension)
	}
	ageRows, err := index.AgeCypher(
		ctx,
		"MATCH ()-[r]->() RETURN count(r)",
		"edges agtype",
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(ageRows) != 1 || ageRows[0]["edges"] == "0" {
		t.Fatalf("AGE graph was not populated: %#v", ageRows)
	}
	manifest.Graph = nil
	if err := index.Project(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}
	ageRows, err = index.AgeCypher(
		ctx,
		"MATCH (n) RETURN count(n)",
		"nodes agtype",
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(ageRows) != 1 || ageRows[0]["nodes"] != "0" {
		t.Fatalf("incremental projection left stale AGE nodes: %#v", ageRows)
	}
	if err := index.ProjectObject(ctx, filestore.ProjectionObject{
		Digest: digest,
		Findings: []filestore.ProjectionFinding{{
			Digest: digest,
			Code:   "manifest_read_failed",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	stale, err := index.Search(ctx, contracts.SearchRequest{Query: "apollo"})
	if err != nil {
		t.Fatal(err)
	}
	if stale.Total != 0 {
		t.Fatalf("projection findings left stale search rows: %#v", stale)
	}
	ageRows, err = index.AgeCypher(
		ctx,
		"MATCH ()-[r]->() RETURN count(r)",
		"edges agtype",
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(ageRows) != 1 || ageRows[0]["edges"] != "0" {
		t.Fatalf("projection findings left stale AGE facts: %#v", ageRows)
	}
	var findingCount int
	if err := index.pool.QueryRow(
		ctx,
		"SELECT count(*) FROM query_projection_state WHERE key = $1",
		"findings:"+string(digest),
	).Scan(&findingCount); err != nil {
		t.Fatal(err)
	}
	if findingCount != 1 {
		t.Fatalf("projection finding was not recorded, count=%d", findingCount)
	}
}
