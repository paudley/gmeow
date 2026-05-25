// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"os"
	"strings"
	"testing"

	"blackat.ca/gmeow/internal/contracts"
	"blackat.ca/gmeow/internal/filestore"
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
		Data:         map[string]any{"summary": "apollo integration"},
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
	manifest.Graph = []contracts.GraphFact{{
		Subject:   "gmeow:entity/Apollo",
		Predicate: "gmeow:mentions",
		Object:    "gmeow:entity/Mission",
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
