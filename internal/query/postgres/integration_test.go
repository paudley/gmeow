// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

func TestPostgresProjectsFilestore(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })
	sourceName := "integration-" + randomHex(t, 8)
	embeddingModel := "fixture-" + randomHex(t, 4)
	referencedModel := "fixture-ref-" + randomHex(t, 4)
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello apollo"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: sourceName,
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
	annotations := []contracts.Annotation{
		{
			ObjectDigest: digest,
			Kind:         "analysis",
			AnalyzerName: "summary",
			AnalyzerVer:  "1",
			Data:         map[string]any{"summary": "apollo integration"},
		},
		{
			ObjectDigest: digest,
			Kind:         "analysis",
			AnalyzerName: "entities",
			AnalyzerVer:  "1",
			Data:         map[string]any{"entities": []any{"apollo"}},
		},
	}
	for _, annotation := range annotations {
		if err := store.WriteAnnotation(ctx, annotation); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.WriteSourceCursor(ctx, contracts.SourceCursor{
		SourceKind: "fixture",
		SourceName: sourceName,
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
	cleanupDigests := []contracts.ObjectDigest{digest}
	t.Cleanup(index.Close)
	t.Cleanup(func() {
		cleanupQueryIntegrationRows(
			context.Background(),
			t,
			index,
			sourceName,
			cleanupDigests...)
	})
	cleanupQueryIntegrationRows(ctx, t, index, sourceName, cleanupDigests...)
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Project(ctx, manifest, annotations); err != nil {
		t.Fatal(err)
	}
	if err := index.ProjectSourceCursor(ctx, contracts.SourceCursor{
		SourceKind: "fixture",
		SourceName: sourceName,
		Cursor:     map[string]any{"offset": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	cursors, err := index.SourceCursors(ctx, contracts.SourceCursorRequest{
		SourceNames: []string{sourceName},
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
		Provenance: contracts.ProvenanceFilter{
			SourceNames: []string{sourceName},
		},
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
			SourceNames: []string{sourceName},
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
		Model:        referencedModel,
		ObjectDigest: embeddingDigest,
		Dimensions:   3,
	}}
	if err := index.Project(ctx, manifest, []contracts.Annotation{{
		ObjectDigest: digest,
		Kind:         "analysis",
		AnalyzerName: "embedding",
		Data: map[string]any{
			"embeddings": []any{map[string]any{
				"id":            "chunk-exact",
				"kind":          "body_chunk",
				"model":         embeddingModel,
				"source_digest": string(digest),
				"ordinal":       float64(1),
				"text_preview":  "apollo integration body",
				"dimensions":    float64(3),
				"vector":        []any{float64(0.1), float64(0.2), float64(0.3)},
			}, map[string]any{
				"id":            "chunk-far",
				"kind":          "header",
				"model":         embeddingModel,
				"source_digest": string(digest),
				"ordinal":       float64(0),
				"text_preview":  "apollo integration header",
				"dimensions":    float64(3),
				"vector":        []any{float64(0.9), float64(0.8), float64(0.7)},
			}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	vector, err := index.VectorSearch(ctx, contracts.VectorSearchRequest{
		Model:      embeddingModel,
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
	if vector.Results[0].EmbeddingID != "chunk-exact" ||
		vector.Results[0].Kind != "body_chunk" ||
		vector.Results[0].TextPreview != "apollo integration body" {
		t.Fatalf("vector response did not expose best chunk metadata: %#v", vector)
	}
	referencedVector, err := index.VectorSearch(ctx, contracts.VectorSearchRequest{
		Model:  referencedModel,
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
	cleanupDigests = append(cleanupDigests, otherDigest)
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
				"model":         embeddingModel,
				"object_digest": string(otherDigest),
				"dimensions":    float64(2),
				"vector":        []any{float64(0.1), float64(0.2)},
			}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	inferredDimension, err := index.VectorSearch(ctx, contracts.VectorSearchRequest{
		Model:  embeddingModel,
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
		Model:      embeddingModel,
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
		fmt.Sprintf(
			"MATCH ()-[r]->() WHERE r.object_digest = %s RETURN count(r)",
			ageStringLiteral(string(digest)),
		),
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
		fmt.Sprintf(
			"MATCH ()-[r]->() WHERE r.object_digest = %s RETURN count(r)",
			ageStringLiteral(string(digest)),
		),
		"edges agtype",
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(ageRows) != 1 || ageRows[0]["edges"] != "0" {
		t.Fatalf("incremental projection left stale AGE edges: %#v", ageRows)
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
	stale, err := index.Search(ctx, contracts.SearchRequest{
		Query: "apollo",
		Provenance: contracts.ProvenanceFilter{
			SourceNames: []string{sourceName},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stale.Total != 0 {
		t.Fatalf("projection findings left stale search rows: %#v", stale)
	}
	ageRows, err = index.AgeCypher(
		ctx,
		fmt.Sprintf(
			"MATCH ()-[r]->() WHERE r.object_digest = %s RETURN count(r)",
			ageStringLiteral(string(digest)),
		),
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

func queryIntegrationDSN(t *testing.T) string {
	t.Helper()
	loaded, err := config.Load(config.Options{
		Path: filepath.Join("..", "..", "..", "gmeow.toml"),
	})
	if err != nil {
		t.Fatalf("load integration config: %v", err)
	}
	return postgresDSN(loaded.Resolved.Postgres, loaded.Resolved.Postgres.Database)
}

func postgresDSN(postgres config.ResolvedPostgres, database string) string {
	dsn := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(postgres.User, postgres.Password),
		Host:   postgres.Host + ":" + strconv.Itoa(postgres.Port),
		Path:   database,
	}
	query := dsn.Query()
	query.Set("sslmode", postgres.SSLMode)
	dsn.RawQuery = query.Encode()
	return dsn.String()
}

func acquireQueryIntegrationLock(
	t *testing.T,
	ctx context.Context,
	dsn string,
) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres for integration lock: %v", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", int64(0x676d656f775154)); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("acquire postgres integration lock: %v", err)
	}

	return conn
}

func releaseQueryIntegrationLock(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	if conn == nil {
		return
	}
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", int64(0x676d656f775154)); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("release postgres integration lock: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close postgres integration lock: %v", err)
	}
}

func cleanupQueryIntegrationRows(
	ctx context.Context,
	t *testing.T,
	index *Index,
	sourceName string,
	digests ...contracts.ObjectDigest,
) {
	t.Helper()
	tx, err := index.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin query integration cleanup transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	for _, digest := range digests {
		if err := deleteAgeFactsForDigest(ctx, tx, digest); err != nil {
			t.Fatalf("cleanup AGE facts for %s: %v", digest, err)
		}
		if _, err := tx.Exec(
			ctx,
			"DELETE FROM query_projection_state WHERE key = $1",
			"findings:"+string(digest),
		); err != nil {
			t.Fatalf("cleanup projection findings for %s: %v", digest, err)
		}
		if _, err := tx.Exec(
			ctx,
			"DELETE FROM query_objects WHERE object_digest = $1",
			digest,
		); err != nil {
			t.Fatalf("cleanup query object %s: %v", digest, err)
		}
	}
	if _, err := tx.Exec(
		ctx,
		"DELETE FROM query_source_cursors WHERE source_name = $1",
		sourceName,
	); err != nil {
		t.Fatalf("cleanup source cursor %s: %v", sourceName, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit query integration cleanup transaction: %v", err)
	}
}

func randomHex(t *testing.T, bytes int) string {
	t.Helper()
	buffer := make([]byte, bytes)
	if _, err := rand.Read(buffer); err != nil {
		t.Fatalf("read random bytes: %v", err)
	}
	return hex.EncodeToString(buffer)
}
