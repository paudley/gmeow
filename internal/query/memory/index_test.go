// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package memory

import (
	"context"
	"testing"
	"time"

	"blackat.ca/gmeow/internal/contracts"
)

func TestSearchFiltersProjectedObjects(t *testing.T) {
	index := New(nil)
	digest := contracts.ObjectDigest(
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	if err := index.Project(context.Background(), contracts.Manifest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  digest,
		ObjectID:      "fixture-object",
		MediaType:     "text/plain",
		Facets:        []contracts.Facet{{Kind: "file"}},
		Titles:        []contracts.Title{{Value: "Apollo Notes"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "unit",
			ObservedAt: time.Now().UTC(),
		}},
		Relationships: []contracts.Relationship{{
			Type: "mentions",
			From: digest,
			To:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		}},
		Compound: contracts.Compound{Parts: []contracts.CompoundPart{{
			Digest: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			Role:   "body",
		}}},
		Graph: []contracts.GraphFact{
			{Subject: "apollo", Predicate: "related", Object: "notes"},
		},
		Keywords: []string{"apollo"},
	}, []contracts.Annotation{{
		ObjectDigest:  digest,
		Kind:          "analysis",
		AnalyzerName:  "summary",
		AnalyzerVer:   "1",
		GeneratedAt:   time.Now().UTC(),
		Data:          map[string]any{"summary": "mission details"},
		SchemaVersion: contracts.SchemaVersionPhase00,
	}}); err != nil {
		t.Fatal(err)
	}
	response, err := index.Search(context.Background(), contracts.SearchRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Query:         "Apollo",
		Facets:        []string{"file"},
		Provenance: contracts.ProvenanceFilter{
			SourceKinds: []string{"fixture"},
		},
		CompoundRoles: []string{"body"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Total != 1 || response.Results[0].ObjectDigest != digest {
		t.Fatalf("unexpected search response: %#v", response)
	}
	graph, err := index.Graph(context.Background(), contracts.GraphRequest{Node: "apollo"})
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Facts) != 1 {
		t.Fatalf("expected graph fact, got %#v", graph)
	}
	status, err := index.AnalysisStatus(
		context.Background(),
		contracts.AnalysisStatusRequest{
			Analyzers: []contracts.AnalyzerSpec{
				{Name: "summary", Version: "2"},
				{Name: "entities", Version: "1"},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Statuses) != 2 ||
		status.Statuses[0].Status != "stale" ||
		status.Statuses[1].Status != "missing" {
		t.Fatalf("expected stale and missing analysis statuses, got %#v", status)
	}
	if err := index.ProjectSourceCursor(context.Background(), contracts.SourceCursor{
		SchemaVersion: contracts.SchemaVersionPhase00,
		SourceKind:    "fixture",
		SourceName:    "unit",
		Cursor:        map[string]any{"page": "next"},
	}); err != nil {
		t.Fatal(err)
	}
	cursors, err := index.SourceCursors(
		context.Background(),
		contracts.SourceCursorRequest{
			SourceKinds: []string{"fixture"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(cursors.Cursors) != 1 || cursors.Cursors[0].SourceName != "unit" {
		t.Fatalf("expected source cursor, got %#v", cursors)
	}
}
