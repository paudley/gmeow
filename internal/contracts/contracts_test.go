// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

import (
	"encoding/json"
	"testing"
	"time"
)

func TestManifestJSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	manifest := Manifest{
		SchemaVersion: SchemaVersionPhase00,
		ObjectDigest:  "digest",
		Facets:        []Facet{{Name: "file"}},
		Provenance: []Provenance{
			{SourceName: "test", SourceKind: "fixture", ObservedAt: now},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Manifest
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ObjectDigest != manifest.ObjectDigest {
		t.Fatalf("unexpected digest: %s", decoded.ObjectDigest)
	}
}

func TestSearchRequestJSONUsesContractKeys(t *testing.T) {
	encoded := []byte(`{
		"schema_version": 1,
		"query": "apollo",
		"provenance": {
			"source_names": ["fixture"],
			"external_ids": ["id-1"]
		},
		"relationships": {
			"types": ["mentions"],
			"roles": ["topic"]
		}
	}`)
	var request SearchRequest
	if err := json.Unmarshal(encoded, &request); err != nil {
		t.Fatal(err)
	}
	if len(request.Provenance.SourceNames) != 1 ||
		request.Provenance.SourceNames[0] != "fixture" ||
		len(request.Relationships.Roles) != 1 ||
		request.Relationships.Roles[0] != "topic" {
		t.Fatalf("request did not unmarshal snake_case filters: %#v", request)
	}
	roundTrip, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if !jsonContainsKey(roundTrip, "provenance") ||
		!jsonContainsKey(roundTrip, "relationships") {
		t.Fatalf("request did not marshal contract keys: %s", roundTrip)
	}
}

func TestAnalysisStatusJSONUsesDataKey(t *testing.T) {
	encoded, err := json.Marshal(AnalysisStatus{
		ObjectDigest: "digest",
		AnalyzerName: "summary",
		Status:       "complete",
		Data:         map[string]any{"summary": "ok"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !jsonContainsKey(encoded, "data") || jsonContainsKey(encoded, "Data") {
		t.Fatalf("analysis status data used wrong JSON key: %s", encoded)
	}
}

func jsonContainsKey(encoded []byte, key string) bool {
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		return false
	}
	_, ok := value[key]
	return ok
}
