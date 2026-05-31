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

func TestContactIntelligenceJSONUsesContractKeys(t *testing.T) {
	assertJSONKeys(t, "contact fact response", ContactFactResponse{
		SchemaVersion: SchemaVersionPhase00,
		Total:         1,
		Limit:         10,
		Facts: []ContactFact{{
			ContactID:     "contact",
			FactKind:      "email",
			Value:         "apollo@example.test",
			Predicate:     "schema:email",
			StatementHash: "statement",
		}},
	}, []string{"schema_version", "facts", "total", "limit"}, []string{"SchemaVersion"})

	assertJSONKeys(t, "contact identity detail request", ContactIdentityDetailRequest{
		Identities: []string{"mailto:apollo@example.test"},
		ContactIDs: []string{"contact"},
		Limit:      10,
	}, []string{"identities", "contact_ids", "limit"}, []string{"ContactIDs"})

	encoded, err := json.Marshal(ContactIdentityDetailResponse{
		SchemaVersion: SchemaVersionPhase00,
		Total:         1,
		Limit:         10,
		Results: []ContactIdentityDetail{{
			MatchedToken:  "apollo@example.test",
			Token:         "apollo@example.test",
			TokenHash:     "hash",
			ContactID:     "contact",
			StatementHash: "statement",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !jsonContainsKey(encoded, "schema_version") ||
		!jsonContainsKey(encoded, "results") ||
		jsonContainsKey(encoded, "SchemaVersion") {
		t.Fatalf("contact identity response used wrong JSON keys: %s", encoded)
	}

	assertJSONKeys(t, "contact neighborhood request", ContactNeighborhoodRequest{
		ContactID: "contact",
		FactKinds: []string{"affiliation"},
		Limit:     10,
	}, []string{"contact_id", "fact_kinds", "limit"}, []string{"ContactID"})

	assertJSONKeys(t, "contact neighborhood result", ContactNeighborhoodResult{
		ContactID:     "contact",
		FactKind:      "affiliation",
		Value:         "Blackcat Informatics",
		Predicate:     "schema:affiliation",
		StatementHash: "statement",
	}, []string{"contact_id", "fact_kind", "value", "predicate"}, []string{"ContactID"})

	assertJSONKeys(t, "contact neighborhood response", ContactNeighborhoodResponse{
		SchemaVersion: SchemaVersionPhase00,
		Total:         1,
		Limit:         10,
		Results: []ContactNeighborhoodResult{{
			ContactID: "contact",
			FactKind:  "affiliation",
			Value:     "Blackcat Informatics",
			Predicate: "schema:affiliation",
		}},
	}, []string{"schema_version", "results", "total", "limit"}, []string{"SchemaVersion"})

	assertJSONKeys(t, "contact analysis input request", ContactAnalysisInputRequest{
		ContactIDs: []string{"contact"},
		FactKinds:  []string{"email"},
		Limit:      10,
	}, []string{"contact_ids", "fact_kinds", "limit"}, []string{"ContactIDs"})

	assertJSONKeys(t, "contact analysis input result", ContactAnalysisInputResult{
		ContactID:        "contact",
		DisplayName:      "Apollo",
		PrimaryEmail:     "apollo@example.test",
		InputText:        "Contact: Apollo",
		FactCount:        1,
		MessageCount:     2,
		ParticipantCount: 3,
	}, []string{"contact_id", "display_name", "primary_email", "input_text"}, []string{"ContactID"})

	assertJSONKeys(t, "contact analysis input response", ContactAnalysisInputResponse{
		SchemaVersion: SchemaVersionPhase00,
		Total:         1,
		Limit:         10,
		Results: []ContactAnalysisInputResult{{
			ContactID: "contact",
			InputText: "Contact: Apollo",
		}},
	}, []string{"schema_version", "results", "total", "limit"}, []string{"SchemaVersion"})

	var request ContactFactRequest
	if err := json.Unmarshal([]byte(`{
		"contact_ids": ["contact"],
		"fact_kinds": ["email"],
		"current": true
	}`), &request); err != nil {
		t.Fatal(err)
	}
	if len(request.ContactIDs) != 1 ||
		request.ContactIDs[0] != "contact" ||
		len(request.FactKinds) != 1 ||
		request.FactKinds[0] != "email" ||
		!request.Current {
		t.Fatalf("contact fact request did not unmarshal contract keys: %#v", request)
	}
}

func assertJSONKeys(
	t *testing.T,
	name string,
	value any,
	required []string,
	forbidden []string,
) {
	t.Helper()

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range required {
		if !jsonContainsKey(encoded, key) {
			t.Fatalf("%s missing JSON key %q: %s", name, key, encoded)
		}
	}
	for _, key := range forbidden {
		if jsonContainsKey(encoded, key) {
			t.Fatalf("%s used wrong JSON key %q: %s", name, key, encoded)
		}
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
