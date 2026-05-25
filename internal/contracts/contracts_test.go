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
