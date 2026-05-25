// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

import "time"

type SchemaVersion int

const SchemaVersionPhase00 SchemaVersion = 1

type ObjectDigest string

type Manifest struct {
	SchemaVersion SchemaVersion  `json:"schema_version"`
	ObjectDigest  ObjectDigest   `json:"object_digest"`
	ObjectID      string         `json:"object_id,omitempty"`
	MediaType     string         `json:"media_type,omitempty"`
	Facets        []Facet        `json:"facets"`
	Provenance    []Provenance   `json:"provenance,omitempty"`
	Relationships []Relationship `json:"relationships,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

type Facet struct {
	Name       string         `json:"name"`
	Version    string         `json:"version,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type Provenance struct {
	SourceName string         `json:"source_name"`
	SourceKind string         `json:"source_kind"`
	ExternalID string         `json:"external_id,omitempty"`
	ObservedAt time.Time      `json:"observed_at"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type Relationship struct {
	Type   string       `json:"type"`
	From   ObjectDigest `json:"from"`
	To     ObjectDigest `json:"to"`
	Role   string       `json:"role,omitempty"`
	Order  int          `json:"order,omitempty"`
	Source string       `json:"source,omitempty"`
}

type SourceEvent struct {
	SchemaVersion SchemaVersion  `json:"schema_version"`
	EventID       string         `json:"event_id"`
	SourceName    string         `json:"source_name"`
	SourceKind    string         `json:"source_kind"`
	ObservedAt    time.Time      `json:"observed_at"`
	ObjectDigest  ObjectDigest   `json:"object_digest,omitempty"`
	Capabilities  []string       `json:"capabilities,omitempty"`
	Payload       map[string]any `json:"payload,omitempty"`
}

type AnalyzerSpec struct {
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	MediaTypes   []string `json:"media_types,omitempty"`
	ContentRoles []string `json:"content_roles,omitempty"`
	Priority     int      `json:"priority,omitempty"`
	WorkerKind   string   `json:"worker_kind,omitempty"`
	Enabled      bool     `json:"enabled"`
}

type AnalyzerJob struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	JobID         string        `json:"job_id"`
	Analyzer      AnalyzerSpec  `json:"analyzer"`
	ObjectDigest  ObjectDigest  `json:"object_digest"`
	Forced        bool          `json:"forced"`
	Priority      int           `json:"priority"`
	CreatedAt     time.Time     `json:"created_at"`
}

type Annotation struct {
	SchemaVersion SchemaVersion  `json:"schema_version"`
	ObjectDigest  ObjectDigest   `json:"object_digest"`
	AnalyzerName  string         `json:"analyzer_name,omitempty"`
	AnalyzerVer   string         `json:"analyzer_version,omitempty"`
	Kind          string         `json:"kind"`
	GeneratedAt   time.Time      `json:"generated_at"`
	Data          map[string]any `json:"data,omitempty"`
}

type SearchRequest struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	Query         string        `json:"query"`
	Facets        []string      `json:"facets,omitempty"`
	Limit         int           `json:"limit,omitempty"`
	Offset        int           `json:"offset,omitempty"`
}

type SearchResult struct {
	ObjectDigest ObjectDigest   `json:"object_digest"`
	Score        float64        `json:"score,omitempty"`
	Title        string         `json:"title,omitempty"`
	Snippet      string         `json:"snippet,omitempty"`
	Facets       []string       `json:"facets,omitempty"`
	Attributes   map[string]any `json:"attributes,omitempty"`
}

type SearchResponse struct {
	SchemaVersion SchemaVersion  `json:"schema_version"`
	Results       []SearchResult `json:"results"`
	Total         int            `json:"total"`
}
