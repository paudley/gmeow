// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

import "time"

type SchemaVersion int

const SchemaVersionPhase00 SchemaVersion = 1

type ObjectDigest string

type Manifest struct {
	SchemaVersion    SchemaVersion  `json:"schema_version"`
	ObjectDigest     ObjectDigest   `json:"digest"`
	ObjectID         string         `json:"object_id"`
	IdentityStrategy string         `json:"identity_strategy"`
	MediaType        string         `json:"media_type,omitempty"`
	Size             int64          `json:"size"`
	Compression      string         `json:"compression"`
	ContentRoles     []string       `json:"content_roles,omitempty"`
	Facets           []Facet        `json:"facets"`
	Titles           []Title        `json:"titles,omitempty"`
	Timestamps       Timestamps     `json:"timestamps,omitempty"`
	Provenance       []Provenance   `json:"provenance,omitempty"`
	Relationships    []Relationship `json:"relationships,omitempty"`
	Compound         Compound       `json:"compound"`
	Analysis         map[string]any `json:"analysis,omitempty"`
	Graph            []GraphFact    `json:"graph,omitempty"`
	Keywords         []string       `json:"keywords,omitempty"`
	Embeddings       []EmbeddingRef `json:"embeddings,omitempty"`
	Overlays         map[string]any `json:"overlays,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

type Facet struct {
	Kind       string         `json:"kind"`
	Name       string         `json:"name,omitempty"`
	Version    string         `json:"version,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

func (facet Facet) FacetKind() string {
	if facet.Kind != "" {
		return facet.Kind
	}
	return facet.Name
}

type Title struct {
	Value      string  `json:"value"`
	Source     string  `json:"source,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
}

type Timestamps struct {
	Created  time.Time `json:"created,omitempty"`
	Modified time.Time `json:"modified,omitempty"`
	Observed time.Time `json:"observed,omitempty"`
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

type Compound struct {
	IsCompound bool           `json:"is_compound"`
	Parts      []CompoundPart `json:"parts,omitempty"`
}

type CompoundPart struct {
	Digest   ObjectDigest   `json:"digest"`
	Role     string         `json:"role"`
	Order    int            `json:"order,omitempty"`
	Required bool           `json:"required,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type GraphFact struct {
	Subject   string         `json:"subject"`
	Predicate string         `json:"predicate"`
	Object    string         `json:"object"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type EmbeddingRef struct {
	Model        string       `json:"model"`
	ObjectDigest ObjectDigest `json:"object_digest"`
	Dimensions   int          `json:"dimensions,omitempty"`
}

type StructurePart struct {
	Digest   ObjectDigest   `json:"digest"`
	Role     string         `json:"role"`
	Order    int            `json:"order,omitempty"`
	Required bool           `json:"required,omitempty"`
	Facets   []string       `json:"facets,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type Structure struct {
	SchemaVersion SchemaVersion              `json:"schema_version"`
	ObjectDigest  ObjectDigest               `json:"digest"`
	ObjectID      string                     `json:"object_id"`
	Facets        []string                   `json:"facets"`
	PartsByRole   map[string][]StructurePart `json:"parts_by_role"`
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
	SchemaVersion SchemaVersion      `json:"schema_version"`
	Query         string             `json:"query"`
	Facets        []string           `json:"facets,omitempty"`
	Provenance    ProvenanceFilter   `json:"provenance,omitempty"`
	Relationships RelationshipFilter `json:"relationships,omitempty"`
	CompoundRoles []string           `json:"compound_roles,omitempty"`
	AnalyzerNames []string           `json:"analyzer_names,omitempty"`
	MediaTypes    []string           `json:"media_types,omitempty"`
	Limit         int                `json:"limit,omitempty"`
	Offset        int                `json:"offset,omitempty"`
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

type ProvenanceFilter struct {
	SourceKinds []string `json:"source_kinds,omitempty"`
	SourceNames []string `json:"source_names,omitempty"`
	ExternalIDs []string `json:"external_ids,omitempty"`
}

type RelationshipFilter struct {
	Types []string     `json:"types,omitempty"`
	From  ObjectDigest `json:"from,omitempty"`
	To    ObjectDigest `json:"to,omitempty"`
	Roles []string     `json:"roles,omitempty"`
	Any   ObjectDigest `json:"any,omitempty"`
}

type RelationshipRequest struct {
	SchemaVersion SchemaVersion      `json:"schema_version"`
	Filter        RelationshipFilter `json:"filter"`
	Limit         int                `json:"limit,omitempty"`
}

type RelationshipResponse struct {
	SchemaVersion SchemaVersion  `json:"schema_version"`
	Relationships []Relationship `json:"relationships"`
}

type GraphRequest struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	Node          string        `json:"node,omitempty"`
	Predicate     string        `json:"predicate,omitempty"`
	Limit         int           `json:"limit,omitempty"`
}

type GraphResponse struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	Facts         []GraphFact   `json:"facts"`
}

type AnalysisStatusRequest struct {
	SchemaVersion SchemaVersion  `json:"schema_version"`
	ObjectDigests []ObjectDigest `json:"object_digests,omitempty"`
	AnalyzerNames []string       `json:"analyzer_names,omitempty"`
	Limit         int            `json:"limit,omitempty"`
}

type AnalysisStatus struct {
	ObjectDigest ObjectDigest   `json:"object_digest"`
	AnalyzerName string         `json:"analyzer_name"`
	AnalyzerVer  string         `json:"analyzer_version,omitempty"`
	Status       string         `json:"status"`
	GeneratedAt  time.Time      `json:"generated_at,omitempty"`
	Data         map[string]any `json:"data,omitempty"`
}

type AnalysisStatusResponse struct {
	SchemaVersion SchemaVersion    `json:"schema_version"`
	Statuses      []AnalysisStatus `json:"statuses"`
}

type VectorSearchRequest struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	Model         string        `json:"model"`
	Dimensions    int           `json:"dimensions,omitempty"`
	Vector        []float32     `json:"vector"`
	Facets        []string      `json:"facets,omitempty"`
	Limit         int           `json:"limit,omitempty"`
}

type VectorSearchResult struct {
	ObjectDigest ObjectDigest `json:"object_digest"`
	Model        string       `json:"model"`
	Distance     float64      `json:"distance"`
}

type VectorSearchResponse struct {
	SchemaVersion SchemaVersion        `json:"schema_version"`
	Results       []VectorSearchResult `json:"results"`
}
