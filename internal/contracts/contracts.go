// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

import "time"

type SchemaVersion int

const SchemaVersionPhase00 SchemaVersion = 1

const (
	PriorityInteractive = "interactive"
	PriorityForced      = "forced"
	PriorityFreshIngest = "fresh_ingest"
	PriorityRepair      = "repair"
	PriorityBackground  = "background"
)

type ObjectDigest string

type Manifest struct {
	Timestamps       Timestamps     `json:"timestamps,omitempty"`
	UpdatedAt        time.Time      `json:"updated_at"`
	CreatedAt        time.Time      `json:"created_at"`
	Overlays         map[string]any `json:"overlays,omitempty"`
	Analysis         map[string]any `json:"analysis,omitempty"`
	ObjectDigest     ObjectDigest   `json:"digest"`
	ObjectID         string         `json:"object_id"`
	IdentityStrategy string         `json:"identity_strategy"`
	MediaType        string         `json:"media_type,omitempty"`
	Compression      string         `json:"compression"`
	Compound         Compound       `json:"compound"`
	Graph            []GraphFact    `json:"graph,omitempty"`
	Relationships    []Relationship `json:"relationships,omitempty"`
	Provenance       []Provenance   `json:"provenance,omitempty"`
	Titles           []Title        `json:"titles,omitempty"`
	Keywords         []string       `json:"keywords,omitempty"`
	Embeddings       []EmbeddingRef `json:"embeddings,omitempty"`
	Facets           []Facet        `json:"facets"`
	ContentRoles     []string       `json:"content_roles,omitempty"`
	SchemaVersion    SchemaVersion  `json:"schema_version"`
	Size             int64          `json:"size"`
}

type Facet struct {
	Metadata   map[string]any `json:"metadata,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
	Kind       string         `json:"kind"`
	Name       string         `json:"name,omitempty"`
	Version    string         `json:"version,omitempty"`
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
	ObservedAt       time.Time      `json:"observed_at"`
	Metadata         map[string]any `json:"metadata,omitempty"`
	Attributes       map[string]any `json:"attributes,omitempty"`
	SourceName       string         `json:"source_name"`
	SourceKind       string         `json:"source_kind"`
	ExternalID       string         `json:"external_id,omitempty"`
	ExternalVersion  string         `json:"external_version,omitempty"`
	CapabilitiesSeen []string       `json:"capabilities_seen,omitempty"`
}

type SourceObjectRef struct {
	SourceKind      string `json:"source_kind"`
	SourceName      string `json:"source_name"`
	ExternalID      string `json:"external_id"`
	ExternalVersion string `json:"external_version,omitempty"`
}

type SourceIngestClaim struct {
	AcquiredAt   time.Time       `json:"acquired_at"`
	SourceObject SourceObjectRef `json:"source_object"`
	ClaimID      string          `json:"claim_id"`
}

type Relationship struct {
	Type   string       `json:"type"`
	From   ObjectDigest `json:"from"`
	To     ObjectDigest `json:"to"`
	Role   string       `json:"role,omitempty"`
	Source string       `json:"source,omitempty"`
	Order  int          `json:"order,omitempty"`
}

type Compound struct {
	Parts      []CompoundPart `json:"parts,omitempty"`
	IsCompound bool           `json:"is_compound"`
}

type CompoundPart struct {
	Metadata map[string]any `json:"metadata,omitempty"`
	Digest   ObjectDigest   `json:"digest"`
	Role     string         `json:"role"`
	Order    int            `json:"order,omitempty"`
	Required bool           `json:"required,omitempty"`
}

type GraphFact struct {
	Metadata  map[string]any `json:"metadata,omitempty"`
	Subject   string         `json:"subject"`
	Predicate string         `json:"predicate"`
	Object    string         `json:"object"`
}

type EmbeddingRef struct {
	Model        string       `json:"model"`
	ObjectDigest ObjectDigest `json:"object_digest"`
	Dimensions   int          `json:"dimensions,omitempty"`
}

type StructurePart struct {
	Metadata map[string]any `json:"metadata,omitempty"`
	Digest   ObjectDigest   `json:"digest"`
	Role     string         `json:"role"`
	Facets   []string       `json:"facets,omitempty"`
	Order    int            `json:"order,omitempty"`
	Required bool           `json:"required,omitempty"`
}

type Structure struct {
	PartsByRole   map[string][]StructurePart `json:"parts_by_role"`
	ObjectDigest  ObjectDigest               `json:"digest"`
	ObjectID      string                     `json:"object_id"`
	Facets        []string                   `json:"facets"`
	SchemaVersion SchemaVersion              `json:"schema_version"`
}

type SourceEvent struct {
	ObservedAt    time.Time      `json:"observed_at"`
	Cursor        map[string]any `json:"cursor,omitempty"`
	Payload       map[string]any `json:"payload,omitempty"`
	EventID       string         `json:"event_id"`
	SourceName    string         `json:"source_name"`
	SourceKind    string         `json:"source_kind"`
	EventKind     string         `json:"event_kind"`
	ExternalID    string         `json:"external_id,omitempty"`
	ObjectDigest  ObjectDigest   `json:"object_digest,omitempty"`
	Capabilities  []string       `json:"capabilities,omitempty"`
	Relationships []Relationship `json:"relationships,omitempty"`
	SchemaVersion SchemaVersion  `json:"schema_version"`
}

type AnalyzerSpec struct {
	Name               string   `json:"name"`
	Version            string   `json:"version"`
	IdempotencyFormula string   `json:"idempotency_key_formula,omitempty"`
	WorkerKind         string   `json:"worker_kind,omitempty"`
	MediaTypes         []string `json:"media_types,omitempty"`
	ContentRoles       []string `json:"content_roles,omitempty"`
	RequiredInputs     []string `json:"required_inputs,omitempty"`
	OutputSections     []string `json:"output_sections,omitempty"`
	Dependencies       []string `json:"dependencies,omitempty"`
	Priority           int      `json:"priority,omitempty"`
	Deterministic      bool     `json:"deterministic,omitempty"`
}

type AnalyzerJob struct {
	CreatedAt      time.Time     `json:"created_at"`
	Deadline       time.Time     `json:"deadline,omitempty"`
	Analyzer       AnalyzerSpec  `json:"analyzer"`
	RequestedBy    string        `json:"requested_by,omitempty"`
	ObjectDigest   ObjectDigest  `json:"object_digest"`
	PriorityClass  string        `json:"priority_class,omitempty"`
	Reason         string        `json:"reason,omitempty"`
	TraceID        string        `json:"trace_id,omitempty"`
	Failure        string        `json:"failure,omitempty"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	JobID          string        `json:"job_id"`
	SchemaVersion  SchemaVersion `json:"schema_version"`
	Attempt        int           `json:"attempt,omitempty"`
	Priority       int           `json:"priority"`
	Forced         bool          `json:"forced"`
}

type SchedulerScanRequest struct {
	PriorityClass string        `json:"priority_class,omitempty"`
	RequestedBy   string        `json:"requested_by,omitempty"`
	Reason        string        `json:"reason,omitempty"`
	TraceID       string        `json:"trace_id,omitempty"`
	SchemaVersion SchemaVersion `json:"schema_version"`
	Forced        bool          `json:"forced,omitempty"`
}

type SchedulerScanResponse struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	Scanned       int           `json:"scanned"`
	Enqueued      int           `json:"enqueued"`
	Skipped       int           `json:"skipped"`
	Failed        int           `json:"failed"`
}

type ObjectChangeRequest struct {
	PriorityClass  string         `json:"priority_class,omitempty"`
	RequestedBy    string         `json:"requested_by,omitempty"`
	Reason         string         `json:"reason,omitempty"`
	TraceID        string         `json:"trace_id,omitempty"`
	SchemaVersion  SchemaVersion  `json:"schema_version"`
	ObjectDigests  []ObjectDigest `json:"object_digests"`
	ProjectionOnly bool           `json:"projection_only,omitempty"`
}

type SchedulerStatus struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	Pending       int           `json:"pending"`
	Retry         int           `json:"retry"`
	Failed        int           `json:"failed"`
	DeadLetter    int           `json:"dead_letter"`
}

type DeadLetterRequest struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	Limit         int           `json:"limit,omitempty"`
}

type DeadLetterResponse struct {
	Jobs          []AnalyzerJob `json:"jobs"`
	SchemaVersion SchemaVersion `json:"schema_version"`
}

type RequeueRequest struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	Limit         int           `json:"limit,omitempty"`
}

type RequeueResponse struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	Requeued      int           `json:"requeued"`
}

type ReconcilePendingResponse struct {
	SchemaVersion    SchemaVersion `json:"schema_version"`
	Checked          int           `json:"checked"`
	DroppedSatisfied int           `json:"dropped_satisfied"`
	DroppedDuplicate int           `json:"dropped_duplicate"`
	Republished      int           `json:"republished"`
	Kept             int           `json:"kept"`
	KeptJobs         []AnalyzerJob `json:"-"`
}

type Annotation struct {
	GeneratedAt   time.Time      `json:"generated_at"`
	Data          map[string]any `json:"data,omitempty"`
	ObjectDigest  ObjectDigest   `json:"object_digest"`
	AnalyzerName  string         `json:"analyzer_name,omitempty"`
	AnalyzerVer   string         `json:"analyzer_version,omitempty"`
	Kind          string         `json:"kind"`
	SchemaVersion SchemaVersion  `json:"schema_version"`
}

type SearchRequest struct {
	Relationships RelationshipFilter `json:"relationships,omitempty"`
	Query         string             `json:"query"`
	Provenance    ProvenanceFilter   `json:"provenance,omitempty"`
	Facets        []string           `json:"facets,omitempty"`
	CompoundRoles []string           `json:"compound_roles,omitempty"`
	AnalyzerNames []string           `json:"analyzer_names,omitempty"`
	MediaTypes    []string           `json:"media_types,omitempty"`
	SchemaVersion SchemaVersion      `json:"schema_version"`
	Limit         int                `json:"limit,omitempty"`
	Offset        int                `json:"offset,omitempty"`
}

type SearchResult struct {
	Attributes   map[string]any `json:"attributes,omitempty"`
	ObjectDigest ObjectDigest   `json:"object_digest"`
	Title        string         `json:"title,omitempty"`
	Snippet      string         `json:"snippet,omitempty"`
	Facets       []string       `json:"facets,omitempty"`
	Score        float64        `json:"score,omitempty"`
}

type SearchResponse struct {
	Results       []SearchResult `json:"results"`
	SchemaVersion SchemaVersion  `json:"schema_version"`
	Total         int            `json:"total"`
}

type ProvenanceFilter struct {
	SourceKinds []string `json:"source_kinds,omitempty"`
	SourceNames []string `json:"source_names,omitempty"`
	ExternalIDs []string `json:"external_ids,omitempty"`
}

type RelationshipFilter struct {
	From  ObjectDigest `json:"from,omitempty"`
	To    ObjectDigest `json:"to,omitempty"`
	Any   ObjectDigest `json:"any,omitempty"`
	Types []string     `json:"types,omitempty"`
	Roles []string     `json:"roles,omitempty"`
}

type RelationshipRequest struct {
	Filter        RelationshipFilter `json:"filter"`
	SchemaVersion SchemaVersion      `json:"schema_version"`
	Limit         int                `json:"limit,omitempty"`
}

type RelationshipResponse struct {
	Relationships []Relationship `json:"relationships"`
	SchemaVersion SchemaVersion  `json:"schema_version"`
}

type GraphRequest struct {
	Node          string        `json:"node,omitempty"`
	Predicate     string        `json:"predicate,omitempty"`
	SchemaVersion SchemaVersion `json:"schema_version"`
	Limit         int           `json:"limit,omitempty"`
}

type GraphResponse struct {
	Facts         []GraphFact   `json:"facts"`
	SchemaVersion SchemaVersion `json:"schema_version"`
}

type AnalysisStatusRequest struct {
	ObjectDigests []ObjectDigest `json:"object_digests,omitempty"`
	AnalyzerNames []string       `json:"analyzer_names,omitempty"`
	Analyzers     []AnalyzerSpec `json:"analyzers,omitempty"`
	SchemaVersion SchemaVersion  `json:"schema_version"`
	Limit         int            `json:"limit,omitempty"`
}

type AnalysisStatus struct {
	GeneratedAt  time.Time      `json:"generated_at,omitempty"`
	Data         map[string]any `json:"data,omitempty"`
	ObjectDigest ObjectDigest   `json:"object_digest"`
	AnalyzerName string         `json:"analyzer_name"`
	AnalyzerVer  string         `json:"analyzer_version,omitempty"`
	Status       string         `json:"status"`
}

type AnalysisStatusResponse struct {
	Statuses      []AnalysisStatus `json:"statuses"`
	SchemaVersion SchemaVersion    `json:"schema_version"`
}

type VectorSearchRequest struct {
	Model         string        `json:"model"`
	Vector        []float32     `json:"vector"`
	Facets        []string      `json:"facets,omitempty"`
	SchemaVersion SchemaVersion `json:"schema_version"`
	Dimensions    int           `json:"dimensions,omitempty"`
	Limit         int           `json:"limit,omitempty"`
}

type VectorSearchResult struct {
	ObjectDigest ObjectDigest `json:"object_digest"`
	Model        string       `json:"model"`
	EmbeddingID  string       `json:"embedding_id,omitempty"`
	Kind         string       `json:"kind,omitempty"`
	SourceDigest ObjectDigest `json:"source_digest,omitempty"`
	TextPreview  string       `json:"text_preview,omitempty"`
	Distance     float64      `json:"distance"`
}

type VectorSearchResponse struct {
	Results       []VectorSearchResult `json:"results"`
	SchemaVersion SchemaVersion        `json:"schema_version"`
}

type SourceCursor struct {
	UpdatedAt     time.Time      `json:"updated_at,omitempty"`
	Cursor        map[string]any `json:"cursor"`
	SourceKind    string         `json:"source_kind"`
	SourceName    string         `json:"source_name"`
	SchemaVersion SchemaVersion  `json:"schema_version"`
}

type SourceCursorRef struct {
	SourceKind string `json:"source_kind"`
	SourceName string `json:"source_name"`
}

type SourceCursorRequest struct {
	SourceKinds   []string      `json:"source_kinds,omitempty"`
	SourceNames   []string      `json:"source_names,omitempty"`
	SchemaVersion SchemaVersion `json:"schema_version"`
	Limit         int           `json:"limit,omitempty"`
}

type SourceCursorResponse struct {
	Cursors       []SourceCursor `json:"cursors"`
	SchemaVersion SchemaVersion  `json:"schema_version"`
}
