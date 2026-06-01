// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

import "time"

const (
	RDFSourceBundleFacetKind = "rdf_source_bundle"
	RDFClaimBundleFacetKind  = "rdf_claim_bundle"
	RDFSourceBundleRole      = "rdf_source_bundle"
	RDFClaimBundleRole       = "rdf_claim_bundle"

	ContactEntityFacetKind = "contact_entity"
	ContactSourceRole      = "contact_source"
	ContactClaimRole       = "contact_claim"
)

type RDFStatement struct {
	ObjectLanguage string       `json:"object_language,omitempty"`
	ObjectDatatype string       `json:"object_datatype,omitempty"`
	SourceDigest   ObjectDigest `json:"source_digest,omitempty"`
	StatementHash  string       `json:"statement_hash"`
	Subject        string       `json:"subject"`
	Predicate      string       `json:"predicate"`
	Object         string       `json:"object"`
	ObjectKind     string       `json:"object_kind"`
}

type ContactFact struct {
	ValidFrom     string       `json:"valid_from,omitempty"`
	ValidUntil    string       `json:"valid_until,omitempty"`
	SourceDigest  ObjectDigest `json:"source_digest,omitempty"`
	StatementHash string       `json:"statement_hash,omitempty"`
	ContactID     string       `json:"contact_id"`
	FactKind      string       `json:"fact_kind"`
	Value         string       `json:"value"`
	Predicate     string       `json:"predicate"`
	Historical    bool         `json:"historical,omitempty"`
}

type ContactAggregate struct {
	ContactID        string        `json:"contact_id"`
	FirstSeenAt      time.Time     `json:"first_seen_at,omitzero"`
	LastSeenAt       time.Time     `json:"last_seen_at,omitzero"`
	DisplayName      string        `json:"display_name,omitempty"`
	PrimaryEmail     string        `json:"primary_email,omitempty"`
	Facts            []ContactFact `json:"facts"`
	SchemaVersion    SchemaVersion `json:"schema_version"`
	FactCount        int           `json:"fact_count"`
	MessageCount     int           `json:"message_count"`
	ParticipantCount int           `json:"participant_count"`
}

type ContactAggregateRequest struct {
	ContactID string `json:"contact_id"`
}

type ContactSearchRequest struct {
	Query  string `json:"query,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	Offset int    `json:"offset,omitempty"`
}

type ContactSearchResult struct {
	ContactID        string    `json:"contact_id"`
	FirstSeenAt      time.Time `json:"first_seen_at,omitzero"`
	LastSeenAt       time.Time `json:"last_seen_at,omitzero"`
	DisplayName      string    `json:"display_name,omitempty"`
	PrimaryEmail     string    `json:"primary_email,omitempty"`
	Score            float64   `json:"score"`
	FactCount        int       `json:"fact_count"`
	MessageCount     int       `json:"message_count"`
	ParticipantCount int       `json:"participant_count"`
}

type ContactSearchResponse struct {
	Results       []ContactSearchResult `json:"results"`
	SchemaVersion SchemaVersion         `json:"schema_version"`
	Total         int                   `json:"total"`
	Limit         int                   `json:"limit"`
	Offset        int                   `json:"offset"`
}

type ContactIdentityResolveRequest struct {
	Identity string `json:"identity"`
}

type ContactIdentityResolveResponse struct {
	ContactIDs    []string      `json:"contact_ids"`
	SchemaVersion SchemaVersion `json:"schema_version"`
}

type ContactFactRequest struct {
	At         string   `json:"at,omitempty"`
	From       string   `json:"from,omitempty"`
	Until      string   `json:"until,omitempty"`
	ContactIDs []string `json:"contact_ids,omitempty"`
	FactKinds  []string `json:"fact_kinds,omitempty"`
	Limit      int      `json:"limit,omitempty"`
	Offset     int      `json:"offset,omitempty"`
	Current    bool     `json:"current,omitempty"`
}

type ContactFactResponse struct {
	Facts         []ContactFact `json:"facts"`
	SchemaVersion SchemaVersion `json:"schema_version"`
	Total         int           `json:"total"`
	Limit         int           `json:"limit"`
	Offset        int           `json:"offset"`
}

type ContactIdentityDetailRequest struct {
	Identities []string `json:"identities,omitempty"`
	ContactIDs []string `json:"contact_ids,omitempty"`
	Limit      int      `json:"limit,omitempty"`
	Offset     int      `json:"offset,omitempty"`
}

type ContactIdentityDetail struct {
	SourceDigest  ObjectDigest `json:"source_digest,omitempty"`
	MatchedToken  string       `json:"matched_token"`
	Token         string       `json:"token"`
	TokenHash     string       `json:"token_hash"`
	ContactID     string       `json:"contact_id"`
	StatementHash string       `json:"statement_hash"`
	ValidFrom     string       `json:"valid_from,omitempty"`
	ValidUntil    string       `json:"valid_until,omitempty"`
}

type ContactIdentityDetailResponse struct {
	Results       []ContactIdentityDetail `json:"results"`
	SchemaVersion SchemaVersion           `json:"schema_version"`
	Total         int                     `json:"total"`
	Limit         int                     `json:"limit"`
	Offset        int                     `json:"offset"`
}

type ContactNeighborhoodRequest struct {
	ContactID string   `json:"contact_id"`
	FactKinds []string `json:"fact_kinds,omitempty"`
	Limit     int      `json:"limit,omitempty"`
	Offset    int      `json:"offset,omitempty"`
}

type ContactNeighborhoodResult struct {
	SourceDigest  ObjectDigest `json:"source_digest,omitempty"`
	StatementHash string       `json:"statement_hash,omitempty"`
	ContactID     string       `json:"contact_id"`
	FactKind      string       `json:"fact_kind"`
	Value         string       `json:"value"`
	Predicate     string       `json:"predicate"`
	ValidFrom     string       `json:"valid_from,omitempty"`
	ValidUntil    string       `json:"valid_until,omitempty"`
}

type ContactNeighborhoodResponse struct {
	Results       []ContactNeighborhoodResult `json:"results"`
	SchemaVersion SchemaVersion               `json:"schema_version"`
	Total         int                         `json:"total"`
	Limit         int                         `json:"limit"`
	Offset        int                         `json:"offset"`
}

type ContactAnalysisInputRequest struct {
	ContactIDs []string `json:"contact_ids,omitempty"`
	FactKinds  []string `json:"fact_kinds,omitempty"`
	Limit      int      `json:"limit,omitempty"`
	Offset     int      `json:"offset,omitempty"`
}

type ContactAnalysisInputResult struct {
	ContactID        string    `json:"contact_id"`
	FirstSeenAt      time.Time `json:"first_seen_at,omitzero"`
	LastSeenAt       time.Time `json:"last_seen_at,omitzero"`
	DisplayName      string    `json:"display_name,omitempty"`
	PrimaryEmail     string    `json:"primary_email,omitempty"`
	InputText        string    `json:"input_text"`
	FactCount        int       `json:"fact_count"`
	MessageCount     int       `json:"message_count"`
	ParticipantCount int       `json:"participant_count"`
}

type ContactAnalysisInputResponse struct {
	Results       []ContactAnalysisInputResult `json:"results"`
	SchemaVersion SchemaVersion                `json:"schema_version"`
	Total         int                          `json:"total"`
	Limit         int                          `json:"limit"`
	Offset        int                          `json:"offset"`
}

type ContactMessageRequest struct {
	ContactID string `json:"contact_id"`
	Role      string `json:"role,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Offset    int    `json:"offset,omitempty"`
}

type ContactMessageResult struct {
	MessageDigest ObjectDigest `json:"message_digest"`
	MessageTime   time.Time    `json:"message_time,omitzero"`
	MessageID     string       `json:"message_id,omitempty"`
	MessageDate   string       `json:"message_date,omitempty"`
	Role          string       `json:"role"`
	Token         string       `json:"token"`
	DisplayName   string       `json:"display_name,omitempty"`
	RawValue      string       `json:"raw_value,omitempty"`
}

type ContactMessageResponse struct {
	Results       []ContactMessageResult `json:"results"`
	SchemaVersion SchemaVersion          `json:"schema_version"`
	Total         int                    `json:"total"`
	Limit         int                    `json:"limit"`
	Offset        int                    `json:"offset"`
}

type ContactAnalysisRequest struct {
	ContactIDs []string `json:"contact_ids,omitempty"`
	FactKinds  []string `json:"fact_kinds,omitempty"`
	Limit      int      `json:"limit,omitempty"`
	Offset     int      `json:"offset,omitempty"`
	Forced     bool     `json:"forced,omitempty"`
}

type ContactAnalysisResult struct {
	ContactID  string `json:"contact_id"`
	Status     string `json:"status"`
	InputHash  string `json:"input_hash,omitempty"`
	Model      string `json:"model,omitempty"`
	Dimensions int    `json:"dimensions,omitempty"`
}

type ContactAnalysisResponse struct {
	Results       []ContactAnalysisResult `json:"results"`
	SchemaVersion SchemaVersion           `json:"schema_version"`
	Analyzed      int                     `json:"analyzed"`
	Skipped       int                     `json:"skipped"`
	Total         int                     `json:"total"`
	Limit         int                     `json:"limit"`
	Offset        int                     `json:"offset"`
}

type ContactAnalysisStatusRequest struct {
	ContactIDs      []string `json:"contact_ids,omitempty"`
	InputHashes     []string `json:"input_hashes,omitempty"`
	AnalyzerName    string   `json:"analyzer_name,omitempty"`
	AnalyzerVersion string   `json:"analyzer_version,omitempty"`
	Model           string   `json:"model,omitempty"`
	Limit           int      `json:"limit,omitempty"`
	Offset          int      `json:"offset,omitempty"`
}

type ContactAnalysisStatusResult struct {
	GeneratedAt     time.Time      `json:"generated_at,omitzero"`
	Metadata        map[string]any `json:"metadata,omitempty"`
	ContactID       string         `json:"contact_id"`
	AnalyzerName    string         `json:"analyzer_name"`
	AnalyzerVersion string         `json:"analyzer_version"`
	Status          string         `json:"status"`
	Model           string         `json:"model,omitempty"`
	InputHash       string         `json:"input_hash,omitempty"`
	InputBytes      int            `json:"input_bytes,omitempty"`
}

type ContactAnalysisStatusResponse struct {
	Results       []ContactAnalysisStatusResult `json:"results"`
	SchemaVersion SchemaVersion                 `json:"schema_version"`
	Total         int                           `json:"total"`
	Limit         int                           `json:"limit"`
	Offset        int                           `json:"offset"`
}

type ContactVectorSearchRequest struct {
	Vector     []float32 `json:"vector"`
	Model      string    `json:"model,omitempty"`
	ContactIDs []string  `json:"contact_ids,omitempty"`
	Limit      int       `json:"limit,omitempty"`
	Dimensions int       `json:"dimensions,omitempty"`
}

type ContactVectorSearchResult struct {
	ContactID    string  `json:"contact_id"`
	DisplayName  string  `json:"display_name,omitempty"`
	PrimaryEmail string  `json:"primary_email,omitempty"`
	Model        string  `json:"model,omitempty"`
	EmbeddingID  string  `json:"embedding_id,omitempty"`
	InputHash    string  `json:"input_hash,omitempty"`
	TextPreview  string  `json:"text_preview,omitempty"`
	Distance     float64 `json:"distance"`
	Dimensions   int     `json:"dimensions,omitempty"`
}

type ContactEmbeddingUpsert struct {
	GeneratedAt     time.Time      `json:"generated_at,omitzero"`
	Metadata        map[string]any `json:"metadata,omitempty"`
	Vector          []float32      `json:"vector,omitempty"`
	ContactID       string         `json:"contact_id"`
	AnalyzerName    string         `json:"analyzer_name"`
	AnalyzerVersion string         `json:"analyzer_version"`
	Status          string         `json:"status"`
	Model           string         `json:"model"`
	InputHash       string         `json:"input_hash"`
	TextPreview     string         `json:"text_preview,omitempty"`
	InputBytes      int            `json:"input_bytes,omitempty"`
}

type ContactVectorSearchResponse struct {
	Results       []ContactVectorSearchResult `json:"results"`
	SchemaVersion SchemaVersion               `json:"schema_version"`
}

type SimilarContactsRequest struct {
	ContactID string `json:"contact_id"`
	Model     string `json:"model,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type SimilarContactsResponse struct {
	Results       []ContactVectorSearchResult `json:"results"`
	SchemaVersion SchemaVersion               `json:"schema_version"`
}
