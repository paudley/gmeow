// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

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
	ContactID     string        `json:"contact_id"`
	DisplayName   string        `json:"display_name,omitempty"`
	PrimaryEmail  string        `json:"primary_email,omitempty"`
	Facts         []ContactFact `json:"facts"`
	SchemaVersion SchemaVersion `json:"schema_version"`
	FactCount     int           `json:"fact_count"`
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
	ContactID    string  `json:"contact_id"`
	DisplayName  string  `json:"display_name,omitempty"`
	PrimaryEmail string  `json:"primary_email,omitempty"`
	Score        float64 `json:"score"`
	FactCount    int     `json:"fact_count"`
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
