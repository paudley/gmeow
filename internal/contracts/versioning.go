// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

const (
	VersionSetFacetKind    = "version_set"
	VersionRecordFacetKind = "version_record"
	VersionDeltaFacetKind  = "version_delta"

	VersionRecordRole = "version_record"
	VersionDeltaRole  = "version_delta"

	VersionRepresentationFull  = "full"
	VersionRepresentationDelta = "delta"

	VersionScaleTrivial = "trivial"
	VersionScaleMinor   = "minor"
	VersionScaleMajor   = "major"

	AnalysisScopeCanonical    = "canonical"
	AnalysisScopeVersionDelta = "version_delta"
	AnalysisScopeInherited    = "inherited"
)

type VersionSetMetadata struct {
	UpdatedAt          string `json:"updated_at,omitempty"`
	DomainKind         string `json:"domain_kind"`
	LogicalID          string `json:"logical_id"`
	CanonicalVersionID string `json:"canonical_version_id"`
	MaxScale           string `json:"max_scale,omitempty"`
	VersionCount       int    `json:"version_count"`
}

type VersionRecordMetadata struct {
	ObservedAt              string            `json:"observed_at,omitempty"`
	PreviousCanonicalID     string            `json:"previous_canonical_id,omitempty"`
	PromotionReason         string            `json:"promotion_reason,omitempty"`
	Representation          string            `json:"representation"`
	VersionID               string            `json:"version_id"`
	VersionSetID            string            `json:"version_set_id"`
	DomainKind              string            `json:"domain_kind"`
	LogicalID               string            `json:"logical_id"`
	BaseVersionID           string            `json:"base_version_id,omitempty"`
	BaseDigest              ObjectDigest      `json:"base_digest,omitempty"`
	FullDigest              ObjectDigest      `json:"full_digest,omitempty"`
	DeltaDigest             ObjectDigest      `json:"delta_digest,omitempty"`
	TargetHash              string            `json:"target_hash"`
	CanonicalizationVersion string            `json:"canonicalization_version"`
	Scale                   string            `json:"scale"`
	Canonical               bool              `json:"canonical"`
	Promoted                bool              `json:"promoted,omitempty"`
	InputFingerprints       map[string]string `json:"input_fingerprints,omitempty"`
}
