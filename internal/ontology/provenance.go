// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package ontology

// The four-clock import-provenance vocabulary (canonical:
// ~/Active/gmeow-ontology/docs/import-provenance.md). Each clock lives on a
// distinct node and must never be conflated — only VALID time feeds resolution's
// co-validity gate. All terms are already registered in the published ontology
// (sources/provenance/temporal modules); these are the IRIs the importer adopts.
const (
	// Valid time (tenure) — on the CLAIM (RDF-star). The only clock the resolution
	// co-validity gate reads.
	ValidFrom  = Gmeow + "validFrom"
	ValidUntil = Gmeow + "validUntil"

	// Assertion time — when an agent observed/asserted the claim (vCard REV, email
	// Date). On the CLAIM, only when the source asserts it.
	AssertedAt = Gmeow + "assertedAt"

	// Derived terminus-ante-quem — recorded no later than the carrier's mtime, when
	// no stronger time is known. On the CLAIM, low gmeow:confidence. Code-populated.
	RecordedNoLaterThan = Gmeow + "recordedNoLaterThan"

	// Carrier time + source identity — on the gmeow:Source (the file/envelope).
	SourceClass      = Gmeow + "Source"
	SourceModifiedAt = Gmeow + "sourceModifiedAt"
	ContentDigest    = Gmeow + "contentDigest"
	SourceLocation   = Gmeow + "sourceLocation"
	HasSource        = Gmeow + "hasSource"

	// Transaction time + ingestion event — on the gmeow:ImportActivity.
	ImportActivityClass = Gmeow + "ImportActivity"
	IngestedAt          = Gmeow + "ingestedAt"
	WasGeneratedBy      = Gmeow + "wasGeneratedBy"
	WasDerivedFrom      = Gmeow + "wasDerivedFrom"

	// Statement-level annotation properties (already used across the importer).
	Confidence = Gmeow + "confidence"

	// OWL-Time source forms the parser recognizes for VALID time on reified tenure
	// nodes (index.ttl style), plus the gmeow superset interval terms.
	TimeHasBeginning  = Time + "hasBeginning"
	TimeHasEnd        = Time + "hasEnd"
	TimeInXSDDateTime = Time + "inXSDDateTime"
	SchemaStartDate   = Schema + "startDate"
	SchemaEndDate     = Schema + "endDate"
	DuringInterval    = Gmeow + "duringInterval"
	StartedAtTime     = Gmeow + "startedAtTime"
	EndedAtTime       = Gmeow + "endedAtTime"
)
