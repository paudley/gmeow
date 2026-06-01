// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestContactFactProtoRoundTrip(t *testing.T) {
	facts := []contracts.ContactFact{{
		SourceDigest:  contracts.ObjectDigest("sha256:source"),
		StatementHash: "statement",
		ContactID:     "contact:alice",
		FactKind:      "email",
		Value:         "alice@example.test",
		Predicate:     "vcard:hasEmail",
		ValidFrom:     "2024-01-01",
		ValidUntil:    "2025-01-01",
		Historical:    true,
	}}

	roundTrip := fromPBContactFacts(toPBContactFacts(facts))
	if len(roundTrip) != 1 || roundTrip[0] != facts[0] {
		t.Fatalf("contact fact round trip = %#v, want %#v", roundTrip, facts)
	}
}

func TestContactVectorResultProtoRoundTrip(t *testing.T) {
	results := []contracts.ContactVectorSearchResult{{
		ContactID:    "contact:alice",
		DisplayName:  "Alice Example",
		PrimaryEmail: "alice@example.test",
		Model:        "embed",
		EmbeddingID:  "embedding",
		InputHash:    "input",
		TextPreview:  "Alice Example",
		Distance:     0.25,
		Dimensions:   3,
	}}

	roundTrip := fromPBContactVectorResults(toPBContactVectorResults(results))
	if len(roundTrip) != 1 || roundTrip[0] != results[0] {
		t.Fatalf("contact vector round trip = %#v, want %#v", roundTrip, results)
	}
}
