// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import "testing"

func TestContactRefCandidatesAddsEntityIRIForBareULID(t *testing.T) {
	const entity = "01KTCY5XZ6SE7QMEXJ8VQM76ZY"

	got := contactRefCandidates(entity)
	want := []string{entity, contactEntityPrefix + entity}
	if len(got) != len(want) {
		t.Fatalf("contactRefCandidates length = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("contactRefCandidates[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestContactRefCandidatesLeavesEntityIRIUnchanged(t *testing.T) {
	const entityIRI = contactEntityPrefix + "01KTCY5XZ6SE7QMEXJ8VQM76ZY"

	got := contactRefCandidates(entityIRI)
	if len(got) != 1 || got[0] != entityIRI {
		t.Fatalf("contactRefCandidates = %#v, want only %q", got, entityIRI)
	}
}
