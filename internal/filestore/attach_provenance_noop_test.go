// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

// TestAttachProvenanceNoOpWhenAlreadySeen pins the "already-seen object is a
// 100% no-op" contract: re-attaching a provenance ref that is already present
// performs no manifest rewrite and reports changed=false, while a genuinely new
// source/version reports changed=true.
func TestAttachProvenanceNoOpWhenAlreadySeen(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	prov := []contracts.Provenance{{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "msg-1",
		ExternalVersion: "v1",
	}}

	changed, err := store.AttachProvenance(ctx, digest, prov)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("first AttachProvenance of a new source ref must report changed")
	}

	before, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}

	// Re-observe the identical ref: must be a pure no-op.
	changed, err = store.AttachProvenance(ctx, digest, prov)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("re-attaching an already-seen source ref must be a no-op")
	}

	after, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatalf(
			"no-op AttachProvenance must not rewrite the manifest: updated_at %s -> %s",
			before.UpdatedAt, after.UpdatedAt,
		)
	}

	// A genuinely new version is a real change.
	changed, err = store.AttachProvenance(ctx, digest, []contracts.Provenance{{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "msg-1",
		ExternalVersion: "v2",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("attaching a new external version must report changed")
	}
}
